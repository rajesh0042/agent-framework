# Copyright (c) Microsoft. All rights reserved.

"""FastAPI app factory for the hosted DAG control-plane API.

Routes (exactly):

- `POST /dags/{dag_id}/runs` - launch a run.
- `GET /runs/{run_id}` - get run status.
- `GET /runs/{run_id}/nodes` - get per-node state.
- `GET /runs/{run_id}/events` - get the run's event log.
- `GET /runs/{run_id}/events/stream` - SSE stream of the run's events.
- `POST /runs/{run_id}/cancel` - cancel a run.
- `POST /runs/{run_id}/retry` - retry a failed run's failed jobs.
- `POST /runs/{run_id}/resume` - launch a new, checkpoint-linked run.
- `POST /webhooks/github` - ingest a GitHub webhook delivery.

Authentication/authorization is entirely app-provided: pass FastAPI
`Depends(...)` dependencies via `dependencies=` to `create_app` (applied to
every route) or wrap the returned `APIRouter` further before mounting it.
This module itself only enforces the GitHub webhook HMAC signature, and only
when the `ControlPlane` was constructed with a `webhook_secret`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Sequence
from typing import Any, cast

from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from fastapi.params import Depends
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from ._control_plane import ControlPlane
from ._exceptions import (
    DagNotFoundError,
    InvalidRunStateError,
    RunNotFoundError,
    SubscriberLimitExceededError,
    WebhookSignatureError,
)
from ._models import NodeState, Run, RunEvent

__all__ = ["LaunchRunRequest", "ResumeRunRequest", "create_app"]

logger = logging.getLogger("agent_framework_hosting_controlplane")

#: How long to wait for a new event before sending a heartbeat comment.
_SSE_POLL_TIMEOUT_SECONDS = 15.0


class LaunchRunRequest(BaseModel):
    """Request body for `POST /dags/{dag_id}/runs`."""

    model_config = ConfigDict(extra="forbid")

    version: str | None = Field(default=None, description="Specific DAG version; defaults to the latest.")
    inputs: dict[str, Any] = Field(default_factory=dict, description="JSON-serializable run inputs.")
    idempotency_key: str | None = Field(default=None, description="Idempotency key scoped to this dag_id.")


class ResumeRunRequest(BaseModel):
    """Request body for `POST /runs/{run_id}/resume`."""

    model_config = ConfigDict(extra="forbid")

    inputs: dict[str, Any] | None = Field(default=None, description="Overrides for the resumed run's inputs.")
    idempotency_key: str | None = Field(default=None, description="Idempotency key scoped to the resumed dag_id.")


def create_app(
    control_plane: ControlPlane,
    *,
    dependencies: Sequence[Depends] | None = None,
    title: str = "Agent Framework Hosting Control Plane",
) -> FastAPI:
    """Build a FastAPI app exposing the hosted DAG control-plane routes.

    Args:
        control_plane: The `ControlPlane` instance backing every route.
        dependencies: Optional FastAPI dependencies (for example
            authentication) applied to every route in this app.
        title: OpenAPI title for the generated app.

    Returns:
        A configured `FastAPI` application. The caller owns serving it
        (uvicorn, inclusion in a larger app, etc).
    """
    app = FastAPI(title=title, dependencies=list(dependencies) if dependencies else None)
    router = APIRouter()

    @router.post("/dags/{dag_id}/runs", response_model=Run, status_code=202)
    async def launch_run(dag_id: str, body: LaunchRunRequest) -> Run:
        return await control_plane.launch(
            dag_id,
            version=body.version,
            inputs=body.inputs,
            idempotency_key=body.idempotency_key,
        )

    @router.get("/runs/{run_id}", response_model=Run)
    async def get_run(run_id: str) -> Run:
        return await control_plane.get_run(run_id)

    @router.get("/runs/{run_id}/nodes", response_model=list[NodeState])
    async def get_nodes(run_id: str) -> list[NodeState]:
        return await control_plane.list_nodes(run_id)

    @router.get("/runs/{run_id}/events", response_model=list[RunEvent])
    async def get_events(run_id: str, after_seq: int | None = None) -> list[RunEvent]:
        return await control_plane.list_events(run_id, after_seq=after_seq)

    @router.get("/runs/{run_id}/events/stream")
    async def stream_events(
        run_id: str,
        request: Request,
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
    ) -> StreamingResponse:
        # Validate the run exists (and surface 404 via the exception handler below) before
        # opening the stream, so a bad run_id doesn't silently hang as an empty SSE connection.
        await control_plane.get_run(run_id)
        after_seq = int(last_event_id) if last_event_id and last_event_id.isdigit() else None
        generator = _sse_event_generator(control_plane, run_id, request, after_seq=after_seq)
        return StreamingResponse(
            generator,
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.post("/runs/{run_id}/cancel", response_model=Run)
    async def cancel_run(run_id: str) -> Run:
        return await control_plane.cancel(run_id)

    @router.post("/runs/{run_id}/retry", response_model=Run)
    async def retry_run(run_id: str) -> Run:
        return await control_plane.retry(run_id)

    @router.post("/runs/{run_id}/resume", response_model=Run)
    async def resume_run(run_id: str, body: ResumeRunRequest) -> Run:
        return await control_plane.resume(run_id, inputs=body.inputs, idempotency_key=body.idempotency_key)

    @router.post("/webhooks/github", status_code=202)
    async def github_webhook(request: Request) -> JSONResponse:
        raw_body = await request.body()
        signature = request.headers.get("X-Hub-Signature-256")
        event_name = request.headers.get("X-GitHub-Event", "")
        try:
            payload: Any = json.loads(raw_body) if raw_body else {}
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Invalid JSON payload.") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Webhook payload must be a JSON object.")
        webhook_payload = cast("dict[str, Any]", payload)
        await control_plane.ingest_webhook(
            event_name=event_name, payload=webhook_payload, signature_header=signature, raw_body=raw_body
        )
        return JSONResponse({"status": "accepted"}, status_code=202)

    app.include_router(router)
    _install_exception_handlers(app)
    return app


async def _sse_event_generator(
    control_plane: ControlPlane,
    run_id: str,
    request: Request,
    *,
    after_seq: int | None,
) -> AsyncIterator[str]:
    """Yield SSE frames: persisted backlog first, then live events, with periodic heartbeats.

    Exiting this generator (including via `GeneratorExit` on client disconnect)
    always releases the broker subscription through the `with` block below, so
    a dropped connection cannot leak a subscriber slot.
    """
    backlog = await control_plane.list_events(run_id, after_seq=after_seq)
    last_seq = after_seq
    for event in backlog:
        yield _format_sse(event)
        last_seq = event.seq

    try:
        with control_plane.event_broker.subscribe(run_id) as queue:
            while True:
                if await request.is_disconnected():
                    return
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=_SSE_POLL_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
                    continue
                if last_seq is not None and event.seq <= last_seq:
                    continue  # Already sent as part of the backlog above; avoid duplicate delivery.
                last_seq = event.seq
                yield _format_sse(event)
    except SubscriberLimitExceededError as exc:
        yield f"event: error\ndata: {json.dumps({'detail': str(exc)})}\n\n"


def _format_sse(event: RunEvent) -> str:
    data = event.model_dump_json()
    return f"id: {event.seq}\nevent: {event.type.value}\ndata: {data}\n\n"


def _install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(DagNotFoundError)
    def _dag_not_found(_: Request, exc: DagNotFoundError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=404)

    @app.exception_handler(RunNotFoundError)
    def _run_not_found(_: Request, exc: RunNotFoundError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=404)

    @app.exception_handler(InvalidRunStateError)
    def _invalid_state(_: Request, exc: InvalidRunStateError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(WebhookSignatureError)
    def _bad_signature(_: Request, exc: WebhookSignatureError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=401)

    @app.exception_handler(SubscriberLimitExceededError)
    def _too_many_subscribers(_: Request, exc: SubscriberLimitExceededError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=429)
