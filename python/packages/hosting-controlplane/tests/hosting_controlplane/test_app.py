# Copyright (c) Microsoft. All rights reserved.

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import httpx
import pytest

from agent_framework_hosting_controlplane import (
    CompilerOptions,
    ControlPlane,
    DagDefinition,
    DagNode,
    FoundryNodeSpec,
    GitHubRepoRef,
    InMemoryDagRegistry,
    InMemoryRunStore,
    ReusableActionRef,
    compile_definition,
    create_app,
)
from agent_framework_hosting_controlplane._github_client import GitHubActionsClient
from agent_framework_hosting_controlplane._models import GitHubRunRef


class FakeGitHubActionsClient:
    def __init__(self) -> None:
        self.dispatched: list[dict[str, object]] = []
        self.cancelled: list[int] = []
        self.rerun: list[int] = []

    async def dispatch_workflow(self, *, workflow_file: str, ref: str, inputs: dict[str, str]) -> None:
        self.dispatched.append({"workflow_file": workflow_file, "ref": ref, "inputs": dict(inputs)})

    async def find_run_by_name(self, *, workflow_file: str, run_name: str, created_after: object = None):  # type: ignore[no-untyped-def]
        return None

    async def get_run(self, *, run_id: int) -> GitHubRunRef:
        return GitHubRunRef(run_id=run_id)

    async def list_jobs(self, *, run_id: int) -> list[object]:
        return []

    async def cancel_run(self, *, run_id: int) -> None:
        self.cancelled.append(run_id)

    async def rerun_failed_jobs(self, *, run_id: int) -> None:
        self.rerun.append(run_id)


async def _make_app_client(control_plane: ControlPlane) -> httpx.AsyncClient:
    app = create_app(control_plane)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.fixture
async def control_plane() -> AsyncIterator[ControlPlane]:
    github_client: GitHubActionsClient = FakeGitHubActionsClient()  # type: ignore[assignment]
    control_plane = ControlPlane(
        dag_registry=InMemoryDagRegistry(),
        run_store=InMemoryRunStore(),
        github_client=github_client,
    )
    definition = DagDefinition(
        dag_id="report",
        nodes=(
            DagNode(id="extract", foundry=FoundryNodeSpec(agent_id="extract-agent")),
            DagNode(id="summarize", foundry=FoundryNodeSpec(agent_id="summarize-agent"), depends_on=("extract",)),
        ),
    )
    options = CompilerOptions(
        repo=GitHubRepoRef(owner="my-org", repo="my-dags"),
        action=ReusableActionRef(uses="my-org/foundry-invoke-action@v1"),
    )
    version = compile_definition(definition, options=options)
    await control_plane.register_dag(version)
    yield control_plane


async def test_launch_run_route(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        response = await client.post("/dags/report/runs", json={"inputs": {"x": 1}})
    assert response.status_code == 202
    body = response.json()
    assert body["dag_id"] == "report"
    assert body["status"] == "queued"


async def test_launch_unknown_dag_returns_404(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        response = await client.post("/dags/missing/runs", json={})
    assert response.status_code == 404


async def test_get_run_route(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        created = await client.post("/dags/report/runs", json={})
        run_id = created.json()["id"]
        response = await client.get(f"/runs/{run_id}")
    assert response.status_code == 200
    assert response.json()["id"] == run_id


async def test_get_run_unknown_returns_404(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        response = await client.get("/runs/does-not-exist")
    assert response.status_code == 404


async def test_get_nodes_route(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        created = await client.post("/dags/report/runs", json={})
        run_id = created.json()["id"]
        response = await client.get(f"/runs/{run_id}/nodes")
    assert response.status_code == 200
    node_ids = {node["node_id"] for node in response.json()}
    assert node_ids == {"extract", "summarize"}


async def test_get_events_route(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        created = await client.post("/dags/report/runs", json={})
        run_id = created.json()["id"]
        response = await client.get(f"/runs/{run_id}/events")
    assert response.status_code == 200
    event_types = [event["type"] for event in response.json()]
    assert "run.registered" in event_types


async def test_cancel_route(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        created = await client.post("/dags/report/runs", json={})
        run_id = created.json()["id"]
        response = await client.post(f"/runs/{run_id}/cancel")
    assert response.status_code == 200
    assert response.json()["status"] == "cancelling"


async def test_retry_route_requires_failed_run(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        created = await client.post("/dags/report/runs", json={})
        run_id = created.json()["id"]
        response = await client.post(f"/runs/{run_id}/retry")
    assert response.status_code == 409


async def test_resume_route_requires_checkpoint(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        created = await client.post("/dags/report/runs", json={})
        run_id = created.json()["id"]
        response = await client.post(f"/runs/{run_id}/resume", json={})
    assert response.status_code == 409


async def test_webhook_route_accepts_payload_without_secret(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        created = await client.post("/dags/report/runs", json={})
        run_id = created.json()["id"]
        payload = {
            "workflow_run": {
                "id": 1,
                "display_title": f"foundry-dag/report/{run_id}",
                "status": "in_progress",
            }
        }
        response = await client.post(
            "/webhooks/github",
            content=json.dumps(payload),
            headers={"X-GitHub-Event": "workflow_run", "Content-Type": "application/json"},
        )
        assert response.status_code == 202

        fetched = await client.get(f"/runs/{run_id}")
        assert fetched.json()["status"] == "running"


async def test_webhook_route_rejects_invalid_json(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        response = await client.post(
            "/webhooks/github",
            content=b"{not json",
            headers={"X-GitHub-Event": "workflow_run", "Content-Type": "application/json"},
        )
    assert response.status_code == 400


async def test_webhook_route_rejects_non_object_json(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        response = await client.post(
            "/webhooks/github",
            content=b"[1, 2, 3]",
            headers={"X-GitHub-Event": "workflow_run", "Content-Type": "application/json"},
        )
    assert response.status_code == 400


async def test_webhook_route_rejects_bad_signature_when_secret_configured() -> None:
    github_client: GitHubActionsClient = FakeGitHubActionsClient()  # type: ignore[assignment]
    control_plane = ControlPlane(
        dag_registry=InMemoryDagRegistry(),
        run_store=InMemoryRunStore(),
        github_client=github_client,
        webhook_secret="shh",
    )
    async with await _make_app_client(control_plane) as client:
        response = await client.post(
            "/webhooks/github",
            content=b'{"workflow_run": {}}',
            headers={
                "X-GitHub-Event": "workflow_run",
                "X-Hub-Signature-256": "sha256=0000",
                "Content-Type": "application/json",
            },
        )
    assert response.status_code == 401


async def test_sse_stream_replays_backlog_then_live_events(control_plane: ControlPlane) -> None:
    # Exercised directly against the generator (rather than through a real streaming HTTP
    # response) so the test is deterministic: a fake `Request` reports "disconnected" as soon
    # as the persisted backlog has been replayed, instead of racing a real heartbeat/live-event
    # timer under the ASGI test transport.
    from agent_framework_hosting_controlplane._app import _sse_event_generator

    run = await control_plane.launch("report")

    class _DisconnectingRequest:
        async def is_disconnected(self) -> bool:
            return True

    frames = [
        frame async for frame in _sse_event_generator(control_plane, run.id, _DisconnectingRequest(), after_seq=None)
    ]  # type: ignore[arg-type]

    ids = [line for frame in frames for line in frame.splitlines() if line.startswith("id:")]
    assert ids == ["id: 1", "id: 2"]
    event_types = [line for frame in frames for line in frame.splitlines() if line.startswith("event:")]
    assert event_types == ["event: run.registered", "event: run.status_changed"]


async def test_sse_stream_does_not_lose_event_persisted_during_initial_replay(control_plane: ControlPlane) -> None:
    from agent_framework_hosting_controlplane import RunEventType
    from agent_framework_hosting_controlplane._app import _sse_event_generator

    run = await control_plane.launch("report")
    original_list_events = control_plane.list_events
    first_call = True

    async def list_events_with_concurrent_publish(run_id: str, *, after_seq: int | None = None):  # type: ignore[no-untyped-def]
        nonlocal first_call
        events = await original_list_events(run_id, after_seq=after_seq)
        if first_call:
            first_call = False
            await control_plane._emit(run_id, RunEventType.RUN_RECONCILED)  # pyright: ignore[reportPrivateUsage]
        return events

    control_plane.list_events = list_events_with_concurrent_publish  # type: ignore[method-assign]

    class _DisconnectAfterCatchupRequest:
        calls = 0

        async def is_disconnected(self) -> bool:
            self.calls += 1
            return self.calls == 1

    frames = [
        frame
        async for frame in _sse_event_generator(
            control_plane,
            run.id,
            _DisconnectAfterCatchupRequest(),
            after_seq=None,
        )
    ]  # type: ignore[arg-type]

    ids = [line for frame in frames for line in frame.splitlines() if line.startswith("id:")]
    assert ids == ["id: 1", "id: 2", "id: 3"]


# Note: a real end-to-end streaming request through `create_app`'s SSE route is deliberately not
# exercised with `httpx.ASGITransport` here. `ASGITransport.handle_async_request` runs the whole
# ASGI call to completion before returning a `Response`/stream to the test client, which means it
# cannot represent an intentionally long-lived connection like this one (the heartbeat loop only
# ever ends via a real client disconnect). `_sse_event_generator` is covered directly above instead.


async def test_sse_stream_unknown_run_returns_404(control_plane: ControlPlane) -> None:
    async with await _make_app_client(control_plane) as client:
        response = await client.get("/runs/does-not-exist/events/stream")
    assert response.status_code == 404


async def test_event_broker_enforces_subscriber_cap() -> None:
    from agent_framework_hosting_controlplane import EventBroker, SubscriberLimitExceededError

    broker = EventBroker(max_subscribers_per_run=1)
    with broker.subscribe("run-1"), pytest.raises(SubscriberLimitExceededError), broker.subscribe("run-1"):
        pass  # pragma: no cover - the cap must reject before entering this block


async def test_event_broker_drops_events_for_a_slow_subscriber_instead_of_growing_unbounded() -> None:
    from agent_framework_hosting_controlplane import EventBroker, RunEvent, RunEventType

    broker = EventBroker(max_queue_size=2)
    with broker.subscribe("run-1") as queue:
        for _ in range(5):
            broker.publish(RunEvent(seq=0, run_id="run-1", type=RunEventType.RUN_STATUS_CHANGED))
        # The queue never grows past its configured bound, even though 5 events were published.
        assert queue.qsize() <= 2
