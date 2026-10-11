# Copyright (c) Microsoft. All rights reserved.

"""The `ControlPlane` service: register, launch, observe, and manage DAG runs.

This module is the only place that understands how to turn GitHub Actions'
raw status/conclusion vocabulary and webhook payloads into the normalized
`RunStatus`/`NodeStatus` state machines defined in `_models.py`, and the only
place that mutates `Run`/`NodeState` snapshots. All state transitions go
through `_apply_run_update`/`_apply_node_update`, which enforce monotonic,
validated transitions and make duplicate webhook/reconciliation deliveries a
no-op.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import uuid
from collections.abc import Callable, Generator, Mapping
from typing import Any, cast

from ._exceptions import (
    DagNotFoundError,
    InvalidRunStateError,
    RunNotFoundError,
    SubscriberLimitExceededError,
    WebhookSignatureError,
)
from ._github_client import GitHubActionsClient
from ._models import (
    TERMINAL_RUN_STATUSES,
    CheckpointRef,
    DagVersion,
    FailureDetail,
    GitHubJobRef,
    GitHubRunRef,
    NodeState,
    NodeStatus,
    Run,
    RunEvent,
    RunEventType,
    RunStatus,
    utcnow,
    validate_node_id,
    validate_node_transition,
    validate_run_transition,
)
from ._registry import DagRegistry
from ._store import RunStore
from ._webhooks import verify_github_signature

__all__ = ["ControlPlane", "EventBroker"]

logger = logging.getLogger("agent_framework_hosting_controlplane")

_RUN_NAME_PREFIX = "foundry-dag/"


class EventBroker:
    """In-process pub/sub for `RunEvent`s, used to drive the SSE endpoint.

    Each subscriber gets a bounded `asyncio.Queue`. A slow subscriber that
    cannot keep up has new events dropped (not buffered without bound); it
    can always recover by reconnecting and replaying from the durable event
    log via `Last-Event-ID`. The per-run subscriber count is also capped so a
    single run cannot be used to exhaust server connections/memory.
    """

    def __init__(self, *, max_queue_size: int = 256, max_subscribers_per_run: int = 64) -> None:
        self._lock = threading.Lock()
        self._subscribers: dict[str, set[asyncio.Queue[RunEvent]]] = {}
        self._max_queue_size = max_queue_size
        self._max_subscribers_per_run = max_subscribers_per_run

    def publish(self, event: RunEvent) -> None:
        """Deliver `event` to every current subscriber of `event.run_id`."""
        with self._lock:
            queues = list(self._subscribers.get(event.run_id, ()))
        for queue in queues:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.debug("Dropping event %s for a slow SSE subscriber of run %s.", event.id, event.run_id)

    @contextlib.contextmanager
    def subscribe(self, run_id: str) -> Generator[asyncio.Queue[RunEvent]]:
        """Register a bounded live subscription for `run_id` for the duration of the context."""
        queue: asyncio.Queue[RunEvent] = asyncio.Queue(maxsize=self._max_queue_size)
        with self._lock:
            subscribers = self._subscribers.setdefault(run_id, set())
            if len(subscribers) >= self._max_subscribers_per_run:
                raise SubscriberLimitExceededError(
                    f"Run {run_id!r} already has the maximum of {self._max_subscribers_per_run} live subscribers."
                )
            subscribers.add(queue)
        try:
            yield queue
        finally:
            with self._lock:
                remaining = self._subscribers.get(run_id)
                if remaining is not None:
                    remaining.discard(queue)
                    if not remaining:
                        del self._subscribers[run_id]


class ControlPlane:
    """Coordinates DAG registration and the lifecycle of DAG runs on GitHub Actions."""

    def __init__(
        self,
        *,
        dag_registry: DagRegistry,
        run_store: RunStore,
        github_client: GitHubActionsClient,
        webhook_secret: str | None = None,
        event_broker: EventBroker | None = None,
        clock: Callable[[], Any] = utcnow,
    ) -> None:
        """Create a `ControlPlane`.

        Args:
            dag_registry: Versioned DAG registration store.
            run_store: Run/node/event state store.
            github_client: Client used to dispatch and poll GitHub Actions.
            webhook_secret: Shared secret used to validate inbound webhook
                signatures. When `None`, webhook signature validation is
                skipped entirely (only appropriate for local development or
                when an app-level gateway already validates deliveries).
            event_broker: Broker used to fan out newly appended events to SSE
                subscribers; a private one is created if omitted.
            clock: Injectable clock, primarily for deterministic tests.
        """
        self.dag_registry = dag_registry
        self.run_store = run_store
        self.github_client = github_client
        self.webhook_secret = webhook_secret
        self.event_broker = event_broker or EventBroker()
        self._clock = clock
        self._run_locks = tuple(asyncio.Lock() for _ in range(64))

    # ----------------------------------------------------------------- #
    # Registration
    # ----------------------------------------------------------------- #

    async def register_dag(self, dag_version: DagVersion) -> DagVersion:
        """Register an already-compiled `DagVersion` (see `agent_framework_hosting_controlplane._compiler`)."""
        return await self.dag_registry.register(dag_version)

    async def get_dag_version(self, dag_id: str, version: str | None = None) -> DagVersion:
        """Return a registered `DagVersion`, raising if it is not registered."""
        dag_version = await self.dag_registry.get(dag_id, version)
        if dag_version is None:
            raise DagNotFoundError(f"DAG {dag_id!r} (version={version!r}) is not registered.")
        return dag_version

    # ----------------------------------------------------------------- #
    # Launch / observe
    # ----------------------------------------------------------------- #

    async def launch(
        self,
        dag_id: str,
        *,
        version: str | None = None,
        inputs: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        parent_run_id: str | None = None,
        resume_from_checkpoint: CheckpointRef | None = None,
    ) -> Run:
        """Launch a new run of `dag_id` and return immediately.

        The returned `Run` reflects only that the launch attempt was
        recorded and (if dispatch succeeded) queued; it does not wait for
        the DAG to make progress. If `idempotency_key` matches a
        previously-launched run for this `dag_id`, that existing run is
        returned unchanged and no new GitHub Actions dispatch is made.

        Args:
            dag_id: The DAG to launch.
            version: Specific DAG version to launch; defaults to the most
                recently registered version.
            inputs: JSON-serializable run inputs (identifiers/URIs only; the
                control plane never carries large payloads).
            idempotency_key: Client-supplied idempotency key, scoped to `dag_id`.
            parent_run_id: Set by `resume()` to link a resumed run to its parent.
            resume_from_checkpoint: Set by `resume()`.

        Returns:
            The newly created (or, for a duplicate idempotency key, the
            previously created) `Run`.
        """
        dag_version = await self.get_dag_version(dag_id, version)

        candidate = Run(
            id=f"run-{uuid.uuid4().hex}",
            dag_id=dag_id,
            dag_version=dag_version.version,
            inputs=dict(inputs) if inputs else {},
            idempotency_key=idempotency_key,
            parent_run_id=parent_run_id,
            resume_from_checkpoint=resume_from_checkpoint,
        )
        stored = await self.run_store.create_run(candidate)
        if stored.id != candidate.id:
            # An existing run already owns this (dag_id, idempotency_key) pair: idempotent replay.
            return stored

        for node in dag_version.definition.nodes:
            await self.run_store.upsert_node_state(NodeState(run_id=stored.id, node_id=node.id))
        await self._emit(stored.id, RunEventType.RUN_REGISTERED, data={"dag_version": dag_version.version})

        dispatch_inputs = {
            "framework_run_id": stored.id,
            "dag_id": dag_id,
            "dag_version": dag_version.version,
            "inputs_json": _json_dumps(stored.inputs),
            "resume_checkpoint_uri": resume_from_checkpoint.uri if resume_from_checkpoint else "",
        }
        failure: FailureDetail | None = None
        try:
            await self.github_client.dispatch_workflow(
                workflow_file=dag_version.workflow_file,
                ref=dag_version.ref,
                inputs=dispatch_inputs,
            )
        except Exception as exc:
            new_status = RunStatus.FAILED
            failure = FailureDetail(code="dispatch_failed", message=str(exc), retryable=True)
        else:
            new_status = RunStatus.QUEUED

        updated = stored.model_copy(update={"status": new_status, "failure": failure, "updated_at": self._clock()})
        await self.run_store.update_run(updated)
        await self._emit(stored.id, RunEventType.RUN_STATUS_CHANGED, data={"reason": "launch"})
        return updated

    async def get_run(self, run_id: str) -> Run:
        """Return a run, raising `RunNotFoundError` if it does not exist."""
        run = await self.run_store.get_run(run_id)
        if run is None:
            raise RunNotFoundError(f"Run {run_id!r} was not found.")
        return run

    async def list_nodes(self, run_id: str) -> list[NodeState]:
        """Return the current state of every node in `run_id`."""
        await self.get_run(run_id)  # raises RunNotFoundError for unknown runs
        return await self.run_store.list_node_states(run_id)

    async def list_events(self, run_id: str, *, after_seq: int | None = None) -> list[RunEvent]:
        """Return the event log for `run_id`, optionally only events after `after_seq`."""
        await self.get_run(run_id)  # raises RunNotFoundError for unknown runs
        return await self.run_store.list_events(run_id, after_seq=after_seq)

    # ----------------------------------------------------------------- #
    # Cancel / retry / resume
    # ----------------------------------------------------------------- #

    async def cancel(self, run_id: str) -> Run:
        """Request cancellation of a run.

        This is idempotent: cancelling an already-cancelling or already
        terminal run simply returns its current state. Cancellation itself
        is best-effort on GitHub's side (see `GitHubActionsClient.cancel_run`).
        """
        async with self._lock_for(run_id):
            run = await self.get_run(run_id)
            if run.status in TERMINAL_RUN_STATUSES or run.status == RunStatus.CANCELLING:
                return run
            if not validate_run_transition(run.status, RunStatus.CANCELLING):
                raise InvalidRunStateError(f"Run {run_id!r} cannot be cancelled from status {run.status.value!r}.")
            updated = run.model_copy(update={"status": RunStatus.CANCELLING, "updated_at": self._clock()})
            await self.run_store.update_run(updated)
        if run.github_run is not None and run.github_run.run_id is not None:
            await self.github_client.cancel_run(run_id=run.github_run.run_id)
        await self._emit(run_id, RunEventType.RUN_STATUS_CHANGED, data={"reason": "cancel"})
        return updated

    async def retry(self, run_id: str) -> Run:
        """Re-run the failed jobs of a failed run.

        Only valid for a run whose normalized status is `FAILED` and that has
        a correlated GitHub Actions run id.
        """
        async with self._lock_for(run_id):
            run = await self.get_run(run_id)
            if run.status != RunStatus.FAILED:
                raise InvalidRunStateError(f"Run {run_id!r} is {run.status.value!r}; only a failed run can be retried.")
            if run.github_run is None or run.github_run.run_id is None:
                raise InvalidRunStateError(f"Run {run_id!r} has no correlated GitHub Actions run to retry.")
            await self.github_client.rerun_failed_jobs(run_id=run.github_run.run_id)
            updated = run.model_copy(update={"status": RunStatus.QUEUED, "failure": None, "updated_at": self._clock()})
            await self.run_store.update_run(updated)

        for state in await self.run_store.list_node_states(run_id):
            if state.status != NodeStatus.FAILED:
                continue
            # Explicitly authorized reset for an operator-initiated retry; bypasses the generic
            # monotonic webhook-transition validator by design (that validator guards against
            # out-of-order webhook delivery, not deliberate operator actions).
            reset_state = state.model_copy(
                update={
                    "status": NodeStatus.QUEUED,
                    "attempt": state.attempt + 1,
                    "failure": None,
                    "updated_at": self._clock(),
                }
            )
            await self.run_store.upsert_node_state(reset_state)
            await self._emit(run_id, RunEventType.NODE_STATUS_CHANGED, node_id=state.node_id, data={"reason": "retry"})

        await self._emit(run_id, RunEventType.RUN_STATUS_CHANGED, data={"reason": "retry"})
        return updated

    async def resume(
        self,
        run_id: str,
        *,
        inputs: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> Run:
        """Launch a new, linked run that resumes `run_id` from its latest checkpoint.

        Raises:
            InvalidRunStateError: If no node in `run_id` has recorded a checkpoint reference.
        """
        prior = await self.get_run(run_id)
        checkpoint = _latest_checkpoint(await self.run_store.list_node_states(run_id))
        if checkpoint is None:
            raise InvalidRunStateError(f"Run {run_id!r} has no recorded checkpoint reference to resume from.")
        return await self.launch(
            prior.dag_id,
            version=prior.dag_version,
            inputs=inputs if inputs is not None else prior.inputs,
            idempotency_key=idempotency_key,
            parent_run_id=prior.id,
            resume_from_checkpoint=checkpoint,
        )

    # ----------------------------------------------------------------- #
    # Webhook ingestion
    # ----------------------------------------------------------------- #

    async def ingest_webhook(
        self,
        *,
        event_name: str,
        payload: Mapping[str, Any],
        signature_header: str | None,
        raw_body: bytes,
    ) -> None:
        """Validate and apply a single GitHub webhook delivery.

        Args:
            event_name: The `X-GitHub-Event` header value (for example `"workflow_run"`).
            payload: The parsed JSON webhook body.
            signature_header: The `X-Hub-Signature-256` header value, if present.
            raw_body: The raw request body bytes (required for signature validation).

        Raises:
            WebhookSignatureError: If a webhook secret is configured and the
                signature does not match.
        """
        if self.webhook_secret is not None and not verify_github_signature(
            secret=self.webhook_secret, payload=raw_body, signature_header=signature_header
        ):
            raise WebhookSignatureError("GitHub webhook signature validation failed.")

        if event_name == "workflow_run":
            await self._handle_workflow_run_event(payload)
        elif event_name == "workflow_job":
            await self._handle_workflow_job_event(payload)
        # Other event types (e.g. "ping") are accepted (so GitHub's webhook setup check succeeds)
        # but intentionally produce no state change.

    async def _handle_workflow_run_event(self, payload: Mapping[str, Any]) -> None:
        workflow_run_raw = payload.get("workflow_run")
        if not isinstance(workflow_run_raw, Mapping):
            return
        workflow_run = cast("Mapping[str, Any]", workflow_run_raw)
        run_name = workflow_run.get("display_title") or workflow_run.get("name")
        parsed = _parse_run_name(run_name) if isinstance(run_name, str) else None
        if parsed is None:
            return
        _dag_id, framework_run_id = parsed
        if await self.run_store.get_run(framework_run_id) is None:
            return

        github_run = GitHubRunRef(
            run_id=workflow_run.get("id"),
            html_url=workflow_run.get("html_url"),
            status=workflow_run.get("status"),
            conclusion=workflow_run.get("conclusion"),
        )
        status = _map_run_status(str(workflow_run.get("status", "")), workflow_run.get("conclusion"))
        failure = _run_failure_from_conclusion(status, workflow_run.get("conclusion"))
        await self._apply_run_update(
            framework_run_id,
            status,
            github_run=github_run,
            failure=failure,
            event_type=RunEventType.WEBHOOK_RECEIVED,
            event_data={"source": "workflow_run"},
        )

    async def _handle_workflow_job_event(self, payload: Mapping[str, Any]) -> None:
        workflow_job_raw = payload.get("workflow_job")
        if not isinstance(workflow_job_raw, Mapping):
            return
        workflow_job = cast("Mapping[str, Any]", workflow_job_raw)
        github_run_id = workflow_job.get("run_id")
        if not isinstance(github_run_id, int):
            return
        run = await self.run_store.get_run_by_github_run_id(github_run_id)
        if run is None:
            return
        node_id = workflow_job.get("name")
        if not isinstance(node_id, str):
            return
        try:
            validate_node_id(node_id)
        except ValueError:
            return  # Not a job this control plane generated; ignore defensively.

        status = _map_node_status(str(workflow_job.get("status", "")), workflow_job.get("conclusion"))
        job_ref = GitHubJobRef(
            job_id=workflow_job.get("id"),
            name=node_id,
            html_url=workflow_job.get("html_url"),
            status=workflow_job.get("status"),
            conclusion=workflow_job.get("conclusion"),
        )
        failure = _node_failure_from_conclusion(status, workflow_job.get("conclusion"))
        await self._apply_node_update(
            run.id,
            node_id,
            status,
            github_job=job_ref,
            failure=failure,
            event_type=RunEventType.WEBHOOK_RECEIVED,
        )

    # ----------------------------------------------------------------- #
    # Reconciliation
    # ----------------------------------------------------------------- #

    async def reconcile(self, run_id: str) -> Run:
        """Poll GitHub Actions directly to catch up on any missed webhook deliveries.

        Safe to call repeatedly (for example from a periodic background
        task): already-terminal runs are returned unchanged without an API call.
        """
        run = await self.get_run(run_id)
        if run.status in TERMINAL_RUN_STATUSES:
            return run

        dag_version = await self.get_dag_version(run.dag_id, run.dag_version)

        if run.github_run is None or run.github_run.run_id is None:
            run_name = f"{_RUN_NAME_PREFIX}{run.dag_id}/{run.id}"
            found = await self.github_client.find_run_by_name(
                workflow_file=dag_version.workflow_file, run_name=run_name, created_after=run.created_at
            )
            if found is None:
                return run
            status = _map_run_status(found.status or "", found.conclusion)
            run = await self._apply_run_update(
                run.id, status, github_run=found, event_type=RunEventType.RUN_RECONCILED, event_data={}
            )

        if run.github_run is None or run.github_run.run_id is None:
            return run

        gh_run_id = run.github_run.run_id
        gh_run = await self.github_client.get_run(run_id=gh_run_id)
        status = _map_run_status(gh_run.status or "", gh_run.conclusion)
        failure = _run_failure_from_conclusion(status, gh_run.conclusion)
        run = await self._apply_run_update(
            run.id, status, github_run=gh_run, failure=failure, event_type=RunEventType.RUN_RECONCILED, event_data={}
        )

        for job in await self.github_client.list_jobs(run_id=gh_run_id):
            if job.name is None:
                continue
            try:
                validate_node_id(job.name)
            except ValueError:
                continue
            node_status = _map_node_status(job.status or "", job.conclusion)
            node_failure = _node_failure_from_conclusion(node_status, job.conclusion)
            await self._apply_node_update(
                run.id,
                job.name,
                node_status,
                github_job=job,
                failure=node_failure,
                event_type=RunEventType.RUN_RECONCILED,
            )
        return run

    # ----------------------------------------------------------------- #
    # Internal helpers
    # ----------------------------------------------------------------- #

    def _lock_for(self, key: str) -> asyncio.Lock:
        return self._run_locks[hash(key) % len(self._run_locks)]

    async def _emit(
        self,
        run_id: str,
        event_type: RunEventType,
        *,
        node_id: str | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> RunEvent:
        event = RunEvent(seq=0, run_id=run_id, type=event_type, node_id=node_id, data=dict(data or {}))
        stored = await self.run_store.append_event(event)
        self.event_broker.publish(stored)
        return stored

    async def _apply_run_update(
        self,
        run_id: str,
        candidate_status: RunStatus,
        *,
        github_run: GitHubRunRef | None = None,
        failure: FailureDetail | None = None,
        event_type: RunEventType,
        event_data: Mapping[str, Any] | None = None,
    ) -> Run:
        async with self._lock_for(run_id):
            run = await self.get_run(run_id)
            changed_status = candidate_status != run.status
            changed_github_run = github_run is not None and github_run != run.github_run
            if not changed_status and not changed_github_run:
                return run  # Fully idempotent duplicate delivery: nothing to persist or emit.
            if changed_status and not validate_run_transition(run.status, candidate_status):
                logger.debug(
                    "Ignoring out-of-order run transition for %s: %s -> %s", run_id, run.status, candidate_status
                )
                return run
            updated = run.model_copy(
                update={
                    "status": candidate_status,
                    "github_run": github_run or run.github_run,
                    "failure": failure if failure is not None else run.failure,
                    "updated_at": self._clock(),
                }
            )
            await self.run_store.update_run(updated)
        await self._emit(run_id, event_type, data=event_data or {})
        return updated

    async def _apply_node_update(
        self,
        run_id: str,
        node_id: str,
        candidate_status: NodeStatus,
        *,
        github_job: GitHubJobRef | None = None,
        failure: FailureDetail | None = None,
        event_type: RunEventType,
        event_data: Mapping[str, Any] | None = None,
    ) -> NodeState:
        async with self._lock_for(f"{run_id}:{node_id}"):
            state = await self.run_store.get_node_state(run_id, node_id)
            if state is None:
                state = NodeState(run_id=run_id, node_id=node_id)
            changed_status = candidate_status != state.status
            changed_job = github_job is not None and github_job != state.github_job
            if not changed_status and not changed_job:
                return state
            if changed_status and not validate_node_transition(state.status, candidate_status):
                logger.debug(
                    "Ignoring out-of-order node transition for %s/%s: %s -> %s",
                    run_id,
                    node_id,
                    state.status,
                    candidate_status,
                )
                return state
            updated = state.model_copy(
                update={
                    "status": candidate_status,
                    "github_job": github_job or state.github_job,
                    "failure": failure if failure is not None else state.failure,
                    "updated_at": self._clock(),
                }
            )
            await self.run_store.upsert_node_state(updated)
        await self._emit(run_id, event_type, node_id=node_id, data=event_data or {})
        return updated


def _json_dumps(data: Mapping[str, Any]) -> str:
    import json

    return json.dumps(data, sort_keys=True)


def _parse_run_name(run_name: str) -> tuple[str, str] | None:
    if not run_name.startswith(_RUN_NAME_PREFIX):
        return None
    remainder = run_name[len(_RUN_NAME_PREFIX) :]
    dag_id, _, framework_run_id = remainder.partition("/")
    if not dag_id or not framework_run_id:
        return None
    return dag_id, framework_run_id


def _map_run_status(status: str, conclusion: str | None) -> RunStatus:
    if status == "completed":
        if conclusion == "success":
            return RunStatus.SUCCEEDED
        if conclusion == "cancelled":
            return RunStatus.CANCELLED
        return RunStatus.FAILED
    if status == "in_progress":
        return RunStatus.RUNNING
    return RunStatus.QUEUED


def _map_node_status(status: str, conclusion: str | None) -> NodeStatus:
    if status == "completed":
        if conclusion == "success":
            return NodeStatus.SUCCEEDED
        if conclusion == "cancelled":
            return NodeStatus.CANCELLED
        if conclusion in ("skipped", "neutral"):
            return NodeStatus.SKIPPED
        return NodeStatus.FAILED
    if status == "in_progress":
        return NodeStatus.RUNNING
    return NodeStatus.QUEUED


def _run_failure_from_conclusion(status: RunStatus, conclusion: str | None) -> FailureDetail | None:
    if status != RunStatus.FAILED:
        return None
    return FailureDetail(code="workflow_run_failed", message=f"GitHub Actions run concluded {conclusion!r}.")


def _node_failure_from_conclusion(status: NodeStatus, conclusion: str | None) -> FailureDetail | None:
    if status != NodeStatus.FAILED:
        return None
    return FailureDetail(code="job_failed", message=f"GitHub Actions job concluded {conclusion!r}.")


def _latest_checkpoint(states: list[NodeState]) -> CheckpointRef | None:
    checkpointed = [state for state in states if state.checkpoint_ref is not None]
    if not checkpointed:
        return None
    latest = max(checkpointed, key=lambda state: state.updated_at)
    return latest.checkpoint_ref
