# Copyright (c) Microsoft. All rights reserved.

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from typing import Any

import pytest

from agent_framework_hosting_controlplane import (
    CheckpointRef,
    CompilerOptions,
    ControlPlane,
    DagDefinition,
    DagNode,
    DagNotFoundError,
    FoundryNodeSpec,
    GitHubJobRef,
    GitHubRepoRef,
    GitHubRunRef,
    InMemoryDagRegistry,
    InMemoryRunStore,
    InvalidRunStateError,
    NodeStatus,
    ReusableActionRef,
    RunEventType,
    RunNotFoundError,
    RunStatus,
    WebhookSignatureError,
    compile_definition,
)
from agent_framework_hosting_controlplane._webhooks import verify_github_signature


class FakeGitHubActionsClient:
    """In-memory `GitHubActionsClient` test double that records calls and lets tests script responses."""

    def __init__(self) -> None:
        self.dispatched: list[dict[str, Any]] = []
        self.cancelled: list[int] = []
        self.rerun: list[int] = []
        self.dispatch_error: Exception | None = None
        self._next_run_id = 1000
        self.runs: dict[int, GitHubRunRef] = {}
        self.jobs: dict[int, list[Any]] = {}
        self.find_result: GitHubRunRef | None = None

    async def dispatch_workflow(self, *, workflow_file: str, ref: str, inputs: Mapping[str, str]) -> None:
        if self.dispatch_error is not None:
            raise self.dispatch_error
        self.dispatched.append({"workflow_file": workflow_file, "ref": ref, "inputs": dict(inputs)})

    async def find_run_by_name(
        self, *, workflow_file: str, run_name: str, created_after: Any = None
    ) -> GitHubRunRef | None:
        return self.find_result

    async def get_run(self, *, run_id: int) -> GitHubRunRef:
        return self.runs[run_id]

    async def list_jobs(self, *, run_id: int) -> list[Any]:
        return self.jobs.get(run_id, [])

    async def cancel_run(self, *, run_id: int) -> None:
        self.cancelled.append(run_id)

    async def rerun_failed_jobs(self, *, run_id: int) -> None:
        self.rerun.append(run_id)


def _options() -> CompilerOptions:
    return CompilerOptions(
        repo=GitHubRepoRef(owner="my-org", repo="my-dags"),
        action=ReusableActionRef(uses="my-org/foundry-invoke-action@v1"),
    )


def _definition(dag_id: str = "report") -> DagDefinition:
    return DagDefinition(
        dag_id=dag_id,
        nodes=(
            DagNode(id="extract", foundry=FoundryNodeSpec(agent_id="extract-agent")),
            DagNode(id="summarize", foundry=FoundryNodeSpec(agent_id="summarize-agent"), depends_on=("extract",)),
        ),
    )


async def _make_control_plane(
    *, github_client: FakeGitHubActionsClient | None = None, webhook_secret: str | None = None
) -> tuple[ControlPlane, FakeGitHubActionsClient]:
    github_client = github_client or FakeGitHubActionsClient()
    control_plane = ControlPlane(
        dag_registry=InMemoryDagRegistry(),
        run_store=InMemoryRunStore(),
        github_client=github_client,  # type: ignore[arg-type]
        webhook_secret=webhook_secret,
    )
    version = compile_definition(_definition(), options=_options())
    await control_plane.register_dag(version)
    return control_plane, github_client


# --------------------------------------------------------------------------- #
# Launch
# --------------------------------------------------------------------------- #


async def test_launch_returns_queued_run_and_dispatches() -> None:
    control_plane, github_client = await _make_control_plane()

    run = await control_plane.launch("report", inputs={"source_uri": "blob://x"})

    assert run.status == RunStatus.QUEUED
    assert run.dag_id == "report"
    assert len(github_client.dispatched) == 1
    dispatched = github_client.dispatched[0]
    assert dispatched["inputs"]["framework_run_id"] == run.id
    assert dispatched["inputs"]["dag_id"] == "report"
    assert json.loads(dispatched["inputs"]["inputs_json"]) == {"source_uri": "blob://x"}

    nodes = await control_plane.list_nodes(run.id)
    assert {node.node_id for node in nodes} == {"extract", "summarize"}
    assert all(node.status == NodeStatus.PENDING for node in nodes)

    events = await control_plane.list_events(run.id)
    assert [event.type.value for event in events] == ["run.registered", "run.status_changed"]


async def test_launch_is_idempotent_by_key() -> None:
    control_plane, github_client = await _make_control_plane()

    first = await control_plane.launch("report", idempotency_key="key-a")
    second = await control_plane.launch("report", idempotency_key="key-a")

    assert first.id == second.id
    assert len(github_client.dispatched) == 1  # No second dispatch for the replayed launch.


async def test_launch_records_failure_when_dispatch_raises() -> None:
    github_client = FakeGitHubActionsClient()
    github_client.dispatch_error = RuntimeError("network down")
    control_plane, _ = await _make_control_plane(github_client=github_client)

    run = await control_plane.launch("report")

    assert run.status == RunStatus.FAILED
    assert run.failure is not None
    assert run.failure.retryable is True


async def test_launch_unknown_dag_raises() -> None:
    control_plane, _ = await _make_control_plane()

    with pytest.raises(DagNotFoundError):
        await control_plane.launch("missing-dag")


# --------------------------------------------------------------------------- #
# Cancel / retry / resume
# --------------------------------------------------------------------------- #


async def test_cancel_is_idempotent_and_calls_github_once_correlated() -> None:
    control_plane, github_client = await _make_control_plane()
    run = await control_plane.launch("report")
    github_client.runs[555] = GitHubRunRef(run_id=555, status="in_progress")
    await control_plane._apply_run_update(  # noqa: SLF001 - simulate a prior webhook correlating the run
        run.id,
        RunStatus.RUNNING,
        github_run=GitHubRunRef(run_id=555),
        event_type=RunEventType.WEBHOOK_RECEIVED,
    )

    cancelled = await control_plane.cancel(run.id)
    assert cancelled.status == RunStatus.CANCELLING
    assert github_client.cancelled == [555]

    # Idempotent: cancelling again is a no-op and does not call GitHub a second time.
    again = await control_plane.cancel(run.id)
    assert again.status == RunStatus.CANCELLING
    assert github_client.cancelled == [555]


async def test_cancel_unknown_run_raises() -> None:
    control_plane, _ = await _make_control_plane()
    with pytest.raises(RunNotFoundError):
        await control_plane.cancel("missing-run")


async def test_retry_requires_failed_status() -> None:
    control_plane, _ = await _make_control_plane()
    run = await control_plane.launch("report")  # status QUEUED, not FAILED
    with pytest.raises(InvalidRunStateError):
        await control_plane.retry(run.id)


async def test_retry_reruns_failed_jobs_and_resets_node_states() -> None:
    control_plane, github_client = await _make_control_plane()
    run = await control_plane.launch("report")
    github_run = GitHubRunRef(run_id=777, status="completed", conclusion="failure")
    await control_plane._apply_run_update(  # noqa: SLF001
        run.id,
        RunStatus.FAILED,
        github_run=github_run,
        event_type=RunEventType.WEBHOOK_RECEIVED,
    )
    await control_plane._apply_node_update(  # noqa: SLF001
        run.id,
        "extract",
        NodeStatus.FAILED,
        event_type=RunEventType.WEBHOOK_RECEIVED,
    )

    retried = await control_plane.retry(run.id)
    assert retried.status == RunStatus.QUEUED
    assert github_client.rerun == [777]

    extract_state = await control_plane.run_store.get_node_state(run.id, "extract")
    assert extract_state is not None
    assert extract_state.status == NodeStatus.QUEUED
    assert extract_state.attempt == 2


async def test_resume_requires_a_checkpoint() -> None:
    control_plane, _ = await _make_control_plane()
    run = await control_plane.launch("report")
    with pytest.raises(InvalidRunStateError):
        await control_plane.resume(run.id)


async def test_resume_launches_linked_run_with_checkpoint_input() -> None:
    control_plane, github_client = await _make_control_plane()
    run = await control_plane.launch("report", inputs={"a": 1})
    checkpoint = CheckpointRef(uri="blob://checkpoints/1")
    await control_plane._apply_node_update(  # noqa: SLF001
        run.id,
        "extract",
        NodeStatus.SUCCEEDED,
        event_type=RunEventType.WEBHOOK_RECEIVED,
    )
    state = await control_plane.run_store.get_node_state(run.id, "extract")
    assert state is not None
    await control_plane.run_store.upsert_node_state(state.model_copy(update={"checkpoint_ref": checkpoint}))

    resumed = await control_plane.resume(run.id)

    assert resumed.parent_run_id == run.id
    assert resumed.resume_from_checkpoint == checkpoint
    dispatched = github_client.dispatched[-1]
    assert dispatched["inputs"]["resume_checkpoint_uri"] == checkpoint.uri


# --------------------------------------------------------------------------- #
# Webhook ingestion
# --------------------------------------------------------------------------- #


async def test_ingest_webhook_requires_valid_signature_when_secret_configured() -> None:
    control_plane, _ = await _make_control_plane(webhook_secret="my-webhook-secret")
    body = b'{"workflow_run": {}}'
    with pytest.raises(WebhookSignatureError):
        await control_plane.ingest_webhook(
            event_name="workflow_run", payload={"workflow_run": {}}, signature_header="sha256=bad", raw_body=body
        )


async def test_ingest_webhook_accepts_valid_signature() -> None:
    secret = "my-webhook-secret"
    control_plane, _ = await _make_control_plane(webhook_secret=secret)
    run = await control_plane.launch("report")

    payload = {
        "workflow_run": {
            "id": 321,
            "display_title": f"foundry-dag/report/{run.id}",
            "status": "in_progress",
            "conclusion": None,
            "html_url": "https://example/run/321",
        }
    }
    body = json.dumps(payload).encode()
    signature = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_github_signature(secret=secret, payload=body, signature_header=signature)

    await control_plane.ingest_webhook(
        event_name="workflow_run", payload=payload, signature_header=signature, raw_body=body
    )

    updated = await control_plane.get_run(run.id)
    assert updated.status == RunStatus.RUNNING
    assert updated.github_run is not None
    assert updated.github_run.run_id == 321


async def test_ingest_workflow_run_webhook_correlates_and_updates_status() -> None:
    control_plane, _ = await _make_control_plane()
    run = await control_plane.launch("report")

    payload = {
        "workflow_run": {
            "id": 42,
            "display_title": f"foundry-dag/report/{run.id}",
            "status": "completed",
            "conclusion": "success",
        }
    }
    await control_plane.ingest_webhook(
        event_name="workflow_run", payload=payload, signature_header=None, raw_body=b"{}"
    )

    updated = await control_plane.get_run(run.id)
    assert updated.status == RunStatus.SUCCEEDED
    assert updated.github_run is not None
    assert updated.github_run.run_id == 42


async def test_ingest_workflow_job_webhook_updates_node_state() -> None:
    control_plane, _ = await _make_control_plane()
    run = await control_plane.launch("report")

    run_payload = {"workflow_run": {"id": 42, "display_title": f"foundry-dag/report/{run.id}", "status": "in_progress"}}
    await control_plane.ingest_webhook(
        event_name="workflow_run", payload=run_payload, signature_header=None, raw_body=b"{}"
    )

    job_payload = {
        "workflow_job": {
            "id": 9001,
            "run_id": 42,
            "name": "extract",
            "status": "completed",
            "conclusion": "success",
        }
    }
    await control_plane.ingest_webhook(
        event_name="workflow_job", payload=job_payload, signature_header=None, raw_body=b"{}"
    )

    node_state = await control_plane.run_store.get_node_state(run.id, "extract")
    assert node_state is not None
    assert node_state.status == NodeStatus.SUCCEEDED
    assert node_state.github_job is not None
    assert node_state.github_job.job_id == 9001


async def test_duplicate_webhook_delivery_is_idempotent() -> None:
    control_plane, _ = await _make_control_plane()
    run = await control_plane.launch("report")
    payload = {"workflow_run": {"id": 42, "display_title": f"foundry-dag/report/{run.id}", "status": "in_progress"}}

    await control_plane.ingest_webhook(
        event_name="workflow_run", payload=payload, signature_header=None, raw_body=b"{}"
    )
    events_after_first = await control_plane.list_events(run.id)

    # Deliver the identical webhook again (e.g. GitHub's at-least-once retry behavior).
    await control_plane.ingest_webhook(
        event_name="workflow_run", payload=payload, signature_header=None, raw_body=b"{}"
    )
    events_after_second = await control_plane.list_events(run.id)

    assert len(events_after_second) == len(events_after_first)  # No new event for the duplicate delivery.


async def test_out_of_order_webhook_is_dropped_not_applied() -> None:
    control_plane, _ = await _make_control_plane()
    run = await control_plane.launch("report")

    success_payload = {
        "workflow_run": {
            "id": 42,
            "display_title": f"foundry-dag/report/{run.id}",
            "status": "completed",
            "conclusion": "success",
        }
    }
    await control_plane.ingest_webhook(
        event_name="workflow_run", payload=success_payload, signature_header=None, raw_body=b"{}"
    )

    stale_running_payload = {
        "workflow_run": {"id": 42, "display_title": f"foundry-dag/report/{run.id}", "status": "in_progress"}
    }
    await control_plane.ingest_webhook(
        event_name="workflow_run", payload=stale_running_payload, signature_header=None, raw_body=b"{}"
    )

    updated = await control_plane.get_run(run.id)
    assert updated.status == RunStatus.SUCCEEDED  # The stale "in_progress" delivery must not regress status.
    assert updated.failure is None
    assert updated.github_run is not None
    assert updated.github_run.conclusion == "success"


async def test_out_of_order_failed_webhook_does_not_attach_failure_to_success() -> None:
    control_plane, _ = await _make_control_plane()
    run = await control_plane.launch("report")

    await control_plane.ingest_webhook(
        event_name="workflow_run",
        payload={
            "workflow_run": {
                "id": 42,
                "display_title": f"foundry-dag/report/{run.id}",
                "status": "completed",
                "conclusion": "success",
            }
        },
        signature_header=None,
        raw_body=b"{}",
    )
    await control_plane.ingest_webhook(
        event_name="workflow_run",
        payload={
            "workflow_run": {
                "id": 42,
                "display_title": f"foundry-dag/report/{run.id}",
                "status": "completed",
                "conclusion": None,
            }
        },
        signature_header=None,
        raw_body=b"{}",
    )

    updated = await control_plane.get_run(run.id)
    assert updated.status == RunStatus.SUCCEEDED
    assert updated.failure is None
    assert updated.github_run is not None
    assert updated.github_run.conclusion == "success"


async def test_ingest_webhook_for_unknown_run_is_ignored() -> None:
    control_plane, _ = await _make_control_plane()
    payload = {"workflow_run": {"id": 1, "display_title": "foundry-dag/report/unknown-run", "status": "in_progress"}}
    # Should not raise; unknown correlated runs are silently ignored.
    await control_plane.ingest_webhook(
        event_name="workflow_run", payload=payload, signature_header=None, raw_body=b"{}"
    )


async def test_ingest_webhook_ignores_unrelated_event_types() -> None:
    control_plane, _ = await _make_control_plane()
    # Must not raise for event types we do not handle (e.g. "ping").
    await control_plane.ingest_webhook(
        event_name="ping", payload={"zen": "hello"}, signature_header=None, raw_body=b"{}"
    )


# --------------------------------------------------------------------------- #
# Reconciliation
# --------------------------------------------------------------------------- #


async def test_reconcile_finds_uncorrelated_run_and_updates_status() -> None:
    control_plane, github_client = await _make_control_plane()
    run = await control_plane.launch("report")
    github_client.find_result = GitHubRunRef(run_id=55, status="in_progress")
    github_client.runs[55] = GitHubRunRef(run_id=55, status="in_progress")
    github_client.jobs[55] = []

    reconciled = await control_plane.reconcile(run.id)

    assert reconciled.github_run is not None
    assert reconciled.github_run.run_id == 55
    assert reconciled.status == RunStatus.RUNNING


async def test_reconcile_updates_node_states_from_job_list() -> None:
    control_plane, github_client = await _make_control_plane()
    run = await control_plane.launch("report")
    github_client.find_result = GitHubRunRef(run_id=66, status="in_progress")
    github_client.runs[66] = GitHubRunRef(run_id=66, status="in_progress")
    github_client.jobs[66] = [GitHubJobRef(job_id=1, name="extract", status="completed", conclusion="success")]

    await control_plane.reconcile(run.id)

    node_state = await control_plane.run_store.get_node_state(run.id, "extract")
    assert node_state is not None
    assert node_state.status == NodeStatus.SUCCEEDED


async def test_reconcile_is_a_no_op_for_terminal_runs() -> None:
    control_plane, github_client = await _make_control_plane()
    run = await control_plane.launch("report")
    payload = {
        "workflow_run": {
            "id": 1,
            "display_title": f"foundry-dag/report/{run.id}",
            "status": "completed",
            "conclusion": "success",
        }
    }
    await control_plane.ingest_webhook(
        event_name="workflow_run", payload=payload, signature_header=None, raw_body=b"{}"
    )

    reconciled = await control_plane.reconcile(run.id)
    assert reconciled.status == RunStatus.SUCCEEDED
    assert github_client.find_result is None  # reconcile must not have called find_run_by_name
