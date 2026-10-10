# Copyright (c) Microsoft. All rights reserved.

"""Typed models for the hosted DAG control-plane.

These models describe the static, content-addressed contract between a
compiled DAG, the GitHub Actions workflow generated from it, and the
run/node/event state tracked while that workflow executes Foundry-hosted
agents. Every model is a frozen Pydantic model so registrations, run
snapshots, and events can be safely shared across threads and serialised to
JSON for storage or API responses without accidental in-place mutation.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

__all__ = [
    "CheckpointRef",
    "CompilerOptions",
    "DagDefinition",
    "DagEdge",
    "DagNode",
    "DagVersion",
    "FailureDetail",
    "FoundryNodeSpec",
    "GitHubJobRef",
    "GitHubRepoRef",
    "GitHubRunRef",
    "NodeState",
    "NodeStatus",
    "OutputRef",
    "RetryPolicy",
    "ReusableActionRef",
    "Run",
    "RunEvent",
    "RunEventType",
    "RunStatus",
    "utcnow",
    "validate_dag_id",
    "validate_node_id",
]

#: GitHub Actions job ids must start with a letter or `_` and contain only
#: alphanumeric characters, `-`, or `_`. We additionally cap the length at 64
#: characters so generated job keys stay well under any practical workflow
#: file size limits and remain readable in the Actions UI.
_NODE_ID_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,63}$")
_DAG_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp.

    Centralised so every model/event uses the same clock source, which keeps
    ordering comparisons meaningful across the registry, store, and API layers.
    """
    return datetime.now(timezone.utc)


def validate_node_id(node_id: str) -> str:
    """Validate that ``node_id`` is safe to use as a GitHub Actions job id.

    Args:
        node_id: Candidate node/job identifier.

    Returns:
        The validated node id, unchanged.

    Raises:
        ValueError: If the id does not match GitHub Actions job id syntax.
    """
    if not _NODE_ID_PATTERN.match(node_id):
        raise ValueError(
            f"Invalid node id {node_id!r}: must start with a letter or underscore and contain only "
            "alphanumeric characters, '-', or '_' (max 64 characters), matching GitHub Actions job id syntax."
        )
    return node_id


def validate_dag_id(dag_id: str) -> str:
    """Validate that ``dag_id`` is safe to use in URLs and storage keys."""
    if not _DAG_ID_PATTERN.match(dag_id):
        raise ValueError(
            f"Invalid dag id {dag_id!r}: must contain only alphanumeric characters, '-', or '_' (max 128 characters)."
        )
    return dag_id


class _Frozen(BaseModel):
    """Base class for immutable, strict models shared across the package."""

    model_config = ConfigDict(frozen=True, extra="forbid")


# --------------------------------------------------------------------------- #
# Static DAG definition
# --------------------------------------------------------------------------- #


class RetryPolicy(_Frozen):
    """Retry policy applied by the reusable action before a node is marked failed."""

    max_attempts: int = Field(default=1, ge=1, le=10)
    backoff_seconds: float = Field(default=0.0, ge=0.0)


class FoundryNodeSpec(_Frozen):
    """Reference to a single hosted Foundry agent invoked by one DAG node.

    The control plane never executes agent logic. This spec only carries the
    identifiers a reusable/composite GitHub Action needs to invoke and poll
    the Foundry-hosted agent, plus a mapping that tells the action which
    upstream node outputs to forward as inputs.
    """

    agent_id: str
    agent_version: str | None = None
    project_endpoint: str | None = None
    input_mapping: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: int | None = Field(default=None, ge=1)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)


class DagNode(_Frozen):
    """A single static node in a DAG, backed by one Foundry-hosted agent."""

    id: str
    foundry: FoundryNodeSpec
    name: str | None = None
    depends_on: tuple[str, ...] = Field(default_factory=tuple)

    @field_validator("id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        return validate_node_id(value)

    @field_validator("depends_on")
    @classmethod
    def _validate_depends_on(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(validate_node_id(v) for v in value)


class DagEdge(_Frozen):
    """An unconditional dependency edge: ``target_id`` runs after ``source_id``."""

    source_id: str
    target_id: str

    @field_validator("source_id", "target_id")
    @classmethod
    def _validate_ids(cls, value: str) -> str:
        return validate_node_id(value)


class DagDefinition(_Frozen):
    """Declarative, static DAG definition accepted by the compiler.

    All edges are unconditional. Conditional routing, dynamic fan-out
    selection, and nested sub-workflows are static-DAG boundary violations;
    they are rejected by the compiler (see ``_compiler.py``), not by this
    model, so that a `Workflow`-derived definition and a hand-authored one
    go through identical validation.
    """

    dag_id: str
    nodes: tuple[DagNode, ...]
    edges: tuple[DagEdge, ...] = Field(default_factory=tuple)
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("dag_id")
    @classmethod
    def _validate_dag_id(cls, value: str) -> str:
        return validate_dag_id(value)


# --------------------------------------------------------------------------- #
# GitHub Actions compilation target
# --------------------------------------------------------------------------- #


class GitHubRepoRef(_Frozen):
    """The GitHub repository that hosts the generated DAG workflow file."""

    owner: str
    repo: str
    default_ref: str = "main"


class ReusableActionRef(_Frozen):
    """Reference to the reusable/composite action each job calls to drive Foundry.

    ``uses`` is copied verbatim into the generated job step (for example
    ``"my-org/foundry-invoke-action@v1"`` or
    ``"./.github/actions/foundry-invoke"``). ``with_inputs`` are additional
    static ``with:`` values (such as a service connection name) merged into
    every job's invocation step alongside the per-node identifiers.
    """

    uses: str
    with_inputs: dict[str, str] = Field(default_factory=dict)


class CompilerOptions(_Frozen):
    """Options controlling how a `DagDefinition` is rendered to a GitHub Actions workflow."""

    repo: GitHubRepoRef
    action: ReusableActionRef
    workflow_file: str = "foundry-dag.yml"
    runs_on: str = "ubuntu-slim"
    """GitHub-hosted runner label used for every generated job.

    Defaults to ``ubuntu-slim`` (the 1 vCPU / 5 GB Actions Linux runner).
    Set this to ``ubuntu-latest`` (or another valid runner label) for GitHub
    Enterprise Server or runner fleets that do not yet offer the slim label.
    """


class DagVersion(_Frozen):
    """An immutable, content-hashed, compiled DAG registration."""

    dag_id: str
    version: str
    definition: DagDefinition
    job_map: dict[str, str]
    """Mapping of DAG node id to the GitHub Actions job key used for it."""
    workflow_json: str
    """The generated workflow file content, as JSON text (valid YAML)."""
    repo: GitHubRepoRef
    workflow_file: str
    ref: str
    created_at: datetime = Field(default_factory=utcnow)


# --------------------------------------------------------------------------- #
# Run / node / event state
# --------------------------------------------------------------------------- #


class RunStatus(str, Enum):
    """Normalized run status, independent of raw GitHub Actions vocabulary."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    CANCELLING = "cancelling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_RUN_STATUSES = frozenset({RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED})

#: Allowed forward transitions. Equal-state transitions are always accepted
#: (idempotent duplicate delivery); terminal states accept no further change.
RUN_TRANSITIONS: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.PENDING: frozenset({
        RunStatus.QUEUED,
        RunStatus.RUNNING,
        RunStatus.CANCELLING,
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
        RunStatus.CANCELLED,
    }),
    RunStatus.QUEUED: frozenset({
        RunStatus.RUNNING,
        RunStatus.CANCELLING,
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
        RunStatus.CANCELLED,
    }),
    RunStatus.RUNNING: frozenset({
        RunStatus.CANCELLING,
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
        RunStatus.CANCELLED,
    }),
    RunStatus.CANCELLING: frozenset({RunStatus.CANCELLED, RunStatus.SUCCEEDED, RunStatus.FAILED}),
    RunStatus.SUCCEEDED: frozenset(),
    RunStatus.FAILED: frozenset(),
    RunStatus.CANCELLED: frozenset(),
}


def validate_run_transition(current: RunStatus, new: RunStatus) -> bool:
    """Return whether moving a run from ``current`` to ``new`` is a valid, monotonic transition."""
    if current == new:
        return True
    return new in RUN_TRANSITIONS[current]


class NodeStatus(str, Enum):
    """Normalized per-node status."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


TERMINAL_NODE_STATUSES = frozenset({
    NodeStatus.SUCCEEDED,
    NodeStatus.FAILED,
    NodeStatus.CANCELLED,
    NodeStatus.SKIPPED,
})

NODE_TRANSITIONS: dict[NodeStatus, frozenset[NodeStatus]] = {
    NodeStatus.PENDING: frozenset({
        NodeStatus.QUEUED,
        NodeStatus.RUNNING,
        NodeStatus.SUCCEEDED,
        NodeStatus.FAILED,
        NodeStatus.CANCELLED,
        NodeStatus.SKIPPED,
    }),
    NodeStatus.QUEUED: frozenset({
        NodeStatus.RUNNING,
        NodeStatus.SUCCEEDED,
        NodeStatus.FAILED,
        NodeStatus.CANCELLED,
        NodeStatus.SKIPPED,
    }),
    NodeStatus.RUNNING: frozenset({NodeStatus.SUCCEEDED, NodeStatus.FAILED, NodeStatus.CANCELLED}),
    NodeStatus.SUCCEEDED: frozenset(),
    NodeStatus.FAILED: frozenset(),
    NodeStatus.CANCELLED: frozenset(),
    NodeStatus.SKIPPED: frozenset(),
}


def validate_node_transition(current: NodeStatus, new: NodeStatus) -> bool:
    """Return whether moving a node from ``current`` to ``new`` is a valid, monotonic transition."""
    if current == new:
        return True
    return new in NODE_TRANSITIONS[current]


class OutputRef(_Frozen):
    """A reference to a node's output, exchanged instead of the output payload itself."""

    uri: str
    media_type: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CheckpointRef(_Frozen):
    """A reference to a Foundry checkpoint, used to resume a run later."""

    uri: str
    kind: str = "foundry-checkpoint"
    metadata: dict[str, Any] = Field(default_factory=dict)


class FailureDetail(_Frozen):
    """Structured failure information for a run or node."""

    code: str
    message: str
    retryable: bool = False
    occurred_at: datetime = Field(default_factory=utcnow)


class GitHubJobRef(_Frozen):
    """Correlated GitHub Actions job identifiers for a single node attempt."""

    job_id: int | None = None
    name: str | None = None
    html_url: str | None = None
    status: str | None = None
    conclusion: str | None = None


class GitHubRunRef(_Frozen):
    """Correlated GitHub Actions workflow run identifiers for a framework run."""

    run_id: int | None = None
    html_url: str | None = None
    status: str | None = None
    conclusion: str | None = None


class NodeState(_Frozen):
    """The latest known snapshot of a single DAG node within one run."""

    run_id: str
    node_id: str
    status: NodeStatus = NodeStatus.PENDING
    attempt: int = 1
    github_job: GitHubJobRef | None = None
    output_ref: OutputRef | None = None
    checkpoint_ref: CheckpointRef | None = None
    failure: FailureDetail | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class Run(_Frozen):
    """The latest known snapshot of a single DAG run."""

    id: str
    """The ``framework_run_id``: a framework-generated identifier unique across all runs."""
    dag_id: str
    dag_version: str
    status: RunStatus = RunStatus.PENDING
    inputs: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None
    parent_run_id: str | None = None
    resume_from_checkpoint: CheckpointRef | None = None
    github_run: GitHubRunRef | None = None
    failure: FailureDetail | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class RunEventType(str, Enum):
    """Discriminator for the kind of change a `RunEvent` records."""

    RUN_REGISTERED = "run.registered"
    RUN_STATUS_CHANGED = "run.status_changed"
    NODE_STATUS_CHANGED = "node.status_changed"
    WEBHOOK_RECEIVED = "webhook.received"
    RUN_RECONCILED = "run.reconciled"


class RunEvent(_Frozen):
    """An immutable, ordered event recorded against a single run.

    ``seq`` is assigned by the store and is strictly increasing per run, so
    SSE subscribers can resume after a dropped connection via
    ``Last-Event-ID`` without the control plane needing a separate cursor type.
    """

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    seq: int
    run_id: str
    type: RunEventType
    node_id: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    data: dict[str, Any] = Field(default_factory=dict)
