# Copyright (c) Microsoft. All rights reserved.

"""Hosted DAG control-plane: compile static Agent Framework workflows to GitHub Actions.

Launch and track Foundry-hosted node runs, and expose a small FastAPI surface over them.

Foundry does the compute. This package only compiles a static DAG into a
GitHub Actions workflow whose jobs call a configurable reusable/composite
action to invoke and poll Foundry, and it tracks the resulting run/node/event
state. See the package README for the OIDC, webhook, and storage-protocol
documentation this boundary depends on.
"""

from __future__ import annotations

from ._app import LaunchRunRequest, ResumeRunRequest, create_app
from ._compiler import compile_definition, compile_workflow
from ._control_plane import ControlPlane, EventBroker
from ._exceptions import (
    ControlPlaneError,
    DagCompilationError,
    DagNotFoundError,
    GitHubClientError,
    InvalidRunStateError,
    RunNotFoundError,
    SubscriberLimitExceededError,
    WebhookSignatureError,
)
from ._github_client import GitHubActionsClient, HttpxGitHubActionsClient
from ._models import (
    TERMINAL_NODE_STATUSES,
    TERMINAL_RUN_STATUSES,
    CheckpointRef,
    CompilerOptions,
    DagDefinition,
    DagEdge,
    DagNode,
    DagVersion,
    FailureDetail,
    FoundryNodeSpec,
    GitHubJobRef,
    GitHubRepoRef,
    GitHubRunRef,
    NodeState,
    NodeStatus,
    OutputRef,
    RetryPolicy,
    ReusableActionRef,
    Run,
    RunEvent,
    RunEventType,
    RunStatus,
    validate_dag_id,
    validate_node_id,
    validate_node_transition,
    validate_run_transition,
)
from ._registry import DagRegistry, InMemoryDagRegistry
from ._store import InMemoryRunStore, RunStore
from ._webhooks import verify_github_signature

__all__ = [
    "TERMINAL_NODE_STATUSES",
    "TERMINAL_RUN_STATUSES",
    "CheckpointRef",
    "CompilerOptions",
    "ControlPlane",
    "ControlPlaneError",
    "DagCompilationError",
    "DagDefinition",
    "DagEdge",
    "DagNode",
    "DagNotFoundError",
    "DagRegistry",
    "DagVersion",
    "EventBroker",
    "FailureDetail",
    "FoundryNodeSpec",
    "GitHubActionsClient",
    "GitHubClientError",
    "GitHubJobRef",
    "GitHubRepoRef",
    "GitHubRunRef",
    "HttpxGitHubActionsClient",
    "InMemoryDagRegistry",
    "InMemoryRunStore",
    "InvalidRunStateError",
    "LaunchRunRequest",
    "NodeState",
    "NodeStatus",
    "OutputRef",
    "ResumeRunRequest",
    "RetryPolicy",
    "ReusableActionRef",
    "Run",
    "RunEvent",
    "RunEventType",
    "RunNotFoundError",
    "RunStatus",
    "RunStore",
    "SubscriberLimitExceededError",
    "WebhookSignatureError",
    "compile_definition",
    "compile_workflow",
    "create_app",
    "validate_dag_id",
    "validate_node_id",
    "validate_node_transition",
    "validate_run_transition",
    "verify_github_signature",
]
