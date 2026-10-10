# Copyright (c) Microsoft. All rights reserved.

from __future__ import annotations

from pydantic import ValidationError
from pytest import raises

from agent_framework_hosting_controlplane import (
    DagEdge,
    DagNode,
    FoundryNodeSpec,
    NodeStatus,
    RunStatus,
    validate_dag_id,
    validate_node_id,
    validate_node_transition,
    validate_run_transition,
)


def test_validate_node_id_accepts_safe_ids() -> None:
    assert validate_node_id("extract") == "extract"
    assert validate_node_id("_private-1") == "_private-1"


def test_validate_node_id_rejects_unsafe_ids() -> None:
    for bad in ("1starts-with-digit", "has space", "has/slash", "", "a" * 65):
        with raises(ValueError):
            validate_node_id(bad)


def test_validate_dag_id_rejects_unsafe_ids() -> None:
    with raises(ValueError):
        validate_dag_id("has space")
    assert validate_dag_id("daily-report_v2") == "daily-report_v2"


def test_dag_node_rejects_unsafe_depends_on() -> None:
    with raises(ValueError):
        DagNode(id="ok", foundry=FoundryNodeSpec(agent_id="a"), depends_on=("bad id",))


def test_dag_edge_rejects_unsafe_ids() -> None:
    with raises(ValueError):
        DagEdge(source_id="bad id", target_id="ok")


def test_run_status_transitions_are_monotonic() -> None:
    assert validate_run_transition(RunStatus.PENDING, RunStatus.QUEUED) is True
    assert validate_run_transition(RunStatus.QUEUED, RunStatus.RUNNING) is True
    assert validate_run_transition(RunStatus.RUNNING, RunStatus.SUCCEEDED) is True
    # Idempotent duplicate delivery: same-status transition always accepted.
    assert validate_run_transition(RunStatus.RUNNING, RunStatus.RUNNING) is True
    # Terminal statuses never transition again, even to another terminal status.
    assert validate_run_transition(RunStatus.SUCCEEDED, RunStatus.FAILED) is False
    assert validate_run_transition(RunStatus.FAILED, RunStatus.RUNNING) is False
    # No moving backwards.
    assert validate_run_transition(RunStatus.RUNNING, RunStatus.PENDING) is False


def test_node_status_transitions_are_monotonic() -> None:
    assert validate_node_transition(NodeStatus.PENDING, NodeStatus.RUNNING) is True
    assert validate_node_transition(NodeStatus.RUNNING, NodeStatus.SUCCEEDED) is True
    assert validate_node_transition(NodeStatus.SUCCEEDED, NodeStatus.SUCCEEDED) is True
    assert validate_node_transition(NodeStatus.SUCCEEDED, NodeStatus.FAILED) is False
    assert validate_node_transition(NodeStatus.FAILED, NodeStatus.RUNNING) is False


def test_models_are_frozen() -> None:
    node = DagNode(id="a", foundry=FoundryNodeSpec(agent_id="agent-a"))
    with raises(ValidationError):
        node.id = "b"  # type: ignore[misc]
