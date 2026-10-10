# Copyright (c) Microsoft. All rights reserved.

"""Compiler that turns a static DAG into a GitHub Actions workflow.

Two equivalent inputs are accepted:

1. An Agent Framework `Workflow` plus an explicit per-executor
   `FoundryNodeSpec` mapping (`compile_workflow`).
2. A hand-authored, declarative `DagDefinition` (`compile_definition`).

Both paths converge on the same validation and rendering logic so a
`Workflow`-derived DAG and a declarative one are held to identical rules:

- Every node id is unique and safe to use as a GitHub Actions job id.
- Every edge references declared nodes.
- The dependency graph is acyclic.
- No conditional or dynamically-selected edges (the static-DAG boundary).
- No nested workflows / superstep semantics.

The emitted workflow is JSON text. JSON is valid YAML, so no YAML library
is required to produce a file GitHub Actions accepts as `*.yml`.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, deque
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from ._exceptions import DagCompilationError
from ._models import (
    CompilerOptions,
    DagDefinition,
    DagEdge,
    DagNode,
    DagVersion,
    FoundryNodeSpec,
)

if TYPE_CHECKING:
    from agent_framework import Workflow

__all__ = ["compile_definition", "compile_workflow"]

#: GitHub Actions `workflow_dispatch` inputs every generated workflow declares.
_FRAMEWORK_RUN_ID_INPUT = "framework_run_id"
_DAG_ID_INPUT = "dag_id"
_DAG_VERSION_INPUT = "dag_version"
_INPUTS_JSON_INPUT = "inputs_json"
_RESUME_CHECKPOINT_INPUT = "resume_checkpoint_uri"

#: Template used for the workflow's `run-name`. GitHub Actions renders this
#: with the dispatch inputs at run creation time, which is the only reliable,
#: race-free way to correlate a `workflow_dispatch` call with the run it
#: created (the dispatch REST call itself returns no run id).
RUN_NAME_TEMPLATE = f"foundry-dag/${{{{ inputs.{_DAG_ID_INPUT} }}}}/${{{{ inputs.{_FRAMEWORK_RUN_ID_INPUT} }}}}"


def compile_workflow(
    workflow: "Workflow",
    node_specs: Mapping[str, FoundryNodeSpec],
    *,
    dag_id: str,
    options: CompilerOptions,
    description: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> DagVersion:
    """Compile an Agent Framework `Workflow` to a versioned GitHub Actions DAG.

    Args:
        workflow: A built `Workflow` whose executors map one-to-one onto
            hosted Foundry agents.
        node_specs: Foundry node specification for every executor id in
            `workflow`. Every executor must have an entry and every entry
            must reference an executor that exists in the workflow.
        dag_id: Stable identifier for the resulting DAG (distinct from any
            single version of it).
        options: GitHub Actions rendering options (target repo, reusable
            action, runner label).
        description: Optional human-readable description carried into the
            `DagDefinition`.
        metadata: Optional free-form metadata carried into the `DagDefinition`.

    Returns:
        The compiled, content-hashed `DagVersion`.

    Raises:
        DagCompilationError: If the workflow violates the static-DAG boundary
            (conditional/dynamic edges, nested workflows, cycles, unsafe or
            unmapped node ids, etc).
    """
    definition = _workflow_to_definition(
        workflow,
        node_specs,
        dag_id=dag_id,
        description=description,
        metadata=metadata,
    )
    return compile_definition(definition, options=options)


def compile_definition(definition: DagDefinition, *, options: CompilerOptions) -> DagVersion:
    """Compile a declarative `DagDefinition` to a versioned GitHub Actions DAG.

    Args:
        definition: The static DAG to compile.
        options: GitHub Actions rendering options (target repo, reusable
            action, runner label).

    Returns:
        The compiled, content-hashed `DagVersion`.

    Raises:
        DagCompilationError: If the definition violates the static-DAG
            boundary (duplicate/unknown node ids, cycles, etc).
    """
    dependencies = _validate_definition(definition)
    _topological_order(definition, dependencies)  # raises DagCompilationError on cycles
    job_map = _build_job_map(definition)
    workflow_json = _render_github_actions_workflow(definition, dependencies, job_map, options)
    version = _content_hash(definition, options)
    return DagVersion(
        dag_id=definition.dag_id,
        version=version,
        definition=definition,
        job_map=job_map,
        workflow_json=workflow_json,
        repo=options.repo,
        workflow_file=options.workflow_file,
        ref=options.repo.default_ref,
    )


# --------------------------------------------------------------------------- #
# Workflow -> DagDefinition
# --------------------------------------------------------------------------- #


def _workflow_to_definition(
    workflow: "Workflow",
    node_specs: Mapping[str, FoundryNodeSpec],
    *,
    dag_id: str,
    description: str | None,
    metadata: Mapping[str, Any] | None,
) -> DagDefinition:
    # Imported lazily so this module (and therefore the package) does not pay
    # for importing agent_framework's workflow engine unless a caller is
    # actually compiling from a Workflow object.
    from agent_framework import (
        FanInEdgeGroup,
        FanOutEdgeGroup,
        SingleEdgeGroup,
        SwitchCaseEdgeGroup,
        WorkflowExecutor,
    )

    # InternalEdgeGroup is not part of the public agent_framework API surface; it is deliberately
    # internal bookkeeping (see its docstring), so this reaches into the private workflow module.
    from agent_framework._workflows._edge import InternalEdgeGroup

    executors = {executor.id: executor for executor in workflow.get_executors_list()}

    for executor_id, executor in executors.items():
        if isinstance(executor, WorkflowExecutor):
            raise DagCompilationError(
                f"Executor {executor_id!r} wraps a nested workflow. The static DAG boundary does not "
                "support nested workflows or superstep semantics; flatten the workflow before compiling."
            )

    missing = sorted(set(executors) - set(node_specs))
    if missing:
        raise DagCompilationError(f"Missing Foundry node spec(s) for executor(s): {missing}.")
    extra = sorted(set(node_specs) - set(executors))
    if extra:
        raise DagCompilationError(f"Foundry node spec(s) provided for unknown executor(s): {extra}.")

    edges: list[DagEdge] = []
    for group in workflow.edge_groups:
        if isinstance(group, InternalEdgeGroup):
            # Auto-generated bookkeeping edge (internal source -> executor), added by the
            # WorkflowBuilder for every executor. It is not a user-authored dependency edge.
            continue
        if isinstance(group, SwitchCaseEdgeGroup):
            raise DagCompilationError(
                f"Edge group {group.id!r} is a conditional switch-case group. The static DAG boundary "
                "does not support conditional or dynamically-selected edges."
            )
        if not isinstance(group, (SingleEdgeGroup, FanOutEdgeGroup, FanInEdgeGroup)):
            raise DagCompilationError(
                f"Edge group {group.id!r} has unsupported type {type(group).__name__!r}. Only "
                "unconditional single, fan-out, and fan-in edge groups are supported."
            )
        for edge in group.edges:
            if edge.has_condition:
                raise DagCompilationError(
                    f"Edge {edge.source_id!r} -> {edge.target_id!r} is conditional. The static DAG "
                    "boundary does not support conditional or dynamically-selected edges."
                )
            edges.append(DagEdge(source_id=edge.source_id, target_id=edge.target_id))

    nodes = tuple(
        DagNode(id=executor_id, foundry=node_specs[executor_id], name=executor_id) for executor_id in sorted(executors)
    )
    return DagDefinition(
        dag_id=dag_id,
        nodes=nodes,
        edges=tuple(edges),
        description=description,
        metadata=dict(metadata) if metadata else {},
    )


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


def _validate_definition(definition: DagDefinition) -> dict[str, set[str]]:
    """Validate node/edge references and return the merged dependency graph.

    Returns:
        A mapping of node id to the set of node ids it directly depends on,
        merging both `DagEdge` entries and each node's `depends_on` list.
    """
    id_counts = Counter(node.id for node in definition.nodes)
    duplicates = sorted(node_id for node_id, count in id_counts.items() if count > 1)
    if duplicates:
        raise DagCompilationError(f"Duplicate node id(s): {duplicates}.")

    lowercase_counts = Counter(node.id.lower() for node in definition.nodes)
    case_collisions = sorted(key for key, count in lowercase_counts.items() if count > 1)
    if case_collisions:
        raise DagCompilationError(
            f"Node id(s) collide case-insensitively, which is ambiguous for GitHub Actions job ids: {case_collisions}."
        )

    node_ids = set(id_counts)
    dependencies: dict[str, set[str]] = {node.id: set(node.depends_on) for node in definition.nodes}

    for node in definition.nodes:
        unknown_deps = sorted(dep for dep in node.depends_on if dep not in node_ids)
        if unknown_deps:
            raise DagCompilationError(f"Node {node.id!r} depends on unknown node(s): {unknown_deps}.")

    for edge in definition.edges:
        if edge.source_id not in node_ids:
            raise DagCompilationError(f"Edge references unknown source node {edge.source_id!r}.")
        if edge.target_id not in node_ids:
            raise DagCompilationError(f"Edge references unknown target node {edge.target_id!r}.")
        if edge.source_id == edge.target_id:
            raise DagCompilationError(f"Edge from {edge.source_id!r} to itself is not allowed.")
        dependencies[edge.target_id].add(edge.source_id)

    return dependencies


def _topological_order(definition: DagDefinition, dependencies: Mapping[str, set[str]]) -> list[str]:
    """Return a stable topological order, raising on cycles."""
    node_ids = [node.id for node in definition.nodes]
    indegree = {node_id: len(dependencies[node_id]) for node_id in node_ids}
    dependents: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for node_id, deps in dependencies.items():
        for dep in deps:
            dependents[dep].append(node_id)

    # Sort for determinism: identical DAG shape always yields identical order,
    # which keeps the rendered workflow (and therefore its content hash) stable.
    ready = deque(sorted(node_id for node_id in node_ids if indegree[node_id] == 0))
    order: list[str] = []
    while ready:
        current = ready.popleft()
        order.append(current)
        for dependent in sorted(dependents[current]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)

    if len(order) != len(node_ids):
        remaining = sorted(node_id for node_id in node_ids if node_id not in order)
        raise DagCompilationError(
            f"DAG {definition.dag_id!r} contains a cycle among node(s): {remaining}. "
            "The static DAG boundary requires an acyclic graph."
        )
    return order


def _build_job_map(definition: DagDefinition) -> dict[str, str]:
    """Build the node id -> GitHub Actions job key mapping.

    Node ids are already validated as safe GitHub Actions job id syntax, so
    the mapping is the identity mapping today. It is still computed and
    returned explicitly (rather than inlined at call sites) so a future
    collision-avoidance strategy (for example a disambiguating prefix) can be
    introduced without changing the `DagVersion` shape.
    """
    return {node.id: node.id for node in definition.nodes}


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #


def _render_github_actions_workflow(
    definition: DagDefinition,
    dependencies: Mapping[str, set[str]],
    job_map: Mapping[str, str],
    options: CompilerOptions,
) -> str:
    jobs: dict[str, Any] = {}
    for node in definition.nodes:
        job_key = job_map[node.id]
        needs = sorted(job_map[dep] for dep in dependencies[node.id])
        upstream_outputs = {
            dep: f"${{{{ needs.{job_map[dep]}.outputs.output_ref }}}}" for dep in sorted(dependencies[node.id])
        }
        with_inputs: dict[str, str] = {
            **options.action.with_inputs,
            "framework-run-id": f"${{{{ inputs.{_FRAMEWORK_RUN_ID_INPUT} }}}}",
            "node-id": node.id,
            "agent-id": node.foundry.agent_id,
            "agent-version": node.foundry.agent_version or "",
            "project-endpoint": node.foundry.project_endpoint or "",
            "input-mapping-json": json.dumps(node.foundry.input_mapping, sort_keys=True),
            "timeout-seconds": str(node.foundry.timeout_seconds) if node.foundry.timeout_seconds else "",
            "max-attempts": str(node.foundry.retry_policy.max_attempts),
            "backoff-seconds": str(node.foundry.retry_policy.backoff_seconds),
            "upstream-outputs-json": json.dumps(upstream_outputs, sort_keys=True),
            "resume-checkpoint-uri": f"${{{{ inputs.{_RESUME_CHECKPOINT_INPUT} }}}}",
        }
        job: dict[str, Any] = {"runs-on": options.runs_on}
        if needs:
            job["needs"] = needs
        job["outputs"] = {
            "output_ref": "${{ steps.invoke.outputs.output-ref }}",
            "checkpoint_ref": "${{ steps.invoke.outputs.checkpoint-ref }}",
        }
        job["steps"] = [
            {
                "id": "invoke",
                "uses": options.action.uses,
                "with": with_inputs,
            }
        ]
        jobs[job_key] = job

    workflow: dict[str, Any] = {
        "name": f"Foundry DAG: {definition.dag_id}",
        "run-name": RUN_NAME_TEMPLATE,
        "on": {
            "workflow_dispatch": {
                "inputs": {
                    _FRAMEWORK_RUN_ID_INPUT: {
                        "description": "Unique framework-assigned run id, used to correlate this dispatch.",
                        "required": True,
                        "type": "string",
                    },
                    _DAG_ID_INPUT: {
                        "description": "The compiled DAG id this run belongs to.",
                        "required": True,
                        "type": "string",
                    },
                    _DAG_VERSION_INPUT: {
                        "description": "The compiled DAG content-hash version this run belongs to.",
                        "required": True,
                        "type": "string",
                    },
                    _INPUTS_JSON_INPUT: {
                        "description": "JSON-encoded run inputs (identifiers/URIs only, not large payloads).",
                        "required": False,
                        "type": "string",
                        "default": "{}",
                    },
                    _RESUME_CHECKPOINT_INPUT: {
                        "description": "Optional checkpoint reference to resume from.",
                        "required": False,
                        "type": "string",
                        "default": "",
                    },
                }
            }
        },
        "permissions": {"id-token": "write", "contents": "read"},
        "jobs": jobs,
    }
    return json.dumps(workflow, sort_keys=True, indent=2)


def _content_hash(definition: DagDefinition, options: CompilerOptions) -> str:
    """Compute a stable content-hash version for a definition + the options that affect its rendering."""
    payload = {
        "definition": definition.model_dump(mode="json"),
        "action_uses": options.action.uses,
        "action_with_inputs": options.action.with_inputs,
        "runs_on": options.runs_on,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(canonical).hexdigest()
    return f"v1-{digest[:16]}"
