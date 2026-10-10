# Copyright (c) Microsoft. All rights reserved.

from __future__ import annotations

import json

from agent_framework import Case, Default, Executor, WorkflowBuilder, WorkflowContext, WorkflowExecutor, handler
from pytest import raises
from typing_extensions import Never

from agent_framework_hosting_controlplane import (
    CompilerOptions,
    DagCompilationError,
    DagDefinition,
    DagEdge,
    DagNode,
    FoundryNodeSpec,
    GitHubRepoRef,
    ReusableActionRef,
    compile_definition,
    compile_workflow,
)


def _options(**overrides: object) -> CompilerOptions:
    defaults: dict[str, object] = {
        "repo": GitHubRepoRef(owner="my-org", repo="my-dags", default_ref="main"),
        "action": ReusableActionRef(uses="my-org/foundry-invoke-action@v1"),
        "workflow_file": "foundry-dag.yml",
    }
    defaults.update(overrides)
    return CompilerOptions(**defaults)  # type: ignore[arg-type]


def _spec(agent_id: str = "agent") -> FoundryNodeSpec:
    return FoundryNodeSpec(agent_id=agent_id)


def _linear_definition(dag_id: str = "linear") -> DagDefinition:
    return DagDefinition(
        dag_id=dag_id,
        nodes=(
            DagNode(id="extract", foundry=_spec("extract-agent")),
            DagNode(id="summarize", foundry=_spec("summarize-agent")),
        ),
        edges=(DagEdge(source_id="extract", target_id="summarize"),),
    )


class PassThrough(Executor):
    @handler
    async def run(self, data: str, ctx: WorkflowContext[str]) -> None:
        await ctx.send_message(data)


class Sink(Executor):
    @handler
    async def run(self, data: str, ctx: "WorkflowContext[Never, str]") -> None:
        await ctx.yield_output(data)


class FanInSink(Executor):
    @handler
    async def run(self, data: list[str], ctx: "WorkflowContext[Never, str]") -> None:
        await ctx.yield_output(", ".join(data))


# --------------------------------------------------------------------------- #
# Declarative DagDefinition compilation
# --------------------------------------------------------------------------- #


def test_compile_definition_renders_valid_json_workflow() -> None:
    version = compile_definition(_linear_definition(), options=_options())

    assert version.dag_id == "linear"
    assert version.job_map == {"extract": "extract", "summarize": "summarize"}

    workflow = json.loads(version.workflow_json)
    assert workflow["permissions"] == {"id-token": "write", "contents": "read"}
    assert set(workflow["jobs"]) == {"extract", "summarize"}
    assert "needs" not in workflow["jobs"]["extract"]
    assert workflow["jobs"]["summarize"]["needs"] == ["extract"]
    assert workflow["jobs"]["summarize"]["runs-on"] == "ubuntu-slim"
    assert "foundry_run_id" not in workflow["on"]["workflow_dispatch"]["inputs"]
    for required_input in ("framework_run_id", "dag_id", "dag_version"):
        assert workflow["on"]["workflow_dispatch"]["inputs"][required_input]["required"] is True


def test_compile_definition_uses_configured_runner_label() -> None:
    version = compile_definition(_linear_definition(), options=_options(runs_on="ubuntu-latest"))
    workflow = json.loads(version.workflow_json)
    assert workflow["jobs"]["extract"]["runs-on"] == "ubuntu-latest"


def test_compile_definition_exchanges_only_output_references() -> None:
    version = compile_definition(_linear_definition(), options=_options())
    workflow = json.loads(version.workflow_json)
    summarize_with = workflow["jobs"]["summarize"]["steps"][0]["with"]
    upstream_outputs = json.loads(summarize_with["upstream-outputs-json"])
    assert upstream_outputs == {"extract": "${{ needs.extract.outputs.output_ref }}"}
    assert workflow["jobs"]["extract"]["outputs"] == {
        "output_ref": "${{ steps.invoke.outputs.output-ref }}",
        "checkpoint_ref": "${{ steps.invoke.outputs.checkpoint-ref }}",
    }


def test_compile_definition_is_deterministic_and_content_hashed() -> None:
    version_a = compile_definition(_linear_definition(), options=_options())
    version_b = compile_definition(_linear_definition(), options=_options())
    assert version_a.version == version_b.version
    assert version_a.workflow_json == version_b.workflow_json

    version_c = compile_definition(_linear_definition(), options=_options(runs_on="ubuntu-latest"))
    assert version_c.version != version_a.version


def test_compile_definition_rejects_duplicate_node_ids() -> None:
    definition = DagDefinition(
        dag_id="dupe",
        nodes=(
            DagNode(id="a", foundry=_spec()),
            DagNode(id="a", foundry=_spec()),
        ),
    )
    with raises(DagCompilationError, match="Duplicate node id"):
        compile_definition(definition, options=_options())


def test_compile_definition_rejects_case_insensitive_collisions() -> None:
    definition = DagDefinition(
        dag_id="case",
        nodes=(
            DagNode(id="Extract", foundry=_spec()),
            DagNode(id="extract", foundry=_spec()),
        ),
    )
    with raises(DagCompilationError, match="collide case-insensitively"):
        compile_definition(definition, options=_options())


def test_compile_definition_rejects_edge_to_unknown_node() -> None:
    definition = DagDefinition(
        dag_id="bad-edge",
        nodes=(DagNode(id="a", foundry=_spec()),),
        edges=(DagEdge(source_id="a", target_id="ghost"),),
    )
    with raises(DagCompilationError, match="unknown"):
        compile_definition(definition, options=_options())


def test_compile_definition_rejects_depends_on_unknown_node() -> None:
    definition = DagDefinition(
        dag_id="bad-deps",
        nodes=(DagNode(id="a", foundry=_spec(), depends_on=("ghost",)),),
    )
    with raises(DagCompilationError, match="unknown node"):
        compile_definition(definition, options=_options())


def test_compile_definition_rejects_cycles() -> None:
    definition = DagDefinition(
        dag_id="cyclic",
        nodes=(
            DagNode(id="a", foundry=_spec()),
            DagNode(id="b", foundry=_spec()),
        ),
        edges=(
            DagEdge(source_id="a", target_id="b"),
            DagEdge(source_id="b", target_id="a"),
        ),
    )
    with raises(DagCompilationError, match="cycle"):
        compile_definition(definition, options=_options())


def test_compile_definition_merges_edges_and_depends_on() -> None:
    definition = DagDefinition(
        dag_id="merged",
        nodes=(
            DagNode(id="a", foundry=_spec()),
            DagNode(id="b", foundry=_spec(), depends_on=("a",)),
        ),
    )
    version = compile_definition(definition, options=_options())
    workflow = json.loads(version.workflow_json)
    assert workflow["jobs"]["b"]["needs"] == ["a"]


# --------------------------------------------------------------------------- #
# Workflow compilation
# --------------------------------------------------------------------------- #


def test_compile_workflow_linear_chain() -> None:
    a = PassThrough(id="a")
    b = Sink(id="b")
    workflow = WorkflowBuilder(start_executor=a).add_edge(a, b).build()

    version = compile_workflow(
        workflow,
        {"a": _spec("agent-a"), "b": _spec("agent-b")},
        dag_id="wf-linear",
        options=_options(),
    )
    rendered = json.loads(version.workflow_json)
    assert set(rendered["jobs"]) == {"a", "b"}
    assert rendered["jobs"]["b"]["needs"] == ["a"]


def test_compile_workflow_fan_out_fan_in() -> None:
    a = PassThrough(id="a")
    b = PassThrough(id="b")
    c = PassThrough(id="c")
    d = FanInSink(id="d")
    workflow = WorkflowBuilder(start_executor=a).add_fan_out_edges(a, [b, c]).add_fan_in_edges([b, c], d).build()
    version = compile_workflow(
        workflow,
        {"a": _spec(), "b": _spec(), "c": _spec(), "d": _spec()},
        dag_id="wf-fan",
        options=_options(),
    )
    rendered = json.loads(version.workflow_json)
    assert sorted(rendered["jobs"]["d"]["needs"]) == ["b", "c"]
    assert rendered["jobs"]["b"]["needs"] == ["a"]
    assert rendered["jobs"]["c"]["needs"] == ["a"]


def test_compile_workflow_rejects_switch_case() -> None:
    a = PassThrough(id="a")
    c = Sink(id="c")
    d = Sink(id="d")
    workflow = (
        WorkflowBuilder(start_executor=a)
        .add_switch_case_edge_group(
            a,
            [Case(condition=lambda data: len(data) > 3, target=c), Default(target=d)],
        )
        .build()
    )
    with raises(DagCompilationError, match="conditional"):
        compile_workflow(
            workflow,
            {"a": _spec(), "c": _spec(), "d": _spec()},
            dag_id="wf-switch",
            options=_options(),
        )


def test_compile_workflow_rejects_nested_workflow() -> None:
    inner_start = PassThrough(id="inner")
    inner = WorkflowBuilder(start_executor=inner_start).build()
    nested = WorkflowExecutor(inner, id="nested")
    a = PassThrough(id="a")
    workflow = WorkflowBuilder(start_executor=a).add_edge(a, nested).build()

    with raises(DagCompilationError, match="nested workflow"):
        compile_workflow(
            workflow,
            {"a": _spec(), "nested": _spec()},
            dag_id="wf-nested",
            options=_options(),
        )


def test_compile_workflow_rejects_cycles() -> None:
    a = PassThrough(id="a")
    b = PassThrough(id="b")
    workflow = WorkflowBuilder(start_executor=a).add_edge(a, b).add_edge(b, a).build()

    with raises(DagCompilationError, match="cycle"):
        compile_workflow(
            workflow,
            {"a": _spec(), "b": _spec()},
            dag_id="wf-cycle",
            options=_options(),
        )


def test_compile_workflow_requires_foundry_spec_for_every_executor() -> None:
    a = PassThrough(id="a")
    b = Sink(id="b")
    workflow = WorkflowBuilder(start_executor=a).add_edge(a, b).build()

    with raises(DagCompilationError, match="Missing Foundry node spec"):
        compile_workflow(workflow, {"a": _spec()}, dag_id="wf-missing", options=_options())


def test_compile_workflow_rejects_specs_for_unknown_executors() -> None:
    a = Sink(id="a")
    workflow = WorkflowBuilder(start_executor=a).build()

    with raises(DagCompilationError, match="unknown executor"):
        compile_workflow(
            workflow,
            {"a": _spec(), "ghost": _spec()},
            dag_id="wf-extra",
            options=_options(),
        )
