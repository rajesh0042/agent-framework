# Copyright (c) Microsoft. All rights reserved.

from __future__ import annotations

import json

from agent_framework_hosting_controlplane import (
    CompilerOptions,
    DagDefinition,
    DagNode,
    FoundryNodeSpec,
    GitHubRepoRef,
    InMemoryDagRegistry,
    ReusableActionRef,
    compile_definition,
)


def _options() -> CompilerOptions:
    return CompilerOptions(
        repo=GitHubRepoRef(owner="my-org", repo="my-dags"),
        action=ReusableActionRef(uses="my-org/foundry-invoke-action@v1"),
    )


def _version(dag_id: str = "report", agent_id: str = "agent-a"):
    definition = DagDefinition(
        dag_id=dag_id,
        nodes=(DagNode(id="a", foundry=FoundryNodeSpec(agent_id=agent_id)),),
    )
    return compile_definition(definition, options=_options())


async def test_register_is_idempotent_for_identical_content() -> None:
    registry = InMemoryDagRegistry()
    version = _version()

    first = await registry.register(version)
    second = await registry.register(version)

    assert first.version == second.version
    assert first is second  # Identical content hash: no new registration stored.


async def test_register_keeps_distinct_versions() -> None:
    registry = InMemoryDagRegistry()
    v1 = await registry.register(_version(agent_id="agent-a"))
    v2 = await registry.register(_version(agent_id="agent-b"))

    assert v1.version != v2.version
    versions = await registry.list_versions("report")
    assert [v.version for v in versions] == [v1.version, v2.version]


async def test_get_without_version_returns_latest() -> None:
    registry = InMemoryDagRegistry()
    await registry.register(_version(agent_id="agent-a"))
    latest = await registry.register(_version(agent_id="agent-b"))

    fetched = await registry.get("report")
    assert fetched is not None
    assert fetched.version == latest.version


async def test_get_specific_version() -> None:
    registry = InMemoryDagRegistry()
    v1 = await registry.register(_version(agent_id="agent-a"))
    await registry.register(_version(agent_id="agent-b"))

    fetched = await registry.get("report", v1.version)
    assert fetched is not None
    assert fetched.version == v1.version


async def test_get_unknown_dag_returns_none() -> None:
    registry = InMemoryDagRegistry()
    assert await registry.get("missing") is None


async def test_list_dag_ids() -> None:
    registry = InMemoryDagRegistry()
    await registry.register(_version(dag_id="a"))
    await registry.register(_version(dag_id="b"))
    assert await registry.list_dag_ids() == ["a", "b"]


async def test_registration_is_immutable_round_trip() -> None:
    registry = InMemoryDagRegistry()
    version = await registry.register(_version())
    # Round-tripping through JSON must reproduce an equivalent definition: registrations are
    # meant to be durable/content-addressed, not just convenient Python object identity.
    reloaded = json.loads(version.definition.model_dump_json())
    assert reloaded["dag_id"] == "report"
