# Copyright (c) Microsoft. All rights reserved.

from __future__ import annotations

import threading

from agent_framework_hosting_controlplane import (
    GitHubRunRef,
    InMemoryRunStore,
    NodeState,
    NodeStatus,
    Run,
    RunEvent,
    RunEventType,
    RunStatus,
)


def _run(run_id: str, *, dag_id: str = "report", idempotency_key: str | None = None) -> Run:
    return Run(id=run_id, dag_id=dag_id, dag_version="v1-abc", idempotency_key=idempotency_key)


async def test_create_run_stores_and_returns_it() -> None:
    store = InMemoryRunStore()
    run = await store.create_run(_run("run-1"))
    assert run.id == "run-1"
    assert await store.get_run("run-1") == run


async def test_create_run_is_idempotent_by_dag_and_key() -> None:
    store = InMemoryRunStore()
    first = await store.create_run(_run("run-1", idempotency_key="key-a"))
    second = await store.create_run(_run("run-2", idempotency_key="key-a"))

    assert second.id == first.id  # Replay: no second run created for the same idempotency key.
    assert await store.get_run("run-2") is None
    assert await store.get_run_by_idempotency_key("report", "key-a") == first


async def test_create_run_different_dag_same_key_is_independent() -> None:
    store = InMemoryRunStore()
    first = await store.create_run(_run("run-1", dag_id="dag-a", idempotency_key="key"))
    second = await store.create_run(_run("run-2", dag_id="dag-b", idempotency_key="key"))
    assert first.id != second.id


async def test_update_run_replaces_snapshot_and_indexes_github_run_id() -> None:
    store = InMemoryRunStore()
    run = await store.create_run(_run("run-1"))
    updated = run.model_copy(update={"status": RunStatus.RUNNING, "github_run": GitHubRunRef(run_id=42)})
    await store.update_run(updated)

    assert (await store.get_run("run-1")).status == RunStatus.RUNNING  # type: ignore[union-attr]
    assert await store.get_run_by_github_run_id(42) == updated


async def test_list_runs_filters_by_dag_id_and_is_ordered() -> None:
    store = InMemoryRunStore()
    await store.create_run(_run("run-1", dag_id="dag-a"))
    await store.create_run(_run("run-2", dag_id="dag-b"))
    await store.create_run(_run("run-3", dag_id="dag-a"))

    all_runs = await store.list_runs()
    assert [run.id for run in all_runs] == ["run-1", "run-2", "run-3"]

    dag_a_runs = await store.list_runs(dag_id="dag-a")
    assert [run.id for run in dag_a_runs] == ["run-1", "run-3"]


async def test_node_state_upsert_and_list_preserves_first_seen_order() -> None:
    store = InMemoryRunStore()
    await store.upsert_node_state(NodeState(run_id="run-1", node_id="b"))
    await store.upsert_node_state(NodeState(run_id="run-1", node_id="a"))
    await store.upsert_node_state(NodeState(run_id="run-1", node_id="b", status=NodeStatus.RUNNING))

    states = await store.list_node_states("run-1")
    assert [state.node_id for state in states] == ["b", "a"]
    assert next(state for state in states if state.node_id == "b").status == NodeStatus.RUNNING


async def test_get_node_state_missing_returns_none() -> None:
    store = InMemoryRunStore()
    assert await store.get_node_state("run-1", "a") is None


async def test_append_event_assigns_monotonic_seq_per_run() -> None:
    store = InMemoryRunStore()
    first = await store.append_event(RunEvent(seq=0, run_id="run-1", type=RunEventType.RUN_REGISTERED))
    second = await store.append_event(RunEvent(seq=0, run_id="run-1", type=RunEventType.RUN_STATUS_CHANGED))
    other_run = await store.append_event(RunEvent(seq=0, run_id="run-2", type=RunEventType.RUN_REGISTERED))

    assert first.seq == 1
    assert second.seq == 2
    assert other_run.seq == 1  # Sequence numbers are scoped per run, not global.


async def test_list_events_filters_after_seq() -> None:
    store = InMemoryRunStore()
    for _ in range(3):
        await store.append_event(RunEvent(seq=0, run_id="run-1", type=RunEventType.NODE_STATUS_CHANGED))

    all_events = await store.list_events("run-1")
    assert [event.seq for event in all_events] == [1, 2, 3]

    tail = await store.list_events("run-1", after_seq=1)
    assert [event.seq for event in tail] == [2, 3]


def test_store_is_thread_safe_under_concurrent_node_upserts() -> None:
    import asyncio

    store = InMemoryRunStore()
    errors: list[BaseException] = []

    def worker(node_id: str) -> None:
        async def run_worker() -> None:
            for attempt in range(1, 6):
                await store.upsert_node_state(NodeState(run_id="run-1", node_id=node_id, attempt=attempt))

        try:
            asyncio.run(run_worker())
        except BaseException as exc:  # noqa: BLE001 - captured for the assertion below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(f"node-{i}",)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors

    async def check() -> None:
        states = await store.list_node_states("run-1")
        assert {state.node_id for state in states} == {f"node-{i}" for i in range(8)}
        assert all(state.attempt == 5 for state in states)

    asyncio.run(check())
