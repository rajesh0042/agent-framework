# Copyright (c) Microsoft. All rights reserved.

"""Protocol and in-memory implementation of normalized run/node/event state.

The `RunStore` protocol is the durability seam for everything the control
plane tracks about a launched run: the run record itself, per-node state
snapshots, and the append-only event log. The in-memory implementation is
thread-safe (guarded by a single `threading.RLock`) and sufficient for tests
and single-process deployments, but it is explicitly not durable: restart the
process and all state is gone. Production deployments should implement
`RunStore` against a database so launches, node updates, and events survive
process restarts.

Idempotency at the store level: `create_run` treats `(dag_id, idempotency_key)`
as a uniqueness constraint and returns the previously stored run instead of
creating a second one, so a client that retries a launch request (for example
after a timed-out response) never creates a duplicate GitHub Actions
dispatch from the control plane's point of view.
"""

from __future__ import annotations

import threading
from typing import Protocol, runtime_checkable

from ._models import NodeState, Run, RunEvent

__all__ = ["InMemoryRunStore", "RunStore"]


@runtime_checkable
class RunStore(Protocol):
    """Storage protocol for run, node, and event state."""

    async def create_run(self, run: Run) -> Run:
        """Create a run, or return the existing run for the same idempotency key.

        If `run.idempotency_key` is set and a run already exists for
        `(run.dag_id, run.idempotency_key)`, that existing run is returned
        unchanged and `run` is discarded. Otherwise `run` is stored and
        returned as-is.
        """
        ...

    async def get_run(self, run_id: str) -> Run | None:
        """Return the run with id `run_id`, or `None` if it does not exist."""
        ...

    async def get_run_by_idempotency_key(self, dag_id: str, idempotency_key: str) -> Run | None:
        """Return the run previously created for `(dag_id, idempotency_key)`, if any."""
        ...

    async def get_run_by_github_run_id(self, github_run_id: int) -> Run | None:
        """Return the run correlated with a specific GitHub Actions `run_id`, if any."""
        ...

    async def update_run(self, run: Run) -> Run:
        """Replace the stored snapshot for `run.id` with `run` and return it."""
        ...

    async def list_runs(self, *, dag_id: str | None = None) -> list[Run]:
        """List runs, optionally filtered to a single `dag_id`, oldest first."""
        ...

    async def upsert_node_state(self, state: NodeState) -> NodeState:
        """Replace the stored snapshot for `(state.run_id, state.node_id)` with `state`."""
        ...

    async def get_node_state(self, run_id: str, node_id: str) -> NodeState | None:
        """Return the latest snapshot for one node in one run, if any."""
        ...

    async def list_node_states(self, run_id: str) -> list[NodeState]:
        """List all node state snapshots for `run_id`."""
        ...

    async def append_event(self, event: RunEvent) -> RunEvent:
        """Append `event` to the run's event log, assigning the next `seq`, and return it."""
        ...

    async def list_events(self, run_id: str, *, after_seq: int | None = None) -> list[RunEvent]:
        """List events for `run_id` in order, optionally only those with `seq > after_seq`."""
        ...


class InMemoryRunStore:
    """Thread-safe, in-process `RunStore` suitable for tests and single-process deployments."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runs: dict[str, Run] = {}
        self._idempotency_index: dict[tuple[str, str], str] = {}
        self._github_run_index: dict[int, str] = {}
        self._node_states: dict[tuple[str, str], NodeState] = {}
        self._node_order: dict[str, list[str]] = {}
        self._events: dict[str, list[RunEvent]] = {}
        self._event_seq: dict[str, int] = {}

    async def create_run(self, run: Run) -> Run:
        with self._lock:
            if run.idempotency_key is not None:
                key = (run.dag_id, run.idempotency_key)
                existing_id = self._idempotency_index.get(key)
                if existing_id is not None:
                    return self._runs[existing_id]
                self._idempotency_index[key] = run.id
            self._runs[run.id] = run
            if run.github_run is not None and run.github_run.run_id is not None:
                self._github_run_index[run.github_run.run_id] = run.id
            self._node_order.setdefault(run.id, [])
            self._events.setdefault(run.id, [])
            self._event_seq.setdefault(run.id, 0)
            return run

    async def get_run(self, run_id: str) -> Run | None:
        with self._lock:
            return self._runs.get(run_id)

    async def get_run_by_idempotency_key(self, dag_id: str, idempotency_key: str) -> Run | None:
        with self._lock:
            run_id = self._idempotency_index.get((dag_id, idempotency_key))
            return self._runs.get(run_id) if run_id is not None else None

    async def get_run_by_github_run_id(self, github_run_id: int) -> Run | None:
        with self._lock:
            run_id = self._github_run_index.get(github_run_id)
            return self._runs.get(run_id) if run_id is not None else None

    async def update_run(self, run: Run) -> Run:
        with self._lock:
            self._runs[run.id] = run
            if run.github_run is not None and run.github_run.run_id is not None:
                self._github_run_index[run.github_run.run_id] = run.id
            return run

    async def list_runs(self, *, dag_id: str | None = None) -> list[Run]:
        with self._lock:
            runs = list(self._runs.values())
        runs.sort(key=lambda run: run.created_at)
        if dag_id is not None:
            runs = [run for run in runs if run.dag_id == dag_id]
        return runs

    async def upsert_node_state(self, state: NodeState) -> NodeState:
        with self._lock:
            key = (state.run_id, state.node_id)
            if key not in self._node_states:
                self._node_order.setdefault(state.run_id, []).append(state.node_id)
            self._node_states[key] = state
            return state

    async def get_node_state(self, run_id: str, node_id: str) -> NodeState | None:
        with self._lock:
            return self._node_states.get((run_id, node_id))

    async def list_node_states(self, run_id: str) -> list[NodeState]:
        with self._lock:
            order = self._node_order.get(run_id, [])
            return [self._node_states[run_id, node_id] for node_id in order if (run_id, node_id) in self._node_states]

    async def append_event(self, event: RunEvent) -> RunEvent:
        with self._lock:
            next_seq = self._event_seq.get(event.run_id, 0) + 1
            stamped = event.model_copy(update={"seq": next_seq})
            self._events.setdefault(event.run_id, []).append(stamped)
            self._event_seq[event.run_id] = next_seq
            return stamped

    async def list_events(self, run_id: str, *, after_seq: int | None = None) -> list[RunEvent]:
        with self._lock:
            events = list(self._events.get(run_id, []))
        if after_seq is not None:
            events = [event for event in events if event.seq > after_seq]
        return events
