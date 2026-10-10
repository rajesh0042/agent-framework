# Copyright (c) Microsoft. All rights reserved.

"""Protocol and in-memory implementation of the versioned DAG registry.

A `DagRegistry` stores immutable, content-hashed `DagVersion` registrations.
Registering the same content twice (identical `dag_id` + `version`) is a
no-op that returns the original registration, so compiling and re-registering
an unchanged DAG is always safe to repeat (for example from CI on every
push). The in-memory implementation is thread-safe but, as its name implies,
not durable; the `DagRegistry` protocol exists precisely so a durable backend
(a database, blob storage, etc.) can be substituted without changing the
`ControlPlane` that depends on it.
"""

from __future__ import annotations

import threading
from typing import Protocol, runtime_checkable

from ._models import DagVersion

__all__ = ["DagRegistry", "InMemoryDagRegistry"]


@runtime_checkable
class DagRegistry(Protocol):
    """Storage protocol for versioned, immutable DAG registrations."""

    async def register(self, dag_version: DagVersion) -> DagVersion:
        """Register a compiled `DagVersion`.

        If a registration with the same `dag_id` and `version` already
        exists, it is returned unchanged (registration is idempotent by
        content hash); the newly supplied `dag_version` is discarded.
        """
        ...

    async def get(self, dag_id: str, version: str | None = None) -> DagVersion | None:
        """Return a specific version, or the most recently registered version if `version` is omitted."""
        ...

    async def list_versions(self, dag_id: str) -> list[DagVersion]:
        """Return all registered versions for `dag_id`, oldest first."""
        ...

    async def list_dag_ids(self) -> list[str]:
        """Return all registered DAG ids."""
        ...


class InMemoryDagRegistry:
    """Thread-safe, in-process `DagRegistry` suitable for tests and single-process deployments."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._versions: dict[str, dict[str, DagVersion]] = {}
        self._insertion_order: dict[str, list[str]] = {}

    async def register(self, dag_version: DagVersion) -> DagVersion:
        with self._lock:
            versions = self._versions.setdefault(dag_version.dag_id, {})
            existing = versions.get(dag_version.version)
            if existing is not None:
                return existing
            versions[dag_version.version] = dag_version
            self._insertion_order.setdefault(dag_version.dag_id, []).append(dag_version.version)
            return dag_version

    async def get(self, dag_id: str, version: str | None = None) -> DagVersion | None:
        with self._lock:
            versions = self._versions.get(dag_id)
            if not versions:
                return None
            if version is not None:
                return versions.get(version)
            order = self._insertion_order.get(dag_id, [])
            return versions.get(order[-1]) if order else None

    async def list_versions(self, dag_id: str) -> list[DagVersion]:
        with self._lock:
            versions = self._versions.get(dag_id, {})
            order = self._insertion_order.get(dag_id, [])
            return [versions[v] for v in order]

    async def list_dag_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._versions)
