"""MemoryPort on Databricks managed agent memory (Beta), via the AgentKit SDK (databricks-agentbricks).

https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory
- Search is full-text BM25, top-N up to 100, no vector similarity.
- Access control is per store; actor_id partitions entries but is not a security boundary.
  Loom therefore binds one store per memory-bearing space and decides which stores a caller may query.
"""
from __future__ import annotations

import threading

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks_agentkit import AgentKitClient

from Loom.ports import MemoryEntry

SEARCH_MAX = 100   # documented top-N cap


def is_not_found(e: BaseException) -> bool:
    """AgentKit wraps SDK errors (AgentCliError); look through the cause chain for NotFound."""
    while e is not None:
        if isinstance(e, NotFound):
            return True
        e = e.__cause__
    return False


class ManagedMemory:
    def __init__(self, w: WorkspaceClient):
        self.client = AgentKitClient(w)
        self._stores: dict = {}
        self._lock = threading.Lock()

    def _store(self, store: str):
        with self._lock:
            if store not in self._stores:
                self._stores[store] = self.client.memory_stores.get(store)
            return self._stores[store]

    def ensure_store(self, store: str, description: str = "") -> str:
        try:
            self._store(store)
        except Exception as e:
            if not is_not_found(e):
                raise
            s = self.client.memory_stores.create(store, description=description or None)
            with self._lock:
                self._stores[store] = s
        return store

    @staticmethod
    def _entry(store: str, m, score: float | None = None) -> MemoryEntry:
        return MemoryEntry(m.name, store, m.actor_id, m.path, m.content, m.description, m.session_id,
                           score, m.create_time)

    def add(self, store, actor_id, path, content, description=None, session_id=None) -> MemoryEntry:
        m = self._store(store).add(actor_id=actor_id, path=path, content=content, description=description,
                                   session_id=session_id)
        return self._entry(store, m)

    def search(self, store, actor_id, query, limit=10, path_prefix=None) -> list[MemoryEntry]:
        res = self._store(store).search(actor_id=actor_id, query=query, limit=min(limit, SEARCH_MAX),
                                        path_prefix=path_prefix)
        return [self._entry(store, r.memory, r.score) for r in res]

    def list(self, store, actor_id, path_prefix=None) -> list[MemoryEntry]:
        return [self._entry(store, m) for m in self._store(store).list(actor_id=actor_id, path_prefix=path_prefix)]

    def delete(self, name: str) -> None:
        # name: memory-stores/{store}/memories/{id}
        parts = name.split("/")
        store, mid = parts[1], parts[-1]
        self._store(store).get(mid).delete()

    def delete_store(self, store: str) -> None:
        try:
            self._store(store).delete()
        except Exception as e:
            if not is_not_found(e):
                raise
        with self._lock:
            self._stores.pop(store, None)
