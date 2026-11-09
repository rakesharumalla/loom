"""SessionPort on Databricks managed agent sessions (Beta), via the AgentKit SDK.

https://docs.databricks.com/aws/en/agents/agent-memory/managed-sessions
Sessions are durable across requests and listable, so the distillation job reads them directly.
"""
from __future__ import annotations

import threading

from databricks.sdk import WorkspaceClient
from databricks_agentkit import AgentKitClient

from Loom.adapters.databricks.memory import is_not_found


class ManagedSessions:
    def __init__(self, w: WorkspaceClient):
        self.client = AgentKitClient(w)
        self._stores: dict = {}
        self._sessions: dict = {}
        self._lock = threading.Lock()

    def _store(self, store: str):
        with self._lock:
            if store not in self._stores:
                self._stores[store] = self.client.session_stores.get(store)
            return self._stores[store]

    def ensure_store(self, store: str) -> str:
        try:
            self._store(store)
        except Exception as e:
            if not is_not_found(e):
                raise
            s = self.client.session_stores.create(store, description="Loom short-term state")
            with self._lock:
                self._stores[store] = s
        return store

    def open(self, store: str, actor_id: str, session_id: str, metadata: dict | None = None) -> str:
        name = f"{store}/{session_id}"
        if name in self._sessions:
            return name
        st = self._store(store)
        try:
            sess = st.get(session_id)
            if sess.actor_id != actor_id:
                raise PermissionError(f"session {session_id} belongs to another actor")
        except Exception as e:
            if not is_not_found(e):
                raise
            sess = st.add(actor_id=actor_id, session_id=session_id, metadata=metadata)
        with self._lock:
            self._sessions[name] = sess
        return name

    def _session(self, name: str):
        if name not in self._sessions:
            store, sid = name.rsplit("/", 1)
            self._sessions[name] = self._store(store).get(sid)
        return self._sessions[name]

    def append(self, session_name: str, items: list[dict]) -> None:
        self._session(session_name).append_items(items)

    def items(self, session_name: str) -> list[dict]:
        return [i.data for i in self._session(session_name).list_items()]

    def list_sessions(self, store: str, actor_id: str | None = None) -> list[dict]:
        flt = f'actor_id = "{actor_id}"' if actor_id else None
        out = []
        for s in self._store(store).list(filter=flt) if flt else self._store(store).list():
            out.append({"name": f"{store}/{s.session_id}", "session_id": s.session_id, "actor_id": s.actor_id,
                        "metadata": s.metadata or {}, "last_activity_time": s.last_activity_time})
        return out
