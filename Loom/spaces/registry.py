"""Space registry: lookup and membership queries over the control-plane store. Implemented in Loom."""
from __future__ import annotations

import copy
import time

from Loom import querylog
from Loom.ports import ControlPlanePort
from Loom.spaces import policy
from Loom.spaces.model import SCOPES, Identity, SpaceRecord


class SpaceRegistry:
    """Reads are cached for ttl_s and invalidated on every write through this registry. With several
    facade replicas, a configure on one replica is visible to the others within ttl_s."""

    def __init__(self, store: ControlPlanePort, ttl_s: float = 5.0):
        self.store = store
        self.ttl_s = ttl_s
        self._cache: tuple[float, list[SpaceRecord]] | None = None

    def _invalidate(self) -> None:
        self._cache = None

    def all(self) -> list[SpaceRecord]:
        now = time.monotonic()
        if self._cache is None or now - self._cache[0] > self.ttl_s:
            recs = sorted(self.store.list_spaces(), key=lambda r: (SCOPES.index(r.space.scope), r.space_id))
            self._cache = (now, recs)
        else:
            querylog.note(f"Space registry (loom.spaces, space_members, space_guardrails, space_bindings) "
                          f"served from the in-process cache ({self.ttl_s:g} s TTL): no SQL", unless="FROM loom.spaces")
        return [copy.deepcopy(r) for r in self._cache[1]]

    def get(self, space_id: str) -> SpaceRecord | None:
        return next((r for r in self.all() if r.space_id == space_id), None)

    def save(self, rec: SpaceRecord) -> bool:
        self._invalidate()
        created = self.store.upsert_space(rec)
        self._invalidate()
        return created

    def delete(self, space_id: str) -> None:
        self._invalidate()
        self.store.delete_space(space_id)
        self._invalidate()

    def memberships(self, identity: Identity) -> dict[str, list[str]]:
        """Scope -> keys of spaces the identity can read (e.g. {"team": ["care"]})."""
        out: dict[str, list[str]] = {s: [] for s in SCOPES}
        for rec in self.all():
            if policy.decide(identity, rec, "read").allowed:
                out[rec.space.scope].append(rec.space.key or "company")
        return out
