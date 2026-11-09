"""In-memory test doubles for every port (unit/parity tests only; not storage adapters)."""
from __future__ import annotations

import copy
import dataclasses
import itertools
import re
import uuid
from datetime import datetime, timedelta, timezone

from Loom.control.fusion import rrf
from Loom.ports import Chunk, DiffEntry, FileVersion, Hit, MemoryEntry
from Loom.spaces.model import SpaceRecord
from pipelines.ingest_core import sha256

_clock = itertools.count()
_ids = itertools.count()


def reset() -> None:
    """Deterministic clock and ids so identical call sequences yield identical state."""
    global _clock, _ids
    _clock, _ids = itertools.count(), itertools.count()


def new_id() -> str:
    return str(uuid.UUID(int=next(_ids) + 1))


def now() -> datetime:
    # strictly increasing timestamps keep ordering deterministic
    return datetime(2026, 10, 1, tzinfo=timezone.utc) + timedelta(milliseconds=next(_clock))


def _terms(s: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", s.lower())


class FakeControl:
    def __init__(self):
        self.spaces: dict[str, SpaceRecord] = {}
        self.audit: list[dict] = []
        self.traces, self.conflicts, self.feedback = [], [], []
        self.promos: dict[str, dict] = {}

    def upsert_space(self, rec):
        created = rec.space_id not in self.spaces
        self.spaces[rec.space_id] = copy.deepcopy(rec)
        return created

    def get_space(self, sid):
        return copy.deepcopy(self.spaces.get(sid))

    def list_spaces(self):
        return [copy.deepcopy(r) for r in self.spaces.values()]

    def delete_space(self, sid):
        self.spaces.pop(sid, None)

    def write_audit(self, row):
        r = {"event_id": str(uuid.uuid4()), "ts": now(), **copy.deepcopy(row)}
        self.audit.append(r)
        return r["event_id"]

    def write_audit_many(self, rows):
        for r in rows:
            self.write_audit(r)

    def audit_for(self, rid):
        return [r for r in self.audit if r["request_id"] == rid]

    def tail_audit(self, n=50):
        return self.audit[-n:]

    def write_trace(self, row):
        self.traces.append(row)

    def write_conflict(self, row):
        self.conflicts.append(row)
        return str(len(self.conflicts))

    def list_conflicts(self, n=50):
        return self.conflicts[-n:]

    def create_promotion(self, row):
        rid = new_id()
        self.promos[rid] = {"request_id": rid, "status": "pending", **row}
        return rid

    def get_promotion(self, rid):
        return copy.deepcopy(self.promos.get(rid))

    def decide_promotion(self, rid, status, approver):
        self.promos[rid].update(status=status, approver=approver)

    def list_promotions(self, status=None):
        return [p for p in self.promos.values() if status is None or p["status"] == status]

    def write_feedback(self, row):
        self.feedback.append(row)


class FakeStorage:
    def __init__(self):
        self.kbs: dict[str, dict] = {}
        self.files: dict[tuple[str, str], list[tuple[FileVersion, bytes]]] = {}
        self.deleted: set[tuple[str, str]] = set()
        self.members: dict[str, dict[str, str]] = {}
        self.snaps: dict[str, dict] = {}
        self.overlays: dict[str, dict] = {}
        self.leases: dict[str, tuple[str, datetime]] = {}

    def create_kb(self, kb_id, name, space_id):
        self.kbs.setdefault(kb_id, {"name": name, "space_id": space_id})

    def add_kb_member(self, kb_id, principal, role):
        self.members.setdefault(kb_id, {})[principal] = role

    def list_kb_members(self, kb_id):
        return sorted(self.members.get(kb_id, {}).items())

    def put_file(self, kb_id, path, data, actor, content_type="text/markdown", _fail_after_volume=False):
        hist = self.files.setdefault((kb_id, path), [])
        fid = hist[0][0].file_id if hist else new_id()
        fv = FileVersion(fid, kb_id, self.kbs[kb_id]["space_id"], path, len(hist) + 1, sha256(data), len(data),
                         True, None, actor, now(), content_type)
        hist.append((fv, data))
        self.deleted.discard((kb_id, path))
        return fv

    def read_file(self, kb_id, path, version=None):
        hist = self.files[(kb_id, path)]
        return hist[-1][1] if version is None else hist[version - 1][1]

    def stat(self, kb_id, path):
        if (kb_id, path) in self.deleted or (kb_id, path) not in self.files:
            return None
        return self.files[(kb_id, path)][-1][0]

    def list_files(self, kb_id, prefix=""):
        return [self.stat(k, p) for (k, p) in self.files if k == kb_id and p.startswith(prefix) and self.stat(k, p)]

    def history(self, kb_id, path):
        return [fv for fv, _ in self.files.get((kb_id, path), [])]

    def delete_file(self, kb_id, path, actor):
        self.deleted.add((kb_id, path))

    def snapshot(self, kb_id, label, actor):
        sid = new_id()
        self.snaps[sid] = {f.path: f.content_hash for f in self.list_files(kb_id)}
        return sid

    def diff(self, kb_id, a, b=None):
        before = self.snaps[a]
        after = self.snaps[b] if b else {f.path: f.content_hash for f in self.list_files(kb_id)}
        out = []
        for p in sorted(set(before) | set(after)):
            if p not in after:
                out.append(DiffEntry(p, "removed", before[p], None))
            elif p not in before:
                out.append(DiffEntry(p, "added", None, after[p]))
            elif before[p] != after[p]:
                out.append(DiffEntry(p, "modified", before[p], after[p]))
        return out

    def restore(self, kb_id, snapshot_id, actor):
        raise NotImplementedError

    def create_overlay(self, kb_id, name, actor):
        raise NotImplementedError

    def overlay_put(self, *a): raise NotImplementedError
    def overlay_delete(self, *a): raise NotImplementedError
    def overlay_read(self, *a): raise NotImplementedError
    def merge_overlay(self, *a): raise NotImplementedError

    def acquire_lease(self, resource, holder, ttl_s):
        cur = self.leases.get(resource)
        if cur and cur[0] != holder and cur[1] > now():
            return False
        self.leases[resource] = (holder, now() + timedelta(seconds=ttl_s))
        return True

    def release_lease(self, resource, holder):
        if self.leases.get(resource, ("",))[0] == holder:
            del self.leases[resource]


class FakeSearch:
    """Term-overlap 'BM25' + a term-overlap stand-in for vectors; filters like the real adapter."""
    def __init__(self):
        self.chunks: dict[str, Chunk] = {}

    def search(self, space_ids, query, caller_principals, k=10, kb_ids=None, mode="hybrid"):
        q = set(_terms(query))
        visible = [c for c in self.chunks.values() if c.space_id in space_ids
                   and set(c.access_groups) & set(caller_principals) and (not kb_ids or c.kb_id in kb_ids)]
        scored = sorted(((len(q & set(_terms(c.content))), c.chunk_id) for c in visible), key=lambda t: (-t[0], t[1]))
        bm25 = [cid for s, cid in scored if s > 0]
        vec = [cid for _, cid in scored]
        fused = rrf({"vector": vec, "bm25": bm25} if mode == "hybrid" else {mode: bm25 if mode == "bm25" else vec})
        out = []
        for cid, score, ranks in fused[:k]:
            c = self.chunks[cid]
            out.append(Hit(cid, c.kb_id, c.space_id, c.file_id, c.version, c.path, c.content, c.metadata,
                           ranks.get("vector"), ranks.get("bm25"), score, now()))
        return out

    def upsert_chunks(self, chunks):
        for c in chunks:
            self.chunks[c.chunk_id] = copy.deepcopy(c)
        return len(chunks)

    def delete_chunks(self, ids):
        return sum(1 for i in ids if self.chunks.pop(i, None))

    def delete_file_chunks(self, file_id):
        return self.delete_chunks([i for i, c in list(self.chunks.items()) if c.file_id == file_id])

    def existing_hashes(self, file_id):
        return {c.chunk_index: c.content_hash for c in self.chunks.values() if c.file_id == file_id}

    def facts_for_subject(self, subject, space_ids, caller_principals):
        return [Hit(c.chunk_id, c.kb_id, c.space_id, c.file_id, c.version, c.path, c.content, c.metadata,
                    None, None, 0.0, now())
                for c in self.chunks.values() if c.space_id in space_ids and subject in (c.metadata.get("facts") or {})
                and set(c.access_groups) & set(caller_principals)]

    def retag_space(self, space_id, access_groups):
        n = 0
        for c in self.chunks.values():
            if c.space_id == space_id:
                c.access_groups = list(access_groups)
                n += 1
        return n


class FakeMemory:
    def __init__(self):
        self.stores: dict[str, list[MemoryEntry]] = {}

    def ensure_store(self, store, description=""):
        self.stores.setdefault(store, [])
        return store

    def add(self, store, actor_id, path, content, description=None, session_id=None):
        e = MemoryEntry(f"{store}/{new_id()}", store, actor_id, path, content, description, session_id,
                        None, now())
        self.stores[store].append(e)
        return e

    def search(self, store, actor_id, query, limit=10, path_prefix=None):
        q = set(_terms(query))
        scored = []
        for e in self.stores.get(store, []):
            if e.actor_id != actor_id or (path_prefix and not e.path.startswith(path_prefix)):
                continue
            s = len(q & set(_terms(e.content + " " + (e.description or ""))))
            if s:
                scored.append((s, e))
        scored.sort(key=lambda t: (-t[0], t[1].name))
        return [dataclasses.replace(e, score=float(s)) for s, e in scored[:limit]]

    def list(self, store, actor_id, path_prefix=None):
        return [e for e in self.stores.get(store, []) if e.actor_id == actor_id
                and (not path_prefix or e.path.startswith(path_prefix))]

    def delete(self, name):
        for es in self.stores.values():
            es[:] = [e for e in es if e.name != name]

    def delete_store(self, store):
        self.stores.pop(store, None)


class FakeSessions:
    def __init__(self):
        self.stores: dict[str, dict[str, dict]] = {}

    def ensure_store(self, store):
        self.stores.setdefault(store, {})
        return store

    def open(self, store, actor_id, session_id, metadata=None):
        name = f"{store}/sessions/{session_id}"
        self.stores[store].setdefault(name, {"actor_id": actor_id, "items": [], "metadata": metadata or {}})
        return name

    def append(self, session_name, items):
        store = session_name.split("/sessions/")[0]
        self.stores[store][session_name]["items"] += items

    def items(self, session_name):
        store = session_name.split("/sessions/")[0]
        return list(self.stores[store][session_name]["items"])

    def list_sessions(self, store, actor_id=None):
        return [{"name": n, "session_id": n.rsplit("/", 1)[-1], "actor_id": s["actor_id"], "metadata": s["metadata"]}
                for n, s in self.stores.get(store, {}).items() if actor_id is None or s["actor_id"] == actor_id]


class FakeEmbedder:
    def embed(self, texts):
        return [[float(len(t) % 7)] for t in texts]
