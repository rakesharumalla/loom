"""Storage ports: the only way Loom reaches storage. This file is the swap surface.

Only Databricks adapters exist (Loom/adapters/databricks). Today's systems map to these ports:

  Port               Today (not implemented here)              Databricks adapter
  StoragePort        MongoDB Atlas metadata + S3 bytes          Lakebase (rows + inline text) + UC Volume (large raw)
  SearchPort         Bedrock KBs + OpenSearch (Titan v2)        Lakebase Search (lakebase_ann + lakebase_bm25, RRF)
  MemoryPort         (none / app-specific)                      Managed agent memory (one store per space)
  SessionPort        (none / app-specific)                      Managed agent sessions
  ControlPlanePort   MongoDB collections                        Lakebase tables (spaces, audit, traces, ...)

Every method takes trusted inputs only: callers pass ids derived from a verified Identity.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator, Protocol, runtime_checkable

from Loom.spaces.model import SpaceRecord


# ---------------------------------------------------------------- value types
@dataclass(frozen=True)
class FileVersion:
    file_id: str
    kb_id: str
    space_id: str
    path: str
    version: int
    content_hash: str
    size: int
    inline: bool                    # True: text in Lakebase row; False: bytes in the Volume
    volume_path: str | None
    created_by: str
    created_at: datetime | None = None
    content_type: str = "text/markdown"


@dataclass(frozen=True)
class DiffEntry:
    path: str
    change: str                     # added | removed | modified
    before: str | None              # content_hash
    after: str | None


@dataclass
class Chunk:
    chunk_id: str
    kb_id: str
    space_id: str
    file_id: str
    version: int
    chunk_index: int
    path: str
    content: str
    content_hash: str
    access_groups: list[str]
    embedding: list[float] | None = None
    metadata: dict = field(default_factory=dict)


@dataclass
class Hit:
    chunk_id: str
    kb_id: str
    space_id: str
    file_id: str
    version: int
    path: str
    content: str
    metadata: dict
    vector_rank: int | None
    bm25_rank: int | None
    rrf: float
    updated_at: datetime | None = None


@dataclass
class MemoryEntry:
    name: str                       # store-qualified entry name
    store: str
    actor_id: str
    path: str
    content: str
    description: str | None
    session_id: str | None
    score: float | None = None
    created_at: datetime | None = None


# ---------------------------------------------------------------- ports
@runtime_checkable
class StoragePort(Protocol):
    """Paths, files, versions, overlays, snapshots, diff/restore, KB members, leases.

    Atomicity: put_file writes the Volume object (if large) BEFORE the metadata transaction.
    Volume objects are content-addressed (/blobs/{sha256}) and write-once, so a crash after the
    Volume write leaves only an unreferenced blob (GC-able), never partial metadata.
    """
    def create_kb(self, kb_id: str, name: str, space_id: str) -> None: ...
    def add_kb_member(self, kb_id: str, principal: str, role: str) -> None: ...
    def list_kb_members(self, kb_id: str) -> list[tuple[str, str]]: ...
    def put_file(self, kb_id: str, path: str, data: bytes, actor: str,
                 content_type: str = "text/markdown", _fail_after_volume: bool = False) -> FileVersion: ...
    def read_file(self, kb_id: str, path: str, version: int | None = None) -> bytes: ...
    def stat(self, kb_id: str, path: str) -> FileVersion | None: ...
    def list_files(self, kb_id: str, prefix: str = "") -> list[FileVersion]: ...
    def history(self, kb_id: str, path: str) -> list[FileVersion]: ...
    def delete_file(self, kb_id: str, path: str, actor: str) -> None: ...
    def snapshot(self, kb_id: str, label: str, actor: str) -> str: ...
    def diff(self, kb_id: str, from_snapshot: str, to_snapshot: str | None = None) -> list[DiffEntry]: ...
    def restore(self, kb_id: str, snapshot_id: str, actor: str) -> list[DiffEntry]: ...
    def create_overlay(self, kb_id: str, name: str, actor: str) -> str: ...
    def overlay_put(self, overlay_id: str, path: str, data: bytes) -> None: ...
    def overlay_delete(self, overlay_id: str, path: str) -> None: ...
    def overlay_read(self, overlay_id: str, path: str) -> bytes | None: ...
    def merge_overlay(self, overlay_id: str, actor: str) -> list[DiffEntry]: ...
    def acquire_lease(self, resource: str, holder: str, ttl_s: int) -> bool: ...
    def release_lease(self, resource: str, holder: str) -> None: ...


@runtime_checkable
class SearchPort(Protocol):
    """Hybrid retrieval over pooled KBs. Filters are applied inside the query (space/kb + access)."""
    def search(self, space_ids: list[str], query: str, caller_principals: list[str], k: int = 10,
               kb_ids: list[str] | None = None, mode: str = "hybrid") -> list[Hit]: ...
    def upsert_chunks(self, chunks: list[Chunk]) -> int: ...
    def delete_chunks(self, chunk_ids: list[str]) -> int: ...
    def delete_file_chunks(self, file_id: str) -> int: ...
    def existing_hashes(self, file_id: str) -> dict[int, str]: ...
    def facts_for_subject(self, subject: str, space_ids: list[str], caller_principals: list[str]) -> list[Hit]: ...
    def retag_space(self, space_id: str, access_groups: list[str]) -> int: ...


@runtime_checkable
class MemoryPort(Protocol):
    """Long-term memory. Managed memory: BM25-only search, store-level ACL, actor_id partitions
    (not a security boundary). Loom binds one store per memory-bearing space."""
    def ensure_store(self, store: str, description: str = "") -> str: ...
    def add(self, store: str, actor_id: str, path: str, content: str,
            description: str | None = None, session_id: str | None = None) -> MemoryEntry: ...
    def search(self, store: str, actor_id: str, query: str, limit: int = 10,
               path_prefix: str | None = None) -> list[MemoryEntry]: ...
    def list(self, store: str, actor_id: str, path_prefix: str | None = None) -> list[MemoryEntry]: ...
    def delete(self, name: str) -> None: ...
    def delete_store(self, store: str) -> None: ...


@runtime_checkable
class SessionPort(Protocol):
    """Short-term conversation state (managed agent sessions)."""
    def ensure_store(self, store: str) -> str: ...
    def open(self, store: str, actor_id: str, session_id: str, metadata: dict | None = None) -> str: ...
    def append(self, session_name: str, items: list[dict]) -> None: ...
    def items(self, session_name: str) -> list[dict]: ...
    def list_sessions(self, store: str, actor_id: str | None = None) -> list[dict]: ...


@runtime_checkable
class ControlPlanePort(Protocol):
    """Persistence for Loom's control plane (spaces, audit, traces, conflicts, promotions)."""
    def upsert_space(self, rec: SpaceRecord) -> bool: ...
    def get_space(self, space_id: str) -> SpaceRecord | None: ...
    def list_spaces(self) -> list[SpaceRecord]: ...
    def delete_space(self, space_id: str) -> None: ...
    def write_audit(self, row: dict) -> str: ...
    def write_audit_many(self, rows: list[dict]) -> None: ...
    def audit_for(self, request_id: str) -> list[dict]: ...
    def tail_audit(self, n: int = 50) -> list[dict]: ...
    def write_trace(self, row: dict) -> None: ...
    def write_conflict(self, row: dict) -> str: ...
    def list_conflicts(self, n: int = 50) -> list[dict]: ...
    def create_promotion(self, row: dict) -> str: ...
    def get_promotion(self, request_id: str) -> dict | None: ...
    def decide_promotion(self, request_id: str, status: str, approver: str) -> None: ...
    def list_promotions(self, status: str | None = None) -> list[dict]: ...
    def write_feedback(self, row: dict) -> None: ...


def iter_batches(items: list[Any], n: int) -> Iterator[list[Any]]:
    for i in range(0, len(items), n):
        yield items[i:i + n]
