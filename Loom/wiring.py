"""Build LoomService on the Databricks adapters. The only module that knows which adapters exist."""
from __future__ import annotations

from databricks.sdk import WorkspaceClient

from config import CONFIG
from Loom.adapters.databricks.capture import record_rest
from Loom.adapters.databricks.embeddings import DatabricksEmbedder
from Loom.adapters.databricks.lakebase_conn import LakebaseConnector
from Loom.adapters.databricks.lakebase_store import LakebaseControl, LakebaseStorage
from Loom.adapters.databricks.memory import ManagedMemory
from Loom.adapters.databricks.search_lakebase import LakebaseSearch
from Loom.adapters.databricks.sessions import ManagedSessions
from Loom.adapters.databricks.volume_store import VolumeStore
from Loom.service import LoomService, PortCallLog, Ports, Traced
from pipelines.ingest_core import Ingestor


def _storage_backend(method: str) -> str:
    return "Lakebase + UC Volume" if method in ("put_file", "read_file", "overlay_put", "overlay_read") else "Lakebase"


def build(w: WorkspaceClient | None = None, branch_path: str | None = None, index_now: bool = True):
    """Returns (service, ingestor, raw adapters dict)."""
    w = w or WorkspaceClient(profile=CONFIG.profile)
    record_rest(w)   # "Show queries" in the UI; no-op outside a capture
    conn = LakebaseConnector(w, branch_path=branch_path)
    embedder = DatabricksEmbedder(w)
    raw = {
        "conn": conn, "embedder": embedder,
        "control": LakebaseControl(conn),
        "storage": LakebaseStorage(conn, VolumeStore(w, CONFIG.volume_path), CONFIG.inline_max_bytes),
        "search": LakebaseSearch(conn, embedder),
        "memory": ManagedMemory(w),
        "sessions": ManagedSessions(w),
    }
    log = PortCallLog()
    ports = Ports(
        control=Traced(raw["control"], "ControlPlanePort", "Lakebase", log),
        storage=Traced(raw["storage"], "StoragePort", _storage_backend, log),
        search=Traced(raw["search"], "SearchPort", "Lakebase Search", log),
        memory=Traced(raw["memory"], "MemoryPort", "Managed agent memory", log),
        sessions=Traced(raw["sessions"], "SessionPort", "Managed agent sessions", log),
    )
    svc = LoomService(ports, port_log=log)
    ingestor = Ingestor(ports.storage, ports.search, embedder, svc.access_for_space)
    if index_now:
        svc.indexer = lambda kb, path: ingestor.index_file(kb, path)
    return svc, ingestor, raw
