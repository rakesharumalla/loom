"""Incremental ingest: source files in a UC Volume -> Lakebase files/versions -> chunks + embeddings.

A source sync lands files in the Volume and triggers this; in the demo it runs as a Lakeflow Job or from the app.

- Hash-based: a file whose bytes are unchanged is skipped; a changed file gets a new version, and only
  chunks whose content hash changed are re-embedded.
- Removed files: files under the synced prefix that no longer exist in the source are soft-deleted
  (history kept) and their chunks are removed from the index.
- Parsing: markdown/text read directly; PDF/DOCX/PPTX/images go through `ai_parse_document` on a SQL
  warehouse (https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document).
- Timing: each change records changed_at (Volume last-modified) and searchable_at (after commit).
Implemented in the pipeline (not a Databricks feature).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

def _repo_root() -> Path:
    """Repo root (the folder holding Loom/). Serverless spark_python_task runs without __file__."""
    cands = []
    try:
        cands.append(Path(__file__).resolve().parents[1])
    except NameError:
        pass
    if sys.argv and sys.argv[0]:
        cands += [Path(sys.argv[0]).resolve().parent.parent, Path(sys.argv[0]).resolve().parent]
    cands += [Path.cwd(), Path.cwd().parent]
    return next((c for c in cands if (c / "Loom").is_dir()), cands[0])


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.adapters.databricks.uc_sql import run_sql  # noqa: E402

TEXT_EXT = {".md", ".txt"}
PARSE_EXT = {".pdf", ".docx", ".doc", ".pptx", ".ppt", ".png", ".jpg", ".jpeg", ".tif", ".tiff"}
# source dir under {volume}/sources -> (space, path prefix inside the space KB)
SYNCS = {"company": ("company", "synced/")}


def list_source(w: WorkspaceClient, root: str) -> dict[str, tuple[str, datetime | None]]:
    """relative path -> (volume path, last_modified)."""
    out = {}

    def walk(d: str):
        for e in w.files.list_directory_contents(d):
            if e.is_directory:
                walk(e.path)
            else:
                lm = datetime.fromtimestamp(e.last_modified / 1000, tz=timezone.utc) if e.last_modified else None
                out[e.path[len(root) + 1:]] = (e.path, lm)
    try:
        walk(root)
    except Exception as ex:  # missing dir -> nothing to sync
        print(f"[ingest] source {root}: {ex}")
    return out


def parse(w: WorkspaceClient, vpath: str) -> bytes:
    ext = Path(vpath).suffix.lower()
    if ext in TEXT_EXT:
        return w.files.download(vpath).contents.read()
    if ext in PARSE_EXT:
        rows = run_sql(w, f"""SELECT ai_parse_document(content):document:elements FROM read_files('{vpath}',
                               format => 'binaryFile')""")
        elements = json.loads(rows[0][0]) if rows and rows[0][0] else []
        text = "\n\n".join(e.get("content", "") for e in elements if isinstance(e, dict))
        return f"---\ntitle: {Path(vpath).stem}\nparsed_by: ai_parse_document\n---\n{text}".encode()
    raise ValueError(f"unsupported file type {ext}")


def run(w: WorkspaceClient | None = None, reindex_bm25: bool = False) -> dict:
    from Loom.wiring import build
    w = w or WorkspaceClient(profile=CONFIG.profile)
    svc, ingestor, raw = build(w, index_now=False)
    storage, conn = raw["storage"], raw["conn"]
    summary = {"files": [], "started": datetime.now(timezone.utc).isoformat()}
    t_job = time.perf_counter()
    for src_dir, (space_id, prefix) in SYNCS.items():
        kb = svc.registry.get(space_id).bindings["kb"]
        source = list_source(w, f"{CONFIG.volume_path}/sources/{src_dir}")
        for rel, (vpath, changed_at) in sorted(source.items()):
            path = prefix + (str(Path(rel).with_suffix(".md")) if Path(rel).suffix.lower() in PARSE_EXT else rel)
            data = parse(w, vpath)
            cur = storage.stat(kb, path)
            if cur and cur.content_hash == __import__("pipelines.ingest_core", fromlist=["x"]).sha256(data):
                st = ingestor.index_file(kb, path)          # repairs missing chunks; normally a no-op
                action = "skip" if st.chunks_embedded == 0 else "reindex"
            else:
                storage.put_file(kb, path, data, "ingest:volume")
                st = ingestor.index_file(kb, path)
                action = "upsert"
            summary["files"].append(_event(conn, kb, path, action, st.chunks_embedded, st.chunks_deleted,
                                           changed_at if action != "skip" else None))
        # removals: synced files that disappeared from the source
        for f in storage.list_files(kb, prefix):
            rel = f.path[len(prefix):]
            if rel not in source and not any(Path(r).with_suffix(".md").as_posix() == rel for r in source):
                storage.delete_file(kb, f.path, "ingest:volume")
                n = ingestor.remove_file(f.file_id)
                summary["files"].append(_event(conn, kb, f.path, "delete", 0, n, datetime.now(timezone.utc)))
    if reindex_bm25:
        summary["bm25_reindex_s"] = round(raw["search"].reindex_bm25(), 2)
    summary["job_s"] = round(time.perf_counter() - t_job, 2)
    return summary


def _event(conn, kb, path, action, embedded, deleted, changed_at) -> dict:
    searchable_at = datetime.now(timezone.utc)
    lag = (searchable_at - changed_at).total_seconds() if changed_at else None
    with conn.tx() as c:
        c.execute("""INSERT INTO loom.ingest_events (kb_id, path, action, chunks_embedded, chunks_deleted, changed_at,
                                                     searchable_at, detail) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                  (kb, path, action, embedded, deleted, changed_at, searchable_at,
                   Jsonb({"change_to_searchable_s": lag})))
    return {"path": path, "action": action, "chunks_embedded": embedded, "chunks_deleted": deleted,
            "change_to_searchable_s": round(lag, 2) if lag is not None else None}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reindex-bm25", action="store_true")
    args, _ = ap.parse_known_args()
    print(json.dumps(run(WorkspaceClient() if _in_databricks() else None, args.reindex_bm25), indent=2, default=str))
    return 0


def _in_databricks() -> bool:
    import os
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


if __name__ == "__main__":
    rc = main()
    if rc:   # serverless job runners treat any SystemExit, even 0, as a failure
        sys.exit(rc)
