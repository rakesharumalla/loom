"""Freshness demo (step 8): edit one source file, delete another, re-ingest, measure change -> searchable.

Prints measured timings only. Run: uv run python scripts/freshness_demo.py [--reset]
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.contract import RetrieveRequest  # noqa: E402
from pipelines import ingest_job  # noqa: E402

SRC = ROOT / "data" / "sources" / "company"


def vol(rel: str) -> str:
    return f"{CONFIG.volume_path}/sources/company/{rel}"


def upload_all(w):
    for f in SRC.glob("*.md"):
        w.files.upload(vol(f.name), io.BytesIO(f.read_bytes()), overwrite=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="restore the original sources first")
    args = ap.parse_args()
    w = WorkspaceClient(profile=CONFIG.profile)
    upload_all(w)   # start from the original sources (--reset kept for compatibility)
    print("[fresh] baseline ingest")
    base = ingest_job.run(w)
    print(json.dumps(base["files"], indent=1))

    from Loom.wiring import build
    from scripts.seed import identities
    svc, _, _ = build(w)
    bob = identities(w)["bob"]
    svc.retrieve(bob, RetrieveRequest(query="warm up", k=1), "job")

    token = f"FRESH-{uuid.uuid4().hex[:6].upper()}"
    edited = (SRC / "node-upgrade.md").read_text() + (
        f"\nUpdate {token}: after upgrading, restart the withdrawal batcher so it reconnects to the node RPC.\n")
    t_change = time.time()
    w.files.upload(vol("node-upgrade.md"), io.BytesIO(edited.encode()), overwrite=True)
    w.files.delete(vol("api-rate-limits.md"))
    print(f"[fresh] edited node-upgrade.md (token {token}), deleted api-rate-limits.md")

    res = ingest_job.run(w)
    t_job_done = time.time()
    print(json.dumps(res["files"], indent=1))

    polls, found_at = 0, None
    while polls < 30:
        polls += 1
        r = svc.retrieve(bob, RetrieveRequest(query=f"node upgrade restart withdrawal batcher {token}", k=5), "job")
        if any(token in x.content for x in r.results):
            found_at = time.time()
            break
        time.sleep(1)
    gone = svc.retrieve(bob, RetrieveRequest(query="public API rate limits requests per second per API key", k=5), "job")
    still = [x.provenance.path for x in gone.results if x.provenance.path == "synced/api-rate-limits.md"]
    print("\n[fresh] measured:")
    print(f"  ingest job wall time (incremental): {res['job_s']:.2f} s")
    print(f"  change -> searchable (upload to first retrieve hit): "
          f"{(found_at - t_change):.2f} s" if found_at else "  edited content NOT found")
    print(f"  of which ingest job: {t_job_done - t_change:.2f} s; polls needed after job: {polls}")
    emb = {f['path']: f['chunks_embedded'] for f in res['files']}
    print(f"  chunks re-embedded per file: {emb}")
    print(f"  deleted file still retrievable: {bool(still)}")
    reidx = ingest_job.run(w, reindex_bm25=True).get("bm25_reindex_s")
    print(f"  BM25 REINDEX (refresh corpus statistics): {reidx} s")
    upload_all(w)   # restore original sources for the next run (deleted file comes back on next ingest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
