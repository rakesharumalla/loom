"""Seed the live demo: spaces via configure, corpus via the ingest path, facts via contribute,
then build Lakebase Search indexes (after the load, since BM25 statistics are computed at build time).
Idempotent: re-running only adds what changed."""
from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.identity import persona_map  # noqa: E402
from Loom.seed import DATA, seed_corpus, seed_facts, seed_spaces, seed_transcripts  # noqa: E402
from Loom.spaces.model import Identity  # noqa: E402
from Loom.wiring import build  # noqa: E402


def identities(w: WorkspaceClient) -> dict[str, Identity]:
    """Seed-job identities, built in trusted code from the real SP application ids."""
    by_app = persona_map(w, CONFIG.personas)
    out = {}
    for app_id, p in by_app.items():
        out[p] = Identity(p, app_id, tuple(CONFIG.personas[p]["groups"]), is_platform_admin=(p == "admin"))
    return out


def main() -> int:
    w = WorkspaceClient(profile=CONFIG.profile)
    svc, ingestor, raw = build(w, index_now=False)
    ids = identities(w)
    t = time.perf_counter()
    spaces = seed_spaces(svc, ids["admin"])
    print(f"[seed] {len(spaces)} spaces configured ({time.perf_counter() - t:.1f}s): {', '.join(spaces)}")
    for label, root in (("hand-written", DATA / "corpus"), ("generated", DATA / "corpus_gen")):
        t = time.perf_counter()
        stats = seed_corpus(svc, ingestor, root=root, workers=8)
        emb = sum(s.chunks_embedded for s in stats)
        print(f"[seed] corpus {label}: {len(stats)} files, {emb} chunks embedded, "
              f"{sum(s.chunks_total for s in stats)} chunks total ({time.perf_counter() - t:.1f}s)")
    t = time.perf_counter()
    with raw["conn"].connect(autocommit=True) as c:
        c.execute((ROOT / "sql" / "indexes.sql").read_text())
        c.execute("SELECT set_config('loom.writer','on',false)")
        n = c.execute("SELECT count(*) FROM loom.chunks").fetchone()[0]
        info = c.execute("SELECT lakebase_ann_index_info('loom.chunks_embedding_ann')").fetchone()[0]
    print(f"[seed] indexes ready ({time.perf_counter() - t:.1f}s); chunks={n}; ann index info={info}")
    t = time.perf_counter()
    res = seed_facts(svc, ids)
    print(f"[seed] facts: {len(res)} contributed ({time.perf_counter() - t:.1f}s): "
          + ", ".join(f"{r.space_id}:{r.status}" for r in res))
    t = time.perf_counter()
    names = seed_transcripts(svc)
    print(f"[seed] transcripts: {len(names)} sessions in managed session stores ({time.perf_counter() - t:.1f}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
