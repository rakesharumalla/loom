"""Benchmark Lakebase Search for Loom. Prints a markdown report. Every number is measured by this run.

Part A (demo corpus, 30 labeled questions): answer recall@10 and SQL latency p50/p95 for vector, bm25
and hybrid, filtered to the persona's readable spaces and access principals.

Part B (pooled-index filter test): a scratch table loom_bench.vec with N vectors (default 100,000) in
6 pooled KBs of skewed sizes (50%, 30%, 15%, 4%, 0.9%, 0.1%). Vectors are generated server-side as
noisy copies of the real chunk embeddings, so the index exercises IVF partitioning at a size the demo
corpus does not reach. Ground truth is exact kNN (index scans disabled) under the same filter.
Measures ANN recall@10 and latency with selective KB filters and an access filter, prefilter off/on.

Usage: uv run python bench/benchmark.py [--rows 100000] [--skip-build] [--out bench/results.md]
"""
from __future__ import annotations

import argparse
import platform
import socket
import statistics
import sys
import time
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

import yaml  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.adapters.databricks.search_lakebase import LakebaseSearch, _vec  # noqa: E402
from Loom.wiring import build  # noqa: E402

KB_SHARES = [("kb1", 0.50), ("kb2", 0.30), ("kb3", 0.15), ("kb4", 0.04), ("kb5", 0.009), ("kb6", 0.001)]


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, round(p / 100 * (len(xs) - 1))))]


def timed(f):
    t = time.perf_counter()
    out = f()
    return out, (time.perf_counter() - t) * 1000


# ---------------------------------------------------------------- Part A
def part_a(svc, raw, ids) -> list[str]:
    qs = yaml.safe_load((ROOT / "data" / "bench_questions.yaml").read_text())["questions"]
    search: LakebaseSearch = raw["search"]
    emb = raw["embedder"]
    vecs, emb_ms = [], []
    for q in qs:
        v, ms = timed(lambda: emb.embed([q["q"]])[0])
        vecs.append(v)
        emb_ms.append(ms)
    rows = ["| mode | recall@10 | p50 ms | p95 ms |", "|---|---|---|---|"]
    for mode in ("vector", "bm25", "hybrid"):
        hits, lat = 0, []
        for q, v in zip(qs, vecs):
            ident = ids[q["persona"]]
            spaces = [r.space_id for r in svc.registry.all()
                      if __import__("Loom.spaces.policy", fromlist=["x"]).decide(ident, r, "read").allowed]
            res, ms = timed(lambda: search.search(spaces, q["q"], ident.principal_tags, k=10, mode=mode, qvec=v))
            lat.append(ms)
            hits += any(h.space_id == q["space"] and h.path == q["path"] for h in res)
        rows.append(f"| {mode} | {hits / len(qs):.2f} ({hits}/{len(qs)}) | {pct(lat, 50):.0f} | {pct(lat, 95):.0f} |")
    rows.append(f"\nQuery embedding ({CONFIG.embedding_endpoint}): p50 {pct(emb_ms, 50):.0f} ms, p95 {pct(emb_ms, 95):.0f} ms "
                f"(n={len(emb_ms)}). SQL latency above excludes embedding (vectors precomputed).")
    return rows


# ---------------------------------------------------------------- Part B
def build_scale(conn, n: int) -> dict:
    with conn.connect(autocommit=True) as c:
        if n == 0:   # --skip-build: read-only (works for identities with only SELECT on loom_bench)
            counts = dict(c.execute("SELECT kb, count(*) FROM loom_bench.vec GROUP BY kb ORDER BY kb").fetchall())
            info = c.execute("SELECT lakebase_ann_index_info('loom_bench.vec_ann')").fetchone()[0]
            return {"load_s": 0.0, "index_s": 0.0, "counts": counts, "info": info}
        c.execute("CREATE SCHEMA IF NOT EXISTS loom_bench")
        c.execute("""CREATE TABLE IF NOT EXISTS loom_bench.vec (id bigserial PRIMARY KEY, kb text NOT NULL,
                     access text[] NOT NULL, embedding vector(1024) NOT NULL)""")
        have = c.execute("SELECT count(*) FROM loom_bench.vec").fetchone()[0]
        t = time.perf_counter()
        if have < n:
            c.execute("SELECT set_config('loom.writer','on',false)")
            c.execute("DROP TABLE IF EXISTS loom_bench.centers")
            c.execute("CREATE TABLE loom_bench.centers AS SELECT row_number() OVER () AS cid, embedding FROM loom.chunks")
            ncent = c.execute("SELECT count(*) FROM loom_bench.centers").fetchone()[0]
            batch = 10000
            for start in range(have, n, batch):
                m = min(batch, n - start)
                c.execute("""
                  INSERT INTO loom_bench.vec (kb, access, embedding)
                  SELECT CASE WHEN u < 0.50 THEN 'kb1' WHEN u < 0.80 THEN 'kb2' WHEN u < 0.95 THEN 'kb3'
                              WHEN u < 0.99 THEN 'kb4' WHEN u < 0.999 THEN 'kb5' ELSE 'kb6' END,
                         CASE WHEN g.i %% 10 = 0 THEN ARRAY['group:restricted'] ELSE ARRAY['group:all'] END,
                         c.embedding + n.noise
                  FROM (SELECT i, random() AS u, 1 + (random() * (%s::int - 1))::int AS cid
                        FROM generate_series(%s::int, %s::int) i) g
                  JOIN loom_bench.centers c ON c.cid = g.cid
                  CROSS JOIN LATERAL (SELECT array_agg((random() - 0.5) * 0.06 + 0 * j)::vector AS noise
                                      FROM generate_series(1, 1024) j WHERE g.i IS NOT NULL) n""", (ncent, start + 1, start + m))
        build_s = time.perf_counter() - t
        t = time.perf_counter()
        c.execute("CREATE INDEX IF NOT EXISTS vec_ann ON loom_bench.vec USING lakebase_ann (embedding vector_cosine_ops)")
        c.execute("CREATE INDEX IF NOT EXISTS vec_kb ON loom_bench.vec (kb)")
        c.execute("CREATE INDEX IF NOT EXISTS vec_access ON loom_bench.vec USING gin (access)")
        c.execute("ANALYZE loom_bench.vec")
        idx_s = time.perf_counter() - t
        counts = dict(c.execute("SELECT kb, count(*) FROM loom_bench.vec GROUP BY kb ORDER BY kb").fetchall())
        info = c.execute("SELECT lakebase_ann_index_info('loom_bench.vec_ann')").fetchone()[0]
    return {"load_s": build_s, "index_s": idx_s, "counts": counts, "info": info}


def knn(conn, qv, where, params, exact: bool, prefilter: bool, probes: str | None = None):
    """Returns (ids, client_ms, server_ms, plan) for one query. server_ms/plan come from EXPLAIN ANALYZE."""
    # Distinct SQL text for the exact baseline: psycopg auto-prepares repeated statements, and a plan cached
    # while index scans are disabled would otherwise be reused for the ANN timings.
    sql = (f"SELECT id FROM loom_bench.vec WHERE {where} ORDER BY embedding <=> %(q)s::vector LIMIT 10"
           + (" /* exact */" if exact else ""))
    with conn.tx() as c:
        if exact:
            c.execute("SET LOCAL enable_indexscan = off; SET LOCAL enable_bitmapscan = off")
        c.execute("SELECT set_config('lakebase_ann.prefilter', %s, true)", ("on" if prefilter else "off",))
        if probes is not None:
            c.execute("SELECT set_config('lakebase_ann.probes', %s, true)", (probes,))
        t = time.perf_counter()
        ids = [r[0] for r in c.execute(sql, {"q": qv, **params})]
        client_ms = (time.perf_counter() - t) * 1000
        plan = c.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, {"q": qv, **params}).fetchone()[0][0]
    return ids, client_ms, plan["Execution Time"], _plan_kind(plan["Plan"])


def _plan_kind(node) -> str:
    kinds = []

    def walk(n):
        if "Index Name" in n:
            kinds.append(f"{n['Node Type']}:{n['Index Name']}")
        elif n["Node Type"] in ("Seq Scan", "Sort"):
            kinds.append(n["Node Type"])
        for ch in n.get("Plans", []):
            walk(ch)
    walk(node)
    return " > ".join(kinds)


def part_b(raw, rows_n: int, skip_build: bool) -> list[str]:
    conn, emb = raw["conn"], raw["embedder"]
    meta = build_scale(conn, rows_n) if not skip_build else build_scale(conn, 0)
    qs = yaml.safe_load((ROOT / "data" / "bench_questions.yaml").read_text())["questions"]
    qvecs = [_vec(v) for v in emb.embed([q["q"] for q in qs])]
    import json
    info = json.loads(meta["info"]) if isinstance(meta["info"], str) else meta["info"]
    dp = (info.get("default_probes") or [])
    probe_variants = [None] + ([str(max(1, dp[0] // 5)), str(dp[0] * 4)] if len(dp) == 1 else [])
    scenarios = [("no filter", "true", {}),
                 ("kb = kb3 (~15%)", "kb = %(kb)s", {"kb": "kb3"}),
                 ("kb = kb5 (~0.9%)", "kb = %(kb)s", {"kb": "kb5"}),
                 ("kb = kb6 (~0.1%)", "kb = %(kb)s", {"kb": "kb6"}),
                 ("access filter (~10% rows)", "access && %(acc)s", {"acc": ["group:restricted"]}),
                 ("kb5 + access (~0.09%)", "kb = %(kb)s AND access && %(acc)s", {"kb": "kb5", "acc": ["group:restricted"]})]
    built = f"Load {meta['load_s']:.0f} s, index build {meta['index_s']:.0f} s. " if meta["load_s"] else "(pre-built table) "
    out = [f"Rows: {sum(meta['counts'].values()):,} ({', '.join(f'{k}={v:,}' for k, v in meta['counts'].items())}). "
           f"{built}Index info: `{meta['info']}`", "",
           "| filter | prefilter | probes | plan | recall@10 | server p50 ms | server p95 ms | client p50 ms | exact server p50 ms |",
           "|---|---|---|---|---|---|---|---|---|"]
    for label, where, params in scenarios:
        exact, exact_ms = [], []
        for qv in qvecs:
            ids, _, sms, _ = knn(conn, qv, where, params, exact=True, prefilter=False)
            exact.append(ids)
            exact_ms.append(sms)
        for prefilter in (False, True):
            for probes in probe_variants:
                rec, cl, sv, plans = [], [], [], set()
                for qv, truth in zip(qvecs, exact):
                    ids, cms, sms, plan = knn(conn, qv, where, params, exact=False, prefilter=prefilter, probes=probes)
                    cl.append(cms)
                    sv.append(sms)
                    plans.add(plan)
                    rec.append(len(set(ids) & set(truth)) / max(1, len(truth)))
                out.append(f"| {label} | {'on' if prefilter else 'off'} | {probes or f'auto ({dp[0] if dp else 0})'} | "
                           f"{'; '.join(sorted(plans))} | {statistics.mean(rec):.3f} | {pct(sv, 50):.2f} | {pct(sv, 95):.2f} | "
                           f"{pct(cl, 50):.0f} | {pct(exact_ms, 50):.1f} |")
    out += ["", "Notes: vectors are noisy copies of 960 real chunk embeddings, so neighbors cluster tightly; "
            "recall on production embeddings may be lower and should be re-measured on customer data. "
            "Server times are EXPLAIN ANALYZE execution times; client times include network round trips."]
    return out


def run(svc, raw, ids, where: str, rows: int = 100_000, skip_build: bool = True, skip_b: bool = False) -> str:
    """Build the markdown report. Callable from the CLI, a job, or the app (in-region)."""
    report = ["# Loom on Databricks: Lakebase Search benchmark", "",
              f"Run {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())} from **{where}**. "
              f"Latency is measured at the client and includes network round trips.", "",
              "## A. Demo corpus, 30 labeled questions (answer recall@10, filtered to caller's spaces)", ""]
    report += part_a(svc, raw, ids)
    if not skip_b:
        report += ["", "## B. Pooled index, selective and access filters (ANN recall@10 vs exact kNN)", ""]
        report += part_b(raw, rows, skip_build)
    return "\n".join(report) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--skip-build", action="store_true")
    ap.add_argument("--skip-b", action="store_true")
    ap.add_argument("--out", default=str(ROOT / "bench" / "results.md"))
    args = ap.parse_args()
    w = WorkspaceClient(profile=CONFIG.profile) if not _in_databricks() else WorkspaceClient()
    svc, _, raw = build(w, index_now=False)
    from scripts.seed import identities
    where = "Databricks job (in-region)" if _in_databricks() else f"client {socket.gethostname()} ({platform.system()})"
    text = run(svc, raw, identities(w), where, args.rows, args.skip_build, args.skip_b)
    Path(args.out).write_text(text)
    print(text)
    return 0


def _in_databricks() -> bool:
    import os
    return "DATABRICKS_RUNTIME_VERSION" in os.environ or "DB_IS_DRIVER" in os.environ


if __name__ == "__main__":
    rc = main()
    if rc:   # serverless job runners treat any SystemExit, even 0, as a failure
        sys.exit(rc)
