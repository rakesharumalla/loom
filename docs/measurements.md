# Measurements (all measured in this workspace; nothing quoted from elsewhere)

Workspace `DEFAULT` (AWS us-east-1), Lakebase project `loom-demo` (PG 18.6, autoscale 0.5–2 CU).
Unless marked "in-region", timings were taken from a laptop client, where each Lakebase round trip
(`SELECT 1` transaction) costs about 100–120 ms of network. Treat client-side numbers as upper bounds.

| What | Result | Where measured | Script |
|---|---|---|---|
| Single-round-trip hybrid search SQL (4 spaces, k=5) | ~190–250 ms client-side (first call ~1 s, pool warm-up) | laptop | inline check |
| Server-side ANN execution, 100k-row pooled table | 0.5–5 ms typical (EXPLAIN ANALYZE) | server | `bench/benchmark.py` part B |
| Answer recall@10, 30 labeled questions (vector / bm25 / hybrid) | 1.00 / 1.00 / 1.00 (templated synthetic corpus) | laptop | `bench/benchmark.py` part A |
| ANN recall@10 vs exact, no filter (100k rows, IVF lists=625, probes 5 / 25 / 100) | 0.957 / 0.957 / 0.957 | server | part B (after fix below) |
| ANN recall@10 vs exact, ~15% KB filter | 0.980 at all probes | server | part B |
| ANN recall@10 vs exact, ~0.9% KB filter, probes 5 / 25 / 100 | 0.673 / 0.997 / 1.000 | server | part B |
| ANN recall@10, ~0.1% KB filter and ~0.09% kb+access, custom plans | 1.000 (planner uses B-tree + exact sort) | server | part B |
| Same ~0.1% filter with psycopg auto-prepare (generic plan after 5 runs) | 0.613 (first 5 queries 1.0, then drops) | server | ad-hoc check; fixed by `prepare_threshold=None` |
| Freshness: source edit → first retrieve hit | 7.95 s (incremental ingest job 4.61 s) | laptop | `scripts/freshness_demo.py` |
| Freshness inside the Databricks App: source edit → first retrieve hit | 3.54 s (ingest 2.32 s; 1 chunk re-embedded, unchanged file skipped) | **in-region (app)** | Admin → Freshness |
| Chunks re-embedded on that edit | 1 (edited file), 0 (unchanged), deleted file's chunk removed | laptop | same |
| BM25 REINDEX (refresh corpus statistics, ~960 chunks) | 0.35 s | laptop | same |
| Lakebase CDF: commit → visible in Delta | 12.5 s, 13.2 s, 16.3 s (2 s polling via SQL warehouse) | laptop | `scripts/cdf_latency.py` |
| Ephemeral space: branch create (copy-on-write, 964 chunks inherited) | 4.7–5.1 s | laptop | `scripts/ephemeral_demo.py` |
| Ephemeral space: configure space + memory store | 3.3–5.2 s | laptop | same |
| Ephemeral space: load + index 10 docs | 14.4–21.6 s | laptop | same |
| Ephemeral space: query | 0.9–1.5 s | laptop | same |
| Ephemeral space: teardown (memory store + branch) | 0.4 s + 0.4–0.6 s | laptop | same |
| Scale-to-zero (60 s timeout): cold first `SELECT 1`, new connection | 1,437 ms and 1,545 ms (state `IDLE` after 70 s) | laptop | same |
| Warm new-connection `SELECT 1` on the same compute | 298–316 ms | laptop | same |
| Distillation job (5 transcripts → 1 lesson + 5 facts) | 25.1 s end to end | laptop | `scripts/distill_demo.py` |

| MLflow eval, full composed `retrieve` (k=5): sample 10 / bench 30 | hit@5 10/10 (MRR 1.000) / 28/30 (MRR 0.933) | laptop | `pipelines/eval_job.py` |
| MLflow eval, composed `retrieve` (k=5), 2 runs | sample hit@5 1.0 / 1.0; bench hit@5 0.933 / 0.967; MRR 0.933 / 0.940 | **in-region (job)** | `loom-eval` job, MLflow runs fa6d47bd…, 686db053… |
| Composed `retrieve` latency over 40 eval questions, 2 runs | p50 276 / 331 ms, p95 1,728 / 1,962 ms | **in-region (job)** | same |
| Composed `retrieve` latency over the 40 eval questions | p50 998 ms, p95 1,769 ms | laptop | same |

In-region numbers (Databricks job) are appended to `bench/results.md` by the `loom-bench` job.

Correction: an earlier part-B table (before 2026-10-09 13:20 UTC) reused a cached exact plan for the ANN timings, because the exact baseline and the ANN query had identical SQL text. Its recall and client latency for the no-filter and access-filter rows were invalid and have been replaced.
