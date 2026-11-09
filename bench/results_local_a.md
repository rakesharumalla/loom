# Loom on Databricks: Lakebase Search benchmark

Run 2026-10-09 02:41 UTC from **client FM70JG36PP (Darwin)**. Latency is measured at the client and includes network round trips.

## A. Demo corpus, 30 labeled questions (answer recall@10, filtered to caller's spaces)

| mode | recall@10 | p50 ms | p95 ms |
|---|---|---|---|
| vector | 1.00 (30/30) | 263 | 350 |
| bm25 | 1.00 (30/30) | 245 | 304 |
| hybrid | 1.00 (30/30) | 283 | 455 |

Query embedding (databricks-gte-large-en): p50 316 ms, p95 725 ms (n=30). SQL latency above excludes embedding (vectors precomputed).
