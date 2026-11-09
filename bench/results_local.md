# Loom on Databricks: Lakebase Search benchmark

Run 2026-10-09 13:11 UTC from **client FM70JG36PP (Darwin)**. Latency is measured at the client and includes network round trips.

## A. Demo corpus, 30 labeled questions (answer recall@10, filtered to caller's spaces)

| mode | recall@10 | p50 ms | p95 ms |
|---|---|---|---|
| vector | 1.00 (30/30) | 252 | 307 |
| bm25 | 0.93 (28/30) | 250 | 296 |
| hybrid | 1.00 (30/30) | 302 | 367 |

Query embedding (databricks-gte-large-en): p50 177 ms, p95 316 ms (n=30). SQL latency above excludes embedding (vectors precomputed).

## B. Pooled index, selective and access filters (ANN recall@10 vs exact kNN)

Rows: 100,000 (kb1=50,198, kb2=29,917, kb3=14,941, kb4=3,930, kb5=917, kb6=97). (pre-built table) Index info: `{"default_epsilon":1.899999976158142,"default_probes":[25],"lists":[625],"version":"_2"}`

| filter | prefilter | probes | plan | recall@10 | server p50 ms | server p95 ms | client p50 ms | exact server p50 ms |
|---|---|---|---|---|---|---|---|---|
| no filter | off | auto (25) | Index Scan:vec_ann | 0.957 | 0.75 | 2.25 | 46 | 1130.1 |
| no filter | off | 5 | Index Scan:vec_ann | 0.957 | 0.44 | 1.30 | 43 | 1130.1 |
| no filter | off | 100 | Index Scan:vec_ann | 0.957 | 4.95 | 8.99 | 50 | 1130.1 |
| no filter | on | auto (25) | Index Scan:vec_ann | 0.957 | 0.74 | 0.87 | 47 | 1130.1 |
| no filter | on | 5 | Index Scan:vec_ann | 0.957 | 0.46 | 0.53 | 44 | 1130.1 |
| no filter | on | 100 | Index Scan:vec_ann | 0.957 | 3.35 | 5.40 | 46 | 1130.1 |
| kb = kb3 (~15%) | off | auto (25) | Index Scan:vec_ann | 0.980 | 0.92 | 2.14 | 43 | 268.4 |
| kb = kb3 (~15%) | off | 5 | Index Scan:vec_ann | 0.980 | 0.57 | 1.28 | 45 | 268.4 |
| kb = kb3 (~15%) | off | 100 | Index Scan:vec_ann | 0.980 | 2.26 | 5.62 | 46 | 268.4 |
| kb = kb3 (~15%) | on | auto (25) | Index Scan:vec_ann | 0.980 | 0.80 | 1.78 | 42 | 268.4 |
| kb = kb3 (~15%) | on | 5 | Index Scan:vec_ann | 0.980 | 0.47 | 0.56 | 42 | 268.4 |
| kb = kb3 (~15%) | on | 100 | Index Scan:vec_ann | 0.980 | 2.81 | 4.75 | 43 | 268.4 |
| kb = kb5 (~0.9%) | off | auto (25) | Index Scan:vec_ann | 0.997 | 6.53 | 11.48 | 56 | 19.8 |
| kb = kb5 (~0.9%) | off | 5 | Index Scan:vec_ann | 0.673 | 4.05 | 9.41 | 54 | 19.8 |
| kb = kb5 (~0.9%) | off | 100 | Index Scan:vec_ann | 1.000 | 9.28 | 16.96 | 61 | 19.8 |
| kb = kb5 (~0.9%) | on | auto (25) | Index Scan:vec_ann | 0.997 | 1.93 | 4.44 | 46 | 19.8 |
| kb = kb5 (~0.9%) | on | 5 | Index Scan:vec_ann | 0.673 | 1.26 | 3.29 | 43 | 19.8 |
| kb = kb5 (~0.9%) | on | 100 | Index Scan:vec_ann | 1.000 | 4.30 | 7.51 | 49 | 19.8 |
| kb = kb6 (~0.1%) | off | auto (25) | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.70 | 1.88 | 43 | 10.8 |
| kb = kb6 (~0.1%) | off | 5 | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.69 | 1.55 | 41 | 10.8 |
| kb = kb6 (~0.1%) | off | 100 | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.69 | 1.48 | 42 | 10.8 |
| kb = kb6 (~0.1%) | on | auto (25) | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.70 | 1.62 | 42 | 10.8 |
| kb = kb6 (~0.1%) | on | 5 | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.68 | 0.72 | 42 | 10.8 |
| kb = kb6 (~0.1%) | on | 100 | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.69 | 1.59 | 43 | 10.8 |
| access filter (~10% rows) | off | auto (25) | Index Scan:vec_ann | 1.000 | 1.02 | 2.81 | 48 | 122.8 |
| access filter (~10% rows) | off | 5 | Index Scan:vec_ann | 1.000 | 0.63 | 1.49 | 43 | 122.8 |
| access filter (~10% rows) | off | 100 | Index Scan:vec_ann | 1.000 | 2.81 | 5.09 | 46 | 122.8 |
| access filter (~10% rows) | on | auto (25) | Index Scan:vec_ann | 1.000 | 0.85 | 1.70 | 44 | 122.8 |
| access filter (~10% rows) | on | 5 | Index Scan:vec_ann | 1.000 | 0.49 | 0.81 | 43 | 122.8 |
| access filter (~10% rows) | on | 100 | Index Scan:vec_ann | 1.000 | 2.88 | 4.38 | 47 | 122.8 |
| kb5 + access (~0.09%) | off | auto (25) | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.29 | 3.01 | 42 | 33.2 |
| kb5 + access (~0.09%) | off | 5 | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.28 | 2.87 | 43 | 33.2 |
| kb5 + access (~0.09%) | off | 100 | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.30 | 2.79 | 44 | 33.2 |
| kb5 + access (~0.09%) | on | auto (25) | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.30 | 2.44 | 43 | 33.2 |
| kb5 + access (~0.09%) | on | 5 | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.29 | 2.88 | 43 | 33.2 |
| kb5 + access (~0.09%) | on | 100 | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.30 | 2.88 | 42 | 33.2 |

Notes: vectors are noisy copies of 960 real chunk embeddings, so neighbors cluster tightly; recall on production embeddings may be lower and should be re-measured on customer data. Server times are EXPLAIN ANALYZE execution times; client times include network round trips.
