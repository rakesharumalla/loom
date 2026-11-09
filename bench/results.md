# Loom on Databricks: Lakebase Search benchmark

Run 2026-10-09 13:20 UTC from **Databricks App (in-region, us-east-1)**. Latency is measured at the client and includes network round trips.

## A. Demo corpus, 30 labeled questions (answer recall@10, filtered to caller's spaces)

| mode | recall@10 | p50 ms | p95 ms |
|---|---|---|---|
| vector | 1.00 (30/30) | 18 | 27 |
| bm25 | 0.93 (28/30) | 18 | 29 |
| hybrid | 1.00 (30/30) | 27 | 40 |

Query embedding (databricks-gte-large-en): p50 172 ms, p95 1911 ms (n=30). SQL latency above excludes embedding (vectors precomputed).

## B. Pooled index, selective and access filters (ANN recall@10 vs exact kNN)

Rows: 100,000 (kb1=50,198, kb2=29,917, kb3=14,941, kb4=3,930, kb5=917, kb6=97). (pre-built table) Index info: `{"default_epsilon":1.899999976158142,"default_probes":[25],"lists":[625],"version":"_2"}`

| filter | prefilter | probes | plan | recall@10 | server p50 ms | server p95 ms | client p50 ms | exact server p50 ms |
|---|---|---|---|---|---|---|---|---|
| no filter | off | auto (25) | Index Scan:vec_ann | 0.957 | 0.61 | 1.52 | 5 | 902.3 |
| no filter | off | 5 | Index Scan:vec_ann | 0.957 | 0.38 | 0.48 | 4 | 902.3 |
| no filter | off | 100 | Index Scan:vec_ann | 0.957 | 2.79 | 3.96 | 6 | 902.3 |
| no filter | on | auto (25) | Index Scan:vec_ann | 0.957 | 0.65 | 0.77 | 4 | 902.3 |
| no filter | on | 5 | Index Scan:vec_ann | 0.957 | 0.38 | 0.46 | 3 | 902.3 |
| no filter | on | 100 | Index Scan:vec_ann | 0.957 | 1.77 | 4.16 | 6 | 902.3 |
| kb = kb3 (~15%) | off | auto (25) | Index Scan:vec_ann | 0.980 | 0.74 | 1.48 | 6 | 271.1 |
| kb = kb3 (~15%) | off | 5 | Index Scan:vec_ann | 0.980 | 0.46 | 0.50 | 4 | 271.1 |
| kb = kb3 (~15%) | off | 100 | Index Scan:vec_ann | 0.980 | 1.83 | 5.36 | 5 | 271.1 |
| kb = kb3 (~15%) | on | auto (25) | Index Scan:vec_ann | 0.980 | 0.61 | 1.20 | 4 | 271.1 |
| kb = kb3 (~15%) | on | 5 | Index Scan:vec_ann | 0.980 | 0.35 | 0.43 | 4 | 271.1 |
| kb = kb3 (~15%) | on | 100 | Index Scan:vec_ann | 0.980 | 1.73 | 4.28 | 5 | 271.1 |
| kb = kb5 (~0.9%) | off | auto (25) | Index Scan:vec_ann | 0.997 | 5.02 | 9.90 | 16 | 18.7 |
| kb = kb5 (~0.9%) | off | 5 | Index Scan:vec_ann | 0.673 | 2.88 | 6.10 | 12 | 18.7 |
| kb = kb5 (~0.9%) | off | 100 | Index Scan:vec_ann | 1.000 | 8.59 | 15.77 | 21 | 18.7 |
| kb = kb5 (~0.9%) | on | auto (25) | Index Scan:vec_ann | 0.997 | 1.32 | 2.62 | 6 | 18.7 |
| kb = kb5 (~0.9%) | on | 5 | Index Scan:vec_ann | 0.673 | 0.79 | 1.94 | 4 | 18.7 |
| kb = kb5 (~0.9%) | on | 100 | Index Scan:vec_ann | 1.000 | 3.88 | 5.15 | 7 | 18.7 |
| kb = kb6 (~0.1%) | off | auto (25) | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.46 | 0.53 | 4 | 10.3 |
| kb = kb6 (~0.1%) | off | 5 | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.44 | 0.47 | 3 | 10.3 |
| kb = kb6 (~0.1%) | off | 100 | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.43 | 0.47 | 3 | 10.3 |
| kb = kb6 (~0.1%) | on | auto (25) | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.44 | 0.90 | 4 | 10.3 |
| kb = kb6 (~0.1%) | on | 5 | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.48 | 0.59 | 4 | 10.3 |
| kb = kb6 (~0.1%) | on | 100 | Sort > Bitmap Index Scan:vec_kb | 1.000 | 0.45 | 0.56 | 4 | 10.3 |
| access filter (~10% rows) | off | auto (25) | Index Scan:vec_ann | 1.000 | 0.83 | 2.54 | 6 | 120.7 |
| access filter (~10% rows) | off | 5 | Index Scan:vec_ann | 1.000 | 0.50 | 1.14 | 4 | 120.7 |
| access filter (~10% rows) | off | 100 | Index Scan:vec_ann | 1.000 | 3.09 | 4.99 | 6 | 120.7 |
| access filter (~10% rows) | on | auto (25) | Index Scan:vec_ann | 1.000 | 0.61 | 1.54 | 4 | 120.7 |
| access filter (~10% rows) | on | 5 | Index Scan:vec_ann | 1.000 | 0.37 | 0.83 | 3 | 120.7 |
| access filter (~10% rows) | on | 100 | Index Scan:vec_ann | 1.000 | 1.86 | 4.39 | 6 | 120.7 |
| kb5 + access (~0.09%) | off | auto (25) | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.06 | 2.08 | 4 | 32.6 |
| kb5 + access (~0.09%) | off | 5 | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.07 | 2.37 | 4 | 32.6 |
| kb5 + access (~0.09%) | off | 100 | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.09 | 2.19 | 5 | 32.6 |
| kb5 + access (~0.09%) | on | auto (25) | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.06 | 2.16 | 4 | 32.6 |
| kb5 + access (~0.09%) | on | 5 | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.08 | 2.21 | 4 | 32.6 |
| kb5 + access (~0.09%) | on | 100 | Sort > Bitmap Index Scan:vec_kb > Bitmap Index Scan:vec_access | 1.000 | 1.06 | 2.51 | 4 | 32.6 |

Notes: vectors are noisy copies of 960 real chunk embeddings, so neighbors cluster tightly; recall on production embeddings may be lower and should be re-measured on customer data. Server times are EXPLAIN ANALYZE execution times; client times include network round trips.
