# Discovery: Loom on Databricks (storage-layer demo)

Date: 2026-10-08. Workspace probed read-only; no cloud resources were created.

## Workspace

| Item | Value |
|---|---|
| CLI profile | `DEFAULT` (valid, OAuth via `databricks-cli`) |
| Host | `https://<workspace>.cloud.databricks.com` (workspace id `<workspace-id>`) |
| Cloud / region | AWS / `us-east-1` (from metastore summary) |
| Caller | `<you>@example.com`, workspace `admins` |
| Metastore default storage root | **none**. New catalogs need an explicit `MANAGED LOCATION`; external location `<external-location>` (`s3://<bucket>/`) is available |
| SQL warehouse | `Serverless Starter Warehouse` (`<warehouse-id>`, stopped) |
| Local tooling | Databricks CLI 1.17.0, `uv`, Python 3.14 (3.12 venv used for SDK probes), `psql` 14.18 |

## Availability

| Capability | Status in this workspace | How verified | Doc |
|---|---|---|---|
| Lakebase Autoscaling | **Available** (`databricks postgres` CLI) | `postgres list-projects` | https://docs.databricks.com/aws/en/oltp/projects/ |
| Lakebase Search (`lakebase_vector`, `lakebase_text`, `lakebase_tokenizer`) | **Documented**; enable is **UI-only** (Project → Settings → *Enable Lakebase Search*). No project-spec field or CLI flag found. | doc + `get-project` spec inspection | https://docs.databricks.com/aws/en/oltp/projects/lakebase-search |
| Managed agent memory | **Available (Beta)**; 9 stores already exist | `w.agent_kit.list_memory_stores()` | https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory |
| Managed agent sessions | **Available (Beta)**; 5 stores already exist | `w.agent_kit.list_session_stores()` | https://docs.databricks.com/aws/en/agents/agent-memory/managed-sessions |
| Databricks Apps | **Available** (14 apps) | `apps list` | https://docs.databricks.com/aws/en/dev-tools/databricks-apps/ |
| Lakeflow Jobs | Available (standard) | CLI | https://docs.databricks.com/aws/en/jobs/ |
| Hosted embedding endpoint | **Available**: `databricks-gte-large-en`, `databricks-bge-large-en`, `databricks-qwen3-embedding-0-6b` (READY) | `serving-endpoints list` | https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models |
| Hosted LLM endpoint | **Available**: e.g. `databricks-claude-sonnet-5-5`, `databricks-claude-haiku-5-5`, `databricks-gpt-5-4-mini` (READY) | `serving-endpoints list` | same as above |
| `ai_parse_document` | Documented (PDF, JPG/PNG, TIFF, DOC/DOCX, PPT/PPTX) | doc | https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document |
| MCP server library | `mcp` 2.2.0, `fastmcp` 4.0.10, `databricks-mcp` 0.9.2 on PyPI; Apps host MCP via Streamable HTTP on `DATABRICKS_APP_PORT` | `uv pip compile`, doc | https://docs.databricks.com/aws/en/agents/mcp-tools/custom-mcp |
| AI Search (optional) | Available (`w.ai_search`) | SDK 0.149.0 | https://docs.databricks.com/aws/en/ai-search/query-ai-search |
| Hosted reranker | **Only inside AI Search** (`databricks.ai_search.reranker.DatabricksReranker` on `index.similarity_search`). No standalone reranker found that can rerank Lakebase Search results. | doc | https://docs.databricks.com/aws/en/ai-search/query-ai-search |
| Lakebase → Delta | **Lakebase Change Data Feed** (Public Preview); CLI `databricks postgres create-cdf-config` | doc + CLI help | https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf |
| Groups `compliance`, `support`, `trading_ops` | **Do not exist** | `groups list` | — |

## Facts from the docs that shape the design

### Lakebase Search
- Enabling restarts all computes in the project, drops active connections, and **cannot be turned off**. It is UI-only. This requires a new project (`loom-demo`) and an explicit OK.
- Install: `CREATE EXTENSION IF NOT EXISTS lakebase_vector CASCADE;` (pulls in pgvector) and `CREATE EXTENSION IF NOT EXISTS lakebase_text;`.
- Index types: `lakebase_ann` (IVF + RaBitQ, pgvector-compatible types/operators such as `<=>`) and `lakebase_bm25` (on `tsvector`).
- BM25 query: `col <@> to_bm25query(to_tsvector('english', q), '<index_name>')`. **Lower score = more relevant.** Tunables `k1`, `b`, `default_limit` via `WITH (...)` / `ALTER INDEX ... SET`.
- **BM25 corpus statistics are computed at index build time, not incrementally** (doc comment in the Get started example). This affects freshness (step 8): new chunks are findable, but scores can drift until reindex. The demo will measure this and document a `REINDEX` policy.
- **ANN prefilter**: by default, non-vector `WHERE` conditions are applied **after** the ANN index returns candidates. `SET lakebase_ann.prefilter = on` evaluates them before full-precision reranking ("best when the filter is cheap and removes most rows"). Other knobs: `lakebase_ann.probes` (shape must match `lists` from `lakebase_ann_index_info(...)`) and `lakebase_ann.epsilon`. Small tables use exact (flat) search.
- **BM25 prefilter**: the docs describe Top-K pushdown (Block-Max WAND) but say nothing on how `WHERE` filters interact with it. **Open question.** The benchmark will measure it empirically.
- The hybrid RRF example in the docs (k=60, top-40 per list) is the template for `SearchPort`.

### Managed agent memory (Beta)
- SDK documented as `pip install databricks-agentbricks` (installed 0.4.0): `from databricks_agentkit import AgentKitClient`, `AgentKitClient(WorkspaceClient())`, `client.memory_stores.create(name)`, `store.add(actor_id=, path=, content=, description=, session_id=, source_type=, write_mode=)`, `store.search(actor_id=, query=, limit=, path_prefix=, session_id=)`, `store.grant_permission(principal_id, permission='WRITE')`. **Matches the prompt** (keyword-only arguments).
- The same API also exists in the plain SDK as `w.agent_kit.*` (`databricks-sdk` 0.149.0). The adapter will use the documented `AgentKitClient`.
- Search is "relevance-ranked, full-text (BM25) … top-N up to 100 entries. It does not support pagination or vector similarity search." **Confirms BM25-only.** (The page intro also says "semantic search"; the Limitations section is the precise statement and takes precedence.)
- "Access control is enforced at the store level. Per-entry and per-actor access control are not [supported]" and "create a separate memory store per boundary". **Confirms store-per-space.**
- `actor_id` docs: set it from a trusted value, "rather than a model- or user-supplied value". **Confirms that `actor_id` is not a security boundary.**
- Additional capability not in the plan: `w.agent_kit.extract_memories(session_store, session_id, memory_store, dry_run, instructions)`, a managed session-to-memory extraction (entries tagged `source_type=DREAMER`). See Deviations below.
- Billing note: during the preview, you pay for the underlying Lakebase instance.
- Store limits: **not stated in the docs**. Open question.

### Managed agent sessions (Beta)
- `client.session_stores.create(name)`, `session_store.add(actor_id=, session_id=)`, `session.append_items([...])`; SDK: `list_sessions(parent, filter=...)`, `list_session_items(parent)`, `fork_session`, `pop_session_item`.
- Sessions are **exportable via list APIs**, so `distill_job.py` can read transcripts from managed sessions directly. No synthetic transcripts table is needed (the job will seed transcripts *into* a session store).
- Permissions are store-level (`session_store.grant_permission(principal_id)`). Deleting sessions does not delete memory.

### Lakebase limits (https://docs.databricks.com/aws/en/oltp/projects/manage-projects#limits)
- 500 branches per project (the Lakebase skill bundled with Claude Code says "10 unarchived". **The doc wins**: 500 branches; 20 concurrently active computes), 500 databases per branch, 500 roles per branch, 6 read replicas per branch, 1000 projects per workspace.
- Branch TTL max 30 days or `no_expiry`.
- **Scale-to-zero timeout: 60 s minimum, 7 days maximum, default 24 h** on this doc page (https://docs.databricks.com/aws/en/oltp/projects/scale-to-zero). The existing project shows `suspend_timeout_duration = 86400s`.

### Identity, roles, and RLS on Lakebase
- `CREATE EXTENSION databricks_auth; SELECT databricks_create_role('<id>', 'USER'|'SERVICE_PRINCIPAL'|'GROUP');` creates OAuth roles for **users, service principals, and groups** (https://docs.databricks.com/aws/en/oltp/projects/postgres-roles).
- RLS: no Lakebase-specific page. Standard Postgres `ENABLE ROW LEVEL SECURITY` / `CREATE POLICY` applies. Group-role membership is how `access_groups` maps to `pg_has_role`/`current_user` checks. To be verified on the new project.

### Unity Catalog volumes
- Read files: `USE CATALOG` + `USE SCHEMA` + `READ VOLUME`; write files: `+ WRITE VOLUME` (https://docs.databricks.com/aws/en/volumes/privileges). There is no DENY in the privilege model; access is shown only through grants.

### Lakebase Change Data Feed (Public Preview)
- Tables need `ALTER TABLE ... REPLICA IDENTITY FULL`. Output lands in `lb_<table>_history` Delta tables. The **target catalog's managed storage must be an external location**, which is satisfied here by creating `loom_demo` on the existing external location. Status: `SELECT * FROM wal2delta.tables;`.

## Deviations from the plan (doc wins)

1. **Lakebase Search enable is a manual UI click.** Setup scripts will create the project, pause, ask you to click *Enable Lakebase Search*, then verify `pg_available_extensions` and continue.
2. **Reranker**: no standalone Databricks-hosted reranker applies to Lakebase Search. The facade omits reranking and says so. An LLM-as-reranker step is possible, but it would be Loom code, not a Databricks reranker.
3. **Distillation**: the plan has our own LLM job. Databricks also offers managed `extract_memories` (session → memory). Proposal: `distill_job.py` stays the primary path (Loom-owned guardrails, conflict, promotion, markdown lessons), and `extract_memories(dry_run=True)` is shown as an optional comparison only, with results passed through the same Loom guardrails before any write. Managed extraction writes directly into a store, which would bypass Loom policy, so the demo never runs it with `dry_run=False` against shared stores.
4. **Sessions are exportable**, so distillation reads managed sessions. There is no synthetic transcripts table.
5. **Branch limit** is 500 per project (not 10).
6. **BM25 statistics are build-time.** Freshness numbers will be reported for "searchable" and, separately, "BM25 stats refreshed".

### Decisions and findings during setup (2026-10-08)

7. **Catalog/schema**: you chose the existing `loom_catalog`. `loom_catalog.kb` already exists (another demo), so the demo uses the new schema `loom_catalog.loom` with volume `raw`. The catalog's storage root is on an external location, which Lakebase CDF requires.
8. **Groups are workspace-local**: groups created through the workspace SCIM API are `WorkspaceGroup`s, and UC accepts only account-level principals (`PRINCIPAL_DOES_NOT_EXIST`). An unrelated *account* group named `compliance` exists, so a `GRANT USE CATALOG ... TO compliance` succeeded by accident. It was revoked immediately and verified absent. Per your decision, the groups are now `loom-compliance`, `loom-support` and `loom-trading-ops` (workspace-local, used for Loom policy and Postgres roles), and **UC volume grants go to each persona service principal**. The README says plainly that group-level UC grants were not possible in this workspace.
9. **Postgres 18**: project `loom-demo` runs PostgreSQL 18.6.
10. **Lakebase Search detection**: `lakebase_vector` is listed in `pg_available_extensions` *before* Search is enabled, but `CREATE EXTENSION` fails with "must be loaded via shared_preload_libraries". Setup therefore checks `SHOW shared_preload_libraries`.
11. **Managed memory paths must be absolute** (`managed_memory_entry.path must be absolute ...`). Fact entries use `/facts/{subject}/{origin}-{observed_ts}`.
12. **Managed memory `create_time` is server-set**, so the observed time of a fact (needed for time precedence) is encoded in the entry path, not taken from `create_time`.
13. **AgentKit SDK error wrapping**: `databricks-agentbricks` wraps SDK `NotFound` in `AgentCliError`. Adapters check the cause chain.
14. **Live adapter checks (memory/sessions)**: entries added for `actor_id=bob` were not returned by a search as `alice`, and vice versa, inside one store. This is the partitioning behavior the docs describe; it is still not a security boundary. A session written by one client was read back by a fresh client. `list_sessions(filter='actor_id = "bob"')` works.
15. **MCP library**: `mcp` 2.2 renamed FastMCP to `MCPServer` (`mcp.server.mcpserver`). Tools read the bearer token from `Context.headers`, and Loom verifies it with Databricks.

## Open questions (need your decision)

1. **Personas / identities.** Alice, Bob, and Carol cannot be real workspace users without real mailboxes. Options:
   - (A, recommended) Create **service principals** `loom-alice`, `loom-bob`, `loom-carol`, `loom-admin`, `loom-service`, plus workspace groups `compliance`, `support`, `trading_ops`. Each persona is then a real Databricks identity: the facade verifies a real OAuth token, UC grants and `databricks_create_role(..., 'GROUP')` are genuine, and memory-store `grant_permission` targets real principals. Team/project membership (Risk/Care/Markets, Atlas/Borealis) lives in Loom `space_members` (implemented in Loom).
   - (B) Simulated identities: the facade issues signed demo tokens. Simpler, but UC/RLS defense in depth would be shown with groups that have no real members.
2. **Catalog.** Create a new catalog `loom_demo` with `MANAGED LOCATION 's3://<bucket>/loom_demo'` (required for Lakebase CDF), schema `kb`, volume `raw`?
3. **Endpoints.** Embedding `databricks-gte-large-en` (1024-dim, to be confirmed by a test call) and LLM `databricks-claude-sonnet-5-5` for distillation?
4. **Lakebase project.** Create a NEW project `loom-demo` (PG 17, autoscale 0.5–2 CU), then you click *Enable Lakebase Search* in the UI (one-way, restarts computes)?
5. BM25 + `WHERE` filter interaction and memory-store count limits are undocumented. OK to measure and report them empirically rather than claim them?
16. **RLS and BYPASSRLS on Lakebase**: the project-owner role, `databricks_superuser` and `cloud_admin` have `BYPASSRLS`, so `FORCE ROW LEVEL SECURITY` alone does not constrain the facade's login role. Chunk reads and writes run under NOLOGIN roles `loom_reader` / `loom_writer` (no BYPASSRLS), entered per transaction with `SET LOCAL ROLE`. Verified live: a query with no access filter, run as `loom_reader` with Bob's principals, sees only company/atlas/care/personal rows, and with no caller set it sees 0 rows.
17. **Lakebase Search syntax verified**: `lakebase_ann` opclass `vector_cosine_ops`; `to_bm25query(..., 'loom.chunks_tsv_bm25')` needs the schema-qualified index name; BM25 scores are negative (lower = better) and non-matching rows are not returned. At 960 rows `lakebase_ann_index_info` reports `"lists": []` (exact flat search), as the docs describe for small datasets.
18. **Client-side latency from a laptop** is dominated by network round trips to us-east-1 (about 100–120 ms per `SELECT 1` transaction, measured). Reported latency figures come from an in-region Databricks job; laptop figures are labeled client-side.
19. **LLM parameters**: `databricks-claude-sonnet-5-5` rejects the `temperature` parameter (`BAD_REQUEST`). The distill job omits it.
20. **Managed `extract_memories`** (`w.agent_kit.extract_memories(..., dry_run=True)`) returned `ai-gateway upstream error status 403` in this workspace. The comparison is reported as unavailable, and distillation uses the Loom pipeline only.
21. **Databricks Apps proxy**: the platform proxy strips the caller's `Authorization` header and forwards `X-Forwarded-Access-Token` (plus `X-Forwarded-Email` and similar). The facade verifies the forwarded token with SCIM `/Me`. Verified live: Bob's SP token resolves to `bob` / `loom-support`.
22. **Bundle deploy resets app resources**: an app resource without a `resources:` list in the bundle replaced the resources attached with `apps create-update` (Lakebase lost; `password authentication failed` for the app SP). The bundle now declares the `postgres` and `serving_endpoint` resources.
23. **Serverless `spark_python_task`**: entry scripts run without `__file__`, and any `SystemExit` (even 0) marks the task failed. Entry points resolve the repo root from argv/cwd and exit only on non-zero codes. One `loom-bench` run aborted with SIGABRT inside the serverless Python process (not diagnosed). In-region benchmark numbers are taken from the app (`/ui/run/bench`) instead.
24. **psycopg auto-prepare vs. lakebase_ann**: psycopg prepares a statement after 5 executions, and Postgres may then use a generic plan. For a very selective filter (about 0.1% of rows) the custom plan uses the B-tree path with an exact sort (recall@10 1.000), while the generic plan used the ANN index with a post-filter (recall@10 0.613 over 30 queries; the first 5 were 1.0). The connector sets `prepare_threshold=None`. Recommendation for Loom: use custom plans (or raise `lakebase_ann.probes`) for highly selective KB filters, and re-measure on production embeddings.
