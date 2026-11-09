# Demo script (about 30 minutes)

App: `https://loom-demo-<workspace-id>.aws.databricksapps.com` (Consumer and Admin tabs).
Before the session: `uv run python scripts/preflight.py` (expect 12/12 PASS) and
`uv run python scripts/cleanup_test_data.py`. Keep a terminal open in the repo.
Numbers to quote come only from `docs/measurements.md` and `bench/results.md`.


## Recommended flow: the Guided demo tab (about 15 minutes)

The app opens on **Guided demo**: 7 steps, one button each. The layer strip at the top of each step lights up the
layers involved (Consumer → Loom control plane → Storage ports → Databricks) and names the Databricks components.
Use **Next →** to advance; switch to the **Explore** tabs (Ask, Memory, Contribute, Admin, Architecture) only when
someone asks "show me more".

| Step | Button | What the customer sees |
|---|---|---|
| 1 Who is asking | (auto) | Alice, Bob, Carol and the spaces each can read |
| 2 Same question, different answers | Ask both | Side by side: included / excluded spaces and sources per person |
| 3 When facts disagree | Ask Bob | Official $50,000 policy beats Bob's newer $25,000 note; rule and rationale |
| 4 Memory follows the person | Save, recall, check Alice | Saved in session A, recalled in new session B, invisible to Alice |
| 5 Fresh knowledge in seconds | Edit + delete, then search | Measured edit → searchable seconds, files re-embedded, deleted file removed |
| 6 Agents learn from incidents | Ask as a new agent session | Distilled lesson with resolution steps and its 5 source sessions |
| 7 One contract, every consumer | Run on all three | REST, MCP and SDK return the identical composed answer |

Before step 6, a lesson must be published (the seed run did this; otherwise Explore → Admin → Pipelines → Distill,
then approve in Promotions). Close with the **Architecture** tab.

The detailed 30-minute version follows.

## 1. Same contract, new storage (3 min)
- **Say**: "Loom API stays. Agents, MCP clients and AI Hub keep calling the same three verbs. We replaced only what is
  behind the ports."
- **Show**: `Loom/ports.py` header table (MongoDB/S3/Bedrock → Lakebase/Volume/Lakebase Search/memory/sessions).
  Admin → *Storage-port view* after any query: which port method hit which backend, with timings.
- **Run**: `uv run pytest -q` (unit + contract parity: identical results and audit across REST/MCP/SDK).
- **Run**: `LOOM_LIVE=1 uv run pytest -q tests/live/test_storage_live.py` → rollback proof: a 120 KB file is written
  to the Volume, an injected failure aborts the metadata transaction, and zero `files`/`file_versions` rows remain.
- **Expected**: 4 passed. **Recovery**: if Lakebase is cold, rerun (first connection wakes the compute).

## 2. Spaces as first-class units (3 min)
- **Say**: "A space owns identity, permissions, guardrails and audit. Databricks provides the storage blocks; the
  space model is Loom code."
- **Click**: Admin → *Spaces & policies*: 10 spaces, members, guardrails, bindings (KB id, memory store).
- **Click**: edit the JSON (project `demo`) → `configure` as **admin** → created, with a new KB and memory store.
  Repeat as **carol** → 403 "configure denied".
- **Recovery**: if the space exists already, the response says "updated", which is fine.

## 3. Compose at read time (5 min)
- **Click**: Consumer → persona **alice** → "escalate potential sanctions hits" → Retrieve. Then **bob**, same query.
- **Expected**: Alice includes `team:risk` (her AML triage doc, account numbers redacted by the space guardrail);
  Bob includes `team:care` and shows `team:risk` *excluded: deny by default*. The trace lists every space considered
  and why. Other users' personal spaces never appear.
- **Say**: "Storage enforces too: chunk rows carry principal tags and Postgres RLS applies under a NOLOGIN role, and the
  Volume is guarded by UC grants. That is defense in depth, not the decision-maker."
- **Show**: `LOOM_LIVE=1 uv run pytest -q tests/live/test_retrieval_live.py::test_rls_defense_in_depth`.
- **Show audit**: Admin → *Audit tail* (rows per space decision, with surface `web`).

## 4. Memory containers (4 min)
- **Click**: Consumer → bob → *Save in session A* → *Recall in NEW session B* → the preference comes back from
  `personal:bob` (managed memory, BM25). Switch to **alice** and recall → nothing (store-level isolation by Loom).
- **Click**: Contribute (bob, fact, propose promotion to `project:atlas`) → `promotion_pending`.
  Admin → *Promotions* → approve as **bob** → refused (proposer). Approve as **alice** → approved. Retrieve as alice →
  the fact now comes from `project:atlas`.
- **Say**: "Managed memory search is BM25 only, with per-store ACLs, so each space gets its own store and Loom
  decides which stores a caller may query. Semantic recall comes from Lakebase Search over KB lessons."

## 5. Conflict resolution (3 min)
- **Click**: bob → "What is the daily withdrawal limit for retail customers?"
- **Expected**: conflict `withdrawal_limit.retail.daily` → resolved by **source_authority** (official policy $50,000
  beats Bob's newer personal note of $25,000), flagged `lower_authority_newer`; the note is marked *superseded*.
- **Click**: bob → "card refunds post business days" → **time_precedence** inside `team:care` (3 days beats 5).
- **Click**: alice → "withdrawals v2 batch window" → **needs_human_review** (tie: two Atlas docs, same authority and date).
- Admin → *Conflict resolver log* shows each decision and its rationale.

## 6. Parity across consumers (3 min)
- **Run**: `uv run python scripts/demo_runner.py --persona bob` → the same retrieve via the web UI route, an MCP
  agent (`/mcp/`), and the Python SDK, each with bob's own OAuth token.
- **Expected**: `identical composed answer across web / MCP / SDK: True`.
- **Run**: `uv run python scripts/service_client_demo.py` (batch `service` identity; a write to `company` is rejected).

## 7. Pooled hybrid search and freshness (4 min)
- **Show**: `bench/results.md`. Part B: with a ~0.9% KB filter, recall@10 drops at low `probes` and recovers at the
  default; very selective filters switch the planner to an exact B-tree + sort plan. Quote only the measured table.
- **Click**: Admin → *Edit one file + delete one, ingest, poll*.
- **Expected**: one file re-embedded (1 chunk), unchanged file skipped, deleted file's chunks removed, plus a
  change-to-searchable time.
- **Recovery**: if the job shows `error`, run `uv run python scripts/freshness_demo.py` from the terminal.

## 8. Semi-offline distillation (4 min)
- **Say**: "Five agent sessions hit the same incident. A batch job reads them from managed sessions and writes a
  markdown lesson, but only into a staging space; a human promotes it."
- **Click**: Admin → *Distill transcripts → lessons* (about 25 s) → Promotions → approve the lesson as the listed
  approver (company → **alice**, team:care → **bob**).
- **Click**: Consumer → bob → "ETH withdrawal queue stuck after a node upgrade, signer timing out" → the lesson is the
  top result, `authority: inferred`, with its five source sessions, so the agent can resolve it without the transcripts.
- **Recovery**: `uv run python scripts/distill_demo.py --approve-pending`.

## 9. If time allows
- Ephemeral space on a Lakebase branch with scale-to-zero: `uv run python scripts/ephemeral_demo.py` (~6 min; prints
  create/load/query/teardown and cold vs warm timings).
- Audit/feedback to Delta via Lakebase CDF: `uv run python scripts/analytics_report.py` (resolution outcomes by rule,
  space usage by scope, excluded reasons, low-rated sources, calls by surface). MLflow eval: the `loom-eval` job.

## General recovery
- Workspace IP ACL can block a changing laptop egress IP (seen once in prep); switch network/VPN and retry.
- App not responding: `databricks bundle run loom_app`; grants: `uv run python scripts/app_grants.py`.
- Stale demo data: `uv run python scripts/cleanup_test_data.py`; re-seed is idempotent: `uv run python scripts/seed.py`.
