# Design decision: long-term memory on Lakebase, sessions stay managed (Option B)

- **Status:** Accepted, not yet implemented. The deployed app still uses managed agent memory.
- **Date:** 2026-10-09
- **Context doc:** [`memory-options.md`](memory-options.md)

## Decision

Move Loom's long-term memory (facts and preferences) from managed agent memory into Loom-owned Lakebase tables.
Keep **managed agent sessions** for short-term conversation state.

## Options considered

| Option | Simplification | Trade-off |
|---|---|---|
| **A. Move memory and sessions to Lakebase** | Largest: one backend, one query per retrieve, one transaction per write | Fully off the managed path Databricks recommends for new agents |
| **B. Move memory only, keep managed sessions** (chosen) | Most of A. Memory causes nearly all of the workarounds below; sessions cost one read and one append per request with a `session_id` | Still two backends, but only one on the retrieve hot path |
| **C. Keep managed memory and sessions** | None | Stays on the recommended path and shows both managed services |

The Databricks docs label [self-managed agent memory](https://docs.databricks.com/aws/en/agents/agent-memory/self-managed-memory)
**legacy** and point new agents to managed memory and sessions. That page's own guidance still covers this case:
use self-managed memory "when you need direct SQL access to memories (for example, to integrate memory with
existing data pipelines or to use a custom schema)". Loom needs both. It already runs its own control plane,
row-level security and hybrid search on Lakebase.

## Why: what managed memory costs Loom today

| Workaround forced by managed memory | Where |
|---|---|
| One memory store per space, because access control is per store and `actor_id` isn't a security boundary | `store_name_for`, `configure` bindings, `scripts/app_grants.py` grants per store, teardown and cleanup |
| A retrieve fans out to one REST call per readable store (4 for bob, about 85–270 ms each), alongside the KB SQL query | `Loom/control/composer.py` thread pool |
| Memory results are BM25-only, so they're merged by rank with the KB's hybrid results instead of in one ranking | `composer.py` cross-backend RRF |
| Fact subject and observed time are encoded in the entry path, because `create_time` is set by the server and there's no metadata to filter on | `fact_path` / `parse_fact_path` |
| The conflict check calls `list` once per store, plus a separate KB query | `LoomService._conflict_check` |
| Writes aren't atomic: the memory write goes over REST, then the audit and conflict rows go over SQL | `LoomService._contribute`, `_decide_promotion` |

Sessions don't cause these problems: they hold only the caller's own personal session, are read once per request
with a `session_id`, and are read in batch by distillation. Keeping them managed retains a managed Databricks
service where it fits well.

## Design

### Table

```sql
CREATE TABLE loom.memories (
  memory_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  space_id      text NOT NULL REFERENCES loom.spaces(space_id) ON DELETE CASCADE,
  subject       text NOT NULL,                 -- normalized, e.g. preference.summary_format
  statement     text NOT NULL,
  origin        text NOT NULL,                 -- stated | inferred | promoted
  observed_at   timestamptz NOT NULL,
  author        text NOT NULL,                 -- verified principal that wrote it
  session_id    text,
  description   text,
  embedding     vector(1024),
  content_tsv   tsvector,
  access_groups text[] NOT NULL,               -- same principal tags as chunks
  created_at    timestamptz NOT NULL DEFAULT now(),
  superseded_by uuid REFERENCES loom.memories(memory_id)
);
-- indexes: (space_id, subject); lakebase_ann on embedding; lakebase_bm25 on content_tsv
```

- **Row-level security:** the same policy shape as `chunks` (`access_groups && loom.caller_principals()`). Reads run
  under `SET LOCAL ROLE loom_reader`; writes under `loom_writer`.
- **Personal memory:** rows in `personal:<principal>` with `access_groups = {user:<principal>}`. Isolation comes from
  `space_id` filtering in policy plus RLS, not from a separate store.
- **Retagging on membership change:** `retag_space` updates `memories` together with `chunks`.

### Code changes

| Area | Change |
|---|---|
| `Loom/ports.py` | `MemoryPort` loses `ensure_store` / `delete_store`; `add` / `search` / `list` take `space_ids` and principals instead of a store and an `actor_id` |
| `Loom/adapters/databricks/memory.py` | Replaced by a Lakebase SQL adapter (or memory search folded into `LakebaseSearch`) |
| `Loom/control/composer.py` | Per-store fan-out removed. One hybrid query returns KB chunks and memory rows; the KB and memory lists still feed RRF so scores stay explainable in the trace |
| `Loom/service.py` | `_conflict_check` is one query by subject over KB facts and memories. `_contribute` and `_decide_promotion` write the memory row, conflict decision and audit rows in one transaction |
| `fact_path` / `parse_fact_path` | Deleted; subject, origin and observed time are columns |
| `configure` | The `memory` binding no longer creates a store; it marks the space as memory-bearing |
| `scripts/app_grants.py`, `teardown.py`, `cleanup_test_data.py` | Memory-store grants and deletes removed (session stores stay) |
| `Loom/seed.py`, `pipelines/distill_job.py` | Write facts through the new adapter; distillation still reads managed sessions |
| `sql/`, `scripts/setup.py` | New migration for `loom.memories`, its indexes, RLS policy and grants; add `memories` to `CDF_TABLES` (`REPLICA IDENTITY FULL`) so it streams to Delta |
| Tests | `tests/fakes.py` FakeMemory follows the new port; `tests/live/test_memory_live.py` rewritten for SQL |

### Migration

1. Create `loom.memories` with indexes and RLS.
2. Backfill: for each memory-bearing space, list its managed store, parse `subject` / `origin` / `observed_at` from
   the path, embed the content, insert rows with the space's access tags.
3. Switch the adapter in `Loom/wiring.py`; run unit, parity and live tests; compare retrieve results for the
   benchmark questions before and after.
4. After a soak period, delete the managed memory stores (`loom-<space>`). Session stores stay.

## Consequences

**Gains**

- One retrieve query instead of one call per store; memory gets vector and hybrid search.
- One access-control model (policy + RLS) for KB and memory; no store per space, no store grants.
- Atomic contribute and promotion writes; conflict check in one query.
- Memory flows to Delta through change data feed with the rest of the control plane.
- The "Show queries" panel shows memory as SQL alongside the KB.

**Costs**

- Off the recommended managed path for long-term memory; future managed-memory features (for example managed
  memory extraction from sessions) won't apply without rework.
- Loom owns the schema, migrations, retention, embeddings cost and index tuning for memory.
- The demo's story changes from "Databricks manages memory per store" to "Loom keeps governed memory on Lakebase,
  sessions stay managed". `docs/demo_script.md` section 4 and the Guided demo step "Memory follows the person" need
  new wording.

**Unchanged**

- The consumer contract (`configure`, `retrieve`, `contribute`) and its REST, MCP and SDK surfaces.
- Managed agent sessions for short-term state, read by distillation.
