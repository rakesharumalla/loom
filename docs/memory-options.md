# Agent memory on Databricks: managed agent memory vs. self-managed on Lakebase

**Short answer:** use managed agent memory when you want long-term agent memory with no infrastructure, and
keyword recall partitioned by user or team is enough. Use self-managed memory on Lakebase when you need semantic
or hybrid search, access control finer than a whole store, rich filtering, or memory in the same database and
transactions as your app data.

## What each one is

**Managed agent memory** (Beta) is a hosted store you reach through REST at `/api/2.0/agents/memory-stores/...`
or the AgentKit SDK. It runs on Lakebase, but you never see the tables. Each entry has an `actor_id`, a `path`,
`content`, a `description` and an optional `session_id`. Conversation history goes in a separate product,
[managed agent sessions](https://docs.databricks.com/aws/en/agents/agent-memory/managed-sessions).

**Self-managed memory on Lakebase** means your own Postgres tables in a Lakebase database. You write them through
SQL, either with a framework's Postgres store (the docs' LangGraph and OpenAI Agents SDK templates use a
checkpointer on Lakebase) or with your own schema. Loom's `loom.chunks` table is an example: vector and BM25 search, metadata, and row-level security, all
in SQL (`Loom/adapters/databricks/search_lakebase.py`, `sql/rls.sql`).

## How they differ

| | Managed agent memory | Self-managed on Lakebase |
|---|---|---|
| **Setup and operations** | None: create a store and call the API | You own the schema, migrations, connection pool, OAuth token refresh, embeddings, retention and scaling |
| **Search** | BM25 full-text only. Top N up to 100, no pagination, no vector search | Anything Postgres can do: vector search (`lakebase_ann`), BM25 (`lakebase_bm25`), hybrid ranking, exact filters |
| **Filtering** | Only by `actor_id` (always required), `path_prefix` and `session_id` | Any column or JSON field, plus joins with other tables |
| **Access control** | Per store only. `actor_id` separates entries but doesn't secure them; anyone with store access can read any actor's entries | Postgres roles, row-level security, per-row access lists. You design the model |
| **Querying many scopes** | One call per store (a Loom retrieve for bob makes 4 calls in parallel) | One SQL query across scopes, filtered by `space_id` and principals |
| **Data model** | Fixed fields. `create_time` is set by the server (Loom puts the observed time in the path) | Any schema: versions, confidence, sources, TTL, conflict state |
| **Transactions** | Separate from your app data | Same database transaction as your other writes (Loom's audit, conflicts and files) |
| **Analytics and testing** | Through the API only | Lakebase change data feed to Delta, and branches for test copies |
| **Latency (measured in Loom)** | About 85–270 ms per REST call | About 35–160 ms per SQL statement; the hybrid search was about 150 ms |
| **Lifecycle** | Beta, and the path Databricks recommends for new agents. The older Unity Catalog-based memory stores are legacy and being turned down | The docs label [self-managed agent memory](https://docs.databricks.com/aws/en/agents/agent-memory/self-managed-memory) legacy, but still describe it for direct SQL access or a custom schema. Lakebase itself is generally available; the tables are yours to maintain |

The Databricks docs disagree on one point. The
[Agent memory and sessions](https://docs.databricks.com/aws/en/generative-ai/agent-framework/stateful-agents)
overview says managed memory is "retrieved by semantic search". The
[managed memory](https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory) page's Limitations
section says search is BM25 full-text with no vector similarity search, and what we saw matches that: Loom's
memory results carry BM25 scores.

## When to use which

**Managed agent memory fits when:**

- Memory is per user or per team: preferences, past decisions, short facts, for example "summaries as three bullets".
- Keyword recall is good enough and each person or group has a modest number of entries.
- Isolation maps cleanly onto stores (one store per tenant, team or app), and you set `actor_id` from the verified
  identity, never from the model.
- You want framework-agnostic memory with no database to run. Pair it with managed sessions for conversation history.

**Self-managed on Lakebase fits when:**

- You need semantic or hybrid recall over a large or growing corpus, or more than 100 results with pagination.
- Access is finer-grained than a store: per document, per group, or by attribute. This is why Loom's knowledge base
  uses row-level security on `loom.chunks`.
- Memory must be filtered or joined with operational data (accounts, tickets, permissions), or written atomically
  with it.
- You need versioning, audit, custom conflict resolution, retention rules, or analytics in Delta.
- You want one query across many scopes rather than one API call per store.

**Using both,** which is what Loom does:

- Managed sessions hold conversation state.
- Managed memory holds personal and team notes, with one store per space because access control is per store.
- Lakebase tables hold the governed knowledge base (hybrid search, row-level security, versions) and the control
  plane (spaces, audit, conflicts, promotions).
- A distillation step (`pipelines/distill_job.py`) moves durable facts from sessions into memory or the knowledge base.

The trade-off we saw in Loom: managed memory is easy to adopt, but per-store access control forces one store per
space, so a retrieve makes one REST call per store. Search is keyword-only, so the same question gets semantic
matches from the knowledge base but only keyword matches from memory. The app's **Show queries** panel (Ask and
Memory tabs) shows both paths side by side.

**Loom's decision:** move long-term memory into Lakebase tables and keep managed sessions (Option B). See
[`design-memory-option-b.md`](design-memory-option-b.md). It is accepted but not yet implemented.

## Sources

- Managed agent memory: https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory
- Managed agent sessions: https://docs.databricks.com/aws/en/agents/agent-memory/managed-sessions
- Agent memory and sessions overview: https://docs.databricks.com/aws/en/generative-ai/agent-framework/stateful-agents
- Managed agent memory (legacy): https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory-stores
- Self-managed agent memory (legacy): https://docs.databricks.com/aws/en/agents/agent-memory/self-managed-memory
- Lakebase Search: https://docs.databricks.com/aws/en/oltp/projects/lakebase-search

The self-managed memory page is marked legacy: for new agents it points to managed agent memory and sessions,
and recommends self-managed memory when you need direct SQL access or a custom schema. Neither page has a
side-by-side comparison; the self-managed column comes from that page, Lakebase's SQL capabilities and this
project's code. Latencies were measured on 2026-10-09 from this project's app and jobs
(us-east-1).
