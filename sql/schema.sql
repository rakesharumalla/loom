-- Loom storage on Lakebase. Idempotent. Applied by scripts/setup.py.
-- Requires Lakebase Search (lakebase_vector, lakebase_text):
-- https://docs.databricks.com/aws/en/oltp/projects/lakebase-search
CREATE EXTENSION IF NOT EXISTS lakebase_vector CASCADE;
CREATE EXTENSION IF NOT EXISTS lakebase_text;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS loom;
SET search_path = loom, public;

-- ---------------------------------------------------------------- control plane (implemented in Loom)
CREATE TABLE IF NOT EXISTS spaces (
  space_id        text PRIMARY KEY,
  scope           text NOT NULL CHECK (scope IN ('personal','project','team','company')),
  name            text NOT NULL,
  owner_principal text NOT NULL,
  config          jsonb NOT NULL DEFAULT '{}'::jsonb,
  ephemeral       boolean NOT NULL DEFAULT false,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS space_members (
  space_id  text NOT NULL REFERENCES spaces(space_id) ON DELETE CASCADE,
  principal text NOT NULL,
  kind      text NOT NULL CHECK (kind IN ('user','group')),
  perm      text NOT NULL CHECK (perm IN ('read','write','admin')),
  PRIMARY KEY (space_id, principal, kind)
);

CREATE TABLE IF NOT EXISTS space_guardrails (
  space_id  text NOT NULL REFERENCES spaces(space_id) ON DELETE CASCADE,
  direction text NOT NULL CHECK (direction IN ('read','write')),
  rules     jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (space_id, direction)
);

CREATE TABLE IF NOT EXISTS space_bindings (
  space_id text NOT NULL REFERENCES spaces(space_id) ON DELETE CASCADE,
  backend  text NOT NULL CHECK (backend IN ('kb','memory','sessions')),
  ref      text NOT NULL,
  PRIMARY KEY (space_id, backend)
);

CREATE TABLE IF NOT EXISTS memory_audit (
  event_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  ts         timestamptz NOT NULL DEFAULT now(),
  request_id text,
  caller     text NOT NULL,
  surface    text NOT NULL,
  action     text NOT NULL,
  space_id   text,
  decision   text NOT NULL,
  detail     jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS memory_audit_ts ON memory_audit (ts DESC);
CREATE INDEX IF NOT EXISTS memory_audit_req ON memory_audit (request_id);

CREATE TABLE IF NOT EXISTS composition_traces (
  trace_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  ts         timestamptz NOT NULL DEFAULT now(),
  request_id text,
  caller     text NOT NULL,
  surface    text NOT NULL,
  query      text NOT NULL,
  trace      jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS conflict_decisions (
  decision_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  ts          timestamptz NOT NULL DEFAULT now(),
  subject     text NOT NULL,
  outcome     text NOT NULL CHECK (outcome IN ('resolved','needs_human_review')),
  rule        text NOT NULL,
  winner      jsonb,
  losers      jsonb NOT NULL DEFAULT '[]'::jsonb,
  rationale   text NOT NULL,
  flags       jsonb NOT NULL DEFAULT '[]'::jsonb,
  reviewed_by text
);

CREATE TABLE IF NOT EXISTS promotion_requests (
  request_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  ts           timestamptz NOT NULL DEFAULT now(),
  proposer     text NOT NULL,
  source_space text NOT NULL,
  target_space text NOT NULL REFERENCES spaces(space_id) ON DELETE CASCADE,
  kind         text NOT NULL CHECK (kind IN ('fact','lesson')),
  payload      jsonb NOT NULL,
  status       text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected')),
  approver     text,
  decided_at   timestamptz
);

CREATE TABLE IF NOT EXISTS feedback (
  feedback_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  ts          timestamptz NOT NULL DEFAULT now(),
  caller      text NOT NULL,
  query       text NOT NULL,
  source_ref  text NOT NULL,
  rating      int NOT NULL CHECK (rating BETWEEN -1 AND 1),
  comment     text
);

-- ---------------------------------------------------------------- storage port (replaces MongoDB + S3 metadata)
CREATE TABLE IF NOT EXISTS kbs (
  kb_id      text PRIMARY KEY,
  name       text NOT NULL,
  space_id   text REFERENCES spaces(space_id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kb_members (
  kb_id     text NOT NULL REFERENCES kbs(kb_id) ON DELETE CASCADE,
  principal text NOT NULL,
  role      text NOT NULL CHECK (role IN ('reader','writer','owner')),
  PRIMARY KEY (kb_id, principal)
);

CREATE TABLE IF NOT EXISTS files (
  file_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  kb_id           text NOT NULL REFERENCES kbs(kb_id) ON DELETE CASCADE,
  space_id        text NOT NULL,
  path            text NOT NULL,
  current_version int  NOT NULL DEFAULT 0,
  deleted         boolean NOT NULL DEFAULT false,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (kb_id, path)
);

CREATE TABLE IF NOT EXISTS file_versions (
  file_id      uuid NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
  version      int  NOT NULL,
  content_hash text NOT NULL,
  inline_text  text,
  volume_path  text,
  size         bigint NOT NULL,
  content_type text NOT NULL DEFAULT 'text/markdown',
  created_by   text NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (file_id, version),
  CHECK (inline_text IS NOT NULL OR volume_path IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  kb_id       text NOT NULL REFERENCES kbs(kb_id) ON DELETE CASCADE,
  label       text,
  created_by  text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  manifest    jsonb NOT NULL   -- {path: {file_id, version, content_hash}}
);

CREATE TABLE IF NOT EXISTS overlays (
  overlay_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  kb_id            text NOT NULL REFERENCES kbs(kb_id) ON DELETE CASCADE,
  name             text NOT NULL,
  base_snapshot_id uuid REFERENCES snapshots(snapshot_id),
  status           text NOT NULL DEFAULT 'open' CHECK (status IN ('open','merged','discarded')),
  created_by       text NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS overlay_entries (
  overlay_id   uuid NOT NULL REFERENCES overlays(overlay_id) ON DELETE CASCADE,
  path         text NOT NULL,
  op           text NOT NULL CHECK (op IN ('put','delete')),
  content_hash text,
  inline_text  text,
  volume_path  text,
  size         bigint,
  PRIMARY KEY (overlay_id, path)
);

-- Leases are rows with an expiry; acquisition is an atomic upsert that only succeeds when the
-- previous lease expired or is held by the same holder. Short critical sections use
-- pg_advisory_xact_lock(hashtext(resource)) instead (see StoragePort docs).
CREATE TABLE IF NOT EXISTS leases (
  resource   text PRIMARY KEY,
  holder     text NOT NULL,
  expires_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
  chunk_id      text PRIMARY KEY,           -- {file_id}:{chunk_index}
  kb_id         text NOT NULL,
  space_id      text NOT NULL,
  file_id       uuid NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
  version       int  NOT NULL,
  chunk_index   int  NOT NULL,
  path          text NOT NULL,
  content       text NOT NULL,
  content_hash  text NOT NULL,
  embedding     vector(1024),
  content_tsv   tsvector,
  access_groups text[] NOT NULL DEFAULT '{}',
  metadata      jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS chunks_kb ON chunks (kb_id);
CREATE INDEX IF NOT EXISTS chunks_space ON chunks (space_id);
CREATE INDEX IF NOT EXISTS chunks_file ON chunks (file_id);
CREATE INDEX IF NOT EXISTS chunks_access ON chunks USING gin (access_groups);

-- Freshness log: file change -> searchable timings emitted by pipelines/ingest_job.py
CREATE TABLE IF NOT EXISTS ingest_events (
  event_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  ts           timestamptz NOT NULL DEFAULT now(),
  kb_id        text NOT NULL,
  path         text NOT NULL,
  action       text NOT NULL,      -- upsert | delete | skip
  chunks_embedded int NOT NULL DEFAULT 0,
  chunks_deleted  int NOT NULL DEFAULT 0,
  changed_at   timestamptz,
  searchable_at timestamptz,
  detail       jsonb NOT NULL DEFAULT '{}'::jsonb
);
