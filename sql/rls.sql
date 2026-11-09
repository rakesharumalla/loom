-- Postgres row-level security on chunks: DEFENSE IN DEPTH ONLY.
-- The decision-maker is Loom/spaces/policy.py. The facade sets loom.caller_principals per
-- transaction from the verified identity (never from a model). RLS catches a facade query that
-- forgets its access filter; it does not protect against a compromised facade, which could set
-- the GUC itself. FORCE makes the policies apply to the table owner too.
SET search_path = loom, public;

CREATE OR REPLACE FUNCTION loom.caller_principals() RETURNS text[]
LANGUAGE sql STABLE AS $$
  SELECT coalesce(string_to_array(nullif(current_setting('loom.caller_principals', true), ''), ','), '{}')
$$;

CREATE OR REPLACE FUNCTION loom.rls_writer() RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT coalesce(current_setting('loom.writer', true), '') = 'on'
$$;

ALTER TABLE chunks ENABLE ROW LEVEL SECURITY;
ALTER TABLE chunks FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS chunks_read ON chunks;
CREATE POLICY chunks_read ON chunks FOR SELECT
  USING (access_groups && loom.caller_principals());

DROP POLICY IF EXISTS chunks_write ON chunks;
CREATE POLICY chunks_write ON chunks FOR ALL
  USING (loom.rls_writer()) WITH CHECK (loom.rls_writer());

-- The Lakebase project owner and databricks_superuser roles have BYPASSRLS, so RLS would not apply to
-- the facade's login role. Chunk access therefore runs under NOLOGIN roles without BYPASSRLS, entered
-- per transaction with SET LOCAL ROLE (granted to the facade identity by scripts/setup.py).
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'loom_reader') THEN CREATE ROLE loom_reader NOLOGIN NOBYPASSRLS; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'loom_writer') THEN CREATE ROLE loom_writer NOLOGIN NOBYPASSRLS; END IF;
END $$;
GRANT USAGE ON SCHEMA loom TO loom_reader, loom_writer;
GRANT SELECT ON chunks TO loom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON chunks TO loom_writer;
GRANT SELECT ON files TO loom_writer;
