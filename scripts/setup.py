"""Idempotent setup for the Loom on Databricks demo.

Phases (run in order by scripts/setup.sh):
  uc          schema + volume in UC, group/SP grants (READ VOLUME / WRITE VOLUME; UC has no DENY)
  identities  groups compliance/support/trading_ops, persona service principals, OAuth secrets in a secret scope
  lakebase    NEW Lakebase project (PG 18); verifies Lakebase Search extensions are available
  schema      Lakebase DDL (sql/schema.sql, sql/rls.sql) + Postgres group roles
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402
from google.protobuf.duration_pb2 import Duration  # noqa: E402
from databricks.sdk.errors import NotFound  # noqa: E402
from databricks.sdk.service import iam  # noqa: E402
from databricks.sdk.service import postgres as pg  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.adapters.databricks.lakebase_conn import LakebaseConnector  # noqa: E402
from Loom.adapters.databricks.uc_sql import run_sql  # noqa: E402

SEARCH_EXTENSIONS = ("lakebase_vector", "lakebase_text")


def log(msg: str) -> None:
    print(f"[setup] {msg}", flush=True)


# ---------------------------------------------------------------- identities
def ensure_group(w: WorkspaceClient, name: str) -> iam.Group:
    found = list(w.groups.list(filter=f'displayName eq "{name}"'))
    if found:
        return found[0]
    log(f"create group {name}")
    return w.groups.create(display_name=name)


def ensure_sp(w: WorkspaceClient, name: str) -> iam.ServicePrincipal:
    found = list(w.service_principals.list(filter=f'displayName eq "{name}"'))
    if found:
        return found[0]
    log(f"create service principal {name}")
    return w.service_principals.create(display_name=name, active=True)


def ensure_member(w: WorkspaceClient, group: iam.Group, member_id: str) -> None:
    g = w.groups.get(group.id)
    if any(m.value == member_id for m in (g.members or [])):
        return
    w.groups.patch(group.id, schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                   operations=[iam.Patch(op=iam.PatchOp.ADD, path="members", value=[{"value": member_id}])])


def identities(w: WorkspaceClient) -> dict[str, iam.ServicePrincipal]:
    groups = {g: ensure_group(w, g) for g in CONFIG.groups}
    sps = {}
    for persona, spec in CONFIG.personas.items():
        sp = ensure_sp(w, spec["sp"])
        sps[persona] = sp
        for g in spec["groups"]:
            ensure_member(w, groups[g], sp.id)

    scopes = {s.name for s in w.secrets.list_scopes()}
    if CONFIG.secret_scope not in scopes:
        log(f"create secret scope {CONFIG.secret_scope}")
        w.secrets.create_scope(scope=CONFIG.secret_scope)
    keys = {s.key for s in w.secrets.list_secrets(CONFIG.secret_scope)}
    for persona, sp in sps.items():
        if f"{persona}-client-secret" in keys:
            continue
        log(f"create OAuth secret for {sp.display_name}")
        sec = w.service_principal_secrets_proxy.create(service_principal_id=sp.id)
        w.secrets.put_secret(CONFIG.secret_scope, f"{persona}-client-id", string_value=sp.application_id)
        w.secrets.put_secret(CONFIG.secret_scope, f"{persona}-client-secret", string_value=sec.secret)
    log("identities ok: " + ", ".join(f"{p}={sp.application_id}" for p, sp in sps.items()))
    return sps


# ---------------------------------------------------------------- unity catalog
def uc(w: WorkspaceClient) -> None:
    sps = {p: ensure_sp(w, s["sp"]) for p, s in CONFIG.personas.items()}
    c, s, v = CONFIG.catalog, CONFIG.schema, CONFIG.volume
    stmts = [
        f"CREATE SCHEMA IF NOT EXISTS {c}.{s} COMMENT 'Loom on Databricks demo'",
        f"CREATE VOLUME IF NOT EXISTS {c}.{s}.{v} COMMENT 'Loom raw files: content-addressed, write-once'",
    ]
    # The loom-* groups are workspace-local, and UC accepts only account-level principals, so
    # volume grants go to each persona SP (see docs/discovery.md, "Deviations").
    readers = [f"`{sps[p].application_id}`" for p in ("alice", "bob", "carol", "service")]
    writers = [f"`{sps['service'].application_id}`", f"`{sps['admin'].application_id}`"]
    for p in sorted(set(readers + writers)):
        stmts += [f"GRANT USE CATALOG ON CATALOG {c} TO {p}", f"GRANT USE SCHEMA ON SCHEMA {c}.{s} TO {p}"]
    for p in readers:
        stmts.append(f"GRANT READ VOLUME ON VOLUME {c}.{s}.{v} TO {p}")
    for p in writers:
        stmts.append(f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {c}.{s}.{v} TO {p}")
    for st in stmts:
        run_sql(w, st)
    log(f"uc ok: {c}.{s}.{v}")


# ---------------------------------------------------------------- lakebase
def lakebase(w: WorkspaceClient) -> bool:
    name = f"projects/{CONFIG.lakebase_project}"
    try:
        proj = w.postgres.get_project(name)
        log(f"project exists: {name} (pg {proj.status.pg_version})")
    except NotFound:
        log(f"create Lakebase project {name} (PG 18)")
        op = w.postgres.create_project(
            project_id=CONFIG.lakebase_project,
            project=pg.Project(spec=pg.ProjectSpec(
                display_name="Loom on Databricks demo",
                pg_version=18,
                default_endpoint_settings=pg.ProjectDefaultEndpointSettings(
                    autoscaling_limit_min_cu=0.5, autoscaling_limit_max_cu=2.0,
                    suspend_timeout_duration=Duration(seconds=1800)),
            )),
        )
        op.wait()
    conn = LakebaseConnector(w)
    with conn.connect(autocommit=True) as c:
        # The extensions are listed in pg_available_extensions even before Lakebase Search is
        # enabled; enabling is what adds lakebase_vector to shared_preload_libraries.
        preload = c.execute("SHOW shared_preload_libraries").fetchone()[0]
    if "lakebase_vector" in preload:
        log("Lakebase Search enabled (lakebase_vector preloaded)")
        return True
    host = w.config.host.rstrip("/")
    log("Lakebase Search is NOT enabled on this project yet.")
    log("Enabling is one-way and restarts all computes. Enable it in the UI:")
    log(f"  {host}/lakebase/projects/{CONFIG.lakebase_project}  ->  Settings  ->  Enable Lakebase Search")
    log("Then re-run scripts/setup.sh.")
    return False


def schema(w: WorkspaceClient) -> None:
    conn = LakebaseConnector(w)
    with conn.connect(autocommit=True) as c:
        for f in ("schema.sql", "rls.sql"):
            c.execute((ROOT / "sql" / f).read_text())
            log(f"applied sql/{f}")
        # the facade identity enters these roles per transaction (SET LOCAL ROLE)
        me = conn.user()
        c.execute(f'GRANT loom_reader, loom_writer TO "{me}"')
        log(f"granted loom_reader, loom_writer to {me}")
        # Databricks groups as Postgres roles (defense in depth for direct access):
        # https://docs.databricks.com/aws/en/oltp/projects/postgres-roles
        c.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
        existing = {r[0] for r in c.execute("SELECT rolname FROM pg_roles")}
        for g in CONFIG.groups:
            if g not in existing:
                c.execute("SELECT databricks_create_role(%s, 'GROUP')", (g,))
                log(f"postgres role for group {g}")
            c.execute(f'GRANT USAGE ON SCHEMA loom TO "{g}"')
            c.execute(f'GRANT SELECT ON loom.chunks TO "{g}"')


CDF_TABLES = ("memory_audit", "composition_traces", "conflict_decisions", "promotion_requests", "feedback",
              "ingest_events", "spaces", "space_members")


def cdf(w: WorkspaceClient) -> None:
    """Lakebase Change Data Feed (Public Preview): loom control-plane tables -> Delta history tables.
    https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf . Only tables with REPLICA IDENTITY FULL
    participate; `chunks` (embeddings) is intentionally left out."""
    conn = LakebaseConnector(w)
    with conn.connect(autocommit=True) as c:
        for t in CDF_TABLES:
            c.execute(f"ALTER TABLE loom.{t} REPLICA IDENTITY FULL")
    run_sql(w, f"CREATE SCHEMA IF NOT EXISTS {CONFIG.catalog}.loom_lakehouse COMMENT 'Loom audit/feedback history (Lakebase CDF)'")
    parent = f"{CONFIG.branch_path}/databases/databricks-postgres"
    existing = [x.name for x in w.postgres.list_cdf_configs(parent=parent)]
    if existing:
        log(f"cdf config exists: {existing}")
        return
    from databricks.sdk.service import postgres as pgs
    op = w.postgres.create_cdf_config(parent=parent, cdf_config_id="loom_events",
                                      cdf_config=pgs.CdfConfig(catalog=CONFIG.catalog, schema="loom_lakehouse",
                                                               postgres_schema="loom"))
    op.wait()
    log(f"cdf config created: {parent}/cdfConfigs/loom_events -> {CONFIG.catalog}.loom_lakehouse")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", default="all", choices=["all", "identities", "uc", "lakebase", "schema", "cdf"])
    args = ap.parse_args()
    w = WorkspaceClient(profile=CONFIG.profile)
    if args.phase in ("all", "identities"):
        identities(w)
    if args.phase in ("all", "uc"):
        uc(w)
    if args.phase in ("all", "lakebase", "schema"):
        if not lakebase(w):
            return 3
    if args.phase in ("all", "schema"):
        schema(w)
    if args.phase in ("all", "cdf"):
        cdf(w)
    log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
