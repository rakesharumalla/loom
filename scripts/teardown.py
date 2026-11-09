"""Remove everything scripts/setup.py and the demo created. Destructive: asks for confirmation.

Deletes: Lakebase project, memory stores bound to Loom spaces (prefix loom-), the Loom session store,
UC volume + schema (loom_catalog.loom only; the catalog itself is left alone), persona service
principals, loom-* workspace groups, the secret scope, the app and bundle job.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.errors import NotFound  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.adapters.databricks.uc_sql import run_sql  # noqa: E402

APP_NAME = "loom-demo"


def step(label, fn):
    try:
        fn()
        print(f"[teardown] removed {label}")
    except NotFound:
        print(f"[teardown] absent  {label}")
    except Exception as e:  # keep going so a partial teardown can be re-run
        print(f"[teardown] FAILED  {label}: {e}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--yes", action="store_true")
    args = ap.parse_args()
    if not args.yes and input(f"Delete Lakebase project {CONFIG.lakebase_project}, loom memory/session stores, "
                              f"{CONFIG.catalog}.{CONFIG.schema}, loom SPs/groups? type 'delete': ") != "delete":
        return 1
    w = WorkspaceClient(profile=CONFIG.profile)

    step(f"app {APP_NAME}", lambda: w.apps.delete(APP_NAME))
    for j in w.jobs.list(name="loom-"):
        step(f"job {j.settings.name}", lambda j=j: w.jobs.delete(j.job_id))
    for ms in w.agent_kit.list_memory_stores():
        if ms.name.split("/")[-1].startswith("loom-"):
            step(ms.name, lambda n=ms.name: w.agent_kit.delete_memory_store(n))
    for ss in w.agent_kit.list_session_stores():
        if ss.name.split("/")[-1].startswith("loom-"):
            step(ss.name, lambda n=ss.name: w.agent_kit.delete_session_store(n))
    step(f"lakebase project {CONFIG.lakebase_project}",
         lambda: w.postgres.delete_project(f"projects/{CONFIG.lakebase_project}").wait())
    step(f"schema {CONFIG.catalog}.{CONFIG.schema}",
         lambda: run_sql(w, f"DROP SCHEMA IF EXISTS {CONFIG.catalog}.{CONFIG.schema} CASCADE"))
    for spec in CONFIG.personas.values():
        for sp in w.service_principals.list(filter=f'displayName eq "{spec["sp"]}"'):
            step(f"service principal {spec['sp']}", lambda sp=sp: w.service_principals.delete(sp.id))
    for g in CONFIG.groups:
        for grp in w.groups.list(filter=f'displayName eq "{g}"'):
            step(f"group {g}", lambda grp=grp: w.groups.delete(grp.id))
    step(f"secret scope {CONFIG.secret_scope}", lambda: w.secrets.delete_scope(CONFIG.secret_scope))
    return 0


if __name__ == "__main__":
    sys.exit(main())
