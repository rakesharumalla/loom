"""Grant the app's service principal what the facade needs (idempotent). Run after `databricks bundle deploy`.

- App resources: Lakebase branch/database (CAN_CONNECT_AND_CREATE), embedding + LLM endpoints (CAN_QUERY)
- Secret scope READ (persona OAuth secrets for the demo persona switcher)
- Postgres: usage + DML on schema loom, membership in loom_reader / loom_writer (RLS roles)
- Managed memory and session stores: store-level grant (the only ACL these stores have)
- UC: USE CATALOG/SCHEMA, READ VOLUME + WRITE VOLUME on the raw volume
"""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service.workspace import AclPermission  # noqa: E402
from databricks_agentkit import AgentKitClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.adapters.databricks.lakebase_conn import LakebaseConnector  # noqa: E402
from Loom.adapters.databricks.uc_sql import run_sql  # noqa: E402

APP = "loom-demo"
w = WorkspaceClient(profile=CONFIG.profile)
app = w.apps.get(APP)
sp = app.service_principal_client_id
print(f"[grants] app {APP} service principal {sp}")

db = f"{CONFIG.branch_path}/databases/databricks-postgres"
body = {"update_mask": "resources", "app": {"resources": [
    {"name": "postgres", "postgres": {"branch": CONFIG.branch_path, "database": db, "permission": "CAN_CONNECT_AND_CREATE"}},
    {"name": "embeddings", "serving_endpoint": {"name": CONFIG.embedding_endpoint, "permission": "CAN_QUERY"}},
    {"name": "llm", "serving_endpoint": {"name": CONFIG.llm_endpoint, "permission": "CAN_QUERY"}},
]}}
out = subprocess.run(["databricks", "apps", "create-update", APP, "--json", json.dumps(body), "--profile", CONFIG.profile],
                     capture_output=True, text=True)
print("[grants] app resources:", "ok" if out.returncode == 0 else out.stderr[-400:])

w.secrets.put_acl(CONFIG.secret_scope, sp, AclPermission.READ)
print("[grants] secret scope READ")

with LakebaseConnector(w).connect(autocommit=True) as c:
    if not c.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (sp,)).fetchone():
        c.execute("SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL')", (sp,))
    for stmt in [f'GRANT USAGE ON SCHEMA loom TO "{sp}"',
                 f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA loom TO "{sp}"',
                 f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA loom TO "{sp}"',
                 f'GRANT EXECUTE ON FUNCTION loom.caller_principals(), loom.rls_writer() TO "{sp}"',
                 f'GRANT loom_reader, loom_writer TO "{sp}"',
                 f'GRANT USAGE ON SCHEMA loom_bench TO "{sp}"',              # in-app benchmark
                 f'GRANT SELECT ON ALL TABLES IN SCHEMA loom_bench TO "{sp}"']:
        c.execute(stmt)
print("[grants] postgres schema loom + loom_reader/loom_writer")

ak = AgentKitClient(w)
n = 0
for store in ak.memory_stores.list():
    if store.name.split("/")[-1].startswith("loom-"):
        store.grant_permission(sp, permission="WRITE")
        n += 1
for store in ak.session_stores.list():
    if store.name.split("/")[-1].startswith("loom-"):
        store.grant_permission(sp, permission="WRITE")
        n += 1
print(f"[grants] {n} memory/session stores")

c, s, v = CONFIG.catalog, CONFIG.schema, CONFIG.volume
for st in [f"GRANT USE CATALOG ON CATALOG {c} TO `{sp}`", f"GRANT USE SCHEMA ON SCHEMA {c}.{s} TO `{sp}`",
           f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {c}.{s}.{v} TO `{sp}`"]:
    run_sql(w, st)
print("[grants] UC volume")

# persona SPs may call the app (SDK service client, MCP agent): CAN_USE on the app
from databricks.sdk.service.apps import AppAccessControlRequest, AppPermissionLevel  # noqa: E402
from Loom.identity import persona_map  # noqa: E402
acl = [AppAccessControlRequest(service_principal_name=app_id, permission_level=AppPermissionLevel.CAN_USE)
       for app_id in persona_map(w, CONFIG.personas)]
w.apps.update_permissions(APP, access_control_list=acl)
print(f"[grants] CAN_USE on app for {len(acl)} persona SPs")
