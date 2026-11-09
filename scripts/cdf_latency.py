"""Measure Lakebase CDF latency: insert rows into loom.feedback, poll the Delta history table until visible.
Observed through a SQL warehouse with a 2 s polling interval, so results include query overhead and
up to 2 s polling granularity. Run: uv run python scripts/cdf_latency.py [--samples 3]"""
import argparse
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.adapters.databricks.lakebase_conn import LakebaseConnector  # noqa: E402
from Loom.adapters.databricks.uc_sql import run_sql, warehouse_id  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--samples", type=int, default=3)
args = ap.parse_args()
w = WorkspaceClient(profile=CONFIG.profile)
wh = warehouse_id(w)
run_sql(w, "SELECT 1", wh)   # warm the warehouse so its start-up is not counted
conn = LakebaseConnector(w)
print("change types seen:", run_sql(w, f"SELECT _pg_change_type, count(*) FROM {CONFIG.catalog}.loom_lakehouse."
                                       "lb_memory_audit_history GROUP BY 1", wh))
lat = []
for i in range(args.samples):
    marker = f"cdf-probe-{uuid.uuid4().hex[:8]}"
    with conn.tx() as c:
        c.execute("INSERT INTO loom.feedback (caller, query, source_ref, rating, comment) VALUES ('probe','cdf','probe',0,%s)",
                  (marker,))
    t0 = time.time()
    while time.time() - t0 < 600:
        n = run_sql(w, f"SELECT count(*) FROM {CONFIG.catalog}.loom_lakehouse.lb_feedback_history "
                       f"WHERE comment = '{marker}'", wh)[0][0]
        if int(n):
            lat.append(round(time.time() - t0, 1))
            print(f"sample {i + 1}: committed in Lakebase -> visible in Delta after {lat[-1]} s")
            break
        time.sleep(2)
    else:
        print(f"sample {i + 1}: not visible after 600 s")
with conn.tx() as c:
    c.execute("DELETE FROM loom.feedback WHERE caller = 'probe'")
print("latencies (s):", lat)
