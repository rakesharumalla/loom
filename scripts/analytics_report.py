"""Create analytics views over the CDF Delta tables and print a markdown report (step 11)."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.adapters.databricks.uc_sql import run_sql, warehouse_id  # noqa: E402

w = WorkspaceClient(profile=CONFIG.profile)
wh = warehouse_id(w)
sql = (ROOT / "sql" / "analytics.sql").read_text().replace("${catalog}", CONFIG.catalog)
for stmt in sql.split(";"):
    if "CREATE" in stmt:
        run_sql(w, stmt[stmt.index("CREATE"):], wh)
views = ["v_resolution_outcomes", "v_space_usage_by_scope", "v_excluded_reasons", "v_low_rated_sources",
         "v_calls_by_surface"]
for v in views:
    rows = run_sql(w, f"SELECT * FROM {CONFIG.catalog}.loom_lakehouse.{v} ORDER BY 1 LIMIT 20", wh)
    print(f"\n### {v}\n")
    for r in rows:
        print("| " + " | ".join(str(x) for x in r) + " |")
