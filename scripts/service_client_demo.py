"""Batch service consumer via the Python SDK (same contract as REST/MCP). Uses the `service` persona's
OAuth token. Run: uv run python scripts/service_client_demo.py [--url http://localhost:8765]"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.identity import PersonaTokens  # noqa: E402
from Loom.sdk import LoomClient, LoomError  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--url", default=None)
ap.add_argument("--persona", default="service")
args = ap.parse_args()
w = WorkspaceClient(profile=CONFIG.profile)
url = args.url or w.apps.get("loom-demo").url
tokens = PersonaTokens(w, CONFIG.secret_scope)
loom = LoomClient(url, token_provider=lambda: tokens.token(args.persona))

for q in ["What is the daily withdrawal limit for verified retail customers?",
          "withdrawal queue stuck after node upgrade"]:
    r = loom.retrieve(q, k=3)
    print(f"\n[{r.trace.caller}] {q}")
    print("  included:", [v.space_id for v in r.trace.considered if v.included])
    for x in r.results:
        print(f"  {x.provenance.backend:7} {x.provenance.space_id:18} {x.provenance.path}")
try:
    r = loom.contribute("Batch note: nightly reconciliation finished.", target_space="company", subject="batch.note")
    print("\ncontribute to company:", r.status, r.reason)
except LoomError as e:
    print("\ncontribute to company:", e)
