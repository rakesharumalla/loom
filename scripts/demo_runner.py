"""Parity across consumers (demo step 6): the same retrieve through the web UI route, an MCP agent, and a
batch service via the SDK, each with the persona's own OAuth token against the deployed app (or a local
server with --url http://localhost:8765). Prints whether the composed answers are identical.

Run: uv run python scripts/demo_runner.py [--persona bob] [--query "..."] [--url https://...]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import httpx  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.identity import PersonaTokens  # noqa: E402
from Loom.mcp_client import call_tool  # noqa: E402
from Loom.sdk import LoomClient  # noqa: E402


def key(resp: dict) -> list:
    """What 'identical composed answer' means: same ordered sources, same included spaces, same conflicts."""
    return [[(r["provenance"]["space_id"], r["provenance"]["ref"]) for r in resp["results"]],
            sorted(v["space_id"] for v in resp["trace"]["considered"] if v["included"]),
            sorted((c["subject"], c["rule"]) for c in resp["trace"]["conflicts"])]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--persona", default="bob")
    ap.add_argument("--query", default="ETH withdrawal queue stuck after a node upgrade, signer timing out")
    ap.add_argument("--url", default=None)
    args = ap.parse_args()
    w = WorkspaceClient(profile=CONFIG.profile)
    url = (args.url or w.apps.get("loom-demo").url).rstrip("/")
    tok = PersonaTokens(w, CONFIG.secret_scope).token(args.persona)
    hdr = {"Authorization": f"Bearer {tok}"}

    web = httpx.post(f"{url}/ui/retrieve", json={"persona": args.persona, "query": args.query, "k": 5},
                     headers=hdr, timeout=120).json()
    sdk = LoomClient(url, token_provider=lambda: tok).retrieve(args.query, k=5).model_dump(mode="json")
    mcp = call_tool(f"{url}/mcp/", tok, "retrieve", {"query": args.query, "k": 5})

    for name, r in (("web UI", web), ("MCP agent", mcp), ("SDK service", sdk)):
        print(f"\n== {name}: caller={r['trace']['caller']} groups={r['trace']['groups']}")
        for x in r["results"]:
            print(f"   {x['provenance']['backend']:8} {x['provenance']['space_id']:16} {x['provenance'].get('path')}")
    same = key(web) == key(mcp) == key(sdk)
    print(f"\nidentical composed answer across web / MCP / SDK: {same}")
    if not same:
        print(json.dumps({"web": key(web), "mcp": key(mcp), "sdk": key(sdk)}, indent=1))
    return 0 if same else 1


if __name__ == "__main__":
    sys.exit(main())
