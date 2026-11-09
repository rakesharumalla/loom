"""Contract parity: identical scenarios on REST, MCP and SDK must give identical results and identical
audit records (apart from `surface`). Each surface runs against a fresh, identically seeded service
behind a real HTTP server (uvicorn), with bearer-token auth on every call."""
import re
import socket
import threading
import time

import httpx
import pytest
import uvicorn

from Loom.api import create_app
from Loom.contract import ConfigureRequest
from Loom.control.audit import comparable
from Loom.identity import StaticTokenVerifier
from Loom.mcp_client import MCPToolError, call_tool
from Loom.mcp_server import build_mcp
from Loom.sdk import LoomClient, LoomError
from tests.conftest import IDS, build_service

TOKENS = {f"tok-{p}": ident for p, ident in IDS.items()}

SCENARIOS = [
    ("retrieve", "bob", {"query": "daily withdrawal limit retail customers", "k": 8}),
    ("retrieve", "alice", {"query": "escalate sanctions hits compliance officer", "hints": {"team": "risk"}}),
    ("retrieve", "carol", {"query": "market data feed lag failover", "k": 5}),
    ("contribute", "bob", {"content": "Bob prefers the exact runbook step first.", "subject": "preference.answer_style"}),
    ("contribute", "carol", {"content": "Care note", "target_space": "team:care", "subject": "x"}),
    ("contribute", "alice", {"content": "The daily retail withdrawal limit is $10,000.", "target_space": "project:atlas",
                             "subject": "withdrawal_limit.retail.daily"}),
    ("configure", "carol", {"space": {"scope": "team", "key": "care", "name": "Care", "owner": "carol"}}),
    ("retrieve", "bob", {"query": "exact runbook step preference", "k": 5}),
]


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    def __init__(self):
        self.svc = build_service()
        verifier = StaticTokenVerifier(TOKENS)
        app = create_app(self.svc, verifier, mcp_server=build_mcp(self.svc, verifier))
        self.port = _free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.server = uvicorn.Server(uvicorn.Config(app, port=self.port, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        for _ in range(100):
            if self.server.started:
                return self
            time.sleep(0.05)
        raise RuntimeError("server did not start")

    def __exit__(self, *a):
        self.server.should_exit = True
        self.thread.join(5)


def run_rest(srv, verb, who, body):
    r = httpx.post(f"{srv.url}/v1/{verb}", json=body, headers={"Authorization": f"Bearer tok-{who}"}, timeout=30)
    return r.json() if r.status_code < 400 else {"error": r.status_code}


def run_sdk(srv, verb, who, body):
    c = LoomClient(srv.url, token_provider=lambda: f"tok-{who}")
    try:
        if verb == "retrieve":
            h = body.get("hints", {})
            return c.retrieve(body["query"], project=h.get("project"), team=h.get("team"), k=body.get("k", 8)).model_dump(mode="json")
        if verb == "contribute":
            return c.contribute(**body).model_dump(mode="json")
        return c.configure(ConfigureRequest(**body)).model_dump(mode="json")
    except LoomError as e:
        return {"error": e.status}


def run_mcp(srv, verb, who, body):
    args = dict(body)
    if verb == "retrieve":
        args.update(args.pop("hints", {}))
    try:
        return call_tool(f"{srv.url}/mcp/", f"tok-{who}", verb, args)
    except MCPToolError as e:
        return {"error": 403 if "denied" in str(e) else str(e)}


def normalize(out):
    """Drop per-call fields: request ids, timings, and the wall-clock time of a new contribution."""
    if isinstance(out, dict):
        drop = {"request_id", "timings_ms"} | ({"observed_at"} if out.get("ref") == "(new)" else set())
        return {k: normalize(v) for k, v in out.items() if k not in drop}
    if isinstance(out, list):
        return [normalize(v) for v in out]
    if isinstance(out, str):
        return re.sub(r"(/facts/[^/]+/[a-z]+)-\d{8}T\d+", r"\1-<ts>", out)
    return out


@pytest.fixture(scope="module")
def runs():
    results = {}
    for surface, fn in (("rest", run_rest), ("sdk", run_sdk), ("mcp", run_mcp)):
        with Server() as srv:
            outs = [normalize(fn(srv, verb, who, body)) for verb, who, body in SCENARIOS]
            results[surface] = (outs, [r for r in srv.svc.ports.control.audit if r["surface"] != "job"])
    return results


def test_results_identical_across_surfaces(runs):
    rest, sdk, mcp = (runs[s][0] for s in ("rest", "sdk", "mcp"))
    for i, (verb, who, _) in enumerate(SCENARIOS):
        assert rest[i] == sdk[i] == mcp[i], f"scenario {i} {verb} as {who} differs"
    assert rest[0]["results"], "retrieve returned results"
    assert rest[4]["status"] == "rejected" and rest[5]["status"] == "held_for_review"
    assert rest[6] == {"error": 403}


def test_audit_identical_except_surface(runs):
    for s in ("rest", "sdk", "mcp"):
        assert {r["surface"] for r in runs[s][1]} == {s}
    rest, sdk, mcp = (normalize(comparable(runs[s][1])) for s in ("rest", "sdk", "mcp"))
    assert rest == sdk == mcp
    assert len(runs["rest"][1]) > len(SCENARIOS)
