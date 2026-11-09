"""Databricks App: the Loom facade (REST + MCP) and a projector-friendly web consumer/admin UI.

The web UI is a consumer of the public contract. Its persona switcher mints a real OAuth token for the
chosen persona's service principal (secrets in the `loom-demo` scope) and every call is verified like any
other caller. This impersonation is a demo affordance; production would use the signed-in user's token.
"""
from __future__ import annotations

import base64
import os
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402
from fastapi import HTTPException, Request  # noqa: E402
from fastapi.responses import FileResponse  # noqa: E402
from pydantic import BaseModel  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.api import create_app  # noqa: E402
from Loom import querylog  # noqa: E402
from Loom.contract import ConfigureRequest, ContributeRequest, Hints, RetrieveRequest  # noqa: E402
from Loom.identity import DatabricksTokenVerifier, PersonaTokens  # noqa: E402
from Loom.mcp_server import build_mcp  # noqa: E402
from Loom.service import PolicyError  # noqa: E402
from Loom.wiring import build  # noqa: E402

IN_APP = "DATABRICKS_APP_PORT" in os.environ
w = WorkspaceClient() if IN_APP else WorkspaceClient(profile=CONFIG.profile)
svc, ingestor, raw = build(w)
PERSONAS = list(CONFIG.personas)


def _persona_map() -> dict[str, str]:
    """application id -> persona, read from the secret scope written by setup (no SCIM listing needed)."""
    out = {}
    for p in PERSONAS:
        v = w.secrets.get_secret(CONFIG.secret_scope, f"{p}-client-id").value
        out[base64.b64decode(v).decode()] = p
    return out


verifier = DatabricksTokenVerifier(w.config.host, _persona_map())
tokens = PersonaTokens(w, CONFIG.secret_scope)
mcp = build_mcp(svc, verifier, proxy_client=w if IN_APP else None)
DOCS = {
    "lakebase": "https://docs.databricks.com/aws/en/oltp/projects/",
    "lakebase_search": "https://docs.databricks.com/aws/en/oltp/projects/lakebase-search",
    "memory": "https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory",
    "sessions": "https://docs.databricks.com/aws/en/agents/agent-memory/managed-sessions",
    "volumes": "https://docs.databricks.com/aws/en/volumes/privileges",
    "cdf": "https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf",
    "branches": "https://docs.databricks.com/aws/en/oltp/projects/manage-branches",
    "apps_mcp": "https://docs.databricks.com/aws/en/agents/mcp-tools/custom-mcp",
}
JOBS: dict[str, dict] = {}
JOB_NAMES = {"ingest": "loom-ingest", "distill": "loom-distill", "bench": "loom-bench", "eval": "loom-eval"}
_job_links: dict | None = None


def job_links() -> dict:
    """Workspace links to the bundle's jobs. Ids come from app resources (app.yaml valueFrom); locally,
    look the jobs up by name (bundle dev mode prefixes names, so match the suffix)."""
    global _job_links
    if _job_links is None:
        ids = {k: os.environ.get(f"LOOM_{k.upper()}_JOB_ID") for k in JOB_NAMES}
        if not all(ids.values()):
            try:
                for j in w.jobs.list():
                    for k, name in JOB_NAMES.items():
                        if not ids[k] and j.settings and (j.settings.name or "").endswith(name):
                            ids[k] = str(j.job_id)
            except Exception:
                pass
        host = w.config.host.rstrip("/")
        _job_links = {k: {"name": JOB_NAMES[k], "id": v, "url": f"{host}/jobs/{v}"} for k, v in ids.items() if v}
    return _job_links


class UIRetrieve(BaseModel):
    persona: str
    query: str
    project: str | None = None
    team: str | None = None
    k: int = 8
    session_id: str | None = None


class UIContribute(BaseModel):
    persona: str
    content: str
    kind: str = "fact"
    target_space: str | None = None
    subject: str | None = None
    propose_promotion_to: str | None = None
    session_id: str | None = None


class UIConfigure(BaseModel):
    persona: str = "admin"
    request: dict


class UIDecide(BaseModel):
    persona: str
    promotion_id: str
    approve: bool = True


def who(persona: str):
    if persona not in PERSONAS:
        raise HTTPException(400, f"unknown persona {persona}")
    return verifier.verify(tokens.token(persona))   # real token, verified like any caller


def run_bg(name: str, fn) -> dict:
    if JOBS.get(name, {}).get("status") == "running":
        return JOBS[name]
    JOBS[name] = {"status": "running", "started": time.time()}

    def go():
        try:
            JOBS[name].update(status="done", result=fn(), seconds=round(time.time() - JOBS[name]["started"], 2))
        except Exception as e:  # surface failures in the UI
            JOBS[name].update(status="error", error=str(e)[:500])
    threading.Thread(target=go, daemon=True).start()
    return JOBS[name]


def ui_routes(app):
    from fastapi.staticfiles import StaticFiles
    app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

    @app.get("/")
    def index():
        return FileResponse(Path(__file__).parent / "static" / "index.html")

    @app.get("/ui/debug/headers")
    def debug_headers(request: Request):
        """Header NAMES only (no values): shows what the Apps proxy forwards."""
        return sorted(request.headers.keys())

    @app.get("/ui/whoami")
    def ui_whoami(persona: str):
        """Who the selected persona is, as Loom sees it: verified groups plus readable/writable spaces."""
        from Loom.spaces import policy
        ident = who(persona)
        rows = []
        for r in svc.registry.all():
            if r.space.scope == "personal" and r.space.key != ident.principal:
                continue
            rd, wr = policy.decide(ident, r, "read"), policy.decide(ident, r, "write")
            rows.append({"space_id": r.space_id, "scope": r.space.scope, "name": r.space.name,
                         "read": rd.allowed, "write": wr.allowed, "via": rd.reason})
        return {"principal": ident.principal, "groups": list(ident.groups), "platform_admin": ident.is_platform_admin,
                "spaces": rows}

    @app.post("/ui/parity")
    def ui_parity(b: UIRetrieve):
        """Same retrieve through the three real surfaces of this app (REST, MCP, SDK) with the persona's token."""
        import asyncio
        import httpx
        from Loom.mcp_client import call_tool_async
        from Loom.sdk import LoomClient
        base = f"http://127.0.0.1:{os.environ.get('DATABRICKS_APP_PORT', '8765')}"
        tok = tokens.token(b.persona)
        rest = httpx.post(f"{base}/v1/retrieve", json={"query": b.query, "k": 5},
                          headers={"Authorization": f"Bearer {tok}"}, timeout=120).json()
        sdk = LoomClient(base, token_provider=lambda: tok).retrieve(b.query, k=5).model_dump(mode="json")
        loop = asyncio.new_event_loop()
        try:
            mcp_r = loop.run_until_complete(call_tool_async(f"{base}/mcp/", tok, "retrieve", {"query": b.query, "k": 5}))
        finally:
            loop.close()
        key = lambda r: [(x["provenance"]["space_id"], x["provenance"]["ref"]) for x in r["results"]]
        out = {name: {"caller": r["trace"]["caller"], "sources": [f'{x["provenance"]["space_id"]} · {x["provenance"].get("path") or x["provenance"]["ref"]}'
                                                               for x in r["results"]]}
               for name, r in (("REST (web, services)", rest), ("MCP (agents)", mcp_r), ("Python SDK (batch)", sdk))}
        return {"identical": key(rest) == key(mcp_r) == key(sdk), "surfaces": out}

    @app.get("/ui/meta")
    def meta():
        return {"personas": PERSONAS, "docs": DOCS, "spaces": [r.space_id for r in svc.registry.all()],
                "lakebase_project": CONFIG.lakebase_project, "volume": CONFIG.volume_path, "jobs": job_links()}

    @app.post("/ui/retrieve")
    def ui_retrieve(b: UIRetrieve):
        req = RetrieveRequest(query=b.query, hints=Hints(project=b.project or None, team=b.team or None), k=b.k,
                              session_id=b.session_id or None)
        ident = who(b.persona)
        with querylog.capture() as q:   # SQL + REST behind this click, for the "Show queries" panel
            r = svc.retrieve(ident, req, "web")
        return r.model_dump(mode="json") | {"queries": q.calls}

    @app.post("/ui/contribute")
    def ui_contribute(b: UIContribute):
        req = ContributeRequest(content=b.content, kind=b.kind, target_space=b.target_space or None,
                                subject=b.subject or None, propose_promotion_to=b.propose_promotion_to or None,
                                session_id=b.session_id or None)
        ident = who(b.persona)
        with querylog.capture() as q:
            r = svc.contribute(ident, req, "web")
        return r.model_dump(mode="json") | {"queries": q.calls}

    @app.post("/ui/configure")
    def ui_configure(b: UIConfigure):
        try:
            return svc.configure(who(b.persona), ConfigureRequest(**b.request), "web")
        except PolicyError as e:
            raise HTTPException(403, str(e))

    @app.post("/ui/promotions/decide")
    def ui_decide(b: UIDecide):
        try:
            return svc.decide_promotion(who(b.persona), b.promotion_id, b.approve, "web")
        except PolicyError as e:
            raise HTTPException(403, str(e))

    @app.get("/ui/admin/{what}")
    def ui_admin(what: str, n: int = 40, with_queries: bool = False):
        if with_queries:   # {"rows": ..., "queries": ...} for the "Show queries" panel
            with querylog.capture() as q:
                rows = ui_admin(what, n)
            return {"rows": rows, "queries": q.calls}
        c = svc.ports.control
        if what == "spaces":
            return [{"space_id": r.space_id, "scope": r.space.scope, "name": r.space.name, "owner": r.space.owner_principal,
                     "members": [m.__dict__ for m in r.members], "guardrails": r.guardrails, "bindings": r.bindings}
                    for r in svc.registry.all()]
        if what == "audit":
            return c.tail_audit(n)
        if what == "conflicts":
            return c.list_conflicts(n)
        if what == "promotions":
            return c.list_promotions()
        if what == "ports":
            return svc.port_log.recent(n)
        if what == "jobs":
            return JOBS
        raise HTTPException(404)

    @app.post("/ui/run/{job}")
    def ui_run(job: str):
        if job == "ingest":
            from pipelines import ingest_job
            return run_bg("ingest", lambda: ingest_job.run(w))
        if job == "distill":
            from pipelines import distill_job
            return run_bg("distill", lambda: distill_job.run(w))
        if job == "freshness-edit":
            return run_bg("freshness", lambda: freshness_edit())
        if job == "bench":
            return run_bg("bench", lambda: bench_in_region())
        raise HTTPException(404)


def bench_in_region() -> dict:
    """Run bench/benchmark.py from inside the app (us-east-1, same region as Lakebase)."""
    from bench import benchmark
    from Loom.spaces.model import Identity
    ids = {p: Identity(p, app_id, tuple(CONFIG.personas[p]["groups"]), is_platform_admin=(p == "admin"))
           for app_id, p in _persona_map().items()}
    text = benchmark.run(svc, raw, ids, "Databricks App (in-region, us-east-1)", skip_build=True)
    return {"markdown": text}


def freshness_edit() -> dict:
    """Edit one synced source and delete another, ingest, then poll retrieve until the edit is searchable."""
    import io
    import uuid
    from pipelines import ingest_job
    src = f"{CONFIG.volume_path}/sources/company"
    base = (ROOT / "data" / "sources" / "company" / "node-upgrade.md").read_text()
    token = f"FRESH-{uuid.uuid4().hex[:6].upper()}"
    t0 = time.time()
    w.files.upload(f"{src}/node-upgrade.md", io.BytesIO((base + f"\nUpdate {token}: restart the withdrawal batcher "
                                                         "after upgrading.\n").encode()), overwrite=True)
    try:
        w.files.delete(f"{src}/api-rate-limits.md")
    except Exception:
        pass
    res = ingest_job.run(w)
    t_job = time.time()
    bob = who("bob")
    found = None
    for _ in range(30):
        r = svc.retrieve(bob, RetrieveRequest(query=f"node upgrade restart batcher {token}", k=5), "web")
        if any(token in x.content for x in r.results):
            found = time.time()
            break
        time.sleep(1)
    for f in ("node-upgrade.md", "api-rate-limits.md"):   # restore sources for the next run
        w.files.upload(f"{src}/{f}", io.BytesIO((ROOT / "data" / "sources" / "company" / f).read_bytes()), overwrite=True)
    return {"token": token, "files": res["files"], "ingest_s": round(t_job - t0, 2),
            "change_to_searchable_s": round(found - t0, 2) if found else None}


app = create_app(svc, verifier, mcp_server=mcp, extra_routes=ui_routes, proxy_client=w if IN_APP else None)
