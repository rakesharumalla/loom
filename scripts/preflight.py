"""Preflight: PASS/FAIL for everything the demo depends on. Run: uv run python scripts/preflight.py"""
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.adapters.databricks.lakebase_conn import LakebaseConnector  # noqa: E402

w = WorkspaceClient(profile=CONFIG.profile)
results = []


def check(name):
    def deco(fn):
        try:
            ok, detail = fn()
        except Exception as e:  # report, keep going
            ok, detail = False, f"{type(e).__name__}: {str(e)[:160]}"
        results.append(ok)
        print(f"{'PASS' if ok else 'FAIL'}  {name:34} {detail}")
        return fn
    return deco


conn = LakebaseConnector(w)


def q(sql, params=()):
    with conn.tx() as c:
        if "loom.chunks" in sql:   # chunks are only readable through the RLS roles
            c.execute("SET LOCAL ROLE loom_writer")
            c.execute("SELECT set_config('loom.writer','on',true)")
        return c.execute(sql, params).fetchall()


@check("Lakebase Search extensions")
def _():
    ext = {r[0] for r in q("SELECT extname FROM pg_extension")}
    need = {"lakebase_vector", "lakebase_text"}
    return need <= ext, ", ".join(sorted(need & ext))


@check("Search indexes (ann + bm25)")
def _():
    idx = {r[0] for r in q("SELECT indexname FROM pg_indexes WHERE schemaname='loom' AND tablename='chunks'")}
    return {"chunks_embedding_ann", "chunks_tsv_bm25"} <= idx, ", ".join(sorted(i for i in idx if "ann" in i or "bm25" in i))


@check("RLS roles (loom_reader/loom_writer)")
def _():
    roles = {r[0] for r in q("SELECT rolname FROM pg_roles WHERE rolname LIKE 'loom_%%' AND NOT rolbypassrls")}
    return {"loom_reader", "loom_writer"} <= roles, ", ".join(sorted(roles))


@check("Groups + persona identities")
def _():
    missing = [g for g in CONFIG.groups if not list(w.groups.list(filter=f'displayName eq "{g}"'))]
    sps = {p: list(w.service_principals.list(filter=f'displayName eq "{s["sp"]}"')) for p, s in CONFIG.personas.items()}
    missing += [p for p, v in sps.items() if not v]
    return not missing, f"{len(CONFIG.groups)} groups, {len(sps)} SPs" + (f"; missing {missing}" if missing else "")


@check("Model endpoints READY")
def _():
    st = {n: str(w.serving_endpoints.get(n).state.ready) for n in (CONFIG.embedding_endpoint, CONFIG.llm_endpoint)}
    return all("READY" in v for v in st.values()), ", ".join(f"{k}={v.split('.')[-1]}" for k, v in st.items())


@check("Data loaded (files, chunks)")
def _():
    files = q("SELECT count(*) FROM loom.files WHERE NOT deleted")[0][0]
    chunks = q("SELECT count(*) FROM loom.chunks")[0][0]
    return files >= 960 and chunks >= 960, f"{files} files, {chunks} chunks"


@check("Spaces and bindings")
def _():
    rows = q("SELECT s.space_id, array_agg(b.backend ORDER BY b.backend) FROM loom.spaces s "
             "LEFT JOIN loom.space_bindings b USING (space_id) GROUP BY 1")
    bad = [s for s, b in rows if "memory" not in (b or []) or "kb" not in (b or [])]
    return len(rows) >= 10 and not bad, f"{len(rows)} spaces" + (f"; unbound {bad}" if bad else "")


@check("Memory + session stores")
def _():
    stores = {s.name.split("/")[-1] for s in w.agent_kit.list_memory_stores()}
    need = {r[0] for r in q("SELECT ref FROM loom.space_bindings WHERE backend='memory'")}
    sess = {s.name.split("/")[-1] for s in w.agent_kit.list_session_stores()}
    need_s = {r[0] for r in q("SELECT ref FROM loom.space_bindings WHERE backend='sessions'")}
    miss = (need - stores) | (need_s - sess)
    return not miss, f"{len(need)} memory, {len(need_s)} session stores" + (f"; missing {miss}" if miss else "")


@check("Lakebase CDF -> Delta streaming")
def _():
    rows = q("SELECT status, count(*) FROM wal2delta.tables GROUP BY 1")
    d = dict(rows)
    return d.get("STREAMING", 0) >= 8, ", ".join(f"{k}={v}" for k, v in d.items())


@check("App deployed")
def _():
    a = w.apps.get("loom-demo")
    return "RUNNING" in str(a.app_status.state), f"{a.url} ({a.app_status.state.value})"


@check("MCP server (tools on app)")
def _():
    from Loom.identity import PersonaTokens
    from Loom.mcp_client import list_tools_async
    url = w.apps.get("loom-demo").url.rstrip("/")
    tools = asyncio.run(list_tools_async(f"{url}/mcp/", PersonaTokens(w, CONFIG.secret_scope).token("bob")))
    return {"retrieve", "contribute", "configure"} <= set(tools), ", ".join(sorted(tools))


@check("Bundle jobs")
def _():
    names = {j.settings.name for j in w.jobs.list() if j.settings.name and "loom-" in j.settings.name}
    need = {"loom-ingest", "loom-distill", "loom-bench", "loom-eval"}
    return all(any(n.endswith(x) for n in names) for x in need), ", ".join(sorted(names))


print(f"\n{sum(results)}/{len(results)} checks passed")
sys.exit(0 if all(results) else 1)
