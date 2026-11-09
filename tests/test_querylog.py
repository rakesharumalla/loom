import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from Loom import querylog
from Loom.adapters.databricks.capture import record_rest
from tests.conftest import IDS, build_service


def test_noop_outside_capture():
    querylog.record("sql", "Lakebase Postgres", "SELECT 1", time.perf_counter())
    querylog.note("x")
    assert not querylog.active()


def test_carry_shares_capture_with_worker_threads():
    def work(i):
        querylog.record("sql", "Lakebase Postgres", f"SELECT {i}", time.perf_counter())
    with querylog.capture() as q:
        with ThreadPoolExecutor(4) as ex:
            list(ex.map(querylog.carry(work), range(4)))
            ex.submit(work, 99).result()          # not carried: not captured
    assert sorted(c["text"] for c in q.calls) == [f"SELECT {i}" for i in range(4)]


def test_note_dedupe_and_unless():
    with querylog.capture() as q:
        querylog.note("cached")
        querylog.note("cached")
        querylog.record("sql", "Lakebase Postgres", "SELECT * FROM loom.spaces", time.perf_counter())
        querylog.note("spaces cached", unless="FROM loom.spaces")
    assert [c["text"] for c in q.calls] == ["cached", "SELECT * FROM loom.spaces"]


def test_shorten_vectors_and_long_text():
    vec = "[" + ",".join(f"{i / 1000:.6f}" for i in range(1024)) + "]"
    assert querylog.shorten({"q": vec})["q"].endswith("(1024 floats)")
    assert querylog.shorten([0.1] * 1024).endswith("(1024 floats)")
    assert querylog.shorten("x" * 1000).endswith("(1000 chars)")
    js = '[{"subject": "a", "statement": "' + "y" * 400 + '"}]'
    assert querylog.shorten(js).endswith(f"({len(js)} chars)")   # JSON array, not a vector
    assert querylog.shorten(("a", 1)) == ("a", 1)


def test_record_rest_wraps_api_client_once():
    seen = []
    api = SimpleNamespace(do=lambda method, path=None, **kw: seen.append((method, path)) or {"ok": True})
    w = SimpleNamespace(api_client=api)
    record_rest(w)
    record_rest(w)
    assert w.api_client.do("GET", "/x") == {"ok": True}              # outside a capture: passthrough
    with querylog.capture() as q:
        w.api_client.do("POST", "/api/2.0/agents/memory-stores/loom-company/entries:search",
                        body={"actor_id": "space", "query": "limits", "limit": 8})
        w.api_client.do("GET", "/api/2.0/agents/memory-stores/loom-company/entries",
                        query={"actor_id": "space", "path_prefix": "/facts/a/"})
    assert len(seen) == 3
    assert [c["target"] for c in q.calls] == ["Managed memory", "Managed memory"]
    assert '"query": "limits"' in q.calls[0]["text"]
    assert q.calls[1]["text"].endswith("?actor_id=space&path_prefix=%2Ffacts%2Fa%2F")


def test_retrieve_with_fakes_still_works_under_capture():
    svc = build_service()
    from Loom.contract import RetrieveRequest
    with querylog.capture():
        r = svc.retrieve(IDS["bob"], RetrieveRequest(query="daily withdrawal limit", k=4), "web")
    assert r.results
