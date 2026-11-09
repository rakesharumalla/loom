"""Step 4/5 acceptance on live data: sample questions find their sources; composition differs by caller;
RLS hides rows when the facade forgets its filter."""
import pytest
import yaml

from Loom.contract import RetrieveRequest
from Loom.seed import DATA


@pytest.fixture(scope="module")
def ids():
    from databricks.sdk import WorkspaceClient
    from scripts.seed import identities
    from config import CONFIG
    return identities(WorkspaceClient(profile=CONFIG.profile))


QUESTIONS = yaml.safe_load((DATA / "sample_questions.yaml").read_text())["questions"]


@pytest.mark.parametrize("q", QUESTIONS, ids=[q["q"][:40] for q in QUESTIONS])
def test_sample_question_finds_source(live, ids, q):
    svc = live[0]
    r = svc.retrieve(ids[q["persona"]], RetrieveRequest(query=q["q"], k=5), "job")
    got = [(x.provenance.space_id, x.provenance.path) for x in r.results if x.provenance.backend == "kb"]
    assert (q["expect"]["space"], q["expect"]["path"]) in got, got


def test_alice_and_bob_compose_differently(live, ids):
    svc = live[0]
    q = RetrieveRequest(query="escalate potential sanctions hits to compliance", k=8)
    a, b = svc.retrieve(ids["alice"], q, "job"), svc.retrieve(ids["bob"], q, "job")
    inc = lambda r: {v.space_id for v in r.trace.considered if v.included}
    assert "team:risk" in inc(a) and "team:risk" not in inc(b)
    assert "team:care" in inc(b) and "team:care" not in inc(a)
    assert any(x.provenance.space_id == "team:risk" for x in a.results)
    assert not any(x.provenance.space_id == "team:risk" for x in b.results)
    rows = svc.ports.control.audit_for(b.request_id)
    assert any(r["space_id"] == "team:risk" and r["decision"] == "excluded" for r in rows)


def test_rls_defense_in_depth(live):
    conn = live[2]["conn"]
    with conn.tx() as c:   # a buggy query with NO access filter, caller = bob
        c.execute("SET LOCAL ROLE loom_reader")
        c.execute("SELECT set_config('loom.caller_principals', 'user:bob,group:loom-support', true)")
        spaces = {r[0] for r in c.execute("SELECT DISTINCT space_id FROM loom.chunks")}
    assert "team:risk" not in spaces and "project:borealis" not in spaces and "company" in spaces
    with conn.tx() as c:   # no caller set at all -> nothing
        c.execute("SET LOCAL ROLE loom_reader")
        assert c.execute("SELECT count(*) FROM loom.chunks").fetchone()[0] == 0
