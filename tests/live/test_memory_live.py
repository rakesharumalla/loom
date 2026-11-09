"""Step 6/7 acceptance on live managed memory + sessions: per-space stores composed at read time,
cross-session recall, promotion with approval, conflicts with rationale."""
import uuid

import pytest

from Loom.contract import ContributeRequest, RetrieveRequest
from Loom.spaces.model import store_name_for


@pytest.fixture(scope="module")
def ids():
    from databricks.sdk import WorkspaceClient
    from config import CONFIG
    from scripts.seed import identities
    return identities(WorkspaceClient(profile=CONFIG.profile))


def memory_spaces(svc, who, ids, q):
    r = svc.retrieve(ids[who], RetrieveRequest(query=q, k=20), "job")
    return r, {x.provenance.space_id for x in r.results if x.provenance.backend == "memory"}


def test_store_per_space_bindings(live):
    svc = live[0]
    for rec in svc.registry.all():
        assert rec.bindings["memory"] == store_name_for(rec.space_id)


def test_bob_and_carol_memory_scopes(live, ids):
    svc = live[0]
    r, mem = memory_spaces(svc, "bob", ids, "withdrawal limit runbook refunds pilot BTC ETH market feed ticks borealis")
    included = {v.space_id for v in r.trace.considered if v.included}
    assert included == {"personal:bob", "project:atlas", "team:care", "company"}
    assert mem <= included and {"personal:bob", "team:care", "project:atlas"} <= mem
    r, mem = memory_spaces(svc, "carol", ids, "market data feed failover ticks borealis answers trading pairs refunds")
    included = {v.space_id for v in r.trace.considered if v.included}
    assert included == {"personal:carol", "project:borealis", "team:markets", "company"}
    assert {"personal:carol", "project:borealis", "team:markets"} <= mem and "team:care" not in mem


def test_preference_recalled_in_new_session(live, ids):
    svc = live[0]
    tag = uuid.uuid4().hex[:6]
    s1, s2 = f"live-{tag}-1", f"live-{tag}-2"
    pref = f"Bob wants incident summaries as three bullet points (pref {tag})."
    c = svc.contribute(ids["bob"], ContributeRequest(content=pref, subject=f"preference.summary_format.{tag}",
                                                     session_id=s1), "job")
    assert c.status == "stored" and c.space_id == "personal:bob"
    r = svc.retrieve(ids["bob"], RetrieveRequest(query=f"incident summaries bullet points pref {tag}", session_id=s2), "job")
    hit = next(x for x in r.results if tag in x.content and x.provenance.backend == "memory")
    assert hit.provenance.space_id == "personal:bob"
    # session state survives across requests: a fresh adapter reads both sessions' turns
    from databricks.sdk import WorkspaceClient
    from config import CONFIG
    from Loom.adapters.databricks.sessions import ManagedSessions
    fresh = ManagedSessions(WorkspaceClient(profile=CONFIG.profile))
    store = svc.registry.get("personal:bob").bindings["sessions"]
    assert any(tag in str(i.get("content")) for i in fresh.items(f"{store}/{s1}"))
    assert any(tag in str(i.get("content")) for i in fresh.items(f"{store}/{s2}"))
    # Alice never sees Bob's personal preference
    ra = svc.retrieve(ids["alice"], RetrieveRequest(query=f"incident summaries bullet points pref {tag}"), "job")
    assert not any(tag in x.content for x in ra.results)


def test_promotion_appears_in_higher_scope(live, ids):
    svc = live[0]
    tag = uuid.uuid4().hex[:6]
    fact = f"Atlas batcher pages on-call when signer queue exceeds 250 requests (rule {tag})."
    c = svc.contribute(ids["bob"], ContributeRequest(content=fact, subject=f"atlas.batcher_page.{tag}",
                                                     propose_promotion_to="project:atlas"), "job")
    assert c.status == "promotion_pending"
    q = RetrieveRequest(query=f"atlas batcher pages signer queue rule {tag}")
    assert not any(tag in x.content for x in svc.retrieve(ids["alice"], q, "job").results)
    d = svc.decide_promotion(ids["alice"], c.promotion_request_id, True, "job")
    assert d.status == "approved"
    hit = next(x for x in svc.retrieve(ids["alice"], q, "job").results if tag in x.content)
    assert hit.provenance.space_id == "project:atlas"
    audit = svc.ports.control.audit_for(d.request_id)
    assert any(r["action"] == "promotion.decide" and r["decision"] == "approved" and r["detail"]["approver"] == "alice"
               for r in audit)


def test_conflicts_live(live, ids):
    svc = live[0]
    r = svc.retrieve(ids["bob"], RetrieveRequest(query="daily withdrawal limit retail customers", k=10), "job")
    c = next(c for c in r.trace.conflicts if c["subject"] == "withdrawal_limit.retail.daily")
    assert c["rule"] == "source_authority" and c["winner"]["authority"] == "official_policy" and c["flags"]
    r = svc.retrieve(ids["bob"], RetrieveRequest(query="card refunds post business days", k=10), "job")
    c = next(c for c in r.trace.conflicts if c["subject"] == "refund.card.sla")
    assert c["rule"] == "time_precedence"
    r = svc.retrieve(ids["alice"], RetrieveRequest(query="withdrawals v2 batches on-chain withdrawals every minutes", k=10), "job")
    c = next(c for c in r.trace.conflicts if c["subject"] == "atlas.batch_window")
    assert c["outcome"] == "needs_human_review"
