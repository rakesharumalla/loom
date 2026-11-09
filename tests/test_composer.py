import pytest

from Loom.contract import ConfigureRequest, ContributeRequest, Hints, RetrieveRequest
from Loom.service import PolicyError


def included(resp):
    return {v.space_id for v in resp.trace.considered if v.included}


def excluded(resp):
    return {v.space_id: v.reason for v in resp.trace.considered if not v.included}


def test_bob_and_carol_scopes(svc, ids):
    bob = svc.retrieve(ids["bob"], RetrieveRequest(query="withdrawal limit"), "rest")
    assert included(bob) == {"personal:bob", "project:atlas", "team:care", "company"}
    assert {"project:borealis", "team:markets", "team:risk"} <= set(excluded(bob))
    assert all(r.startswith("deny by default") for r in excluded(bob).values())
    carol = svc.retrieve(ids["carol"], RetrieveRequest(query="withdrawal limit"), "rest")
    assert included(carol) == {"personal:carol", "project:borealis", "team:markets", "company"}
    # other users' personal spaces are never even candidates
    assert not any(v.space_id.startswith("personal:") and v.space_id != "personal:carol"
                   for v in carol.trace.considered)


def test_alice_and_bob_same_query_differ_and_audit(svc, ids):
    q = RetrieveRequest(query="escalate to compliance within one hour sanctions", k=8)
    a = svc.retrieve(ids["alice"], q, "rest")
    b = svc.retrieve(ids["bob"], q, "rest")
    a_spaces = {r.provenance.space_id for r in a.results}
    b_spaces = {r.provenance.space_id for r in b.results}
    assert "team:risk" in a_spaces and "team:risk" not in b_spaces
    assert a_spaces != b_spaces
    rows = svc.ports.control.audit_for(b.request_id)
    assert {r["action"] for r in rows} >= {"retrieve", "retrieve.space"}
    assert any(r["space_id"] == "team:risk" and r["decision"] == "excluded" for r in rows)


def test_read_guardrails_redact(svc, ids):
    r = svc.retrieve(ids["alice"], RetrieveRequest(query="AML alert triage training example customer"), "rest")
    risk = [x for x in r.results if x.provenance.space_id == "team:risk"]
    assert risk and all("ACCT-" not in x.content for x in risk)
    assert any("redacted" in g for v in r.trace.considered if v.space_id == "team:risk" for g in v.guardrails_applied)


def test_hint_narrows_projects(svc, ids):
    r = svc.retrieve(ids["bob"], RetrieveRequest(query="batch", hints=Hints(project="borealis")), "rest")
    assert excluded(r)["project:atlas"] == "not selected: hint project=borealis"
    assert excluded(r)["project:borealis"].startswith("deny by default")


def test_conflict_authority_over_time_in_retrieve(svc, ids):
    r = svc.retrieve(ids["bob"], RetrieveRequest(query="daily withdrawal limit retail customers", k=10), "rest")
    c = next(c for c in r.trace.conflicts if c["subject"] == "withdrawal_limit.retail.daily")
    assert c["rule"] == "source_authority" and c["winner"]["authority"] == "official_policy"
    assert c["flags"] and "lower_authority_newer" in c["flags"][0]
    note = next(x for x in r.results if x.provenance.backend == "memory" and "$25,000" in x.content)
    assert note.superseded_by == c["winner"]["ref"]


def test_tie_needs_review(svc, ids):
    r = svc.retrieve(ids["alice"], RetrieveRequest(query="withdrawals v2 batches on-chain withdrawals every minutes"), "rest")
    c = next(c for c in r.trace.conflicts if c["subject"] == "atlas.batch_window")
    assert c["outcome"] == "needs_human_review" and c["rule"] == "tie"


def test_time_precedence_in_team_memory(svc, ids):
    r = svc.retrieve(ids["bob"], RetrieveRequest(query="card refunds post business days"), "rest")
    c = next(c for c in r.trace.conflicts if c["subject"] == "refund.card.sla")
    assert c["rule"] == "time_precedence" and "3 business days" in c["winner"]["statement"]


def test_configure_admin_only(svc, ids):
    req = ConfigureRequest(space={"scope": "team", "key": "care", "name": "x", "owner": "bob"})
    with pytest.raises(PolicyError):
        svc.configure(ids["carol"], req, "rest")


def test_contribute_guardrail_and_policy(svc, ids):
    r = svc.contribute(ids["bob"], ContributeRequest(content="api key: abc123", subject="x"), "rest")
    assert r.status == "rejected" and "blocked" in r.reason
    r = svc.contribute(ids["bob"], ContributeRequest(content="hi", target_space="team:markets", subject="x"), "rest")
    assert r.status == "rejected" and r.reason.startswith("deny by default")


def test_shared_space_contradiction_held_for_review(svc, ids):
    r = svc.contribute(ids["alice"], ContributeRequest(content="The daily retail withdrawal limit is $10,000.",
                                                       subject="withdrawal_limit.retail.daily",
                                                       target_space="project:atlas"), "rest")
    assert r.status == "held_for_review" and "official_policy" in r.reason


def test_promotion_requires_other_approver_with_write(svc, ids):
    r = svc.contribute(ids["bob"], ContributeRequest(
        content="Atlas batcher alerts fire when the signer queue exceeds 200 requests.",
        subject="atlas.batcher_alert", propose_promotion_to="project:atlas"), "rest")
    assert r.status == "promotion_pending" and r.space_id == "personal:bob"
    with pytest.raises(PolicyError, match="own request"):
        svc.decide_promotion(ids["bob"], r.promotion_request_id, True, "rest")
    with pytest.raises(PolicyError, match="lacks write"):
        svc.decide_promotion(ids["carol"], r.promotion_request_id, True, "rest")
    before = svc.retrieve(ids["alice"], RetrieveRequest(query="atlas batcher signer queue alerts"), "rest")
    assert not any("200 requests" in x.content for x in before.results)
    d = svc.decide_promotion(ids["alice"], r.promotion_request_id, True, "rest")
    assert d.status == "approved"
    after = svc.retrieve(ids["alice"], RetrieveRequest(query="atlas batcher signer queue alerts"), "rest")
    hit = next(x for x in after.results if "200 requests" in x.content)
    assert hit.provenance.space_id == "project:atlas"
    rows = svc.ports.control.audit_for(d.request_id)
    assert rows[-1]["decision"] == "approved" and rows[-1]["detail"]["approver"] == "alice"


def test_same_scope_promotion_rejected_before_any_write(svc, ids):
    before = len(svc.ports.memory.stores["loom-project-atlas"])
    r = svc.contribute(ids["alice"], ContributeRequest(content="Atlas note", subject="atlas.note", target_space="project:atlas",
                                                       propose_promotion_to="project:atlas"), "web")
    assert r.status == "rejected" and "higher scope" in r.reason
    assert len(svc.ports.memory.stores["loom-project-atlas"]) == before
