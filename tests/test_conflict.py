from datetime import datetime, timedelta, timezone

from Loom.control.conflict import ConflictResolver, FactCandidate, authority_label

T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)


def fact(stmt, authority, days=0, space="company", subject="withdrawal_limit.retail.daily"):
    return FactCandidate(subject, stmt, space, authority, T0 + timedelta(days=days), f"ref-{stmt[:8]}")


def test_authority_beats_time_and_flags_newer_lower():
    policy = fact("Daily retail withdrawal limit is $50,000", "official_policy", days=0)
    note = fact("Daily retail withdrawal limit is $25,000", "personal", days=10, space="personal:bob")
    r = ConflictResolver().resolve("withdrawal.limit", [policy, note])
    assert r.outcome == "resolved" and r.rule == "source_authority"
    assert r.winner is policy and r.losers == [note]
    assert r.flags and r.flags[0].startswith("lower_authority_newer")


def test_time_precedence_within_equal_authority():
    old = fact("Runbook step 3: restart the signer", "team", days=0, space="team:care")
    new = fact("Runbook step 3: drain the queue before restarting the signer", "team", days=5, space="team:care")
    r = ConflictResolver().resolve("runbook.step3", [old, new])
    assert r.rule == "time_precedence" and r.winner is new and not r.flags


def test_tie_needs_human_review():
    a = fact("Escalate to compliance within 1 hour", "team", space="team:risk")
    b = fact("Escalate to compliance within 4 hours", "team", space="team:care")
    r = ConflictResolver().resolve("escalation.sla", [a, b])
    assert r.outcome == "needs_human_review" and r.rule == "tie" and r.winner is None


def test_agreement_is_not_a_conflict():
    a = fact("Limit is $50,000.", "company")
    b = fact("limit is $50,000", "personal", days=3)
    assert ConflictResolver().resolve("x", [a, b]) is None


def test_inferred_ranks_below_personal_and_custom_ranking():
    inferred = fact("Use node v2", "inferred", days=9)
    personal = fact("Use node v3", "personal", days=1)
    assert ConflictResolver().resolve("x", [inferred, personal]).winner is personal
    flipped = ConflictResolver(authority=["inferred", "personal"])
    assert flipped.resolve("x", [inferred, personal]).winner is inferred


def test_authority_label():
    assert authority_label("company", "official_policy") == "official_policy"
    assert authority_label("team", None) == "team"
    assert authority_label("company", "inferred") == "inferred"
