from Loom.spaces import policy
from Loom.spaces.model import Identity, Member, Space, SpaceRecord

BOB = Identity("bob", "app-bob", ("loom-support",))
CAROL = Identity("carol", "app-carol", ("loom-trading-ops",))
ADMIN = Identity("admin", "app-admin", (), is_platform_admin=True)


def rec(members, owner="admin", guardrails=None):
    return SpaceRecord(Space("team:care", "team", "Care", owner), members,
                       guardrails or {"read": {"max_items": 3}, "write": {}})


def test_direct_read_allows_read_only():
    r = rec([Member("bob", "user", "read")])
    assert policy.decide(BOB, r, "read").allowed
    d = policy.decide(BOB, r, "write")
    assert not d.allowed and "write required" in d.reason


def test_write_implies_read():
    r = rec([Member("bob", "user", "write")])
    assert policy.decide(BOB, r, "read").allowed
    assert policy.decide(BOB, r, "write").allowed
    assert not policy.decide(BOB, r, "admin").allowed


def test_admin_and_owner():
    assert policy.decide(BOB, rec([Member("bob", "user", "admin")]), "admin").allowed
    owned = rec([], owner="bob")
    assert policy.decide(BOB, owned, "admin").reason == "admin via owner"


def test_group_inheritance_takes_highest_grant():
    r = rec([Member("bob", "user", "read"), Member("loom-support", "group", "write")])
    d = policy.decide(BOB, r, "write")
    assert d.allowed and d.reason == "write via group:loom-support"


def test_deny_by_default():
    r = rec([Member("loom-support", "group", "read")])
    d = policy.decide(CAROL, r, "read")
    assert not d.allowed and d.reason.startswith("deny by default")


def test_platform_admin_configures_but_cannot_read():
    r = rec([Member("bob", "user", "read")], owner="bob")
    assert policy.decide(ADMIN, r, "admin").allowed
    assert not policy.decide(ADMIN, r, "read").allowed


def test_read_decision_carries_read_guardrails():
    d = policy.decide(BOB, rec([Member("bob", "user", "read")]), "read")
    assert d.guardrails == {"max_items": 3}
