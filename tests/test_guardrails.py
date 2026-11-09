from Loom.control import guardrails as g

ACCT = {"pattern": r"\bACCT-\d{6}\b", "label": "ACCOUNT"}


def test_read_redaction_and_cap():
    items = [{"content": f"customer ACCT-12345{i} reported delay"} for i in range(4)]
    out, actions = g.apply_read(items, {"redact": [ACCT], "max_items": 2})
    assert len(out) == 2
    assert all("[REDACTED:ACCOUNT]" in it["content"] and "ACCT-" not in it["content"] for it in out)
    assert "capped 4 -> 2 items" in actions


def test_write_redacts_and_allows():
    v = g.check_write("Bob prefers email. ACCT-000123", "fact", {"redact": [ACCT]}, "personal:bob", "bob")
    assert v.allowed and "ACCT-" not in v.content and v.actions == ["redacted 1x ACCOUNT"]


def test_write_kind_allowlist_and_size_and_block():
    assert not g.check_write("x", "file", {"allowed_kinds": ["fact"]}, "s", "c").allowed
    assert not g.check_write("x" * 10, "fact", {"max_chars": 5}, "s", "c").allowed
    v = g.check_write("seed phrase: abandon abandon", "fact", {"block_patterns": [r"(?i)seed phrase"]}, "s", "c")
    assert not v.allowed and "blocked" in v.reason


def test_rate_limit_window():
    lim = g.RateLimiter()
    rules = {"rate_limit_per_minute": 2}
    assert g.check_write("a", "fact", rules, "s", "c", lim, now=0).allowed
    assert g.check_write("b", "fact", rules, "s", "c", lim, now=1).allowed
    assert not g.check_write("c", "fact", rules, "s", "c", lim, now=2).allowed
    assert g.check_write("d", "fact", rules, "s", "c", lim, now=62).allowed
