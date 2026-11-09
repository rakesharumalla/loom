"""Per-space guardrails, applied by Loom before anything reaches a consumer or a store.

Rule shape (space_guardrails.rules):
  read:  {"redact": [{"pattern": "...", "label": "ACCOUNT"}], "max_items": 5}
  write: {"redact": [...], "allowed_kinds": ["fact","lesson","file"], "max_chars": 4000,
          "block_patterns": ["..."], "rate_limit_per_minute": 10}
Implemented in Loom. The rate limiter is in-process (demo-grade); production would use a shared store.
"""
from __future__ import annotations

import re
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field


@dataclass
class WriteVerdict:
    allowed: bool
    content: str
    actions: list[str] = field(default_factory=list)
    reason: str = "ok"


def redact(text: str, rules: list[dict]) -> tuple[str, list[str]]:
    actions = []
    for r in rules or []:
        text, n = re.subn(r["pattern"], f"[REDACTED:{r.get('label', 'PII')}]", text)
        if n:
            actions.append(f"redacted {n}x {r.get('label', 'PII')}")
    return text, actions


def apply_read(items: list[dict], rules: dict) -> tuple[list[dict], list[str]]:
    """Redact item content and cap the number of items from this space."""
    actions: list[str] = []
    out = []
    for it in items:
        content, acts = redact(it["content"], rules.get("redact", []))
        out.append({**it, "content": content, "redactions": it.get("redactions", []) + acts})
        actions += acts
    cap = rules.get("max_items")
    if cap is not None and len(out) > cap:
        actions.append(f"capped {len(out)} -> {cap} items")
        out = out[:cap]
    return out, actions


class RateLimiter:
    def __init__(self):
        self._hits: dict[tuple[str, str], deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key: tuple[str, str], per_minute: int, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        with self._lock:
            q = self._hits[key]
            while q and now - q[0] > 60:
                q.popleft()
            if len(q) >= per_minute:
                return False
            q.append(now)
            return True


def check_write(content: str, kind: str, rules: dict, space_id: str, caller: str,
                limiter: RateLimiter | None = None, now: float | None = None) -> WriteVerdict:
    kinds = rules.get("allowed_kinds")
    if kinds is not None and kind not in kinds:
        return WriteVerdict(False, content, [], f"kind '{kind}' not allowed in {space_id} (allowed: {kinds})")
    max_chars = rules.get("max_chars")
    if max_chars is not None and len(content) > max_chars:
        return WriteVerdict(False, content, [], f"content {len(content)} chars exceeds max_chars {max_chars}")
    for p in rules.get("block_patterns", []):
        if re.search(p, content):
            return WriteVerdict(False, content, [], f"blocked by pattern {p!r}")
    rpm = rules.get("rate_limit_per_minute")
    if rpm is not None and limiter is not None and not limiter.allow((space_id, caller), rpm, now):
        return WriteVerdict(False, content, [], f"rate limit {rpm}/min exceeded")
    content, acts = redact(content, rules.get("redact", []))
    return WriteVerdict(True, content, acts)
