"""Conflict resolution for contradictory facts. Implemented in Loom.

Facts about the same normalized `subject` with different statements conflict. Resolution order:
  1. source authority (configurable ranking, default below)
  2. time precedence (newer wins within equal authority)
  3. tie (same authority, timestamps within tie_window) -> needs_human_review
A winner from (1) that is older than a lower-authority candidate is flagged "lower_authority_newer".
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime

DEFAULT_AUTHORITY = ["official_policy", "company", "team", "project", "personal", "inferred"]


@dataclass(frozen=True)
class FactCandidate:
    subject: str
    statement: str
    space_id: str
    authority: str          # one of the ranking labels
    observed_at: datetime
    ref: str                # memory entry name or file path@version
    source: str = ""        # human-readable provenance

    def view(self) -> dict:
        d = asdict(self)
        d["observed_at"] = self.observed_at.isoformat()
        return d


@dataclass
class Resolution:
    subject: str
    outcome: str                       # resolved | needs_human_review
    rule: str                          # source_authority | time_precedence | tie
    winner: FactCandidate | None
    losers: list[FactCandidate]
    rationale: str
    flags: list[str] = field(default_factory=list)

    def view(self) -> dict:
        return {"subject": self.subject, "outcome": self.outcome, "rule": self.rule,
                "winner": self.winner.view() if self.winner else None,
                "losers": [c.view() for c in self.losers], "rationale": self.rationale, "flags": self.flags}


def normalize_subject(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", ".", s.lower()).strip(".")


def _norm(statement: str) -> str:
    return re.sub(r"\s+", " ", statement.lower()).strip(" .")


def authority_label(scope: str, source_kind: str | None) -> str:
    """Map provenance to a ranking label: official docs and inferred facts override scope."""
    if source_kind in ("official_policy", "inferred"):
        return source_kind
    return scope


class ConflictResolver:
    def __init__(self, authority: list[str] | None = None, tie_window_s: float = 1.0):
        self.authority = authority or DEFAULT_AUTHORITY
        self.tie_window_s = tie_window_s

    def rank(self, label: str) -> int:
        return self.authority.index(label) if label in self.authority else len(self.authority)

    def resolve_all(self, candidates: list[FactCandidate]) -> list[Resolution]:
        groups: dict[str, list[FactCandidate]] = {}
        for c in candidates:
            groups.setdefault(normalize_subject(c.subject), []).append(c)
        out = []
        for subject, cs in sorted(groups.items()):
            r = self.resolve(subject, cs)
            if r:
                out.append(r)
        return out

    def resolve(self, subject: str, cs: list[FactCandidate]) -> Resolution | None:
        if len({_norm(c.statement) for c in cs}) < 2:
            return None  # agreement (or a single fact): nothing to resolve
        best_rank = min(self.rank(c.authority) for c in cs)
        top = sorted([c for c in cs if self.rank(c.authority) == best_rank],
                     key=lambda c: c.observed_at, reverse=True)
        top_label = self.authority[best_rank] if best_rank < len(self.authority) else "unranked"

        if len({_norm(c.statement) for c in top}) == 1:
            winner = top[0]
            rule = "source_authority"
            rationale = (f"'{top_label}' outranks " +
                         ", ".join(sorted({c.authority for c in cs if c not in top})) +
                         f" in ranking {' > '.join(self.authority)}")
        else:
            newest, second = top[0], next(c for c in top[1:] if _norm(c.statement) != _norm(top[0].statement))
            gap = (newest.observed_at - second.observed_at).total_seconds()
            if gap <= self.tie_window_s:
                return Resolution(subject, "needs_human_review", "tie", None, cs,
                                  f"{len(top)} '{top_label}' sources disagree with timestamps within "
                                  f"{self.tie_window_s}s; escalated to human review")
            winner, rule = newest, "time_precedence"
            rationale = (f"equal authority '{top_label}'; newer statement wins "
                         f"({newest.observed_at.isoformat()} vs {second.observed_at.isoformat()})")

        losers = [c for c in cs if c is not winner]
        flags = []
        newer_lower = [c for c in losers if self.rank(c.authority) > best_rank and c.observed_at > winner.observed_at]
        if newer_lower:
            flags.append("lower_authority_newer: " + ", ".join(f"{c.authority}@{c.space_id}" for c in newer_lower))
        return Resolution(subject, "resolved", rule, winner, losers, rationale, flags)
