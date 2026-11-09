"""The single authority for "may principal P do X on space S, and under which guardrails".

Implemented in Loom. Storage-level controls (Postgres RLS, UC volume grants, memory-store ACLs) are
defense in depth only; no other module makes access decisions.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from Loom.spaces.model import PERM_RANK, Identity, SpaceRecord

Action = Literal["read", "write", "admin"]


@dataclass(frozen=True)
class Decision:
    allowed: bool
    action: Action
    space_id: str
    effective_perm: str | None
    reason: str
    guardrails: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {"allowed": self.allowed, "action": self.action, "space_id": self.space_id,
                "effective_perm": self.effective_perm, "reason": self.reason}


def effective_perm(identity: Identity, rec: SpaceRecord) -> tuple[str | None, str]:
    """Highest permission from owner, direct user grants, and group grants. Returns (perm, via)."""
    best: tuple[int, str | None, str] = (0, None, "no membership")
    if rec.space.owner_principal == identity.principal:
        best = (PERM_RANK["admin"], "admin", "owner")
    for m in rec.members:
        hit = (m.kind == "user" and m.principal == identity.principal) or \
              (m.kind == "group" and m.principal in identity.groups)
        if hit and PERM_RANK[m.perm] > best[0]:
            best = (PERM_RANK[m.perm], m.perm, f"{m.kind}:{m.principal}")
    return best[1], best[2]


def decide(identity: Identity, rec: SpaceRecord, action: Action) -> Decision:
    perm, via = effective_perm(identity, rec)
    if action == "admin" and identity.is_platform_admin and perm != "admin":
        # Platform admins configure spaces; this grants configure rights, not read access.
        return Decision(True, action, rec.space_id, perm, "platform admin may configure")
    if perm is None:
        return Decision(False, action, rec.space_id, None, "deny by default: no membership")
    if PERM_RANK[perm] < PERM_RANK[action]:
        return Decision(False, action, rec.space_id, perm, f"has {perm} via {via}; {action} required")
    rules = rec.guardrails.get(action, {}) if action in ("read", "write") else {}
    return Decision(True, action, rec.space_id, perm, f"{perm} via {via}", rules)
