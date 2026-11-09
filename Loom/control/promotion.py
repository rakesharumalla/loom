"""Promotion: move a contribution or distilled lesson to a higher scope, only with approval.

Implemented in Loom (demo-grade). A request is applied only when an approver holding write on the
target space approves; the proposer cannot approve their own request. The applied entry keeps
provenance back to the source; this is explicit publication, not read-time composition.
"""
from __future__ import annotations

from dataclasses import dataclass

from Loom.ports import ControlPlanePort
from Loom.spaces import policy
from Loom.spaces.model import SCOPES, Identity
from Loom.spaces.registry import SpaceRegistry


@dataclass
class PromotionCheck:
    ok: bool
    reason: str


class Promotions:
    def __init__(self, registry: SpaceRegistry, store: ControlPlanePort):
        self.registry = registry
        self.store = store

    def validate_target(self, source_space: str, target_space: str) -> str | None:
        """None if the promotion is allowed in principle, else the reason it is not."""
        src, tgt = self.registry.get(source_space), self.registry.get(target_space)
        if not tgt or not src:
            return f"unknown space {target_space if not tgt else source_space}"
        if SCOPES.index(tgt.space.scope) <= SCOPES.index(src.space.scope):
            return f"promotion must go to a higher scope ({src.space_id} is {src.space.scope}, {tgt.space_id} is {tgt.space.scope})"
        return None

    def propose(self, proposer: Identity, source_space: str, target_space: str, kind: str, payload: dict) -> str:
        reason = self.validate_target(source_space, target_space)
        if reason:
            raise ValueError(reason)
        return self.store.create_promotion({"proposer": proposer.principal, "source_space": source_space,
                                            "target_space": target_space, "kind": kind, "payload": payload})

    def check_approver(self, approver: Identity, request_id: str) -> tuple[PromotionCheck, dict | None]:
        req = self.store.get_promotion(request_id)
        if not req:
            return PromotionCheck(False, "unknown promotion request"), None
        if req["status"] != "pending":
            return PromotionCheck(False, f"already {req['status']}"), req
        if req["proposer"] == approver.principal:
            return PromotionCheck(False, "proposer cannot approve their own request"), req
        tgt = self.registry.get(req["target_space"])
        d = policy.decide(approver, tgt, "write") if tgt else None
        if not d or not d.allowed:
            return PromotionCheck(False, f"approver lacks write on {req['target_space']}: "
                                         f"{d.reason if d else 'space missing'}"), req
        return PromotionCheck(True, d.reason), req
