"""LoomService: the one implementation of the consumer contract. REST, MCP and SDK all call it.

Control plane (implemented in Loom): policy, guardrails, conflict resolution, promotion, audit.
Data plane: reached only through ports (Loom/ports.py).
"""
from __future__ import annotations

import collections
from concurrent.futures import ThreadPoolExecutor
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from Loom import querylog
from Loom.contract import (ConfigureRequest, ConfigureResponse, ContributeRequest, ContributeResponse,
                           PromotionDecisionResponse, RetrieveRequest, RetrieveResponse)
from Loom.control.audit import Auditor, new_request_id
from Loom.control.composer import FACT_PREFIX, Composer, _ts, fact_path, memory_actor, parse_fact_path
from Loom.control.conflict import ConflictResolver, FactCandidate, authority_label, normalize_subject
from Loom.control import guardrails
from Loom.control.promotion import Promotions
from Loom.ports import ControlPlanePort, MemoryPort, SearchPort, SessionPort, StoragePort
from Loom.spaces import policy
from Loom.spaces.model import Identity, Member, Space, SpaceRecord, space_id_for, store_name_for
from Loom.spaces.registry import SpaceRegistry


# ---------------------------------------------------------------- storage-port view (admin)
class PortCallLog:
    """Ring buffer of port calls for the admin storage-port view."""
    def __init__(self, n: int = 300):
        self.calls: collections.deque = collections.deque(maxlen=n)
        self._lock = threading.Lock()

    def add(self, port: str, backend: str, method: str, ms: float, ok: bool) -> None:
        with self._lock:
            self.calls.append({"ts": time.time(), "port": port, "backend": backend, "method": method,
                               "ms": round(ms, 2), "ok": ok})

    def recent(self, n: int = 100) -> list[dict]:
        with self._lock:
            return list(self.calls)[-n:]


class Traced:
    """Proxy that records each call on a port with the backend it hits."""
    def __init__(self, inner: Any, port: str, backend: str | Callable[[str], str], log: PortCallLog):
        self._inner, self._port, self._backend, self._log = inner, port, backend, log

    def __getattr__(self, name: str):
        attr = getattr(self._inner, name)
        if not callable(attr) or name.startswith("_"):
            return attr

        def call(*a, **kw):
            t, ok = time.perf_counter(), True
            try:
                return attr(*a, **kw)
            except Exception:
                ok = False
                raise
            finally:
                backend = self._backend(name) if callable(self._backend) else self._backend
                self._log.add(self._port, backend, name, (time.perf_counter() - t) * 1000, ok)
        return call


@dataclass
class Ports:
    control: ControlPlanePort
    storage: StoragePort
    search: SearchPort
    memory: MemoryPort
    sessions: SessionPort


class PolicyError(Exception):
    pass


@dataclass
class LoomService:
    ports: Ports
    resolver: ConflictResolver = field(default_factory=ConflictResolver)
    indexer: Callable[[str, str], Any] | None = None      # (kb_id, path) -> stats; makes files searchable now
    port_log: PortCallLog = field(default_factory=PortCallLog)
    rate_limiter: guardrails.RateLimiter = field(default_factory=guardrails.RateLimiter)

    def __post_init__(self):
        self.registry = SpaceRegistry(self.ports.control)
        self.promotions = Promotions(self.registry, self.ports.control)
        self.composer = Composer(self.registry, self.ports.search, self.ports.memory, self.ports.sessions,
                                 self.ports.control, self.resolver)

    def _audit(self, identity: Identity, surface: str) -> Auditor:
        return Auditor(self.ports.control, surface, new_request_id(), identity.principal)

    # ================================================================ configure (admin only)
    def configure(self, identity: Identity, req: ConfigureRequest, surface: str) -> ConfigureResponse:
        a = self._audit(identity, surface)
        try:
            return self._configure(identity, req, a)
        finally:
            a.flush()

    def _configure(self, identity: Identity, req: ConfigureRequest, a: Auditor) -> ConfigureResponse:
        sid = space_id_for(req.space.scope, req.space.key)
        existing = self.registry.get(sid)
        probe = existing or SpaceRecord(Space(sid, req.space.scope, req.space.name, req.space.owner))
        d = policy.decide(identity, probe, "admin")
        if not d.allowed:
            a.record("configure", "denied", sid, reason=d.reason)
            raise PolicyError(f"configure denied on {sid}: {d.reason}")
        rec = SpaceRecord(
            Space(sid, req.space.scope, req.space.name, req.space.owner, req.space.config, req.space.ephemeral),
            [Member(m.principal, m.kind, m.perm) for m in req.members],
            {"read": req.guardrails.read, "write": req.guardrails.write},
            dict(existing.bindings) if existing else {},
        )
        created = self.registry.save(rec)          # space row first: KBs reference it
        for backend in req.bindings:
            if backend in rec.bindings:
                continue
            if backend == "kb":
                kb_id = "kb-" + sid.replace(":", "-")
                self.ports.storage.create_kb(kb_id, req.space.name, sid)
                rec.bindings["kb"] = kb_id
            elif backend == "memory":
                rec.bindings["memory"] = self.ports.memory.ensure_store(store_name_for(sid), f"Loom space {sid}")
            elif backend == "sessions":
                rec.bindings["sessions"] = self.ports.sessions.ensure_store(store_name_for(sid) + "-sessions")
        self.registry.save(rec)                    # persist bindings
        if "kb" in rec.bindings:
            self.ports.search.retag_space(sid, self.access_tags(rec))
            for m in rec.members:
                self.ports.storage.add_kb_member(rec.bindings["kb"], f"{m.kind}:{m.principal}",
                                                 {"read": "reader", "write": "writer", "admin": "owner"}[m.perm])
        a.record("configure", "created" if created else "updated", sid, reason=d.reason,
                 members=[f"{m.kind}:{m.principal}:{m.perm}" for m in rec.members],
                 bindings=rec.bindings, guardrails=rec.guardrails)
        return ConfigureResponse(request_id=a.request_id, space_id=sid, created=created, bindings=rec.bindings)

    @staticmethod
    def access_tags(rec: SpaceRecord) -> list[str]:
        """Principal tags stored on chunks for RLS defense in depth (mirrors read membership)."""
        return sorted({f"{m.kind}:{m.principal}" for m in rec.members} | {f"user:{rec.space.owner_principal}"})

    def access_for_space(self, space_id: str) -> list[str]:
        rec = self.registry.get(space_id)
        return self.access_tags(rec) if rec else []

    # ================================================================ retrieve
    def retrieve(self, identity: Identity, req: RetrieveRequest, surface: str) -> RetrieveResponse:
        a = self._audit(identity, surface)
        try:
            results, trace = self.composer.compose(identity, req.query, req.hints, req.k, a, req.session_id)
        finally:
            a.flush()
        self.ports.control.write_trace({"request_id": a.request_id, "caller": identity.principal,
                                        "surface": surface, "query": req.query, "trace": trace.model_dump()})
        self._record_turn(identity, req.session_id, {"type": "message", "role": "user", "content": req.query})
        return RetrieveResponse(request_id=a.request_id, results=results, trace=trace)

    def _record_turn(self, identity: Identity, session_id: str | None, item: dict) -> None:
        """Short-term state: the caller's turns go to their personal space's managed session."""
        if not session_id:
            return
        rec = self.registry.get(f"personal:{identity.principal}")
        if rec and rec.bindings.get("sessions"):
            name = self.ports.sessions.open(rec.bindings["sessions"], identity.principal, session_id)
            self.ports.sessions.append(name, [item])

    # ================================================================ contribute
    def contribute(self, identity: Identity, req: ContributeRequest, surface: str,
                   _origin: str = "stated") -> ContributeResponse:
        a = self._audit(identity, surface)
        try:
            return self._contribute(identity, req, a, _origin)
        finally:
            a.flush()

    def _contribute(self, identity: Identity, req: ContributeRequest, a: Auditor, _origin: str) -> ContributeResponse:
        sid = req.target_space or f"personal:{identity.principal}"
        rec = self.registry.get(sid)
        if not rec:
            a.record("contribute", "rejected", sid, reason="unknown space")
            return ContributeResponse(request_id=a.request_id, status="rejected", space_id=sid, reason="unknown space")
        d = policy.decide(identity, rec, "write")
        if not d.allowed:
            a.record("contribute", "denied", sid, reason=d.reason, kind=req.kind)
            return ContributeResponse(request_id=a.request_id, status="rejected", space_id=sid, reason=d.reason)
        if req.propose_promotion_to:
            reason = self.promotions.validate_target(sid, req.propose_promotion_to)
            if reason:
                a.record("contribute", "rejected", sid, reason=reason, kind=req.kind)
                return ContributeResponse(request_id=a.request_id, status="rejected", space_id=sid, reason=reason)
        v = guardrails.check_write(req.content, req.kind, d.guardrails, sid, identity.principal, self.rate_limiter)
        if not v.allowed:
            a.record("contribute", "guardrail_blocked", sid, reason=v.reason, kind=req.kind)
            return ContributeResponse(request_id=a.request_id, status="rejected", space_id=sid, reason=v.reason)

        conflict_view = None
        if req.kind == "fact":
            subject = normalize_subject(req.subject or "note." + re.sub(r"\W+", "-", req.content.lower())[:40])
            observed = _ts(req.observed_at) if req.observed_at else datetime.now(timezone.utc)
            res = self._conflict_check(identity, rec, subject, v.content, _origin, observed)
            if res is not None:
                conflict_view = res.view()
                self.ports.control.write_conflict({"subject": res.subject, "outcome": res.outcome, "rule": res.rule,
                                                   "winner": conflict_view["winner"], "losers": conflict_view["losers"],
                                                   "rationale": res.rationale, "flags": res.flags})
                loses_shared = rec.space.scope != "personal" and (res.winner is None or res.winner.ref != "(new)")
                if res.outcome == "needs_human_review" or loses_shared:
                    why = res.rationale if res.outcome == "needs_human_review" else \
                        f"contradicts higher-precedence fact {res.winner.ref}: {res.rationale}"
                    a.record("contribute", "held_for_review", sid, subject=subject, rule=res.rule, reason=why)
                    return ContributeResponse(request_id=a.request_id, status="held_for_review", space_id=sid,
                                              reason=why, guardrail_actions=v.actions, conflict=conflict_view)
            entry = self.ports.memory.add(rec.bindings["memory"], memory_actor(rec, identity),
                                          fact_path(subject, _origin, observed), v.content,
                                          description=f"{subject}: {v.content[:120]}", session_id=req.session_id)
            ref = entry.name
        else:
            if "kb" not in rec.bindings:
                return ContributeResponse(request_id=a.request_id, status="rejected", space_id=sid,
                                          reason="space has no KB binding")
            path = req.path or f"{req.kind}s/{re.sub(r'[^a-z0-9]+', '-', req.content[:48].lower()).strip('-')}.md"
            fv = self.ports.storage.put_file(rec.bindings["kb"], path, v.content.encode(), identity.principal)
            if self.indexer:
                self.indexer(rec.bindings["kb"], path)
            ref = f"{rec.bindings['kb']}:{path}@v{fv.version}"

        status, promo_id = "stored", None
        if req.propose_promotion_to:
            promo_id = self.promotions.propose(identity, sid, req.propose_promotion_to, "fact" if req.kind == "fact" else "lesson",
                                               {"content": v.content, "subject": req.subject, "path": req.path,
                                                "source_ref": ref, "kind": req.kind})
            status = "promotion_pending"
            a.record("promotion.propose", "pending", req.propose_promotion_to, source=sid, promotion_id=promo_id)
        a.record("contribute", status, sid, kind=req.kind, ref=ref, guardrails=v.actions,
                 conflict=conflict_view["rule"] if conflict_view else None)
        self._record_turn(identity, req.session_id, {"type": "contribution", "role": "user", "kind": req.kind,
                                                     "content": v.content, "space": sid})
        return ContributeResponse(request_id=a.request_id, status=status, space_id=sid, ref=ref,
                                  guardrail_actions=v.actions, conflict=conflict_view, promotion_request_id=promo_id)

    def _conflict_check(self, identity: Identity, rec: SpaceRecord, subject: str, statement: str, origin: str,
                        observed: datetime):
        """Compare a new fact with existing facts on the same subject in spaces the caller can read."""
        readable = [r for r in self.registry.all() if policy.decide(identity, r, "read").allowed]
        cands = [FactCandidate(subject, statement, rec.space_id,
                               authority_label(rec.space.scope, "inferred" if origin == "inferred" else None),
                               observed, "(new)", "this contribution")]
        kb_spaces = [r.space_id for r in readable if r.bindings.get("kb")]
        recs = {r.space_id: r for r in readable}
        mem_recs = [r for r in readable if r.bindings.get("memory")]

        def mem_list(r):
            return r, self.ports.memory.list(r.bindings["memory"], memory_actor(r, identity),
                                             path_prefix=f"{FACT_PREFIX}{subject}/")

        # one lookup per readable store + one KB query, all in parallel
        with ThreadPoolExecutor(max_workers=1 + len(mem_recs)) as ex:
            kb_future = ex.submit(querylog.carry(self.ports.search.facts_for_subject), subject, kb_spaces,
                                  identity.principal_tags)
            mem_results = list(ex.map(querylog.carry(mem_list), mem_recs))
            kb_hits = kb_future.result()
        for r, entries in mem_results:
            for e in entries:
                parsed = parse_fact_path(e.path)
                cands.append(FactCandidate(subject, e.content, r.space_id,
                                           authority_label(r.space.scope, "inferred" if parsed and parsed[1] == "inferred" else None),
                                           (parsed and parsed[2]) or e.created_at or _ts(None), e.name,
                                           f"memory {r.bindings['memory']}"))
        for h in kb_hits:
            md = h.metadata or {}
            stmt = (md.get("facts") or {}).get(subject)
            if stmt is not None:
                cands.append(FactCandidate(subject, str(stmt), h.space_id,
                                           authority_label(recs[h.space_id].space.scope, md.get("authority")),
                                           _ts(md.get("effective_date") or h.updated_at), h.chunk_id, f"{h.path}@v{h.version}"))
        return self.resolver.resolve(subject, cands)

    # ================================================================ promotion approval (admin action)
    def decide_promotion(self, identity: Identity, request_id: str, approve: bool, surface: str) -> PromotionDecisionResponse:
        a = self._audit(identity, surface)
        try:
            return self._decide_promotion(identity, request_id, approve, a)
        finally:
            a.flush()

    def _decide_promotion(self, identity: Identity, request_id: str, approve: bool, a: Auditor) -> PromotionDecisionResponse:
        check, req = self.promotions.check_approver(identity, request_id)
        target = req["target_space"] if req else ""
        if not check.ok:
            a.record("promotion.decide", "denied", target or None, promotion_id=request_id, reason=check.reason)
            raise PolicyError(check.reason)
        if not approve:
            self.ports.control.decide_promotion(request_id, "rejected", identity.principal)
            a.record("promotion.decide", "rejected", target, promotion_id=request_id, approver=identity.principal)
            return PromotionDecisionResponse(request_id=a.request_id, promotion_id=request_id, status="rejected",
                                             target_space=target)
        rec = self.registry.get(target)
        p = req["payload"]
        if req["kind"] == "fact":
            subject = normalize_subject(p.get("subject") or "note")
            entry = self.ports.memory.add(rec.bindings["memory"], memory_actor(rec, identity),
                                          fact_path(subject, "promoted", datetime.now(timezone.utc)), p["content"],
                                          description=f"{subject}: {p['content'][:120]} (promoted from {req['source_space']})")
            ref = entry.name
        else:
            path = p.get("path") or "lessons/promoted.md"
            fv = self.ports.storage.put_file(rec.bindings["kb"], path, p["content"].encode(), identity.principal)
            if self.indexer:
                self.indexer(rec.bindings["kb"], path)
            ref = f"{rec.bindings['kb']}:{path}@v{fv.version}"
        self.ports.control.decide_promotion(request_id, "approved", identity.principal)
        a.record("promotion.decide", "approved", target, promotion_id=request_id, approver=identity.principal,
                 approver_reason=check.reason, source=req["source_space"], source_ref=p.get("source_ref"), ref=ref)
        return PromotionDecisionResponse(request_id=a.request_id, promotion_id=request_id, status="approved",
                                         target_space=target, ref=ref,
                                         reason=check.reason)
