"""compose(caller, task, hints): read-time composition across spaces. Implemented in Loom.

  1. identity comes from the verified token (resolved by the surface, never from a model)
  2. candidates: own personal space, project/team spaces (narrowed by hints), company
  3. authorize read per space via spaces/policy.py; excluded spaces keep their reason
  4. fan out to each included space's bound backends (KB search is one pooled, filtered query)
  5. apply each space's read guardrails to that space's items
  6. merge with RRF, dedupe, resolve conflicting facts across spaces
  7. audit + composition trace
Content is never copied between spaces: results are references into each space's own backend.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timezone

from Loom.contract import (CompositionTrace, Hints, Provenance, ResultItem, ScoreBreakdown,
                           SpaceDecisionView)
from Loom.control import guardrails
from Loom.control.audit import Auditor
from Loom.control.conflict import ConflictResolver, FactCandidate, authority_label
from Loom.control.fusion import RRF_K, rrf
from Loom import querylog
from Loom.ports import ControlPlanePort, MemoryPort, SearchPort, SessionPort
from Loom.spaces import policy
from Loom.spaces.model import SHARED_ACTOR, Identity, SpaceRecord
from Loom.spaces.registry import SpaceRegistry

FACT_PREFIX = "/facts/"  # managed memory requires absolute paths


def memory_actor(rec: SpaceRecord, identity: Identity) -> str:
    """actor_id is set here from the verified identity; it partitions, it does not authorize."""
    return identity.principal if rec.space.scope == "personal" else SHARED_ACTOR


TS_FMT = "%Y%m%dT%H%M%S%f"


def fact_path(subject: str, origin: str, observed_at: datetime) -> str:
    """Observed time lives in the path: managed memory sets create_time server-side."""
    return f"{FACT_PREFIX}{subject}/{origin}-{observed_at.astimezone(timezone.utc).strftime(TS_FMT)}"


def parse_fact_path(path: str) -> tuple[str, str, datetime | None] | None:
    """facts/{subject}/{origin}-{ts} -> (subject, origin, observed_at)"""
    if not path.startswith(FACT_PREFIX):
        return None
    parts = path[len(FACT_PREFIX):].split("/")
    if len(parts) < 2:
        return None
    origin, _, ts = parts[1].partition("-")
    try:
        observed = datetime.strptime(ts, TS_FMT).replace(tzinfo=timezone.utc)
    except ValueError:
        observed = None
    return parts[0], origin, observed


@dataclass
class Composer:
    registry: SpaceRegistry
    search: SearchPort
    memory: MemoryPort
    sessions: SessionPort
    store: ControlPlanePort
    resolver: ConflictResolver

    # ---------------------------------------------------------------- step 2
    def candidates(self, identity: Identity, hints: Hints) -> list[tuple[SpaceRecord, str | None]]:
        """(space, pre-exclusion reason). Other users' personal spaces are never candidates."""
        out = []
        for rec in self.registry.all():
            s = rec.space
            if s.scope == "personal":
                if s.key == identity.principal:
                    out.append((rec, None))
                continue
            hint = hints.project if s.scope == "project" else hints.team if s.scope == "team" else None
            if hint and s.key != hint:
                out.append((rec, f"not selected: hint {s.scope}={hint}"))
            else:
                out.append((rec, None))
        return out

    # ---------------------------------------------------------------- compose
    def compose(self, identity: Identity, query: str, hints: Hints, k: int, audit: Auditor,
                session_id: str | None = None) -> tuple[list[ResultItem], CompositionTrace]:
        t0 = time.perf_counter()
        timings: dict[str, float] = {}
        views: dict[str, SpaceDecisionView] = {}
        included: list[tuple[SpaceRecord, policy.Decision]] = []

        for rec, pre in self.candidates(identity, hints):
            if pre:
                views[rec.space_id] = SpaceDecisionView(space_id=rec.space_id, scope=rec.space.scope,
                                                        included=False, reason=pre)
                continue
            d = policy.decide(identity, rec, "read")
            views[rec.space_id] = SpaceDecisionView(space_id=rec.space_id, scope=rec.space.scope,
                                                    included=d.allowed, reason=d.reason,
                                                    backends=sorted(rec.bindings) if d.allowed else [])
            if d.allowed:
                included.append((rec, d))
        timings["policy"] = _ms(t0)

        # step 4: fan out in parallel. KB: one pooled query filtered to included spaces + caller principals.
        # Memory: one store per space, BM25-only search (managed memory has no vector search).
        per_space: dict[str, list[dict]] = {rec.space_id: [] for rec, _ in included}
        kb_spaces = [rec.space_id for rec, _ in included if "kb" in rec.bindings]
        recs = {rec.space_id: rec for rec, _ in included}
        mem_recs = [rec for rec, _ in included if rec.bindings.get("memory")]

        def kb_call():
            t = time.perf_counter()
            out = self.search.search(kb_spaces, query, identity.principal_tags, k=max(k * 3, 20)) if kb_spaces else []
            return out, _ms(t)

        def mem_call(rec):
            return rec, self.memory.search(rec.bindings["memory"], memory_actor(rec, identity), query, limit=k)

        t = time.perf_counter()
        with ThreadPoolExecutor(max_workers=1 + len(mem_recs)) as ex:
            kb_future = ex.submit(querylog.carry(kb_call))
            mem_results = list(ex.map(querylog.carry(mem_call), mem_recs))
            hits, timings["kb_search"] = kb_future.result()
        timings["fanout"] = _ms(t)
        for h in hits:
            per_space.setdefault(h.space_id, []).append({
                "id": f"kb:{h.chunk_id}", "content": h.content, "backend": "kb", "space_id": h.space_id,
                "ref": h.chunk_id, "path": h.path, "version": h.version, "metadata": h.metadata,
                "vector_rank": h.vector_rank, "bm25_rank": h.bm25_rank, "rrf": h.rrf,
                "created_at": h.updated_at.isoformat() if h.updated_at else None})
        for rec, entries in mem_results:
            for e in entries:
                per_space[rec.space_id].append({
                    "id": f"mem:{e.name}", "content": e.content, "backend": "memory", "space_id": rec.space_id,
                    "ref": e.name, "path": e.path, "memory_bm25": e.score, "description": e.description,
                    "session_id": e.session_id, "created_at": e.created_at.isoformat() if e.created_at else None})

        # sessions: short-term state of the caller's current session (personal space only)
        if session_id:
            t = time.perf_counter()
            for rec, _ in included:
                if rec.space.scope == "personal" and rec.bindings.get("sessions"):
                    name = self.sessions.open(rec.bindings["sessions"], identity.principal, session_id)
                    for i, item in enumerate(reversed(self.sessions.items(name)[-6:])):
                        text = item.get("content") if isinstance(item.get("content"), str) else str(item)
                        per_space[rec.space_id].append({
                            "id": f"sess:{name}:{i}", "content": text, "backend": "sessions",
                            "space_id": rec.space_id, "ref": name, "session_rank": i + 1})
            timings["session_read"] = _ms(t)

        # step 6a: merge across backends with RRF (each backend's own ranking feeds one fused list)
        pool = {it["id"]: it for items in per_space.values() for it in items}
        kb_ranked = [it["id"] for it in sorted((i for i in pool.values() if i["backend"] == "kb"),
                                               key=lambda i: -i["rrf"])]
        mem_ranked = [it["id"] for it in sorted((i for i in pool.values() if i["backend"] == "memory"),
                                                key=lambda i: -(i["memory_bm25"] or 0))]
        sess_ranked = [it["id"] for it in sorted((i for i in pool.values() if i["backend"] == "sessions"),
                                                 key=lambda i: i["session_rank"])]
        fused = rrf({"kb": kb_ranked, "memory": mem_ranked, "sessions": sess_ranked}, k=RRF_K)
        score_of = {_id: score for _id, score, _ in fused}

        # step 5: read guardrails per space, applied in fused order so a cap keeps the space's best items
        for rec, d in included:
            ranked_items = sorted(per_space[rec.space_id], key=lambda it: -score_of[it["id"]])
            items, actions = guardrails.apply_read(ranked_items, d.guardrails)
            per_space[rec.space_id] = items
            v = views[rec.space_id]
            v.guardrails_applied = actions
            v.items = len(items)
        allowed = {it["id"]: it for items in per_space.values() for it in items}

        # step 6b: dedupe identical content, keep fused order
        seen_content: set[str] = set()
        ordered = []
        for _id, score, _ in fused:
            if _id not in allowed:
                continue
            it = allowed[_id]
            key = " ".join(it["content"].split()).lower()
            if key in seen_content:
                continue
            seen_content.add(key)
            ordered.append((it, score))

        resolutions = self.resolver.resolve_all(self._fact_candidates([it for it, _ in ordered], recs))
        superseded: dict[str, str] = {}
        conflict_views = []
        for r in resolutions:
            view = r.view()
            conflict_views.append(view)
            self.store.write_conflict({"subject": r.subject, "outcome": r.outcome, "rule": r.rule,
                                       "winner": view["winner"], "losers": view["losers"],
                                       "rationale": r.rationale, "flags": r.flags})
            audit.record("conflict", r.outcome, None, subject=r.subject, rule=r.rule,
                         winner=view["winner"]["ref"] if r.winner else None)
            if r.winner:
                for loser in r.losers:
                    superseded[loser.ref] = r.winner.ref

        results = [self._item(it, score, recs[it["space_id"]], superseded) for it, score in ordered[:k]]

        # step 7: audit + trace
        for v in views.values():
            audit.record("retrieve.space", "included" if v.included else "excluded", v.space_id,
                         reason=v.reason, guardrails=v.guardrails_applied, items=v.items)
        audit.record("retrieve", "ok", None, query=query, hints=hints.model_dump(),
                     results=[r.provenance.ref for r in results])
        timings["total"] = _ms(t0)
        trace = CompositionTrace(caller=identity.principal, groups=list(identity.groups), hints=hints,
                                 considered=sorted(views.values(), key=lambda v: (not v.included, v.space_id)),
                                 conflicts=conflict_views, timings_ms=timings)
        return results, trace

    # ---------------------------------------------------------------- helpers
    def _fact_candidates(self, items: list[dict], recs: dict[str, SpaceRecord]) -> list[FactCandidate]:
        out = []
        for it in items:
            rec = recs[it["space_id"]]
            if it["backend"] == "memory":
                parsed = parse_fact_path(it.get("path") or "")
                if parsed:
                    subject, origin, observed = parsed
                    out.append(FactCandidate(subject, it["content"], rec.space_id,
                                             authority_label(rec.space.scope, "inferred" if origin == "inferred" else None),
                                             observed or _ts(it.get("created_at")), it["ref"],
                                             f"memory {rec.bindings.get('memory')}"))
            elif it["backend"] == "kb":
                md = it.get("metadata") or {}
                for subject, statement in (md.get("facts") or {}).items():
                    out.append(FactCandidate(subject, str(statement), rec.space_id,
                                             authority_label(rec.space.scope, md.get("authority")),
                                             _ts(md.get("effective_date") or it.get("created_at")), it["ref"],
                                             f"{it['path']}@v{it['version']}"))
        return out

    @staticmethod
    def _item(it: dict, score: float, rec: SpaceRecord, superseded: dict[str, str]) -> ResultItem:
        md = it.get("metadata") or {}
        return ResultItem(
            content=it["content"],
            provenance=Provenance(backend=it["backend"], space_id=rec.space_id, scope=rec.space.scope,
                                  ref=it["ref"], path=it.get("path"), version=it.get("version"),
                                  authority=md.get("authority") or (
                                      "inferred" if (parse_fact_path(it.get("path") or "") or ("", "", None))[1] == "inferred"
                                      else rec.space.scope),
                                  created_at=it.get("created_at"),
                                  extra={k: md[k] for k in ("title", "source_sessions", "confidence") if k in md}
                                  | ({"session_id": it["session_id"]} if it.get("session_id") else {})),
            score=ScoreBreakdown(vector_rank=it.get("vector_rank"), bm25_rank=it.get("bm25_rank"),
                                 rrf=it.get("rrf"), memory_bm25=it.get("memory_bm25"), fused=round(score, 6)),
            redactions=it.get("redactions", []),
            superseded_by=superseded.get(it["ref"]),
        )


def _ms(t: float) -> float:
    return round((time.perf_counter() - t) * 1000, 2)


def _ts(v):
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
    if isinstance(v, str) and v:
        try:
            d = datetime.fromisoformat(v.replace("Z", "+00:00"))
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime(1970, 1, 1, tzinfo=timezone.utc)
