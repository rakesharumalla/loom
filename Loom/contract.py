"""The consumer contract: exactly three verbs, configure / retrieve / contribute.

REST (api.py), MCP (mcp_server.py) and the Python SDK (sdk.py) expose these same models; the
surfaces differ only in transport. The caller is never a request field: it comes from the
verified token on every surface.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

Scope = Literal["personal", "project", "team", "company"]
Kind = Literal["fact", "file", "lesson"]


# ---------------------------------------------------------------- configure (admin only)
class MemberSpec(BaseModel):
    principal: str
    kind: Literal["user", "group"] = "user"
    perm: Literal["read", "write", "admin"] = "read"


class GuardrailSpec(BaseModel):
    read: dict = Field(default_factory=dict)
    write: dict = Field(default_factory=dict)


class SpaceSpec(BaseModel):
    scope: Scope
    key: str | None = Field(None, description="personal: user, project/team: id, company: omitted")
    name: str
    owner: str
    config: dict = Field(default_factory=dict)
    ephemeral: bool = False


class ConfigureRequest(BaseModel):
    space: SpaceSpec
    members: list[MemberSpec] = Field(default_factory=list)
    guardrails: GuardrailSpec = Field(default_factory=GuardrailSpec)
    bindings: list[Literal["kb", "memory", "sessions"]] = Field(default_factory=lambda: ["kb", "memory"])


class ConfigureResponse(BaseModel):
    request_id: str
    space_id: str
    created: bool
    bindings: dict[str, str]


# ---------------------------------------------------------------- retrieve
class Hints(BaseModel):
    project: str | None = None
    team: str | None = None


class RetrieveRequest(BaseModel):
    query: str
    hints: Hints = Field(default_factory=Hints)
    k: int = Field(8, ge=1, le=50)
    session_id: str | None = None


class ScoreBreakdown(BaseModel):
    vector_rank: int | None = None
    bm25_rank: int | None = None
    rrf: float | None = None
    memory_bm25: float | None = None
    fused: float


class Provenance(BaseModel):
    backend: Literal["kb", "memory", "sessions"]
    space_id: str
    scope: Scope
    ref: str
    path: str | None = None
    version: int | None = None
    authority: str | None = None
    created_at: str | None = None
    extra: dict = Field(default_factory=dict)


class ResultItem(BaseModel):
    content: str
    provenance: Provenance
    score: ScoreBreakdown
    redactions: list[str] = Field(default_factory=list)
    superseded_by: str | None = None


class SpaceDecisionView(BaseModel):
    space_id: str
    scope: Scope
    included: bool
    reason: str
    backends: list[str] = Field(default_factory=list)
    guardrails_applied: list[str] = Field(default_factory=list)
    items: int = 0


class CompositionTrace(BaseModel):
    caller: str
    groups: list[str]
    hints: Hints
    considered: list[SpaceDecisionView]
    conflicts: list[dict] = Field(default_factory=list)
    timings_ms: dict[str, float] = Field(default_factory=dict)
    note: str = "Composition, policy, guardrails and conflict resolution are implemented in Loom."


class RetrieveResponse(BaseModel):
    request_id: str
    results: list[ResultItem]
    trace: CompositionTrace


# ---------------------------------------------------------------- contribute
class ContributeRequest(BaseModel):
    content: str
    kind: Kind = "fact"
    target_space: str | None = Field(None, description="defaults to personal:{caller}")
    subject: str | None = Field(None, description="normalized subject key for facts (conflict detection)")
    path: str | None = Field(None, description="file/lesson path inside the space KB")
    propose_promotion_to: str | None = None
    session_id: str | None = None
    observed_at: str | None = Field(None, description="ISO time the fact was stated (default: now)")


class ContributeResponse(BaseModel):
    request_id: str
    status: Literal["stored", "rejected", "held_for_review", "promotion_pending"]
    space_id: str
    ref: str | None = None
    reason: str = ""
    guardrail_actions: list[str] = Field(default_factory=list)
    conflict: dict | None = None
    promotion_request_id: str | None = None


# ---------------------------------------------------------------- promotion (part of contribute workflow)
class PromotionDecisionRequest(BaseModel):
    request_id: str
    approve: bool


class PromotionDecisionResponse(BaseModel):
    request_id: str
    promotion_id: str
    status: Literal["approved", "rejected"]
    target_space: str
    ref: str | None = None
    reason: str = ""
