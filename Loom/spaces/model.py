"""Space: the first-class unit of knowledge. Implemented in Loom (not a Databricks feature)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Scope = Literal["personal", "project", "team", "company"]
Perm = Literal["read", "write", "admin"]
Backend = Literal["kb", "memory", "sessions"]
SCOPES: tuple[Scope, ...] = ("personal", "project", "team", "company")
PERM_RANK: dict[str, int] = {"read": 1, "write": 2, "admin": 3}

# Space whose memory store is shared by all readers: managed memory partitions by actor_id, and
# search is per actor, so shared spaces store facts under one space-level actor id.
SHARED_ACTOR = "space"


def space_id_for(scope: Scope, key: str | None = None) -> str:
    return "company" if scope == "company" else f"{scope}:{key}"


def store_name_for(space_id: str) -> str:
    """One managed memory store per memory-bearing space (store-level ACL is the only boundary)."""
    return "loom-" + space_id.replace(":", "-").replace("_", "-")


@dataclass(frozen=True)
class Identity:
    """Verified caller. Built only from a validated token, never from model or request input."""
    principal: str                       # persona name, e.g. "bob"
    application_id: str                  # Databricks SP application id behind the token
    groups: tuple[str, ...] = ()         # Databricks group memberships (from SCIM)
    is_platform_admin: bool = False

    @property
    def principal_tags(self) -> list[str]:
        return [f"user:{self.principal}"] + [f"group:{g}" for g in self.groups]


@dataclass(frozen=True)
class Member:
    principal: str
    kind: Literal["user", "group"]
    perm: Perm


@dataclass
class Space:
    space_id: str
    scope: Scope
    name: str
    owner_principal: str
    config: dict = field(default_factory=dict)
    ephemeral: bool = False

    @property
    def key(self) -> str | None:
        return None if self.scope == "company" else self.space_id.split(":", 1)[1]


@dataclass
class SpaceRecord:
    space: Space
    members: list[Member] = field(default_factory=list)
    guardrails: dict[str, dict] = field(default_factory=lambda: {"read": {}, "write": {}})
    bindings: dict[str, str] = field(default_factory=dict)   # backend -> ref

    @property
    def space_id(self) -> str:
        return self.space.space_id
