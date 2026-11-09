"""Demo configuration. Values come from env vars (see .env.example); no secrets live here."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Config:
    profile: str = _env("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    catalog: str = _env("LOOM_CATALOG", "loom_catalog")
    schema: str = _env("LOOM_SCHEMA", "loom")
    volume: str = _env("LOOM_VOLUME", "raw")
    lakebase_project: str = _env("LOOM_LAKEBASE_PROJECT", "loom-demo")
    lakebase_branch: str = _env("LOOM_LAKEBASE_BRANCH", "production")
    lakebase_database: str = _env("LOOM_LAKEBASE_DATABASE", "databricks_postgres")
    pg_schema: str = _env("LOOM_PG_SCHEMA", "loom")
    embedding_endpoint: str = _env("LOOM_EMBEDDING_ENDPOINT", "databricks-gte-large-en")
    embedding_dim: int = int(_env("LOOM_EMBEDDING_DIM", "1024"))
    llm_endpoint: str = _env("LOOM_LLM_ENDPOINT", "databricks-claude-sonnet-5-5")
    secret_scope: str = _env("LOOM_SECRET_SCOPE", "loom-demo")
    session_store: str = _env("LOOM_SESSION_STORE", "loom-sessions")
    # Files at or below this size are stored inline in Lakebase; larger raw files go to the Volume.
    inline_max_bytes: int = int(_env("LOOM_INLINE_MAX_BYTES", "65536"))
    groups: tuple[str, ...] = ("loom-compliance", "loom-support", "loom-trading-ops")
    personas: dict[str, dict] = field(default_factory=lambda: {
        "alice": {"sp": "loom-alice", "groups": ["loom-compliance"], "teams": ["risk"], "projects": ["atlas"]},
        "bob": {"sp": "loom-bob", "groups": ["loom-support"], "teams": ["care"], "projects": ["atlas"]},
        "carol": {"sp": "loom-carol", "groups": ["loom-trading-ops"], "teams": ["markets"], "projects": ["borealis"]},
        "admin": {"sp": "loom-admin", "groups": [], "teams": [], "projects": []},
        "service": {"sp": "loom-service", "groups": [], "teams": [], "projects": []},
    })

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def branch_path(self) -> str:
        return f"projects/{self.lakebase_project}/branches/{self.lakebase_branch}"


CONFIG = Config()
