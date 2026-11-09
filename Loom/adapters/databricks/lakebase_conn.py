"""Lakebase (Postgres) connections with OAuth token refresh.

Docs: https://docs.databricks.com/aws/en/oltp/projects/connect (OAuth tokens expire after 1 hour).
"""
from __future__ import annotations

import socket
import subprocess
import threading
import time
from contextlib import contextmanager

import psycopg
from databricks.sdk import WorkspaceClient
from psycopg_pool import ConnectionPool

from config import CONFIG
from Loom.adapters.databricks.capture import RecordingCursor

_TOKEN_TTL_S = 45 * 60


class LakebaseConnector:
    """Builds psycopg connections to one Lakebase branch as the current Databricks identity."""

    def __init__(self, w: WorkspaceClient, branch_path: str | None = None, database: str | None = None):
        self.w = w
        self.branch_path = branch_path or CONFIG.branch_path
        self.database = database or CONFIG.lakebase_database
        self._lock = threading.Lock()
        self._token: str | None = None
        self._token_at = 0.0
        self._endpoint: str | None = None
        self._host: str | None = None
        self._user: str | None = None
        self._pool: ConnectionPool | None = None

    def endpoint_name(self) -> str:
        if self._endpoint is None:
            eps = list(self.w.postgres.list_endpoints(parent=self.branch_path))
            rw = [e for e in eps if "READ_WRITE" in str(e.status.endpoint_type if e.status else "")] or eps
            if not rw:
                raise RuntimeError(f"no endpoint on {self.branch_path}")
            self._endpoint = rw[0].name
            self._host = rw[0].status.hosts.host
        return self._endpoint

    def host(self) -> str:
        self.endpoint_name()
        return self._host  # type: ignore[return-value]

    def user(self) -> str:
        if self._user is None:
            me = self.w.current_user.me()
            # Service principals connect with their application (client) id.
            self._user = me.user_name or getattr(me, "application_id", None)
        return self._user

    def token(self) -> str:
        with self._lock:
            if self._token is None or time.time() - self._token_at > _TOKEN_TTL_S:
                self._token = self.w.postgres.generate_database_credential(endpoint=self.endpoint_name()).token
                self._token_at = time.time()
            return self._token

    def conninfo(self) -> dict:
        host = self.host()
        # prepare_threshold=None: psycopg otherwise auto-prepares a statement after 5 runs, and the cached
        # generic plan can choose the ANN index for very selective filters where a custom plan would use the
        # exact B-tree path (measured: kb filter at ~0.1% of rows, recall@10 0.613 vs 1.000).
        info = {"host": host, "dbname": self.database, "user": self.user(), "sslmode": "require",
                "prepare_threshold": None}
        addr = _resolve(host)
        if addr:
            info["hostaddr"] = addr
        return info

    def connect(self, **kw) -> psycopg.Connection:
        return psycopg.connect(**self.conninfo(), password=self.token(), cursor_factory=RecordingCursor, **kw)

    def pool(self) -> ConnectionPool:
        if self._pool is None:
            connector = self

            class _OAuthConnection(psycopg.Connection):
                @classmethod
                def connect(cls, conninfo: str = "", **kwargs):  # type: ignore[override]
                    kwargs["password"] = connector.token()
                    return super().connect(conninfo, **kwargs)

            self._pool = ConnectionPool(
                kwargs=self.conninfo(), connection_class=_OAuthConnection,
                min_size=1, max_size=8, max_lifetime=_TOKEN_TTL_S, open=True,
                configure=lambda c: setattr(c, "cursor_factory", RecordingCursor),
            )
        return self._pool

    @contextmanager
    def tx(self):
        """One ACID transaction from the pool: commits on success, rolls back on any exception."""
        with self.pool().connection() as conn:
            with conn.transaction():
                yield conn


def _resolve(host: str) -> str | None:
    """macOS getaddrinfo can fail on long Lakebase hostnames; fall back to dig and pass hostaddr."""
    try:
        socket.getaddrinfo(host, 5432)
        return None
    except socket.gaierror:
        out = subprocess.run(["dig", "+short", host], capture_output=True, text=True).stdout.split()
        ips = [x for x in out if x[:1].isdigit()]
        return ips[-1] if ips else None
