"""Feeds Loom.querylog from the two places every backend call passes through.

- SQL: a psycopg cursor class installed on every Lakebase connection; statements are rendered with their
  bound parameters (client-side mogrify, display only; execution still uses server-side binding).
- REST: the WorkspaceClient's ApiClient.do, shared by the SDK services (Model Serving, Lakebase API) and
  the AgentKit client (managed memory and sessions).
"""
from __future__ import annotations

import json
import time
from urllib.parse import urlencode

import psycopg

from Loom import querylog

_REST_TARGETS = (("/api/2.0/agents/memory-stores", "Managed memory"),
                 ("/api/2.0/agents/session-stores", "Managed sessions"),
                 ("/serving-endpoints", "Model Serving"),
                 ("/api/2.0/postgres", "Lakebase API"),
                 ("/api/2.0/fs/files", "UC Volume"))


def render_sql(conn, query, params) -> str:
    text = query.as_string(conn) if hasattr(query, "as_string") else str(query)
    text = "\n".join(line.strip() for line in text.strip().splitlines()).replace(" UNION ALL ", "\nUNION ALL\n")
    if params is None:
        return text
    try:
        with psycopg.ClientCursor(conn) as cc:
            return cc.mogrify(text, querylog.shorten(params))
    except Exception:
        return text + f"\n-- params: {querylog.shorten(params)!r}"


class RecordingCursor(psycopg.Cursor):
    def execute(self, query, params=None, **kw):
        if not querylog.active():
            return super().execute(query, params, **kw)
        t, ok = time.perf_counter(), True
        try:
            return super().execute(query, params, **kw)
        except Exception:
            ok = False
            raise
        finally:
            rows = f"{self.rowcount} row{'' if self.rowcount == 1 else 's'}" if ok and self.rowcount >= 0 else None
            querylog.record("sql", "Lakebase Postgres", render_sql(self.connection, query, params), t, ok, rows)

    def executemany(self, query, params_seq, **kw):
        if not querylog.active():
            return super().executemany(query, params_seq, **kw)
        params_seq = list(params_seq)
        t, ok = time.perf_counter(), True
        try:
            return super().executemany(query, params_seq, **kw)
        except Exception:
            ok = False
            raise
        finally:
            text = render_sql(self.connection, query, params_seq[0] if params_seq else None)
            if len(params_seq) > 1:
                text += f"\n-- executemany: first of {len(params_seq)} rows shown"
            querylog.record("sql", "Lakebase Postgres", text, t, ok, f"{len(params_seq)} rows")


def record_rest(w) -> None:
    """Wrap w.api_client.do once; every SDK service on this client shares that ApiClient."""
    api = w.api_client
    if getattr(api, "_loom_recording", False):
        return
    inner = api.do

    def do(method, path=None, *a, **kw):
        if not querylog.active():
            return inner(method, path, *a, **kw)
        t, ok = time.perf_counter(), True
        try:
            return inner(method, path, *a, **kw)
        except Exception:
            ok = False
            raise
        finally:
            p = path or kw.get("url") or ""
            q, body = kw.get("query"), kw.get("body")
            text = f"{method} {p}" + (f"?{urlencode(q)}" if q else "")
            if body:
                text += "\n" + json.dumps(querylog.shorten(body), indent=2, default=str)
            target = next((name for prefix, name in _REST_TARGETS if p.startswith(prefix)), "Databricks REST")
            querylog.record("rest", target, text, t, ok)

    api.do = do
    api._loom_recording = True
