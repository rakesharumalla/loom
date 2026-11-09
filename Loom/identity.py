"""Verified identity. The only source of Identity objects.

A bearer token is accepted only after Databricks validates it (SCIM /Me with that token). The
resulting principal and groups come from Databricks, never from request bodies, tool arguments,
or model output.
"""
from __future__ import annotations

import hashlib
import threading
import time
from typing import Protocol

from databricks.sdk import WorkspaceClient

from Loom.spaces.model import Identity

PLATFORM_ADMINS = {"admin"}


class Unauthorized(Exception):
    pass


class TokenVerifier(Protocol):
    def verify(self, token: str) -> Identity: ...


class DatabricksTokenVerifier:
    """Verifies Databricks OAuth tokens and maps the service principal to a Loom persona."""

    def __init__(self, host: str, persona_by_app_id: dict[str, str], ttl_s: int = 300):
        self.host = host
        self.persona_by_app_id = persona_by_app_id
        self.ttl_s = ttl_s
        self._cache: dict[str, tuple[float, Identity]] = {}
        self._lock = threading.Lock()

    def verify(self, token: str) -> Identity:
        if not token:
            raise Unauthorized("missing bearer token")
        key = hashlib.sha256(token.encode()).hexdigest()
        with self._lock:
            hit = self._cache.get(key)
            if hit and time.time() - hit[0] < self.ttl_s:
                return hit[1]
        try:
            me = WorkspaceClient(host=self.host, token=token, auth_type="pat").current_user.me()
        except Exception as e:  # invalid/expired token
            raise Unauthorized(f"token rejected by Databricks: {type(e).__name__}") from e
        app_id = getattr(me, "application_id", None) or me.user_name   # SCIM /Me: SP userName = application id
        persona = self.persona_by_app_id.get(app_id or "")
        if not persona:
            raise Unauthorized(f"principal {app_id} is not a Loom persona")
        ident = Identity(persona, app_id, tuple(sorted(g.display for g in (me.groups or [])
                                                                  if g.display and g.display.startswith("loom-"))),
                         is_platform_admin=persona in PLATFORM_ADMINS)
        with self._lock:
            self._cache[key] = (time.time(), ident)
        return ident


    def verify_proxy(self, principal: str, w: WorkspaceClient) -> Identity:
        """Identity asserted by the Databricks Apps proxy (X-Forwarded-* headers). Only trusted inside
        Databricks Apps, where the proxy is the only way to reach the app. Groups come from SCIM."""
        persona = self.persona_by_app_id.get(principal)
        if not persona:
            raise Unauthorized(f"principal {principal} is not a Loom persona")
        with self._lock:
            hit = self._cache.get("proxy:" + principal)
            if hit and time.time() - hit[0] < self.ttl_s:
                return hit[1]
        sps = list(w.service_principals.list(filter=f'applicationId eq "{principal}"', attributes="groups"))
        groups = tuple(sorted(g.display for sp in sps for g in (sp.groups or []) if g.display and g.display.startswith("loom-")))
        ident = Identity(persona, principal, groups, is_platform_admin=persona in PLATFORM_ADMINS)
        with self._lock:
            self._cache["proxy:" + principal] = (time.time(), ident)
        return ident


def identity_from_headers(verifier, headers, proxy_client: WorkspaceClient | None = None) -> Identity:
    """Bearer token -> X-Forwarded-Access-Token -> (inside Databricks Apps only) proxy identity headers."""
    h = {k.lower(): v for k, v in (headers or {}).items()}
    bearer = h.get("authorization", "").removeprefix("Bearer ").strip()
    if bearer:
        return verifier.verify(bearer)
    if h.get("x-forwarded-access-token"):
        return verifier.verify(h["x-forwarded-access-token"])
    principal = h.get("x-forwarded-email") or h.get("x-forwarded-user") or h.get("x-forwarded-preferred-username")
    if principal and proxy_client is not None and hasattr(verifier, "verify_proxy"):
        return verifier.verify_proxy(principal, proxy_client)
    raise Unauthorized("missing bearer token")


class StaticTokenVerifier:
    """Test double: fixed token -> identity table."""

    def __init__(self, table: dict[str, Identity]):
        self.table = table

    def verify(self, token: str) -> Identity:
        if token not in self.table:
            raise Unauthorized("unknown token")
        return self.table[token]


def persona_map(w: WorkspaceClient, personas: dict[str, dict]) -> dict[str, str]:
    by_name = {spec["sp"]: p for p, spec in personas.items()}
    out = {}
    for name, p in by_name.items():
        for sp in w.service_principals.list(filter=f'displayName eq "{name}"'):
            out[sp.application_id] = p
    return out


class PersonaTokens:
    """Mints OAuth (M2M) tokens for persona SPs from the secret scope. Used by the demo web app's
    persona switcher, the SDK demo and the MCP demo client: each call then carries a real token
    that the facade verifies like any other caller."""

    def __init__(self, w: WorkspaceClient, scope: str):
        self.w, self.scope = w, scope
        self._cache: dict[str, tuple[float, str]] = {}
        self._lock = threading.Lock()

    def _secret(self, key: str) -> str:
        import base64
        return base64.b64decode(self.w.secrets.get_secret(self.scope, key).value).decode()

    def token(self, persona: str) -> str:
        with self._lock:
            hit = self._cache.get(persona)
            if hit and time.time() - hit[0] < 45 * 60:
                return hit[1]
        cid, secret = self._secret(f"{persona}-client-id"), self._secret(f"{persona}-client-secret")
        sp_client = WorkspaceClient(host=self.w.config.host, client_id=cid, client_secret=secret,
                                    auth_type="oauth-m2m")
        tok = sp_client.config.authenticate()["Authorization"].split(" ", 1)[1]
        with self._lock:
            self._cache[persona] = (time.time(), tok)
        return tok
