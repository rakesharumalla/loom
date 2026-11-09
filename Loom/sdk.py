"""Python SDK for services (batch consumers): the same contract over HTTP.

    from Loom.sdk import LoomClient
    loom = LoomClient("https://<app-url>", token_provider=lambda: my_databricks_oauth_token())
    resp = loom.retrieve("withdrawal queue stuck after node upgrade", project="atlas")
"""
from __future__ import annotations

from typing import Callable

import httpx

from Loom.contract import (ConfigureRequest, ConfigureResponse, ContributeRequest, ContributeResponse, Hints,
                           RetrieveRequest, RetrieveResponse)


class LoomError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(f"{status}: {detail}")
        self.status, self.detail = status, detail


class LoomClient:
    def __init__(self, base_url: str, token_provider: Callable[[], str], client: httpx.Client | None = None,
                 timeout: float = 60.0):
        self._token = token_provider
        self._http = client or httpx.Client(base_url=base_url, timeout=timeout)

    def _post(self, path: str, body: dict) -> dict:
        r = self._http.post(path, json=body, headers={"Authorization": f"Bearer {self._token()}",
                                                      "X-Loom-Surface": "sdk"})
        if r.status_code >= 400:
            raise LoomError(r.status_code, r.json().get("detail", r.text) if r.headers.get(
                "content-type", "").startswith("application/json") else r.text)
        return r.json()

    def configure(self, req: ConfigureRequest) -> ConfigureResponse:
        return ConfigureResponse(**self._post("/v1/configure", req.model_dump(mode="json")))

    def retrieve(self, query: str, project: str | None = None, team: str | None = None, k: int = 8,
                 session_id: str | None = None) -> RetrieveResponse:
        req = RetrieveRequest(query=query, hints=Hints(project=project, team=team), k=k, session_id=session_id)
        return RetrieveResponse(**self._post("/v1/retrieve", req.model_dump(mode="json")))

    def contribute(self, content: str, kind: str = "fact", **kw) -> ContributeResponse:
        req = ContributeRequest(content=content, kind=kind, **kw)
        return ContributeResponse(**self._post("/v1/contribute", req.model_dump(mode="json")))
