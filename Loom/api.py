"""REST surface of the consumer contract (plus admin views). Transport only: every route calls
LoomService with the identity verified from the bearer token."""

import contextlib
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from Loom.contract import (ConfigureRequest, ConfigureResponse, ContributeRequest, ContributeResponse,
                           PromotionDecisionResponse, RetrieveRequest, RetrieveResponse)
from Loom.identity import TokenVerifier, Unauthorized, identity_from_headers
from Loom.service import LoomService, PolicyError
from Loom.spaces.model import Identity

# Transport label recorded in audit rows. Self-declared by the client; it is not a trust signal.
Surface = Literal["rest", "sdk", "web"]


class FeedbackRequest(BaseModel):
    query: str
    source_ref: str
    rating: int
    comment: str | None = None


def create_app(service: LoomService, verifier: TokenVerifier, mcp_server=None, extra_routes=None,
               proxy_client=None) -> FastAPI:
    lifespan = None
    if mcp_server is not None:
        @contextlib.asynccontextmanager
        async def lifespan(app):  # MCP streamable HTTP needs its session manager running
            async with mcp_server.session_manager.run():
                yield

    app = FastAPI(title="Loom facade (demo)", version="0.1.0", lifespan=lifespan,
                  description="Thin Loom facade over Databricks storage ports. Spaces, composition, "
                              "policy, guardrails, conflict resolution and promotion are implemented in Loom.")
    app.state.service = service

    def caller(request: Request) -> Identity:
        try:
            return identity_from_headers(verifier, request.headers, proxy_client)
        except Unauthorized as e:
            raise HTTPException(401, str(e))

    def surface(x_loom_surface: Annotated[str | None, Header()] = None) -> str:
        return x_loom_surface if x_loom_surface in ("rest", "sdk", "web") else "rest"

    Caller = Annotated[Identity, Depends(caller)]
    SurfaceDep = Annotated[str, Depends(surface)]

    @app.post("/v1/configure", response_model=ConfigureResponse)
    def configure(req: ConfigureRequest, who: Caller, s: SurfaceDep):
        try:
            return service.configure(who, req, s)
        except PolicyError as e:
            raise HTTPException(403, str(e))

    @app.post("/v1/retrieve", response_model=RetrieveResponse)
    def retrieve(req: RetrieveRequest, who: Caller, s: SurfaceDep):
        return service.retrieve(who, req, s)

    @app.post("/v1/contribute", response_model=ContributeResponse)
    def contribute(req: ContributeRequest, who: Caller, s: SurfaceDep):
        return service.contribute(who, req, s)

    # ---- admin / control-plane views (not part of the consumer contract)
    @app.post("/v1/admin/promotions/{promotion_id}/decide", response_model=PromotionDecisionResponse)
    def decide(promotion_id: str, approve: bool, who: Caller, s: SurfaceDep):
        try:
            return service.decide_promotion(who, promotion_id, approve, s)
        except PolicyError as e:
            raise HTTPException(403, str(e))

    @app.get("/v1/admin/promotions")
    def promotions(who: Caller, status: str | None = None):
        return service.ports.control.list_promotions(status)

    @app.get("/v1/admin/spaces")
    def spaces(who: Caller):
        return [{"space_id": r.space_id, "scope": r.space.scope, "name": r.space.name, "owner": r.space.owner_principal,
                 "members": [m.__dict__ for m in r.members], "guardrails": r.guardrails, "bindings": r.bindings}
                for r in service.registry.all()]

    @app.get("/v1/admin/audit")
    def audit(who: Caller, n: int = 50, request_id: str | None = None):
        c = service.ports.control
        return c.audit_for(request_id) if request_id else c.tail_audit(n)

    @app.get("/v1/admin/conflicts")
    def conflicts(who: Caller, n: int = 50):
        return service.ports.control.list_conflicts(n)

    @app.get("/v1/admin/ports")
    def ports(who: Caller, n: int = 100):
        return service.port_log.recent(n)

    @app.post("/v1/feedback")
    def feedback(req: FeedbackRequest, who: Caller):
        service.ports.control.write_feedback({"caller": who.principal, **req.model_dump()})
        return {"ok": True}

    @app.get("/v1/whoami")
    def whoami(who: Caller):
        return {"principal": who.principal, "groups": list(who.groups), "platform_admin": who.is_platform_admin}

    if extra_routes:
        extra_routes(app)
    if mcp_server is not None:
        app.mount("/mcp", mcp_server.streamable_http_app(streamable_http_path="/", stateless_http=True,
                                                         json_response=True))
    return app
