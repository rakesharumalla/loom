"""MCP surface of the consumer contract.

The same three verbs as REST. Identity comes from the HTTP Authorization header of the MCP request,
verified with Databricks; tool arguments never carry identity, so a model cannot choose who it is.
"""
from __future__ import annotations

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from Loom.contract import ConfigureRequest, ContributeRequest, Hints, RetrieveRequest
from Loom.identity import TokenVerifier, Unauthorized, identity_from_headers
from Loom.service import LoomService, PolicyError
from Loom.spaces.model import Identity


def build_mcp(service: LoomService, verifier: TokenVerifier, proxy_client=None) -> MCPServer:
    mcp = MCPServer(name="loom", instructions=(
        "Loom knowledge tools. retrieve returns context composed from the spaces you may read, with a "
        "composition trace. contribute stores a fact, file, or lesson subject to policy, guardrails and "
        "conflict checks. configure is admin-only."))

    def caller(ctx: Context) -> Identity:
        try:
            return identity_from_headers(verifier, ctx.headers or {}, proxy_client)
        except Unauthorized as e:
            raise ToolError(f"denied: {e}")

    @mcp.tool(description="Retrieve composed context for a task/query. Optional hints: project, team.")
    def retrieve(query: str, ctx: Context, project: str | None = None, team: str | None = None,
                 k: int = 8, session_id: str | None = None) -> dict:
        req = RetrieveRequest(query=query, hints=Hints(project=project, team=team), k=k, session_id=session_id)
        return service.retrieve(caller(ctx), req, "mcp").model_dump(mode="json")

    @mcp.tool(description="Contribute a fact, file or lesson. Default target is your personal space.")
    def contribute(content: str, ctx: Context, kind: str = "fact", target_space: str | None = None,
                   subject: str | None = None, path: str | None = None,
                   propose_promotion_to: str | None = None, session_id: str | None = None,
                   observed_at: str | None = None) -> dict:
        req = ContributeRequest(content=content, kind=kind, target_space=target_space, subject=subject, path=path,
                                propose_promotion_to=propose_promotion_to, session_id=session_id,
                                observed_at=observed_at)
        return service.contribute(caller(ctx), req, "mcp").model_dump(mode="json")

    @mcp.tool(description="Admin only: create or update a space (members, guardrails, bindings).")
    def configure(space: dict, ctx: Context, members: list[dict] | None = None, guardrails: dict | None = None,
                  bindings: list[str] | None = None) -> dict:
        req = ConfigureRequest(space=space, members=members or [], guardrails=guardrails or {},
                               **({"bindings": bindings} if bindings is not None else {}))
        try:
            return service.configure(caller(ctx), req, "mcp").model_dump(mode="json")
        except PolicyError as e:
            raise ToolError(f"denied: {e}")

    return mcp
