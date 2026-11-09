"""Minimal MCP client used by the demo agent and the parity tests (streamable HTTP + bearer token)."""
from __future__ import annotations

import asyncio
import json

from mcp.client.client import Client
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client


class MCPToolError(Exception):
    pass


async def call_tool_async(url: str, token: str, tool: str, args: dict) -> dict:
    http = create_mcp_http_client(headers={"Authorization": f"Bearer {token}"})
    async with Client(streamable_http_client(url, http_client=http)) as client:
        res = await client.call_tool(tool, args)
    structured = getattr(res, "structured_content", None) or getattr(res, "structuredContent", None)
    is_error = getattr(res, "is_error", None) or getattr(res, "isError", False)
    text = "".join(getattr(c, "text", "") for c in (res.content or []))
    if is_error:
        raise MCPToolError(text)
    if isinstance(structured, dict):
        return structured.get("result", structured) if set(structured) == {"result"} else structured
    return json.loads(text)


def call_tool(url: str, token: str, tool: str, args: dict) -> dict:
    return asyncio.run(call_tool_async(url, token, tool, args))


async def list_tools_async(url: str, token: str) -> list[str]:
    http = create_mcp_http_client(headers={"Authorization": f"Bearer {token}"})
    async with Client(streamable_http_client(url, http_client=http)) as client:
        return [t.name for t in (await client.list_tools()).tools]
