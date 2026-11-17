import asyncio
import hashlib
import hmac
import json
import os
import sqlite3
from contextlib import asynccontextmanager

import httpx
from anyio import CancelScope
from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from .model_router import UPSTREAM_ERROR, route_completion, stream_completion, token_reservation
from .rate_limiter import SqliteRateLimiter


MAX_BODY_BYTES = 1_000_000
MCP_HEADERS = ("mcp-session-id", "mcp-protocol-version")


class BodyTooLarge(ValueError):
    pass


async def read_json(request):
    payload = bytearray()
    async for chunk in request.stream():
        if len(payload) + len(chunk) > MAX_BODY_BYTES:
            raise BodyTooLarge
        payload.extend(chunk)
    value = json.loads(payload.decode("utf-8"))
    json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
    return value


def bearer(request):
    parts = request.headers.get("authorization", "").split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].encode()
    return b""


def matches(token, key):
    return bool(token and key and hmac.compare_digest(token, key.encode()))


def rpc_error(request_id, code, message, status=200):
    return JSONResponse({"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}, status)


def rpc_result(request_id, result):
    return JSONResponse({"jsonrpc": "2.0", "id": request_id, "result": result})


class UpstreamStream(StreamingResponse):
    def __init__(self, upstream, content, **options):
        super().__init__(content, **options)
        self.upstream = upstream

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            with CancelScope(shield=True):
                await self.body_iterator.aclose()
                await self.upstream.aclose()


async def proxy_mcp(request):
    token = bearer(request)
    if matches(token, os.getenv("MCP_VIEWER_TOKEN")):
        role = "viewer"
    elif matches(token, os.getenv("MCP_ADMIN_TOKEN")):
        role = "admin"
    else:
        return rpc_error(None, -32001, "Unauthorized", 401)

    try:
        body = await read_json(request)
    except BodyTooLarge:
        return rpc_error(None, -32600, "Request too large", 413)
    except (ValueError, RecursionError):
        return rpc_error(None, -32700, "Invalid JSON", 400)

    if not isinstance(body, dict) or body.get("jsonrpc") != "2.0" or not isinstance(body.get("method"), str):
        return rpc_error(None, -32600, "Invalid Request", 400)
    request_id = body.get("id")
    if type(request_id) not in (str, int, type(None)):
        return rpc_error(None, -32600, "Invalid Request ID", 400)
    if body["method"] in ("tools/list", "tools/call") and "id" not in body:
        return Response(status_code=202)
    if body["method"] == "tools/call":
        params = body.get("params")
        name = params.get("name") if isinstance(params, dict) else None
        if not isinstance(name, str) or not name:
            return rpc_error(request_id, -32602, "Invalid tool name")
        if name.startswith("admin_") and role != "admin":
            return rpc_error(request_id, -32001, "Unauthorized Tool Call")

    state = request.app.state
    url = os.getenv("MCP_DOWNSTREAM_URL")
    client = state.client if url else state.mock_client
    headers = {name: request.headers[name] for name in ("accept", *MCP_HEADERS) if name in request.headers}
    try:
        outbound = httpx.Request("POST", url or "http://mock-mcp/mcp", headers=headers, json=body)
        upstream = await client.send(outbound, stream=True)
    except (httpx.HTTPError, httpx.InvalidURL, ValueError):
        return rpc_error(request_id, -32603, "Downstream unavailable", 502)
    forwarded = {name: upstream.headers[name] for name in ("content-type", *MCP_HEADERS) if name in upstream.headers}
    return UpstreamStream(upstream, upstream.aiter_bytes(), status_code=upstream.status_code, headers=forwarded)


async def mock_mcp(request):
    body = await request.json()
    method, request_id = body["method"], body.get("id")
    if "id" not in body:
        return Response(status_code=202)
    if method == "tools/list":
        return rpc_result(request_id, {"tools": [
            {"name": name, "inputSchema": {"type": "object"}} for name in ("get_status", "admin_reset_key")
        ]})
    if method == "tools/call":
        name = body["params"]["name"]
        if name in ("get_status", "admin_reset_key"):
            return rpc_result(request_id, {"content": [{"type": "text", "text": f"Mock result for {name}"}]})
        return rpc_error(request_id, -32602, "Unknown tool")
    if method == "initialize":
        return rpc_result(request_id, {
            "protocolVersion": "2025-03-26", "capabilities": {"tools": {}},
            "serverInfo": {"name": "mock-mcp", "version": "1.0"},
        })
    if method == "ping":
        return rpc_result(request_id, {})
    return rpc_error(request_id, -32601, "Method not found")


def gateway_error(status, code, message):
    return JSONResponse({"error": {"type": "gateway_error", "code": code, "message": message}}, status)


async def completions(request):
    token = bearer(request)
    keys = os.getenv("LLM_TENANT_KEYS", "").split(",")
    if not any(matches(token, key.strip()) for key in keys):
        return gateway_error(401, "unauthorized", "Invalid tenant API key")
    try:
        body = await read_json(request)
        if not isinstance(body, dict) or not isinstance(body.get("messages"), list) or not body["messages"]:
            raise ValueError
        body["stream"] = True
        tokens = token_reservation(body)
    except BodyTooLarge:
        return gateway_error(413, "request_too_large", "Request too large")
    except (ValueError, RecursionError):
        return gateway_error(400, "invalid_request", "Invalid completion request")

    primary = os.getenv("PRIMARY_LLM_URL"), os.getenv("PRIMARY_LLM_API_KEY")
    backup = os.getenv("BACKUP_LLM_URL"), os.getenv("BACKUP_LLM_API_KEY")
    if not primary[0] or not backup[0]:
        return JSONResponse(UPSTREAM_ERROR, 502)
    state = request.app.state
    tenant = hashlib.sha256(token).hexdigest()
    try:
        allowed = await asyncio.to_thread(state.limiter.consume, tenant, tokens)
    except sqlite3.Error:
        return gateway_error(503, "quota_unavailable", "Token quota unavailable")
    if not allowed:
        return gateway_error(429, "rate_limit_exceeded", "Token limit exceeded")

    try:
        upstream, events, first = await route_completion(
            state.client, body, primary, backup, os.getenv("BACKUP_LLM_MODEL")
        )
    except (httpx.HTTPError, httpx.InvalidURL, TimeoutError, ValueError, RecursionError):
        return JSONResponse(UPSTREAM_ERROR, 502)
    return UpstreamStream(
        upstream, stream_completion(events, first), media_type="text/event-stream",
        headers={"cache-control": "no-cache", "x-accel-buffering": "no"},
    )


mock_mcp_app = Starlette(routes=[Route("/mcp", mock_mcp, methods=["POST"])])


@asynccontextmanager
async def lifespan(app):
    app.state.limiter = await asyncio.to_thread(SqliteRateLimiter, os.getenv("SQLITE_PATH", "data/gateway.sqlite"))
    async with httpx.AsyncClient(timeout=30) as client, httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mock_mcp_app)
    ) as mock_client:
        app.state.client = client
        app.state.mock_client = mock_client
        yield


app = Starlette(lifespan=lifespan, routes=[
    Route("/mcp", proxy_mcp, methods=["POST"]),
    Route("/v1/chat/completions", completions, methods=["POST"]),
])
