# AI Field Developer Assessment

The Python solution requires Python 3.11 or newer. Run the commands from this folder.

```sh
uv venv --python 3.12
uv pip install -r requirements.txt
```

## 1. MCP tools over stdio

```sh
.venv/bin/python -m src.mcp_server
```

Use this command as the launch configuration in an MCP client. The official SDK handles initialization, discovery, and transport. Pydantic rejects missing and extra fields, type coercion, IDs outside `CUST-[A-Z0-9]{5}`, non-finite or non-positive amounts, and refund reasons shorter than 10 characters after trimming. Invalid tool arguments return JSON-RPC `-32602`. The SDK writes JSON-RPC to stdout and Python logging uses stderr. Both tools return demo data.

## 2. MCP security gateway

```sh
MCP_ADMIN_TOKEN=dev-admin MCP_VIEWER_TOKEN=dev-viewer LLM_TENANT_KEYS=dev-tenant \
  .venv/bin/python -m uvicorn src.gateway:app --port 3000
```

```sh
curl -sS http://127.0.0.1:3000/mcp \
  -H 'Authorization: Bearer dev-viewer' -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

curl -sS http://127.0.0.1:3000/mcp \
  -H 'Authorization: Bearer dev-viewer' -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"admin_reset_key"}}'
```

The gateway authenticates callers using configured bearer tokens. Both roles receive the downstream `tools/list` result, including admin tool names. A viewer call to `admin_*` returns `-32001` with the original request ID before any downstream request. The gateway forwards approved JSON-RPC messages and MCP protocol/session headers and relays the downstream HTTP status and body. `MCP_DOWNSTREAM_URL` points to an external MCP HTTP endpoint. Without it, the gateway uses an in-process mock downstream.

## 3. Streaming PII guardrail

Set these variables before starting the same gateway:

| Variable | Value |
| --- | --- |
| `LLM_TENANT_KEYS` | Comma-separated list of gateway tenant API keys |
| `PRIMARY_LLM_URL`, `BACKUP_LLM_URL` | Full OpenAI-compatible chat completion URLs |
| `PRIMARY_LLM_API_KEY`, `BACKUP_LLM_API_KEY` | Optional provider credentials |
| `BACKUP_LLM_MODEL` | Optional model name override for the secondary provider |
| `SQLITE_PATH` | Optional SQLite file path, defaults to `data/gateway.sqlite` |

```sh
curl -NsS http://127.0.0.1:3000/v1/chat/completions \
  -H 'Authorization: Bearer dev-tenant' -H 'Content-Type: application/json' \
  -d '{"model":"your-primary-model","messages":[{"role":"user","content":"Write a greeting."}],"max_tokens":100}'
```

The gateway requests SSE and processes each complete event as it arrives. For each choice, it emits safe text immediately and holds only the unfinished runs that may contain an email, SSN, or card number. The guardrail catches matches split across deltas and flushes pending text before completion. Oversized SSE events and unsupported delta types, such as tool calls, end the stream with a sanitized error. This endpoint supports text deltas and conventional ASCII PII formats. Pattern matching can also mask harmless digit sequences or unusually long unbroken text.

## 4. Quota and model fallback

Each request reserves estimated prompt tokens (serialized request UTF-8 bytes divided by four, rounded up) plus the output cap for every requested choice. The gateway supplies a 1,024 token output cap when absent. SQLite stores reservations under the API key hash. Expiration, quota checking, and insertion use one write transaction, so concurrent requests share the same 50,000 token limit over the preceding 60 seconds. The limit measures reservations because admission happens before the provider can report actual usage.

The router gives the primary three seconds to return headers and the first SSE data event. A timeout or HTTP 429 switches to the backup, using the same quota reservation. The initial deadline ends once the stream begins; a later failure returns a generic SSE error. Upstream failures before streaming return a standardized HTTP JSON error.

Run the regression checks with `.venv/bin/python -m unittest discover -s tests -v`.

The existing Node implementation remains available via `npm run mcp` and `npm start`.
