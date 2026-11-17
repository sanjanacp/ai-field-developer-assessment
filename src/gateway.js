import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { SqliteRateLimiter } from "./rate-limiter.js";
import { estimateTokens, gatewayError, routeCompletion } from "./model-router.js";
import { PiiRedactor } from "./pii-redactor.js";

const port = Number(process.env.PORT ?? 3000);
const limiter = new SqliteRateLimiter(process.env.SQLITE_PATH ?? "data/gateway.sqlite");

function readJson(req, maxBytes = 1_000_000) {
  return new Promise((resolve, reject) => {
    let size = 0;
    let body = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      size += Buffer.byteLength(chunk);
      if (size > maxBytes) {
        reject(Object.assign(new Error("body too large"), { status: 413 }));
        req.destroy();
      } else body += chunk;
    });
    req.on("end", () => {
      try { resolve(JSON.parse(body)); } catch { reject(Object.assign(new Error("invalid JSON"), { status: 400 })); }
    });
    req.on("error", reject);
  });
}

function sendJson(res, status, body) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

function bearer(req) {
  const match = /^Bearer\s+(.+)$/i.exec(req.headers.authorization ?? "");
  return match?.[1];
}

function roleFor(token) {
  if (process.env.MCP_ADMIN_TOKEN && token === process.env.MCP_ADMIN_TOKEN) return "admin";
  if (process.env.MCP_VIEWER_TOKEN && token === process.env.MCP_VIEWER_TOKEN) return "viewer";
  return null;
}

function rpcError(id, code, message) {
  return { jsonrpc: "2.0", id: id ?? null, error: { code, message } };
}

async function mockMcp(body, res) {
  if (body.method === "tools/list") {
    return sendJson(res, 200, { jsonrpc: "2.0", id: body.id, result: { tools: [{ name: "get_status" }, { name: "admin_reset_key" }] } });
  }
  if (body.method === "tools/call") {
    return sendJson(res, 200, { jsonrpc: "2.0", id: body.id, result: { content: [{ type: "text", text: `Mock result for ${body.params?.name ?? "unknown"}` }] } });
  }
  return sendJson(res, 200, rpcError(body.id, -32601, "Method not found"));
}

async function proxyMcp(req, res) {
  let body;
  try { body = await readJson(req); } catch (error) { return sendJson(res, error.status ?? 400, rpcError(null, -32700, "Invalid JSON")); }
  if (!body || body.jsonrpc !== "2.0" || typeof body.method !== "string") return sendJson(res, 400, rpcError(body?.id, -32600, "Invalid Request"));

  const token = bearer(req);
  const role = token && roleFor(token);
  if (!role) return sendJson(res, 401, rpcError(body.id, -32001, "Unauthorized"));

  if (body.method === "tools/call" && typeof body.params?.name === "string" && body.params.name.startsWith("admin_") && role !== "admin") {
    return sendJson(res, 200, rpcError(body.id, -32001, "Unauthorized Tool Call"));
  }

  try {
    if (process.env.MCP_DOWNSTREAM_URL) {
      const upstream = await fetch(process.env.MCP_DOWNSTREAM_URL, {
        method: "POST",
        headers: { "content-type": req.headers["content-type"] ?? "application/json" },
        body: JSON.stringify(body),
      });
      res.writeHead(upstream.status, { "content-type": upstream.headers.get("content-type") ?? "application/json" });
      return res.end(Buffer.from(await upstream.arrayBuffer()));
    }
    return mockMcp(body, res);
  } catch {
    return sendJson(res, 502, rpcError(body.id, -32603, "Downstream unavailable"));
  }
}

async function writeSse(response, res) {
  const decoder = new TextDecoder();
  let buffer = "";
  let done = false;
  const redactors = new Map();
  const reader = response.body.getReader();

  const frame = (lines) => {
    const data = lines.filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trimStart()).join("\n");
    if (!data || data === "[DONE]") {
      if (data === "[DONE]" && !done) {
        for (const [index, redactor] of redactors) {
          const tail = redactor.push("", true);
          if (tail) res.write(`data: ${JSON.stringify({ choices: [{ index, delta: { content: tail } }] })}\n\n`);
        }
        done = true;
      }
      res.write(`${lines.join("\n")}\n\n`);
      return;
    }
    try {
      const event = JSON.parse(data);
      for (const choice of event.choices ?? []) {
        const content = choice.delta?.content;
        if (typeof content !== "string") continue;
        const index = choice.index ?? 0;
        const redactor = redactors.get(index) ?? new PiiRedactor();
        redactors.set(index, redactor);
        choice.delta.content = redactor.push(content);
      }
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    } catch {
      res.write("data: {\"error\":\"Invalid upstream stream event\"}\n\n");
    }
  };

  try {
    while (true) {
      const { value, done: ended } = await reader.read();
      if (ended) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() ?? "";
      let eventLines = [];
      for (const line of lines) {
        if (line === "") {
          frame(eventLines);
          eventLines = [];
        } else eventLines.push(line);
      }
      if (eventLines.length) buffer = `${eventLines.join("\n")}\n${buffer}`;
    }
    buffer += decoder.decode();
    if (buffer.trim()) frame(buffer.split(/\r?\n/));
    if (!done) {
      for (const [index, redactor] of redactors) {
        const tail = redactor.push("", true);
        if (tail) res.write(`data: ${JSON.stringify({ choices: [{ index, delta: { content: tail } }] })}\n\n`);
      }
    }
    res.end();
  } catch {
    res.destroy();
  }
}

async function proxyCompletion(req, res) {
  let body;
  try { body = await readJson(req); } catch (error) { return gatewayError(res, error.status ?? 400); }
  if (!body || typeof body !== "object" || Array.isArray(body)) return gatewayError(res, 400);
  const token = bearer(req);
  if (!token) return gatewayError(res, 401);
  const tenant = createHash("sha256").update(token).digest("hex");
  let reservation;
  try { reservation = limiter.consume(tenant, estimateTokens(body)); } catch { return gatewayError(res); }
  if (!reservation.allowed) return gatewayError(res, 429);

  try {
    const upstream = await routeCompletion(body);
    if (!upstream.ok) return gatewayError(res);
    res.writeHead(upstream.status, {
      "content-type": upstream.headers.get("content-type") ?? "application/json",
      "cache-control": "no-cache",
      "x-accel-buffering": "no",
    });
    if (upstream.headers.get("content-type")?.includes("text/event-stream") && upstream.body) return await writeSse(upstream, res);
    res.end(Buffer.from(await upstream.arrayBuffer()));
  } catch {
    gatewayError(res);
  }
}

const server = createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/health") return sendJson(res, 200, { status: "ok" });
  if (req.method === "POST" && req.url === "/mcp") return proxyMcp(req, res);
  if (req.method === "POST" && req.url === "/mock-mcp") {
    try { return await mockMcp(await readJson(req), res); } catch { return sendJson(res, 400, rpcError(null, -32700, "Invalid JSON")); }
  }
  if (req.method === "POST" && req.url === "/v1/chat/completions") return proxyCompletion(req, res);
  sendJson(res, 404, { error: "Not found" });
});

server.listen(port, () => process.stderr.write(`Gateway listening on port ${port}\n`));
process.on("SIGINT", () => { limiter.close(); server.close(() => process.exit(0)); });
process.on("SIGTERM", () => { limiter.close(); server.close(() => process.exit(0)); });
