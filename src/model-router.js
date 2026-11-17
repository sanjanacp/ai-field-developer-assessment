export function estimateTokens(body) {
  const input = JSON.stringify(body.messages ?? body.prompt ?? "");
  const requestedOutput = body.max_tokens ?? body.max_completion_tokens;
  const output = Number.isSafeInteger(requestedOutput) && requestedOutput > 0 ? requestedOutput : 0;
  return Math.ceil(input.length / 4) + output;
}

export async function routeCompletion(body, env = process.env) {
  if (!env.PRIMARY_LLM_URL || !env.BACKUP_LLM_URL) throw new Error("upstream unavailable");
  const headers = { "content-type": "application/json" };
  if (env.LLM_API_KEY) headers.authorization = `Bearer ${env.LLM_API_KEY}`;
  const request = { method: "POST", headers, body: JSON.stringify(body) };

  try {
    const primary = await fetch(env.PRIMARY_LLM_URL, { ...request, signal: AbortSignal.timeout(3000) });
    if (primary.status !== 429) return primary;
    await primary.body?.cancel();
  } catch (error) {
    if (error.name !== "TimeoutError" && error.name !== "AbortError") throw error;
  }

  return fetch(env.BACKUP_LLM_URL, { ...request, signal: AbortSignal.timeout(30_000) });
}

export function gatewayError(res, status = 502) {
  if (res.headersSent) {
    res.destroy();
    return;
  }
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify({ error: { type: "gateway_error", code: status === 429 ? "rate_limit_exceeded" : "upstream_failure", message: status === 429 ? "Token limit exceeded" : "Upstream request failed" } }));
}
