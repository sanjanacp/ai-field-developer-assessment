import asyncio
import json
import re
from contextlib import aclosing

import httpx
from anyio import CancelScope

from .pii_redactor import PiiRedactor


MAX_EVENT_BYTES = 256 * 1024
LINE_BREAK = re.compile(rb"\r\n|\r|\n")
UPSTREAM_ERROR = {
    "error": {"type": "gateway_error", "code": "upstream_failure", "message": "Upstream request failed"}
}


def token_reservation(body: dict) -> int:
    caps = {"max_tokens", "max_completion_tokens"} & body.keys()
    if len(caps) > 1:
        raise ValueError("Specify only one output token limit")
    cap = next(iter(caps), "max_tokens")
    output = body.setdefault(cap, 1024)
    count = body.get("n", 1)
    if type(output) is not int or output < 1 or type(count) is not int or not 1 <= count <= 16:
        raise ValueError("Invalid output token limit or choice count")
    prompt = json.dumps(body, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return (len(prompt.encode()) + 3) // 4 + output * count


async def sse_data(response):
    line = bytearray()
    fields = []
    size = 0
    trailing_cr = False
    async for chunk in response.aiter_bytes():
        if not chunk:
            continue
        start = int(trailing_cr and chunk.startswith(b"\n"))
        trailing_cr = chunk.endswith(b"\r")
        for delimiter in LINE_BREAK.finditer(chunk, start):
            part = chunk[start:delimiter.start()]
            size += len(part) + len(delimiter[0])
            if size > MAX_EVENT_BYTES:
                raise ValueError("Oversized SSE event")
            line.extend(part)
            start = delimiter.end()
            if not line:
                if fields:
                    yield b"\n".join(fields)
                    fields.clear()
                size = 0
            elif line.startswith(b"data:"):
                value = bytes(line[5:])
                fields.append(value.removeprefix(b" "))
            line.clear()
        size += len(chunk) - start
        if size > MAX_EVENT_BYTES:
            raise ValueError("Oversized SSE event")
        line.extend(chunk[start:])


def sse_frame(event):
    return b"data: " + json.dumps(event, separators=(",", ":"), allow_nan=False).encode() + b"\n\n"


def redact_event(data, redactors, count):
    event = json.loads(data)
    if not isinstance(event, dict) or "error" in event or not isinstance(event.get("choices"), list):
        raise ValueError("Invalid completion event")
    for choice in event["choices"]:
        if not isinstance(choice, dict):
            raise ValueError("Invalid completion choice")
        index = choice.get("index")
        delta = choice.get("delta")
        if type(index) is not int or not 0 <= index < count or not isinstance(delta, dict):
            raise ValueError("Invalid completion delta")
        if any(value is not None for key, value in delta.items() if key not in {"role", "content"}):
            raise ValueError("Unsupported completion delta")
        text = delta.get("content")
        if text is not None and not isinstance(text, str):
            raise ValueError("Invalid completion text")
        if index not in redactors:
            redactors[index] = PiiRedactor()
        finished = choice.get("finish_reason") is not None
        clean = redactors[index].push(text or "", final=finished)
        if text is not None or clean:
            delta["content"] = clean
        if finished:
            del redactors[index]
        if "logprobs" in choice:
            choice["logprobs"] = None
    return event


async def redacted_sse(upstream, count):
    redactors = {}
    metadata = {}
    async with aclosing(sse_data(upstream)) as events:
        async for data in events:
            if data == b"[DONE]":
                choices = []
                for index, redactor in redactors.items():
                    tail = redactor.push(final=True)
                    if tail:
                        choices.append({"index": index, "delta": {"content": tail}, "finish_reason": None})
                if choices:
                    yield sse_frame({**metadata, "choices": choices})
                yield b"data: [DONE]\n\n"
                return
            event = redact_event(data, redactors, count)
            for key in ("id", "object", "model", "created", "system_fingerprint", "service_tier"):
                if key in event:
                    metadata[key] = event[key]
            yield sse_frame(event)
    raise ValueError("Incomplete upstream stream")


async def open_completion(client, url, key, body, timeout):
    upstream = None
    events = None
    try:
        async with asyncio.timeout(timeout):
            headers = {"accept": "text/event-stream", "accept-encoding": "identity"}
            if key:
                headers["authorization"] = f"Bearer {key}"
            request = httpx.Request("POST", url, headers=headers, json=body)
            upstream = await client.send(request, stream=True)
            upstream.raise_for_status()
            if upstream.headers.get("content-type", "").split(";")[0].strip().lower() != "text/event-stream":
                raise ValueError("Expected an SSE response")
            events = redacted_sse(upstream, body.get("n", 1))
            return upstream, events, await anext(events)
    except BaseException:
        with CancelScope(shield=True):
            if events is not None:
                await events.aclose()
            if upstream is not None:
                await upstream.aclose()
        raise


async def route_completion(client, body, primary, backup, backup_model=None):
    try:
        return await open_completion(client, *primary, body, timeout=3)
    except httpx.HTTPStatusError as error:
        if error.response.status_code != 429:
            raise
    except (TimeoutError, httpx.TimeoutException):
        pass
    if backup_model:
        body = {**body, "model": backup_model}
    return await open_completion(client, *backup, body, timeout=30)


async def stream_completion(events, first):
    async with aclosing(events):
        yield first
        try:
            async for frame in events:
                yield frame
        except (httpx.HTTPError, ValueError, RecursionError):
            yield sse_frame(UPSTREAM_ERROR)
