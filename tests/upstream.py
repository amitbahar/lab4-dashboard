#!/usr/bin/env python3
"""A fake Anthropic Messages API upstream for testing proxy.py. Stdlib only.

    python3 tests/upstream.py --port 0            # prints "PORT <n>" once listening
    python3 tests/upstream.py --scenario slow     # default scenario for every request

Scenario selection, first match wins:
  1. request header `x-test-scenario: <name>`
  2. a keyword in the text of the last message (see KEYWORDS), e.g. "please FORCE_ERROR"
  3. the server's --scenario default (normally "success")

Scenarios (see SCENARIOS): success, tools, multi_tool, agents, json, http_error,
stream_error, drop, slow, busy, bad_tool_json. A request with `"stream": false`
(or no `stream`) gets the scenario's reply as one JSON message; drop, slow, busy
and stream_error only differ when streaming. `POST .../count_tokens` returns
`{"input_tokens": n}`; `HEAD` and `GET` of any path return 200.

Every reply is a pure function of the request body, so sending the same request
twice (directly and through the proxy) yields byte-identical bodies.

The pure builders (`reply_blocks`, `sse_events`, `sse_bytes`, `json_reply`) are
also used by tests/make_fixtures.py, so fixtures and the live fake agree.

Test-only endpoints (call them on the upstream directly, not via the proxy):
  GET  /__log    every request seen: [{method, path, headers, body_b64}]
  POST /__reset  clear the log
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCENARIOS = ("success", "tools", "multi_tool", "agents", "json", "http_error", "stream_error",
             "drop", "slow", "busy", "bad_tool_json")
# Checked in order, so MULTI_TOOL wins over TOOL.
KEYWORDS = (
    ("FORCE_ERROR", "http_error"), ("STREAM_ERROR", "stream_error"), ("DROP", "drop"),
    ("SLOW", "slow"), ("BUSY", "busy"), ("BAD_JSON", "bad_tool_json"),
    ("MULTI_TOOL", "multi_tool"), ("AGENTS", "agents"), ("TOOL", "tools"), ("JSON_REPLY", "json"),
)
# The `agents` scenario replies with two Agent calls carrying these prompts, so a
# test can start subagent threads whose first prompt matches the call.
AGENT_PROMPTS = ("Survey the python files. MULTI_TOOL", "Survey the docs. TOOL")
ERROR_BODY = {"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}}
STREAM_ERROR_EVENT = {"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded (mid-stream)"}}

# Tunables (also settable from the command line).
OPTS = {"slow_delay": 0.3, "busy_count": 400, "busy_size": 64, "delta_delay": 0.0, "default": "success"}


# ---------------------------------------------------------------------------
# Pure reply builders
# ---------------------------------------------------------------------------

def digest(body: dict | bytes | str) -> str:
    if isinstance(body, dict):
        body = json.dumps(body, sort_keys=True)
    if isinstance(body, str):
        body = body.encode()
    return hashlib.sha1(body).hexdigest()[:12]


def last_text(body: dict) -> str:
    msgs = body.get("messages") or []
    return json.dumps(msgs[-1]) if msgs else ""


def pick_scenario(body: dict, header: str | None = None, default: str = "success") -> str:
    if header and header in SCENARIOS:
        return header
    last = last_text(body)
    for word, name in KEYWORDS:
        if word in last:
            return name
    return default


def _tool(h: str, n: int, name: str, inp: dict) -> dict:
    return {"type": "tool_use", "id": f"toolu_{h}_{n}", "name": name, "input": inp}


def reply_blocks(scenario: str, body: dict) -> list[dict]:
    """The assembled content of the reply for this scenario."""
    h = digest(body)
    if scenario == "busy":
        text = "".join(f"w{i:04d}" + "x" * max(0, OPTS["busy_size"] - 5) for i in range(OPTS["busy_count"]))
        return [{"type": "text", "text": text}]
    blocks: list[dict] = [
        {"type": "thinking", "thinking": "Let me think about this.", "signature": "sig_" + h},
        {"type": "text", "text": "The proxy **streams** each chunk.\n\n- one\n- `two`\n"},
    ]
    if scenario in ("tools", "bad_tool_json", "slow"):
        blocks.append(_tool(h, 1, "Bash", {"command": "ls -la", "description": "List files"}))
    elif scenario == "multi_tool":
        blocks += [_tool(h, 1, "Read", {"file_path": "/tmp/a.py"}),
                   _tool(h, 2, "Read", {"file_path": "/tmp/b.py"}),
                   _tool(h, 3, "Bash", {"command": "wc -l /tmp/a.py /tmp/b.py"})]
    elif scenario == "agents":
        blocks += [_tool(h, k + 1, "Agent", {"description": f"Survey {k}", "prompt": p})
                   for k, p in enumerate(AGENT_PROMPTS)]
    return blocks


def usage_for(body: dict, out_tokens: int) -> dict:
    n = len(json.dumps(body))
    return {"input_tokens": 12, "cache_creation_input_tokens": 900, "cache_read_input_tokens": n // 4,
            "output_tokens": out_tokens}


def json_reply(scenario: str, body: dict) -> dict:
    blocks = reply_blocks(scenario, body)
    tools = any(b["type"] == "tool_use" for b in blocks)
    return {"id": "msg_" + digest(body), "type": "message", "role": "assistant",
            "model": body.get("model") or "claude-sonnet-4-5", "content": blocks,
            "stop_reason": "tool_use" if tools else "end_turn", "stop_sequence": None,
            "usage": usage_for(body, 312)}


def split(s: str, n: int) -> list[str]:
    """Split s into about n pieces (at least one)."""
    if not s:
        return [""]
    step = max(1, -(-len(s) // n))
    return [s[i:i + step] for i in range(0, len(s), step)]


def sse_events(scenario: str, body: dict) -> list[tuple[str, dict]]:
    """The SSE events of a streamed reply, as (event name, data) pairs.

    For `stream_error` the list ends with the `error` event; for `drop` it ends
    where the connection should be cut (the server does that, not this list).
    """
    blocks = reply_blocks(scenario, body)
    tools = any(b["type"] == "tool_use" for b in blocks)
    msg = json_reply(scenario, body)
    ev: list[tuple[str, dict]] = [("message_start", {"type": "message_start", "message": {
        "id": msg["id"], "type": "message", "role": "assistant", "model": msg["model"], "content": [],
        "stop_reason": None, "stop_sequence": None, "usage": {**usage_for(body, 1)}}})]
    ev.append(("ping", {"type": "ping"}))
    for i, b in enumerate(blocks):
        t = b["type"]
        if t == "thinking":
            ev.append(("content_block_start", {"type": "content_block_start", "index": i,
                                               "content_block": {"type": "thinking", "thinking": "", "signature": ""}}))
            for p in split(b["thinking"], 3):
                ev.append(("content_block_delta", {"type": "content_block_delta", "index": i,
                                                   "delta": {"type": "thinking_delta", "thinking": p}}))
            ev.append(("content_block_delta", {"type": "content_block_delta", "index": i,
                                               "delta": {"type": "signature_delta", "signature": b["signature"]}}))
        elif t == "text":
            ev.append(("content_block_start", {"type": "content_block_start", "index": i,
                                               "content_block": {"type": "text", "text": ""}}))
            if scenario == "busy":
                pieces = [b["text"][k:k + OPTS["busy_size"]] for k in range(0, len(b["text"]), OPTS["busy_size"])]
            else:
                pieces = split(b["text"], 4)
            for p in pieces:
                ev.append(("content_block_delta", {"type": "content_block_delta", "index": i,
                                                   "delta": {"type": "text_delta", "text": p}}))
            if scenario == "stream_error":
                ev.append(("error", STREAM_ERROR_EVENT))
                return ev
            if scenario == "drop":
                return ev
        elif t == "tool_use":
            ev.append(("content_block_start", {"type": "content_block_start", "index": i, "content_block": {
                "type": "tool_use", "id": b["id"], "name": b["name"], "input": {}}}))
            js = json.dumps(b["input"])
            if scenario == "bad_tool_json":
                js = js[:-1] + ', "oops": '   # truncated: not valid JSON
            for p in split(js, 3):
                ev.append(("content_block_delta", {"type": "content_block_delta", "index": i,
                                                   "delta": {"type": "input_json_delta", "partial_json": p}}))
        ev.append(("content_block_stop", {"type": "content_block_stop", "index": i}))
    ev.append(("message_delta", {"type": "message_delta",
                                 "delta": {"stop_reason": "tool_use" if tools else "end_turn", "stop_sequence": None},
                                 "usage": {"output_tokens": 312}}))
    ev.append(("message_stop", {"type": "message_stop"}))
    return ev


def sse_frame(name: str, data: dict) -> bytes:
    return f"event: {name}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n".encode()


def sse_bytes(events: list[tuple[str, dict]]) -> bytes:
    return b"".join(sse_frame(n, d) for n, d in events)


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

LOG: list[dict] = []
LOG_LOCK = threading.Lock()


class Upstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):  # noqa: A002
        pass

    def _record(self, body: bytes) -> None:
        with LOG_LOCK:
            LOG.append({"method": self.command, "path": self.path, "headers": list(self.headers.items()),
                        "body_b64": base64.b64encode(body).decode()})

    def _send_json(self, status: int, obj, reason: str | None = None) -> None:
        out = json.dumps(obj).encode()
        self.send_response(status, reason)
        self.send_header("Content-Type", "application/json")
        self.send_header("request-id", "req_test")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(out)

    def _body(self) -> bytes:
        n = int(self.headers.get("content-length") or 0)
        return self.rfile.read(n) if n else b""

    def do_HEAD(self):
        self._record(b"")
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path == "/__log":
            with LOG_LOCK:
                return self._send_json(200, LOG)
        self._record(b"")
        self._send_json(200, {"data": [{"id": "claude-sonnet-4-5", "type": "model"}]})

    def do_POST(self):
        raw = self._body()
        if self.path == "/__reset":
            with LOG_LOCK:
                LOG.clear()
            return self._send_json(200, {"ok": True})
        self._record(raw)
        try:
            body = json.loads(raw) if raw else {}
        except ValueError:
            return self._send_json(400, {"type": "error", "error": {"type": "invalid_request_error",
                                                                     "message": "bad json"}})
        if self.path.split("?")[0].endswith("/count_tokens"):
            return self._send_json(200, {"input_tokens": len(raw) // 4})
        scenario = pick_scenario(body, self.headers.get("x-test-scenario"), OPTS["default"])
        if scenario == "http_error":
            return self._send_json(529, ERROR_BODY, "Overloaded")
        if not body.get("stream"):
            return self._send_json(200, json_reply(scenario, body))
        self.stream(scenario, body)

    def stream(self, scenario: str, body: dict) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("request-id", "req_test")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        delay = OPTS["slow_delay"] if scenario == "slow" else OPTS["delta_delay"]

        def chunk(b: bytes) -> None:
            self.wfile.write(b"%x\r\n%s\r\n" % (len(b), b))
            self.wfile.flush()

        try:
            for name, data in sse_events(scenario, body):
                chunk(sse_frame(name, data))
                if delay:
                    time.sleep(delay)
            if scenario == "drop":
                time.sleep(0.1)
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True
                return
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except OSError:
            self.close_connection = True  # the proxy (or client) hung up


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def serve(port: int = 0, host: str = "127.0.0.1") -> tuple[Server, int]:
    """Start the fake upstream on a background thread; returns (server, port)."""
    srv = Server((host, port), Upstream)
    threading.Thread(target=srv.serve_forever, name="upstream", daemon=True).start()
    return srv, srv.server_address[1]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--port", type=int, default=0, help="0 picks a free port (printed as 'PORT n')")
    ap.add_argument("--scenario", default="success", choices=SCENARIOS)
    ap.add_argument("--slow-delay", type=float, default=OPTS["slow_delay"], help="seconds between events (slow)")
    ap.add_argument("--delta-delay", type=float, default=0.0, help="seconds between events (all streams)")
    ap.add_argument("--busy-count", type=int, default=OPTS["busy_count"])
    ap.add_argument("--busy-size", type=int, default=OPTS["busy_size"])
    a = ap.parse_args()
    OPTS.update(default=a.scenario, slow_delay=a.slow_delay, delta_delay=a.delta_delay,
                busy_count=a.busy_count, busy_size=a.busy_size)
    srv = Server(("127.0.0.1", a.port), Upstream)
    print(f"PORT {srv.server_address[1]}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
