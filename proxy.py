#!/usr/bin/env python3
"""proxy.py: the agent-traffic proxy for Lab 4, with a live dashboard.

A reverse proxy that forwards every request and response unchanged, prints one
line per exchange, and streams a copy of the traffic to a browser dashboard.
Standard library only; nothing to install.

    python3 proxy.py                       # proxy on :8041, dashboard on :8042
    ANTHROPIC_BASE_URL=http://localhost:8041 claude
    open http://localhost:8042

The proxy changes nothing: headers go through untouched (a subscription login
needs its bearer token and its `anthropic-beta` header), and the reply is
streamed back byte for byte as it arrives. The recorder only ever sees copies;
all of its parsing runs on a worker thread, and its failures are logged and
never reach the relay loop.

    python3 proxy.py --port 9000 --dash-port 9001
    python3 proxy.py --verbose             # also print full request/response dumps
    python3 proxy.py --upstream https://chatgpt.com/backend-api/codex   # for Codex
"""

from __future__ import annotations

import argparse
import codecs
import http.client
import json
import os
import queue
import sys
import threading
import time
import traceback
from collections import OrderedDict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

# Headers that describe one hop of the connection, not the message. They are
# never forwarded. `accept-encoding` is dropped too, so the API answers in plain
# text and what we record is readable.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}

# Header values that carry credentials. They are masked everywhere we show them
# (terminal, dashboard, event stream); the forwarded request keeps them intact.
SENSITIVE = {
    "authorization", "x-api-key", "cookie", "set-cookie", "proxy-authorization",
    "x-auth-token",
}

UPSTREAM = "https://api.anthropic.com"
VERBOSE = False
DASHBOARD_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")

# Prices in USD per million tokens: (input, output, cache write 5m, cache read).
# Keyed by model-id prefix; the longest matching prefix wins. Edited by hand.
# Prices as of 2025-11 (https://www.anthropic.com/pricing). A model that matches
# no prefix shows "—" for cost instead of a guess.
PRICES_AS_OF = "2025-11"
PRICES = {
    "claude-opus-4-5": (5.00, 25.00, 6.25, 0.50),
    "claude-opus-4-1": (15.00, 75.00, 18.75, 1.50),
    "claude-opus-4": (15.00, 75.00, 18.75, 1.50),
    "claude-sonnet-4": (3.00, 15.00, 3.75, 0.30),
    "claude-3-7-sonnet": (3.00, 15.00, 3.75, 0.30),
    "claude-haiku-4-5": (1.00, 5.00, 1.25, 0.10),
    "claude-3-5-haiku": (0.80, 4.00, 1.00, 0.08),
}

# Replay buffer limits (D5).
MAX_EXCHANGES = 500
MAX_BYTES = 200 * 1024 * 1024
# Per-browser queue length; a browser that falls this far behind is dropped
# and re-syncs from the buffer when its EventSource reconnects.
SUB_QUEUE = 20000
# Raw response text kept per exchange for the Raw section.
MAX_RAW = 8 * 1024 * 1024
STREAM_EVENTS = {"block_start", "block_delta", "block_stop"}
# Identifies this proxy run. The page reloads itself when it changes (or when
# dashboard.html is edited), so a tab never keeps running stale code or state.
BOOT_ID = int(time.time() * 1000)


def page_version() -> dict:
    try:
        mtime = os.path.getmtime(DASHBOARD_HTML)
    except OSError:
        mtime = None
    return {"boot": BOOT_ID, "page_mtime": mtime}


def now_ms() -> float:
    return time.time() * 1000


def stamp(t_ms: float | None = None) -> str:
    dt = datetime.fromtimestamp(t_ms / 1000) if t_ms else datetime.now()
    return dt.strftime("%H:%M:%S")


def mask(value: str) -> str:
    value = str(value)
    return (value[:10] if len(value) > 14 else "") + "…(masked)"


def mask_headers(items) -> list[list[str]]:
    return [[k, mask(v) if k.lower() in SENSITIVE else v] for k, v in items]


def price_for(model: str | None):
    if not model:
        return None
    best = max((p for p in PRICES if model.startswith(p)), key=len, default=None)
    return PRICES[best] if best else None


def cost_of(model: str | None, usage: dict | None) -> float | None:
    p = price_for(model)
    if not p or not usage:
        return None
    inp, out, cw, cr = p
    cache = usage.get("cache_creation") or {}
    e5 = cache.get("ephemeral_5m_input_tokens")
    e1 = cache.get("ephemeral_1h_input_tokens")
    if e5 is not None or e1 is not None:
        write = (e5 or 0) * cw + (e1 or 0) * inp * 2
    else:
        write = (usage.get("cache_creation_input_tokens") or 0) * cw
    return (
        (usage.get("input_tokens") or 0) * inp
        + (usage.get("output_tokens") or 0) * out
        + (usage.get("cache_read_input_tokens") or 0) * cr
        + write
    ) / 1e6


def fmt_tok(n: int) -> str:
    if n < 1000:
        return str(n)
    if n < 1_000_000:
        return f"{n / 1000:.1f}k"
    return f"{n / 1e6:.2f}M"


# ---------------------------------------------------------------------------
# Recorder: replay buffer + fan-out to browsers
# ---------------------------------------------------------------------------


class Subscriber:
    def __init__(self) -> None:
        self.q: queue.Queue = queue.Queue(maxsize=SUB_QUEUE)
        self.dead = False

    def offer(self, frame: str) -> None:
        if self.dead:
            return
        try:
            self.q.put_nowait(frame)
        except queue.Full:
            # Too slow: drop everything queued and tell the handler to close.
            # The browser reconnects on its own and replays the buffer.
            self.dead = True
            try:
                while True:
                    self.q.get_nowait()
            except queue.Empty:
                pass
            self.q.put_nowait(None)


def frame(event: str, payload) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"


class Recorder:
    """Keeps the replay buffer and pushes events to every connected browser.

    The relay threads only call `submit`, which never blocks; a single worker
    thread runs the parsing and publishing in order.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.exchanges: OrderedDict[int, list[tuple[str, str]]] = OrderedDict()
        self.sizes: dict[int, int] = {}
        self.total = 0
        self.evicted = 0
        self.subs: set[Subscriber] = set()
        self.next_id = 0
        self.tasks: queue.SimpleQueue = queue.SimpleQueue()
        threading.Thread(target=self._work, name="recorder", daemon=True).start()

    # --- called from relay threads -------------------------------------------------
    def new_id(self) -> int:
        with self.lock:
            self.next_id += 1
            return self.next_id

    def submit(self, fn, *args) -> None:
        try:
            self.tasks.put((fn, args))
        except Exception:  # pragma: no cover - SimpleQueue.put does not fail
            pass

    # --- worker ------------------------------------------------------------------
    def _work(self) -> None:
        while True:
            fn, args = self.tasks.get()
            try:
                fn(*args)
            except Exception:
                print(f"[{stamp()}] recorder error (traffic unaffected):", file=sys.stderr)
                traceback.print_exc()

    def publish(self, xid: int, event: str, payload: dict, compact: bool = False) -> None:
        fr = frame(event, payload)
        with self.lock:
            entry = self.exchanges.get(xid)
            if entry is None and event == "exchange_start":
                entry = self.exchanges[xid] = []
                self.sizes[xid] = 0
            if entry is not None:
                if compact:
                    # The reply is assembled in response_end: drop the deltas.
                    kept = [(e, f) for e, f in entry if e not in STREAM_EVENTS]
                    self.total -= self.sizes[xid]
                    entry[:] = kept
                    self.sizes[xid] = sum(len(f) for _, f in kept)
                    self.total += self.sizes[xid]
                entry.append((event, fr))
                self.sizes[xid] += len(fr)
                self.total += len(fr)
            for sub in list(self.subs):
                sub.offer(fr)
            gone = []
            while len(self.exchanges) > 1 and (
                len(self.exchanges) > MAX_EXCHANGES or self.total > MAX_BYTES
            ):
                old, _ = self.exchanges.popitem(last=False)
                self.total -= self.sizes.pop(old)
                self.evicted += 1
                gone.append(old)
            if gone:
                ev = frame("evicted", {"ids": gone, "total": self.evicted})
                for sub in list(self.subs):
                    sub.offer(ev)

    # --- browsers ----------------------------------------------------------------
    def subscribe(self) -> tuple[Subscriber, list[str]]:
        sub = Subscriber()
        with self.lock:
            hello = frame("reset", {
                **page_version(),
                "evicted": self.evicted,
                "upstream": UPSTREAM,
                "prices": {k: {"input": v[0], "output": v[1], "cache_write": v[2], "cache_read": v[3]}
                           for k, v in PRICES.items()},
                "prices_as_of": PRICES_AS_OF,
            })
            frames = [hello] + [f for entry in self.exchanges.values() for _, f in entry]
            frames.append(frame("replay_done", {"count": len(self.exchanges)}))
            self.subs.add(sub)
        return sub, frames

    def unsubscribe(self, sub: Subscriber) -> None:
        with self.lock:
            self.subs.discard(sub)


REC = Recorder()


class SSEParser:
    """Incremental `text/event-stream` parser: feed bytes, get (event, data)."""

    def __init__(self) -> None:
        self.dec = codecs.getincrementaldecoder("utf-8")("replace")
        self.buf = ""

    def feed(self, chunk: bytes) -> list[tuple[str, str]]:
        self.buf = (self.buf + self.dec.decode(chunk)).replace("\r\n", "\n")
        out = []
        while (i := self.buf.find("\n\n")) >= 0:
            raw, self.buf = self.buf[:i], self.buf[i + 2:]
            event, data = "message", []
            for line in raw.split("\n"):
                if not line or line.startswith(":"):
                    continue
                name, _, val = line.partition(":")
                if val.startswith(" "):
                    val = val[1:]
                if name == "event":
                    event = val
                elif name == "data":
                    data.append(val)
            if data:
                out.append((event, "\n".join(data)))
        return out


class Recording:
    """Everything recorded about one HTTP exchange. Runs on the recorder worker."""

    def __init__(self, xid: int, t0: float, method: str, path: str, headers, body: bytes) -> None:
        self.xid, self.t0, self.method, self.path = xid, t0, method, path
        self.headers = list(headers)
        self.body = body
        self.status: int | None = None
        self.is_sse = False
        self.parser = SSEParser()
        self.raw = bytearray()
        self.raw_truncated = False
        self.resp_bytes = 0
        self.t_first_byte: float | None = None
        self.t_first_token: float | None = None
        self.blocks: dict[int, dict] = {}
        self.partial: dict[int, str] = {}
        self.model = None
        self.message_id = None
        self.usage: dict = {}
        self.stop_reason = None
        self.errors: list[dict] = []
        self.request = None

    def pub(self, event: str, payload: dict, compact: bool = False) -> None:
        payload["id"] = self.xid
        REC.publish(self.xid, event, payload, compact)

    def start(self) -> None:
        req = None
        if self.body:
            try:
                req = json.loads(self.body)
            except ValueError:
                req = self.body.decode("utf-8", "replace")
        self.request = req
        self.pub("exchange_start", {
            "t": self.t0, "method": self.method, "path": self.path,
            "headers": mask_headers(self.headers), "request": req,
            "request_bytes": len(self.body),
        })

    def response_start(self, status: int, reason: str, headers, t: float) -> None:
        self.status = status
        ctype = next((v for k, v in headers if k.lower() == "content-type"), "")
        self.is_sse = "text/event-stream" in ctype
        self.pub("response_start", {
            "t": t, "status": status, "reason": reason, "headers": mask_headers(headers),
        })

    def feed(self, chunk: bytes, t: float) -> None:
        if self.t_first_byte is None:
            self.t_first_byte = t
        self.resp_bytes += len(chunk)
        if len(self.raw) + len(chunk) <= MAX_RAW:
            self.raw += chunk
        else:
            self.raw_truncated = True
        if self.is_sse:
            for event, data in self.parser.feed(chunk):
                self.on_sse(event, data, t)

    def on_sse(self, event: str, data: str, t: float) -> None:
        try:
            d = json.loads(data)
        except ValueError:
            return
        if not isinstance(d, dict):
            return
        typ = d.get("type", event)
        if typ == "message_start":
            m = d.get("message") or {}
            self.model, self.message_id = m.get("model"), m.get("id")
            self.usage = dict(m.get("usage") or {})
            self.pub("response_start", {
                "t": t, "status": self.status, "model": self.model,
                "message_id": self.message_id, "usage": self.usage,
            })
        elif typ == "content_block_start":
            idx = d.get("index", len(self.blocks))
            cb = dict(d.get("content_block") or {})
            self.blocks[idx] = cb
            self.partial[idx] = ""
            payload = {"t": t, "index": idx, "type": cb.get("type"),
                       "name": cb.get("name"), "tool_use_id": cb.get("id")}
            for key in ("text", "thinking", "data"):
                if cb.get(key):
                    payload[key] = cb[key]
            self.pub("block_start", payload)
        elif typ == "content_block_delta":
            idx = d.get("index", 0)
            blk = self.blocks.setdefault(idx, {"type": "text", "text": ""})
            delta = d.get("delta") or {}
            dt = delta.get("type")
            if dt in ("text_delta", "thinking_delta", "input_json_delta") and self.t_first_token is None:
                self.t_first_token = t
            if dt == "text_delta":
                blk["text"] = blk.get("text", "") + delta.get("text", "")
                self.pub("block_delta", {"index": idx, "text": delta.get("text", "")})
            elif dt == "thinking_delta":
                blk["thinking"] = blk.get("thinking", "") + delta.get("thinking", "")
                self.pub("block_delta", {"index": idx, "thinking": delta.get("thinking", "")})
            elif dt == "input_json_delta":
                self.partial[idx] = self.partial.get(idx, "") + delta.get("partial_json", "")
                self.pub("block_delta", {"index": idx, "partial_json": delta.get("partial_json", "")})
            elif dt == "signature_delta":
                blk["signature"] = blk.get("signature", "") + delta.get("signature", "")
            elif dt == "citations_delta":
                blk.setdefault("citations", []).append(delta.get("citation"))
        elif typ == "content_block_stop":
            idx = d.get("index", 0)
            blk = self.blocks.get(idx, {})
            pj = self.partial.get(idx)
            if pj:
                try:
                    blk["input"] = json.loads(pj)
                except ValueError:
                    blk["input"] = {"_partial_json": pj}
            payload = {"t": t, "index": idx}
            if blk.get("type") in ("tool_use", "server_tool_use"):
                payload["input"] = blk.get("input")
            self.pub("block_stop", payload)
        elif typ == "message_delta":
            delta = d.get("delta") or {}
            if delta.get("stop_reason"):
                self.stop_reason = delta["stop_reason"]
            for k, v in (d.get("usage") or {}).items():
                if v is not None:
                    self.usage[k] = v
        elif typ == "error":
            self.error("stream_error", self.status, d.get("error", d), t)

    def error(self, kind: str, status, body, t: float) -> None:
        self.errors.append({"kind": kind, "status": status})
        self.pub("exchange_error", {"t": t, "kind": kind, "status": status, "body": body})

    def unreachable(self, message: str, t: float) -> None:
        self.status = 502
        self.error("unreachable", 502, message, t)
        self.finish(t)

    def finish(self, t: float, aborted: bool = False, read_error: str | None = None) -> None:
        if read_error:
            self.error("stream_error", self.status, {"message": read_error}, t)
        text = self.raw.decode("utf-8", "replace")
        parsed = None
        if not self.is_sse and text:
            try:
                parsed = json.loads(text)
            except ValueError:
                parsed = None
        if self.status and self.status >= 400 and not any(e["kind"] == "http" for e in self.errors) \
                and not any(e["kind"] == "unreachable" for e in self.errors):
            self.error("http", self.status, parsed if parsed is not None else text[:20000], t)
        elif isinstance(parsed, dict) and parsed.get("type") == "message":
            self.model = parsed.get("model")
            self.message_id = parsed.get("id")
            self.usage = parsed.get("usage") or {}
            self.stop_reason = parsed.get("stop_reason")
            self.blocks = dict(enumerate(parsed.get("content") or []))
        self.pub("response_end", {
            "t": t, "status": self.status, "model": self.model, "message_id": self.message_id,
            "stop_reason": self.stop_reason, "usage": self.usage,
            "content": [self.blocks[i] for i in sorted(self.blocks)],
            "raw": text, "raw_truncated": self.raw_truncated,
            "response_bytes": self.resp_bytes,
            "t_first_byte": self.t_first_byte, "t_first_token": self.t_first_token,
            "aborted": aborted, "json": parsed if not self.is_sse else None,
        }, compact=True)
        self.log_line(t, aborted)

    def log_line(self, t: float, aborted: bool) -> None:
        parts = [stamp(self.t0), f"#{self.xid}", self.method, self.path,
                 str(self.status or "---"), f"{(t - self.t0) / 1000:.1f}s"]
        if self.usage:
            u = self.usage
            tin = sum(u.get(k) or 0 for k in
                      ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"))
            parts.append(f"{fmt_tok(tin)}/{fmt_tok(u.get('output_tokens') or 0)}")
            c = cost_of(self.model, u)
            parts.append(f"${c:.2f}" if c is not None else "$—")
        if self.errors:
            parts.append("ERROR " + ",".join(e["kind"] for e in self.errors))
        if aborted:
            parts.append("(client closed)")
        print(" ".join(parts), flush=True)


# ---------------------------------------------------------------------------
# The proxy
# ---------------------------------------------------------------------------


class Tap(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass  # we print our own lines

    def do_POST(self) -> None:
        self.relay()

    def do_GET(self) -> None:
        self.relay()

    def do_HEAD(self) -> None:
        self.relay()

    def dump_request(self, body: bytes) -> None:
        print(f"\n{'=' * 78}\n[{stamp()}] >>> {self.command} {self.path}", flush=True)
        for name, value in self.headers.items():
            if name.lower() in SENSITIVE:
                value = mask(value)
            print(f"    {name}: {value}")
        if body:
            try:
                print(json.dumps(json.loads(body), indent=2, ensure_ascii=False))
            except ValueError:
                print(body.decode("utf-8", "replace"))

    def relay(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        rec = Recording(REC.new_id(), now_ms(), self.command, self.path, self.headers.items(), body)
        REC.submit(rec.start)
        if VERBOSE:
            self.dump_request(body)

        # Forward it, headers intact.
        target = urlsplit(UPSTREAM)
        conn_cls = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        # Connect with a short timeout (a lost SYN must not stall the agent for
        # minutes); the long timeout is for reading a slow stream.
        upstream = conn_cls(target.netloc, timeout=10)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
        try:
            upstream.connect()
            upstream.sock.settimeout(600)
            upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
            reply = upstream.getresponse()
        except (OSError, http.client.HTTPException) as e:
            REC.submit(rec.unreachable, f"cannot reach {UPSTREAM}: {e}", now_ms())
            if VERBOSE:
                print(f"\n[{stamp()}] !!! cannot reach {UPSTREAM}: {e}", flush=True)
            self.send_error(502, f"proxy: cannot reach {UPSTREAM}: {e}")
            self.close_connection = True
            return

        # Stream the reply back as it arrives; the recorder gets a copy of each chunk.
        resp_headers = reply.getheaders()
        REC.submit(rec.response_start, reply.status, reply.reason, resp_headers, now_ms())
        if VERBOSE:
            print(f"\n[{stamp()}] <<< {reply.status} {reply.reason}", flush=True)
        self.close_connection = True
        aborted, read_error = False, None
        # Keep the upstream's body framing, so a stream the upstream cuts off also reaches the agent
        # as cut off: a chunked reply is re-chunked (the final 0-chunk is sent only on a clean end),
        # and a fixed-length reply keeps its Content-Length. Body bytes are unchanged either way.
        chunked = bool(reply.chunked)
        length = reply.getheader("content-length")
        try:
            # send_response_only: no proxy Server/Date headers next to the upstream's own.
            self.send_response_only(reply.status, reply.reason)
            for name, value in resp_headers:
                if name.lower() not in SKIP and name.lower() != "content-encoding":
                    self.send_header(name, value)
            if chunked:
                self.send_header("Transfer-Encoding", "chunked")
            elif length is not None:
                self.send_header("Content-Length", length)
            self.send_header("Connection", "close")
            self.end_headers()
        except OSError:
            aborted = True  # the agent hung up before the reply started
        try:
            while self.command != "HEAD" and not aborted:
                try:
                    chunk = reply.read1(4096)  # read1: pass on each chunk as it lands
                except (OSError, http.client.HTTPException) as e:
                    # e.g. IncompleteRead when the upstream drops a chunked stream
                    read_error = f"upstream read failed: {type(e).__name__}: {e}"
                    break
                if not chunk:
                    if chunked:
                        try:
                            self.wfile.write(b"0\r\n\r\n")  # clean end of a chunked body
                            self.wfile.flush()
                        except OSError:
                            aborted = True
                    break
                try:
                    self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk) if chunked else chunk)
                    self.wfile.flush()
                except OSError:
                    aborted = True  # the agent hung up (e.g. interrupted)
                    REC.submit(rec.feed, chunk, now_ms())
                    break
                REC.submit(rec.feed, chunk, now_ms())
                if VERBOSE:
                    sys.stdout.write(chunk.decode("utf-8", "replace"))
                    sys.stdout.flush()
        except BaseException as e:
            read_error = read_error or f"proxy relay failed: {type(e).__name__}: {e}"
            raise
        finally:
            # Always close out the exchange, or its card stays "in flight" forever.
            REC.submit(rec.finish, now_ms(), aborted, read_error)
        if VERBOSE:
            print(f"\n[{stamp()}] --- done", flush=True)


# ---------------------------------------------------------------------------
# The dashboard server
# ---------------------------------------------------------------------------


class Dash(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            try:
                with open(DASHBOARD_HTML, "rb") as f:
                    page = f.read()
            except OSError as e:
                self.send_error(500, f"cannot read {DASHBOARD_HTML}: {e}")
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(page)
        elif path == "/events":
            self.stream()
        else:
            self.send_error(404)

    def stream(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        sub, frames = REC.subscribe()
        try:
            batch: list[str] = []
            size = 0
            for fr in frames:
                batch.append(fr)
                size += len(fr)
                if size > 1 << 20:
                    self.wfile.write("".join(batch).encode())
                    batch, size = [], 0
            self.wfile.write(("".join(batch) + "retry: 1000\n\n").encode())
            self.wfile.flush()
            while True:
                try:
                    fr = sub.q.get(timeout=15)
                except queue.Empty:
                    fr = frame("ping", page_version())  # keep-alive; also lets the page notice a restart or an edit
                if fr is None:
                    break  # fell behind; the browser reconnects and re-syncs
                out = [fr]
                while len(out) < 500:  # write whatever else is already waiting
                    try:
                        nxt = sub.q.get_nowait()
                    except queue.Empty:
                        break
                    if nxt is None:
                        sub.dead = True
                        break
                    out.append(nxt)
                self.wfile.write("".join(out).encode())
                self.wfile.flush()
                if sub.dead:
                    break
        except OSError:
            pass
        finally:
            REC.unsubscribe(sub)


def main() -> None:
    global UPSTREAM, VERBOSE
    parser = argparse.ArgumentParser(description="Forward an agent's API traffic and show it live.")
    parser.add_argument("--port", type=int, default=8041)
    parser.add_argument("--dash-port", type=int, default=8042, help="dashboard port (default 8042)")
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--verbose", action="store_true", help="print full request and response dumps")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    VERBOSE = args.verbose

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Tap)
    server.daemon_threads = True
    try:
        dash = ThreadingHTTPServer(("127.0.0.1", args.dash_port), Dash)
        dash.daemon_threads = True
        threading.Thread(target=dash.serve_forever, name="dashboard", daemon=True).start()
        dash_line = f"dashboard on      http://localhost:{args.dash_port}"
    except OSError as e:
        dash_line = f"dashboard DISABLED (port {args.dash_port}: {e}); proxying continues"

    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(dash_line)
    print(f"run the agent with:  ANTHROPIC_BASE_URL=http://localhost:{args.port} claude")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
