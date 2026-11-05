#!/usr/bin/env python3
"""Tests of proxy.py against the fake upstream in tests/upstream.py. Stdlib unittest only.

    python3 -m unittest tests.test_proxy -v       (from the repo root)
    python3 tests/test_proxy.py -v

proxy.py runs as a subprocess on free ports (never 8041/8042/8080); the fake
upstream runs in-process on a free port. Covers SPEC §3 (events), §10 #9-#10 and
§13.8 #10: byte-identical forwarding, one terminal line per exchange, the event
sequence of every scenario, credential masking, replay on late join, and that a
stalled or vanished /events client never delays relay traffic.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import upstream as up  # noqa: E402

PROXY = os.path.join(ROOT, "proxy.py")
FORBIDDEN_PORTS = {8041, 8042, 8080}
SECRET = "sk-ant-oat01-TESTSECRET-abcdefghijklmnopqrstuvwxyz0123456789"
API_KEY = "sk-ant-api03-APIKEYSECRET-zyxwvutsrqponmlkjihgfedcba"
HOP = {"host", "connection", "keep-alive", "transfer-encoding", "te", "trailers", "upgrade",
       "proxy-authorization", "proxy-authenticate", "accept-encoding", "content-length"}
LINE = re.compile(r"^\d\d:\d\d:\d\d #(\d+) (\S+) (\S+) (\S+) ")


def free_port() -> int:
    for _ in range(50):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        if port not in FORBIDDEN_PORTS:
            return port
    raise RuntimeError("no free port")


def headers(**extra) -> dict:
    return {"content-type": "application/json", "authorization": "Bearer " + SECRET,
            "x-api-key": API_KEY, "anthropic-version": "2023-06-01",
            "anthropic-beta": "claude-code-20250219,interleaved-thinking-2025-05-14",
            "user-agent": "claude-cli/2.0.0 (external, cli)",
            "x-claude-code-session-id": "11111111-aaaa-4aaa-8aaa-000000000001", **extra}


def body_for(text: str, stream: bool = True, **kw) -> dict:
    return {"model": "claude-sonnet-4-5-20250929", "max_tokens": 1000, "stream": stream,
            "system": [{"type": "text", "text": "You are a test."}],
            "tools": [{"name": "Bash", "description": "Run", "input_schema": {"type": "object"}}],
            "messages": [{"role": "user", "content": [{"type": "text", "text": text}]}], **kw}


def request(port: int, method: str, path: str, body: dict | bytes | None = None, hdrs: dict | None = None,
            timeout: float = 30) -> tuple[int, list, bytes, str | None]:
    """Returns (status, headers, body bytes, error); error is set when the body read failed."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    data = json.dumps(body).encode() if isinstance(body, dict) else body
    try:
        conn.request(method, path, body=data, headers=hdrs if hdrs is not None else headers())
        r = conn.getresponse()
        chunks, err = [], None
        while True:
            try:
                c = r.read1(65536) if method != "HEAD" else b""
            except (http.client.IncompleteRead, OSError) as e:
                err = type(e).__name__
                break
            if not c:
                break
            chunks.append(c)
        return r.status, r.getheaders(), b"".join(chunks), err
    finally:
        conn.close()


class Events:
    """A /events subscriber on a raw socket; parses SSE into self.events [(name, data)]."""

    def __init__(self, port: int, read: bool = True, rcvbuf: int | None = None) -> None:
        self.sock = socket.socket()
        if rcvbuf:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        self.sock.connect(("127.0.0.1", port))
        self.sock.sendall(b"GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n")
        self.raw = bytearray()
        self.events: list[tuple[str, dict]] = []
        self.cond = threading.Condition()
        self.closed = False
        if read:
            threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        buf = b""
        head_done = False
        while True:
            try:
                chunk = self.sock.recv(65536)
            except OSError:
                chunk = b""
            if not chunk:
                break
            with self.cond:
                self.raw += chunk
                buf += chunk
                if not head_done:
                    i = buf.find(b"\r\n\r\n")
                    if i < 0:
                        continue
                    buf, head_done = buf[i + 4:], True
                while (i := buf.find(b"\n\n")) >= 0:
                    block, buf = buf[:i].decode(), buf[i + 2:]
                    name, data = "message", []
                    for line in block.split("\n"):
                        k, _, v = line.partition(":")
                        v = v[1:] if v.startswith(" ") else v
                        if k == "event":
                            name = v
                        elif k == "data":
                            data.append(v)
                    if data:
                        self.events.append((name, json.loads("\n".join(data))))
                self.cond.notify_all()
        with self.cond:
            self.closed = True
            self.cond.notify_all()

    def wait(self, pred, timeout: float = 10.0):
        end = time.time() + timeout
        with self.cond:
            while True:
                r = pred(self.events)
                if r:
                    return r
                left = end - time.time()
                if left <= 0:
                    raise AssertionError(f"timed out; events so far: {[e for e, _ in self.events][-30:]}")
                self.cond.wait(left)

    def of(self, xid: int) -> list[tuple[str, dict]]:
        with self.cond:
            return [(e, d) for e, d in self.events if d.get("id") == xid]

    def wait_end(self, xid: int, timeout: float = 10.0) -> list[tuple[str, dict]]:
        self.wait(lambda evs: any(e == "response_end" and d.get("id") == xid for e, d in evs), timeout)
        return self.of(xid)

    def ids(self) -> list[int]:
        with self.cond:
            return [d["id"] for e, d in self.events if e == "exchange_start"]

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()


def names(evs) -> list[str]:
    """Event names with runs of block_delta collapsed to one 'block_delta+'."""
    out: list[str] = []
    for e, _ in evs:
        n = "block_delta+" if e == "block_delta" else e
        if not (out and out[-1] == n == "block_delta+"):
            out.append(n)
    return out


class Proxy:
    """proxy.py in a subprocess, with its stdout lines collected."""

    def __init__(self, upstream_url: str, *extra: str) -> None:
        for attempt in range(5):
            self.port, self.dash = free_port(), free_port()
            if self.port == self.dash:
                continue
            self.proc = subprocess.Popen(
                [sys.executable, "-u", PROXY, "--port", str(self.port), "--dash-port", str(self.dash),
                 "--upstream", upstream_url, *extra],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8")
            self.lines: list[str] = []
            self.err: list[str] = []
            self.cond = threading.Condition()
            threading.Thread(target=self._pump, args=(self.proc.stdout, self.lines), daemon=True).start()
            threading.Thread(target=self._pump, args=(self.proc.stderr, self.err), daemon=True).start()
            try:
                self.wait_line(lambda ls: any("run the agent with" in x for x in ls), 10)
            except AssertionError:
                self.stop()
                continue
            if any("DISABLED" in x for x in self.lines):
                self.stop()
                continue
            self._wait_port(self.port)
            self._wait_port(self.dash)
            return
        raise RuntimeError("could not start proxy.py: " + "".join(self.err[-20:]))

    def _pump(self, f, into) -> None:
        for line in f:
            with self.cond:
                into.append(line.rstrip("\n"))
                self.cond.notify_all()

    def _wait_port(self, port: int) -> None:
        end = time.time() + 5
        while time.time() < end:
            try:
                socket.create_connection(("127.0.0.1", port), 0.5).close()
                return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(f"port {port} not listening")

    def wait_line(self, pred, timeout: float = 10.0):
        end = time.time() + timeout
        with self.cond:
            while not (r := pred(self.lines)):
                left = end - time.time()
                if left <= 0 or (self.proc.poll() is not None and not r):
                    raise AssertionError(f"proxy output: {self.lines[-10:]} {self.err[-10:]}")
                self.cond.wait(min(left, 0.2))
            return r

    def exchange_lines(self) -> list[str]:
        with self.cond:
            return [x for x in self.lines if LINE.match(x)]

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(5)
        for f in (self.proc.stdout, self.proc.stderr):
            try:
                f.close()
            except Exception:
                pass


class ProxyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        up.OPTS.update(slow_delay=0.2, busy_count=400, busy_size=64)
        cls.up_srv, cls.up_port = up.serve(0)
        cls.proxy = Proxy(f"http://127.0.0.1:{cls.up_port}")
        cls.ev = Events(cls.proxy.dash)
        cls.ev.wait(lambda evs: any(e == "replay_done" for e, _ in evs))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.ev.close()
        cls.proxy.stop()
        cls.up_srv.shutdown()
        cls.up_srv.server_close()

    def setUp(self) -> None:
        request(self.up_port, "POST", "/__reset", b"", {})

    # --- helpers -----------------------------------------------------------
    def log(self) -> list[dict]:
        return json.loads(request(self.up_port, "GET", "/__log", None, {})[2])

    def via_proxy(self, method, path, body=None, hdrs=None):
        """Send through the proxy; returns (status, headers, body, err, exchange id, events of it)."""
        before = set(self.ev.ids())
        res = request(self.proxy.port, method, path, body, hdrs)
        xid = self.ev.wait(lambda evs: [d["id"] for e, d in evs if e == "exchange_start" and d["id"] not in before
                                        and d.get("path") == path and d.get("method") == method])[-1]
        return (*res, xid, self.ev.wait_end(xid))

    def compare_direct(self, method, path, body=None, hdrs=None):
        """Same request direct and via the proxy: bodies, statuses and forwarded requests must match."""
        hdrs = headers() if hdrs is None else hdrs
        d_status, d_headers, d_body, d_err = request(self.up_port, method, path, body, hdrs)
        p_status, p_headers, p_body, p_err, xid, evs = self.via_proxy(method, path, body, hdrs)
        self.assertEqual(p_status, d_status)
        self.assertEqual(p_body, d_body, "response body bytes differ between direct and proxied")
        direct, proxied = self.log()[-2:]
        self.assertEqual(proxied["body_b64"], direct["body_b64"], "forwarded request body differs")
        self.assertEqual(proxied["method"], method)
        self.assertEqual(proxied["path"], path)
        fwd = {k.lower(): v for k, v in proxied["headers"]}
        for k, v in hdrs.items():
            if k.lower() not in HOP:
                self.assertEqual(fwd.get(k.lower()), v, f"request header {k} not forwarded intact")
        ph = {k.lower(): v for k, v in p_headers}
        for k, v in d_headers:
            if k.lower() not in HOP and k.lower() not in ("date", "server", "content-encoding"):
                self.assertEqual(ph.get(k.lower()), v, f"response header {k} not relayed")
        return p_status, p_body, xid, evs

    # --- forwarding fidelity (§13.8 #10) ----------------------------------------
    def test_byte_identical_streams(self):
        for text in ("plain", "run TOOL", "MULTI_TOOL", "AGENTS", "BAD_JSON", "STREAM_ERROR", "BUSY"):
            with self.subTest(text=text):
                status, body, _, _ = self.compare_direct("POST", "/v1/messages", body_for(text))
                self.assertEqual(status, 200)
                self.assertTrue(body.startswith(b"event: message_start"))

    def test_byte_identical_json_error_head_get_count(self):
        cases = [("POST", "/v1/messages", body_for("plain", stream=False), 200),
                 ("POST", "/v1/messages", body_for("FORCE_ERROR"), 529),
                 ("POST", "/v1/messages/count_tokens", body_for("x", stream=False), 200),
                 ("POST", "/v1/messages?beta=true", body_for("query string", stream=True), 200),
                 ("GET", "/v1/models", None, 200),
                 ("HEAD", "/", None, 200)]
        for method, path, body, want in cases:
            with self.subTest(method=method, path=path):
                status, _, _, _ = self.compare_direct(method, path, body)
                self.assertEqual(status, want)

    def test_non_json_request_body_forwarded(self):
        raw = b"\x00\xffnot json\n" * 10
        status, _, _, evs = self.compare_direct("POST", "/v1/other", raw)
        self.assertEqual(status, 400)
        start = dict(evs)["exchange_start"]
        self.assertIsInstance(start["request"], str)
        self.assertEqual(start["request_bytes"], len(raw))

    def test_concurrent_requests_byte_identical(self):
        texts = [f"parallel {i} " + ("TOOL" if i % 2 else "MULTI_TOOL") for i in range(6)]
        direct = {t: request(self.up_port, "POST", "/v1/messages", body_for(t))[2] for t in texts}
        out: dict[str, bytes] = {}
        ths = [threading.Thread(target=lambda t=t: out.__setitem__(
            t, request(self.proxy.port, "POST", "/v1/messages", body_for(t))[2])) for t in texts]
        for th in ths:
            th.start()
        for th in ths:
            th.join(30)
        self.assertEqual(out, direct)

    def test_dropped_upstream_stream(self):
        # Direct: the client sees a truncated chunked body. Through the proxy the same bytes
        # arrive, then the connection closes (the proxy relays without chunked framing).
        d_status, _, d_body, d_err = request(self.up_port, "POST", "/v1/messages", body_for("DROP"))
        p_status, _, p_body, p_err, xid, evs = self.via_proxy("POST", "/v1/messages", body_for("DROP"))
        self.assertEqual((d_status, p_status), (200, 200))
        self.assertEqual(d_err, "IncompleteRead")
        self.assertEqual(p_body, d_body, "bytes before the drop must be relayed unchanged")
        n = names(evs)
        self.assertEqual(n[-2:], ["exchange_error", "response_end"], n)
        err = dict(evs)["exchange_error"]
        self.assertEqual(err["kind"], "stream_error")
        self.assertIn("IncompleteRead", err["body"]["message"])
        self.assertNotIn("message_stop", dict(evs)["response_end"]["raw"])

    # Regression: send_response() used to add the proxy's own Server/Date next to the upstream's.
    def test_no_duplicate_response_headers(self):
        _, hdrs, _, _ = request(self.proxy.port, "POST", "/v1/messages", body_for("plain", stream=False))
        keys = [k.lower() for k, _ in hdrs]
        self.assertEqual(sorted(k for k in set(keys) if keys.count(k) > 1), [],
                         f"duplicate response headers: {hdrs}")

    # Regression: the reply used to be relayed close-delimited, so a dropped chunked stream
    # looked like a clean end to the agent. The upstream's chunked framing is now kept.
    def test_dropped_upstream_stream_is_visible_to_client(self):
        _, _, _, err = request(self.proxy.port, "POST", "/v1/messages", body_for("DROP"))
        self.assertIsNotNone(err, "the client should see the truncated stream as an error")

    # --- event sequences (SPEC §3) -------------------------------------------
    def test_events_success_stream(self):
        body = body_for("run TOOL")
        _, _, _, _, xid, evs = self.via_proxy("POST", "/v1/messages", body)
        self.assertEqual(names(evs), [
            "exchange_start", "response_start", "response_start",
            "block_start", "block_delta+", "block_stop",      # thinking
            "block_start", "block_delta+", "block_stop",      # text
            "block_start", "block_delta+", "block_stop",      # tool_use
            "response_end"])
        rs1, rs2 = [d for e, d in evs if e == "response_start"]
        self.assertEqual(rs1["status"], 200)
        self.assertIn("headers", rs1)
        self.assertEqual(rs2["model"], body["model"])
        self.assertTrue(rs2["message_id"].startswith("msg_"))
        self.assertIn("input_tokens", rs2["usage"])
        starts = [d for e, d in evs if e == "block_start"]
        self.assertEqual([s["type"] for s in starts], ["thinking", "text", "tool_use"])
        self.assertEqual(starts[2]["name"], "Bash")
        self.assertTrue(starts[2]["tool_use_id"].startswith("toolu_"))
        stop = [d for e, d in evs if e == "block_stop"][2]
        self.assertEqual(stop["input"], {"command": "ls -la", "description": "List files"})
        end = dict(evs)["response_end"]
        self.assertEqual(end["content"], up.reply_blocks("tools", body))
        self.assertEqual(end["stop_reason"], "tool_use")
        self.assertEqual(end["usage"]["output_tokens"], 312)
        self.assertFalse(end["aborted"])
        self.assertIsNone(end["json"])
        self.assertEqual(end["raw"].encode(), up.sse_bytes(up.sse_events("tools", body)))
        self.assertEqual(end["response_bytes"], len(end["raw"].encode()))
        self.assertIsNotNone(end["t_first_byte"])
        self.assertIsNotNone(end["t_first_token"])

    def test_events_multi_tool(self):
        body = body_for("MULTI_TOOL")
        evs = self.via_proxy("POST", "/v1/messages", body)[5]
        starts = [d for e, d in evs if e == "block_start" and d["type"] == "tool_use"]
        self.assertEqual([s["name"] for s in starts], ["Read", "Read", "Bash"])
        self.assertEqual(len({s["tool_use_id"] for s in starts}), 3, "distinct call ids")

    def test_events_json_reply(self):
        body = body_for("plain", stream=False)
        evs = self.via_proxy("POST", "/v1/messages", body)[5]
        self.assertEqual(names(evs), ["exchange_start", "response_start", "response_end"])
        end = dict(evs)["response_end"]
        self.assertEqual(end["json"], up.json_reply("success", body))
        self.assertEqual(end["content"], up.reply_blocks("success", body))
        self.assertEqual(end["stop_reason"], "end_turn")

    def test_events_http_error(self):
        evs = self.via_proxy("POST", "/v1/messages", body_for("FORCE_ERROR"))[5]
        self.assertEqual(names(evs), ["exchange_start", "response_start", "exchange_error", "response_end"])
        err = dict(evs)["exchange_error"]
        self.assertEqual((err["kind"], err["status"]), ("http", 529))
        self.assertEqual(err["body"], up.ERROR_BODY)
        self.assertEqual(dict(evs)["response_end"]["status"], 529)

    def test_events_stream_error_event(self):
        evs = self.via_proxy("POST", "/v1/messages", body_for("STREAM_ERROR"))[5]
        n = names(evs)
        self.assertIn("exchange_error", n)
        self.assertEqual(n[-1], "response_end")
        errs = [d for e, d in evs if e == "exchange_error"]
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0]["kind"], "stream_error")
        self.assertEqual(errs[0]["body"], up.STREAM_ERROR_EVENT["error"])
        self.assertIsNone(dict(evs)["response_end"]["stop_reason"])

    def test_events_malformed_tool_json(self):
        evs = self.via_proxy("POST", "/v1/messages", body_for("BAD_JSON"))[5]
        stop = [d for e, d in evs if e == "block_stop"][-1]
        self.assertIn("_partial_json", stop["input"])
        self.assertTrue(stop["input"]["_partial_json"].endswith('"oops": '))
        tool = [b for b in dict(evs)["response_end"]["content"] if b["type"] == "tool_use"][0]
        self.assertEqual(tool["input"], stop["input"])

    def test_events_head_and_count_tokens(self):
        evs = self.via_proxy("HEAD", "/")[5]
        self.assertEqual(names(evs), ["exchange_start", "response_start", "response_end"])
        self.assertEqual(dict(evs)["response_end"]["raw"], "")
        evs = self.via_proxy("POST", "/v1/messages/count_tokens", body_for("x", stream=False))[5]
        self.assertEqual(names(evs), ["exchange_start", "response_start", "response_end"])
        self.assertIn("input_tokens", dict(evs)["response_end"]["json"])

    def test_events_client_abort(self):
        sock = socket.create_connection(("127.0.0.1", self.proxy.port))
        data = json.dumps(body_for("SLOW")).encode()
        hdr = "".join(f"{k}: {v}\r\n" for k, v in headers().items())
        before = set(self.ev.ids())
        sock.sendall(f"POST /v1/messages HTTP/1.1\r\nHost: x\r\n{hdr}content-length: {len(data)}\r\n\r\n".encode() + data)
        got = b""
        while b"content_block_start" not in got:
            got += sock.recv(65536)
        sock.close()
        xid = self.ev.wait(lambda evs: [d["id"] for e, d in evs if e == "exchange_start" and d["id"] not in before])[0]
        evs = self.ev.wait_end(xid, timeout=15)
        end = dict(evs)["response_end"]
        self.assertTrue(end["aborted"], "client hang-up must be recorded as aborted")
        self.assertNotIn("exchange_error", names(evs))
        self.assertLess(len(end["raw"]), len(up.sse_bytes(up.sse_events("slow", body_for("SLOW")))))
        self.proxy.wait_line(lambda ls: [x for x in ls if f"#{xid} " in x and "(client closed)" in x])

    def test_unreachable_upstream(self):
        dead = free_port()  # nothing listens here
        p = Proxy(f"http://127.0.0.1:{dead}")
        ev = Events(p.dash)
        try:
            ev.wait(lambda evs: any(e == "replay_done" for e, _ in evs))
            status, _, body, _ = request(p.port, "POST", "/v1/messages", body_for("hi"))
            self.assertEqual(status, 502)
            xid = ev.wait(lambda evs: ev.ids())[0]
            evs = ev.wait_end(xid)
            self.assertEqual(names(evs), ["exchange_start", "exchange_error", "response_end"])
            err = dict(evs)["exchange_error"]
            self.assertEqual((err["kind"], err["status"]), ("unreachable", 502))
            self.assertEqual(dict(evs)["response_end"]["status"], 502)
            p.wait_line(lambda ls: [x for x in ls if LINE.match(x) and "ERROR unreachable" in x])
        finally:
            ev.close()
            p.stop()

    # --- terminal output (D9) --------------------------------------------------
    def test_one_terminal_line_per_exchange(self):
        xids = []
        for text in ("plain", "TOOL", "FORCE_ERROR", "STREAM_ERROR", "DROP"):
            xids.append(self.via_proxy("POST", "/v1/messages", body_for(text))[4])
        xids.append(self.via_proxy("HEAD", "/")[4])
        self.proxy.wait_line(lambda ls: all(any(f" #{x} " in l for l in ls) for x in xids))
        time.sleep(0.2)
        lines = self.proxy.exchange_lines()
        for x in xids:
            mine = [l for l in lines if LINE.match(l).group(1) == str(x)]
            self.assertEqual(len(mine), 1, f"#{x}: {mine}")
        by = {int(LINE.match(l).group(1)): l for l in lines}
        self.assertRegex(by[xids[0]], r"POST /v1/messages 200 \d+\.\ds \S+/\S+ \$")
        self.assertIn("ERROR http", by[xids[2]])
        self.assertIn("ERROR stream_error", by[xids[3]])
        self.assertIn("ERROR stream_error", by[xids[4]])
        self.assertRegex(by[xids[5]], r"HEAD / 200 ")
        out = "\n".join(self.proxy.lines)
        self.assertNotIn(SECRET, out)
        self.assertNotIn("event: ", out, "no response dumps without --verbose")

    # --- credentials (§10 #10) -----------------------------------------------
    def test_credentials_masked_in_events(self):
        hdrs = headers(cookie="session=" + SECRET + "; other=1")
        _, _, _, _, xid, evs = self.via_proxy("POST", "/v1/messages", body_for("TOOL"), hdrs)
        raw = bytes(self.ev.raw).decode("utf-8", "replace")
        for secret in (SECRET, API_KEY, SECRET[10:]):
            self.assertNotIn(secret, raw, "a credential leaked into /events")
        sent = dict((k.lower(), v) for k, v in dict(evs)["exchange_start"]["headers"])
        self.assertTrue(sent["authorization"].endswith("…(masked)"))
        self.assertTrue(sent["x-api-key"].endswith("…(masked)"))
        self.assertTrue(sent["cookie"].endswith("…(masked)"))
        self.assertEqual(sent["x-claude-code-session-id"], hdrs["x-claude-code-session-id"])
        fwd = dict((k.lower(), v) for k, v in self.log()[-1]["headers"])
        self.assertEqual(fwd["authorization"], "Bearer " + SECRET, "the forwarded request keeps the token")
        self.assertEqual(fwd["x-api-key"], API_KEY)

    # --- replay (§10 #8) ---------------------------------------------------------
    def test_replay_on_late_join(self):
        xids = [self.via_proxy("POST", "/v1/messages", body_for(t))[4] for t in ("TOOL", "FORCE_ERROR")]
        late = Events(self.proxy.dash)
        try:
            late.wait(lambda evs: any(e == "replay_done" for e, _ in evs))
            first, hello = late.events[0]
            self.assertEqual(first, "reset")
            for k in ("boot", "page_mtime", "evicted", "upstream", "prices", "prices_as_of"):
                self.assertIn(k, hello)
            self.assertEqual(hello["upstream"], f"http://127.0.0.1:{self.up_port}")
            done = [d for e, d in late.events if e == "replay_done"][0]
            replayed = late.ids()
            self.assertEqual(done["count"], len(replayed))
            self.assertEqual(replayed, sorted(replayed))
            for x in xids:
                self.assertIn(x, replayed)
                mine = late.of(x)
                self.assertNotIn("block_delta", [e for e, _ in mine], "finished exchanges are compacted")
                live = [(e, d) for e, d in self.ev.of(x) if e not in proxy_stream_events()]
                self.assertEqual(mine, live, "replay == live stream minus deltas")
        finally:
            late.close()

    # --- isolation (§10 #9, §13.8 #10) -------------------------------------------
    def test_stalled_and_vanished_events_clients_do_not_delay_relay(self):
        body = body_for("BUSY")
        up.OPTS.update(busy_count=6000, busy_size=200)  # ~1.2 MB of text, ~6000 deltas
        try:
            expected = request(self.up_port, "POST", "/v1/messages", body)[2]
            t = time.time()
            self.assertEqual(request(self.proxy.port, "POST", "/v1/messages", body)[2], expected)
            baseline = time.time() - t
            stalled = [Events(self.proxy.dash, read=False, rcvbuf=4096) for _ in range(3)]
            vanished = Events(self.proxy.dash, read=False)
            vanished.close()
            try:
                times = []
                for _ in range(2):
                    t = time.time()
                    self.assertEqual(request(self.proxy.port, "POST", "/v1/messages", body)[2], expected)
                    times.append(time.time() - t)
                self.assertLess(max(times), baseline * 3 + 1.0,
                                f"relay slowed by stalled /events clients: {baseline:.2f}s -> {times}")
                # Relay of a small request is still immediate.
                t = time.time()
                self.assertEqual(request(self.proxy.port, "POST", "/v1/messages", body_for("plain"))[0], 200)
                self.assertLess(time.time() - t, 2.0)
            finally:
                for s in stalled:
                    s.close()
        finally:
            up.OPTS.update(busy_count=400, busy_size=64)

    # --- dashboard port -----------------------------------------------------------
    def test_dashboard_serves_page(self):
        status, hdrs, body, _ = request(self.proxy.dash, "GET", "/", None, {})
        self.assertEqual(status, 200)
        with open(os.path.join(ROOT, "dashboard.html"), "rb") as f:
            self.assertEqual(body, f.read())
        h = {k.lower(): v for k, v in hdrs}
        self.assertEqual(h["cache-control"], "no-store")
        self.assertTrue(h["content-type"].startswith("text/html"))
        self.assertEqual(request(self.proxy.dash, "GET", "/nope", None, {})[0], 404)
        conn = http.client.HTTPConnection("127.0.0.1", self.proxy.dash, timeout=5)
        conn.request("GET", "/events")
        r = conn.getresponse()
        self.assertTrue(r.getheader("content-type").startswith("text/event-stream"))
        conn.close()

    def test_cli_options(self):
        out = subprocess.run([sys.executable, PROXY, "--help"], capture_output=True, text=True, timeout=10).stdout
        for opt in ("--port", "--dash-port", "--upstream", "--verbose"):
            self.assertIn(opt, out)


def proxy_stream_events() -> set[str]:
    return {"block_start", "block_delta", "block_stop"}


if __name__ == "__main__":
    unittest.main()
