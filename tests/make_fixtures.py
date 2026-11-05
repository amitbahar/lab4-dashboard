#!/usr/bin/env python3
"""Generate the deterministic dashboard event fixtures in tests/fixtures/.

    python3 tests/make_fixtures.py           # (re)write tests/fixtures/*.jsonl + manifest.json
    python3 tests/make_fixtures.py --check   # exit 1 if the committed files are stale

The events are produced by proxy.py's own `Recording` class (its publisher is
swapped for a capture), fed with SSE bytes from tests/upstream.py. So every
line is exactly what proxy.py would put on /events, including credential
masking, and the fixtures follow proxy.py if its wire format changes.

Each fixture line is `{"event": <name>, "data": <payload>}`. Fixtures hold the
live stream (deltas included) and never `reset` / `replay_done`: the fixture
server adds those, and compacts finished exchanges on replay like proxy.py.

`manifest.json` describes every fixture and what a correct page should derive
from it (exchange ids still present, lifecycle state per id, call/result
pairing per call id), so UI tests can assert on it.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.path.join(HERE, "fixtures")
sys.dont_write_bytecode = True  # importing proxy.py must not litter the repo root
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

import proxy  # noqa: E402
import upstream as up  # noqa: E402

T0 = 1767261600000.0  # 2026-01-01 10:00:00 UTC, in ms
SECRET = "sk-ant-oat01-FIXTURESECRET-0123456789abcdefghijklmnopqrstuvwxyz"
SESSION_A = "11111111-aaaa-4aaa-8aaa-000000000001"
SESSION_B = "22222222-bbbb-4bbb-8bbb-000000000002"

SYSTEM = [{"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude.",
           "cache_control": {"type": "ephemeral"}},
          {"type": "text", "text": "You are an interactive CLI tool.\n" + "\n".join(f"Rule {i}: be careful." for i in range(1, 30))}]
TOOLS = [
    {"name": "Bash", "description": "Executes a bash command.\nUse carefully.",
     "input_schema": {"type": "object", "properties": {"command": {"type": "string"},
                                                        "description": {"type": "string"}},
                      "required": ["command"]}},
    {"name": "Read", "description": "Reads a file.", "input_schema": {
        "type": "object", "properties": {"file_path": {"type": "string"}}, "required": ["file_path"]}},
    {"name": "Agent", "description": "Launch a subagent.", "input_schema": {
        "type": "object", "properties": {"description": {"type": "string"}, "prompt": {"type": "string"}}}},
]
SUB_SYSTEM = [{"type": "text", "text": "You are a subagent of Claude Code. Do the task and report."}]
MODEL = "claude-sonnet-4-5-20250929"
HAIKU = "claude-haiku-4-5-20251001"


def headers(session=SESSION_A, extra=()):
    return [("host", "localhost:8041"), ("content-type", "application/json"),
            ("authorization", "Bearer " + SECRET), ("anthropic-version", "2023-06-01"),
            ("anthropic-beta", "claude-code-20250219,interleaved-thinking-2025-05-14"),
            ("user-agent", "claude-cli/2.0.0 (external, cli)"),
            ("x-claude-code-session-id", session), *extra]


SSE_HEADERS = [("Content-Type", "text/event-stream; charset=utf-8"), ("request-id", "req_fixture"),
               ("Transfer-Encoding", "chunked")]
JSON_HEADERS = [("Content-Type", "application/json"), ("request-id", "req_fixture")]


def user(text):
    return {"role": "user", "content": [{"type": "text", "text": text}]}


def results(*pairs, error_ids=()):
    return {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": tid, "content": out, **({"is_error": True} if tid in error_ids else {})}
        for tid, out in pairs]}


def assistant(blocks):
    return {"role": "assistant", "content": [b for b in blocks if b["type"] != "thinking"]}


def msg_body(messages, system=SYSTEM, tools=TOOLS, model=MODEL, stream=True, **kw):
    b = {"model": model, "max_tokens": 32000, "stream": stream, "system": system, "messages": messages}
    if tools:
        b["tools"] = tools
    b.update(kw)
    return b


class Capture:
    """Stands in for proxy.REC: serializes each published event immediately (as frame() does)."""

    def __init__(self):
        self.events = []

    def publish(self, xid, event, payload, compact=False):
        self.events.append({"event": event, "data": json.loads(json.dumps(payload, ensure_ascii=False, default=str))})


class Fixture:
    def __init__(self, name, description):
        self.name, self.description = name, description
        self.cap = proxy.REC = Capture()  # proxy.Recording publishes through proxy.REC
        self.t = T0
        self.next_id = 1
        self.expect = {"states": {}, "evicted": [], "calls": {}, "orphan_results": [], "notes": []}

    def tick(self, ms=40):
        self.t += ms
        return self.t

    def x(self, body=None, method="POST", path="/v1/messages", hdrs=None, scenario=None, raw_body=None):
        xid = self.next_id
        self.next_id += 1
        return Exchange(self, xid, body, method, path, hdrs or headers(), scenario, raw_body)

    def run(self, *exchanges):
        """Run exchanges to completion one after another."""
        for e in exchanges:
            for step in e.steps():
                step()

    def interleave(self, *exchanges, pattern=None):
        """Run several exchanges concurrently: one step from each in turn
        (or following `pattern`, a string of exchange indexes like "0011010")."""
        gens = [iter(e.steps()) for e in exchanges]
        alive = list(range(len(gens)))
        order = [int(c) for c in pattern] if pattern else []
        while alive:
            i = order.pop(0) if order else alive[0]
            if i not in alive:
                continue
            try:
                next(gens[i])()
            except StopIteration:
                alive.remove(i)
            if not order:
                alive = alive[1:] + alive[:1]  # round robin

    def raw(self, event, data):
        self.cap.events.append({"event": event, "data": data})

    def evict(self, ids):
        self.expect["evicted"] += list(ids)
        self.raw("evicted", {"ids": list(ids), "total": len(self.expect["evicted"])})

    def write(self):
        path = os.path.join(OUT, self.name + ".jsonl")
        text = "".join(json.dumps(e, ensure_ascii=False, sort_keys=True) + "\n" for e in self.cap.events)
        return path, text

    def manifest(self):
        ids = sorted({e["data"]["id"] for e in self.cap.events if e["event"] == "exchange_start"})
        live = [i for i in ids if i not in self.expect["evicted"]]
        return {"description": self.description, "events": len(self.cap.events), "exchange_ids": ids,
                "live_ids": live, **self.expect,
                "states": {str(k): v for k, v in sorted(self.expect["states"].items())}}


class Exchange:
    """One HTTP exchange, as a sequence of steps recorded through proxy.Recording."""

    def __init__(self, fx, xid, body, method, path, hdrs, scenario, raw_body):
        self.fx, self.xid, self.body, self.method, self.path, self.hdrs = fx, xid, body, method, path, hdrs
        self.scenario = scenario or (up.pick_scenario(body) if isinstance(body, dict) else "success")
        self.raw_body = raw_body
        self.outcome = "completed"
        self.mode = "auto"
        self.cut = None          # stop after this many SSE events
        self.blocks = None

    # Configure ---------------------------------------------------------------
    def status(self, state):
        self.fx.expect["states"][self.xid] = state
        return self

    def abort_after(self, n):
        """The client hangs up after n SSE events (response_end.aborted = true)."""
        self.mode, self.cut = "abort", n
        return self

    def drop_after(self, n):
        """The upstream drops the chunked stream after n SSE events."""
        self.mode, self.cut = "drop", n
        return self

    def unreachable(self):
        self.mode = "unreachable"
        return self

    def hang_after(self, n):
        """Stop after n SSE events with no terminal event (still in flight at fixture end).
        n = 0 stops before response_start: the exchange is still "sending"."""
        self.mode, self.cut = "hang", n
        return self

    def reply(self):
        return up.reply_blocks(self.scenario, self.body)

    def calls(self):
        return [b for b in self.reply() if b["type"] == "tool_use"]

    # Steps -------------------------------------------------------------------
    def steps(self):
        fx = self.fx
        body_bytes = self.raw_body if self.raw_body is not None else (
            json.dumps(self.body).encode() if self.body is not None else b"")
        rec = proxy.Recording(self.xid, fx.t, self.method, self.path, self.hdrs, body_bytes)
        stream = isinstance(self.body, dict) and self.body.get("stream") and self.method == "POST" \
            and not self.path.endswith("count_tokens")
        yield lambda: (setattr(rec, "t0", fx.tick()), rec.start())
        if self.mode == "unreachable":
            yield lambda: rec.unreachable(f"cannot reach https://api.anthropic.com: [Errno 111] Connection refused",
                                          fx.tick(900))
            fx.expect["states"].setdefault(self.xid, "failed")
            return
        if self.method == "HEAD":
            yield lambda: rec.response_start(200, "OK", [("Content-Length", "0")], fx.tick())
            yield lambda: rec.finish(fx.tick())
            fx.expect["states"].setdefault(self.xid, "completed")
            return
        if self.method == "GET" or self.path.endswith("count_tokens") or self.scenario == "http_error" or not stream:
            if self.path.endswith("count_tokens"):
                status, reason, obj = 200, "OK", {"input_tokens": len(body_bytes) // 4}
            elif self.method == "GET":
                status, reason, obj = 200, "OK", {"data": [{"id": MODEL, "type": "model"}]}
            elif not isinstance(self.body, dict):
                status, reason, obj = 400, "Bad Request", {"type": "error", "error": {
                    "type": "invalid_request_error", "message": "bad json"}}
            elif self.scenario == "http_error":
                status, reason, obj = 529, "Overloaded", up.ERROR_BODY
            else:
                status, reason, obj = 200, "OK", up.json_reply(self.scenario, self.body)
            out = json.dumps(obj).encode()
            yield lambda: rec.response_start(status, reason, JSON_HEADERS + [("Content-Length", str(len(out)))],
                                             fx.tick(300))
            yield lambda: rec.feed(out, fx.tick())
            yield lambda: rec.finish(fx.tick())
            fx.expect["states"].setdefault(self.xid, "failed" if status >= 400 else "completed")
            return
        events = up.sse_events(self.scenario, self.body)
        if self.cut is not None:
            events = events[:self.cut]
        if self.mode == "hang" and self.cut == 0:
            fx.expect["states"].setdefault(self.xid, "sending")
            return
        yield lambda: rec.response_start(200, "OK", SSE_HEADERS, fx.tick(250))
        for name, data in events:
            yield (lambda fr=up.sse_frame(name, data): rec.feed(fr, fx.tick(30)))
        if self.mode == "hang":
            fx.expect["states"].setdefault(self.xid, "streaming" if events else "sending")
            return
        if self.mode == "abort":
            yield lambda: rec.finish(fx.tick(), aborted=True)
            fx.expect["states"].setdefault(self.xid, "interrupted")
        elif self.mode == "drop":
            yield lambda: rec.finish(fx.tick(), read_error="upstream read failed: IncompleteRead: "
                                                           "IncompleteRead(0 bytes read)")
            fx.expect["states"].setdefault(self.xid, "failed")
        else:
            yield lambda: rec.finish(fx.tick())
            fx.expect["states"].setdefault(self.xid, "failed" if self.scenario == "stream_error" else "completed")


def call(fx, xid, block, result_in=None, error=False, note=None):
    """Record the expected pairing of one call id."""
    fx.expect["calls"][block["id"]] = {"name": block["name"], "call_in": xid, "result_in": result_in,
                                       "is_error": error, **({"note": note} if note else {})}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def fx_basic():
    fx = Fixture("basic", "One main thread: HEAD probe, prompt -> thinking+text+Bash call, tool result -> "
                          "final text; a side call (no tools), count_tokens, a non-streaming JSON reply.")
    fx.run(fx.x(None, method="HEAD", path="/"))
    m1 = [user("<system-reminder>be nice</system-reminder>"), user("please run TOOL ls")]
    m1 = [{"role": "user", "content": m1[0]["content"] + m1[1]["content"]}]
    e1 = fx.x(msg_body(m1))
    fx.run(e1)
    c = e1.calls()[0]
    m2 = m1 + [assistant(e1.reply()), results((c["id"], "total 12\nproxy.py\nSPEC.md"))]
    e2 = fx.x(msg_body(m2))
    call(fx, e1.xid, c, result_in=e2.xid)
    fx.run(e2)
    fx.run(fx.x(msg_body([user("Write a 5-word title for: please run ls")], system=None, tools=None,
                         model=HAIKU, max_tokens=50)))
    fx.run(fx.x(msg_body(m2, stream=False), path="/v1/messages/count_tokens"))
    m3 = m2 + [assistant(e2.reply()), user("thanks. JSON_REPLY please")]
    fx.run(fx.x(msg_body(m3, stream=False)))
    return fx


def fx_concurrent():
    fx = Fixture("concurrent_threads",
                 "Main thread replies with two Agent calls; the two subagent threads then stream "
                 "interleaved event by event, both calling Read/Bash (repeated tool names, distinct "
                 "call ids; subagent 1 has three calls in one reply). Main gets the Agent results "
                 "in reverse order and ends with a reply whose calls are still pending.")
    m1 = [user("Explore the repo with two subagents. AGENTS")]
    e1 = fx.x(msg_body(m1))
    fx.run(e1)
    agent_a, agent_b = e1.calls()
    sa1 = fx.x(msg_body([user(agent_a["input"]["prompt"])], system=SUB_SYSTEM))
    sb1 = fx.x(msg_body([user(agent_b["input"]["prompt"])], system=SUB_SYSTEM))
    fx.interleave(sa1, sb1)
    sa2 = fx.x(msg_body([user(agent_a["input"]["prompt"]), assistant(sa1.reply()),
                         results(*[(c["id"], f"contents of call {k}") for k, c in enumerate(sa1.calls())])],
                        system=SUB_SYSTEM))
    sb2 = fx.x(msg_body([user(agent_b["input"]["prompt"]), assistant(sb1.reply()),
                         results((sb1.calls()[0]["id"], "README.md\nSPEC.md"))], system=SUB_SYSTEM))
    fx.interleave(sb2, sa2, pattern="0101011100101")
    for c in sa1.calls():
        call(fx, sa1.xid, c, result_in=sa2.xid)
    call(fx, sb1.xid, sb1.calls()[0], result_in=sb2.xid)
    m2 = m1 + [assistant(e1.reply()),
               results((agent_b["id"], "Docs: README and SPEC."), (agent_a["id"], "Python: a.py and b.py."))]
    e2 = fx.x(msg_body(m2))
    call(fx, e1.xid, agent_a, result_in=e2.xid, note=f"starts thread of #{sa1.xid}")
    call(fx, e1.xid, agent_b, result_in=e2.xid, note=f"starts thread of #{sb1.xid}")
    fx.run(e2)
    m3 = m2 + [assistant(e2.reply()), user("Now read both files. MULTI_TOOL")]
    e3 = fx.x(msg_body(m3))
    fx.run(e3)
    for c in e3.calls():
        call(fx, e3.xid, c, result_in=None, note="waiting for result (no later request)")
    fx.expect["threads"] = {"main": [e1.xid, e2.xid, e3.xid], "agent_a": [sa1.xid, sa2.xid],
                            "agent_b": [sb1.xid, sb2.xid]}
    return fx


def fx_tool_pairing():
    fx = Fixture("tool_pairing",
                 "Multiple tool calls in one reply whose results come back in a different order; one "
                 "is_error result; history resent with old tool_results in every later request; a "
                 "result that arrives before its call's block_start is seen; an orphan result whose "
                 "call never appears; a call left without result followed by a same-thread prompt.")
    m1 = [user("Read both files then count lines. MULTI_TOOL")]
    e1 = fx.x(msg_body(m1))
    fx.run(e1)
    a, b, c = e1.calls()
    # Results out of order: c, a, b; b failed.
    m2 = m1 + [assistant(e1.reply()),
               results((c["id"], "  10 /tmp/a.py\n  20 /tmp/b.py"), (a["id"], "print('a')"),
                       (b["id"], "Error: file not found"), error_ids=(b["id"],))]
    e2 = fx.x(msg_body(m2))
    for blk in (a, c):
        call(fx, e1.xid, blk, result_in=e2.xid)
    call(fx, e1.xid, b, result_in=e2.xid, error=True)
    fx.run(e2)
    # e2 replied with a text-only answer; a new prompt asks for a tool.
    m3 = m2 + [assistant(e2.reply()), user("run it. TOOL")]
    e3 = fx.x(msg_body(m3))
    # The result for e3's call arrives (in e4's request) while e3 is still streaming,
    # before e3's tool_use block_start: the page must index the result first.
    e3_call = e3.calls()[0]
    m4 = m3 + [assistant(e3.reply()), results((e3_call["id"], "total 0"))]
    e4 = fx.x(msg_body(m4))
    call(fx, e3.xid, e3_call, result_in=e4.xid, note="result's exchange_start precedes the call's block_start")
    # e3 steps: start, response_start, then 9 events before the tool block start; e4 jumps in.
    fx.interleave(e3, e4, pattern="00" + "0" * 9 + "1" + "1" * 30 + "0" * 30)
    # Orphan: a result whose tool_use id was never seen anywhere (history resent includes m4).
    m5 = m4 + [assistant(e4.reply()),
               results(("toolu_orphan_never_called", "I am an orphan result"))]
    e5 = fx.x(msg_body(m5))
    fx.run(e5)
    fx.expect["orphan_results"].append({"tool_use_id": "toolu_orphan_never_called", "in": e5.xid})
    # A call with no result, then the same thread continues with a plain prompt.
    m6 = m5 + [assistant(e5.reply()), user("one more. TOOL")]
    e6 = fx.x(msg_body(m6))
    fx.run(e6)
    m7 = m6 + [assistant(e6.reply()), user("never mind, stop.")]
    e7 = fx.x(msg_body(m7))
    fx.run(e7)
    call(fx, e6.xid, e6.calls()[0], result_in=None, note="no result received (same-thread continuation)")
    fx.expect["notes"].append(f"requests {e3.xid}..{e7.xid} resend the results of #{e2.xid} in their "
                              f"history; each call must pair once, with the first origin (#{e2.xid}).")
    return fx


def fx_failures():
    fx = Fixture("failures",
                 "Lifecycle: HTTP 529 JSON error, an SSE `error` event mid-stream, client abort "
                 "(response_end.aborted), upstream drop mid chunked stream (stream_error then "
                 "response_end), unreachable upstream (502), malformed final tool JSON, and two "
                 "exchanges still in flight at the end (sending, streaming).")
    base = [user("hello")]
    fx.run(fx.x(msg_body(base + [assistant([{"type": "text", "text": "hi"}]), user("now FORCE_ERROR")])))
    fx.run(fx.x(msg_body([user("tell me a story STREAM_ERROR")])))
    fx.run(fx.x(msg_body([user("a long answer please")])).abort_after(6))
    fx.run(fx.x(msg_body([user("dropped answer please")])).drop_after(5))
    fx.run(fx.x(msg_body([user("anyone there?")])).unreachable())
    bad = fx.x(msg_body([user("broken tool input BAD_JSON")]))
    fx.run(bad)
    fx.expect["calls"][bad.calls()[0]["id"]] = {"name": "Bash", "call_in": bad.xid, "result_in": None,
                                                "is_error": False, "note": "final input is malformed: "
                                                "block_stop.input = {'_partial_json': ...}"}
    # Aborted while a tool call was being written: partial tool input preserved.
    tool_abort = fx.x(msg_body([user("interrupt me during the TOOL call")])).abort_after(16)
    fx.run(tool_abort)
    fx.run(fx.x(msg_body([user("still sending")])).hang_after(0))
    fx.run(fx.x(msg_body([user("still streaming")])).hang_after(5))
    return fx


def fx_eviction():
    fx = Fixture("eviction",
                 "Five exchanges in one thread (each a Bash call answered in the next request), "
                 "then an `evicted` event removes the first three, including #3 whose call is "
                 "answered in surviving #4.")
    msgs = [user("step 0. TOOL")]
    for k in range(5):
        e = fx.x(msg_body(msgs))
        fx.run(e)
        c = e.calls()[0]
        call(fx, e.xid, c, result_in=e.xid + 1 if k < 4 else None)
        msgs = msgs + [assistant(e.reply()), results((c["id"], f"out {k}")), user(f"step {k + 1}. TOOL")]
        msgs = [*msgs[:-2], {"role": "user", "content": msgs[-2]["content"] + msgs[-1]["content"]}]
    fx.evict([1, 2, 3])
    fx.expect["notes"].append("ids 1-3 evicted; the result in #4 refers to the call in evicted #3: "
                              "it must be labelled evicted, not orphan")
    return fx


def fx_multi_session():
    fx = Fixture("multi_session",
                 "Two Claude Code sessions (different x-claude-code-session-id headers) with the "
                 "same first prompt, interleaved in time.")
    a = fx.x(msg_body([user("hello TOOL")]), hdrs=headers(SESSION_A))
    b = fx.x(msg_body([user("hello TOOL")]), hdrs=headers(SESSION_B))
    fx.interleave(a, b)
    a2 = fx.x(msg_body([user("hello TOOL"), assistant(a.reply()), results((a.calls()[0]["id"], "A out"))]),
              hdrs=headers(SESSION_A))
    b2 = fx.x(msg_body([user("hello TOOL"), assistant(b.reply()), results((b.calls()[0]["id"], "B out"))]),
              hdrs=headers(SESSION_B))
    fx.interleave(b2, a2)
    call(fx, a.xid, a.calls()[0], result_in=a2.xid)
    call(fx, b.xid, b.calls()[0], result_in=b2.xid)
    fx.expect["notes"].append("identical first prompts and identical call ids across sessions "
                              "(the fake upstream derives ids from the body): pairing must not cross "
                              "sessions" if a.calls()[0]["id"] == b.calls()[0]["id"] else
                              "sessions have distinct call ids")
    fx.expect["sessions"] = {SESSION_A: [a.xid, a2.xid], SESSION_B: [b.xid, b2.xid]}
    return fx


XSS = '<script>window.__pwned=1</script><img src=x onerror="window.__pwned=1">'


def fx_hostile():
    fx = Fixture("hostile",
                 "Untrusted content everywhere: <script>/<img onerror> in system, prompt, tool input, "
                 "tool result, model text, thinking, error bodies, header values and a plain-text "
                 "response; a 200 KB single-line string; a 5000-line result; unicode.")
    big_line = "A" * 200_000
    many_lines = "\n".join(f"line {i} {XSS if i % 1000 == 0 else ''}" for i in range(5000))
    sysp = [{"type": "text", "text": "System " + XSS + " ünïcødé ✓ 🎉"}]
    m1 = [user("Prompt " + XSS + " " + big_line[:5000] + " TOOL")]
    e1 = fx.x(msg_body(m1, system=sysp), hdrs=headers(extra=[("x-evil", XSS)]))
    fx.run(e1)
    c = e1.calls()[0]
    m2 = m1 + [assistant(e1.reply()), results((c["id"], many_lines))]
    e2 = fx.x(msg_body(m2, system=sysp))
    call(fx, e1.xid, c, result_in=e2.xid)
    fx.run(e2)
    # Hand-written SSE reply with hostile text, thinking and tool input, via raw Recording feed.
    m3 = m2 + [assistant(e2.reply()), user(big_line)]
    fx.run(HostileReply(fx, msg_body(m3, system=sysp)))
    # Plain-text (non-JSON) error response containing HTML.
    fx.run(PlainText(fx, msg_body([user("plain")], system=sysp)))
    # Non-JSON request body.
    fx.run(fx.x(None, raw_body=("not json " + XSS).encode(), path="/v1/other"))
    return fx


class HostileReply(Exchange):
    def __init__(self, fx, body):
        xid = fx.next_id
        fx.next_id += 1
        super().__init__(fx, xid, body, "POST", "/v1/messages", headers(), "success", None)

    def reply(self):
        return [{"type": "thinking", "thinking": "think " + XSS, "signature": "s"},
                {"type": "text", "text": "# Title " + XSS + "\n\n```html\n" + XSS + "\n```\n[link](javascript:alert(1))"},
                {"type": "tool_use", "id": "toolu_hostile_1", "name": "Bash" + XSS,
                 "input": {"command": "echo '" + XSS + "'", "description": XSS}}]

    def steps(self):
        fx = self.fx
        rec = proxy.Recording(self.xid, fx.t, "POST", self.path, self.hdrs, json.dumps(self.body).encode())
        yield lambda: (setattr(rec, "t0", fx.tick()), rec.start())
        yield lambda: rec.response_start(200, "OK", SSE_HEADERS, fx.tick())
        ev = [("message_start", {"type": "message_start", "message": {"id": "msg_hostile", "model": MODEL,
                                                                      "usage": {"input_tokens": 5, "output_tokens": 1}}})]
        for i, b in enumerate(self.reply()):
            cb = dict(b)
            if b["type"] == "tool_use":
                cb["input"] = {}
            elif b["type"] == "thinking":
                cb["thinking"] = ""
            else:
                cb["text"] = ""
            ev.append(("content_block_start", {"type": "content_block_start", "index": i, "content_block": cb}))
            key, dt = {"thinking": ("thinking", "thinking_delta"), "text": ("text", "text_delta"),
                       "tool_use": ("partial_json", "input_json_delta")}[b["type"]]
            val = json.dumps(b["input"]) if b["type"] == "tool_use" else b[b["type"]]
            for p in up.split(val, 3):
                ev.append(("content_block_delta", {"type": "content_block_delta", "index": i,
                                                   "delta": {"type": dt, key: p}}))
            ev.append(("content_block_stop", {"type": "content_block_stop", "index": i}))
        ev.append(("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use"},
                                     "usage": {"output_tokens": 50}}))
        ev.append(("message_stop", {"type": "message_stop"}))
        for name, data in ev:
            yield (lambda fr=up.sse_frame(name, data): rec.feed(fr, fx.tick(20)))
        yield lambda: rec.finish(fx.tick())
        fx.expect["states"][self.xid] = "completed"
        fx.expect["calls"]["toolu_hostile_1"] = {"name": "Bash" + XSS, "call_in": self.xid, "result_in": None,
                                                 "is_error": False, "note": "pending"}


class PlainText(Exchange):
    def __init__(self, fx, body):
        xid = fx.next_id
        fx.next_id += 1
        super().__init__(fx, xid, body, "POST", "/v1/messages", headers(), "success", None)

    def steps(self):
        fx = self.fx
        rec = proxy.Recording(self.xid, fx.t, "POST", self.path, self.hdrs, json.dumps(self.body).encode())
        out = ("<html><body><h1>502 Bad Gateway</h1>" + XSS + "</body></html>").encode()
        yield lambda: (setattr(rec, "t0", fx.tick()), rec.start())
        yield lambda: rec.response_start(502, "Bad Gateway", [("Content-Type", "text/html"),
                                                              ("Set-Cookie", "session=" + SECRET)], fx.tick())
        yield lambda: rec.feed(out, fx.tick())
        yield lambda: rec.finish(fx.tick())
        fx.expect["states"][self.xid] = "failed"


FIXTURES = [fx_basic, fx_concurrent, fx_tool_pairing, fx_failures, fx_eviction, fx_multi_session, fx_hostile]


def build():
    """Returns {path: text} for every fixture file plus the manifest."""
    files, manifest = {}, {}
    for make in FIXTURES:
        saved = proxy.REC
        try:
            with contextlib.redirect_stdout(io.StringIO()):  # Recording prints a terminal line
                fx = make()
        finally:
            proxy.REC = saved
        path, text = fx.write()
        files[path] = text
        manifest[fx.name] = fx.manifest()
    files[os.path.join(OUT, "manifest.json")] = json.dumps(manifest, indent=1, ensure_ascii=False, sort_keys=True) + "\n"
    return files


def main():
    check = "--check" in sys.argv[1:]
    files = build()
    stale = []
    for path, text in files.items():
        try:
            with open(path, encoding="utf-8") as f:
                same = f.read() == text
        except OSError:
            same = False
        if not same:
            stale.append(os.path.relpath(path, ROOT))
            if not check:
                os.makedirs(OUT, exist_ok=True)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(text)
    if check:
        if stale:
            print("stale fixtures (run python3 tests/make_fixtures.py):", ", ".join(stale))
            sys.exit(1)
        print(f"fixtures up to date ({len(files) - 1} fixtures)")
    else:
        print(f"wrote {len(stale)} file(s); {len(files)} total in {os.path.relpath(OUT, ROOT)}/")


if __name__ == "__main__":
    main()
