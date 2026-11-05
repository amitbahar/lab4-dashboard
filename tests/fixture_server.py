#!/usr/bin/env python3
"""Serve dashboard.html with a fixture replayed on /events, like proxy.py's dashboard port.

    python3 tests/fixture_server.py --fixture failures            # prints "PORT <n>"
    python3 tests/fixture_server.py --fixture basic --mode live --delay 200
    open http://127.0.0.1:<n>/

It behaves like proxy.py's `Dash` + `Recorder`: every /events client gets a
`reset` (boot, page_mtime, evicted, upstream, prices), the retained buffer,
`replay_done`, `retry: 1000`, then live events and a `ping` every 15 s. The
buffer is compacted like proxy.py's (a finished exchange keeps no block_*
events) and `evicted` events remove exchanges from it.

Modes:
  replay  the fixture is "already recorded": everything (up to --pause-at) is
          in the buffer before the first client connects. Default.
  live    publishing starts when the first client connects, --delay ms apart.

Control endpoints (GET, return JSON status):
  /control/status
  /control/load?fixture=NAME[&mode=live&delay=MS&pause_at=N&drop_after=N&new_boot=1]
        replace the timeline; drops connected /events clients (they reconnect
        and get a reset). new_boot=1 changes the boot id (a "proxy restart":
        the page reloads itself).
  /control/pause            stop publishing after the current event
  /control/step?n=K         publish K more events while paused
  /control/resume           publish the rest
  /control/drop             close every /events connection (EventSource reconnects)
  /control/emit             POST {"event": ..., "data": ...}: publish one extra event
  /control/wait?published=N block (up to 10 s) until N events are published
  /fixtures                 the manifest (tests/fixtures/manifest.json)
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import sys
import threading
import time
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FIXTURES = os.path.join(HERE, "fixtures")
sys.dont_write_bytecode = True  # importing proxy.py must not litter the repo root
sys.path.insert(0, ROOT)

import proxy  # noqa: E402  (for frame(), PRICES and STREAM_EVENTS; importing it starts nothing)

PAGE = os.path.join(ROOT, "dashboard.html")


def load_fixture(name: str) -> list[dict]:
    path = name if os.path.sep in name else os.path.join(FIXTURES, name + ".jsonl")
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


class Client:
    def __init__(self) -> None:
        self.q: queue.Queue = queue.Queue()


class Timeline:
    def __init__(self) -> None:
        self.cond = threading.Condition()
        self.boot = int(time.time() * 1000)
        self.clients: set[Client] = set()
        self.load("basic" if os.path.exists(os.path.join(FIXTURES, "basic.jsonl")) else None)
        threading.Thread(target=self._publisher, name="publisher", daemon=True).start()

    # --- state -------------------------------------------------------------------
    def load(self, fixture, mode="replay", delay=0.0, pause_at=None, drop_after=None, new_boot=False) -> None:
        with self.cond:
            self.fixture = fixture
            self.events = load_fixture(fixture) if fixture else []
            self.mode, self.delay = mode, float(delay)
            self.limit = None if pause_at is None else int(pause_at)
            self.drop_after = None if drop_after is None else int(drop_after)
            self.published = 0
            self.started = mode == "replay"
            self.buffer: OrderedDict[int, list[tuple[str, str]]] = OrderedDict()
            self.evicted_total = 0
            if new_boot:
                self.boot += 1
            self._drop_locked()
            if mode == "replay":
                while self._can_publish():
                    self._publish_locked(self.events[self.published])
            self.cond.notify_all()

    def status(self) -> dict:
        with self.cond:
            return {"fixture": self.fixture, "mode": self.mode, "delay": self.delay, "published": self.published,
                    "total": len(self.events), "paused_at": self.limit, "clients": len(self.clients),
                    "buffered_ids": list(self.buffer), "evicted": self.evicted_total, "boot": self.boot}

    def page_version(self) -> dict:
        try:
            mtime = os.path.getmtime(PAGE)
        except OSError:
            mtime = None
        return {"boot": self.boot, "page_mtime": mtime}

    # --- publishing --------------------------------------------------------------
    def _can_publish(self) -> bool:
        return (self.started and self.published < len(self.events)
                and (self.limit is None or self.published < self.limit))

    def _publisher(self) -> None:
        while True:
            with self.cond:
                while not self._can_publish():
                    self.cond.wait()
                self._publish_locked(self.events[self.published])
                delay = self.delay if self.mode == "live" else 0
            if delay:
                time.sleep(delay / 1000)

    def _publish_locked(self, ev: dict, count: bool = True) -> None:
        name, data = ev["event"], ev["data"]
        fr = proxy.frame(name, data)
        if count:
            self.published += 1
        if name == "evicted":
            for xid in data.get("ids", []):
                self.buffer.pop(xid, None)
            self.evicted_total = data.get("total", self.evicted_total + len(data.get("ids", [])))
        elif "id" in data:
            entry = self.buffer.get(data["id"])
            if entry is None and name == "exchange_start":
                entry = self.buffer[data["id"]] = []
            if entry is not None:
                if name == "response_end":
                    entry[:] = [(e, f) for e, f in entry if e not in proxy.STREAM_EVENTS]
                entry.append((name, fr))
        for c in self.clients:
            c.q.put(fr)
        if count and self.drop_after is not None and self.published >= self.drop_after:
            self.drop_after = None
            self._drop_locked()
        self.cond.notify_all()

    def emit(self, ev: dict) -> None:
        with self.cond:
            self._publish_locked(ev, count=False)

    def _drop_locked(self) -> None:
        for c in self.clients:
            c.q.put(None)
        self.clients.clear()

    def drop(self) -> None:
        with self.cond:
            self._drop_locked()

    def set_limit(self, how: str, n: int = 1) -> None:
        with self.cond:
            if how == "pause":
                self.limit = self.published
            elif how == "step":
                self.limit = (self.published if self.limit is None else self.limit) + n
            else:
                self.limit = None
            self.cond.notify_all()

    def wait_published(self, n: int, timeout: float = 10.0) -> bool:
        end = time.time() + timeout
        with self.cond:
            while self.published < n:
                left = end - time.time()
                if left <= 0:
                    return False
                self.cond.wait(left)
            return True

    # --- clients -----------------------------------------------------------------
    def subscribe(self) -> tuple[Client, list[str]]:
        c = Client()
        with self.cond:
            hello = proxy.frame("reset", {
                **self.page_version(), "evicted": self.evicted_total,
                "upstream": f"fixture:{self.fixture}",
                "prices": {k: {"input": v[0], "output": v[1], "cache_write": v[2], "cache_read": v[3]}
                           for k, v in proxy.PRICES.items()},
                "prices_as_of": proxy.PRICES_AS_OF,
            })
            frames = [hello] + [f for entry in self.buffer.values() for _, f in entry]
            frames.append(proxy.frame("replay_done", {"count": len(self.buffer)}))
            self.clients.add(c)
            if not self.started:
                self.started = True
                self.cond.notify_all()
        return c, frames

    def unsubscribe(self, c: Client) -> None:
        with self.cond:
            self.clients.discard(c)


TL: Timeline


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):  # noqa: A002
        pass

    def _json(self, obj, status=200) -> None:
        out = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(out)

    def do_POST(self) -> None:
        url = urlsplit(self.path)
        if url.path == "/control/emit":
            n = int(self.headers.get("content-length") or 0)
            try:
                TL.emit(json.loads(self.rfile.read(n)))
            except (ValueError, KeyError) as e:
                return self._json({"error": str(e)}, 400)
            return self._json(TL.status())
        self.send_error(404)

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        q = {k: v[-1] for k, v in parse_qs(url.query).items()}
        path = url.path
        if path in ("/", "/index.html"):
            try:
                with open(PAGE, "rb") as f:
                    page = f.read()
            except OSError as e:
                return self.send_error(500, str(e))
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(page)
        elif path == "/events":
            self.stream()
        elif path == "/fixtures":
            with open(os.path.join(FIXTURES, "manifest.json"), encoding="utf-8") as f:
                self._json(json.load(f))
        elif path == "/control/status":
            self._json(TL.status())
        elif path == "/control/load":
            try:
                TL.load(q.get("fixture") or TL.fixture, q.get("mode", "replay"), q.get("delay", 0),
                        q.get("pause_at"), q.get("drop_after"), q.get("new_boot") == "1")
            except OSError as e:
                return self._json({"error": str(e)}, 404)
            self._json(TL.status())
        elif path in ("/control/pause", "/control/resume", "/control/step"):
            TL.set_limit(path.rsplit("/", 1)[1], int(q.get("n", 1)))
            self._json(TL.status())
        elif path == "/control/drop":
            TL.drop()
            self._json(TL.status())
        elif path == "/control/wait":
            ok = TL.wait_published(int(q.get("published", 0)), float(q.get("timeout", 10)))
            self._json({**TL.status(), "ok": ok}, 200 if ok else 408)
        else:
            self.send_error(404)

    def stream(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        c, frames = TL.subscribe()
        try:
            self.wfile.write(("".join(frames) + "retry: 1000\n\n").encode())
            self.wfile.flush()
            while True:
                try:
                    fr = c.q.get(timeout=15)
                except queue.Empty:
                    fr = proxy.frame("ping", TL.page_version())
                if fr is None:
                    break
                self.wfile.write(fr.encode())
                self.wfile.flush()
        except OSError:
            pass
        finally:
            TL.unsubscribe(c)


class Server(ThreadingHTTPServer):
    daemon_threads = True


def main() -> None:
    global TL, PAGE
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--port", type=int, default=0, help="0 picks a free port (printed as 'PORT n')")
    ap.add_argument("--fixture", default="basic", help="name in tests/fixtures/ or a path to a .jsonl")
    ap.add_argument("--mode", choices=("replay", "live"), default="replay")
    ap.add_argument("--delay", type=float, default=0, help="ms between events in live mode")
    ap.add_argument("--pause-at", type=int, help="stop after publishing N events")
    ap.add_argument("--drop-after", type=int, help="close /events connections once N events are published")
    ap.add_argument("--page", default=PAGE, help="the dashboard HTML to serve (default: repo dashboard.html)")
    a = ap.parse_args()
    PAGE = os.path.abspath(a.page)
    TL = Timeline()
    TL.load(a.fixture, a.mode, a.delay, a.pause_at, a.drop_after)
    srv = Server(("127.0.0.1", a.port), Handler)
    print(f"PORT {srv.server_address[1]}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
