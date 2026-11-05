# Tests

Dependency-free: Python 3 stdlib, Node 22 built-ins, and Chromium
(`/usr/bin/chromium`, or set `$CHROMIUM`). Nothing to install.

```
tests/run.sh        # everything; prints a summary, exits nonzero on failure
tests/run.sh -v     # same, with the full output of each step
```

Single pieces:

```
python3 tests/make_fixtures.py --check           # fixtures match proxy.py's wire format
python3 -m unittest tests.test_proxy -v          # proxy.py against the fake upstream
node --test tests/test_page_smoke.mjs            # dashboard.html in headless Chromium
```

All servers pick free ports (never 8041, 8042 or 8080) and every process
the tests start is stopped on exit, including on failure.

## What is here

| file | what it is |
|---|---|
| `upstream.py` | Fake Anthropic Messages API. Scenarios chosen by header `x-test-scenario`, by a keyword in the last message, or by `--scenario`. `GET /__log` returns every request it received. |
| `make_fixtures.py` | Generates `fixtures/*.jsonl` and `fixtures/manifest.json` by running proxy.py's own `Recording` on SSE from `upstream.py`. |
| `fixtures/` | Deterministic dashboard event streams, one `{"event", "data"}` per line, in exactly proxy.py's wire format. |
| `fixture_server.py` | Serves `dashboard.html` at `/` and replays a fixture on `/events` the way proxy.py does. It can pace, pause, step and drop the stream, and it has control endpoints for tests. |
| `cdp.mjs` | Small headless-Chromium driver over the DevTools protocol, with realistic mouse clicks and keys. |
| `harness.mjs` | Shared helpers for page tests: starts the fixture server, opens the dashboard, waits for the replay, checks for problems. |
| `test_proxy.py` | `unittest` tests of proxy.py (see below). |
| `test_page_smoke.mjs` | Layout-agnostic browser smoke test (see below). |
| `run.sh` | Runs all of the above. |

### Fake upstream scenarios (`upstream.py`)

| scenario | keyword | reply |
|---|---|---|
| `success` | (default) | thinking + text |
| `tools` | `TOOL` | thinking + text + one `Bash` call |
| `multi_tool` | `MULTI_TOOL` | `Read`, `Read` and `Bash` calls in one reply |
| `agents` | `AGENTS` | two `Agent` calls whose prompts are `upstream.AGENT_PROMPTS` |
| `json` | `JSON_REPLY` | (same as success; non-streaming whenever `stream` is false) |
| `http_error` | `FORCE_ERROR` | HTTP 529 with an `overloaded_error` JSON body |
| `stream_error` | `STREAM_ERROR` | SSE `error` event after a few text deltas |
| `drop` | `DROP` | connection cut mid chunked stream |
| `slow` | `SLOW` | `--slow-delay` seconds between events (default 0.3 s) |
| `busy` | `BUSY` | `--busy-count` text deltas of `--busy-size` bytes |
| `bad_tool_json` | `BAD_JSON` | final tool input is malformed JSON |

Any request with `"stream": false` gets a JSON message. `.../count_tokens`
returns `{"input_tokens": n}`. `HEAD` and `GET` return 200. Replies are a pure
function of the request body, so a request sent direct and one sent through
the proxy can be compared byte for byte.

### `test_proxy.py`

- Request bodies, request headers, response status, headers and body bytes are
  identical whether a request goes direct or through the proxy. This covers
  every scenario plus JSON, 529, count_tokens, GET, HEAD, a non-JSON body and
  6 concurrent requests (§13.8 #10).
- The event sequence for each scenario, as names with delta runs collapsed:
  success stream (two `response_start`s), multi-tool, JSON reply, HTTP error,
  stream `error` event, dropped stream (`exchange_error` stream_error then
  `response_end`), unreachable upstream (502 + `exchange_error` unreachable),
  malformed tool JSON (`_partial_json`), HEAD, count_tokens, and client abort
  (`aborted: true`).
- Exactly one terminal line per exchange, with ERROR / `(client closed)`
  markers and no dumps without `--verbose`.
- Credentials are masked on /events (none of the token, API key or cookie
  appears) while the forwarded request keeps them.
- Replay on late join: `reset` fields, compaction (no deltas for finished
  exchanges), and the `replay_done` count.
- Three stalled /events clients plus one vanished client don't slow a relay of
  about 1.2 MB (§10 #9).
- The dashboard port serves `dashboard.html` byte for byte, and the CLI
  options exist.

### `test_page_smoke.mjs`

For every fixture, it replays the fixture and checks that the page holds
exactly the non-evicted exchange ids. It then clicks every element with a
`data-id` attribute (real pointer events) and checks that there were no
exceptions, no `console.error`, no `render error` text, no executed or
injected `<script>`, inline handlers, `javascript:` URLs or `<img src=x>`, and
that the page made no requests other than `/` and `/events` and opened no
WebSockets. It also covers live pacing, pause and resume, an SSE drop
mid-stream and after completion (one reconnect, no duplicates), a proxy
restart (new boot id), and a self-check that `problems()` really detects
injected markup. It doesn't depend on the card layout. It reads only the
page's `S` state (`S.replaying`, `S.ex`) and `[data-id]` attributes.

## Known bugs, marked rather than fixed

- **proxy.py**: proxied responses carry `Server` and `Date` twice: the proxy's
  own, then the upstream's (`test_no_duplicate_response_headers`,
  expectedFailure).
- **proxy.py**: replies are relayed with `Connection: close` framing, so an
  upstream that drops a chunked stream looks like a clean end of body to the
  agent (`test_dropped_upstream_stream_is_visible_to_client`,
  expectedFailure). The dashboard still records the `stream_error`.
- **dashboard.html**: after a `reset` with a new boot id, `checkVersion()`
  schedules a reload but `handle()` keeps applying the rest of that replay
  to the stale state. The re-sent `exchange_start` for an id it already
  holds throws a TypeError in `analyze()` (`th.key`). The smoke test is
  marked `todo`.

When one of these is fixed, the marked test starts passing. Remove its
`expectedFailure` or `todo`.

## Fixtures

| fixture | covers |
|---|---|
| `basic` | HEAD probe; prompt with reminder, then thinking + text + Bash; tool result, then text; side call (no tools, haiku); count_tokens; non-streaming JSON reply |
| `concurrent_threads` | main thread with two Agent calls; two subagent threads streaming interleaved event by event; repeated tool names (Read/Bash) with distinct ids; three calls in one reply; Agent results returned in reverse order; final reply with pending calls |
| `tool_pairing` | three calls in one reply with results out of order, one `is_error`; a result whose request arrives before its call's `block_start`; history resent with old results in every later request; an orphan result (`toolu_orphan_never_called`); a call with no result followed by a same-thread prompt |
| `failures` | 529 JSON error; SSE `error` mid-stream; client abort (`aborted`) during text and during tool input; dropped stream; unreachable (502); malformed final tool JSON; one exchange still sending and one still streaming at the end |
| `eviction` | 5 chained Bash calls, then `evicted` ids 1–3 (#4 holds the result of evicted #3's call) |
| `multi_session` | two `x-claude-code-session-id` values with identical prompts (and identical call ids, because the fake derives ids from the body), interleaved |
| `hostile` | `<script>`/`<img onerror>` in system, prompt, header value, tool name, input and result, model text and Markdown (`javascript:` link), thinking, a `text/html` 502 body and a `Set-Cookie`; a non-JSON request body; a 200 KB line; a 5000-line result; unicode |

`fixtures/manifest.json` describes each fixture as follows, so tests can
assert on what a correct page should derive:

```json
{"<fixture>": {
  "description": "...",
  "events": 130,                       // lines in the .jsonl
  "exchange_ids": [1, 2, ...],         // every exchange_start id
  "live_ids": [4, 5],                  // ids left after evictions
  "evicted": [1, 2, 3],
  "states": {"1": "completed", ...},   // sending | streaming | completed | failed | interrupted (§13.4)
  "calls": {"<tool_use id>": {"name": "Read", "call_in": 3, "result_in": 4 | null,
                              "is_error": false, "note": "..."}},
  "orphan_results": [{"tool_use_id": "...", "in": 5}],
  "notes": ["..."],
  "threads": {...}, "sessions": {...}  // where relevant
}}
```

### Adding a fixture

1. In `make_fixtures.py`, write `def fx_name(): fx = Fixture("name", "description"); ...; return fx`
   and add it to `FIXTURES`. Building blocks:
   - `fx.x(body, method=, path=, hdrs=headers(session, extra))` makes an
     exchange. Its scenario comes from the body's keyword, as in
     `upstream.py`.
   - Modifiers: `.abort_after(n)`, `.drop_after(n)`, `.hang_after(n)`
     (`0` means still sending) and `.unreachable()`.
   - `fx.run(a, b)` runs exchanges one after another.
     `fx.interleave(a, b, pattern="0101")` runs them step by step
     (round robin after the pattern).
   - `fx.evict([ids])` adds an eviction and `fx.raw(event, data)` adds any
     other event.
   - `e.reply()` and `e.calls()` give the reply's blocks, which you use to
     build the next request's history (`assistant(...)`, `results(...)`,
     `user(...)`).
   - `call(fx, xid, block, result_in=, error=, note=)` and
     `fx.expect[...]` record the manifest expectations. States are recorded
     automatically.
2. Run `python3 tests/make_fixtures.py` and commit both the `.jsonl` file and
   `manifest.json`. `run.sh` fails while they are stale.
3. Add the name to the list in `test_page_smoke.mjs`.

## Fixture server

```
python3 tests/fixture_server.py --fixture failures [--mode live --delay 50] [--pause-at N] [--drop-after N] [--port 0]
```

It prints `PORT <n>`. Endpoints (all GET except emit):

| endpoint | effect |
|---|---|
| `/` | the dashboard page (`--page` selects another file) |
| `/events` | SSE: `reset`, the retained buffer, `replay_done`, then live events and a `ping` every 15 s |
| `/fixtures` | the manifest |
| `/control/status` | `{fixture, mode, published, total, paused_at, clients, buffered_ids, evicted, boot}` |
| `/control/load?fixture=&mode=&delay=&pause_at=&drop_after=&new_boot=1` | replace the timeline and drop clients; `new_boot=1` simulates a proxy restart |
| `/control/pause`, `/control/step?n=K`, `/control/resume` | control publishing |
| `/control/drop` | close every `/events` connection; EventSource reconnects about 1 s later |
| `/control/wait?published=N` | block until N events are published |
| `POST /control/emit` | publish one extra `{"event","data"}` |

In `replay` mode (the default), the fixture is already in the buffer when the
page connects. In `live` mode, publishing starts when the first client
connects and events go out `delay` ms apart. Like proxy.py, the buffer drops
`block_*` events of finished exchanges and removes evicted exchanges.

## Writing a page test (harness.mjs / cdp.mjs)

```js
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { startFixtureServer, openDashboard, showFixture, waitReady, waitReconnect,
         problems, exchangeIds, clickEachExchange, settle, eventStreamCount, closeAll } from './harness.mjs';

const srv = await startFixtureServer();            // { port, url, control(p, params), emit(e, d), manifest(), stop() }
const { page } = await openDashboard(srv);         // browser via cdp.launch(), page at srv.url
after(closeAll);

test('selection survives a burst', async () => {
  const man = await showFixture(page, srv, 'concurrent_threads', { pause_at: 60, wait: false });
  await page.waitFor(`S.replaying === false && S.ex.size > 0`);
  await page.click('[data-id="2"]');               // real pointer events
  await srv.control('resume');
  await page.waitFor(`S.ex.size === ${man.live_ids.length}`);
  assert.deepEqual(await problems(page), []);
});
```

`Page` (cdp.mjs) has `eval(exprOrFn, ...args)`, `waitFor(expr, {timeout})`,
`click(selector | {x, y}, {dx, dy, hold})`, `point(selector)`,
`key('j' | 'Enter' | 'Home', {modifiers: ['Shift']})`, `type(text)`,
`wheel(selector, dy)`, `goto(url)`, `reload()`, `screenshot(file)` and
`clearLogs()`. It also exposes `exceptions`, `consoleErrors`, `console`,
`requests` and `webSockets`. `launch({width, height, chromium, args})`
returns a `Browser` with `newPage(url)` and `close()`. The viewport defaults
to 1400×900, because at 800 px the layout stacks and clicks miss.
