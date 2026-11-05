# SPEC: Live traffic dashboard for `proxy.py`

Status: **ready to build**. All design decisions are settled (§11).

## 1. Goal

While an agent works through `proxy.py`, you open a browser page that updates
live. It shows **every request the harness sends and every reply, as they
happen, newest first**. You can:

- tell apart at a glance your prompt, the model's reply, a tool call and its
  result, plus the less obvious pieces (system instructions, tool definitions,
  thinking, injected reminders, errors);
- open any request and drill down into each of its parts, down to the exact
  JSON;
- see what changed since the previous request;
- read statistics (tokens, cost, time, tool usage) next to the messages,
  never instead of them.

Readability is the main requirement. Messages pile up fast while the agent
works. Every layout choice below is there so you can scan quickly and open
details only on demand.

### Non-goals

- Changing traffic. The proxy stays a pure observer and forwards bytes
  unchanged.
- Being a general HTTP debugger, a multi-user service, or long-term storage.
- Authentication. The dashboard is local only (D10).

## 2. What the traffic actually looks like

These facts about the Anthropic Messages API as Claude Code uses it drive the
design:

1. **Each request resends the whole conversation.** `POST /v1/messages`
   carries `system`, `tools` and the full `messages` array. Request N
   contains everything in request N-1 plus a few new messages at the end.
   "The conversation so far" and "the new message" are therefore computed:
   the new part is the suffix that wasn't in the previous request of the
   same thread.
2. **A tool's result arrives in a later request.** The `tool_use` block
   comes in reply N. The matching `tool_result` (same `tool_use_id`) comes
   inside a `user` message of request N+1.
3. **A `user` message is often not the user.** It may carry the human's
   prompt, tool results, or Claude Code's injected
   `<system-reminder>…</system-reminder>` text. Each needs its own label.
4. **Replies are streamed as SSE.** The sequence is `message_start` →
   (`content_block_start` → `content_block_delta`… → `content_block_stop`)
   for each block → `message_delta` (`stop_reason`, `usage`) →
   `message_stop`. The block types are `text`, `tool_use` (its input is
   partial JSON until the block stops) and `thinking`. Errors can arrive as
   an HTTP status or as an `error` event in the middle of the stream.
5. **Several conversations run at once.** Besides the main loop there are
   subagents (from the Agent tool) and small side calls (titles, summaries,
   `count_tokens`). They interleave in time. Each one is a separate thread
   with its own system prompt and history.
6. **`system` and `tools` are large** (tens of KB) and usually identical
   from one request to the next.
7. **Not every request is a message call.** `HEAD`, `GET` and
   `count_tokens` requests also go through.

## 3. Architecture ✅

```
agent ──HTTP :8041──▶ proxy.py ──HTTPS──▶ api.anthropic.com
                         │ (copies request body + response chunks; forwarding unchanged)
                         ▼
                    recorder ──SSE /events──▶ browser
                         └─ dashboard server on 127.0.0.1:8042 (GET / = page)
```

- ✅ **The dashboard runs in the same process, on a second port**
  (default `8042`, set with `--dash-port`). The proxy port forwards every path
  upstream, so the dashboard must not share it.
- **Forwarding is untouched.** The recorder receives *copies*. Its failures
  are caught and logged, and it never blocks the relay loop. A slow or
  closed browser cannot slow the agent: each browser gets a bounded queue,
  and events are dropped for that browser if its queue overflows, after
  which it re-syncs.
- **Live channel.** Server-Sent Events (`text/event-stream`). They are
  one-way, need only the standard library and reconnect automatically.
- **Page.** One self-contained HTML file with inline CSS and JS. It has no
  build step and no CDN, so it works offline. The JS computes diffs, pairs
  tools and does the rendering.
- **Late join and reload.** The server keeps an in-memory replay buffer
  (D5: 500 exchanges / 200 MB). A new or reloaded tab replays it and then continues live.
- **Carried over from `proxy.py`.** Standard library only, binds to
  `127.0.0.1`, one command to start, credentials masked.
- **Terminal output** becomes one line per exchange. `--verbose` keeps
  today's full dumps (D9).

### Events (server → page)

| event            | payload (main fields) |
|------------------|-----------------------|
| `exchange_start` | `id`, `t`, `method`, `path`, masked `headers`, `request` (parsed JSON, or raw text) |
| `response_start` | `id`, `t`, `status`, `model`, `message_id`, initial `usage` |
| `block_start`    | `id`, `index`, `type`, `name`, `tool_use_id` |
| `block_delta`    | `id`, `index`, `text` / `thinking` / `partial_json` |
| `block_stop`     | `id`, `index` |
| `response_end`   | `id`, `t`, `stop_reason`, final `usage`, assembled `content` |
| `exchange_error` | `id`, `t`, kind (`unreachable` / `http` / `stream_error`), status, body |

The proxy parses the SSE. A non-streaming JSON reply becomes one
`response_end`.

## 4. Page layout

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ ● live  · 1 in flight (4.2s)      Requests 37 · Tokens 812k in / 21k out ·   │
│                                   Cost $1.84 · Errors 1 ⚠    [Stats ▾]       │
├───────────────┬──────────────────────────────────────────────────────────────┤
│ FILTERS       │  ↑ 3 new requests                                            │
│ Threads       │ ┌──────────────────────────────────────────────────────────┐ │
│  ☑ main       │ │ #37 14:02:11 main · sonnet · ⏳ streaming… · 2 tools      │ │
│  ☑ agent:1    │ │   NEW: ⚙ result Read proxy.py (132 lines)                 │ │
│  ☐ side calls │ │   REPLY: "The proxy streams…" ▌  ▶ Bash · ls -la           │ │
│ Tools         │ └──────────────────────────────────────────────────────────┘ │
│  Bash   14    │ ┌──────────────────────────────────────────────────────────┐ │
│  Read    9    │ │ #36 14:02:07 main · sonnet · 200 · 3.1s · 41k/312 · $0.13 │ │
│  Edit    2 ⚠1 │ │   NEW: 👤 "now read proxy.py and explain"                 │ │
│ Kinds         │ │   REPLY: ▶ Read · proxy.py                                │ │
│  ☑ thinking   │ └──────────────────────────────────────────────────────────┘ │
│  ☑ reminders  │   … older requests …                                         │
│ ☐ errors only │                                                              │
└───────────────┴──────────────────────────────────────────────────────────────┘
```

- **Header bar.** Connection status, any requests in flight (with a live
  timer) and the session totals. A Stats panel expands (§8).
- **Filter sidebar** (§9).
- **Request feed, newest first.** One card per HTTP exchange. A new card
  goes on top, and the reply in the top card streams in place.

### Newest-first without losing your place

- If you're at the top, new cards appear and push older ones down.
- If you've scrolled down to read, new cards are inserted above **without
  moving what you're looking at** (the page keeps its scroll anchor). A
  pill reading "↑ N new requests" appears. Clicking it, or pressing `Home`,
  jumps to the top.
- Cards never re-order. Expanded or collapsed state is kept while a card
  updates.
- A newly arrived card is highlighted briefly. There is no other motion.

## 5. A request card

### Collapsed (the default): one glance per request

- **Row 1, the header.** `#n`, time, thread tag (colour and name), model, an
  HTTP status chip, latency, tokens in/out (with cache read/write when
  present), cost, and `stop_reason`. A state chip shows `sending`,
  `thinking`, `streaming`, `tool input…`, `done` or **`error`**.
- **Row 2, NEW.** A summary of only what's new in this request, using the
  message vocabulary icons (§6), for example
  `👤 "now read proxy.py…"` or `⚙ 2 results: Read ✓, Bash ✗`.
- **Row 3, REPLY.** The first line of the model's text, plus a chip for each
  tool call (`▶ Read · proxy.py`).
- The header shows a **Δ badge** if `system` or `tools` changed compared
  with the previous request of the thread.

### Expanded: the parts of a request, clearly separated

The expanded card shows the sections below in this fixed order. Each section
has its own header strip and accent colour, its own collapse toggle, and a
size hint (e.g. `System · 18.4 KB`), so the parts never run together:

1. **System instructions.** Shown **in full**. Long text is collapsed to the
   first ~15 lines with "show all N lines". The string or list-of-blocks
   form, and any `cache_control` markers, are shown as small tags.
2. **Tool definitions (N).** A list of tool names with the first line of
   each description. Each one expands to its full description and its input
   schema as readable JSON (§7). When the tools are unchanged from the
   previous request of the thread, the section reads "same as #12" with a
   link (D13).
3. **Conversation so far (M messages).** Collapsed by default. When opened,
   it shows the earlier messages rendered with the message vocabulary.
   Earlier tool calls are paired with their results here too.
4. **New in this request.** Open by default. These are the messages added
   since the previous request of this thread: the human prompt, tool
   results (each linked back to the call that requested it, with "↖ from
   #36") and reminders.
5. **Reply.** Open by default. It shows the model's text (rendered), any
   thinking, and tool calls. **Each tool call is shown beside its result**
   (§5.1).
6. **Changes since previous request.** See §5.2.
7. **Stats.** Tokens by type, cost breakdown, timing (time to first byte,
   time to first token, total) and request/response sizes.
8. **Raw.** Masked request headers, the request JSON, response headers and
   the raw SSE text, each with a copy button.

### 5.1 Tool call beside its result

```
┌─ ▶ Bash ──────────────────────────────┬─ ⚙ result  (in #38, +1.9s) ─────────┐
│ command: ls -la                       │ total 12                             │
│ description: List files               │ -rw-r--r-- proxy.py                  │
│ [input JSON ▸]                        │ -rw-r--r-- SPEC.md       [all 4 ▸]   │
└───────────────────────────────────────┴──────────────────────────────────────┘
```

- The call is on the left and the result on the right, on the same row.
  Narrow windows stack them vertically.
- Until the next request arrives, the right side shows **"⏳ waiting for
  result"**. When the request containing the matching `tool_result`
  arrives, the result fills in, with a link to that request and the delay.
- An `is_error` result gets a red frame and an **✗** in every summary that
  mentions it.
- A result that never arrives (the thread moves on without it, or the agent
  is interrupted) shows **"no result received"**.
- The same pairing appears in both the request's Reply section and the next
  request's New section. One is the primary view and the other is a link,
  so the content is never shown twice at full size.

### 5.2 Changes since the previous request

Each request is compared with the previous request **of the same thread**:

- **messages:** "+3 appended" (expected), or a **⚠ history rewritten**
  warning when the old messages are no longer a prefix. That happens after
  compaction, edited retries or truncation. The warning includes a message
  diff.
- **system:** unchanged, or a line diff of the text.
- **tools:** added, removed and changed tools by name. Changed descriptions
  or schemas get a diff.
- **params:** `model`, `max_tokens`, `temperature`, `thinking`, `metadata`,
  `stream`, and so on, shown as a changed-fields table.
- **headers:** changes in `anthropic-beta`, `anthropic-version` and
  `user-agent` (masked values stay masked).

## 6. Message vocabulary

Each kind has its own **icon, label, accent colour and indentation**. Shape
and position carry the meaning as well as colour, so the kinds stay apart
for colour-blind readers and in either theme.

| kind | source | display |
|---|---|---|
| 👤 **Prompt** | `user` text that isn't a tool result or reminder | strong accent; full text |
| 🔔 **Reminder** | `<system-reminder>` spans in user text | muted; tags stripped; one-line preview; expands (D7) |
| 🤖 **Model text** | assistant `text` | strong accent; Markdown rendered; streams with a cursor |
| 💭 **Thinking** | assistant `thinking` | muted; collapsed with a length hint (D6) |
| ▶ **Tool call** | assistant `tool_use` | `Name · main argument` chip; paired (§5.1) |
| ⚙ **Tool result** | `user` `tool_result` | monospace; first ~10 lines; ✗ red if error |
| 🖼 **Image / document** | `image` / `document` blocks | type, size and media type; no inline base64 |
| ⚠ **Error** | HTTP ≥ 400, stream `error`, proxy 502 | red banner on the card and in the header |

The **main argument** of each tool goes in its chip: `command` for Bash,
`file_path` for Read/Edit/Write, `pattern` for Grep/Glob, `description`
for Agent, and `url` for WebFetch. Any other tool shows its first short
string argument.

## 7. Formatting rules

- **Long text is collapsible everywhere.** Each long text shows a preview of
  N lines and a "show all (N lines, X KB)" control. Nothing is silently
  truncated. Expand-all and collapse-all buttons appear on each card and
  globally.
- **JSON is readable.** It is pretty-printed with syntax colouring and
  foldable objects and arrays. Folds open to depth 2 by default, and a
  folded node shows its child count. Strings longer than a line are clipped
  with an expander. Each JSON block has a copy button.
- **Prose and code are distinguished.** Model text is rendered as Markdown,
  and any HTML in it is always escaped. Tool input and output, schemas and
  raw data use monospace with wrapping.
- **Sections are visibly separate.** Each part of a request (§5) has a
  header strip, accent and spacing. Nested content is indented, never just
  re-coloured.
- **Numbers are human-readable.** Sizes use KB, tokens use `41.2k`, times
  use `3.1s`, and the exact value is shown in a tooltip.
- **Light and dark themes** follow `prefers-color-scheme`, with a manual
  toggle.
- **It stays fast with long sessions.** Collapsed sections render their
  body only when opened. Stream deltas are coalesced per animation frame.

## 8. Statistics

Statistics are shown *in addition to* the messages and link back to them.

- **Per request** (in the card header and its Stats section): input,
  output, cache-read and cache-write tokens; cost; latency (time to first
  byte, time to first token, total); and request and response size.
- **Session panel** (expands from the header):
  - totals and a breakdown by thread and by model;
  - a **tool usage table** with calls, errors, average result size and
    average time from call to result per tool. Clicking a tool filters the
    feed to it;
  - a small chart of tokens and cost per request over time. Clicking a bar
    opens that request.
- **Cost** is computed from a built-in price table per model (input,
  output, cache write, cache read). An unknown model shows `—` instead of a
  guessed number (D11).

## 9. Filtering and navigation

- **By tool.** Show only requests whose reply calls, or whose new messages
  return results for, the selected tools. Inside a card the matching pairs
  are highlighted.
- **By thread.** Main, each subagent and side calls, with a checkbox each.
  Side calls are off by default (D3). A subagent thread links to the Agent
  call that started it.
- **By kind.** Hide thinking, reminders and other kinds inside the cards.
- **Errors only.** The error count in the header is always visible. Clicking
  it applies this filter, and `e` / `Shift+E` jumps to the next or previous
  error.
- **Non-message requests** (`HEAD`, `GET`, `count_tokens`) are hidden by
  default. A toggle shows them as compact one-line cards.
- **Text search** over loaded content (nice-to-have).
- **Keyboard shortcuts.** `j`/`k` for the next or previous card, `o` /
  `Enter` to expand, `Home` for the newest, and `/` for search.

## 10. Acceptance checks

1. Run `python3 proxy.py`, open `http://localhost:8042`, start
   `ANTHROPIC_BASE_URL=http://localhost:8041 claude` and send a prompt. A
   card appears on top with the prompt under **NEW**, and the reply streams
   live under **REPLY**.
2. Expanding the card shows System (full), Tools (N), Conversation so far,
   New, Reply, Changes, Stats and Raw, each visibly separate.
3. Ask for something that uses tools. Each tool call shows "waiting for
   result" and then fills in **beside** its result once the next request
   arrives, with a link to that request.
4. Over a 20-turn session, "New" never repeats earlier messages, and
   "Changes" reports "+N appended" (or flags a rewrite).
5. Scrolling down during a burst doesn't move the content you're reading.
   The "↑ N new" pill appears.
6. A failing tool and a forced API error, for example a bad upstream, are
   visible from the header without opening anything.
7. Filtering by `Bash` leaves only requests that involve Bash.
8. Reloading the page mid-session restores everything in the buffer.
9. Closing the tab, or a dashboard failure, never delays or alters the
   agent's traffic.
10. No full token or API key appears on the page or in the event stream.

## 11. Decisions

All decisions are settled. ✅ = decided.

| # | Topic | Decision |
|---|---|---|
| D1 | Where the dashboard is served | ✅ Same process, second port `127.0.0.1:8042` (`--dash-port`). |
| D2 | Primary view | ✅ Request feed, one card per HTTP exchange, newest first (§4–§5). |
| D3 | Concurrent threads | ✅ One interleaved feed with a thread tag (colour and name) on every card. Side calls are hidden by default and turned on in the Threads filter. Subagent threads are **linked to the Agent tool call that started them**: the subagent's first user prompt is matched against the call's `prompt` input, and the link goes both ways ("↘ thread agent:1" on the call, "↖ started by #36" on the thread's first card). A thread that can't be matched stays unlinked. |
| D4 | Tool call ↔ result | ✅ Side by side (§5.1). |
| D5 | Replay buffer | ✅ In memory, keeping the last **500 exchanges or 200 MB**, whichever comes first. The oldest are evicted first, and the page shows "N older requests evicted". No disk persistence in v1. `--save file.jsonl` is a possible later addition. |
| D6 | Thinking blocks | ✅ Collapsed by default, with a length hint (`💭 thinking · 1.2k chars`). Redacted thinking is shown as `💭 redacted`. |
| D7 | System reminders | ✅ Their own muted 🔔 kind, with the tags stripped in the display. Raw keeps them as sent. |
| D8 | APIs supported | ✅ Anthropic Messages API only in v1. Parsing sits behind an adapter interface. Requests the adapter doesn't recognise (e.g. Codex / OpenAI Responses) become generic cards showing the header row and Raw only. |
| D9 | Terminal output | ✅ One line per exchange by default (`14:02:11 #37 POST /v1/messages 200 3.1s 41k/312 $0.13`). `--verbose` restores today's full dumps. |
| D10 | Access to the page | ✅ Bound to `127.0.0.1`, no token. It is a local lab tool, and anyone on this machine can open it. |
| D11 | Price table | ✅ A small dictionary in the code, keyed by model-id prefix, with input, output, cache-write and cache-read prices per MTok and a "prices as of <date>" comment. It is edited by hand. An unknown model shows `—` for cost. |
| D12 | Markdown | ✅ A minimal hand-written renderer covering headings, lists, fenced and inline code, bold and italic, and links. It escapes all HTML first and has no dependencies. |
| D13 | Unchanged tool definitions | ✅ A card whose tools match the previous request of its thread shows "Tools (N) · same as #12" with a link. The full list appears when the tools changed (with a Δ badge) and on a thread's first request. System instructions are **always** shown in full in every card, as required. |

## 12. Command line

```
python3 proxy.py [--port 8041] [--dash-port 8042] [--upstream URL] [--verbose]
```

At startup the proxy prints the proxy URL, the dashboard URL and the
`ANTHROPIC_BASE_URL=… claude` line to run the agent with.
