# SPEC.md history

Three commits touch `SPEC.md`, all on 2026-11-05. For each: what changed, why,
and where the reason comes from. "Session" means the Claude Code session that
built and fixed the dashboard; its traffic is what the dashboard observed.

## 1. `b6b4f80` — Initial commit: naive proxy and dashboard spec (amit)

**What:** §1–§12 of the spec, status "ready to build", with the 131-line
`proxy.py` that only prints traffic. Thirteen decisions (D1–D13) settle the
layout: an expanding-card feed, newest first, one card per HTTP exchange.

**Reason:** none in the commit message. The spec itself gives it (§1): messages
pile up fast while an agent works, so the page must be scannable with details
opened only on demand. *Source: the spec text.*

Build from this spec: `2a2fa57` (`dashboard.html`, 1654 lines; `proxy.py`
rewritten with a recorder and a second port). Two fixes followed, both edits to
that build:

- `efdaed4` — clicking a section header collapsed the whole card.
- `ce80570` — clicks on a streaming card were lost (its rows were redrawn many
  times a second); a dropped upstream stream left a card "in flight" forever;
  a tab kept stale code and state across a proxy restart.

## 2. `a501e76` — docs: plan two-pane dashboard improvements (Tair Israeli)

**What:** +213 lines: §13, an implementation plan that overrides §4–§11 where
they conflict. The card feed is replaced by a list pane and a detail pane
(§13.1); view state is kept apart from exchange data and keyed by exchange ID
(§13.2); a live event log, explicit lifecycle states, tool pairing by call ID
only, stricter JSON views, a build sequence and ten acceptance checks (§13.3–8).
Status becomes "implementation plan proposed".

**Reason:** the message gives none. Looking deeper:

- *The build before it* (`ce80570`): every bug fixed so far came from putting
  interactive details inside a feed that redraws constantly. The session's
  user hit all three (lost clicks, "closing a section closes the card", cards
  inherited as expanded after a restart). §13.1 removes the cause; §13.2's
  "state keyed by exchange ID, never DOM position" and "arrivals never replace
  a selection" are those bugs restated as requirements. *Source: the session
  and commits `efdaed4`, `ce80570`.*
- *The code* is quoted in §13.3 ("`renderRaw` makes the raw response available
  only after completion"), so the author read `dashboard.html`. *Source: the
  diff of `a501e76`.*
- §13.4's "do not invent terminal states based on elapsed time" matches the
  stuck-card bug, whose fix belonged in the proxy, not in a page timeout.
  *Source: inference from `ce80570`; not stated by the author.*

## 3. `028589a` — spec: record session grouping, stale-page reload and test tooling (amit)

**What:** §13.9, three additions agreed before building §13: group threads by
Claude Code session; the page reloads itself when the proxy's boot ID or the
page file changes; tests use only stdlib, Node built-ins and system Chromium.

**Reason:** from the session. (1) Two sessions through one proxy were merged
into one `main` thread because threads were matched by message history only;
every request carries `x-claude-code-session-id`, so grouping by it is cheap.
(2) The auto-reload was already shipped in `ce80570` and conflicts with
§13.2's view-state rules, so it had to be written down. (3) §13.7 forbids new
dependencies, but the page logic is JavaScript, so Python-only tests cannot
cover it. *Source: the session's discussion before implementation.*

Build from §13: `47739fb` (two-pane page), `83a2b03`, `3196274`/`af65eee`
(test harness, 44 tests), `020d58b` (three bugs the harness found), `68dcce6`.

## Rebuilding vs. editing

The code was built from the spec once (`2a2fa57`). From `efdaed4` on, every
change was an edit. The §13 revision (`47739fb`) replaced `dashboard.html` as a
file, but kept its renderers, reducer and helpers and rewrote only the layout,
view state and pairing; `proxy.py` was never rebuilt. So the switch to editing
happened right after the first build and was never reversed.

## Findings in the traffic

From the session's own traffic through the proxy (110 requests, one session):

- **Rewrites are rare; branches are common.** Of 17 "history rewritten" flags,
  16 were one-shot side calls that share an opening message (titles,
  summaries). Only one was real. Fixed in `83a2b03`.
- **Dropped streams happen.** Two replies got headers after ~40 s and were then
  cut mid-stream. Python raises `IncompleteRead`, not `OSError`; the first
  build did not catch it (`ce80570`). The proxy also relayed replies
  close-delimited, so the agent could not tell a cut stream from a complete
  one (`020d58b`).
- **Every request identifies its session** twice: the
  `x-claude-code-session-id` header and `metadata.user_id`.
- **The main loop is a minority of requests.** 26 main, 17 side calls, 67 in
  one subagent thread; the subagent was linked to its Agent call by prompt text.
- **Routing is fragile.** `ANTHROPIC_BASE_URL` set in the shell is overridden
  by `~/.claude/settings.json`; a session started that way never reached the
  proxy. The `--settings` flag works.
- **Cost shows `—`** for the models in use: the price table has no 5.x entries.
