// Shared helpers for browser tests of dashboard.html against tests/fixture_server.py.
// Node 22 built-ins only.
//
//   import { startFixtureServer, openDashboard, problems } from './harness.mjs';
//   const srv = await startFixtureServer();               // python3 tests/fixture_server.py --port 0
//   const { browser, page } = await openDashboard(srv);   // headless Chromium on srv.url
//   await showFixture(page, srv, 'failures');             // load + navigate + wait for replay
//   assert.deepEqual(await problems(page), []);
//   await closeAll();                                     // browsers + servers started here
//
// Layout-agnostic on purpose: readiness and counts come from the page's top-level
// `S` state object (S.replaying, S.ex), not from DOM structure.

import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { launch, sleep } from './cdp.mjs';

export const TESTS = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.dirname(TESTS);
const started = [];

/** Starts tests/fixture_server.py on a free port. Returns
 *  { port, url, proc, control(path, params) -> status JSON, manifest() -> manifest JSON, stop() }. */
export async function startFixtureServer({ fixture = 'basic', page, args = [] } = {}) {
  const argv = [path.join(TESTS, 'fixture_server.py'), '--port', '0', '--fixture', fixture, ...(page ? ['--page', page] : []), ...args];
  const proc = spawn(process.env.PYTHON || 'python3', argv, { stdio: ['ignore', 'pipe', 'pipe'] });
  let err = '';
  proc.stderr.on('data', d => { err = (err + d).slice(-4000); });
  const port = await new Promise((resolve, reject) => {
    let out = '';
    const t = setTimeout(() => reject(new Error('fixture_server did not start: ' + err)), 10000);
    proc.stdout.on('data', d => {
      out += d; const m = out.match(/PORT (\d+)/);
      if (m) { clearTimeout(t); resolve(+m[1]); }
    });
    proc.on('exit', c => { clearTimeout(t); reject(new Error(`fixture_server exited (${c}): ${err}`)); });
  });
  const url = `http://127.0.0.1:${port}/`;
  const srv = {
    port, url, proc,
    async control(p, params = {}) {
      const qs = new URLSearchParams(Object.entries(params).filter(([, v]) => v != null).map(([k, v]) => [k, String(v)]));
      const r = await fetch(`${url}control/${p}${qs.size ? '?' + qs : ''}`);
      const body = await r.json();
      if (!r.ok) throw new Error(`control/${p} -> ${r.status} ${JSON.stringify(body)}`);
      return body;
    },
    async emit(event, data) {
      const r = await fetch(`${url}control/emit`, { method: 'POST', body: JSON.stringify({ event, data }) });
      return r.json();
    },
    async manifest() { return (await fetch(url + 'fixtures')).json(); },
    async stop() {
      if (proc.exitCode === null && proc.signalCode === null) {
        proc.kill('SIGTERM');
        await Promise.race([new Promise(r => proc.once('exit', r)), sleep(2000)]);
        if (proc.exitCode === null && proc.signalCode === null) proc.kill('SIGKILL');
      }
    },
  };
  started.push(srv);
  return srv;
}

/** Launches a browser and opens the dashboard. Returns { browser, page }. */
export async function openDashboard(srv, opts = {}) {
  const browser = await launch(opts);
  started.push(browser);
  const page = await browser.newPage(srv.url);
  return { browser, page };
}

/** Waits until the page has applied the full replay (`replay_done` handled) and painted. */
export async function waitReady(page, { count, timeout = 15000 } = {}) {
  await page.waitFor(`typeof S !== 'undefined' && S.replaying === false`, { timeout, message: 'replay_done applied' });
  if (count != null) await page.waitFor(`S.ex.size === ${count}`, { timeout, message: `S.ex.size === ${count}` });
  await settle(page);
}

/** Two animation frames: lets coalesced renders flush. */
export const settle = page => page.eval(`new Promise(r => requestAnimationFrame(() => requestAnimationFrame(() => setTimeout(r, 30))))`);

/** Loads a fixture into the server, (re)navigates the page and waits for the replay.
 *  opts: { mode: 'replay'|'live', delay, pause_at, drop_after, new_boot, wait: true } */
export async function showFixture(page, srv, name, { wait = true, ...opts } = {}) {
  const man = (await srv.manifest())[name];
  await srv.control('load', { fixture: name, ...opts });
  page.clearLogs();
  await page.goto(srv.url);
  if (wait && (opts.mode || 'replay') === 'replay' && opts.pause_at == null) await waitReady(page, { count: man?.live_ids.length });
  return man;
}

/** Number of /events requests the page has made since its last clearLogs(). */
export const eventStreamCount = page => page.requests.filter(r => { try { return new URL(r.url).pathname === '/events'; } catch { return false; } }).length;

/** Waits until the page has opened more than `before` /events connections, the server sees a
 *  client again and the new replay is applied. Use after srv.control('drop') or drop_after. */
export async function waitReconnect(page, srv, before, { count, timeout = 15000 } = {}) {
  const end = Date.now() + timeout;
  while (eventStreamCount(page) <= before) { if (Date.now() > end) throw new Error('page did not reconnect /events'); await sleep(50); }
  while ((await srv.control('status')).clients < 1) { if (Date.now() > end) throw new Error('server saw no client'); await sleep(50); }
  await sleep(150);
  await waitReady(page, { count, timeout: Math.max(1000, end - Date.now()) });
}

/** Ids of the exchanges the page currently holds (from S.ex). */
export const exchangeIds = page => page.eval(`[...S.ex.keys()].sort((a, b) => a - b)`);

/** Clicks (with real pointer events) the first visible element carrying data-id=<id>, for each
 *  id. Layout-agnostic: it only exercises whatever rendering a click triggers. Returns ids clicked. */
export async function clickEachExchange(page, ids) {
  const clicked = [];
  for (const id of ids) {
    const sel = `[data-id="${id}"]`;
    if (!(await page.eval(`!!document.querySelector('${sel}')`))) continue;
    try { await page.click(sel); clicked.push(id); } catch { /* invisible (filtered) */ }
    await settle(page);
  }
  return clicked;
}

/** Everything that should never happen on the page, as a list of strings (empty = fine):
 *  uncaught exceptions, console errors, "render error" text, executed or injected
 *  script/handler/javascript: content from untrusted payloads. */
export async function problems(page) {
  const out = [...page.exceptions.map(e => 'exception: ' + e.slice(0, 500)), ...page.consoleErrors.map(e => 'console.error: ' + e.slice(0, 500))];
  const dom = await page.eval(() => {
    const p = [];
    const text = document.body.innerText;
    const i = text.indexOf('render error');
    if (i >= 0) p.push('render error text: ' + text.slice(i, i + 200));
    if (window.__pwned !== undefined) p.push('untrusted script executed (window.__pwned set)');
    const scripts = [...document.querySelectorAll('script')].filter(s => s.textContent.includes('__pwned') || s.src);
    if (scripts.length) p.push(`${scripts.length} injected <script> element(s)`);
    for (const el of document.body.querySelectorAll('*')) {
      for (const a of el.attributes) {
        if (/^on/i.test(a.name)) p.push(`inline handler ${a.name} on <${el.tagName.toLowerCase()}>`);
        if (/^(href|src|action|formaction)$/i.test(a.name) && /^\s*javascript:/i.test(a.value)) p.push(`javascript: URL in ${a.name}`);
      }
      if (/^(IFRAME|OBJECT|EMBED)$/.test(el.tagName)) p.push(`injected <${el.tagName.toLowerCase()}>`);
    }
    if (document.querySelector('img[src="x"]')) p.push('injected <img src=x>');
    return [...new Set(p)].slice(0, 20);
  });
  return [...out, ...dom];
}

/** Network requests the page made that are not the page itself, /events or a favicon,
 *  plus any WebSocket. Empty = "SSE only, no polling" (SPEC §13.8 #10). */
export function foreignRequests(page, srv) {
  const ok = new Set(['/', '/events', '/favicon.ico', '/index.html']);
  const bad = page.requests.filter(r => { try { const u = new URL(r.url); return !(u.origin === new URL(srv.url).origin && ok.has(u.pathname)) && !r.url.startsWith('data:'); } catch { return true; } }).map(r => `${r.method} ${r.url}`);
  return [...bad, ...page.webSockets.map(u => 'websocket ' + u)];
}

/** Closes every browser and server started through this module. */
export async function closeAll() {
  for (const x of started.splice(0).reverse()) { try { await (x.close ? x.close() : x.stop()); } catch {} }
}
process.on('exit', () => { for (const x of started) { try { (x.proc?.kill ? x.proc : null)?.kill('SIGKILL'); } catch {} } });
