// Minimal headless-Chromium driver over the DevTools protocol. Node 22 built-ins only
// (global WebSocket and fetch); no npm packages.
//
//   import { launch } from './cdp.mjs';
//   const browser = await launch();                 // headless, 1400x900, temp profile
//   const page = await browser.newPage('http://127.0.0.1:1234/');
//   await page.waitFor('window.S && S.ex.size === 6');
//   await page.click('[data-id="3"]');              // real pointer events, not el.click()
//   await page.key('j');
//   page.exceptions                                 // uncaught exceptions seen so far
//   await browser.close();                          // also runs on process exit
//
// Lessons baked in: the viewport is forced to 1400x900 (at the 800px default the layout
// stacks and clicks miss); clicks scroll the target into view first and send
// mouseMoved/mousePressed/(hold)/mouseReleased, because element.click() fires no pointer
// events and gives false results.

import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export const sleep = ms => new Promise(r => setTimeout(r, ms));
const live = new Set();  // browsers to kill if the process exits early

function killAll() {
  for (const b of live) { try { b.proc.kill('SIGKILL'); } catch {} try { fs.rmSync(b.dir, { recursive: true, force: true }); } catch {} }
}
process.on('exit', killAll);
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { killAll(); process.exit(130); });

class Conn {
  constructor(url) {
    this.ws = new WebSocket(url);
    this.id = 0; this.pending = new Map(); this.listeners = new Map();
    this.ws.onmessage = m => {
      const d = JSON.parse(m.data);
      if (d.id && this.pending.has(d.id)) {
        const { resolve, reject, method } = this.pending.get(d.id); this.pending.delete(d.id);
        d.error ? reject(new Error(`${method}: ${d.error.message}`)) : resolve(d.result);
      } else if (d.method) for (const fn of this.listeners.get(d.method) || []) fn(d.params);
    };
    this.ws.onclose = () => { for (const { reject, method } of this.pending.values()) reject(new Error(`${method}: connection closed`)); this.pending.clear(); };
    this.open = new Promise((res, rej) => { this.ws.onopen = res; this.ws.onerror = () => rej(new Error('CDP websocket error: ' + url)); });
  }
  send(method, params = {}) {
    return new Promise((resolve, reject) => {
      const id = ++this.id; this.pending.set(id, { resolve, reject, method });
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }
  on(method, fn) { if (!this.listeners.has(method)) this.listeners.set(method, []); this.listeners.get(method).push(fn); }
  off(method, fn) { const l = this.listeners.get(method); if (l) l.splice(l.indexOf(fn) >>> 0, 1); }
  once(method) { return new Promise(r => { const f = p => { this.off(method, f); r(p); }; this.on(method, f); }); }
  close() { try { this.ws.close(); } catch {} }
}

const KEYS = {
  Enter: { code: 'Enter', keyCode: 13, text: '\r' }, Escape: { code: 'Escape', keyCode: 27 },
  Home: { code: 'Home', keyCode: 36 }, End: { code: 'End', keyCode: 35 }, Tab: { code: 'Tab', keyCode: 9 },
  ArrowUp: { code: 'ArrowUp', keyCode: 38 }, ArrowDown: { code: 'ArrowDown', keyCode: 40 },
  ArrowLeft: { code: 'ArrowLeft', keyCode: 37 }, ArrowRight: { code: 'ArrowRight', keyCode: 39 },
  PageUp: { code: 'PageUp', keyCode: 33 }, PageDown: { code: 'PageDown', keyCode: 34 },
  Backspace: { code: 'Backspace', keyCode: 8 },
};
const MODS = { Alt: 1, Control: 2, Meta: 4, Shift: 8 };

export class Page {
  constructor(conn, target, browser) {
    this.conn = conn; this.target = target; this.browser = browser;
    this.exceptions = [];     // uncaught exceptions: strings
    this.consoleErrors = [];  // console.error(...) calls: strings
    this.console = [];        // every console call: {type, text}
    this.requests = [];       // {url, type} for every network request (Network domain)
    this.webSockets = [];     // URLs of WebSockets the page opened
    conn.on('Runtime.exceptionThrown', p => {
      const e = p.exceptionDetails;
      this.exceptions.push((e.exception?.description || e.text || 'exception') + (e.url ? ` @${e.url}:${e.lineNumber}` : ''));
    });
    conn.on('Runtime.consoleAPICalled', p => {
      const text = p.args.map(a => a.value ?? a.description ?? a.type).join(' ');
      this.console.push({ type: p.type, text });
      if (p.type === 'error' || p.type === 'assert') this.consoleErrors.push(text);
    });
    conn.on('Network.requestWillBeSent', p => this.requests.push({ url: p.request.url, type: p.type, method: p.request.method }));
    conn.on('Network.webSocketCreated', p => this.webSockets.push(p.url));
  }

  async init({ width = 1400, height = 900 } = {}) {
    await this.conn.open;
    await Promise.all([this.conn.send('Runtime.enable'), this.conn.send('Page.enable'), this.conn.send('Network.enable')]);
    await this.conn.send('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: 1, mobile: false });
    return this;
  }

  /** Clears collected exceptions / console / network logs. */
  clearLogs() { this.exceptions.length = 0; this.consoleErrors.length = 0; this.console.length = 0; this.requests.length = 0; this.webSockets.length = 0; }

  async goto(url, { waitLoad = true, timeout = 15000 } = {}) {
    const loaded = waitLoad ? this.conn.once('Page.loadEventFired') : null;
    await this.conn.send('Page.navigate', { url });
    if (loaded) await Promise.race([loaded, sleep(timeout).then(() => { throw new Error('page load timeout: ' + url); })]);
  }
  reload() { return this.conn.send('Page.reload', { ignoreCache: true }); }

  /** Evaluates an expression (string) or a function with JSON-serialisable args in the page;
   *  returns its value (promises awaited). Throws on exceptions in the evaluated code. */
  async eval(expr, ...args) {
    const expression = typeof expr === 'function' ? `(${expr})(...${JSON.stringify(args)})` : expr;
    const r = await this.conn.send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true, userGesture: true });
    if (r.exceptionDetails) throw new Error('eval failed: ' + (r.exceptionDetails.exception?.description || r.exceptionDetails.text) + '\n  in: ' + expression.slice(0, 200));
    return r.result.value;
  }

  /** Polls `expr` (string or function) until it returns a truthy value; returns that value. */
  async waitFor(expr, { timeout = 10000, interval = 50, message } = {}) {
    const end = Date.now() + timeout; let last;
    while (Date.now() < end) {
      try { last = await this.eval(expr); if (last) return last; } catch (e) { last = e.message; }
      await sleep(interval);
    }
    throw new Error(`waitFor timed out after ${timeout}ms: ${message || String(expr).slice(0, 200)} (last: ${JSON.stringify(last)})`);
  }

  /** Center (or offset) of the first element matching `selector`, scrolled into view. null if none/invisible. */
  async point(selector, { dx = null, dy = null } = {}) {
    return this.eval((sel, dx, dy) => {
      const el = document.querySelector(sel); if (!el) return null;
      el.scrollIntoView({ block: 'center', inline: 'nearest' });
      const b = el.getBoundingClientRect(); if (!b.width || !b.height) return null;
      return { x: b.x + (dx ?? Math.min(b.width / 2, 30)), y: b.y + (dy ?? b.height / 2) };
    }, selector, dx, dy);
  }

  /** A real mouse click on the first element matching `selector` (or at {x, y}).
   *  Default point is 30px from the element's left edge, vertically centred. */
  async click(target, { hold = 110, after = 120, clickCount = 1, dx = null, dy = null, button = 'left' } = {}) {
    const p = typeof target === 'string' ? await this.point(target, { dx, dy }) : target;
    if (!p) throw new Error('click: no visible element for ' + target);
    await sleep(30);
    const base = { x: p.x, y: p.y, button, clickCount };
    await this.conn.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: p.x, y: p.y });
    await this.conn.send('Input.dispatchMouseEvent', { ...base, type: 'mousePressed', buttons: 1 });
    await sleep(hold);
    await this.conn.send('Input.dispatchMouseEvent', { ...base, type: 'mouseReleased', buttons: 0 });
    await sleep(after);
    return p;
  }

  /** Scroll the element under `selector` (or the page) by dy pixels with a mouse wheel event. */
  async wheel(selector, dy, { dx = 0 } = {}) {
    const p = selector ? await this.point(selector) : { x: 700, y: 450 };
    await this.conn.send('Input.dispatchMouseEvent', { type: 'mouseWheel', x: p.x, y: p.y, deltaX: dx, deltaY: dy });
    await sleep(100);
  }

  /** Press one key: a printable character ('j', '/', 'E') or a name in KEYS ('Enter', 'Home').
   *  modifiers: array of 'Shift' | 'Control' | 'Alt' | 'Meta'. */
  async key(key, { modifiers = [] } = {}) {
    const mod = modifiers.reduce((m, k) => m | MODS[k], 0);
    const named = KEYS[key];
    const ev = named
      ? { key, code: named.code, windowsVirtualKeyCode: named.keyCode, text: named.text }
      : { key, code: /^[a-z]$/i.test(key) ? 'Key' + key.toUpperCase() : undefined, text: key,
          windowsVirtualKeyCode: key.toUpperCase().charCodeAt(0) };
    await this.conn.send('Input.dispatchKeyEvent', { type: ev.text ? 'keyDown' : 'rawKeyDown', modifiers: mod, ...ev });
    await this.conn.send('Input.dispatchKeyEvent', { type: 'keyUp', modifiers: mod, key: ev.key, code: ev.code, windowsVirtualKeyCode: ev.windowsVirtualKeyCode });
    await sleep(60);
  }

  /** Types text into the focused element. */
  async type(text) { await this.conn.send('Input.insertText', { text }); await sleep(60); }

  async screenshot(file) {
    const r = await this.conn.send('Page.captureScreenshot', { format: 'png' });
    fs.writeFileSync(file, Buffer.from(r.data, 'base64'));
    return file;
  }

  async close() {
    try { await fetch(`http://127.0.0.1:${this.browser.port}/json/close/${this.target.id}`); } catch {}
    this.conn.close();
  }
}

export class Browser {
  constructor(proc, dir, port, opts) { this.proc = proc; this.dir = dir; this.port = port; this.opts = opts; this.pages = []; }

  /** Opens a new tab at `url` (waits for load). */
  async newPage(url = 'about:blank', { width = this.opts.width, height = this.opts.height } = {}) {
    const r = await fetch(`http://127.0.0.1:${this.port}/json/new?about:blank`, { method: 'PUT' });
    const target = await r.json();
    const page = await new Page(new Conn(target.webSocketDebuggerUrl), target, this).init({ width, height });
    this.pages.push(page);
    if (url !== 'about:blank') await page.goto(url);
    return page;
  }

  async close() {
    for (const p of this.pages) p.conn.close();
    if (this.proc.exitCode === null) {
      this.proc.kill('SIGTERM');
      await Promise.race([new Promise(r => this.proc.once('exit', r)), sleep(3000)]);
      if (this.proc.exitCode === null && this.proc.signalCode === null) this.proc.kill('SIGKILL');
    }
    fs.rmSync(this.dir, { recursive: true, force: true });
    live.delete(this);
  }
}

// Chromium puts its singleton socket under $TMPDIR and aborts if that path exceeds the
// 108-byte unix socket limit, so hand it a short temp dir.
function shortTmp() {
  const t = process.env.TMPDIR || os.tmpdir();
  return t.length < 60 ? t : '/tmp';
}

/** Launches headless Chromium with a fresh temp profile on a free debugging port. */
export async function launch({ chromium = process.env.CHROMIUM || '/usr/bin/chromium', width = 1400, height = 900,
  headless = true, args = [], timeout = 20000 } = {}) {
  const dir = fs.mkdtempSync(path.join(process.env.TEST_TMP || os.tmpdir(), 'cdp-profile-'));
  const proc = spawn(chromium, [
    ...(headless ? ['--headless=new'] : []), '--remote-debugging-port=0', `--user-data-dir=${dir}`,
    '--no-first-run', '--no-default-browser-check', '--disable-extensions', '--disable-background-networking',
    '--disable-component-update', '--disable-sync', '--mute-audio', `--window-size=${width},${height}`, ...args, 'about:blank',
  ], { stdio: ['ignore', 'ignore', 'pipe'], env: { ...process.env, TMPDIR: shortTmp() } });
  let stderr = '';
  proc.stderr.on('data', d => { stderr = (stderr + d).slice(-4000); });
  const b = new Browser(proc, dir, null, { width, height });
  live.add(b);
  const portFile = path.join(dir, 'DevToolsActivePort');
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (proc.exitCode !== null) break;
    try { const [port] = fs.readFileSync(portFile, 'utf8').split('\n'); if (port) { b.port = +port; break; } } catch {}
    await sleep(50);
  }
  if (!b.port) { await b.close(); throw new Error('chromium did not start: ' + stderr.slice(-1000)); }
  // Wait until the HTTP endpoint answers.
  while (Date.now() < end) { try { await fetch(`http://127.0.0.1:${b.port}/json/version`); break; } catch { await sleep(50); } }
  return b;
}
