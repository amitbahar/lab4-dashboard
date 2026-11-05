// Two-pane UI behaviour tests (SPEC §13.8), on top of harness.mjs and the fixtures.
//   node --test tests/test_page_ui.mjs
//
// Unlike test_page_smoke.mjs these tests know the two-pane layout: #list rows (.item, data-id),
// the #detailScroll pane, section headers (.sechead) and the page's top-level S / V / F state.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { after, before, test } from 'node:test';
import { closeAll, openDashboard, problems, settle, showFixture, startFixtureServer, waitReady, TESTS } from './harness.mjs';
import { sleep } from './cdp.mjs';

let srv, page, manifest;
const events = name => fs.readFileSync(path.join(TESTS, 'fixtures', name + '.jsonl'), 'utf8').trim().split('\n').map(l => JSON.parse(l));
const row = id => `#row-${id}`;
const sec = name => `[...document.querySelectorAll('#detailBody .sechead')].find(s => s.textContent.includes(${JSON.stringify(name)}))`;
const selected = () => page.eval(`V.selected`);
const isOpenSec = name => page.eval(`${sec(name)}?.parentElement.classList.contains('open')`);
// A real click on an element given as a JS expression (page.click itself takes a CSS selector).
const clickExpr = async expr => page.click(await page.eval(`(() => { const el = ${expr}; el.scrollIntoView({ block: 'center' });
  const b = el.getBoundingClientRect(); return { x: b.x + Math.min(b.width / 2, 30), y: b.y + b.height / 2 }; })()`));
const offsetOf = id => page.eval(`(() => { const r = document.querySelector('${row(id)}'); return r.getBoundingClientRect().top - listScroll.getBoundingClientRect().top; })()`);

// Synthetic completed exchanges, used to make bursts of new requests.
let nextId = 1000;
async function burst(n) {
  for (let i = 0; i < n; i++) {
    const id = nextId++, t = Date.now();
    await srv.emit('exchange_start', { id, t, method: 'POST', path: '/v1/messages', headers: [['x-claude-code-session-id', 'burst-session']], request_bytes: 10,
      request: { model: 'claude-sonnet-4-5', max_tokens: 10, tools: [{ name: 'Bash', input_schema: { type: 'object' } }], messages: [{ role: 'user', content: 'burst ' + id }] } });
    await srv.emit('response_start', { id, t, status: 200, headers: [] });
    await srv.emit('response_end', { id, t: t + 5, status: 200, stop_reason: 'end_turn', usage: { input_tokens: 1, output_tokens: 1 },
      content: [{ type: 'text', text: 'reply ' + id }], raw: '', response_bytes: 1, aborted: false });
  }
  await page.waitFor(`S.ex.has(${nextId - 1})`, { message: 'burst arrived' });
  await settle(page);
}

before(async () => {
  srv = await startFixtureServer();
  manifest = await srv.manifest();
  ({ page } = await openDashboard(srv, { width: 1400, height: 560 }));
});
after(closeAll);

test('§13.8 #1: newest-first list and a detail pane with all eight sections', async () => {
  await showFixture(page, srv, 'basic');
  const ids = await page.eval(`[...document.querySelectorAll('#list .item')].map(r => +r.dataset.id)`);
  assert.deepEqual(ids, [...manifest.basic.live_ids].reverse(), 'rows are newest first');
  await page.click(row(2));
  assert.equal(await selected(), 2);
  assert.equal(await page.eval(`document.querySelector('${row(2)}').getAttribute('aria-selected')`), 'true');
  const titles = await page.eval(`[...document.querySelectorAll('#detailBody .sechead .t')].map(t => t.textContent.split(' (')[0])`);
  assert.deepEqual(titles, ['System instructions', 'Tool definitions', 'Conversation so far', 'New in this request', 'Reply', 'Changes since previous request', 'Stats', 'Raw']);
  assert.deepEqual(await problems(page), []);
});

test('§13.8 #2: a burst keeps selection, folds, focus and the list anchor (≤1px)', async () => {
  await showFixture(page, srv, 'concurrent_threads');
  await burst(14);                              // enough rows to scroll the list
  await page.click(row(1));                     // an older request
  await clickExpr(sec('Tool definitions'));
  await clickExpr(`document.querySelector('#detailBody .acc-tools .foldhead')`);
  await page.eval(`listScroll.scrollTop = 400; document.querySelector('#detailScroll').scrollTop = 150`);
  await settle(page);
  const anchor = await page.eval(`listAnchor()`);
  const detailTop = await page.eval(`document.querySelector('#detailScroll').scrollTop`);
  await burst(20);
  assert.equal(await selected(), 1, 'selection unchanged');
  assert.equal(await isOpenSec('Tool definitions'), true, 'section fold unchanged');
  assert.equal(await page.eval(`!!document.querySelector('#detailBody .acc-tools .foldbody')`), true, 'nested fold unchanged');
  assert.ok(Math.abs((await offsetOf(anchor.id)) - anchor.offset) <= 1, 'list anchor within 1px');
  assert.equal(await page.eval(`document.querySelector('#detailScroll').scrollTop`), detailTop, 'detail scroll unchanged');
  assert.match(await page.eval(`document.querySelector('#pill').hidden ? '' : document.querySelector('#pill').textContent`), /20 new requests/);
  await page.click('#pill');
  assert.equal(await page.eval(`listScroll.scrollTop`), 0, 'the pill scrolls the list to the newest rows');
  assert.equal(await selected(), 1, 'the pill does not change the selection');
});

test('§13.8 #3: switching requests restores scroll and folds; filters keep a hidden selection', async () => {
  await showFixture(page, srv, 'tool_pairing');
  await page.click(row(1));
  await page.eval(`setAllSections(S.ex.get(1), true); renderBody(S.ex.get(1), document.querySelector('#detailBody'))`);
  await page.eval(`document.querySelector('#detailScroll').scrollTop = 300`);
  await settle(page);
  const top1 = await page.eval(`document.querySelector('#detailScroll').scrollTop`);
  assert.ok(top1 > 0, 'detail is scrollable');
  await page.click(row(2));
  await clickExpr(sec('Reply'));               // close Reply on #2
  await page.click(row(1));
  assert.equal(await page.eval(`document.querySelector('#detailScroll').scrollTop`), top1, '#1 detail scroll restored');
  assert.equal(await isOpenSec('Raw'), true, '#1 folds restored');
  await page.click(row(2));
  assert.equal(await isOpenSec('Reply'), false, '#2 folds restored');
  // filter out the selection: details stay, with a notice and a reveal action
  await page.eval(`F.search = 'zzz-matches-nothing'; applyFilters()`);
  await settle(page);
  assert.equal(await page.eval(`document.querySelector('${row(2)}').hidden`), true);
  assert.equal(await selected(), 2, 'filters never replace the selection');
  assert.match(await page.eval(`document.querySelector('#detailNotice').textContent`), /hidden by the current filters/);
  await clickExpr(`document.querySelector('#detailNotice button')`);
  assert.equal(await page.eval(`document.querySelector('${row(2)}').hidden`), false, 'reveal shows the row');
  await page.eval(`F.search = ''; F.force.clear(); applyFilters()`);
});

test('§13.8 #4: live events before response_end, tail-follow, provisional then final', async () => {
  const ev = events('basic');
  const start = ev.findIndex(e => e.event === 'exchange_start' && e.data.id === 2);
  const firstDelta = ev.findIndex((e, i) => i > start && e.event === 'block_delta' && e.data.id === 2 && e.data.text != null);
  await showFixture(page, srv, 'basic', { mode: 'live', pause_at: firstDelta + 1, wait: false });
  await page.waitFor(`S.ex.has(2) && S.replaying === false`);
  await settle(page);
  await page.click(row(2));
  await clickExpr(sec('Raw'));
  const names = () => page.eval(`[...document.querySelectorAll('#detailBody .logbox .ev')].map(e => e.textContent)`);
  assert.ok((await names()).includes('block_delta'), 'deltas are visible before the end');
  assert.ok(!(await names()).includes('response_end'));
  assert.equal(await page.eval(`!!document.querySelector('#detailBody .acc-reply .prov')`), true, 'streaming text is labelled provisional');
  // scroll the log up: tail-follow stops and a "new events" button appears
  await page.eval(`(() => { const b = document.querySelector('#detailBody .logbox'); b.style.maxHeight = '60px'; b.scrollTop = 0; })()`);
  await srv.control('step', { n: 4 }); await sleep(150); await settle(page);
  assert.equal(await page.eval(`document.querySelector('#detailBody .logbox').scrollTop`), 0, 'no tail-follow while scrolled up');
  assert.equal(await page.eval(`document.querySelector('#detailBody .lognew').hidden`), false, 'new-events indicator shown');
  // back to the bottom: following resumes
  await page.eval(`(() => { const b = document.querySelector('#detailBody .logbox'); b.scrollTop = b.scrollHeight; b.dispatchEvent(new Event('scroll')); })()`);
  await srv.control('step', { n: 3 }); await sleep(150); await settle(page);
  assert.equal(await page.eval(`(() => { const b = document.querySelector('#detailBody .logbox'); return b.scrollTop + b.clientHeight >= b.scrollHeight - 4; })()`), true, 'tail-follow resumed');
  await srv.control('resume');
  await page.waitFor(`S.ex.get(2)?.done`);
  await settle(page);
  assert.equal(await page.eval(`!!document.querySelector('#detailBody .acc-reply .prov')`), false, 'final text is no longer provisional');
  assert.ok(await page.eval(`!!document.querySelector('#detailBody .acc-reply .md, #detailBody .acc-reply .pair')`), 'final rendering present');
  assert.deepEqual(await problems(page), []);
});

test('§13.8 #5: lifecycle labels; a dashboard disconnect is not an interruption', async () => {
  await showFixture(page, srv, 'failures');
  const want = manifest.failures.states;
  const got = await page.eval(`Object.fromEntries([...S.ex.values()].map(e => [e.id, lifecycle(e).s]))`);
  assert.deepEqual(got, want);
  const label = await page.eval(`[...document.querySelectorAll('#list .item')].map(r => r.dataset.id + ':' + r.querySelector('.head .chip.life')?.textContent)`);
  for (const l of label) {
    const [id, txt] = l.split(':');
    const word = { failed: 'Failed', interrupted: 'Interrupted', completed: 'Completed', sending: 'Sending', streaming: 'Streaming' }[want[id]];
    assert.ok(txt?.includes(word), `#${id} shows ${word}, got ${txt}`);
  }
  await srv.control('drop');
  await page.waitFor(`document.querySelector('#conn').textContent !== 'live'`, { timeout: 3000 }).catch(() => {});
  await page.waitFor(`document.querySelector('#conn').textContent === 'live' && S.replaying === false`, { timeout: 8000 });
  await settle(page);
  assert.deepEqual(await page.eval(`Object.fromEntries([...S.ex.values()].map(e => [e.id, lifecycle(e).s]))`), want, 'states unchanged after reconnect');
});

test('§13.8 #6: tool results pair by call ID only, within a session', async () => {
  for (const name of ['tool_pairing', 'concurrent_threads', 'multi_session', 'eviction']) {
    await showFixture(page, srv, name);
    const live = new Set(manifest[name].live_ids);
    for (const [cid, c] of Object.entries(manifest[name].calls)) {
      if (!live.has(c.call_in)) continue;
      const t = await page.eval(`(() => { const ex = S.ex.get(${c.call_in}); const t = toolOf(ex, ${JSON.stringify(cid)}); return t && { resultEx: t.resultEx ?? null, isError: !!t.isError, missing: !!t.missing, collision: !!t.collision }; })()`);
      assert.ok(t, `${name}: call ${cid} known`);
      assert.equal(t.collision, false, `${name}: ${cid} not flagged as a collision`);
      assert.equal(t.resultEx, c.result_in, `${name}: ${cid} result in #${c.result_in}`);
      if (c.result_in != null) assert.equal(t.isError, c.is_error, `${name}: ${cid} error flag`);
      if (c.note?.startsWith('no result received')) assert.equal(t.missing, true, `${name}: ${cid} marked as no result received`);
    }
  }
  // multi_session: the same call ID in two sessions is two calls, each paired in its own session
  await showFixture(page, srv, 'multi_session');
  const pairs = await page.eval(`[...S.tools.values()].map(t => [t.callEx, t.resultEx])`);
  for (const [c, r] of pairs) if (r != null) assert.equal(await page.eval(`S.ex.get(${c}).session === S.ex.get(${r}).session`), true, `#${c}→#${r} stays in one session`);
  // tool_pairing: the orphan result is labelled as such
  await showFixture(page, srv, 'tool_pairing');
  const orphan = manifest.tool_pairing.orphan_results[0];
  await page.click(row(orphan.in));
  assert.match(await page.eval(`document.querySelector('#detailBody .acc-new').textContent`), /orphan result/);
  // eviction: a result whose call was evicted says so, not "orphan"
  await showFixture(page, srv, 'eviction');
  await page.click(row(4));
  const newText = await page.eval(`document.querySelector('#detailBody .acc-new').textContent`);
  assert.match(newText, /evicted/); assert.doesNotMatch(newText, /orphan/);
});

test('§13.8 #7: reconnect keeps selection and folds without duplicates; an evicted selection shows a placeholder', async () => {
  await showFixture(page, srv, 'concurrent_threads');
  await page.click(row(3));
  await clickExpr(sec('Tool definitions'));
  await srv.control('drop');
  await page.waitFor(`document.querySelector('#conn').textContent === 'live' && S.replaying === false`, { timeout: 8000 });
  await sleep(300); await settle(page);
  assert.equal(await selected(), 3);
  assert.equal(await isOpenSec('Tool definitions'), true);
  const rows = await page.eval(`[...document.querySelectorAll('#list .item')].map(r => r.dataset.id)`);
  assert.equal(new Set(rows).size, rows.length, 'no duplicate rows');
  assert.equal(rows.length, manifest.concurrent_threads.live_ids.length);
  // eviction of the selected request, live
  const ev = events('eviction');
  const evictAt = ev.findIndex(e => e.event === 'evicted');
  await showFixture(page, srv, 'eviction', { mode: 'live', pause_at: evictAt, wait: false });
  await page.waitFor(`S.ex.size === 5 && S.replaying === false`);
  await settle(page);
  await page.click(row(2));
  await srv.control('resume');
  await page.waitFor(`!S.ex.has(2)`);
  await settle(page);
  assert.equal(await selected(), 2, 'selection is not silently replaced');
  assert.match(await page.eval(`document.querySelector('#detailBody').textContent`), /no longer available/);
  assert.match(await page.eval(`document.querySelector('#evictNote').textContent`), /3 older requests evicted/);
});

test('§13.8 #8: hostile payloads stay escaped; long values are expandable and copyable', async () => {
  await showFixture(page, srv, 'hostile');
  let more = 0, copy = 0;
  for (const id of manifest.hostile.live_ids) {
    if (await page.eval(`document.querySelector('${row(id)}').hidden`)) await page.eval(`selectEx(${id}, { reveal: false, show: false })`);  // side call / non-message: hidden by default
    else await page.click(row(id));
    await page.eval(`setAllSections(S.ex.get(${id}), true); renderBody(S.ex.get(${id}), document.querySelector('#detailBody'))`);
    await settle(page);
    more += await page.eval(`document.querySelectorAll('#detailBody .more').length`);
    copy += await page.eval(`document.querySelectorAll('#detailBody .copy').length`);
  }
  assert.deepEqual(await problems(page), []);
  assert.ok(more > 0, 'long text has show-all controls');
  assert.ok(copy > 0, 'copy controls present');
});

test('keyboard: j/k move the selection, Enter focuses details, Home scrolls the list only', async () => {
  await showFixture(page, srv, 'basic');
  const vis = await page.eval(`visibleRows().map(r => +r.dataset.id)`);
  await page.click(row(vis[0]));
  await page.eval(`document.activeElement.blur()`);
  await page.key('j'); await page.key('j'); await settle(page);
  assert.equal(await selected(), vis[2], 'two rows down: ' + vis);
  await page.key('k'); await settle(page);
  assert.equal(await selected(), vis[1]);
  await page.key('Enter'); await settle(page);
  assert.equal(await page.eval(`document.activeElement.id`), 'detailScroll');
  await page.eval(`listScroll.scrollTop = 200`);
  await page.key('Home'); await settle(page);
  assert.equal(await page.eval(`listScroll.scrollTop`), 0);
  assert.equal(await selected(), vis[1]);
});
