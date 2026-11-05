// Browser smoke test: dashboard.html against tests/fixture_server.py in headless Chromium.
//   node --test tests/test_page_smoke.mjs        (or just: node tests/test_page_smoke.mjs)
//
// Deliberately layout-agnostic (the card feed is being replaced by a two-pane list/detail
// layout): it checks that every fixture loads without exceptions, "render error" text or
// executed/injected untrusted markup, that the page holds the right exchanges (via the
// top-level `S` state), survives live streaming and SSE reconnects, and talks to the
// server over SSE only. UI behaviour tests belong in their own files, built on harness.mjs.

import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import {
  clickEachExchange, closeAll, eventStreamCount, exchangeIds, foreignRequests, openDashboard, problems, settle,
  showFixture, startFixtureServer, waitReady, waitReconnect,
} from './harness.mjs';

let srv, page, manifest;

before(async () => {
  srv = await startFixtureServer();
  manifest = await srv.manifest();
  ({ page } = await openDashboard(srv));
});
after(closeAll);

describe('every fixture renders cleanly (replay)', () => {
  // Names are read synchronously so each fixture is its own test.
  for (const name of ['basic', 'concurrent_threads', 'tool_pairing', 'failures', 'eviction', 'multi_session', 'hostile']) {
    test(name, async () => {
      const man = await showFixture(page, srv, name);
      assert.ok(man, `fixture ${name} is in the manifest`);
      assert.deepEqual(await exchangeIds(page), man.live_ids, 'page holds exactly the non-evicted exchanges');
      const clicked = await clickEachExchange(page, man.live_ids);
      await settle(page);
      assert.deepEqual(await problems(page), [], `problems after loading ${name} and clicking ${clicked.length} exchange(s)`);
      assert.deepEqual(foreignRequests(page, srv), [], 'only the page and /events were fetched');
    });
  }
});

test('live streaming with pacing reaches the same final state', async () => {
  const man = manifest.failures;
  await showFixture(page, srv, 'failures', { mode: 'live', delay: 5 });
  await srv.control('wait', { published: man.events });
  await waitReady(page, { count: man.live_ids.length });

  assert.deepEqual(await problems(page), []);
});

test('paused mid-stream, then resumed', async () => {
  const man = manifest.concurrent_threads;
  await showFixture(page, srv, 'concurrent_threads', { pause_at: 40, wait: false });
  await page.waitFor(`typeof S !== 'undefined' && S.replaying === false && S.ex.size > 0`);
  const partial = await exchangeIds(page);
  assert.ok(partial.length < man.live_ids.length, `paused page shows a prefix (${partial})`);
  assert.deepEqual(await problems(page), []);
  await srv.control('resume');
  await page.waitFor(`S.ex.size === ${man.live_ids.length}`);
  await settle(page);
  assert.deepEqual(await problems(page), []);
});

test('SSE drop mid-stream: page reconnects, re-syncs, no duplicates', async () => {
  const man = manifest.tool_pairing;
  await showFixture(page, srv, 'tool_pairing', { mode: 'live', delay: 3, drop_after: 50 });
  await srv.control('wait', { published: 50 });
  await waitReconnect(page, srv, 1);
  await srv.control('wait', { published: man.events });
  await page.waitFor(`S.ex.size === ${man.live_ids.length}`);
  await settle(page);
  assert.deepEqual(await exchangeIds(page), man.live_ids);
  assert.equal(eventStreamCount(page), 2, 'exactly one reconnect');
  assert.deepEqual(await problems(page), []);
});

test('explicit drop after completion keeps state consistent', async () => {
  const man = manifest.basic;
  await showFixture(page, srv, 'basic');
  const n = eventStreamCount(page);
  await srv.control('drop');
  await waitReconnect(page, srv, n, { count: man.live_ids.length });
  assert.deepEqual(await exchangeIds(page), man.live_ids);
  assert.deepEqual(await problems(page), []);
});

// Regression: after a reset with a new boot id the page used to keep applying that replay to its
// stale state (a re-sent exchange_start became its own parent and analyze() threw).
test('new boot id (proxy restart) makes the page reload and replay', async () => {
  const man = manifest.multi_session;
  await showFixture(page, srv, 'multi_session');
  await page.eval(`window.__beforeRestart = 1`);
  await srv.control('load', { fixture: 'multi_session', new_boot: 1 });
  await page.waitFor(`window.__beforeRestart === undefined`, { message: 'page reloaded' });
  await waitReady(page, { count: man.live_ids.length });
  assert.deepEqual(await problems(page), []);
});

test('problems() is not vacuous: it catches injected markup', async () => {
  await showFixture(page, srv, 'basic');
  await page.eval(`document.body.insertAdjacentHTML('beforeend', '<div id="__probe"><img src="x" onerror="window.__pwned=1"></div>')`);
  await page.waitFor(`window.__pwned === 1`);
  const p = await problems(page);
  assert.ok(p.some(x => x.includes('__pwned')) && p.some(x => x.includes('onerror')), JSON.stringify(p));
  await page.eval(`document.getElementById('__probe').remove(); delete window.__pwned`);
  page.clearLogs();
});
