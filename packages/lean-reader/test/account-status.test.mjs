import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';

// Account status (AI Factory step 2): with each account's allowance reading the
// reader sends whether the account is signed in, its banked resets (count,
// expiry, type and status) and its credit balance. Every identity below is
// synthetic; none of these strings may reach an upload, the outbox or a printout.
const bundle = new URL('../dist/plimsoll-lean.mjs', import.meta.url);
const reader = fs.existsSync(bundle) ? await import(bundle.href) : {};
const now = Date.parse('2026-10-30T20:00:00Z');
const key = id => 'sha256:' + crypto.createHash('sha256').update(JSON.stringify({ stringValue: id })).digest('hex').slice(0, 16);
const SECRETS = ['synthetic.claude@example.test', 'Synthetic.Claude@Example.test', 'synthetic-claude-uuid', 'synthetic-codex-provider-id',
  'synthetic-signed-out-provider-id', 'synthetic-org-id', 'Synthetic Org', 'reset-credit-id-0001', 'reset-credit-id-0002', 'reset-credit-id-0003',
  'Bonus week from support', 'Granted after an outage', 'credit-bucket-id-77', 'Synthetic description of a reset'];
const STATUS_FIELDS = ['tool', 'accountKey', 'observedAt', 'accountState', 'signedIn', 'authState', 'resets', 'credits'];

function fixture(t) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'lean-status-'));
  t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  return home;
}
function write(home, name, value) {
  const file = path.join(home, name);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, typeof value === 'string' ? value : JSON.stringify(value));
  return file;
}
const window = (slot, seconds, used, resetAt, observedAt) => ({ slot, windowSeconds: seconds, usedPercent: used, remainingPercent: 100 - used,
  resetAt, resetPrecision: resetAt ? 'second' : 'not_reported', observedAt, exhausted: used >= 100 });
const credit = (n, extra = {}) => ({ id: `reset-credit-id-000${n}`, title: n === 1 ? 'Bonus week from support' : 'Granted after an outage',
  description: 'Synthetic description of a reset', grantedAt: '2026-10-07T16:00:00.000Z', expiresAt: '2026-11-06T16:00:00.000Z',
  resetType: 'weekly_reset', status: 'available', ...extra });
function native(generatedAt = '2026-10-30T19:53:12.000Z', overrides = {}) {
  return { schemaVersion: 1, generatedAt, accounts: [
    { provider: 'claude', state: 'OK', identity: { email: 'synthetic.claude@example.test', organizationId: 'synthetic-org-id', organizationName: 'Synthetic Org',
      planType: 'max', authenticated: true, observedAt: generatedAt },
      source: { diagnostics: { authState: 'logged_in', authLoggedIn: true } },
      quotaBuckets: [{ limitId: 'seven_day', windows: [window('seven_day', 604800, 40, '2026-11-02T00:00:00.000Z', generatedAt)] }] },
    { provider: 'codex', state: 'OK', identity: { providerAccountId: 'synthetic-codex-provider-id', planType: 'promax', authenticated: true },
      source: { diagnostics: { authState: 'logged_in', authLoggedIn: true } },
      resetCredits: { availableCount: 2, observedAt: generatedAt, credits: [credit(1), credit(2, { expiresAt: '2026-11-20T16:00:00.000Z', status: 'available' })] },
      entitlements: { rateLimitResetCredits: { availableCount: 9, credits: [credit(3)] } },
      quotaBuckets: [{ limitId: 'codex', planType: 'promax', id: 'credit-bucket-id-77', credits: { balance: 1234.5, hasCredits: true, unlimited: false, id: 'credit-bucket-id-77' },
        windows: [window('primary', 604800, 63, '2026-11-04T13:26:11.000Z', generatedAt)] }], ...overrides.codex },
    // Signed out: the provider check says so although the identity still claims a session; no windows at all.
    { provider: 'codex', state: 'DEGRADED', identity: { providerAccountId: 'synthetic-signed-out-provider-id', planType: 'pro', authenticated: true },
      source: { diagnostics: { authState: 'needs_login', authLoggedIn: false } },
      entitlements: { rateLimitResetCredits: { availableCount: 1, credits: [credit(3, { status: 'expired', resetType: 'Not a token!' })] } },
      quotaBuckets: [] },
    // No key can be made for this one: it is never sent.
    { provider: 'codex', state: 'OK', identity: { planType: 'pro' }, source: { diagnostics: { authLoggedIn: true } }, quotaBuckets: [] },
  ] };
}
function home(t) {
  const dir = fixture(t);
  write(dir, '.claude-seats/synthetic/.claude.json', { oauthAccount: { accountUuid: 'synthetic-claude-uuid', emailAddress: 'Synthetic.Claude@Example.test' } });
  return dir;
}
async function server(t, refuse = () => false) {
  const bodies = [];
  const instance = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const raw = Buffer.concat(chunks).toString(), body = JSON.parse(raw);
    bodies.push({ raw, body });
    if (refuse(body)) { res.writeHead(400).end(); return; }
    res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ ok: true }));
  });
  await new Promise(resolve => instance.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => instance.close(resolve)));
  return { bodies, origin: `http://127.0.0.1:${instance.address().port}` };
}
function setup(dir, origin, snapshot = native()) {
  const allowanceFile = write(dir, 'native/native-account-usage.json', snapshot);
  const credentialsFrom = write(dir, 'credentials.json', { uploadUrl: origin, installKey: 'synthetic-key', uploadSigningSecret: 'synthetic-secret', tenantId: '00000000-0000-4000-8000-000000000000' });
  return { allowanceFile, args: { home: dir, env: {}, now, command: 'run', config: { credentialsFrom, allowanceFile } } };
}
const statusBodies = bodies => bodies.filter(b => b.body.allowanceAccounts);
const outboxOf = dir => path.join(dir, 'Library/Application Support/Plimsoll Lean/allowance.json');

test('account status goes in its own upload with counts, expiry, type, status, balance and sign-in state only', async t => {
  const dir = home(t), { bodies, origin } = await server(t);
  const { args } = setup(dir, origin);
  await reader.runPass(args);
  const sent = statusBodies(bodies);
  assert.equal(sent.length, 1);
  const body = sent[0].body;
  assert.deepEqual([body.hours, body.sessions, body.seats, body.allowance], [[], [], [], undefined], 'status rides alone, never with readings or usage');
  // The readings went first, in their own upload.
  assert.ok(bodies.findIndex(b => b.body.allowance) < bodies.findIndex(b => b.body.allowanceAccounts));
  for (const secret of [...SECRETS, dir]) for (const { raw } of bodies) assert.equal(raw.includes(secret), false, `an upload leaks ${secret}`);
  for (const secret of [...SECRETS, dir]) assert.equal(fs.readFileSync(outboxOf(dir), 'utf8').includes(secret), false, `the outbox keeps ${secret}`);
  const rows = Object.fromEntries(body.allowanceAccounts.map(row => [row.accountKey, row]));
  assert.deepEqual(Object.keys(rows).sort(), [key('synthetic-claude-uuid'), key('synthetic-codex-provider-id'), key('synthetic-signed-out-provider-id')].sort());
  for (const row of body.allowanceAccounts) {
    assert.deepEqual(Object.keys(row), STATUS_FIELDS);
    for (const item of row.resets?.credits ?? []) assert.deepEqual(Object.keys(item), ['expiresAt', 'resetType', 'status']);
    if (row.credits) assert.deepEqual(Object.keys(row.credits), ['balance', 'hasCredits', 'unlimited']);
  }
  assert.deepEqual(rows[key('synthetic-codex-provider-id')], { tool: 'codex', accountKey: key('synthetic-codex-provider-id'), observedAt: '2026-10-30T19:53:12.000Z',
    accountState: 'OK', signedIn: true, authState: 'logged_in',
    // resetCredits wins over the entitlement copy of the same list.
    resets: { available: 2, credits: [{ expiresAt: '2026-11-06T16:00:00.000Z', resetType: 'weekly_reset', status: 'available' },
      { expiresAt: '2026-11-20T16:00:00.000Z', resetType: 'weekly_reset', status: 'available' }] },
    credits: { balance: '1234.5', hasCredits: true, unlimited: false } });
  // The provider check's own answer wins over the identity's flag; an account with no windows still reports.
  assert.deepEqual(rows[key('synthetic-signed-out-provider-id')], { tool: 'codex', accountKey: key('synthetic-signed-out-provider-id'),
    observedAt: '2026-10-30T19:53:12.000Z', accountState: 'DEGRADED', signedIn: false, authState: 'needs_login',
    resets: { available: 1, credits: [{ expiresAt: '2026-11-06T16:00:00.000Z', resetType: null, status: 'expired' }] }, credits: null });
  assert.deepEqual(rows[key('synthetic-claude-uuid')], { tool: 'claude_code', accountKey: key('synthetic-claude-uuid'), observedAt: '2026-10-30T19:53:12.000Z',
    accountState: 'OK', signedIn: true, authState: 'logged_in', resets: null, credits: null });
  const outbox = reader.readOutbox(outboxOf(dir));
  assert.deepEqual([Object.keys(outbox.status.pending).length, Object.keys(outbox.status.seen).length, outbox.status.lastError], [0, 3, null]);
});

test('one status per account per hour: the same snapshot sends nothing, the next hour sends again', async t => {
  const dir = home(t), { bodies, origin } = await server(t);
  const { args } = setup(dir, origin);
  await reader.runPass(args);
  await reader.runPass({ ...args, now: now + 300_000 });
  assert.equal(statusBodies(bodies).length, 1);
  // A newer snapshot in the same hour replaces the queued row before it is sent; here it is already sent, so it goes once more.
  write(dir, 'native/native-account-usage.json', native('2026-10-30T20:02:00.000Z'));
  await reader.runPass({ ...args, now: now + 600_000 });
  assert.equal(statusBodies(bodies).length, 2);
  assert.deepEqual(statusBodies(bodies)[1].body.allowanceAccounts.map(row => row.observedAt), Array(3).fill('2026-10-30T20:02:00.000Z'));
  // A dry run queues the next hour but sends nothing.
  write(dir, 'native/native-account-usage.json', native('2026-10-30T21:01:00.000Z'));
  await reader.runPass({ ...args, now: now + 3_900_000, dryRun: true });
  assert.equal(statusBodies(bodies).length, 2);
  assert.equal(Object.keys(reader.readOutbox(outboxOf(dir)).status.pending).length, 3);
});

test('a cloud that refuses account status never holds back allowance readings; status keeps its own backoff', async t => {
  const dir = home(t), { bodies, origin } = await server(t, body => Boolean(body.allowanceAccounts));
  const { args } = setup(dir, origin);
  await reader.runPass(args);
  assert.equal(bodies.filter(b => b.body.allowance).length, 1, 'readings were sent');
  const outbox = reader.readOutbox(outboxOf(dir));
  assert.deepEqual([Object.keys(outbox.pending).length, outbox.lastError, outbox.failures], [0, null, 0], 'readings are not held back');
  assert.deepEqual([Object.keys(outbox.status.pending).length, outbox.status.lastError, outbox.status.failures, outbox.status.nextSend],
    [3, 'upload_http_400', 1, now + 60_000]);
  await reader.runPass({ ...args, now: now + 30_000 });
  assert.equal(statusBodies(bodies).length, 1, 'nothing is retried inside the backoff');
  await reader.runPass({ ...args, now: now + 61_000 });
  assert.equal(statusBodies(bodies).length, 2, 'after the backoff the status rows go again');
});

test('an outbox saved before account status loads with an empty status queue; status and allowance-keys print no identity', async t => {
  const dir = home(t), { origin } = await server(t);
  const { allowanceFile } = setup(dir, origin);
  const old = { version: 1, pending: {}, seen: { '["codex","sha256:0000000000000000","codex","primary"]': '2026-10-30T18:00:00.000Z' },
    lastSend: null, lastError: null, readError: null, failures: 0, nextSend: 0 };
  write(dir, 'Library/Application Support/Plimsoll Lean/allowance.json', old);
  const outbox = reader.readOutbox(outboxOf(dir));
  assert.deepEqual(outbox.status, { pending: {}, seen: {}, lastSend: null, lastError: null, failures: 0, nextSend: 0 });
  assert.equal(Object.keys(outbox.seen).length, 1);

  const config = write(dir, 'config.json', { allowanceFile });
  const keys = spawnSync(process.execPath, [new URL(bundle).pathname, 'allowance-keys', '--config', config, '--state-dir', path.join(dir, 'state')], {
    encoding: 'utf8', env: { ...process.env, HOME: dir, CLAUDE_CONFIG_DIR: '' } });
  assert.equal(keys.status, 0, keys.stderr);
  for (const secret of [...SECRETS, dir]) assert.equal(keys.stdout.includes(secret), false, `allowance-keys prints ${secret}`);
  assert.deepEqual(JSON.parse(keys.stdout).lines.map(line => [line.tool, line.signedIn, line.resets]),
    [['claude_code', true, null], ['codex', true, 2], ['codex', false, 1], ['codex', null, null]]);
  const status = spawnSync(process.execPath, [new URL(bundle).pathname, 'status', '--config', config, '--state-dir', path.join(dir, 'Library/Application Support/Plimsoll Lean')],
    { encoding: 'utf8', env: { ...process.env, HOME: dir } });
  assert.equal(status.status, 0, status.stderr);
  for (const secret of [...SECRETS, dir]) assert.equal(status.stdout.includes(secret), false, `status prints ${secret}`);
  assert.deepEqual(JSON.parse(status.stdout).allowance.accountStatus, { pendingRows: 0, accounts: 0, lastSend: null, lastError: null, nextSend: null });
});

test('bounded: at most twelve resets per account, a balance that is not a plain decimal is dropped, a bad snapshot time falls back', async t => {
  const dir = home(t), { bodies, origin } = await server(t);
  const many = Array.from({ length: 20 }, (_, n) => credit(1, { expiresAt: new Date(Date.UTC(2026, 10, 1 + n)).toISOString() }));
  const snapshot = native('not a time', { codex: { resetCredits: { availableCount: 20, credits: many },
    quotaBuckets: [{ limitId: 'codex', observedAt: '2026-10-30T19:40:00.000Z', credits: { balance: '1e400', hasCredits: 'yes', unlimited: null },
      windows: [window('primary', 604800, 10, '2026-11-04T13:26:11.000Z', '2026-10-30T19:41:00.000Z')] }] } });
  const { args } = setup(dir, origin, snapshot);
  await reader.runPass(args);
  const row = statusBodies(bodies)[0].body.allowanceAccounts.find(r => r.accountKey === key('synthetic-codex-provider-id'));
  assert.equal(row.resets.available, 20);
  assert.equal(row.resets.credits.length, 12);
  assert.equal(row.credits, null, 'no usable balance or flag: no credits object');
  assert.equal(row.observedAt, '2026-10-30T19:41:00.000Z', "the account's newest reading dates a snapshot without its own time");
});
