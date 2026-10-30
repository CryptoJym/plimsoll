import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';

const bundle = new URL('../dist/plimsoll-lean.mjs', import.meta.url);
const reader = fs.existsSync(bundle) ? await import(bundle.href) : {};
const now = Date.parse('2026-10-30T20:00:00Z');
const key = id => 'sha256:' + crypto.createHash('sha256').update(JSON.stringify({ stringValue: id })).digest('hex').slice(0, 16);
// Synthetic identities. None of these strings may reach an upload or a printout.
const SECRETS = ['Synthetic.Claude@Example.test', 'synthetic.claude@example.test', 'synthetic-claude-uuid', 'synthetic-codex-provider-id',
  'synthetic-org-id', 'Synthetic Org', 'unmatched@example.test'];

function fixture(t) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'lean-allowance-'));
  t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  return home;
}
function write(home, name, value) {
  const file = path.join(home, name);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, typeof value === 'string' ? value : JSON.stringify(value));
  return file;
}
const window = (slot, seconds, used, resetAt, observedAt, extra = {}) => ({ slot, windowSeconds: seconds, usedPercent: used, remainingPercent: 100 - used,
  resetAt, resetPrecision: resetAt ? 'hour' : 'not_reported', observedAt, exhausted: used >= 100, ...extra });
function native(observedAt = '2026-10-30T19:53:12.000Z', used = 11) {
  return { schemaVersion: 1, generatedAt: observedAt, accounts: [
    { provider: 'claude', state: 'OK', accountKey: 'native-tag-1',
      identity: { email: 'synthetic.claude@example.test', organizationId: 'synthetic-org-id', organizationName: 'Synthetic Org', planType: 'max', observedAt },
      quotaBuckets: [
        { limitId: 'five_hour', planType: null, modelScope: null, observedAt, exhausted: false, windows: [window('five_hour', 18000, 0, null, observedAt)] },
        { limitId: 'seven_day', planType: null, modelScope: null, observedAt, exhausted: false, windows: [window('seven_day', 604800, used, '2026-11-02T00:00:00.000Z', observedAt)] },
        { limitId: 'weekly_scoped:Fable', planType: null, modelScope: 'Fable', observedAt, exhausted: false, windows: [window('weekly_scoped:Fable', 604800, 0, '2026-11-02T00:00:00.000Z', observedAt)] },
      ] },
    { provider: 'claude', state: 'DEGRADED', identity: { email: 'unmatched@example.test', planType: 'max' },
      quotaBuckets: [{ limitId: 'seven_day', windows: [window('seven_day', 604800, 50, '2026-11-01T00:00:00.000Z', '2026-10-20T03:35:44.093Z')] }] },
    { provider: 'codex', state: 'DEGRADED', identity: { providerAccountId: 'synthetic-codex-provider-id', planType: 'pro' },
      quotaBuckets: [{ limitId: 'codex', planType: 'promax', modelScope: null, observedAt: '2026-10-02T13:05:27.167Z', exhausted: true,
        windows: [window('primary', 604800, 100, '2026-10-06T08:00:00.000Z', '2026-10-02T13:05:27.167Z', { resetPrecision: 'second' })] }] },
    { provider: 'codex', state: 'DEGRADED', identity: { planType: 'pro' }, quotaBuckets: [{ limitId: 'codex', windows: [window('primary', 604800, 3, null, observedAt)] }] },
    { provider: 'grok', state: 'OK', identity: {}, quotaBuckets: [{ limitId: 'grok', windows: [window('primary', 604800, 1, null, observedAt)] }] },
  ] };
}
function home(t) {
  const dir = fixture(t);
  write(dir, '.claude-seats/synthetic/.claude.json', { oauthAccount: { accountUuid: 'synthetic-claude-uuid', emailAddress: 'Synthetic.Claude@Example.test' } });
  write(dir, '.claude-accounts/broken/.claude.json', '{"oauthAccount":');
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
function setup(t, dir, origin, extra = {}) {
  const allowanceFile = write(dir, 'native/native-account-usage.json', native());
  const credentialsFrom = write(dir, 'credentials.json', { uploadUrl: origin, installKey: 'synthetic-key', uploadSigningSecret: 'synthetic-secret', tenantId: '00000000-0000-4000-8000-000000000000' });
  return { allowanceFile, args: { home: dir, env: {}, now, command: 'run', config: { credentialsFrom, allowanceFile, ...extra } } };
}

test('allowance rows carry hashed keys only, in their own uploads, one per window per hour', async t => {
  const dir = home(t), { bodies, origin } = await server(t);
  const { args } = setup(t, dir, origin);
  await reader.runPass(args);
  const sent = bodies.filter(b => b.body.allowance);
  assert.equal(sent.length, 1);
  const body = sent[0].body;
  assert.deepEqual([body.hours, body.sessions, body.seats], [[], [], []]);
  for (const secret of [...SECRETS, dir]) assert.equal(sent[0].raw.includes(secret), false, 'upload leaks a synthetic identity or path');
  const rows = body.allowance.map(r => [r.tool, r.accountKey, r.bucket, r.slot, r.plan, r.modelScope, r.usedPercent, r.resetsAt, r.exhausted, r.accountState]);
  assert.deepEqual(rows.sort(), [
    ['claude_code', key('synthetic-claude-uuid'), 'five_hour', 'five_hour', 'max', null, 0, null, false, 'OK'],
    ['claude_code', key('synthetic-claude-uuid'), 'seven_day', 'seven_day', 'max', null, 11, '2026-11-02T00:00:00.000Z', false, 'OK'],
    ['claude_code', key('synthetic-claude-uuid'), 'weekly_scoped:Fable', 'weekly_scoped:Fable', 'max', 'Fable', 0, '2026-11-02T00:00:00.000Z', false, 'OK'],
    ['codex', key('synthetic-codex-provider-id'), 'codex', 'primary', 'promax', null, 100, '2026-10-06T08:00:00.000Z', true, 'DEGRADED'],
  ].sort());
  assert.ok(body.allowance.every(r => Object.keys(r).length === 14 && /^sha256:[0-9a-f]{16}$/.test(r.accountKey)));
  const outbox = reader.readOutbox(path.join(dir, 'Library/Application Support/Plimsoll Lean/allowance.json'));
  assert.deepEqual([Object.keys(outbox.pending).length, Object.keys(outbox.seen).length, outbox.lastError, outbox.readError], [0, 4, null, null]);

  // The same snapshot again sends nothing; a newer one sends only what moved.
  await reader.runPass({ ...args, now: now + 300_000 });
  assert.equal(bodies.filter(b => b.body.allowance).length, 1);
  write(dir, 'native/native-account-usage.json', native('2026-10-30T20:01:12.000Z', 12));
  await reader.runPass({ ...args, now: now + 600_000 });
  const next = bodies.filter(b => b.body.allowance);
  assert.equal(next.length, 2);
  assert.deepEqual(next[1].body.allowance.map(r => [r.bucket, r.usedPercent, r.observedAt]).sort(), [
    ['five_hour', 0, '2026-10-30T20:01:12.000Z'], ['seven_day', 12, '2026-10-30T20:01:12.000Z'], ['weekly_scoped:Fable', 0, '2026-10-30T20:01:12.000Z']]);
});

test('a cloud that refuses allowance never holds back usage, and the outbox keeps its rows to retry', async t => {
  const dir = home(t), { bodies, origin } = await server(t, body => Boolean(body.allowance));
  const { args } = setup(t, dir, origin);
  fs.mkdirSync(path.join(dir, '.claude/projects/x'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.claude/projects/x/a.jsonl'), JSON.stringify({ type: 'assistant', timestamp: '2026-10-30T19:02:03Z', sessionId: 's',
    message: { id: 'one', model: 'synthetic-model', usage: { input_tokens: 10, output_tokens: 1 } } }) + '\n');
  const result = await reader.runPass(args);
  assert.equal(result.state.lastError, null);
  assert.equal(result.stats.sent, 1);
  assert.ok(bodies.some(b => !b.body.allowance && b.body.hours.length === 1));
  const outbox = reader.readOutbox(path.join(dir, 'Library/Application Support/Plimsoll Lean/allowance.json'));
  assert.deepEqual([Object.keys(outbox.pending).length, outbox.lastError, outbox.failures, outbox.nextSend], [4, 'upload_http_400', 1, now + 60_000]);
  // Inside the backoff nothing is retried; after it the rows go again.
  await reader.runPass({ ...args, now: now + 30_000 });
  assert.equal(bodies.filter(b => b.body.allowance).length, 1);
  await reader.runPass({ ...args, now: now + 61_000 });
  assert.equal(bodies.filter(b => b.body.allowance).length, 2);
});

test('the allowance step runs when the usage pass fails, never without the setting, and not in a dry run send', async t => {
  const dir = home(t), { bodies, origin } = await server(t);
  const { args } = setup(t, dir, origin);
  const stateDir = path.join(dir, 'Library/Application Support/Plimsoll Lean');
  write(dir, 'Library/Application Support/Plimsoll Lean/state.json', '{"version":9}');
  await assert.rejects(reader.runPass(args), /invalid_state/);
  assert.equal(bodies.filter(b => b.body.allowance).length, 1, 'allowance went before the usage pass failed');
  fs.rmSync(path.join(stateDir, 'state.json'));

  const other = home(t), quiet = await server(t);
  const plain = setup(t, other, quiet.origin);
  await reader.runPass({ ...plain.args, config: { credentialsFrom: plain.args.config.credentialsFrom } });
  assert.equal(fs.existsSync(path.join(other, 'Library/Application Support/Plimsoll Lean/allowance.json')), false);
  assert.equal(quiet.bodies.length, 0);
  await reader.runPass({ ...plain.args, dryRun: true });
  assert.equal(quiet.bodies.length, 0);
  assert.equal(Object.keys(reader.readOutbox(path.join(other, 'Library/Application Support/Plimsoll Lean/allowance.json')).pending).length, 4);
  await reader.runPass({ ...plain.args, command: 'backfill', since: '2026-10-29T00:00:00Z', until: '2026-10-30T00:00:00Z' });
  assert.equal(quiet.bodies.filter(b => b.body.allowance).length, 0, 'a backfill leaves allowance to the next run');
});

test('an unreadable snapshot is recorded without stopping the run; allowance-keys prints keys and counts only', async t => {
  const dir = home(t), { origin } = await server(t);
  const { args, allowanceFile } = setup(t, dir, origin);
  fs.writeFileSync(allowanceFile, '{"accounts": [');
  await reader.runPass(args);
  const outbox = reader.readOutbox(path.join(dir, 'Library/Application Support/Plimsoll Lean/allowance.json'));
  assert.equal(outbox.readError, 'allowance_unreadable');

  fs.writeFileSync(allowanceFile, JSON.stringify(native()));
  const config = write(dir, 'config.json', { allowanceFile });
  const run = spawnSync(process.execPath, [new URL(bundle).pathname, 'allowance-keys', '--config', config, '--state-dir', path.join(dir, 'state')], {
    encoding: 'utf8', env: { ...process.env, HOME: dir, CLAUDE_CONFIG_DIR: '' } });
  assert.equal(run.status, 0, run.stderr);
  const out = JSON.parse(run.stdout);
  for (const secret of [...SECRETS, dir]) assert.equal(run.stdout.includes(secret), false, 'printout leaks a synthetic identity or path');
  assert.deepEqual([out.allowanceFile, out.accounts, out.keyed, out.withSeat, out.withHours], ['read', 4, 2, 0, 0]);
  assert.deepEqual(out.lines.map(l => [l.tool, l.accountKey, l.windows]), [
    ['claude_code', key('synthetic-claude-uuid'), 3], ['claude_code', null, 0], ['codex', key('synthetic-codex-provider-id'), 1], ['codex', null, 0]]);
  const status = spawnSync(process.execPath, [new URL(bundle).pathname, 'status', '--config', config, '--state-dir', path.join(dir, 'Library/Application Support/Plimsoll Lean')], { encoding: 'utf8', env: { ...process.env, HOME: dir } });
  assert.equal(JSON.parse(status.stdout).allowance.readError, 'allowance_unreadable');
});
