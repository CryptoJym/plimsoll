import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { scan, accountKey, makeBatches, sendPending, runPass } from '../dist/plimsoll-lean.mjs';

const since = '2026-11-08T00:00:00Z', until = '2026-11-10T00:00:00Z', now = Date.parse(until);
const identity = { accountUuid: 'fixture-uuid', emailAddress: 'Fixture@Example.test' }, key = accountKey(identity.accountUuid);
function setup(t) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'lean-sdk-')); fs.chmodSync(home, 0o700);
  t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  const write = (name, value) => {
    const file = path.join(home, name); fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, typeof value === 'string' ? value : JSON.stringify(value)); return file;
  };
  write('.claude-seats/known/.claude.json', { oauthAccount: identity });
  return { home, write, options: { home, env: {}, since, until, now, backfill: true } };
}
const reply = (id, sessionId = 'sdk-session', timestamp = '2026-11-09T01:02:03Z') => ({ type: 'assistant', timestamp, sessionId,
  message: { id, model: 'fixture-model', usage: { input_tokens: 10, output_tokens: 20 } } });
const line = value => JSON.stringify(value) + '\n';
const total = rows => rows.reduce((n, r) => n + r.requests, 0);

test('missing seat identity uses learned/shared keys; local labels never enter the upload', t => {
  const { home, write, options } = setup(t);
  write('.claude-seats/fixture-example-test/.claude.json', {});
  write('.claude-seats/fixture-example-test/projects/x/a.jsonl', line(reply('one', 'seat-session')));
  let result = scan(options);
  assert.equal(result.rows[0].accountKey, key);
  const shared = '.local/state/plimsoll-lean/account-keys.json';
  const labels = 'Library/Application Support/Plimsoll Lean/account-labels.json';
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(home, labels))), { [key]: 'fixture@example.test' });
  fs.unlinkSync(path.join(home, '.claude-seats/known/.claude.json'));
  for (const content of [{ 'fixture@example.test': key }, line({ email: 'fixture@example.test', accountKey: key }), line({ email: 'fixture@example.test', accountKey: key }) + line({ email: 'invalid', accountKey: 'raw-id' })]) {
    write(shared, content); result = scan({ ...options, state: undefined }); assert.equal(result.rows[0].accountKey, key);
  }
  const body = JSON.stringify(makeBatches(result.state, 'fixture-tenant', now));
  assert.ok(!body.includes('fixture@example.test') && !body.includes('fixture-uuid'));
  assert.equal(fs.statSync(path.join(home, shared)).mode & 0o777, 0o600);
  write('.claude-seats/conflict/.claude.json', { oauthAccount: { ...identity, accountUuid: 'different' } });
  assert.equal(scan(options).rows[0].accountKey, 'unknown');
});

test('glob SDK roots join launches by session and retain day/session totals across rereads and midnight', async t => {
  const { home, write, options } = setup(t);
  const launch = '.local/state/fleet-launch/launches-sdk.jsonl';
  write(launch, line({ kind: 'sdk', session: 'sdk-session', account: 'fixture-example-test', project: 'colony-brain', at: since }));
  const a = write('temp/hindsight-claude-code-1/projects/x/a.jsonl', line(reply('one', 'sdk-session', '2026-11-08T23:59:59Z')));
  const b = write('temp/hindsight-claude-code-2/projects/x/b.jsonl', line(reply('one', 'sdk-session', '2026-11-08T23:59:59Z')) + line(reply('two')));
  const config = { rootsOnly: true, roots: [{ tool: 'claude_code', path: home + '/temp/hindsight-claude-code-*/projects' }] };
  let result = await runPass({ ...options, config, command: 'backfill', dryRun: true });
  assert.equal(total(result.rows), 2); assert.ok(result.rows.every(r => r.accountKey === key && r.projectHint === 'colony-brain'));
  assert.deepEqual(Object.keys(result.state.files), []);
  const batches = makeBatches(result.state, 'fixture-tenant', now), summary = batches.flatMap(b => b.body.sessions)[0];
  assert.deepEqual([summary.firstSeen, summary.lastSeen], ['2026-11-08T23:59:59.000Z', '2026-11-09T01:02:03.000Z']);
  const partial = line(reply('three')); fs.appendFileSync(b, partial.slice(0, -8));
  result = scan({ ...options, config, backfill: false, state: result.state }); assert.equal(total(result.rows), 2);
  fs.appendFileSync(b, partial.slice(-8)); fs.unlinkSync(a);
  result = scan({ ...options, config, backfill: false, state: result.state }); assert.equal(total(result.rows), 3);
  result = scan({ ...options, config, state: result.state }); assert.equal(total(result.rows), 3);
  fs.unlinkSync(path.join(home, launch)); fs.appendFileSync(b, line(reply('four')));
  result = scan({ ...options, config, backfill: false, state: result.state });
  assert.equal(total(result.rows), 4); assert.ok(result.rows.every(r => r.accountKey === key && r.projectHint === 'colony-brain'));
  const server = http.createServer((req, res) => { req.resume(); res.end('{"ok":true}'); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve)); t.after(() => new Promise(resolve => server.close(resolve)));
  const credentialsFrom = write('synthetic-upload.json', { uploadUrl: `http://127.0.0.1:${server.address().port}`, tenantId: 'fixture', installKey: 'fixture', uploadSigningSecret: 'fixture' });
  await sendPending(result.state, { credentialsFrom }, { home, now });
  assert.equal(makeBatches(result.state, 'fixture', now).length, 0);
});

test('a sidecar can name a fixed-root session; learning a glob account replaces its unknown key', t => {
  const { home, write, options } = setup(t);
  const fixed = write('.claude/projects/x/a.jsonl', line(reply('fixed', 'fixed-session')));
  const temp = write('temp/root/projects/a.jsonl', line(reply('temp')));
  const config = { roots: [{ tool: 'claude_code', path: home + '/temp/*/projects' }] };
  let result = scan({ ...options, config }); assert.ok(result.rows.every(r => r.accountKey === 'unknown'));
  write('.local/state/fleet-launch/launches-sdk.jsonl', ['sdk-session', 'fixed-session'].map(session => line({ kind: 'sdk', session, account: 'fixture@example.test', project: 'colony-brain' })).join(''));
  result = scan({ ...options, config, backfill: false, state: result.state });
  assert.equal(total(result.rows.filter(r => r.accountKey === key)), 2);
  assert.equal(total(result.rows.filter(r => r.accountKey === 'unknown')), 0);
  assert.ok(Object.values(result.state.velocity).filter(r => r.replies).every(r => r.accountKey === key));
  assert.deepEqual(Object.keys(result.state.files), [fs.realpathSync(fixed)]); assert.ok(!Object.hasOwn(result.state.files, fs.realpathSync(temp)));
});
