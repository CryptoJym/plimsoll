import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const bundle = new URL('../dist/plimsoll-lean.mjs', import.meta.url);
test('the standalone reader has been built', () => assert.ok(fs.existsSync(bundle), 'missing standalone reader'));
const reader = fs.existsSync(bundle) ? await import(bundle.href) : {};
const since = '2026-10-20T00:00:00Z', until = '2026-10-22T00:00:00Z';
const now = Date.parse(until);
function fixture(t) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'lean-test-'));
  t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  return home;
}
function write(home, name, lines) {
  const file = path.join(home, name);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, typeof lines === 'string' ? lines : Array.isArray(lines) ? lines.map(x => JSON.stringify(x)).join('\n') + '\n' : JSON.stringify(lines));
  return file;
}
const claude = (id, extra = {}) => ({ type: 'assistant', timestamp: '2026-10-20T01:02:03Z', sessionId: 'synthetic-session', message: { id, model: 'synthetic-model', content: [{ text: 'PRIVATE SENTINEL' }], usage: { input_tokens: 10, cache_read_input_tokens: 20, cache_creation_input_tokens: 30, output_tokens: 40 } }, ...extra });
const meta = (id = 'codex-session', extra = {}) => ({ type: 'session_meta', payload: { id, creator_account_id: 'synthetic-account', ...extra } });
const context = { type: 'turn_context', payload: { model: 'synthetic-codex' } };
const usage = (id, timestamp = '2026-10-20T02:00:00Z') => ({ type: 'token_usage_record', timestamp, payload: { response_id: id, usage: { input_tokens: 100, cached_input_tokens: 20, cache_write_input_tokens: 10, output_tokens: 40 } } });
const count = (n, timestamp, rate_limits) => ({ type: 'event_msg', timestamp, payload: { type: 'token_count', info: { total_token_usage: { input_tokens: n, cached_input_tokens: n / 5, output_tokens: n / 2, total_tokens: n * 1.5 } }, rate_limits } });
const options = home => ({ home, env: {}, since, until, now, backfill: true });
function totals(rows) {
  return rows.reduce((a, r) => { for (const k of ['requests', 'freshInput', 'cacheRead', 'cacheWrite', 'output']) a[k] += r[k]; return a; }, { requests: 0, freshInput: 0, cacheRead: 0, cacheWrite: 0, output: 0 });
}

test('Claude replies deduplicate across repeated lines, copied resumptions and subagents', t => {
  const home = fixture(t);
  write(home, '.claude/projects/synthetic/a.jsonl', [claude('a'), claude('a'), claude('synthetic', { message: { id: 'synthetic', model: '<synthetic>', usage: { input_tokens: 999 } } })]);
  write(home, '.claude/projects/synthetic/subagents/b.jsonl', [claude('a'), claude('b')]);
  const { rows, state } = reader.scan(options(home));
  assert.deepEqual(totals(rows), { requests: 2, freshInput: 20, cacheRead: 40, cacheWrite: 60, output: 80 });
  assert.equal(JSON.stringify(state).includes('PRIVATE SENTINEL'), false);
});

test('Claude alternate ids, unknown model, hour edges and invalid lines follow the reference', t => {
  const home = fixture(t);
  write(home, '.claude/projects/x/a.jsonl', [claude('before', { timestamp: '2026-10-19T23:59:59Z' }), claude('edge', { timestamp: since }), claude('next', { timestamp: '2026-10-20T02:00:00Z' }), claude('after', { timestamp: until }), claude(null, { requestId: 'request-id', message: { usage: { input_tokens: 1 } } }), claude(null, { uuid: 'uuid', message: { usage: { output_tokens: 2 } } })]);
  fs.appendFileSync(path.join(home, '.claude/projects/x/a.jsonl'), '{"type":"assistant","usage":bad}\n');
  const { rows } = reader.scan(options(home));
  assert.equal(totals(rows).requests, 4);
  assert.ok(rows.some(r => r.model === 'unknown'));
  assert.ok(rows.every(r => Date.parse(r.hour) >= Date.parse(since) && Date.parse(r.hour) < Date.parse(until)));
});

test('Codex named usage excludes cumulative counters even when the record comes last', t => {
  const home = fixture(t);
  write(home, '.codex/sessions/rollout-a.jsonl', [meta(), context, count(100, since), count(200, '2026-10-20T01:00:00Z'), usage('reply'), usage('reply')]);
  write(home, '.codex/sessions/rollout-copy.jsonl', [meta(), context, usage('reply'), usage('second')]);
  const { rows } = reader.scan(options(home));
  assert.deepEqual(totals(rows), { requests: 2, freshInput: 140, cacheRead: 40, cacheWrite: 20, output: 80 });
});

test('Codex cumulative fallback skips the baseline, zero steps and handles counter resets', t => {
  const home = fixture(t);
  write(home, '.codex/sessions/rollout-a.jsonl', [meta(), context, count(100, since), count(200, '2026-10-20T01:00:00Z'), count(200, '2026-10-20T01:05:00Z'), count(20, '2026-10-20T02:00:00Z')]);
  assert.deepEqual(totals(reader.scan(options(home)).rows), { requests: 2, freshInput: 96, cacheRead: 24, cacheWrite: 0, output: 60 });
});

test('appending named records retracts previously counted fallback and survives a restart', t => {
  const home = fixture(t);
  const file = write(home, '.codex/sessions/rollout-a.jsonl', [meta(), context, count(100, since), count(200, '2026-10-20T01:00:00Z')]);
  let result = reader.scan(options(home));
  assert.equal(totals(result.rows).freshInput, 80);
  fs.appendFileSync(file, JSON.stringify(usage('named')) + '\n');
  result = reader.scan({ ...options(home), backfill: false, state: JSON.parse(JSON.stringify(result.state)) });
  assert.deepEqual(totals(result.rows), { requests: 1, freshInput: 70, cacheRead: 20, cacheWrite: 10, output: 40 });
});

test('incremental cursors resume partial lines; truncation and inode replacement do not duplicate', t => {
  const home = fixture(t);
  const file = write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  const partial = JSON.stringify(claude('two'));
  fs.appendFileSync(file, partial.slice(0, 35));
  let result = reader.scan(options(home));
  fs.appendFileSync(file, partial.slice(35) + '\n');
  result = reader.scan({ ...options(home), backfill: false, state: result.state });
  assert.equal(totals(result.rows).requests, 2);
  fs.writeFileSync(file, JSON.stringify(claude('one')) + '\n');
  result = reader.scan({ ...options(home), backfill: false, state: result.state });
  assert.equal(totals(result.rows).requests, 2);
  fs.renameSync(file, file + '.old');
  fs.writeFileSync(file, JSON.stringify(claude('two')) + '\n' + JSON.stringify(claude('three')) + '\n');
  result = reader.scan({ ...options(home), backfill: false, state: result.state });
  assert.equal(totals(result.rows).requests, 3);
});

test('incremental fallback retains counters and deduplicates rereads', t => {
  const home = fixture(t);
  const file = write(home, '.codex/sessions/rollout-a.jsonl', [meta(), context, count(100, since), count(200, '2026-10-20T01:00:00Z')]);
  let result = reader.scan(options(home));
  fs.appendFileSync(file, JSON.stringify(count(300, '2026-10-20T02:00:00Z')) + '\n');
  result = reader.scan({ ...options(home), backfill: false, state: result.state });
  assert.equal(totals(result.rows).requests, 2);
  result = reader.scan({ ...options(home), backfill: false, state: result.state });
  assert.equal(result.stats.bytesRead, 0);
  assert.equal(totals(result.rows).requests, 2);
});

test('giant usage lines preserve trailing usage without retaining content; other giant lines are skipped', t => {
  const home = fixture(t);
  const reply = claude('giant');
  reply.message.content = [{ text: 'x'.repeat(4 * 1024 * 1024) + '\\" quoted\n PRIVATE SENTINEL' }];
  write(home, '.claude/projects/x/a.jsonl', [reply, { type: 'user', content: 'y'.repeat(4 * 1024 * 1024) }, claude('small')]);
  const result = reader.scan(options(home));
  assert.equal(totals(result.rows).requests, 2);
  assert.ok(JSON.stringify(result.state).length < 20_000);
  assert.equal(JSON.stringify(result.state).includes('PRIVATE SENTINEL'), false);
});

test('all optional roots, environment roots and aliases are discovered once', t => {
  const home = fixture(t);
  for (const [i, dir] of ['.claude/projects', '.claude-seats/seat/projects', '.claude-accounts/seat/projects', 'extra-claude/projects', '.codex/sessions', '.codex-profiles/seat/sessions', '.codex-accounts/seat/sessions', '.codex-lanepool/seat/sessions', 'extra-codex/sessions', 'custom'].entries()) {
    write(home, dir + (dir.includes('codex') ? '/rollout-a.jsonl' : '/a.jsonl'), dir.includes('codex') ? [meta(String(i)), context, usage(String(i))] : [claude(String(i))]);
  }
  fs.symlinkSync(path.join(home, '.claude/projects'), path.join(home, 'alias'));
  const result = reader.scan({ ...options(home), env: { CLAUDE_CONFIG_DIR: path.join(home, 'extra-claude'), CODEX_HOME: path.join(home, 'extra-codex') }, config: { roots: [{ tool: 'claude_code', path: path.join(home, 'custom') }, { tool: 'claude_code', path: path.join(home, 'alias') }] } });
  assert.equal(totals(result.rows).requests, 10);
});

test('project hints prefer newest launch tags; worktrees name the main repo; home repos remain unnamed', t => {
  const home = fixture(t);
  fs.mkdirSync(path.join(home, '.git'));
  fs.mkdirSync(path.join(home, 'main/.git/worktrees/branch'), { recursive: true });
  write(home, 'branch/.git', 'gitdir: ../main/.git/worktrees/branch\n');
  write(home, '.local/state/plimsoll-lean/launch-tags.jsonl', [{ tool: 'claude_code', sessionKey: 'tagged', project: 'Older synthetic', at: since }, { tool: 'claude_code', sessionKey: 'tagged', project: 'Synthetic tag', at: until }]);
  write(home, '.claude/projects/x/a.jsonl', [claude('one', { cwd: path.join(home, 'branch/src') }), claude('two', { sessionId: 'tagged', cwd: path.join(home, 'main') }), claude('three', { sessionId: 'home', cwd: path.join(home, 'one/two/three/four') })]);
  const result = reader.scan(options(home));
  assert.ok(result.rows.some(r => r.projectHint === 'main' && r.projectSource === 'git_repo'));
  assert.ok(result.rows.some(r => r.projectHint === 'Synthetic tag' && r.projectSource === 'launch_tag'));
  assert.ok(result.rows.some(r => r.sessionKey === 'home' && r.projectHint === null && r.projectSource === 'none'));
  assert.equal(Object.values(result.state.sessions).find(s => s.sessionKey === 'home').folderLabel, 'one/two/three');
});

test('account keys exactly reuse the collector OTLP protected-value derivation', t => {
  const home = fixture(t);
  const raw = 'synthetic-account';
  const key = 'sha256:' + crypto.createHash('sha256').update(JSON.stringify({ stringValue: raw })).digest('hex').slice(0, 16);
  write(home, '.claude.json', { oauthAccount: { accountUuid: raw, emailAddress: 'synthetic@example.invalid' } });
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  write(home, '.codex/sessions/rollout-a.jsonl', [meta(), context, usage('one')]);
  assert.ok(reader.scan(options(home)).rows.every(r => r.accountKey === key));
  fs.unlinkSync(path.join(home, '.claude.json'));
  write(home, '.claude/.claude.json', { oauthAccount: { accountUuid: 'synthetic-stale-explicit-profile' } });
  assert.equal(reader.scan(options(home)).rows.find(r => r.tool === 'claude_code').accountKey, 'unknown');
});

test('seats retain the newest reading per account even with out-of-order files', t => {
  const home = fixture(t);
  const rate = n => ({ plan_type: 'synthetic-plan', primary: { used_percent: n, window_minutes: 300, resets_at: 1792638000 }, credits: { balance: '12.34' } });
  write(home, '.codex/sessions/rollout-a.jsonl', [meta(), count(1, '2026-10-20T02:00:00Z', rate(125))]);
  write(home, '.codex/sessions/rollout-b.jsonl', [meta(), count(1, '2026-10-20T01:00:00Z', rate(20))]);
  const seats = Object.values(reader.scan(options(home)).state.seats);
  assert.equal(seats.length, 1);
  assert.equal(seats[0].usedPercent, 125);
  assert.equal(seats[0].creditsBalance, '12.34');
  assert.ok(seats[0].resetsAt.endsWith('Z'));
});

test('titles are opt-in and folder labels can be disabled', t => {
  const home = fixture(t);
  write(home, '.claude/projects/x/a.jsonl', [{ type: 'summary', summary: 'Synthetic title' }, claude('one', { cwd: path.join(home, 'folder') })]);
  let result = reader.scan(options(home));
  assert.equal(Object.values(result.state.sessions)[0].title, null);
  result = reader.scan({ ...options(home), config: { sendSessionTitles: true, sendFolderLabels: false } });
  assert.equal(Object.values(result.state.sessions)[0].title, 'Synthetic title');
  assert.equal(Object.values(result.state.sessions)[0].folderLabel, null);
});

test('atomic state round-trips and delivered closed hours leave the eight-day window', t => {
  const home = fixture(t);
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  const { state } = reader.scan(options(home));
  const file = path.join(home, 'state.json');
  reader.saveState(file, state);
  assert.deepEqual(reader.readState(file), state);
  assert.equal(fs.statSync(file).mode & 0o777, 0o600);
  state.pending = {};
  const result = reader.scan({ home, env: {}, state, now: now + 10 * 86400_000 });
  assert.equal(result.rows.length, 0);
});

test('the lock excludes live owners and recovers a stale PID without removing a successor', t => {
  const home = fixture(t);
  const lock = path.join(home, 'reader.lock');
  const release = reader.acquireLock(lock);
  assert.throws(() => reader.acquireLock(lock), /locked/);
  release();
  fs.writeFileSync(lock, JSON.stringify({ pid: 2147483647, token: 'stale' }));
  const release2 = reader.acquireLock(lock);
  for (const file of fs.readdirSync(lock)) fs.unlinkSync(path.join(lock, file));
  fs.rmdirSync(lock);
  fs.mkdirSync(lock);
  const successor = path.join(lock, 'successor.json');
  fs.writeFileSync(successor, JSON.stringify({ pid: process.pid, token: 'successor' }));
  release2();
  assert.equal(JSON.parse(fs.readFileSync(successor)).token, 'successor');
});

test('local mock verifies exact HMAC, replacement replay, pending retry, and data allowlist', async t => {
  const home = fixture(t), stored = new Map();
  let fail = true, accepted = 0;
  const secret = 'synthetic-signing-secret';
  const server = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const raw = Buffer.concat(chunks).toString();
    const expected = 'sha256=' + crypto.createHmac('sha256', secret).update(req.headers['x-plimsoll-upload-timestamp'] + '.' + raw).digest('hex');
    assert.equal(req.url, '/api/work-intelligence/lean/usage');
    assert.equal(req.headers['x-plimsoll-upload-signature'], expected);
    assert.equal(req.headers['x-plimsoll-install-key'], 'synthetic-key');
    assert.ok(Buffer.byteLength(raw) <= 1024 * 1024);
    assert.equal(raw.includes('PRIVATE SENTINEL'), false);
    if (fail) { res.writeHead(503).end(); return; }
    const body = JSON.parse(raw);
    assert.equal(body.schema, 'plimsoll.lean.usage.v1');
    assert.ok(body.hours.length <= 2000 && body.sessions.length <= 500 && body.seats.length <= 50);
    for (const row of body.hours) stored.set([row.tool, row.accountKey, row.model, row.sessionKey, row.hour].join('|'), row);
    accepted++;
    res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ ok: true }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const credentialsFrom = write(home, 'credentials.json', { uploadUrl: origin + '/other', installKey: 'synthetic-key', uploadSigningSecret: secret, tenantId: '00000000-0000-0000-0000-000000000000' });
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  const args = { ...options(home), config: { credentialsFrom }, command: 'backfill' };
  let result = await reader.runPass(args);
  assert.ok(Object.keys(result.state.pending).length > 0);
  assert.equal(accepted, 0);
  fail = false;
  result = await reader.runPass({ ...args, command: 'run', backfill: false, now: now + 600_000 });
  assert.equal(Object.keys(result.state.pending).length, 0);
  assert.equal(accepted, 1);
  const before = JSON.stringify([...stored.values()]);
  await reader.runPass({ ...args, now: now + 1200_000 });
  assert.equal(JSON.stringify([...stored.values()]), before);
  assert.equal(accepted, 2);
  t.diagnostic(JSON.stringify({ acceptedSends: accepted, storedHoursAfterReplay: stored.size, storedRequests: [...stored.values()].reduce((n, r) => n + r.requests, 0), pendingAfterRetry: Object.keys(result.state.pending).length }));
});

test('uploads split all array and byte limits and refuse credential redirects', async t => {
  const home = fixture(t);
  const state = reader.emptyState();
  state.metadataPending = true;
  for (let i = 0; i < 2101; i++) {
    state.hours[String(i)] = { hour: '2026-10-20T00:00:00.000Z', tool: 'codex', accountKey: 'unknown', model: 'synthetic', sessionKey: String(i), projectHint: 'x'.repeat(512), projectSource: 'launch_tag', requests: 1, freshInput: 1, cacheRead: 0, cacheWrite: 0, output: 0 };
    state.pending[String(i)] = true;
    state.sessions[String(i)] = { tool: 'codex', sessionKey: String(i), title: null, folderLabel: null, projectHint: null, firstSeen: since, lastSeen: until };
  }
  for (let i = 0; i < 51; i++) state.seats[String(i)] = { tool: 'codex', accountKey: String(i), observedAt: until, plan: null, usedPercent: null, windowMinutes: null, resetsAt: null, creditsBalance: null };
  const batches = reader.makeBatches(state, '00000000-0000-0000-0000-000000000000', now);
  assert.equal(batches.reduce((n, b) => n + b.body.hours.length, 0), 2101);
  assert.equal(batches.reduce((n, b) => n + b.body.sessions.length, 0), 2101);
  assert.equal(batches.reduce((n, b) => n + b.body.seats.length, 0), 51);
  for (const b of batches) {
    assert.ok(Buffer.byteLength(JSON.stringify(b.body)) <= 1024 * 1024);
    assert.ok(b.body.hours.length <= 2000 && b.body.sessions.length <= 500 && b.body.seats.length <= 50);
  }
  let requests = 0;
  const server = http.createServer((req, res) => { requests++; res.writeHead(302, { location: '/leak' }).end(); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const credentialsFrom = write(home, 'credentials.json', { uploadUrl: `http://127.0.0.1:${server.address().port}`, installKey: 'synthetic', uploadSigningSecret: 'synthetic', tenantId: 'synthetic' });
  await reader.sendPending(state, { credentialsFrom }, { home, now });
  assert.equal(requests, 1);
  assert.ok(Object.keys(state.pending).length > 0);
});

test('LaunchAgent uses collector Node and required scheduling without installing', t => {
  const home = fixture(t);
  const node = process.execPath;
  write(home, 'Library/LaunchAgents/com.plimsoll.collector.plist', `<?xml version="1.0"?><plist><dict><key>ProgramArguments</key><array><string>${node}</string><string>/synthetic/collector.mjs</string></array></dict></plist>`);
  const xml = reader.launchAgent(home, '/synthetic/lean.mjs');
  for (const text of ['com.plimsoll.lean-reader', node, '<integer>300</integer>', '<key>RunAtLoad</key>', '<key>LowPriorityIO</key>', '<string>Background</string>', '<integer>10</integer>']) assert.ok(xml.includes(text));
  assert.equal(fs.existsSync(path.join(home, 'Library/LaunchAgents/com.plimsoll.lean-reader.plist')), false);
});

test('print-config never reads credentials and redacts unknown secret fields', t => {
  const home = fixture(t);
  const config = write(home, '.config/plimsoll-lean/config.json', { credentialsFrom: '/missing', installKey: 'PRIVATE KEY', uploadSigningSecret: 'PRIVATE SECRET', endpoint: 'https://example.invalid/path?secret=PRIVATE', sendSessionTitles: false });
  const result = spawnSync(process.execPath, [fileURLToPath(bundle), 'print-config', '--config', config], { encoding: 'utf8', env: { ...process.env, HOME: home, PLIMSOLL_LEAN_STATE_DIR: path.join(home, 'state') } });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout.includes('PRIVATE'), false);
  assert.equal(result.stdout.includes('uploadSigningSecret'), false);
});

test('fallback deduplication survives a rewritten prefix that changes byte positions', t => {
  const home = fixture(t);
  const records = [meta(), context, count(100, since), count(200, '2026-10-20T01:00:00Z')];
  const file = write(home, '.codex/sessions/rollout-a.jsonl', records);
  let result = reader.scan(options(home));
  fs.renameSync(file, file + '.old');
  fs.writeFileSync(file, records.map(r => JSON.stringify(r, null, 0) + ' ').join('\n') + '\n');
  result = reader.scan({ ...options(home), backfill: false, state: result.state });
  assert.equal(totals(result.rows).requests, 1);
});

test('the upload boundary strips unexpected fields even from an old or edited state', () => {
  const state = reader.emptyState();
  state.hours.one = { ...totals([]), hour: since, tool: 'claude_code', accountKey: 'unknown', model: 'synthetic', sessionKey: 'synthetic', projectHint: null, projectSource: 'none', prompt: 'PRIVATE SENTINEL' };
  state.pending.one = true;
  assert.equal(JSON.stringify(reader.makeBatches(state, 'synthetic', now)).includes('PRIVATE SENTINEL'), false);
});

test('launch tags added after capture update existing totals without rereading the transcript', t => {
  const home = fixture(t);
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  let result = reader.scan(options(home));
  write(home, '.local/state/plimsoll-lean/launch-tags.jsonl', [{ tool: 'claude_code', sessionKey: 'synthetic-session', project: 'Late synthetic tag', at: until }]);
  result = reader.scan({ ...options(home), backfill: false, state: result.state });
  assert.equal(result.rows[0].projectHint, 'Late synthetic tag');
  assert.equal(result.stats.bytesRead, 0);
  assert.equal(Object.values(result.state.sessions)[0].projectHint, 'Late synthetic tag');
});

test('a truncated named-record file never starts adding cumulative fallback on top', t => {
  const home = fixture(t);
  const records = [meta(), context, count(100, since), count(200, '2026-10-20T01:00:00Z')];
  const file = write(home, '.codex/sessions/rollout-a.jsonl', [...records, usage('named')]);
  let result = reader.scan(options(home));
  write(home, '.codex/sessions/rollout-a.jsonl', records);
  result = reader.scan({ ...options(home), backfill: false, state: result.state });
  fs.appendFileSync(file, JSON.stringify(count(300, '2026-10-20T03:00:00Z')) + '\n');
  result = reader.scan({ ...options(home), backfill: false, state: result.state });
  assert.equal(totals(result.rows).requests, 1);
});

test('a partial-hour backfill sends full touched-hour totals', t => {
  const home = fixture(t);
  write(home, '.claude/projects/x/a.jsonl', [claude('one', { timestamp: '2026-10-20T01:10:00Z' }), claude('two', { timestamp: '2026-10-20T01:40:00Z' })]);
  const result = reader.scan({ ...options(home), since: '2026-10-20T01:30:00Z', until: '2026-10-20T01:45:00Z' });
  assert.equal(totals(result.rows).requests, 2);
});

test('an unreadable source prevents a backfill from sending incomplete replacement totals', async t => {
  const home = fixture(t);
  const file = write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  const args = { ...options(home), command: 'backfill', dryRun: true };
  await reader.runPass(args);
  const stateFile = path.join(home, 'Library/Application Support/Plimsoll Lean/state.json');
  const before = fs.readFileSync(stateFile, 'utf8');
  fs.chmodSync(file, 0);
  try {
    await assert.rejects(reader.runPass(args), /source_unreadable/);
    assert.equal(fs.readFileSync(stateFile, 'utf8'), before);
  } finally { fs.chmodSync(file, 0o600); }
});

test('a giant Claude reply with an empty usage object still counts its request', t => {
  const home = fixture(t);
  const reply = claude('empty-usage');
  reply.message.content = [{ text: 'x'.repeat(2 * 1024 * 1024) }];
  reply.message.usage = {};
  write(home, '.claude/projects/x/a.jsonl', [reply]);
  assert.equal(totals(reader.scan(options(home)).rows).requests, 1);
});

test('malformed escapes in ignored giant content invalidate the whole usage line', t => {
  const home = fixture(t);
  const reply = claude('malformed');
  reply.message.content = [{ text: 'x'.repeat(2 * 1024 * 1024) }];
  const bad = JSON.stringify(reply).replace('"text":"', '"text":"\\q');
  write(home, '.claude/projects/x/a.jsonl', bad + '\n');
  assert.equal(totals(reader.scan(options(home)).rows).requests, 0);
});

test('only root path and tool settings survive print-config', t => {
  const home = fixture(t);
  const config = write(home, '.config/plimsoll-lean/config.json', { roots: [{ tool: 'codex', path: '/synthetic/sessions', uploadSigningSecret: 'PRIVATE SENTINEL' }] });
  const result = spawnSync(process.execPath, [fileURLToPath(bundle), 'print-config', '--config', config], { encoding: 'utf8' });
  assert.equal(result.status, 0);
  assert.equal(result.stdout.includes('PRIVATE SENTINEL'), false);
});

test('the collector LaunchAgent takes precedence over other Plimsoll agents', t => {
  const home = fixture(t);
  const plist = node => `<?xml version="1.0"?><plist><dict><key>ProgramArguments</key><array><string>${node}</string></array></dict></plist>`;
  write(home, 'Library/LaunchAgents/com.plimsoll.aaa.plist', plist('/synthetic/other/node'));
  write(home, 'Library/LaunchAgents/com.plimsoll.collector.plist', plist(process.execPath));
  assert.ok(reader.launchAgent(home, '/synthetic/lean.mjs').includes(process.execPath));
});

test('capacity refusal preserves the committed state instead of dropping pending rows', t => {
  const home = fixture(t), file = path.join(home, 'state.json');
  const state = reader.emptyState();
  reader.saveState(file, state);
  const before = fs.readFileSync(file, 'utf8');
  state.sessions.giant = { title: 'x'.repeat(10 * 1024 * 1024) };
  assert.throws(() => reader.saveState(file, state), /state_capacity/);
  assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('run logs rotate once and contain only the run summary', async t => {
  const home = fixture(t);
  const log = write(home, 'Library/Logs/plimsoll-lean/reader.log', 'x'.repeat(5_000_000));
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  await reader.runPass({ ...options(home), dryRun: true });
  assert.equal(fs.statSync(log + '.1').size, 5_000_000);
  const row = JSON.parse(fs.readFileSync(log, 'utf8'));
  assert.equal(row.files, 1);
  assert.equal(row.rows, 1);
  assert.equal(JSON.stringify(row).includes('PRIVATE SENTINEL'), false);
});

test('the copied standalone and symlinked command execute without node_modules', t => {
  const home = fixture(t);
  const standalone = path.join(home, 'plimsoll-lean.mjs');
  fs.copyFileSync(bundle, standalone);
  const alias = path.join(home, 'lean-alias.mjs');
  fs.symlinkSync(standalone, alias);
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  for (const executable of [standalone, alias]) {
    const result = spawnSync(process.execPath, [executable, 'backfill', '--since', since, '--until', until, '--dry-run', '--json'], { encoding: 'utf8', cwd: home, env: { ...process.env, HOME: home, CLAUDE_CONFIG_DIR: path.join(home, '.claude'), CODEX_HOME: path.join(home, '.codex') } });
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /^\{/, 'the CLI must execute, including through a symlink');
    assert.equal(totals(JSON.parse(result.stdout).rows).requests, 1);
  }
});

test('a scan refuses growing state before its deduplication and metadata exceed the bound', t => {
  const home = fixture(t);
  const records = Array.from({ length: 6000 }, (_, i) => claude('bounded-' + i, { sessionId: String(i).padEnd(500, 's'), message: { id: 'bounded-' + i, model: 'm'.repeat(512), usage: { input_tokens: 1 } } }));
  write(home, '.claude/projects/x/a.jsonl', records);
  assert.throws(() => reader.scan(options(home)), /state_capacity/);
});

test('historical launch tags are streamed without retaining inactive sessions', t => {
  const home = fixture(t);
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  const file = write(home, '.local/state/plimsoll-lean/launch-tags.jsonl', [{ tool: 'claude_code', sessionKey: 'synthetic-session', project: 'Active synthetic tag', at: since }]);
  const fd = fs.openSync(file, 'a');
  try {
    for (let batch = 0; batch < 200; batch++) {
      const lines = Array.from({ length: 1000 }, (_, i) => JSON.stringify({ tool: 'claude_code', sessionKey: `inactive-${batch}-${i}`, project: 'x'.repeat(512), at: since })).join('\n') + '\n';
      fs.writeSync(fd, lines);
    }
  } finally { fs.closeSync(fd); }
  const result = spawnSync(process.execPath, [fileURLToPath(bundle), 'backfill', '--since', since, '--until', until, '--dry-run', '--json'], { encoding: 'utf8', env: { ...process.env, HOME: home, CODEX_HOME: path.join(home, '.codex'), CLAUDE_CONFIG_DIR: path.join(home, '.claude') } });
  assert.equal(result.status, 0, result.stderr);
  const output = JSON.parse(result.stdout);
  assert.equal(output.rows[0].projectHint, 'Active synthetic tag');
  assert.ok(output.run.peakMemoryBytes < 128 * 1024 * 1024, `inactive tags must not stay resident: ${output.run.peakMemoryBytes}`);
});

test('launch tags preserve UTF-8 characters split across read chunks', t => {
  const home = fixture(t);
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  const project = '汉字示例';
  const tag = JSON.stringify({ tool: 'claude_code', sessionKey: 'synthetic-session', project, at: since });
  const prefix = 65535 - Buffer.byteLength(tag.slice(0, tag.indexOf(project))) - 1;
  write(home, '.local/state/plimsoll-lean/launch-tags.jsonl', 'x'.repeat(prefix) + '\n' + tag + '\n');
  assert.equal(reader.scan(options(home)).rows[0].projectHint, project);
});

test('binary collector plists resolve their Node through built-in macOS tooling', { skip: process.platform !== 'darwin' }, t => {
  const home = fixture(t), node = '/synthetic/collector/node';
  const file = write(home, 'Library/LaunchAgents/com.plimsoll.collector.plist', `<?xml version="1.0"?><plist><dict><key>ProgramArguments</key><array><string>${node}</string></array></dict></plist>`);
  const converted = spawnSync('/usr/bin/plutil', ['-convert', 'binary1', file]);
  assert.equal(converted.status, 0);
  assert.ok(reader.launchAgent(home, '/synthetic/lean.mjs').includes(node));
});

test('an explicit default Claude config directory uses its own account metadata', t => {
  const home = fixture(t);
  write(home, '.claude.json', { oauthAccount: { accountUuid: 'synthetic-old-default' } });
  write(home, '.claude/.claude.json', { oauthAccount: { accountUuid: 'synthetic-explicit' } });
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  const result = reader.scan({ ...options(home), env: { CLAUDE_CONFIG_DIR: path.join(home, '.claude') } });
  assert.equal(result.rows[0].accountKey, reader.accountKey('synthetic-explicit'));
});
