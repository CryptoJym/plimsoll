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

test('rootsOnly confines a replay to its explicitly listed source', t => {
  const home = fixture(t), selected = path.join(home, 'selected', 'sessions');
  fs.mkdirSync(selected, { recursive: true });
  const result = reader.roots(home, { CLAUDE_CONFIG_DIR: home + '/other', CODEX_HOME: home + '/other-codex' }, { rootsOnly: true, roots: [{ tool: 'codex', path: selected }] });
  assert.deepEqual(result, [{ tool: 'codex', path: fs.realpathSync(selected) }]);
});

test('velocity replay reuses the hours account key when a profile switched accounts', t => {
  const home = fixture(t), map = path.join(home, 'velocity-account-map.json');
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  fs.writeFileSync(map, JSON.stringify({ ['claude_code\tsynthetic-model\tsynthetic-session\t2026-10-20T01:00:00.000Z']: 'historical-account' }));
  const result = reader.scan({ ...options(home), velocityOnly: true, command: 'velocity-backfill', backfill: false, config: { velocityAccountMapFile: map } });
  assert.equal(Object.values(result.state.velocity)[0].accountKey, 'historical-account');
});

test('project hints prefer newest launch tags; worktrees name the main repo; home repos remain unnamed', t => {
  const home = fixture(t);
  fs.mkdirSync(path.join(home, '.git'));
  fs.mkdirSync(path.join(home, 'primary/.git/worktrees/branch'), { recursive: true });
  write(home, 'branch/.git', 'gitdir: ../primary/.git/worktrees/branch\n');
  write(home, '.local/state/plimsoll-lean/launch-tags.jsonl', [{ tool: 'claude_code', sessionKey: 'tagged', project: 'Older synthetic', at: since }, { tool: 'claude_code', sessionKey: 'tagged', project: 'Synthetic tag', at: until }]);
  write(home, '.claude/projects/x/a.jsonl', [claude('one', { cwd: path.join(home, 'branch/src') }), claude('two', { sessionId: 'tagged', cwd: path.join(home, 'primary') }), claude('three', { sessionId: 'home', cwd: path.join(home, 'one/two/three/four') })]);
  const result = reader.scan(options(home));
  assert.ok(result.rows.some(r => r.projectHint === 'primary' && r.projectSource === 'git_repo'));
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
  for (const text of ['com.plimsoll.lean-reader', node, '<integer>300</integer>', '<key>RunAtLoad</key>', '<key>LowPriorityIO</key>', '<string>Background</string>', '<integer>10</integer>', '--jitless --max-old-space-size=48 --max-semi-space-size=1']) assert.ok(xml.includes(text));
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

test('IDs read in a pass count at their packed size, so a pass fits when the state it saves does', t => {
  const home = fixture(t), n = 300_000;
  // Held as a JSON map in flight these IDs alone would pass the 9 MiB ceiling; packed they take 16 bytes each.
  const lines = [JSON.stringify(meta()), JSON.stringify(context)];
  for (let i = 0; i < n; i++) lines.push(JSON.stringify(usage('synthetic-' + i)));
  write(home, '.codex/sessions/2026/10/20/rollout-large.jsonl', lines.join('\n') + '\n');
  const file = path.join(home, 'state.json');
  const result = reader.scan(options(home));
  assert.equal(totals(result.rows).requests, n);
  const bytes = reader.saveState(file, result.state);
  assert.ok(bytes > n * 16 && bytes < 9 * 1024 * 1024, String(bytes));
  const again = reader.scan({ ...options(home), state: reader.readState(file) });
  assert.equal(totals(again.rows).requests, n, 'a backfill over the saved state neither fails nor recounts');
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

// Evidence fixtures use only temporary repositories and invented transcripts.
const evidenceOptions = home => ({ ...options(home), config: { sendEvidence: true } });
const session = result => Object.values(result.state.sessions)[0];
const toolReply = (id, name, input, extra = {}) => claude(id, { message: { ...claude(id).message, content: [{ type: 'tool_use', id: 'call-' + id, name, input }] }, ...extra });
function repository(home, name, branch = 'main') {
  write(home, name + '/.git/HEAD', `ref: refs/heads/${branch}\n`);
  return path.join(home, name);
}

test('Claude file tools, nested paths and shell calls count each repository once per call', t => {
  const home = fixture(t), alpha = repository(home, 'alpha'), beta = repository(home, 'beta'), quoted = repository(home, 'quoted repo');
  repository(home, '.', 'private-home-branch');
  const cwd = path.join(home, 'Projects');
  const records = [
    toolReply('read', 'Read', { file_path: alpha + '/src/private-file.ts' }),
    toolReply('edit', 'Edit', { path: '../alpha/new-file.ts', old_string: 'PRIVATE OLD', new_string: 'PRIVATE NEW' }),
    toolReply('write', 'Write', { file_path: beta + '/new-file.ts', content: 'PRIVATE CONTENT' }),
    toolReply('notebook', 'NotebookEdit', { notebook_path: alpha + '/notebook.ipynb', new_source: 'PRIVATE CELL' }),
    toolReply('glob', 'Glob', { path: alpha, pattern: 'PRIVATE PATTERN' }),
    toolReply('multi', 'MultiEdit', { edits: [{ file_path: alpha + '/a.ts' }, { path: beta + '/b.ts' }] }),
    toolReply('shell', 'Bash', { command: `cat ${alpha}/a.ts ${alpha}/b.ts` }),
    toolReply('git', 'Bash', { command: `git -C '${beta}' status` }),
    toolReply('quoted', 'Bash', { command: `cat "${quoted}/src/private-file.ts"` }),
    toolReply('outside', 'Read', { file_path: home + '/outside/a.ts' }),
    toolReply('home', 'Read', { file_path: home + '/home-file.ts' }),
    toolReply('root', 'Read', { file_path: '/' }),
    { type: 'assistant', timestamp: '2026-10-20T01:03:00Z', message: { content: [{ type: 'tool_use', id: 'no-usage', name: 'Read', input: { file_path: alpha + '/last.ts' } }] } },
  ];
  for (const record of records) Object.assign(record, { cwd, sessionId: 'synthetic-session', gitBranch: 'feature/historical' });
  write(home, '.claude/projects/x/a.jsonl', records);
  write(home, '.claude/projects/x/copy.jsonl', [records[0]]);
  const result = reader.scan(evidenceOptions(home));
  assert.deepEqual(session(result).reposTouched, [{ name: 'alpha', events: 7 }, { name: 'beta', events: 3 }, { name: 'quoted repo', events: 1 }]);
  assert.equal(session(result).gitBranch, 'feature/historical');
  const body = JSON.stringify(reader.makeBatches(result.state, 'synthetic', now, { sendEvidence: true }));
  for (const forbidden of ['private-file.ts', 'PRIVATE', home, 'file_path', 'notebook_path', 'old_string']) assert.equal(body.includes(forbidden), false, forbidden);
  assert.equal(JSON.stringify(result.state.evidence).includes('PRIVATE'), false);
});

test('Codex function arguments, workdirs and both patch formats provide evidence', t => {
  const home = fixture(t), alpha = repository(home, 'alpha'), beta = repository(home, 'beta');
  const call = (id, name, args) => ({ type: 'response_item', timestamp: '2026-10-20T02:00:00Z', payload: { type: 'function_call', name, call_id: id, arguments: typeof args === 'string' ? args : JSON.stringify(args) } });
  const custom = { type: 'response_item', timestamp: '2026-10-20T02:00:01Z', payload: { type: 'custom_tool_call', name: 'functions.apply_patch', call_id: 'custom', input: `*** Begin Patch\n*** Add File: ${beta}/new.ts\n+PRIVATE CODE\n*** Update File: ${alpha}/old.ts\n@@\n-PRIVATE OLD\n+PRIVATE NEW\n*** Move to: ${beta}/moved.ts\n*** End Patch` } };
  write(home, '.codex/sessions/rollout-evidence.jsonl', [
    meta('codex-session', { cwd: path.join(home, 'Projects'), git: { branch: 'feature/recorded' } }), context,
    call('cwd', 'functions.exec_command', { workdir: alpha, cmd: 'cat relative.ts' }),
    call('patch', 'functions.apply_patch', { workdir: alpha, patch: '*** Begin Patch\n*** Update File: src/file.ts\n@@\n+PRIVATE CODE\n*** Delete File: stale.ts\n*** End Patch' }),
    call('diff', 'apply_patch', { cwd: beta, patch: '--- a/src/a.ts\n+++ b/src/a.ts\n+PRIVATE CODE' }),
    custom,
    call('git', 'functions.shell_command', { command: `git -C ${beta} diff` }),
    call('argv', 'functions.shell', { command: ['git', '-C', '../beta', 'status'] }),
    { type: 'response_item', timestamp: '2026-10-20T02:00:02Z', payload: { type: 'function_call_output', output: JSON.stringify({ file_path: alpha + '/PRIVATE OUTPUT' }) } },
    call('malformed', 'Read', '{bad json'), usage('one'),
  ]);
  const result = reader.scan(evidenceOptions(home));
  assert.deepEqual(session(result).reposTouched, [{ name: 'beta', events: 4 }, { name: 'alpha', events: 3 }]);
  assert.equal(session(result).gitBranch, 'feature/recorded');
  assert.equal(JSON.stringify(result.state.evidence).includes('PRIVATE'), false);
});

test('worktree evidence uses the main repository and its own HEAD; detached and absent branches are null', t => {
  const home = fixture(t);
  repository(home, 'primary', 'main-branch');
  write(home, 'primary/.git/worktrees/lane/HEAD', 'ref: refs/heads/feature/worktree\n');
  write(home, 'primary/.git/worktrees/lane/commondir', '../..\n');
  const cwd = path.join(home, 'lane');
  write(home, 'lane/.git', 'gitdir: ../primary/.git/worktrees/lane\n');
  write(home, '.claude/projects/x/a.jsonl', [toolReply('one', 'Read', { file_path: cwd + '/new.ts' }, { cwd })]);
  let result = reader.scan(evidenceOptions(home));
  assert.deepEqual(session(result).reposTouched, [{ name: 'primary', events: 1 }]);
  assert.equal(session(result).gitBranch, 'feature/worktree');
  write(home, 'primary/.git/worktrees/lane/HEAD', '0123456789abcdef0123456789abcdef01234567\n');
  result = reader.scan(evidenceOptions(home));
  assert.equal(session(result).gitBranch, null);
  write(home, '.claude/projects/x/a.jsonl', [claude('outside', { cwd: path.join(home, 'outside') })]);
  result = reader.scan(evidenceOptions(home));
  assert.equal(session(result).gitBranch, null);
  assert.deepEqual(session(result).reposTouched, []);
});

test('home repositories, disk roots, tool output and paths outside repositories never name touched repositories', t => {
  const home = fixture(t);
  repository(home, '.', 'home-branch');
  write(home, '.claude/projects/x/a.jsonl', [toolReply('home', 'Read', { file_path: home + '/PRIVATE.ts' }, { cwd: home }), toolReply('root', 'Read', { file_path: '/' }, { cwd: '/' }), toolReply('outside', 'Read', { file_path: path.join(os.tmpdir(), 'synthetic-no-repo', 'PRIVATE.ts') })]);
  const result = reader.scan(evidenceOptions(home));
  assert.deepEqual(session(result).reposTouched, []);
  assert.equal(session(result).gitBranch, null);
});

test('repositories are ranked by event count before the twenty-item wire cap', t => {
  const home = fixture(t), calls = [];
  for (let i = 1; i <= 25; i++) {
    const dir = repository(home, 'repo-' + String(i).padStart(2, '0'));
    for (let j = 0; j < i; j++) calls.push(toolReply(`${i}-${j}`, 'Read', { file_path: dir + '/file.ts' }));
  }
  write(home, '.claude/projects/x/a.jsonl', calls);
  const result = reader.scan(evidenceOptions(home));
  assert.equal(session(result).reposTouched.length, 20);
  assert.deepEqual(session(result).reposTouched[0], { name: 'repo-25', events: 25 });
  assert.deepEqual(session(result).reposTouched.at(-1), { name: 'repo-06', events: 6 });
});

test('evidence survives restart, backfill, partial lines, duplicate calls and enabling on a legacy cursor', t => {
  const home = fixture(t), repo = repository(home, 'alpha');
  const file = write(home, '.claude/projects/x/a.jsonl', [toolReply('one', 'Read', { file_path: repo + '/file.ts' }, { cwd: repo })]);
  const legacy = reader.scan(options(home));
  // The v1 ID representation written by 75bb4bee, with the same committed cursor.
  legacy.state.version = 1;
  legacy.state.seen = { [crypto.createHash('sha256').update('claude_code:' + JSON.stringify('one')).digest('hex').slice(0, 24)]: Math.floor(Date.parse('2026-10-20T01:02:03Z') / 3600000) };
  delete legacy.state.seenPacked;
  const before = JSON.stringify(legacy.rows);
  assert.equal(legacy.state.version, 1);
  assert.equal(legacy.state.seenPacked, undefined);
  let result = reader.scan({ ...evidenceOptions(home), backfill: false, state: JSON.parse(JSON.stringify(legacy.state)) });
  assert.equal(result.state.version, 2);
  assert.deepEqual(session(result).reposTouched, [{ name: 'alpha', events: 1 }]);
  assert.equal(JSON.stringify(result.rows), before);
  result = reader.scan({ ...evidenceOptions(home), state: JSON.parse(JSON.stringify(result.state)) });
  assert.deepEqual(session(result).reposTouched, [{ name: 'alpha', events: 1 }]);
  const next = JSON.stringify(toolReply('two', 'Read', { path: repo + '/another.ts' }, { cwd: repo }));
  fs.appendFileSync(file, next.slice(0, 80));
  result = reader.scan({ ...evidenceOptions(home), state: result.state, backfill: false });
  assert.deepEqual(session(result).reposTouched, [{ name: 'alpha', events: 1 }]);
  fs.appendFileSync(file, next.slice(80) + '\n' + next + '\n');
  result = reader.scan({ ...evidenceOptions(home), state: result.state, backfill: false });
  assert.deepEqual(session(result).reposTouched, [{ name: 'alpha', events: 2 }]);
  result = reader.scan({ ...options(home), state: result.state, backfill: false });
  assert.equal(Object.hasOwn(session(result), 'reposTouched'), false);
  fs.appendFileSync(file, JSON.stringify(toolReply('three', 'Read', { path: repo + '/third.ts' }, { cwd: repo })) + '\n');
  result = reader.scan({ ...options(home), state: result.state, backfill: false });
  result = reader.scan({ ...evidenceOptions(home), state: JSON.parse(JSON.stringify(result.state)), backfill: false });
  assert.deepEqual(session(result).reposTouched, [{ name: 'alpha', events: 3 }]);
  assert.equal(totals(result.rows).requests, 3);
});

test('lane marker at cwd or a parent wins over Git; invalid markers and markers above home are ignored', t => {
  const home = fixture(t), cwd = repository(home, 'copy');
  const tag = { project: 'source-repository', schema: 'conductor-source-project/v1' };
  const file = write(home, '.claude/projects/x/a.jsonl', [claude('one', { cwd })]);
  write(home, 'copy/.conductor-project.json', tag);
  let result = reader.scan(evidenceOptions(home));
  assert.equal(result.rows[0].projectHint, 'source-repository');
  assert.equal(result.rows[0].projectSource, 'launch_tag');
  fs.writeFileSync(file, JSON.stringify(claude('nested', { cwd: cwd + '/nested/folder' })) + '\n');
  result = reader.scan(evidenceOptions(home));
  assert.equal(result.rows[0].projectHint, 'source-repository');
  result = reader.scan(options(home));
  assert.equal(result.rows[0].projectHint, 'source-repository', 'markers apply with evidence off');
  assert.equal(result.rows[0].projectSource, 'launch_tag');
  write(home, 'copy/.conductor-project.json', { ...tag, project: '/PRIVATE/path' });
  assert.equal(reader.scan(evidenceOptions(home)).rows[0].projectHint, 'copy');
  write(home, 'copy/.conductor-project.json', { ...tag, schema: 'wrong' });
  assert.equal(reader.scan(evidenceOptions(home)).rows[0].projectHint, 'copy');
  // The real temp parent is above this synthetic home, and must not be read.
  const inner = path.join(home, 'inner');
  write(home, '.conductor-project.json', tag);
  write(inner, '.claude/projects/x/a.jsonl', [claude('inner', { cwd: inner + '/Projects' })]);
  assert.equal(reader.scan(evidenceOptions(inner)).rows[0].projectHint, null);
  write(inner, '.conductor-project.json', tag);
  assert.equal(reader.scan(evidenceOptions(inner)).rows[0].projectSource, 'launch_tag');
});

test('explicit session launch tags outrank a copied-folder marker', t => {
  const home = fixture(t), cwd = repository(home, 'copy');
  write(home, 'copy/.conductor-project.json', { project: 'folder-source', schema: 'conductor-source-project/v1' });
  write(home, '.local/state/plimsoll-lean/launch-tags.jsonl', [{ tool: 'claude_code', sessionKey: 'synthetic-session', project: 'explicit-source', at: since }]);
  write(home, '.claude/projects/x/a.jsonl', [claude('one', { cwd })]);
  const result = reader.scan(evidenceOptions(home));
  assert.equal(result.rows[0].projectHint, 'explicit-source');
  assert.equal(session(result).projectHint, 'explicit-source');
});

test('a lane marker found later names existing totals without rereading or recounting, with evidence off or on', t => {
  for (const evidence of [false, true]) {
    const home = fixture(t), cwd = path.join(home, 'unversioned-lane');
    const scanOptions = evidence ? evidenceOptions(home) : options(home);
    write(home, '.claude/projects/x/a.jsonl', [claude('one', { cwd })]);
    const before = reader.scan(scanOptions);
    assert.equal(before.rows[0].projectHint, null);
    for (const key of Object.keys(before.state.pending)) delete before.state.pending[key];
    write(home, 'unversioned-lane/.conductor-project.json', { project: 'source-repository', schema: 'conductor-source-project/v1' });
    const result = reader.scan({ ...scanOptions, backfill: false, state: before.state });
    assert.equal(result.stats.bytesRead, 0);
    assert.equal(result.rows[0].projectHint, 'source-repository');
    assert.equal(result.rows[0].projectSource, 'launch_tag');
    assert.equal(Object.keys(result.state.pending).length, 1, 'the corrected row is sent again');
    assert.equal(session(result).projectHint, 'source-repository');
    assert.equal(totals(result.rows).requests, 1);
  }
});

test('enabling evidence keeps a lane marker on existing totals without recounting usage', t => {
  const home = fixture(t), cwd = path.join(home, 'unversioned-lane');
  write(home, 'unversioned-lane/.conductor-project.json', { project: 'source-repository', schema: 'conductor-source-project/v1' });
  write(home, '.claude/projects/x/a.jsonl', [claude('one', { cwd })]);
  const legacy = reader.scan(options(home));
  assert.equal(legacy.rows[0].projectHint, 'source-repository');
  const result = reader.scan({ ...evidenceOptions(home), backfill: false, state: legacy.state });
  assert.equal(result.rows[0].projectHint, 'source-repository');
  assert.equal(result.rows[0].projectSource, 'launch_tag');
  assert.equal(session(result).projectHint, 'source-repository');
  assert.equal(totals(result.rows).requests, 1);
});

test('checkout folders named for their role name no project; lane markers still name them', t => {
  const home = fixture(t);
  const generic = ['work', 'repo', 'head', 'base', 'main', 'main-10d48c98', 'src', 'app', 'tree', 'pr', 'pr1623', 'upstream', 'Work'];
  const records = generic.map((name, i) => claude('generic-' + i, { sessionId: 'generic-' + i, cwd: repository(home, 'lanes/' + i + '/' + name) + '/nested' }));
  records.push(claude('named', { sessionId: 'named', cwd: repository(home, 'lanes/named/synthetic-project') }));
  records.push(claude('marked', { sessionId: 'marked', cwd: repository(home, 'lanes/marked/work') }));
  write(home, 'lanes/marked/.conductor-project.json', { project: 'marked-project', schema: 'conductor-source-project/v1' });
  // Lane cleanup can delete a work folder; the lane folder's marker still names it in a later backfill.
  records.push(claude('deleted', { sessionId: 'deleted', cwd: path.join(home, 'lanes/deleted/work/nested') }));
  write(home, 'lanes/deleted/.conductor-project.json', { project: 'deleted-project', schema: 'conductor-source-project/v1' });
  write(home, '.claude/projects/x/a.jsonl', records);
  for (const scanOptions of [options(home), evidenceOptions(home)]) {
    const result = reader.scan(scanOptions);
    const row = key => result.rows.find(r => r.sessionKey === key);
    for (let i = 0; i < generic.length; i++) {
      assert.equal(row('generic-' + i).projectHint, null, generic[i]);
      assert.equal(row('generic-' + i).projectSource, 'none', generic[i]);
    }
    assert.equal(Object.values(result.state.sessions).find(s => s.sessionKey === 'generic-0').folderLabel, 'lanes/0/work');
    assert.deepEqual([row('named').projectHint, row('named').projectSource], ['synthetic-project', 'git_repo']);
    assert.deepEqual([row('marked').projectHint, row('marked').projectSource], ['marked-project', 'launch_tag']);
    assert.deepEqual([row('deleted').projectHint, row('deleted').projectSource], ['deleted-project', 'launch_tag']);
  }
});

test('touched repositories skip checkout folders named for their role', t => {
  const home = fixture(t), work = repository(home, 'lane/work', 'lane-base'), named = repository(home, 'synthetic-project', 'feature/x');
  write(home, '.claude/projects/x/a.jsonl', [toolReply('work', 'Read', { file_path: work + '/a.ts' }, { cwd: work }), toolReply('named', 'Read', { file_path: named + '/b.ts' }, { cwd: work })]);
  const result = reader.scan(evidenceOptions(home));
  assert.deepEqual(session(result).reposTouched, [{ name: 'synthetic-project', events: 1 }]);
});

test('flag-off upload bytes match the frozen 75bb4bee session shape, including with evidence in state', t => {
  const home = fixture(t);
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  const result = reader.scan(options(home));
  const legacy = '{"schema":"plimsoll.lean.usage.v1","tenantId":"synthetic","reader":{"version":"0.1.0","machineName":' + JSON.stringify(os.hostname().split('.')[0]) + '},"sentAt":"2026-10-22T00:00:00.000Z","hours":[{"hour":"2026-10-20T01:00:00.000Z","tool":"claude_code","accountKey":"unknown","model":"synthetic-model","sessionKey":"synthetic-session","projectHint":null,"projectSource":"none","requests":1,"freshInput":10,"cacheRead":20,"cacheWrite":30,"output":40}],"sessions":[{"tool":"claude_code","sessionKey":"synthetic-session","title":null,"folderLabel":null,"projectHint":null,"firstSeen":"2026-10-20T01:02:03.000Z","lastSeen":"2026-10-20T01:02:03.000Z"}],"seats":[]}';
  session(result).gitBranch = 'feature/example'; session(result).reposTouched = [{ name: 'alpha', events: 2 }];
  for (const config of [{}, { sendEvidence: false }, { sendEvidence: 'true' }]) assert.equal(JSON.stringify(reader.makeBatches(result.state, 'synthetic', now, config)[0].body), legacy);
  const body = reader.makeBatches(result.state, 'synthetic', now, { sendEvidence: true })[0].body;
  assert.equal(body.sessions[0].gitBranch, 'feature/example');
  assert.deepEqual(body.sessions[0].reposTouched, [{ name: 'alpha', events: 2 }]);
  delete body.sessions[0].gitBranch; delete body.sessions[0].reposTouched;
  assert.equal(JSON.stringify(body), legacy);
});

test('the wire boundary revalidates evidence and strips every nested extra field', t => {
  const home = fixture(t);
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  const result = reader.scan(options(home));
  Object.assign(session(result), { gitBranch: '/PRIVATE/path', reposTouched: [
    { name: 'valid', events: 3, file_path: '/PRIVATE/file.ts' }, { name: '/PRIVATE/path', events: 5 }, { name: 'PRIVATE\\path', events: 5 }, { name: '.', events: 1 }, { name: 'bad-count', events: 0 }, { name: 'bad-count', events: 1.5 }, null,
  ] });
  const body = reader.makeBatches(result.state, 'synthetic', now, { sendEvidence: true })[0].body;
  assert.equal(body.sessions[0].gitBranch, null);
  assert.deepEqual(body.sessions[0].reposTouched, [{ name: 'valid', events: 3 }]);
  assert.equal(JSON.stringify(body).includes('PRIVATE'), false);
});

test('sendEvidence is a strict boolean config option and session titles remain independent', t => {
  const home = fixture(t), cwd = repository(home, 'alpha');
  write(home, '.claude/projects/x/a.jsonl', [{ type: 'summary', summary: 'Synthetic title' }, toolReply('one', 'Read', { path: cwd + '/file.ts' }, { cwd })]);
  const config = write(home, 'config.json', { sendEvidence: true, sendSessionTitles: true, dryRun: true });
  const printed = spawnSync(process.execPath, [fileURLToPath(bundle), 'print-config', '--config', config], { encoding: 'utf8' });
  assert.equal(printed.status, 0, printed.stderr);
  assert.equal(JSON.parse(printed.stdout).sendEvidence, true);
  const executed = spawnSync(process.execPath, [fileURLToPath(bundle), 'backfill', '--since', since, '--until', until, '--config', config, '--json'], { encoding: 'utf8', env: { ...process.env, HOME: home, CLAUDE_CONFIG_DIR: home + '/.claude', CODEX_HOME: home + '/.codex' } });
  assert.equal(executed.status, 0, executed.stderr);
  assert.equal(JSON.parse(executed.stdout).sessions[0].title, 'Synthetic title');
  assert.deepEqual(JSON.parse(executed.stdout).sessions[0].reposTouched, [{ name: 'alpha', events: 1 }]);
  const result = reader.scan(evidenceOptions(home));
  assert.equal(session(result).title, null);
  assert.deepEqual(session(result).reposTouched, [{ name: 'alpha', events: 1 }]);
});

test('giant Claude content still projects tool paths at the tail without retaining text or edits', t => {
  const home = fixture(t), cwd = repository(home, 'alpha');
  const reply = claude('giant-evidence', { cwd });
  reply.message.content = [{ type: 'text', text: 'PRIVATE SENTINEL'.repeat(100_000) }, { type: 'tool_use', id: 'giant-call', name: 'Edit', input: { new_string: 'PRIVATE CODE'.repeat(100_000), file_path: cwd + '/PRIVATE-file.ts' } }];
  write(home, '.claude/projects/x/a.jsonl', [reply]);
  const result = reader.scan(evidenceOptions(home));
  assert.equal(totals(result.rows).requests, 1);
  assert.deepEqual(session(result).reposTouched, [{ name: 'alpha', events: 1 }]);
  assert.equal(JSON.stringify(result.state.evidence).includes('PRIVATE'), false);
  assert.ok(JSON.stringify(result.state).length < 20_000);
});

test('signed sends forward the sendEvidence setting to the actual body', async t => {
  const home = fixture(t), cwd = repository(home, 'alpha'), bodies = [];
  const server = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    bodies.push(JSON.parse(Buffer.concat(chunks).toString()));
    res.setHeader('content-type', 'application/json'); res.end('{"ok":true}');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const credentialsFrom = write(home, 'credentials.json', { uploadUrl: `http://127.0.0.1:${server.address().port}`, installKey: 'synthetic', uploadSigningSecret: 'synthetic', tenantId: 'synthetic' });
  write(home, '.claude/projects/x/a.jsonl', [toolReply('one', 'Read', { path: cwd + '/file.ts' }, { cwd })]);
  const { state } = reader.scan(evidenceOptions(home));
  await reader.sendPending(state, { credentialsFrom, sendEvidence: true }, { home, now });
  assert.deepEqual(bodies[0].sessions[0].reposTouched, [{ name: 'alpha', events: 1 }]);
  state.metadataPending = true;
  await reader.sendPending(state, { credentialsFrom, sendEvidence: false }, { home, now });
  assert.equal(Object.hasOwn(bodies[1].sessions[0], 'reposTouched'), false);
  assert.equal(Object.hasOwn(bodies[1].sessions[0], 'gitBranch'), false);
});

test('streamed state encoding matches JSON.stringify and compact deduplication survives reload', t => {
  const home = fixture(t), cwd = repository(home, 'alpha');
  const file = write(home, '.claude/projects/x/a.jsonl', [toolReply('two', 'Read', { path: cwd + '/two.ts' }, { cwd }), toolReply('one', 'Read', { path: cwd + '/one.ts' }, { cwd })]);
  const result = reader.scan(evidenceOptions(home));
  Object.values(result.state.files)[0].context.title = 'Synthetic "quotes", \\slashes and 汉字 🧪\n';
  result.state.sessions.extra = { tool: 'codex', sessionKey: 'synthetic-extra', title: null, folderLabel: null, projectHint: null, firstSeen: since, lastSeen: until, gitBranch: undefined, reposTouched: [{ name: 'alpha', events: 2 }] };
  const saved = path.join(home, 'state.json');
  reader.saveState(saved, result.state);
  assert.equal(fs.readFileSync(saved, 'utf8'), JSON.stringify(result.state));
  assert.ok(result.state.seenPacked && result.state.evidenceSeenPacked);
  assert.equal(Object.keys(result.state.seen).length, 0);
  fs.appendFileSync(file, JSON.stringify(toolReply('two', 'Read', { path: cwd + '/two.ts' }, { cwd })) + '\n');
  const resumed = reader.scan({ ...evidenceOptions(home), backfill: false, state: reader.readState(saved) });
  assert.equal(totals(resumed.rows).requests, 2);
  assert.deepEqual(session(resumed).reposTouched, [{ name: 'alpha', events: 2 }]);
});

test('giant Codex argument JSON and patch strings preserve paths after private content', t => {
  const home = fixture(t), cwd = repository(home, 'alpha');
  write(home, '.codex/sessions/rollout-giant-tools.jsonl', [meta('codex-session', { cwd }), context,
    { type: 'response_item', timestamp: since, payload: { type: 'function_call', name: 'Edit', call_id: 'giant-json', arguments: JSON.stringify({ new_string: 'PRIVATE CONTENT'.repeat(100_000), file_path: cwd + '/PRIVATE.ts' }) } },
    { type: 'response_item', timestamp: since, payload: { type: 'custom_tool_call', name: 'apply_patch', call_id: 'giant-patch', input: '*** Begin Patch\n*** Update File: ' + cwd + '/PRIVATE.ts\n@@\n+' + 'PRIVATE CODE'.repeat(100_000) + '\n*** Add File: ' + cwd + '/PRIVATE-new.ts\n+synthetic\n*** End Patch' } }, usage('one'),
  ]);
  const result = reader.scan(evidenceOptions(home));
  assert.equal(totals(result.rows).requests, 1);
  assert.deepEqual(session(result).reposTouched, [{ name: 'alpha', events: 2 }]);
  assert.equal(JSON.stringify(result.state.evidence).includes('PRIVATE'), false);
});

test('the standalone evidence-enabled incremental command preserves counts with existing state', t => {
  const home = fixture(t), cwd = repository(home, 'alpha');
  write(home, '.claude/projects/x/a.jsonl', [toolReply('one', 'Read', { path: cwd + '/file.ts' }, { cwd })]);
  const result = reader.scan(options(home)), dir = path.join(home, 'state');
  reader.saveState(path.join(dir, 'state.json'), result.state);
  const config = write(home, 'config.json', { sendEvidence: false, dryRun: true });
  const execute = () => spawnSync(process.execPath, [fileURLToPath(bundle), 'run', '--since', since, '--until', until, '--state-dir', dir, '--config', config, '--json'], { encoding: 'utf8', env: { ...process.env, NODE_OPTIONS: '', HOME: home, CLAUDE_CONFIG_DIR: home + '/.claude', CODEX_HOME: home + '/.codex' } });
  const off = execute();
  assert.equal(off.status, 0, off.stderr);
  assert.equal(Object.hasOwn(JSON.parse(off.stdout).sessions[0], 'reposTouched'), false);
  fs.writeFileSync(config, JSON.stringify({ sendEvidence: true, dryRun: true }));
  const migration = execute();
  assert.equal(migration.status, 0, migration.stderr);
  assert.deepEqual(JSON.parse(migration.stdout).sessions[0].reposTouched, [{ name: 'alpha', events: 1 }]);
  const output = execute();
  assert.equal(output.status, 0, output.stderr);
  const parsed = JSON.parse(output.stdout);
  assert.equal(totals(parsed.rows).requests, 1);
  assert.deepEqual(parsed.sessions[0].reposTouched, [{ name: 'alpha', events: 1 }]);
  assert.equal(parsed.run.bytesRead, 0);
  assert.equal(fs.existsSync(path.join(dir, 'reader.lock')), false);
  t.diagnostic(JSON.stringify({ incrementalPeakMemoryBytes: parsed.run.peakMemoryBytes, incrementalMilliseconds: parsed.run.milliseconds }));
});

test('Codex code-mode wrappers reveal literal working directories, shell paths and patch headers', t => {
  const home = fixture(t), alpha = repository(home, 'alpha'), beta = repository(home, 'beta');
  const wrapper = (id, input) => ({ type: 'response_item', timestamp: since, payload: { type: 'custom_tool_call', name: 'functions.exec', call_id: id, input } });
  write(home, '.codex/sessions/rollout-code-mode.jsonl', [meta('codex-session', { cwd: home + '/Projects' }), context,
    wrapper('shell', `text(await tools.exec_command({cmd: ${JSON.stringify('cat ' + beta + '/PRIVATE.ts')}, workdir: ${JSON.stringify(alpha)}}));`),
    wrapper('patch', `await tools.apply_patch(${JSON.stringify('*** Begin Patch\n*** Update File: ' + alpha + '/PRIVATE.ts\n@@\n+PRIVATE CODE\n*** End Patch')});`),
    wrapper('dynamic', 'await tools.exec_command({workdir: `${PRIVATE_DYNAMIC_PATH}`, cmd: "true"});'), usage('one'),
  ]);
  const result = reader.scan(evidenceOptions(home));
  assert.deepEqual(session(result).reposTouched, [{ name: 'alpha', events: 2 }, { name: 'beta', events: 1 }]);
  assert.equal(JSON.stringify(result.state.evidence).includes('PRIVATE'), false);
});

test('bounded asynchronous discovery matches synchronous capture across nested sources and aliases', async t => {
  const home = fixture(t), cwd = repository(home, 'alpha');
  const first = write(home, '.claude/projects/x/a.jsonl', [toolReply('one', 'Read', { path: cwd + '/file.ts' }, { cwd })]);
  write(home, '.claude/projects/x/nested/b.jsonl', [claude('two', { cwd })]);
  fs.symlinkSync(first, path.join(home, '.claude/projects/x/copy.jsonl'));
  fs.linkSync(first, path.join(home, '.claude/projects/x/hardlink.jsonl'));
  const old = write(home, '.claude/projects/x/old.jsonl', [claude('ignored-old')]);
  fs.utimesSync(old, 0, 0);
  write(home, '.codex/sessions/nested/rollout-a.jsonl', [meta('codex-session', { cwd }), context, usage('one')]);
  const sync = reader.scan(evidenceOptions(home));
  const asynchronous = await reader.runPass({ ...evidenceOptions(home), dryRun: true });
  assert.equal(asynchronous.stats.files, sync.stats.files);
  assert.deepEqual(asynchronous.rows, sync.rows);
  assert.deepEqual(reader.makeBatches(asynchronous.state, 'synthetic', now, { sendEvidence: true }), reader.makeBatches(sync.state, 'synthetic', now, { sendEvidence: true }));
});

test('calls without tool IDs use message identity and ordinal while copied messages deduplicate', t => {
  const home = fixture(t), cwd = repository(home, 'alpha');
  const call = { type: 'tool_use', name: 'Read', input: { file_path: cwd + '/file.ts' } };
  const first = claude('one', { cwd, message: { ...claude('one').message, content: [call, call] } });
  const second = claude('two', { cwd, message: { ...claude('two').message, content: [call] } });
  write(home, '.claude/projects/x/a.jsonl', [first, second]);
  write(home, '.claude/projects/x/copy.jsonl', [first]);
  assert.deepEqual(session(reader.scan(evidenceOptions(home))).reposTouched, [{ name: 'alpha', events: 3 }]);
});

test('evidence follows the cloud upload schema: case variants merge, names and branches the cloud refuses are dropped', () => {
  const state = reader.emptyState();
  const base = { tool: 'claude_code', title: null, folderLabel: null, projectHint: null, firstSeen: '2026-10-21T00:00:00.000Z', lastSeen: '2026-10-21T01:00:00.000Z' };
  state.sessions.a = { ...base, sessionKey: 'a', gitBranch: 'feature/' + 'x'.repeat(232), reposTouched: [
    { name: 'Alpha', events: 2 }, { name: 'alpha', events: 5 }, { name: 'with:colon', events: 9 }, { name: ' padded', events: 9 },
    { name: 'n'.repeat(161), events: 9 }, { name: 'n'.repeat(160), events: 1 },
  ] };
  state.sessions.b = { ...base, sessionKey: 'b', gitBranch: 'feature/' + 'x'.repeat(233), reposTouched: [] };
  state.metadataPending = true;
  const sessions = reader.makeBatches(state, 'synthetic', now, { sendEvidence: true })[0].body.sessions;
  const a = sessions.find(s => s.sessionKey === 'a'), b = sessions.find(s => s.sessionKey === 'b');
  assert.deepEqual(a.reposTouched, [{ name: 'alpha', events: 7 }, { name: 'n'.repeat(160), events: 1 }]);
  assert.equal(a.gitBranch.length, 240);
  assert.equal(b.gitBranch, null);
  assert.deepEqual(b.reposTouched, []);
});

test('files in folders where macOS can make a background job wait are never opened', t => {
  const home = fixture(t), inner = repository(home, 'Documents/inner', 'feature/private');
  write(home, 'Documents/worktree/.git', `gitdir: ${path.join(home, 'Projects/main/.git/worktrees/wt')}\n`);
  write(home, 'Documents/worktree/.conductor-project.json', JSON.stringify({ project: 'lane-project', schema: 'conductor-source-project/v1' }));
  fs.symlinkSync(path.join(home, 'Documents'), path.join(home, 'linked-documents'));
  const opened = [], real = { readFileSync: fs.readFileSync, openSync: fs.openSync };
  fs.readFileSync = function (p, ...rest) { opened.push(String(p)); return real.readFileSync.call(this, p, ...rest); };
  fs.openSync = function (p, ...rest) { opened.push(String(p)); return real.openSync.call(this, p, ...rest); };
  t.after(() => Object.assign(fs, real));
  const records = [
    toolReply('docs', 'Read', { file_path: inner + '/src/a.ts' }),
    toolReply('linked', 'Read', { file_path: path.join(home, 'linked-documents/inner/src/b.ts') }),
  ].map(r => Object.assign(r, { cwd: path.join(home, 'Documents/worktree'), sessionId: 'synthetic-session' }));
  write(home, '.claude/projects/x/a.jsonl', records);
  const result = reader.scan(evidenceOptions(home));
  Object.assign(fs, real);
  const documents = [path.join(home, 'Documents'), path.join(fs.realpathSync(home), 'Documents')];
  assert.deepEqual(opened.filter(p => documents.some(d => p.startsWith(d)) || p.includes('linked-documents')), []);
  assert.deepEqual(session(result).reposTouched, [{ name: 'inner', events: 2 }]);
  assert.equal(session(result).gitBranch, null);
  assert.equal(session(result).projectHint, null);
  assert.equal(Object.values(result.state.hours)[0].projectSource, 'none');
});

test('a collector config made large by its capture roots still supplies upload credentials', async t => {
  const home = fixture(t), bodies = [];
  const server = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    bodies.push(JSON.parse(Buffer.concat(chunks).toString()));
    res.setHeader('content-type', 'application/json'); res.end('{"ok":true}');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const captureRoots = Array.from({ length: 40_000 }, (_, i) => `/synthetic/lanes/${String(i).padStart(6, '0')}/work/projects`);
  const credentialsFrom = write(home, 'credentials.json', { uploadUrl: `http://127.0.0.1:${server.address().port}`, installKey: 'synthetic', uploadSigningSecret: 'synthetic', tenantId: 'synthetic', captureRoots });
  assert.ok(fs.statSync(credentialsFrom).size > 1024 * 1024);
  write(home, '.claude/projects/x/a.jsonl', [claude('one')]);
  const { state } = reader.scan(options(home));
  assert.equal(await reader.sendPending(state, { credentialsFrom }, { home, now }), 1);
  assert.equal(state.lastError, null);
  assert.equal(bodies.length, 1);
});
