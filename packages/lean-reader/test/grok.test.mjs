import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import * as reader from '../dist/plimsoll-lean.mjs';
import { since, until, sessionId, options, usage, turn, modelTurn, write, session, expectedRow, totals } from './grok-fixtures.mjs';

function fixture(t) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'lean-grok-'));
  t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  return home;
}
const persisted = result => JSON.parse(JSON.stringify(result.state));

test('Grok per-turn usage sums by UTC hour, model and session', t => {
  const home = fixture(t);
  const one = turn('synthetic-one'), two = turn('synthetic-two');
  two.params.update.usage = usage({ inputTokens: 50, cachedReadTokens: 10, outputTokens: 5, modelCalls: 2 });
  const nextHour = turn('synthetic-next', { timestamp: Date.parse('2026-10-19T20:15:00-06:00') / 1000 });
  const otherSession = turn('synthetic-session-two');
  otherSession.params.sessionId = '00000000-0000-4000-8000-000000000002';
  session(home, [one, two, nextHour, otherSession]);
  assert.deepEqual(reader.scan(options(home)).rows, [
    expectedRow({ requests: 5, freshInput: 60, cacheRead: 110, cacheWrite: 14, output: 14 }),
    expectedRow({ sessionKey: otherSession.params.sessionId }),
    expectedRow({ hour: '2026-10-20T02:00:00.000Z' }),
  ]);
});

test('Grok modelUsage splits one turn, ignores top-level totals and strips only the -build suffix', t => {
  const home = fixture(t);
  session(home, [modelTurn('synthetic-two-models', {
    'grok-4.7-build': usage(), 'grok-4.6-build': usage({ inputTokens: 80, cachedReadTokens: 30, modelCalls: 2 }),
    'grok-4.7-build-preview': usage({ inputTokens: 10, cachedReadTokens: 0, modelCalls: 1 }),
  })]);
  assert.deepEqual(reader.scan(options(home)).rows, [
    expectedRow(), expectedRow({ model: 'grok-4.6', requests: 2, freshInput: 50, cacheRead: 30 }),
    expectedRow({ model: 'grok-4.7-build-preview', requests: 1, freshInput: 10, cacheRead: 0 }),
  ]);
});

test('Grok falls back to summary current_model_id and counts old turns without modelCalls once', t => {
  const home = fixture(t), old = turn('synthetic-old');
  delete old.params.update.usage.modelCalls;
  const oldModel = usage(); delete oldModel.modelCalls;
  session(home, [old, modelTurn('synthetic-old-model', { 'grok-4.6-build': oldModel })], { summary: { current_model_id: 'grok-4.6-build' } });
  assert.deepEqual(reader.scan(options(home)).rows, [expectedRow({ model: 'grok-4.6', requests: 2, freshInput: 40, cacheRead: 200, cacheWrite: 14, output: 18 })]);
});

test('Grok preserves an explicit zero modelCalls and clamps fresh input without subtracting cache writes', t => {
  const home = fixture(t), record = turn('synthetic-zero');
  record.params.update.usage = usage({ inputTokens: 2, cachedReadTokens: 3, cacheCreationTokens: 10, modelCalls: 0 });
  session(home, [record]);
  assert.deepEqual(reader.scan(options(home)).rows, [expectedRow({ requests: 0, freshInput: 0, cacheRead: 3, cacheWrite: 10 })]);
});

test('Grok eventId deduplication survives reread, append, partial lines, replacement, replay and saved state', t => {
  const home = fixture(t), first = modelTurn('synthetic-event', { 'grok-4.7-build': usage(), 'grok-4.6-build': usage() });
  const file = session(home, [first, first]);
  let result = reader.scan(options(home));
  assert.equal(totals(result.rows).requests, 6);
  const resume = () => result = reader.scan({ ...options(home), backfill: false, state: persisted(result) });
  resume(); assert.equal(result.stats.bytesRead, 0);
  // Replayed event with a new model, time and session is still the same event.
  const replay = modelTurn('synthetic-event', { 'grok-4.5-build': usage() }, { timestamp: Date.parse(until) / 1000 - 1 });
  replay.params.sessionId = 'synthetic-replayed-session';
  const next = JSON.stringify(turn('synthetic-new'));
  fs.appendFileSync(file, JSON.stringify(replay) + '\n' + next.slice(0, 70));
  resume(); assert.equal(totals(result.rows).requests, 6);
  fs.appendFileSync(file, next.slice(70) + '\n');
  resume(); assert.equal(totals(result.rows).requests, 9);
  fs.writeFileSync(file, JSON.stringify(first) + '\n');
  resume(); assert.equal(totals(result.rows).requests, 9);
  fs.renameSync(file, file + '.old');
  fs.writeFileSync(file, JSON.stringify(first) + '\n' + JSON.stringify(turn('synthetic-new')) + '\n');
  session(home, [first, turn('synthetic-new')], { root: '.grok-replay' });
  const stateFile = path.join(home, 'state.json'); reader.saveState(stateFile, result.state);
  result = reader.scan({ ...options(home), backfill: false, state: reader.readState(stateFile) });
  assert.equal(totals(result.rows).requests, 9);
  result = reader.scan({ ...options(home), state: persisted(result) });
  assert.equal(totals(result.rows).requests, 9);
  assert.equal(result.rows.some(r => r.model === 'grok-4.5'), false);
});

test('Grok distinct eventIds count even when prompt IDs, timestamps and usage match', t => {
  const home = fixture(t);
  session(home, [turn('synthetic-distinct-a'), turn('synthetic-distinct-b')]);
  assert.equal(totals(reader.scan(options(home)).rows).requests, 6);
});

test('Grok sync and async discovery cover default, wildcard, GROK_HOME, configured roots and aliases', async t => {
  const home = fixture(t);
  for (const [i, root] of ['.grok', '.grok-seat-a', '.grok-arbitrary', 'env-profile', 'explicit-profile'].entries()) {
    session(home, [turn('synthetic-root-' + i)], { root });
  }
  fs.symlinkSync(path.join(home, '.grok'), path.join(home, '.grok-alias'));
  fs.symlinkSync(path.join(home, '.grok/sessions'), path.join(home, '.grok/sessions/loop'));
  const args = { ...options(home), env: { GROK_HOME: path.join(home, 'env-profile') }, config: { roots: [{ tool: 'grok', path: '~/explicit-profile/sessions' }, '~/.grok/sessions'] } };
  const sync = reader.scan(args), asyncResult = await reader.runPass({ ...args, dryRun: true });
  assert.equal(totals(sync.rows).requests, 15);
  assert.deepEqual(asyncResult.rows, sync.rows);
  assert.equal(sync.stats.files, 5);
  assert.equal(asyncResult.stats.files, 5);
});

test('Grok never opens chat_history, events, subagent metadata or other JSONL files', async t => {
  const home = fixture(t), file = session(home, [turn('synthetic-only-updates')]), folder = path.dirname(file);
  for (const name of ['chat_history.jsonl', 'events.jsonl', 'signals.jsonl', 'other.jsonl', 'subagents/meta.json']) write(home, path.relative(home, path.join(folder, name)), [turn('synthetic-forbidden-' + name)]);
  const opened = [], real = { openSync: fs.openSync, readFileSync: fs.readFileSync };
  fs.openSync = function (file, ...args) { opened.push(String(file)); return real.openSync.call(this, file, ...args); };
  fs.readFileSync = function (file, ...args) { opened.push(String(file)); return real.readFileSync.call(this, file, ...args); };
  t.after(() => Object.assign(fs, real));
  const args = { ...options(home), config: { sendEvidence: true } };
  assert.deepEqual(reader.scan(args).rows, [expectedRow()]);
  assert.deepEqual((await reader.runPass({ ...args, dryRun: true })).rows, [expectedRow()]);
  const sourceReads = opened.filter(f => f.includes(path.join(home, '.grok') + path.sep));
  assert.ok(sourceReads.length > 0);
  assert.ok(sourceReads.every(f => ['auth.json', 'summary.json', 'updates.jsonl'].includes(path.basename(f))), sourceReads.join('\n'));
});

test('Grok auth projects only user_id; credentials and summary prose never enter parsed JSON, state or uploads', t => {
  const home = fixture(t), user = 'synthetic-user-only';
  const secret = 'SYNTHETIC_SECRET_SENTINEL_' + 'x'.repeat(150_000);
  write(home, '.grok/auth.json', { 'https://auth.x.ai::00000000-0000-4000-8000-000000000099': { key: secret, refresh_token: secret, user_id: user } });
  session(home, [turn('synthetic-identity')], { summary: { current_model_id: 'grok-4.7-build', summary: secret, title: secret, info: { cwd: path.join(home, 'projects/example') } } });
  const parse = JSON.parse;
  JSON.parse = function (text, ...args) { assert.equal(String(text).includes('SYNTHETIC_SECRET_SENTINEL'), false); return parse.call(this, text, ...args); };
  t.after(() => { JSON.parse = parse; });
  const result = reader.scan({ ...options(home), config: { sendSessionTitles: true, sendEvidence: true } });
  const hash = 'sha256:' + crypto.createHash('sha256').update(JSON.stringify({ stringValue: user })).digest('hex').slice(0, 16);
  assert.equal(result.rows[0].accountKey, hash);
  const serialized = JSON.stringify([result.state, reader.makeBatches(result.state, 'synthetic-tenant', Date.parse(until))]);
  for (const marker of [user, 'SYNTHETIC_SECRET_SENTINEL', 'refresh_token', 'auth.x.ai::']) assert.equal(serialized.includes(marker), false);
  assert.equal(Object.values(result.state.sessions)[0].title, null);
});

test('Grok absent, malformed, wrong-issuer or ambiguous identity remains grok:unknown', t => {
  const home = fixture(t);
  const identities = [undefined, '{bad', { 'https://auth.x.ai::synthetic': { key: 'synthetic-unused' } }, { 'https://other.invalid::synthetic': { user_id: 'synthetic-wrong' } },
    { 'https://auth.x.ai::synthetic-a': { user_id: 'synthetic-a' }, 'https://auth.x.ai::synthetic-b': { user_id: 'synthetic-b' } }];
  for (const [i, auth] of identities.entries()) {
    const root = '.grok-identity-' + i;
    session(home, [turn('synthetic-identity-' + i)], { root });
    if (auth !== undefined) write(home, root + '/auth.json', auth);
  }
  assert.ok(reader.scan(options(home)).rows.every(r => r.accountKey === 'grok:unknown'));
});

test('Grok summary supplies folder labels and session times, including metadata-only updates', t => {
  const home = fixture(t), file = session(home, [turn('synthetic-metadata')]);
  let result = reader.scan(options(home)), info = Object.values(result.state.sessions)[0];
  assert.equal(info.folderLabel, 'projects/example');
  assert.equal(info.firstSeen, '2026-10-20T00:00:00.000Z');
  assert.equal(info.lastSeen, '2026-10-20T03:00:00.000Z');
  write(home, path.relative(home, path.join(path.dirname(file), 'summary.json')), { info: { cwd: path.join(home, 'projects/example') }, created_at: since, updated_at: '2026-10-21T01:00:00Z', current_model_id: 'grok-4.6-build' });
  result = reader.scan({ ...options(home), backfill: false, state: persisted(result), config: { sendFolderLabels: false } });
  info = Object.values(result.state.sessions)[0];
  assert.equal(info.lastSeen, '2026-10-21T01:00:00.000Z');
  assert.equal(info.folderLabel, null);
  assert.deepEqual(result.rows, [expectedRow()]);
  assert.equal(result.stats.bytesRead, 0);
});

test('Grok launch tags and folder markers obey existing opt-in attribution and wire rules', t => {
  const home = fixture(t);
  session(home, [turn('synthetic-tag')]);
  write(home, 'projects/.conductor-project.json', { schema: 'conductor-source-project/v1', project: 'Synthetic project' });
  const args = { ...options(home), config: { sendEvidence: true } };
  let result = reader.scan(args);
  assert.equal(result.rows[0].projectHint, 'Synthetic project');
  write(home, '.local/state/plimsoll-lean/launch-tags.jsonl', [{ tool: 'grok', sessionKey: sessionId, project: 'Synthetic tag', at: until }]);
  result = reader.scan({ ...args, backfill: false, state: persisted(result) });
  assert.equal(result.rows[0].projectHint, 'Synthetic tag');
  const body = reader.makeBatches(result.state, 'synthetic-tenant', Date.parse(until))[0].body;
  assert.equal(body.hours[0].tool, 'grok');
  assert.equal(body.hours[0].model, 'grok-4.7');
  assert.equal(body.sessions[0].folderLabel, null);
});

test('Grok protected roots, metadata links and update links are never opened in either discovery path', async t => {
  const home = fixture(t), protectedRoots = ['Desktop', 'Documents', 'Downloads', 'Movies', 'Music', 'Pictures', 'Library/Mobile Documents', 'Library/CloudStorage'];
  const roots = protectedRoots.map((folder, i) => { session(home, [turn('synthetic-protected-' + i)], { root: folder + '/grok' }); return { tool: 'grok', path: path.join(home, folder, 'grok/sessions') }; });
  const file = session(home, [turn('synthetic-safe')]);
  fs.symlinkSync(path.join(home, 'Documents/grok'), path.join(home, '.grok-protected'));
  fs.unlinkSync(path.join(path.dirname(file), 'summary.json'));
  fs.symlinkSync(path.join(home, 'Documents/grok/sessions', encodeURIComponent('/synthetic/project'), sessionId, 'summary.json'), path.join(path.dirname(file), 'summary.json'));
  write(home, 'Documents/grok/auth.json', { 'https://auth.x.ai::synthetic-protected': { user_id: 'synthetic-protected' } });
  fs.symlinkSync(path.join(home, 'Documents/grok/auth.json'), path.join(home, '.grok/auth.json'));
  const alias = path.join(home, '.grok/sessions/alias'); fs.mkdirSync(alias);
  fs.symlinkSync(path.join(home, 'Documents/grok/sessions', encodeURIComponent('/synthetic/project'), sessionId, 'updates.jsonl'), path.join(alias, 'updates.jsonl'));
  const real = fs.openSync, opened = [];
  fs.openSync = function (file, ...args) { opened.push(String(file)); return real.call(this, file, ...args); };
  t.after(() => { fs.openSync = real; });
  const args = { ...options(home), config: { roots: [...roots, { tool: 'grok', path: '/Volumes/synthetic-never-open' }] } };
  assert.deepEqual(reader.scan(args).rows, [expectedRow({ model: 'unknown' })]);
  assert.deepEqual((await reader.runPass({ ...args, dryRun: true })).rows, [expectedRow({ model: 'unknown' })]);
  assert.equal(opened.some(f => protectedRoots.some(folder => f.startsWith(path.join(home, folder) + path.sep)) || f.startsWith('/Volumes/')), false);
  for (const blockedPath of [path.join(path.dirname(file), 'summary.json'), path.join(home, '.grok/auth.json'), path.join(alias, 'updates.jsonl')]) assert.equal(opened.includes(blockedPath), false);
});

test('Grok ignores incomplete, malformed and non-turn records and honors UTC window boundaries', t => {
  const home = fixture(t), wrong = turn('synthetic-wrong'); wrong.params.update.sessionUpdate = 'agent_message_chunk';
  const missing = turn('synthetic-missing'); delete missing.params._meta.eventId;
  const edge = turn('synthetic-edge', { timestamp: Date.parse(since) / 1000 });
  const file = session(home, [wrong, missing, turn('synthetic-before', { timestamp: Date.parse(since) / 1000 - 1 }), edge, turn('synthetic-after', { timestamp: Date.parse(until) / 1000 })]);
  fs.appendFileSync(file, '{bad json}\n' + JSON.stringify(turn('synthetic-partial')).slice(0, 80));
  assert.deepEqual(reader.scan(options(home)).rows, [expectedRow({ hour: '2026-10-20T00:00:00.000Z' })]);
});

test('Grok streams large update records without retaining prose or losing usage at the tail', t => {
  const home = fixture(t), record = modelTurn('synthetic-large', { 'grok-4.7-build': usage() });
  const file = session(home, []);
  const prefix = '{"ignored":"' + 'x'.repeat(2 * 1024 * 1024) + 'SYNTHETIC_PRIVATE_TEXT",';
  fs.writeFileSync(file, prefix + JSON.stringify(record).slice(1) + '\n' + prefix + '"invalid":"\\q"}\n');
  const result = reader.scan(options(home));
  assert.deepEqual(result.rows, [expectedRow()]);
  assert.equal(JSON.stringify(result.state).includes('SYNTHETIC_PRIVATE_TEXT'), false);
  assert.ok(JSON.stringify(result.state).length < 20_000);
});

test('Grok rejects over-budget model maps without keeping partial usage', t => {
  const home = fixture(t), models = {};
  for (let i = 0; i < 2000; i++) models['synthetic-model-' + i] = usage();
  session(home, [modelTurn('synthetic-over-budget', models), turn('synthetic-after-budget')]);
  assert.deepEqual(reader.scan(options(home)).rows, [expectedRow()]);
});

test('Grok discovers 2200 synthetic sessions and counts 1800 usage turns once', t => {
  const home = fixture(t);
  for (let i = 0; i < 2200; i++) {
    const id = 'synthetic-scale-' + i, record = turn(id); record.params.sessionId = id;
    session(home, i < 1800 ? [record, record] : [], { id });
  }
  const result = reader.scan(options(home));
  assert.equal(result.stats.files, 2200);
  assert.equal(result.rows.length, 1800);
  assert.equal(totals(result.rows).requests, 5400);
  assert.equal(totals(result.rows).freshInput, 36_000);
  assert.ok(Buffer.byteLength(JSON.stringify(result.state)) < 9 * 1024 * 1024);
});

test('Grok standalone --dry-run uses a synthetic GROK_HOME and prints exact rows without upload credentials', t => {
  const home = fixture(t);
  session(home, [modelTurn('synthetic-cli', { 'grok-4.7-build': usage(), 'grok-4.6-build': usage({ modelCalls: 1 }) })], { root: 'cli-profile' });
  const config = write(home, 'config.json', { credentialsFrom: path.join(home, 'absent-credentials.json') });
  const result = spawnSync(process.execPath, [new URL('../dist/plimsoll-lean.mjs', import.meta.url).pathname, 'backfill', '--since', since, '--until', until, '--dry-run', '--json', '--config', config, '--state-dir', path.join(home, 'cli-state')], {
    encoding: 'utf8', env: { ...process.env, HOME: home, GROK_HOME: path.join(home, 'cli-profile'), CODEX_HOME: '', CLAUDE_CONFIG_DIR: '' },
  });
  assert.equal(result.status, 0, result.stderr);
  const value = JSON.parse(result.stdout);
  assert.deepEqual(value.rows, [expectedRow(), expectedRow({ model: 'grok-4.6', requests: 1 })]);
  assert.equal(value.run.sent, 0);
  assert.equal(value.run.errors, 0);
  assert.equal(value.run.files, 1);
  assert.equal(value.sessions[0].folderLabel, 'projects/example');
});

test('Grok explicit config roots survive print-config and GROK_HOME is preserved in the LaunchAgent', t => {
  const home = fixture(t), config = write(home, 'config.json', { roots: [{ tool: 'grok', path: '~/custom/sessions' }] });
  const cli = new URL('../dist/plimsoll-lean.mjs', import.meta.url).pathname;
  const result = spawnSync(process.execPath, [cli, 'print-config', '--config', config], { encoding: 'utf8', env: { ...process.env, HOME: home } });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout).roots, [{ tool: 'grok', path: '~/custom/sessions' }]);
  const previous = process.env.GROK_HOME;
  try { process.env.GROK_HOME = '/synthetic/grok&profile'; assert.ok(reader.launchAgent(home, cli).includes('<key>GROK_HOME</key><string>/synthetic/grok&amp;profile</string>')); }
  finally { if (previous === undefined) delete process.env.GROK_HOME; else process.env.GROK_HOME = previous; }
});

test('Grok counts turns whose method carries the vendor prefix or none, and skips other methods', t => {
  const home = fixture(t);
  const prefixed = turn('synthetic-prefixed'), bare = turn('synthetic-bare', { method: 'session/update' });
  const other = turn('synthetic-other', { method: '_x.ai/session/notify' });
  session(home, [prefixed, bare, other]);
  assert.deepEqual(reader.scan(options(home)).rows, [expectedRow({ requests: 6, freshInput: 40, cacheRead: 200, cacheWrite: 14, output: 18 })]);
});
