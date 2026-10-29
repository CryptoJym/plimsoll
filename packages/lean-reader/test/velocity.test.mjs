import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const reader = await import(new URL('../dist/plimsoll-lean.mjs', import.meta.url));
const window = { since: '2026-10-20T00:00:00Z', until: '2026-10-21T00:00:00Z', now: Date.parse('2026-10-21T00:00:00Z'), backfill: true, config: { sendVelocity: true } };
const fixture = () => fs.mkdtempSync(path.join(os.tmpdir(), 'lean-velocity-'));
const write = (home, relative, rows) => { const file = path.join(home, relative); fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, rows.map(row => JSON.stringify(row)).join('\n') + '\n'); return file; };

test('velocity preserves hour totals and records tier, effort, reasoning and reply timing', t => {
  const home = fixture(); t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  write(home, '.claude/projects/demo/replies.jsonl', [
    { type: 'user', timestamp: '2026-10-20T00:00:00Z', sessionId: 'claude-session', message: { role: 'user', content: 'prompt' } },
    { type: 'assistant', timestamp: '2026-10-20T00:00:02Z', sessionId: 'claude-session', entrypoint: 'CLI', effort: 'MAX', message: { id: 'claude-reply', model: 'claude-test', content: [{ type: 'text', text: 'reply' }], usage: { service_tier: 'Priority', input_tokens: 10, cache_read_input_tokens: 20, cache_creation_input_tokens: 3, output_tokens: 100, output_tokens_details: { thinking_tokens: 7 } } } },
  ]);
  write(home, '.codex/sessions/rollout-codex.jsonl', [
    { type: 'session_meta', timestamp: '2026-10-20T00:00:00Z', payload: { id: 'codex-session', creator_account_id: 'codex-account', originator: 'CLI' } },
    { type: 'event_msg', timestamp: '2026-10-20T00:00:01Z', payload: { type: 'thread_settings_applied', thread_settings: { service_tier: 'priority', reasoning_effort: 'max', model: 'gpt-test' } } },
    { type: 'event_msg', timestamp: '2026-10-20T00:00:02Z', payload: { type: 'task_started' } },
    { type: 'response_item', timestamp: '2026-10-20T00:00:03Z', payload: { type: 'function_call', name: 'apply_patch' } },
    { type: 'response_item', timestamp: '2026-10-20T00:00:04Z', payload: { type: 'function_call_output' } },
    { type: 'token_usage_record', timestamp: '2026-10-20T00:00:06Z', payload: { response_id: 'codex-reply', usage: { input_tokens: 10, cached_input_tokens: 2, cache_write_input_tokens: 1, output_tokens: 100, reasoning_output_tokens: 8 } } },
    { type: 'event_msg', timestamp: '2026-10-20T00:00:07Z', payload: { type: 'task_complete', duration_ms: 5000, time_to_first_token_ms: 500 } },
  ]);
  const result = reader.scan({ ...window, home, env: {} });
  const velocity = Object.values(result.state.velocity);
  assert.equal(result.rows.reduce((sum, row) => sum + row.requests, 0), velocity.reduce((sum, row) => sum + row.replies, 0));
  assert.deepEqual(velocity.map(row => [row.tool, row.tier, row.effort, row.timed]).sort(), [['claude_code', 'priority', 'max', 1], ['codex', 'priority', 'max', 1]]);
  assert.deepEqual(velocity.find(row => row.tool === 'claude_code'), { ...velocity.find(row => row.tool === 'claude_code'), reasoning: 7, sDur: 2, sOut: 100, sOutDur: 200, sOut2: 10000, sDur2: 4 });
  const session = Object.values(result.state.sessions).find(row => row.tool === 'codex');
  assert.equal(session.actions.edit, 1);
  assert.equal(session.entrypoint, 'cli');
});

test('velocity-only backfill leaves the existing hours and session totals untouched', t => {
  const home = fixture(); t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  write(home, '.claude/projects/demo/replies.jsonl', [{ type: 'user', timestamp: '2026-10-20T00:00:00Z', sessionId: 's', message: { role: 'user', content: 'x' } }, { type: 'assistant', timestamp: '2026-10-20T00:00:02Z', sessionId: 's', message: { id: 'r', model: 'm', usage: { input_tokens: 1, output_tokens: 100 } } }]);
  const first = reader.scan({ ...window, home, env: {} });
  const hours = JSON.stringify(first.state.hours), sessions = JSON.stringify(first.state.sessions);
  const replay = reader.scan({ ...window, home, env: {}, state: JSON.parse(JSON.stringify(first.state)), velocityOnly: true });
  assert.equal(JSON.stringify(replay.state.hours), hours);
  assert.equal(JSON.stringify(replay.state.sessions), sessions);
  const batch = reader.makeBatches(replay.state, 'tenant', window.now, { sendVelocity: true }, true)[0].body;
  assert.equal(batch.hours.length, 0);
  assert.equal(batch.sessions.length, 0);
  assert.equal(batch.seats.length, 0);
  assert.equal(batch.velocity.length, 1);
});

test('velocity replay reads archived Codex zstd rollouts without changing legacy rows', t => {
  const home = fixture(); t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  const source = write(home, '.codex/archived_sessions/rollout-2026-10-20T00-00-00-zst.jsonl', [
    { type: 'session_meta', timestamp: '2026-10-20T00:00:00Z', payload: { id: 'zst-session', originator: 'cli' } },
    { type: 'event_msg', timestamp: '2026-10-20T00:00:01Z', payload: { type: 'task_started' } },
    { type: 'token_usage_record', timestamp: '2026-10-20T00:00:03Z', payload: { response_id: 'zst-reply', usage: { input_tokens: 10, output_tokens: 100 } } },
  ]);
  const compressed = source + '.zst', zstd = spawnSync('zstd', ['-q', '-f', source, '-o', compressed]);
  if (zstd.status !== 0) { t.skip('zstd CLI unavailable'); return; }
  fs.unlinkSync(source);
  const discovered = [{ tool: 'codex', path: path.join(home, '.codex', 'sessions') }, { tool: 'codex', path: path.join(home, '.codex', 'archived_sessions') }];
  const ordinary = reader.scan({ ...window, home, env: {}, discovered });
  assert.equal(ordinary.stats.files, 0, 'ordinary passes must not admit archived zstd rollouts');
  const replay = reader.scan({ ...window, home, env: {}, velocityOnly: true, discovered });
  const rows = Object.values(replay.state.velocity);
  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0], { ...rows[0], replies: 1, output: 100, model: 'unknown', sessionKey: 'zst-session' });
  assert.equal(replay.stats.files, 1);
});

test('velocity replay skips configured roots in macOS protected folders', t => {
  const home = fixture(); t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  write(home, 'Documents/codex/archived_sessions/rollout-2026-10-20T00-00-00.jsonl', [
    { type: 'session_meta', timestamp: '2026-10-20T00:00:00Z', payload: { id: 'protected-session' } },
    { type: 'token_usage_record', timestamp: '2026-10-20T00:00:01Z', payload: { response_id: 'protected-reply', usage: { input_tokens: 1, output_tokens: 1 } } },
  ]);
  const replay = reader.scan({ ...window, home, env: {}, velocityOnly: true, discovered: [{ tool: 'codex', path: path.join(home, 'Documents', 'codex') }] });
  assert.equal(replay.stats.files, 0);
  assert.equal(Object.keys(replay.state.velocity).length, 0);
});
