import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { reader, window, fixture, write, restart, total } from './review-fixture.mjs';

const prefix = [
  { type: 'session_meta', payload: { id: 'synthetic-session' } },
  { type: 'turn_context', payload: { model: 'synthetic-model' } },
  { type: 'event_msg', timestamp: window.since, payload: { type: 'task_started' } },
  { type: 'token_count', timestamp: window.since, payload: { info: { total_token_usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 } } } },
];
const fallback = { type: 'token_count', timestamp: '2026-10-20T00:00:02Z', payload: { info: { total_token_usage: { input_tokens: 100, output_tokens: 100, total_tokens: 200 } } } };
const named = { type: 'token_usage_record', timestamp: '2026-10-20T00:00:02Z', payload: { response_id: 'synthetic-reply', usage: { input_tokens: 100, output_tokens: 100 } } };
const turn = { type: 'event_msg', timestamp: '2026-10-20T00:00:03Z', payload: { type: 'task_complete', duration_ms: 3000, time_to_first_token_ms: 100 } };
const append = (file, rows) => fs.appendFileSync(file, rows.map(row => JSON.stringify(row)).join('\n') + '\n');

for (const position of ['unread-fallback', 'already-counted-fallback', 'named-after-replay', 'named-outside-window', 'ordinary-backfill']) {
  test(`normal catch-up preserves replayed velocity and named replacement: ${position}`, t => {
    const home = fixture(t), file = write(home, '.codex/sessions/rollout-synthetic.jsonl', prefix);
    if (position === 'already-counted-fallback') append(file, [fallback]);
    let state = reader.scan({ ...window, home }).state;
    if (position !== 'already-counted-fallback') append(file, [fallback]);
    const replacement = position === 'named-outside-window' ? { ...named, timestamp: '2026-10-20T01:00:02Z' } : named;
    if (position !== 'named-after-replay') append(file, [replacement, turn]);
    state = restart(home, reader.scan({ ...window, home, state, velocityOnly: true, until: '2026-10-20T01:00:00Z' }).state);
    if (position === 'named-after-replay') append(file, [replacement, turn]);
    state = reader.scan({ ...window, home, state, backfill: position === 'ordinary-backfill' }).state;
    assert.equal(total(state, 'output'), 100);
    assert.equal(total(state, 'output', 'velocity'), 100);
    assert.equal(total(state, 'replies', 'velocity'), 1);
    assert.equal(total(state, 'turns', 'velocity'), 1);
    assert.equal(Object.keys(state.velocityReplays ?? {}).length, 0);
  });
}
