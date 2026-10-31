import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { reader, window, fixture, write, restart, total } from './review-fixture.mjs';

const stamp = second => `2026-10-20T00:00:0${second}Z`;
const count = (n, second) => ({ type: 'event_msg', timestamp: stamp(second), payload: { type: 'token_count', info: { total_token_usage: { input_tokens: n, cached_input_tokens: n / 10, cache_write_input_tokens: n / 20, output_tokens: n, reasoning_output_tokens: n / 4, total_tokens: n * 2 } } } });
const prefix = [
  { type: 'session_meta', payload: { id: 'synthetic-session' } },
  { type: 'turn_context', payload: { model: 'synthetic-model' } },
  { type: 'event_msg', timestamp: stamp(0), payload: { type: 'task_started' } },
  count(0, 0), count(100, 2),
];
const named = { type: 'token_usage_record', timestamp: stamp(2), payload: { response_id: 'synthetic-reply', usage: { input_tokens: 100, cached_input_tokens: 10, cache_write_input_tokens: 5, output_tokens: 100, reasoning_output_tokens: 25 } } };

for (const mode of ['same-pass', 'restart', 'legacy-restart', 'velocity-replay']) {
  test(`named usage retracts every provisional velocity metric: ${mode}`, t => {
    const home = fixture(t), file = write(home, '.codex/sessions/rollout-synthetic.jsonl', prefix);
    let state;
    if (mode.includes('restart')) {
      state = reader.scan({ ...window, home }).state;
      if (mode === 'legacy-restart') for (const tracked of Object.values(state.files)) {
        delete tracked.velocityFallback; delete tracked.fallbackLastReplyAt;
      }
      state = restart(home, state);
    }
    fs.appendFileSync(file, JSON.stringify(named) + '\n');
    state = reader.scan({ ...window, home, state, velocityOnly: mode === 'velocity-replay' }).state;
    if (mode !== 'velocity-replay') assert.equal(total(state, 'output'), 100);
    assert.equal(total(state, 'output', 'velocity'), 100);
    assert.equal(total(state, 'replies', 'velocity'), 1);
    const [row] = Object.values(state.velocity);
    assert.deepEqual([row.freshInput, row.cacheRead, row.cacheWrite, row.reasoning], [85, 10, 5, 25]);
    assert.deepEqual([row.timed, row.sOut, row.sDur, row.sOutDur, row.sOut2, row.sDur2], [1, 100, 2, 200, 10000, 4]);
    assert.equal(row.hist.reduce((sum, value) => sum + value, 0), 1);
  });
}

test('fallback retraction preserves another source in the same velocity row during backfill', t => {
  const home = fixture(t);
  write(home, '.codex/sessions/rollout-a.jsonl', [...prefix.slice(0, 3), { ...named, payload: { ...named.payload, response_id: 'synthetic-other' } }]);
  const file = write(home, '.codex/sessions/rollout-z.jsonl', prefix);
  let state = reader.scan({ ...window, home }).state;
  fs.appendFileSync(file, JSON.stringify(named) + '\n');
  state = reader.scan({ ...window, home, state: restart(home, state), backfill: true }).state;
  assert.equal(total(state, 'output'), 200);
  assert.equal(total(state, 'output', 'velocity'), 200);
  assert.equal(total(state, 'timed', 'velocity'), 2);
});
