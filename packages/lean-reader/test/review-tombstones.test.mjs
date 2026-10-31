import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { reader, window, fixture, write, reply } from './review-fixture.mjs';

for (const velocityOnly of [false, true]) {
  test(`a removed transcript queues a zero velocity replacement, replay=${velocityOnly}`, t => {
    const home = fixture(t), file = write(home, '.claude/projects/synthetic/a.jsonl', [reply('removed')]);
    const state = reader.scan({ ...window, home }).state;
    const [key] = Object.keys(state.velocity);
    state.pending = {}; state.velocityPending = {}; state.metadataPending = false;
    fs.writeFileSync(file, '');
    const result = reader.scan({ ...window, home, state, backfill: true, velocityOnly });
    assert.equal(result.state.velocity[key].replies, 0);
    assert.equal(result.state.velocityPending[key], true);
    const batches = reader.makeBatches(result.state, 'synthetic-tenant', window.now, window.config, velocityOnly);
    const rows = batches.flatMap(batch => batch.body.velocity ?? []);
    assert.equal(rows.length, 1);
    for (const field of ['replies', 'freshInput', 'cacheRead', 'cacheWrite', 'output', 'reasoning', 'timed', 'sOut', 'sDur', 'sOutDur', 'sOut2', 'sDur2', 'turns', 'turnMs', 'ttftMs', 'ttftN']) assert.equal(rows[0][field], 0);
    assert.deepEqual(rows[0].hist, Array(16).fill(0));
  });
}
