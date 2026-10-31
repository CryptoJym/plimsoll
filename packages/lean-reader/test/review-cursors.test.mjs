import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { reader, window, fixture, write, restart, reply, total } from './review-fixture.mjs';

test('velocity replay preserves saved usage cursors and replies outside its window', t => {
  const home = fixture(t), file = write(home, '.claude/projects/synthetic/a.jsonl', [reply('first')]);
  let state = restart(home, reader.scan({ ...window, home }).state);
  fs.appendFileSync(file, [reply('second'), reply('later', '2026-10-20T01:00:02Z')].map(row => JSON.stringify(row)).join('\n') + '\n');
  write(home, '.claude/projects/synthetic/b.jsonl', [reply('new-source')]);
  const legacy = value => Object.fromEntries(Object.entries(value).filter(([key]) => !['velocity', 'velocityPending'].includes(key)));
  const before = structuredClone(legacy(state));
  state = restart(home, reader.scan({ ...window, home, state, command: 'velocity-backfill', until: '2026-10-20T01:00:00Z', config: { sendVelocity: true, sendEvidence: true } }).state);
  assert.deepEqual(legacy(state), before, 'replay must not persist usage, session, evidence or seat mutations');
  state = reader.scan({ ...window, home, state }).state;
  assert.equal(total(state, 'requests'), 4, 'the next normal scan must still consume all new usage');
});
