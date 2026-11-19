import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { reader, window, fixture, write, restart, reply, total } from './review-fixture.mjs';

test('velocity replay preserves saved usage cursors and replies outside its window', t => {
  const home = fixture(t), file = write(home, '.claude/projects/synthetic/a.jsonl', [reply('first')]);
  let state = restart(home, reader.scan({ ...window, home }).state);
  fs.appendFileSync(file, [reply('second'), reply('later', '2026-10-20T01:00:02Z')].map(row => JSON.stringify(row)).join('\n') + '\n');
  write(home, '.claude/projects/synthetic/b.jsonl', [reply('new-source')]);
  const legacy = value => Object.fromEntries(Object.entries(value).filter(([key]) => !['velocity', 'velocityPending', 'velocityReplays'].includes(key)));
  const before = structuredClone(legacy(state));
  state = restart(home, reader.scan({ ...window, home, state, command: 'velocity-backfill', until: '2026-10-20T01:00:00Z', config: { sendVelocity: true, sendEvidence: true } }).state);
  assert.deepEqual(legacy(state), before, 'replay must not persist usage, session, evidence or seat mutations');
  state = reader.scan({ ...window, home, state }).state;
  assert.equal(total(state, 'requests'), 4, 'the next normal scan must still consume all new usage');
  assert.equal(total(state, 'replies', 'velocity'), 4, 'usage catch-up must not count replayed velocity twice');
  assert.equal(Object.keys(state.velocityReplays ?? {}).length, 0, 'caught-up cursors release replay markers');
});

test('a settled transcript backdated into the window is not read again; an append still is', t => {
  // Claude's transcript lease sets kept sessions' mtime to seven days ago, back
  // inside the eight-day window, long after their cursors were pruned.
  const home = fixture(t), file = write(home, '.claude/projects/synthetic/kept.jsonl', [reply('first')]), key = fs.realpathSync(file);
  const day = 86400_000, at = Date.parse('2026-10-21T00:00:00Z'), options = { home, env: {}, config: { sendVelocity: true } };
  fs.utimesSync(file, new Date('2026-10-20T00:00:05Z'), new Date('2026-10-20T00:00:05Z'));
  let { state } = reader.scan({ ...options, now: at });
  assert.equal(total(state, 'requests'), 1);
  assert.equal(state.files[key].offset, fs.statSync(file).size);
  state.pending = {};
  state = restart(home, reader.scan({ ...options, state, now: at + 10 * day }).state);
  assert.equal(state.files[key], undefined, 'the cursor left the window');
  assert.deepEqual(state.settled[key].slice(0, 3), [fs.statSync(file).dev, fs.statSync(file).ino, fs.statSync(file).size]);
  fs.utimesSync(file, new Date(at + 3 * day), new Date(at + 3 * day));
  let result = reader.scan({ ...options, state, now: at + 10 * day + 60_000 });
  assert.equal(result.stats.bytesRead, 0, 'an unchanged settled file is not read again from byte 0');
  assert.equal(result.stats.read.sources.claude_code.settled, 1);
  fs.appendFileSync(file, JSON.stringify(reply('resumed', '2026-10-30T12:00:00Z')) + '\n');
  result = reader.scan({ ...options, state: result.state, now: at + 10 * day + 120_000 });
  assert.equal(total(result.state, 'requests'), 1, 'the appended reply counts once; the old one stays outside the window');
  assert.equal(result.state.settled[key], undefined);
  assert.equal(result.state.files[key].offset, fs.statSync(file).size);
});
