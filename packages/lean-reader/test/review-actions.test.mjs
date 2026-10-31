import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { reader, window, fixture, write, restart, reply, total } from './review-fixture.mjs';

test('Claude actions count accepted messages once across duplicates and copied resumptions', t => {
  const home = fixture(t), content = [{ type: 'tool_use', id: 'synthetic-call', name: 'Read', input: {} }];
  const first = reply('first', undefined, content);
  write(home, '.claude/projects/synthetic/a.jsonl', [first, first]);
  let state = reader.scan({ ...window, home }).state;
  assert.equal(Object.values(state.sessions)[0].actions.read, 1);
  const copied = write(home, '.claude/projects/synthetic/b.jsonl', [first, reply('second', undefined, content)]);
  state = reader.scan({ ...window, home, state: restart(home, state) }).state;
  assert.equal(Object.values(state.sessions)[0].actions.read, 2);
  fs.appendFileSync(copied, JSON.stringify(reply('third', undefined, content)) + '\n');
  state = reader.scan({ ...window, home, state: restart(home, state) }).state;
  assert.equal(total(state, 'requests'), 3);
  assert.equal(Object.values(state.sessions)[0].actions.read, 3, 'a copied file must not overwrite the canonical session count');
  const before = structuredClone(state.sessions);
  state = reader.scan({ ...window, home, state, velocityOnly: true }).state;
  assert.deepEqual(state.sessions, before);
});
