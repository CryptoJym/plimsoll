import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { runPass } from '../dist/plimsoll-lean.mjs';

// One SDK glob root of 40 transcripts, read through the real pass with a
// simulated clock: the first pass sweeps and builds the folder index, a settled
// root is then walked without a file stat, a new transcript is found through
// its folder's mtime, an append to a settled transcript waits for the hourly
// sweep, and every reader.log line names its largest read.
const day = new Date().toISOString().slice(0, 10), MINUTE = 60_000;
const line = (session, j) => JSON.stringify({ type: 'assistant', timestamp: day + 'T01:02:03Z', sessionId: session, message: { id: session + '-' + j, model: 'fixture-model',
  content: [{ type: 'text', text: 'x'.repeat(2000) }], usage: { input_tokens: 10, cache_read_input_tokens: 100, cache_creation_input_tokens: 5, output_tokens: 20 } } }) + '\n';
test('the SDK walk serves an unchanged root from its folder index and still finds new and appended transcripts', async () => {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'lean-walk-'))), stateDir = path.join(home, 'state');
  const root = path.join(home, 'temp/hindsight-claude-code-a/projects/p');
  fs.mkdirSync(root, { recursive: true });
  const write = (name, replies, at) => { const file = path.join(root, name); fs.appendFileSync(file, Array.from({ length: replies }, (_, j) => line(name + at, j)).join('')); fs.utimesSync(file, new Date(at), new Date(at)); };
  const t0 = Date.now();
  for (let i = 0; i < 40; i++) write(`s${String(i).padStart(2, '0')}.jsonl`, i === 7 ? 50 : 2, t0 - MINUTE);
  const options = { home, env: {}, stateDir, command: 'run', config: { rootsOnly: true, dryRun: true, roots: [{ tool: 'claude_code', path: home + '/temp/hindsight-claude-code-*/projects' }] } };
  const run = async now => (await runPass({ ...options, now })).stats;
  const logged = () => fs.readFileSync(path.join(home, 'Library/Logs/plimsoll-lean/reader.log'), 'utf8').trim().split('\n').map(l => JSON.parse(l)).at(-1);
  const first = await run(t0);
  assert.equal(first.read.sdk.swept, true); assert.equal(first.read.sources.sdk.read, 40); assert.ok(first.read.sdk.stat >= 40);
  assert.deepEqual(logged().read.top[0], { file: path.join(root, 's07.jsonl'), bytes: fs.statSync(path.join(root, 's07.jsonl')).size, from: 0, why: 'sdk' });
  assert.equal(typeof logged().timing.scanMs, 'number');
  await run(t0 + 6 * MINUTE); await run(t0 + 12 * MINUTE); // the watermark passes the transcripts, which settle on the next stat
  const settled = await run(t0 + 18 * MINUTE);
  assert.equal(settled.read.sdk.indexed, 2); assert.ok(settled.read.sdk.stat <= 2, JSON.stringify(settled.read.sdk)); assert.equal(settled.bytesRead, 0);
  write('s40.jsonl', 3, t0 + 23 * MINUTE); // a new name changes the folder's mtime
  const added = await run(t0 + 24 * MINUTE);
  assert.equal(added.read.sources.sdk.read, 1); assert.ok(added.read.sdk.stat <= 5, JSON.stringify(added.read.sdk));
  write('s00.jsonl', 3, t0 + 29 * MINUTE); // an append leaves the folder's mtime alone
  const appended = await run(t0 + 30 * MINUTE);
  assert.equal(appended.bytesRead, 0); assert.equal(appended.read.sdk.swept, false);
  const swept = await run(t0 + 61 * MINUTE);
  assert.equal(swept.read.sdk.swept, true); assert.equal(swept.read.sources.sdk.read, 1); assert.equal(swept.read.top[0].file, path.join(root, 's00.jsonl'));
  fs.rmSync(home, { recursive: true, force: true });
});
