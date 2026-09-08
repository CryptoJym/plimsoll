import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { isolatedEnvironment } from './run-proof';
import { createProofCompletion } from './lib/proof-completion';

const names = ['oversized-continuation-state-proof', 'oversized-continuation-transaction-proof',
  'oversized-continuation-boundary-proof', 'oversized-continuation-ready-proof',
  'oversized-continuation-provider-proof', 'oversized-continuation-cadence-proof', 'jsonl-read-generation-proof'];
const completion = createProofCompletion('oversized-continuation-suite', names.length);
const repo = path.resolve(import.meta.dirname, '..');
const out = path.join(repo, 'evidence/oversized-continuation'); fs.mkdirSync(out, { recursive: true });
for (const name of names) {
  const entry = path.join(repo, 'scripts', name + '.ts');
  const hash = () => createHash('sha256').update(fs.readFileSync(entry)).digest('hex'), before = hash();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'oversized-suite-')), env = isolatedEnvironment(root);
  try {
    const result = spawnSync(process.execPath, ['--import', path.join(repo, 'node_modules/tsx/dist/loader.mjs'), entry],
      { cwd: repo, env, encoding: 'utf8', timeout: 180000, maxBuffer: 8 * 1024 * 1024 });
    fs.writeFileSync(path.join(out, name + '.txt'), result.stdout + '\n' + result.stderr);
    assert.equal(result.status, 0, name + ': exit'); assert.equal(hash(), before, name + ': source unchanged');
    const text = result.stdout.trim(), start = text.lastIndexOf('\n{') + 1;
    const summary = JSON.parse(text.slice(start));
    if (name === 'oversized-continuation-cadence-proof') {
      assert.equal(summary.passed, true); assert.equal(summary.actualWorker, true); assert.equal(summary.results.length, 2);
      assert(summary.results.every((r: any) => r.passed));
    } else {
      assert.equal(summary.status, 'PASS');
      if (name === 'oversized-continuation-state-proof') { assert.equal(summary.checks.length, 10); assert.equal(summary.columns.length, 21); }
      else if (name === 'jsonl-read-generation-proof') { assert.equal(summary.checks, 12); assert.equal(summary.receipts.length, 12); }
      else assert.equal(summary.receipts.length, 2);
    }
    fs.writeFileSync(path.join(out, name + '.json'), JSON.stringify({ sourceSha256: before, summary }, null, 2));
    completion.check(name); console.log(JSON.stringify({ name, status: 'PASS' }));
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}
completion.complete();
