// Run the unchanged legacy fixture suite, recording every public scan/runPass
// result. Compare JSON bytes, including order and every row field, across builds.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const pkg = fileURLToPath(new URL('../', import.meta.url));
const [label, bundle] = process.argv.slice(2);
if (!['baseline', 'candidate'].includes(label) || !bundle) throw new Error('usage: capture-legacy.mjs baseline|candidate BUNDLE');
const evidence = path.join(pkg, 'evidence/grok'); fs.mkdirSync(evidence, { recursive: true });
const output = path.join(evidence, label + '-rows.jsonl'); fs.writeFileSync(output, '');
const scratch = fs.mkdtempSync(path.join(evidence, '.capture-'));
const original = fs.readFileSync(new URL('./reader.test.mjs', import.meta.url), 'utf8');
const source = original.replace("const bundle = new URL('../dist/plimsoll-lean.mjs', import.meta.url);", `const bundle = new URL(${JSON.stringify(pathToFileURL(path.resolve(bundle)).href)});`)
  .replace('const reader = fs.existsSync(bundle) ? await import(bundle.href) : {};', `
const implementation = fs.existsSync(bundle) ? await import(bundle.href) : {};
const capture = (kind, result) => {
  fs.appendFileSync(${JSON.stringify(output)}, JSON.stringify({ kind, rows: result.rows }) + '\\n');
  return result;
};
const reader = new Proxy(implementation, { get(target, key) {
  if (key === 'scan') return (...args) => capture('scan', target.scan(...args));
  if (key === 'runPass') return async (...args) => capture('runPass', await target.runPass(...args));
  return target[key];
} });`);
try {
  const test = path.join(scratch, 'reader.test.mjs'); fs.writeFileSync(test, source);
  const result = spawnSync(process.execPath, ['--test', test], { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 });
  fs.writeFileSync(path.join(evidence, label + '-parity-tests.log'), result.stdout + result.stderr);
  if (result.status !== 0) throw new Error(label + ' fixture tests failed: ' + result.status);
  const bytes = fs.readFileSync(output), records = bytes.toString().trim().split('\n').map(JSON.parse);
  const receipt = { label, legacyTestSha256: crypto.createHash('sha256').update(original).digest('hex'), captures: records.length, rows: records.reduce((n, r) => n + r.rows.length, 0), bytes: bytes.length, sha256: crypto.createHash('sha256').update(bytes).digest('hex') };
  if (label === 'candidate') {
    const baseline = fs.readFileSync(path.join(evidence, 'baseline-rows.jsonl'));
    if (!baseline.equals(bytes)) throw new Error('legacy row bytes differ');
    receipt.byteIdentical = true;
  }
  fs.writeFileSync(path.join(evidence, label + '-parity.json'), JSON.stringify(receipt, null, 2) + '\n');
  console.log(JSON.stringify(receipt));
} finally { fs.rmSync(scratch, { recursive: true, force: true }); }
