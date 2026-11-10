// One offline check, using Node 22.20+/24's native TS transformation. No install.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { registerHooks, stripTypeScriptTypes } from 'node:module';
registerHooks({
  resolve(specifier, context, next) {
    if (specifier.startsWith('.') && specifier.endsWith('.js')) {
      const ts = new URL(specifier.slice(0, -3) + '.ts', context.parentURL);
      if (fs.existsSync(ts)) return { url: ts.href, shortCircuit: true };
    }
    return next(specifier, context);
  },
  load(url, context, next) {
    if (url.endsWith('.ts')) return { format: 'module', source: stripTypeScriptTypes(fs.readFileSync(new URL(url), 'utf8'), { mode: 'transform' }), shortCircuit: true };
    return next(url, context);
  },
});
const { scan } = await import('./src/reader.ts');
const baseline = process.env.PLIMSOLL_BASELINE_READER ? (await import(pathToFileURL(path.resolve(process.env.PLIMSOLL_BASELINE_READER)).href)).scan : null;
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'plimsoll-pilot-check-'));
const usage = { input_tokens: 100, cache_read_input_tokens: 20, cache_creation_input_tokens: 10, output_tokens: 30, output_tokens_details: { thinking_tokens: 10 } };
const advisor = { type: 'advisor_message', model: 'claude-opus-5-5', input_tokens: 20, cache_read_input_tokens: 20, cache_creation_input_tokens: 10, output_tokens: 10 };
const sum = rows => rows.reduce((n, r) => n + r.freshInput + r.cacheRead + r.cacheWrite + r.output, 0);
const receipts = [];
try {
  for (const [name, extra, huge, expected] of [['normal', false, false, 160], ['advisor', true, false, 220], ['oversized', true, true, 220]]) {
    const home = path.join(dir, name); fs.mkdirSync(home);
    const file = path.join(home, 'fixture.jsonl');
    const record = { type: 'assistant', timestamp: '2026-11-09T10:00:01Z', sessionId: name, cwd: home, effort: 'max',
      message: { id: name, model: 'claude-sonnet-4-6', content: [{ type: 'tool_use', name: 'Read' }, { type: 'text', text: huge ? 'x'.repeat(1100000) : 'ok' }],
        usage: { ...usage, ...(extra ? { iterations: [{ type: 'executor_message', input_tokens: 9999 }, advisor, { ...advisor, model: '<synthetic>' }] } : {}) } } };
    const text = JSON.stringify(record) + '\n'; fs.writeFileSync(file, text.repeat(3));
    for (const sdk of [false, true]) {
      const opts = { home, env: {}, now: Date.parse('2026-11-09T11:00:00Z'), since: '2026-11-09T00:00:00Z', until: '2026-11-10T00:00:00Z', config: { rootsOnly: true },
        discovered: [{ tool: 'claude_code', path: home, aggregate: sdk, files: [{ file, stat: fs.statSync(file) }] }] };
      const first = scan(opts);
      // SDK capture commits bounded slices; drain repeats before asserting a
      // fully consumed source reads zero bytes on the unchanged pass.
      let settled = first;
      while (sdk && settled.state.sdkFiles[file][3] < fs.statSync(file).size) settled = scan({ ...opts, state: settled.state });
      const second = scan({ ...opts, state: JSON.parse(JSON.stringify(settled.state)) });
      assert.equal(sum(first.rows), expected); assert.equal(sum(second.rows), expected); assert.equal(second.stats.bytesRead, 0);
      const executor = first.rows.find(r => r.model === 'claude-sonnet-4-6');
      assert.equal(executor.requests, 1); assert.equal(executor.cacheRead, 20); assert.equal(executor.cacheWrite, 10);
      if (extra) { const row = first.rows.find(r => r.model === advisor.model); assert.equal(row.requests, 1); assert.equal(row.cacheRead, 20); assert.equal(row.cacheWrite, 10); }
      if (!sdk) {
        const velocities = Object.values(first.state.velocity); assert.equal(velocities.reduce((n, v) => n + v.reasoning, 0), 10);
        const actions = Object.values(first.state.sessions)[0].actions;
        if (!huge) assert.equal(actions.read, 1);
      }
      if (baseline) { const old = baseline(opts); assert.equal(sum(old.rows), 160); receipts.push({ name, sdk, baseline_tokens: sum(old.rows), pilot_tokens: sum(first.rows), unchanged_bytes: second.stats.bytesRead }); }
    }
    if (huge) {
      const evidence = scan({ home, env: {}, now: Date.parse('2026-11-09T11:00:00Z'), config: { rootsOnly: true, sendEvidence: true }, discovered: [{ tool: 'claude_code', path: home, files: [{ file, stat: fs.statSync(file) }] }] });
      assert.equal(sum(evidence.rows), 220); assert.equal(Object.values(evidence.state.sessions)[0].actions.read, 1);
    }
  }
  if (baseline) {
    const home = path.join(dir, 'migration'); fs.mkdirSync(home); const file = path.join(home, 'fixture.jsonl');
    const record = { type: 'assistant', timestamp: '2026-11-09T10:00:01Z', sessionId: 'migration', cwd: home,
      message: { id: 'legacy-id', model: 'claude-sonnet-4-6', usage: { ...usage, iterations: [advisor] } } };
    const text = JSON.stringify(record) + '\n'; fs.writeFileSync(file, text);
    const opts = { home, env: {}, now: Date.parse('2026-11-09T11:00:00Z'), config: { rootsOnly: true }, discovered: [{ tool: 'claude_code', path: home, files: [{ file, stat: fs.statSync(file) }] }] };
    for (const sdk of [false, true]) {
      fs.writeFileSync(file, text); opts.discovered[0].aggregate = sdk;
      const old = baseline(opts); fs.appendFileSync(file, text);
      assert.equal(sum(scan({ ...opts, state: old.state }).rows), 220);
    }
  }
  console.log(JSON.stringify({ status: 'PASS', provider_calls: 0, node: process.version, receipts }));
} finally { fs.rmSync(dir, { recursive: true, force: true }); }
