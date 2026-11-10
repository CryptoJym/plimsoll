// Fixture-only benchmark; neither source roots nor credentials from this Mac.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import assert from 'node:assert/strict';

const out = path.resolve(process.argv[2]); fs.mkdirSync(out, { recursive: true, mode: 0o700 }); fs.chmodSync(out, 0o700);
const home = fs.mkdtempSync(path.join(out, 'fixture-')); fs.chmodSync(home, 0o700);
const write = (file, value) => { fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 }); fs.writeFileSync(file, value, { mode: 0o600 }); };
const day = '2026-11-09', since = day + 'T00:00:00Z', until = '2026-11-10T00:00:00Z';
write(path.join(home, '.claude.json'), JSON.stringify({ oauthAccount: { accountUuid: 'fixture-account-uuid', emailAddress: 'brain@example.test' } }));
const launchFile = path.join(home, '.local/state/fleet-launch/launches-sdk.jsonl'); write(launchFile, '');
const launches = fs.openSync(launchFile, 'a');
for (let i = 0; i < 20_000; i++) {
  const session = '00000000-0000-4000-8000-' + String(i).padStart(12, '0');
  const root = path.join(home, 'var/folders/aa/bb/T', 'hindsight-claude-code-' + String(i % 30).padStart(2, '0'), 'projects');
  write(path.join(root, session + '.jsonl'), JSON.stringify({ type: 'assistant', timestamp: day + 'T01:02:03Z', sessionId: session,
    message: { id: 'fixture-reply-' + i, model: 'fixture-model', usage: { input_tokens: 10, output_tokens: 20 } } }) + '\n');
  fs.writeSync(launches, JSON.stringify({ kind: 'sdk', session, account: 'brain-example-test', project: 'colony-brain', area: 'memory', asked_by: 'colony-brain:fixture', at: since }) + '\n');
}
fs.closeSync(launches);
const config = path.join(home, 'config.json'), stateDir = path.join(home, 'state');
write(config, JSON.stringify({ rootsOnly: true, roots: [{ tool: 'claude_code', path: home + '/var/folders/*/*/T/hindsight-claude-code-*/projects' }] }));
const stdout = fs.openSync(path.join(out, 'backfill.json'), 'w'), stderr = fs.openSync(path.join(out, 'time.txt'), 'w');
const args = [fileURLToPath(new URL('./dist/plimsoll-lean.mjs', import.meta.url)), 'backfill', '--since', since, '--until', until, '--dry-run', '--json', '--state-dir', stateDir, '--config', config];
const start = performance.now();
const result = spawnSync('/usr/bin/time', ['-l', process.execPath, ...args], { env: { ...process.env, HOME: home, CLAUDE_CONFIG_DIR: '', CODEX_HOME: '', GROK_HOME: '', NODE_OPTIONS: '', TMPDIR: out }, stdio: ['ignore', stdout, stderr], timeout: 60_000 });
const milliseconds = performance.now() - start; fs.closeSync(stdout); fs.closeSync(stderr);
assert.equal(result.status, 0, fs.readFileSync(path.join(out, 'time.txt'), 'utf8'));
const output = JSON.parse(fs.readFileSync(path.join(out, 'backfill.json'))), stateFile = path.join(stateDir, 'state.json');
const stateBytes = fs.statSync(stateFile).size, rss = Number(fs.readFileSync(path.join(out, 'time.txt'), 'utf8').match(/(\d+)\s+maximum resident set size/)?.[1]);
assert.equal(output.sessions.length, 20_000); assert.equal(output.rows.reduce((n, r) => n + r.requests, 0), 20_000);
assert.ok(output.rows.every(r => r.accountKey !== 'unknown' && r.projectHint === 'colony-brain'));
const receipt = { transcripts: 20_000, roots: 30, milliseconds: Math.round(milliseconds), peakRssBytes: rss, stateBytes, filesTracked: Object.keys(JSON.parse(fs.readFileSync(stateFile)).files).length,
  under60Seconds: milliseconds < 60_000, under128MiB: rss < 128 * 1024 * 1024, under10MB: stateBytes < 10_000_000, node: process.version, stateFile };
write(path.join(out, 'footprint.json'), JSON.stringify(receipt, null, 2) + '\n'); console.log(JSON.stringify(receipt));
assert.ok(receipt.under60Seconds && receipt.under128MiB && receipt.under10MB);
