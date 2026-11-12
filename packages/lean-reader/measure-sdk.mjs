// One fixture-only proof through the real CLI; no native roots or credentials.
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';

assert.equal(process.versions.node.split('.')[0], '22', 'Run this proof with Node 22');
const out = path.resolve(process.argv[2]);
assert.ok(out.startsWith('/private/tmp/'), 'Use a private directory under /private/tmp');
fs.mkdirSync(out, { recursive: true, mode: 0o700 }); fs.chmodSync(out, 0o700);
const home = fs.mkdtempSync(path.join(out, 'fixture-')); fs.chmodSync(home, 0o700);
const write = (file, value) => { fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 }); fs.writeFileSync(file, value, { mode: 0o600 }); };
const day = new Date().toISOString().slice(0, 10), since = day + 'T00:00:00Z';
const until = new Date(Date.parse(since) + 86_400_000).toISOString();
const NODE_OPTIONS = '--jitless --max-old-space-size=256 --max-semi-space-size=1';
const identity = { accountUuid: 'fixture-account-uuid', emailAddress: 'brain@example.test' };
const accountKey = 'sha256:' + crypto.createHash('sha256').update(JSON.stringify({ stringValue: identity.accountUuid })).digest('hex').slice(0, 16);
write(path.join(home, '.claude.json'), JSON.stringify({ oauthAccount: identity }));
const launchFile = path.join(home, '.local/state/fleet-launch/launches-sdk.jsonl'); write(launchFile, '');
const SIZE = 500_000, REPLIES = 100, cloud = new Map(), uploads = new Map(), receipts = [];
let count = 0;
function addFiles(n) {
  for (let end = count + n; count < end; count++) {
    const session = 'sdk-fixture-' + String(count).padStart(6, '0');
    const root = path.join(home, 'temp', 'hindsight-claude-code-' + String(count % 30).padStart(2, '0'), 'projects');
    let content = '';
    for (let j = 0; j < REPLIES; j++) content += JSON.stringify({ type: 'assistant', timestamp: day + 'T01:02:03Z', sessionId: session,
      message: { id: session + '-' + String(j).padStart(3, '0'), model: 'fixture-model', content: [{ type: 'text', text: 'synthetic output '.repeat(260) }],
        usage: { input_tokens: 10, cache_read_input_tokens: 120000, cache_creation_input_tokens: 300, output_tokens: 20 } } }) + '\n';
    const shell = JSON.stringify({ type: 'user', message: { content: '' } }) + '\n';
    content += JSON.stringify({ type: 'user', message: { content: 'x'.repeat(SIZE - Buffer.byteLength(content) - Buffer.byteLength(shell)) } }) + '\n';
    assert.equal(Buffer.byteLength(content), SIZE);
    write(path.join(root, session + '.jsonl'), content);
    fs.appendFileSync(launchFile, JSON.stringify({ kind: 'sdk', session, account: 'brain-example-test', project: 'colony-brain', at: since }) + '\n');
  }
}
const server = http.createServer((req, res) => {
  let text = ''; req.on('data', chunk => { text += chunk; });
  req.on('end', () => {
    const body = JSON.parse(text); assert.equal(body.schema, 'plimsoll.lean.usage.v1');
    assert.equal(req.url, '/api/work-intelligence/lean/usage');
    for (const row of body.hours) {
      assert.equal(row.accountKey, accountKey); assert.equal(row.projectHint, 'colony-brain');
      assert.equal(row.requests, REPLIES); assert.equal(row.freshInput, REPLIES * 10);
      assert.equal(row.cacheRead, REPLIES * 120000); assert.equal(row.cacheWrite, REPLIES * 300); assert.equal(row.output, REPLIES * 20);
      const key = JSON.stringify([row.hour, row.sessionKey, row.model]);
      uploads.set(key, (uploads.get(key) ?? 0) + 1); cloud.set(key, row);
    }
    res.end('{"ok":true}');
  });
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const config = path.join(home, 'config.json'), stateDir = path.join(home, 'state');
const credentialsFrom = path.join(home, 'fixture-upload.json');
write(credentialsFrom, JSON.stringify({ uploadUrl: `http://127.0.0.1:${server.address().port}`, tenantId: 'fixture', installKey: 'fixture', uploadSigningSecret: 'fixture' }));
write(config, JSON.stringify({ rootsOnly: true, credentialsFrom, roots: [{ tool: 'claude_code', path: home + '/temp/hindsight-claude-code-*/projects' }] }));
const cli = fileURLToPath(new URL('./dist/plimsoll-lean.mjs', import.meta.url));
const stateFile = path.join(stateDir, 'state.json'), journal = path.join(stateDir, 'sdk-journal.jsonl'), stopped = path.join(home, 'stopped');
const preload = path.join(home, 'stop.cjs');
write(preload, `const fs = require('node:fs'); const sync = fs.fsyncSync; let n = 0;
fs.fsyncSync = function(fd) { const result = sync.call(this, fd);
  if (fs.existsSync(process.env.FIXTURE_JOURNAL) && fs.fstatSync(fd).ino === fs.statSync(process.env.FIXTURE_JOURNAL).ino && ++n === 10) {
    fs.writeFileSync(process.env.FIXTURE_STOPPED, 'between files'); process.kill(process.pid, 'SIGSTOP');
  } return result;
};\n`);
async function run(label, interrupt = false) {
  const stdout = fs.openSync(path.join(out, label + '.json'), 'w'), stderr = fs.openSync(path.join(out, label + '.time.txt'), 'w');
  const start = performance.now();
  const args = [...(interrupt ? ['--require', preload] : []), cli, 'run', '--since', since, '--until', until, '--state-dir', stateDir, '--config', config];
  let watcher, killed;
  const child = spawn('/usr/bin/time', ['-l', process.execPath, ...args], { env: { ...process.env, HOME: home, CLAUDE_CONFIG_DIR: '', CODEX_HOME: '', GROK_HOME: '', NODE_OPTIONS, TMPDIR: out, FIXTURE_JOURNAL: journal, FIXTURE_STOPPED: stopped }, stdio: ['ignore', stdout, stderr] });
  if (interrupt) watcher = fs.watch(home, () => {
    if (killed || !fs.existsSync(stopped)) return;
    const owner = fs.readdirSync(path.join(stateDir, 'reader.lock'))[0];
    const { pid } = JSON.parse(fs.readFileSync(path.join(stateDir, 'reader.lock', owner)));
    killed = true; process.kill(pid, 'SIGTERM'); process.kill(pid, 'SIGCONT');
  });
  const timer = setTimeout(() => { child.kill('SIGKILL'); }, 25_000);
  const [status, signal] = await once(child, 'close'); clearTimeout(timer); watcher?.close(); fs.closeSync(stdout); fs.closeSync(stderr);
  const timing = fs.readFileSync(path.join(out, label + '.time.txt'), 'utf8');
  if (interrupt) { assert.ok(killed); assert.notEqual(status, 0); return; }
  assert.equal(status, 0, timing + '\n' + signal);
  const stats = JSON.parse(fs.readFileSync(path.join(out, label + '.json')));
  const peakRssBytes = Number(timing.match(/(\d+)\s+maximum resident set size/)?.[1]);
  const milliseconds = Math.round(performance.now() - start), stateBytes = fs.statSync(stateFile).size;
  const receipt = { pass: label, files: stats.files, bytesRead: stats.bytesRead, sent: stats.sent, milliseconds, peakRssBytes, stateBytes };
  console.log(JSON.stringify(receipt)); receipts.push(receipt);
  assert.ok(milliseconds < 20_000 && peakRssBytes > 0 && peakRssBytes < 100_000_000 && stateBytes < 16_000_000, JSON.stringify(receipt));
  const state = JSON.parse(fs.readFileSync(stateFile));
  assert.equal(Object.keys(state.files).length, 0);
  assert.equal(fs.statSync(journal).size, 0);
  assert.ok(Object.values(state.sdk).every(sessions => Object.values(sessions).every(row => !row[6])));
  assert.ok([...uploads.values()].every(n => n === 1), 'Every day/session total sent once');
  return stats;
}
try {
  addFiles(3000);
  for (let pass = 1; pass <= 3; pass++) { if (pass > 1) addFiles(10); await run('pass-' + pass); }
  assert.equal(cloud.size, count); assert.equal([...cloud.values()].reduce((n, row) => n + row.requests, 0), count * REPLIES);
  const before = count; addFiles(120); await run('interrupted', true);
  const committed = fs.readFileSync(journal, 'utf8').trim().split('\n').map(JSON.parse);
  assert.equal(committed.length, 10);
  const resumed = await run('recovered');
  assert.equal(resumed.files, count - before - committed.length);
  assert.equal(resumed.bytesRead, resumed.files * SIZE);
  assert.equal(cloud.size, count);
  const idle = await run('unchanged'); assert.equal(idle.bytesRead, 0); assert.equal(idle.sent, 0);
  const result = { status: 'PASS', node: process.version, NODE_OPTIONS, roots: 30, initialTranscripts: 3000, transcriptBytes: SIZE, assistantUsageLines: REPLIES,
    countedRequests: count * REPLIES, uniqueDaySessions: cloud.size, interruptedCommittedFiles: committed.length, resumedFiles: resumed.files, receipts };
  write(path.join(out, 'footprint.json'), JSON.stringify(result, null, 2) + '\n'); console.log(JSON.stringify(result));
} finally { await new Promise(resolve => server.close(resolve)); }
