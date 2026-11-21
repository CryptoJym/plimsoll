import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import * as reader from '../dist/plimsoll-lean.mjs';

test('antigravity transcripts capture model tokens, git repo hints, dedup on rescan, ignore scratch/, and emit Background LaunchAgent', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'plimsoll-lean-ag-'));
  try {
    const repo = path.join(home, 'Projects', 'second-string');
    fs.mkdirSync(path.join(repo, '.git'), { recursive: true });
    fs.writeFileSync(path.join(home, '.gemini', 'installation_id').replace(/installation_id$/, '') || home, '').toString;
  } catch { /* create parent first */ }
  try {
    fs.mkdirSync(path.join(home, '.gemini'), { recursive: true });
    fs.writeFileSync(path.join(home, '.gemini', 'installation_id'), 'test-install-id-123\n');
    const conv = '11111111-2222-3333-4444-555555555555';
    const logs = path.join(home, '.gemini', 'antigravity', 'brain', conv, '.system_generated', 'logs');
    fs.mkdirSync(logs, { recursive: true });
    // Put a decoy file in scratch/ to prove we never walk scratch/
    const scratch = path.join(home, '.gemini', 'antigravity', 'brain', conv, 'scratch', 'node_modules');
    fs.mkdirSync(scratch, { recursive: true });
    fs.writeFileSync(path.join(scratch, 'decoy.jsonl'), '{"type":"PLANNER_RESPONSE","created_at":"2026-11-21T18:10:00Z","model":"gemini-4-neon-xhigh","input_tokens":999,"output_tokens":999}\n');

    const transcript = path.join(logs, 'transcript.jsonl');
    const targetFile = path.join(home, 'Projects', 'second-string', 'src', 'index.ts');
    const lines = [
      JSON.stringify({ step_index: 0, type: 'USER_INPUT', created_at: '2026-11-21T18:10:00Z', content: 'Fix feed' }),
      JSON.stringify({
        step_index: 1, type: 'PLANNER_RESPONSE', created_at: '2026-11-21T18:10:05Z',
        model: 'gemini-4-neon-xhigh', input_tokens: 12000, cache_read_tokens: 50000, output_tokens: 800,
        tool_calls: [{ name: 'read_file', args: { Path: JSON.stringify(targetFile) } }],
      }),
      JSON.stringify({
        step_index: 2, type: 'PLANNER_RESPONSE', created_at: '2026-11-21T18:10:15Z',
        model: 'gemini-4-hydrogen-tiered-high', input_tokens: 3000, cache_read_tokens: 20000, output_tokens: 400,
      }),
    ];
    fs.writeFileSync(transcript, lines.join('\n') + '\n');
    const now = Date.parse('2026-11-21T18:30:00Z');
    fs.utimesSync(transcript, new Date(now), new Date(now));

    const first = await reader.runPass({ home, now, dryRun: true });
    const agRows = Object.values(first.state.hours).filter(r => r.tool === 'antigravity');
    assert.equal(agRows.length, 2);
    const neon = agRows.find(r => r.model === 'gemini-4-neon-xhigh');
    assert.ok(neon);
    assert.equal(neon.requests, 1);
    assert.equal(neon.freshInput, 12000);
    assert.equal(neon.cacheRead, 50000);
    assert.equal(neon.cacheWrite, 0);
    assert.equal(neon.output, 800);
    assert.equal(neon.projectHint, 'second-string');
    assert.equal(neon.projectSource, 'git_repo');
    assert.match(neon.accountKey, /^sha256:[0-9a-f]{16}$/);

    // Incremental pass with no changes must not double-count
    const second = await reader.runPass({ home, now: now + 60_000, dryRun: true });
    const agRows2 = Object.values(second.state.hours).filter(r => r.tool === 'antigravity');
    assert.equal(agRows2.find(r => r.model === 'gemini-4-neon-xhigh')?.requests, 1);

    // LaunchAgent XML carries LimitLoadToSessionType=Background
    const xml = reader.launchAgent(home, '/synthetic/plimsoll-lean.mjs');
    assert.ok(xml.includes('<key>LimitLoadToSessionType</key><string>Background</string>'));
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
});
