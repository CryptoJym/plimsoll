import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const repo = path.resolve(import.meta.dirname, '..');
const loader = path.join(repo, 'node_modules/tsx/dist/loader.mjs');
const cli = path.join(repo, 'packages/collector-cli/src/cli.ts');
const Database = createRequire(path.join(repo, 'package.json'))('better-sqlite3');
const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'claude-status-r4-'));
const home = path.join(root, 'home');
const config = path.join(root, 'claude');
const collector = path.join(home, '.plimsoll');
const env = { ...process.env, HOME: home, USERPROFILE: home, CODEX_HOME: path.join(home, '.codex'),
  CLAUDE_CONFIG_DIR: config, PLIMSOLL_HOME: collector, PLIMSOLL_FIXTURE_ROOT: root,
  XDG_CONFIG_HOME: path.join(home, '.config'), XDG_CACHE_HOME: path.join(home, '.cache'),
  XDG_STATE_HOME: path.join(home, '.local/state'), TMPDIR: path.join(home, 'tmp'),
  NEXT_TELEMETRY_DISABLED: '1' };
const key = id => 'sha256:' + crypto.createHash('sha256')
  .update(JSON.stringify({ stringValue: id })).digest('hex').slice(0, 16);
const quote = value => "'" + value.replaceAll("'", "'\\''") + "'";

function cliResult(args, extraEnv = {}) {
  const result = spawnSync(process.execPath, ['--import', loader, cli, 'setup', 'claude-status-line', ...args],
    { cwd: repo, env: { ...env, ...extraEnv }, encoding: 'utf8', timeout: 30_000 });
  assert.equal(result.status, 0, result.stderr);
  const rows = JSON.parse(result.stdout).results;
  return rows.find(row => row.configDir === config);
}

function checkAccountSwitch() {
  const accountFile = path.join(config, '.claude.json');
  const accountA = 'r4-claude-account-alpha';
  const accountB = 'r4-claude-account-beta';
  const replacement = path.join(root, 'account-b.json');
  const switchScript = path.join(root, 'switch.sh');
  fs.writeFileSync(accountFile, JSON.stringify({ oauthAccount: { accountUuid: accountA } }));
  fs.writeFileSync(replacement, JSON.stringify({ oauthAccount: { accountUuid: accountB } }));
  fs.writeFileSync(switchScript, '#!/bin/sh\ncat >/dev/null\n/bin/cp ' +
    quote(replacement) + ' ' + quote(accountFile) + '\nprintf original');
  fs.chmodSync(switchScript, 0o700);
  fs.writeFileSync(path.join(config, 'settings.json'),
    JSON.stringify({ statusLine: { type: 'command', command: quote(switchScript) } }));
  assert.equal(cliResult([]).outcome, 'chained');
  const command = JSON.parse(fs.readFileSync(path.join(config, 'settings.json'), 'utf8')).statusLine.command;
  for (let index = 0; index < 2; index += 1) {
    const input = JSON.stringify({ rate_limits: {
      five_hour: { used_percentage: 37 + index * 2, resets_at: 1790676000 },
    } });
    const result = spawnSync('/bin/sh', ['-c', command],
      { env, input, encoding: 'utf8', timeout: 30_000 });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout, 'original');
  }
  const ledger = path.join(collector, 'work-ledger.sqlite');
  const db = new Database(ledger, { readonly: true });
  let rows;
  try {
    rows = db.prepare("select payload_json from buffered_events where event_type='plan_limit_observation'")
      .all().map(row => JSON.parse(row.payload_json));
  } finally { db.close(); }
  assert.equal(rows.length, 1,
    'the ambiguous account-switch reading must be skipped');
  assert.equal(rows[0].metadata['user.account_uuid'], key(accountB),
    'a later reading with B on both sides of the chain belongs to B');
  assert.equal(rows[0].metadata.planLimitUsedPercent, 39);
  assert.equal(rows.some(row => row.metadata.planLimitUsedPercent === 37), false,
    'the first reading must not be emitted without an account');
  assert.notEqual(rows[0].metadata['user.account_uuid'], key(accountA));
  for (const candidate of [ledger, ledger + '-wal', ledger + '-shm']) {
    if (!fs.existsSync(candidate)) continue;
    const bytes = fs.readFileSync(candidate);
    assert.equal(bytes.includes(Buffer.from(accountA)), false);
    assert.equal(bytes.includes(Buffer.from(accountB)), false);
  }
  console.log(JSON.stringify({ proof: 'claude-status-line-r4-account-switch', checks: 9, passed: 9 }));
}

const raceHook = String.raw`
const fs = require('node:fs');
const original = {
  readFileSync: fs.readFileSync, writeFileSync: fs.writeFileSync,
  renameSync: fs.renameSync, lstatSync: fs.lstatSync, openSync: fs.openSync,
};
const settings = process.env.R4_SETTINGS;
const stage = process.env.R4_STAGE;
const marker = process.env.R4_MARKER;
let side;
let fired = false;
let settingsLstats = 0;
function edit(file) {
  const doc = fs.existsSync(file) ? JSON.parse(original.readFileSync(file, 'utf8')) : {};
  doc.concurrentOperatorEdit = stage;
  original.writeFileSync(file, JSON.stringify(doc));
  original.writeFileSync(marker, stage);
  fired = true;
}
fs.lstatSync = function(file, ...args) {
  if (String(file) === settings) {
    settingsLstats++;
    if (!fired && stage === 'before_temp_write' && settingsLstats === 2) edit(settings);
  }
  return original.lstatSync.call(this, file, ...args);
};
fs.renameSync = function(from, to) {
  const movingSettings = String(from) === settings && String(to).includes('.plimsoll-uninstall-');
  if (movingSettings && stage === 'before_rename') edit(settings);
  const result = original.renameSync.call(this, from, to);
  if (movingSettings) side = String(to);
  if (movingSettings && stage === 'after_rename') edit(settings);
  return result;
};
fs.openSync = function(file, ...args) {
  if (!fired && stage === 'before_side_compare' && String(file) === side) edit(side);
  return original.openSync.call(this, file, ...args);
};
fs.writeFileSync = function(file, ...args) {
  const restoring = String(file) === settings;
  if (!fired && restoring && stage === 'before_restore_link') edit(settings);
  const result = original.writeFileSync.call(this, file, ...args);
  if (!fired && restoring && stage === 'after_restore_link_side_edit') edit(side);
  return result;
};
`;

function checkRestoreRaces() {
  const initial = Buffer.from('{\n  "statusLine":{"type":"command","command":"printf original","padding":2,"refreshInterval":60},\n  "theme":"dark"\n}\n');
  const stages = ['before_temp_write', 'before_rename', 'after_rename',
    'before_side_compare', 'before_restore_link', 'after_restore_link_side_edit'];
  const hook = path.join(root, 'race-hook.cjs');
  fs.writeFileSync(hook, raceHook);
  const settings = path.join(config, 'settings.json');
  for (const stage of stages) {
    fs.rmSync(config, { recursive: true, force: true });
    fs.mkdirSync(config, { recursive: true, mode: 0o700 });
    fs.writeFileSync(settings, initial);
    assert.equal(cliResult([]).outcome, 'chained');
    const marker = path.join(root, `${stage}.fired`);
    const result = cliResult(['--uninstall'], { R4_SETTINGS: settings, R4_STAGE: stage,
      R4_MARKER: marker, NODE_OPTIONS: `${env.NODE_OPTIONS ?? ''} --require=${hook}` });
    assert.equal(fs.existsSync(marker), true, `injection did not fire: ${stage}`);
    assert.equal(result.outcome, 'status_line_changed', stage);
    if (stage !== 'before_temp_write') {
      assert.equal(typeof result.retainedPath, 'string', stage);
      assert.equal(fs.existsSync(result.retainedPath), true, stage);
    }
    const live = fs.existsSync(settings) ? fs.readFileSync(settings, 'utf8') : '';
    const retained = result.retainedPath ? fs.readFileSync(result.retainedPath, 'utf8') : '';
    assert.ok(live.includes(`"concurrentOperatorEdit":"${stage}"`) ||
      retained.includes(`"concurrentOperatorEdit":"${stage}"`), stage);
    assert.equal(fs.existsSync(path.join(config, '.plimsoll-status-line-original.json')), true, stage);
    console.log(JSON.stringify({ case: stage, operatorEditPreserved: true,
      retainedPathReported: Boolean(result.retainedPath) }));
  }
  fs.rmSync(config, { recursive: true, force: true });
  fs.mkdirSync(config, { recursive: true, mode: 0o700 });
  fs.writeFileSync(settings, initial);
  assert.equal(cliResult([]).outcome, 'chained');
  assert.equal(cliResult(['--uninstall']).outcome, 'restored');
  assert.ok(fs.readFileSync(settings).equals(initial));
  assert.equal(fs.readdirSync(config).filter(name => name.startsWith('settings.json.plimsoll-uninstall-')).length, 1);
  console.log(JSON.stringify({ proof: 'claude-status-line-r4-restore-races',
    raceStages: stages.length, noRaceOriginalBytesRestored: true }));
}

try {
  for (const dir of [home, config, collector, env.TMPDIR]) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  if (process.argv[2] === undefined) { checkAccountSwitch(); checkRestoreRaces(); }
  else if (process.argv[2] === 'account') checkAccountSwitch();
  else if (process.argv[2] === 'restore') checkRestoreRaces();
  else throw new Error('usage: claude-status-line-r4-proof.mjs account|restore');
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
