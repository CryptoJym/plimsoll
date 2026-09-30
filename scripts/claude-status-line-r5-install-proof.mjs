import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const repo = path.resolve(import.meta.dirname, '..');
const loader = path.join(repo, 'node_modules/tsx/dist/loader.mjs');
const cli = path.join(repo, 'packages/collector-cli/src/cli.ts');
const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'claude-install-r5-'));
const home = path.join(root, 'home');
const config = path.join(root, 'claude');
const settings = path.join(config, 'settings.json');
const backup = path.join(config, '.plimsoll-status-line-original.json');
const target = path.join(root, 'operator-target.json');
const marker = path.join(root, 'injection-fired');
const hook = path.join(root, 'install-hook.cjs');
const initial = '{"statusLine":{"type":"command","command":"printf original","padding":2,"refreshInterval":60},"theme":"dark"}\n';
const env = { ...process.env, HOME: home, USERPROFILE: home,
  CLAUDE_CONFIG_DIR: config, CODEX_HOME: path.join(home, '.codex'),
  PLIMSOLL_HOME: path.join(home, '.plimsoll'), PLIMSOLL_FIXTURE_ROOT: root,
  XDG_CONFIG_HOME: path.join(home, '.config'),
  XDG_CACHE_HOME: path.join(home, '.cache'),
  XDG_STATE_HOME: path.join(home, '.local/state'),
  TMPDIR: path.join(home, 'tmp'), NEXT_TELEMETRY_DISABLED: '1' };

fs.writeFileSync(hook, String.raw`
const fs = require('node:fs');
const original = { mkdir: fs.mkdirSync, lstat: fs.lstatSync,
  open: fs.openSync, write: fs.writeFileSync, rename: fs.renameSync,
  unlink: fs.unlinkSync, symlink: fs.symlinkSync, readFileSync: fs.readFileSync };
const settings = process.env.R5_SETTINGS;
const backup = process.env.R5_BACKUP;
const config = process.env.R5_CONFIG;
const target = process.env.R5_TARGET;
const marker = process.env.R5_MARKER;
const stage = process.env.R5_STAGE;
let settingsLstats = 0, settingsOpens = 0, metadataCommitted = false, firedOnce = false;
function fired() { original.write(marker, stage); firedOnce = true; }
function edit() {
  const doc = JSON.parse(original.readFileSync(settings, 'utf8'));
  doc.operatorEdit = stage;
  original.write(settings, JSON.stringify(doc));
  fired();
}
function makeSymlink() {
  try { original.unlink(settings); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  original.symlink(target, settings);
  fired();
}
fs.mkdirSync = function(file, ...args) {
  if (!firedOnce && stage === 'mkdir_symlink' && String(file) === config) makeSymlink();
  return original.mkdir.call(this, file, ...args);
};
fs.lstatSync = function(file, ...args) {
  if (String(file) === settings) {
    settingsLstats++;
    if (!firedOnce && stage === 'initial_lstat_symlink' && settingsLstats === 1) makeSymlink();
  }
  return original.lstat.call(this, file, ...args);
};
fs.openSync = function(file, ...args) {
  if (String(file) === settings) {
    settingsOpens++;
    if (!firedOnce && stage === 'initial_open_symlink' && settingsOpens === 1) makeSymlink();
    if (!firedOnce && stage === 'configure_read_edit' && settingsOpens === 2) edit();
    if (!firedOnce && stage === 'dryrun_read_edit' && settingsOpens === 3) edit();
    if (!firedOnce && stage === 'commit_read_edit' && settingsOpens === 5) edit();
    if (!firedOnce && stage === 'final_read_symlink' && metadataCommitted) makeSymlink();
  }
  if (!firedOnce && stage === 'post_commit_symlink' && String(file) === backup) makeSymlink();
  return original.open.call(this, file, ...args);
};
fs.writeFileSync = function(file, ...args) {
  if (!firedOnce && stage === 'backup_write_edit' && String(file) === backup) edit();
  if (!firedOnce && stage === 'metadata_write_edit' &&
      String(file).includes('.plimsoll-status-line-original.json.plimsoll-write-')) edit();
  return original.write.call(this, file, ...args);
};
fs.renameSync = function(from, to) {
  const result = original.rename.call(this, from, to);
  if (stage === 'final_read_symlink' && String(to) === backup &&
      String(from).includes('.plimsoll-write-')) metadataCommitted = true;
  return result;
};
`);

function lstat(file) {
  try { return fs.lstatSync(file); } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
}
function scratch() {
  fs.rmSync(config, { recursive: true, force: true });
  fs.mkdirSync(config, { recursive: true, mode: 0o700 });
  fs.mkdirSync(env.TMPDIR, { recursive: true, mode: 0o700 });
  fs.writeFileSync(settings, initial);
  fs.writeFileSync(target, '{"operatorTarget":true}');
  fs.rmSync(marker, { force: true });
}
function run(stage) {
  const childEnv = { ...env, R5_SETTINGS: settings, R5_BACKUP: backup,
    R5_CONFIG: config, R5_TARGET: target, R5_MARKER: marker, R5_STAGE: stage,
    NODE_OPTIONS: [env.NODE_OPTIONS, '--require=' + hook].filter(Boolean).join(' ') };
  const child = spawnSync(process.execPath,
    ['--import', loader, cli, 'setup', 'claude-status-line'],
    { cwd: repo, env: childEnv, encoding: 'utf8', timeout: 30_000 });
  let result;
  try { result = JSON.parse(child.stdout).results.find(row => row.configDir === config); }
  catch { result = null; }
  return { code: child.status, stderr: child.stderr, result };
}

try {
  for (const dir of [home, config, env.TMPDIR]) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  for (const stage of ['mkdir_symlink', 'initial_lstat_symlink', 'initial_open_symlink',
    'backup_write_edit', 'configure_read_edit', 'dryrun_read_edit', 'commit_read_edit',
    'post_commit_symlink', 'metadata_write_edit', 'final_read_symlink']) {
    scratch();
    const response = run(stage);
    const live = lstat(settings);
    const injected = fs.existsSync(marker);
    const observation = { stage, code: response.code, outcome: response.result?.outcome ?? null,
      injected, liveSymlink: Boolean(live?.isSymbolicLink()),
      operatorEditPreserved: Boolean(live?.isFile() &&
        fs.readFileSync(settings, 'utf8').includes(`"operatorEdit":"${stage}"`)),
      errorReported: /settings_changed|SOURCE_CHANGED/.test(response.stderr) };
    console.log(JSON.stringify(observation));
    assert.equal(injected, true, stage);
    if (stage.includes('symlink')) {
      assert.equal(live?.isSymbolicLink(), true, stage);
      assert.equal(fs.readlinkSync(settings), target, stage);
      assert.equal(fs.readFileSync(target, 'utf8'), '{"operatorTarget":true}', stage);
      assert.equal(response.result?.outcome, 'settings_is_symlink', response.stderr);
    } else {
      assert.equal(observation.operatorEditPreserved, true, stage);
      if (stage === 'metadata_write_edit') {
        assert.equal(response.result?.outcome, 'status_line_changed', response.stderr);
      } else assert.equal(observation.errorReported, true, response.stderr);
    }
  }
  console.log(JSON.stringify({ proof: 'claude-status-line-r5-install-steps', checks: 10, passed: 10 }));
} finally { fs.rmSync(root, { recursive: true, force: true }); }
