import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const repo = path.resolve(import.meta.dirname, '..');
const loader = path.join(repo, 'node_modules/tsx/dist/loader.mjs');
const cli = path.join(repo, 'packages/collector-cli/src/cli.ts');
const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'claude-status-r5-'));
const home = path.join(root, 'home');
const config = path.join(root, 'claude');
const settings = path.join(config, 'settings.json');
const backup = path.join(config, '.plimsoll-status-line-original.json');
const target = path.join(root, 'operator-target.json');
const marker = path.join(root, 'injection-fired');
const hook = path.join(root, 'race-hook.cjs');
const initial = Buffer.from('{\n  "statusLine":{"type":"command","command":"printf original","padding":2,"refreshInterval":60},\n  "theme":"dark"\n}\n');
const env = { ...process.env, HOME: home, USERPROFILE: home,
  CLAUDE_CONFIG_DIR: config, CODEX_HOME: path.join(home, '.codex'),
  PLIMSOLL_HOME: path.join(home, '.plimsoll'), PLIMSOLL_FIXTURE_ROOT: root,
  XDG_CONFIG_HOME: path.join(home, '.config'),
  XDG_CACHE_HOME: path.join(home, '.cache'),
  XDG_STATE_HOME: path.join(home, '.local/state'),
  TMPDIR: path.join(home, 'tmp'), NEXT_TELEMETRY_DISABLED: '1' };

function lstat(file) {
  try { return fs.lstatSync(file); } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
}
function cliResult(uninstall = false, stage = 'normal', inheritedFd) {
  const childEnv = { ...env, R5_SETTINGS: settings, R5_TARGET: target,
    R5_MARKER: marker, R5_STAGE: stage };
  if (stage !== 'normal') childEnv.NODE_OPTIONS = [env.NODE_OPTIONS,
    '--require=' + hook].filter(Boolean).join(' ');
  const child = spawnSync(process.execPath,
    ['--import', loader, cli, 'setup', 'claude-status-line',
      ...(uninstall ? ['--uninstall'] : [])],
    { cwd: repo, env: childEnv, encoding: 'utf8', timeout: 30_000,
      stdio: inheritedFd === undefined ? ['ignore', 'pipe', 'pipe'] :
        ['ignore', 'pipe', 'pipe', inheritedFd] });
  let result;
  try { result = JSON.parse(child.stdout).results.find(row => row.configDir === config); }
  catch { result = null; }
  return { code: child.status, stderr: child.stderr, result };
}
function scratch(initiallyMissing = false) {
  fs.rmSync(config, { recursive: true, force: true });
  fs.mkdirSync(config, { recursive: true, mode: 0o700 });
  fs.mkdirSync(env.TMPDIR, { recursive: true, mode: 0o700 });
  if (!initiallyMissing) fs.writeFileSync(settings, initial);
  fs.writeFileSync(target, '{"operatorTarget":true}');
  fs.rmSync(marker, { force: true });
  const install = cliResult();
  assert.equal(install.code, 0, install.stderr);
  assert.equal(install.result?.outcome, initiallyMissing ? 'installed' : 'chained');
}

fs.writeFileSync(hook, String.raw`
const fs = require('node:fs');
const original = { rename: fs.renameSync, open: fs.openSync, close: fs.closeSync,
  unlink: fs.unlinkSync, write: fs.writeFileSync, symlink: fs.symlinkSync };
const settings = process.env.R5_SETTINGS;
const target = process.env.R5_TARGET;
const marker = process.env.R5_MARKER;
const stage = process.env.R5_STAGE;
let side, sideOpens = 0, finalSideDescriptor;
function fired() { original.write(marker, stage); }
fs.renameSync = function(from, to) {
  const moving = String(from) === settings && String(to).includes('.plimsoll-uninstall-');
  if (moving && (stage === 'symlink_before_rename' ||
      stage === 'dangling_symlink_before_rename')) {
    original.unlink(settings);
    original.symlink(stage === 'dangling_symlink_before_rename' ?
      target + '-missing' : target, settings);
    fired();
  }
  const result = original.rename.call(this, from, to);
  if (moving) side = String(to);
  if (moving && stage === 'new_file_after_move') {
    original.write(settings, '{"operatorNewFile":true}');
    fired();
  }
  if (moving && stage === 'dangling_symlink_after_move_initially_missing') {
    original.symlink(target + '-missing', settings);
    fired();
  }
  return result;
};
fs.openSync = function(file, ...args) {
  if (String(file) === settings && stage === 'symlink_before_initial_open') {
    original.unlink(settings);
    original.symlink(target, settings);
    fired();
  }
  if (side && String(file) === side) {
    sideOpens++;
    if (stage === 'symlink_before_repair_write' && sideOpens === 1) {
      fs.ftruncateSync(3, 0);
      fs.writeSync(3, '{"operatorEditBeforeRepair":true}', 0, 'utf8');
    }
    if (stage === 'late_side_edit' && sideOpens === 2) {
      fs.ftruncateSync(3, 0);
      fs.writeSync(3, '{"lateOperatorEdit":true}', 0, 'utf8');
      fired();
    }
  }
  const descriptor = original.open.call(this, file, ...args);
  if (side && String(file) === side && sideOpens === 2) finalSideDescriptor = descriptor;
  return descriptor;
};
fs.writeFileSync = function(file, ...args) {
  if (String(file) === settings &&
      (stage === 'symlink_before_restore_write' || stage === 'symlink_before_repair_write')) {
    original.symlink(target, settings);
    fired();
  }
  return original.write.call(this, file, ...args);
};
fs.closeSync = function(descriptor) {
  const result = original.close.call(this, descriptor);
  if (stage === 'symlink_after_final_side_read' && descriptor === finalSideDescriptor) {
    original.unlink(settings);
    original.symlink(target, settings);
    fired();
  }
  return result;
};
`);

function checkRestoreAdversarial() {
  for (const stage of ['normal', 'new_file_after_move', 'symlink_before_rename',
    'dangling_symlink_before_rename', 'dangling_symlink_after_move_initially_missing',
    'late_side_edit', 'symlink_after_final_side_read',
    'symlink_before_restore_write', 'symlink_before_repair_write']) {
    scratch(stage === 'dangling_symlink_after_move_initially_missing');
    const descriptor = ['late_side_edit', 'symlink_before_repair_write'].includes(stage)
      ? fs.openSync(settings, 'r+') : undefined;
    let response;
    try { response = cliResult(true, stage, descriptor); }
    finally { if (descriptor !== undefined) fs.closeSync(descriptor); }
    const sides = fs.readdirSync(config).filter(name => name.startsWith('settings.json.plimsoll-uninstall-'));
    const retained = response.result?.retainedPath;
    const live = lstat(settings);
    const side = sides.length === 1 ? path.join(config, sides[0]) : null;
    const observation = { stage, code: response.code, outcome: response.result?.outcome,
      injected: fs.existsSync(marker), retainedPath: retained ?? null,
      sideCount: sides.length, liveSymlink: Boolean(live?.isSymbolicLink()),
      sideSymlink: Boolean(side && lstat(side)?.isSymbolicLink()),
      lateEditRetained: Boolean(side && !lstat(side)?.isSymbolicLink() &&
        fs.readFileSync(side).includes(Buffer.from('lateOperatorEdit'))) };
    console.log(JSON.stringify(observation));
    assert.equal(response.code, 0, response.stderr);
    assert.equal(sides.length, 1, stage);
    assert.equal(retained, side, stage);
    assert.equal(lstat(backup)?.isFile(), true, stage);
    if (stage !== 'normal') assert.equal(observation.injected, true, stage);
    if (stage === 'normal') {
      assert.equal(response.result?.outcome, 'restored');
      assert.ok(fs.readFileSync(settings).equals(initial));
      assert.ok(fs.readFileSync(side).includes(Buffer.from('__plimsoll-capacity-statusline-proxy')));
    } else if (stage === 'new_file_after_move') {
      assert.equal(response.result?.outcome, 'status_line_changed');
      assert.equal(fs.readFileSync(settings, 'utf8'), '{"operatorNewFile":true}');
    } else if (stage === 'late_side_edit') {
      assert.equal(response.result?.outcome, 'status_line_changed');
      assert.equal(observation.lateEditRetained, true);
    } else {
      assert.equal(response.result?.outcome, 'settings_is_symlink');
      assert.equal(observation.liveSymlink, true);
      assert.equal(lstat(settings)?.isSymbolicLink(), true);
      if (stage.endsWith('before_rename')) {
        assert.equal(observation.sideSymlink, true);
        assert.equal(fs.readlinkSync(settings), fs.readlinkSync(side));
      }
      assert.equal(fs.readFileSync(target, 'utf8'), '{"operatorTarget":true}');
    }
  }
  console.log(JSON.stringify({ proof: 'claude-status-line-r5-adversarial', checks: 9, passed: 9 }));
}

function checkReinstallAfterUninstall() {
  scratch();
  assert.equal(cliResult(true).result?.outcome, 'restored');
  const revised = Buffer.from('{"statusLine":{"type":"command","command":"printf revised","padding":4},"theme":"light"}\n');
  fs.writeFileSync(settings, revised);
  const reinstall = cliResult();
  assert.equal(reinstall.code, 0, reinstall.stderr);
  assert.equal(reinstall.result?.outcome, 'chained');
  assert.equal(typeof reinstall.result.metadataRetainedPath, 'string');
  assert.equal(fs.lstatSync(reinstall.result.metadataRetainedPath).isFile(), true);
  assert.equal(cliResult(true).result?.outcome, 'restored');
  assert.ok(fs.readFileSync(settings).equals(revised));
  console.log(JSON.stringify({ proof: 'claude-status-line-r5-reinstall',
    revisedStatusLineRestored: true, oldMetadataRetained: true }));
}

function checkPreMoveSymlinks() {
  for (const stage of ['symlink_at_start', 'symlink_before_initial_open']) {
    scratch();
    if (stage === 'symlink_at_start') {
      fs.unlinkSync(settings);
      fs.symlinkSync(target, settings);
    }
    const response = cliResult(true, stage === 'symlink_at_start' ? 'normal' : stage);
    assert.equal(response.code, 0, response.stderr);
    assert.equal(response.result?.outcome, 'settings_is_symlink');
    assert.equal(fs.lstatSync(settings).isSymbolicLink(), true);
    assert.equal(fs.readlinkSync(settings), target);
    assert.equal(fs.readFileSync(target, 'utf8'), '{"operatorTarget":true}');
    assert.equal(fs.readdirSync(config).filter(name => name.startsWith('settings.json.plimsoll-uninstall-')).length, 0);
    if (stage !== 'symlink_at_start') assert.equal(fs.existsSync(marker), true);
    console.log(JSON.stringify({ stage, outcome: response.result.outcome,
      operatorSymlinkKept: true, movedOriginal: false }));
  }
}

try {
  for (const dir of [home, config, env.TMPDIR]) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  checkRestoreAdversarial();
  checkPreMoveSymlinks();
  checkReinstallAfterUninstall();
} finally { fs.rmSync(root, { recursive: true, force: true }); }
