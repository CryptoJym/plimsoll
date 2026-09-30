import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "status-line-edits-proof-"));
const home = path.join(root, "home");
const defaultDir = path.join(root, "default");
const alternate = path.join(root, "alternate");
const newDir = path.join(root, "new");
const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
const cli = path.resolve("packages/collector-cli/src/cli.ts");
const env = { ...process.env, HOME: home, USERPROFILE: home, CODEX_HOME: path.join(home, ".codex"),
  PLIMSOLL_HOME: path.join(home, ".plimsoll"), CLAUDE_CONFIG_DIR: defaultDir,
  PLIMSOLL_FIXTURE_ROOT: root,
  XDG_CONFIG_HOME: path.join(home, ".config"), XDG_CACHE_HOME: path.join(home, ".cache"),
  XDG_STATE_HOME: path.join(home, ".local", "state"), TMPDIR: path.join(home, "tmp"),
  NEXT_TELEMETRY_DISABLED: "1" };
const runCli = (...args: string[]) => {
  const result = spawnSync(process.execPath, ["--import", loader, cli, "setup", "claude-status-line", ...args],
    { env, encoding: "utf8", timeout: 30_000 });
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout) as { results: Array<{ configDir: string; outcome: string }> };
};

try {
  for (const dir of [home, defaultDir, alternate, newDir, env.TMPDIR, env.PLIMSOLL_HOME, env.CODEX_HOME])
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const originalStatusLine = { type: "command", command: "printf original", padding: 2, refreshInterval: 60 };
  const settingsPath = path.join(alternate, "settings.json");
  fs.writeFileSync(settingsPath, JSON.stringify({ statusLine: originalStatusLine, theme: "dark" }));
  runCli("--config-dir", alternate);
  const installed = JSON.parse(fs.readFileSync(settingsPath, "utf8"));
  assert.equal(installed.statusLine.padding, 2);
  assert.equal(installed.statusLine.refreshInterval, 60);
  assert.match(installed.statusLine.command, /__plimsoll-capacity-statusline-proxy/);

  installed.theme = "light";
  installed.operatorChangedAfterInstall = true;
  fs.writeFileSync(settingsPath, JSON.stringify(installed));
  const removed = runCli("--config-dir", alternate, "--uninstall");
  assert.equal(removed.results.find(row => row.configDir === alternate)?.outcome, "restored");
  const after = JSON.parse(fs.readFileSync(settingsPath, "utf8"));
  assert.deepEqual(after.statusLine, originalStatusLine);
  assert.equal(after.theme, "light");
  assert.equal(after.operatorChangedAfterInstall, true);

  runCli("--config-dir", alternate);
  const changed = JSON.parse(fs.readFileSync(settingsPath, "utf8"));
  changed.statusLine.command = "printf changed-by-operator";
  fs.writeFileSync(settingsPath, JSON.stringify(changed));
  const left = runCli("--config-dir", alternate, "--uninstall");
  assert.equal(left.results.find(row => row.configDir === alternate)?.outcome, "status_line_changed");
  assert.deepEqual(JSON.parse(fs.readFileSync(settingsPath, "utf8")), changed);
  assert.ok(fs.existsSync(path.join(alternate, ".plimsoll-status-line-original.json")));

  runCli("--config-dir", newDir);
  const newSettings = path.join(newDir, "settings.json");
  const withEdit = JSON.parse(fs.readFileSync(newSettings, "utf8"));
  withEdit.operatorOnly = "kept";
  fs.writeFileSync(newSettings, JSON.stringify(withEdit));
  runCli("--config-dir", newDir, "--uninstall");
  assert.deepEqual(JSON.parse(fs.readFileSync(newSettings, "utf8")), { operatorOnly: "kept" });
  console.log(JSON.stringify({ proof: "claude-status-line-edits", paddingPreserved: true,
    refreshIntervalPreserved: true, otherEditsPreserved: true, changedStatusLineLeftAlone: true,
    newFileOperatorEditPreserved: true }));
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
