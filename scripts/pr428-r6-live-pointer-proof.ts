/** A copied lifecycle pair cannot vouch for a stopped owner's edited command. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { collectorConfigSchema, writeCollectorConfigTransactionally } from
  "../packages/collector-cli/src/config";
import { installLaunchAgent, launchAgentOwnedTemplatePath } from
  "../packages/collector-cli/src/launch-agent";

const repo = path.resolve(import.meta.dirname, "..");
const root = fs.mkdtempSync(path.join(repo, "pr428-r6-live-pointer-"));
const cli = path.join(repo, "packages/collector-cli/dist/cli.mjs");
const saved = { ...process.env };
try {
  const home = path.join(root, "home");
  const support = path.join(home, ".plimsoll");
  const lifecycle = path.join(support, "lifecycle");
  const a = path.join(lifecycle, "versions", "0.7.44", "darwin-arm64");
  const b = path.join(lifecycle, "versions", "0.7.45", "darwin-arm64");
  const runtimeA = path.join(a, "cli.mjs");
  const runtimeB = path.join(b, "cli.mjs");
  for (const directory of [support, a, b, path.join(home, "tmp")])
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  fs.writeFileSync(runtimeA, "// installed runtime A\n", { mode: 0o600 });
  fs.writeFileSync(runtimeB, "// copied runtime B\n", { mode: 0o600 });
  fs.symlinkSync(a, path.join(lifecycle, "current"), "dir");
  Object.assign(process.env, { HOME: home, USERPROFILE: home, PLIMSOLL_HOME: support,
    CODEX_HOME: path.join(home, ".codex"), CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
    TMPDIR: path.join(home, "tmp") });
  const installed = installLaunchAgent({ homeDir: home, repoRoot: a,
    workingDirectory: a, programArguments: [process.execPath, runtimeA, "start"] });
  fs.rmSync(launchAgentOwnedTemplatePath(home));
  for (const name of fs.readdirSync(support))
    if (/^launch-agent-template-[0-9a-f]{64}\.identity\.json$/.test(name))
      fs.rmSync(path.join(support, name));
  writeCollectorConfigTransactionally(collectorConfigSchema.parse({ port: 49390 }),
    path.join(support, "collector.config.json"));
  const installId = "22345678-1234-4234-8234-123456789abc";
  fs.writeFileSync(path.join(lifecycle, "installation.json"), `${JSON.stringify({
    schemaVersion: 1, installId, executablePath: runtimeB })}\n`, { mode: 0o600 });
  fs.writeFileSync(path.join(lifecycle, "state.json"), `${JSON.stringify({
    schemaVersion: 1, version: "0.7.45", executablePath: runtimeB, installId })}\n`,
  { mode: 0o600 });
  const before = fs.readFileSync(installed.plistPath, "utf8");
  const edited = before.replaceAll(runtimeA, runtimeB).replaceAll(a, b);
  assert.notEqual(edited, before);
  fs.writeFileSync(installed.plistPath, edited, { mode: 0o600 });
  const result = spawnSync(process.execPath, [cli, "join", "--token-stdin", "--url",
    "http://127.0.0.1:49390"], {
    env: { ...process.env, CI: "", GITHUB_ACTIONS: "" }, input: "\n",
    encoding: "utf8", timeout: 20_000,
  });
  const status = /"status":\s*"([^"]+)"/.exec(result.stdout)?.[1] ?? null;
  console.log(JSON.stringify({ proof: "pr428-r6-live-pointer", status, exit: result.status,
    current: fs.realpathSync(path.join(lifecycle, "current")), records: runtimeB }));
  assert.equal(status, "join_preflight_failed");
  assert.notEqual(result.status, 0);
} finally {
  for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
  Object.assign(process.env, saved);
  fs.rmSync(root, { recursive: true, force: true });
}
