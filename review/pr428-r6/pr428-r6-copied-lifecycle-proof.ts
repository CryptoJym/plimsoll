/** A copied pair of lifecycle records must not become this install's identity. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { collectorConfigSchema, writeCollectorConfigTransactionally } from
  "../../packages/collector-cli/src/config";
import { installLaunchAgent, launchAgentOwnedTemplatePath } from
  "../../packages/collector-cli/src/launch-agent";

const repo = path.resolve(import.meta.dirname, "..", "..");
const root = fs.mkdtempSync(path.join(repo, "pr428-r6-copied-install-"));
const previousEnv = { ...process.env };
try {
  const home = path.join(root, "home");
  const data = path.join(home, ".plimsoll");
  const versions = path.join(data, "lifecycle", "versions");
  const current = path.join(versions, "0.7.43", "darwin-arm64", "cli.mjs");
  const previous = path.join(versions, "0.7.44", "darwin-arm64", "cli.mjs");
  fs.mkdirSync(path.dirname(current), { recursive: true, mode: 0o700 });
  fs.mkdirSync(path.dirname(previous), { recursive: true, mode: 0o700 });
  fs.writeFileSync(current, "// installed runtime A\n", { mode: 0o600 });
  fs.writeFileSync(previous, "// previous installation B\n", { mode: 0o600 });
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.PLIMSOLL_HOME = data;
  process.env.CODEX_HOME = path.join(home, ".codex");
  process.env.CLAUDE_CONFIG_DIR = path.join(home, ".claude");
  process.env.TMPDIR = path.join(home, "tmp");
  fs.mkdirSync(process.env.TMPDIR, { recursive: true, mode: 0o700 });
  const installed = installLaunchAgent({ homeDir: home, repoRoot: path.dirname(current),
    workingDirectory: path.dirname(current),
    programArguments: [process.execPath, current, "start"] });
  const currentLink = path.join(data, "lifecycle", "current");
  fs.symlinkSync(path.dirname(path.dirname(current)), currentLink, "dir");
  fs.rmSync(launchAgentOwnedTemplatePath(home));
  for (const name of fs.readdirSync(data)) {
    if (/^launch-agent-template-[0-9a-f]{64}\.identity\.json$/.test(name))
      fs.rmSync(path.join(data, name));
  }
  writeCollectorConfigTransactionally(collectorConfigSchema.parse({ port: 49390 }),
    path.join(data, "collector.config.json"));

  // Restore both internally consistent records from the previous install's
  // backup. Their ID is not the identity of the LaunchAgent just installed.
  const backup = path.join(root, "previous-install-backup");
  fs.mkdirSync(backup, { recursive: true, mode: 0o700 });
  const oldId = "22345678-1234-4234-8234-123456789abc";
  fs.writeFileSync(path.join(backup, "installation.json"), `${JSON.stringify({
    schemaVersion: 1, installId: oldId, executablePath: previous })}\n`, { mode: 0o600 });
  fs.writeFileSync(path.join(backup, "state.json"), `${JSON.stringify({
    schemaVersion: 1, version: "0.7.44", executablePath: previous,
    installId: oldId })}\n`, { mode: 0o600 });
  for (const name of ["installation.json", "state.json"])
    fs.copyFileSync(path.join(backup, name), path.join(data, "lifecycle", name));
  const copiedInstallation = JSON.parse(fs.readFileSync(path.join(data, "lifecycle",
    "installation.json"), "utf8")) as { installId: string; executablePath: string };
  const copiedState = JSON.parse(fs.readFileSync(path.join(data, "lifecycle",
    "state.json"), "utf8")) as { installId: string; executablePath: string };
  const copiedRecordsMatch = copiedInstallation.installId === copiedState.installId &&
    copiedInstallation.executablePath === copiedState.executablePath;
  assert.ok(copiedRecordsMatch);
  assert.notEqual(fs.realpathSync(currentLink), path.dirname(path.dirname(previous)));

  const before = fs.readFileSync(installed.plistPath, "utf8");
  const edited = before.replace(`<string>${current}</string>`, `<string>${previous}</string>`)
    .replace(`<string>${path.dirname(current)}</string>`,
      `<string>${path.dirname(previous)}</string>`);
  assert.notEqual(edited, before);
  fs.writeFileSync(installed.plistPath, edited, { mode: 0o600 });
  const cli = path.join(repo, "packages/collector-cli/dist/cli.mjs");
  const child = spawnSync(process.execPath, [cli, "join", "--token-stdin", "--url",
    "http://127.0.0.1:49390"], { env: { ...process.env, CI: "", GITHUB_ACTIONS: "" },
    input: "\n", encoding: "utf8", timeout: 20_000 });
  const status = /"status":\s*"([^"]+)"/.exec(child.stdout)?.[1] ?? null;
  console.log(JSON.stringify({ status, exit: child.status, copiedInstallId: oldId,
    currentProgram: current, copiedProgram: previous,
    currentLinkTarget: fs.realpathSync(currentLink),
    copiedStateMatchesCopiedInstallation: copiedRecordsMatch }));
  assert.equal(status, "join_preflight_failed",
    "copied lifecycle records vouched for an owner-edited stopped agent");
} finally {
  for (const key of Object.keys(process.env)) if (!(key in previousEnv)) delete process.env[key];
  Object.assign(process.env, previousEnv);
  fs.rmSync(root, { recursive: true, force: true });
}
