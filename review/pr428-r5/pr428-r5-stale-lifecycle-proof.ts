/** Stale or forged lifecycle state must not bless an owner-edited stopped agent. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { collectorConfigSchema, writeCollectorConfigTransactionally } from
  "../../packages/collector-cli/src/config";
import { installLaunchAgent, launchAgentOwnedTemplatePath } from
  "../../packages/collector-cli/src/launch-agent";

const repo = path.resolve(import.meta.dirname, "..", "..");
const root = fs.mkdtempSync(path.join(repo, "pr428-r5-stale-state-"));
const cli = path.join(repo, "packages/collector-cli/dist/cli.mjs");
const originalEnv = { ...process.env };
function trial(label: string, symlinkParent: boolean) {
  const home = path.join(root, label, "home");
  const data = path.join(home, ".plimsoll");
  const versions = path.join(data, "lifecycle", "versions");
  const runtime = path.join(versions, "0.7.43", "darwin-arm64");
  const original = path.join(runtime, "cli.mjs");
  const prior = path.join(versions, "0.7.44");
  const external = path.join(root, label, "writable-elsewhere");
  const owner = path.join(prior, "owner-cli.mjs");
  const currentInstallId = "12345678-1234-4234-8234-123456789abc";
  const staleInstallId = "22345678-1234-4234-8234-123456789abc";
  fs.mkdirSync(data, { recursive: true, mode: 0o700 });
  fs.mkdirSync(runtime, { recursive: true, mode: 0o700 });
  fs.mkdirSync(versions, { recursive: true, mode: 0o700 });
  if (symlinkParent) {
    fs.mkdirSync(external, { recursive: true, mode: 0o700 });
    fs.symlinkSync(external, prior);
  } else fs.mkdirSync(prior, { recursive: true, mode: 0o700 });
  fs.writeFileSync(original, "// current installed runtime\n", { mode: 0o600 });
  fs.writeFileSync(owner, "// stale or outside runtime\n", { mode: 0o600 });
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.PLIMSOLL_HOME = data;
  process.env.CODEX_HOME = path.join(home, ".codex");
  process.env.CLAUDE_CONFIG_DIR = path.join(home, ".claude");
  process.env.TMPDIR = path.join(home, "tmp");
  fs.mkdirSync(process.env.TMPDIR, { recursive: true, mode: 0o700 });
  const installed = installLaunchAgent({ homeDir: home, repoRoot: runtime,
    workingDirectory: runtime, programArguments: [process.execPath, original, "start"] });
  fs.rmSync(launchAgentOwnedTemplatePath(home));
  for (const name of fs.readdirSync(data)) {
    if (/^launch-agent-template-[0-9a-f]{64}\.identity\.json$/.test(name))
      fs.rmSync(path.join(data, name));
  }
  writeCollectorConfigTransactionally(collectorConfigSchema.parse({ port: 49390 }),
    path.join(data, "collector.config.json"));
  const statePath = path.join(data, "lifecycle", "state.json");
  fs.writeFileSync(path.join(data, "lifecycle", "installation.json"), `${JSON.stringify({
    schemaVersion: 1, installId: currentInstallId,
    executablePath: symlinkParent ? owner : original,
  })}\n`, { mode: 0o600 });
  fs.writeFileSync(statePath, `${JSON.stringify({ schemaVersion: 1, version: "0.7.44",
    executablePath: owner, installId: symlinkParent ? currentInstallId : staleInstallId })}\n`,
  { mode: 0o600 });
  const recordedInstallation = JSON.parse(fs.readFileSync(
    path.join(data, "lifecycle", "installation.json"), "utf8")) as {
      installId: string; executablePath: string;
    };
  const recordedState = JSON.parse(fs.readFileSync(statePath, "utf8")) as {
    installId: string; executablePath: string;
  };
  const independentEvidenceMatches = recordedInstallation.installId === recordedState.installId &&
    recordedInstallation.executablePath === recordedState.executablePath;
  const before = fs.readFileSync(installed.plistPath, "utf8");
  const changed = before.replace(`<string>${original}</string>`, `<string>${owner}</string>`)
    .replace(`<string>${runtime}</string>`, `<string>${path.dirname(owner)}</string>`);
  assert.notEqual(changed, before);
  fs.writeFileSync(installed.plistPath, changed, { mode: 0o600 });
  const joined = spawnSync(process.execPath, [cli, "join", "--token-stdin", "--url",
    "http://127.0.0.1:49390"], { env: { ...process.env, CI: "", GITHUB_ACTIONS: "" },
    input: "\n", encoding: "utf8", timeout: 20_000 });
  return { label, symlinkParent, status: /"status":\s*"([^"]+)"/.exec(joined.stdout)?.[1] ?? null,
    exit: joined.status, independentEvidenceMatches,
    ownerPath: owner, statePath, plistPath: installed.plistPath };
}
try {
  const stale = trial("stale-previous-install", false);
  const forged = trial("symlinked-versions", true);
  console.log(JSON.stringify({ stale, forged }));
  assert.equal(forged.independentEvidenceMatches, true,
    "symlinked-parent fixture must match the current installation identity and runtime");
  assert.equal(stale.status, "join_preflight_failed",
    "stale lifecycle state vouched for an edited command");
  assert.equal(forged.status, "join_preflight_failed",
    "lifecycle state followed a symlink to a writable external script");
} finally {
  for (const key of Object.keys(process.env)) if (!(key in originalEnv)) delete process.env[key];
  Object.assign(process.env, originalEnv);
  fs.rmSync(root, { recursive: true, force: true });
}
