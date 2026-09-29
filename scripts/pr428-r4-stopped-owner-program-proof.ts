/** A stopped template-less agent must not define its own trusted executable. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { collectorConfigSchema, writeCollectorConfigTransactionally } from
  "../packages/collector-cli/src/config";
import { installLaunchAgent, launchAgentOwnedTemplatePath } from
  "../packages/collector-cli/src/launch-agent";

const repo = path.resolve(import.meta.dirname, "..");
const root = fs.mkdtempSync(path.join(repo, "pr428-r4-stopped-program-"));
const home = path.join(root, "home");
const data = path.join(home, ".plimsoll");
const runtime = path.join(root, "installed-runtime");
const original = path.join(runtime, "cli.mjs");
const owner = path.join(runtime, "owner-cli.mjs");
const cli = path.join(repo, "packages/collector-cli/dist/cli.mjs");
const originalEnv = { ...process.env };
try {
  fs.mkdirSync(data, { recursive: true, mode: 0o700 });
  fs.mkdirSync(runtime, { recursive: true, mode: 0o700 });
  fs.writeFileSync(original, "// installed runtime fixture\n", { mode: 0o600 });
  fs.writeFileSync(owner, "// owner alternative fixture\n", { mode: 0o600 });
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
  const before = fs.readFileSync(installed.plistPath, "utf8");
  const changed = before.replace(`<string>${original}</string>`, `<string>${owner}</string>`);
  assert.notEqual(changed, before);
  fs.writeFileSync(installed.plistPath, changed, { mode: 0o600 });
  // The outer workflow is CI; the fixture child must exercise owner setup.
  const setupEnv = { ...process.env, CI: "", GITHUB_ACTIONS: "" };
  const joined = spawnSync(process.execPath, [cli, "join", "--token-stdin", "--url",
    "http://127.0.0.1:49390"], { env: setupEnv, input: "\n",
    encoding: "utf8", timeout: 20_000 });
  const status = /"status":\s*"([^"]+)"/.exec(joined.stdout)?.[1] ?? null;
  console.log(JSON.stringify({ status, exit: joined.status,
    ownerScriptExists: fs.existsSync(owner), lifecycleStateExists:
      fs.existsSync(path.join(data, "lifecycle", "state.json")),
    templateExists: fs.existsSync(launchAgentOwnedTemplatePath(home)) }));
  assert.equal(status, "join_preflight_failed",
    "join accepted an owner-edited executable from a stopped template-less agent");
  assert.ok(joined.stderr.includes("ProgramArguments") &&
    joined.stderr.includes("--replace-launch-agent"),
    "the refusal must name the edit and the explicit replacement path");
  const explicit = spawnSync(process.execPath, [cli, "join", "--replace-launch-agent",
    "--token-stdin", "--url", "http://127.0.0.1:49390"],
    { env: setupEnv, input: "\n", encoding: "utf8", timeout: 20_000 });
  const explicitStatus = /"status":\s*"([^"]+)"/.exec(explicit.stdout)?.[1] ?? null;
  assert.equal(explicitStatus, "join_failed",
    "explicit replacement should pass local preflight before reading a token");
} finally {
  for (const key of Object.keys(process.env)) if (!(key in originalEnv)) delete process.env[key];
  Object.assign(process.env, originalEnv);
  fs.rmSync(root, { recursive: true, force: true });
}
