/** A pre-template install using the invoking CLI survives later shell PATH drift. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { collectorConfigSchema, writeCollectorConfigTransactionally } from
  "../packages/collector-cli/src/config";
import { installLaunchAgent, inspectLaunchAgentOwnership, launchAgentOwnedTemplatePath } from
  "../packages/collector-cli/src/launch-agent";

const repo = path.resolve(import.meta.dirname, "..");
const root = fs.mkdtempSync(path.join(repo, "pr428-r4-legacy-path-"));
const home = path.join(root, "home");
const data = path.join(home, ".plimsoll");
const cli = path.join(repo, "packages/collector-cli/dist/cli.mjs");
const originalEnv = { ...process.env };
try {
  fs.mkdirSync(data, { recursive: true, mode: 0o700 });
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.PLIMSOLL_HOME = data;
  process.env.CODEX_HOME = path.join(home, ".codex");
  process.env.CLAUDE_CONFIG_DIR = path.join(home, ".claude");
  process.env.TMPDIR = path.join(home, "tmp");
  fs.mkdirSync(process.env.TMPDIR, { recursive: true, mode: 0o700 });
  const runtime = { programArguments: [process.execPath, cli, "start"],
    workingDirectory: path.dirname(cli) };
  const options = { homeDir: home, repoRoot: path.dirname(cli), ...runtime };
  installLaunchAgent(options);
  assert.ok(fs.existsSync(launchAgentOwnedTemplatePath(home)));
  for (const name of fs.readdirSync(data)) {
    if (/^launch-agent-template-[0-9a-f]{64}\.identity\.json$/.test(name))
      fs.rmSync(path.join(data, name));
  }
  fs.rmSync(launchAgentOwnedTemplatePath(home));
  writeCollectorConfigTransactionally(collectorConfigSchema.parse({ port: 49390 }),
    path.join(data, "collector.config.json"));
  process.env.PATH = `${process.env.PATH}:/opt/new-toolchain`;
  const ownership = inspectLaunchAgentOwnership({ homeDir: home, legacyRuntime: runtime });
  const joined = spawnSync(process.execPath, [cli, "join", "--token-stdin", "--url",
    "http://127.0.0.1:49390"], { env: { ...process.env }, input: "\n",
    encoding: "utf8", timeout: 20_000 });
  const status = /"status":\s*"([^"]+)"/.exec(joined.stdout)?.[1] ?? null;
  console.log(JSON.stringify({ evidence: ownership.evidence,
    ownerEditedKeys: ownership.ownerEditedKeys, runtimeDriftKeys: ownership.runtimeDriftKeys,
    joinStatus: status, joinExit: joined.status }));
  assert.ok(!ownership.ownerEditedKeys.includes("EnvironmentVariables.PATH"),
    "shell PATH drift was misclassified as an owner edit on a legacy template");
  assert.notEqual(status, "join_preflight_failed",
    "join refused before reading its token on a legacy template and changed shell PATH");
} finally {
  for (const key of Object.keys(process.env)) if (!(key in originalEnv)) delete process.env[key];
  Object.assign(process.env, originalEnv);
  fs.rmSync(root, { recursive: true, force: true });
}
