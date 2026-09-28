/** Directed review regressions for PR #428. Run after runtime:build in an isolated HOME. */
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { discoverCaptureRootCandidates } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema, writeCollectorConfigTransactionally } from "../packages/collector-cli/src/config";
import { installLaunchAgent, inspectLaunchAgentOwnership, launchAgentOwnedTemplatePath } from
  "../packages/collector-cli/src/launch-agent";

const root = fs.mkdtempSync(path.join(process.cwd(), "pr428-r3-review-"));
const current = path.join(process.cwd(), "packages/collector-cli/dist/cli.mjs");
const failures: string[] = [];
const selected = process.argv[2] ?? "all";
if (!["all", "r3-3", "r3-4", "r3-5", "r3-6"].includes(selected))
  throw new Error(`Unknown round-three review case: ${selected}`);
const active = (name: string) => selected === "all" || selected === name;
function check(name: string, condition: boolean, observed: unknown) {
  console.log(JSON.stringify({ name, status: condition ? "PASS" : "FAIL", observed }));
  if (!condition) failures.push(name);
}
function withHome<T>(home: string, data: string | null, action: () => T): T {
  const oldHome = process.env.HOME, oldData = process.env.PLIMSOLL_HOME;
  process.env.HOME = home;
  if (data === null) delete process.env.PLIMSOLL_HOME;
  else process.env.PLIMSOLL_HOME = data;
  try { return action(); }
  finally {
    if (oldHome === undefined) delete process.env.HOME; else process.env.HOME = oldHome;
    if (oldData === undefined) delete process.env.PLIMSOLL_HOME; else process.env.PLIMSOLL_HOME = oldData;
  }
}
function manifestFixture(name: string, runtime = current, dataOverride: boolean | null = true) {
  const home = path.join(root, name), data = path.join(home, ".plimsoll");
  fs.mkdirSync(data, { recursive: true, mode: 0o700 });
  const options = { homeDir: home, repoRoot: path.dirname(runtime),
    workingDirectory: path.dirname(runtime), programArguments: [process.execPath, runtime, "start"] };
  const configuredData = dataOverride ? data : null;
  const installed = withHome(home, configuredData, () => installLaunchAgent(options));
  return { home, data, options, installed, configuredData };
}
function ownerKeys(fixture: ReturnType<typeof manifestFixture>) {
  return withHome(fixture.home, fixture.configuredData, () => inspectLaunchAgentOwnership({
    homeDir: fixture.home,
    legacyRuntime: { programArguments: fixture.options.programArguments,
      workingDirectory: fixture.options.workingDirectory },
  }).ownerEditedKeys);
}
function addPathEntry(plistPath: string) {
  const original = fs.readFileSync(plistPath, "utf8");
  const edited = original.replace(/(<key>PATH<\/key>\s*<string>)([^<]*)(<\/string>)/,
    (_match, open: string, value: string, close: string) => `${open}${value}:/opt/owner-custom-bin${close}`);
  if (edited === original) throw new Error("PATH fixture edit did not apply");
  fs.writeFileSync(plistPath, edited, { mode: 0o600 });
  return edited;
}
function joinStatus(home: string, data: string) {
  const env = { ...process.env, HOME: home, USERPROFILE: home, PLIMSOLL_HOME: data,
    CODEX_HOME: path.join(home, ".codex"), CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
    XDG_CONFIG_HOME: path.join(home, ".config"), XDG_CACHE_HOME: path.join(home, ".cache"),
    XDG_STATE_HOME: path.join(home, ".local/state"), TMPDIR: path.join(home, "tmp") };
  fs.mkdirSync(env.TMPDIR, { recursive: true, mode: 0o700 });
  const run = spawnSync(process.execPath, [current, "join", "--token-stdin", "--url",
    "http://127.0.0.1:49390"], { env, input: "\n", encoding: "utf8", timeout: 20_000 });
  return { exit: run.status, status: /"status":\s*"([^"]+)"/.exec(run.stdout)?.[1] ?? null };
}
try {
  if (active("r3-3")) {
  const fakeHome = path.join(root, "unrelated-studio");
  const fake = path.join(fakeHome, ".clientai/studio/borg/conductors/not-an-agent/profile/sessions/2026/09/28");
  fs.mkdirSync(fake, { recursive: true, mode: 0o700 });
  const invalidId = `deadbeef-${"-".repeat(27)}`;
  fs.writeFileSync(path.join(fake, `rollout-2026-09-28T00-00-00-${invalidId}.jsonl`),
    `${JSON.stringify({ type: "session_meta", timestamp: "2026-09-28T00:00:00Z",
      payload: { id: invalidId } })}\n`);
  const candidate = discoverCaptureRootCandidates(fakeHome).find((entry) =>
    entry.shape === "studio_codex_conductor");
  check("invalid_codex_rollout_stays_out_of_auto_enrollment", candidate?.autoEnroll === false,
    { autoEnroll: candidate?.autoEnroll, reason: candidate?.reason ?? null });
  }

  if (active("r3-4")) {
  const extraHome = manifestFixture("extra-home", current, false);
  const original = fs.readFileSync(extraHome.installed.plistPath, "utf8");
  const edited = original.replace("<key>EnvironmentVariables</key>\n  <dict>",
    `<key>EnvironmentVariables</key>\n  <dict>\n    <key>PLIMSOLL_HOME</key>\n    <string>${path.join(extraHome.home, "other-data")}</string>`);
  if (edited === original) throw new Error("PLIMSOLL_HOME fixture edit did not apply");
  fs.writeFileSync(extraHome.installed.plistPath, edited, { mode: 0o600 });
  const extraKeys = ownerKeys(extraHome);
  check("owner_added_home_is_detected_against_owned_template",
    extraKeys.includes("EnvironmentVariables.PLIMSOLL_HOME"), extraKeys);
  }

  if (active("r3-6")) {
  const removed = manifestFixture("removed-template");
  const template = launchAgentOwnedTemplatePath(removed.home);
  fs.rmSync(template);
  const missingState = withHome(removed.home, removed.data, () =>
    inspectLaunchAgentOwnership({ homeDir: removed.home }).evidence);
  check("deleted_template_is_named_missing", String(missingState) === "template_missing", missingState);
  const noop = withHome(removed.home, removed.data, () => installLaunchAgent(removed.options));
  addPathEntry(removed.installed.plistPath);
  const removedKeys = ownerKeys(removed);
  check("deleted_template_cannot_hide_owner_path_edit", fs.existsSync(template) ||
    removedKeys.includes("EnvironmentVariables.PATH"),
    { exactInstallStatus: noop.receipt.status, templateExists: fs.existsSync(template), editedKeys: removedKeys });

  const tampered = manifestFixture("edited-template");
  const tamperedManifest = addPathEntry(tampered.installed.plistPath);
  const tamperedTemplate = launchAgentOwnedTemplatePath(tampered.home);
  const object = JSON.parse(fs.readFileSync(tamperedTemplate, "utf8")) as {
    manifest: string; manifestDigest: string;
  };
  object.manifest = tamperedManifest;
  object.manifestDigest = `sha256:${createHash("sha256").update(tamperedManifest).digest("hex")}`;
  fs.writeFileSync(tamperedTemplate, `${JSON.stringify(object)}\n`, { mode: 0o600 });
  const tamperedInspection = withHome(tampered.home, tampered.data, () =>
    inspectLaunchAgentOwnership({ homeDir: tampered.home,
      legacyRuntime: { programArguments: tampered.options.programArguments,
        workingDirectory: tampered.options.workingDirectory } }));
  const tamperedKeys = tamperedInspection.ownerEditedKeys;
  check("self_consistent_template_edit_does_not_erase_owner_edit", tamperedKeys.length > 0, tamperedKeys);
  check("edited_template_is_named_changed", String(tamperedInspection.evidence) === "template_changed",
    tamperedInspection.evidence);
  }

  if (active("r3-5")) {
  const priorRuntime = path.join(root, "old-runtime", "cli.mjs");
  fs.mkdirSync(path.dirname(priorRuntime), { recursive: true, mode: 0o700 });
  fs.writeFileSync(priorRuntime, "// fixture runtime path\n", { mode: 0o600 });
  const stopped = manifestFixture("stopped-pre-template", priorRuntime);
  fs.rmSync(launchAgentOwnedTemplatePath(stopped.home));
  writeCollectorConfigTransactionally(collectorConfigSchema.parse({ port: 49390 }),
    path.join(stopped.data, "collector.config.json"));
  const joined = joinStatus(stopped.home, stopped.data);
  check("stopped_legacy_without_owner_edits_passes_preflight",
    joined.status !== "join_preflight_failed", joined);
  }
} finally { fs.rmSync(root, { recursive: true, force: true }); }
console.log(JSON.stringify({ proof: "pr428-r3-review-defects", selected, failures }));
if (failures.length) process.exitCode = 1;
