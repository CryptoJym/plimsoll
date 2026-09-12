/**
 * Fleet Codex seat profile config proof (bead eco-6hoxj.52).
 *
 * Fleet Codex lanes run with CODEX_HOME=~/.codex-profiles/<slug>, so the
 * managed hooks and `[otel]` exporters `setup` merges into ~/.codex/config.toml
 * never reached them: a seat lane was captured by the rollout scanner only — no
 * live hook events and no spans — while a default-home Codex session emits
 * both. This proof pins the profile target family shut from both ends under a
 * fixture HOME carrying three profiles (fleet hooks only, already managed,
 * malformed) plus a symlinked profile, a dangling link and a directory with no
 * config.toml:
 *
 *   a) library: discovery names every profile directory and marks the one
 *      without config.toml, the dry run plans exactly the managed keys under
 *      `codexProfile[<slug>]`, the apply merges additively (the `[otel]`
 *      fragment identical to the default target's, the Plimsoll hook present
 *      once per event, the fleet's own hooks and unknown keys byte-identical,
 *      file mode preserved, backup written), the second apply is a byte no-op,
 *      a malformed profile is refused without a write or a backup, and a
 *      real-home profile path is refused by the fixture-root guard;
 *   b) command: `setup --dry-run`, `setup --yes`, a second `setup --dry-run`
 *      and `doctor --read-only --json` report the same profiles — the exact
 *      plan lines, every line `unchanged` on the re-run, the malformed profile
 *      refused and byte-identical, the profile without config.toml never
 *      created, and ~/.codex/config.toml handled exactly as before (bare plan
 *      keys, `applied`);
 *   c) ownership of the exit code: a refused profile never fails `setup`
 *      (exit 0 with a malformed profile present) while a refused owned target
 *      still exits 1;
 *   d) secrets: no hook command in any profile carries a token literal — the
 *      hooks reference the same per-user `plimsoll.headers` file the default
 *      target references — while the `[otel]` exporter headers carry exactly
 *      what the default target carries, because Codex's exporter has no file
 *      indirection. Nothing printed by setup or doctor contains the token.
 *
 * Every path is synthetic and below a per-run sandbox; the tokens are fixture
 * credentials minted into the fixture Plimsoll home.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import { parse as parseToml } from "smol-toml";

import { useFixtureRoot } from "./lib/fixture-root";
import {
  ManagedConfigTargetError,
  applyCodexConfig,
  discoverCodexProfiles,
  generateCodexConfigToml,
} from "../packages/collector-config/src/index";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";

type Check = { name: string; passed: true; detail: Record<string, unknown> };

const repoRoot = path.resolve(import.meta.dirname, "..");
const cli = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const loader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
const checks: Check[] = [];

const FLEET_PROFILE = "fleet-hooks-only-profile";
const MANAGED_PROFILE = "already-managed-profile";
const MALFORMED_PROFILE = "malformed-toml-profile";
const LINKED_PROFILE = "relocated-link-profile";
const DANGLING_PROFILE = "dangling-link-profile";
const BARE_PROFILE = "no-config-profile";
/** Marker that must never reach a receipt; it lives in the malformed profile. */
const MALFORMED_MARKER = "synthetic-malformed-profile-marker";
/** The six targets Plimsoll declares; only these may decide `setup`'s exit code. */
const OWNED_TARGETS = ["claude", "gemini", "grokHeaders", "grok", "codexHeaders", "codex"] as const;
/** Managed keys a Codex target plans, in the order the merge emits them. */
const MANAGED_KEYS = [
  "otel.environment",
  "otel.log_user_prompt",
  "otel.exporter.otlp-http.endpoint",
  "otel.exporter.otlp-http.protocol",
  "otel.exporter.otlp-http.headers",
  "otel.trace_exporter.otlp-http.endpoint",
  "otel.trace_exporter.otlp-http.protocol",
  "otel.trace_exporter.otlp-http.headers",
  "otel.metrics_exporter.otlp-http.endpoint",
  "otel.metrics_exporter.otlp-http.protocol",
  "otel.metrics_exporter.otlp-http.headers",
  "features.hooks",
  "hooks.UserPromptSubmit",
  "hooks.PostToolUse",
  "hooks.Stop",
] as const;
/** What a clean fleet profile plans: everything added but its existing `features.hooks`. */
const CLEAN_ACTIONS = MANAGED_KEYS.map((key) => (key === "features.hooks" ? "unchanged" : "added"));

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, passed: true, detail });
}

function sha256(value: string | Buffer) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function digestOf(file: string) {
  return sha256(fs.readFileSync(file));
}

function backups(file: string) {
  const directory = path.dirname(file);
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory).filter((name) => name.includes(".plimsoll-backup-")).sort();
}

/**
 * The fleet conductor's own profile config, with synthetic commands: never a
 * real Inbox or Mem0 command line. The shape is the one the fleet writes — the
 * inline array-of-tables hook layout, foreign hook state tables, and unknown
 * top-level keys — with no `[otel]` table and no Plimsoll hook anywhere, which
 * is exactly the measured gap.
 */
function fleetProfileToml() {
  const fleetHook = (phase: string) =>
    `{ type = "command", command = "/synthetic/fleet/codex-hook --phase ${phase} --synthetic", timeout = 8 }`;
  return [
    "# Synthetic fleet Codex profile.",
    'model = "synthetic-fleet-model"',
    'approval_policy = "on-request"',
    "",
    "[features]",
    "hooks = true",
    "",
    "[hooks]",
    `UserPromptSubmit= [{ hooks = [${fleetHook("checkpoint")}] }]`,
    `PostToolUse= [{ matcher = ".*", hooks = [${fleetHook("active")}] }]`,
    `Stop= [{ hooks = [${fleetHook("end")}] }]`,
    `SessionStart= [{ matcher = "startup|resume", hooks = [${fleetHook("start")}] }]`,
    `SessionEnd= [{ hooks = [${fleetHook("end")}] }]`,
    "",
    "[hooks.state]",
    "",
    '[hooks.state."synthetic:session_start:0:0"]',
    'trusted_hash = "sha256:0000000000000000000000000000000000000000000000000000000000000000"',
    "",
  ].join("\n");
}

function writeProfileConfig(root: string, slug: string, content: string, mode = 0o600) {
  const file = path.join(root, slug, "config.toml");
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, content, { mode });
  fs.chmodSync(file, mode);
  return file;
}

/**
 * The fleet profile as it looks once the managed content is already merged in.
 * Built by the merge itself on a staging copy outside the profiles root, so the
 * "already managed" fixture is the real reconciled output rather than a
 * hand-written guess at it.
 */
function managedProfileToml(stagingRoot: string, slug: string, generated: string) {
  fs.mkdirSync(stagingRoot, { recursive: true, mode: 0o700 });
  const staging = path.join(stagingRoot, `${slug}.toml`);
  fs.writeFileSync(staging, fleetProfileToml(), { mode: 0o600 });
  applyCodexConfig(staging, generated);
  return fs.readFileSync(staging, "utf8");
}

/** Every Plimsoll hook command a document carries, per event. */
function plimsollCommands(document: Record<string, any>, event: string) {
  const commands: string[] = [];
  const visit = (value: unknown) => {
    if (Array.isArray(value)) return value.forEach(visit);
    if (!value || typeof value !== "object") return;
    for (const [key, entry] of Object.entries(value)) {
      if (key === "command" && typeof entry === "string" && entry.includes("/hooks/codex")) {
        commands.push(entry);
      }
      visit(entry);
    }
  };
  visit(document.hooks?.[event]);
  return commands;
}

/** Every command a document carries anywhere under [hooks], Plimsoll's or not. */
function allHookCommands(document: Record<string, any>) {
  const commands: string[] = [];
  const visit = (value: unknown) => {
    if (Array.isArray(value)) return value.forEach(visit);
    if (!value || typeof value !== "object") return;
    for (const [key, entry] of Object.entries(value)) {
      if (key === "command" && typeof entry === "string") commands.push(entry);
      visit(entry);
    }
  };
  visit(document.hooks);
  return commands;
}

function foreignCommands(document: Record<string, any>) {
  return allHookCommands(document).filter((command) => !command.includes("/hooks/codex")).sort();
}

function runCli(args: string[], env: NodeJS.ProcessEnv) {
  const result = spawnSync(process.execPath, ["--import", loader, cli, ...args], {
    cwd: repoRoot,
    env: {
      PATH: process.env.PATH,
      LANG: "en_US.UTF-8",
      TZ: "UTC",
      PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "500",
      ...env,
    },
    encoding: "utf8",
    timeout: 180_000,
  });
  return { code: result.status, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}

/** The last complete JSON document a command printed. */
function lastJson(stdout: string) {
  const start = stdout.lastIndexOf("\n{");
  const candidate = start === -1 ? stdout.slice(stdout.indexOf("{")) : stdout.slice(start + 1);
  return JSON.parse(candidate) as Record<string, unknown>;
}

/** The plan lines a target printed, with its path and prefix stripped. */
function planEntries(stdout: string, file: string, target: string) {
  return stdout.split("\n")
    .filter((line) => line.startsWith(`${file}: ${target}.`))
    .map((line) => line.slice(`${file}: ${target}.`.length));
}

function expectedPlanEntries(actions: readonly string[]) {
  return MANAGED_KEYS.map((key, index) => `${key} ${actions[index]}`);
}

function libraryChecks(fixtureRoot: string) {
  const home = path.join(fixtureRoot, "library-home");
  const profilesRoot = path.join(home, ".codex-profiles");
  const port = 49160;
  const headerFile = path.join(home, ".codex", "plimsoll.headers");
  const syntheticToken = "synthetic-codex-producer-token-library-0000000";
  const generated = generateCodexConfigToml({
    repoRoot: "/synthetic/plimsoll/source",
    port,
    dataMode: "metadata",
    codexProducerToken: syntheticToken,
    codexHeaderFile: headerFile,
  });

  const fleetFile = writeProfileConfig(profilesRoot, FLEET_PROFILE, fleetProfileToml(), 0o640);
  const managedFile = writeProfileConfig(
    profilesRoot,
    MANAGED_PROFILE,
    managedProfileToml(path.join(fixtureRoot, "library-staging"), MANAGED_PROFILE, generated),
  );
  const malformedFile = writeProfileConfig(
    profilesRoot,
    MALFORMED_PROFILE,
    `[hooks\n${MALFORMED_MARKER} = = this is not toml\n`,
  );
  fs.mkdirSync(path.join(profilesRoot, BARE_PROFILE), { recursive: true, mode: 0o700 });
  // A file that is not a profile directory must not become a target.
  fs.writeFileSync(path.join(profilesRoot, "README"), "synthetic\n", { mode: 0o600 });
  const fleetBefore = fs.readFileSync(fleetFile, "utf8");
  const fleetForeignBefore = foreignCommands(parseToml(fleetBefore) as Record<string, any>);
  const malformedBefore = digestOf(malformedFile);

  const discovered = discoverCodexProfiles(home);
  check(
    "discovery_lists_every_profile_directory_and_marks_the_one_without_config",
    discovered.length === 4 &&
      discovered.map((profile) => profile.slug).join(",") ===
        [FLEET_PROFILE, MANAGED_PROFILE, MALFORMED_PROFILE, BARE_PROFILE].sort().join(",") &&
      discovered.filter((profile) => profile.hasConfig).length === 3 &&
      discovered.find((profile) => profile.slug === BARE_PROFILE)?.hasConfig === false,
    { profiles: discovered.map((profile) => ({ slug: profile.slug, hasConfig: profile.hasConfig })) },
  );
  check(
    "discovery_returns_no_profiles_for_a_home_without_a_profiles_directory",
    discoverCodexProfiles(path.join(fixtureRoot, "home-without-profiles")).length === 0,
    { home: "home-without-profiles" },
  );

  const fleetTarget = `codexProfile[${FLEET_PROFILE}]`;
  const preview = applyCodexConfig(fleetFile, generated, { dryRun: true, managedTarget: fleetTarget });
  check(
    "dry_run_plans_exactly_the_managed_profile_keys_and_writes_nothing",
    preview.changed &&
      isDeepStrictEqual(
        preview.plan?.map((entry) => `${entry.key} ${entry.action}`),
        expectedPlanEntries(CLEAN_ACTIONS).map((entry) => `${fleetTarget}.${entry}`),
      ) &&
      preview.backupPath === undefined &&
      fs.readFileSync(fleetFile, "utf8") === fleetBefore &&
      backups(fleetFile).length === 0,
    {
      keys: preview.plan?.map((entry) => `${entry.key} ${entry.action}`),
      backups: backups(fleetFile).length,
    },
  );
  const managedTarget = `codexProfile[${MANAGED_PROFILE}]`;
  const managedPreview = applyCodexConfig(managedFile, generated, { dryRun: true, managedTarget });
  check(
    "an_already_managed_profile_plans_every_key_unchanged",
    !managedPreview.changed &&
      isDeepStrictEqual(
        managedPreview.plan?.map((entry) => `${entry.key} ${entry.action}`),
        MANAGED_KEYS.map((key) => `${managedTarget}.${key} unchanged`),
      ),
    { changed: managedPreview.changed, keys: managedPreview.plan?.length ?? 0 },
  );
  check(
    "the_default_codex_target_still_plans_bare_managed_keys",
    isDeepStrictEqual(
      applyCodexConfig(fleetFile, generated, { dryRun: true }).plan?.map((entry) => `${entry.key} ${entry.action}`),
      expectedPlanEntries(CLEAN_ACTIONS),
    ),
    { target: "codex" },
  );

  const modeBefore = fs.statSync(fleetFile).mode & 0o777;
  const applied = applyCodexConfig(fleetFile, generated, { managedTarget: fleetTarget });
  const mergedSource = fs.readFileSync(fleetFile, "utf8");
  const merged = parseToml(mergedSource) as Record<string, any>;
  const expected = parseToml(generated) as Record<string, any>;
  check(
    "apply_merges_the_otel_fragment_and_hooks_without_disturbing_the_profile",
    applied.changed &&
      typeof applied.backupPath === "string" &&
      backups(fleetFile).length === 1 &&
      isDeepStrictEqual(merged.otel, expected.otel) &&
      merged.features.hooks === true &&
      merged.model === "synthetic-fleet-model" &&
      merged.approval_policy === "on-request" &&
      ["UserPromptSubmit", "PostToolUse", "Stop"].every((event) =>
        plimsollCommands(merged, event).length === 1) &&
      isDeepStrictEqual(foreignCommands(merged), fleetForeignBefore) &&
      isDeepStrictEqual(merged.hooks.state, (parseToml(fleetBefore) as Record<string, any>).hooks.state) &&
      (fs.statSync(fleetFile).mode & 0o777) === modeBefore,
    {
      ownedPerEvent: Object.fromEntries(["UserPromptSubmit", "PostToolUse", "Stop"].map((event) =>
        [event, plimsollCommands(merged, event).length])),
      foreignCommands: foreignCommands(merged).length,
      mode: (fs.statSync(fleetFile).mode & 0o777).toString(8),
    },
  );
  check(
    "the_managed_hook_command_is_token_free_and_reads_the_default_targets_header_file",
    !mergedSource.includes(`x-plimsoll-token: ${syntheticToken}`) &&
      allHookCommands(merged).every((command) => !command.includes(syntheticToken)) &&
      ["UserPromptSubmit", "PostToolUse", "Stop"].every((event) =>
        plimsollCommands(merged, event)[0] ===
          `curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H @${headerFile} --data-binary @- http://127.0.0.1:${port}/hooks/codex || true`) &&
      // The exporter tables keep the inline token: Codex's OTLP exporter has no
      // file or environment source for a header value, so a profile carries
      // exactly what the default target carries and nothing more.
      isDeepStrictEqual(
        merged.otel.exporter["otlp-http"].headers,
        { "x-plimsoll-source": "codex", "x-plimsoll-token": syntheticToken },
      ),
    { headerFileReferenced: true, tokenInHookCommand: false },
  );
  check(
    "a_profile_without_config_toml_is_never_created_by_an_apply",
    !fs.existsSync(path.join(profilesRoot, BARE_PROFILE, "config.toml")),
    { profile: BARE_PROFILE },
  );

  const afterFirst = digestOf(fleetFile);
  const backupsAfterFirst = backups(fleetFile);
  const second = applyCodexConfig(fleetFile, generated, { managedTarget: fleetTarget });
  check(
    "the_second_apply_is_a_byte_identical_no_op_without_backup_churn",
    !second.changed &&
      second.backupPath === undefined &&
      second.plan?.every((entry) => entry.action === "unchanged") &&
      digestOf(fleetFile) === afterFirst &&
      isDeepStrictEqual(backups(fleetFile), backupsAfterFirst),
    { backups: backupsAfterFirst.length, digestStable: digestOf(fleetFile) === afterFirst },
  );

  let malformedRefusal = "no_refusal";
  try {
    applyCodexConfig(malformedFile, generated, { managedTarget: `codexProfile[${MALFORMED_PROFILE}]` });
  } catch (error) {
    malformedRefusal = error instanceof Error ? error.message : String(error);
  }
  check(
    "a_malformed_profile_is_refused_without_a_write_or_a_backup",
    malformedRefusal.includes("existing Codex config.toml is invalid") &&
      !malformedRefusal.includes(MALFORMED_MARKER) &&
      digestOf(malformedFile) === malformedBefore &&
      backups(malformedFile).length === 0,
    { bytesStable: digestOf(malformedFile) === malformedBefore, backups: backups(malformedFile).length },
  );

  // The guard: a profile path inside the operator's real home is refused before
  // any filesystem call. The path below is named, never created or read.
  const operatorProfile = path.join(os.userInfo().homedir, ".codex-profiles", "synthetic-slug", "config.toml");
  let refusal = "no_refusal";
  try {
    applyCodexConfig(operatorProfile, generated, { managedTarget: "codexProfile[synthetic-slug]" });
  } catch (error) {
    refusal = error instanceof ManagedConfigTargetError
      ? error.code
      : `unexpected:${error instanceof Error ? error.message : String(error)}`;
  }
  check(
    "a_real_home_profile_path_is_refused_by_the_fixture_root_guard",
    refusal === "TARGET_INSIDE_REAL_HOME" &&
      !fs.existsSync(path.join(os.userInfo().homedir, ".codex-profiles", "synthetic-slug")),
    { refusal },
  );
}

/**
 * A fixture home wired for the CLI: fixture collector config, fixture producer
 * credentials, and the environment overlay every child run gets.
 */
function commandHome(fixtureRoot: string, name: string, port: number) {
  const home = path.join(fixtureRoot, name);
  const plimsollHome = path.join(home, ".plimsoll");
  const profilesRoot = path.join(home, ".codex-profiles");
  fs.mkdirSync(plimsollHome, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(plimsollHome, "collector.config.json"),
    `${JSON.stringify({ port }, null, 2)}\n`, { mode: 0o600 });
  const auth = loadOrCreateLocalIngestAuth(plimsollHome);
  const generated = generateCodexConfigToml({
    repoRoot: "/synthetic/plimsoll/source",
    port,
    dataMode: "metadata",
    codexProducerToken: auth.codexProducer,
    codexHeaderFile: path.join(home, ".codex", "plimsoll.headers"),
  });
  const env = {
    HOME: home,
    USERPROFILE: home,
    PLIMSOLL_HOME: plimsollHome,
    CODEX_HOME: path.join(home, ".codex"),
    GROK_HOME: path.join(home, ".grok"),
    CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
    PLIMSOLL_FIXTURE_ROOT: fixtureRoot,
  };
  return { home, plimsollHome, profilesRoot, auth, generated, env };
}

/** Every declared target's entry in a setup summary, by target name. */
function ownedStatuses(targets: Record<string, Record<string, unknown> | undefined>) {
  return Object.fromEntries(OWNED_TARGETS.map((name) => [name, targets[name]?.status ?? "absent"]));
}

function commandChecks(fixtureRoot: string) {
  const { home, profilesRoot, auth, generated, env } = commandHome(fixtureRoot, "command-home", 49161);
  const codexFile = path.join(home, ".codex", "config.toml");
  const fleetFile = writeProfileConfig(profilesRoot, FLEET_PROFILE, fleetProfileToml());
  const managedFile = writeProfileConfig(
    profilesRoot,
    MANAGED_PROFILE,
    managedProfileToml(path.join(fixtureRoot, "command-staging"), MANAGED_PROFILE, generated),
  );
  const malformedFile = writeProfileConfig(
    profilesRoot,
    MALFORMED_PROFILE,
    `[hooks\n${MALFORMED_MARKER} = = this is not toml\n`,
  );
  fs.mkdirSync(path.join(profilesRoot, BARE_PROFILE), { recursive: true, mode: 0o700 });
  // A profile the conductor relocated and linked into place, and a link whose
  // target was never created.
  const relocated = path.join(fixtureRoot, "relocated-profiles");
  const linkedTarget = path.join(relocated, "linked-target");
  writeProfileConfig(relocated, "linked-target", fleetProfileToml());
  const danglingTarget = path.join(relocated, "never-created-target");
  fs.symlinkSync(linkedTarget, path.join(profilesRoot, LINKED_PROFILE));
  fs.symlinkSync(danglingTarget, path.join(profilesRoot, DANGLING_PROFILE));
  const linkedFile = path.join(fs.realpathSync(linkedTarget), "config.toml");
  const managedBefore = digestOf(managedFile);
  const malformedBefore = digestOf(malformedFile);

  const fleetTarget = `codexProfile[${FLEET_PROFILE}]`;
  const managedTarget = `codexProfile[${MANAGED_PROFILE}]`;
  const linkedTargetName = `codexProfile[${LINKED_PROFILE}]`;
  const malformedTarget = `codexProfile[${MALFORMED_PROFILE}]`;

  const doctorBefore = runCli(["doctor", "--read-only", "--json"], env);
  const profilesBefore = (lastJson(doctorBefore.stdout).telemetry as Record<string, unknown>)
    .codexProfiles as Array<Record<string, unknown>>;
  const reportedBefore = (slug: string) => profilesBefore.find((profile) => profile.slug === slug);
  check(
    "doctor_reports_every_discovered_profile_and_names_the_unmanaged_ones",
    profilesBefore.length === 6 &&
      reportedBefore(FLEET_PROFILE)?.diagnostic === "codex_profile_config_unmanaged" &&
      (reportedBefore(FLEET_PROFILE)?.missing as string[]).length > 0 &&
      reportedBefore(MANAGED_PROFILE)?.status === "valid" &&
      reportedBefore(MANAGED_PROFILE)?.diagnostic === undefined &&
      reportedBefore(MALFORMED_PROFILE)?.status === "unreadable" &&
      reportedBefore(MALFORMED_PROFILE)?.diagnostic === "codex_profile_config_unmanaged" &&
      reportedBefore(LINKED_PROFILE)?.status === "incomplete" &&
      reportedBefore(BARE_PROFILE)?.status === "skipped" &&
      reportedBefore(BARE_PROFILE)?.diagnostic === undefined &&
      reportedBefore(DANGLING_PROFILE)?.status === "skipped" &&
      lastJson(doctorBefore.stdout).ok === false,
    {
      profiles: profilesBefore.map((profile) => ({
        slug: profile.slug,
        status: profile.status,
        diagnostic: profile.diagnostic ?? null,
      })),
    },
  );
  check(
    "doctor_prints_neither_a_token_nor_a_byte_of_a_malformed_profile",
    !doctorBefore.stdout.includes(auth.codexProducer) &&
      !doctorBefore.stdout.includes(MALFORMED_MARKER),
    { scanned: "doctor stdout" },
  );

  const dryRun = runCli(["setup", "--dry-run"], env);
  const dryTargets = lastJson(dryRun.stdout).targets as Record<string, Record<string, unknown> | undefined>;
  check(
    "setup_dry_run_prints_the_exact_profile_plan_lines_and_writes_nothing",
    dryRun.code === 0 &&
      isDeepStrictEqual(
        planEntries(dryRun.stdout, fleetFile, fleetTarget),
        expectedPlanEntries(CLEAN_ACTIONS),
      ) &&
      isDeepStrictEqual(
        planEntries(dryRun.stdout, linkedFile, linkedTargetName),
        expectedPlanEntries(CLEAN_ACTIONS),
      ) &&
      isDeepStrictEqual(
        planEntries(dryRun.stdout, managedFile, managedTarget),
        MANAGED_KEYS.map((key) => `${key} unchanged`),
      ) &&
      dryTargets[fleetTarget]?.status === "would_apply" &&
      dryTargets[managedTarget]?.status === "unchanged" &&
      dryTargets[malformedTarget]?.status === "refused" &&
      dryTargets[`codexProfile[${BARE_PROFILE}]`] === undefined &&
      dryTargets[`codexProfile[${DANGLING_PROFILE}]`] === undefined &&
      OWNED_TARGETS.every((name) => dryTargets[name]?.status === "would_apply") &&
      digestOf(managedFile) === managedBefore &&
      !fs.existsSync(codexFile),
    {
      lines: planEntries(dryRun.stdout, fleetFile, fleetTarget),
      profileTargets: Object.keys(dryTargets).filter((name) => name.startsWith("codexProfile[")),
      owned: ownedStatuses(dryTargets),
    },
  );
  check(
    "a_malformed_profile_never_sets_the_exit_code_of_setup_dry_run",
    dryRun.code === 0 &&
      String(dryTargets[malformedTarget]?.reason ?? "").includes("existing Codex config.toml is invalid") &&
      !dryRun.stdout.includes(MALFORMED_MARKER) &&
      !dryRun.stdout.includes(auth.codexProducer),
    { code: dryRun.code, reasonNamesTheFile: true },
  );

  const applyRun = runCli(["setup", "--yes"], env);
  const applied = lastJson(applyRun.stdout) as Record<string, Record<string, unknown> | undefined>;
  const fleetApplied = parseToml(fs.readFileSync(fleetFile, "utf8")) as Record<string, any>;
  const codexApplied = parseToml(fs.readFileSync(codexFile, "utf8")) as Record<string, any>;
  check(
    "setup_applies_every_discovered_profile_beside_the_existing_targets",
    applyRun.code === 0 &&
      (applied.status as unknown) === "setup_applied" &&
      applied[fleetTarget]?.status === "applied" &&
      typeof applied[fleetTarget]?.backup === "string" &&
      backups(fleetFile).length === 1 &&
      applied[linkedTargetName]?.status === "applied" &&
      applied[linkedTargetName]?.path === linkedFile &&
      applied[managedTarget]?.status === "unchanged" &&
      applied[malformedTarget]?.status === "refused" &&
      OWNED_TARGETS.every((name) => applied[name]?.status === "applied") &&
      digestOf(managedFile) === managedBefore &&
      digestOf(malformedFile) === malformedBefore &&
      !fs.existsSync(path.join(profilesRoot, BARE_PROFILE, "config.toml")) &&
      !fs.existsSync(danglingTarget),
    {
      code: applyRun.code,
      profileTargets: Object.fromEntries(Object.entries(applied)
        .filter(([name]) => name.startsWith("codexProfile["))
        .map(([name, entry]) => [name, entry?.status])),
      owned: ownedStatuses(applied),
    },
  );
  check(
    "an_applied_profile_carries_the_same_managed_content_as_the_default_codex_target",
    isDeepStrictEqual(fleetApplied.otel, codexApplied.otel) &&
      isDeepStrictEqual(fleetApplied.otel, (parseToml(generated) as Record<string, any>).otel) &&
      ["UserPromptSubmit", "PostToolUse", "Stop"].every((event) =>
        plimsollCommands(fleetApplied, event).length === 1 &&
        plimsollCommands(fleetApplied, event)[0] === plimsollCommands(codexApplied, event)[0]) &&
      isDeepStrictEqual(
        fs.readFileSync(fleetFile, "utf8"),
        fs.readFileSync(managedFile, "utf8"),
      ),
    { profiles: [FLEET_PROFILE, MANAGED_PROFILE] },
  );
  check(
    "no_profile_hook_command_carries_a_token_literal_and_the_fleet_hooks_survive",
    [fleetFile, managedFile, linkedFile].every((file) => {
      const document = parseToml(fs.readFileSync(file, "utf8")) as Record<string, any>;
      return allHookCommands(document).every((command) => !command.includes(auth.codexProducer)) &&
        allHookCommands(document).some((command) => command.includes(`-H @${path.join(home, ".codex", "plimsoll.headers")} `)) &&
        isDeepStrictEqual(
          foreignCommands(document),
          foreignCommands(parseToml(fleetProfileToml()) as Record<string, any>),
        );
    }) &&
      !applyRun.stdout.includes(auth.codexProducer),
    { files: [FLEET_PROFILE, MANAGED_PROFILE, LINKED_PROFILE] },
  );

  const reDryRun = runCli(["setup", "--dry-run"], env);
  const reDryTargets = lastJson(reDryRun.stdout).targets as Record<string, Record<string, unknown> | undefined>;
  check(
    "a_second_dry_run_reports_every_profile_line_unchanged",
    reDryRun.code === 0 &&
      [
        [fleetFile, fleetTarget],
        [managedFile, managedTarget],
        [linkedFile, linkedTargetName],
      ].every(([file, target]) => isDeepStrictEqual(
        planEntries(reDryRun.stdout, file!, target!),
        MANAGED_KEYS.map((key) => `${key} unchanged`),
      )) &&
      [fleetTarget, managedTarget, linkedTargetName].every((target) =>
        reDryTargets[target]?.status === "unchanged") &&
      isDeepStrictEqual(backups(fleetFile).length, 1),
    { code: reDryRun.code, backups: backups(fleetFile).length },
  );

  const doctorAfter = runCli(["doctor", "--read-only", "--json"], env);
  const profilesAfter = (lastJson(doctorAfter.stdout).telemetry as Record<string, unknown>)
    .codexProfiles as Array<Record<string, unknown>>;
  const reportedAfter = (slug: string) => profilesAfter.find((profile) => profile.slug === slug);
  check(
    "doctor_reports_no_unmanaged_profile_after_setup_except_the_malformed_one",
    profilesAfter.length === 6 &&
      [FLEET_PROFILE, MANAGED_PROFILE, LINKED_PROFILE].every((slug) =>
        reportedAfter(slug)?.status === "valid" && reportedAfter(slug)?.diagnostic === undefined) &&
      reportedAfter(MALFORMED_PROFILE)?.status === "unreadable" &&
      isDeepStrictEqual(reportedAfter(MALFORMED_PROFILE)?.missing, ["valid TOML"]) &&
      reportedAfter(BARE_PROFILE)?.status === "skipped" &&
      !doctorAfter.stdout.includes(MALFORMED_MARKER) &&
      !doctorAfter.stdout.includes(auth.codexProducer),
    { profiles: profilesAfter.map((profile) => ({ slug: profile.slug, status: profile.status })) },
  );
  check(
    "a_discovered_profile_never_changes_doctors_ok_verdict",
    typeof lastJson(doctorAfter.stdout).ok === "boolean" &&
      ((lastJson(doctorAfter.stdout).telemetry as Record<string, unknown>).ok as boolean) === true,
    { telemetryOk: true },
  );
}

/**
 * The other half of the exit-code rule: a target Plimsoll does declare still
 * fails the run. Same malformed document, this time in ~/.codex/config.toml.
 */
function ownedRefusalChecks(fixtureRoot: string) {
  const { home, profilesRoot, env } = commandHome(fixtureRoot, "owned-refusal-home", 49162);
  writeProfileConfig(profilesRoot, FLEET_PROFILE, fleetProfileToml());
  const ownedCodexFile = path.join(home, ".codex", "config.toml");
  fs.mkdirSync(path.dirname(ownedCodexFile), { recursive: true, mode: 0o700 });
  fs.writeFileSync(ownedCodexFile, `[hooks\n${MALFORMED_MARKER} = = this is not toml\n`, { mode: 0o600 });
  const dryRun = runCli(["setup", "--dry-run"], env);
  const targets = lastJson(dryRun.stdout).targets as Record<string, Record<string, unknown> | undefined>;
  check(
    "a_refused_owned_target_still_sets_the_exit_code",
    dryRun.code === 1 &&
      targets.codex?.status === "refused" &&
      String(targets.codex?.reason ?? "").includes("existing Codex config.toml is invalid") &&
      targets[`codexProfile[${FLEET_PROFILE}]`]?.status === "would_apply",
    { code: dryRun.code, codex: targets.codex?.status },
  );
}

function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-codex-profiles-proof-"));
  const fixtureRoot = path.join(sandbox, "fixture");
  const fixture = useFixtureRoot(fixtureRoot, {
    home: path.join(fixtureRoot, "must-remain-absent-operator-home"),
  });
  try {
    libraryChecks(fixture.root);
    commandChecks(fixture.root);
    ownedRefusalChecks(fixture.root);
    check(
      "the_fixture_home_the_guard_protects_was_never_created",
      !fs.existsSync(fixture.home),
      { home: "must-remain-absent-operator-home" },
    );
    console.log(JSON.stringify({ bead: "eco-6hoxj.52", ok: true, checks }, null, 2));
  } finally {
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main();
