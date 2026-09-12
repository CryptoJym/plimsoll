/**
 * Self-healing managed-config reconcile proof (bead eco-6hoxj.50).
 *
 * `setup --yes` was the only thing that ever applied the managed telemetry
 * block. The fleet's seat and conductor tooling rewrites
 * ~/.claude-seats/<slug>/settings.json and ~/.codex-profiles/<slug>/config.toml
 * whenever a seat or profile churns, and the rewritten file silently loses that
 * block: doctor reports `claude_seat_settings_unmanaged` /
 * `codex_profile_config_unmanaged`, nobody acts, and the lane emits transcript
 * and rollout rows only until the next release re-runs setup.
 *
 * This proof pins the reconcile shut from both ends under a fixture HOME:
 *
 *   1. after `setup --yes`, a seat's settings.json and a profile's config.toml
 *      are rewritten fleet-style without the managed block; `setup --reconcile`
 *      re-adds exactly the managed keys, leaves every foreign hook and unknown
 *      key byte-identical, writes a backup per changed file, writes one receipt
 *      naming both targets, and clears doctor's unmanaged diagnostics;
 *   2. a healthy home plans every target `unchanged` and writes nothing at all:
 *      no backup, no receipt, no changed file — the pinned no-op contract;
 *   3. a malformed profile is reported and left byte-identical with exit 0, and
 *      the per-file backoff then skips it (a second run inside the window says
 *      `backoff` and does not re-plan it);
 *   4. a file another writer changes between the plan and the apply is skipped
 *      with `changed_during_plan`, keeping the concurrent writer's bytes and
 *      writing no backup;
 *   5. the kill-switch disables the in-process schedule — asserted through
 *      doctor's `managedConfig.reconcile.enabled` and through the maintenance
 *      loop's decision function, which refuses before it reads a single managed
 *      file;
 *   6. the schedule runs the reconcile only when doctor's readback reports
 *      drift: the decision function is pinned with both states plus the
 *      not-yet-due state.
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
import { parse as parseToml } from "smol-toml";

import { useFixtureRoot } from "./lib/fixture-root";
import {
  generateClaudeCodeSettings,
  generateCodexConfigToml,
} from "../packages/collector-config/src/index";
import {
  type ManagedConfigReadback,
  composeManagedClaudeTargets,
  decideManagedConfigReconcile,
  managedConfigReconcileStatePath,
  readManagedConfigReconcileState,
  runManagedConfigReconcile,
} from "../packages/collector-cli/src/managed-config-reconcile";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";

type Check = { name: string; passed: true; detail: Record<string, unknown> };

const repoRoot = path.resolve(import.meta.dirname, "..");
const cli = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const loader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
const checks: Check[] = [];

const CHURNED_SEAT = "churned-seat";
const QUIET_SEAT = "already-managed-seat";
const CHURNED_PROFILE = "churned-profile";
const MALFORMED_PROFILE = "malformed-profile";
/** Marker that must never reach a receipt; it lives in the malformed profile. */
const MALFORMED_MARKER = "synthetic-malformed-profile-marker";

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

function receipts(plimsollHome: string) {
  const directory = path.join(plimsollHome, "receipts");
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory)
    .filter((name) => name.startsWith("managed-config-reconcile-"))
    .sort();
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

function targetReport(result: Record<string, unknown>, name: string) {
  return (result.targets as Array<Record<string, unknown>>).find((entry) => entry.name === name);
}

/**
 * The fleet's own seat settings, with synthetic hook commands: never a real
 * Inbox or Mem0 command line. No managed env and no managed hook anywhere,
 * which is exactly what the seat tooling leaves behind after churn.
 */
function fleetSeatDocument(marker: string) {
  const fleetHook = (event: string) => ({
    hooks: [
      { type: "command", command: `/synthetic/fleet/seat-hook --event ${event} --synthetic`, timeout: 5 },
    ],
  });
  return {
    model: "synthetic-fleet-model",
    // An unknown top-level key the reconcile must carry through untouched.
    fleetSeatRevision: marker,
    hooks: {
      SessionStart: [fleetHook("SessionStart")],
      UserPromptSubmit: [fleetHook("UserPromptSubmit")],
      PostToolUse: [{ matcher: ".*", ...fleetHook("PostToolUse") }],
      Stop: [fleetHook("Stop")],
      SessionEnd: [fleetHook("SessionEnd")],
    },
  };
}

/** The fleet conductor's own profile config: no [otel] table, no Plimsoll hook. */
function fleetProfileToml(marker: string) {
  const fleetHook = (phase: string) =>
    `{ type = "command", command = "/synthetic/fleet/codex-hook --phase ${phase} --synthetic", timeout = 8 }`;
  return [
    "# Synthetic fleet Codex profile.",
    'model = "synthetic-fleet-model"',
    `fleet_profile_revision = ${JSON.stringify(marker)}`,
    "",
    "[features]",
    "hooks = true",
    "",
    "[hooks]",
    `UserPromptSubmit= [{ hooks = [${fleetHook("checkpoint")}] }]`,
    `PostToolUse= [{ matcher = ".*", hooks = [${fleetHook("active")}] }]`,
    `Stop= [{ hooks = [${fleetHook("end")}] }]`,
    `SessionStart= [{ hooks = [${fleetHook("start")}] }]`,
    "",
  ].join("\n");
}

function writeSeatSettings(root: string, slug: string, document: unknown, mode = 0o600) {
  const file = path.join(root, slug, "settings.json");
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, `${JSON.stringify(document, null, 2)}\n`, { mode });
  fs.chmodSync(file, mode);
  return file;
}

function writeProfileConfig(root: string, slug: string, body: string, mode = 0o600) {
  const file = path.join(root, slug, "config.toml");
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, body, { mode });
  fs.chmodSync(file, mode);
  return file;
}

/** Every hook command in a document, managed and foreign alike. */
function hookCommands(value: unknown): string[] {
  const found: string[] = [];
  const visit = (node: unknown) => {
    if (Array.isArray(node)) {
      for (const entry of node) visit(entry);
      return;
    }
    if (node && typeof node === "object") {
      const record = node as Record<string, unknown>;
      if (typeof record.command === "string") found.push(record.command);
      for (const entry of Object.values(record)) visit(entry);
    }
  };
  visit(value);
  return found.sort();
}

function seatDocument(file: string) {
  return JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, any>;
}

function profileDocument(file: string) {
  return parseToml(fs.readFileSync(file, "utf8")) as Record<string, any>;
}

/**
 * Scenarios 1, 2, 3 and 5 through the real command surface: `setup --yes`,
 * `setup --reconcile` and `doctor --read-only --json` in one fixture home whose
 * seats and profiles churn between runs.
 */
function commandChecks(fixtureRoot: string) {
  const home = path.join(fixtureRoot, "command-home");
  const plimsollHome = path.join(home, ".plimsoll");
  const seatsRoot = path.join(home, ".claude-seats");
  const profilesRoot = path.join(home, ".codex-profiles");
  const port = 49171;
  fs.mkdirSync(plimsollHome, { recursive: true, mode: 0o700 });
  const configFile = path.join(plimsollHome, "collector.config.json");
  fs.writeFileSync(configFile, `${JSON.stringify({ port }, null, 2)}\n`, { mode: 0o600 });
  loadOrCreateLocalIngestAuth(plimsollHome);

  const churnedSeatFile = writeSeatSettings(seatsRoot, CHURNED_SEAT, fleetSeatDocument("r1"));
  const quietSeatFile = writeSeatSettings(seatsRoot, QUIET_SEAT, fleetSeatDocument("quiet"));
  const churnedProfileFile = writeProfileConfig(profilesRoot, CHURNED_PROFILE, fleetProfileToml("r1"));

  const env = {
    HOME: home,
    USERPROFILE: home,
    PLIMSOLL_HOME: plimsollHome,
    CODEX_HOME: path.join(home, ".codex"),
    GROK_HOME: path.join(home, ".grok"),
    CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
    PLIMSOLL_FIXTURE_ROOT: fixtureRoot,
  };

  const installed = runCli(["setup", "--yes"], env);
  check(
    "setup_yes_installs_the_managed_block_in_the_home_targets_and_both_seat_families",
    installed.code === 0 &&
      lastJson(installed.stdout).status === "setup_applied" &&
      (lastJson(installed.stdout)[`claudeSeat[${CHURNED_SEAT}]`] as Record<string, unknown>)?.status === "applied" &&
      (lastJson(installed.stdout)[`codexProfile[${CHURNED_PROFILE}]`] as Record<string, unknown>)?.status === "applied",
    { code: installed.code },
  );

  // ---- scenario 2: a healthy home reconciles to a true no-op ---------------
  const healthySeatDigest = digestOf(churnedSeatFile);
  const healthyProfileDigest = digestOf(churnedProfileFile);
  const healthySeatBackups = backups(churnedSeatFile).length;
  const healthyProfileBackups = backups(churnedProfileFile).length;
  const healthy = runCli(["setup", "--reconcile"], env);
  const healthyJson = lastJson(healthy.stdout);
  check(
    "a_healthy_home_plans_every_target_unchanged_and_writes_nothing",
    healthy.code === 0 &&
      healthyJson.status === "managed_config_unchanged" &&
      healthyJson.applied === 0 &&
      healthyJson.refused === 0 &&
      healthyJson.receiptPath === null &&
      (healthyJson.targets as Array<Record<string, unknown>>)
        .every((entry) => entry.status === "unchanged") &&
      (healthyJson.targets as unknown[]).length === 5 &&
      receipts(plimsollHome).length === 0 &&
      digestOf(churnedSeatFile) === healthySeatDigest &&
      digestOf(churnedProfileFile) === healthyProfileDigest &&
      backups(churnedSeatFile).length === healthySeatBackups &&
      backups(churnedProfileFile).length === healthyProfileBackups,
    {
      status: healthyJson.status,
      targets: (healthyJson.targets as Array<Record<string, unknown>>).map((entry) => entry.status),
      receipts: receipts(plimsollHome).length,
    },
  );
  check(
    "a_no_op_reconcile_still_stamps_the_run_so_the_cadence_can_space_itself",
    readManagedConfigReconcileState(plimsollHome).lastRunAt !== null &&
      readManagedConfigReconcileState(plimsollHome).lastApplied === 0 &&
      fs.existsSync(managedConfigReconcileStatePath(plimsollHome)),
    { lastRunAt: typeof readManagedConfigReconcileState(plimsollHome).lastRunAt },
  );

  // ---- scenario 1: seat and profile churn, then reconcile -----------------
  fs.writeFileSync(churnedSeatFile, `${JSON.stringify(fleetSeatDocument("r2"), null, 2)}\n`, { mode: 0o600 });
  fs.writeFileSync(churnedProfileFile, fleetProfileToml("r2"), { mode: 0o600 });
  const churnedSeatForeign = hookCommands(seatDocument(churnedSeatFile).hooks);
  const churnedProfileForeign = hookCommands(profileDocument(churnedProfileFile).hooks);
  const quietSeatBefore = digestOf(quietSeatFile);
  const homeClaudeFile = path.join(home, ".claude", "settings.json");
  const homeCodexFile = path.join(home, ".codex", "config.toml");
  const homeClaudeBefore = digestOf(homeClaudeFile);
  const homeCodexBefore = digestOf(homeCodexFile);

  const doctorDrifted = lastJson(runCli(["doctor", "--read-only", "--json"], env).stdout);
  const driftedSeats = (doctorDrifted.telemetry as Record<string, unknown>).claudeSeats as Array<Record<string, unknown>>;
  const driftedProfiles = (doctorDrifted.telemetry as Record<string, unknown>).codexProfiles as Array<Record<string, unknown>>;
  check(
    "doctor_reports_the_churned_seat_and_profile_as_unmanaged_before_the_reconcile",
    driftedSeats.find((seat) => seat.slug === CHURNED_SEAT)?.diagnostic === "claude_seat_settings_unmanaged" &&
      driftedProfiles.find((profile) => profile.slug === CHURNED_PROFILE)?.diagnostic === "codex_profile_config_unmanaged" &&
      driftedSeats.find((seat) => seat.slug === QUIET_SEAT)?.diagnostic === undefined,
    {
      seats: driftedSeats.map((seat) => ({ slug: seat.slug, status: seat.status })),
      profiles: driftedProfiles.map((profile) => ({ slug: profile.slug, status: profile.status })),
    },
  );

  const healed = runCli(["setup", "--reconcile"], env);
  const healedJson = lastJson(healed.stdout);
  const healedSeat = targetReport(healedJson, `claudeSeat[${CHURNED_SEAT}]`);
  const healedProfile = targetReport(healedJson, `codexProfile[${CHURNED_PROFILE}]`);
  check(
    "reconcile_reapplies_exactly_the_two_churned_targets_and_leaves_the_rest_unchanged",
    healed.code === 0 &&
      healedJson.status === "managed_config_reconciled" &&
      healedJson.applied === 2 &&
      healedJson.refused === 0 &&
      healedJson.unchanged === 3 &&
      healedSeat?.status === "applied" &&
      healedProfile?.status === "applied" &&
      targetReport(healedJson, `claudeSeat[${QUIET_SEAT}]`)?.status === "unchanged" &&
      targetReport(healedJson, "claude")?.status === "unchanged" &&
      targetReport(healedJson, "codex")?.status === "unchanged" &&
      digestOf(quietSeatFile) === quietSeatBefore &&
      digestOf(homeClaudeFile) === homeClaudeBefore &&
      digestOf(homeCodexFile) === homeCodexBefore,
    {
      applied: healedJson.applied,
      targets: (healedJson.targets as Array<Record<string, unknown>>)
        .map((entry) => `${entry.name}:${entry.status}`),
    },
  );
  check(
    "the_reconcile_plan_only_ever_adds_or_updates_managed_keys",
    (healedJson.targets as Array<Record<string, unknown>>).every((entry) =>
      ((entry.plan ?? []) as Array<Record<string, unknown>>)
        .every((line) => ["added", "updated", "unchanged"].includes(String(line.action)))) &&
      ((healedSeat?.plan ?? []) as Array<Record<string, unknown>>).some((line) => line.action !== "unchanged") &&
      ((healedProfile?.plan ?? []) as Array<Record<string, unknown>>).some((line) => line.action !== "unchanged") &&
      ((healedSeat?.plan ?? []) as Array<Record<string, unknown>>)
        .every((line) => String(line.key).startsWith(`claudeSeat[${CHURNED_SEAT}].`)) &&
      ((healedProfile?.plan ?? []) as Array<Record<string, unknown>>)
        .every((line) => String(line.key).startsWith(`codexProfile[${CHURNED_PROFILE}].`)),
    {
      seatPlan: ((healedSeat?.plan ?? []) as Array<Record<string, unknown>>).length,
      profilePlan: ((healedProfile?.plan ?? []) as Array<Record<string, unknown>>).length,
    },
  );
  check(
    "the_reconcile_printed_one_plan_line_per_managed_key_and_one_status_line_per_changed_target",
    healed.stdout.split("\n")
      .filter((line) => line.startsWith(`${churnedSeatFile}: claudeSeat[${CHURNED_SEAT}].`)).length ===
      ((healedSeat?.plan ?? []) as unknown[]).length &&
      healed.stdout.includes(`${churnedSeatFile}: target applied`) &&
      healed.stdout.includes(`${churnedProfileFile}: target applied`),
    {},
  );

  const healedSeatDocument = seatDocument(churnedSeatFile);
  const healedProfileDocument = profileDocument(churnedProfileFile);
  const managedClaudeEnv = generateClaudeCodeSettings({
    repoRoot,
    port,
    dataMode: "metadata",
    claudeCodeProducerToken: loadOrCreateLocalIngestAuth(plimsollHome).claudeCodeProducer,
  }).env;
  check(
    "the_healed_seat_carries_the_managed_keys_with_every_foreign_hook_and_unknown_key_intact",
    Object.entries(managedClaudeEnv)
      .every(([key, value]) => healedSeatDocument.env?.[key] === value) &&
      healedSeatDocument.model === "synthetic-fleet-model" &&
      healedSeatDocument.fleetSeatRevision === "r2" &&
      churnedSeatForeign.every((command) => hookCommands(healedSeatDocument.hooks).includes(command)) &&
      hookCommands(healedSeatDocument.hooks).length === churnedSeatForeign.length,
    { foreignHooks: churnedSeatForeign.length },
  );
  check(
    "the_healed_profile_carries_the_managed_otel_tables_with_the_fleet_hooks_intact",
    healedProfileDocument.otel?.exporter?.["otlp-http"]?.endpoint === `http://127.0.0.1:${port}/v1/logs` &&
      healedProfileDocument.otel?.trace_exporter?.["otlp-http"]?.endpoint === `http://127.0.0.1:${port}/v1/traces` &&
      healedProfileDocument.model === "synthetic-fleet-model" &&
      healedProfileDocument.fleet_profile_revision === "r2" &&
      churnedProfileForeign.every((command) => hookCommands(healedProfileDocument.hooks).includes(command)),
    { foreignHooks: churnedProfileForeign.length },
  );
  check(
    "every_healed_file_got_a_backup_before_it_was_changed",
    backups(churnedSeatFile).length === healthySeatBackups + 1 &&
      backups(churnedProfileFile).length === healthyProfileBackups + 1 &&
      typeof healedSeat?.backup === "string" &&
      typeof healedProfile?.backup === "string" &&
      fs.existsSync(String(healedSeat?.backup)) &&
      fs.existsSync(String(healedProfile?.backup)),
    { seatBackups: backups(churnedSeatFile).length },
  );

  const receiptNames = receipts(plimsollHome);
  const receipt = JSON.parse(
    fs.readFileSync(path.join(plimsollHome, "receipts", receiptNames[0]), "utf8"),
  ) as Record<string, unknown>;
  check(
    "the_applied_run_wrote_exactly_one_receipt_naming_both_targets_with_their_backups",
    receiptNames.length === 1 &&
      receipt.status === "managed_config_reconciled" &&
      receipt.applied === 2 &&
      targetReport(receipt, `claudeSeat[${CHURNED_SEAT}]`)?.status === "applied" &&
      targetReport(receipt, `codexProfile[${CHURNED_PROFILE}]`)?.status === "applied" &&
      typeof targetReport(receipt, `claudeSeat[${CHURNED_SEAT}]`)?.backup === "string" &&
      typeof targetReport(receipt, `codexProfile[${CHURNED_PROFILE}]`)?.backup === "string" &&
      (fs.statSync(path.join(plimsollHome, "receipts", receiptNames[0])).mode & 0o777) === 0o600,
    { receipts: receiptNames },
  );

  const doctorHealed = lastJson(runCli(["doctor", "--read-only", "--json"], env).stdout);
  const healedSeats = (doctorHealed.telemetry as Record<string, unknown>).claudeSeats as Array<Record<string, unknown>>;
  const healedProfiles = (doctorHealed.telemetry as Record<string, unknown>).codexProfiles as Array<Record<string, unknown>>;
  const reconcileSection = (doctorHealed.managedConfig as Record<string, unknown>)
    .reconcile as Record<string, unknown>;
  check(
    "doctor_reports_no_unmanaged_seat_or_profile_after_the_reconcile",
    healedSeats.every((seat) => seat.diagnostic === undefined) &&
      healedProfiles.every((profile) => profile.diagnostic === undefined),
    {
      seats: healedSeats.map((seat) => ({ slug: seat.slug, status: seat.status })),
      profiles: healedProfiles.map((profile) => ({ slug: profile.slug, status: profile.status })),
    },
  );
  check(
    "doctor_reports_the_reconcile_schedule_state_as_counts_and_stamps_only",
    Object.keys(reconcileSection).sort().join(",") ===
      "enabled,intervalSeconds,lastApplied,lastRefused,lastRunAt,nextEligibleAt" &&
      reconcileSection.enabled === true &&
      reconcileSection.intervalSeconds === 600 &&
      reconcileSection.lastApplied === 2 &&
      reconcileSection.lastRefused === 0 &&
      typeof reconcileSection.lastRunAt === "string" &&
      Date.parse(String(reconcileSection.nextEligibleAt)) ===
        Date.parse(String(reconcileSection.lastRunAt)) + 600_000,
    { reconcile: reconcileSection },
  );
  check(
    "no_reconcile_output_or_receipt_carries_a_producer_token",
    !healed.stdout.includes(loadOrCreateLocalIngestAuth(plimsollHome).claudeCodeProducer) &&
      !JSON.stringify(receipt).includes(loadOrCreateLocalIngestAuth(plimsollHome).codexProducer),
    {},
  );

  // ---- scenario 3: a malformed profile is reported, untouched, backed off --
  const malformedFile = writeProfileConfig(
    profilesRoot,
    MALFORMED_PROFILE,
    `[hooks\n${MALFORMED_MARKER} = = this is not toml\n`,
  );
  const malformedBefore = digestOf(malformedFile);
  const refusedRun = runCli(["setup", "--reconcile"], env);
  const refusedJson = lastJson(refusedRun.stdout);
  const refusedProfile = targetReport(refusedJson, `codexProfile[${MALFORMED_PROFILE}]`);
  check(
    "a_malformed_profile_is_reported_left_byte_identical_and_never_fails_the_run",
    refusedRun.code === 0 &&
      refusedJson.refused === 1 &&
      refusedJson.applied === 0 &&
      refusedJson.ownedRefusal === false &&
      refusedProfile?.status === "refused" &&
      typeof refusedProfile?.reason === "string" &&
      digestOf(malformedFile) === malformedBefore &&
      backups(malformedFile).length === 0 &&
      refusedRun.stdout.includes(`${malformedFile}: target refused`),
    { code: refusedRun.code, reason: refusedProfile?.reason },
  );
  check(
    "the_refusal_receipt_never_carries_the_malformed_file_content",
    !JSON.stringify(refusedJson).includes(MALFORMED_MARKER) &&
      receipts(plimsollHome).length === 2 &&
      !fs.readFileSync(path.join(plimsollHome, "receipts", receipts(plimsollHome)[1]), "utf8")
        .includes(MALFORMED_MARKER),
    { receipts: receipts(plimsollHome).length },
  );

  const backedOffRun = runCli(["setup", "--reconcile"], env);
  const backedOffJson = lastJson(backedOffRun.stdout);
  const backedOffProfile = targetReport(backedOffJson, `codexProfile[${MALFORMED_PROFILE}]`);
  const backoffUntil = Date.parse(String(backedOffProfile?.nextEligibleAt));
  check(
    "a_second_run_inside_the_backoff_window_skips_the_refused_profile_and_says_so",
    backedOffRun.code === 0 &&
      backedOffProfile?.status === "skipped" &&
      backedOffProfile?.reason === "backoff" &&
      backedOffProfile?.plan === undefined &&
      backoffUntil - Date.parse(String(refusedJson.startedAt)) === 60 * 60 * 1000 &&
      backedOffJson.refused === 0 &&
      backedOffJson.applied === 0 &&
      backedOffJson.receiptPath === null &&
      receipts(plimsollHome).length === 2 &&
      digestOf(malformedFile) === malformedBefore &&
      backedOffRun.stdout.includes(`${malformedFile}: target skipped: backoff`),
    { status: backedOffProfile?.status, reason: backedOffProfile?.reason },
  );

  // ---- scenario 5: the kill-switch --------------------------------------
  fs.writeFileSync(
    configFile,
    `${JSON.stringify({ port, managedConfig: { reconcile: { enabled: false, intervalSeconds: 900 } } }, null, 2)}\n`,
    { mode: 0o600 },
  );
  const doctorDisabled = lastJson(runCli(["doctor", "--read-only", "--json"], env).stdout);
  const disabledSection = (doctorDisabled.managedConfig as Record<string, unknown>)
    .reconcile as Record<string, unknown>;
  check(
    "the_kill_switch_is_visible_in_doctor_without_changing_any_other_reconcile_field",
    disabledSection.enabled === false &&
      disabledSection.intervalSeconds === 900 &&
      typeof disabledSection.lastRunAt === "string",
    { reconcile: disabledSection },
  );
  return { plimsollHome, port };
}

/**
 * Scenario 4 at the library boundary: another writer changes the file between
 * the plan and the apply. `onPlanned` is the deterministic stand-in for that
 * writer; production callers leave it unset.
 */
function concurrentWriterChecks(fixtureRoot: string) {
  const home = path.join(fixtureRoot, "concurrent-home");
  const collectorHome = path.join(fixtureRoot, "concurrent-plimsoll-home");
  const seatsRoot = path.join(home, ".claude-seats");
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  const claudeFile = path.join(home, ".claude", "settings.json");
  fs.mkdirSync(path.dirname(claudeFile), { recursive: true, mode: 0o700 });
  fs.writeFileSync(claudeFile, `${JSON.stringify(fleetSeatDocument("home"), null, 2)}\n`, { mode: 0o600 });
  const seatFile = writeSeatSettings(seatsRoot, CHURNED_SEAT, fleetSeatDocument("r1"));
  const toolOptions = {
    repoRoot,
    port: 49172,
    dataMode: "metadata" as const,
    claudeCodeProducerToken: "synthetic-claude-producer-token-concurrent-00",
  };
  const targets = composeManagedClaudeTargets(claudeFile, home);
  const racedDocument = { ...fleetSeatDocument("r3"), racedByAnotherWriter: true };
  const result = runManagedConfigReconcile({
    collectorHome,
    targets,
    toolOptions,
    onPlanned: (target) => {
      if (target.name !== `claudeSeat[${CHURNED_SEAT}]`) return;
      // The seat tooling rewrites the file inside the plan -> apply window.
      fs.writeFileSync(seatFile, `${JSON.stringify(racedDocument, null, 2)}\n`, { mode: 0o600 });
    },
  });
  const raced = result.targets.find((entry) => entry.name === `claudeSeat[${CHURNED_SEAT}]`);
  const racedAfter = seatDocument(seatFile);
  check(
    "a_file_another_writer_changed_between_the_plan_and_the_apply_is_skipped_with_a_reason",
    raced?.status === "skipped" &&
      raced?.reason === "changed_during_plan" &&
      racedAfter.racedByAnotherWriter === true &&
      racedAfter.env === undefined &&
      backups(seatFile).length === 0 &&
      result.applied === 1 &&
      result.skipped === 1 &&
      result.targets.find((entry) => entry.name === "claude")?.status === "applied",
    { status: raced?.status, reason: raced?.reason, applied: result.applied },
  );
  check(
    "the_skipped_file_is_reconciled_on_the_next_run_because_no_backoff_was_armed",
    readManagedConfigReconcileState(collectorHome).lastApplied === 1 &&
      Object.keys(readManagedConfigReconcileState(collectorHome).backoff).length === 0 &&
      runManagedConfigReconcile({ collectorHome, targets, toolOptions })
        .targets.find((entry) => entry.name === `claudeSeat[${CHURNED_SEAT}]`)?.status === "applied",
    {},
  );
  check(
    "reconcile_never_provisions_a_target_whose_file_does_not_exist",
    runManagedConfigReconcile({
      collectorHome,
      targets: composeManagedClaudeTargets(
        path.join(fixtureRoot, "absent-home", ".claude", "settings.json"),
        path.join(fixtureRoot, "absent-home"),
      ),
      toolOptions,
    }).targets.every((entry) => entry.status === "skipped" && entry.reason === "absent") &&
      !fs.existsSync(path.join(fixtureRoot, "absent-home")),
    {},
  );
}

/**
 * Scenario 6 (and the decision half of scenario 5): the maintenance loop's
 * decision function, pinned with both drift states without waiting out a
 * cadence.
 */
function decisionChecks() {
  const now = Date.parse("2026-09-12T12:00:00.000Z");
  const base = { intervalSeconds: 600, now, lastRunAt: now - 600_000 };
  let driftReads = 0;
  const drift = (value: number) => () => {
    driftReads += 1;
    return value;
  };

  const disabled = decideManagedConfigReconcile({ ...base, enabled: false, drift: drift(3) });
  check(
    "the_kill_switch_refuses_before_the_schedule_reads_a_single_managed_file",
    disabled.run === false && disabled.reason === "disabled" && driftReads === 0,
    { decision: disabled, driftReads },
  );

  const tooSoon = decideManagedConfigReconcile({
    ...base,
    lastRunAt: now - 60_000,
    enabled: true,
    drift: drift(3),
  });
  check(
    "a_cadence_that_is_not_due_refuses_before_the_schedule_reads_a_single_managed_file",
    tooSoon.run === false &&
      tooSoon.reason === "interval_not_elapsed" &&
      tooSoon.nextEligibleAt === new Date(now + 540_000).toISOString() &&
      driftReads === 0,
    { decision: tooSoon, driftReads },
  );

  const healthy = decideManagedConfigReconcile({ ...base, enabled: true, drift: drift(0) });
  check(
    "a_due_cadence_with_no_drifted_target_does_not_run_the_reconcile",
    healthy.run === false && healthy.reason === "no_drift" && driftReads === 1,
    { decision: healthy, driftReads },
  );

  const drifted = decideManagedConfigReconcile({ ...base, enabled: true, drift: drift(1) });
  check(
    "a_due_cadence_runs_the_reconcile_only_when_doctors_readback_reports_drift",
    drifted.run === true && drifted.reason === "drift" && driftReads === 2,
    { decision: drifted, driftReads },
  );

  const firstBoot = decideManagedConfigReconcile({
    ...base,
    lastRunAt: null,
    enabled: true,
    drift: drift(1),
  });
  check(
    "a_collector_that_has_never_reconciled_is_eligible_immediately",
    firstBoot.run === true &&
      firstBoot.reason === "drift" &&
      firstBoot.nextEligibleAt === new Date(now).toISOString(),
    { decision: firstBoot },
  );

  // Only `incomplete` is drift: reconcile cannot heal a missing or malformed
  // file, so counting those would make a host loop on a permanent no-op.
  const readbacks: ManagedConfigReadback[] = [
    { ok: true, status: "valid", missing: [] },
    { ok: false, status: "missing", missing: ["config file"] },
    { ok: false, status: "invalid", missing: ["valid TOML"] },
    { ok: false, status: "incomplete", missing: ["otel.environment"] },
  ];
  check(
    "only_a_readable_file_whose_managed_block_is_stale_counts_as_drift",
    readbacks.filter((readback) => readback.status === "incomplete").length === 1,
    { statuses: readbacks.map((readback) => readback.status) },
  );
}

function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-managed-config-reconcile-proof-"));
  const fixtureRoot = path.join(sandbox, "fixture");
  const fixture = useFixtureRoot(fixtureRoot, {
    home: path.join(fixtureRoot, "must-remain-absent-operator-home"),
  });
  try {
    commandChecks(fixture.root);
    concurrentWriterChecks(fixture.root);
    decisionChecks();
    check(
      "the_fixture_home_the_guard_protects_was_never_created",
      !fs.existsSync(fixture.home),
      { home: "must-remain-absent-operator-home" },
    );
    console.log(JSON.stringify({ bead: "eco-6hoxj.50", ok: true, checks }, null, 2));
  } finally {
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main();
