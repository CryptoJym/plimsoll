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
  MANAGED_CONFIG_BACKUPS_KEPT_PER_FILE,
  MANAGED_CONFIG_RECEIPTS_KEPT,
  type ManagedConfigReadback,
  type ManagedConfigTarget,
  composeManagedClaudeTargets,
  composeManagedCodexTargets,
  decideManagedConfigReconcile,
  isManagedConfigConcurrencyFailure,
  managedConfigReconcileDoctorSection,
  managedConfigReconcileStatePath,
  readManagedConfigReconcileSettings,
  readManagedConfigReconcileState,
  runManagedConfigReconcile,
  runManagedConfigReconcileAsync,
  stampManagedConfigReconcileDecision,
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
/** A seat directory that exists with no settings.json in it (review r1, F7). */
const EMPTY_SEAT = "provisioned-but-empty-seat";
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
  // A seat directory the fleet created but has not written settings.json into
  // yet (review r1, F7). Setup must not provision it and the reconcile must
  // still name it, as `skipped: absent`, rather than drop it from the report.
  fs.mkdirSync(path.join(seatsRoot, EMPTY_SEAT), { recursive: true, mode: 0o700 });

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

  check(
    "setup_yes_never_provisions_a_seat_directory_that_has_no_settings_file",
    !fs.existsSync(path.join(seatsRoot, EMPTY_SEAT, "settings.json")) &&
      lastJson(installed.stdout)[`claudeSeat[${EMPTY_SEAT}]`] === undefined,
    { seat: EMPTY_SEAT },
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
        .filter((entry) => entry.name !== `claudeSeat[${EMPTY_SEAT}]`)
        .every((entry) => entry.status === "unchanged") &&
      (healthyJson.targets as unknown[]).length === 6 &&
      healthyJson.absent === 1 &&
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
  const firstNoOpStamp = readManagedConfigReconcileState(plimsollHome);
  const secondNoOp = runCli(["setup", "--reconcile"], env);
  const secondNoOpStamp = readManagedConfigReconcileState(plimsollHome);
  check(
    "a_no_op_reconcile_still_stamps_the_run_so_the_cadence_can_space_itself",
    firstNoOpStamp.lastRunAt !== null &&
      firstNoOpStamp.lastApplied === 0 &&
      fs.existsSync(managedConfigReconcileStatePath(plimsollHome)),
    { lastRunAt: typeof firstNoOpStamp.lastRunAt },
  );
  check(
    // review r1, F2: an operator must be able to tell "the cadence ran and
    // found nothing" from "the cadence never ran".
    "a_no_op_run_advances_the_stamp_and_records_unchanged_without_writing_a_receipt",
    secondNoOp.code === 0 &&
      secondNoOpStamp.lastResult === "unchanged" &&
      firstNoOpStamp.lastResult === "unchanged" &&
      secondNoOpStamp.lastRunAt !== null &&
      Date.parse(String(secondNoOpStamp.lastRunAt)) >= Date.parse(String(firstNoOpStamp.lastRunAt)) &&
      secondNoOpStamp.lastRunAt !== firstNoOpStamp.lastRunAt &&
      secondNoOpStamp.lastApplied === 0 &&
      secondNoOpStamp.lastRefused === 0 &&
      secondNoOpStamp.lastAbsent === 1 &&
      receipts(plimsollHome).length === 0,
    {
      first: firstNoOpStamp.lastRunAt,
      second: secondNoOpStamp.lastRunAt,
      lastResult: secondNoOpStamp.lastResult,
      receipts: receipts(plimsollHome).length,
    },
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
      healedJson.absent === 1 &&
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

  check(
    // review r1, F7: an operator reading a receipt could not tell "this seat
    // has no settings.json yet" from "this seat does not exist".
    "a_seat_directory_with_no_config_file_is_named_as_skipped_absent_in_the_receipt",
    targetReport(receipt, `claudeSeat[${EMPTY_SEAT}]`)?.status === "skipped" &&
      targetReport(receipt, `claudeSeat[${EMPTY_SEAT}]`)?.reason === "absent" &&
      receipt.absent === 1 &&
      !fs.existsSync(path.join(seatsRoot, EMPTY_SEAT, "settings.json")) &&
      healed.stdout.includes(`${path.join(seatsRoot, EMPTY_SEAT, "settings.json")}: target skipped: absent`),
    { entry: targetReport(receipt, `claudeSeat[${EMPTY_SEAT}]`) },
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
      "enabled,intervalSeconds,lastAbsent,lastApplied,lastRefused,lastResult,lastRunAt,nextEligibleAt" &&
      reconcileSection.lastResult === "applied" &&
      reconcileSection.lastAbsent === 1 &&
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
  // ---- review r1, F4: a backoff is invalidated when the file is fixed -----
  fs.writeFileSync(configFile, `${JSON.stringify({ port }, null, 2)}\n`, { mode: 0o600 });
  const healedProfileBody = fleetProfileToml("fixed");
  fs.writeFileSync(malformedFile, healedProfileBody, { mode: 0o600 });
  const healedAfterBackoff = runCli(["setup", "--reconcile"], env);
  const healedAfterBackoffJson = lastJson(healedAfterBackoff.stdout);
  const healedFormerlyMalformed = targetReport(
    healedAfterBackoffJson,
    `codexProfile[${MALFORMED_PROFILE}]`,
  );
  check(
    // The backoff is about a *file*, not a name: a transient malformed file
    // that the fleet fixes must not cost a full hour of no managed telemetry.
    "a_backoff_is_dropped_as_soon_as_the_refused_file_identity_changes_so_a_fixed_file_heals_next_tick",
    healedAfterBackoff.code === 0 &&
      healedFormerlyMalformed?.status === "applied" &&
      healedFormerlyMalformed?.reason === undefined &&
      Date.parse(String(healedAfterBackoffJson.startedAt)) < backoffUntil &&
      readManagedConfigReconcileState(plimsollHome).backoff[
        `codexProfile[${MALFORMED_PROFILE}]`
      ] === undefined,
    {
      status: healedFormerlyMalformed?.status,
      insideTheOldWindow:
        Date.parse(String(healedAfterBackoffJson.startedAt)) < backoffUntil,
    },
  );

  // ---- review r1, F8: --reconcile is a parsed flag, not an argv token -----
  const usageStamp = readManagedConfigReconcileState(plimsollHome).lastRunAt;
  const usageError = runCli(["setup", "--claude-settings", "--reconcile"], env);
  check(
    "setup_with_a_settings_flag_and_no_path_is_a_usage_error_not_a_reconcile",
    usageError.code === 2 &&
      usageError.stderr.includes("--claude-settings needs a path") &&
      usageError.stdout.trim() === "" &&
      readManagedConfigReconcileState(plimsollHome).lastRunAt === usageStamp,
    { code: usageError.code, stderr: usageError.stderr.trim() },
  );

  // Two sources resolving to one header file: --grok-hooks and --codex-config
  // in one directory make both hooks read the same plimsoll.headers, so each
  // source's hook would send the other's token.
  const collisionRoot = path.join(home, "collided-header-root");
  fs.mkdirSync(collisionRoot, { recursive: true, mode: 0o700 });
  const collisionArgs = [
    "--grok-hooks",
    path.join(collisionRoot, "plimsoll.json"),
    "--codex-config",
    path.join(collisionRoot, "config.toml"),
  ];
  const collisionHeaderFile = path.join(collisionRoot, "plimsoll.headers");
  const boundaryStamp = readManagedConfigReconcileState(plimsollHome).lastRunAt;
  const collidedSetup = runCli(["setup", ...collisionArgs, "--yes"], env);
  const collidedReconcile = runCli(["setup", ...collisionArgs, "--reconcile"], env);
  const collidedReconcileJson = lastJson(collidedReconcile.stdout);
  const boundaryLine = `${collisionHeaderFile}: target refused: ${collisionHeaderFile}: two managed sources resolve to the same header file; refusing this target.`;
  check(
    // review r1, F8: the reconcile branch returned before this audit, so a host
    // with a hand-edited colliding pair was refused by --yes and silently
    // reconciled by --reconcile.
    "the_header_audience_boundary_refuses_under_reconcile_exactly_as_under_yes",
    collidedSetup.code === 1 &&
      collidedReconcile.code === 1 &&
      collidedSetup.stdout.includes(boundaryLine) &&
      collidedReconcile.stdout.includes(boundaryLine) &&
      collidedReconcileJson.status === "managed_config_header_audience_conflict" &&
      collidedReconcileJson.ownedRefusal === true &&
      collidedReconcileJson.applied === 0 &&
      collidedReconcileJson.receiptPath === null &&
      !fs.existsSync(collisionHeaderFile),
    {
      setupCode: collidedSetup.code,
      reconcileCode: collidedReconcile.code,
      status: collidedReconcileJson.status,
    },
  );
  check(
    "a_refused_audience_boundary_reconcile_writes_nothing_at_all",
    receipts(plimsollHome).length === 3 &&
      !fs.existsSync(path.join(collisionRoot, "plimsoll.json")) &&
      readManagedConfigReconcileState(plimsollHome).lastRunAt === boundaryStamp,
    { receipts: receipts(plimsollHome).length },
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
 * review r1, F3: the kill-switch and the cadence are read from the config file
 * on every tick, so flipping the flag stops a *running* collector instead of
 * waiting for a restart. Doctor already reads the file fresh; this is what
 * makes the daemon agree with it.
 */
function liveSettingsChecks(fixtureRoot: string) {
  const collectorHome = path.join(fixtureRoot, "live-settings-home");
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  const configFile = path.join(collectorHome, "collector.config.json");
  const boot = { enabled: true, intervalSeconds: 600 };
  const writeConfig = (reconcile: { enabled: boolean; intervalSeconds: number }) =>
    fs.writeFileSync(
      configFile,
      `${JSON.stringify({ port: 49173, managedConfig: { reconcile } }, null, 2)}\n`,
      { mode: 0o600 },
    );
  const now = Date.parse("2026-09-12T12:00:00.000Z");
  const decide = (settings: { enabled: boolean; intervalSeconds: number }) =>
    decideManagedConfigReconcile({
      enabled: settings.enabled,
      intervalSeconds: settings.intervalSeconds,
      now,
      lastRunAt: now - 3_600_000,
      drift: () => 1,
    });

  writeConfig({ enabled: true, intervalSeconds: 600 });
  const armed = readManagedConfigReconcileSettings(configFile, boot);
  const armedDecision = decide(armed);
  // The operator flips the kill-switch between two ticks of the same process.
  writeConfig({ enabled: false, intervalSeconds: 600 });
  const disarmed = readManagedConfigReconcileSettings(configFile, boot);
  const disarmedDecision = decide(disarmed);
  check(
    "flipping_the_kill_switch_in_the_config_file_makes_the_very_next_decision_decline",
    armed.enabled === true &&
      armed.source === "config_file" &&
      armedDecision.run === true &&
      disarmed.enabled === false &&
      disarmed.source === "config_file" &&
      disarmedDecision.run === false &&
      disarmedDecision.reason === "disabled",
    { armed: armedDecision, disarmed: disarmedDecision },
  );

  // The last run was an hour ago; a cadence lengthened to two hours is not due.
  writeConfig({ enabled: true, intervalSeconds: 7200 });
  const relengthened = readManagedConfigReconcileSettings(configFile, boot);
  check(
    "a_changed_interval_is_read_from_the_config_file_too_so_the_cadence_follows_it",
    relengthened.intervalSeconds === 7200 &&
      decide(relengthened).run === false &&
      decide(relengthened).reason === "interval_not_elapsed",
    { intervalSeconds: relengthened.intervalSeconds },
  );

  fs.writeFileSync(configFile, "{ this is not json", { mode: 0o600 });
  const fallback = readManagedConfigReconcileSettings(configFile, { enabled: false, intervalSeconds: 900 });
  fs.rmSync(configFile);
  const absentFallback = readManagedConfigReconcileSettings(configFile, boot);
  check(
    "an_unreadable_or_absent_config_file_falls_back_to_the_settings_captured_at_boot",
    fallback.source === "boot_config" &&
      fallback.enabled === false &&
      fallback.intervalSeconds === 900 &&
      absentFallback.source === "boot_config" &&
      absentFallback.enabled === true,
    { fallback, absentFallback },
  );
}

/**
 * review r1, F2: only the readback-that-found-nothing tick stamps. The other
 * two non-running decisions must not, or `nextEligibleAt` walks forward forever
 * and the interval gate goes inert.
 */
function decisionStampChecks(fixtureRoot: string) {
  const collectorHome = path.join(fixtureRoot, "decision-stamp-home");
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  const at = "2026-09-12T12:00:00.000Z";
  const stamped = stampManagedConfigReconcileDecision(
    collectorHome,
    { run: false, reason: "no_drift", nextEligibleAt: at },
    { at, absent: 2 },
  );
  const afterNoDrift = readManagedConfigReconcileState(collectorHome);
  const notStamped = [
    stampManagedConfigReconcileDecision(
      collectorHome,
      { run: false, reason: "disabled", nextEligibleAt: null },
      { at: "2026-09-12T13:00:00.000Z" },
    ),
    stampManagedConfigReconcileDecision(
      collectorHome,
      { run: false, reason: "interval_not_elapsed", nextEligibleAt: at },
      { at: "2026-09-12T13:00:00.000Z" },
    ),
    stampManagedConfigReconcileDecision(
      collectorHome,
      { run: true, reason: "drift", nextEligibleAt: at },
      { at: "2026-09-12T13:00:00.000Z" },
    ),
  ];
  const afterOthers = readManagedConfigReconcileState(collectorHome);
  check(
    "a_cadence_that_ran_its_readback_and_found_nothing_stamps_lastRunAt_and_lastResult_unchanged",
    stamped === true &&
      afterNoDrift.lastRunAt === at &&
      afterNoDrift.lastResult === "unchanged" &&
      afterNoDrift.lastApplied === 0 &&
      afterNoDrift.lastRefused === 0 &&
      afterNoDrift.lastAbsent === 2 &&
      !fs.existsSync(path.join(collectorHome, "receipts")),
    { state: afterNoDrift },
  );
  check(
    "a_disabled_or_not_yet_due_or_running_decision_never_moves_the_stamp",
    notStamped.every((value) => value === false) && afterOthers.lastRunAt === at,
    { notStamped, lastRunAt: afterOthers.lastRunAt },
  );
}

/**
 * review r1, F4: a transactional apply that loses a race with another writer
 * fails closed. That is a concurrency outcome, not a malformed file: it must be
 * skipped for this tick with no backoff, exactly like `changed_during_plan`.
 * A genuine refusal must still arm the hour.
 */
function applyRaceChecks(fixtureRoot: string) {
  const collectorHome = path.join(fixtureRoot, "apply-race-home");
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  const seatFile = writeSeatSettings(path.join(fixtureRoot, "apply-race-seats"), CHURNED_SEAT, fleetSeatDocument("race"));
  const toolOptions = {
    repoRoot,
    port: 49174,
    dataMode: "metadata" as const,
    claudeCodeProducerToken: "synthetic-claude-producer-token-apply-race-0",
  };
  // A target whose *plan* succeeds and whose *apply* fails the way the
  // transactional writer fails when another writer lands inside it.
  const racingTarget = (message: string): ManagedConfigTarget => ({
    name: `claudeSeat[${CHURNED_SEAT}]`,
    path: seatFile,
    family: "claude",
    discovered: true,
    run: (options, dryRun) => {
      const planned = composeManagedClaudeTargets(seatFile, path.join(fixtureRoot, "apply-race-absent"))[0]!
        .run(options, true);
      if (dryRun) return planned;
      throw new Error(message);
    },
  });

  const lost = runManagedConfigReconcile({
    collectorHome,
    targets: [racingTarget("CLAUDE_CONFIG_COMMIT_CLAIM_MISMATCH")],
    toolOptions,
  });
  const lostEntry = lost.targets[0];
  const lostState = readManagedConfigReconcileState(collectorHome);
  check(
    "an_apply_that_loses_a_race_is_skipped_for_this_tick_and_arms_no_backoff",
    lostEntry?.status === "skipped" &&
      lostEntry?.reason === "changed_during_apply" &&
      lostEntry?.nextEligibleAt === undefined &&
      lost.refused === 0 &&
      lost.skipped === 1 &&
      Object.keys(lostState.backoff).length === 0 &&
      lostState.lastResult === "skipped" &&
      backups(seatFile).length === 0,
    { entry: lostEntry, backoff: lostState.backoff },
  );

  const refused = runManagedConfigReconcile({
    collectorHome,
    targets: [racingTarget("CLAUDE_CONFIG_MALFORMED_JSON")],
    toolOptions,
  });
  const refusedState = readManagedConfigReconcileState(collectorHome);
  check(
    "an_apply_that_fails_for_any_other_reason_is_still_a_refusal_that_arms_the_hour",
    refused.targets[0]?.status === "refused" &&
      refused.refused === 1 &&
      Object.keys(refusedState.backoff).length === 1 &&
      refusedState.lastResult === "refused",
    { entry: refused.targets[0] },
  );
  check(
    "only_the_transactional_concurrency_failures_are_classified_as_a_lost_race",
    [
      "CLAUDE_CONFIG_COMMIT_CLAIM_MISMATCH",
      "CODEX_CONFIG_BOUND_CONTENT_CHANGED",
      "CLAUDE_CONFIG_VISIBLE_IDENTITY_MISMATCH",
      "/x/config.toml: config.toml was replaced after planning; refusing to read through a link or create a backup/write.",
      "/x/config.toml: bound config.toml content changed before commit; refusing to read through a link or create a backup/write.",
    ].every((reason) => isManagedConfigConcurrencyFailure(reason)) &&
      [
        "CLAUDE_CONFIG_MALFORMED_JSON",
        "CLAUDE_CONFIG_INVALID_ROOT",
        "CODEX_CONFIG_UNSAFE_LEAF_MODE",
        "/x/config.toml: config.toml is a symbolic link; refusing to read through a link or create a backup/write.",
        "EACCES: permission denied",
      ].every((reason) => !isManagedConfigConcurrencyFailure(reason)),
    {},
  );
}

/**
 * review r2, R1: the plan is a real read of the managed file — `applyCodexConfig`
 * binds the preimage *before* its dry-run early return — so a writer that
 * replaces the file inside that window throws the same transactional
 * concurrency failure the apply throws. It must be the same outcome: skipped
 * for this tick, no backoff. Before this round the plan-path catch refused and
 * armed the hour, which made the README's "losing a race with another writer is
 * not a refusal and arms no backoff" false one window earlier than F4's.
 */
function planRaceChecks(fixtureRoot: string) {
  const seats = path.join(fixtureRoot, "plan-race-seats");
  const seatFile = writeSeatSettings(seats, CHURNED_SEAT, fleetSeatDocument("plan-race"));
  const toolOptions = {
    repoRoot,
    port: 49176,
    dataMode: "metadata" as const,
    claudeCodeProducerToken: "synthetic-claude-producer-token-plan-race-00",
  };
  // The exact strings apply.ts throws out of its preimage reads:
  // `unsafePath(file, "config.toml was replaced while opening it")` at
  // apply.ts:1379 for Codex, and `claudeFail("LEAF_CHANGED")` inside
  // `readClaudePreimage` for Claude.
  const codexRace = `${seatFile}: config.toml was replaced while opening it; refusing to read through a link or create a backup/write.`;
  const claudeRace = "CLAUDE_CONFIG_LEAF_CHANGED";
  const planRacingTarget = (message: string): ManagedConfigTarget => ({
    name: `claudeSeat[${CHURNED_SEAT}]`,
    path: seatFile,
    family: "claude",
    discovered: true,
    run: (_options, dryRun) => {
      // The plan itself loses the race, so the apply is never reached.
      if (dryRun) throw new Error(message);
      throw new Error("unreachable: the plan never handed this target to an apply");
    },
  });
  const raced = [codexRace, claudeRace].map((message, index) => {
    const collectorHome = path.join(fixtureRoot, `plan-race-home-${index}`);
    fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
    const result = runManagedConfigReconcile({
      collectorHome,
      targets: [planRacingTarget(message)],
      toolOptions,
    });
    return { result, state: readManagedConfigReconcileState(collectorHome) };
  });
  check(
    "a_plan_that_loses_a_race_is_skipped_for_this_tick_and_arms_no_backoff",
    raced.every(
      ({ result, state }) =>
        result.targets[0]?.status === "skipped" &&
        result.targets[0]?.reason === "changed_during_plan" &&
        result.targets[0]?.nextEligibleAt === undefined &&
        result.refused === 0 &&
        result.skipped === 1 &&
        Object.keys(state.backoff).length === 0 &&
        state.lastResult === "skipped" &&
        result.receiptPath === null,
    ) && backups(seatFile).length === 0,
    { messages: [codexRace, claudeRace], entries: raced.map(({ result }) => result.targets[0]) },
  );

  const refusedHome = path.join(fixtureRoot, "plan-race-refusal-home");
  fs.mkdirSync(refusedHome, { recursive: true, mode: 0o700 });
  const refused = runManagedConfigReconcile({
    collectorHome: refusedHome,
    targets: [planRacingTarget("CLAUDE_CONFIG_MALFORMED_JSON")],
    toolOptions,
  });
  const refusedState = readManagedConfigReconcileState(refusedHome);
  check(
    "a_plan_that_fails_for_any_other_reason_is_still_a_refusal_that_arms_the_hour",
    refused.targets[0]?.status === "refused" &&
      refused.refused === 1 &&
      typeof refused.targets[0]?.nextEligibleAt === "string" &&
      Object.keys(refusedState.backoff).length === 1 &&
      refusedState.lastResult === "refused",
    { entry: refused.targets[0] },
  );
}

/**
 * review r2, R3 and R4: the run stamp, the backoff map and the backup record
 * are read-modify-written by two processes (the daemon cadence and an
 * operator's `setup --reconcile`) over one collector home, and the daemon's
 * receipts reported `durationMs: 0` because the duration was read from the
 * fixed clock the tick passes for its deadlines.
 */
function stateIntegrityChecks(fixtureRoot: string) {
  const collectorHome = path.join(fixtureRoot, "state-integrity-home");
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  const seats = path.join(fixtureRoot, "state-integrity-seats");
  const absent = path.join(fixtureRoot, "state-integrity-absent");
  const toolOptions = {
    repoRoot,
    port: 49177,
    dataMode: "metadata" as const,
    claudeCodeProducerToken: "synthetic-claude-producer-token-state-0000000",
  };
  const seatTarget = (slug: string): ManagedConfigTarget => {
    const file = writeSeatSettings(seats, slug, fleetSeatDocument(slug));
    const composed = composeManagedClaudeTargets(file, absent)[0]!;
    return { ...composed, name: `claudeSeat[${slug}]`, path: file, discovered: true };
  };

  // R3: a fixed run clock (what the daemon passes so every deadline in one tick
  // comes from one instant) must not flatten the receipt's duration.
  const fixedNow = Date.parse("2026-09-12T14:00:00.000Z");
  let monotonicReads = 0;
  const timed = runManagedConfigReconcile({
    collectorHome,
    targets: [seatTarget("timed-seat")],
    toolOptions,
    now: () => fixedNow,
    monotonicNow: () => (monotonicReads++ === 0 ? 1_000 : 1_250),
  });
  const timedReceipt = JSON.parse(fs.readFileSync(timed.receiptPath!, "utf8")) as Record<string, unknown>;
  check(
    "a_receipt_reports_real_wall_time_even_when_the_run_clock_is_fixed",
    timed.applied === 1 &&
      timed.startedAt === new Date(fixedNow).toISOString() &&
      timed.durationMs === 250 &&
      timedReceipt.durationMs === 250,
    { durationMs: timed.durationMs, receiptDurationMs: timedReceipt.durationMs },
  );

  // R4: run A (the operator) refuses a target and finishes *after* run B (the
  // daemon tick) has already written its own stamp and backup record from a
  // snapshot that predates A's refusal. Nothing either run decided may be lost.
  const refusingTarget: ManagedConfigTarget = {
    name: "claudeSeat[refused-seat]",
    path: writeSeatSettings(seats, "refused-seat", fleetSeatDocument("refused")),
    family: "claude",
    discovered: true,
    run: () => {
      throw new Error("CLAUDE_CONFIG_MALFORMED_JSON");
    },
  };
  const interleavedTarget = seatTarget("interleaved-seat");
  const runA = { at: Date.parse("2026-09-12T15:00:00.000Z") };
  const runB = { at: Date.parse("2026-09-12T15:00:01.000Z") };
  let innerRuns = 0;
  const outer = runManagedConfigReconcile({
    collectorHome,
    targets: [refusingTarget, seatTarget("outer-seat")],
    toolOptions,
    now: () => runA.at,
    // The seam fires between the last target's plan and its apply: the second
    // process's whole run lands inside this one's window.
    onPlanned: () => {
      if (innerRuns++ > 0) return;
      runManagedConfigReconcile({
        collectorHome,
        targets: [interleavedTarget],
        toolOptions,
        now: () => runB.at,
      });
    },
  });
  const merged = readManagedConfigReconcileState(collectorHome);
  check(
    "two_interleaved_runs_keep_both_the_armed_backoff_and_the_newer_run_stamp",
    outer.refused === 1 &&
      // The run that finished last started first: its stamp must not walk the
      // cadence backwards, and the interleaved run's applied count survives.
      merged.lastRunAt === new Date(runB.at).toISOString() &&
      merged.lastApplied === 1 &&
      // The refusal the last writer never saw is still armed.
      merged.backoff["claudeSeat[refused-seat]"] !== undefined &&
      Object.keys(merged.backoff).length === 1 &&
      // Both runs' backup records survive one another's write.
      merged.backups["claudeSeat[interleaved-seat]"]?.names.length === 1 &&
      merged.backups["claudeSeat[outer-seat]"]?.names.length === 1,
    {
      lastRunAt: merged.lastRunAt,
      backoff: Object.keys(merged.backoff),
      backups: Object.keys(merged.backups),
    },
  );

  // R6: a host with no Plimsoll-local credentials manages nothing at all.
  const unavailableHome = path.join(fixtureRoot, "state-unavailable-home");
  fs.mkdirSync(unavailableHome, { recursive: true, mode: 0o700 });
  const at = "2026-09-12T16:00:00.000Z";
  const stamped = stampManagedConfigReconcileDecision(
    unavailableHome,
    { run: false, reason: "no_drift", nextEligibleAt: at },
    { at, result: "unavailable" },
  );
  const unavailable = managedConfigReconcileDoctorSection(unavailableHome, {
    enabled: true,
    intervalSeconds: 600,
  });
  check(
    "a_tick_with_no_local_credentials_stamps_unavailable_rather_than_unchanged",
    stamped === true &&
      unavailable.lastResult === "unavailable" &&
      unavailable.lastRunAt === at &&
      unavailable.nextEligibleAt === "2026-09-12T16:10:00.000Z",
    { doctor: unavailable },
  );
}

/**
 * review r2, R2: `setup --yes` writes into the same
 * `<basename>.plimsoll-backup-*` namespace the cadence prunes, so matching the
 * namespace let the cadence delete the installer's pre-install backup — the
 * only copy of the host's pre-Plimsoll bytes. The reviewer's arm: one 11-day
 * old installer backup plus six older-than-24h cadence backups, then one more
 * apply.
 */
function setupBackupChecks(fixtureRoot: string) {
  const home = path.join(fixtureRoot, "setup-backup-home");
  const collectorHome = path.join(fixtureRoot, "setup-backup-plimsoll-home");
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  const claudeFile = path.join(home, ".claude", "settings.json");
  fs.mkdirSync(path.dirname(claudeFile), { recursive: true, mode: 0o700 });
  const toolOptions = {
    repoRoot,
    port: 49178,
    dataMode: "metadata" as const,
    claudeCodeProducerToken: "synthetic-claude-producer-token-setup-bak-00",
  };
  const churn = (round: number, prune: Record<string, number> | undefined) => {
    fs.writeFileSync(claudeFile, `${JSON.stringify(fleetSeatDocument(`setup-backup-${round}`), null, 2)}\n`, {
      mode: 0o600,
    });
    return runManagedConfigReconcile({
      collectorHome,
      targets: composeManagedClaudeTargets(claudeFile, path.join(fixtureRoot, "setup-backup-absent")),
      toolOptions,
      ...(prune ? { prune } : {}),
    });
  };
  const age = (file: string, ms: number) => {
    const when = (Date.now() - ms) / 1000;
    fs.utimesSync(file, when, when);
  };

  // What `setup --yes` leaves behind: the pre-install copy, in the same
  // namespace, written by a code path this change never touches.
  const setupBackup = `${claudeFile}.plimsoll-backup-2026-09-01T00-00-00-000Z`;
  fs.writeFileSync(setupBackup, '{"preInstall":"synthetic pre-plimsoll bytes"}\n', { mode: 0o600 });
  age(setupBackup, 11 * 24 * 60 * 60 * 1000);

  // Six cadence backups, every one of them older than the 24 h floor and all
  // newer than the installer's.
  for (let round = 0; round < 6; round += 1) churn(round, { backupsPerFile: 99, minBackupAgeMs: 0 });
  const cadenceBackups = backups(claudeFile).filter((name) => !setupBackup.endsWith(name));
  for (const [index, name] of cadenceBackups.entries()) {
    age(path.join(path.dirname(claudeFile), name), (7 - index) * 25 * 60 * 60 * 1000);
  }
  const before = backups(claudeFile);
  const beforeState = readManagedConfigReconcileState(collectorHome);

  churn(6, undefined);
  const after = backups(claudeFile);
  const state = readManagedConfigReconcileState(collectorHome);
  const record = state.backups["claude"]?.names ?? [];
  const setupBackupName = path.basename(setupBackup);
  check(
    "the_installers_own_pre_install_backup_is_never_pruned_by_the_cadence",
    before.length === 7 &&
      before.includes(setupBackupName) &&
      after.includes(setupBackupName) &&
      fs.readFileSync(setupBackup, "utf8").includes("pre-plimsoll") &&
      // The cadence's own set is still bounded, and the installer's backup was
      // never in the record the prune works from.
      after.filter((name) => name !== setupBackupName).length === MANAGED_CONFIG_BACKUPS_KEPT_PER_FILE &&
      !record.includes(setupBackupName) &&
      record.length === MANAGED_CONFIG_BACKUPS_KEPT_PER_FILE &&
      (beforeState.backups["claude"]?.names.length ?? 0) === 6,
    { before: before.length, after: after.length, recorded: record.length },
  );

  // A backup the cadence did not write is not a candidate even when it is the
  // only thing left: an operator who clears the state file cannot make the
  // installer's copy prunable.
  fs.rmSync(managedConfigReconcileStatePath(collectorHome));
  // keep 0 with the age floor lifted: every backup on disk would be a candidate
  // under the old namespace rule. With an empty record the only thing this run
  // may prune is the backup it just wrote itself.
  churn(7, { backupsPerFile: 0, minBackupAgeMs: 0 });
  const unrecorded = backups(claudeFile);
  check(
    "a_backup_the_cadence_did_not_record_is_never_a_pruning_candidate",
    // Every backup that was there before this run is still there; the only one
    // this run could have pruned is the one it wrote itself, and that one is
    // newer than the run's own start so the age floor holds it too.
    after.every((name) => unrecorded.includes(name)) &&
      unrecorded.includes(setupBackupName) &&
      unrecorded.length === after.length + 1 &&
      (readManagedConfigReconcileState(collectorHome).backups["claude"]?.names.length ?? 0) === 1,
    { backups: unrecorded.length, survived: after.length },
  );
}

/**
 * review r1, F5: backups are written *beside* the managed file — inside the
 * fleet-owned seat and profile directories — and receipts accumulate in the
 * collector home. Nothing pruned either.
 */
function pruneChecks(fixtureRoot: string) {
  const home = path.join(fixtureRoot, "prune-home");
  const collectorHome = path.join(fixtureRoot, "prune-plimsoll-home");
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  const claudeFile = path.join(home, ".claude", "settings.json");
  fs.mkdirSync(path.dirname(claudeFile), { recursive: true, mode: 0o700 });
  const toolOptions = {
    repoRoot,
    port: 49175,
    dataMode: "metadata" as const,
    claudeCodeProducerToken: "synthetic-claude-producer-token-prune-000000",
  };
  const churn = (round: number, prune: Record<string, number> | undefined) => {
    fs.writeFileSync(claudeFile, `${JSON.stringify(fleetSeatDocument(`churn-${round}`), null, 2)}\n`, { mode: 0o600 });
    return runManagedConfigReconcile({
      collectorHome,
      targets: composeManagedClaudeTargets(claudeFile, path.join(fixtureRoot, "prune-absent-seats")),
      toolOptions,
      ...(prune ? { prune } : {}),
    });
  };

  // Default policy: a backup younger than 24 h is live rollback material and is
  // never pruned, whatever the count says.
  for (let round = 0; round < 8; round += 1) churn(round, undefined);
  const youngBackups = backups(claudeFile).length;
  check(
    "a_backup_younger_than_24h_is_never_pruned_however_many_there_are",
    youngBackups === 8 && MANAGED_CONFIG_BACKUPS_KEPT_PER_FILE === 5,
    { backups: youngBackups },
  );

  // The same churn with the age floor lifted: the count bound takes over. The
  // oldest backup this file has is never a candidate (review r2, R2), so the
  // surviving set is the five newest *plus* that first one.
  const oldestBackup = backups(claudeFile)[0];
  const pruned = churn(8, { minBackupAgeMs: 0 });
  const survivors = backups(claudeFile);
  check(
    "at_most_five_prunable_backups_survive_plus_the_oldest_backup_which_is_never_pruned",
    survivors.length === MANAGED_CONFIG_BACKUPS_KEPT_PER_FILE + 1 &&
      survivors[0] === oldestBackup &&
      pruned.applied === 1 &&
      // The survivors are the newest, and the file itself is untouched by the prune.
      survivors.slice(1).join(",") ===
        backups(claudeFile).slice(-MANAGED_CONFIG_BACKUPS_KEPT_PER_FILE).join(",") &&
      fs.existsSync(claudeFile),
    { backups: survivors.length, oldestKept: survivors[0] === oldestBackup },
  );

  for (let round = 9; round < 9 + MANAGED_CONFIG_RECEIPTS_KEPT + 6; round += 1) {
    churn(round, { minBackupAgeMs: 0 });
  }
  setupBackupChecks(fixtureRoot);
  const kept = receipts(collectorHome);
  check(
    "at_most_twenty_reconcile_receipts_survive_and_the_newest_are_the_survivors",
    kept.length === MANAGED_CONFIG_RECEIPTS_KEPT &&
      backups(claudeFile).length === MANAGED_CONFIG_BACKUPS_KEPT_PER_FILE + 1 &&
      kept.join(",") === [...kept].sort().join(","),
    { receipts: kept.length, backups: backups(claudeFile).length },
  );
  return { collectorHome, claudeFile };
}

/**
 * review r1, F6: at fleet scale (6 Claude seats + 20 Codex profiles + the two
 * home targets = 28 targets) the synchronous tick blocked the collector's event
 * loop — the same process that serves /hooks/* and the OTLP receiver — for
 * ~0.6 s on a full-churn run. The daemon's tick now yields between targets, so
 * the longest chunk it can owe the loop is one target.
 */
async function eventLoopBoundChecks(fixtureRoot: string) {
  const home = path.join(fixtureRoot, "scale-home");
  const collectorHome = path.join(fixtureRoot, "scale-plimsoll-home");
  const seatsRoot = path.join(home, ".claude-seats");
  const profilesRoot = path.join(home, ".codex-profiles");
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  const claudeFile = path.join(home, ".claude", "settings.json");
  const codexFile = path.join(home, ".codex", "config.toml");
  const codexHeaderFile = path.join(home, ".codex", "plimsoll.headers");
  fs.mkdirSync(path.dirname(claudeFile), { recursive: true, mode: 0o700 });
  fs.mkdirSync(path.dirname(codexFile), { recursive: true, mode: 0o700 });
  fs.writeFileSync(codexHeaderFile, "x-plimsoll-producer: synthetic-scale-producer-token\n", { mode: 0o600 });
  const toolOptions = {
    repoRoot,
    port: 49176,
    dataMode: "metadata" as const,
    claudeCodeProducerToken: "synthetic-claude-producer-token-scale-000000",
    codexProducerToken: "synthetic-codex-producer-token-scale-00000000",
    codexHeaderFile,
  };
  const SEATS = 6;
  const PROFILES = 20;
  const seed = () => {
    fs.writeFileSync(claudeFile, `${JSON.stringify(fleetSeatDocument("scale-home"), null, 2)}\n`, { mode: 0o600 });
    fs.writeFileSync(codexFile, fleetProfileToml("scale-home"), { mode: 0o600 });
    for (let index = 0; index < SEATS; index += 1) {
      writeSeatSettings(seatsRoot, `scale-seat-${index}`, fleetSeatDocument(`scale-${index}`));
    }
    for (let index = 0; index < PROFILES; index += 1) {
      writeProfileConfig(profilesRoot, `scale-profile-${index}`, fleetProfileToml(`scale-${index}`));
    }
  };
  const targets = () => [
    ...composeManagedClaudeTargets(claudeFile, home),
    ...composeManagedCodexTargets(codexFile, home),
  ];

  seed();
  const targetCount = targets().length;
  const readbackStart = process.hrtime.bigint();
  const readback = runManagedConfigReconcile({
    collectorHome,
    targets: targets(),
    toolOptions,
    dryRun: true,
  });
  const readbackMs = Number(process.hrtime.bigint() - readbackStart) / 1e6;

  // The synchronous full-churn run: what the daemon used to do in one chunk.
  seed();
  const syncStart = process.hrtime.bigint();
  const syncRun = runManagedConfigReconcile({
    collectorHome,
    targets: targets(),
    toolOptions,
    prune: { minBackupAgeMs: 0 },
  });
  const syncMs = Number(process.hrtime.bigint() - syncStart) / 1e6;

  // The same work through the daemon's async entrypoint, with a loop-lag
  // sampler measuring the longest stretch the event loop was actually held.
  seed();
  let longestChunkMs = 0;
  let samples = 0;
  const SAMPLE_MS = 4;
  let previous = process.hrtime.bigint();
  const sampler = setInterval(() => {
    const nowNs = process.hrtime.bigint();
    const heldMs = Number(nowNs - previous) / 1e6 - SAMPLE_MS;
    previous = nowNs;
    samples += 1;
    if (heldMs > longestChunkMs) longestChunkMs = heldMs;
  }, SAMPLE_MS);
  const asyncStart = process.hrtime.bigint();
  const asyncRun = await runManagedConfigReconcileAsync({
    collectorHome,
    targets: targets(),
    toolOptions,
    prune: { minBackupAgeMs: 0 },
  });
  const asyncMs = Number(process.hrtime.bigint() - asyncStart) / 1e6;
  clearInterval(sampler);

  const LONGEST_CHUNK_BUDGET_MS = 50;
  check(
    "the_fleet_scale_fixture_is_the_reviewers_twenty_eight_targets",
    targetCount === SEATS + PROFILES + 2 && readback.targets.length === targetCount,
    { targets: targetCount, seats: SEATS, profiles: PROFILES },
  );
  check(
    "the_async_tick_never_holds_the_event_loop_for_more_than_the_chunk_budget_on_a_full_churn_run",
    asyncRun.applied === targetCount &&
      samples > 0 &&
      longestChunkMs < LONGEST_CHUNK_BUDGET_MS,
    {
      targets: targetCount,
      applied: asyncRun.applied,
      longestChunkMs: Number(longestChunkMs.toFixed(1)),
      budgetMs: LONGEST_CHUNK_BUDGET_MS,
      samples,
      syncRunMs: Number(syncMs.toFixed(1)),
      asyncRunMs: Number(asyncMs.toFixed(1)),
      driftReadbackMs: Number(readbackMs.toFixed(1)),
    },
  );
  check(
    "the_async_tick_reconciles_exactly_what_the_synchronous_run_reconciles",
    syncRun.applied === asyncRun.applied &&
      syncRun.refused === asyncRun.refused &&
      syncRun.skipped === asyncRun.skipped &&
      syncRun.targets.map((entry) => `${entry.name}:${entry.status}`).join(",") ===
        asyncRun.targets.map((entry) => `${entry.name}:${entry.status}`).join(","),
    { sync: syncRun.applied, async: asyncRun.applied },
  );
  return {
    targets: targetCount,
    driftReadbackMs: Number(readbackMs.toFixed(1)),
    syncRunMs: Number(syncMs.toFixed(1)),
    asyncRunMs: Number(asyncMs.toFixed(1)),
    longestChunkMs: Number(longestChunkMs.toFixed(1)),
  };
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

async function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-managed-config-reconcile-proof-"));
  const fixtureRoot = path.join(sandbox, "fixture");
  const fixture = useFixtureRoot(fixtureRoot, {
    home: path.join(fixtureRoot, "must-remain-absent-operator-home"),
  });
  try {
    commandChecks(fixture.root);
    concurrentWriterChecks(fixture.root);
    applyRaceChecks(fixture.root);
    planRaceChecks(fixture.root);
    stateIntegrityChecks(fixture.root);
    liveSettingsChecks(fixture.root);
    decisionStampChecks(fixture.root);
    pruneChecks(fixture.root);
    const scale = await eventLoopBoundChecks(fixture.root);
    decisionChecks();
    check(
      "the_fixture_home_the_guard_protects_was_never_created",
      !fs.existsSync(fixture.home),
      { home: "must-remain-absent-operator-home" },
    );
    console.log(JSON.stringify({ bead: "eco-6hoxj.50", ok: true, scale, checks }, null, 2));
  } finally {
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
