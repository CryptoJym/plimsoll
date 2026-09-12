/**
 * Fleet Claude seat settings proof (bead eco-6hoxj.48).
 *
 * Fleet lanes run Claude Code with CLAUDE_CONFIG_DIR=~/.claude-seats/<slug>, so
 * the managed exporter and hooks `setup` merges into ~/.claude/settings.json
 * never reached them: a one-turn seat session produced a single transcript row,
 * no spans and no hook events. This proof pins the seat target family shut from
 * both ends under a fixture HOME with three seats — fleet-style hooks only,
 * already managed, and no settings.json at all:
 *
 *   a) library: the dry run plans exactly the managed keys under
 *      `claudeSeat[<slug>]`, the apply merges additively (managed env present,
 *      Plimsoll hooks exactly once, the seat's own hooks byte-identical and in
 *      order, unknown keys untouched, file mode preserved), the second apply is
 *      a byte no-op with no backup churn, a reconcile after a foreign hook edit
 *      preserves that edit, and a real-home seat path is refused;
 *   b) command: `setup --dry-run`, `setup --yes` and `doctor --read-only --json`
 *      report the same seats — doctor names the unmanaged seat before setup and
 *      none after, the settings-less seat is skipped and reported and is never
 *      created, and a second `setup --yes` is a no-op.
 *
 * Every path is synthetic and below a per-run fixture root. Tokens are fixture
 * credentials minted into the fixture Plimsoll home; nothing here prints one.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";

import { useFixtureRoot } from "./lib/fixture-root";
import {
  ManagedConfigTargetError,
  applyClaudeSettings,
  discoverClaudeSeats,
  generateClaudeCodeSettings,
} from "../packages/collector-config/src/index";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";

type Check = { name: string; passed: true; detail: Record<string, unknown> };

const repoRoot = path.resolve(import.meta.dirname, "..");
const cli = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const loader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
const checks: Check[] = [];

const FLEET_SEAT = "fleet-hooks-only-seat";
const MANAGED_SEAT = "already-managed-seat";
const BARE_SEAT = "no-settings-seat";

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

function readJson(file: string) {
  return JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>;
}

function writeSeatSettings(root: string, slug: string, document: unknown, mode = 0o600) {
  const file = path.join(root, slug, "settings.json");
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, `${JSON.stringify(document, null, 2)}\n`, { mode });
  fs.chmodSync(file, mode);
  return file;
}

/**
 * The fleet's own seat hooks, with synthetic commands: never a real Inbox or
 * Mem0 command line. Shape and events mirror what the seat tooling writes —
 * three of the events collide with the managed ones, which is exactly the merge
 * this proof has to get right.
 */
function fleetHookGroup(event: string) {
  return {
    hooks: [
      {
        type: "command",
        command: `/synthetic/fleet/seat-hook --event ${event} --synthetic`,
        timeout: 5,
      },
    ],
  };
}

function fleetSeatDocument() {
  return {
    // An unknown top-level key the merge must carry through untouched.
    model: "synthetic-fleet-model",
    hooks: {
      SessionStart: [fleetHookGroup("SessionStart")],
      SubagentStart: [fleetHookGroup("SubagentStart")],
      UserPromptSubmit: [fleetHookGroup("UserPromptSubmit")],
      Stop: [fleetHookGroup("Stop")],
      SessionEnd: [fleetHookGroup("SessionEnd")],
      PostToolUse: [{ matcher: ".*", ...fleetHookGroup("PostToolUse") }],
    },
  };
}

/** The fleet seat as it looks once the managed content is already merged in. */
function managedSeatDocument(generated: ReturnType<typeof generateClaudeCodeSettings>) {
  const fleet = fleetSeatDocument();
  return {
    model: fleet.model,
    env: { ...generated.env },
    hooks: {
      SessionStart: fleet.hooks.SessionStart,
      SubagentStart: fleet.hooks.SubagentStart,
      UserPromptSubmit: [...fleet.hooks.UserPromptSubmit, ...generated.hooks.UserPromptSubmit],
      Stop: [...fleet.hooks.Stop, ...generated.hooks.Stop],
      SessionEnd: fleet.hooks.SessionEnd,
      PostToolUse: [...fleet.hooks.PostToolUse, ...generated.hooks.PostToolUse],
    },
  };
}

/** Managed keys a seat plan must report, in the order the merge emits them. */
function expectedPlanKeys(target: string, generated: ReturnType<typeof generateClaudeCodeSettings>) {
  return [
    ...Object.keys(generated.env).map((key) => `${target}.env.${key}`),
    ...Object.keys(generated.hooks).map((event) => `${target}.hooks.${event}`),
  ];
}

/** The seat's own hook groups, extracted so a merge can be compared to them. */
function foreignHookGroups(document: Record<string, unknown>) {
  const hooks = (document.hooks ?? {}) as Record<string, unknown[]>;
  return Object.fromEntries(
    Object.entries(hooks).map(([event, groups]) => [
      event,
      groups.filter((group) => !JSON.stringify(group).includes("/hooks/claude-code")),
    ]),
  );
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

function libraryChecks(fixtureRoot: string) {
  const home = path.join(fixtureRoot, "library-home");
  const seatsRoot = path.join(home, ".claude-seats");
  const port = 49148;
  const generated = generateClaudeCodeSettings({
    repoRoot: "/synthetic/plimsoll/source",
    port,
    dataMode: "metadata",
  });

  const fleetSeatFile = writeSeatSettings(seatsRoot, FLEET_SEAT, fleetSeatDocument(), 0o640);
  const managedSeatFile = writeSeatSettings(seatsRoot, MANAGED_SEAT, managedSeatDocument(generated));
  fs.mkdirSync(path.join(seatsRoot, BARE_SEAT), { recursive: true, mode: 0o700 });
  // A file that is not a seat directory must not become a target.
  fs.writeFileSync(path.join(seatsRoot, "README"), "synthetic\n", { mode: 0o600 });
  const fleetSeatBefore = readJson(fleetSeatFile);
  const fleetForeignBefore = foreignHookGroups(fleetSeatBefore);

  const discovered = discoverClaudeSeats(home);
  check(
    "discovery_lists_every_seat_directory_and_marks_the_one_without_settings",
    discovered.length === 3 &&
      discovered.map((seat) => seat.slug).join(",") === [MANAGED_SEAT, FLEET_SEAT, BARE_SEAT].sort().join(",") &&
      discovered.filter((seat) => seat.hasSettings).length === 2 &&
      discovered.find((seat) => seat.slug === BARE_SEAT)?.hasSettings === false,
    { seats: discovered.map((seat) => ({ slug: seat.slug, hasSettings: seat.hasSettings })) },
  );
  check(
    "discovery_returns_no_seats_for_a_home_without_a_seats_directory",
    discoverClaudeSeats(path.join(fixtureRoot, "home-without-seats")).length === 0,
    { home: "home-without-seats" },
  );

  const fleetTarget = `claudeSeat[${FLEET_SEAT}]`;
  const preview = applyClaudeSettings(fleetSeatFile, generated, {
    dryRun: true,
    managedTarget: fleetTarget,
  });
  check(
    "dry_run_plans_exactly_the_managed_seat_keys_and_writes_nothing",
    preview.changed &&
      JSON.stringify(preview.plan?.map((entry) => entry.key)) ===
        JSON.stringify(expectedPlanKeys(fleetTarget, generated)) &&
      preview.plan?.filter((entry) => entry.action === "added").length === Object.keys(generated.env).length &&
      preview.plan?.filter((entry) => entry.action === "updated").length === Object.keys(generated.hooks).length &&
      preview.backupPath === undefined &&
      digestOf(fleetSeatFile) === sha256(`${JSON.stringify(fleetSeatBefore, null, 2)}\n`) &&
      backups(fleetSeatFile).length === 0,
    {
      keys: preview.plan?.map((entry) => `${entry.key} ${entry.action}`),
      backups: backups(fleetSeatFile).length,
    },
  );

  const managedTarget = `claudeSeat[${MANAGED_SEAT}]`;
  const managedPreview = applyClaudeSettings(managedSeatFile, generated, {
    dryRun: true,
    managedTarget,
  });
  check(
    "an_already_managed_seat_plans_every_key_unchanged",
    !managedPreview.changed &&
      managedPreview.plan?.every((entry) => entry.action === "unchanged") &&
      JSON.stringify(managedPreview.plan?.map((entry) => entry.key)) ===
        JSON.stringify(expectedPlanKeys(managedTarget, generated)),
    { changed: managedPreview.changed, keys: managedPreview.plan?.length ?? 0 },
  );

  const modeBefore = fs.statSync(fleetSeatFile).mode & 0o777;
  const applied = applyClaudeSettings(fleetSeatFile, generated, { managedTarget: fleetTarget });
  const merged = readJson(fleetSeatFile);
  const mergedHooks = merged.hooks as Record<string, unknown[]>;
  const ownedCount = (event: string) =>
    mergedHooks[event].filter((group) => JSON.stringify(group).includes("/hooks/claude-code")).length;
  check(
    "apply_merges_the_managed_env_and_hooks_without_disturbing_the_seat",
    applied.changed &&
      typeof applied.backupPath === "string" &&
      Object.entries(generated.env).every(([key, value]) =>
        (merged.env as Record<string, string>)[key] === value) &&
      merged.model === fleetSeatBefore.model &&
      Object.keys(generated.hooks).every((event) => ownedCount(event) === 1) &&
      JSON.stringify(foreignHookGroups(merged)) === JSON.stringify(fleetForeignBefore) &&
      Object.keys(mergedHooks).join(",") === Object.keys(fleetSeatBefore.hooks as object).join(",") &&
      (fs.statSync(fleetSeatFile).mode & 0o777) === modeBefore,
    {
      ownedPerEvent: Object.fromEntries(Object.keys(generated.hooks).map((event) => [event, ownedCount(event)])),
      hookEvents: Object.keys(mergedHooks),
      mode: (fs.statSync(fleetSeatFile).mode & 0o777).toString(8),
    },
  );
  check(
    "the_seats_own_hook_groups_keep_their_position_ahead_of_the_managed_group",
    Object.keys(generated.hooks).every((event) => {
      const groups = mergedHooks[event];
      return !JSON.stringify(groups[0]).includes("/hooks/claude-code") &&
        JSON.stringify(groups[groups.length - 1]).includes("/hooks/claude-code");
    }),
    { events: Object.keys(generated.hooks) },
  );
  check(
    "a_seat_without_settings_json_is_never_created_by_an_apply",
    !fs.existsSync(path.join(seatsRoot, BARE_SEAT, "settings.json")),
    { seat: BARE_SEAT },
  );

  const afterFirst = digestOf(fleetSeatFile);
  const backupsAfterFirst = backups(fleetSeatFile);
  const second = applyClaudeSettings(fleetSeatFile, generated, { managedTarget: fleetTarget });
  check(
    "the_second_apply_is_a_byte_identical_no_op_without_backup_churn",
    !second.changed &&
      second.backupPath === undefined &&
      second.plan?.every((entry) => entry.action === "unchanged") &&
      digestOf(fleetSeatFile) === afterFirst &&
      JSON.stringify(backups(fleetSeatFile)) === JSON.stringify(backupsAfterFirst),
    { backups: backupsAfterFirst.length, digestStable: digestOf(fleetSeatFile) === afterFirst },
  );

  // The seat tooling keeps editing its own hooks after setup ran; a reconcile
  // must carry the edit forward, not revert it and not duplicate the managed
  // group.
  const edited = readJson(fleetSeatFile);
  const editedHooks = edited.hooks as Record<string, unknown[]>;
  editedHooks.UserPromptSubmit = [
    fleetHookGroup("UserPromptSubmitEdited"),
    ...editedHooks.UserPromptSubmit.slice(1),
  ];
  editedHooks.SessionStart = [fleetHookGroup("SessionStart"), fleetHookGroup("SessionStartSecond")];
  fs.writeFileSync(fleetSeatFile, `${JSON.stringify(edited, null, 2)}\n`, { mode: 0o600 });
  const editedForeign = foreignHookGroups(edited);
  const reconciled = applyClaudeSettings(fleetSeatFile, generated, { managedTarget: fleetTarget });
  const afterEdit = readJson(fleetSeatFile);
  check(
    "a_reconcile_after_a_foreign_hook_edit_preserves_the_edit_and_keeps_one_managed_group",
    !reconciled.changed &&
      JSON.stringify(foreignHookGroups(afterEdit)) === JSON.stringify(editedForeign) &&
      Object.keys(generated.hooks).every((event) =>
        (afterEdit.hooks as Record<string, unknown[]>)[event]
          .filter((group) => JSON.stringify(group).includes("/hooks/claude-code")).length === 1),
    { changed: reconciled.changed, foreignPreserved: true },
  );

  // The guard: a seat path inside the operator's real home is refused before
  // any filesystem call. The path below is named, never created or read.
  const operatorSeat = path.join(os.userInfo().homedir, ".claude-seats", "synthetic-slug", "settings.json");
  let refusal = "no_refusal";
  try {
    applyClaudeSettings(operatorSeat, generated, { managedTarget: "claudeSeat[synthetic-slug]" });
  } catch (error) {
    refusal = error instanceof ManagedConfigTargetError
      ? error.code
      : `unexpected:${error instanceof Error ? error.message : String(error)}`;
  }
  check(
    "a_real_home_seat_path_is_refused_by_the_fixture_root_guard",
    refusal === "TARGET_INSIDE_REAL_HOME" &&
      !fs.existsSync(path.join(os.userInfo().homedir, ".claude-seats", "synthetic-slug")),
    { refusal },
  );

  return { home, seatsRoot };
}

function commandChecks(fixtureRoot: string) {
  const home = path.join(fixtureRoot, "command-home");
  const plimsollHome = path.join(home, ".plimsoll");
  const seatsRoot = path.join(home, ".claude-seats");
  const port = 49149;
  fs.mkdirSync(plimsollHome, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(plimsollHome, "collector.config.json"),
    `${JSON.stringify({ port }, null, 2)}\n`, { mode: 0o600 });
  // Provision the fixture producer credentials up front so the already-managed
  // seat below carries exactly the token `setup` will generate with. The value
  // never leaves this process.
  const auth = loadOrCreateLocalIngestAuth(plimsollHome);
  const generated = generateClaudeCodeSettings({
    repoRoot: "/synthetic/plimsoll/source",
    port,
    dataMode: "metadata",
    claudeCodeProducerToken: auth.claudeCodeProducer,
  });

  const fleetSeatFile = writeSeatSettings(seatsRoot, FLEET_SEAT, fleetSeatDocument());
  const managedSeatFile = writeSeatSettings(seatsRoot, MANAGED_SEAT, managedSeatDocument(generated));
  fs.mkdirSync(path.join(seatsRoot, BARE_SEAT), { recursive: true, mode: 0o700 });
  const managedSeatBefore = digestOf(managedSeatFile);

  const env = {
    HOME: home,
    USERPROFILE: home,
    PLIMSOLL_HOME: plimsollHome,
    CODEX_HOME: path.join(home, ".codex"),
    GROK_HOME: path.join(home, ".grok"),
    CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
    PLIMSOLL_FIXTURE_ROOT: fixtureRoot,
  };

  const doctorBefore = runCli(["doctor", "--read-only", "--json"], env);
  const seatsBefore = (lastJson(doctorBefore.stdout).telemetry as Record<string, unknown>)
    .claudeSeats as Array<Record<string, unknown>>;
  const seatBefore = (slug: string) => seatsBefore.find((seat) => seat.slug === slug);
  check(
    "doctor_reports_the_unmanaged_seat_and_skips_the_seat_without_settings",
    seatsBefore.length === 3 &&
      seatBefore(FLEET_SEAT)?.diagnostic === "claude_seat_settings_unmanaged" &&
      Array.isArray(seatBefore(FLEET_SEAT)?.missing) &&
      (seatBefore(FLEET_SEAT)?.missing as string[]).length > 0 &&
      seatBefore(MANAGED_SEAT)?.diagnostic === undefined &&
      seatBefore(MANAGED_SEAT)?.status === "valid" &&
      seatBefore(BARE_SEAT)?.status === "skipped" &&
      seatBefore(BARE_SEAT)?.diagnostic === undefined,
    {
      seats: seatsBefore.map((seat) => ({
        slug: seat.slug,
        status: seat.status,
        diagnostic: seat.diagnostic ?? null,
        missing: (seat.missing as string[]).length,
      })),
    },
  );
  check(
    "doctor_never_prints_a_managed_value_for_a_seat",
    !doctorBefore.stdout.includes(auth.claudeCodeProducer) &&
      !doctorBefore.stdout.includes(generated.env.OTEL_EXPORTER_OTLP_HEADERS),
    { scanned: "doctor stdout" },
  );

  const dryRun = runCli(["setup", "--dry-run"], env);
  const fleetLines = dryRun.stdout.split("\n")
    .filter((line) => line.includes(`claudeSeat[${FLEET_SEAT}].`));
  const dryTargets = lastJson(dryRun.stdout).targets as Record<string, Record<string, unknown>>;
  check(
    "setup_dry_run_reports_one_line_per_managed_seat_key_and_writes_nothing",
    dryRun.code === 0 &&
      fleetLines.length === expectedPlanKeys(`claudeSeat[${FLEET_SEAT}]`, generated).length &&
      fleetLines.every((line) => line.startsWith(`${fleetSeatFile}: claudeSeat[${FLEET_SEAT}].`)) &&
      fleetLines.filter((line) => line.endsWith(" added")).length === Object.keys(generated.env).length &&
      fleetLines.filter((line) => line.endsWith(" updated")).length === Object.keys(generated.hooks).length &&
      dryTargets[`claudeSeat[${FLEET_SEAT}]`]?.status === "would_apply" &&
      dryTargets[`claudeSeat[${MANAGED_SEAT}]`]?.status === "unchanged" &&
      dryTargets[`claudeSeat[${BARE_SEAT}]`] === undefined &&
      dryTargets.claude?.status === "would_apply" &&
      digestOf(managedSeatFile) === managedSeatBefore,
    {
      lines: fleetLines.length,
      seatTargets: Object.keys(dryTargets).filter((name) => name.startsWith("claudeSeat[")),
    },
  );

  const applyRun = runCli(["setup", "--yes"], env);
  const appliedJson = lastJson(applyRun.stdout);
  const fleetApplied = appliedJson[`claudeSeat[${FLEET_SEAT}]`] as Record<string, unknown>;
  const managedApplied = appliedJson[`claudeSeat[${MANAGED_SEAT}]`] as Record<string, unknown>;
  const fleetAfterApply = digestOf(fleetSeatFile);
  const fleetBackups = backups(fleetSeatFile);
  check(
    "setup_applies_every_discovered_seat_beside_the_existing_targets",
    applyRun.code === 0 &&
      appliedJson.status === "setup_applied" &&
      fleetApplied?.status === "applied" &&
      typeof fleetApplied?.backup === "string" &&
      managedApplied?.status === "unchanged" &&
      appliedJson[`claudeSeat[${BARE_SEAT}]`] === undefined &&
      (appliedJson.claude as Record<string, unknown>)?.status === "applied" &&
      digestOf(managedSeatFile) === managedSeatBefore &&
      fleetBackups.length === 1 &&
      !fs.existsSync(path.join(seatsRoot, BARE_SEAT, "settings.json")),
    {
      seatTargets: Object.keys(appliedJson).filter((name) => name.startsWith("claudeSeat[")),
      fleetBackups: fleetBackups.length,
    },
  );
  // Key order follows each document's own history (the fleet seat gains `env`
  // after its existing keys), so the equivalence that matters is the content.
  check(
    "the_applied_seat_now_carries_the_same_managed_content_as_the_already_managed_seat",
    isDeepStrictEqual(readJson(fleetSeatFile), readJson(managedSeatFile)),
    { seats: [FLEET_SEAT, MANAGED_SEAT] },
  );

  const doctorAfter = runCli(["doctor", "--read-only", "--json"], env);
  const seatsAfter = (lastJson(doctorAfter.stdout).telemetry as Record<string, unknown>)
    .claudeSeats as Array<Record<string, unknown>>;
  check(
    "doctor_reports_no_unmanaged_seat_after_setup",
    seatsAfter.length === 3 &&
      seatsAfter.every((seat) => seat.diagnostic === undefined) &&
      seatsAfter.filter((seat) => seat.status === "valid").length === 2 &&
      seatsAfter.find((seat) => seat.slug === BARE_SEAT)?.status === "skipped",
    { seats: seatsAfter.map((seat) => ({ slug: seat.slug, status: seat.status })) },
  );

  const secondApply = runCli(["setup", "--yes"], env);
  const secondJson = lastJson(secondApply.stdout);
  check(
    "a_second_setup_run_is_a_seat_no_op_without_new_backups",
    secondApply.code === 0 &&
      secondJson.status === "setup_noop" &&
      ((secondJson.targets as Record<string, Record<string, unknown>>)[`claudeSeat[${FLEET_SEAT}]`]?.status ===
        "unchanged") &&
      ((secondJson.targets as Record<string, Record<string, unknown>>)[`claudeSeat[${MANAGED_SEAT}]`]?.status ===
        "unchanged") &&
      digestOf(fleetSeatFile) === fleetAfterApply &&
      JSON.stringify(backups(fleetSeatFile)) === JSON.stringify(fleetBackups),
    { status: secondJson.status, backups: fleetBackups.length },
  );
}

function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-claude-seats-proof-"));
  // Declare the fixture root before the first apply: every seat below lives
  // inside it and the guard refuses anything that does not.
  const fixture = useFixtureRoot(sandbox, { home: path.join(sandbox, "must-remain-absent-operator-home") });
  try {
    libraryChecks(fixture.root);
    commandChecks(fixture.root);
    check(
      "the_fixture_home_the_guard_protects_was_never_created",
      !fs.existsSync(fixture.home),
      { home: "must-remain-absent-operator-home" },
    );
    console.log(JSON.stringify({ bead: "eco-6hoxj.48", ok: true, checks }, null, 2));
  } finally {
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main();
