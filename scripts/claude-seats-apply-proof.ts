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
 *      created, and a second `setup --yes` is a no-op;
 *   c) symlinks: a seat directory reached through a link is managed like a real
 *      one, a dangling link is reported `skipped`, and a link that resolves out
 *      of the declared fixture root is a seat only in as much as the guard lets
 *      it be — the apply is refused on the resolved path;
 *   d) ownership of the exit code: a seat file Plimsoll does not own (malformed
 *      JSON, mode 0000) is reported refused but never fails `setup`, whose six
 *      declared targets still apply, and doctor names such a seat `unreadable`
 *      without printing a byte of it.
 *
 * Every path is synthetic and below a per-run sandbox. The simulated foreign
 * home in (c) sits beside the declared fixture root on purpose: it is what the
 * guard has to refuse. Tokens are fixture credentials minted into the fixture
 * Plimsoll home; nothing here prints one.
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
const LINKED_SEAT = "relocated-link-seat";
const DANGLING_SEAT = "dangling-link-seat";
const FOREIGN_SEAT = "foreign-home-link-seat";
const MALFORMED_SEAT = "malformed-json-seat";
const UNREADABLE_SEAT = "mode-0000-seat";
/** Markers that must never reach a receipt; both live in unreadable seat files. */
const MALFORMED_MARKER = "synthetic-malformed-seat-marker";
const UNREADABLE_MARKER = "synthetic-unreadable-seat-marker";
/** The six targets Plimsoll declares; only these may decide `setup`'s exit code. */
const OWNED_TARGETS = ["claude", "gemini", "grokHeaders", "grok", "codexHeaders", "codex"] as const;

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

/**
 * A fixture home wired for the CLI: fixture collector config, fixture producer
 * credentials, and the environment overlay every child run gets. Used by the
 * symlink and unowned-seat halves below; the original command half keeps its
 * own inline setup.
 */
function commandHome(fixtureRoot: string, name: string, port: number) {
  const home = path.join(fixtureRoot, name);
  const plimsollHome = path.join(home, ".plimsoll");
  const seatsRoot = path.join(home, ".claude-seats");
  fs.mkdirSync(plimsollHome, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(plimsollHome, "collector.config.json"),
    `${JSON.stringify({ port }, null, 2)}\n`, { mode: 0o600 });
  const auth = loadOrCreateLocalIngestAuth(plimsollHome);
  const generated = generateClaudeCodeSettings({
    repoRoot: "/synthetic/plimsoll/source",
    port,
    dataMode: "metadata",
    claudeCodeProducerToken: auth.claudeCodeProducer,
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
  return { home, plimsollHome, seatsRoot, auth, generated, env };
}

/** Every declared target's entry in a setup summary, by target name. */
function ownedStatuses(targets: Record<string, Record<string, unknown> | undefined>) {
  return Object.fromEntries(OWNED_TARGETS.map((name) => [name, targets[name]?.status ?? "absent"]));
}

/**
 * Symlinked seats (review r1, finding 2). readdirSync does not follow links, so
 * a relocated seat used to be invisible to setup and doctor alike. Discovery now
 * resolves the link — which also hands the managed-config guard the real target,
 * so a link out of the fixture root is refused on the path it actually names.
 */
function symlinkSeatChecks(sandbox: string, fixtureRoot: string) {
  const { seatsRoot, generated, env } = commandHome(fixtureRoot, "symlink-home", 49150);
  const relocated = path.join(fixtureRoot, "relocated-seats");
  // A seat the tooling moved to another directory and linked into place.
  const linkedTarget = path.join(relocated, "linked-target");
  writeSeatSettings(relocated, "linked-target", fleetSeatDocument());
  // "Another user's home": a real directory beside the declared fixture root,
  // which is exactly what the guard exists to refuse.
  const foreignHome = path.join(sandbox, "foreign-home");
  const foreignTarget = path.join(foreignHome, ".claude-seats", FOREIGN_SEAT);
  writeSeatSettings(path.join(foreignHome, ".claude-seats"), FOREIGN_SEAT, fleetSeatDocument());
  const danglingTarget = path.join(relocated, "never-created-target");

  fs.mkdirSync(seatsRoot, { recursive: true, mode: 0o700 });
  writeSeatSettings(seatsRoot, MANAGED_SEAT, managedSeatDocument(generated));
  fs.symlinkSync(linkedTarget, path.join(seatsRoot, LINKED_SEAT));
  fs.symlinkSync(foreignTarget, path.join(seatsRoot, FOREIGN_SEAT));
  fs.symlinkSync(danglingTarget, path.join(seatsRoot, DANGLING_SEAT));
  // A link to a file is not a seat, exactly like the plain README file.
  fs.writeFileSync(path.join(seatsRoot, "README"), "synthetic\n", { mode: 0o600 });
  fs.symlinkSync(path.join(seatsRoot, "README"), path.join(seatsRoot, "readme-link"));

  const discovered = discoverClaudeSeats(path.dirname(seatsRoot));
  const seat = (slug: string) => discovered.find((entry) => entry.slug === slug);
  check(
    "a_symlinked_seat_directory_is_discovered_at_its_resolved_path",
    discovered.map((entry) => entry.slug).join(",") ===
      [MANAGED_SEAT, LINKED_SEAT, FOREIGN_SEAT, DANGLING_SEAT].sort().join(",") &&
      seat(LINKED_SEAT)?.path === path.join(fs.realpathSync(linkedTarget), "settings.json") &&
      seat(LINKED_SEAT)?.hasSettings === true &&
      seat(FOREIGN_SEAT)?.path === path.join(fs.realpathSync(foreignTarget), "settings.json") &&
      seat(DANGLING_SEAT)?.hasSettings === false,
    {
      seats: discovered.map((entry) => ({ slug: entry.slug, hasSettings: entry.hasSettings })),
      notSeats: ["README", "readme-link"],
    },
  );

  const linkedFile = seat(LINKED_SEAT)!.path;
  const foreignFile = seat(FOREIGN_SEAT)!.path;
  const foreignBefore = digestOf(foreignFile);
  const applyRun = runCli(["setup", "--yes"], env);
  const applied = lastJson(applyRun.stdout) as Record<string, Record<string, unknown> | undefined>;
  check(
    "a_symlinked_seat_is_managed_like_a_real_seat_directory",
    applyRun.code === 0 &&
      applied[`claudeSeat[${LINKED_SEAT}]`]?.status === "applied" &&
      applied[`claudeSeat[${LINKED_SEAT}]`]?.path === linkedFile &&
      isDeepStrictEqual(readJson(linkedFile), readJson(path.join(seatsRoot, MANAGED_SEAT, "settings.json"))) &&
      OWNED_TARGETS.every((name) => applied[name]?.status === "applied"),
    { linked: applied[`claudeSeat[${LINKED_SEAT}]`]?.status, owned: ownedStatuses(applied) },
  );
  check(
    "a_seat_link_out_of_the_fixture_root_is_refused_on_its_resolved_path",
    applyRun.code === 0 &&
      applied[`claudeSeat[${FOREIGN_SEAT}]`]?.status === "refused" &&
      String(applied[`claudeSeat[${FOREIGN_SEAT}]`]?.reason ?? "")
        .includes("MANAGED_CONFIG_TARGET_OUTSIDE_FIXTURE_ROOT") &&
      String(applied[`claudeSeat[${FOREIGN_SEAT}]`]?.reason ?? "").includes(foreignFile) &&
      digestOf(foreignFile) === foreignBefore,
    {
      status: applied[`claudeSeat[${FOREIGN_SEAT}]`]?.status,
      resolvedTargetUnchanged: digestOf(foreignFile) === foreignBefore,
    },
  );

  const doctorRun = runCli(["doctor", "--read-only", "--json"], env);
  const seats = (lastJson(doctorRun.stdout).telemetry as Record<string, unknown>)
    .claudeSeats as Array<Record<string, unknown>>;
  const reported = (slug: string) => seats.find((entry) => entry.slug === slug);
  check(
    "a_dangling_seat_link_is_reported_skipped_and_never_created",
    reported(DANGLING_SEAT)?.status === "skipped" &&
      reported(DANGLING_SEAT)?.diagnostic === undefined &&
      !fs.existsSync(danglingTarget) &&
      reported(LINKED_SEAT)?.status === "valid" &&
      reported(LINKED_SEAT)?.diagnostic === undefined,
    {
      seats: seats.map((entry) => ({ slug: entry.slug, status: entry.status })),
      danglingTargetCreated: fs.existsSync(danglingTarget),
    },
  );
}

/**
 * Seat files Plimsoll does not own (review r1, finding 1). A malformed or
 * unreadable seat settings.json belongs to the seat tooling: `setup` reports the
 * refusal and keeps going, its own six targets decide the exit code, and doctor
 * is where the seat shows up — as `unreadable`, never as bytes.
 */
function unownedSeatChecks(fixtureRoot: string) {
  const { seatsRoot, generated, auth, env } = commandHome(fixtureRoot, "unowned-home", 49151);
  const fleetSeatFile = writeSeatSettings(seatsRoot, FLEET_SEAT, fleetSeatDocument());
  const malformedFile = path.join(seatsRoot, MALFORMED_SEAT, "settings.json");
  fs.mkdirSync(path.dirname(malformedFile), { recursive: true, mode: 0o700 });
  fs.writeFileSync(malformedFile, `{ ${MALFORMED_MARKER} this is not json `, { mode: 0o600 });
  const unreadableFile = writeSeatSettings(seatsRoot, UNREADABLE_SEAT, { marker: UNREADABLE_MARKER });
  fs.chmodSync(unreadableFile, 0o000);
  const malformedBefore = digestOf(malformedFile);

  const malformedTarget = `claudeSeat[${MALFORMED_SEAT}]`;
  const unreadableTarget = `claudeSeat[${UNREADABLE_SEAT}]`;
  const dryRun = runCli(["setup", "--dry-run"], env);
  const dryTargets = lastJson(dryRun.stdout).targets as Record<string, Record<string, unknown> | undefined>;
  check(
    "a_refused_seat_never_sets_the_exit_code_of_setup_dry_run",
    dryRun.code === 0 &&
      dryTargets[malformedTarget]?.status === "refused" &&
      String(dryTargets[malformedTarget]?.reason ?? "").includes("CLAUDE_CONFIG_MALFORMED_JSON") &&
      dryTargets[unreadableTarget]?.status === "refused" &&
      dryTargets[`claudeSeat[${FLEET_SEAT}]`]?.status === "would_apply" &&
      OWNED_TARGETS.every((name) => dryTargets[name]?.status === "would_apply"),
    {
      code: dryRun.code,
      refused: [malformedTarget, unreadableTarget].map((name) => dryTargets[name]?.status),
      owned: ownedStatuses(dryTargets),
    },
  );

  const applyRun = runCli(["setup", "--yes"], env);
  const applied = lastJson(applyRun.stdout) as Record<string, Record<string, unknown> | undefined>;
  check(
    "a_refused_seat_never_sets_the_exit_code_of_setup_yes",
    applyRun.code === 0 &&
      (applied.status as unknown) === "setup_applied" &&
      applied[malformedTarget]?.status === "refused" &&
      applied[unreadableTarget]?.status === "refused" &&
      applied[`claudeSeat[${FLEET_SEAT}]`]?.status === "applied" &&
      OWNED_TARGETS.every((name) => applied[name]?.status === "applied") &&
      digestOf(malformedFile) === malformedBefore &&
      (fs.statSync(unreadableFile).mode & 0o777) === 0o000,
    {
      code: applyRun.code,
      owned: ownedStatuses(applied),
      seatBytesStable: digestOf(malformedFile) === malformedBefore,
    },
  );

  const doctorRun = runCli(["doctor", "--read-only", "--json"], env);
  const seats = (lastJson(doctorRun.stdout).telemetry as Record<string, unknown>)
    .claudeSeats as Array<Record<string, unknown>>;
  const reported = (slug: string) => seats.find((entry) => entry.slug === slug);
  check(
    "doctor_reports_an_unreadable_seat_without_printing_a_byte_of_it",
    reported(MALFORMED_SEAT)?.status === "unreadable" &&
      reported(MALFORMED_SEAT)?.diagnostic === "claude_seat_settings_unmanaged" &&
      reported(UNREADABLE_SEAT)?.status === "unreadable" &&
      reported(UNREADABLE_SEAT)?.diagnostic === "claude_seat_settings_unmanaged" &&
      isDeepStrictEqual(reported(MALFORMED_SEAT)?.missing, ["readable JSON"]) &&
      reported(FLEET_SEAT)?.status === "valid" &&
      !doctorRun.stdout.includes(MALFORMED_MARKER) &&
      !doctorRun.stdout.includes(UNREADABLE_MARKER) &&
      !doctorRun.stdout.includes(auth.claudeCodeProducer) &&
      !doctorRun.stdout.includes(generated.env.OTEL_EXPORTER_OTLP_HEADERS),
    {
      seats: seats.map((entry) => ({ slug: entry.slug, status: entry.status })),
      markersPrinted: false,
      appliedSeat: path.basename(path.dirname(fleetSeatFile)),
    },
  );
  // Leave the sandbox removable: a 0000 file needs no chmod to unlink, but the
  // proof should not depend on that.
  fs.chmodSync(unreadableFile, 0o600);

  // The other half of the same rule: a target Plimsoll does declare still fails
  // the run. Same malformed document, this time in ~/.claude/settings.json.
  const owned = commandHome(fixtureRoot, "owned-refusal-home", 49152);
  const ownedClaudeFile = path.join(owned.home, ".claude", "settings.json");
  fs.mkdirSync(path.dirname(ownedClaudeFile), { recursive: true, mode: 0o700 });
  fs.writeFileSync(ownedClaudeFile, `{ ${MALFORMED_MARKER} this is not json `, { mode: 0o600 });
  const ownedDryRun = runCli(["setup", "--dry-run"], owned.env);
  const ownedTargets = lastJson(ownedDryRun.stdout).targets as Record<string, Record<string, unknown> | undefined>;
  check(
    "a_refused_owned_target_still_sets_the_exit_code",
    ownedDryRun.code === 1 &&
      ownedTargets.claude?.status === "refused" &&
      String(ownedTargets.claude?.reason ?? "").includes("CLAUDE_CONFIG_MALFORMED_JSON"),
    { code: ownedDryRun.code, claude: ownedTargets.claude?.status },
  );
}

function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-claude-seats-proof-"));
  // Declare the fixture root before the first apply: every seat below lives
  // inside it and the guard refuses anything that does not. The root is a child
  // of the sandbox so the symlink half can put a simulated foreign home beside
  // it — outside the root, still disposable with everything else.
  const fixtureRoot = path.join(sandbox, "fixture");
  const fixture = useFixtureRoot(fixtureRoot, {
    home: path.join(fixtureRoot, "must-remain-absent-operator-home"),
  });
  try {
    libraryChecks(fixture.root);
    commandChecks(fixture.root);
    symlinkSeatChecks(sandbox, fixture.root);
    unownedSeatChecks(fixture.root);
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
