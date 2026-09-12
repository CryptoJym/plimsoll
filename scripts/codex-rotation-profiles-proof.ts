/**
 * Codex producer-token rotation across fleet seat profiles (bead eco-6hoxj.54).
 *
 * Since bead eco-6hoxj.52 every discovered ~/.codex-profiles/<slug>/config.toml
 * carries the Codex producer token inline in three `[otel]` exporter `headers`
 * tables, but `rotate-producer-token --source codex` rewrote only
 * ~/.codex/config.toml and ~/.codex/plimsoll.headers: the review of that bead
 * reproduced a rotation to T2 that left every profile holding T1, so each
 * profile lane's OTLP logs, traces and metrics were rejected the moment the
 * grace window closed — silently, from a command that reported success.
 *
 * Under a fixture HOME carrying a managed default config and header file, two
 * managed profiles (one at mode 0640), one unmanaged profile and one malformed
 * profile this proof pins:
 *
 *   a) the dry run mints nothing, writes nothing and lists the exact
 *      `codexProfile[<slug>].otel.<exporter>.headers updated` plan lines;
 *   b) the rotation rewrites exactly the two managed profiles — backup per
 *      profile, file mode preserved, foreign hook commands byte-identical, the
 *      superseded token gone from all three exporter tables — while the
 *      unmanaged and the malformed profile stay byte-identical with no backup;
 *   c) the reviewer's reproduction fails to reproduce: no profile, and nothing
 *      the command printed, still carries T1, and doctor reports both managed
 *      profiles `valid` against the rotated token;
 *   d) a malformed profile never sets the exit code (exit 0), and the default
 *      target and header file behave exactly as before — on a home without
 *      profiles the payload keys and targets are the pre-bead ones;
 *   e) no hook command in any profile carries a token literal (the hooks point
 *      at the same per-user header file), and the backups the rotation wrote
 *      restore every profile byte-for-byte while the superseded token is still
 *      inside its grace window, so the rollback path covers the profiles too;
 *   f) doctor path hygiene: a profile — and a Claude seat — symlinked to a
 *      directory outside $HOME is reported at its link path under $HOME with
 *      `outsideHome: true`, and no absolute outside path reaches the receipt.
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
import { loadOrCreateLocalIngestAuth, producerRotationState, readLocalIngestAuth } from "../packages/collector-cli/src/local-auth";

type Check = { name: string; passed: true; detail: Record<string, unknown> };

const repoRoot = path.resolve(import.meta.dirname, "..");
const cli = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const loader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
const checks: Check[] = [];

/** Managed by `setup`, then rotated: the two authenticated profile lanes. */
const MANAGED_PROFILES = ["rot-managed-profile", "rot-managed-mode-profile"] as const;
/** Written after `setup`, so it never received the managed block. */
const UNMANAGED_PROFILE = "rot-unmanaged-profile";
/** Fleet-conductor state Plimsoll cannot parse. */
const MALFORMED_PROFILE = "rot-malformed-profile";
/** Marker that must never reach a receipt; it lives in the malformed profile. */
const MALFORMED_MARKER = "synthetic-malformed-rotation-marker";
/** The exporter tables that carry the producer token inline. */
const EXPORTERS = ["exporter", "trace_exporter", "metrics_exporter"] as const;
/** Payload keys `rotate-producer-token` printed before this bead. */
const PRE_BEAD_PAYLOAD_KEYS = [
  "status",
  "source",
  "rotated",
  "graceSeconds",
  "previousTokenExpiresAt",
  "targets",
  "nextSteps",
] as const;

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

function modeOf(file: string) {
  return fs.statSync(file).mode & 0o777;
}

function backups(file: string) {
  const directory = path.dirname(file);
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory).filter((name) => name.includes(".plimsoll-backup-")).sort();
}

/**
 * The fleet conductor's own profile config, with synthetic commands: never a
 * real command line. No `[otel]` table and no Plimsoll hook, which is what a
 * profile looks like before `setup` manages it.
 */
function fleetProfileToml() {
  const fleetHook = (phase: string) =>
    `{ type = "command", command = "/synthetic/fleet/codex-hook --phase ${phase} --synthetic", timeout = 8 }`;
  return [
    "# Synthetic fleet Codex profile.",
    'model = "synthetic-fleet-model"',
    "",
    "[features]",
    "hooks = true",
    "",
    "[hooks]",
    `UserPromptSubmit= [{ hooks = [${fleetHook("checkpoint")}] }]`,
    `PostToolUse= [{ matcher = ".*", hooks = [${fleetHook("active")}] }]`,
    `Stop= [{ hooks = [${fleetHook("end")}] }]`,
    `SessionStart= [{ matcher = "startup|resume", hooks = [${fleetHook("start")}] }]`,
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

function foreignCommands(file: string) {
  const document = parseToml(fs.readFileSync(file, "utf8")) as Record<string, any>;
  return allHookCommands(document).filter((command) => !command.includes("/hooks/codex")).sort();
}

/** The `x-plimsoll-token` value each managed exporter table carries, in order. */
function exporterTokens(file: string) {
  const document = parseToml(fs.readFileSync(file, "utf8")) as Record<string, any>;
  return EXPORTERS.map((exporter) => document.otel?.[exporter]?.["otlp-http"]?.headers?.["x-plimsoll-token"]);
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
  return JSON.parse(candidate) as Record<string, any>;
}

/**
 * A fixture home wired for the CLI: fixture collector config, fixture producer
 * credentials, and the environment overlay every child run gets.
 */
function commandHome(fixtureRoot: string, name: string, port: number) {
  const home = path.join(fixtureRoot, name);
  const plimsollHome = path.join(home, ".plimsoll");
  fs.mkdirSync(plimsollHome, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(plimsollHome, "collector.config.json"),
    `${JSON.stringify({ port }, null, 2)}\n`, { mode: 0o600 });
  loadOrCreateLocalIngestAuth(plimsollHome);
  const env = {
    HOME: home,
    USERPROFILE: home,
    PLIMSOLL_HOME: plimsollHome,
    CODEX_HOME: path.join(home, ".codex"),
    GROK_HOME: path.join(home, ".grok"),
    CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
    PLIMSOLL_FIXTURE_ROOT: fixtureRoot,
  };
  return {
    home,
    plimsollHome,
    profilesRoot: path.join(home, ".codex-profiles"),
    seatsRoot: path.join(home, ".claude-seats"),
    codexFile: path.join(home, ".codex", "config.toml"),
    headerFile: path.join(home, ".codex", "plimsoll.headers"),
    env,
  };
}

/**
 * The bead's subject: a rotation on a host that carries fleet profiles. The two
 * managed profiles are managed by `setup` itself rather than a hand-written
 * fixture, so what rotation finds is exactly what an operator's host holds.
 */
function rotationChecks(fixtureRoot: string) {
  const { home, plimsollHome, profilesRoot, codexFile, headerFile, env } =
    commandHome(fixtureRoot, "rotation-home", 49170);
  const managedFiles = MANAGED_PROFILES.map((slug, index) =>
    writeProfileConfig(profilesRoot, slug, fleetProfileToml(), index === 1 ? 0o640 : 0o600));

  const setup = runCli(["setup", "--yes"], env);
  const beforeAuth = readLocalIngestAuth(plimsollHome)!;
  const previousToken = beforeAuth.codexProducer;
  check(
    "setup_leaves_the_default_target_and_both_profiles_holding_the_same_token",
    setup.code === 0 &&
      exporterTokens(codexFile).every((token) => token === previousToken) &&
      managedFiles.every((file) => exporterTokens(file).every((token) => token === previousToken)) &&
      fs.readFileSync(headerFile, "utf8") === `x-plimsoll-token: ${previousToken}\n`,
    { setup: setup.code, profiles: MANAGED_PROFILES },
  );

  // Written after `setup`: the conductor's own profile, never given the managed
  // block, and one it left unparseable. Neither is an authenticated consumer.
  const unmanagedFile = writeProfileConfig(profilesRoot, UNMANAGED_PROFILE, fleetProfileToml());
  const malformedFile = writeProfileConfig(
    profilesRoot,
    MALFORMED_PROFILE,
    `[hooks\n${MALFORMED_MARKER} = = this is not toml\n`,
  );
  const before = {
    codex: digestOf(codexFile),
    header: digestOf(headerFile),
    managed: managedFiles.map(digestOf),
    managedContent: managedFiles.map((file) => fs.readFileSync(file, "utf8")),
    managedModes: managedFiles.map(modeOf),
    managedForeign: managedFiles.map(foreignCommands),
    managedBackups: managedFiles.map((file) => backups(file).length),
    unmanaged: digestOf(unmanagedFile),
    malformed: digestOf(malformedFile),
    credential: digestOf(path.join(plimsollHome, "local-ingest-auth.json")),
  };

  // ---- a) dry run ---------------------------------------------------------
  const dryRun = runCli(["rotate-producer-token", "--source", "codex", "--dry-run"], env);
  const dryPayload = lastJson(dryRun.stdout);
  const planLines = (file: string, slug: string) =>
    EXPORTERS.map((exporter) => `${file}: codexProfile[${slug}].otel.${exporter}.otlp-http.headers updated`);
  const expectedPlanLines = MANAGED_PROFILES.flatMap((slug, index) => planLines(managedFiles[index], slug));
  check(
    "dry_run_lists_every_managed_profile_exporter_header_it_would_update",
    dryRun.code === 0 &&
      dryPayload.status === "rotation_dry_run" &&
      expectedPlanLines.every((line) => dryRun.stdout.includes(line)) &&
      (dryPayload.targets as Array<Record<string, unknown>>).length === 4 &&
      managedFiles.every((file) =>
        (dryPayload.targets as Array<Record<string, unknown>>).some((target) => target.path === file)
      ),
    {
      code: dryRun.code,
      targets: (dryPayload.targets as Array<Record<string, unknown>>).length,
      planLines: expectedPlanLines.length,
    },
  );
  check(
    "dry_run_reports_the_unmanaged_and_malformed_profiles_and_rewrites_nothing",
    (dryPayload.profilesSkipped as Array<Record<string, unknown>>).length === 2 &&
      (dryPayload.profilesSkipped as Array<Record<string, unknown>>).some((entry) =>
        entry.slug === UNMANAGED_PROFILE && entry.reason === "codex_profile_config_unmanaged"
      ) &&
      (dryPayload.profilesSkipped as Array<Record<string, unknown>>).some((entry) =>
        entry.slug === MALFORMED_PROFILE && entry.reason === "codex_profile_config_unreadable"
      ) &&
      digestOf(path.join(plimsollHome, "local-ingest-auth.json")) === before.credential &&
      managedFiles.map(digestOf).join(",") === before.managed.join(",") &&
      managedFiles.every((file, index) => backups(file).length === before.managedBackups[index]) &&
      digestOf(codexFile) === before.codex &&
      digestOf(headerFile) === before.header &&
      !dryRun.stdout.includes(MALFORMED_MARKER),
    { skipped: dryPayload.profilesSkipped },
  );

  // ---- b) rotation --------------------------------------------------------
  const rotate = runCli(["rotate-producer-token", "--source", "codex", "--grace-seconds", "60"], env);
  const payload = lastJson(rotate.stdout);
  const rotatedAuth = readLocalIngestAuth(plimsollHome)!;
  const rotatedToken = rotatedAuth.codexProducer;
  const targetsByPath = Object.fromEntries(
    (payload.targets as Array<Record<string, any>>).map((target) => [target.path, target]),
  );
  check(
    "rotation_rewrites_exactly_the_two_managed_profiles_with_a_backup_each",
    rotate.code === 0 &&
      payload.status === "rotation_applied" &&
      rotatedToken !== previousToken &&
      managedFiles.every((file) => targetsByPath[file]?.status === "rotated") &&
      managedFiles.every((file) => typeof targetsByPath[file]?.backup === "string") &&
      managedFiles.every((file, index) => backups(file).length === before.managedBackups[index] + 1) &&
      managedFiles.every((file) => exporterTokens(file).every((token) => token === rotatedToken)),
    {
      code: rotate.code,
      statuses: managedFiles.map((file) => targetsByPath[file]?.status),
      backups: managedFiles.map((file) => backups(file).length),
    },
  );
  check(
    "rotation_preserves_each_profiles_file_mode_and_foreign_hooks",
    managedFiles.every((file, index) => modeOf(file) === before.managedModes[index]) &&
      managedFiles.every((file, index) =>
        foreignCommands(file).join(" ") === before.managedForeign[index].join(" ")
      ),
    {
      modes: managedFiles.map(modeOf),
      expectedModes: before.managedModes,
      foreign: managedFiles.map((file) => foreignCommands(file).length),
    },
  );
  check(
    "an_unmanaged_and_a_malformed_profile_are_byte_identical_after_the_rotation",
    digestOf(unmanagedFile) === before.unmanaged &&
      digestOf(malformedFile) === before.malformed &&
      backups(unmanagedFile).length === 0 &&
      backups(malformedFile).length === 0 &&
      (payload.profilesSkipped as Array<Record<string, unknown>>).length === 2 &&
      targetsByPath[unmanagedFile] === undefined &&
      targetsByPath[malformedFile] === undefined,
    { skipped: payload.profilesSkipped },
  );
  check(
    "a_malformed_profile_never_sets_the_exit_code",
    rotate.code === 0 && fs.existsSync(malformedFile) && !rotate.stdout.includes(MALFORMED_MARKER),
    { code: rotate.code },
  );

  // ---- c) the reviewer's reproduction -------------------------------------
  check(
    "the_superseded_token_survives_in_no_profile_and_in_nothing_the_command_printed",
    managedFiles.every((file) => !fs.readFileSync(file, "utf8").includes(previousToken)) &&
      !fs.readFileSync(codexFile, "utf8").includes(previousToken) &&
      fs.readFileSync(headerFile, "utf8") === `x-plimsoll-token: ${rotatedToken}\n` &&
      [previousToken, rotatedToken, rotatedAuth.managementRead].every((value) =>
        !rotate.stdout.includes(value) && !rotate.stderr.includes(value)
      ),
    { profiles: MANAGED_PROFILES.length },
  );
  const doctor = runCli(["doctor", "--read-only", "--json"], env);
  const reported = (lastJson(doctor.stdout).telemetry as Record<string, any>).codexProfiles as Array<Record<string, any>>;
  const reportedProfile = (slug: string) => reported.find((profile) => profile.slug === slug);
  check(
    "doctor_reports_every_rotated_profile_valid_against_the_new_token",
    MANAGED_PROFILES.every((slug) => reportedProfile(slug)?.status === "valid") &&
      MANAGED_PROFILES.every((slug) => reportedProfile(slug)?.diagnostic === undefined) &&
      reportedProfile(UNMANAGED_PROFILE)?.diagnostic === "codex_profile_config_unmanaged" &&
      reportedProfile(MALFORMED_PROFILE)?.status === "unreadable" &&
      [previousToken, rotatedToken].every((value) => !doctor.stdout.includes(value)),
    { statuses: reported.map((profile) => `${profile.slug}:${profile.status}`) },
  );

  // ---- e) secrets and rollback -------------------------------------------
  check(
    "no_hook_command_in_any_profile_carries_a_token_literal",
    managedFiles.every((file) =>
      allHookCommands(parseToml(fs.readFileSync(file, "utf8")) as Record<string, any>).every((command) =>
        !command.includes(previousToken) && !command.includes(rotatedToken)
      )
    ) &&
      managedFiles.every((file) =>
        allHookCommands(parseToml(fs.readFileSync(file, "utf8")) as Record<string, any>)
          .some((command) => command.includes(headerFile))
      ),
    { profiles: MANAGED_PROFILES.length },
  );
  const rotationState = producerRotationState(rotatedAuth, "codex");
  const restored = managedFiles.map((file, index) => {
    const backup = path.join(path.dirname(file), backups(file).at(-1)!);
    fs.copyFileSync(backup, file);
    return fs.readFileSync(file, "utf8") === before.managedContent[index];
  });
  check(
    "the_rotation_backups_roll_every_profile_back_inside_the_grace_window",
    restored.every(Boolean) &&
      managedFiles.every((file) => exporterTokens(file).every((token) => token === previousToken)) &&
      rotatedAuth.rotations?.codex?.token === previousToken &&
      rotationState.state === "active" &&
      (rotationState.secondsRemaining ?? 0) > 0,
    { restored: restored.length, rotation: rotationState.state },
  );
}

/**
 * The default target on a host with no fleet profiles: the payload keys, the
 * two targets and the exit code are the ones `rotate-producer-token` printed
 * before this bead.
 */
function defaultTargetChecks(fixtureRoot: string) {
  const { plimsollHome, codexFile, headerFile, env } = commandHome(fixtureRoot, "no-profiles-home", 49171);
  const setup = runCli(["setup", "--yes"], env);
  const previousToken = readLocalIngestAuth(plimsollHome)!.codexProducer;
  const rotate = runCli(["rotate-producer-token", "--source", "codex", "--grace-seconds", "60"], env);
  const payload = lastJson(rotate.stdout);
  const rotatedToken = readLocalIngestAuth(plimsollHome)!.codexProducer;
  check(
    "a_home_without_profiles_rotates_exactly_the_two_declared_targets",
    setup.code === 0 &&
      rotate.code === 0 &&
      payload.status === "rotation_applied" &&
      Object.keys(payload).join(",") === PRE_BEAD_PAYLOAD_KEYS.join(",") &&
      (payload.targets as Array<Record<string, any>>).map((target) => target.path).join(",") ===
        [headerFile, codexFile].join(",") &&
      (payload.targets as Array<Record<string, any>>).every((target) => target.status === "rotated") &&
      (payload.nextSteps as string[]).length === 2 &&
      fs.readFileSync(headerFile, "utf8") === `x-plimsoll-token: ${rotatedToken}\n` &&
      exporterTokens(codexFile).every((token) => token === rotatedToken) &&
      rotatedToken !== previousToken,
    { code: rotate.code, keys: Object.keys(payload), nextSteps: (payload.nextSteps as string[]).length },
  );
}

/**
 * Doctor path hygiene (review r1 finding 3). A profile and a seat the tooling
 * relocated to shared storage resolve outside $HOME; the receipt must name them
 * by their link under $HOME and say so, never by the outside path.
 */
function doctorPathChecks(fixtureRoot: string) {
  const { home, profilesRoot, seatsRoot, env } = commandHome(fixtureRoot, "doctor-path-home", 49172);
  const relocated = path.join(fixtureRoot, "relocated-outside-home");
  const profileTarget = path.join(relocated, "relocated-profile-target");
  writeProfileConfig(relocated, "relocated-profile-target", fleetProfileToml());
  fs.mkdirSync(profilesRoot, { recursive: true, mode: 0o700 });
  fs.symlinkSync(profileTarget, path.join(profilesRoot, "linked-outside-profile"));
  const seatTarget = path.join(relocated, "relocated-seat-target");
  fs.mkdirSync(seatTarget, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(seatTarget, "settings.json"),
    `${JSON.stringify({ model: "synthetic-fleet-seat" }, null, 2)}\n`, { mode: 0o600 });
  fs.mkdirSync(seatsRoot, { recursive: true, mode: 0o700 });
  fs.symlinkSync(seatTarget, path.join(seatsRoot, "linked-outside-seat"));
  // The same layout inside the home must stay exactly as it was reported before.
  const insideTarget = path.join(home, "relocated-inside-home", "inside-profile-target");
  writeProfileConfig(path.join(home, "relocated-inside-home"), "inside-profile-target", fleetProfileToml());
  fs.symlinkSync(insideTarget, path.join(profilesRoot, "linked-inside-profile"));

  const doctor = runCli(["doctor", "--read-only", "--json"], env);
  const telemetry = lastJson(doctor.stdout).telemetry as Record<string, any>;
  const profile = (telemetry.codexProfiles as Array<Record<string, any>>)
    .find((entry) => entry.slug === "linked-outside-profile");
  const insideProfile = (telemetry.codexProfiles as Array<Record<string, any>>)
    .find((entry) => entry.slug === "linked-inside-profile");
  const seat = (telemetry.claudeSeats as Array<Record<string, any>>)
    .find((entry) => entry.slug === "linked-outside-seat");
  check(
    "doctor_names_a_relocated_profile_and_seat_by_their_link_under_home",
    profile?.path === path.join(profilesRoot, "linked-outside-profile", "config.toml") &&
      profile?.outsideHome === true &&
      seat?.path === path.join(seatsRoot, "linked-outside-seat", "settings.json") &&
      seat?.outsideHome === true,
    {
      profile: path.relative(home, String(profile?.path)),
      seat: path.relative(home, String(seat?.path)),
      outsideHome: [profile?.outsideHome, seat?.outsideHome],
    },
  );
  // The fixture home may itself sit under a symlinked root (/var -> /private/var
  // on macOS), so "under $HOME" means under the home or under its resolved form.
  const homeRoots = [home, fs.realpathSync(home)];
  const underHome = (candidate: string) =>
    homeRoots.some((root) => candidate.startsWith(root + path.sep));
  const reportedPaths = [
    ...(telemetry.codexProfiles as Array<Record<string, any>>),
    ...(telemetry.claudeSeats as Array<Record<string, any>>),
  ].map((entry) => String(entry.path));
  check(
    "no_absolute_path_outside_home_reaches_the_doctor_receipt",
    !doctor.stdout.includes(relocated) &&
      !doctor.stdout.includes(fs.realpathSync(relocated)) &&
      reportedPaths.every(underHome),
    { paths: reportedPaths.length, outside: reportedPaths.filter((entry) => !underHome(entry)).length },
  );
  check(
    "a_profile_linked_inside_the_home_is_reported_exactly_as_before",
    insideProfile?.path === path.join(fs.realpathSync(insideTarget), "config.toml") &&
      insideProfile?.outsideHome === undefined,
    { path: path.relative(fs.realpathSync(home), String(insideProfile?.path)) },
  );
}

function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-codex-rotation-profiles-proof-"));
  const fixtureRoot = path.join(sandbox, "fixture");
  const fixture = useFixtureRoot(fixtureRoot, {
    home: path.join(fixtureRoot, "must-remain-absent-operator-home"),
  });
  try {
    rotationChecks(fixture.root);
    defaultTargetChecks(fixture.root);
    doctorPathChecks(fixture.root);
    check(
      "the_fixture_home_the_guard_protects_was_never_created",
      !fs.existsSync(fixture.home),
      { home: "must-remain-absent-operator-home" },
    );
    console.log(JSON.stringify({ bead: "eco-6hoxj.54", ok: true, checks }, null, 2));
  } finally {
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main();
