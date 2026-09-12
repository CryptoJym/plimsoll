/**
 * Proof for bead eco-6hoxj.53: `plimsoll capture-roots discover|add`.
 *
 * Capture roots are minted at enrollment. A host that later gains a native
 * root — a new Claude seat, a new Codex profile, or a `~/.claude/projects` an
 * older enrollment never registered — had no product path to register it, and
 * the repair ran as host-sealed one-off scripts. This proof pins the product
 * command against the semantics those scripts were reviewed under: the same
 * identity derivation, append-only writes, a baseline fenced at the append
 * time, and a receipt.
 *
 * Everything runs under a disposable fixture HOME and PLIMSOLL_HOME. Nothing
 * here reads or writes an operator home, and no LaunchAgent is ever installed,
 * loaded or unloaded.
 */
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  beginAutomaticCaptureBaseline,
  classifyCaptureBaselineFile,
  completeAutomaticCaptureBaseline,
} from "../packages/collector-cli/src/capture-baseline";
import { deriveCaptureRootIdentity } from "../packages/collector-cli/src/capture-root-inventory";
import { installLaunchAgent, uninstallLaunchAgent } from "../packages/collector-cli/src/launch-agent";
import { LifecycleMutationAuthority } from "../packages/collector-cli/src/lifecycle-authority";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { useFixtureRoot } from "./lib/fixture-root";

type CommandResult = { code: number | null; stdout: string; stderr: string };
type Check = { name: string; passed: boolean; detail: unknown };

const root = path.resolve(import.meta.dirname, "..");
const cli = path.join(root, "packages", "collector-cli", "src", "cli.ts");
const tsxLoader = path.join(root, "node_modules", "tsx", "dist", "loader.mjs");
const proofWorkflow = path.join(root, ".github", "workflows", "proof.yml");
const checks: Check[] = [];

const MACHINE = "fixture-machine";
const WORKSPACE = "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10";
const DEVICE = "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d";
const BASELINE_BEFORE = "2026-01-01T00:00:00.000Z";

function check(name: string, condition: unknown, detail: unknown) {
  checks.push({ name, passed: Boolean(condition), detail });
  if (!condition) throw new Error(`${name}: ${JSON.stringify(detail)}`);
}

function command(args: string[], env: NodeJS.ProcessEnv, cwd: string): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--import", tsxLoader, cli, ...args], {
      cwd,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => { stdout += chunk; });
    child.stderr.on("data", (chunk: string) => { stderr += chunk; });
    child.once("error", reject);
    child.once("close", (code) => resolve({ code, stdout, stderr }));
  });
}

function sha256(file: string) {
  return createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

function parse(result: CommandResult) {
  return JSON.parse(result.stdout) as Record<string, any>;
}

function backups(directory: string) {
  return fs.readdirSync(directory).filter((name) => name.includes(".plimsoll-backup-"));
}

function fixtureRoot(source: "codex" | "claude_code", directory: string, epoch: string) {
  return { ...deriveCaptureRootIdentity(MACHINE, source, directory), installationEpochId: epoch, source, directory };
}

async function main() {
  // Realpath up front: macOS hands out /var/... temp roots whose physical
  // path is /private/var/..., and a capture root is always its physical path.
  const sandbox = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capture-roots-add-proof-")));
  const fixture = useFixtureRoot(sandbox, { home: path.join(sandbox, "home") });
  const home = fixture.home;
  const plimsollHome = path.join(sandbox, "plimsoll-home");
  const configPath = path.join(plimsollHome, "collector.config.json");
  const ledgerPath = path.join(plimsollHome, "work-ledger.sqlite");

  try {
    const workflow = fs.readFileSync(proofWorkflow, "utf8");
    check(
      "ci_workflow_runs_this_proof_once",
      [...workflow.matchAll(/^\s+run:\s*(.+?)\s*$/gm)]
        .filter((match) => match[1] === "pnpm proof:capture-roots-add").length === 1,
      proofWorkflow,
    );

    // A host as it actually looks: a registered Codex root, a registered seat
    // root, a registered root whose directory has since gone, and two native
    // directories nothing ever registered.
    const claudeProjects = path.join(home, ".claude", "projects");
    const seatProjects = path.join(home, ".claude-seats", "seat-a", "projects");
    const codexSessions = path.join(home, ".codex", "sessions");
    const codexProfileSessions = path.join(home, ".codex-profiles", "profile-a", "sessions");
    const departedRoot = path.join(home, ".codex-profiles", "departed", "sessions");
    const outsideHome = path.join(sandbox, "outside-home-sessions");
    const notADirectory = path.join(home, ".codex", "sessions.txt");
    for (const directory of [claudeProjects, seatProjects, codexSessions, codexProfileSessions, outsideHome]) {
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    }
    fs.mkdirSync(plimsollHome, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(claudeProjects, "history.jsonl"), "{}\n", { mode: 0o600 });
    // A transcript in a root that is ALREADY registered, born long after the
    // enrollment baseline at BASELINE_BEFORE. Registering another root must
    // not flip this file from capturable to excluded (review r1, finding 1).
    const seatTranscript = path.join(seatProjects, "live-session.jsonl");
    fs.writeFileSync(seatTranscript, "{}\n", { mode: 0o600 });
    fs.writeFileSync(notADirectory, "not a directory\n", { mode: 0o600 });
    // A dangling seat link must never be reported as a candidate.
    fs.symlinkSync(path.join(home, ".claude-seats", "absent"), path.join(home, ".claude-seats", "dangling"));

    // An enrolled ledger: the workspace binding already exists, so opening the
    // buffer later is not itself an enrollment change, and both providers
    // carry a complete baseline taken long before this append.
    const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: WORKSPACE, deviceId: DEVICE });
    let epoch: string;
    try {
      epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
      for (const source of ["codex", "claude_code"] as const) {
        const begun = beginAutomaticCaptureBaseline(buffer.database, source, {
          startedAt: BASELINE_BEFORE,
          filesDiscovered: 0,
        });
        completeAutomaticCaptureBaseline(buffer.database, source, {
          runId: begun.latestRun!.runId,
          completedAt: BASELINE_BEFORE,
        });
      }
    } finally {
      buffer.close();
    }
    const enrolledRoots = [
      fixtureRoot("codex", codexSessions, epoch),
      fixtureRoot("claude_code", seatProjects, epoch),
      fixtureRoot("codex", departedRoot, epoch),
    ];
    const enrolled = collectorConfigSchema.parse({
      port: 48999,
      tenantId: WORKSPACE,
      deviceId: DEVICE,
      installKey: "fixture-install-key",
      managed: true,
      captureRoots: enrolledRoots,
    });
    const enrolledBytes = `${JSON.stringify(enrolled, null, 2)}\n`;
    fs.writeFileSync(configPath, enrolledBytes, { mode: 0o600 });

    const baselineState = () => {
      const database = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      try {
        return Object.fromEntries(
          (database
            .prepare("select source, status, started_at as startedAt from automatic_capture_baseline_state")
            .all() as Array<{ source: string; status: string; startedAt: string }>)
            .map((row) => [row.source, { status: row.status, startedAt: row.startedAt }]),
        );
      } finally {
        database.close();
      }
    };
    check(
      "fixture_baselines_start_complete",
      Object.values(baselineState()).every((row: any) => row.status === "complete" && row.startedAt === BASELINE_BEFORE),
      baselineState(),
    );

    // The tailer's own classification branch, reproduced with the repo's own
    // export, on a stat-only observation — exactly how a live scan decides.
    const classify = (source: "codex" | "claude_code", file: string, observedAt: string) => {
      const database = new Database(ledgerPath, { fileMustExist: true });
      try {
        const identity = fs.lstatSync(file, { bigint: true });
        const decision = classifyCaptureBaselineFile(database, source, {
          path: file,
          device: identity.dev,
          inode: identity.ino,
          size: identity.size,
          birthtimeNs: identity.birthtimeNs,
        }, { mode: "automatic", observedAt });
        return `${decision.decision}/${decision.reason}`;
      } finally {
        database.close();
      }
    };

    const generationRows = (source: "codex" | "claude_code") => {
      const database = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      try {
        return (database
          .prepare("select count(*) as rows from automatic_capture_baseline_generations where source = ?")
          .get(source) as { rows: number }).rows;
      } finally {
        database.close();
      }
    };
    // Real macOS file immutability, the reviewer's own injection: the
    // transactional config write's atomic rename fails with EPERM.
    const chflags = (file: string, flagValue: "uchg" | "nouchg") => {
      const result = spawnSync("/usr/bin/chflags", [flagValue, file], { stdio: ["ignore", "pipe", "pipe"] });
      if (result.status !== 0) throw new Error(`chflags ${flagValue} failed: ${result.stderr?.toString() ?? ""}`);
    };
    // A fixture-only SQLite trigger: a real refusal of exactly the rollback
    // statement, so the "the fence could not be removed" branch is measured
    // rather than asserted.
    const ledgerExec = (statement: string) => {
      const database = new Database(ledgerPath, { fileMustExist: true });
      try { database.exec(statement); } finally { database.close(); }
    };

    const stubBin = path.join(sandbox, "stub-bin");
    fs.mkdirSync(stubBin, { recursive: true });
    fs.symlinkSync(process.execPath, path.join(stubBin, "node"));
    // The only launchctl this proof can reach is a stub that refuses every
    // subcommand. The main fixture has no LaunchAgent at all, so the command
    // skips the restart without ever invoking it; the restart-failure arm at
    // the end uses its own fixture home, where `print` answers exactly as
    // launchd answers for an unknown label and `bootstrap` fails.
    fs.writeFileSync(
      path.join(stubBin, "launchctl"),
      '#!/bin/sh\n' +
      'if [ "$1" = "print" ]; then\n' +
      '  echo "Could not find service \\"com.plimsoll.collector\\" in domain for user gui: $(/usr/bin/id -u)" >&2\n' +
      '  exit 113\n' +
      'fi\n' +
      'exit 113\n',
      { mode: 0o700 },
    );
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      ...fixture.env,
      HOME: home,
      USERPROFILE: home,
      PLIMSOLL_HOME: plimsollHome,
      CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
      CODEX_HOME: path.join(home, ".codex"),
      GROK_HOME: path.join(home, ".grok"),
      PATH: `${stubBin}:/usr/bin:/bin`,
      PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "500",
    };
    const neutralCwd = path.join(sandbox, "neutral-cwd");
    fs.mkdirSync(neutralCwd, { recursive: true });
    const run = (args: string[]) => command(args, env, neutralCwd);

    // ---- discover -------------------------------------------------------
    const discovered = await run(["capture-roots", "discover", "--json"]);
    check("discover_succeeds", discovered.code === 0, discovered);
    const discovery = parse(discovered);
    const entryFor = (directory: string) =>
      discovery.roots.find((entry: any) => entry.directory === path.relative(home, directory));
    check(
      "discover_reports_registered_candidate_and_missing",
      discovery.counts.registered === 2 && discovery.counts.candidate === 2 && discovery.counts.missing === 1,
      discovery.counts,
    );
    check(
      "discover_marks_unregistered_native_directories_as_candidates",
      entryFor(claudeProjects)?.state === "candidate" &&
        entryFor(claudeProjects)?.source === "claude_code" &&
        entryFor(codexProfileSessions)?.state === "candidate" &&
        entryFor(codexProfileSessions)?.source === "codex",
      discovery.roots,
    );
    check(
      "discover_marks_configured_roots_registered_and_absent_roots_missing",
      entryFor(codexSessions)?.state === "registered" &&
        entryFor(seatProjects)?.state === "registered" &&
        entryFor(departedRoot)?.state === "missing",
      discovery.roots,
    );
    check(
      "discover_reports_paths_relative_to_home_only",
      discovery.roots.every((entry: any) => entry.directory === null || !path.isAbsolute(entry.directory)) &&
        !JSON.stringify(discovery.roots).includes(sandbox),
      discovery.roots,
    );
    check(
      "discover_skips_dangling_seat_links",
      !discovery.roots.some((entry: any) => String(entry.directory ?? "").includes("dangling")),
      discovery.roots,
    );
    check("discover_writes_nothing", sha256(configPath) === createHash("sha256").update(enrolledBytes).digest("hex"), configPath);

    // ---- doctor ---------------------------------------------------------
    const doctor = await run(["doctor", "--read-only", "--json"]);
    const doctorPayload = parse(doctor);
    check(
      "doctor_reports_unregistered_candidates",
      doctorPayload.captureRoots.unregisteredCandidates.count === 2 &&
        doctorPayload.captureRoots.unregisteredCandidates.directories.includes(path.relative(home, claudeProjects)) &&
        doctorPayload.captureRoots.unregisteredCandidates.directories.includes(path.relative(home, codexProfileSessions)) &&
        doctorPayload.captureRoots.configured === 3,
      doctorPayload.captureRoots,
    );
    check(
      "doctor_ok_semantics_unchanged",
      doctorPayload.ok === false && doctorPayload.readiness === "not_installed" && doctorPayload.readOnly === true,
      { ok: doctorPayload.ok, readiness: doctorPayload.readiness },
    );

    // ---- dry run --------------------------------------------------------
    const beforeSha = sha256(configPath);
    const dryRun = await run([
      "capture-roots", "add", "--source", "claude_code", "--directory", claudeProjects,
      "--machine", MACHINE, "--dry-run", "--json",
    ]);
    check("dry_run_succeeds", dryRun.code === 0, dryRun);
    const plan = parse(dryRun);
    const expectedIdentity = deriveCaptureRootIdentity(MACHINE, "claude_code", claudeProjects);
    check(
      "dry_run_prints_before_and_after_digests_and_the_derived_root",
      plan.status === "capture_roots_add_plan" && plan.applied === false &&
        plan.beforeSha256 === beforeSha && plan.afterSha256 !== beforeSha &&
        plan.addedRoots.length === 1 && plan.addedRoots[0].rootId === expectedIdentity.rootId &&
        plan.addedRoots[0].profileId === expectedIdentity.profileId &&
        plan.addedRoots[0].installationEpochId === epoch,
      plan,
    );
    check(
      "dry_run_changes_nothing",
      sha256(configPath) === beforeSha && backups(plimsollHome).length === 0 &&
        !fs.existsSync(path.join(plimsollHome, "receipts")),
      { configSha: sha256(configPath), backups: backups(plimsollHome) },
    );
    check(
      "dry_run_leaves_baselines_alone",
      Object.values(baselineState()).every((row: any) => row.startedAt === BASELINE_BEFORE),
      baselineState(),
    );

    // ---- classification before the add ----------------------------------
    // The reviewer's measured table, reproduced here so a regression to a
    // provider-wide fence fails this proof rather than a later review.
    const classifiedBefore = {
      registeredRoot: classify("claude_code", seatTranscript, "2026-09-12T09:00:00.000Z"),
      newRootExisting: classify("claude_code", path.join(claudeProjects, "history.jsonl"), "2026-09-12T09:00:00.000Z"),
    };
    check(
      "classification_before_add_captures_both_transcripts",
      classifiedBefore.registeredRoot === "capture/generation_not_baselined" &&
        classifiedBefore.newRootExisting === "capture/generation_not_baselined",
      classifiedBefore,
    );

    // ---- apply ----------------------------------------------------------
    const applied = await run([
      "capture-roots", "add", "--source", "claude_code", "--directory", claudeProjects,
      "--machine", MACHINE, "--json",
    ]);
    check("add_succeeds", applied.code === 0, applied);
    const receipt = parse(applied);
    const afterConfig = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
    check(
      "add_appends_exactly_one_root_with_the_derived_identity",
      afterConfig.captureRoots!.length === 4 &&
        afterConfig.captureRoots![3].rootId === expectedIdentity.rootId &&
        afterConfig.captureRoots![3].profileId === expectedIdentity.profileId &&
        afterConfig.captureRoots![3].source === "claude_code" &&
        afterConfig.captureRoots![3].directory === claudeProjects &&
        afterConfig.captureRoots![3].installationEpochId === epoch,
      afterConfig.captureRoots,
    );
    check(
      "add_leaves_every_existing_root_and_enrollment_field_byte_identical",
      JSON.stringify(afterConfig.captureRoots!.slice(0, 3)) === JSON.stringify(enrolled.captureRoots) &&
        JSON.stringify({ ...afterConfig, captureRoots: null }) === JSON.stringify({ ...enrolled, captureRoots: null }),
      { before: enrolled.captureRoots, after: afterConfig.captureRoots!.slice(0, 3) },
    );
    check(
      "add_matches_the_digests_it_planned",
      receipt.beforeSha256 === beforeSha && receipt.afterSha256 === sha256(configPath) &&
        receipt.writtenSha256 === sha256(configPath) && receipt.afterSha256 === plan.afterSha256,
      { receipt: { before: receipt.beforeSha256, after: receipt.afterSha256 }, actual: sha256(configPath) },
    );
    const backup = backups(plimsollHome);
    check(
      "add_writes_one_timestamped_backup_of_the_previous_config",
      backup.length === 1 && fs.readFileSync(path.join(plimsollHome, backup[0]), "utf8") === enrolledBytes,
      backup,
    );
    const seeded = baselineState();
    check(
      "add_leaves_every_provider_baseline_cutoff_untouched",
      seeded.claude_code.startedAt === BASELINE_BEFORE && seeded.claude_code.status === "complete" &&
        seeded.codex.startedAt === BASELINE_BEFORE && seeded.codex.status === "complete",
      seeded,
    );
    check(
      "add_receipt_records_the_generations_it_fenced_not_a_moved_cutoff",
      receipt.baseline.sources.length === 1 && receipt.baseline.sources[0] === "claude_code" &&
        receipt.baseline.providerCutoffMoved === false &&
        receipt.baseline.seededAt === receipt.appendedAt &&
        receipt.baseline.generationsSealed === 1 && receipt.baseline.filesFenced === 1 &&
        receipt.baseline.before.claude_code.startedAt === BASELINE_BEFORE &&
        receipt.baseline.after.claude_code.startedAt === BASELINE_BEFORE &&
        receipt.baseline.after.claude_code.status === "complete" &&
        receipt.addedRoots[0].preexistingFiles === 1,
      receipt.baseline,
    );

    // ---- classification after the add (review r1, finding 1) -------------
    const newRootAfterAdd = path.join(claudeProjects, "session-after-add.jsonl");
    fs.writeFileSync(newRootAfterAdd, "{}\n", { mode: 0o600 });
    const classifiedAfter = {
      registeredRoot: classify("claude_code", seatTranscript, "2026-09-12T09:30:00.000Z"),
      newRootExisting: classify("claude_code", path.join(claudeProjects, "history.jsonl"), "2026-09-12T09:30:00.000Z"),
      newRootAfterAdd: classify("claude_code", newRootAfterAdd, "2026-09-12T09:30:00.000Z"),
    };
    check(
      "add_does_not_re_fence_a_file_in_an_already_registered_root",
      classifiedBefore.registeredRoot === "capture/generation_not_baselined" &&
        classifiedAfter.registeredRoot === "capture/generation_not_baselined",
      classifiedAfter,
    );
    check(
      "add_fences_the_files_already_present_in_the_new_root",
      classifiedBefore.newRootExisting === "capture/generation_not_baselined" &&
        classifiedAfter.newRootExisting === "exclude/preexisting_generation",
      classifiedAfter,
    );
    check(
      "add_still_captures_a_file_created_in_the_new_root_afterwards",
      classifiedAfter.newRootAfterAdd === "capture/generation_not_baselined",
      classifiedAfter,
    );
    check(
      "baseline_state_stays_valid_after_the_fence",
      baselineState().claude_code.status === "complete" && baselineState().codex.status === "complete",
      baselineState(),
    );
    check(
      "add_skips_the_restart_with_a_reason_when_no_launch_agent_is_installed",
      receipt.restart.attempted === false && receipt.restart.skipped === true &&
        receipt.restart.reason === "launch_agent_not_installed",
      receipt.restart,
    );
    const receiptFile = path.join(plimsollHome, "receipts", path.basename(receipt.receiptPath));
    check(
      "add_writes_a_receipt_under_the_collector_home",
      fs.existsSync(receiptFile) &&
        JSON.parse(fs.readFileSync(receiptFile, "utf8")).afterSha256 === receipt.afterSha256,
      receipt.receiptPath,
    );
    check(
      "add_receipt_is_private",
      (fs.statSync(receiptFile).mode & 0o777) === 0o600,
      (fs.statSync(receiptFile).mode & 0o777).toString(8),
    );

    // ---- refusals -------------------------------------------------------
    const appliedSha = sha256(configPath);
    const refusals: Array<[string, string[], string]> = [
      ["duplicate_directory", ["--source", "claude_code", "--directory", claudeProjects], "duplicate_directory"],
      ["outside_home", ["--source", "codex", "--directory", outsideHome], "path_outside_home"],
      ["missing_directory", ["--source", "codex", "--directory", path.join(home, ".codex-profiles", "absent", "sessions")], "directory_missing"],
      ["not_a_directory", ["--source", "codex", "--directory", notADirectory], "not_a_directory"],
      ["unknown_source", ["--source", "gemini_cli", "--directory", codexProfileSessions], "unknown_source"],
    ];
    for (const [label, args, reason] of refusals) {
      const refused = await run(["capture-roots", "add", ...args, "--machine", MACHINE, "--json"]);
      check(
        `add_refuses_${label}`,
        refused.code === 1 && parse(refused).reason === reason && sha256(configPath) === appliedSha,
        refused,
      );
    }
    check(
      "refusals_write_no_further_backup_or_receipt",
      backups(plimsollHome).length === 1 && fs.readdirSync(path.join(plimsollHome, "receipts")).length === 1,
      { backups: backups(plimsollHome), receipts: fs.readdirSync(path.join(plimsollHome, "receipts")) },
    );

    // ---- $HOME with a symlinked component (review r1, finding 2) --------
    // `discover` resolves the home through realpath; `add` must resolve it the
    // same way, or it refuses `path_outside_home` for the exact directory
    // discovery just listed. The sandbox is realpathed up front, so the
    // condition is created deliberately here rather than stepped around.
    const linkedHome = path.join(sandbox, "home-link");
    fs.symlinkSync(home, linkedHome);
    const linkedEnv = { ...env, HOME: linkedHome, USERPROFILE: linkedHome,
      CLAUDE_CONFIG_DIR: path.join(linkedHome, ".claude"),
      CODEX_HOME: path.join(linkedHome, ".codex"),
      GROK_HOME: path.join(linkedHome, ".grok") };
    const linkedDirectory = path.join(linkedHome, ".codex-profiles", "profile-a", "sessions");
    const linkedDiscovery = parse(await command(["capture-roots", "discover", "--json"], linkedEnv, neutralCwd));
    check(
      "discover_lists_the_candidate_under_a_symlinked_home",
      linkedDiscovery.roots.some((entry: any) =>
        entry.directory === path.relative(home, codexProfileSessions) && entry.state === "candidate"),
      linkedDiscovery.roots,
    );
    const linkedAdd = await command([
      "capture-roots", "add", "--source", "codex", "--directory", linkedDirectory,
      "--machine", MACHINE, "--dry-run", "--json",
    ], linkedEnv, neutralCwd);
    check(
      "add_accepts_a_candidate_under_a_symlinked_home",
      linkedAdd.code === 0 && parse(linkedAdd).status === "capture_roots_add_plan" &&
        parse(linkedAdd).addedRoots[0].directory === path.relative(home, codexProfileSessions) &&
        sha256(configPath) === appliedSha,
      linkedAdd,
    );

    // A real (non-dry-run) add through that same symlinked home is refused
    // before anything is written: the LaunchAgent manifest inspection this
    // command must do first walks the home with no-follow semantics and
    // refuses a `$HOME` that is a symlink (`LAUNCH_AGENT_UNSAFE_HOME`). That
    // guard is repo-wide and is deliberately not scoped for this command
    // (r1 finding 2 remainder); it is documented in README instead.
    const linkedRealAdd = await command([
      "capture-roots", "add", "--source", "codex", "--directory", linkedDirectory,
      "--machine", MACHINE, "--json",
    ], linkedEnv, neutralCwd);
    check(
      "a_real_add_through_a_symlinked_home_refuses_before_writing_anything",
      linkedRealAdd.code === 1 &&
        parse(linkedRealAdd).status === "capture_roots_add_refused" &&
        parse(linkedRealAdd).reason === "launch_agent_manifest_invalid" &&
        String(parse(linkedRealAdd).detail).includes("UNSAFE_HOME") &&
        sha256(configPath) === appliedSha && backups(plimsollHome).length === 1,
      linkedRealAdd,
    );

    // ---- an unknown top-level config field (review r1, finding 3) -------
    // `collectorConfigSchema` strips keys it does not know, and `captureRoots`
    // is written by fleet enrollment tooling outside this repository. The key
    // must survive the write untouched.
    const withUnknown = JSON.parse(fs.readFileSync(configPath, "utf8"));
    withUnknown.futureEnrollmentField = { minted: "2026-09-12T00:00:00.000Z", by: "fleet-enrollment" };
    fs.writeFileSync(configPath, `${JSON.stringify(withUnknown, null, 2)}\n`, { mode: 0o600 });
    const carriedAdd = await run([
      "capture-roots", "add", "--source", "codex", "--directory", codexProfileSessions,
      "--machine", MACHINE, "--json",
    ]);
    const afterUnknown = JSON.parse(fs.readFileSync(configPath, "utf8"));
    check(
      "add_carries_an_unknown_top_level_config_field_through_the_write",
      carriedAdd.code === 0 &&
        JSON.stringify(afterUnknown.futureEnrollmentField) === JSON.stringify(withUnknown.futureEnrollmentField) &&
        parse(carriedAdd).carriedUnknownKeys.includes("futureEnrollmentField") &&
        Object.keys(withUnknown).every((key) => key in afterUnknown),
      {
        before: Object.keys(withUnknown).sort(),
        after: Object.keys(afterUnknown).sort(),
        carried: parse(carriedAdd).carriedUnknownKeys,
      },
    );

    // ---- a failure between the unload and the load (review r1, finding 4)
    // The ledger the fence is written to is made unopenable, so the command
    // throws after the collector would have been stopped and after the backup
    // exists, but before the config is written. Nothing may be left half
    // applied and nothing may be left stopped.
    const seatB = path.join(home, ".claude-seats", "seat-b", "projects");
    fs.mkdirSync(seatB, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(seatB, "seat-b.jsonl"), "{}\n", { mode: 0o600 });
    const beforeFailureSha = sha256(configPath);
    const backupsBeforeFailure = backups(plimsollHome).length;
    fs.chmodSync(ledgerPath, 0o000);
    const injected = await run([
      "capture-roots", "add", "--source", "claude_code", "--directory", seatB,
      "--machine", MACHINE, "--json",
    ]);
    fs.chmodSync(ledgerPath, 0o600);
    const injectedReceipt = parse(injected);
    const failureBackup = backups(plimsollHome).sort().at(-1)!;
    check(
      "a_failure_after_the_unload_exits_1_and_reports_the_failed_step",
      injected.code === 1 && injectedReceipt.status === "capture_roots_add_failed" &&
        injectedReceipt.applied === false && injectedReceipt.failure?.step === "baseline_seed" &&
        injectedReceipt.recovery === "config_unchanged_restored_state_matches_backup",
      { code: injected.code, failure: injectedReceipt.failure, recovery: injectedReceipt.recovery },
    );
    check(
      "a_failure_after_the_unload_leaves_the_config_byte_identical_to_the_backup",
      sha256(configPath) === beforeFailureSha &&
        backups(plimsollHome).length === backupsBeforeFailure + 1 &&
        createHash("sha256").update(fs.readFileSync(path.join(plimsollHome, failureBackup)))
          .digest("hex") === beforeFailureSha,
      { config: sha256(configPath), backup: failureBackup },
    );
    check(
      "a_failure_after_the_unload_still_takes_the_restart_path",
      injectedReceipt.restart.attempted === false && injectedReceipt.restart.skipped === true &&
        injectedReceipt.restart.reason === "launch_agent_not_installed" &&
        fs.existsSync(path.join(plimsollHome, "receipts", path.basename(injectedReceipt.receiptPath))),
      injectedReceipt.restart,
    );
    check(
      "a_failure_after_the_unload_wrote_no_generation_rows",
      baselineState().claude_code.status === "complete" &&
        baselineState().claude_code.startedAt === BASELINE_BEFORE,
      baselineState(),
    );

    // ---- N5: an ambiguous walk, refused by default and overridable -------
    // A symlinked `.jsonl` inside the target directory is the tailer's own
    // error class. The refusal stays the default (the fence would not be
    // provable), but it now names the entries, and `--allow-scan-errors`
    // registers the root and leaves exactly those entries unfenced.
    const ambiguousRoot = path.join(home, ".claude-seats", "seat-ambiguous", "projects");
    fs.mkdirSync(ambiguousRoot, { recursive: true, mode: 0o700 });
    const ambiguousPlain = path.join(ambiguousRoot, "plain.jsonl");
    fs.writeFileSync(ambiguousPlain, "{}\n", { mode: 0o600 });
    const ambiguousLink = path.join(ambiguousRoot, "linked.jsonl");
    fs.symlinkSync(ambiguousPlain, ambiguousLink);
    const beforeAmbiguousSha = sha256(configPath);
    const ambiguousRefused = await run([
      "capture-roots", "add", "--source", "claude_code", "--directory", ambiguousRoot,
      "--machine", MACHINE, "--json",
    ]);
    const ambiguousRefusal = parse(ambiguousRefused);
    check(
      "add_refuses_an_ambiguous_scan_by_default_and_names_the_entries",
      ambiguousRefused.code === 1 &&
        ambiguousRefusal.reason === "capture_root_scan_ambiguous" &&
        ambiguousRefusal.errors === 1 &&
        ambiguousRefusal.entries.length === 1 &&
        ambiguousRefusal.entries[0].path === path.relative(home, ambiguousLink) &&
        ambiguousRefusal.entries[0].reason === "not_a_regular_file" &&
        sha256(configPath) === beforeAmbiguousSha,
      ambiguousRefusal,
    );
    const allowed = await run([
      "capture-roots", "add", "--source", "claude_code", "--directory", ambiguousRoot,
      "--machine", MACHINE, "--allow-scan-errors", "--json",
    ]);
    const allowedReceipt = parse(allowed);
    check(
      "allow_scan_errors_registers_the_root_and_names_what_it_left_unfenced",
      allowed.code === 0 && allowedReceipt.applied === true &&
        allowedReceipt.allowScanErrors === true &&
        allowedReceipt.scanAmbiguities.length === 1 &&
        allowedReceipt.scanAmbiguities[0].directory === path.relative(home, ambiguousRoot) &&
        allowedReceipt.scanAmbiguities[0].entries[0].path === path.relative(home, ambiguousLink) &&
        allowedReceipt.addedRoots[0].ambiguousEntries === 1 &&
        allowedReceipt.addedRoots[0].preexistingFiles === 1 &&
        allowedReceipt.baseline.generationsSealed === 1,
      { code: allowed.code, ambiguities: allowedReceipt.scanAmbiguities, added: allowedReceipt.addedRoots },
    );
    check(
      "allow_scan_errors_fences_the_resolvable_file_and_leaves_the_ambiguous_one_capturable",
      classify("claude_code", ambiguousPlain, "2026-09-12T10:00:00.000Z") === "exclude/preexisting_generation" &&
        classify("claude_code", ambiguousLink, "2026-09-12T10:00:00.000Z").startsWith("capture/"),
      {
        plain: classify("claude_code", ambiguousPlain, "2026-09-12T10:00:00.000Z"),
        linked: classify("claude_code", ambiguousLink, "2026-09-12T10:00:00.000Z"),
      },
    );

    // ---- N1: a failure at the config write rolls the fence back ----------
    // The reviewer's reproduction: `chflags uchg` on the config makes the
    // transactional write's atomic rename fail with EPERM, after the fence
    // has already been committed. Nothing may stay fenced for a directory
    // the config does not name, and `recovery` may not claim a rollback that
    // did not happen.
    const seatC = path.join(home, ".claude-seats", "seat-c", "projects");
    fs.mkdirSync(seatC, { recursive: true, mode: 0o700 });
    const seatCTranscript = path.join(seatC, "seat-c.jsonl");
    fs.writeFileSync(seatCTranscript, "{}\n", { mode: 0o600 });
    const beforeWriteFailureSha = sha256(configPath);
    const rowsBeforeWriteFailure = generationRows("claude_code");
    chflags(configPath, "uchg");
    const writeFailed = await run([
      "capture-roots", "add", "--source", "claude_code", "--directory", seatC,
      "--machine", MACHINE, "--json",
    ]);
    chflags(configPath, "nouchg");
    const writeFailedReceipt = parse(writeFailed);
    check(
      "a_config_write_failure_exits_1_and_names_the_step",
      writeFailed.code === 1 && writeFailedReceipt.status === "capture_roots_add_failed" &&
        writeFailedReceipt.applied === false && writeFailedReceipt.failure?.step === "config_write" &&
        sha256(configPath) === beforeWriteFailureSha,
      { code: writeFailed.code, failure: writeFailedReceipt.failure, config: sha256(configPath) },
    );
    check(
      "a_config_write_failure_rolls_the_fence_back_and_says_so",
      writeFailedReceipt.recovery === "config_unchanged_fence_rolled_back" &&
        writeFailedReceipt.fenceRollback?.attempted === true &&
        writeFailedReceipt.fenceRollback?.complete === true &&
        writeFailedReceipt.fenceRollback?.generationsSealed === 1 &&
        writeFailedReceipt.fenceRollback?.generationsRemoved === 1 &&
        writeFailedReceipt.fenceRollback?.retainedFiles.length === 0,
      { recovery: writeFailedReceipt.recovery, rollback: writeFailedReceipt.fenceRollback },
    );
    check(
      "a_config_write_failure_leaves_zero_generation_rows_from_this_run",
      generationRows("claude_code") === rowsBeforeWriteFailure &&
        classify("claude_code", seatCTranscript, "2026-09-12T10:05:00.000Z") === "capture/generation_not_baselined" &&
        baselineState().claude_code.status === "complete",
      {
        rows: generationRows("claude_code"),
        before: rowsBeforeWriteFailure,
        classified: classify("claude_code", seatCTranscript, "2026-09-12T10:05:00.000Z"),
      },
    );
    const retried = await run([
      "capture-roots", "add", "--source", "claude_code", "--directory", seatC,
      "--machine", MACHINE, "--json",
    ]);
    const retriedReceipt = parse(retried);
    check(
      "the_retry_after_the_config_write_failure_succeeds_and_seals_exactly_once",
      retried.code === 0 && retriedReceipt.applied === true &&
        retriedReceipt.baseline.generationsSealed === 1 &&
        retriedReceipt.baseline.generationsAlreadySealed === 0 &&
        retriedReceipt.baseline.seededAt === retriedReceipt.appendedAt &&
        generationRows("claude_code") === rowsBeforeWriteFailure + 1 &&
        classify("claude_code", seatCTranscript, "2026-09-12T10:10:00.000Z") === "exclude/preexisting_generation",
      { code: retried.code, baseline: retriedReceipt.baseline, rows: generationRows("claude_code") },
    );

    // ---- N1/N3: a rollback that cannot run, and the retry after it -------
    // A `before delete` trigger on the generation table is a real SQLite
    // refusal of exactly the rollback statement, so the fence survives a
    // config-write failure. The receipt must then say `ledger_fence_retained`
    // and name the files still fenced — never a clean rollback. The retry
    // that follows finds its rows already in place and must report that,
    // not a seeding time it did not produce (review N3).
    const seatD = path.join(home, ".claude-seats", "seat-d", "projects");
    fs.mkdirSync(seatD, { recursive: true, mode: 0o700 });
    const seatDTranscript = path.join(seatD, "seat-d.jsonl");
    fs.writeFileSync(seatDTranscript, "{}\n", { mode: 0o600 });
    const beforeRetainedSha = sha256(configPath);
    const rowsBeforeRetained = generationRows("claude_code");
    ledgerExec(
      "create trigger fixture_block_generation_delete before delete on " +
      "automatic_capture_baseline_generations begin select raise(abort, 'fixture_blocked_delete'); end",
    );
    chflags(configPath, "uchg");
    const retained = await run([
      "capture-roots", "add", "--source", "claude_code", "--directory", seatD,
      "--machine", MACHINE, "--json",
    ]);
    chflags(configPath, "nouchg");
    ledgerExec("drop trigger fixture_block_generation_delete");
    const retainedReceipt = parse(retained);
    check(
      "a_fence_that_cannot_be_rolled_back_is_reported_as_retained_with_its_files",
      retained.code === 1 && retainedReceipt.applied === false &&
        retainedReceipt.failure?.step === "config_write" &&
        retainedReceipt.recovery === "ledger_fence_retained" &&
        retainedReceipt.fenceRollback?.complete === false &&
        retainedReceipt.fenceRollback?.generationsRemoved === 0 &&
        retainedReceipt.fenceRollback?.retainedFiles.includes(path.relative(home, seatDTranscript)) &&
        sha256(configPath) === beforeRetainedSha &&
        generationRows("claude_code") === rowsBeforeRetained + 1,
      { code: retained.code, recovery: retainedReceipt.recovery, rollback: retainedReceipt.fenceRollback },
    );
    check(
      "the_retained_fence_is_the_real_state_of_the_ledger",
      classify("claude_code", seatDTranscript, "2026-09-12T10:15:00.000Z") === "exclude/preexisting_generation",
      classify("claude_code", seatDTranscript, "2026-09-12T10:15:00.000Z"),
    );
    const retriedRetained = await run([
      "capture-roots", "add", "--source", "claude_code", "--directory", seatD,
      "--machine", MACHINE, "--json",
    ]);
    const retriedRetainedReceipt = parse(retriedRetained);
    check(
      "a_retry_whose_fence_is_already_in_place_reports_already_sealed_and_no_seeded_at",
      retriedRetained.code === 0 && retriedRetainedReceipt.applied === true &&
        retriedRetainedReceipt.baseline.seededAt === null &&
        retriedRetainedReceipt.baseline.generationsSealed === 0 &&
        retriedRetainedReceipt.baseline.generationsAlreadySealed === 1 &&
        retriedRetainedReceipt.baseline.seals[0].sealed === false &&
        retriedRetainedReceipt.baseline.seals[0].reason === "already_sealed" &&
        retriedRetainedReceipt.baseline.seals[0].generationsAlreadySealed === 1 &&
        generationRows("claude_code") === rowsBeforeRetained + 1,
      { code: retriedRetained.code, baseline: retriedRetainedReceipt.baseline },
    );

    // ---- N2: a backup that was never created is never published ----------
    // The config's own directory is made read-only, so the backup's `wx`
    // open fails before anything else runs. The receipt may not name a
    // backup path a recovery script would hash to ENOENT.
    const seatE = path.join(home, ".claude-seats", "seat-e", "projects");
    fs.mkdirSync(seatE, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(seatE, "seat-e.jsonl"), "{}\n", { mode: 0o600 });
    const beforeBackupFailureSha = sha256(configPath);
    const backupsBeforeBackupFailure = backups(plimsollHome).length;
    fs.chmodSync(plimsollHome, 0o500);
    const backupFailed = await run([
      "capture-roots", "add", "--source", "claude_code", "--directory", seatE,
      "--machine", MACHINE, "--json",
    ]);
    fs.chmodSync(plimsollHome, 0o700);
    const backupFailedReceipt = parse(backupFailed);
    check(
      "a_backup_failure_publishes_no_backup_path_and_names_the_backup_step",
      backupFailed.code === 1 && backupFailedReceipt.status === "capture_roots_add_failed" &&
        backupFailedReceipt.failure?.step === "backup" &&
        backupFailedReceipt.applied === false &&
        backupFailedReceipt.backupPath === null &&
        backupFailedReceipt.backupWritten === false &&
        backupFailedReceipt.recovery === "config_unchanged_no_backup_written",
      {
        code: backupFailed.code,
        failure: backupFailedReceipt.failure,
        backupPath: backupFailedReceipt.backupPath,
        recovery: backupFailedReceipt.recovery,
      },
    );
    check(
      "a_backup_failure_leaves_no_backup_file_and_no_generation_row_behind",
      backups(plimsollHome).length === backupsBeforeBackupFailure &&
        sha256(configPath) === beforeBackupFailureSha &&
        generationRows("claude_code") === rowsBeforeRetained + 1 &&
        backupFailedReceipt.fenceRollback === null,
      { backups: backups(plimsollHome).length, before: backupsBeforeBackupFailure },
    );

    // A config whose roots do not reproduce their own ids under any candidate
    // label is the r3 helper's identity guard: refuse rather than append under
    // a derivation this host does not use.
    const codexSessionsSpare = path.join(home, ".codex-profiles", "profile-b", "sessions");
    fs.mkdirSync(codexSessionsSpare, { recursive: true, mode: 0o700 });
    const tampered = JSON.parse(fs.readFileSync(configPath, "utf8"));
    tampered.captureRoots[0].rootId = "root-000000000000000000000000";
    fs.writeFileSync(configPath, `${JSON.stringify(tampered, null, 2)}\n`, { mode: 0o600 });
    const tamperedSha = sha256(configPath);
    const mismatch = await run([
      "capture-roots", "add", "--source", "codex", "--directory", codexSessionsSpare,
      "--machine", MACHINE, "--json",
    ]);
    check(
      "add_refuses_identity_derivation_mismatch",
      mismatch.code === 1 && parse(mismatch).reason === "identity_derivation_mismatch" &&
        sha256(configPath) === tamperedSha,
      mismatch,
    );
    const unresolved = await run([
      "capture-roots", "add", "--source", "codex", "--directory", codexSessionsSpare, "--json",
    ]);
    // Finding 6: no `--machine` and nothing to recover a label from is a
    // different operator problem from a label the config contradicts, and the
    // candidates actually tried are named.
    check(
      "add_refuses_identity_machine_unresolved_when_no_machine_was_given",
      unresolved.code === 1 && parse(unresolved).reason === "identity_machine_unresolved" &&
        Array.isArray(parse(unresolved).machineCandidates) &&
        parse(unresolved).machineCandidates.length > 0 &&
        !parse(unresolved).machineCandidates.includes("") &&
        sha256(configPath) === tamperedSha,
      unresolved,
    );
    check(
      "plimsoll_machine_is_not_an_undocumented_candidate",
      !fs.readFileSync(path.join(root, "packages", "collector-cli", "src", "cli.ts"), "utf8")
        .includes("PLIMSOLL_MACHINE"),
      "PLIMSOLL_MACHINE",
    );

    check(
      "no_launch_agent_was_installed_by_this_proof",
      !fs.existsSync(path.join(home, "Library", "LaunchAgents")),
      path.join(home, "Library", "LaunchAgents"),
    );

    // ---- a restart that does not come back (review r1, finding 5) -------
    // An entirely separate fixture home so the check above stays literally
    // true of the home every other arm used. The manifest is written by this
    // repository's own installer (no launchctl), and the only launchctl on
    // PATH is the stub that fails every subcommand, so `load` fails
    // deterministically and nothing on this host is ever cycled.
    const restartSandbox = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capture-roots-restart-")));
    const restartFixture = useFixtureRoot(restartSandbox, { home: path.join(restartSandbox, "home") });
    try {
      const restartHome = restartFixture.home;
      const restartPlimsollHome = path.join(restartSandbox, "plimsoll-home");
      const restartConfigPath = path.join(restartPlimsollHome, "collector.config.json");
      const restartLedgerPath = path.join(restartPlimsollHome, "work-ledger.sqlite");
      const restartCodex = path.join(restartHome, ".codex", "sessions");
      const restartClaude = path.join(restartHome, ".claude", "projects");
      for (const directory of [restartCodex, restartClaude, restartPlimsollHome]) {
        fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      }
      const restartBuffer = new LocalEventBuffer(restartLedgerPath, { workspaceId: WORKSPACE, deviceId: DEVICE });
      let restartEpoch: string;
      try {
        restartEpoch = restartBuffer.workspaceBinding()!.currentInstallationEpochId!;
        for (const source of ["codex", "claude_code"] as const) {
          const begun = beginAutomaticCaptureBaseline(restartBuffer.database, source, {
            startedAt: BASELINE_BEFORE, filesDiscovered: 0,
          });
          completeAutomaticCaptureBaseline(restartBuffer.database, source, {
            runId: begun.latestRun!.runId, completedAt: BASELINE_BEFORE,
          });
        }
      } finally {
        restartBuffer.close();
      }
      const restartEnrolled = collectorConfigSchema.parse({
        port: 48998, tenantId: WORKSPACE, deviceId: DEVICE, installKey: "fixture-install-key",
        managed: true, captureRoots: [fixtureRoot("codex", restartCodex, restartEpoch)],
      });
      fs.writeFileSync(restartConfigPath, `${JSON.stringify(restartEnrolled, null, 2)}\n`, { mode: 0o600 });

      const syntheticPnpm = path.join(restartSandbox, "pnpm");
      fs.writeFileSync(syntheticPnpm, "#!/bin/sh\nexit 0\n", { mode: 0o700 });
      const installed = installLaunchAgent({
        homeDir: restartHome,
        repoRoot: path.join(restartSandbox, "repo"),
        pnpmPath: syntheticPnpm,
        mutationAuthority: new LifecycleMutationAuthority(path.join(restartSandbox, "install-authority")),
      });
      check(
        "restart_fixture_has_an_owned_manifest_written_without_launchctl",
        installed.receipt.status === "installed" && fs.existsSync(installed.plistPath),
        installed.receipt.status,
      );

      const restartResult = await command([
        "capture-roots", "add", "--source", "claude_code", "--directory", restartClaude,
        "--machine", MACHINE, "--json",
      ], {
        ...env,
        ...restartFixture.env,
        HOME: restartHome,
        USERPROFILE: restartHome,
        PLIMSOLL_HOME: restartPlimsollHome,
        CLAUDE_CONFIG_DIR: path.join(restartHome, ".claude"),
        CODEX_HOME: path.join(restartHome, ".codex"),
        GROK_HOME: path.join(restartHome, ".grok"),
      }, neutralCwd);
      const restartReceipt = parse(restartResult);
      check(
        "a_failed_restart_exits_1_and_names_the_step_that_failed",
        restartResult.code === 1 && restartReceipt.status === "capture_roots_add_failed" &&
          restartReceipt.restart.attempted === true && restartReceipt.restart.verified === false &&
          ["load", "daemon_verification"].includes(restartReceipt.restart.failedStep),
        { code: restartResult.code, status: restartReceipt.status, reason: restartReceipt.reason,
          failedStep: restartReceipt.restart?.failedStep, load: restartReceipt.restart?.load?.status,
          unload: restartReceipt.unload?.status ?? restartReceipt.restart?.unload?.status },
      );
      check(
        "a_failed_restart_still_leaves_the_config_fully_applied_with_its_fence",
        restartReceipt.applied === true &&
          restartReceipt.writtenSha256 === sha256(restartConfigPath) &&
          restartReceipt.writtenSha256 === restartReceipt.afterSha256 &&
          collectorConfigSchema.parse(JSON.parse(fs.readFileSync(restartConfigPath, "utf8")))
            .captureRoots!.length === 2,
        { applied: restartReceipt.applied, written: restartReceipt.writtenSha256 },
      );
      uninstallLaunchAgent({
        homeDir: restartHome,
        mutationAuthority: new LifecycleMutationAuthority(path.join(restartSandbox, "install-authority")),
      });
    } finally {
      restartFixture.restore();
      fs.rmSync(restartSandbox, { recursive: true, force: true });
    }

    // ---- N4: a failure after the bootout still reaches the restart -------
    // The bootout is the point the collector goes down. Everything the
    // unload does after it — the terminal-state observation, the PID
    // reconciler, the fence release — runs while it is down, and r2 left
    // that whole region outside the restart discipline: a throw, or a
    // terminal state that cannot be proven stopped, returned with no load
    // attempted and the collector stopped.
    //
    // Injection: a stub `launchctl` whose `print` keeps reporting a live pid.
    // The prior state is therefore "reported" (so a real bootout is issued),
    // the bootout succeeds, and the post-bootout observation can never prove
    // the job stopped. `bootstrap` then fails, so nothing on this host is
    // ever cycled and the load failure is deterministic.
    const bootoutSandbox = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capture-roots-bootout-")));
    const bootoutFixture = useFixtureRoot(bootoutSandbox, { home: path.join(bootoutSandbox, "home") });
    try {
      const bootoutHome = bootoutFixture.home;
      const bootoutPlimsollHome = path.join(bootoutSandbox, "plimsoll-home");
      const bootoutConfigPath = path.join(bootoutPlimsollHome, "collector.config.json");
      const bootoutLedgerPath = path.join(bootoutPlimsollHome, "work-ledger.sqlite");
      const bootoutCodex = path.join(bootoutHome, ".codex", "sessions");
      const bootoutClaude = path.join(bootoutHome, ".claude", "projects");
      for (const directory of [bootoutCodex, bootoutClaude, bootoutPlimsollHome]) {
        fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      }
      fs.writeFileSync(path.join(bootoutClaude, "bootout.jsonl"), "{}\n", { mode: 0o600 });
      const bootoutBuffer = new LocalEventBuffer(bootoutLedgerPath, { workspaceId: WORKSPACE, deviceId: DEVICE });
      let bootoutEpoch: string;
      try {
        bootoutEpoch = bootoutBuffer.workspaceBinding()!.currentInstallationEpochId!;
        for (const source of ["codex", "claude_code"] as const) {
          const begun = beginAutomaticCaptureBaseline(bootoutBuffer.database, source, {
            startedAt: BASELINE_BEFORE, filesDiscovered: 0,
          });
          completeAutomaticCaptureBaseline(bootoutBuffer.database, source, {
            runId: begun.latestRun!.runId, completedAt: BASELINE_BEFORE,
          });
        }
      } finally {
        bootoutBuffer.close();
      }
      const bootoutEnrolled = collectorConfigSchema.parse({
        port: 48997, tenantId: WORKSPACE, deviceId: DEVICE, installKey: "fixture-install-key",
        managed: true, captureRoots: [fixtureRoot("codex", bootoutCodex, bootoutEpoch)],
      });
      const bootoutBytes = `${JSON.stringify(bootoutEnrolled, null, 2)}\n`;
      fs.writeFileSync(bootoutConfigPath, bootoutBytes, { mode: 0o600 });

      const bootoutBin = path.join(bootoutSandbox, "stub-bin");
      fs.mkdirSync(bootoutBin, { recursive: true });
      fs.symlinkSync(process.execPath, path.join(bootoutBin, "node"));
      // `print` answers with this proof's own live pid: a job launchd still
      // reports. `bootout` succeeds silently (its stdio is inherited, so it
      // must print nothing), everything else fails.
      fs.writeFileSync(
        path.join(bootoutBin, "launchctl"),
        '#!/bin/sh\n' +
        'if [ "$1" = "print" ]; then\n' +
        `  echo "	pid = ${process.pid}"\n` +
        '  exit 0\n' +
        'fi\n' +
        'if [ "$1" = "bootout" ]; then\n' +
        '  exit 0\n' +
        'fi\n' +
        'exit 113\n',
        { mode: 0o700 },
      );
      const syntheticBootoutPnpm = path.join(bootoutSandbox, "pnpm");
      fs.writeFileSync(syntheticBootoutPnpm, "#!/bin/sh\nexit 0\n", { mode: 0o700 });
      const bootoutInstalled = installLaunchAgent({
        homeDir: bootoutHome,
        repoRoot: path.join(bootoutSandbox, "repo"),
        pnpmPath: syntheticBootoutPnpm,
        mutationAuthority: new LifecycleMutationAuthority(path.join(bootoutSandbox, "install-authority")),
      });
      check(
        "bootout_fixture_has_an_owned_manifest_written_without_launchctl",
        bootoutInstalled.receipt.status === "installed" && fs.existsSync(bootoutInstalled.plistPath),
        bootoutInstalled.receipt.status,
      );

      const bootoutResult = await command([
        "capture-roots", "add", "--source", "claude_code", "--directory", bootoutClaude,
        "--machine", MACHINE, "--json",
      ], {
        ...env,
        ...bootoutFixture.env,
        HOME: bootoutHome,
        USERPROFILE: bootoutHome,
        PLIMSOLL_HOME: bootoutPlimsollHome,
        CLAUDE_CONFIG_DIR: path.join(bootoutHome, ".claude"),
        CODEX_HOME: path.join(bootoutHome, ".codex"),
        GROK_HOME: path.join(bootoutHome, ".grok"),
        PATH: `${bootoutBin}:/usr/bin:/bin`,
      }, neutralCwd);
      const bootoutReceipt = parse(bootoutResult);
      check(
        "a_failure_after_the_bootout_still_takes_the_restart_path",
        bootoutResult.code === 1 && bootoutReceipt.status === "capture_roots_add_failed" &&
          bootoutReceipt.failure?.step === "unload" &&
          bootoutReceipt.restart.attempted === true &&
          bootoutReceipt.restart.skipped === false &&
          bootoutReceipt.restart.unload?.bootoutAttempted === true &&
          bootoutReceipt.restart.load !== undefined &&
          bootoutReceipt.restart.verified === false,
        {
          code: bootoutResult.code,
          failure: bootoutReceipt.failure,
          restart: {
            attempted: bootoutReceipt.restart?.attempted,
            load: bootoutReceipt.restart?.load?.loaded,
            unload: bootoutReceipt.restart?.unload?.status,
          },
        },
      );
      check(
        "a_failure_after_the_bootout_changes_no_config_and_writes_no_backup",
        bootoutReceipt.applied === false &&
          bootoutReceipt.backupPath === null &&
          bootoutReceipt.backupWritten === false &&
          bootoutReceipt.recovery === "config_unchanged_no_backup_written" &&
          bootoutReceipt.fenceRollback === null &&
          fs.readFileSync(bootoutConfigPath, "utf8") === bootoutBytes &&
          backups(bootoutPlimsollHome).length === 0,
        {
          applied: bootoutReceipt.applied,
          recovery: bootoutReceipt.recovery,
          backups: backups(bootoutPlimsollHome),
        },
      );
      uninstallLaunchAgent({
        homeDir: bootoutHome,
        mutationAuthority: new LifecycleMutationAuthority(path.join(bootoutSandbox, "install-authority")),
      });
    } finally {
      bootoutFixture.restore();
      fs.rmSync(bootoutSandbox, { recursive: true, force: true });
    }

    console.log(JSON.stringify({
      proof: "capture-roots-add",
      checks: checks.length,
      passed: checks.filter((entry) => entry.passed).length,
      names: checks.map((entry) => entry.name),
    }, null, 2));
  } finally {
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(JSON.stringify({
    proof: "capture-roots-add",
    failed: checks.find((entry) => !entry.passed)?.name ?? "unknown",
    error: error instanceof Error ? error.message : String(error),
  }, null, 2));
  process.exitCode = 1;
});
