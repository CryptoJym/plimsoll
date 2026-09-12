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
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  beginAutomaticCaptureBaseline,
  completeAutomaticCaptureBaseline,
} from "../packages/collector-cli/src/capture-baseline";
import { deriveCaptureRootIdentity } from "../packages/collector-cli/src/capture-root-inventory";
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

    const stubBin = path.join(sandbox, "stub-bin");
    fs.mkdirSync(stubBin, { recursive: true });
    fs.symlinkSync(process.execPath, path.join(stubBin, "node"));
    // launchctl must never be reachable from this proof; the fixture has no
    // LaunchAgent, so the command must skip the restart on its own.
    fs.writeFileSync(path.join(stubBin, "launchctl"), "#!/bin/sh\nexit 113\n", { mode: 0o700 });
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
      "add_seeds_only_the_new_root_provider_baseline_at_the_append_time",
      seeded.claude_code.startedAt === receipt.appendedAt && seeded.claude_code.status === "in_progress" &&
        seeded.codex.startedAt === BASELINE_BEFORE && seeded.codex.status === "complete",
      seeded,
    );
    check(
      "add_receipt_records_the_baseline_it_moved",
      receipt.baseline.sources.length === 1 && receipt.baseline.sources[0] === "claude_code" &&
        receipt.baseline.before.claude_code.startedAt === BASELINE_BEFORE &&
        receipt.baseline.after.claude_code.startedAt === receipt.appendedAt,
      receipt.baseline,
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

    // A config whose roots do not reproduce their own ids under any candidate
    // label is the r3 helper's identity guard: refuse rather than append under
    // a derivation this host does not use.
    const tampered = JSON.parse(fs.readFileSync(configPath, "utf8"));
    tampered.captureRoots[0].rootId = "root-000000000000000000000000";
    fs.writeFileSync(configPath, `${JSON.stringify(tampered, null, 2)}\n`, { mode: 0o600 });
    const tamperedSha = sha256(configPath);
    const mismatch = await run([
      "capture-roots", "add", "--source", "codex", "--directory", codexProfileSessions,
      "--machine", MACHINE, "--json",
    ]);
    check(
      "add_refuses_identity_derivation_mismatch",
      mismatch.code === 1 && parse(mismatch).reason === "identity_derivation_mismatch" &&
        sha256(configPath) === tamperedSha,
      mismatch,
    );
    const unresolved = await run([
      "capture-roots", "add", "--source", "codex", "--directory", codexProfileSessions, "--json",
    ]);
    check(
      "add_refuses_when_no_candidate_label_reproduces_the_configured_ids",
      unresolved.code === 1 && parse(unresolved).reason === "identity_derivation_mismatch" &&
        sha256(configPath) === tamperedSha,
      unresolved,
    );

    check(
      "no_launch_agent_was_installed_by_this_proof",
      !fs.existsSync(path.join(home, "Library", "LaunchAgents")),
      path.join(home, "Library", "LaunchAgents"),
    );

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
