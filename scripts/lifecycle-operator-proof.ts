/**
 * Issue #103/#158 packaged lifecycle operator proof.
 *
 * Drives the REAL packaged CLI process (`node dist/cli.mjs lifecycle …`)
 * end to end against a disposable HOME: self-artifact pinning with vendored
 * native companions, digest-verified immutable staging, transactional
 * manifest activation, failed-readiness auto-rollback over a LIVE WAL
 * ledger, preview-default uninstall/purge, support bundles, and the shared
 * mutation-authority fence. A stubbed `launchctl` on PATH counts invocations;
 * the contract asserts ZERO service-manager calls. No network, no live
 * collector, no credentials outside the fixture home.
 */
import { build } from "esbuild";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import {
  LaunchAgentManifestLifecycleService,
  SqliteOnlineBackupAdapter,
  resolveArtifactFromBundle,
} from "../packages/collector-cli/src/lifecycle-adapters";
import {
  defaultLifecycleAuthorityRoot,
  LifecycleMutationAuthority,
} from "../packages/collector-cli/src/lifecycle-authority";
import {
  LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
  PURGE_CONFIRMATION,
  immutableRuntimeRelativePath,
  validateRuntimeArtifact,
} from "../packages/collector-cli/src/lifecycle";
import {
  collectorBufferPath,
  collectorConfigPath,
  collectorHome,
} from "../packages/collector-cli/src/config";
import { defaultBackfillStatePath } from "../packages/collector-cli/src/upload-history";
import {
  inspectLaunchAgentManifest,
  launchAgentPlistPath,
  readLaunchAgentProgramArguments,
} from "../packages/collector-cli/src/launch-agent";
import { PLIMSOLL_VERSION } from "../packages/collector-cli/src/version";

type Check = { name: string; passed: boolean; detail?: unknown };
const checks: Check[] = [];

function check(name: string, condition: unknown, detail?: unknown) {
  const row = { name, passed: Boolean(condition), ...(detail !== undefined ? { detail } : {}) };
  checks.push(row);
  console.log(`${row.passed ? "PASS" : "FAIL"} ${name}`);
  if (!row.passed) throw new Error(`${name}: ${JSON.stringify(detail ?? null)}`);
}

const REPO_ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const BUNDLE_PATH = path.join(REPO_ROOT, "packages", "collector-cli", "dist", "cli.mjs");
const SOURCE_ENTRY = path.join(REPO_ROOT, "packages", "collector-cli", "src", "cli.ts");
const TSX_ENTRY = path.join(REPO_ROOT, "node_modules", "tsx", "dist", "cli.mjs");

const ARCHITECTURE = process.arch === "x64" ? "x64" as const : "arm64" as const;
const NODE_MAJOR = Number(process.versions.node.split(".", 1)[0]);

function runtimeExecutablePath(versionsRoot: string, version: string) {
  return path.join(versionsRoot, version, `darwin-${ARCHITECTURE}`, "bin", "plimsoll.mjs");
}

function sha256File(file: string): `sha256:${string}` {
  return `sha256:${createHash("sha256").update(fs.readFileSync(file)).digest("hex")}`;
}

const TMP_ROOT = fs.realpathSync(os.tmpdir());
const fixtureHome = fs.mkdtempSync(path.join(TMP_ROOT, "plimsoll-operator-home-"));
if (path.resolve(fixtureHome) === path.resolve(os.homedir())) {
  throw new Error("fixture home collided with the real user home; refusing to run");
}
const stubBin = path.join(fixtureHome, "stubbin");
fs.mkdirSync(stubBin, { mode: 0o700 });
const launchctlLog = path.join(stubBin, "invocations.log");
fs.writeFileSync(path.join(stubBin, "launchctl"), `#!/bin/sh\necho "$@" >> "${launchctlLog}"\nexit 3\n`, { mode: 0o700 });

const LIFECYCLE_ROOT = path.join(collectorHome(fixtureHome), "lifecycle");
const VERSIONS_ROOT = path.join(LIFECYCLE_ROOT, "versions");

const childEnv: NodeJS.ProcessEnv = {
  ...process.env,
  HOME: fixtureHome,
  PATH: `${stubBin}:${process.env.PATH ?? ""}`,
};
delete childEnv.PLIMSOLL_HOME;
delete childEnv.PLIMSOLL_COLLECTOR_DATA_MODE;

type CliResult = { code: number | null; stdoutText: string; stderrText: string };

function cli(args: readonly string[]): CliResult {
  const result = spawnSync(process.execPath, [BUNDLE_PATH, ...args], {
    cwd: fixtureHome,
    env: childEnv,
    encoding: "utf8",
    timeout: 120_000,
  });
  return { code: result.status, stdoutText: result.stdout ?? "", stderrText: result.stderr ?? "" };
}

function cliJson(args: readonly string[]): Record<string, unknown> {
  const result = cli(args);
  if (result.code !== 0) {
    throw new Error(`lifecycle ${args.join(" ")} failed: ${result.stderrText.slice(0, 400)}`);
  }
  return JSON.parse(result.stdoutText) as Record<string, unknown>;
}

function expectFailure(args: readonly string[], pattern: RegExp) {
  const result = cli(args);
  const combined = `${result.stderrText}\n${result.stdoutText}`;
  return { result, matched: result.code !== 0 && pattern.test(combined), combined };
}

function readFileIfExists(file: string): string | null {
  try {
    return fs.readFileSync(file, "utf8");
  } catch {
    return null;
  }
}

function launchctlInvocationCount(): number {
  const log = readFileIfExists(launchctlLog);
  return log ? log.split("\n").filter((line) => line.trim()).length : 0;
}

function manifestDecision() {
  return readLaunchAgentProgramArguments({ homeDir: fixtureHome });
}

let liveLedger: Database.Database | null = null;

async function main(): Promise<void> {
  try {
  // --- Build the real packaged bundle --------------------------------------
  fs.mkdirSync(path.dirname(BUNDLE_PATH), { recursive: true });
  await build({
    entryPoints: [SOURCE_ENTRY],
    bundle: true,
    platform: "node",
    target: "node20",
    format: "esm",
    outfile: BUNDLE_PATH,
    external: ["better-sqlite3"],
    sourcemap: false,
    logLevel: "silent",
  });
  fs.chmodSync(BUNDLE_PATH, 0o755);
  check("bundle_built", fs.statSync(BUNDLE_PATH).size > 1024);

  // --- Seed durable state inside the fixture home ---------------------------
  fs.mkdirSync(path.dirname(collectorConfigPath(fixtureHome)), { recursive: true, mode: 0o700 });
  fs.writeFileSync(collectorConfigPath(fixtureHome), `${JSON.stringify({})}\n`, { mode: 0o600 });
  liveLedger = new Database(collectorBufferPath(fixtureHome));
  liveLedger.pragma("journal_mode = WAL");
  liveLedger.exec("create table if not exists operator_proof_probe (id text primary key, payload text not null)");
  liveLedger.prepare("insert or replace into operator_proof_probe (id, payload) values ('sentinel', 'live-wal-row')").run();
  check("fixture_ledger_seeded_wal",
    (liveLedger.prepare("pragma journal_mode").get() as { journal_mode: string }).journal_mode === "wal");

  // --- Guard rail is armed ---------------------------------------------------
  const probeLaunchctl = spawnSync("launchctl", ["print", "proof.armed"], {
    env: childEnv, encoding: "utf8", timeout: 10_000,
  });
  check("launchctl_stub_armed", probeLaunchctl.status === 3 && launchctlInvocationCount() === 1);

  // --- Self-pinned update -----------------------------------------------------
  const update = cliJson(["lifecycle", "update", "--operation-id", "op-u1", "--artifact", "self"]);
  const updateReceipt = update.receipt as Record<string, unknown>;
  check("self_update_completes", updateReceipt.status === "completed", updateReceipt);
  check("self_update_versions", updateReceipt.fromVersion === null && updateReceipt.toVersion === PLIMSOLL_VERSION);
  const updateBoundary = update.boundary as Record<string, unknown>;
  check("boundary_never_touches_live_service_or_moves_credentials",
    updateBoundary.liveServiceTouched === false && updateBoundary.credentialsMoved === false &&
    updateBoundary.leave === "distinct_operation_not_performed" &&
    updateBoundary.revoke === "hosted_owner_operation_not_performed");

  const stagedRuntime = runtimeExecutablePath(VERSIONS_ROOT, PLIMSOLL_VERSION);
  void immutableRuntimeRelativePath;
  check("staged_runtime_digest_matches_bundle",
    fs.readFileSync(stagedRuntime).equals(fs.readFileSync(BUNDLE_PATH)), stagedRuntime);

  const nativeSource = (() => {
    let cursor = path.dirname(BUNDLE_PATH);
    while (true) {
      const candidate = path.join(cursor, "node_modules", "better-sqlite3", "build", "Release", "better_sqlite3.node");
      if (fs.existsSync(candidate)) return candidate;
      const parent = path.dirname(cursor);
      if (parent === cursor) throw new Error("source better_sqlite3.node not found near bundle");
      cursor = parent;
    }
  })();
  const vendoredBinding = path.join(
    path.dirname(path.dirname(stagedRuntime)),
    "node_modules", "better-sqlite3", "build", "Release", "better_sqlite3.node",
  );
  check("native_binding_vendored_into_immutable_runtime", fs.existsSync(vendoredBinding), vendoredBinding);
  check("vendored_binding_digest_matches_source",
    fs.readFileSync(vendoredBinding).equals(fs.readFileSync(nativeSource)));

  const decision = manifestDecision();
  check("manifest_points_at_staged_runtime_only",
    JSON.stringify(decision.programArguments) === JSON.stringify([process.execPath, stagedRuntime, "start"]) &&
    decision.workingDirectory === path.dirname(stagedRuntime),
    decision);
  const visible = inspectLaunchAgentManifest({ homeDir: fixtureHome });
  check("manifest_visible_and_owned", visible.ok && visible.manifestDigest !== null);

  const stateJson = JSON.parse(readFileIfExists(path.join(LIFECYCLE_ROOT, "state.json"))!) as {
    schemaVersion: number;
    version: string;
    executablePath: string;
  };
  check("state_records_installed_version_and_path",
    stateJson.schemaVersion === 1 && stateJson.version === PLIMSOLL_VERSION &&
    stateJson.executablePath === stagedRuntime);
  check("receipts_persisted",
    fs.existsSync(path.join(LIFECYCLE_ROOT, "completed-operations", "op-u1.json")) &&
    fs.existsSync(path.join(LIFECYCLE_ROOT, "receipts", "op-u1-update.json")));
  check("operation_marker_blocks_reuse",
    expectFailure(["lifecycle", "update", "--operation-id", "op-u1", "--artifact", "self"],
      /operationId was already completed/).matched);
  check("journal_cleared_after_completion",
    !fs.existsSync(path.join(LIFECYCLE_ROOT, "journal.json")));

  // --- Support bundle ---------------------------------------------------------
  const support = cliJson(["lifecycle", "support-bundle", "--operation-id", "op-sb1"]);
  const supportReceipt = support.receipt as Record<string, unknown>;
  const supportBundle = support.bundle as Record<string, unknown>;
  check("support_bundle_generated", supportReceipt.status === "generated");
  const supportReadiness = supportBundle.readiness as Record<string, unknown>;
  check("support_bundle_reports_ready_install",
    supportReadiness.ready === true && supportReadiness.reason === "ready", supportReadiness);
  check("support_bundle_counters_bounded_integers",
    Object.values(supportBundle.counters as Record<string, unknown>).every(
      (value) => typeof value === "number" && Number.isInteger(value) && value >= 0,
    ));
  const boundedLogs = supportBundle.boundedLogs as unknown[];
  check("support_bundle_logs_content_free_counts_only",
    Array.isArray(boundedLogs) && boundedLogs.length <= 32 &&
    boundedLogs.every((row) => {
      const record = row as Record<string, unknown>;
      return typeof record.code === "string" && typeof record.count === "number" &&
        record.source !== undefined && record.severity !== undefined;
    }));

  // --- Rollback to an explicitly-versioned artifact -----------------------------
  const rollback = cliJson([
    "lifecycle", "rollback", "--operation-id", "op-r1",
    "--artifact", BUNDLE_PATH, "--artifact-version", "9.9.9-test",
  ]);
  const rollbackReceipt = rollback.receipt as Record<string, unknown>;
  check("explicit_rollback_completes",
    rollbackReceipt.status === "completed" && rollbackReceipt.toVersion === "9.9.9-test", rollbackReceipt);
  check("both_runtimes_retained",
    fs.existsSync(runtimeExecutablePath(VERSIONS_ROOT, PLIMSOLL_VERSION)) &&
    fs.existsSync(runtimeExecutablePath(VERSIONS_ROOT, "9.9.9-test")));
  check("manifest_follows_rolled_back_pointer",
    manifestDecision().programArguments[1] ===
      runtimeExecutablePath(VERSIONS_ROOT, "9.9.9-test"),
    manifestDecision());
  check("tool_version_consistent_across_receipts",
    updateReceipt.toolVersion === PLIMSOLL_VERSION &&
    rollbackReceipt.toolVersion === PLIMSOLL_VERSION &&
    supportReceipt.toolVersion === PLIMSOLL_VERSION);

  // --- Failed readiness auto-restores over a LIVE WAL ledger ---------------------
  liveLedger.prepare("insert or replace into operator_proof_probe (id, payload) values ('pre-failure', 'written-under-open-wal')").run();
  fs.writeFileSync(collectorConfigPath(fixtureHome), "{corrupted", { mode: 0o600 });
  const failed = cli(["lifecycle", "update", "--operation-id", "op-f1", "--artifact", "self"]);
  check("failed_readiness_fails_the_command", failed.code !== 0, failed.stderrText.slice(0, 300));
  check("failure_names_config_incompatibility",
    /readiness failed: config_incompatible/.test(failed.stderrText), failed.stderrText.slice(-400));
  const opF1Receipt = JSON.parse(
    readFileIfExists(path.join(LIFECYCLE_ROOT, "receipts", "op-f1-update.json"))!,
  ) as Record<string, unknown>;
  check("auto_rollback_receipt_recorded",
    opF1Receipt.status === "rolled_back" && opF1Receipt.restoredVersion === "9.9.9-test", opF1Receipt);
  const stateAfterRollback = JSON.parse(
    readFileIfExists(path.join(LIFECYCLE_ROOT, "state.json"))!,
  ) as { version: string };
  check("runtime_state_restored_to_prior_version", stateAfterRollback.version === "9.9.9-test");
  check("manifest_restored_to_prior_runtime",
    manifestDecision().programArguments[1] ===
      runtimeExecutablePath(VERSIONS_ROOT, "9.9.9-test"));
  check("config_restored_to_preoperation_bytes",
    readFileIfExists(collectorConfigPath(fixtureHome)) === "{corrupted");
  check("journal_cleared_after_auto_rollback",
    !fs.existsSync(path.join(LIFECYCLE_ROOT, "journal.json")));
  liveLedger.close();
  liveLedger = null;
  const reopened = new Database(collectorBufferPath(fixtureHome), { readonly: true });
  const sentinel = reopened.prepare("select payload from operator_proof_probe where id = 'sentinel'").get() as { payload: string } | undefined;
  const preFailure = reopened.prepare("select payload from operator_proof_probe where id = 'pre-failure'").get() as { payload: string } | undefined;
  reopened.close();
  check("live_wal_ledger_survives_snapshot_restore_cycle",
    sentinel?.payload === "live-wal-row" && preFailure?.payload === "written-under-open-wal", { sentinel, preFailure });
  fs.writeFileSync(collectorConfigPath(fixtureHome), `${JSON.stringify({})}\n`, { mode: 0o600 });

  // --- Shared mutation authority refuses concurrent operators --------------------
  const authority = new LifecycleMutationAuthority(defaultLifecycleAuthorityRoot(fixtureHome));
  const held = authority.acquire({ leaseMs: 60_000 });
  check("authority_grants_lease_to_proof", held.kind === "acquired");
  if (held.kind === "acquired") {
    const blocked = expectFailure(
      ["lifecycle", "update", "--operation-id", "op-c1", "--artifact", "self"],
      /another lifecycle operation owns the lock/,
    );
    check("concurrent_operator_refused_by_fence", blocked.matched, blocked.combined.slice(0, 300));
    held.lease.release();
    const afterRelease = cliJson(["lifecycle", "update", "--operation-id", "op-c1", "--artifact", "self"]);
    check("fence_release_restores_operator_access",
      (afterRelease.receipt as Record<string, unknown>).status === "completed");
  }

  // --- Uninstall preview/apply -----------------------------------------------------
  const uninstallPreview = cliJson(["lifecycle", "uninstall", "--operation-id", "op-un1"]);
  const uninstallPreviewReceipt = uninstallPreview.receipt as Record<string, unknown>;
  check("uninstall_defaults_to_preview", uninstallPreviewReceipt.status === "preview", uninstallPreviewReceipt);
  check("preview_removes_nothing",
    fs.existsSync(path.join(LIFECYCLE_ROOT, "state.json")) &&
    fs.existsSync(launchAgentPlistPath(fixtureHome)));

  const uninstallApply = cliJson(["lifecycle", "uninstall", "--operation-id", "op-un2", "--apply"]);
  const uninstallApplyReceipt = uninstallApply.receipt as Record<string, unknown>;
  check("uninstall_apply_completes", uninstallApplyReceipt.status === "completed", uninstallApplyReceipt);
  check("uninstall_owned_targets_declared",
    JSON.stringify(uninstallApplyReceipt.ownedTargets) ===
      JSON.stringify(["service_manifest", "tool_config_fragments", "runtime_pointer", "runtime_versions"]),
    uninstallApplyReceipt.ownedTargets);
  check("uninstall_retains_data_targets",
    JSON.stringify([...(uninstallApplyReceipt.retainedTargets as string[])].sort()) ===
      JSON.stringify([...LIFECYCLE_UNINSTALL_RETAINED_TARGETS].sort()));
  check("uninstall_removed_manifest_runtime_and_pointer",
    inspectLaunchAgentManifest({ homeDir: fixtureHome }).status === "missing" &&
    !fs.existsSync(path.join(LIFECYCLE_ROOT, "state.json")) &&
    !fs.existsSync(path.join(LIFECYCLE_ROOT, "current")) &&
    !fs.existsSync(VERSIONS_ROOT));

  // --- Reinstall, then purge semantics ----------------------------------------------
  const reinstall = cliJson(["lifecycle", "update", "--operation-id", "op-e1", "--artifact", "self"]);
  check("reinstall_after_uninstall_completes", (reinstall.receipt as Record<string, unknown>).status === "completed");

  check("purge_rejects_wrong_confirmation",
    expectFailure(
      ["lifecycle", "purge", "--operation-id", "op-pw", "--apply", "--confirm-exact", "WRONG CONFIRMATION"],
      /purge/,
    ).matched);
  check("wrong_confirmation_deletes_nothing", fs.existsSync(collectorBufferPath(fixtureHome)));

  const purgePreview = cliJson(["lifecycle", "purge", "--operation-id", "op-p2"]);
  check("purge_defaults_to_preview", (purgePreview.receipt as Record<string, unknown>).status === "preview");
  check("purge_preview_preserves_data",
    fs.existsSync(collectorBufferPath(fixtureHome)) && fs.existsSync(collectorConfigPath(fixtureHome)));

  const purgeApply = cliJson([
    "lifecycle", "purge", "--operation-id", "op-p3", "--apply", "--confirm-exact", PURGE_CONFIRMATION,
  ]);
  const purgeApplyReceipt = purgeApply.receipt as Record<string, unknown>;
  check("purge_apply_with_exact_confirmation", purgeApplyReceipt.status === "purged", purgeApplyReceipt);
  check("purge_removed_ledger_config_history_snapshots",
    !fs.existsSync(collectorBufferPath(fixtureHome)) &&
    !fs.existsSync(collectorConfigPath(fixtureHome)) &&
    !fs.existsSync(defaultBackfillStatePath(fixtureHome)) &&
    !fs.existsSync(path.join(LIFECYCLE_ROOT, "snapshots")));
  check("purge_does_not_touch_service_manifest", fs.existsSync(launchAgentPlistPath(fixtureHome)));
  check("purge_retains_workspace_membership_only",
    JSON.stringify(purgeApplyReceipt.retainedTargets) === JSON.stringify(["workspace_membership"]));

  // --- Argument guards (hostile operator input) --------------------------------------
  check("unknown_action_refused",
    expectFailure(["lifecycle", "explode", "--operation-id", "g1"], /Expected lifecycle update\|rollback/).matched);
  check("relative_artifact_path_refused",
    expectFailure(["lifecycle", "update", "--operation-id", "g2", "--artifact", "dist/cli.mjs"],
      /--artifact must be `self` or an absolute path/).matched);
  check("explicit_artifact_requires_version",
    expectFailure(["lifecycle", "update", "--operation-id", "g3", "--artifact", BUNDLE_PATH],
      /explicit --artifact paths require --artifact-version/).matched);
  check("purge_apply_without_confirmation_refused",
    expectFailure(["lifecycle", "purge", "--operation-id", "g4", "--apply"],
      /purge exact confirmation missing|purge requires exact confirmation/).matched);
  const sourceRefusal = spawnSync(
    process.execPath,
    [TSX_ENTRY, SOURCE_ENTRY, "lifecycle", "update", "--operation-id", "g5", "--artifact", "self"],
    { cwd: REPO_ROOT, env: childEnv, encoding: "utf8", timeout: 180_000 },
  );
  check("self_artifact_refused_from_source_checkout",
    sourceRefusal.status !== 0 &&
      /--artifact self requires running from a packaged plimsoll bundle/.test(`${sourceRefusal.stderr}${sourceRefusal.stdout}`),
    String(sourceRefusal.stderr).slice(-300));

  // --- In-process adapter units ---------------------------------------------------------
  const unitRoot = fs.mkdtempSync(path.join(TMP_ROOT, "plimsoll-operator-unit-"));
  try {
    const backup = new SqliteOnlineBackupAdapter();
    const missingSnapshot = await backup.snapshot({
      source: path.join(unitRoot, "missing.sqlite"),
      destination: path.join(unitRoot, "out.sqlite"),
    });
    check("backup_missing_source_is_false", missingSnapshot === false);

    const source = new Database(path.join(unitRoot, "source.sqlite"));
    source.pragma("journal_mode = WAL");
    source.exec("create table t (v text)");
    source.prepare("insert into t values ('row')").run();
    const snapDestination = path.join(unitRoot, "snapshot.sqlite");
    const snapshotOk = await backup.snapshot({
      source: path.join(unitRoot, "source.sqlite"),
      destination: snapDestination,
    });
    check("online_backup_snapshot_succeeds", snapshotOk === true);
    source.close();
    const snapDb = new Database(snapDestination, { readonly: true });
    const snapRow = snapDb.prepare("select v from t").get() as { v: string } | undefined;
    snapDb.close();
    check("quiesced_snapshot_readable_with_rows", snapRow?.v === "row");

    fs.writeFileSync(path.join(unitRoot, "restored.sqlite-wal"), "stale-wal");
    fs.writeFileSync(path.join(unitRoot, "restored.sqlite-shm"), "stale-shm");
    await backup.restore({ source: snapDestination, destination: path.join(unitRoot, "restored.sqlite") });
    check("restore_clears_stale_wal_shm",
      !fs.existsSync(path.join(unitRoot, "restored.sqlite-wal")) &&
      !fs.existsSync(path.join(unitRoot, "restored.sqlite-shm")));
    const restoredDb = new Database(path.join(unitRoot, "restored.sqlite"), { readonly: true });
    const restoredRow = restoredDb.prepare("select v from t").get() as { v: string } | undefined;
    restoredDb.close();
    check("restored_database_matches_snapshot", restoredRow?.v === "row");

    // Readiness matrix through the production service adapter.
    const matrixRoot = fs.mkdtempSync(path.join(TMP_ROOT, "plimsoll-operator-matrix-"));
    try {
      const lifecycleRoot = path.join(matrixRoot, "lifecycle");
      const matrixHome = matrixRoot;
      fs.mkdirSync(path.dirname(collectorConfigPath(matrixHome)), { recursive: true, mode: 0o700 });
      fs.writeFileSync(collectorConfigPath(matrixHome), `${JSON.stringify({})}\n`, { mode: 0o600 });
      const service = new LaunchAgentManifestLifecycleService({ homeDir: matrixHome, lifecycleRoot });
      const readinessInput = { signal: new AbortController().signal, deadlineMs: 500 };
      const stagedForMatrix = path.join(lifecycleRoot, "versions", "1.0.0", `darwin-${ARCHITECTURE}`, "bin", "plimsoll.mjs");

      const noManifest = await service.readiness("1.0.0", readinessInput);
      check("readiness_manifest_missing_service_unready",
        !noManifest.ready && noManifest.reason === "service_unready" && noManifest.runtimeVersion === null,
        noManifest);

      const wrongExe = path.join(matrixRoot, "elsewhere", "cli.mjs");
      fs.mkdirSync(path.dirname(wrongExe), { recursive: true, mode: 0o700 });
      fs.writeFileSync(wrongExe, "// not plimsoll\n");
      await service.activate({ executablePath: wrongExe, version: "0.0.0-wrong" });
      const mismatched = await service.readiness("1.0.0", readinessInput);
      check("readiness_pointer_mismatch_runtime_mismatch",
        !mismatched.ready && mismatched.reason === "runtime_mismatch" && mismatched.runtimeVersion === "0.0.0-wrong",
        mismatched);

      fs.mkdirSync(path.dirname(stagedForMatrix), { recursive: true, mode: 0o700 });
      fs.copyFileSync(wrongExe, stagedForMatrix);
      await service.activate({ executablePath: stagedForMatrix, version: "1.0.0" });
      const ready = await service.readiness("1.0.0", readinessInput);
      check("readiness_ready_when_all_boundaries_hold",
        ready.ready && ready.reason === "ready" && ready.runtimeVersion === "1.0.0" &&
        ready.serviceReady && ready.configCompatible && ready.databaseCompatible,
        ready);

      fs.writeFileSync(collectorConfigPath(matrixHome), "{broken", { mode: 0o600 });
      const badConfig = await service.readiness("1.0.0", readinessInput);
      check("readiness_invalid_config_detected",
        !badConfig.ready && badConfig.reason === "config_incompatible" && badConfig.configCompatible === false);
      fs.writeFileSync(collectorConfigPath(matrixHome), `${JSON.stringify({})}\n`, { mode: 0o600 });

      fs.writeFileSync(collectorBufferPath(matrixHome), Buffer.from([0xde, 0xad, 0xbe, 0xef]));
      const badDatabase = await service.readiness("1.0.0", readinessInput);
      check("readiness_corrupt_ledger_detected",
        !badDatabase.ready && badDatabase.reason === "database_incompatible" && badDatabase.databaseCompatible === false);
    } finally {
      fs.rmSync(matrixRoot, { recursive: true, force: true });
    }

    // Hostile artifact resolution.
    let closureError: string | null = null;
    try {
      resolveArtifactFromBundle({ bundlePath: path.join(unitRoot, "lonely.mjs"), version: "1.0.0" });
    } catch (error) {
      closureError = (error as Error).message;
    }
    check("artifact_without_native_closure_refused",
      closureError !== null && /no vendored native dependency closure/.test(closureError), closureError);

    let missingError: string | null = null;
    try {
      resolveArtifactFromBundle({ bundlePath: path.join(unitRoot, "nope.mjs"), version: "1.0.0" });
    } catch (error) {
      missingError = (error as Error).message;
    }
    check("missing_artifact_refused", missingError !== null && /artifact bundle is missing/.test(missingError));

    let traversalRefused = false;
    try {
      resolveArtifactFromBundle({ bundlePath: SOURCE_ENTRY.replace(/\.ts$/, ".ts"), version: "../evil" });
    } catch {
      traversalRefused = true;
    }
    check("version_traversal_refused", traversalRefused);

    const hostileBase = {
      version: "1.0.0",
      platform: "darwin" as const,
      architecture: "arm64" as const,
      nodeMajor: NODE_MAJOR,
      sha256: sha256File(BUNDLE_PATH),
      sourcePath: BUNDLE_PATH,
    };
    let companionTraversalRefused = false;
    try {
      validateRuntimeArtifact({
        ...hostileBase,
        files: [{
          relativePath: "../escape.node",
          sha256: sha256File(BUNDLE_PATH),
          sourcePath: BUNDLE_PATH,
        }],
      });
    } catch {
      companionTraversalRefused = true;
    }
    check("companion_relative_path_traversal_refused", companionTraversalRefused);

    let executableCollisionRefused = false;
    try {
      validateRuntimeArtifact({
        ...hostileBase,
        files: [{
          relativePath: "bin/plimsoll.mjs",
          sha256: sha256File(BUNDLE_PATH),
          sourcePath: BUNDLE_PATH,
        }],
      });
    } catch {
      executableCollisionRefused = true;
    }
    check("companion_executable_collision_refused", executableCollisionRefused);
  } finally {
    fs.rmSync(unitRoot, { recursive: true, force: true });
  }

  // --- Final: the whole exercise never invoked the service manager -------------
  check("zero_launchctl_invocations_end_to_end", launchctlInvocationCount() === 1, launchctlInvocationCount());

  console.log(`lifecycle-operator proof: ${checks.length} checks, all passed`);
} finally {
    liveLedger?.close();
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
