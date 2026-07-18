/**
 * Issue #103 lifecycle proof. Every path is under one temporary ownership
 * root; service and database operations are injected fixtures. No launchctl,
 * network, browser, registry, installed config, or live ledger is touched.
 */
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  LIFECYCLE_PURGE_ONLY_TARGETS,
  LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
  LifecycleInterruption,
  LifecycleManager,
  PURGE_CONFIRMATION,
  immutableRuntimeRelativePath,
  lifecycleBoundaryStatement,
  sanitizeSupportSnapshot,
  validateRuntimeArtifact,
  type LifecycleAdapter,
  type LifecycleJournal,
  type LifecycleReadiness,
  type LifecycleSupportSnapshot,
  type RuntimeArtifact,
} from "../packages/collector-cli/src/lifecycle";
import { runLifecycleCommand } from "../packages/collector-cli/src/lifecycle-command";
import {
  FilesystemLifecycleAdapter,
  type LifecycleDatabaseAdapter,
  type LifecycleServiceAdapter,
  type ManagedLifecyclePaths,
} from "../packages/collector-cli/src/lifecycle-filesystem";
import { PLIMSOLL_VERSION } from "../packages/collector-cli/src/version";

type Check = { name: string; passed: boolean; detail: unknown };
const checks: Check[] = [];

function check(name: string, condition: unknown, detail: unknown) {
  const row = { name, passed: Boolean(condition), detail };
  checks.push(row);
  console.log(`${row.passed ? "PASS" : "FAIL"} ${name}`);
  if (!condition) throw new Error(`${name}: ${JSON.stringify(detail)}`);
}

function digest(file: string) {
  return `sha256:${createHash("sha256").update(fs.readFileSync(file)).digest("hex")}` as const;
}

function write(file: string, content: string, mode = 0o600) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, content, { mode });
}

function mode(file: string) {
  return fs.statSync(file).mode & 0o777;
}

type Fixture = ReturnType<typeof fixture>;

function fixture(name: string) {
  const ownershipRoot = fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-lifecycle-${name}-`));
  const paths: ManagedLifecyclePaths = {
    ownershipRoot,
    lifecycleRoot: path.join(ownershipRoot, "private", "lifecycle"),
    artifactSourceRoot: path.join(ownershipRoot, "artifacts"),
    collectorConfig: path.join(ownershipRoot, "private", "collector.config.json"),
    database: path.join(ownershipRoot, "private", "work-ledger.sqlite"),
    serviceManifest: path.join(ownershipRoot, "Library", "LaunchAgents", "com.plimsoll.collector.plist"),
    ownedToolFragments: [
      path.join(ownershipRoot, "tool-fragments", "claude.plimsoll.json"),
      path.join(ownershipRoot, "tool-fragments", "codex.plimsoll.toml"),
    ],
    history: [path.join(ownershipRoot, "private", "history.ndjson")],
  };
  write(paths.collectorConfig, '{"tenantId":"fixture","installKey":"credential-sentinel"}\n');
  write(paths.database, "ledger-v1\n");
  write(paths.history[0]!, "history-sentinel\n");
  for (const fragment of paths.ownedToolFragments) write(fragment, "plimsoll-owned-fragment\n");

  let runtimeVersion: string | null = null;
  let failHealth = false;
  let migrateOnActivate = false;
  let activations = 0;
  let removals = 0;
  let supportGetterAccesses = 0;
  const service: LifecycleServiceAdapter = {
    async activate(input) {
      activations += 1;
      runtimeVersion = input.version;
      write(paths.serviceManifest, `version=${input.version}\nexecutable=${input.executablePath}\n`);
      if (migrateOnActivate) {
        write(paths.collectorConfig, `config-${input.version}\n`);
        write(paths.database, `database-${input.version}\n`);
      }
    },
    async restore(input) {
      runtimeVersion = input.version;
    },
    async remove() {
      removals += 1;
      runtimeVersion = null;
    },
    async readiness(expectedVersion) {
      const ready = !failHealth && runtimeVersion === expectedVersion;
      return {
        ready,
        runtimeVersion,
        serviceReady: ready,
        configCompatible: ready,
        databaseCompatible: ready,
        reason: ready ? "ready" : failHealth ? "service_unready" : "runtime_mismatch",
      } as LifecycleReadiness;
    },
    async supportSnapshot() {
      const acceptedRowWithUnknowns = {
        source: "lifecycle",
        severity: "warn",
        code: "health.retry",
        count: 2,
        path: "/Users/row-path-sentinel",
        nested: { credential: "nested-secret-sentinel" },
        Content: "case-secret-sentinel",
        "cοntent": "unicode-secret-sentinel",
      };
      Object.defineProperty(acceptedRowWithUnknowns, "content", {
        enumerable: true,
        get() {
          supportGetterAccesses += 1;
          return "getter-secret-sentinel";
        },
      });
      const inheritedRow = Object.assign(
        Object.create({ content: "prototype-secret-sentinel" }),
        { source: "collector_stdout", severity: "info", code: "prototype.row", count: 1 },
      );
      const approvedGetterRow = { source: "collector_stdout", severity: "info", count: 1 };
      Object.defineProperty(approvedGetterRow, "code", {
        enumerable: true,
        get() {
          supportGetterAccesses += 1;
          return "getter.code";
        },
      });
      const readiness = await this.readiness(runtimeVersion ?? "missing", {
        signal: new AbortController().signal,
        deadlineMs: 1_000,
      }) as LifecycleReadiness & Record<string, unknown>;
      readiness.content = "readiness-content-sentinel";
      readiness.nested = { path: "/Users/readiness-path-sentinel" };
      const snapshot = {
        installedVersion: runtimeVersion,
        runtimeVersion,
        platform: "darwin",
        architecture: "arm64",
        nodeMajor: 22,
        readiness,
        counters: {
          activeDelivery: 7,
          deadDelivery: 2,
          tokenAttributedEvents: 99,
          maintenancePending: 1,
          content: "counter-content-sentinel",
        },
        boundedLogs: [
          acceptedRowWithUnknowns,
          inheritedRow,
          approvedGetterRow,
          { source: "collector_stderr", severity: "error", code: "/Users/private/secret", count: 1 },
        ],
        absolutePath: "/Users/private/secret",
        content: "prompt-sentinel",
        credential: "credential-sentinel",
      };
      Object.defineProperty(snapshot, "unknownGetter", {
        enumerable: true,
        get() {
          supportGetterAccesses += 1;
          return "outer-getter-secret-sentinel";
        },
      });
      return snapshot as unknown as LifecycleSupportSnapshot;
    },
  };
  const database: LifecycleDatabaseAdapter = {
    async snapshot(input) {
      if (!fs.existsSync(input.source)) return false;
      fs.mkdirSync(path.dirname(input.destination), { recursive: true, mode: 0o700 });
      fs.copyFileSync(input.source, input.destination);
      fs.chmodSync(input.destination, 0o600);
      return true;
    },
    async restore(input) {
      fs.mkdirSync(path.dirname(input.destination), { recursive: true, mode: 0o700 });
      fs.copyFileSync(input.source, input.destination);
      fs.chmodSync(input.destination, 0o600);
      fs.rmSync(`${input.destination}-wal`, { force: true });
      fs.rmSync(`${input.destination}-shm`, { force: true });
    },
  };
  const adapter = new FilesystemLifecycleAdapter(paths, service, database);
  const artifact = (version: string, architecture: "arm64" | "x64" = "arm64", nodeMajor = 22): RuntimeArtifact => {
    const sourcePath = path.join(ownershipRoot, "artifacts", `${version}-${architecture}-node${nodeMajor}`);
    write(sourcePath, `#!/bin/sh\n# ${version}-${architecture}-node${nodeMajor}\n`, 0o700);
    return { version, platform: "darwin", architecture, nodeMajor, sha256: digest(sourcePath), sourcePath };
  };
  return {
    ownershipRoot,
    paths,
    adapter,
    artifact,
    service,
    database,
    setFailHealth(value: boolean) { failHealth = value; },
    setMigrateOnActivate(value: boolean) { migrateOnActivate = value; },
    get runtimeVersion() { return runtimeVersion; },
    get activations() { return activations; },
    get removals() { return removals; },
    get supportGetterAccesses() { return supportGetterAccesses; },
    cleanup() { fs.rmSync(ownershipRoot, { recursive: true, force: true }); },
  };
}

function faultingAdapter(base: LifecycleAdapter, overrides: Partial<LifecycleAdapter>): LifecycleAdapter {
  return new Proxy(base, {
    get(target, property) {
      const override = overrides[property as keyof LifecycleAdapter];
      if (override) return typeof override === "function" ? override.bind(overrides) : override;
      const value = target[property as keyof LifecycleAdapter];
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

async function rejection(action: () => unknown | Promise<unknown>) {
  try {
    await action();
    return null;
  } catch (error) {
    return error as Error;
  }
}

async function main() {
  check("proof_runs_on_exact_node_22", process.versions.node.startsWith("22."), process.versions.node);
  const packageVersion = JSON.parse(fs.readFileSync(path.resolve(import.meta.dirname, "../packages/collector-cli/package.json"), "utf8")).version;
  check("canonical_version_comes_from_cli_package", PLIMSOLL_VERSION === packageVersion, { runtime: PLIMSOLL_VERSION, packageVersion });

  const compatibility: { node: number; arch: "arm64" | "x64"; accepted: boolean }[] = [];
  const metadata = fixture("metadata");
  try {
    for (const node of [19, 22, 25]) {
      for (const arch of ["arm64", "x64"] as const) {
        const candidate = metadata.artifact(`metadata-${node}-${arch}`, arch, node);
        const error = await rejection(() => validateRuntimeArtifact(candidate));
        compatibility.push({ node, arch, accepted: !error });
      }
    }
    check(
      "node_19_and_25_rejected_node_22_arm64_x64_accepted",
      compatibility.every((row) => row.accepted === (row.node === 22)),
      compatibility,
    );
  } finally {
    metadata.cleanup();
  }

  const happy = fixture("happy");
  try {
    const manager = new LifecycleManager(happy.adapter);
    const v1 = happy.artifact("0.6.0");
    const installed = await manager.update({ operationId: "install-v1", artifact: v1 });
    const runtimePath = path.join(happy.paths.lifecycleRoot, immutableRuntimeRelativePath(v1));
    check("update_completes_with_one_version_receipt", installed.status === "completed" && installed.toolVersion === PLIMSOLL_VERSION && installed.toVersion === "0.6.0", installed);
    check("runtime_is_version_pinned_absolute_regular_file", path.isAbsolute(runtimePath) && fs.lstatSync(runtimePath).isFile() && !fs.lstatSync(runtimePath).isSymbolicLink(), runtimePath);
    check("private_runtime_permissions_are_0700", mode(happy.paths.lifecycleRoot) === 0o700 && mode(runtimePath) === 0o700, { root: mode(happy.paths.lifecycleRoot), runtime: mode(runtimePath) });
    const statePath = path.join(happy.paths.lifecycleRoot, "state.json");
    const receiptsRoot = path.join(happy.paths.lifecycleRoot, "receipts");
    const completedRoot = path.join(happy.paths.lifecycleRoot, "completed-operations");
    check(
      "state_receipts_and_completed_markers_permissions_are_0600",
      mode(statePath) === 0o600 &&
        fs.readdirSync(receiptsRoot).every((file) => mode(path.join(receiptsRoot, file)) === 0o600) &&
        fs.readdirSync(completedRoot).every((file) => mode(path.join(completedRoot, file)) === 0o600),
      { receipts: fs.readdirSync(receiptsRoot), completed: fs.readdirSync(completedRoot) },
    );

    const initialReceiptPath = path.join(receiptsRoot, "install-v1-update.json");
    const initialReceipt = fs.readFileSync(initialReceiptPath, "utf8");
    const initialSnapshots = fs.readdirSync(path.join(happy.paths.lifecycleRoot, "snapshots"));
    const reused = await rejection(() => manager.update({
      operationId: "install-v1",
      artifact: happy.artifact("0.6.1"),
    }));
    check(
      "completed_operation_id_reuse_is_rejected_without_snapshot_or_false_restore",
      reused?.message.includes("already completed") === true &&
        happy.runtimeVersion === "0.6.0" &&
        fs.readFileSync(initialReceiptPath, "utf8") === initialReceipt &&
        (JSON.parse(initialReceipt) as { restoredVersion?: unknown }).restoredVersion === null &&
        JSON.stringify(fs.readdirSync(path.join(happy.paths.lifecycleRoot, "snapshots"))) === JSON.stringify(initialSnapshots),
      { message: reused?.message, runtime: happy.runtimeVersion },
    );

    const beforeConfig = fs.readFileSync(happy.paths.collectorConfig, "utf8");
    const beforeDatabase = fs.readFileSync(happy.paths.database, "utf8");
    const beforeService = fs.readFileSync(happy.paths.serviceManifest, "utf8");
    happy.setMigrateOnActivate(true);
    happy.setFailHealth(true);
    const failed = await rejection(() => manager.update({ operationId: "update-health-fail", artifact: happy.artifact("0.7.0") }));
    check("health_failure_fails_update", failed?.message.includes("readiness failed") === true, failed?.message);
    check("health_failure_restores_runtime_config_database_service", happy.runtimeVersion === "0.6.0" && fs.readFileSync(happy.paths.collectorConfig, "utf8") === beforeConfig && fs.readFileSync(happy.paths.database, "utf8") === beforeDatabase && fs.readFileSync(happy.paths.serviceManifest, "utf8") === beforeService, { runtime: happy.runtimeVersion });

    happy.setMigrateOnActivate(false);
    happy.setFailHealth(false);
    const diskFull = Object.assign(new Error("fixture disk full"), { code: "ENOSPC" });
    const diskAdapter = faultingAdapter(happy.adapter, { async stage() { throw diskFull; } });
    const diskError = await rejection(() => new LifecycleManager(diskAdapter).update({ operationId: "update-disk-full", artifact: happy.artifact("0.7.1") }));
    check("disk_full_before_switch_restores_previous_install", (diskError as NodeJS.ErrnoException)?.code === "ENOSPC" && happy.runtimeVersion === "0.6.0", { code: (diskError as NodeJS.ErrnoException)?.code, runtime: happy.runtimeVersion });

    const interruptedArtifact = happy.artifact("0.8.0");
    let interruptedOnce = false;
    const interruptedAdapter = faultingAdapter(happy.adapter, {
      async writeJournal(journal: LifecycleJournal) {
        await happy.adapter.writeJournal(journal);
        if (!interruptedOnce && journal.phase === "switched") {
          interruptedOnce = true;
          throw new LifecycleInterruption("fixture interruption");
        }
      },
    });
    const interrupted = await rejection(() => new LifecycleManager(interruptedAdapter).update({ operationId: "update-interrupted", artifact: interruptedArtifact }));
    check("interruption_leaves_0600_reopen_journal", interrupted instanceof LifecycleInterruption && mode(path.join(happy.paths.lifecycleRoot, "journal.json")) === 0o600, interrupted?.message);
    const resumed = await manager.update({ operationId: "update-interrupted", artifact: interruptedArtifact });
    check("reopen_resumes_idempotently_to_verified", resumed.status === "completed" && happy.runtimeVersion === "0.8.0" && !fs.existsSync(path.join(happy.paths.lifecycleRoot, "journal.json")), resumed);

    const rollback = await manager.rollback({ operationId: "rollback-v1", artifact: v1 });
    check("explicit_rollback_uses_same_transaction_and_returns_to_v1", rollback.operation === "rollback" && rollback.fromVersion === "0.8.0" && happy.runtimeVersion === "0.6.0", rollback);

    const snapshotsRoot = path.join(happy.paths.lifecycleRoot, "snapshots");
    const secretSnapshot = path.join(snapshotsRoot, "install-v1", "config");
    const previewDigest = createHash("sha256").update(fs.readFileSync(happy.paths.database)).update(fs.readFileSync(happy.paths.history[0]!)).digest("hex");
    const preview = await runLifecycleCommand({
      argv: ["uninstall", "--operation-id", "uninstall-preview"],
      adapter: happy.adapter,
      resolveArtifact: async () => { throw new Error("not used"); },
    });
    const previewRoundTrip = JSON.parse(JSON.stringify(preview.receipt)) as typeof preview.receipt;
    const previewPersisted = JSON.parse(fs.readFileSync(
      path.join(receiptsRoot, "uninstall-preview-uninstall.json"),
      "utf8",
    )) as typeof preview.receipt;
    check(
      "uninstall_command_preview_discloses_retained_and_purge_only_snapshots",
      preview.receipt.status === "preview" &&
        fs.existsSync(runtimePath) && fs.existsSync(happy.paths.serviceManifest) && fs.existsSync(secretSnapshot) &&
        !preview.receipt.ownedTargets.includes("lifecycle_snapshots") &&
        LIFECYCLE_UNINSTALL_RETAINED_TARGETS.every((target) => preview.receipt.retainedTargets.includes(target)) &&
        LIFECYCLE_PURGE_ONLY_TARGETS.every((target) => preview.receipt.purgeOnlyTargets.includes(target)) &&
        JSON.stringify(previewRoundTrip.retainedTargets) === JSON.stringify(previewPersisted.retainedTargets) &&
        JSON.stringify(previewRoundTrip.purgeOnlyTargets) === JSON.stringify(previewPersisted.purgeOnlyTargets),
      { command: previewRoundTrip, persisted: previewPersisted },
    );

    const appliedOutput = await runLifecycleCommand({
      argv: ["uninstall", "--operation-id", "uninstall-apply", "--apply"],
      adapter: happy.adapter,
      resolveArtifact: async () => { throw new Error("not used"); },
    });
    const applied = appliedOutput.receipt;
    const preservedDigest = createHash("sha256").update(fs.readFileSync(happy.paths.database)).update(fs.readFileSync(happy.paths.history[0]!)).digest("hex");
    check("uninstall_apply_removes_only_owned_runtime_service_fragments", applied.status === "completed" && !fs.existsSync(happy.paths.serviceManifest) && happy.paths.ownedToolFragments.every((file) => !fs.existsSync(file)) && !fs.existsSync(path.join(happy.paths.lifecycleRoot, "current")), applied);
    check(
      "uninstall_apply_receipt_cannot_imply_purge_only_snapshots_were_deleted",
      previewDigest === preservedDigest &&
        fs.readFileSync(happy.paths.collectorConfig, "utf8").includes("credential-sentinel") &&
        fs.existsSync(secretSnapshot) &&
        !applied.ownedTargets.includes("lifecycle_snapshots") &&
        LIFECYCLE_UNINSTALL_RETAINED_TARGETS.every((target) => applied.retainedTargets.includes(target)) &&
        LIFECYCLE_PURGE_ONLY_TARGETS.every((target) => applied.purgeOnlyTargets.includes(target)) &&
        applied.preserved.includes("workspace_membership"),
      applied,
    );

    check("lifecycle_snapshot_contains_owned_secret_copy_before_purge", fs.readFileSync(secretSnapshot, "utf8").includes("credential-sentinel"), secretSnapshot);
    const purgePreview = await runLifecycleCommand({
      argv: ["purge", "--operation-id", "purge-preview"],
      adapter: happy.adapter,
      resolveArtifact: async () => { throw new Error("not used"); },
    });
    check(
      "purge_defaults_to_preview_and_lists_secret_bearing_snapshots",
      purgePreview.receipt.status === "preview" &&
        LIFECYCLE_PURGE_ONLY_TARGETS.every((target) => purgePreview.receipt.ownedTargets.includes(target)) &&
        LIFECYCLE_UNINSTALL_RETAINED_TARGETS.every((target) => purgePreview.receipt.retainedTargets.includes(target)) &&
        fs.existsSync(secretSnapshot) && fs.existsSync(happy.paths.database),
      purgePreview.receipt,
    );
    const badPurge = await rejection(() => manager.purge({ operationId: "purge-bad", apply: true, confirmation: "yes" }));
    check("purge_rejects_non_exact_confirmation", badPurge?.message.includes("exact confirmation") === true && fs.existsSync(happy.paths.database), badPurge?.message);
    const purged = await manager.purge({ operationId: "purge-exact", apply: true, confirmation: PURGE_CONFIRMATION });
    check(
      "purge_is_separate_exact_and_deletes_live_plus_snapshot_secret_copies",
      purged.status === "purged" &&
        LIFECYCLE_PURGE_ONLY_TARGETS.every((target) => purged.ownedTargets.includes(target)) &&
        purged.retainedTargets.length === 1 && purged.retainedTargets[0] === "workspace_membership" &&
        !fs.existsSync(happy.paths.collectorConfig) &&
        !fs.existsSync(happy.paths.database) &&
        !fs.existsSync(happy.paths.history[0]!) &&
        fs.readdirSync(snapshotsRoot).length === 0,
      purged,
    );
  } finally {
    happy.cleanup();
  }

  const recovery = fixture("rollback-required");
  try {
    const manager = new LifecycleManager(recovery.adapter);
    await manager.update({ operationId: "recovery-install", artifact: recovery.artifact("0.6.0") });
    const v2 = recovery.artifact("0.7.0");
    recovery.setMigrateOnActivate(true);
    recovery.setFailHealth(true);
    let restoreAttempts = 0;
    const restoreFailureAdapter = faultingAdapter(recovery.adapter, {
      async restore(snapshotId) {
        restoreAttempts += 1;
        if (restoreAttempts === 1) throw new Error("fixture restore unavailable");
        await recovery.adapter.restore(snapshotId);
      },
    });
    const blocked = await rejection(() => new LifecycleManager(restoreFailureAdapter).update({
      operationId: "recovery-update",
      artifact: v2,
    }));
    const blockedJournal = JSON.parse(fs.readFileSync(path.join(recovery.paths.lifecycleRoot, "journal.json"), "utf8")) as LifecycleJournal;
    const recoveryReceiptPath = path.join(recovery.paths.lifecycleRoot, "receipts", "recovery-update-update.json");
    const blockedReceipt = JSON.parse(fs.readFileSync(recoveryReceiptPath, "utf8")) as { status: string; restoredVersion: string | null };
    const lockReleasedAfterFailure = await recovery.adapter.acquireLock("recovery-lock-probe");
    if (lockReleasedAfterFailure) await recovery.adapter.releaseLock("recovery-lock-probe");
    check(
      "restore_failure_persists_rollback_required_and_releases_lock",
      blocked?.message === "rollback required: restore failed; retry the same operationId" &&
        blockedJournal.phase === "rollback_required" &&
        blockedReceipt.status === "rollback_required" &&
        blockedReceipt.restoredVersion === null &&
        recovery.runtimeVersion === "0.7.0" && lockReleasedAfterFailure,
      { message: blocked?.message, journal: blockedJournal, receipt: blockedReceipt, runtime: recovery.runtimeVersion },
    );

    // Make the target healthy before reopen. A broken implementation could
    // now accept v2; the journal must instead force the pending rollback.
    recovery.setFailHealth(false);
    const recovered = await manager.update({ operationId: "recovery-update", artifact: v2 });
    check(
      "reopen_retries_required_rollback_and_never_commits_target",
      recovered.status === "rolled_back" &&
        recovered.restoredVersion === "0.6.0" &&
        recovery.runtimeVersion === "0.6.0" &&
        !fs.existsSync(path.join(recovery.paths.lifecycleRoot, "journal.json")),
      recovered,
    );
    const completedRaces = await Promise.all([
      rejection(() => manager.update({ operationId: "recovery-update", artifact: v2 })),
      rejection(() => manager.update({ operationId: "recovery-update", artifact: v2 })),
    ]);
    const finalRecoveryReceipt = JSON.parse(fs.readFileSync(recoveryReceiptPath, "utf8")) as { status: string; restoredVersion: string | null };
    check(
      "completed_recovery_is_idempotent_under_reuse_race_without_false_restore",
      completedRaces.every((error) => error !== null) &&
        recovery.runtimeVersion === "0.6.0" &&
        finalRecoveryReceipt.status === "rolled_back" &&
        finalRecoveryReceipt.restoredVersion === "0.6.0",
      { messages: completedRaces.map((error) => error?.message), receipt: finalRecoveryReceipt },
    );
  } finally {
    recovery.cleanup();
  }

  const deadline = fixture("readiness-deadline");
  try {
    const manager = new LifecycleManager(deadline.adapter);
    await manager.update({ operationId: "deadline-install", artifact: deadline.artifact("0.6.0") });
    const beforeConfig = fs.readFileSync(deadline.paths.collectorConfig, "utf8");
    const beforeDatabase = fs.readFileSync(deadline.paths.database, "utf8");
    deadline.setMigrateOnActivate(true);
    let aborts = 0;
    const neverReadyAdapter = faultingAdapter(deadline.adapter, {
      async readiness(_expectedVersion, input) {
        return new Promise<LifecycleReadiness>(() => {
          input.signal.addEventListener("abort", () => { aborts += 1; }, { once: true });
        });
      },
    });
    const startedAt = Date.now();
    const timeout = await rejection(() => new LifecycleManager(neverReadyAdapter, { readinessTimeoutMs: 20 }).update({
      operationId: "deadline-update",
      artifact: deadline.artifact("0.7.0"),
    }));
    const elapsedMs = Date.now() - startedAt;
    const timeoutReceipt = JSON.parse(fs.readFileSync(
      path.join(deadline.paths.lifecycleRoot, "receipts", "deadline-update-update.json"),
      "utf8",
    )) as { status: string; restoredVersion: string | null };
    const lockReleasedAfterTimeout = await deadline.adapter.acquireLock("deadline-lock-probe");
    if (lockReleasedAfterTimeout) await deadline.adapter.releaseLock("deadline-lock-probe");
    check(
      "readiness_deadline_aborts_rolls_back_and_releases_lock",
      timeout?.message === "readiness deadline exceeded" &&
        elapsedMs < 500 && aborts === 1 &&
        timeoutReceipt.status === "rolled_back" && timeoutReceipt.restoredVersion === "0.6.0" &&
        deadline.runtimeVersion === "0.6.0" &&
        fs.readFileSync(deadline.paths.collectorConfig, "utf8") === beforeConfig &&
        fs.readFileSync(deadline.paths.database, "utf8") === beforeDatabase &&
        !fs.existsSync(path.join(deadline.paths.lifecycleRoot, "journal.json")) &&
        lockReleasedAfterTimeout,
      { message: timeout?.message, elapsedMs, aborts, receipt: timeoutReceipt, runtime: deadline.runtimeVersion },
    );
  } finally {
    deadline.cleanup();
  }

  const race = fixture("race");
  try {
    const [winner, loser] = await Promise.all([
      race.adapter.acquireLock("race-a"),
      race.adapter.acquireLock("race-b"),
    ]);
    check("exclusive_lock_allows_exactly_one_concurrent_owner", Number(winner) + Number(loser) === 1, { winner, loser });
    await race.adapter.releaseLock(winner ? "race-a" : "race-b");
  } finally {
    race.cleanup();
  }

  const reopen = fixture("reopen-snapshot");
  try {
    const artifact = reopen.artifact("0.6.0");
    const incompleteSnapshot = path.join(reopen.paths.lifecycleRoot, "snapshots", "resume-snapshot");
    fs.mkdirSync(incompleteSnapshot, { recursive: true, mode: 0o700 });
    write(path.join(incompleteSnapshot, "partial"), "incomplete\n");
    const staleStage = path.join(reopen.paths.lifecycleRoot, `${immutableRuntimeRelativePath(artifact)}.staging`);
    write(staleStage, "partial-runtime\n", 0o700);
    const receipt = await new LifecycleManager(reopen.adapter).update({ operationId: "resume-snapshot", artifact });
    check("incomplete_snapshot_and_stage_reopen_idempotently", receipt.status === "completed" && !fs.existsSync(path.join(incompleteSnapshot, "partial")) && !fs.existsSync(staleStage) && reopen.runtimeVersion === "0.6.0", receipt);
  } finally {
    reopen.cleanup();
  }

  const lifecycleLink = fixture("lifecycle-root-symlink");
  try {
    const outside = path.join(lifecycleLink.ownershipRoot, "outside-lifecycle");
    fs.mkdirSync(outside, { mode: 0o700 });
    fs.symlinkSync(outside, lifecycleLink.paths.lifecycleRoot, "dir");
    const error = await rejection(() => new LifecycleManager(lifecycleLink.adapter).update({
      operationId: "lifecycle-link",
      artifact: lifecycleLink.artifact("0.6.0"),
    }));
    check(
      "preexisting_lifecycle_root_symlink_is_rejected_before_managed_write",
      error?.message.includes("symlink") === true && fs.readdirSync(outside).length === 0 && lifecycleLink.runtimeVersion === null,
      error?.message,
    );
  } finally {
    lifecycleLink.cleanup();
  }

  const snapshotsLink = fixture("snapshots-root-symlink");
  try {
    const outside = path.join(snapshotsLink.ownershipRoot, "outside-snapshots");
    fs.mkdirSync(outside, { mode: 0o700 });
    fs.mkdirSync(snapshotsLink.paths.lifecycleRoot, { recursive: true, mode: 0o700 });
    fs.symlinkSync(outside, path.join(snapshotsLink.paths.lifecycleRoot, "snapshots"), "dir");
    const error = await rejection(() => new LifecycleManager(snapshotsLink.adapter).update({
      operationId: "snapshots-link",
      artifact: snapshotsLink.artifact("0.6.0"),
    }));
    check(
      "preexisting_snapshots_root_symlink_is_rejected_before_snapshot_write",
      error?.message.includes("symlink") === true && fs.readdirSync(outside).length === 0 && snapshotsLink.runtimeVersion === null,
      error?.message,
    );
  } finally {
    snapshotsLink.cleanup();
  }

  const ancestorSwap = fixture("snapshot-ancestor-swap");
  try {
    const snapshotsRoot = path.join(ancestorSwap.paths.lifecycleRoot, "snapshots");
    const outside = path.join(ancestorSwap.ownershipRoot, "outside-ancestor-swap");
    fs.mkdirSync(outside, { mode: 0o700 });
    let swapped = false;
    const swapAdapter = faultingAdapter(ancestorSwap.adapter, {
      async writeJournal(journal) {
        await ancestorSwap.adapter.writeJournal(journal);
        if (!swapped && journal.phase === "prepared") {
          swapped = true;
          fs.rmSync(snapshotsRoot, { recursive: true, force: true });
          fs.symlinkSync(outside, snapshotsRoot, "dir");
        }
      },
    });
    const error = await rejection(() => new LifecycleManager(swapAdapter).update({
      operationId: "ancestor-swap",
      artifact: ancestorSwap.artifact("0.6.0"),
    }));
    check(
      "snapshot_ancestor_swap_after_prepare_is_rejected_before_snapshot_write",
      error?.message.includes("symlink") === true && swapped && fs.readdirSync(outside).length === 0 && ancestorSwap.runtimeVersion === null,
      error?.message,
    );
  } finally {
    ancestorSwap.cleanup();
  }

  const leafSwap = fixture("snapshot-leaf-swap");
  try {
    const snapshotsRoot = path.join(leafSwap.paths.lifecycleRoot, "snapshots");
    const outside = path.join(leafSwap.ownershipRoot, "outside-leaf-swap");
    fs.mkdirSync(outside, { mode: 0o700 });
    let swapped = false;
    const swapAdapter = faultingAdapter(leafSwap.adapter, {
      async writeJournal(journal) {
        await leafSwap.adapter.writeJournal(journal);
        if (!swapped && journal.phase === "prepared") {
          swapped = true;
          fs.symlinkSync(outside, path.join(snapshotsRoot, journal.operationId), "dir");
        }
      },
    });
    const error = await rejection(() => new LifecycleManager(swapAdapter).update({
      operationId: "leaf-swap",
      artifact: leafSwap.artifact("0.6.0"),
    }));
    check(
      "snapshot_leaf_swap_after_prepare_is_rejected_before_snapshot_write",
      error?.message.includes("symlink") === true && swapped && fs.readdirSync(outside).length === 0 && leafSwap.runtimeVersion === null,
      error?.message,
    );
  } finally {
    leafSwap.cleanup();
  }

  const hostile = fixture("hostile");
  try {
    const symlinkSource = path.join(hostile.ownershipRoot, "artifacts", "symlink-runtime");
    const realSource = path.join(hostile.ownershipRoot, "artifacts", "real-runtime");
    write(realSource, "runtime\n", 0o700);
    fs.symlinkSync(realSource, symlinkSource);
    const symlinkArtifact: RuntimeArtifact = { version: "symlink", platform: "darwin", architecture: "arm64", nodeMajor: 22, sha256: digest(realSource), sourcePath: symlinkSource };
    const symlinkError = await rejection(() => new LifecycleManager(hostile.adapter).update({ operationId: "symlink-source", artifact: symlinkArtifact }));
    check("symlink_artifact_is_rejected_and_not_switched", symlinkError?.message.includes("symlink") === true && hostile.runtimeVersion === null, symlinkError?.message);

    const privateHealthReason = "/Users/private/credential-sentinel";
    const hostileHealthAdapter = faultingAdapter(hostile.adapter, {
      async readiness() {
        return {
          ready: true,
          runtimeVersion: "0.7.0",
          serviceReady: true,
          configCompatible: true,
          databaseCompatible: true,
          reason: privateHealthReason,
        } as unknown as LifecycleReadiness;
      },
    });
    const hostileHealth = await rejection(() => new LifecycleManager(hostileHealthAdapter).update({ operationId: "hostile-health", artifact: hostile.artifact("0.7.0") }));
    check("untrusted_health_reason_fails_closed_without_leaking", hostileHealth?.message === "readiness failed: service_unready" && !hostileHealth.message.includes(privateHealthReason), hostileHealth?.message);

    fs.mkdirSync(hostile.paths.lifecycleRoot, { recursive: true, mode: 0o700 });
    write(path.join(hostile.paths.lifecycleRoot, "journal.json"), "{malformed", 0o600);
    const malformed = await rejection(() => new LifecycleManager(hostile.adapter).update({ operationId: "malformed-journal", artifact: hostile.artifact("0.7.0") }));
    check("malformed_journal_fails_closed_without_switch", malformed?.message.includes("malformed") === true && hostile.runtimeVersion === null, malformed?.message);

    const traversal = await rejection(() => new FilesystemLifecycleAdapter({ ...hostile.paths, lifecycleRoot: path.join(hostile.ownershipRoot, "..", "escape") }, hostile.service, hostile.database));
    check("ownership_path_escape_is_rejected", traversal?.message.includes("child of the ownership root") === true, traversal?.message);
  } finally {
    hostile.cleanup();
  }

  const support = fixture("support");
  try {
    const manager = new LifecycleManager(support.adapter);
    await manager.update({ operationId: "support-install", artifact: support.artifact("0.6.0") });
    const output = await manager.supportBundle("support-bundle");
    const serialized = JSON.stringify(output);
    const privateSentinels = [
      "/Users/", "prompt-sentinel", "credential-sentinel", "row-path-sentinel",
      "nested-secret-sentinel", "case-secret-sentinel", "unicode-secret-sentinel",
      "prototype-secret-sentinel", "getter-secret-sentinel", "outer-getter-secret-sentinel",
      "counter-content-sentinel", "readiness-content-sentinel", support.ownershipRoot,
    ];
    const roundTrip = JSON.parse(serialized) as typeof output;
    const exactBundleKeys = Object.keys(roundTrip.bundle).sort().join(",") ===
      ["architecture", "boundedLogs", "counters", "installedVersion", "nodeMajor", "platform", "readiness", "runtimeVersion"].sort().join(",");
    const exactReadinessKeys = Object.keys(roundTrip.bundle.readiness).sort().join(",") ===
      ["configCompatible", "databaseCompatible", "ready", "reason", "runtimeVersion", "serviceReady"].sort().join(",");
    const exactCounterKeys = Object.keys(roundTrip.bundle.counters).sort().join(",") ===
      ["activeDelivery", "deadDelivery", "maintenancePending", "tokenAttributedEvents"].sort().join(",");
    const exactLogKeys = roundTrip.bundle.boundedLogs.every((row) =>
      Object.keys(row).sort().join(",") === ["code", "count", "severity", "source"].sort().join(","));
    const receiptPath = path.join(support.paths.lifecycleRoot, "receipts", "support-bundle-support_bundle.json");
    const persistedReceipt = fs.readFileSync(receiptPath, "utf8");
    const receiptRoundTrip = JSON.parse(persistedReceipt) as Record<string, unknown>;
    const exactReceiptKeys = Object.keys(receiptRoundTrip).sort().join(",") ===
      ["fromVersion", "health", "operation", "operationId", "ownedTargets", "preserved", "purgeOnlyTargets", "restoredVersion", "retainedTargets", "schemaVersion", "status", "toVersion", "toolVersion"].sort().join(",");
    const exactReceiptHealthKeys = Boolean(receiptRoundTrip.health) &&
      Object.keys(receiptRoundTrip.health as object).sort().join(",") ===
        ["configCompatible", "databaseCompatible", "ready", "reason", "runtimeVersion", "serviceReady"].sort().join(",");
    check(
      "support_bundle_rebuilds_every_object_from_recursive_exact_allowlists",
      roundTrip.bundle.boundedLogs.length === 1 &&
        roundTrip.bundle.boundedLogs[0]?.code === "health.retry" &&
        exactBundleKeys && exactReadinessKeys && exactCounterKeys && exactLogKeys &&
        support.supportGetterAccesses === 0,
      { bundle: roundTrip.bundle, getterAccesses: support.supportGetterAccesses },
    );
    check(
      "support_json_roundtrip_and_persisted_receipt_exclude_all_private_aliases",
      exactReceiptKeys && exactReceiptHealthKeys &&
        privateSentinels.every((sentinel) => !serialized.includes(sentinel) && !persistedReceipt.includes(sentinel)),
      { exactReceiptKeys, exactReceiptHealthKeys, serializedBytes: Buffer.byteLength(serialized), receiptBytes: Buffer.byteLength(persistedReceipt) },
    );
    for (let index = 0; index < 40; index += 1) await manager.supportBundle(`bounded-${String(index).padStart(2, "0")}`);
    check("lifecycle_receipts_are_bounded_to_32", fs.readdirSync(path.join(support.paths.lifecycleRoot, "receipts")).filter((file) => file.endsWith(".json")).length === 32, fs.readdirSync(path.join(support.paths.lifecycleRoot, "receipts")).length);
    check("support_unknown_getters_are_never_invoked_across_repeated_bundles", support.supportGetterAccesses === 0, support.supportGetterAccesses);
    const sanitized = sanitizeSupportSnapshot({
      installedVersion: "0.6.0",
      runtimeVersion: "0.6.0",
      platform: "darwin",
      architecture: "arm64",
      nodeMajor: 22,
      readiness: { ready: true, runtimeVersion: "0.6.0", serviceReady: true, configCompatible: true, databaseCompatible: true, reason: "ready" },
      counters: { activeDelivery: -1, deadDelivery: 0, tokenAttributedEvents: 0, maintenancePending: 0 },
      boundedLogs: [{ source: "lifecycle", severity: "warn", code: "ok", count: Number.MAX_SAFE_INTEGER }],
    });
    check("support_counts_are_nonnegative_and_bounded", sanitized.counters.activeDelivery === 0 && sanitized.boundedLogs.at(-1)?.count === 1_000_000, sanitized);
  } finally {
    support.cleanup();
  }

  const boundary = lifecycleBoundaryStatement();
  check("leave_and_revoke_are_distinct_unperformed_operations", boundary.leave === "distinct_operation_not_performed" && boundary.revoke === "hosted_owner_operation_not_performed" && boundary.credentialsMoved === false && boundary.liveServiceTouched === false, boundary);

  const failed = checks.filter((row) => !row.passed);
  console.log(JSON.stringify({ proof: "lifecycle", checks: checks.length, passed: checks.length - failed.length, failed: failed.map((row) => row.name), liveStateTouched: false }));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
