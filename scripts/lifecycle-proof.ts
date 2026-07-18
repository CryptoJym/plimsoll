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
      return {
        installedVersion: runtimeVersion,
        runtimeVersion,
        platform: "darwin",
        architecture: "arm64",
        nodeMajor: 22,
        readiness: await this.readiness(runtimeVersion ?? "missing"),
        counters: {
          activeDelivery: 7,
          deadDelivery: 2,
          tokenAttributedEvents: 99,
          maintenancePending: 1,
        },
        boundedLogs: [
          { source: "lifecycle", severity: "warn", code: "health.retry", count: 2 },
          { source: "collector_stderr", severity: "error", code: "/Users/private/secret", count: 1 },
        ],
        absolutePath: "/Users/private/secret",
        content: "prompt-sentinel",
        credential: "credential-sentinel",
      } as LifecycleSupportSnapshot;
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
    check("state_and_receipts_permissions_are_0600", mode(statePath) === 0o600 && fs.readdirSync(receiptsRoot).every((file) => mode(path.join(receiptsRoot, file)) === 0o600), fs.readdirSync(receiptsRoot));

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

    const previewDigest = createHash("sha256").update(fs.readFileSync(happy.paths.database)).update(fs.readFileSync(happy.paths.history[0]!)).digest("hex");
    const preview = await runLifecycleCommand({
      argv: ["uninstall", "--operation-id", "uninstall-preview"],
      adapter: happy.adapter,
      resolveArtifact: async () => { throw new Error("not used"); },
    });
    check("uninstall_command_defaults_to_preview", preview.receipt.status === "preview" && fs.existsSync(runtimePath) && fs.existsSync(happy.paths.serviceManifest), preview);

    const applied = await manager.uninstall({ operationId: "uninstall-apply", apply: true });
    const preservedDigest = createHash("sha256").update(fs.readFileSync(happy.paths.database)).update(fs.readFileSync(happy.paths.history[0]!)).digest("hex");
    check("uninstall_apply_removes_only_owned_runtime_service_fragments", applied.status === "completed" && !fs.existsSync(happy.paths.serviceManifest) && happy.paths.ownedToolFragments.every((file) => !fs.existsSync(file)) && !fs.existsSync(path.join(happy.paths.lifecycleRoot, "current")), applied);
    check("uninstall_preserves_ledger_history_credentials_and_membership", previewDigest === preservedDigest && fs.readFileSync(happy.paths.collectorConfig, "utf8").includes("credential-sentinel") && applied.preserved.includes("workspace_membership"), applied.preserved);

    const badPurge = await rejection(() => manager.purge({ operationId: "purge-bad", confirmation: "yes" }));
    check("purge_rejects_non_exact_confirmation", badPurge?.message.includes("exact confirmation") === true && fs.existsSync(happy.paths.database), badPurge?.message);
    const purged = await manager.purge({ operationId: "purge-exact", confirmation: PURGE_CONFIRMATION });
    check("purge_is_separate_and_exact_then_removes_owned_data", purged.status === "purged" && !fs.existsSync(happy.paths.collectorConfig) && !fs.existsSync(happy.paths.database) && !fs.existsSync(happy.paths.history[0]!), purged);
  } finally {
    happy.cleanup();
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
    check("support_bundle_is_allowlisted_and_excludes_paths_content_credentials", output.bundle.boundedLogs.length === 1 && !serialized.includes("/Users/") && !serialized.includes("prompt-sentinel") && !serialized.includes("credential-sentinel") && !serialized.includes(support.ownershipRoot), output.bundle);
    for (let index = 0; index < 40; index += 1) await manager.supportBundle(`bounded-${String(index).padStart(2, "0")}`);
    check("lifecycle_receipts_are_bounded_to_32", fs.readdirSync(path.join(support.paths.lifecycleRoot, "receipts")).filter((file) => file.endsWith(".json")).length === 32, fs.readdirSync(path.join(support.paths.lifecycleRoot, "receipts")).length);
    const raw = await support.service.supportSnapshot();
    const sanitized = sanitizeSupportSnapshot({ ...raw, counters: { ...raw.counters, activeDelivery: -1 }, boundedLogs: [...raw.boundedLogs, { source: "lifecycle", severity: "warn", code: "ok", count: Number.MAX_SAFE_INTEGER }] });
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
