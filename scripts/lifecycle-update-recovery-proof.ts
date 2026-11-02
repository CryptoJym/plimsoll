import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import { createRequire } from "node:module";
import path from "node:path";
import { Worker } from "node:worker_threads";

import Database from "better-sqlite3";

import { collectorConfigSchema, collectorSecretConfigSchema } from "../packages/collector-cli/src/config";
import { SqliteLedgerSnapshotAdapter } from "../packages/collector-cli/src/lifecycle-adapters";
import { LifecycleMutationAuthority } from "../packages/collector-cli/src/lifecycle-authority";
import { lifecycleCommandExitCode, lifecycleSnapshotDatabaseMaxBytes, runLifecycleCommand } from "../packages/collector-cli/src/lifecycle-command";
import { FilesystemLifecycleAdapter, type LifecycleServiceAdapter } from "../packages/collector-cli/src/lifecycle-filesystem";
import { LifecycleInterruption, LifecycleManager, parseCompletionReceipt, type LifecyclePhase, type LifecycleReceipt, type RuntimeArtifact } from "../packages/collector-cli/src/lifecycle";
import { createProofCompletion } from "./lib/proof-completion";

const childMode = process.argv.includes("--emit-interruption");
const completion = childMode ? null : createProofCompletion("lifecycle-update-recovery", 16);
const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "lifecycle-update-recovery-")));
const digest = (file: string) => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
const checks: Array<{ name: string; passed: boolean; detail?: unknown }> = [];
async function check(name: string, run: () => unknown | Promise<unknown>) {
  try { const detail = await run(); checks.push({ name, passed: true, detail }); completion!.check(name); }
  catch (error) { checks.push({ name, passed: false, detail: String(error) }); completion!.check(name, false); }
}

function fixture(name: string, maxBytes?: number, slowCheckpointMs = 0) {
  const home = path.join(root, name);
  fs.mkdirSync(home, { recursive: true, mode: 0o700 });
  const paths = {
    ownershipRoot: home, lifecycleRoot: path.join(home, "lifecycle"), artifactSourceRoot: path.join(home, "artifacts"),
    collectorConfig: path.join(home, "config.json"), database: path.join(home, "ledger.sqlite"),
    serviceManifest: path.join(home, "service.plist"), ownedToolFragments: [], history: [], statusSummary: path.join(home, "status.json"),
  };
  fs.mkdirSync(paths.artifactSourceRoot);
  fs.writeFileSync(paths.collectorConfig, JSON.stringify({ port: 48271, installKey: "synthetic-install-sentinel", uploadSigningSecret: "synthetic-signing-sentinel" }));
  fs.writeFileSync(paths.serviceManifest, "synthetic-service");
  const db = new Database(paths.database);
  db.pragma("journal_mode = WAL"); db.exec("CREATE TABLE proof_rows(id INTEGER PRIMARY KEY); INSERT INTO proof_rows VALUES(1)"); db.close();
  let activeVersion: string | null = null;
  let readinessHook: (() => void) | undefined;
  const service: LifecycleServiceAdapter = {
    async activate({ version }) { activeVersion = version; },
    async restore({ version }) { activeVersion = version; }, async remove() {},
    async readiness(expectedVersion) {
      readinessHook?.();
      return { ready: true, runtimeVersion: expectedVersion, serviceReady: true, configCompatible: true, databaseCompatible: true, reason: "ready" };
    }, async supportSnapshot() { throw new Error("unused"); },
  };
  const ttlMs = slowCheckpointMs ? 1500 : 30_000;
  let checkpointStarts = 0;
  const database = new SqliteLedgerSnapshotAdapter({ openHandles: () => [],
    ...(slowCheckpointMs ? { quiesceWorker: (source, options) => {
      checkpointStarts++;
      return new Worker(source.replace('const [checkpoint] =', `Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ${slowCheckpointMs}); const [checkpoint] =`), options);
    } } : {}),
  });
  const authority = new LifecycleMutationAuthority(path.join(home, "authority"), { defaultLeaseMs: ttlMs });
  const adapter = new FilesystemLifecycleAdapter(paths, service, database, authority, { snapshotDatabaseMaxBytes: maxBytes });
  const artifact = (version: string): RuntimeArtifact => {
    const sourcePath = path.join(paths.artifactSourceRoot, version + ".mjs");
    fs.writeFileSync(sourcePath, `// synthetic runtime ${version}\n`);
    return { version, sourcePath, sha256: `sha256:${digest(sourcePath)}`, platform: "darwin", architecture: process.arch as "arm64" | "x64", nodeMajor: 22 };
  };
  return { paths, adapter, artifact, ttlMs, authorityRoot: path.join(home, "authority"), get checkpointStarts() { return checkpointStarts; },
    get activeVersion() { return activeVersion; }, setReadiness(hook: () => void) { readinessHook = hook; } };
}

async function interrupted(phase: LifecyclePhase, processBoundary = false) {
  const f = fixture(`interrupted-${phase}`);
  const artifact = f.artifact("1.0.0");
  const original = f.adapter.writeJournal.bind(f.adapter);
  f.adapter.writeJournal = async journal => {
    await original(journal);
    if (journal.phase === phase) throw new LifecycleInterruption("synthetic fence superseded");
  };
  const argv = ["update", "--operation-id", `resume-${phase}`, "--artifact", artifact.sourcePath, "--artifact-version", artifact.version, "--retention", "keep-all"];
  const result = await runLifecycleCommand({ argv, adapter: f.adapter, resolveArtifact: async () => artifact });
  if (processBoundary) {
    console.log(JSON.stringify(result)); process.exitCode = lifecycleCommandExitCode(result); return;
  }
  assert.equal(result.receipt.status, "interrupted"); assert.equal(result.receipt.phase, phase);
  assert.equal(result.receipt.operationId, `resume-${phase}`);
  assert.match(result.receipt.resumeCommand!, new RegExp(`resume-${phase}`));
  assert.match(result.receipt.resumeCommand!, /--artifact-version/); assert.match(result.receipt.resumeCommand!, /keep-all/);
  const inventory = await f.adapter.inspectSnapshots({ keep: 2 });
  assert.equal(inventory.snapshots[0]!.reason, "unfinished_operation");
  assert.equal(await f.adapter.operationIdExists(`resume-${phase}`), false);
  const persisted = fs.readdirSync(path.join(f.paths.lifecycleRoot, "receipts")).filter(name => name.includes("-interrupted-"));
  assert.equal(persisted.length, 1);
  assert.equal(JSON.parse(fs.readFileSync(path.join(f.paths.lifecycleRoot, "receipts", persisted[0]!), "utf8")).status, "interrupted");
  f.adapter.writeJournal = original;
  f.adapter.snapshot = async () => { throw new Error("resume must not snapshot again"); };
  f.adapter.stage = async () => { throw new Error("resume must not stage again"); };
  f.adapter.switchTo = async () => { throw new Error("resume must not switch again"); };
  const resumed = await runLifecycleCommand({ argv, adapter: f.adapter, resolveArtifact: async () => artifact });
  assert.equal(resumed.receipt.status, "completed"); assert.equal(await f.adapter.readJournal(), null);
  assert.equal(resumed.receipt.completionSequence, 1);
  return { phase, interruptionReceipts: persisted.length, completionSequence: resumed.receipt.completionSequence };
}

async function main() {
  await check("checkpoint_longer_than_fence_ttl_completes_update", async () => {
    const f = fixture("slow-checkpoint", undefined, 4000);
    const start = Date.now();
    const receipt = await new LifecycleManager(f.adapter).update({ operationId: "slow-update", artifact: f.artifact("1.0.0") });
    const elapsedMs = Date.now() - start;
    assert.equal(receipt.status, "completed"); assert.equal(receipt.snapshot?.method, "clone");
    assert.equal(f.checkpointStarts, 1); assert(elapsedMs > f.ttlMs * 2); assert.equal(await f.adapter.readJournal(), null);
    return { ttlMs: f.ttlMs, injectedCheckpointMs: 4000, elapsedMs, snapshot: receipt.snapshot };
  });
  await check("lost_fence_during_checkpoint_never_clones_or_switches_and_keeps_unfinished_snapshot", async () => {
    const f = fixture("superseded-checkpoint", undefined, 3000);
    let switched = false;
    f.adapter.switchTo = async () => { switched = true; };
    const update = new LifecycleManager(f.adapter).update({ operationId: "superseded", artifact: f.artifact("1.0.0") });
    const timer = setTimeout(() => {
      const leases = path.join(f.authorityRoot, "leases");
      const file = path.join(leases, fs.readdirSync(leases).find(name => name.endsWith(".json"))!);
      const lease = JSON.parse(fs.readFileSync(file, "utf8"));
      fs.writeFileSync(file, JSON.stringify({ ...lease, expiresAtMs: Date.now() - 1 }) + "\n");
    }, 750);
    try { await assert.rejects(update, LifecycleInterruption); } finally { clearTimeout(timer); }
    assert.equal(switched, false); assert.equal((await f.adapter.readJournal())!.phase, "prepared");
    const snapshot = path.join(f.paths.lifecycleRoot, "snapshots", "superseded");
    assert(fs.existsSync(snapshot)); assert(!fs.existsSync(path.join(snapshot, "database")));
    return { switched, journalPhase: "prepared", snapshotKept: true };
  });
  for (const phase of ["verified", "switched"] as const) await check(`interrupted_${phase}_has_receipt_and_resumes_without_repeating_mutations`, () => interrupted(phase));
  await check("interrupted_command_process_exits_one_with_json_stdout", () => {
    const result = spawnSync(process.execPath, ["--import", "tsx", path.resolve("scripts/lifecycle-update-recovery-proof.ts"), "--emit-interruption"], { encoding: "utf8", timeout: 30_000 });
    assert.equal(result.status, 1, result.stderr); const output = JSON.parse(result.stdout);
    assert.equal(output.receipt.status, "interrupted"); assert.equal(output.receipt.phase, "verified");
    assert.match(output.receipt.resumeCommand, /resume-verified/); assert(output.boundary);
    return { exitCode: result.status, stdoutBytes: result.stdout.length };
  });
  await check("snapshot_size_bound_skips_ledger_keeps_config_service_and_reports_inventory", async () => {
    const f = fixture("size-bound", 0);
    const bytes = fs.statSync(f.paths.database).size;
    const receipt = await new LifecycleManager(f.adapter).update({ operationId: "skip-ledger", artifact: f.artifact("1.0.0") });
    assert.equal(receipt.snapshot?.method, "skipped_by_operator"); assert.equal(receipt.snapshot?.databaseBytes, bytes);
    assert.match(receipt.snapshot!.cannotRestoreReason!, /skipped/);
    const directory = path.join(f.paths.lifecycleRoot, "snapshots", "skip-ledger");
    for (const file of ["config", "service"]) assert(fs.existsSync(path.join(directory, file)));
    assert(!fs.existsSync(path.join(directory, "database")));
    const inventory = await f.adapter.inspectSnapshots({ keep: 2 });
    assert.match(inventory.snapshots[0]!.cannotRestoreReason!, /skipped/);
    assert(parseCompletionReceipt(receipt, "skip-ledger"));
    return { measuredBytes: bytes, method: receipt.snapshot!.method, cannotRestoreReason: inventory.snapshots[0]!.cannotRestoreReason };
  });
  await check("skipped_snapshot_rollback_restores_runtime_config_service_and_preserves_live_ledger", async () => {
    const f = fixture("skip-rollback", 0);
    await new LifecycleManager(f.adapter).update({ operationId: "base", artifact: f.artifact("1.0.0") });
    const beforeConfig = digest(f.paths.collectorConfig); const beforeService = digest(f.paths.serviceManifest);
    f.setReadiness(() => {
      const db = new Database(f.paths.database); db.exec("INSERT INTO proof_rows VALUES(2)"); db.close();
      fs.writeFileSync(f.paths.collectorConfig, "changed-config"); fs.writeFileSync(f.paths.serviceManifest, "changed-service");
      throw new Error("synthetic readiness failure");
    });
    await assert.rejects(new LifecycleManager(f.adapter).update({ operationId: "failed", artifact: f.artifact("2.0.0") }), /synthetic readiness/);
    const receipt: LifecycleReceipt = JSON.parse(fs.readFileSync(path.join(f.paths.lifecycleRoot, "receipts", "failed-update.json"), "utf8"));
    assert.equal(receipt.restore?.method, "skipped_by_operator"); assert.equal(receipt.status, "rolled_back");
    assert.equal(f.activeVersion, "1.0.0"); assert.equal(digest(f.paths.collectorConfig), beforeConfig); assert.equal(digest(f.paths.serviceManifest), beforeService);
    const db = new Database(f.paths.database); const rows = db.prepare("SELECT count(*) AS n FROM proof_rows").get() as { n: number }; db.close();
    assert.equal(rows.n, 2); assert(parseCompletionReceipt(receipt, "failed"));
    return { ledgerRowsAfterRollback: rows.n, restore: receipt.restore };
  });
  await check("moved_database_snapshot_refuses_rollback_and_leaves_all_live_files_untouched", async () => {
    const f = fixture("moved-database"); const before = digest(f.paths.database);
    const configBefore = digest(f.paths.collectorConfig); const serviceBefore = digest(f.paths.serviceManifest);
    f.adapter.stage = async () => {
      const database = path.join(f.paths.lifecycleRoot, "snapshots", "missing", "database");
      fs.renameSync(database, path.join(root, "moved-database-archive.sqlite"));
      throw new Error("synthetic stage failure");
    };
    await assert.rejects(new LifecycleManager(f.adapter).update({ operationId: "missing", artifact: f.artifact("1.0.0") }), /move it back/);
    const receipt: LifecycleReceipt = JSON.parse(fs.readFileSync(path.join(f.paths.lifecycleRoot, "receipts", "missing-update.json"), "utf8"));
    assert.equal(receipt.restoreRefusal?.reason, "snapshot_database_missing"); assert.equal(receipt.status, "rollback_required");
    assert.equal(digest(f.paths.database), before); assert.equal(digest(f.paths.collectorConfig), configBefore); assert.equal(digest(f.paths.serviceManifest), serviceBefore);
    return { status: receipt.status, refusal: receipt.restoreRefusal?.reason };
  });
  await check("preflight_reports_clone_page_cost_and_honors_size_bound", async () => {
    const f = fixture("preflight", 0); const manager = new LifecycleManager(f.adapter);
    const plan = await manager.preflightUpdate();
    assert.equal(plan.method, "skipped_by_operator"); assert.equal(plan.requiredFreeBytes, 0);
    assert.match(plan.cloneCost!, /every page rewritten afterward costs new space/); assert.match(plan.cannotRestoreReason!, /skipped/);
    assert(!fs.existsSync(f.paths.lifecycleRoot)); return { method: plan.method, requiredFreeBytes: plan.requiredFreeBytes };
  });
  await check("capture_redaction_uses_schema_fields_and_receipt_never_contains_values", async () => {
    const f = fixture("captures"); fs.mkdirSync(f.paths.lifecycleRoot);
    const capture = path.join(f.paths.lifecycleRoot, "capture-config-before-synthetic.json");
    const secretFields = Object.keys(collectorSecretConfigSchema.shape);
    const values = Object.fromEntries(secretFields.map(key => [key, `synthetic-secret-${key}`]));
    fs.writeFileSync(capture, JSON.stringify({ ...values, nested: [values], port: 48271 }));
    const result = await runLifecycleCommand({ argv: ["captures", "redact", "--operation-id", "redact-synthetic"], adapter: f.adapter, resolveArtifact: async () => { throw new Error("unused"); } });
    assert.equal(result.receipt.status, "completed");
    const redacted = fs.readFileSync(capture, "utf8"); const receipt = JSON.stringify(result);
    for (const value of Object.values(values)) { assert(!redacted.includes(value)); assert(!receipt.includes(value)); }
    assert.deepEqual(result.receipt.captureRedaction, [{ file: path.basename(capture), fields: secretFields.sort() }]);
    assert.equal(fs.statSync(capture).mode & 0o777, 0o600);
    assert.equal(JSON.parse(redacted).port, 48271);
    return { files: 1, fields: secretFields };
  });
  await check("capture_redaction_refuses_symlinks_without_touching_target", async () => {
    const f = fixture("capture-symlink"); fs.mkdirSync(f.paths.lifecycleRoot);
    const target = path.join(root, "synthetic-target.json"); fs.writeFileSync(target, '{"installKey":"synthetic"}'); const before = digest(target);
    fs.symlinkSync(target, path.join(f.paths.lifecycleRoot, "capture-config-before-link.json"));
    await assert.rejects(f.adapter.redactCaptures("redact-link"), /symlink/); assert.equal(digest(target), before);
  });
  await check("capture_parse_failure_never_echoes_secret_values", async () => {
    const f = fixture("capture-malformed"); fs.mkdirSync(f.paths.lifecycleRoot);
    const sentinel = "synthetic-malformed-secret";
    fs.writeFileSync(path.join(f.paths.lifecycleRoot, "capture-config-before-malformed.json"), '{"installKey":"' + sentinel);
    try { await f.adapter.redactCaptures("redact-malformed"); assert.fail("must refuse"); }
    catch (error) { assert.match(String(error), /capture JSON is invalid/); assert(!String(error).includes(sentinel)); }
  });
  await check("size_bound_config_default_and_strict_cli_values", () => {
    assert.equal(collectorConfigSchema.parse({}).snapshotDatabaseMaxBytes, undefined);
    assert.equal(collectorConfigSchema.parse({ snapshotDatabaseMaxBytes: 0 }).snapshotDatabaseMaxBytes, 0);
    assert.equal(lifecycleSnapshotDatabaseMaxBytes([], 4096), 4096);
    assert.equal(lifecycleSnapshotDatabaseMaxBytes(["--snapshot-database-max-bytes", "0"], 4096), 0);
    for (const raw of ["-1", "1.5", "NaN", "9007199254740992", "--artifact"]) assert.throws(() => lifecycleSnapshotDatabaseMaxBytes(["--snapshot-database-max-bytes", raw]));
    assert.throws(() => lifecycleSnapshotDatabaseMaxBytes(["--snapshot-database-max-bytes", "1", "--snapshot-database-max-bytes", "2"]));
  });
  await check("actual_cli_preflight_and_capture_redaction_use_private_home_and_emit_json", () => {
    const collector = path.join(os.homedir(), "cli-lifecycle-fixture");
    fs.mkdirSync(path.join(collector, "lifecycle"), { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(collector, "collector.config.json"), JSON.stringify(collectorConfigSchema.parse({ snapshotDatabaseMaxBytes: 0 })), { mode: 0o600 });
    const db = new Database(path.join(collector, "work-ledger.sqlite")); db.pragma("journal_mode = WAL"); db.exec("CREATE TABLE proof_rows(id)"); db.close();
    const capture = path.join(collector, "lifecycle", "capture-config-before-cli.json");
    fs.writeFileSync(capture, '{"installKey":"synthetic-cli-key","uploadSigningSecret":"synthetic-cli-signing"}', { mode: 0o600 });
    const cli = (...args: string[]) => spawnSync(process.execPath, ["--import", "tsx", path.resolve("packages/collector-cli/src/cli.ts"), "lifecycle", ...args], {
      env: { ...process.env, PLIMSOLL_HOME: collector }, encoding: "utf8", timeout: 30_000,
    });
    const preflight = cli("update", "--preflight"); assert.equal(preflight.status, 0, preflight.stderr);
    assert.equal(JSON.parse(preflight.stdout).preflight.method, "skipped_by_operator");
    assert.match(JSON.parse(preflight.stdout).preflight.cloneCost, /shares blocks/);
    const redaction = cli("captures", "redact", "--operation-id", "cli-redact");
    assert.equal(redaction.status, 0, redaction.stderr); assert.equal(JSON.parse(redaction.stdout).receipt.status, "completed");
    assert(!redaction.stdout.includes("synthetic-cli-key")); assert(!fs.readFileSync(capture, "utf8").includes("synthetic-cli-key"));
    const invalid = cli("update", "--preflight", "--snapshot-database-max-bytes", "-1"); assert.equal(invalid.status, 1);
    return { preflightExit: preflight.status, redactionExit: redaction.status, invalidBoundExit: invalid.status };
  });
  await check("packaged_cli_interruption_exits_one_and_its_exact_resume_command_completes", () => {
    const collector = path.join(os.homedir(), "packaged-cli-collector");
    const dist = path.join(os.homedir(), "packaged-cli-build");
    fs.mkdirSync(collector, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(collector, "collector.config.json"), JSON.stringify(collectorConfigSchema.parse({ snapshotDatabaseMaxBytes: 0 })), { mode: 0o600 });
    const db = new Database(path.join(collector, "work-ledger.sqlite")); db.pragma("journal_mode = WAL"); db.exec("CREATE TABLE proof_rows(id)"); db.close();
    const built = spawnSync(process.execPath, ["--import", "tsx", path.resolve("scripts/build-packaged-runtime.ts"), "--dist", dist], { encoding: "utf8", timeout: 30_000 });
    assert.equal(built.status, 0, built.stderr);
    // As with an installed package, provide its already checked native closure.
    const require = createRequire(import.meta.url);
    const sqliteManifest = require.resolve("better-sqlite3/package.json");
    const bindingsManifest = createRequire(sqliteManifest).resolve("bindings/package.json");
    const uriManifest = createRequire(bindingsManifest).resolve("file-uri-to-path/package.json");
    for (const [name, manifest] of [["better-sqlite3", sqliteManifest], ["bindings", bindingsManifest], ["file-uri-to-path", uriManifest]]) {
      fs.cpSync(path.dirname(manifest!), path.join(dist, "node_modules", name!), { recursive: true, dereference: true });
    }
    const fault = path.join(collector, "synthetic-interruption.mjs");
    // Exercise the real CLI without a production fault flag: one fixture-only
    // preload expires its own synthetic lease immediately after verified.
    fs.writeFileSync(fault, `import fs from "node:fs"; import path from "node:path";
      const collector = ${JSON.stringify(collector)};
      const journal = path.join(collector, "lifecycle", "journal.json");
      const marker = path.join(collector, "fault-used");
      const original = fs.renameSync;
      fs.renameSync = function(source, destination) {
        const result = original.call(this, source, destination);
        if (destination === journal && !fs.existsSync(marker) && JSON.parse(fs.readFileSync(journal, "utf8")).phase === "verified") {
          const leases = path.join(collector, "lifecycle-authority", "leases");
          const lease = path.join(leases, fs.readdirSync(leases).find(name => name.endsWith(".json")));
          const value = JSON.parse(fs.readFileSync(lease, "utf8"));
          fs.writeFileSync(marker, "used"); fs.writeFileSync(lease, JSON.stringify({ ...value, expiresAtMs: Date.now() - 1 }) + "\\n");
        }
        return result;
      };`);
    const env = { ...process.env, PLIMSOLL_HOME: collector };
    const first = spawnSync(process.execPath, ["--import", fault, path.join(dist, "cli.mjs"), "lifecycle", "update", "--operation-id", "packaged-resume", "--artifact", "self", "--retention", "keep-all"], { env, encoding: "utf8", timeout: 30_000 });
    assert.equal(first.status, 1, first.stderr);
    assert(first.stdout.trim(), `empty interruption stdout: ${first.stderr}`);
    const interrupted = JSON.parse(first.stdout); assert.equal(interrupted.receipt.status, "interrupted");
    assert.equal(interrupted.receipt.phase, "verified"); assert.equal(interrupted.receipt.operationId, "packaged-resume");
    const resumed = spawnSync("/bin/sh", ["-c", interrupted.receipt.resumeCommand], { env, encoding: "utf8", timeout: 30_000 });
    assert.equal(resumed.status, 0, resumed.stderr); assert(resumed.stdout.trim(), `empty resume stdout: ${resumed.stderr}`); assert.equal(JSON.parse(resumed.stdout).receipt.status, "completed");
    return { interruptionExit: first.status, resumeExit: resumed.status, phase: interrupted.receipt.phase, stdoutBytes: first.stdout.length };
  });
  await check("unbounded_snapshot_still_restores_the_ledger", async () => {
    const f = fixture("unbounded");
    const receipt = await new LifecycleManager(f.adapter).update({ operationId: "unbounded", artifact: f.artifact("1.0.0") });
    assert.equal(receipt.snapshot?.method, "clone"); assert(!receipt.snapshot?.cannotRestoreReason);
    assert(fs.existsSync(path.join(f.paths.lifecycleRoot, "snapshots", "unbounded", "database")));
    const db = new Database(f.paths.database); db.exec("INSERT INTO proof_rows VALUES(2)"); db.close();
    assert(await f.adapter.acquireLock("restore-default"));
    let restore;
    try { restore = await f.adapter.restore("unbounded", "restore-default"); }
    finally { await f.adapter.releaseLock("restore-default"); }
    const restored = new Database(f.paths.database); const rows = (restored.prepare("SELECT count(*) AS n FROM proof_rows").get() as { n: number }).n; restored.close();
    assert.equal(rows, 1);
    return { method: receipt.snapshot!.method, databaseBytes: receipt.snapshot!.databaseBytes, restoredRows: rows, restoreMethod: restore?.method };
  });
  console.log(JSON.stringify({ checks, passed: checks.filter(c => c.passed).length, failed: checks.filter(c => !c.passed).length }, null, 2));
  completion!.complete(); if (checks.some(c => !c.passed)) process.exitCode = 1;
}

(childMode ? interrupted("verified", true) : main()).catch(error => { console.error(error); process.exitCode = 1; });
