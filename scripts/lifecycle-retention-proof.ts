import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("lifecycle-retention", 73);
/**
 * eco-6hoxj.163.30: lifecycle update snapshots are bounded and cheap.
 *
 * Drives the production lifecycle composition (real filesystem adapter, real
 * SQLite ledger snapshot adapter, real mutation authority) over a disposable
 * collector home with a few-hundred-MB WAL ledger. Only the service boundary
 * is a fixture: nothing here loads, unloads or inspects a real LaunchAgent,
 * and no live collector, ledger or config outside the proof root is touched.
 *
 * Proves: N+3 sequential updates leave exactly the retained snapshots and
 * runtimes; an unfinished operation's snapshot is never pruned; rollback
 * after pruning still restores; a quiesced snapshot is an APFS clone that
 * costs ~0 free space and passes integrity_check while the lock holds out
 * other processes; a forced clone failure uses the online backup; a ledger
 * another process has open is refused before any change (r2), and so is a
 * full copy without room; a crash in the
 * middle of pruning resumes safely; `snapshots prune` dry runs change nothing.
 * eco-6hoxj.163.47: `--retention keep-all` updates and rollbacks remove no
 * snapshot, runtime, trash entry or display receipt, record what retention
 * would have removed, and leave history a later prune still handles; any
 * other use of the flag fails before any change.
 */
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import {
  cloneFileOrFail,
  composeLifecycleAdapter,
  SqliteLedgerSnapshotAdapter,
} from "../packages/collector-cli/src/lifecycle-adapters";
import {
  LifecycleInterruption,
  LifecycleManager,
  PURGE_CONFIRMATION,
  parseCompletionReceipt,
  planLifecycleRetention,
  type LifecycleAdapter,
  type LifecycleJournal,
  type LifecycleReadiness,
  type LifecycleReceipt,
  type RuntimeArtifact,
} from "../packages/collector-cli/src/lifecycle";
import type { LifecycleDatabaseAdapter, LifecycleServiceAdapter } from "../packages/collector-cli/src/lifecycle-filesystem";
import { collectorBufferPath, collectorConfigPath } from "../packages/collector-cli/src/config";
import { LifecycleMutationAuthority } from "../packages/collector-cli/src/lifecycle-authority";
import { launchAgentPlistPath } from "../packages/collector-cli/src/launch-agent";

type Check = { name: string; passed: boolean };
const checks: Check[] = [];

function check(name: string, condition: unknown, detail?: unknown) {
  const passed = Boolean(condition);
  checks.push({ name, passed });
  completion.check(name, passed);
  console.log(`${passed ? "PASS" : "FAIL"} ${name}`);
  if (!passed) throw new Error(`${name}: ${JSON.stringify(detail ?? null)}`);
}

const REPO_ROOT = path.resolve(import.meta.dirname, "..");
const CLI_ENTRY = path.join(REPO_ROOT, "packages", "collector-cli", "src", "cli.ts");
const TSX_LOADER = path.join(REPO_ROOT, "node_modules", "tsx", "dist", "loader.mjs");
const BETTER_SQLITE3 = createRequire(import.meta.url).resolve("better-sqlite3");
const NODE_MAJOR = Number(process.versions.node.split(".", 1)[0]);
const ARCHITECTURE = process.arch === "x64" ? "x64" as const : "arm64" as const;
const LEDGER_SENTINEL = `ledger-content-sentinel-${randomBytes(6).toString("hex")}`;
const CONFIG_SENTINEL = `config-secret-sentinel-${randomBytes(6).toString("hex")}`;
const ROOT = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-retention-")));

const sha256 = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
const freeBytes = (directory: string) => {
  const stat = fs.statfsSync(directory);
  return stat.bavail * stat.bsize;
};
const exists = (file: string) => fs.existsSync(file);
const listDirectory = (directory: string) => exists(directory) ? fs.readdirSync(directory).sort() : [];
const same = (left: readonly string[], right: readonly string[]) =>
  JSON.stringify([...left].sort()) === JSON.stringify([...right].sort());

async function rejection(action: () => Promise<unknown>) {
  try {
    await action();
    return null;
  } catch (error) {
    return error as Error & { code?: string };
  }
}

type Home = ReturnType<typeof createHome>;

/** One disposable collector home with its own PLIMSOLL_HOME, ledger and fixture service. */
function createHome(name: string, ledgerMegabytes: number) {
  const home = path.join(ROOT, name);
  const collector = path.join(home, ".plimsoll");
  fs.mkdirSync(collector, { recursive: true, mode: 0o700 });
  fs.chmodSync(home, 0o700);
  process.env.PLIMSOLL_HOME = collector;
  const ledger = collectorBufferPath(home);
  const lifecycleRoot = path.join(collector, "lifecycle");
  fs.writeFileSync(collectorConfigPath(home), `${JSON.stringify({ installKey: CONFIG_SENTINEL })}\n`, { mode: 0o600 });
  const db = new Database(ledger);
  db.pragma("journal_mode = WAL");
  db.exec("create table proof_rows (id integer primary key, label text not null, payload blob not null)");
  const insert = db.prepare("insert into proof_rows (label, payload) values (?, ?)");
  const rows = ledgerMegabytes * 8;
  for (let batch = 0; batch < rows; batch += 256) {
    db.transaction(() => {
      for (let index = batch; index < Math.min(rows, batch + 256); index += 1) {
        insert.run(index === 0 ? LEDGER_SENTINEL : "seed", randomBytes(128 * 1024));
      }
    })();
    db.pragma("wal_checkpoint(TRUNCATE)");
  }
  db.close();
  const manifest = launchAgentPlistPath(home);
  const service = {
    version: null as string | null,
    failHealth: false,
    migrateOnActivate: false,
    activations: 0,
  };
  const writeManifest = (executablePath: string | null) => {
    if (executablePath === null) {
      fs.rmSync(manifest, { force: true });
      return;
    }
    fs.mkdirSync(path.dirname(manifest), { recursive: true, mode: 0o700 });
    fs.writeFileSync(manifest, `<plist><array><string>${executablePath}</string><string>start</string></array></plist>\n`, { mode: 0o600 });
  };
  const fixtureService: LifecycleServiceAdapter = {
    async activate(input) {
      service.activations += 1;
      service.version = input.version;
      writeManifest(input.executablePath);
      if (service.migrateOnActivate) {
        const migrating = new Database(ledger);
        migrating.prepare("insert into proof_rows (label, payload) values ('migration', zeroblob(4096))").run();
        migrating.close();
      }
    },
    async restore(input) {
      service.version = input.version;
      writeManifest(input.executablePath);
    },
    async remove() {
      service.version = null;
      writeManifest(null);
    },
    async readiness(expectedVersion): Promise<LifecycleReadiness> {
      const ready = !service.failHealth && service.version === expectedVersion;
      return {
        ready,
        runtimeVersion: service.version,
        serviceReady: ready,
        configCompatible: true,
        databaseCompatible: true,
        reason: ready ? "ready" : "service_unready",
      };
    },
    async supportSnapshot() {
      throw new Error("support snapshots are not exercised by this proof");
    },
  };
  const artifact = (version: string): RuntimeArtifact => {
    const sourcePath = path.join(home, "artifacts", `plimsoll-${version}.mjs`);
    fs.mkdirSync(path.dirname(sourcePath), { recursive: true, mode: 0o700 });
    fs.writeFileSync(sourcePath, `// retention proof runtime ${version}; never executed\n${"/".repeat(64 * 1024)}\n`, { mode: 0o700 });
    return {
      version,
      platform: "darwin",
      architecture: ARCHITECTURE,
      nodeMajor: NODE_MAJOR,
      sha256: `sha256:${sha256(fs.readFileSync(sourcePath))}`,
      sourcePath,
    };
  };
  const adapter = (database?: LifecycleDatabaseAdapter, keepAll = false): LifecycleAdapter => {
    process.env.PLIMSOLL_HOME = collector;
    return composeLifecycleAdapter({
      homeDir: home,
      lifecycleRoot,
      artifactSourceRoot: home,
      service: fixtureService,
      ...(database ? { database } : {}),
      ...(keepAll ? { keepAll } : {}),
    });
  };
  return {
    name,
    home,
    collector,
    ledger,
    lifecycleRoot,
    manifest,
    service,
    artifact,
    adapter,
    manager: (database?: LifecycleDatabaseAdapter) => new LifecycleManager(adapter(database)),
    keepAllManager: () => new LifecycleManager(adapter(undefined, true)),
    snapshots: () => listDirectory(path.join(lifecycleRoot, "snapshots")),
    versions: () => listDirectory(path.join(lifecycleRoot, "versions")),
    trash: () => listDirectory(path.join(lifecycleRoot, "trash")),
    receipt: (operationId: string, operation = "update") =>
      JSON.parse(fs.readFileSync(path.join(lifecycleRoot, "receipts", `${operationId}-${operation}.json`), "utf8")) as LifecycleReceipt,
    metadata: (snapshotId: string) =>
      JSON.parse(fs.readFileSync(path.join(lifecycleRoot, "snapshots", snapshotId, "snapshot.json"), "utf8")) as {
        currentVersion: string | null;
        database?: { method: string | null; quiesced: boolean; cloneFallback: string | null; bytes: number };
      },
    ledgerBytes: () => fs.statSync(ledger).size,
  };
}

function appendRow(home: Home, label: string) {
  const db = new Database(home.ledger);
  db.prepare("insert into proof_rows (label, payload) values (?, zeroblob(1024))").run(label);
  db.close();
}

/** A writer that dies without closing leaves committed frames only in the WAL. */
function crashWriter(home: Home, label: string) {
  const result = spawnSync(process.execPath, ["-e", `
    const Database = require(${JSON.stringify(BETTER_SQLITE3)});
    const db = new Database(${JSON.stringify(home.ledger)});
    db.pragma("wal_autocheckpoint = 0");
    db.prepare("insert into proof_rows (label, payload) values (?, zeroblob(2048))").run(${JSON.stringify(label)});
    process.kill(process.pid, "SIGKILL");`], { timeout: 60_000 });
  return result.signal;
}

const walBytes = (ledger: string) => exists(`${ledger}-wal`) ? fs.statSync(`${ledger}-wal`).size : 0;

function ledgerDigest(file: string) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try {
    const rows = db.prepare("select id, label, hex(substr(payload, 1, 16)) head, length(payload) size from proof_rows order by id").all();
    return sha256(JSON.stringify(rows));
  } finally {
    db.close();
  }
}

/**
 * Integrity and content of a snapshot without touching it: a read-only open
 * would leave -wal/-shm files beside the snapshot, so inspect a clone copy.
 */
function inspectSnapshotDatabase(home: Home, snapshotId: string) {
  const scratch = fs.mkdtempSync(path.join(ROOT, "inspect-"));
  const copy = path.join(scratch, "database");
  try {
    fs.copyFileSync(path.join(home.lifecycleRoot, "snapshots", snapshotId, "database"), copy);
    const db = new Database(copy, { readonly: true, fileMustExist: true });
    let integrity: unknown;
    try {
      integrity = db.pragma("integrity_check", { simple: true });
    } finally {
      db.close();
    }
    return { integrity, digest: ledgerDigest(copy) };
  } finally {
    fs.rmSync(scratch, { recursive: true, force: true });
  }
}

function integrityOf(file: string) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try {
    return db.pragma("integrity_check", { simple: true });
  } finally {
    db.close();
  }
}

/** Another process tries to read the ledger without waiting: "busy" or "read". */
function probeFromOtherProcess(ledger: string) {
  const result = spawnSync(process.execPath, ["-e", `
    const Database = require(${JSON.stringify(BETTER_SQLITE3)});
    try {
      const db = new Database(${JSON.stringify(ledger)}, { timeout: 0, fileMustExist: true });
      db.prepare("select count(*) from proof_rows").get();
      db.close();
      process.stdout.write("read");
    } catch (error) {
      process.stdout.write(/^SQLITE_(BUSY|LOCKED)/.test(String(error.code)) ? "busy" : "error:" + error.code);
    }`], { encoding: "utf8", timeout: 60_000 });
  return result.stdout.trim();
}

/** Measures the volume's free bytes consumed by each snapshot the inner adapter takes. */
function measuring(inner: LifecycleDatabaseAdapter, samples: Array<{ method: string | null; consumed: number }>): LifecycleDatabaseAdapter {
  return {
    async snapshot(input) {
      const before = freeBytes(path.dirname(input.destination));
      const outcome = await inner.snapshot(input);
      samples.push({
        method: typeof outcome === "boolean" ? null : outcome.method,
        consumed: before - freeBytes(path.dirname(input.destination)),
      });
      return outcome;
    },
    restore: (input) => inner.restore(input),
  };
}

/** Relative path, type, mode, size, mtime and (small-file) content of a tree. */
function treeDigest(root: string) {
  const rows: string[] = [];
  const walk = (directory: string) => {
    for (const name of fs.readdirSync(directory).sort()) {
      const absolute = path.join(directory, name);
      const stat = fs.lstatSync(absolute, { bigint: true });
      const relative = path.relative(root, absolute);
      if (stat.isDirectory()) {
        rows.push(`d ${relative} ${stat.mode} ${stat.mtimeNs}`);
        walk(absolute);
      } else if (stat.isSymbolicLink()) {
        rows.push(`l ${relative} ${fs.readlinkSync(absolute)}`);
      } else {
        const content = stat.size < 1024n * 1024n ? sha256(fs.readFileSync(absolute)) : `ino:${stat.ino}`;
        rows.push(`f ${relative} ${stat.mode} ${stat.size} ${stat.mtimeNs} ${content}`);
      }
    }
  };
  walk(root);
  return { digest: sha256(rows.join("\n")), entries: rows.length };
}

/** Relative path of every entry under root (never following a link). */
function relativeEntries(root: string) {
  const entries: string[] = [];
  const walk = (directory: string) => {
    for (const name of fs.readdirSync(directory).sort()) {
      const absolute = path.join(directory, name);
      entries.push(path.relative(root, absolute));
      if (fs.lstatSync(absolute).isDirectory()) walk(absolute);
    }
  };
  walk(root);
  return entries;
}

function cliEnvironment(home: Home): NodeJS.ProcessEnv {
  return {
    PATH: process.env.PATH,
    HOME: home.home,
    PLIMSOLL_HOME: home.collector,
    TMPDIR: process.env.TMPDIR,
    CODEX_HOME: path.join(home.home, ".codex"),
    CLAUDE_CONFIG_DIR: path.join(home.home, ".claude"),
    LANG: "en_US.UTF-8",
    TZ: "UTC",
  };
}

function cli(home: Home, args: readonly string[], imports: readonly string[] = [], pathPrefix?: string) {
  const env = cliEnvironment(home);
  if (pathPrefix) env.PATH = `${pathPrefix}${path.delimiter}${env.PATH ?? ""}`;
  const result = spawnSync(process.execPath, [
    "--import", TSX_LOADER, ...imports.flatMap((file) => ["--import", file]), CLI_ENTRY, ...args,
  ], { cwd: home.home, env, encoding: "utf8", timeout: 180_000 });
  return { code: result.status, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}

function valueBlind(text: string) {
  return !text.includes(ROOT) && !text.includes(LEDGER_SENTINEL) && !text.includes(CONFIG_SENTINEL);
}

async function main() {
  try {
    // ---- N+3 sequential updates through the production composition --------
    const primary = createHome("primary", 288);
    const ledgerBytes = primary.ledgerBytes();
    check("fixture_ledger_is_a_few_hundred_megabytes_of_wal_sqlite", ledgerBytes >= 256 * 1024 * 1024, { ledgerBytes });
    const updates = ["1.0.0", "1.0.1", "1.0.2", "1.0.3", "1.0.4"];
    const expected: Array<{ snapshots: string[]; versions: string[] }> = [
      { snapshots: ["u1"], versions: ["1.0.0"] },
      { snapshots: ["u1", "u2"], versions: ["1.0.0", "1.0.1"] },
      { snapshots: ["u2", "u3"], versions: ["1.0.0", "1.0.1", "1.0.2"] },
      { snapshots: ["u3", "u4"], versions: ["1.0.1", "1.0.2", "1.0.3"] },
      { snapshots: ["u4", "u5"], versions: ["1.0.2", "1.0.3", "1.0.4"] },
    ];
    const sequence: Array<{ snapshots: string[]; versions: string[] }> = [];
    const digestsBefore = new Map<string, string>();
    const freeConsumedByUpdate: number[] = [];
    let lastReceipt: LifecycleReceipt | null = null;
    for (const [index, version] of updates.entries()) {
      const operationId = `u${index + 1}`;
      appendRow(primary, `before-${operationId}`);
      digestsBefore.set(operationId, ledgerDigest(primary.ledger));
      const before = freeBytes(primary.collector);
      lastReceipt = await primary.manager().update({ operationId, artifact: primary.artifact(version) });
      freeConsumedByUpdate.push(before - freeBytes(primary.collector));
      sequence.push({ snapshots: primary.snapshots(), versions: primary.versions() });
    }
    check("n_plus_3_updates_leave_exactly_the_two_newest_snapshots_after_each_update",
      sequence.every((row, index) => same(row.snapshots, expected[index]!.snapshots)), { sequence, expected });
    check("n_plus_3_updates_leave_exactly_the_current_runtime_and_the_ones_retained_snapshots_restore",
      sequence.every((row, index) => same(row.versions, expected[index]!.versions)), { sequence, expected });
    const u5 = lastReceipt!;
    check("receipt_records_each_removal_by_name_and_bytes",
      u5.status === "completed" && u5.retention?.status === "applied" &&
      same(u5.retention.removed.map((item) => `${item.kind}:${item.name}`), ["snapshot:u3", "runtime_version:1.0.1"]) &&
      u5.retention.removed.every((item) => Number.isSafeInteger(item.bytes) && item.bytes > 0) &&
      u5.retention.removedBytes === u5.retention.removed.reduce((total, item) => total + item.bytes, 0) &&
      same(u5.retention.keptSnapshots, ["u4", "u5"]) && same(u5.retention.keptVersions, ["1.0.2", "1.0.3", "1.0.4"]),
      u5.retention);
    check("removed_snapshot_bytes_are_the_full_snapshot_including_the_ledger_copy",
      u5.retention!.removed.find((item) => item.kind === "snapshot")!.bytes >= ledgerBytes, u5.retention);
    const u5Persisted = fs.readFileSync(path.join(primary.lifecycleRoot, "receipts", "u5-update.json"), "utf8");
    const u5Marker = fs.readFileSync(path.join(primary.lifecycleRoot, "completed-operations", "u5.json"), "utf8");
    check("persisted_receipt_and_operation_marker_carry_the_retention_record",
      JSON.stringify(JSON.parse(u5Persisted).retention) === JSON.stringify(u5.retention) &&
      JSON.stringify(JSON.parse(u5Marker).retention) === JSON.stringify(u5.retention));
    check("receipts_are_value_blind_names_and_bytes_only",
      valueBlind(u5Persisted) && valueBlind(JSON.stringify(u5)), { bytes: u5Persisted.length });
    check("operation_markers_of_pruned_snapshots_still_block_id_reuse",
      ["u1", "u2", "u3"].every((id) => exists(path.join(primary.lifecycleRoot, "completed-operations", `${id}.json`))) &&
      (await rejection(() => primary.manager().update({ operationId: "u1", artifact: primary.artifact("1.0.9") })))?.message
        .includes("already completed") === true);
    check("trash_is_empty_after_completed_retention", primary.trash().length === 0, primary.trash());

    // ---- Quiesced snapshots are APFS clones: ~0 space, consistent ---------
    const u5Meta = primary.metadata("u5");
    check("quiesced_snapshot_method_is_clone_in_metadata_and_receipt",
      u5Meta.database?.method === "clone" && u5Meta.database.quiesced === true && u5Meta.database.cloneFallback === null &&
      u5.snapshot?.method === "clone" && u5.snapshot.quiesced === true && u5.snapshot.databaseBytes === u5Meta.database.bytes,
      { metadata: u5Meta.database, receipt: u5.snapshot });
    check("every_sequential_update_snapshot_was_a_clone",
      ["u4", "u5"].every((id) => primary.metadata(id).database?.method === "clone"));
    // u1 and u2 prune nothing, so their free-space change is the snapshot cost.
    const cloneCost = Math.min(freeConsumedByUpdate[0]!, freeConsumedByUpdate[1]!);
    check("clone_snapshot_uses_about_zero_free_space_at_creation",
      cloneCost < ledgerBytes * 0.25, { cloneCost, ledgerBytes, freeConsumedByUpdate });
    const u4Inspection = inspectSnapshotDatabase(primary, "u4");
    check("clone_snapshot_passes_integrity_check", u4Inspection.integrity === "ok", u4Inspection.integrity);
    check("clone_snapshot_holds_exactly_the_ledger_at_its_update",
      u4Inspection.digest === digestsBefore.get("u4") && u4Inspection.digest !== digestsBefore.get("u5"));

    const lockSamples: string[] = [];
    const cloneWithProbe = (source: string, destination: string) => {
      lockSamples.push(probeFromOtherProcess(source));
      const cloned = cloneFileOrFail(source, destination);
      lockSamples.push(probeFromOtherProcess(source));
      return cloned;
    };
    const probedSamples: Array<{ method: string | null; consumed: number }> = [];
    const killedWith = crashWriter(primary, "crash-left-wal");
    const beforeU6 = ledgerDigest(primary.ledger);
    const walBeforeU6 = walBytes(primary.ledger);
    const u6 = await primary.manager(measuring(new SqliteLedgerSnapshotAdapter({ clone: cloneWithProbe }), probedSamples))
      .update({ operationId: "u6", artifact: primary.artifact("1.0.5") });
    check("exclusive_lock_keeps_other_processes_out_before_and_after_the_clone",
      same(lockSamples, ["busy", "busy"]) && u6.snapshot?.method === "clone", { lockSamples, snapshot: u6.snapshot });
    check("lock_is_released_after_the_snapshot", probeFromOtherProcess(primary.ledger) === "read");
    check("measured_clone_snapshot_consumes_under_a_quarter_of_the_ledger",
      probedSamples.length === 1 && probedSamples[0]!.method === "clone" && probedSamples[0]!.consumed < ledgerBytes * 0.25,
      { probedSamples, ledgerBytes });
    check("crash_left_wal_is_checkpointed_into_the_clone_and_emptied",
      killedWith === "SIGKILL" && walBeforeU6 > 0 && inspectSnapshotDatabase(primary, "u6").digest === beforeU6 &&
      walBytes(primary.ledger) === 0, { killedWith, walBeforeU6, walAfter: walBytes(primary.ledger) });

    // ---- Forced clone failure uses the online backup; a live writer is refused
    const fallbackSamples: Array<{ method: string | null; consumed: number }> = [];
    appendRow(primary, "before-u7");
    const beforeU7 = ledgerDigest(primary.ledger);
    const u7 = await primary.manager(measuring(new SqliteLedgerSnapshotAdapter({ clone: () => false }), fallbackSamples))
      .update({ operationId: "u7", artifact: primary.artifact("1.0.6") });
    check("forced_clone_failure_falls_back_to_online_backup_and_records_why",
      u7.snapshot?.method === "online_backup" && u7.snapshot.cloneFallback === "clone_unsupported" &&
      primary.metadata("u7").database?.method === "online_backup", u7.snapshot);
    check("online_backup_fallback_is_a_full_copy",
      fallbackSamples[0]!.method === "online_backup" && fallbackSamples[0]!.consumed > ledgerBytes * 0.5,
      { fallbackSamples, ledgerBytes });
    const u7Inspection = inspectSnapshotDatabase(primary, "u7");
    check("online_backup_fallback_passes_integrity_and_matches_the_ledger",
      u7Inspection.integrity === "ok" && u7Inspection.digest === beforeU7);

    const writer = spawn(process.execPath, ["-e", `
      const Database = require(${JSON.stringify(BETTER_SQLITE3)});
      const db = new Database(${JSON.stringify(primary.ledger)});
      db.prepare("insert into proof_rows (label, payload) values ('live-writer', zeroblob(1024))").run();
      process.stdout.write("ready\\n");
      process.stdin.on("data", () => {});
      process.stdin.on("end", () => { db.close(); process.exit(0); });`], { stdio: ["pipe", "pipe", "inherit"] });
    await new Promise<void>((resolve, reject) => {
      writer.stdout!.once("data", () => resolve());
      writer.once("exit", () => reject(new Error("live writer exited early")));
    });
    let inUse: (Error & { code?: string }) | null;
    try {
      inUse = await rejection(() => primary.manager().update({ operationId: "u8", artifact: primary.artifact("1.0.7") }));
    } finally {
      writer.stdin!.end();
      await new Promise((resolve) => writer.once("exit", resolve));
    }
    // r2 (B1): a ledger another process has open is refused before any change,
    // never copied: a rollback would have to replace it under that process.
    const inUseReceipt = primary.receipt("u8");
    check("ledger_open_in_another_process_is_never_cloned",
      inUse?.code === "LIFECYCLE_SNAPSHOT_REFUSED" && inUseReceipt.status === "refused" &&
      inUseReceipt.refusal?.reason === "ledger_in_use" && inUseReceipt.refusal.cloneFallback === "ledger_in_use" &&
      !primary.snapshots().includes("u8") && !primary.versions().includes("1.0.7"),
      { message: inUse?.message, receipt: inUseReceipt });
    const u8 = await primary.manager().update({ operationId: "u8", artifact: primary.artifact("1.0.7") });
    const u8Inspection = inspectSnapshotDatabase(primary, "u8");
    const u8HasWriterRow = (() => {
      const scratch = fs.mkdtempSync(path.join(ROOT, "inspect-"));
      try {
        fs.copyFileSync(path.join(primary.lifecycleRoot, "snapshots", "u8", "database"), path.join(scratch, "db"));
        const db = new Database(path.join(scratch, "db"), { readonly: true });
        try {
          return (db.prepare("select count(*) n from proof_rows where label = 'live-writer'").get() as { n: number }).n === 1;
        } finally {
          db.close();
        }
      } finally {
        fs.rmSync(scratch, { recursive: true, force: true });
      }
    })();
    check("live_writer_snapshot_is_consistent_and_includes_committed_rows",
      u8.snapshot?.method === "clone" && u8.snapshot.quiesced === true && u8Inspection.integrity === "ok" && u8HasWriterRow,
      u8.snapshot);
    check("retention_stays_bounded_across_clone_and_full_copy_snapshots",
      same(primary.snapshots(), ["u7", "u8"]) && same(primary.versions(), ["1.0.5", "1.0.6", "1.0.7"]),
      { snapshots: primary.snapshots(), versions: primary.versions() });

    // ---- A full copy without room is refused before any change ------------
    const stateBefore = fs.readFileSync(path.join(primary.lifecycleRoot, "state.json"), "utf8");
    const manifestBefore = fs.readFileSync(primary.manifest, "utf8");
    const activationsBefore = primary.service.activations;
    const tight = new SqliteLedgerSnapshotAdapter({
      clone: () => false, cloneSupported: () => false, freeBytes: () => Math.floor(ledgerBytes / 2),
    });
    const refused = await rejection(() => primary.manager(tight).update({ operationId: "r1", artifact: primary.artifact("1.0.8") }));
    const refusedReceipt = primary.receipt("r1");
    check("full_copy_without_room_is_refused_with_a_clear_receipt_reason",
      refused?.code === "LIFECYCLE_SNAPSHOT_REFUSED" && /refused before any change/.test(refused.message) &&
      refusedReceipt.status === "refused" && refusedReceipt.refusal?.reason === "insufficient_free_space" &&
      refusedReceipt.refusal.method === "online_backup" && refusedReceipt.refusal.cloneFallback === "clone_unsupported" &&
      refusedReceipt.refusal.requiredFreeBytes >= ledgerBytes + 2 * 1024 ** 3 &&
      refusedReceipt.refusal.freeBytes === Math.floor(ledgerBytes / 2),
      { message: refused?.message, receipt: refusedReceipt });
    check("refusal_changes_nothing_no_journal_snapshot_runtime_state_manifest_or_service_call",
      !exists(path.join(primary.lifecycleRoot, "journal.json")) && !primary.snapshots().includes("r1") &&
      !primary.versions().includes("1.0.8") &&
      fs.readFileSync(path.join(primary.lifecycleRoot, "state.json"), "utf8") === stateBefore &&
      fs.readFileSync(primary.manifest, "utf8") === manifestBefore && primary.service.activations === activationsBefore);
    check("refused_operation_id_is_not_consumed",
      !exists(path.join(primary.lifecycleRoot, "completed-operations", "r1.json")));
    const noRoomButClone = new SqliteLedgerSnapshotAdapter({ freeBytes: () => 0 });
    const r1 = await primary.manager(noRoomButClone).update({ operationId: "r1", artifact: primary.artifact("1.0.8") });
    check("same_operation_retries_after_refusal_and_a_clone_needs_no_copy_space",
      r1.status === "completed" && r1.snapshot?.method === "clone", r1.snapshot);

    const livePlan = await primary.manager().preflightUpdate();
    check("preflight_predicts_a_clone_that_needs_no_copy_space",
      livePlan.method === "clone" && livePlan.cloneCapable && livePlan.requiredFreeBytes === 0 && livePlan.ok &&
      livePlan.requiredFreeBytesIfCloneFails === 2 * livePlan.ledgerBytes + livePlan.headroomBytes, livePlan);
    const tightPlan = await primary.manager(tight).preflightUpdate();
    check("preflight_refuses_a_full_copy_without_room_before_the_service_is_stopped",
      tightPlan.method === "online_backup" && !tightPlan.ok && tightPlan.reason === "insufficient_free_space" &&
      tightPlan.requiredFreeBytes > tightPlan.freeBytes, tightPlan);
    check("preflight_leaves_no_clone_probe_behind", primary.trash().length === 0, primary.trash());

    // ---- An unfinished operation's snapshot is never pruned ---------------
    let interrupted = false;
    const base = primary.adapter();
    const interrupting = new Proxy(base, {
      get(target, property) {
        if (property === "writeJournal") {
          return async (journal: LifecycleJournal) => {
            await target.writeJournal(journal);
            if (!interrupted && journal.phase === "switched") {
              interrupted = true;
              throw new LifecycleInterruption("proof interruption after switch");
            }
          };
        }
        const value = target[property as keyof LifecycleAdapter];
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    const pausedArtifact = primary.artifact("1.1.0");
    const paused = await rejection(() => new LifecycleManager(interrupting).update({ operationId: "paused", artifact: pausedArtifact }));
    const journalBefore = fs.readFileSync(path.join(primary.lifecycleRoot, "journal.json"), "utf8");
    // A snapshot no journal or receipt accounts for (unknown provenance).
    fs.mkdirSync(path.join(primary.lifecycleRoot, "snapshots", "orphan-unknown"), { mode: 0o700 });
    fs.copyFileSync(path.join(primary.lifecycleRoot, "snapshots", "paused", "snapshot.json"),
      path.join(primary.lifecycleRoot, "snapshots", "orphan-unknown", "snapshot.json"));
    const recoveryPrune = await primary.manager().pruneSnapshots({ operationId: "prune-during-recovery", keep: 1, apply: true });
    const pausedJournal = JSON.parse(journalBefore) as LifecycleJournal;
    check("interrupted_update_leaves_a_journal_that_references_its_snapshot",
      paused instanceof LifecycleInterruption && pausedJournal.phase === "switched" && pausedJournal.snapshotId === "paused");
    check("prune_with_keep_1_never_touches_the_unfinished_operation_or_unknown_snapshots",
      primary.snapshots().includes("paused") && primary.snapshots().includes("orphan-unknown") &&
      fs.readFileSync(path.join(primary.lifecycleRoot, "journal.json"), "utf8") === journalBefore,
      { snapshots: primary.snapshots() });
    check("prune_keeps_both_runtimes_of_the_unfinished_operation",
      primary.versions().includes(pausedJournal.toVersion) && primary.versions().includes(pausedJournal.fromVersion!),
      { versions: primary.versions(), journal: pausedJournal });
    check("prune_still_removes_what_nothing_needs_and_records_it",
      recoveryPrune.receipt?.status === "completed" && recoveryPrune.receipt.operation === "snapshots_prune" &&
      same(primary.snapshots(), ["orphan-unknown", "paused", "r1"]) &&
      same(recoveryPrune.retention.removed.map((item) => `${item.kind}:${item.name}`),
        ["snapshot:u8", "runtime_version:1.0.6"]) && same(primary.versions(), ["1.0.7", "1.0.8", "1.1.0"]) &&
      primary.receipt("prune-during-recovery", "snapshots_prune").retention?.removedBytes === recoveryPrune.retention.removedBytes,
      { snapshots: primary.snapshots(), retention: recoveryPrune.retention });
    const resumed = await primary.manager().update({ operationId: "paused", artifact: pausedArtifact });
    check("interrupted_update_resumes_after_prune_and_completes",
      resumed.status === "completed" && primary.service.version === "1.1.0" && !exists(path.join(primary.lifecycleRoot, "journal.json")));
    check("completed_retention_after_recovery_keeps_two_and_leaves_the_unknown_snapshot",
      same(primary.snapshots(), ["orphan-unknown", "paused", "r1"]) && same(primary.versions(), ["1.0.7", "1.0.8", "1.1.0"]),
      { snapshots: primary.snapshots(), versions: primary.versions() });

    // ---- Rollback after pruning still restores ----------------------------
    appendRow(primary, "before-failed-update");
    const beforeFailure = ledgerDigest(primary.ledger);
    primary.service.failHealth = true;
    primary.service.migrateOnActivate = true;
    const failureSamples: Array<{ method: string | null; consumed: number }> = [];
    const failureFreeBefore = freeBytes(primary.collector);
    const failed = await rejection(() => primary.manager(measuring(new SqliteLedgerSnapshotAdapter(), failureSamples))
      .update({ operationId: "fails-after-prune", artifact: primary.artifact("1.2.0") }));
    const failureConsumed = failureFreeBefore - freeBytes(primary.collector);
    primary.service.failHealth = false;
    primary.service.migrateOnActivate = false;
    const rolledBack = primary.receipt("fails-after-prune");
    check("failed_update_after_pruning_rolls_back_from_its_clone_snapshot",
      /readiness failed/.test(failed?.message ?? "") && rolledBack.status === "rolled_back" &&
      rolledBack.restoredVersion === "1.1.0" && rolledBack.snapshot?.method === "clone" && primary.service.version === "1.1.0" &&
      JSON.parse(fs.readFileSync(path.join(primary.lifecycleRoot, "state.json"), "utf8")).version === "1.1.0",
      rolledBack);
    check("rollback_restores_the_exact_pre_update_ledger",
      ledgerDigest(primary.ledger) === beforeFailure && integrityOf(primary.ledger) === "ok");
    check("clone_snapshot_and_clone_restore_need_no_copy_space",
      failureSamples[0]?.method === "clone" && failureConsumed < ledgerBytes * 0.25, { failureSamples, failureConsumed, ledgerBytes });
    check("rolled_back_update_runs_no_retention", rolledBack.retention === undefined && primary.snapshots().includes("fails-after-prune"));
    const next = await primary.manager().update({ operationId: "after-rollback", artifact: primary.artifact("1.2.1") });
    check("next_completed_update_prunes_the_rolled_back_snapshot",
      next.retention?.removed.some((item) => item.kind === "snapshot" && item.name === "fails-after-prune") === true &&
      !primary.snapshots().includes("fails-after-prune") && same(primary.snapshots(), ["after-rollback", "orphan-unknown", "paused"]),
      { snapshots: primary.snapshots(), retention: next.retention });

    // ---- Policy: re-pins never cost the only way back ---------------------
    // An update to 0.7.37 followed by three same-version re-pins: the two
    // newest snapshots both restore 0.7.37 itself, so the update's snapshot,
    // the only one that restores 0.7.36, must be kept too.
    // r2 (B4): completion order is the durable sequence, not a timestamp.
    const repin = (id: string, restoresVersion: string, sequence: number) => ({
      snapshot: { id, bytes: 1, metadataValid: true, restoresVersion, method: "clone" as const },
      operation: {
        id, kind: "update" as const, status: "completed" as const, fromVersion: restoresVersion, toVersion: "0.7.37",
        sequence, predatesSequence: false,
      },
    });
    const repins = [repin("upgrade", "0.7.36", 1), repin("repin-1", "0.7.37", 2), repin("repin-2", "0.7.37", 3), repin("repin-3", "0.7.37", 4)];
    const repinPlan = planLifecycleRetention({
      installedVersion: "0.7.37",
      pinnedVersions: [],
      journal: null,
      operations: repins.map((row) => row.operation),
      order: { proven: true, legacyChain: true },
      snapshots: repins.map((row) => row.snapshot),
      versions: [{ version: "0.7.35", bytes: 1 }, { version: "0.7.36", bytes: 1 }, { version: "0.7.37", bytes: 1 }],
    }, 2);
    check("same_version_repins_never_prune_the_only_snapshot_that_restores_the_previous_runtime",
      JSON.stringify(repinPlan.snapshots.map((row) => [row.id, row.keep, row.reason])) === JSON.stringify([
        ["upgrade", true, "restores_previous_version"], ["repin-1", false, "older_completed"],
        ["repin-2", true, "newest_completed"], ["repin-3", true, "newest_completed"],
      ]) &&
      JSON.stringify(repinPlan.versions.map((row) => [row.version, row.keep])) ===
        JSON.stringify([["0.7.35", false], ["0.7.36", true], ["0.7.37", true]]),
      repinPlan);

    // ---- Operator commands: list, dry run, apply, crash, resume -----------
    const ops = createHome("operator", 4);
    for (const [index, version] of ["2.0.0", "2.0.1", "2.0.2", "2.0.3"].entries()) {
      appendRow(ops, `ops-${index}`);
      await ops.manager().update({ operationId: `ops-${index + 1}`, artifact: ops.artifact(version) });
    }
    check("operator_home_retains_two_snapshots_after_four_updates",
      same(ops.snapshots(), ["ops-3", "ops-4"]) && same(ops.versions(), ["2.0.1", "2.0.2", "2.0.3"]));

    const treeBefore = treeDigest(ops.home);
    const listText = cli(ops, ["lifecycle", "snapshots", "list", "--keep", "1"]);
    const listJson = cli(ops, ["lifecycle", "snapshots", "list", "--json", "--keep", "1"]);
    const dryRun = cli(ops, ["lifecycle", "snapshots", "prune", "--keep", "1"]);
    const treeAfter = treeDigest(ops.home);
    check("list_and_prune_dry_run_commands_succeed",
      listText.code === 0 && listJson.code === 0 && dryRun.code === 0,
      { list: listText.stderr.slice(-400), json: listJson.stderr.slice(-400), dry: dryRun.stderr.slice(-400) });
    check("snapshots_prune_dry_run_and_list_change_nothing",
      treeBefore.digest === treeAfter.digest && treeBefore.entries > 20, { before: treeBefore, after: treeAfter });
    const preflight = cli(ops, ["lifecycle", "update", "--preflight"]);
    const listed = JSON.parse(listJson.stdout) as { snapshots: { snapshots: Array<Record<string, unknown>>; versions: Array<Record<string, unknown>> } };
    const listedRows = listed.snapshots.snapshots;
    check("list_reports_id_created_bytes_method_state_and_retention_decision",
      listedRows.length === 2 && listedRows.every((row) =>
        typeof row.id === "string" && typeof row.createdAt === "string" && typeof row.bytes === "number" && (row.bytes as number) > 0 &&
        row.method === "clone" && row.operationState === "completed") &&
      listedRows.find((row) => row.id === "ops-4")?.retention === "keep" &&
      listedRows.find((row) => row.id === "ops-3")?.retention === "prune",
      listedRows);
    check("text_list_shows_each_decision_and_the_matching_apply_command",
      /ops-4 .* keep \(newest_completed\)/.test(listText.stdout) && /ops-3 .* prune \(older_completed\)/.test(listText.stdout) &&
      /2\.0\.1 .* prune \(unreferenced\)/.test(listText.stdout) && /Prunable: 2 item\(s\)/.test(listText.stdout) &&
      listText.stdout.includes("`plimsoll lifecycle snapshots prune --keep 1 --apply`"),
      listText.stdout);
    const dry = JSON.parse(dryRun.stdout) as { receipt: unknown; retention: { status: string; removed: Array<{ kind: string; name: string; bytes: number }> } };
    check("dry_run_previews_exact_removals_without_a_receipt",
      dry.receipt === null && dry.retention.status === "preview" &&
      same(dry.retention.removed.map((item) => `${item.kind}:${item.name}`), ["snapshot:ops-3", "runtime_version:2.0.1"]),
      dry);
    check("operator_output_is_value_blind",
      [listText.stdout, listJson.stdout, dryRun.stdout, preflight.stdout].every(valueBlind));
    const preflightJson = JSON.parse(preflight.stdout) as { preflight: { method: string; ok: boolean } };
    check("cli_preflight_reports_clone_on_apfs_and_leaves_no_probe",
      preflight.code === 0 && preflightJson.preflight.method === "clone" && preflightJson.preflight.ok && ops.trash().length === 0,
      { preflight: preflightJson, stderr: preflight.stderr.slice(-300) });
    const keepZero = cli(ops, ["lifecycle", "snapshots", "prune", "--keep", "0", "--apply"]);
    check("keep_zero_is_refused", keepZero.code !== 0 && /--keep must be an integer from 1/.test(keepZero.stderr) &&
      same(ops.snapshots(), ["ops-3", "ops-4"]), keepZero.stderr.slice(-300));

    const crashHook = path.join(ROOT, "crash-after-first-trash-rename.mjs");
    fs.writeFileSync(crashHook, `import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
const rename = fs.renameSync;
fs.renameSync = function (from, to) {
  const result = rename.apply(this, arguments);
  if (String(to).startsWith(${JSON.stringify(path.join(ops.lifecycleRoot, "trash") + path.sep)})) process.exit(86);
  return result;
};
syncBuiltinESMExports();
`);
    const crashed = cli(ops, ["lifecycle", "snapshots", "prune", "--keep", "1", "--apply", "--operation-id", "crash-prune"], [crashHook]);
    const trashAfterCrash = ops.trash();
    check("prune_crashes_right_after_its_first_rename_into_the_trash",
      crashed.code === 86 && trashAfterCrash.length === 1 && trashAfterCrash[0]!.startsWith("snapshot+ops-3+") &&
      !ops.snapshots().includes("ops-3") && ops.versions().includes("2.0.1") &&
      !exists(path.join(ops.lifecycleRoot, "receipts", "crash-prune-snapshots_prune.json")),
      { code: crashed.code, trash: trashAfterCrash, stderr: crashed.stderr.slice(-300) });
    const pending = JSON.parse(cli(ops, ["lifecycle", "snapshots", "list", "--json"]).stdout) as {
      snapshots: { pendingRemoval: Array<{ kind: string; name: string; bytes: number }> };
    };
    check("list_shows_the_interrupted_removal_as_pending",
      pending.snapshots.pendingRemoval.length === 1 && pending.snapshots.pendingRemoval[0]!.name === "ops-3" &&
      pending.snapshots.pendingRemoval[0]!.bytes > 0, pending.snapshots.pendingRemoval);
    // The crashed process still holds its mutation lease. Recover after its
    // natural expiry with an injected clock instead of editing the lease.
    const heldLease = new LifecycleMutationAuthority(path.join(ops.collector, "lifecycle-authority")).observe();
    check("crashed_prune_still_holds_its_mutation_lease", heldLease.kind === "held", heldLease);
    const expiresAtMs = heldLease.kind === "held" ? heldLease.expiresAtMs : Date.now();
    const clock = path.join(ROOT, "after-lease-expiry.mjs");
    fs.writeFileSync(clock, `const now = Date.now; const offset = ${expiresAtMs} - now() + 1; Date.now = () => now() + offset;\n`);
    const recovered = cli(ops, ["lifecycle", "snapshots", "prune", "--keep", "1", "--apply", "--operation-id", "recover-prune"], [clock]);
    const recoveredJson = JSON.parse(recovered.stdout || "{}") as { receipt?: LifecycleReceipt };
    check("next_prune_finishes_the_interrupted_removal_and_records_it",
      recovered.code === 0 && recoveredJson.receipt?.status === "completed" &&
      same(recoveredJson.receipt.retention!.recovered.map((item) => `${item.kind}:${item.name}`), ["snapshot:ops-3"]) &&
      same(recoveredJson.receipt.retention!.removed.map((item) => `${item.kind}:${item.name}`), ["runtime_version:2.0.1"]),
      { code: recovered.code, stdout: recovered.stdout.slice(-600), stderr: recovered.stderr.slice(-300) });
    check("resumed_prune_leaves_exactly_the_retained_set_and_an_empty_trash",
      same(ops.snapshots(), ["ops-4"]) && same(ops.versions(), ["2.0.2", "2.0.3"]) && ops.trash().length === 0,
      { snapshots: ops.snapshots(), versions: ops.versions(), trash: ops.trash() });
    check("resumed_state_still_rolls_back",
      await (async () => {
        ops.service.failHealth = true;
        const before = ledgerDigest(ops.ledger);
        const error = await rejection(() => ops.manager().update({ operationId: "ops-fail", artifact: ops.artifact("2.1.0") }));
        ops.service.failHealth = false;
        return /readiness failed/.test(error?.message ?? "") && ops.receipt("ops-fail").status === "rolled_back" &&
          ledgerDigest(ops.ledger) === before && ops.service.version === "2.0.3";
      })());

    const secretTrash = path.join(ops.lifecycleRoot, "trash", "snapshot+left-behind+000000000000");
    fs.mkdirSync(secretTrash, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(secretTrash, "config"), CONFIG_SENTINEL, { mode: 0o600 });
    await ops.manager().purge({ operationId: "purge-all", apply: true, confirmation: PURGE_CONFIRMATION });
    check("purge_also_deletes_secret_bearing_copies_awaiting_removal",
      !exists(path.join(ops.lifecycleRoot, "trash")) && !exists(path.join(ops.lifecycleRoot, "snapshots")));

    // ---- The real CLI update path quiesces and clones --------------------
    // The command process itself must hold no ledger connection, or every
    // production update would silently fall back to a full copy. The real
    // LaunchAgent manifest service writes only this home's plist; a stub
    // launchctl on PATH proves no service manager is ever invoked.
    const endToEnd = createHome("cli-update", 8);
    fs.writeFileSync(collectorConfigPath(endToEnd.home), "{}\n", { mode: 0o600 });
    const stub = path.join(ROOT, "stub-bin");
    fs.mkdirSync(stub, { mode: 0o700 });
    fs.writeFileSync(path.join(stub, "launchctl"), `#!/bin/sh\necho "$@" >> ${JSON.stringify(path.join(stub, "calls.log"))}\nexit 3\n`, { mode: 0o700 });
    const updateArgs = (operationId: string, version: string) =>
      ["lifecycle", "update", "--operation-id", operationId, "--artifact", CLI_ENTRY, "--artifact-version", version];
    const cliUpdates = ["9.0.0-proof", "9.0.1-proof", "9.0.2-proof"].map((version, index) =>
      cli(endToEnd, updateArgs(`cli-${index + 1}`, version), [], stub));
    const lastCliUpdate = JSON.parse(cliUpdates.at(-1)!.stdout || "{}") as { receipt?: LifecycleReceipt };
    check("real_cli_update_clones_the_quiesced_ledger_and_applies_retention",
      cliUpdates.every((run) => run.code === 0) && lastCliUpdate.receipt?.status === "completed" &&
      lastCliUpdate.receipt.snapshot?.method === "clone" && lastCliUpdate.receipt.snapshot.quiesced === true &&
      same(lastCliUpdate.receipt.retention!.removed.map((item) => `${item.kind}:${item.name}`), ["snapshot:cli-1"]) &&
      same(endToEnd.snapshots(), ["cli-2", "cli-3"]) &&
      same(endToEnd.versions(), ["9.0.0-proof", "9.0.1-proof", "9.0.2-proof"]) && !exists(path.join(stub, "calls.log")),
      { codes: cliUpdates.map((run) => run.code), receipt: lastCliUpdate.receipt, stderr: cliUpdates.map((run) => run.stderr.slice(-300)) });

    // ---- Operator keep-all: the operation removes nothing -----------------
    // Managed rollout windows pass --retention keep-all so an update never
    // deletes a host's earlier snapshots; cleanup stays a separate prune.
    const keeper = createHome("keep-all", 4);
    const keepAllReceipts: LifecycleReceipt[] = [];
    for (const [index, version] of ["4.0.0", "4.0.1", "4.0.2", "4.0.3"].entries()) {
      appendRow(keeper, `keep-${index}`);
      keepAllReceipts.push(await keeper.keepAllManager().update({ operationId: `k${index + 1}`, artifact: keeper.artifact(version) }));
    }
    const k5 = await keeper.keepAllManager().rollback({ operationId: "k5", artifact: keeper.artifact("4.0.2") });
    keepAllReceipts.push(k5);
    check("keep_all_updates_and_rollback_remove_no_snapshot_or_runtime",
      same(keeper.snapshots(), ["k1", "k2", "k3", "k4", "k5"]) && same(keeper.versions(), ["4.0.0", "4.0.1", "4.0.2", "4.0.3"]) &&
      keepAllReceipts.every((receipt) => receipt.status === "completed" && receipt.retention?.status === "skipped" &&
        receipt.retention.skippedReason === "skipped_by_operator" && receipt.retention.removed.length === 0 &&
        receipt.retention.removedBytes === 0 && receipt.retention.recovered.length === 0),
      { snapshots: keeper.snapshots(), versions: keeper.versions(), retention: keepAllReceipts.map((receipt) => receipt.retention) });
    const wouldRemove = (receipt: LifecycleReceipt) =>
      (receipt.retention?.wouldRemove ?? []).map((item) => `${item.kind}:${item.name}`);
    const keepAllPreview = await keeper.manager().pruneSnapshots({ operationId: "keep-all-preview" });
    check("keep_all_receipt_records_exactly_what_retention_would_have_removed",
      keepAllPreview.receipt === null && keepAllPreview.retention.status === "preview" &&
      same(wouldRemove(k5), ["snapshot:k1", "snapshot:k2", "snapshot:k3", "runtime_version:4.0.0", "runtime_version:4.0.1"]) &&
      same(wouldRemove(k5), keepAllPreview.retention.removed.map((item) => `${item.kind}:${item.name}`)) &&
      k5.retention!.wouldRemove!.every((item) => Number.isSafeInteger(item.bytes) && item.bytes > 0),
      { k5: k5.retention, preview: keepAllPreview.retention });
    const k5Marker = JSON.parse(fs.readFileSync(path.join(keeper.lifecycleRoot, "completed-operations", "k5.json"), "utf8")) as
      Record<string, unknown> & { retention: Record<string, unknown> };
    const keepAllListing = await keeper.manager().listSnapshots();
    check("keep_all_receipts_stay_known_ordered_operations_for_later_retention",
      parseCompletionReceipt(k5Marker, "k5")?.status === "completed" &&
      JSON.stringify(k5Marker.retention) === JSON.stringify(k5.retention) && keepAllListing.blockedReason === null &&
      keepAllListing.snapshots.length === 5 && keepAllListing.snapshots.every((row) => row.operationState === "completed"),
      keepAllListing);
    const withRetention = (retention: Record<string, unknown>) => ({ ...k5Marker, retention });
    check("only_an_operator_keep_all_record_may_carry_would_remove_and_it_recovers_nothing",
      parseCompletionReceipt(withRetention({ ...k5Marker.retention, skippedReason: "retention_failed" }), "k5") === null &&
      parseCompletionReceipt(withRetention({ ...k5Marker.retention, recovered: [{ kind: "snapshot", name: "k0", bytes: 1 }] }), "k5") === null &&
      parseCompletionReceipt(withRetention({ ...k5Marker.retention, wouldRemove: [{ kind: "snapshot", name: "../k0", bytes: 1 }] }), "k5") === null &&
      parseCompletionReceipt(withRetention(Object.fromEntries(Object.entries(k5Marker.retention)
        .filter(([key]) => key !== "wouldRemove"))), "k5") !== null);

    // A full display-receipt directory and an interrupted earlier removal stay as they are.
    const keepReceipts = path.join(keeper.lifecycleRoot, "receipts");
    const seededReceipts = Array.from({ length: 32 }, (_unused, index) => `0-seed-${String(index).padStart(2, "0")}-update.json`);
    for (const name of seededReceipts) fs.writeFileSync(path.join(keepReceipts, name), "{}\n", { mode: 0o600 });
    const pendingTrash = `snapshot+k0-interrupted+${"a".repeat(12)}`;
    fs.mkdirSync(path.join(keeper.lifecycleRoot, "trash", pendingTrash), { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(keeper.lifecycleRoot, "trash", pendingTrash, "database"), "interrupted removal\n", { mode: 0o600 });
    const entriesBeforeK6 = relativeEntries(keeper.lifecycleRoot);
    const receiptsBeforeK6 = listDirectory(keepReceipts).length;
    const contentDigests = () => new Map([
      ...keeper.snapshots().map((id) => [`snapshot:${id}`, treeDigest(path.join(keeper.lifecycleRoot, "snapshots", id)).digest] as const),
      ...keeper.versions().map((version) => [`runtime:${version}`, treeDigest(path.join(keeper.lifecycleRoot, "versions", version)).digest] as const),
    ]);
    const digestsBeforeK6 = contentDigests();
    const k6 = await keeper.keepAllManager().update({ operationId: "k6", artifact: keeper.artifact("4.0.4") });
    const entriesAfterK6 = new Set(relativeEntries(keeper.lifecycleRoot));
    const digestsAfterK6 = contentDigests();
    check("keep_all_update_keeps_every_entry_the_trash_and_a_full_display_receipt_directory",
      k6.retention?.skippedReason === "skipped_by_operator" && k6.retention.recovered.length === 0 &&
      receiptsBeforeK6 === 37 && listDirectory(keepReceipts).length === 38 &&
      entriesBeforeK6.every((entry) => entriesAfterK6.has(entry)) && keeper.trash().includes(pendingTrash) &&
      [...digestsBeforeK6].every(([key, digest]) => digestsAfterK6.get(key) === digest),
      { receipts: listDirectory(keepReceipts).length, missing: entriesBeforeK6.filter((entry) => !entriesAfterK6.has(entry)),
        changed: [...digestsBeforeK6].filter(([key, digest]) => digestsAfterK6.get(key) !== digest).map(([key]) => key) });

    // A keep-all update that fails readiness rolls back without trimming receipts or finishing the trash.
    const failing = createHome("keep-all-fail", 2);
    await failing.keepAllManager().update({ operationId: "f1", artifact: failing.artifact("6.0.0") });
    await failing.keepAllManager().update({ operationId: "f2", artifact: failing.artifact("6.0.1") });
    const failReceipts = path.join(failing.lifecycleRoot, "receipts");
    for (const name of seededReceipts) fs.writeFileSync(path.join(failReceipts, name), "{}\n", { mode: 0o600 });
    fs.mkdirSync(path.join(failing.lifecycleRoot, "trash", pendingTrash), { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(failing.lifecycleRoot, "trash", pendingTrash, "database"), "interrupted removal\n", { mode: 0o600 });
    const entriesBeforeF3 = relativeEntries(failing.lifecycleRoot);
    failing.service.failHealth = true;
    const failedF3 = await rejection(() => failing.keepAllManager().update({ operationId: "f3", artifact: failing.artifact("6.0.2") }));
    failing.service.failHealth = false;
    const f3 = failing.receipt("f3");
    const entriesAfterF3 = new Set(relativeEntries(failing.lifecycleRoot));
    check("keep_all_update_that_fails_readiness_rolls_back_without_trimming_receipts_or_touching_the_trash",
      failedF3 !== null && f3.status === "rolled_back" && f3.retention === undefined &&
      entriesBeforeF3.every((entry) => entriesAfterF3.has(entry)) && listDirectory(failReceipts).length === 32 + 3 &&
      failing.trash().includes(pendingTrash) && same(failing.snapshots(), ["f1", "f2", "f3"]),
      { error: failedF3?.message, status: f3.status, receipts: listDirectory(failReceipts).length,
        missing: entriesBeforeF3.filter((entry) => !entriesAfterF3.has(entry)) });
    // The owner's later cleanup: prune removes what the receipt said and finishes the interrupted removal.
    const laterPrune = await keeper.manager().pruneSnapshots({ operationId: "keep-all-later-prune", apply: true });
    check("later_prune_removes_what_keep_all_recorded_and_finishes_the_interrupted_removal",
      laterPrune.retention.status === "applied" && wouldRemove(k6).length > 0 &&
      same(laterPrune.retention.removed.map((item) => `${item.kind}:${item.name}`), wouldRemove(k6)) &&
      same(laterPrune.retention.recovered.map((item) => `${item.kind}:${item.name}`), ["snapshot:k0-interrupted"]) &&
      same(keeper.snapshots(), ["k5", "k6"]) && keeper.trash().length === 0,
      { prune: laterPrune.retention, k6: k6.retention, snapshots: keeper.snapshots() });

    // The real CLI: keep-all removes nothing; any other use of the flag changes nothing.
    const flagHome = createHome("keep-all-cli", 2);
    fs.writeFileSync(collectorConfigPath(flagHome.home), "{}\n", { mode: 0o600 });
    const keepAllRuns = ["9.1.0-proof", "9.1.1-proof", "9.1.2-proof"].map((version, index) =>
      cli(flagHome, [...updateArgs(`cli-keep-${index + 1}`, version), "--retention", "keep-all"], [], stub));
    const lastKeepAll = JSON.parse(keepAllRuns.at(-1)!.stdout || "{}") as { receipt?: LifecycleReceipt };
    const cliRollback = cli(flagHome, ["lifecycle", "rollback", "--operation-id", "cli-keep-rb", "--artifact", CLI_ENTRY,
      "--artifact-version", "9.1.1-proof", "--retention", "keep-all"], [], stub);
    const rolledBackTo = JSON.parse(cliRollback.stdout || "{}") as { receipt?: LifecycleReceipt };
    check("real_cli_update_and_rollback_with_retention_keep_all_remove_nothing",
      keepAllRuns.every((run) => run.code === 0) && lastKeepAll.receipt?.retention?.skippedReason === "skipped_by_operator" &&
      same(wouldRemove(lastKeepAll.receipt), ["snapshot:cli-keep-1"]) &&
      cliRollback.code === 0 && rolledBackTo.receipt?.operation === "rollback" && rolledBackTo.receipt.status === "completed" &&
      rolledBackTo.receipt.retention?.skippedReason === "skipped_by_operator" &&
      same(flagHome.snapshots(), ["cli-keep-1", "cli-keep-2", "cli-keep-3", "cli-keep-rb"]) &&
      same(flagHome.versions(), ["9.1.0-proof", "9.1.1-proof", "9.1.2-proof"]) && !exists(path.join(stub, "calls.log")),
      { codes: keepAllRuns.map((run) => run.code), receipt: lastKeepAll.receipt, rollback: rolledBackTo.receipt ?? cliRollback.stderr.slice(-300),
        stderr: keepAllRuns.map((run) => run.stderr.slice(-300)) });
    const flagTree = treeDigest(flagHome.home);
    const misuses = [
      [...updateArgs("cli-bad-1", "9.2.0-proof"), "--retention"],
      [...updateArgs("cli-bad-2", "9.2.0-proof"), "--retention", "keep-2"],
      [...updateArgs("cli-bad-3", "9.2.0-proof"), "--retention=keep-all"],
      [...updateArgs("cli-bad-4", "9.2.0-proof"), "--retention", "keep-all", "--retention", "keep-all"],
      ["lifecycle", "snapshots", "prune", "--apply", "--retention", "keep-all"],
      [...updateArgs("cli-bad-5", "9.2.0-proof"), "--keep-all"],
      [...updateArgs("cli-bad-6", "9.2.0-proof"), "--retension", "keep-all"],
      [...updateArgs("cli-bad-7", "9.2.0-proof"), "--Retention", "keep-all"],
      ["lifecycle", "rollback", "--operation-id", "cli-bad-8", "--artifact", CLI_ENTRY, "--artifact-version", "9.1.0-proof", "keep-all"],
    ].map((args) => cli(flagHome, args, [], stub));
    check("misused_or_misspelled_retention_flags_fail_before_any_change",
      misuses.every((run) => run.code !== 0 && run.stderr.includes("--retention")) &&
      treeDigest(flagHome.home).digest === flagTree.digest && !exists(path.join(stub, "calls.log")),
      misuses.map((run) => ({ code: run.code, stderr: run.stderr.slice(-200) })));

    // eco-6hoxj.163.52: a keep-all update whose read-only preview is blocked
    // records no wouldRemove ("would remove nothing" would hide why), and the
    // record that blocked it stays as it was.
    const blockedPreview = createHome("keep-all-blocked", 2);
    for (const [index, version] of ["8.0.0", "8.0.1", "8.0.2"].entries()) {
      await blockedPreview.keepAllManager().update({ operationId: `b${index + 1}`, artifact: blockedPreview.artifact(version) });
    }
    const unreadableRecord = path.join(blockedPreview.lifecycleRoot, "removals", "b0-unreadable.json");
    fs.mkdirSync(path.dirname(unreadableRecord), { recursive: true, mode: 0o700 });
    fs.writeFileSync(unreadableRecord, "{ not a removal record\n", { mode: 0o600 });
    const unreadableBytes = fs.readFileSync(unreadableRecord);
    const b4 = await blockedPreview.keepAllManager().update({ operationId: "b4", artifact: blockedPreview.artifact("8.0.3") });
    const b4Marker = JSON.parse(fs.readFileSync(path.join(blockedPreview.lifecycleRoot, "completed-operations", "b4.json"), "utf8")) as
      LifecycleReceipt;
    const blockedListing = await blockedPreview.manager().listSnapshots();
    check("keep_all_update_with_a_blocked_preview_records_no_would_remove_and_keeps_the_record",
      b4.status === "completed" && b4.retention?.skippedReason === "skipped_by_operator" && !("wouldRemove" in b4.retention) &&
      b4Marker.retention !== undefined && !("wouldRemove" in b4Marker.retention) &&
      blockedListing.blockedReason === "removal_record_unreadable" &&
      exists(unreadableRecord) && fs.readFileSync(unreadableRecord).equals(unreadableBytes) &&
      same(blockedPreview.snapshots(), ["b1", "b2", "b3", "b4"]),
      { retention: b4.retention, blockedReason: blockedListing.blockedReason, recordKept: exists(unreadableRecord) });

    // ---- Hostile layout: retention never follows a symlink ----------------
    const hostile = createHome("hostile", 1);
    await hostile.manager().update({ operationId: "h1", artifact: hostile.artifact("3.0.0") });
    await hostile.manager().update({ operationId: "h2", artifact: hostile.artifact("3.0.1") });
    const outside = path.join(ROOT, "outside-hostile");
    fs.mkdirSync(path.join(outside, "keep-me"), { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(outside, "keep-me", "data"), "outside the lifecycle root\n");
    fs.symlinkSync(outside, path.join(hostile.lifecycleRoot, "versions", "0.0.1-link"), "dir");
    fs.symlinkSync(outside, path.join(hostile.lifecycleRoot, "snapshots", "link-snapshot"), "dir");
    await hostile.manager().update({ operationId: "h3", artifact: hostile.artifact("3.0.2") });
    await hostile.manager().update({ operationId: "h4", artifact: hostile.artifact("3.0.3") });
    fs.mkdirSync(path.join(hostile.lifecycleRoot, "snapshots", "no-metadata"), { mode: 0o700 });
    const blocked = await hostile.manager().listSnapshots({ keep: 1 });
    check("snapshot_without_metadata_keeps_every_runtime_it_might_restore",
      blocked.snapshots.find((row) => row.id === "h3")?.retention === "prune" &&
      blocked.snapshots.find((row) => row.id === "no-metadata")?.reason === "operation_unknown" &&
      blocked.versions.find((row) => row.version === "3.0.1")?.reason === "restore_target_unknown" &&
      blocked.versions.every((row) => row.retention === "keep"), blocked);
    fs.rmSync(path.join(hostile.lifecycleRoot, "snapshots", "no-metadata"), { recursive: true });
    check("retention_never_follows_or_removes_symlinked_entries",
      exists(path.join(outside, "keep-me", "data")) &&
      fs.lstatSync(path.join(hostile.lifecycleRoot, "versions", "0.0.1-link")).isSymbolicLink() &&
      fs.lstatSync(path.join(hostile.lifecycleRoot, "snapshots", "link-snapshot")).isSymbolicLink() &&
      same(hostile.snapshots(), ["h3", "h4", "link-snapshot"]), { snapshots: hostile.snapshots(), versions: hostile.versions() });
    fs.rmSync(path.join(hostile.lifecycleRoot, "snapshots"), { recursive: true, force: true });
    fs.mkdirSync(path.join(ROOT, "outside-snapshots"), { mode: 0o700 });
    fs.symlinkSync(path.join(ROOT, "outside-snapshots"), path.join(hostile.lifecycleRoot, "snapshots"), "dir");
    const symlinkedRoot = await hostile.manager().update({ operationId: "h5", artifact: hostile.artifact("3.0.4") }).catch((error: Error) => error);
    check("symlinked_snapshots_root_fails_closed_before_any_write",
      symlinkedRoot instanceof Error && /symlink/.test(symlinkedRoot.message) &&
      listDirectory(path.join(ROOT, "outside-snapshots")).length === 0, String(symlinkedRoot));

    console.log(JSON.stringify({
      proof: "lifecycle-retention",
      checks: checks.length,
      passed: checks.filter((row) => row.passed).length,
      failed: checks.filter((row) => !row.passed).map((row) => row.name),
      measurements: {
        ledgerBytes,
        freeBytesConsumedByCloneUpdates: freeConsumedByUpdate,
        freeBytesConsumedByMeasuredCloneSnapshot: probedSamples[0]!.consumed,
        freeBytesConsumedByOnlineBackupSnapshot: fallbackSamples[0]!.consumed,
        freeBytesConsumedByFailedUpdateCloneSnapshotAndRestore: failureConsumed,
      },
      liveStateTouched: false,
    }));
  } finally {
    fs.rmSync(ROOT, { recursive: true, force: true });
  }
  completion.complete();
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
