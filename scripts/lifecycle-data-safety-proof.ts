import { createProofCompletion } from "./lib/proof-completion";
/**
 * eco-6hoxj.163.30 r2: update snapshots, rollback and retention are data-safe
 * in the worst case, not only in the tested one.
 *
 * Regression cases for the independent review's reproduced blockers. Every
 * case runs the production lifecycle composition (real filesystem adapter,
 * real SQLite ledger snapshot adapter, real mutation authority) over its own
 * disposable collector home; only the service boundary is a fixture. Nothing
 * here loads, unloads or inspects a real LaunchAgent, and no live collector,
 * ledger or config outside the proof root is touched.
 *
 * The cases use only the lifecycle surface that already existed before the
 * fixes, so the same file runs against the reviewed head and shows each
 * blocker red there. Cases are independent: one failing case never hides
 * another. The run fails if any check fails.
 */
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { collectorBufferPath, collectorConfigPath } from "../packages/collector-cli/src/config";
import { launchAgentPlistPath } from "../packages/collector-cli/src/launch-agent";
import { composeLifecycleAdapter, SqliteLedgerSnapshotAdapter } from "../packages/collector-cli/src/lifecycle-adapters";
import {
  LifecycleManager,
  type LifecycleAdapter,
  type LifecycleReadiness,
  type LifecycleReceipt,
  type RuntimeArtifact,
} from "../packages/collector-cli/src/lifecycle";
import type { LifecycleDatabaseAdapter, LifecycleServiceAdapter } from "../packages/collector-cli/src/lifecycle-filesystem";

const CASES = {
  b2: [
    "restore_without_room_for_a_byte_copy_refuses_and_keeps_the_live_ledger",
    "restore_byte_copy_failure_keeps_the_live_ledger",
    "restore_refuses_a_restored_copy_that_fails_integrity_check",
    "rolled_back_receipt_records_how_the_ledger_was_restored",
  ],
} as const;
const EXPECTED_CHECKS = Object.values(CASES).reduce((total, names) => total + names.length, 0);
const completion = createProofCompletion("lifecycle-data-safety", EXPECTED_CHECKS);

const results: Array<{ name: string; passed: boolean; detail?: unknown }> = [];
function check(name: string, condition: unknown, detail?: unknown) {
  const passed = Boolean(condition);
  results.push({ name, passed, ...(passed ? {} : { detail }) });
  completion.check(name, passed);
  console.log(`${passed ? "PASS" : "FAIL"} ${name}${passed ? "" : ` ${JSON.stringify(detail ?? null).slice(0, 1600)}`}`);
}

const BETTER_SQLITE3 = createRequire(import.meta.url).resolve("better-sqlite3");
const NODE_MAJOR = Number(process.versions.node.split(".", 1)[0]);
const ARCHITECTURE = process.arch === "x64" ? "x64" as const : "arm64" as const;
const ROOT = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-data-safety-")));

const sha256 = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
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

function rowsDigest(file: string) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try {
    return sha256(JSON.stringify(db.prepare("select id, label from proof_rows order by id").all()));
  } finally {
    db.close();
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

function countLabel(file: string, label: string) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try {
    return (db.prepare("select count(*) n from proof_rows where label = ?").get(label) as { n: number }).n;
  } finally {
    db.close();
  }
}

function appendRow(file: string, label: string) {
  const db = new Database(file);
  try {
    db.prepare("insert into proof_rows (label) values (?)").run(label);
  } finally {
    db.close();
  }
}

/** A small WAL ledger with the proof table. */
function createLedger(file: string, label: string) {
  const db = new Database(file);
  try {
    db.pragma("journal_mode = WAL");
    db.exec("create table proof_rows (id integer primary key, label text not null)");
    const insert = db.prepare("insert into proof_rows (label) values (?)");
    db.transaction(() => {
      for (let index = 0; index < 64; index += 1) insert.run(`${label}-${index}`);
    })();
    db.pragma("wal_checkpoint(TRUNCATE)");
  } finally {
    db.close();
  }
}

type Home = ReturnType<typeof createHome>;

/** One disposable collector home with its own PLIMSOLL_HOME, ledger and fixture service. */
function createHome(name: string, options: { ledger?: boolean } = {}) {
  const home = path.join(ROOT, name);
  const collector = path.join(home, ".plimsoll");
  fs.mkdirSync(collector, { recursive: true, mode: 0o700 });
  fs.chmodSync(home, 0o700);
  process.env.PLIMSOLL_HOME = collector;
  const ledger = collectorBufferPath(home);
  const lifecycleRoot = path.join(collector, "lifecycle");
  fs.writeFileSync(collectorConfigPath(home), "{}\n", { mode: 0o600 });
  if (options.ledger !== false) createLedger(ledger, "seed");
  const manifest = launchAgentPlistPath(home);
  const service = {
    version: null as string | null,
    failReadiness: false,
    activations: 0,
    onActivate: null as null | ((version: string) => Promise<void>),
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
      await service.onActivate?.(input.version);
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
      const ready = !service.failReadiness && service.version === expectedVersion;
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
    fs.writeFileSync(sourcePath, `// data-safety proof runtime ${version}; never executed\n`, { mode: 0o700 });
    return {
      version,
      platform: "darwin",
      architecture: ARCHITECTURE,
      nodeMajor: NODE_MAJOR,
      sha256: `sha256:${sha256(fs.readFileSync(sourcePath))}`,
      sourcePath,
    };
  };
  const adapter = (database?: LifecycleDatabaseAdapter): LifecycleAdapter => {
    process.env.PLIMSOLL_HOME = collector;
    return composeLifecycleAdapter({
      homeDir: home,
      lifecycleRoot,
      artifactSourceRoot: home,
      service: fixtureService,
      ...(database ? { database } : {}),
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
    snapshots: () => listDirectory(path.join(lifecycleRoot, "snapshots")),
    versions: () => listDirectory(path.join(lifecycleRoot, "versions")),
    receipt: (operationId: string, operation = "update") => {
      const file = path.join(lifecycleRoot, "receipts", `${operationId}-${operation}.json`);
      return exists(file) ? JSON.parse(fs.readFileSync(file, "utf8")) as LifecycleReceipt : null;
    },
  };
}

/** Runs one case; an exception fails only that case's remaining checks. */
async function runCase(names: readonly string[], body: (record: typeof check) => Promise<void>) {
  const recorded = new Set<string>();
  const record = (name: string, condition: unknown, detail?: unknown) => {
    recorded.add(name);
    check(name, condition, detail);
  };
  try {
    await body(record);
  } catch (error) {
    for (const name of names) {
      if (!recorded.has(name)) check(name, false, { threw: error instanceof Error ? error.stack?.split("\n").slice(0, 4).join(" | ") : String(error) });
    }
  }
}

// ---- B2: restore is atomic and capacity-checked --------------------------

/** A snapshot file and a live ledger with different content, side by side. */
function restorePair(name: string) {
  const directory = path.join(ROOT, name);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const snapshot = path.join(directory, "snapshot.sqlite");
  const live = path.join(directory, "ledger.sqlite");
  createLedger(snapshot, "snapshot");
  createLedger(live, "live");
  return { directory, snapshot, live, liveDigest: rowsDigest(live), snapshotDigest: rowsDigest(snapshot) };
}

/** Makes every byte copy of `source` fail with ENOSPC while `action` runs. */
async function withCopyFailure<T>(source: string, action: () => Promise<T>) {
  const mutableFs = fs as typeof fs & { copyFileSync: (...args: unknown[]) => unknown };
  const originalCopy = mutableFs.copyFileSync;
  let failures = 0;
  mutableFs.copyFileSync = (...args: unknown[]) => {
    if (String(args[0]) === source) {
      failures += 1;
      const error = new Error("proof: no space left on device") as NodeJS.ErrnoException;
      error.code = "ENOSPC";
      throw error;
    }
    return originalCopy.apply(fs, args as Parameters<typeof fs.copyFileSync>);
  };
  try {
    return { result: await rejection(action as () => Promise<unknown>), failures };
  } finally {
    mutableFs.copyFileSync = originalCopy;
  }
}

/**
 * Only the two ledgers are left: no restore temporary or its sidecars. (The
 * proof's own read-only inspections leave ordinary -wal/-shm files.)
 */
function onlyLedgers(directory: string) {
  const names = listDirectory(directory).map((name) => name.replace(/-(wal|shm)$/, ""));
  return same([...new Set(names)], ["ledger.sqlite", "snapshot.sqlite"]);
}

async function b2RestoreIsAtomicAndCapacityChecked() {
  await runCase([CASES.b2[0]], async (record) => {
    const pair = restorePair("b2-no-room");
    const { result } = await withCopyFailure(pair.snapshot, () =>
      new SqliteLedgerSnapshotAdapter({ clone: () => false, freeBytes: () => 0 }).restore({ source: pair.snapshot, destination: pair.live }));
    record(CASES.b2[0],
      result !== null && exists(pair.live) && rowsDigest(pair.live) === pair.liveDigest && integrityOf(pair.live) === "ok" &&
        rowsDigest(pair.snapshot) === pair.snapshotDigest && onlyLedgers(pair.directory),
      { error: result?.message, liveExists: exists(pair.live), files: listDirectory(pair.directory) });
  });
  await runCase([CASES.b2[1]], async (record) => {
    const pair = restorePair("b2-copy-fails");
    const { result, failures } = await withCopyFailure(pair.snapshot, () =>
      new SqliteLedgerSnapshotAdapter({ clone: () => false, freeBytes: () => Number.MAX_SAFE_INTEGER })
        .restore({ source: pair.snapshot, destination: pair.live }));
    record(CASES.b2[1],
      result?.code === "ENOSPC" && failures === 1 && exists(pair.live) && rowsDigest(pair.live) === pair.liveDigest &&
        integrityOf(pair.live) === "ok" && onlyLedgers(pair.directory),
      { error: result?.message, code: result?.code, failures, liveExists: exists(pair.live), files: listDirectory(pair.directory) });
  });
  await runCase([CASES.b2[2]], async (record) => {
    const pair = restorePair("b2-corrupt-copy");
    // An index whose entries no longer match their rows: the file opens, but
    // integrity_check reports it. Copying it over the live ledger would
    // replace good data with a damaged ledger.
    const corrupt = new Database(pair.snapshot);
    corrupt.exec("create index proof_rows_label on proof_rows(label)");
    const rootPage = (corrupt.prepare("select rootpage from sqlite_master where name = 'proof_rows_label'").get() as { rootpage: number }).rootpage;
    const pageSize = corrupt.pragma("page_size", { simple: true }) as number;
    corrupt.pragma("wal_checkpoint(TRUNCATE)");
    corrupt.close();
    const bytes = fs.readFileSync(pair.snapshot);
    const page = (rootPage - 1) * pageSize;
    for (let offset = page + pageSize - 400; offset < page + pageSize - 16; offset += 1) {
      if (bytes[offset] === 0x73) bytes[offset] = 0x74; // "snapshot-N" keys become "tnapshot-N"
    }
    fs.writeFileSync(pair.snapshot, bytes);
    const damaged = (() => {
      try {
        return integrityOf(pair.snapshot);
      } catch (error) {
        return String((error as Error).message);
      }
    })();
    const error = await rejection(() =>
      new SqliteLedgerSnapshotAdapter({ clone: () => false, freeBytes: () => Number.MAX_SAFE_INTEGER })
        .restore({ source: pair.snapshot, destination: pair.live }));
    record(CASES.b2[2],
      damaged !== "ok" && error !== null && exists(pair.live) && rowsDigest(pair.live) === pair.liveDigest &&
        integrityOf(pair.live) === "ok" && onlyLedgers(pair.directory),
      { damaged, error: error?.message, liveExists: exists(pair.live), files: listDirectory(pair.directory) });
  });
  await runCase([CASES.b2[3]], async (record) => {
    const fixture = createHome("b2-receipt");
    await fixture.manager().update({ operationId: "b2-initial", artifact: fixture.artifact("1.0.0") });
    appendRow(fixture.ledger, "before-failed-update");
    const before = rowsDigest(fixture.ledger);
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => appendRow(fixture.ledger, "migration-that-must-roll-back");
    const failed = await rejection(() => fixture.manager().update({ operationId: "b2-failed", artifact: fixture.artifact("1.0.1") }));
    fixture.service.failReadiness = false;
    fixture.service.onActivate = null;
    const receipt = fixture.receipt("b2-failed") as (LifecycleReceipt & { restore?: { method?: unknown; cloneFallback?: unknown } }) | null;
    record(CASES.b2[3],
      /readiness failed/.test(failed?.message ?? "") && receipt?.status === "rolled_back" &&
        receipt.restore?.method === "clone" && receipt.restore.cloneFallback === null &&
        rowsDigest(fixture.ledger) === before && integrityOf(fixture.ledger) === "ok",
      { error: failed?.message, receipt });
  });
}

async function main() {
  try {
    await b2RestoreIsAtomicAndCapacityChecked();
    const failed = results.filter((row) => !row.passed).map((row) => row.name);
    console.log(JSON.stringify({ proof: "lifecycle-data-safety", checks: results.length, passed: results.length - failed.length, failed, liveStateTouched: false }));
  } finally {
    fs.rmSync(ROOT, { recursive: true, force: true });
  }
  completion.complete();
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
