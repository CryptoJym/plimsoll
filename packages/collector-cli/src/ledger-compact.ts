import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";

import Database from "better-sqlite3";

import { assertPrivateDirectory } from "./fresh-ledger-cutover";
import { acquireLedgerConnectionLock } from "./ledger-connection";

/**
 * Offline compaction of the collector ledger: `VACUUM INTO` a new file on any
 * volume, then verify it. The live ledger is never modified; giving its space
 * back is the operator's swap, printed in the receipt with its undo.
 *
 * Refusals (nothing written): a collector connection still open on the ledger
 * (the opener barrier is taken exclusively for the whole run), a destination
 * that exists, a destination directory that is not private (owned, 0700, no
 * symlink or group/other-writable ancestor), or too little free space.
 */

export type LedgerCompactionVerify = "counts" | "quick_check" | "none";

export type LedgerCompactionPlan = {
  status: "ready" | "refused";
  reason: string | null;
  ledgerPath: string;
  intoPath: string;
  ledgerBytes: number | null;
  walBytes: number | null;
  pageSize: number | null;
  pageCount: number | null;
  freelistPages: number | null;
  /** Pages in use times the page size: the compacted file's expected size. */
  liveBytesEstimate: number | null;
  destinationFreeBytes: number | null;
  requiredFreeBytes: number | null;
  sameDevice: boolean | null;
};

export type LedgerCompactionReceipt = Omit<LedgerCompactionPlan, "status"> & {
  status: "compacted" | "refused";
  compactedBytes: number | null;
  reclaimableBytes: number | null;
  journalMode: string | null;
  elapsedMs: number;
  verification: {
    mode: LedgerCompactionVerify;
    passed: boolean | null;
    tables: number | null;
    mismatches: Array<{ table: string; source: number; target: number }>;
    detail: string | null;
  };
  /** Row counts alone cannot prove raw_rowid links or receipt cursors survived. */
  lineage: { passed: boolean | null; tables: string[]; detail: string | null };
  receiptPath: string | null;
  swap: string[];
  undo: string[];
};

const FREE_SPACE_MARGIN = 1.05;
const FREE_SPACE_FLOOR_BYTES = 64 * 1024 * 1024;

function sizeOf(file: string): number | null {
  try { return fs.statSync(file).size; } catch { return null; }
}

function freeBytes(directory: string): number | null {
  try {
    const stat = fs.statfsSync(directory);
    return Number(stat.bavail) * Number(stat.bsize);
  } catch { return null; }
}

function assertDestination(ledgerPath: string, intoPath: string) {
  if (!path.isAbsolute(ledgerPath) || !path.isAbsolute(intoPath)) throw new Error("paths_must_be_absolute");
  const ledger = path.resolve(ledgerPath), into = path.resolve(intoPath);
  if (ledger === into || path.dirname(into) === path.dirname(ledger)) throw new Error("destination_must_leave_ledger_directory");
  for (const sidecar of [into, `${into}-wal`, `${into}-shm`, `${into}-journal`, `${into}.compaction-receipt.json`]) {
    if (fs.existsSync(sidecar)) throw new Error(`destination_exists:${path.basename(sidecar)}`);
  }
  const old = fs.lstatSync(ledger);
  if (!old.isFile() || old.isSymbolicLink()) throw new Error("ledger_not_regular");
  const directory = assertPrivateDirectory(path.dirname(into), "destination_directory_unsafe");
  if (fs.realpathSync(path.dirname(into)) !== path.dirname(into)) throw new Error("destination_directory_unsafe:symlink");
  return { old, directory };
}

const shellQuote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
/** Move a stopped ledger together with its sidecars, refusing to overwrite
 * any existing backup. A rejected copy's WAL must never accompany the undo. */
function moveLedgerFamily(from: string, to: string) {
  const absent = ["", "-wal", "-shm"].map(suffix =>
    `[ ! -e ${shellQuote(to + suffix)} ] && [ ! -L ${shellQuote(to + suffix)} ]`).join(" && ");
  return `( ${absent} && mv ${shellQuote(from)} ${shellQuote(to)} && ` +
    `for s in -wal -shm; do if [ -e ${shellQuote(from)}"$s" ] || [ -L ${shellQuote(from)}"$s" ]; then ` +
    `mv ${shellQuote(from)}"$s" ${shellQuote(to)}"$s" || exit 1; fi; done )`;
}

function swapSteps(ledgerPath: string, intoPath: string) {
  const aside = `${ledgerPath}.before-compaction`;
  return {
    swap: [
      "With the collector stopped (plimsoll unload-launch-agent) and this receipt's verification and lineage passed:",
      moveLedgerFamily(ledgerPath, aside),
      `[ ! -e ${shellQuote(ledgerPath)} ] && [ ! -L ${shellQuote(ledgerPath)} ] && cp ${shellQuote(intoPath)} ${shellQuote(ledgerPath)} && chmod 600 ${shellQuote(ledgerPath)}`,
      "plimsoll load-launch-agent, then check /status (retention, delivery and the weekly report fields) before deleting anything.",
      `Keep "${aside}" until the next weekly report is acknowledged; the opener barrier sidecar (${path.basename(ledgerPath)}.connections.lock.sqlite) is never moved or deleted.`,
    ],
    undo: [
      "plimsoll unload-launch-agent",
      `${moveLedgerFamily(ledgerPath, `${ledgerPath}.compacted-rejected`)} && ${moveLedgerFamily(aside, ledgerPath)}`,
      "plimsoll load-launch-agent",
    ],
  };
}

/** Read-only preflight. Holds the opener barrier exclusively while it reads. */
export function planLedgerCompaction(input: { ledgerPath: string; intoPath: string }): LedgerCompactionPlan {
  const base: LedgerCompactionPlan = {
    status: "refused", reason: null, ledgerPath: input.ledgerPath, intoPath: input.intoPath,
    ledgerBytes: sizeOf(input.ledgerPath), walBytes: sizeOf(`${input.ledgerPath}-wal`),
    pageSize: null, pageCount: null, freelistPages: null, liveBytesEstimate: null,
    destinationFreeBytes: null, requiredFreeBytes: null, sameDevice: null,
  };
  let barrier: ReturnType<typeof acquireLedgerConnectionLock> | undefined;
  try {
    const { old, directory } = assertDestination(input.ledgerPath, input.intoPath);
    base.sameDevice = old.dev === directory.dev;
    barrier = acquireLedgerConnectionLock(input.ledgerPath, "exclusive");
    const db = new Database(input.ledgerPath, { fileMustExist: true, readonly: true, timeout: 0 });
    try {
      base.pageSize = Number(db.pragma("page_size", { simple: true }));
      base.pageCount = Number(db.pragma("page_count", { simple: true }));
      base.freelistPages = Number(db.pragma("freelist_count", { simple: true }));
    } finally { db.close(); }
    base.liveBytesEstimate = Math.max(0, base.pageCount - base.freelistPages) * base.pageSize;
    base.destinationFreeBytes = freeBytes(path.dirname(input.intoPath));
    base.requiredFreeBytes = Math.ceil(base.liveBytesEstimate * FREE_SPACE_MARGIN) + FREE_SPACE_FLOOR_BYTES;
    if (base.destinationFreeBytes === null) {
      return { ...base, status: "refused", reason: "destination_free_space_unavailable" };
    }
    if (base.destinationFreeBytes !== null && base.destinationFreeBytes < base.requiredFreeBytes) {
      return { ...base, status: "refused", reason: "destination_free_space_insufficient" };
    }
    return { ...base, status: "ready", reason: null };
  } catch (error) {
    const code = (error as { code?: string }).code;
    return { ...base, status: "refused",
      reason: code === "LEDGER_SWITCH_IN_PROGRESS" ? "ledger_in_use: stop the collector first"
        : error instanceof Error ? error.message : String(error) };
  } finally { barrier?.release(); }
}

function countRows(db: Database.Database, table: string): number {
  return (db.prepare(`select count(*) as n from "${table.replaceAll('"', '""')}"`).get() as { n: number }).n;
}

/** Compact the ledger into `intoPath` and verify the copy. Nothing in the
 * source changes. The opener barrier is held exclusively throughout, so a
 * collector started during the run refuses to open until it finishes. */
export function compactLedger(input: {
  ledgerPath: string; intoPath: string; verify?: LedgerCompactionVerify;
  onProgress?: (step: string) => void;
}): LedgerCompactionReceipt {
  const started = performance.now();
  const verify = input.verify ?? "counts";
  const plan = planLedgerCompaction(input);
  const { swap, undo } = swapSteps(input.ledgerPath, input.intoPath);
  const receipt: LedgerCompactionReceipt = {
    ...plan, status: "refused", compactedBytes: null, reclaimableBytes: null, journalMode: null,
    elapsedMs: 0, verification: { mode: verify, passed: null, tables: null, mismatches: [], detail: null },
    lineage: { passed: null, tables: [], detail: null },
    receiptPath: null, swap, undo,
  };
  if (plan.status !== "ready") return { ...receipt, elapsedMs: Math.round(performance.now() - started) };
  let barrier: ReturnType<typeof acquireLedgerConnectionLock> | undefined;
  try {
    barrier = acquireLedgerConnectionLock(input.ledgerPath, "exclusive");
    assertDestination(input.ledgerPath, input.intoPath);
    // A writable last connection may checkpoint on close, changing the source
    // and consuming its WAL. Both the plan and copy use a read-only source.
    const source = new Database(input.ledgerPath, { fileMustExist: true, readonly: true, timeout: 0 });
    try {
      input.onProgress?.("vacuum_into");
      source.prepare("vacuum into ?").run(input.intoPath);
      fs.chmodSync(input.intoPath, 0o600);
      receipt.compactedBytes = sizeOf(input.intoPath);
      receipt.reclaimableBytes = receipt.ledgerBytes !== null && receipt.compactedBytes !== null
        ? Math.max(0, receipt.ledgerBytes - receipt.compactedBytes) : null;
      input.onProgress?.("verify");
      const target = new Database(input.intoPath, { fileMustExist: true, readonly: true, timeout: 0 });
      try {
        receipt.journalMode = String(target.pragma("journal_mode", { simple: true }));
        if (verify === "quick_check") {
          const rows = target.pragma("quick_check") as Array<Record<string, string>>;
          const detail = rows.map((row) => Object.values(row)[0]).join("; ");
          receipt.verification = { mode: verify, passed: detail === "ok", tables: null, mismatches: [], detail };
        } else if (verify === "counts") {
          const tables = (source.prepare(`select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name`)
            .all() as Array<{ name: string }>).map((row) => row.name);
          const targetTables = new Set((target.prepare(`select name from sqlite_master where type='table' and name not like 'sqlite_%'`)
            .all() as Array<{ name: string }>).map((row) => row.name));
          const mismatches: Array<{ table: string; source: number; target: number }> = [];
          for (const table of tables) {
            const sourceRows = countRows(source, table);
            const targetRows = targetTables.has(table) ? countRows(target, table) : -1;
            if (sourceRows !== targetRows) mismatches.push({ table, source: sourceRows, target: targetRows });
            input.onProgress?.(`counted:${table}`);
          }
          const sourceObjects = (source.prepare(`select count(*) as n from sqlite_master`).get() as { n: number }).n;
          const targetObjects = (target.prepare(`select count(*) as n from sqlite_master`).get() as { n: number }).n;
          if (sourceObjects !== targetObjects) mismatches.push({ table: "sqlite_master", source: sourceObjects, target: targetObjects });
          receipt.verification = { mode: verify, passed: mismatches.length === 0, tables: tables.length, mismatches,
            detail: mismatches.length === 0 ? `${tables.length} tables and ${sourceObjects} schema objects match` : "row counts differ" };
        } else {
          receipt.verification = { mode: verify, passed: null, tables: null, mismatches: [], detail: "not verified" };
        }
      } finally { target.close(); }
      // Always check identity, including --verify none: pending delivery,
      // summary/finance links and the secondary cursor address these rowids.
      source.prepare("attach database ? as compacted").run(input.intoPath);
      try {
        const tables: string[] = [];
        for (const [table, key] of [["buffered_events", "id"], ["upload_receipts", "delivery_id"]] as const) {
          if (!source.prepare("select 1 from main.sqlite_master where type='table' and name=?").get(table)) continue;
          tables.push(table);
          const mismatch = source.prepare(`select 1 from main.${table} s
            left join compacted.${table} t on t.rowid=s.rowid
            where t.rowid is null or t.${key} is not s.${key} limit 1`).get();
          if (mismatch) {
            receipt.lineage = { passed: false, tables, detail: `rowid_identity_mismatch:${table}` };
            throw new Error(`compaction_lineage_mismatch:${table}`);
          }
        }
        receipt.lineage = { passed: true, tables, detail: "rowid identities preserved" };
      } finally { source.exec("detach database compacted"); }
      if (receipt.verification.passed === false) throw new Error("compaction_verification_failed");
      const fd = fs.openSync(input.intoPath, "r");
      try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    } finally { source.close(); }
    const receiptPath = `${input.intoPath}.compaction-receipt.json`;
    const compacted: LedgerCompactionReceipt = {
      ...receipt, status: "compacted", elapsedMs: Math.round(performance.now() - started), receiptPath,
    };
    fs.writeFileSync(receiptPath, `${JSON.stringify(compacted, null, 2)}\n`, { mode: 0o600, flag: "wx" });
    return compacted;
  } catch (error) {
    const code = (error as { code?: string }).code;
    return { ...receipt, status: "refused", elapsedMs: Math.round(performance.now() - started),
      reason: code === "LEDGER_SWITCH_IN_PROGRESS" ? "ledger_in_use: stop the collector first"
        : error instanceof Error ? error.message : String(error) };
  } finally { barrier?.release(); }
}
