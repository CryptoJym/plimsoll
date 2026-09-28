/** A failed capture transaction must be visible even when SQLite cannot accept
 * its gap. Marker I/O is deliberately outside the writer transaction. */
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";
import { CaptureGapWriteError, captureFileKeyDigest, recordFaultIntervalGap, rolloutGapScope } from "./capture-gaps";

export type CaptureFault = {
  faultId: string;
  kind: "gap_write_failed" | "cursor_advance_failed" | "storage_full";
  atMs: number;
  source: string | null;
  fileKeyDigest: string | null;
};

export type CaptureDurabilityStatus = {
  state: "startup_checking" | "verified" | "fault_live" | "restart_unverified";
  restartUnverified: boolean;
  walkVerified: boolean;
  cleanMarkerObservedAtStart: boolean;
  faultMarkerPersisted: boolean | null;
  storageFull: boolean;
  faults: CaptureFault[];
};

const byDatabase = new WeakMap<Database.Database, CaptureDurability>();
const markerName = "capture-fault.json";
const cleanName = "capture-clean-shutdown";
const SHA256 = /^[0-9a-f]{64}$/;
const MACHINE_HASH = `sha256:${captureFileKeyDigest(os.hostname())}`;

function faultFromJson(value: unknown): CaptureFault | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const row = value as Record<string, unknown>;
  if (row.v !== 1 || typeof row.faultId !== "string" || !SHA256.test(row.faultId) ||
      !["gap_write_failed", "cursor_advance_failed", "storage_full"].includes(String(row.kind)) ||
      !Number.isSafeInteger(row.atMs) || Number(row.atMs) < 0 ||
      !(row.source === null || typeof row.source === "string") ||
      !(row.fileKeyDigest === null || typeof row.fileKeyDigest === "string" && SHA256.test(row.fileKeyDigest))) return null;
  return { faultId: row.faultId, kind: row.kind as CaptureFault["kind"], atMs: row.atMs as number,
    source: row.source as string | null, fileKeyDigest: row.fileKeyDigest as string | null };
}

function fsyncDirectory(directory: string): void {
  const fd = fs.openSync(directory, "r");
  try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
}

function writeAtomically(file: string, body: string): void {
  const temp = `${file}.${process.pid}.${crypto.randomBytes(6).toString("hex")}.tmp`;
  let fd: number | undefined;
  try {
    fd = fs.openSync(temp, "wx", 0o600);
    fs.writeFileSync(fd, body);
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fd = undefined;
    fs.renameSync(temp, file);
    fsyncDirectory(path.dirname(file));
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
    try { fs.unlinkSync(temp); } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
}

export class CaptureDurability {
  private readonly markerPath: string;
  private readonly cleanPath: string;
  private readonly ledgerPath: string;
  private readonly openFaults = new Map<string, CaptureFault>();
  private restartUnverified = false;
  private restartGapPersisted = true;
  private walkVerified = false;
  private coverageHold: { scope: string; updated: Map<string, string> } | null = null;
  private readonly cleanMarkerObservedAtStart: boolean;
  private markerPersisted: boolean | null = null;

  constructor(private readonly db: Database.Database, ledgerPath: string, newLedger: boolean) {
    this.ledgerPath = ledgerPath;
    this.markerPath = path.join(path.dirname(ledgerPath), markerName);
    this.cleanPath = path.join(path.dirname(ledgerPath), cleanName);
    this.coverageHold = this.coverageSnapshot();
    let clean = false;
    try {
      clean = fs.existsSync(this.cleanPath);
      if (clean) {
        fs.unlinkSync(this.cleanPath);
        fsyncDirectory(path.dirname(this.cleanPath));
      }
    } catch { this.restartUnverified = true; }
    this.cleanMarkerObservedAtStart = clean;
    if (!newLedger && !clean) this.restartUnverified = true;
    this.refreshFromDisk();
    if (this.openFaults.size > 0) this.restartUnverified = true;
    if (this.restartUnverified) this.recordRestartGap();
    byDatabase.set(db, this);
  }

  private recordRestartGap(): void {
    try {
      const scope = rolloutGapScope(this.db);
      const gapId = crypto.createHash("sha256")
        .update(JSON.stringify(["plimsoll-restart-unverified-v1", scope.installationEpochId]))
        .digest("hex");
      this.db.prepare(`insert or ignore into capture_gaps
        (gap_id,workspace_id,installation_epoch_id,source,machine_hash,epoch_key,started_at_ms,ended_at_ms,
         interval_basis,count_basis,reason)
        values (?,?,?,?,?,?,?,null,'fault_interval','unknown','restart_unverified')`)
        .run(gapId, scope.workspaceId, scope.installationEpochId, "collector", MACHINE_HASH,
          scope.installationEpochId, scope.epochStartMs);
    } catch {
      this.restartGapPersisted = false;
      // The process bit still withdraws claims until SQLite recovers.
    }
  }

  /** Refreshes another process's marker before opening the claim transaction. */
  refreshFromDisk(): void {
    try {
      const raw = fs.readFileSync(this.markerPath, "utf8");
      const marker = faultFromJson(JSON.parse(raw));
      if (marker) {
        this.openFaults.set(marker.faultId, marker);
        this.markerPersisted = true;
      } else this.restartUnverified = true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") this.restartUnverified = true;
    }
    try {
      const rows = this.db.prepare(`select fault_id as faultId,kind,at_ms as atMs,
        source,file_key_digest as fileKeyDigest from capture_faults where resolved_at_ms is null`)
        .all() as CaptureFault[];
      for (const row of rows) this.openFaults.set(row.faultId, row);
      for (const fault of this.openFaults.values()) this.mirrorFault(fault);
    } catch { this.restartUnverified = true; }
    this.refreshFreshCoverage();
  }

  private coverageSnapshot(): { scope: string; updated: Map<string, string> } | null {
    try {
      const scope = rolloutGapScope(this.db);
      let rows: Array<{ source: string; updatedAt: string }> = [];
      try {
        rows = this.db.prepare(`select source,updated_at as updatedAt from capture_coverage_state
          where workspace_id=? and installation_epoch_id=?
            and source in ('codex','claude_code','grok')`).all(
          scope.workspaceId, scope.installationEpochId,
        ) as Array<{ source: string; updatedAt: string }>;
      } catch { /* No coverage table means no prior completed walk. */ }
      return { scope: `${scope.workspaceId}\u0000${scope.installationEpochId}`,
        updated: new Map(rows.map((row) => [row.source, row.updatedAt])) };
    } catch { return null; }
  }

  /** Each source must have a completion write after this process started or
   * after the most recent fault. Comparing with the saved rows avoids a
   * same-millisecond restart treating old coverage as a fresh walk. */
  private refreshFreshCoverage(): void {
    if (this.walkVerified) return;
    try {
      const current = this.coverageSnapshot();
      if (!current) return;
      if (this.coverageHold?.scope !== current.scope) {
        this.coverageHold = current;
        return;
      }
      if (current.updated.size === 3 && [...current.updated].every(([source, updatedAt]) =>
        updatedAt !== this.coverageHold?.updated.get(source))) {
        this.walkVerified = true;
      }
    } catch { /* A missing or unreadable check never certifies capture. */ }
  }

  private mirrorFault(fault: CaptureFault): void {
    this.db.prepare(`insert or ignore into capture_faults
      (fault_id,kind,source,file_key_digest,at_ms,detail)
      values (@faultId,@kind,@source,@fileKeyDigest,@atMs,'capture_transaction_failed')`).run(fault);
  }

  /** Call only after the source transaction has rolled back. */
  reportGapFailure(error: unknown): void {
    if (!(error instanceof CaptureGapWriteError)) return;
    this.walkVerified = false;
    this.coverageHold = this.coverageSnapshot();
    const cause = error.cause as { code?: unknown } | undefined;
    const kind: CaptureFault["kind"] = cause?.code === "SQLITE_FULL" ? "storage_full" : "gap_write_failed";
    const fault = [...this.openFaults.values()][0] ?? {
      faultId: crypto.createHash("sha256").update(crypto.randomUUID()).digest("hex"),
      kind, atMs: Date.now(), source: error.source, fileKeyDigest: error.fileKeyDigest,
    };
    this.openFaults.set(fault.faultId, fault);
    try {
      writeAtomically(this.markerPath, JSON.stringify({ v: 1, ...fault }));
      this.markerPersisted = true;
    } catch { this.markerPersisted = false; }
    try { this.mirrorFault(fault); } catch { /* Marker or process bit remains authoritative. */ }
  }

  /** Called inside a retry transaction, before its cursor/admission advances. */
  repairInTransaction(): void {
    for (const fault of this.openFaults.values()) {
      this.mirrorFault(fault);
      recordFaultIntervalGap(this.db, { ...fault, repairedAtMs: Date.now() });
    }
  }

  /** Called only after every configured root completed a fresh coverage walk. */
  markFreshWalkComplete(): void {
    this.walkVerified = true;
    if (!this.restartGapPersisted || this.openFaults.size) return;
    try {
      if (!this.db.prepare(`select 1 from capture_faults where resolved_at_ms is null limit 1`).get()) {
        this.restartUnverified = false;
      }
    } catch { /* Keep the restart hold when the mirror cannot be checked. */ }
  }

  status(): CaptureDurabilityStatus {
    return {
      state: this.restartUnverified ? "restart_unverified" : this.openFaults.size ? "fault_live"
        : this.walkVerified ? "verified" : "startup_checking",
      restartUnverified: this.restartUnverified,
      walkVerified: this.walkVerified,
      cleanMarkerObservedAtStart: this.cleanMarkerObservedAtStart,
      faultMarkerPersisted: this.openFaults.size ? this.markerPersisted : null,
      storageFull: [...this.openFaults.values()].some((fault) => fault.kind === "storage_full"),
      faults: [...this.openFaults.values()],
    };
  }

  /** The daemon invokes this after all shutdown work succeeded. */
  markCleanShutdown(): boolean {
    if (this.restartUnverified || !this.walkVerified || this.openFaults.size) return false;
    try {
      if (fs.existsSync(this.markerPath)) return false;
      const check = new Database(this.ledgerPath, { readonly: true, fileMustExist: true });
      let unresolved: unknown;
      try {
        unresolved = check.prepare(`select 1 from capture_faults where resolved_at_ms is null limit 1`).get();
      } finally { check.close(); }
      if (unresolved) return false;
      writeAtomically(this.cleanPath, JSON.stringify({ v: 1, atMs: Date.now() }));
      return true;
    } catch { return false; }
  }
}

export function captureDurabilityFor(db: Database.Database): CaptureDurability | undefined {
  return byDatabase.get(db);
}
