/** Durable, epoch-scoped capture uncertainty. Callers write gaps inside the
 * same immediate transaction as their capture cursor or admission. */
import { createHash } from "node:crypto";
import os from "node:os";
import type Database from "better-sqlite3";
import type { CaptureSkippedRecord } from "../capture-record-loss";

const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");
const tupleHash = (parts: readonly (string | number)[]) => sha256(parts.map((part) => {
  const value = String(part);
  return `${Buffer.byteLength(value)}:${value}`;
}).join(""));
// LocalEventBuffer uses the same host identity for captured rows. Cache it
// before any writer transaction; even hostname lookup stays outside the lock.
const MACHINE_HASH = `sha256:${sha256(os.hostname())}`;
const safeMs = (value: number) => Number.isSafeInteger(value) && value >= 0;

export function ensureCaptureGapSchema(db: Database.Database): void {
  db.exec(`
    create table if not exists capture_gaps (
      gap_id text primary key,
      workspace_id text not null default '',
      installation_epoch_id text not null default '',
      source text not null,
      session_id text,
      machine_hash text not null default '',
      epoch_key text not null default '',
      started_at_ms integer not null,
      ended_at_ms integer,
      interval_basis text not null check (interval_basis in
        ('counted_interval','file_write_interval','epoch_open','fault_interval')),
      resolved_at_ms integer,
      dropped_rows integer,
      dropped_usage_rows integer,
      count_basis text not null check (count_basis in ('counted','unknown')),
      reason text not null check (reason in
        ('footprint_cap','backpressure','unknown_type','contract_violation',
         'gap_record_unavailable','tailer_unread','record_exceeds_byte_budget',
         'generation_rewrite_ambiguous','coverage_walk_incomplete','restart_unverified')),
      file_key_digest text,
      unread_bytes integer,
      upload_state text not null default 'pending'
        check (upload_state in ('pending','in_flight','acked')),
      revision integer not null default 1 check (revision > 0),
      check (ended_at_ms is null or ended_at_ms >= started_at_ms),
      check (resolved_at_ms is null or resolved_at_ms >= started_at_ms),
      check ((count_basis = 'unknown' and dropped_rows is null and dropped_usage_rows is null)
          or (count_basis = 'counted' and dropped_rows is not null and dropped_rows >= 0))
    );
    create index if not exists capture_gaps_open_epoch
      on capture_gaps(workspace_id, installation_epoch_id, source, file_key_digest)
      where resolved_at_ms is null;
    create index if not exists capture_gaps_upload
      on capture_gaps(upload_state, gap_id, revision);
    create table if not exists capture_faults (
      fault_id text primary key,
      kind text not null check (kind in
        ('gap_write_failed','cursor_advance_failed','storage_full')),
      source text,
      file_key_digest text,
      at_ms integer not null,
      resolved_at_ms integer,
      detail text,
      check (resolved_at_ms is null or resolved_at_ms >= at_ms)
    );
  `);
}

type FileGapInput = {
  workspaceId: string;
  installationEpochId: string;
  source: string;
  fileKeyDigest: string;
  reason: "tailer_unread" | "record_exceeds_byte_budget" | "generation_rewrite_ambiguous";
  epochStartMs: number;
  lastWriteAtMs: number;
  unreadBytes: number;
  /** The cursor's path-free inode/birth-time generation identity, when known. */
  generationIdentity?: string;
  machineHash?: string;
};

export function fileGapId(input: Pick<FileGapInput, "installationEpochId" | "source" | "fileKeyDigest" | "generationIdentity">): string {
  return tupleHash(["plimsoll-file-gap-v1", input.installationEpochId,
    input.source, input.fileKeyDigest, input.generationIdentity ?? "absent", "unread"]);
}

/** Keep a failed gap DML distinct from ordinary SQLite contention. */
export class CaptureGapWriteError extends Error {
  constructor(readonly source: string, readonly fileKeyDigest: string | null, cause: unknown) {
    super("gap_record_unavailable", { cause });
    this.name = "CaptureGapWriteError";
  }
}

function gapWrite<T>(source: string, digest: string | null, write: () => T): T {
  try { return write(); }
  catch (error) { throw new CaptureGapWriteError(source, digest, error); }
}

/** Only the caller's existing tailer transaction may surround this write. */
export function declareUnresolvedFileGap(db: Database.Database, input: FileGapInput): { gapId: string } {
  if (!safeMs(input.epochStartMs) || !safeMs(input.lastWriteAtMs) ||
      !safeMs(input.unreadBytes) || !/^[a-f0-9]{64}$/.test(input.fileKeyDigest)) {
    throw new Error("invalid_capture_gap_input");
  }
  const gapId = fileGapId(input);
  gapWrite(input.source, input.fileKeyDigest, () => db.prepare(`insert into capture_gaps
    (gap_id,workspace_id,installation_epoch_id,source,machine_hash,epoch_key,
     started_at_ms,ended_at_ms,interval_basis,resolved_at_ms,dropped_rows,
     dropped_usage_rows,count_basis,reason,file_key_digest,unread_bytes)
    values (@gapId,@workspaceId,@installationEpochId,@source,@machineHash,
      @installationEpochId,@epochStartMs,null,'epoch_open',null,null,null,
      'unknown',@reason,@fileKeyDigest,@unreadBytes)
    on conflict(gap_id) do update set
      reason=excluded.reason, unread_bytes=excluded.unread_bytes,
      revision=capture_gaps.revision+1, upload_state='pending'
    where capture_gaps.resolved_at_ms is null and
      (capture_gaps.reason is not excluded.reason or
       capture_gaps.unread_bytes is not excluded.unread_bytes)`).run({
    ...input, gapId, machineHash: input.machineHash ?? MACHINE_HASH,
  }));
  return { gapId };
}

export function resolveCaptureGap(db: Database.Database, gapId: string, atMs: number): void {
  if (!safeMs(atMs)) throw new Error("invalid_capture_gap_resolution");
  gapWrite("unknown", null, () => db.prepare(`update capture_gaps set resolved_at_ms=?,revision=revision+1,upload_state='pending'
    where gap_id=? and resolved_at_ms is null and started_at_ms<=?`).run(atMs, gapId, atMs));
}

export function coverageCompleteForPeriod(db: Database.Database,
  period: { startMs: number; endMs: number }, throughMs: number): boolean {
  if (!safeMs(period.startMs) || !safeMs(period.endMs) ||
      !safeMs(throughMs) || period.endMs <= period.startMs || throughMs < period.endMs) return false;
  const row = db.prepare(`select 1 from capture_gaps
    where resolved_at_ms is null and started_at_ms < ?
      and (ended_at_ms is null or ended_at_ms > ?) limit 1`)
    .get(period.endMs, period.startMs);
  return row === undefined;
}

export function rolloutGapScope(db: Database.Database) {
  const row = db.prepare(`select current_workspace_id as workspaceId,
      current_installation_epoch_id as installationEpochId,
      current_installation_epoch_started_at as epochStartedAt
    from collector_workspace_binding where singleton=1`).get() as {
    workspaceId: string; installationEpochId: string | null; epochStartedAt: string | null;
  } | undefined;
  const epochStartMs = row?.epochStartedAt ? Date.parse(row.epochStartedAt) : 0;
  return {
    workspaceId: row?.workspaceId ?? "unbound",
    installationEpochId: row?.installationEpochId ?? "unbound",
    epochStartMs: safeMs(epochStartMs) ? epochStartMs : 0,
  };
}

export function recordCountedJsonlGap(db: Database.Database, input: {
  installationEpochId: string; workspaceId: string; epochStartMs: number;
  fileKeyDigest: string; offset: number; fingerprint: string; atMs: number;
  source: "codex" | "claude_code";
  kind: CaptureSkippedRecord["kind"];
}) {
  const gapId = tupleHash(["plimsoll-jsonl-skip-v1", input.installationEpochId,
    input.source, input.fileKeyDigest, input.offset, input.fingerprint]);
  gapWrite(input.source, input.fileKeyDigest, () => db.prepare(`insert or ignore into capture_gaps
    (gap_id,workspace_id,installation_epoch_id,source,machine_hash,epoch_key,started_at_ms,
     ended_at_ms,interval_basis,dropped_rows,dropped_usage_rows,count_basis,
     reason,file_key_digest)
    values (@gapId,@workspaceId,@installationEpochId,@source,@machineHash,@installationEpochId,
      @epochStartMs,@atMs,'counted_interval',1,@droppedUsageRows,'counted',
      'record_exceeds_byte_budget',@fileKeyDigest)`).run({
    ...input, gapId, machineHash: MACHINE_HASH,
    droppedUsageRows: input.kind === "codex_token_count" || input.kind === "claude_assistant" ? 1
      : input.kind === "unknown" ? null : 0,
  }));
  return { gapId };
}

export const captureFileKeyDigest = sha256;
