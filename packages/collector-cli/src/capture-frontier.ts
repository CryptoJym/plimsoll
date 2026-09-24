import type Database from "better-sqlite3";

/**
 * Capture watermark v1 (eco-6hoxj.163.18) — the capture half.
 *
 * For each tailed usage source, the start time of the latest capture pass that
 * finished without truncation, deferral or read/parse errors, scoped to the
 * ledger's current workspace and installation epoch. A pass that starts at S
 * and completes has appended every event written to its sources before S, so
 * the upload capture claim may attest capture no later than the minimum of
 * both sources' values. Push-based sources (hooks, OTLP, live usage) append on
 * receipt and are bounded by delivery alone.
 */
export const CAPTURE_FRONTIER_SOURCES = ["codex", "claude_code"] as const;
export type CaptureFrontierSource = (typeof CAPTURE_FRONTIER_SOURCES)[number];

export type CaptureFrontier = {
  workspaceId: string;
  installationEpochId: string;
  epochStartedAt: string;
  /** Minimum over both sources; null until each completed a pass in this epoch. */
  capturedThrough: string | null;
};

const canonical = (value: unknown): value is string => {
  if (typeof value !== "string") return false;
  const ms = Date.parse(value);
  return Number.isFinite(ms) && new Date(ms).toISOString() === value;
};

export function ensureCaptureFrontierSchema(database: Database.Database): void {
  database.exec(`
    create table if not exists capture_frontier (
      workspace_id text not null,
      installation_epoch_id text not null,
      source text not null check (source in ('codex','claude_code')),
      complete_through text not null,
      updated_at text not null,
      primary key (workspace_id, installation_epoch_id, source)
    ) without rowid;
  `);
}

function currentEpoch(database: Database.Database) {
  const table = database
    .prepare(`select 1 from sqlite_master where type='table' and name='collector_workspace_binding'`)
    .get();
  if (!table) return null;
  const row = database
    .prepare(
      `select current_workspace_id as workspaceId,
         current_installation_epoch_id as installationEpochId,
         current_installation_epoch_started_at as epochStartedAt
       from collector_workspace_binding where singleton = 1`,
    )
    .get() as { workspaceId: string | null; installationEpochId: string | null; epochStartedAt: string | null } | undefined;
  if (!row?.workspaceId || !row.installationEpochId || !row.epochStartedAt) return null;
  const epochStartedMs = Date.parse(row.epochStartedAt);
  if (!Number.isFinite(epochStartedMs)) return null;
  return {
    workspaceId: row.workspaceId,
    installationEpochId: row.installationEpochId,
    epochStartedAt: new Date(epochStartedMs).toISOString(),
  };
}

/**
 * Record a complete pass that started at `scanAt`. Never moves backwards, and
 * ignores a pass that began before the current epoch or in the future.
 */
export function recordCompleteCapturePass(
  database: Database.Database,
  source: CaptureFrontierSource,
  scanAt: string,
  now = new Date(),
): boolean {
  const epoch = currentEpoch(database);
  if (!epoch || !canonical(scanAt)) return false;
  const scanMs = Date.parse(scanAt);
  if (scanMs < Date.parse(epoch.epochStartedAt) || scanMs > now.getTime()) return false;
  ensureCaptureFrontierSchema(database);
  database
    .prepare(
      `insert into capture_frontier (workspace_id, installation_epoch_id, source, complete_through, updated_at)
       values (@workspaceId, @installationEpochId, @source, @scanAt, @now)
       on conflict (workspace_id, installation_epoch_id, source) do update set
         complete_through = max(complete_through, excluded.complete_through),
         updated_at = excluded.updated_at`,
    )
    .run({ ...epoch, source, scanAt, now: now.toISOString() });
  return true;
}

/** Whether one automatic or explicit scan result is a complete capture pass. */
export function isCompleteCapturePass(result: {
  aborted?: boolean;
  bytesDeferred: number;
  deferredGenerations?: number;
  parseErrors: number;
  discoveryErrors: number;
  statErrors: number;
  readErrors: number;
  activity: { truncated: boolean };
}): boolean {
  return !result.aborted && !result.activity.truncated && result.bytesDeferred === 0 &&
    (result.deferredGenerations ?? 0) === 0 && result.parseErrors === 0 &&
    result.discoveryErrors === 0 && result.statErrors === 0 && result.readErrors === 0;
}

export function captureFrontier(database: Database.Database): CaptureFrontier | null {
  const epoch = currentEpoch(database);
  if (!epoch) return null;
  const table = database
    .prepare(`select 1 from sqlite_master where type='table' and name='capture_frontier'`)
    .get();
  const rows = table
    ? (database
        .prepare(
          `select source, complete_through as completeThrough from capture_frontier
           where workspace_id = ? and installation_epoch_id = ?`,
        )
        .all(epoch.workspaceId, epoch.installationEpochId) as Array<{ source: string; completeThrough: string }>)
    : [];
  const bySource = new Map(rows.map((row) => [row.source, row.completeThrough]));
  const values = CAPTURE_FRONTIER_SOURCES.map((source) => bySource.get(source));
  const capturedThrough = values.every((value): value is string => canonical(value))
    ? values.reduce((a, b) => (Date.parse(b) < Date.parse(a) ? b : a))
    : null;
  return { ...epoch, capturedThrough };
}
