import type Database from "better-sqlite3";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";

/** The only new local projection of tool facts. It contains identifiers and counts, never payloads. */
export function ensureWeeklyToolStatsSchema(db: Database.Database): void {
  db.exec(`
    create table if not exists tool_stat_attempt_dimensions (
      operation_id text primary key,
      event_id text not null,
      workspace_id text not null,
      device_id text not null,
      runtime_version text not null,
      collector_version text not null
    );
    create index if not exists tool_stat_attempt_dimensions_event_id_idx
      on tool_stat_attempt_dimensions(event_id);
    create trigger if not exists tool_stat_dimensions_fact_delete
      after delete on tool_attempt_facts begin
        delete from tool_stat_attempt_dimensions where operation_id = old.operation_id;
      end;
    create trigger if not exists tool_stat_dimensions_event_delete
      after delete on buffered_events begin
        delete from tool_stat_attempt_dimensions where event_id = old.id;
      end;
    create table if not exists weekly_tool_stats_control (
      workspace_id text not null,
      device_id text not null,
      first_week text not null,
      primary key(workspace_id,device_id)
    );
    create table if not exists weekly_tool_stats_uploads (
      workspace_id text not null,
      device_id text not null,
      week_start text not null,
      report_sequence integer not null check(report_sequence > 0),
      digest text not null,
      body_json text not null,
      delivered integer not null default 0 check(delivered in (0,1)),
      primary key(workspace_id,device_id,week_start)
    );
  `);
}

const SAFE_VERSION = /^(?:unknown|v?\d{1,5}(?:\.\d{1,5}){1,3})$/;
export function boundedRuntimeVersion(value: unknown): string {
  return typeof value === "string" && SAFE_VERSION.test(value) ? value : "unknown";
}

export function recordToolStatsDimension(db: Database.Database, input: {
  operationId: string; eventId: string; runtimeVersion: unknown; collectorVersion: string;
}): void {
  const event = db.prepare(`select workspace_id as workspaceId, device_id as deviceId
    from buffered_events where id = ?`).get(input.eventId) as { workspaceId: string | null; deviceId: string | null } | undefined;
  if (!event?.workspaceId || !event.deviceId) return;
  db.prepare(`insert or ignore into tool_stat_attempt_dimensions
    (operation_id,event_id,workspace_id,device_id,runtime_version,collector_version) values (?,?,?,?,?,?)`)
    .run(input.operationId, input.eventId, event.workspaceId, event.deviceId,
      boundedRuntimeVersion(input.runtimeVersion), boundedRuntimeVersion(input.collectorVersion));
}

export type ToolStatsCounts = { attempts: number; failures: number; unknown: number; retries: number; longestChain: number };
export type ToolStatsSession = ToolStatsCounts & { sessionId: string; firstAt: string; lastAt: string };
export type ToolStatsCell = ToolStatsCounts & {
  toolClass: string; toolName: string; runtime: string; runtimeVersion: string; collectorVersion: string;
  sessions: ToolStatsSession[];
};
export type WeeklyToolStats = {
  weekStart: string;
  coverage: Array<{ runtime: string; sessions: number; sessionsWithToolEvents: number }>;
  cells: ToolStatsCell[];
};

export function utcWeekStart(at: Date): string {
  const day = new Date(Date.UTC(at.getUTCFullYear(), at.getUTCMonth(), at.getUTCDate()));
  day.setUTCDate(day.getUTCDate() - ((day.getUTCDay() + 6) % 7));
  return day.toISOString().slice(0, 10);
}

function weekEnd(weekStart: string): string {
  const start = new Date(`${weekStart}T00:00:00.000Z`);
  if (Number.isNaN(start.getTime()) || utcWeekStart(start) !== weekStart) throw new Error("invalid_week_start");
  start.setUTCDate(start.getUTCDate() + 7);
  return start.toISOString();
}

type Attempt = {
  operationId: string; source: string; sessionId: string; toolClass: string; toolName: string;
  startedAt: string; resultStatus: string; retryOf: string | null;
  runtimeVersion: string; collectorVersion: string;
};

function emptyCounts(): ToolStatsCounts { return { attempts: 0, failures: 0, unknown: 0, retries: 0, longestChain: 0 }; }

/** A complete calendar week, scoped to the joined workspace and local installation identity. */
export function aggregateToolStatsWeek(db: Database.Database, input: {
  workspaceId: string; deviceId: string; weekStart: string;
}): WeeklyToolStats {
  const start = `${input.weekStart}T00:00:00.000Z`;
  const end = weekEnd(input.weekStart);
  const coverageEligible = terminalPrivacyEligibilitySql(db, "buffered_events");
  const attemptEligible = terminalPrivacyEligibilitySql(db, "e");
  const coverageRows = db.prepare(`select source as runtime, session_id as sessionId,
    max(case when event_type in ('tool_use','tool_result') then 1 else 0 end) as hasTool
    from buffered_events where workspace_id = ? and device_id = ? and session_id is not null
      and observed_at >= ? and observed_at < ? and data_mode = 'metadata' and ${coverageEligible}
    group by source, session_id`).all(input.workspaceId, input.deviceId, start, end) as
      Array<{ runtime: string; sessionId: string; hasTool: number }>;
  const coverageMap = new Map<string, { runtime: string; sessions: number; sessionsWithToolEvents: number }>();
  for (const row of coverageRows) {
    const coverage = coverageMap.get(row.runtime) ?? { runtime: row.runtime, sessions: 0, sessionsWithToolEvents: 0 };
    coverage.sessions += 1;
    coverage.sessionsWithToolEvents += row.hasTool ? 1 : 0;
    coverageMap.set(row.runtime, coverage);
  }
  const attempts = db.prepare(`select a.operation_id as operationId, a.source, a.session_id as sessionId,
    a.tool_class as toolClass, a.tool_name as toolName, a.started_at as startedAt,
    a.result_status as resultStatus, a.retry_of as retryOf,
    d.runtime_version as runtimeVersion, d.collector_version as collectorVersion
    from tool_attempt_facts a join tool_stat_attempt_dimensions d on d.operation_id = a.operation_id
    join buffered_events e on e.id = d.event_id
    where d.workspace_id = ? and d.device_id = ? and a.started_at >= ? and a.started_at < ?
      and e.data_mode = 'metadata' and ${attemptEligible}
    order by a.started_at, a.operation_id`).all(input.workspaceId, input.deviceId, start, end) as Attempt[];
  const cells = new Map<string, ToolStatsCell>();
  const sessionsByCell = new Map<string, Map<string, ToolStatsSession>>();
  const attemptById = new Map(attempts.map((attempt) => [attempt.operationId, attempt]));
  const chainByOperation = new Map<string, number>();
  const chainDepth = (operationId: string): number => {
    const path: string[] = [];
    const seen = new Set<string>();
    let cursor: string | null = operationId;
    while (cursor && attemptById.has(cursor) && !chainByOperation.has(cursor) && !seen.has(cursor)) {
      seen.add(cursor);
      path.push(cursor);
      cursor = attemptById.get(cursor)!.retryOf;
    }
    let depth = cursor ? (chainByOperation.get(cursor) ?? 0) : 0;
    for (let index = path.length - 1; index >= 0; index -= 1) {
      depth += 1;
      chainByOperation.set(path[index]!, depth);
    }
    return chainByOperation.get(operationId) ?? 0;
  };
  for (const attempt of attempts) {
    const key = JSON.stringify([attempt.toolClass, attempt.toolName, attempt.source,
      attempt.runtimeVersion, attempt.collectorVersion]);
    const cell = cells.get(key) ?? { ...emptyCounts(), toolClass: attempt.toolClass,
      toolName: attempt.toolName, runtime: attempt.source, runtimeVersion: attempt.runtimeVersion,
      collectorVersion: attempt.collectorVersion, sessions: [] };
    const depth = chainDepth(attempt.operationId);
    cell.attempts += 1;
    cell.failures += Number(attempt.resultStatus === "failure");
    cell.unknown += Number(attempt.resultStatus === "unknown");
    cell.retries += Number(attempt.retryOf !== null);
    cell.longestChain = Math.max(cell.longestChain, depth);
    const sessionMap = sessionsByCell.get(key) ?? new Map<string, ToolStatsSession>();
    let session = sessionMap.get(attempt.sessionId);
    if (!session) {
      session = { ...emptyCounts(), sessionId: attempt.sessionId,
        firstAt: attempt.startedAt, lastAt: attempt.startedAt };
      cell.sessions.push(session);
      sessionMap.set(attempt.sessionId, session);
      sessionsByCell.set(key, sessionMap);
    }
    session.attempts += 1;
    session.failures += Number(attempt.resultStatus === "failure");
    session.unknown += Number(attempt.resultStatus === "unknown");
    session.retries += Number(attempt.retryOf !== null);
    session.longestChain = Math.max(session.longestChain, depth);
    if (attempt.startedAt < session.firstAt) session.firstAt = attempt.startedAt;
    if (attempt.startedAt > session.lastAt) session.lastAt = attempt.startedAt;
    cells.set(key, cell);
  }
  return { weekStart: input.weekStart,
    coverage: [...coverageMap.values()].sort((a, b) => a.runtime.localeCompare(b.runtime)),
    cells: [...cells.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([, cell]) => ({
      ...cell, sessions: cell.sessions.sort((a, b) => a.sessionId.localeCompare(b.sessionId)),
    })) };
}
