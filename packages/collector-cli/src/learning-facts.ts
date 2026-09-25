import type Database from "better-sqlite3";
import { recordRuntimeFactDrop, type RuntimeFactDropReason } from "./runtime-fact-drops";

import {
  aiInteractionEventSchema,
  deriveTechniqueExposureId,
  deterministicLearningFactId,
  techniqueExposureFactSchema,
  techniqueExposureInputSchema,
  toolAttemptFactSchema,
  toolAttemptResultSignalSchema,
  toolAttemptStartSignalSchema,
  toolSourceSchema,
  validateTechniqueExposureFactIdentity,
  workEpisodeFactSchema,
  type ActionClass,
  type AiInteractionEvent,
  type TechniqueExposureFact,
  type ToolAttemptErrorCategory,
  type ToolAttemptFact,
  type ToolAttemptResultStatus,
  type ToolAttemptSignal,
  type ToolFactClass,
  type ToolFactName,
  type ToolSource,
  type WorkComplexityBand,
  type WorkClass,
  type WorkEpisodeFact,
} from "../../shared/src/index";

export { deterministicLearningFactId } from "../../shared/src/index";

export type LearningFactLimits = {
  attempts: number;
  episodes: number;
  exposures: number;
  techniqueIdentities: number;
};

export const DEFAULT_LEARNING_FACT_LIMITS: LearningFactLimits = {
  attempts: 100_000,
  episodes: 10_000,
  exposures: 10_000,
  techniqueIdentities: 256,
};

export const DEFAULT_LEARNING_FACT_MAINTENANCE_BATCH = 256;

export type LearningFactTableName =
  | "tool_attempt_facts"
  | "work_episode_facts"
  | "technique_exposure_facts"
  | "technique_identity_registry";

export type LearningFactTableStatus = {
  rowCount: number;
  limit: number;
  fillRatio: number;
  evictedCount: number;
  maintenanceNeeded: boolean;
};

export type LearningFactStatus = {
  tables: Record<LearningFactTableName, LearningFactTableStatus>;
  totalRows: number;
  totalLimit: number;
  totalEvicted: number;
  maintenanceNeeded: boolean;
};

export type LearningFactWindow = {
  requestedStartInclusive: string;
  effectiveStartInclusive: string | null;
  startInclusive: string;
  endExclusive: string;
  days: number;
  effectiveDays: number | null;
  shortenedByRetention: boolean | null;
  reason: "retention" | "coverage_unknown" | null;
};

export type LearningFactMaintenanceResult = {
  /** Maximum oldest roots selected per table; dependent rows are additional. */
  requested: number;
  selectedRoots: number;
  evicted: number;
  bounded: boolean;
  tables: Array<{
    table: LearningFactTableName;
    evicted: number;
    rowCount: number;
    limit: number;
  }>;
};

export type LearningFactWriteResult<T> = {
  inserted: boolean;
  /** Null only when the write was intentionally dropped. */
  fact: T | null;
  dropped?: boolean;
  dropReason?: RuntimeFactDropReason;
};

type LearningFactTableDefinition = {
  name: LearningFactTableName;
  limit: keyof LearningFactLimits;
  idColumn: string;
  retentionColumn: string;
};

type LearningFactEvictionCounts = Record<LearningFactTableName, number>;
type CapacityDecision = "admit" | "outside_retention_window" | "required_reference";

const LEARNING_FACT_TABLES: readonly LearningFactTableDefinition[] = [
  {
    name: "tool_attempt_facts",
    limit: "attempts",
    idColumn: "operation_id",
    retentionColumn: "started_at",
  },
  {
    name: "work_episode_facts",
    limit: "episodes",
    idColumn: "episode_id",
    retentionColumn: "started_at",
  },
  {
    name: "technique_exposure_facts",
    limit: "exposures",
    idColumn: "exposure_id",
    retentionColumn: "exposed_at",
  },
  {
    name: "technique_identity_registry",
    limit: "techniqueIdentities",
    idColumn: "technique_key",
    retentionColumn: "first_seen_at",
  },
];

const MAX_SOURCE_OPERATION_KEY_BYTES = 1_024;

// SQLite's %f rounds fractions; Date.parse (the fact schema's clock) truncates
// them to milliseconds. Extract the first three fractional digits instead so
// both sides rank .0009Z, .5Z and .500Z exactly the same way. strftime('%s')
// handles the timezone offset and supplies the whole UTC second.
function retentionInstantExpression(timestampColumn: string) {
  return `cast(strftime('%s', ${timestampColumn}) as integer) * 1000 +
    case when instr(${timestampColumn}, '.') = 0 then 0 else
      cast(substr(
        substr(${timestampColumn}, instr(${timestampColumn}, '.') + 1,
          length(${timestampColumn}) - instr(${timestampColumn}, '.') -
          case when substr(${timestampColumn}, -1) = 'Z' then 1 else 6 end
        ) || '000', 1, 3
      ) as integer)
    end`;
}

// Fact validation uses Date.parse, not SQLite's narrower date grammar. Keep
// this as the sole clock for public writes and legacy-row backfill.
function retentionInstant(timestamp: string): number | null {
  const value = timestamp.trim();
  if (!value || !/(?:Z|[+-]\d{2}:\d{2})$/.test(value)) return null;
  const milliseconds = Date.parse(value);
  return Number.isFinite(milliseconds) ? milliseconds : null;
}

// Loss boundaries are exclusive. Keep even an untrusted future loss visible
// from this operation, without letting its timestamp block a future window.
function lossCutoffLimit(): number {
  return Math.max(Date.now(), new Date().getTime()) + 1;
}

function capacityDropCount(db: Database.Database): number {
  if (!db.prepare(`select 1 from sqlite_master where type='table'
    and name='runtime_fact_drops'`).get()) return 0;
  return (db.prepare(`select coalesce(sum(dropped_count), 0) as n from runtime_fact_drops
    where reason in ('outside_retention_window', 'protected_reference_at_capacity')`
  ).get() as { n: number }).n;
}

/** Read-only coverage boundary for status and the materializer. */
export function readLearningFactWindow(db: Database.Database, until: string, days: number): LearningFactWindow {
  const untilMs = Date.parse(until);
  const requestedMs = untilMs - days * 86_400_000;
  const requestedStartInclusive = new Date(requestedMs).toISOString();
  const unknown = (): LearningFactWindow => ({ requestedStartInclusive,
    effectiveStartInclusive: null, startInclusive: until,
    endExclusive: until, days, effectiveDays: null,
    shortenedByRetention: null, reason: "coverage_unknown" });
  const stateTable = db.prepare(`select 1 from sqlite_master where type='table'
    and name='learning_fact_table_state'`).get();
  if (!stateTable) return unknown();
  const columns = new Set((db.pragma("table_info(learning_fact_table_state)") as
    Array<{ name: string }>).map((column) => column.name));
  if (!columns.has("loss_through_ms") || !columns.has("loss_evicted_count") ||
      !columns.has("loss_drop_count")) return unknown();
  const states = db.prepare(`select table_name as tableName, evicted_count as evictedCount,
    loss_through_ms as lossThroughMs, loss_evicted_count as lossEvictedCount,
    loss_drop_count as lossDropCount
    from learning_fact_table_state`).all() as Array<{
      tableName: LearningFactTableName; evictedCount: number;
      lossThroughMs: number | null; lossEvictedCount: number; lossDropCount: number;
  }>;
  if (states.length !== LEARNING_FACT_TABLES.length ||
      states.some((state) => state.evictedCount !== state.lossEvictedCount) ||
      states.find((state) => state.tableName === "tool_attempt_facts")?.lossDropCount !==
        capacityDropCount(db)) return unknown();
  let effectiveMs = requestedMs;
  for (const state of states) {
    if (state.tableName === "technique_identity_registry") continue;
    if (state.lossThroughMs !== null) effectiveMs = Math.max(effectiveMs, state.lossThroughMs);
  }
  if (db.prepare(`select 1 from sqlite_master where type='table'
    and name='runtime_fact_drops'`).get()) {
    // 0.7.39 refused all new attempts on Studio0 at its hard cap. Its last
    // refusal is a conservative boundary for continuous coverage.
    const drop = db.prepare(`select last_dropped_at as at from runtime_fact_drops
      where reason = 'capacity_exceeded'`).get() as { at: string } | undefined;
    const droppedMs = drop ? Date.parse(drop.at) : NaN;
    if (Number.isFinite(droppedMs)) effectiveMs = Math.max(effectiveMs, droppedMs + 1);
  }
  effectiveMs = Math.min(effectiveMs, untilMs);
  const effectiveStartInclusive = new Date(effectiveMs).toISOString();
  const shortenedByRetention = effectiveMs > requestedMs;
  return { requestedStartInclusive, effectiveStartInclusive,
    startInclusive: effectiveStartInclusive, endExclusive: until, days,
    effectiveDays: Number(((untilMs - effectiveMs) / 86_400_000).toFixed(3)),
    shortenedByRetention, reason: shortenedByRetention ? "retention" : null };
}

function boundedDimensionId(value: string, name: string) {
  const trimmed = value.trim();
  if (
    trimmed.length < 1 ||
    trimmed.length > 96 ||
    !/^[a-zA-Z0-9][a-zA-Z0-9._:-]*$/.test(trimmed)
  ) {
    throw new Error(`${name} must be a bounded metadata identifier`);
  }
  return trimmed;
}

function boundedOperationKey(value: string) {
  if (!value || Buffer.byteLength(value, "utf8") > MAX_SOURCE_OPERATION_KEY_BYTES) {
    throw new Error("sourceOperationKey must be between 1 and 1024 UTF-8 bytes");
  }
  return value;
}

export function deterministicToolOperationId(input: {
  source: ToolSource;
  sessionId: string;
  sourceOperationKey: string;
}) {
  return deterministicLearningFactId([
    "tool-operation-v1",
    toolSourceSchema.parse(input.source),
    boundedDimensionId(input.sessionId, "sessionId"),
    boundedOperationKey(input.sourceOperationKey),
  ]);
}

function lowCardinalityTool(actionClass: ActionClass): {
  toolClass: ToolFactClass;
  toolName: ToolFactName;
} {
  const toolName: ToolFactName = actionClass;
  if (["read", "write", "edit"].includes(actionClass)) {
    return { toolClass: "local_io", toolName };
  }
  if (["browser", "mcp"].includes(actionClass)) {
    return { toolClass: "network", toolName };
  }
  if (["continue", "review"].includes(actionClass)) {
    return { toolClass: "coordination", toolName };
  }
  if (["shell", "test", "validate"].includes(actionClass)) {
    return { toolClass: "compute", toolName };
  }
  return { toolClass: "other", toolName: "other" };
}

export type ToolInteractionAdapterInput = {
  event: AiInteractionEvent;
  /** Ephemeral source correlation ID. It is hashed and never persisted. */
  sourceOperationKey: string;
  /** Ephemeral correlation ID for an explicitly declared retry parent. */
  retryOfSourceOperationKey?: string;
  episodeId?: string;
  /** Authoritative adapter result. Producer metadata is never consulted. */
  resultStatus?: ToolAttemptResultStatus;
  errorCategory?: ToolAttemptErrorCategory;
};

/**
 * Convert an already-normalized tool interaction into one safe promoted
 * signal. Only typed top-level event fields are read. event.metadata is
 * intentionally ignored: producer success claims, prompts, commands, paths,
 * error messages, stacks, and arguments cannot become fact truth.
 */
export function adaptToolInteractionEvent(
  input: ToolInteractionAdapterInput,
): ToolAttemptSignal {
  const event = aiInteractionEventSchema.parse(input.event);
  if (event.eventType !== "tool_use" && event.eventType !== "tool_result") {
    throw new Error("Learning facts accept only tool_use or tool_result events");
  }
  if (!event.sessionId) throw new Error("Tool attempt facts require sessionId");
  const operationId = deterministicToolOperationId({
    source: event.source,
    sessionId: event.sessionId,
    sourceOperationKey: input.sourceOperationKey,
  });

  if (event.eventType === "tool_use") {
    const tool = lowCardinalityTool(event.actionClass);
    const retryOf = input.retryOfSourceOperationKey
      ? deterministicToolOperationId({
          source: event.source,
          sessionId: event.sessionId,
          sourceOperationKey: input.retryOfSourceOperationKey,
        })
      : undefined;
    return toolAttemptStartSignalSchema.parse({
      kind: "attempt",
      operationId,
      source: event.source,
      sessionId: event.sessionId,
      episodeId: input.episodeId,
      ...tool,
      startedAt: event.observedAt,
      retryOf,
    });
  }

  return toolAttemptResultSignalSchema.parse({
    kind: "result",
    operationId,
    source: event.source,
    sessionId: event.sessionId,
    endedAt: event.observedAt,
    resultStatus: input.resultStatus ?? "unknown",
    errorCategory: input.errorCategory,
  });
}

export function buildWorkEpisodeFact(input: {
  source: ToolSource;
  sessionId: string;
  sourceEpisodeKey: string;
  workClass: WorkClass;
  complexityBand: WorkComplexityBand;
  parentEpisodeId?: string;
  startedAt: string;
  endedAt?: string;
}): WorkEpisodeFact {
  const episodeId = deterministicLearningFactId([
    "work-episode-v1",
    toolSourceSchema.parse(input.source),
    boundedDimensionId(input.sessionId, "sessionId"),
    boundedOperationKey(input.sourceEpisodeKey),
  ]);
  const durationMs = input.endedAt
    ? Date.parse(input.endedAt) - Date.parse(input.startedAt)
    : undefined;
  return workEpisodeFactSchema.parse({
    episodeId,
    source: input.source,
    sessionId: input.sessionId,
    workClass: input.workClass,
    complexityBand: input.complexityBand,
    parentEpisodeId: input.parentEpisodeId,
    startedAt: input.startedAt,
    endedAt: input.endedAt,
    durationMs,
  });
}

export function buildTechniqueExposureFact(input: {
  episodeId: string;
  techniqueId: string;
  techniqueVersion?: string;
  contentDigest?: string;
  assignmentId: string;
  workClass: WorkClass;
  complexityBand: WorkComplexityBand;
  exposedAt: string;
  mode: "control" | "treatment";
}): TechniqueExposureFact {
  const canonical = techniqueExposureInputSchema.parse(input);
  const exposureId = deriveTechniqueExposureId(canonical);
  return techniqueExposureFactSchema.parse({
    ...canonical,
    exposureId,
    assertion: "exposure_only",
  });
}

function validateLimits(input: Partial<LearningFactLimits>): LearningFactLimits {
  const limits = { ...DEFAULT_LEARNING_FACT_LIMITS, ...input };
  for (const [name, value] of Object.entries(limits)) {
    if (!Number.isSafeInteger(value) || value < 1) {
      throw new Error(`Learning fact limit ${name} must be a positive safe integer`);
    }
  }
  return limits;
}

type AttemptRow = {
  operationId: string;
  source: string;
  sessionId: string;
  episodeId: string | null;
  toolClass: string;
  toolName: string;
  startedAt: string;
  endedAt: string | null;
  durationMs: number | null;
  resultStatus: string;
  errorCategory: string;
  retryOf: string | null;
};

function attemptFromRow(row: AttemptRow): ToolAttemptFact {
  return toolAttemptFactSchema.parse({
    operationId: row.operationId,
    source: row.source,
    sessionId: row.sessionId,
    episodeId: row.episodeId ?? undefined,
    toolClass: row.toolClass,
    toolName: row.toolName,
    startedAt: row.startedAt,
    endedAt: row.endedAt ?? undefined,
    durationMs: row.durationMs ?? undefined,
    resultStatus: row.resultStatus,
    errorCategory: row.errorCategory,
    retryOf: row.retryOf ?? undefined,
  });
}

const ATTEMPT_SELECT = `select operation_id as operationId, source, session_id as sessionId,
  episode_id as episodeId, tool_class as toolClass, tool_name as toolName,
  started_at as startedAt, ended_at as endedAt, duration_ms as durationMs,
  result_status as resultStatus, error_category as errorCategory, retry_of as retryOf
  from tool_attempt_facts`;

/** Explicit local-only promoted fact store. No method creates upload rows. */
export class LearningFactStore {
  private readonly limits: LearningFactLimits;

  constructor(
    private readonly db: Database.Database,
    limits: Partial<LearningFactLimits> = {},
  ) {
    this.limits = validateLimits(limits);
    this.ensureSchema();
  }

  private ensureSchema() {
    const migrate = this.db.transaction(() => {
      this.db.exec(`
      create table if not exists work_episode_facts (
        episode_id text primary key,
        source text not null,
        session_id text not null check(length(session_id) between 1 and 96),
        work_class text not null check(work_class in ('implementation','debugging','review','research','operations','other')),
        complexity_band text not null check(complexity_band in ('low','medium','high','unknown')),
        parent_episode_id text,
        started_at text not null,
        ended_at text,
        duration_ms integer check(duration_ms is null or duration_ms between 0 and 2592000000),
        created_at text not null,
        check((ended_at is null) = (duration_ms is null))
      );
      create table if not exists tool_attempt_facts (
        operation_id text primary key,
        source text not null,
        session_id text not null check(length(session_id) between 1 and 96),
        episode_id text,
        tool_class text not null check(tool_class in ('compute','local_io','network','coordination','other')),
        tool_name text not null check(tool_name in ('continue','validate','test','edit','read','write','shell','mcp','browser','review','other')),
        started_at text not null,
        ended_at text,
        duration_ms integer check(duration_ms is null or duration_ms between 0 and 604800000),
        result_status text not null check(result_status in ('success','failure','unknown')),
        error_category text not null check(error_category in ('none','auth','rate_limit','timeout','network','validation','not_found','conflict','provider','tool','unknown')),
        retry_of text,
        created_at text not null,
        updated_at text not null,
        check(operation_id <> retry_of),
        check((ended_at is null) = (duration_ms is null)),
        check(result_status = 'unknown' or ended_at is not null)
      );
      create table if not exists technique_exposure_facts (
        exposure_id text primary key,
        episode_id text not null,
        technique_id text not null check(length(technique_id) between 1 and 96),
        technique_version text,
        content_digest text,
        assignment_id text not null check(length(assignment_id) between 1 and 96),
        work_class text not null check(work_class in ('implementation','debugging','review','research','operations','other')),
        complexity_band text not null check(complexity_band in ('low','medium','high','unknown')),
        exposed_at text not null,
        mode text not null check(mode in ('control','treatment')),
        assertion text not null check(assertion = 'exposure_only'),
        created_at text not null,
        check(technique_version is not null or content_digest is not null),
        check(exposed_at = strftime('%Y-%m-%dT%H:%M:%fZ', exposed_at))
      );
      create table if not exists technique_identity_registry (
        technique_key text primary key,
        technique_id text not null,
        technique_version text,
        content_digest text,
        first_seen_at text not null
      );
      create table if not exists learning_fact_table_state (
        table_name text primary key check(table_name in (
          'tool_attempt_facts',
          'work_episode_facts',
          'technique_exposure_facts',
          'technique_identity_registry'
        )),
        row_count integer not null check(row_count >= 0),
        evicted_count integer not null default 0 check(evicted_count >= 0),
        maintenance_needed integer not null default 0 check(maintenance_needed in (0, 1)),
        updated_at text not null
      );
      create index if not exists idx_attempt_session_time
        on tool_attempt_facts(session_id, started_at);
      create index if not exists idx_attempt_episode_time
        on tool_attempt_facts(episode_id, started_at);
      create index if not exists idx_attempt_retry
        on tool_attempt_facts(retry_of);
      create index if not exists idx_attempt_dimensions
        on tool_attempt_facts(tool_class, tool_name, result_status, started_at);
      create index if not exists idx_episode_session_time
        on work_episode_facts(session_id, started_at);
      create index if not exists idx_episode_dimensions
        on work_episode_facts(work_class, complexity_band, started_at);
      create index if not exists idx_exposure_technique_time
        on technique_exposure_facts(technique_id, exposed_at);
      create index if not exists idx_exposure_assignment
        on technique_exposure_facts(assignment_id);
      create index if not exists idx_exposure_episode
        on technique_exposure_facts(episode_id, exposed_at);
      create unique index if not exists idx_exposure_semantic_identity
        on technique_exposure_facts(
          episode_id,
          technique_id,
          coalesce(technique_version, ''),
          coalesce(content_digest, ''),
          assignment_id,
          exposed_at,
          mode
        );
      create index if not exists idx_attempt_retention
        on tool_attempt_facts(started_at, operation_id);
      create index if not exists idx_episode_retention
        on work_episode_facts(started_at, episode_id);
      create index if not exists idx_exposure_retention
        on technique_exposure_facts(exposed_at, exposure_id);
      create index if not exists idx_technique_identity_retention
        on technique_identity_registry(first_seen_at, technique_key);
    `);
      // The previous VIRTUAL expression used SQLite's narrower timestamp
      // grammar, so accepted spellings such as a lowercase 't' could block the
      // entire upgrade. Replace it with a nullable stored key. The old binary
      // writes explicit column lists and can still insert after a downgrade;
      // we repair any such NULL keys before the first retention decision on
      // every subsequent open.
      for (const definition of LEARNING_FACT_TABLES) {
        const column = (this.db.pragma(`table_xinfo(${definition.name})`) as
          Array<{ name: string; hidden: number }>).find((entry) => entry.name === "retention_ms");
        if (column?.hidden) {
          this.db.exec(`drop index if exists idx_${definition.name}_retention_ms`);
          this.db.exec(`alter table ${definition.name} drop column retention_ms`);
        }
        if (!column || column.hidden) {
          this.db.exec(`alter table ${definition.name} add column retention_ms integer`);
        }
        // NULL means the key came from an old/raw writer, or predates this
        // verification marker. A SQLite trigger key is only provisional until
        // the same TypeScript fact validator used by public writes checks it.
        const verifiedColumn = (this.db.pragma(`table_xinfo(${definition.name})`) as
          Array<{ name: string }>).some((entry) => entry.name === "retention_verified");
        if (!verifiedColumn) {
          this.db.exec(`alter table ${definition.name} add column retention_verified integer`);
        }
        this.db.exec(`create index if not exists idx_${definition.name}_retention_ms
          on ${definition.name}(retention_ms, ${definition.idColumn})`);
        this.db.exec(`create index if not exists idx_${definition.name}_retention_unverified
          on ${definition.name}(${definition.idColumn}) where retention_verified is null`);
        // Compatibility for raw SQL and old collectors' ordinary uppercase-T
        // ISO writes, including offsets. Do not use SQLite's parser for the
        // broader admitted grammar: the next open backfills those with
        // Date.parse. Public writes always supply the key directly.
        // This trigger never rejects an old writer's accepted timestamp.
        this.db.exec(`create trigger if not exists learning_fact_retention_${definition.name}_insert
          after insert on ${definition.name}
          when new.retention_ms is null and
               substr(new.${definition.retentionColumn}, 5, 1) = '-' and
               substr(new.${definition.retentionColumn}, 8, 1) = '-' and
               substr(new.${definition.retentionColumn}, 11, 1) = 'T' and
               substr(new.${definition.retentionColumn}, 14, 1) = ':' and
               substr(new.${definition.retentionColumn}, 17, 1) = ':' and
               strftime('%s', new.${definition.retentionColumn}) is not null
          begin
            update ${definition.name}
               set retention_ms = ${retentionInstantExpression(definition.retentionColumn)}
             where ${definition.idColumn} = new.${definition.idColumn};
          end`);
      }
      // Existing ledgers created before optional episode parent linkage keep
      // their original table shape; add the column in the same transaction as
      // the state and trigger upgrade.
      const workEpisodeColumns = new Set(
        (this.db.pragma("table_info(work_episode_facts)") as Array<{ name: string }>)
          .map((column) => column.name),
      );
      if (!workEpisodeColumns.has("parent_episode_id")) {
        this.db.exec(`alter table work_episode_facts add column parent_episode_id text`);
      }
      this.db.exec(`
        create index if not exists idx_episode_parent
          on work_episode_facts(parent_episode_id, episode_id)
      `);

      const stateColumns = new Set(
        (this.db.pragma("table_info(learning_fact_table_state)") as Array<{ name: string }>)
          .map((column) => column.name),
      );
      let requiresRecount = false;
      if (!stateColumns.has("evicted_count")) {
        this.db.exec(
          `alter table learning_fact_table_state add column evicted_count integer not null default 0`,
        );
        requiresRecount = true;
      }
      if (!stateColumns.has("maintenance_needed")) {
        this.db.exec(
          `alter table learning_fact_table_state add column maintenance_needed integer not null default 0`,
        );
        requiresRecount = true;
      }
      if (!stateColumns.has("updated_at")) {
        this.db.exec(
          `alter table learning_fact_table_state add column updated_at text not null default ''`,
        );
        requiresRecount = true;
      }
      const firstCoverageOpen = !stateColumns.has("loss_through_ms");
      if (firstCoverageOpen) {
        this.db.exec(`alter table learning_fact_table_state add column loss_through_ms integer`);
      }
      if (!stateColumns.has("loss_evicted_count")) {
        this.db.exec(`alter table learning_fact_table_state
          add column loss_evicted_count integer not null default 0`);
      }
      if (!stateColumns.has("loss_drop_count")) {
        this.db.exec(`alter table learning_fact_table_state
          add column loss_drop_count integer not null default 0`);
      }

      const triggerNames = new Set(
        (this.db.prepare(
          `select name from sqlite_master
             where type = 'trigger' and name like 'learning_fact_state_%'`,
        ).all() as Array<{ name: string }>).map((row) => row.name),
      );
      const triggersComplete = LEARNING_FACT_TABLES.every((definition) => {
        const prefix = `learning_fact_state_${definition.name}`;
        return triggerNames.has(`${prefix}_insert`) && triggerNames.has(`${prefix}_delete`);
      });
      if (!triggersComplete) requiresRecount = true;

      for (const definition of LEARNING_FACT_TABLES) {
        const existing = this.db
          .prepare(`select 1 from learning_fact_table_state where table_name = ?`)
          .get(definition.name);
        if (!existing) {
          // A zero placeholder lets the trigger set be installed before the
          // authoritative startup recount. This entire operation is one
          // transaction, so no writer can observe the placeholder.
          this.db.prepare(
            `insert into learning_fact_table_state
               (table_name, row_count, evicted_count, maintenance_needed, updated_at)
             values (?, 0, 0, 0, ?)`,
          ).run(definition.name, new Date().toISOString());
          requiresRecount = true;
        }
      }

      // State creation, trigger installation, and reconciliation deliberately
      // share this transaction. If a process dies during trigger creation,
      // SQLite rolls the whole upgrade back; the next open sees missing
      // triggers and recounts before trusting state.
      for (const definition of LEARNING_FACT_TABLES) {
        const triggerPrefix = `learning_fact_state_${definition.name}`;
        this.db.exec(`
          create trigger if not exists ${triggerPrefix}_insert
          after insert on ${definition.name}
          begin
            update learning_fact_table_state
               set row_count = row_count + 1,
                   updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             where table_name = '${definition.name}';
          end;
          create trigger if not exists ${triggerPrefix}_delete
          after delete on ${definition.name}
          begin
            update learning_fact_table_state
               set row_count = max(0, row_count - 1),
                   updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             where table_name = '${definition.name}';
          end;
        `);
      }

      if (requiresRecount) {
        for (const definition of LEARNING_FACT_TABLES) {
          const rowCount = (
            this.db
              .prepare(`select count(*) as n from ${definition.name}`)
              .get() as { n: number }
          ).n;
          const limit = this.limits[definition.limit];
          this.db.prepare(
            `update learning_fact_table_state
                set row_count = ?,
                    maintenance_needed = case when ? > ? then 1 else 0 end,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
              where table_name = ?`,
          ).run(rowCount, rowCount, limit, definition.name);
        }
      }

      // An indexed pass verifies all old/raw rows, including those that the
      // SQLite compatibility trigger gave a non-NULL but wrong key. This and
      // the subsequent trim share the IMMEDIATE transaction; no invalid row
      // can evict a valid one during startup.
      this.verifyRetentionKeys();

      // 0.7.40 counted evictions and capacity drops without the lost fact's
      // timestamp. Verify old rows first, then bound unexplained loss by both
      // this open and the newest trustworthy retained timestamp. A clock set
      // forward remains conservative; a clock set behind cannot claim facts
      // newer than the retained ledger.
      const openedAt = lossCutoffLimit();
      // An earlier build may already have persisted an unbounded cutoff.
      this.db.prepare(`update learning_fact_table_state set loss_through_ms = ?
        where loss_through_ms > ?`).run(openedAt, openedAt);
      const dropCount = capacityDropCount(this.db);
      const trackedDrops = (this.db.prepare(`select loss_drop_count as n
        from learning_fact_table_state where table_name = 'tool_attempt_facts'`
      ).get() as { n: number }).n;
      for (const definition of LEARNING_FACT_TABLES) {
        const state = this.db.prepare(`select evicted_count as evicted, loss_evicted_count as tracked
          from learning_fact_table_state where table_name = ?`
        ).get(definition.name) as { evicted: number; tracked: number };
        const unexplainedEviction = state.evicted > state.tracked;
        if (!unexplainedEviction && !(dropCount !== trackedDrops &&
          definition.name !== "technique_identity_registry")) continue;
        const newest = this.db.prepare(`select max(retention_ms) as ms from ${definition.name}`
        ).get() as { ms: number | null };
        this.advanceLossCutoff(definition,
          Math.max(openedAt, newest.ms === null ? openedAt : newest.ms + 1));
        if (unexplainedEviction) this.db.prepare(`update learning_fact_table_state
          set loss_evicted_count = evicted_count where table_name = ?`).run(definition.name);
      }
      if (dropCount !== trackedDrops) this.db.prepare(`update learning_fact_table_state
        set loss_drop_count = ? where table_name = 'tool_attempt_facts'`).run(dropCount);

      // A ledger written by an older version may already be over a configured
      // limit. Restore the hard bound before the first post-upgrade write.
      this.trimOverCapacity();
    });
    migrate.immediate();
  }

  private emptyEvictionCounts(): LearningFactEvictionCounts {
    return {
      tool_attempt_facts: 0,
      work_episode_facts: 0,
      technique_exposure_facts: 0,
      technique_identity_registry: 0,
    };
  }

  private verifyRetentionKeys() {
    for (const definition of LEARNING_FACT_TABLES) {
      const select = this.db.prepare(
        `select ${definition.idColumn} as id, ${definition.retentionColumn} as timestamp
           from ${definition.name} where retention_verified is null limit 256`,
      );
      const update = this.db.prepare(
        `update ${definition.name} set retention_ms = ?, retention_verified = 1
          where ${definition.idColumn} = ?`,
      );
      while (true) {
        const rows = select.all() as Array<{ id: string; timestamp: string }>;
        if (rows.length === 0) break;
        for (const row of rows) {
          const valid = this.unverifiedFactValid(definition, row.id);
          if (valid === null) continue; // Removed with an earlier graph root.
          const milliseconds = retentionInstant(row.timestamp);
          if (!valid || milliseconds === null) {
            // A raw legacy row can bypass fact validation. Discard its whole
            // graph, rather than letting it poison the index or abort startup.
            this.evictByIds(definition, [row.id], false);
            recordRuntimeFactDrop(this.db, "invalid_retention_timestamp");
          } else {
            update.run(milliseconds, row.id);
          }
        }
      }
    }
  }

  private unverifiedFactValid(definition: LearningFactTableDefinition, id: string): boolean | null {
    try {
      if (definition.name === "tool_attempt_facts") {
        const row = this.db.prepare(`${ATTEMPT_SELECT} where operation_id = ?`)
          .get(id) as AttemptRow | undefined;
        if (!row) return null;
        attemptFromRow(row);
      } else if (definition.name === "work_episode_facts") {
        const row = this.db.prepare(`select episode_id as episodeId, source,
          session_id as sessionId, work_class as workClass,
          complexity_band as complexityBand, parent_episode_id as parentEpisodeId,
          started_at as startedAt, ended_at as endedAt, duration_ms as durationMs
          from work_episode_facts where episode_id = ?`).get(id) as Record<string, unknown> | undefined;
        if (!row) return null;
        workEpisodeFactSchema.parse({ ...row,
          parentEpisodeId: row.parentEpisodeId ?? undefined,
          endedAt: row.endedAt ?? undefined, durationMs: row.durationMs ?? undefined });
      } else if (definition.name === "technique_exposure_facts") {
        const row = this.db.prepare(`select exposure_id as exposureId, episode_id as episodeId,
          technique_id as techniqueId, technique_version as techniqueVersion,
          content_digest as contentDigest, assignment_id as assignmentId,
          work_class as workClass, complexity_band as complexityBand,
          exposed_at as exposedAt, mode, assertion
          from technique_exposure_facts where exposure_id = ?`).get(id) as Record<string, unknown> | undefined;
        if (!row) return null;
        techniqueExposureFactSchema.parse({ ...row,
          techniqueVersion: row.techniqueVersion ?? undefined,
          contentDigest: row.contentDigest ?? undefined });
      } else {
        const exists = this.db.prepare(`select 1 from technique_identity_registry where technique_key = ?`).get(id);
        if (!exists) return null;
      }
      return true;
    } catch {
      return false;
    }
  }

  private verifyPendingRetentionKeys() {
    if (!LEARNING_FACT_TABLES.some((definition) => this.db.prepare(
      `select 1 from ${definition.name} where retention_verified is null limit 1`,
    ).get())) return;
    const reconcile = this.db.transaction(() => {
      this.verifyRetentionKeys();
    });
    reconcile.immediate();
  }

  private markMaintenance(definition: LearningFactTableDefinition) {
    const limit = this.limits[definition.limit];
    this.db.prepare(
      `update learning_fact_table_state
          set maintenance_needed = case when row_count > ? then 1 else 0 end,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        where table_name = ?`,
    ).run(limit, definition.name);
  }

  private addEvictionCounts(counts: LearningFactEvictionCounts) {
    for (const definition of LEARNING_FACT_TABLES) {
      const evicted = counts[definition.name];
      if (evicted > 0) {
        this.db.prepare(
          `update learning_fact_table_state
              set evicted_count = evicted_count + ?,
                  loss_evicted_count = evicted_count + ?,
                  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            where table_name = ?`,
        ).run(evicted, evicted, definition.name);
      }
      if (evicted > 0) this.markMaintenance(definition);
    }
  }

  private advanceLossCutoff(definition: LearningFactTableDefinition, throughMs: number | null) {
    if (throughMs === null) return;
    const boundedThroughMs = Math.min(throughMs, lossCutoffLimit());
    this.db.prepare(`update learning_fact_table_state
      set loss_through_ms = max(coalesce(loss_through_ms, ?), ?)
      where table_name = ?`).run(boundedThroughMs, boundedThroughMs, definition.name);
  }

  private syncCapacityDropCount() {
    this.db.prepare(`update learning_fact_table_state set loss_drop_count = ?
      where table_name = 'tool_attempt_facts'`).run(capacityDropCount(this.db));
  }

  private noteAttemptLostToEvictedReference(
    referenceTable: "tool_attempt_facts" | "work_episode_facts", startedAt: string,
  ): boolean {
    const state = this.db.prepare(`select evicted_count as n from learning_fact_table_state
      where table_name = ?`).get(referenceTable) as { n: number } | undefined;
    if (!state?.n) return false;
    const at = retentionInstant(startedAt);
    if (at === null) return false;
    this.advanceLossCutoff(LEARNING_FACT_TABLES[0]!, at + 1);
    return true;
  }

  private deleteEpisodeGraph(rootIds: string[], countEviction = true): LearningFactEvictionCounts {
    const counts = this.emptyEvictionCounts();
    const episodeIds = new Set(rootIds);
    const pending = [...rootIds];
    const findChildren = this.db.prepare(
      `select episode_id as episodeId from work_episode_facts where parent_episode_id = ?`,
    );
    while (pending.length > 0) {
      const parentId = pending.pop()!;
      const children = findChildren.all(parentId) as Array<{ episodeId: string }>;
      for (const child of children) {
        if (episodeIds.has(child.episodeId)) continue;
        episodeIds.add(child.episodeId);
        pending.push(child.episodeId);
      }
    }

    const removeAttempts = this.db.prepare(`delete from tool_attempt_facts where episode_id = ?`);
    const removeExposures = this.db.prepare(`delete from technique_exposure_facts where episode_id = ?`);
    const removeEpisodes = this.db.prepare(`delete from work_episode_facts where episode_id = ?`);
    // Remove dependent facts and child episodes in the same transaction.
    for (const episodeId of [...episodeIds].reverse()) {
      for (const definition of LEARNING_FACT_TABLES.slice(0, 3)) {
        if (countEviction) {
          const last = this.db.prepare(`select max(retention_ms) as ms from ${definition.name}
            where episode_id = ?`).get(episodeId) as { ms: number | null };
          this.advanceLossCutoff(definition, last.ms === null ? null : last.ms + 1);
        } else {
          // Invalid raw roots may have provisional or missing retention keys.
          // Preserve the timestamps of valid dependents before deleting them.
          const rows = this.db.prepare(`select ${definition.retentionColumn} as timestamp,
            retention_ms as ms from ${definition.name} where episode_id = ?`
          ).all(episodeId) as Array<{ timestamp: string; ms: number | null }>;
          let newestDeleted: number | null = null;
          for (const row of rows) {
            const at = retentionInstant(row.timestamp) ?? row.ms ?? Date.now();
            newestDeleted = Math.max(newestDeleted ?? at, at);
          }
          this.advanceLossCutoff(definition, newestDeleted === null ? null : newestDeleted + 1);
        }
      }
      counts.tool_attempt_facts += removeAttempts.run(episodeId).changes;
      counts.technique_exposure_facts += removeExposures.run(episodeId).changes;
      counts.work_episode_facts += removeEpisodes.run(episodeId).changes;
    }
    if (countEviction) this.addEvictionCounts(counts);
    else for (const definition of LEARNING_FACT_TABLES) this.markMaintenance(definition);
    return counts;
  }

  private evictOldest(
    definition: LearningFactTableDefinition,
    requested: number,
  ): LearningFactEvictionCounts {
    if (requested <= 0) return this.emptyEvictionCounts();
    const ids = this.db.prepare(
      `select ${definition.idColumn} as id
         from ${definition.name}
        order by retention_ms, ${definition.idColumn}
        limit ?`,
    ).all(requested) as Array<{ id: string }>;
    return this.evictByIds(definition, ids.map((row) => row.id));
  }

  private evictByIds(
    definition: LearningFactTableDefinition,
    victimIds: string[],
    countEviction = true,
  ): LearningFactEvictionCounts {
    const counts = this.emptyEvictionCounts();
    if (victimIds.length === 0) return counts;
    if (definition.name === "work_episode_facts") {
      return this.deleteEpisodeGraph(victimIds, countEviction);
    }
    const ids = victimIds.map((id) => ({ id }));
    // Retry links are local references too. Evict descendants of a removed
    // attempt using the retry index, never leave a dangling retained retry.
    if (definition.name === "tool_attempt_facts") {
      const seen = new Set(ids.map((row) => row.id));
      const children = this.db.prepare(`select operation_id as id from tool_attempt_facts where retry_of = ?`);
      for (let index = 0; index < ids.length; index += 1) {
        for (const child of children.all(ids[index].id) as Array<{ id: string }>) {
          if (!seen.has(child.id)) { seen.add(child.id); ids.push(child); }
        }
      }
    }
    const remove = this.db.prepare(
      `delete from ${definition.name} where ${definition.idColumn} = ?`,
    );
    const readRetention = this.db.prepare(countEviction
      ? `select retention_ms as ms from ${definition.name} where ${definition.idColumn} = ?`
      : `select retention_ms as ms, ${definition.retentionColumn} as timestamp
        from ${definition.name} where ${definition.idColumn} = ?`);
    let lastDeletedMs: number | null = null;
    for (const row of ids) {
      const victim = readRetention.get(row.id) as { ms: number | null; timestamp?: string } | undefined;
      if (victim) {
        const at = countEviction ? victim.ms :
          retentionInstant(victim.timestamp!) ?? victim.ms ?? Date.now();
        if (at !== null) lastDeletedMs = Math.max(lastDeletedMs ?? at, at);
      }
      counts[definition.name] += remove.run(row.id).changes;
    }
    this.advanceLossCutoff(definition, lastDeletedMs === null ? null : lastDeletedMs + 1);
    if (countEviction) this.addEvictionCounts(counts);
    else for (const table of LEARNING_FACT_TABLES) this.markMaintenance(table);
    return counts;
  }

  private trimOverCapacity() {
    for (const definition of LEARNING_FACT_TABLES) {
      const state = this.db.prepare(
        `select row_count as rowCount from learning_fact_table_state where table_name = ?`,
      ).get(definition.name) as { rowCount: number } | undefined;
      if (!state) throw new Error(`LearningFactStateMissing:${definition.name}`);
      const over = state.rowCount - this.limits[definition.limit];
      if (over > 0) this.evictOldest(definition, over);
      this.markMaintenance(definition);
    }
  }

  private evictionWouldRemoveRequired(
    table: "tool_attempt_facts" | "work_episode_facts",
    victimId: string,
    requiredId: string,
  ): boolean {
    // Follow only the graph that this victim's eviction would delete. An
    // ancestor/retry target anywhere in that graph must survive if the new
    // dependent is to be admitted. Both child lookups use existing indexes.
    const children = table === "work_episode_facts"
      ? this.db.prepare(`select episode_id as id from work_episode_facts where parent_episode_id = ?`)
      : this.db.prepare(`select operation_id as id from tool_attempt_facts where retry_of = ?`);
    const pending = [victimId];
    const seen = new Set<string>();
    while (pending.length > 0) {
      const id = pending.pop()!;
      if (id === requiredId) return true;
      if (seen.has(id)) continue;
      seen.add(id);
      for (const child of children.all(id) as Array<{ id: string }>) pending.push(child.id);
    }
    return false;
  }

  private capacityPressure(
    table: LearningFactTableName,
    id: string,
    retentionMs: number,
    requiredId?: string,
  ): CapacityDecision {
    const definition = LEARNING_FACT_TABLES.find((entry) => entry.name === table)!;
    const existing = this.db
      .prepare(`select 1 from ${definition.name} where ${definition.idColumn} = ?`)
      .get(id);
    if (existing) return "admit";
    const state = this.db
      .prepare(
        `select row_count as rowCount
           from learning_fact_table_state where table_name = ?`,
      )
      .get(definition.name) as { rowCount: number } | undefined;
    if (!state) throw new Error(`LearningFactStateMissing:${definition.name}`);
    const limit = this.limits[definition.limit];
    if (state.rowCount > limit) {
      // Plan the existing overflow eviction before deleting anything. Its
      // cascade can remove the incoming fact's required target, or expose a
      // newer oldest row that would reject the candidate on retention order.
      const roots = `select ${definition.idColumn} as id from ${definition.name}
        order by retention_ms, ${definition.idColumn} limit ?`;
      const parentColumn = table === "tool_attempt_facts" ? "retry_of"
        : table === "work_episode_facts" ? "parent_episode_id" : null;
      const victims = new Set((this.db.prepare(parentColumn
        ? `with recursive roots(id) as (${roots}), victims(id) as (
             select id from roots union
             select child.${definition.idColumn} from ${definition.name} child
               join victims on child.${parentColumn} = victims.id
           ) select id from victims`
        : roots).all(state.rowCount - limit) as Array<{ id: string }>).map((row) => row.id));
      if (requiredId && victims.has(requiredId)) {
        this.advanceLossCutoff(definition, retentionMs + 1);
        return "required_reference";
      }
      if (state.rowCount - victims.size === limit) {
        const retained = this.db.prepare(
          `select ${definition.idColumn} as id, retention_ms as retentionMs
             from ${definition.name} order by retention_ms, ${definition.idColumn}`,
        );
        let rejected: CapacityDecision | null = null;
        for (const row of retained.iterate() as Iterable<{ id: string; retentionMs: number }>) {
          if (victims.has(row.id)) continue;
          if (retentionMs < row.retentionMs || (retentionMs === row.retentionMs && id < row.id)) {
            rejected = "outside_retention_window";
            break;
          }
          if (requiredId && (table === "tool_attempt_facts" || table === "work_episode_facts") &&
              this.evictionWouldRemoveRequired(table, row.id, requiredId)) {
            rejected = "required_reference";
            break;
          }
          break;
        }
        if (rejected) {
          this.advanceLossCutoff(definition, retentionMs + 1);
          return rejected;
        }
      }
      // Only an admissible candidate pays for overflow repair. Reuse the same
      // oldest roots and deletion accounting as maintenance and normal writes.
      this.evictOldest(definition, state.rowCount - limit);
      state.rowCount = (this.db.prepare(
        `select row_count as rowCount from learning_fact_table_state where table_name = ?`,
      ).get(definition.name) as { rowCount: number }).rowCount;
    }
    if (state.rowCount < limit) return "admit";
    const oldest = this.db.prepare(
      `select ${definition.idColumn} as id, retention_ms as retentionMs
         from ${definition.name}
        order by retention_ms, ${definition.idColumn}
        limit 1`,
    ).get() as { id: string; retentionMs: number } | undefined;
    if (!oldest) throw new Error(`LearningFactStateMismatch:${definition.name}`);
    // Retain the greatest (UTC millisecond, identity) tuple. Comparing raw ISO
    // strings would misorder offset-bearing timestamps at day boundaries.
    if (retentionMs < oldest.retentionMs ||
        (retentionMs === oldest.retentionMs && id < oldest.id)) {
      this.advanceLossCutoff(definition, retentionMs + 1);
      return "outside_retention_window";
    }
    if (requiredId &&
        (table === "tool_attempt_facts" || table === "work_episode_facts") &&
        this.evictionWouldRemoveRequired(table, oldest.id, requiredId)) {
      this.advanceLossCutoff(definition, retentionMs + 1);
      return "required_reference";
    }
    this.evictOldest(definition, 1);
    return "admit";
  }

  private dropFact<T>(reason: RuntimeFactDropReason = "stale_reference"): LearningFactWriteResult<T> {
    recordRuntimeFactDrop(this.db, reason);
    if (reason === "outside_retention_window" || reason === "protected_reference_at_capacity") {
      this.syncCapacityDropCount();
    }
    return { inserted: false, fact: null, dropped: true, dropReason: reason };
  }

  recordToolSignal(input: unknown): LearningFactWriteResult<ToolAttemptFact> {
    const signal = input as ToolAttemptSignal;
    return this.db.transaction(() => {
      this.verifyPendingRetentionKeys();
      if (signal?.kind === "attempt") {
        const start = toolAttemptStartSignalSchema.parse(signal);
        const existing = this.db
          .prepare(`${ATTEMPT_SELECT} where operation_id = ?`)
          .get(start.operationId) as AttemptRow | undefined;
        if (existing) {
          const fact = attemptFromRow(existing);
          const same =
            fact.source === start.source &&
            fact.sessionId === start.sessionId &&
            fact.episodeId === start.episodeId &&
            fact.toolClass === start.toolClass &&
            fact.toolName === start.toolName &&
            fact.startedAt === start.startedAt &&
            fact.retryOf === start.retryOf;
          if (!same) throw new Error("ToolAttemptIdentityConflict");
          return { inserted: false, fact };
        }
        if (start.episodeId) {
          const episode = this.db
            .prepare(
              `select source, session_id as sessionId, started_at as startedAt,
                 ended_at as endedAt
               from work_episode_facts where episode_id = ?`,
            )
            .get(start.episodeId) as {
              source: ToolSource;
              sessionId: string;
              startedAt: string;
              endedAt: string | null;
            } | undefined;
          if (!episode) {
            this.noteAttemptLostToEvictedReference("work_episode_facts", start.startedAt);
            return this.dropFact<ToolAttemptFact>();
          }
          if (episode.source !== start.source || episode.sessionId !== start.sessionId) {
            throw new Error("ToolAttemptEpisodeIdentityConflict");
          }
          if (Date.parse(start.startedAt) < Date.parse(episode.startedAt)) {
            throw new Error("ToolAttemptPrecedesEpisodeStart");
          }
          if (
            episode.endedAt !== null &&
            Date.parse(start.startedAt) > Date.parse(episode.endedAt)
          ) {
            throw new Error("ToolAttemptStartsAfterEpisodeEnd");
          }
        }
        if (start.retryOf) {
          const retryTarget = this.db
            .prepare(`${ATTEMPT_SELECT} where operation_id = ?`)
            .get(start.retryOf) as AttemptRow | undefined;
          if (!retryTarget) {
            if (this.noteAttemptLostToEvictedReference("tool_attempt_facts", start.startedAt)) {
              return this.dropFact<ToolAttemptFact>("retry_target_missing");
            }
            throw new Error("ToolAttemptRetryTargetMissing");
          }
          const target = attemptFromRow(retryTarget);
          if (
            target.source !== start.source ||
            target.sessionId !== start.sessionId ||
            target.episodeId !== start.episodeId ||
            Date.parse(target.startedAt) > Date.parse(start.startedAt)
          ) {
            throw new Error("ToolAttemptRetryTargetConflict");
          }
        }
        const retentionMs = retentionInstant(start.startedAt);
        if (retentionMs === null) return this.dropFact<ToolAttemptFact>("invalid_retention_timestamp");
        const admission = this.capacityPressure(
          "tool_attempt_facts", start.operationId, retentionMs, start.retryOf,
        );
        if (admission !== "admit") {
          return this.dropFact<ToolAttemptFact>(
            admission === "required_reference" ? "protected_reference_at_capacity" : "outside_retention_window",
          );
        }
        if (start.retryOf && !this.db.prepare(
          `select 1 from tool_attempt_facts where operation_id = ?`,
        ).get(start.retryOf)) {
          // Defensive invariant: an unexpected trigger must roll back this
          // transaction's evictions, never commit a dropped fact's net loss.
          throw new Error("ToolAttemptRetryTargetMissing");
        }
        const now = new Date().toISOString();
        this.db.prepare(
          `insert into tool_attempt_facts
            (operation_id, source, session_id, episode_id, tool_class, tool_name,
             started_at, ended_at, duration_ms, result_status, error_category,
             retry_of, created_at, updated_at, retention_ms, retention_verified)
           values
            (@operationId, @source, @sessionId, @episodeId, @toolClass, @toolName,
             @startedAt, null, null, 'unknown', 'unknown', @retryOf, @now, @now, @retentionMs, 1)`,
        ).run({
          ...start,
          episodeId: start.episodeId ?? null,
          retryOf: start.retryOf ?? null,
          now,
          retentionMs,
        });
        const row = this.db
          .prepare(`${ATTEMPT_SELECT} where operation_id = ?`)
          .get(start.operationId) as AttemptRow;
        return { inserted: true, fact: attemptFromRow(row) };
      }

      const result = toolAttemptResultSignalSchema.parse(signal);
      const existing = this.db
        .prepare(`${ATTEMPT_SELECT} where operation_id = ?`)
        .get(result.operationId) as AttemptRow | undefined;
      if (!existing) return this.dropFact<ToolAttemptFact>("unpaired_result");
      const current = attemptFromRow(existing);
      if (current.source !== result.source || current.sessionId !== result.sessionId) {
        throw new Error("ToolAttemptResultIdentityConflict");
      }
      const durationMs = Date.parse(result.endedAt) - Date.parse(current.startedAt);
      if (durationMs < 0) throw new Error("ToolAttemptResultPrecedesStart");
      const resultStatus = result.resultStatus;
      const errorCategory =
        resultStatus === "success"
          ? "none"
          : resultStatus === "failure"
            ? result.errorCategory ?? "unknown"
            : "unknown";
      if (current.episodeId) {
        const episode = this.db
          .prepare(
            `select ended_at as endedAt from work_episode_facts where episode_id = ?`,
          )
          .get(current.episodeId) as { endedAt: string | null } | undefined;
        if (!episode) return this.dropFact<ToolAttemptFact>();
        if (
          episode.endedAt !== null &&
          Date.parse(result.endedAt) > Date.parse(episode.endedAt)
        ) {
          throw new Error("ToolAttemptResultAfterEpisodeEnd");
        }
      }
      const completed = toolAttemptFactSchema.parse({
        ...current,
        endedAt: result.endedAt,
        durationMs,
        resultStatus,
        errorCategory,
      });
      if (current.endedAt) {
        if (JSON.stringify(current) !== JSON.stringify(completed)) {
          throw new Error("ToolAttemptResultConflict");
        }
        return { inserted: false, fact: current };
      }
      this.db.prepare(
        `update tool_attempt_facts set
           ended_at = @endedAt,
           duration_ms = @durationMs,
           result_status = @resultStatus,
           error_category = @errorCategory,
           updated_at = @updatedAt
         where operation_id = @operationId`,
      ).run({ ...completed, updatedAt: new Date().toISOString() });
      return { inserted: false, fact: completed };
    })();
  }

  recordWorkEpisode(input: unknown): LearningFactWriteResult<WorkEpisodeFact> {
    const fact = workEpisodeFactSchema.parse(input);
    return this.db.transaction(() => {
      this.verifyPendingRetentionKeys();
      if (fact.parentEpisodeId) {
        const parent = this.db
          .prepare(
            `select source, session_id as sessionId, started_at as startedAt
             from work_episode_facts where episode_id = ?`,
          )
          .get(fact.parentEpisodeId) as {
            source: ToolSource;
            sessionId: string;
            startedAt: string;
          } | undefined;
        if (!parent) return this.dropFact<WorkEpisodeFact>();
        if (parent.source !== fact.source || parent.sessionId !== fact.sessionId) {
          throw new Error("WorkEpisodeParentIdentityConflict");
        }
        if (Date.parse(parent.startedAt) > Date.parse(fact.startedAt)) {
          throw new Error("WorkEpisodeParentPrecedesChildStart");
        }
      }
      const row = this.db
        .prepare(
          `select episode_id as episodeId, source, session_id as sessionId,
             work_class as workClass, complexity_band as complexityBand,
             parent_episode_id as parentEpisodeId,
             started_at as startedAt, ended_at as endedAt, duration_ms as durationMs
           from work_episode_facts where episode_id = ?`,
        )
        .get(fact.episodeId) as (Omit<WorkEpisodeFact, "endedAt" | "durationMs"> & {
          endedAt: string | null;
          durationMs: number | null;
        }) | undefined;
      if (row) {
        const existing = workEpisodeFactSchema.parse({
          ...row,
          parentEpisodeId: row.parentEpisodeId ?? undefined,
          endedAt: row.endedAt ?? undefined,
          durationMs: row.durationMs ?? undefined,
        });
        if (JSON.stringify(existing) !== JSON.stringify(fact)) {
          throw new Error("WorkEpisodeIdentityConflict");
        }
        return { inserted: false, fact: existing };
      }
      const retentionMs = retentionInstant(fact.startedAt);
      if (retentionMs === null) return this.dropFact<WorkEpisodeFact>("invalid_retention_timestamp");
      const admission = this.capacityPressure(
        "work_episode_facts", fact.episodeId, retentionMs, fact.parentEpisodeId,
      );
      if (admission !== "admit") {
        return this.dropFact<WorkEpisodeFact>(
          admission === "required_reference" ? "protected_reference_at_capacity" : "outside_retention_window",
        );
      }
      if (fact.parentEpisodeId) {
        const parentStillExists = this.db
          .prepare(`select 1 from work_episode_facts where episode_id = ?`)
          .get(fact.parentEpisodeId);
        // The parent may itself have been the oldest graph root. Do not write
        // a child that would be orphaned by the same bounded eviction.
        if (!parentStillExists) throw new Error("WorkEpisodeParentMissing");
      }
      this.db.prepare(
        `insert into work_episode_facts
          (episode_id, source, session_id, work_class, complexity_band,
           parent_episode_id, started_at, ended_at, duration_ms, created_at,
           retention_ms, retention_verified)
         values
          (@episodeId, @source, @sessionId, @workClass, @complexityBand,
           @parentEpisodeId, @startedAt, @endedAt, @durationMs, @createdAt, @retentionMs, 1)`,
      ).run({
        ...fact,
        parentEpisodeId: fact.parentEpisodeId ?? null,
        endedAt: fact.endedAt ?? null,
        durationMs: fact.durationMs ?? null,
        createdAt: new Date().toISOString(),
        retentionMs,
      });
      return { inserted: true, fact };
    })();
  }

  recordTechniqueExposure(
    input: unknown,
    options: { outcomeObservedAt?: string } = {},
  ): LearningFactWriteResult<TechniqueExposureFact> {
    const fact = validateTechniqueExposureFactIdentity(input);
    return this.db.transaction(() => {
      this.verifyPendingRetentionKeys();
      if (options.outcomeObservedAt !== undefined) {
        const outcomeMs = Date.parse(options.outcomeObservedAt);
        if (
          Number.isNaN(outcomeMs) ||
          !/(?:Z|[+-]\d{2}:\d{2})$/.test(options.outcomeObservedAt)
        ) {
          throw new Error("outcomeObservedAt must be an ISO timestamp with timezone");
        }
        if (Date.parse(fact.exposedAt) > outcomeMs) {
          throw new Error("RetrospectiveTechniqueExposureRejected");
        }
      }
      const episode = this.db
        .prepare(
          `select work_class as workClass, complexity_band as complexityBand,
             started_at as startedAt, ended_at as endedAt
           from work_episode_facts where episode_id = ?`,
        )
        .get(fact.episodeId) as {
          workClass: WorkClass;
          complexityBand: WorkComplexityBand;
          startedAt: string;
          endedAt: string | null;
        } | undefined;
      if (!episode) return this.dropFact<TechniqueExposureFact>();
      if (
        episode.workClass !== fact.workClass ||
        episode.complexityBand !== fact.complexityBand
      ) {
        throw new Error("TechniqueExposureEpisodeDimensionsConflict");
      }
      const exposureMs = Date.parse(fact.exposedAt);
      if (!Number.isFinite(exposureMs)) return this.dropFact<TechniqueExposureFact>("invalid_retention_timestamp");
      if (
        exposureMs < Date.parse(episode.startedAt) ||
        (episode.endedAt !== null && exposureMs > Date.parse(episode.endedAt))
      ) {
        throw new Error("TechniqueExposureOutsideEpisode");
      }
      const existing = this.db
        .prepare(
          `select exposure_id as exposureId, episode_id as episodeId,
             technique_id as techniqueId, technique_version as techniqueVersion,
             content_digest as contentDigest, assignment_id as assignmentId,
             work_class as workClass, complexity_band as complexityBand,
             exposed_at as exposedAt, mode, assertion
           from technique_exposure_facts where exposure_id = ?`,
        )
        .get(fact.exposureId) as Record<string, unknown> | undefined;
      if (existing) {
        const stored = techniqueExposureFactSchema.parse({
          ...existing,
          techniqueVersion: existing.techniqueVersion ?? undefined,
          contentDigest: existing.contentDigest ?? undefined,
        });
        if (JSON.stringify(stored) !== JSON.stringify(fact)) {
          throw new Error("TechniqueExposureIdentityConflict");
        }
        return { inserted: false, fact: stored };
      }
      const techniqueKey = deterministicLearningFactId([
        fact.techniqueId,
        fact.techniqueVersion ?? "",
        fact.contentDigest ?? "",
      ]);
      const now = new Date().toISOString();
      const nowMs = retentionInstant(now);
      if (nowMs === null) return this.dropFact<TechniqueExposureFact>("invalid_retention_timestamp");
      if (this.capacityPressure("technique_exposure_facts", fact.exposureId, exposureMs) !== "admit") {
        return this.dropFact<TechniqueExposureFact>("outside_retention_window");
      }
      if (this.capacityPressure("technique_identity_registry", techniqueKey, nowMs) === "admit") {
        this.db.prepare(
        `insert or ignore into technique_identity_registry
          (technique_key, technique_id, technique_version, content_digest, first_seen_at,
           retention_ms, retention_verified)
         values (@techniqueKey, @techniqueId, @techniqueVersion, @contentDigest,
           @now, @nowMs, 1)`,
        ).run({
          techniqueKey,
          techniqueId: fact.techniqueId,
          techniqueVersion: fact.techniqueVersion ?? null,
          contentDigest: fact.contentDigest ?? null,
          now,
          nowMs,
        });
      } else {
        recordRuntimeFactDrop(this.db, "outside_retention_window");
        this.syncCapacityDropCount();
      }
      this.db.prepare(
        `insert into technique_exposure_facts
          (exposure_id, episode_id, technique_id, technique_version, content_digest,
           assignment_id, work_class, complexity_band, exposed_at, mode, assertion,
           created_at, retention_ms, retention_verified)
         values
          (@exposureId, @episodeId, @techniqueId, @techniqueVersion, @contentDigest,
           @assignmentId, @workClass, @complexityBand, @exposedAt, @mode, @assertion,
           @createdAt, @exposureMs, 1)`,
      ).run({
        ...fact,
        techniqueVersion: fact.techniqueVersion ?? null,
        contentDigest: fact.contentDigest ?? null,
        createdAt: now,
        exposureMs,
      });
      return { inserted: true, fact };
    })();
  }

  status(): LearningFactStatus {
    const stateRows = this.db.prepare(
      `select table_name as tableName, row_count as rowCount,
          evicted_count as evictedCount, maintenance_needed as maintenanceNeeded
         from learning_fact_table_state order by table_name`,
    ).all() as Array<{
      tableName: LearningFactTableName;
      rowCount: number;
      evictedCount: number;
      maintenanceNeeded: number;
    }>;
    const states = new Map(stateRows.map((row) => [row.tableName, row]));
    const tables = {} as Record<LearningFactTableName, LearningFactTableStatus>;
    let totalRows = 0;
    let totalLimit = 0;
    let totalEvicted = 0;
    let maintenanceNeeded = false;
    for (const definition of LEARNING_FACT_TABLES) {
      const row = states.get(definition.name);
      if (!row) throw new Error(`LearningFactStateMissing:${definition.name}`);
      const limit = this.limits[definition.limit];
      const needsMaintenance = row.maintenanceNeeded === 1 || row.rowCount > limit;
      tables[definition.name] = {
        rowCount: row.rowCount,
        limit,
        fillRatio: row.rowCount / limit,
        evictedCount: row.evictedCount,
        maintenanceNeeded: needsMaintenance,
      };
      totalRows += row.rowCount;
      totalLimit += limit;
      totalEvicted += row.evictedCount;
      maintenanceNeeded ||= needsMaintenance;
    }
    return { tables, totalRows, totalLimit, totalEvicted, maintenanceNeeded };
  }

  statusWithWindow(until = new Date().toISOString(), days = 7): LearningFactStatus & {
    analysisWindow: LearningFactWindow;
  } {
    return this.db.transaction(() => ({ ...this.status(),
      analysisWindow: readLearningFactWindow(this.db, until, days) }))();
  }

  /**
   * Fair repair for overflow left by older writers or changed limits. Each
   * table gets a bounded root budget; deleting an episode/retry also removes
   * its dependent facts atomically. Normal writes already enforce the cap.
   */
  runMaintenance(
    maxRows = DEFAULT_LEARNING_FACT_MAINTENANCE_BATCH,
  ): LearningFactMaintenanceResult {
    const requested = Number.isSafeInteger(maxRows) && maxRows > 0
      ? Math.min(maxRows, 4_096)
      : DEFAULT_LEARNING_FACT_MAINTENANCE_BATCH;
    return this.db.transaction(() => {
      this.verifyPendingRetentionKeys();
      // The budget is per table. A shared counter lets a sustained attempt
      // backlog starve the smaller episode/exposure/identity tables; bounded
      // per-table turns guarantee progress for every over-cap table.
      const evictedByTable = this.emptyEvictionCounts();
      let selectedRoots = 0;
      for (const definition of LEARNING_FACT_TABLES) {
        const state = this.db.prepare(
          `select row_count as rowCount from learning_fact_table_state where table_name = ?`,
        ).get(definition.name) as { rowCount: number } | undefined;
        if (!state) throw new Error(`LearningFactStateMissing:${definition.name}`);
        const limit = this.limits[definition.limit];
        const over = Math.max(0, state.rowCount - limit);
        if (over > 0) {
          const roots = Math.min(over, requested);
          selectedRoots += roots;
          const counts = this.evictOldest(definition, roots);
          for (const table of LEARNING_FACT_TABLES) {
            evictedByTable[table.name] += counts[table.name];
          }
        }
        this.markMaintenance(definition);
      }
      const tables = LEARNING_FACT_TABLES.map((definition) => {
        const after = this.db.prepare(
          `select row_count as rowCount from learning_fact_table_state where table_name = ?`,
        ).get(definition.name) as { rowCount: number };
        return {
          table: definition.name,
          evicted: evictedByTable[definition.name],
          rowCount: after.rowCount,
          limit: this.limits[definition.limit],
        };
      });
      const evicted = tables.reduce((total, table) => total + table.evicted, 0);
      return {
        requested,
        selectedRoots,
        evicted,
        // `requested` bounds the number of selected roots per table. Episode
        // graph cleanup can additionally remove related rows, which is
        // intentionally included in the aggregate receipt and counters.
        bounded: selectedRoots <= requested * LEARNING_FACT_TABLES.length,
        tables,
      };
    })();
  }

  attempts(): ToolAttemptFact[] {
    this.verifyPendingRetentionKeys();
    return (
      this.db
        .prepare(`${ATTEMPT_SELECT} order by retention_ms, operation_id`)
        .all() as AttemptRow[]
    ).map(attemptFromRow);
  }

  episodes(): WorkEpisodeFact[] {
    this.verifyPendingRetentionKeys();
    return (
      this.db.prepare(
        `select episode_id as episodeId, source, session_id as sessionId,
           work_class as workClass, complexity_band as complexityBand,
           parent_episode_id as parentEpisodeId,
           started_at as startedAt, ended_at as endedAt, duration_ms as durationMs
         from work_episode_facts order by retention_ms, episode_id`,
      ).all() as Array<Record<string, unknown>>
    ).map((row) =>
      workEpisodeFactSchema.parse({
        ...row,
        parentEpisodeId: row.parentEpisodeId ?? undefined,
        endedAt: row.endedAt ?? undefined,
        durationMs: row.durationMs ?? undefined,
      }),
    );
  }

  episodeById(episodeId: string): WorkEpisodeFact | undefined {
    this.verifyPendingRetentionKeys();
    const row = this.db
      .prepare(
        `select episode_id as episodeId, source, session_id as sessionId,
           work_class as workClass, complexity_band as complexityBand,
           parent_episode_id as parentEpisodeId,
           started_at as startedAt, ended_at as endedAt, duration_ms as durationMs
         from work_episode_facts where episode_id = ?`,
      )
      .get(episodeId) as Record<string, unknown> | undefined;
    return row
      ? workEpisodeFactSchema.parse({
          ...row,
          parentEpisodeId: row.parentEpisodeId ?? undefined,
          endedAt: row.endedAt ?? undefined,
          durationMs: row.durationMs ?? undefined,
        })
      : undefined;
  }

  exposures(): TechniqueExposureFact[] {
    this.verifyPendingRetentionKeys();
    return (
      this.db.prepare(
        `select exposure_id as exposureId, episode_id as episodeId,
           technique_id as techniqueId, technique_version as techniqueVersion,
           content_digest as contentDigest, assignment_id as assignmentId,
           work_class as workClass, complexity_band as complexityBand,
           exposed_at as exposedAt, mode, assertion
         from technique_exposure_facts order by retention_ms, exposure_id`,
      ).all() as Array<Record<string, unknown>>
    ).map((row) =>
      techniqueExposureFactSchema.parse({
        ...row,
        techniqueVersion: row.techniqueVersion ?? undefined,
        contentDigest: row.contentDigest ?? undefined,
      }),
    );
  }
}
