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
                  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            where table_name = ?`,
        ).run(evicted, definition.name);
      }
      if (evicted > 0) this.markMaintenance(definition);
    }
  }

  private deleteEpisodeGraph(rootIds: string[]): LearningFactEvictionCounts {
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
      counts.tool_attempt_facts += removeAttempts.run(episodeId).changes;
      counts.technique_exposure_facts += removeExposures.run(episodeId).changes;
      counts.work_episode_facts += removeEpisodes.run(episodeId).changes;
    }
    this.addEvictionCounts(counts);
    return counts;
  }

  private evictOldest(
    definition: LearningFactTableDefinition,
    requested: number,
  ): LearningFactEvictionCounts {
    const counts = this.emptyEvictionCounts();
    if (requested <= 0) return counts;
    const ids = this.db.prepare(
      `select ${definition.idColumn} as id
         from ${definition.name}
        order by ${definition.retentionColumn}, ${definition.idColumn}
        limit ?`,
    ).all(requested) as Array<{ id: string }>;
    if (definition.name === "work_episode_facts") {
      return this.deleteEpisodeGraph(ids.map((row) => row.id));
    }
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
    for (const row of ids) counts[definition.name] += remove.run(row.id).changes;
    this.addEvictionCounts(counts);
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

  private capacityPressure(
    table: LearningFactTableName,
    id: string,
    retentionTimestamp: string,
  ): boolean {
    const definition = LEARNING_FACT_TABLES.find((entry) => entry.name === table)!;
    const existing = this.db
      .prepare(`select 1 from ${definition.name} where ${definition.idColumn} = ?`)
      .get(id);
    if (existing) return true;
    const state = this.db
      .prepare(
        `select row_count as rowCount
           from learning_fact_table_state where table_name = ?`,
      )
      .get(definition.name) as { rowCount: number } | undefined;
    if (!state) throw new Error(`LearningFactStateMissing:${definition.name}`);
    const limit = this.limits[definition.limit];
    if (state.rowCount > limit) {
      // A legacy/raw writer can leave overflow. Repair only those rows before
      // comparing the new candidate, so even a rejected write leaves a bound.
      this.evictOldest(definition, state.rowCount - limit);
      state.rowCount = (this.db.prepare(
        `select row_count as rowCount from learning_fact_table_state where table_name = ?`,
      ).get(definition.name) as { rowCount: number }).rowCount;
    }
    if (state.rowCount < limit) return true;
    const oldest = this.db.prepare(
      `select ${definition.idColumn} as id, ${definition.retentionColumn} as retentionTimestamp
         from ${definition.name}
        order by ${definition.retentionColumn}, ${definition.idColumn}
        limit 1`,
    ).get() as { id: string; retentionTimestamp: string } | undefined;
    if (!oldest) throw new Error(`LearningFactStateMismatch:${definition.name}`);
    // These persisted identifiers and ISO timestamp strings use SQLite BINARY
    // ordering. A full table keeps the greater (timestamp, identity) tuple.
    if (retentionTimestamp < oldest.retentionTimestamp ||
        (retentionTimestamp === oldest.retentionTimestamp && id < oldest.id)) {
      return false;
    }
    this.evictOldest(definition, 1);
    return true;
  }

  private dropFact<T>(reason: RuntimeFactDropReason = "stale_reference"): LearningFactWriteResult<T> {
    recordRuntimeFactDrop(this.db, reason);
    return { inserted: false, fact: null, dropped: true, dropReason: reason };
  }

  recordToolSignal(input: unknown): LearningFactWriteResult<ToolAttemptFact> {
    const signal = input as ToolAttemptSignal;
    return this.db.transaction(() => {
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
          if (!episode) return this.dropFact<ToolAttemptFact>();
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
          if (!retryTarget) throw new Error("ToolAttemptRetryTargetMissing");
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
        if (!this.capacityPressure("tool_attempt_facts", start.operationId, start.startedAt)) {
          return this.dropFact<ToolAttemptFact>("outside_retention_window");
        }
        if (start.retryOf && !this.db.prepare(
          `select 1 from tool_attempt_facts where operation_id = ?`,
        ).get(start.retryOf)) {
          return this.dropFact<ToolAttemptFact>("retry_target_missing");
        }
        const now = new Date().toISOString();
        this.db.prepare(
          `insert into tool_attempt_facts
            (operation_id, source, session_id, episode_id, tool_class, tool_name,
             started_at, ended_at, duration_ms, result_status, error_category,
             retry_of, created_at, updated_at)
           values
            (@operationId, @source, @sessionId, @episodeId, @toolClass, @toolName,
             @startedAt, null, null, 'unknown', 'unknown', @retryOf, @now, @now)`,
        ).run({
          ...start,
          episodeId: start.episodeId ?? null,
          retryOf: start.retryOf ?? null,
          now,
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
      if (!this.capacityPressure("work_episode_facts", fact.episodeId, fact.startedAt)) {
        return this.dropFact<WorkEpisodeFact>("outside_retention_window");
      }
      if (fact.parentEpisodeId) {
        const parentStillExists = this.db
          .prepare(`select 1 from work_episode_facts where episode_id = ?`)
          .get(fact.parentEpisodeId);
        // The parent may itself have been the oldest graph root. Do not write
        // a child that would be orphaned by the same bounded eviction.
        if (!parentStillExists) return this.dropFact<WorkEpisodeFact>();
      }
      this.db.prepare(
        `insert into work_episode_facts
          (episode_id, source, session_id, work_class, complexity_band,
           parent_episode_id, started_at, ended_at, duration_ms, created_at)
         values
          (@episodeId, @source, @sessionId, @workClass, @complexityBand,
           @parentEpisodeId, @startedAt, @endedAt, @durationMs, @createdAt)`,
      ).run({
        ...fact,
        parentEpisodeId: fact.parentEpisodeId ?? null,
        endedAt: fact.endedAt ?? null,
        durationMs: fact.durationMs ?? null,
        createdAt: new Date().toISOString(),
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
      if (!this.capacityPressure("technique_exposure_facts", fact.exposureId, fact.exposedAt)) {
        return this.dropFact<TechniqueExposureFact>("outside_retention_window");
      }
      const techniqueKey = deterministicLearningFactId([
        fact.techniqueId,
        fact.techniqueVersion ?? "",
        fact.contentDigest ?? "",
      ]);
      const now = new Date().toISOString();
      if (this.capacityPressure("technique_identity_registry", techniqueKey, now)) {
        this.db.prepare(
        `insert or ignore into technique_identity_registry
          (technique_key, technique_id, technique_version, content_digest, first_seen_at)
         values (@techniqueKey, @techniqueId, @techniqueVersion, @contentDigest, @now)`,
        ).run({
          techniqueKey,
          techniqueId: fact.techniqueId,
          techniqueVersion: fact.techniqueVersion ?? null,
          contentDigest: fact.contentDigest ?? null,
          now,
        });
      } else {
        recordRuntimeFactDrop(this.db, "outside_retention_window");
      }
      this.db.prepare(
        `insert into technique_exposure_facts
          (exposure_id, episode_id, technique_id, technique_version, content_digest,
           assignment_id, work_class, complexity_band, exposed_at, mode, assertion, created_at)
         values
          (@exposureId, @episodeId, @techniqueId, @techniqueVersion, @contentDigest,
           @assignmentId, @workClass, @complexityBand, @exposedAt, @mode, @assertion, @createdAt)`,
      ).run({
        ...fact,
        techniqueVersion: fact.techniqueVersion ?? null,
        contentDigest: fact.contentDigest ?? null,
        createdAt: now,
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
    return (
      this.db
        .prepare(`${ATTEMPT_SELECT} order by started_at, operation_id`)
        .all() as AttemptRow[]
    ).map(attemptFromRow);
  }

  episodes(): WorkEpisodeFact[] {
    return (
      this.db.prepare(
        `select episode_id as episodeId, source, session_id as sessionId,
           work_class as workClass, complexity_band as complexityBand,
           parent_episode_id as parentEpisodeId,
           started_at as startedAt, ended_at as endedAt, duration_ms as durationMs
         from work_episode_facts order by started_at, episode_id`,
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
    return (
      this.db.prepare(
        `select exposure_id as exposureId, episode_id as episodeId,
           technique_id as techniqueId, technique_version as techniqueVersion,
           content_digest as contentDigest, assignment_id as assignmentId,
           work_class as workClass, complexity_band as complexityBand,
           exposed_at as exposedAt, mode, assertion
         from technique_exposure_facts order by exposed_at, exposure_id`,
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
