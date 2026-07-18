import type Database from "better-sqlite3";

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
    this.db.exec(`
      create table if not exists work_episode_facts (
        episode_id text primary key,
        source text not null,
        session_id text not null check(length(session_id) between 1 and 96),
        work_class text not null check(work_class in ('implementation','debugging','review','research','operations','other')),
        complexity_band text not null check(complexity_band in ('low','medium','high','unknown')),
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
    `);
  }

  private assertCapacity(
    table: string,
    idColumn: string,
    id: string,
    limit: number,
  ) {
    const existing = this.db
      .prepare(`select 1 from ${table} where ${idColumn} = ?`)
      .get(id);
    if (existing) return;
    const count = (
      this.db.prepare(`select count(*) as n from ${table}`).get() as { n: number }
    ).n;
    if (count >= limit) throw new Error(`LearningFactCapacityExceeded:${table}`);
  }

  recordToolSignal(input: unknown): { inserted: boolean; fact: ToolAttemptFact } {
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
          if (!episode) throw new Error("ToolAttemptEpisodeMissing");
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
        this.assertCapacity(
          "tool_attempt_facts",
          "operation_id",
          start.operationId,
          this.limits.attempts,
        );
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
      if (!existing) throw new Error("ToolAttemptStartMissing");
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
        if (!episode) throw new Error("ToolAttemptEpisodeMissing");
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

  recordWorkEpisode(input: unknown): { inserted: boolean; fact: WorkEpisodeFact } {
    const fact = workEpisodeFactSchema.parse(input);
    return this.db.transaction(() => {
      const row = this.db
        .prepare(
          `select episode_id as episodeId, source, session_id as sessionId,
             work_class as workClass, complexity_band as complexityBand,
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
          endedAt: row.endedAt ?? undefined,
          durationMs: row.durationMs ?? undefined,
        });
        if (JSON.stringify(existing) !== JSON.stringify(fact)) {
          throw new Error("WorkEpisodeIdentityConflict");
        }
        return { inserted: false, fact: existing };
      }
      this.assertCapacity(
        "work_episode_facts",
        "episode_id",
        fact.episodeId,
        this.limits.episodes,
      );
      this.db.prepare(
        `insert into work_episode_facts
          (episode_id, source, session_id, work_class, complexity_band,
           started_at, ended_at, duration_ms, created_at)
         values
          (@episodeId, @source, @sessionId, @workClass, @complexityBand,
           @startedAt, @endedAt, @durationMs, @createdAt)`,
      ).run({
        ...fact,
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
  ): { inserted: boolean; fact: TechniqueExposureFact } {
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
      if (!episode) throw new Error("TechniqueExposureEpisodeMissing");
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
      this.assertCapacity(
        "technique_exposure_facts",
        "exposure_id",
        fact.exposureId,
        this.limits.exposures,
      );
      const techniqueKey = deterministicLearningFactId([
        fact.techniqueId,
        fact.techniqueVersion ?? "",
        fact.contentDigest ?? "",
      ]);
      this.assertCapacity(
        "technique_identity_registry",
        "technique_key",
        techniqueKey,
        this.limits.techniqueIdentities,
      );
      const now = new Date().toISOString();
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
           started_at as startedAt, ended_at as endedAt, duration_ms as durationMs
         from work_episode_facts order by started_at, episode_id`,
      ).all() as Array<Record<string, unknown>>
    ).map((row) =>
      workEpisodeFactSchema.parse({
        ...row,
        endedAt: row.endedAt ?? undefined,
        durationMs: row.durationMs ?? undefined,
      }),
    );
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
