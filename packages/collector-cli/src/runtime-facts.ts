import type Database from "better-sqlite3";

import {
  adaptToolInteractionEvent,
  buildTechniqueExposureFact,
  buildWorkEpisodeFact,
  deterministicToolOperationId,
  type LearningFactStore,
} from "./learning-facts";
import type {
  AiInteractionEvent,
  TechniqueExposureInput,
  ToolAttemptErrorCategory,
  ToolAttemptResultStatus,
  WorkComplexityBand,
  WorkClass,
} from "../../shared/src/index";

/**
 * Production promotion of authoritative capture signals into local
 * learning facts (issue #156). Only typed, collector-validated fields on the
 * normalized event are consulted: prompts, commands, arguments, paths, error
 * text, stacks, and free-form producer metadata never reach this module.
 * Outcome truth comes exclusively from collector-generated protocol signals
 * (`otelHasError`/`otelStatusCode` are produced by our own OTLP boundary from
 * validated inputs; producer attributes cannot set them because their
 * metadata disposition is generated-only).
 *
 * Operation correlation stays ephemeral: raw producer correlation keys are
 * read solely to derive the ledger-local one-way operation identity and are
 * never written to any fact table.
 */

export type RuntimeFactDropReason =
  | "missing_session"
  | "episode_seed_failed"
  | "invalid_signal"
  | "identity_conflict"
  | "unpaired_result"
  | "retry_target_missing"
  | "capacity_exceeded";

/** Producer correlation aliases admitted as bounded identifiers upstream. */
const CORRELATION_KEYS = ["call_id", "request_id"] as const;
const RETRY_OF_KEY = "plimsoll.retry_of";

/** Fixed, content-free classification for implicit session episodes. */
const IMPLICIT_EPISODE_KEY = "session";
const IMPLICIT_WORK_CLASS: WorkClass = "other";
const IMPLICIT_COMPLEXITY_BAND: WorkComplexityBand = "unknown";

type DropRow = { reason: RuntimeFactDropReason; droppedCount: number; lastDroppedAt: string };

export function ensureRuntimeFactDropSchema(db: Database.Database) {
  db.exec(`
    create table if not exists runtime_fact_drops (
      reason text primary key,
      dropped_count integer not null check(dropped_count >= 0),
      last_dropped_at text not null
    );
  `);
}

function dropTimestamp() {
  return new Date().toISOString();
}

/** One bounded dirty-state row per drop reason; never throws into capture. */
export function recordRuntimeFactDrop(db: Database.Database, reason: RuntimeFactDropReason) {
  try {
    ensureRuntimeFactDropSchema(db);
    db.prepare(
      `insert into runtime_fact_drops (reason, dropped_count, last_dropped_at)
       values (?, 1, ?)
       on conflict(reason) do update set
         dropped_count = dropped_count + 1,
         last_dropped_at = excluded.last_dropped_at`,
    ).run(reason, dropTimestamp());
  } catch {
    // Drop accounting must never break capture.
  }
}

export function runtimeFactDropCounters(db: Database.Database): DropRow[] {
  try {
    return db.prepare(
      `select reason, dropped_count as droppedCount, last_dropped_at as lastDroppedAt
       from runtime_fact_drops order by reason`,
    ).all() as DropRow[];
  } catch {
    ensureRuntimeFactDropSchema(db);
    return [];
  }
}

function topLevelMetadataString(event: AiInteractionEvent, key: string) {
  const metadata = event.metadata as Record<string, unknown> | undefined;
  const value = metadata?.[key];
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

/** Ephemeral operation correlation key; callers persist only its hash. */
export function runtimeCorrelationKeyOf(event: AiInteractionEvent) {
  for (const key of CORRELATION_KEYS) {
    const value = topLevelMetadataString(event, key);
    if (value) return value;
  }
  return undefined;
}

function errorReasonFor(error: unknown): RuntimeFactDropReason {
  const message = error instanceof Error ? error.message : String(error);
  if (message.includes("LearningFactCapacityExceeded")) return "capacity_exceeded";
  if (message.includes("ToolAttemptRetryTargetMissing")) return "retry_target_missing";
  if (
    message.includes("ToolAttemptIdentityConflict") ||
    message.includes("ToolAttemptResultIdentityConflict") ||
    message.includes("ToolAttemptResultConflict")
  ) {
    return "identity_conflict";
  }
  if (message.includes("ToolAttemptStartMissing")) return "unpaired_result";
  return "invalid_signal";
}

/**
 * Collector-generated outcome authority. `otelHasError`/`otelStatusCode`
 * exist in event metadata only when our own OTLP boundary derived them from
 * validated protocol fields, so a hostile producer cannot forge success or
 * failure here.
 */
function authoritativeResultOf(event: AiInteractionEvent): {
  resultStatus: ToolAttemptResultStatus;
  errorCategory?: ToolAttemptErrorCategory;
} {
  const metadata = event.metadata as Record<string, unknown> | undefined;
  if (metadata?.otelHasError === true) {
    return { resultStatus: "failure", errorCategory: "unknown" };
  }
  const statusCode = metadata?.otelStatusCode;
  const upper = typeof statusCode === "string" ? statusCode.toUpperCase() : undefined;
  if (upper === "OK" || statusCode === 1 || statusCode === "1") {
    return { resultStatus: "success" };
  }
  return { resultStatus: "unknown" };
}

export type RuntimeFactPromotion = {
  attempted: boolean;
  attemptInserted: boolean;
  resultApplied: boolean;
};

const IDLE_PROMOTION: RuntimeFactPromotion = {
  attempted: false,
  attemptInserted: false,
  resultApplied: false,
};

/**
 * Promote one newly appended tool_use/tool_result event into at most one
 * attempt-fact append plus its result update, and at most one episode row.
 * Never throws: capture availability always outranks fact promotion.
 */
export function promoteRuntimeLearningFacts(
  target: { database: Database.Database; learningFacts: LearningFactStore },
  event: AiInteractionEvent,
): RuntimeFactPromotion {
  if (event.eventType !== "tool_use" && event.eventType !== "tool_result") {
    return IDLE_PROMOTION;
  }
  const store = target.learningFacts;
  if (!event.sessionId) {
    recordRuntimeFactDrop(target.database, "missing_session");
    return IDLE_PROMOTION;
  }

  // Bounded implicit work episode: fixed dimensions, no content is read.
  // Re-seeding with a newer observation timestamp must not conflict with the
  // already-stored episode; recovery reads the durable row by identity.
  let episodeId: string | undefined;
  const implicitEpisode = buildWorkEpisodeFact({
    source: event.source,
    sessionId: event.sessionId,
    sourceEpisodeKey: IMPLICIT_EPISODE_KEY,
    workClass: IMPLICIT_WORK_CLASS,
    complexityBand: IMPLICIT_COMPLEXITY_BAND,
    startedAt: event.observedAt,
  });
  try {
    store.recordWorkEpisode(implicitEpisode);
    episodeId = implicitEpisode.episodeId;
  } catch {
    if (store.episodeById(implicitEpisode.episodeId)) {
      episodeId = implicitEpisode.episodeId;
    } else {
      recordRuntimeFactDrop(target.database, "episode_seed_failed");
    }
  }

  const sourceOperationKey = runtimeCorrelationKeyOf(event) ?? event.id;
  const retryOfSourceOperationKey = topLevelMetadataString(event, RETRY_OF_KEY);

  if (event.eventType === "tool_use") {
    if (retryOfSourceOperationKey === sourceOperationKey) {
      recordRuntimeFactDrop(target.database, "invalid_signal");
      return { attempted: true, attemptInserted: false, resultApplied: false };
    }
    try {
      const start = adaptToolInteractionEvent({
        event,
        sourceOperationKey,
        retryOfSourceOperationKey,
        episodeId,
      });
      const recorded = store.recordToolSignal(start);
      // A completion-bearing signal on the same observation (OTLP tool spans
      // export start, end, and status together) closes the attempt in this
      // same promotion: at most one fact append plus one result update.
      let resultApplied = false;
      const { resultStatus, errorCategory } = authoritativeResultOf(event);
      const spanEnd = topLevelMetadataString(event, "otelSpanEndAt");
      const endedAt = spanEnd ?? (resultStatus !== "unknown" ? event.observedAt : undefined);
      if (endedAt) {
        try {
          store.recordToolSignal({
            kind: "result",
            operationId: deterministicToolOperationId({
              source: event.source,
              sessionId: event.sessionId,
              sourceOperationKey,
            }),
            source: event.source,
            sessionId: event.sessionId,
            endedAt,
            resultStatus,
            errorCategory,
          });
          resultApplied = true;
        } catch (error) {
          recordRuntimeFactDrop(target.database, errorReasonFor(error));
        }
      }
      return {
        attempted: true,
        attemptInserted: recorded.inserted,
        resultApplied,
      };
    } catch (error) {
      recordRuntimeFactDrop(target.database, errorReasonFor(error));
      return { attempted: true, attemptInserted: false, resultApplied: false };
    }
  }

  try {
    const { resultStatus, errorCategory } = authoritativeResultOf(event);
    const result = adaptToolInteractionEvent({
      event,
      sourceOperationKey,
      resultStatus,
      errorCategory,
    });
    store.recordToolSignal(result);
    return { attempted: false, attemptInserted: false, resultApplied: true };
  } catch (error) {
    recordRuntimeFactDrop(target.database, errorReasonFor(error));
    return { attempted: false, attemptInserted: false, resultApplied: false };
  }
}

/**
 * The only sanctioned production entry point for technique/skill exposure.
 * Exposure exists solely as an explicit prospective operator assignment
 * carrying technique identity, assignment id, exposure time, and mode; the
 * wrapper enforces prospectiveness against wall-clock time so retrospective
 * assignments fail closed. Capture code never calls this.
 */
export function recordExplicitTechniqueAssignment(
  target: { learningFacts: LearningFactStore },
  input: TechniqueExposureInput,
  options: { now?: () => Date } = {},
) {
  const fact = buildTechniqueExposureFact(input);
  const now = options.now ?? (() => new Date());
  return target.learningFacts.recordTechniqueExposure(fact, {
    outcomeObservedAt: now().toISOString(),
  });
}
