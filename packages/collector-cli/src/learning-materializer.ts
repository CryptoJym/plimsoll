import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import Database from "better-sqlite3";

import {
  assertLearningReviewOutputPath,
  canonicalLearningJson,
  computeLearningPairDigest,
  compileLearningEvidencePacket,
  pullTimelineFactSchema,
  type LearningAttribution,
  type LearningCohort,
  type LearningEvidenceManifest,
  type LearningObservation,
  type LearningOutcomePair,
  type TechniqueExposureFact,
} from "../../shared/src/index";
import { parseOutcomePerformanceRecord, type OutcomePerformanceRecord } from "./performance-layer";
import { PLIMSOLL_VERSION } from "./version";

export const LEARNING_MATERIALIZATION_SCHEMA = "plimsoll.learning-materialization.v1" as const;
export const LEARNING_MATERIALIZATION_STATE_SCHEMA = "plimsoll.learning-materialization-state.v1" as const;

const TOKEN_DIMENSIONS = ["inputTokens", "outputTokens", "cacheReadTokens", "cacheWriteTokens"] as const;
export type TokenDimension = (typeof TOKEN_DIMENSIONS)[number];
export type TokenConservation = Record<
  TokenDimension,
  { knownSum: number; knownEventCount: number; unknownEventCount: number }
>;
const LINEAGE_UNKNOWN_FIRST_PREDICATE =
  `payload_json like '%"counterLineage":"unknown_nonzero_first"%'`;

export const MATERIALIZATION_GATES = {
  statisticalMinCompletePairs: 3,
  statisticalMinActorClusters: 3,
  statisticalMinRepoClusters: 3,
  privacyMinCompletePairs: 3,
  minimumAttributionCoverage: 0.5,
  maxAbsoluteOutcome: 30 * 24 * 60 * 60 * 1000,
  maxPairs: 10_000,
  maxCounterexamples: 20,
  maxRuntimeMs: 5_000,
} as const;

type TokenUsageRow = {
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheCreationTokens: number | null;
};

type MaterializationState = {
  schemaVersion: typeof LEARNING_MATERIALIZATION_STATE_SCHEMA;
  usageHighWaterRowId: number;
  conservation: TokenConservation;
  unvalidatedLineageEvents: number;
  lastSourceFingerprint: string | null;
  generation: number;
};

function emptyConservation(): TokenConservation {
  return {
    inputTokens: { knownSum: 0, knownEventCount: 0, unknownEventCount: 0 },
    outputTokens: { knownSum: 0, knownEventCount: 0, unknownEventCount: 0 },
    cacheReadTokens: { knownSum: 0, knownEventCount: 0, unknownEventCount: 0 },
    cacheWriteTokens: { knownSum: 0, knownEventCount: 0, unknownEventCount: 0 },
  };
}

function initialState(): MaterializationState {
  return {
    schemaVersion: LEARNING_MATERIALIZATION_STATE_SCHEMA,
    usageHighWaterRowId: 0,
    conservation: emptyConservation(),
    unvalidatedLineageEvents: 0,
    lastSourceFingerprint: null,
    generation: 0,
  };
}

/** Durable high-water mark + running token conservation for the materializer. */
export class LearningMaterializationStateStore {
  private readonly db: Database.Database;

  constructor(statePath: string) {
    fs.mkdirSync(path.dirname(statePath), { recursive: true });
    this.db = new Database(statePath);
    this.db.pragma("journal_mode = WAL");
    this.db.exec(`
      create table if not exists learning_materialization_state (
        singleton integer primary key check (singleton = 1),
        canonical_json text not null,
        updated_at text not null
      );
    `);
  }

  read(): MaterializationState {
    const row = this.db
      .prepare(
        "select canonical_json as canonicalJson from learning_materialization_state where singleton = 1",
      )
      .get() as { canonicalJson: string } | undefined;
    if (!row) return initialState();
    const value = JSON.parse(row.canonicalJson) as MaterializationState;
    if (value.schemaVersion !== LEARNING_MATERIALIZATION_STATE_SCHEMA) {
      throw new Error(`Unsupported materialization state schema: ${String(value.schemaVersion)}`);
    }
    return value;
  }

  write(state: MaterializationState, now: string): void {
    this.db
      .prepare(
        `insert into learning_materialization_state (singleton, canonical_json, updated_at)
         values (1, ?, ?)
         on conflict(singleton) do update set
           canonical_json = excluded.canonical_json, updated_at = excluded.updated_at`,
      )
      .run(canonicalLearningJson(state), now);
  }

  close(): void {
    this.db.close();
  }
}

function conservationFromRows(rows: readonly TokenUsageRow[]): TokenConservation {
  const conservation = emptyConservation();
  for (const row of rows) {
    const values: Record<TokenDimension, number | null> = {
      inputTokens: row.inputTokens,
      outputTokens: row.outputTokens,
      cacheReadTokens: row.cacheReadTokens,
      cacheWriteTokens: row.cacheCreationTokens,
    };
    for (const dimension of TOKEN_DIMENSIONS) {
      const value = values[dimension];
      if (value === null || !Number.isFinite(value)) {
        conservation[dimension].unknownEventCount += 1;
      } else {
        conservation[dimension].knownSum += value;
        conservation[dimension].knownEventCount += 1;
      }
    }
  }
  return conservation;
}

function mergeConservation(base: TokenConservation, delta: TokenConservation): TokenConservation {
  const merged = emptyConservation();
  for (const dimension of TOKEN_DIMENSIONS) {
    merged[dimension] = {
      knownSum: base[dimension].knownSum + delta[dimension].knownSum,
      knownEventCount: base[dimension].knownEventCount + delta[dimension].knownEventCount,
      unknownEventCount: base[dimension].unknownEventCount + delta[dimension].unknownEventCount,
    };
  }
  return merged;
}

/** Normalize a stored repo label into the deterministic external repository id. */
export function repoLabelToExternalId(label: string): string | null {
  const trimmed = label.trim().replace(/\.git$/i, "");
  const match = trimmed.match(/^https?:\/\/github\.com\/([^/\s]+)\/([^/\s]+)$/i) ??
    trimmed.match(/^git@github\.com:([^/\s]+)\/([^/\s]+)$/i) ??
    trimmed.match(/^github\.com\/([^/\s]+)\/([^/\s]+)$/i);
  if (!match) return null;
  return `github:repository:${match[1]!.toLowerCase()}/${match[2]!.toLowerCase()}`;
}

function openReadonlyDatabase(filePath: string): Database.Database | null {
  if (!fs.existsSync(filePath)) return null;
  return new Database(filePath, { readonly: true, fileMustExist: true });
}

function isoWeekId(isoTimestamp: string): string {
  const day = new Date(isoTimestamp);
  day.setUTCHours(0, 0, 0, 0);
  const dayNumber = (day.getUTCDay() + 6) % 7;
  day.setUTCDate(day.getUTCDate() - dayNumber + 3);
  const firstThursday = new Date(Date.UTC(day.getUTCFullYear(), 0, 4));
  const firstDayNumber = (firstThursday.getUTCDay() + 6) % 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() - firstDayNumber + 3);
  const week =
    1 + Math.round((day.getTime() - firstThursday.getTime()) / (7 * 24 * 60 * 60 * 1000));
  return `${day.getUTCFullYear()}-W${String(week).padStart(2, "0")}`;
}

export type MaterializerShortfalls = {
  episodesMissingSessionUsage: number;
  episodesUnresolvedAttempts: number;
  exposuresWithoutEpisode: number;
  retrospectiveExposures: number;
  unmappedProjectIdentities: number;
  sessionHeadShaAmbiguous: number;
  outcomeLinkageUnavailable: number;
};

function emptyShortfalls(): MaterializerShortfalls {
  return {
    episodesMissingSessionUsage: 0,
    episodesUnresolvedAttempts: 0,
    exposuresWithoutEpisode: 0,
    retrospectiveExposures: 0,
    unmappedProjectIdentities: 0,
    sessionHeadShaAmbiguous: 0,
    outcomeLinkageUnavailable: 0,
  };
}

export type LearningMaterializationInput = {
  ledgerPath: string;
  outcomeStorePath: string | null;
  statePath: string;
  outPath: string | null;
  /** Workspace root for the output path guard; defaults to process.cwd(). */
  workspaceRoot?: string;
  until: string;
  windowDays: number;
  maxNewUsageEvents: number;
};

export type LearningMaterializationSources = {
  ledgerPresent: boolean;
  outcomeStorePresent: boolean;
  ledgerAbsentReason?: string;
  outcomeStoreAbsentReason?: string;
};

export type LearningMaterializationReceipt = {
  schema: typeof LEARNING_MATERIALIZATION_SCHEMA;
  scheduled: false;
  continuousLoop: false;
  modelCalls: 0;
  localOnly: true;
  status: "computed" | "unchanged" | "blocked_dependencies";
  sources: LearningMaterializationSources;
  window: { startInclusive: string; endExclusive: string; days: number };
  until: string;
  scanned: {
    newUsageRows: number;
    usageBacklogRemaining: number;
    episodes: number;
    attempts: number;
    unresolvedAttemptOperations: number;
    exposures: number;
    outcomePerformanceRecords: number;
  };
  usageHighWater: { before: number; after: number };
  conservation: TokenConservation;
  unvalidatedLineageEvents: number;
  shortfalls: MaterializerShortfalls;
  pairing: {
    candidateAssignments: number;
    formedPairs: number;
    techniqueContracts: number;
  };
  dependencyReasons: string[];
  sourceFingerprint: string | null;
  generation: number;
  packetClaimClass: string | null;
  notEstimableReasons: string[];
  outputWritten: boolean;
  outputPath: string | null;
};

type ExposureRow = {
  exposureId: string;
  episodeId: string;
  techniqueId: string;
  techniqueVersion: string | null;
  contentDigest: string | null;
  assignmentId: string;
  workClass: string;
  complexityBand: string;
  exposedAt: string;
  mode: "control" | "treatment";
};

type EpisodeRow = {
  episodeId: string;
  sessionId: string;
  workClass: string;
  complexityBand: string;
  startedAt: string;
  endedAt: string | null;
};

/**
 * The one bounded materialization pass. It reads immutable local facts behind
 * a durable high-water mark, joins truthful allocation, token origin, the
 * materialized immutable outcome read model, work episodes, check-attempt
 * completeness, and prospective technique exposure, and writes at most one
 * versioned evidence packet. It is manual/on-demand: no dashboard request,
 * idle collector loop, or continuous analyst ever calls it.
 */
export function runLearningMaterialization(
  input: LearningMaterializationInput,
): LearningMaterializationReceipt {
  const untilMs = Date.parse(input.until);
  if (!Number.isFinite(untilMs)) {
    throw new Error(`until expects an ISO timestamp, got: ${input.until}`);
  }
  const until = new Date(untilMs).toISOString();
  if (!Number.isInteger(input.windowDays) || input.windowDays < 1 || input.windowDays > 365) {
    throw new Error(`windowDays expects an integer from 1 through 365, got: ${input.windowDays}`);
  }
  if (!Number.isInteger(input.maxNewUsageEvents) || input.maxNewUsageEvents < 0) {
    throw new Error("maxNewUsageEvents expects a non-negative integer");
  }
  const windowStartMs = untilMs - input.windowDays * 24 * 60 * 60 * 1000;
  const windowStart = new Date(windowStartMs).toISOString();

  const stateStore = new LearningMaterializationStateStore(input.statePath);
  let ledger: Database.Database | null = null;
  let outcomeDb: Database.Database | null = null;
  try {
    const stateBefore = stateStore.read();
    if (!fs.existsSync(input.ledgerPath)) {
      return {
        schema: LEARNING_MATERIALIZATION_SCHEMA,
        scheduled: false,
        continuousLoop: false,
        modelCalls: 0,
        localOnly: true,
        status: "blocked_dependencies",
        sources: {
          ledgerPresent: false,
          outcomeStorePresent: false,
          ledgerAbsentReason: `no local ledger at ${input.ledgerPath}`,
        },
        window: { startInclusive: windowStart, endExclusive: until, days: input.windowDays },
        until,
        scanned: {
          newUsageRows: 0,
          usageBacklogRemaining: 0,
          episodes: 0,
          attempts: 0,
          unresolvedAttemptOperations: 0,
          exposures: 0,
          outcomePerformanceRecords: 0,
        },
        usageHighWater: { before: stateBefore.usageHighWaterRowId, after: stateBefore.usageHighWaterRowId },
        conservation: stateBefore.conservation,
        unvalidatedLineageEvents: stateBefore.unvalidatedLineageEvents,
        shortfalls: emptyShortfalls(),
        pairing: { candidateAssignments: 0, formedPairs: 0, techniqueContracts: 0 },
        dependencyReasons: ["ledger_absent"],
        sourceFingerprint: null,
        generation: stateBefore.generation,
        packetClaimClass: null,
        notEstimableReasons: [],
        outputWritten: false,
        outputPath: null,
      };
    }
    ledger = openReadonlyDatabase(input.ledgerPath)!;
    outcomeDb = input.outcomeStorePath ? openReadonlyDatabase(input.outcomeStorePath) : null;
    const shortfalls = emptyShortfalls();

    // 1. Bounded incremental usage scan behind the durable high-water mark.
    //    This is the only surface that touches raw event history, and it never
    //    rescans rows below the watermark.
    const usageRows = ledger.prepare(
      `select rowid as rowId,
              input_tokens as inputTokens, output_tokens as outputTokens,
              cache_read_tokens as cacheReadTokens, cache_creation_tokens as cacheCreationTokens,
              ${LINEAGE_UNKNOWN_FIRST_PREDICATE} as lineageUnknownFirst
       from buffered_events
       where rowid > ? and model is not null
       order by rowid
       limit ?`,
    ).all(stateBefore.usageHighWaterRowId, input.maxNewUsageEvents) as Array<
      TokenUsageRow & { rowId: number; lineageUnknownFirst: 0 | 1 }
    >;
    const lineageUnknownFirst = usageRows.reduce((total, row) => total + row.lineageUnknownFirst, 0);
    const usageDelta = conservationFromRows(usageRows);
    const usageHighWaterAfter = usageRows.reduce(
      (max, row) => Math.max(max, row.rowId),
      stateBefore.usageHighWaterRowId,
    );
    const usageBacklogRemaining = (ledger.prepare(
      `select count(*) as n from buffered_events where rowid > ? and model is not null`,
    ).get(usageHighWaterAfter) as { n: number }).n;

    // 2. Immutable, hard-capped learning-fact tables (≤10k episodes / ≤10k
    //    exposures / ≤100k attempts by store capacity limits).
    const episodeRows = ledger.prepare(
      `select episode_id as episodeId, session_id as sessionId, work_class as workClass,
              complexity_band as complexityBand, started_at as startedAt, ended_at as endedAt
       from work_episode_facts order by started_at, episode_id`,
    ).all() as EpisodeRow[];
    const exposureRows = ledger.prepare(
      `select exposure_id as exposureId, episode_id as episodeId, technique_id as techniqueId,
              technique_version as techniqueVersion, content_digest as contentDigest,
              assignment_id as assignmentId, work_class as workClass,
              complexity_band as complexityBand, exposed_at as exposedAt, mode
       from technique_exposure_facts order by exposed_at, exposure_id`,
    ).all() as ExposureRow[];
    const attemptCounts = ledger.prepare(
      `select count(*) as total,
              sum(case when result_status = 'unknown' then 1 else 0 end) as unresolved,
              count(distinct case when result_status = 'unknown' then episode_id end) as unresolvedEpisodes
       from tool_attempt_facts where episode_id is not null`,
    ).get() as { total: number; unresolved: number | null; unresolvedEpisodes: number | null };
    const unresolvedEpisodeIds = new Set(
      (ledger.prepare(
        `select distinct episode_id as episodeId from tool_attempt_facts
         where episode_id is not null and result_status = 'unknown'`,
      ).all() as Array<{ episodeId: string }>).map((row) => row.episodeId),
    );

    // 3. Materialized immutable-outcome surfaces only. Raw provider history is
    //    never fetched; the deterministic session→pull binding comes from the
    //    stored merge facts (mergeSha × repository identity), and effect inputs
    //    come from the derived performance read model. Absence anywhere stays
    //    literal UNKNOWN / linkage-unavailable downstream.
    const performanceByPull = new Map<string, OutcomePerformanceRecord[]>();
    const pullIdByRepoAndMergeSha = new Map<string, { repositoryExternalId: string; pullExternalId: string }>();
    let outcomePerformanceRecords = 0;
    if (outcomeDb) {
      const hasPerformanceTable = Boolean(outcomeDb.prepare(
        `select 1 from sqlite_master where type='table' and name='outcome_timeline_performance'`,
      ).get());
      if (hasPerformanceTable) {
        const rows = outcomeDb.prepare(
          `select canonical_json as canonicalJson from outcome_timeline_performance`,
        ).all() as Array<{ canonicalJson: string }>;
        for (const row of rows) {
          const record = parseOutcomePerformanceRecord(JSON.parse(row.canonicalJson));
          outcomePerformanceRecords += 1;
          const key = `${record.repositoryExternalId}\u0000${record.pullExternalId}`;
          const existing = performanceByPull.get(key) ?? [];
          existing.push(record);
          performanceByPull.set(key, existing);
        }
      }
      const hasFactsTable = Boolean(outcomeDb.prepare(
        `select 1 from sqlite_master where type='table' and name='outcome_timeline_facts'`,
      ).get());
      if (hasFactsTable) {
        const mergeRows = outcomeDb.prepare(
          `select canonical_json as canonicalJson from outcome_timeline_facts where kind = 'merge'`,
        ).all() as Array<{ canonicalJson: string }>;
        for (const row of mergeRows) {
          const fact = pullTimelineFactSchema.parse(JSON.parse(row.canonicalJson));
          if (fact.kind !== "merge") continue;
          pullIdByRepoAndMergeSha.set(
            `${fact.repositoryExternalId}\u0000${fact.mergeSha}`,
            { repositoryExternalId: fact.repositoryExternalId, pullExternalId: fact.pullExternalId },
          );
        }
      }
    }

    // 4. Truthful project allocation from stored repo labels only.
    const repoHashToExternalId = new Map<string, string>();
    const labelRows = ledger.prepare(
      `select repo_hash as repoHash, label from repo_labels order by repo_hash, label`,
    ).all() as Array<{ repoHash: string; label: string }>;
    for (const row of labelRows) {
      if (repoHashToExternalId.has(row.repoHash)) continue;
      const externalId = repoLabelToExternalId(row.label);
      if (externalId) repoHashToExternalId.set(row.repoHash, externalId);
    }

    // 5. Join prospective exposures to episodes, session token origin, and
    //    outcomes; group treatment/control halves by explicit assignment.
    const sessionIndex = new Map<string, {
      models: Map<string, number>;
      machines: Set<string>;
      repoHashes: Set<string>;
      headShas: Set<string>;
    }>();
    for (const row of ledger.prepare(
      `select session_id as sessionId, model, machine, repo_hash as repoHash, head_sha as headSha
       from buffered_events where model is not null and session_id is not null`,
    ).all() as Array<{ sessionId: string; model: string; machine: string | null; repoHash: string | null; headSha: string | null }>) {
      let entry = sessionIndex.get(row.sessionId);
      if (!entry) {
        entry = { models: new Map(), machines: new Set(), repoHashes: new Set(), headShas: new Set() };
        sessionIndex.set(row.sessionId, entry);
      }
      entry.models.set(row.model, (entry.models.get(row.model) ?? 0) + 1);
      if (row.machine) entry.machines.add(row.machine);
      if (row.repoHash) entry.repoHashes.add(row.repoHash);
      if (row.headSha) entry.headShas.add(row.headSha);
    }

    const episodeById = new Map(episodeRows.map((row) => [row.episodeId, row]));
    const contracts = new Map<string, LearningEvidenceManifest["techniqueContract"]>();
    const candidatesByAssignment = new Map<
      string,
      Record<"exposed" | "control", LearningObservation | undefined>
    >();
    const usedObservationEpisodeIds = new Set<string>();
    for (const exposure of exposureRows) {
      const episode = episodeById.get(exposure.episodeId);
      if (!episode) {
        shortfalls.exposuresWithoutEpisode += 1;
        continue;
      }
      if (Date.parse(exposure.exposedAt) > Date.parse(episode.startedAt)) {
        shortfalls.retrospectiveExposures += 1;
        continue;
      }
      if (episode.startedAt < windowStart || episode.startedAt >= until) continue;
      const contractKey = [
        exposure.techniqueId,
        exposure.techniqueVersion ?? "",
        exposure.contentDigest ?? "",
      ].join("\u001f");
      contracts.set(contractKey, {
        techniqueId: exposure.techniqueId,
        techniqueVersion: exposure.techniqueVersion,
        contentDigest: exposure.contentDigest,
      });
      if (usedObservationEpisodeIds.has(exposure.episodeId)) {
        throw new Error(`episode_double_use:${exposure.episodeId}`);
      }
      const usage = sessionIndex.get(episode.sessionId);
      const modelEntry = usage
        ? [...usage.models.entries()].sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))[0]
        : undefined;
      const machine = usage ? [...usage.machines].sort()[0] : undefined;
      const repoHash = usage ? [...usage.repoHashes].sort()[0] : undefined;
      const projectId = repoHash ? repoHashToExternalId.get(repoHash) : undefined;
      if (!usage || !modelEntry || !machine || !repoHash) {
        shortfalls.episodesMissingSessionUsage += 1;
        continue;
      }
      if (usage.repoHashes.size !== 1 || !projectId) {
        shortfalls.unmappedProjectIdentities += 1;
        continue;
      }
      if (unresolvedEpisodeIds.has(exposure.episodeId)) {
        shortfalls.episodesUnresolvedAttempts += 1;
        continue;
      }
      if (usage.headShas.size !== 1) {
        shortfalls.sessionHeadShaAmbiguous += 1;
        continue;
      }
      const boundPull = pullIdByRepoAndMergeSha.get(`${projectId}\u0000${[...usage.headShas][0]}`);
      const recordKey = boundPull
        ? `${boundPull.repositoryExternalId}\u0000${boundPull.pullExternalId}`
        : null;
      const candidateRecords = recordKey ? performanceByPull.get(recordKey) : undefined;
      const record = candidateRecords && candidateRecords.length === 1 ? candidateRecords[0] : undefined;
      if (!record) shortfalls.outcomeLinkageUnavailable += 1;
      const timeToGreen =
        record && record.coverage === "COMPLETE" && record.checkOutcome !== "UNKNOWN"
          ? record.timeToGreenMs
          : null;
      const cohort: LearningCohort = {
        projectId,
        workType: exposure.workClass as LearningCohort["workType"],
        complexityBand: exposure.complexityBand as LearningCohort["complexityBand"],
        modelId: modelEntry[0],
        toolVersion: PLIMSOLL_VERSION,
        actorClusterId: machine,
        repoClusterId: repoHash,
        epochId: isoWeekId(episode.startedAt),
      };
      // Canonical stored-fact representation: absent optional keys, never
      // explicit undefined values.
      const storedExposure: TechniqueExposureFact = {
        exposureId: exposure.exposureId,
        episodeId: exposure.episodeId,
        techniqueId: exposure.techniqueId,
        assignmentId: exposure.assignmentId,
        workClass: exposure.workClass as TechniqueExposureFact["workClass"],
        complexityBand: exposure.complexityBand as TechniqueExposureFact["complexityBand"],
        exposedAt: exposure.exposedAt,
        mode: exposure.mode,
        assertion: "exposure_only",
        ...(exposure.techniqueVersion !== null ? { techniqueVersion: exposure.techniqueVersion } : {}),
        ...(exposure.contentDigest !== null ? { contentDigest: exposure.contentDigest } : {}),
      };
      const observation: LearningObservation = {
        observationId: `obs-${exposure.exposureId}`,
        workStartedAt: episode.startedAt,
        outcomeObservedAt: record?.occurredAt ?? until,
        cohort,
        exposure: storedExposure,
        outcome: {
          metricId: "time_to_first_green",
          metricVersion: "1.0.0",
          unit: "milliseconds",
          direction: "lower_is_better",
          value: timeToGreen,
        },
        attribution: {
          method: record ? "deterministic_linkage" : "none",
          projectAllocation: record ? "exact" : "unknown",
          coverage: record ? 1 : 0,
        } satisfies LearningAttribution,
      };
      usedObservationEpisodeIds.add(exposure.episodeId);
      const bucket: Record<"exposed" | "control", LearningObservation | undefined> =
        candidatesByAssignment.get(exposure.assignmentId) ?? { exposed: undefined, control: undefined };
      if (exposure.mode === "treatment") bucket.exposed = observation;
      else bucket.control = observation;
      candidatesByAssignment.set(exposure.assignmentId, bucket);
    }

    const pairs: LearningOutcomePair[] = [];
    const assignmentIds = [...candidatesByAssignment.keys()].sort();
    for (const [index, assignmentId] of assignmentIds.entries()) {
      const bucket = candidatesByAssignment.get(assignmentId)!;
      if (!bucket.exposed || !bucket.control) continue;
      pairs.push({
        pairId: `pair-${String(index).padStart(6, "0")}-${assignmentId}`,
        exposed: bucket.exposed,
        control: bucket.control,
      });
    }

    const sources: LearningMaterializationSources = {
      ledgerPresent: true,
      outcomeStorePresent: Boolean(outcomeDb),
      ...(outcomeDb ? {} : { outcomeStoreAbsentReason: input.outcomeStorePath
        ? `no outcome timeline store at ${input.outcomeStorePath}`
        : "no outcome timeline store configured" }),
    };
    const scanned = {
      newUsageRows: usageRows.length,
      usageBacklogRemaining,
      episodes: episodeRows.length,
      attempts: attemptCounts.total,
      unresolvedAttemptOperations: attemptCounts.unresolved ?? 0,
      exposures: exposureRows.length,
      outcomePerformanceRecords,
    };
    const dependencyReasons = collectDependencyReasons({
      pairs: pairs.length,
      contracts: contracts.size,
      shortfalls,
      outcomeStorePresent: Boolean(outcomeDb),
    });

    // 6. Without complete assignments under one explicit technique contract
    //    there is no manifest: dependencies are reported, never substituted.
    if (pairs.length === 0 || contracts.size !== 1) {
      stateStore.write({
        ...stateBefore,
        usageHighWaterRowId: usageHighWaterAfter,
        conservation: mergeConservation(stateBefore.conservation, usageDelta),
        unvalidatedLineageEvents: stateBefore.unvalidatedLineageEvents + lineageUnknownFirst,
      }, new Date().toISOString());
      return {
        schema: LEARNING_MATERIALIZATION_SCHEMA,
        scheduled: false,
        continuousLoop: false,
        modelCalls: 0,
        localOnly: true,
        status: "blocked_dependencies",
        sources,
        window: { startInclusive: windowStart, endExclusive: until, days: input.windowDays },
        until,
        scanned,
        usageHighWater: { before: stateBefore.usageHighWaterRowId, after: usageHighWaterAfter },
        conservation: mergeConservation(stateBefore.conservation, usageDelta),
        unvalidatedLineageEvents: stateBefore.unvalidatedLineageEvents + lineageUnknownFirst,
        shortfalls,
        pairing: {
          candidateAssignments: candidatesByAssignment.size,
          formedPairs: pairs.length,
          techniqueContracts: contracts.size,
        },
        dependencyReasons,
        sourceFingerprint: null,
        generation: stateBefore.generation,
        packetClaimClass: null,
        notEstimableReasons: [],
        outputWritten: false,
        outputPath: null,
      };
    }

    // 7. Compile the versioned evidence packet through the shared contract.
    const techniqueContract = [...contracts.values()][0]!;
    const queryMaterial = canonicalLearningJson({
      window: { startInclusive: windowStart, endExclusive: until },
      gates: MATERIALIZATION_GATES,
      outcomeMetric: "time_to_first_green@1.0.0(milliseconds,lower_is_better)",
      toolVersion: PLIMSOLL_VERSION,
      factCounts: {
        episodes: episodeRows.length,
        attempts: attemptCounts.total,
        exposures: exposureRows.length,
      },
    });
    const manifest: LearningEvidenceManifest = {
      schemaVersion: "1.0.0",
      analysisVersion: "1.0.0",
      analysisId: "learning-materialization-weekly",
      source: {
        snapshotId: "plimsoll-local-learning-facts",
        queryHash: createHash("sha256").update(queryMaterial).digest("hex"),
        rowDigest: computeLearningPairDigest(pairs),
        declaredPairCount: pairs.length,
        sourceKind: "local_owned_aggregate",
      },
      metricVersions: {
        outcomeMetric: "1.0.0",
        techniqueExposure: "1.0.0",
        projectAllocation: "1.0.0",
      },
      outcomeContract: {
        metricId: "time_to_first_green",
        metricVersion: "1.0.0",
        unit: "milliseconds",
        direction: "lower_is_better",
      },
      techniqueContract,
      window: { startInclusive: windowStart, endExclusive: until },
      asOf: until,
      hypothesisFamily: {
        familyId: "learning-materialization-weekly",
        hypothesisIndex: 1,
        hypothesesTested: 1,
        selectionPolicy: "pre_registered",
        correction: "none",
        familyWiseAlpha: 0.05,
        registeredAt: windowStart,
      },
      gates: { ...MATERIALIZATION_GATES },
      declaredConfounders: ["session_linkage_starvation", "provider_check_coverage"],
      pairs,
    };
    const run = compileLearningEvidencePacket(manifest, {
      previousSourceFingerprint: stateBefore.lastSourceFingerprint,
    });

    let outputWritten = false;
    let resolvedOutput: string | null = null;
    if (run.status === "computed") {
      if (!input.outPath) throw new Error("an output path is required when a packet computes");
      resolvedOutput = assertLearningReviewOutputPath(input.outPath, input.workspaceRoot ?? process.cwd());
      fs.mkdirSync(path.dirname(resolvedOutput), { recursive: true });
      const temporary = `${resolvedOutput}.tmp-${process.pid}`;
      try {
        fs.writeFileSync(temporary, `${JSON.stringify(run.packet, null, 2)}\n`, {
          encoding: "utf8",
          mode: 0o600,
        });
        fs.renameSync(temporary, resolvedOutput);
      } finally {
        fs.rmSync(temporary, { force: true });
      }
      outputWritten = true;
    }
    const advancedState: MaterializationState = {
      schemaVersion: LEARNING_MATERIALIZATION_STATE_SCHEMA,
      usageHighWaterRowId: usageHighWaterAfter,
      conservation: mergeConservation(stateBefore.conservation, usageDelta),
      unvalidatedLineageEvents: stateBefore.unvalidatedLineageEvents + lineageUnknownFirst,
      lastSourceFingerprint: run.sourceFingerprint,
      generation: run.status === "computed" ? stateBefore.generation + 1 : stateBefore.generation,
    };
    stateStore.write(advancedState, new Date().toISOString());
    return {
      schema: LEARNING_MATERIALIZATION_SCHEMA,
      scheduled: false,
      continuousLoop: false,
      modelCalls: 0,
      localOnly: true,
      status: run.status,
      sources,
      window: { startInclusive: windowStart, endExclusive: until, days: input.windowDays },
      until,
      scanned,
      usageHighWater: { before: stateBefore.usageHighWaterRowId, after: usageHighWaterAfter },
      conservation: advancedState.conservation,
      unvalidatedLineageEvents: advancedState.unvalidatedLineageEvents,
      shortfalls,
      pairing: {
        candidateAssignments: candidatesByAssignment.size,
        formedPairs: pairs.length,
        techniqueContracts: contracts.size,
      },
      dependencyReasons,
      sourceFingerprint: run.sourceFingerprint,
      generation: advancedState.generation,
      packetClaimClass: run.packet?.claimClass ?? null,
      notEstimableReasons: run.packet ? [...run.packet.notEstimableReasons] : [],
      outputWritten,
      outputPath: outputWritten ? resolvedOutput : null,
    };
  } finally {
    ledger?.close();
    outcomeDb?.close();
    stateStore.close();
  }
}

function collectDependencyReasons(input: {
  pairs: number;
  contracts: number;
  shortfalls: MaterializerShortfalls;
  outcomeStorePresent: boolean;
}): string[] {
  const reasons: string[] = [];
  if (!input.outcomeStorePresent) reasons.push("outcome_timeline_store_absent");
  if (input.pairs === 0) reasons.push("no_complete_treatment_control_assignments");
  if (input.contracts === 0) reasons.push("no_prospective_technique_exposure");
  if (input.contracts > 1) reasons.push("mixed_technique_contracts");
  if (input.shortfalls.retrospectiveExposures > 0) reasons.push("retrospective_exposure_excluded");
  if (input.shortfalls.episodesMissingSessionUsage > 0) reasons.push("episode_session_usage_incomplete");
  if (input.shortfalls.unmappedProjectIdentities > 0) reasons.push("project_allocation_unmapped");
  if (input.shortfalls.sessionHeadShaAmbiguous > 0) reasons.push("session_head_sha_ambiguous");
  if (input.shortfalls.outcomeLinkageUnavailable > 0) reasons.push("outcome_linkage_unavailable");
  if (input.shortfalls.episodesUnresolvedAttempts > 0) reasons.push("unresolved_attempt_evidence");
  return reasons;
}
