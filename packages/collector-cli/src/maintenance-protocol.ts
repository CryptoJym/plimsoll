import type { CollectorMaintenanceRunResult, MaintenanceRunOutcome } from "./maintenance";
import type { MaintenanceProgress, MaintenanceProgressStage } from "./maintenance-progress";
import {
  validRepoContextRequest,
  validRepoContextResult,
  type RepoContextRequest,
  type RepoContextResult,
} from "./repo-context";

export const MAINTENANCE_PROTOCOL_SCHEMA = 2 as const;
export const MAINTENANCE_PROTOCOL_MAX_BYTES = 64 * 1024;
export const MAINTENANCE_PROTOCOL_MAX_FRAMES_PER_JOB = 128;
export const MAINTENANCE_PROTOCOL_MAX_REPO_CONTEXTS = 8;

export type MaintenanceRunRequest = {
  schema: typeof MAINTENANCE_PROTOCOL_SCHEMA;
  type: "run";
  generation: number;
  nonce: string;
  deadlineMs: number;
  quarantine: MaintenanceProgress | null;
  repoContexts: RepoContextRequest[];
};

export type MaintenanceShutdownRequest = {
  schema: typeof MAINTENANCE_PROTOCOL_SCHEMA;
  type: "shutdown";
  nonce: string;
};

export type MaintenanceAckRequest = {
  schema: typeof MAINTENANCE_PROTOCOL_SCHEMA;
  type: "ack";
  generation: number;
  nonce: string;
  sequence: number;
};

export type MaintenanceWorkerRequest =
  | MaintenanceRunRequest
  | MaintenanceShutdownRequest
  | MaintenanceAckRequest;

export type MaintenanceReadyReceipt = {
  schema: typeof MAINTENANCE_PROTOCOL_SCHEMA;
  type: "ready";
  spawnNonce: string;
};

export type MaintenanceResultReceipt = {
  schema: typeof MAINTENANCE_PROTOCOL_SCHEMA;
  type: "result";
  generation: number;
  nonce: string;
  sequence: number;
  result: MaintenanceRunOutcome;
  repoContexts: RepoContextResult[];
};

export type MaintenanceErrorReceipt = {
  schema: typeof MAINTENANCE_PROTOCOL_SCHEMA;
  type: "error";
  generation: number;
  nonce: string;
  sequence: number;
  reason: "maintenance_failed" | "worker_busy" | "invalid_request";
};

export type MaintenanceProgressReceipt = {
  schema: typeof MAINTENANCE_PROTOCOL_SCHEMA;
  type: "progress";
  generation: number;
  nonce: string;
  sequence: number;
  stage: MaintenanceProgressStage;
  source: "codex" | "claude_code";
  candidateHash: string | null;
};

export type MaintenanceClosedReceipt = {
  schema: typeof MAINTENANCE_PROTOCOL_SCHEMA;
  type: "closed";
  nonce: string;
};

export type MaintenanceWorkerReceipt =
  | MaintenanceReadyReceipt
  | MaintenanceResultReceipt
  | MaintenanceProgressReceipt
  | MaintenanceErrorReceipt
  | MaintenanceClosedReceipt;

const NONCE = /^[a-f0-9-]{16,80}$/i;

function boundedFrame(value: unknown) {
  try {
    return Buffer.byteLength(JSON.stringify(value), "utf8") <= MAINTENANCE_PROTOCOL_MAX_BYTES;
  } catch {
    return false;
  }
}

function validGeneration(value: unknown) {
  return Number.isSafeInteger(value) && Number(value) > 0;
}

function validNonce(value: unknown) {
  return typeof value === "string" && NONCE.test(value);
}

function exactKeys(row: Record<string, unknown>, keys: readonly string[]) {
  const actual = Object.keys(row).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function count(value: unknown) {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

const PROGRESS_STAGES = new Set<MaintenanceProgressStage>([
  "source_scan",
  "discovery_directory",
  "discovery_read",
  "candidate_metadata",
  "jsonl_open",
  "jsonl_validation",
  "git_context",
]);

function parseProgress(value: unknown): MaintenanceProgress | null {
  const row = record(value);
  if (!row || !exactKeys(row, ["source", "stage", "candidateHash"]) ||
    (row.source !== "codex" && row.source !== "claude_code") ||
    typeof row.stage !== "string" || !PROGRESS_STAGES.has(row.stage as MaintenanceProgressStage) ||
    (row.candidateHash !== null &&
      (typeof row.candidateHash !== "string" || !/^sha256:[a-f0-9]{64}$/i.test(row.candidateHash))) ||
    (row.stage === "source_scan" ? row.candidateHash !== null : row.candidateHash === null)) return null;
  return {
    source: row.source,
    stage: row.stage as MaintenanceProgressStage,
    candidateHash: row.candidateHash as string | null,
  };
}

function parseRepoContextRequests(value: unknown): RepoContextRequest[] | null {
  if (!Array.isArray(value) || value.length > MAINTENANCE_PROTOCOL_MAX_REPO_CONTEXTS) return null;
  const parsed: RepoContextRequest[] = [];
  const ids = new Set<string>();
  for (const candidate of value) {
    if (!validRepoContextRequest(candidate) || ids.has(candidate.contextId)) return null;
    ids.add(candidate.contextId);
    parsed.push({
      contextId: candidate.contextId,
      source: candidate.source,
      cwd: candidate.cwd,
    });
  }
  return parsed;
}

function parseRepoContextResults(value: unknown): RepoContextResult[] | null {
  if (!Array.isArray(value) || value.length > MAINTENANCE_PROTOCOL_MAX_REPO_CONTEXTS) return null;
  const parsed: RepoContextResult[] = [];
  const ids = new Set<string>();
  for (const candidate of value) {
    if (!validRepoContextResult(candidate) || ids.has(candidate.contextId)) return null;
    ids.add(candidate.contextId);
    parsed.push({
      contextId: candidate.contextId,
      repoHash: candidate.repoHash,
      branchHash: candidate.branchHash,
      headSha: candidate.headSha,
      resolvedAt: candidate.resolvedAt,
      resolverVersion: candidate.resolverVersion,
    });
  }
  return parsed;
}

export function parseMaintenanceWorkerRequest(value: unknown): MaintenanceWorkerRequest | null {
  if (!boundedFrame(value) || !value || typeof value !== "object") return null;
  const row = value as Record<string, unknown>;
  if (row.schema !== MAINTENANCE_PROTOCOL_SCHEMA || !validNonce(row.nonce)) return null;
  if (row.type === "shutdown") {
    if (!exactKeys(row, ["schema", "type", "nonce"])) return null;
    return {
      schema: MAINTENANCE_PROTOCOL_SCHEMA,
      type: "shutdown",
      nonce: row.nonce as string,
    };
  }
  if (row.type === "ack") {
    if (!exactKeys(row, ["schema", "type", "generation", "nonce", "sequence"]) ||
      !validGeneration(row.generation) || !validGeneration(row.sequence)) return null;
    return {
      schema: MAINTENANCE_PROTOCOL_SCHEMA,
      type: "ack",
      generation: Number(row.generation),
      nonce: row.nonce as string,
      sequence: Number(row.sequence),
    };
  }
  if (
    row.type !== "run" ||
    !exactKeys(row, [
      "schema",
      "type",
      "generation",
      "nonce",
      "deadlineMs",
      "quarantine",
      "repoContexts",
    ]) ||
    !validGeneration(row.generation) ||
    !Number.isSafeInteger(row.deadlineMs) ||
    Number(row.deadlineMs) < 1 ||
    Number(row.deadlineMs) > 60_000
  ) return null;
  const quarantine = row.quarantine === null ? null : parseProgress(row.quarantine);
  if (row.quarantine !== null && !quarantine) return null;
  const repoContexts = parseRepoContextRequests(row.repoContexts);
  if (!repoContexts) return null;
  return {
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "run",
    generation: Number(row.generation),
    nonce: row.nonce as string,
    deadlineMs: Number(row.deadlineMs),
    quarantine,
    repoContexts,
  };
}

export function projectMaintenanceResult(result: CollectorMaintenanceRunResult): MaintenanceRunOutcome {
  return {
    recentOnly: true,
    rollout: {
      filesRead: result.rollout.filesRead,
      parseErrors: result.rollout.parseErrors,
      eventsAppended: result.rollout.eventsAppended,
      activity: { discoveryEntries: result.rollout.activity.discoveryEntries },
    },
    transcript: {
      filesRead: result.transcript.filesRead,
      parseErrors: result.transcript.parseErrors,
      eventsAppended: result.transcript.eventsAppended,
      activity: { discoveryEntries: result.transcript.activity.discoveryEntries },
    },
    reconciliation: {
      rowsChanged: result.reconciliation.rowsChanged,
      rowsVisited: result.reconciliation.rowsVisited,
    },
    repricing: {
      repriced: result.repricing.repriced,
      rowsVisited: result.repricing.rowsVisited,
    },
    enrichment: {
      backward: result.enrichment.backward,
      forward: result.enrichment.forward,
      rowsVisited: result.enrichment.rowsVisited,
    },
    rawEventWrites: result.rawEventWrites,
  };
}

function parseMaintenanceResult(value: unknown): MaintenanceRunOutcome | null {
  const row = record(value);
  if (!row || !exactKeys(row, ["recentOnly", "rollout", "transcript", "reconciliation", "repricing", "enrichment", "rawEventWrites"]) ||
    row.recentOnly !== true || !count(row.rawEventWrites)) return null;
  const parseSource = (value: unknown) => {
    const source = record(value);
    if (!source || !exactKeys(source, ["filesRead", "parseErrors", "eventsAppended", "activity"]) ||
      !count(source.filesRead) || !count(source.parseErrors) || !count(source.eventsAppended)) return null;
    const activity = record(source.activity);
    if (!activity || !exactKeys(activity, ["discoveryEntries"]) || !count(activity.discoveryEntries)) return null;
    return {
      filesRead: Number(source.filesRead),
      parseErrors: Number(source.parseErrors),
      eventsAppended: Number(source.eventsAppended),
      activity: { discoveryEntries: Number(activity.discoveryEntries) },
    };
  };
  const rollout = parseSource(row.rollout);
  const transcript = parseSource(row.transcript);
  const reconciliation = record(row.reconciliation);
  const repricing = record(row.repricing);
  const enrichment = record(row.enrichment);
  if (!rollout || !transcript ||
    !reconciliation || !exactKeys(reconciliation, ["rowsChanged", "rowsVisited"]) || !count(reconciliation.rowsChanged) || !count(reconciliation.rowsVisited) ||
    !repricing || !exactKeys(repricing, ["repriced", "rowsVisited"]) || !count(repricing.repriced) || !count(repricing.rowsVisited) ||
    !enrichment || !exactKeys(enrichment, ["backward", "forward", "rowsVisited"]) || !count(enrichment.backward) || !count(enrichment.forward) || !count(enrichment.rowsVisited)) return null;
  return {
    recentOnly: true,
    rollout,
    transcript,
    reconciliation: { rowsChanged: Number(reconciliation.rowsChanged), rowsVisited: Number(reconciliation.rowsVisited) },
    repricing: { repriced: Number(repricing.repriced), rowsVisited: Number(repricing.rowsVisited) },
    enrichment: { backward: Number(enrichment.backward), forward: Number(enrichment.forward), rowsVisited: Number(enrichment.rowsVisited) },
    rawEventWrites: Number(row.rawEventWrites),
  };
}

export function parseMaintenanceWorkerReceipt(value: unknown): MaintenanceWorkerReceipt | null {
  if (!boundedFrame(value) || !value || typeof value !== "object") return null;
  const row = value as Record<string, unknown>;
  if (row.schema !== MAINTENANCE_PROTOCOL_SCHEMA) return null;
  if (row.type === "ready") {
    if (!exactKeys(row, ["schema", "type", "spawnNonce"])) return null;
    if (!validNonce(row.spawnNonce)) return null;
    return row as MaintenanceReadyReceipt;
  }
  if (row.type === "closed") {
    if (!exactKeys(row, ["schema", "type", "nonce"])) return null;
    if (!validNonce(row.nonce)) return null;
    return row as MaintenanceClosedReceipt;
  }
  if (!validGeneration(row.generation) || !validNonce(row.nonce)) return null;
  if (row.type === "result") {
    if (!exactKeys(row, [
      "schema",
      "type",
      "generation",
      "nonce",
      "sequence",
      "result",
      "repoContexts",
    ]) || !validGeneration(row.sequence)) return null;
    const result = parseMaintenanceResult(row.result);
    const repoContexts = parseRepoContextResults(row.repoContexts);
    if (!result || !repoContexts) return null;
    return {
      schema: MAINTENANCE_PROTOCOL_SCHEMA,
      type: "result",
      generation: Number(row.generation),
      nonce: row.nonce as string,
      sequence: Number(row.sequence),
      result,
      repoContexts,
    };
  }
  if (row.type === "progress") {
    if (!exactKeys(row, ["schema", "type", "generation", "nonce", "sequence", "stage", "source", "candidateHash"]) || !validGeneration(row.sequence)) return null;
    const progress = parseProgress({ source: row.source, stage: row.stage, candidateHash: row.candidateHash });
    if (!progress) return null;
    return {
      schema: MAINTENANCE_PROTOCOL_SCHEMA,
      type: "progress",
      generation: Number(row.generation),
      nonce: row.nonce as string,
      sequence: Number(row.sequence),
      ...progress,
    };
  }
  if (row.type === "error") {
    if (!exactKeys(row, ["schema", "type", "generation", "nonce", "sequence", "reason"]) || !validGeneration(row.sequence)) return null;
    if (!["maintenance_failed", "worker_busy", "invalid_request"].includes(String(row.reason))) return null;
    return row as MaintenanceErrorReceipt;
  }
  return null;
}

export function maintenanceProtocolFrameBytes(value: unknown) {
  try {
    return Buffer.byteLength(JSON.stringify(value), "utf8");
  } catch {
    return Number.POSITIVE_INFINITY;
  }
}
