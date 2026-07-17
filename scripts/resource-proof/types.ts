export const RESOURCE_PROOF_SCHEMA = "plimsoll.resource-proof.v1" as const;

export const WORK_COUNTER_NAMES = [
  "eventsObserved",
  "eventsAdmitted",
  "eventsDropped",
  "rawEventWrites",
  "rawEventRewrites",
  "rawRowsScanned",
  "filesOpened",
  "fileBytesRead",
  "fullHistoryFileReads",
  "projectionRowsVisited",
  "projectionRowsWritten",
  "outboxRowsEnqueued",
  "outboxAttempts",
  "deadLettersWritten",
  "repriceRowsVisited",
  "reconciliationRowsVisited",
  "enrichmentRowsVisited",
  "maintenanceRuns",
  "overlappingJobs",
  "listenersCreated",
  "restartRequests",
  "filesystemEntriesScanned",
  "learningFactRowsWritten",
] as const;

export type WorkCounterName = (typeof WORK_COUNTER_NAMES)[number];
export type WorkCounters = Record<WorkCounterName, number>;
export type ScenarioStatus = "pass" | "fail" | "not_wired" | "skipped";

export type ScenarioReceipt = {
  id: string;
  required: boolean;
  status: ScenarioStatus;
  detail: string;
  durationMs: number | null;
  counters: WorkCounters;
  measurements?: Record<string, number | string | boolean | null>;
  blockedBy?: string[];
};

export type ResourceProofReceipt = {
  schema: typeof RESOURCE_PROOF_SCHEMA;
  generatedAt: string;
  overall: "pass" | "scaffold_ready" | "fail";
  gateReady: boolean;
  requireIntegrated: boolean;
  environment: {
    isolation: "temporary-home-db-and-session-roots";
    loopbackPort: "held-and-challenged" | "unverified";
    providerNetwork: "not-configured";
    credentials: "scrubbed-by-allowlist" | "unverified";
    liveStateTouched: false;
    node: string;
    platform: NodeJS.Platform;
  };
  summary: {
    passed: number;
    failed: number;
    notWired: number;
    skipped: number;
    requiredIncomplete: number;
  };
  scenarios: ScenarioReceipt[];
};

export function emptyWorkCounters(): WorkCounters {
  return Object.fromEntries(WORK_COUNTER_NAMES.map((name) => [name, 0])) as WorkCounters;
}
