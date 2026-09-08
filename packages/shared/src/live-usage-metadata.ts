/** Generated observer metadata. Validation describes evidence, never grants authority. */
export const LIVE_USAGE_EVENT_TYPE = "usage_live" as const;
export const LIVE_USAGE_SOURCE_VERSION = "codex.app-server.usage.v1" as const;

export type LiveUsageObservation = {
  intervalStart: string;
  intervalEnd: string;
  attributionState: "qualified" | "unresolved";
  financeEligibility: "unqualified_observer";
  totalTokens: number;
  reasoningOutputTokens: number;
};

function canonicalTimestamp(value: unknown): value is string {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) return false;
  const milliseconds = Date.parse(value);
  return Number.isFinite(milliseconds) && new Date(milliseconds).toISOString() === value;
}

function counter(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && !Object.is(value, -0);
}

type LiveUsageCounters = {
  inputTokens?: unknown;
  outputTokens?: unknown;
  cacheReadTokens?: unknown;
  cacheCreationTokens?: unknown;
};

export function validLiveUsageCounters(usage: LiveUsageCounters, observation: LiveUsageObservation): boolean {
  const values = [usage.inputTokens, usage.outputTokens, usage.cacheReadTokens, usage.cacheCreationTokens,
    observation.totalTokens, observation.reasoningOutputTokens];
  return values.every(counter) && values.some(value => (value as number) > 0) &&
    (usage.cacheReadTokens as number) <= (usage.inputTokens as number) &&
    observation.reasoningOutputTokens <= (usage.outputTokens as number);
}

/** Validate the complete generated event before treating its interval as evidence. */
export function readLiveUsageEventObservation(event: LiveUsageCounters & {
  id: string;
  source: string;
  eventType?: string;
  observedAt: string;
  metadata: Record<string, unknown>;
}): LiveUsageObservation | null {
  const metadata = event.metadata;
  const identifier = (value: unknown) => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(value);
  // The existing cloud transport derives a UUID from a local non-UUID event id.
  // Its source and logical ids retain the original observer identity unchanged.
  if (event.source !== "codex" || event.eventType !== LIVE_USAGE_EVENT_TYPE ||
      !identifier(event.id) || !identifier(metadata.sourceEventId) ||
      metadata.logicalSourceEventId !== metadata.sourceEventId ||
      !identifier(metadata.captureRootId) || !identifier(metadata.captureProfileId) ||
      !identifier(metadata.installationEpochId) || typeof metadata.sourcePayloadDigest !== "string" ||
      !/^[a-f0-9]{64}$/.test(metadata.sourcePayloadDigest)) return null;
  const observation = readLiveUsageObservation(metadata, event.observedAt);
  return observation && validLiveUsageCounters(event, observation) ? observation : null;
}

export function readLiveUsageObservation(
  metadata: Record<string, unknown>,
  observedAt: string,
): LiveUsageObservation | null {
  if (metadata.sourceVersion !== LIVE_USAGE_SOURCE_VERSION ||
      metadata.sourceIdentityEvidenceRef !== "native_runtime_observed_interval_v1" ||
      metadata.liveObservationKind !== "observed_interval" ||
      metadata.liveFinanceEligibility !== "unqualified_observer" ||
      (metadata.liveAttributionState !== "qualified" && metadata.liveAttributionState !== "unresolved") ||
      !canonicalTimestamp(metadata.liveIntervalStart) || !canonicalTimestamp(metadata.liveIntervalEnd) ||
      metadata.liveIntervalEnd !== observedAt || metadata.liveIntervalStart > metadata.liveIntervalEnd ||
      (metadata.liveIntervalStart === metadata.liveIntervalEnd && metadata.liveAttributionState !== "unresolved") ||
      !counter(metadata.liveTotalTokens) || !counter(metadata.liveReasoningOutputTokens)) return null;
  return {
    intervalStart: metadata.liveIntervalStart,
    intervalEnd: metadata.liveIntervalEnd,
    attributionState: metadata.liveAttributionState,
    financeEligibility: "unqualified_observer",
    totalTokens: metadata.liveTotalTokens,
    reasoningOutputTokens: metadata.liveReasoningOutputTokens,
  };
}
