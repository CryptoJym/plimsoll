import assert from "node:assert/strict";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";
import { admittedMetadataAttributes, validatedMetadataAttribute } from "../packages/shared/src/analytical-metadata";
import { readLiveUsageObservation } from "../packages/shared/src/live-usage-metadata";
import { usageFactFromEvent } from "../packages/shared/src/economics/event-adapter";
import { buildWorkspaceEconomics } from "../packages/shared/src/economics/service";

const start = "2026-09-08T10:00:00.000Z";
const end = "2026-09-08T10:00:05.000Z";
const metadata = {
  sourceVersion: "codex.app-server.usage.v1",
  sourceIdentityEvidenceRef: "native_runtime_observed_interval_v1",
  sourceEventId: "fixture-live-event",
  logicalSourceEventId: "fixture-live-event",
  sourcePayloadDigest: "a".repeat(64),
  captureRootId: "fixture-root",
  captureProfileId: "fixture-profile",
  installationEpochId: "fixture-epoch",
  liveObservationKind: "observed_interval",
  liveIntervalStart: start,
  liveIntervalEnd: end,
  liveAttributionState: "qualified",
  liveFinanceEligibility: "unqualified_observer",
  liveTotalTokens: 12,
  liveReasoningOutputTokens: 2,
};
const event = { id: "fixture-live-event", sessionId: "fixture-thread", source: "codex", eventType: "usage_live", observedAt: end, inputTokens: 10, outputTokens: 2, cacheReadTokens: 0, cacheCreationTokens: 0, metadata };
const checks: string[] = [];
function check(name: string, run: () => void) { run(); checks.push(name); }

check("valid_interval_and_unpriced_event", () => {
  assert.equal(readLiveUsageObservation(metadata, end)?.intervalStart, start);
  assert.equal(aiInteractionEventSchema.parse(event).eventType, "usage_live");
});
check("generated_fields_survive_outbound_validation_but_not_otlp", () => {
  for (const [key, value] of Object.entries(metadata)) {
    assert.equal(validatedMetadataAttribute(key, value).accepted, true, key);
    for (const surface of ["record", "resource", "scope"] as const) {
      assert.deepEqual(admittedMetadataAttributes({ [key]: value }, surface).attributes, {});
    }
  }
});
check("missing_or_mismatched_generated_lineage_refused", () => {
  for (const key of ["sourceEventId", "logicalSourceEventId", "sourcePayloadDigest", "captureRootId", "captureProfileId", "installationEpochId"]) {
    const missing: Record<string, unknown> = { ...metadata };
    delete missing[key];
    assert.equal(aiInteractionEventSchema.safeParse({ ...event, metadata: missing }).success, false, key);
  }
  for (const patch of [{ sourceEventId: "another-event" }, { logicalSourceEventId: "another-event" },
    { sourcePayloadDigest: "A".repeat(64) }, { captureRootId: "unbounded".repeat(30) }]) {
    assert.equal(aiInteractionEventSchema.safeParse({ ...event, metadata: { ...metadata, ...patch } }).success, false);
  }
});
check("counter_relationships_and_safe_integer_bounds_refused", () => {
  for (const patch of [{ outputTokens: 1 }, { cacheReadTokens: 11 }, { inputTokens: undefined },
    { outputTokens: -0 }, { cacheCreationTokens: Number.MAX_SAFE_INTEGER + 1 }]) {
    assert.equal(aiInteractionEventSchema.safeParse({ ...event, ...patch }).success, false);
  }
  assert.equal(aiInteractionEventSchema.safeParse({ ...event, inputTokens: 0, outputTokens: 0,
    metadata: { ...metadata, liveTotalTokens: 0, liveReasoningOutputTokens: 0 } }).success, false);
});
check("stored_invalid_observers_have_no_valid_interval", () => {
  const row = { ...event, tenantId: "fixture-workspace", receivedAt: end, accountId: null,
    model: null, projectKey: null, costUsd: null };
  for (const patch of [{ outputTokens: 1 }, { metadata: { ...metadata, sourcePayloadDigest: undefined } }]) {
    assert.equal(usageFactFromEvent({ ...row, ...patch }).observedInterval, null);
  }
});
check("ordinary_stored_events_cannot_be_reclassified_by_live_metadata", () => {
  const row = { ...event, eventType: "assistant_response", tenantId: "fixture-workspace", receivedAt: end,
    accountId: null, model: "ordinary-model", projectKey: null, costUsd: 0.5, metadata: { sourceVersion: metadata.sourceVersion } };
  const fact = usageFactFromEvent(row);
  assert.equal(fact.observedInterval, undefined);
  assert.equal(fact.sourceVersion, "legacy_unversioned");
  assert.equal(fact.model, "ordinary-model");
  assert.equal(fact.costUsd, 0.5);
});
check("economics_quarantines_missing_lineage_and_invalid_reasoning", () => {
  const row = { ...event, tenantId: "fixture-workspace", receivedAt: end, accountId: null,
    model: null, projectKey: null, costUsd: null };
  const missing = usageFactFromEvent({ ...row, metadata: {} });
  assert.equal(missing.payloadDigest, ""); assert.equal(missing.sourceEventId, "");
  const valid = usageFactFromEvent(row);
  const invalidReasoning = { ...valid, outputTokens: 1 };
  for (const fact of [missing, invalidReasoning]) {
    const view = buildWorkspaceEconomics({ tenantId: row.tenantId, events: [fact],
      period: { start, end: "2026-09-08T10:01:00.000Z" }, now: "2026-09-08T11:00:00.000Z",
      complete: true, observedThrough: "2026-09-08T10:01:00.000Z" });
    assert.equal(view.usage.events, 0);
    assert.equal(view.coverage.state, "unavailable");
    assert.ok(view.evidenceGaps.includes("observed_interval_evidence_missing"));
  }
});
check("canonical_packet_digest_survives_outbound_without_normalization", () => {
  assert.deepEqual(validatedMetadataAttribute("sourcePayloadDigest", "a".repeat(64)), { accepted: true, value: "a".repeat(64) });
  for (const value of ["A".repeat(64), "a".repeat(63), ` ${"a".repeat(64)}`]) {
    assert.equal(validatedMetadataAttribute("sourcePayloadDigest", value).accepted, false);
  }
});
check("malformed_or_noncanonical_intervals_refused", () => {
  for (const patch of [
    { liveIntervalStart: "2026-09-08T10:00:06.000Z" },
    { liveIntervalStart: "2026-02-30T10:00:00.000Z" },
    { liveIntervalStart: "2026-09-08T10:00:00Z" },
    { liveIntervalEnd: "2026-09-08T10:00:04.000Z" },
    { liveFinanceEligibility: "qualified" },
    { liveAttributionState: "inferred" },
    { liveTotalTokens: Number.MAX_SAFE_INTEGER + 1 },
    { liveReasoningOutputTokens: -0 },
  ]) assert.equal(aiInteractionEventSchema.safeParse({ ...event, metadata: { ...metadata, ...patch } }).success, false);
});
check("zero_duration_requires_unresolved_attribution", () => {
  assert.equal(readLiveUsageObservation({ ...metadata, liveIntervalStart: end }, end), null);
  assert.ok(readLiveUsageObservation({ ...metadata, liveIntervalStart: end, liveAttributionState: "unresolved" }, end));
});
check("observer_never_invents_identity_model_or_cost", () => {
  for (const patch of [{ actorId: "actor" }, { model: "model" }, { costUsd: 0 }, { source: "claude_code" }, { dataMode: "evidence" }, { sessionId: undefined }]) {
    assert.equal(aiInteractionEventSchema.safeParse({ ...event, ...patch }).success, false);
  }
});
check("ordinary_event_schema_preserved", () => {
  assert.equal(aiInteractionEventSchema.safeParse({ ...event, eventType: "assistant_response", model: "model", actorId: "actor", costUsd: 0, metadata: {} }).success, true);
});
console.log(JSON.stringify({ state: "PASS_SHARED_LIVE_INTERFACE_ONLY", checks, nativeProof: false }));
