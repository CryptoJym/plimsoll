import type { UsageFact } from "./contracts";
import { digest } from "./validation";
export type StoredUsageEvent={
  id: string;
  tenantId: string;
  source: string;
  observedAt: string;
  receivedAt: string;
  sessionId: string|null;
  accountId: string|null;
  model: string|null;
  projectKey: string|null;
  inputTokens: number|null;
  outputTokens: number|null;
  cacheReadTokens: number|null;
  cacheCreationTokens: number|null;
  costUsd: number|null;
  metadata: Record<string,unknown>;
};
export function safeIdentity(value: unknown): string|null {
  return typeof value==="string"&&/^[A-Za-z0-9][A-Za-z0-9:._/#-]{0,255}$/.test(value)? value:null;
}
/** Only allowlisted metadata enters economics. No raw payload or inferred login ownership. */
export function usageFactFromEvent(row: StoredUsageEvent): UsageFact {
  const m=row.metadata;
  const kind=m.costKind==="reported"||m.costKind==="estimated"? m.costKind:m.costEstimated===true? "estimated":"unknown";
  const workItemId=safeIdentity(m.workItemId);
  const explicitProject=workItemId&&safeIdentity(m.workEvidenceRef)? safeIdentity(m.dispatchProjectKey):null;
  const projectKey=m.workAttributionState==="conflict"? null:explicitProject??safeIdentity(row.projectKey);
  const sourceEventId=safeIdentity(m.sourceEventId)??row.id;
  const amounts={
    inputTokens: row.inputTokens,outputTokens: row.outputTokens,cacheReadTokens: row.cacheReadTokens,
    cacheCreationTokens: row.cacheCreationTokens,costUsd: row.costUsd
  };
  const suppliedDigest=typeof m.sourcePayloadDigest==="string"&&/^[a-f0-9]{64}$/.test(m.sourcePayloadDigest)? m.sourcePayloadDigest:null;
  return {
    tenantId: row.tenantId,installationEpochId: safeIdentity(m.installationEpochId),source: row.source,sourceEventId,
    sourceVersion: safeIdentity(m.sourceVersion)??"legacy_unversioned",schemaVersion: "stored_usage_v1",
    observedAt: row.observedAt,receivedAt: row.receivedAt,timePrecision: "millisecond",nativeSessionId: safeIdentity(row.sessionId),
    attemptId: safeIdentity(m.attemptId),parentAttemptId: safeIdentity(m.parentAttemptId),accountId: safeIdentity(m.captureAccountHash)??safeIdentity(row.accountId),
    model: row.model,logicalEventId: safeIdentity(m.logicalSourceEventId),identityEvidenceRef: safeIdentity(m.sourceIdentityEvidenceRef),
    payloadDigest: suppliedDigest??digest({ source: row.source,sourceEventId,observedAt: row.observedAt,model: row.model,...amounts }),
    evidenceRef: `event:${row.id}`,projectKey,companyRef: safeIdentity(m.companyRef),workItemId,
    acceptedOutcomeId: safeIdentity(m.acceptedOutcomeId),attributionSource: projectKey===null? "unallocated":explicitProject&&safeIdentity(m.workEvidenceRef)? "dispatch":"workspace",
    ...amounts,costKind: kind,rateVersion: safeIdentity(m.rateVersion),rateEffectiveAt: typeof m.rateEffectiveAt==="string"? m.rateEffectiveAt:null
  };
}
