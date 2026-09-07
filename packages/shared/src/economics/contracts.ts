/** Metadata-only economics boundary. Finance records enter only through an attested adapter. */
export const ECONOMICS_SCHEMA_VERSION="plimsoll.workspace-economics.v1" as const;
export type Period={
  start: string;
  end: string;
};
export type CostKind="reported"|"estimated"|"unknown";
export type AttributionSource="dispatch"|"registry"|"workspace"|"unallocated";
export type UsageAmounts={
  inputTokens: number|null;
  outputTokens: number|null;
  cacheReadTokens: number|null;
  cacheCreationTokens: number|null;
};
export type UsageFact=UsageAmounts&{
  tenantId: string;
  installationEpochId: string|null;
  source: string;
  sourceEventId: string;
  sourceVersion: string;
  schemaVersion: string;
  observedAt: string;
  receivedAt: string;
  timePrecision: "millisecond"|"second"|"unknown";
  nativeSessionId: string|null;
  attemptId: string|null;
  parentAttemptId: string|null;
  accountId: string|null;
  model: string|null;
  /** Provider-native identity or an attested alias, independent of profile, account and installation. */
  logicalEventId: string|null;
  identityEvidenceRef: string|null;
  payloadDigest: string;
  evidenceRef: string;
  projectKey: string|null;
  companyRef: string|null;
  workItemId: string|null;
  acceptedOutcomeId: string|null;
  attributionSource: AttributionSource;
  costUsd: number|null;
  costKind: CostKind;
  rateVersion: string|null;
  rateEffectiveAt: string|null;
  allocation?: {
    policyVersion: string;
    evidenceRef: string;
    weights: Array<{
      projectKey: string|null;
      weight: number;
    }>;
  };
};
export type ProjectMapping={
  projectKey: string;
  companyRef: string|null;
  workItemId: string|null;
  source: Exclude<AttributionSource,"unallocated">;
  effectiveFrom: string;
  effectiveTo: string|null;
  evidenceRef: string;
};
export type AcceptanceFact={
  tenantId: string;
  id: string;
  workItemId: string;
  projectKey: string;
  artifactRef: string;
  artifactRevision: string;
  criteriaVersion: string;
  reviewerRef: string;
  evidenceRef: string;
  acceptedAt: string;
  exposureEndsAt: string;
  reopenedAt: string|null;
  supersededBy: string|null;
};
export type CostSummary={
  reportedUsd: number|null;
  estimatedUsd: number|null;
  unclassifiedUsd: number|null;
  pricedEvents: number;
  unpricedEvents: number;
  rateUnboundEvents: number;
};
export type UsageSummary=UsageAmounts&{
  events: number;
  unknownTokenEvents: number;
};
export type FinanceLine={
  tenantId: string;
  companyRef: string;
  sourceLineId: string;
  sourceVersion: string;
  evidenceRef: string;
  statementRef: string;
  attestationRef: string;
  period: Period;
  currency: string;
  exponent: number;
  amountMinor: string;
  kind: "recognized_expense"|"recognized_revenue"|"cash_movement";
  taxTreatment: string;
  projectKey: string|null;
};
export type ExpenseSplit={
  id: string;
  kind: "direct"|"shared";
  amountMinor: string;
  weights: Array<{
    projectKey: string|null;
    weight: number;
  }>;
};
export type ExpenseAllocationPlan={
  tenantId: string;
  sourceLineId: string;
  sourceVersion: string;
  period: Period;
  policyVersion: string;
  generation: number;
  manifestDigest: string;
  evidenceRef: string;
  splits: ExpenseSplit[];
};
export type ExpenseAllocation={
  sourceLineId: string;
  sourceVersion: string;
  projectKey: string|null;
  companyRef: string;
  currency: string;
  exponent: number;
  amountMinor: string;
  kind: "direct"|"shared"|"unallocated";
  splitId: string|null;
  policyVersion: string|null;
  generation: number|null;
  period: Period;
  evidenceRef: string;
  reversalOf: string|null;
  allocationId: string;
};
export type AllocationReversal={
  tenantId: string;
  sourceLineId: string;
  sourceVersion: string;
  evidenceRef: string;
  /** Reverses the original accepted rows exactly; no current weights supplied. */
  allocationIds: string[];
};
export type FinanceSnapshot={
  tenantId: string;
  complete: boolean;
  period: Period;
  observedThrough: string;
  manifestDigest: string;
  attestationRef: string;
  lines: FinanceLine[];
  plans: ExpenseAllocationPlan[];
  reversals: AllocationReversal[];
  /** Original accepted Finance rows, only for exact credit reversal. */
  priorAllocations?: ExpenseAllocation[];
};
export type FinanceView={
  state: "reconciled"|"partial"|"unavailable";
  reasons: string[];
  allocations: ExpenseAllocation[];
  currencies: Array<{
    currency: string;
    exponent: number;
    recognizedRevenueMinor: string|null;
    recognizedDirectExpenseMinor: string|null;
    allocatedSharedExpenseMinor: string|null;
    unallocatedExpenseMinor: string|null;
    cashMovementMinor: string|null;
    contributionMinor: string|null;
  }>;
};
export type BaselineSample={
  tenantId: string;
  workItemId: string;
  acceptanceId: string;
  cohort: string;
  startedAt: string;
  acceptedAt: string;
  fullWorkflowDurationMs: number;
  costMinor: string|null;
  currency: string|null;
  exponent: number|null;
  includesReviewAndRepair: boolean;
  evidenceRef: string;
};
export type ForecastInput={
  cohort: string;
  remainingItems: number;
  baseline: BaselineSample[];
};
export type ForecastView={
  state: "scenario"|"unavailable";
  method: "comparable_workflow_mean"|null;
  sampleSize: number;
  remainingItems: number|null;
  estimatedCostMinor: string|null;
  currency: string|null;
  exponent: number|null;
  estimatedDurationMs: number|null;
  p50At: null;
  p80At: null;
  reasons: string[];
  evidenceRefs: string[];
};
export type ExperimentFact={
  tenantId: string;
  caseId: string;
  hypothesis: string;
  cohort: string;
  ownerRef: string;
  changeHash: string;
  window: Period;
  assignment: "randomized"|"matched";
  prespecifiedAt: string;
  practicalEffectThreshold: number;
  acceptanceThreshold: number;
  baselineAssigned: number;
  treatmentAssigned: number;
  baselineCompleted: number;
  treatmentCompleted: number;
  baselineAccepted: number;
  treatmentAccepted: number;
  baselineCostMinor: string;
  treatmentCostMinor: string;
  implementationCostMinor: string;
  currency: string;
  exponent: number;
  reviewAndRepairIncluded: boolean;
  independentAcceptanceRef: string|null;
  rollbackRef: string|null;
  evidenceRefs: string[];
  confounders: string[];
};
export type ExperimentDecision={
  caseId: string;
  ownerRef: string;
  decision: "adopt"|"revert"|"inconclusive";
  reasons: string[];
  evidenceRefs: string[];
  observedEffect: number|null;
};
export type ProjectEconomics={
  projectKey: string|null;
  companyRef: string|null;
  workItems: number;
  acceptedOutcomes: number;
  matureAcceptedOutcomes: number;
  usage: UsageSummary;
  costs: CostSummary;
};
export type WorkspaceEconomics={
  schemaVersion: typeof ECONOMICS_SCHEMA_VERSION;
  tenantId: string;
  generatedAt: string;
  period: Period;
  coverage: {
    state: "complete"|"partial"|"unavailable";
    observedThrough: string|null;
    capturedEvents: number;
    admittedEvents: number;
    duplicateEvents: number;
    quarantinedEvents: number;
    attributedEvents: number;
    unallocatedEvents: number;
    workLinkedEvents: number;
    acceptedOutcomeLinkedEvents: number;
    truncated: boolean;
  };
  usage: UsageSummary;
  costs: CostSummary;
  projects: ProjectEconomics[];
  finance: FinanceView;
  forecast: ForecastView;
  experiments: ExperimentDecision[];
  evidenceGaps: string[];
};
export type EconomicsInput={
  tenantId: string;
  period: Period;
  now: string;
  events: UsageFact[];
  complete: boolean;
  observedThrough: string|null;
  truncated?: boolean;
  projectMappings?: ProjectMapping[];
  acceptances?: AcceptanceFact[];
  finance?: FinanceSnapshot|null;
  forecast?: ForecastInput|null;
  experiments?: ExperimentFact[];
};
