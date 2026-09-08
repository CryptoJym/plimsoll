import { ECONOMICS_SCHEMA_VERSION,type AcceptanceFact,type CostSummary,type EconomicsInput,type ProjectEconomics,type UsageFact,type UsageSummary,type WorkspaceEconomics } from "./contracts";
import { allocateMinorUnits,reconcileFinance } from "./finance";
import { evaluateExperiment,forecastWork } from "./decisions";
import { assertTenant,count,digest,identifier,period,reject,timestamp } from "./validation";
import { LIVE_USAGE_SOURCE_VERSION,readLiveUsageObservation,validLiveUsageCounters } from "../live-usage-metadata";
const tokenKeys=["inputTokens","outputTokens","cacheReadTokens","cacheCreationTokens"] as const;
const ZERO=BigInt(0);
function nanos(value: number): bigint {
  if(!Number.isFinite(value)||value<0||!Number.isSafeInteger(Math.round(value*1e9)))
    reject("usage_cost");
  return BigInt(Math.round(value*1e9));
}
function signature(row: UsageFact) {
  return digest({
    payloadDigest: row.payloadDigest,source: row.source,observedAt: row.observedAt,model: row.model,
    inputTokens: row.inputTokens,outputTokens: row.outputTokens,cacheReadTokens: row.cacheReadTokens,cacheCreationTokens: row.cacheCreationTokens,
    costUsd: row.costUsd,costKind: row.costKind,projectKey: row.projectKey,companyRef: row.companyRef,workItemId: row.workItemId,
    allocation: row.allocation??null,observedInterval: row.observedInterval??null
  });
}
/** Dedupe observations separately from logical consumption. Conflicts quarantine the whole key. */
export function reconcileUsage(tenantId: string,events: UsageFact[],now: string) {
  const nowMs=timestamp(now),groups=new Map<string,UsageFact[]>(),observations=new Map<string,Set<string>>();
  const epochs=new Map<string,Set<string|null>>(),invalid=new Set<UsageFact>(),reasons=new Set<string>();
  const quarantined: UsageFact[]=[];
  if(events.length>10000)
    reject("usage_limit");
  for(const row of events) {
    assertTenant(tenantId,row.tenantId);
    // A damaged observer has no substitute native identity or packet digest.
    // Quarantine it before the ordinary-fact validators require those fields.
    if((row.sourceVersion===LIVE_USAGE_SOURCE_VERSION||row.observedInterval!==undefined)&&!row.observedInterval) {
      quarantined.push(row);
      reasons.add("observed_interval_evidence_missing");
      continue;
    }
    for(const v of [row.source,row.sourceEventId,row.sourceVersion,row.schemaVersion,row.evidenceRef])
      identifier(v);
    for(const v of [row.installationEpochId,row.attemptId,row.parentAttemptId,row.nativeSessionId,row.accountId,row.projectKey,row.companyRef,row.workItemId,row.acceptedOutcomeId,row.logicalEventId,row.identityEvidenceRef])
      if(v!==null)
        identifier(v);
    for(const key of tokenKeys)
      if(row[key]!==null)
        count(row[key]);
    if(row.costUsd!==null)
      nanos(row.costUsd);
    if(!/^[a-f0-9]{64}$/.test(row.payloadDigest))
      reject("payload_digest");
    const observed=timestamp(row.observedAt),received=timestamp(row.receivedAt);
    if(observed>nowMs||received>nowMs||received<observed) {
      invalid.add(row);
      reasons.add("event_time_invalid");
    }
    if(row.logicalEventId!==null&&!row.identityEvidenceRef) {
      invalid.add(row);
      reasons.add("logical_identity_unattested");
    }
    const obsKey=JSON.stringify([row.source,row.installationEpochId,row.sourceEventId]);
    const signatures=observations.get(obsKey)??new Set<string>();
    signatures.add(signature(row));
    observations.set(obsKey,signatures);
    const sourceKey=JSON.stringify([row.source,row.sourceEventId]);
    const sourceEpochs=epochs.get(sourceKey)??new Set<string|null>();
    sourceEpochs.add(row.installationEpochId);
    epochs.set(sourceKey,sourceEpochs);
    const key=row.logicalEventId&&row.identityEvidenceRef? JSON.stringify([row.source,"logical",row.logicalEventId]):JSON.stringify([row.source,"observation",row.installationEpochId,row.sourceEventId]);
    const group=groups.get(key)??[];
    group.push(row);
    groups.set(key,group);
  }
  const admitted: UsageFact[]=[];
  let duplicateEvents=0;
  for(const group of groups.values()) {
    const conflict=group.some(row => invalid.has(row)||observations.get(JSON.stringify([row.source,row.installationEpochId,row.sourceEventId]))!.size>1);
    const overlap=group.some(row => !row.logicalEventId&&epochs.get(JSON.stringify([row.source,row.sourceEventId]))!.size>1);
    if(conflict||overlap||new Set(group.map(signature)).size>1) {
      quarantined.push(...group);
      reasons.add(overlap? "cross_epoch_identity_unresolved":"source_event_conflict");
      continue;
    }
    admitted.push([...group].sort((a,b) => a.receivedAt.localeCompare(b.receivedAt))[0]);
    duplicateEvents+=group.length-1;
  }
  return { admitted,quarantined,duplicateEvents,reasons: [...reasons].sort() };
}
export function summarizeUsage(events: UsageFact[]): UsageSummary {
  const result: UsageSummary={ events: events.length,inputTokens: 0,outputTokens: 0,cacheReadTokens: 0,cacheCreationTokens: 0,unknownTokenEvents: 0 };
  for(const key of tokenKeys) {
    if(events.some(row => row[key]===null))
      result[key]=null;
    else {
      const sum=events.reduce((total,row) => total+BigInt(row[key]!),ZERO);
      if(sum>BigInt(Number.MAX_SAFE_INTEGER))
        reject("token_sum_overflow");
      result[key]=Number(sum);
    }
  }
  result.unknownTokenEvents=events.filter(row => tokenKeys.some(key => row[key]===null)).length;
  return result;
}
export function summarizeCosts(events: UsageFact[]): CostSummary {
  const sums={ reported: ZERO,estimated: ZERO,unknown: ZERO },counts={ reported: 0,estimated: 0,unknown: 0 };
  for(const row of events)
    if(row.costUsd!==null) {
      sums[row.costKind]+=nanos(row.costUsd);
      counts[row.costKind]++;
    }
  for(const value of Object.values(sums))
    if(value>BigInt(Number.MAX_SAFE_INTEGER))
      reject("cost_sum_overflow");
  return {
    reportedUsd: counts.reported? Number(sums.reported)/1e9:null,estimatedUsd: counts.estimated? Number(sums.estimated)/1e9:null,
    unclassifiedUsd: counts.unknown? Number(sums.unknown)/1e9:null,pricedEvents: events.filter(row => row.costUsd!==null).length,
    unpricedEvents: events.filter(row => row.costUsd===null).length,
    rateUnboundEvents: events.filter(row => row.costKind==="estimated"&&(!row.rateVersion||!row.rateEffectiveAt)).length
  };
}
function acceptancesFor(input: EconomicsInput): AcceptanceFact[] {
  const byId=new Map<string,AcceptanceFact>();
  for(const row of input.acceptances??[]) {
    assertTenant(input.tenantId,row.tenantId);
    for(const value of [row.id,row.workItemId,row.projectKey,row.artifactRef,row.artifactRevision,row.criteriaVersion,row.reviewerRef,row.evidenceRef])
      identifier(value);
    const at=timestamp(row.acceptedAt),end=timestamp(row.exposureEndsAt);
    if(end<at||at>timestamp(input.now))
      reject("acceptance_time");
    if(row.reopenedAt!==null&&(timestamp(row.reopenedAt)<at||timestamp(row.reopenedAt)>timestamp(input.now)))
      reject("reopen_time");
    const prior=byId.get(row.id);
    if(prior&&digest(prior)!==digest(row))
      reject("acceptance_conflict");
    byId.set(row.id,row);
  }
  const active=[...byId.values()].filter(row => row.supersededBy===null),work=new Set<string>();
  for(const row of active) {
    if(work.has(row.workItemId))
      reject("multiple_active_acceptances");
    work.add(row.workItemId);
  }
  return [...byId.values()];
}
export function buildWorkspaceEconomics(input: EconomicsInput): WorkspaceEconomics {
  identifier(input.tenantId);
  const span=period(input.period),nowMs=timestamp(input.now);
  if(span.end>nowMs)
    reject("future_period");
  if(input.observedThrough!==null&&(timestamp(input.observedThrough)>nowMs||timestamp(input.observedThrough)<span.start))
    reject("usage_watermark");
  const reconciliation=reconcileUsage(input.tenantId,input.events,input.now);
  const gaps=new Set(reconciliation.reasons);
  let intervalCoverageIncomplete=false;
  const isLive=(row: UsageFact) => row.sourceVersion===LIVE_USAGE_SOURCE_VERSION||row.observedInterval!==undefined;
  const events=reconciliation.admitted.filter(row => {
    if(!isLive(row))
      return timestamp(row.observedAt)>=span.start&&timestamp(row.observedAt)<span.end;
    const interval=row.observedInterval;
    const valid=interval&&readLiveUsageObservation({
      sourceVersion: LIVE_USAGE_SOURCE_VERSION,sourceIdentityEvidenceRef: "native_runtime_observed_interval_v1",
      liveObservationKind: "observed_interval",liveFinanceEligibility: interval.financeEligibility,
      liveIntervalStart: interval.intervalStart,liveIntervalEnd: interval.intervalEnd,
      liveAttributionState: interval.attributionState,liveTotalTokens: interval.totalTokens,
      liveReasoningOutputTokens: interval.reasoningOutputTokens
    },row.observedAt);
    if(!valid || !validLiveUsageCounters(row,valid)) {
      gaps.add("observed_interval_evidence_missing");
      intervalCoverageIncomplete=true;
      return false;
    }
    const start=timestamp(interval.intervalStart),end=timestamp(interval.intervalEnd);
    if(end<span.start||start>=span.end) return false;
    gaps.add("observed_interval_partial_coverage");
    intervalCoverageIncomplete=true;
    // Preserve the source fact; do not prorate or assign its end to this period.
    if(start<span.start||end>=span.end) {
      gaps.add("observed_interval_crosses_period");
      return false;
    }
    return true;
  }).map(row => {
    if(isLive(row)) {
      row={ ...row,model:null,costUsd:null,costKind:"unknown",rateVersion:null,rateEffectiveAt:null };
      if(row.observedInterval?.attributionState!=="qualified") {
        gaps.add("observed_interval_attribution_unresolved");
        return { ...row,projectKey:null,companyRef:null,workItemId:null,acceptedOutcomeId:null,
          accountId:null,attributionSource:"unallocated" as const,allocation:undefined };
      }
    }
    if(row.attributionSource==="dispatch")
      return row;
    const mappings=(input.projectMappings??[]).filter(mapping => mapping.source==="registry"&&mapping.workItemId!==null&&mapping.workItemId===row.workItemId&&
      timestamp(mapping.effectiveFrom)<=timestamp(row.observedInterval?.intervalStart??row.observedAt)&&
      (mapping.effectiveTo===null||timestamp(row.observedInterval?.intervalEnd??row.observedAt)<timestamp(mapping.effectiveTo)));
    for(const mapping of mappings) {
      identifier(mapping.evidenceRef);
      identifier(mapping.projectKey);
    }
    if(new Set(mappings.map(mapping => `${mapping.companyRef}:${mapping.projectKey}`)).size>1)
      reject("registry_scope_conflict");
    const mapping=mappings[0];
    return mapping? { ...row,projectKey: mapping.projectKey,companyRef: mapping.companyRef,attributionSource: "registry" as const }:row;
  });
  const accepted=acceptancesFor(input);
  const complete=input.complete&&!input.truncated&&input.observedThrough!==null&&timestamp(input.observedThrough)>=span.end&&!reconciliation.quarantined.length&&!intervalCoverageIncomplete;
  if(!complete)
    gaps.add("usage_coverage_incomplete");
  if(events.some(row => !row.workItemId))
    gaps.add("work_identity_missing");
  if(events.some(row => !row.installationEpochId))
    gaps.add("installation_epoch_missing");
  if(events.some(row => !row.logicalEventId))
    gaps.add("logical_identity_missing");
  const groups=new Map<string|null,UsageFact[]>();
  const partCounts=new Map<UsageFact,number>();
  for(const row of events) {
    const key=row.attributionSource==="unallocated"? null:row.projectKey;
    if(row.allocation) {
      identifier(row.allocation.policyVersion);
      identifier(row.allocation.evidenceRef);
      if(row.attributionSource==="dispatch"&&key!==null&&row.allocation.weights.some(edge => edge.projectKey!==key))
        reject("explicit_allocation_conflict");
      const allocations=allocateMinorUnits("1",row.allocation.weights);
      const amounts=Object.fromEntries(tokenKeys.map(token => [token,row[token]===null? null:allocateMinorUnits(String(row[token]),row.allocation!.weights)]));
      const costs=row.costUsd===null? null:allocateMinorUnits(nanos(row.costUsd).toString(),row.allocation.weights);
      for(const edge of allocations) {
        const part: UsageFact={ ...row,projectKey: edge.projectKey,costUsd: costs===null? null:Number(costs.find(cost => cost.projectKey===edge.projectKey)!.amountMinor)/1e9 };
        for(const token of tokenKeys)
          part[token]=amounts[token]===null? null:Number(amounts[token]!.find((value: {
            projectKey: string|null;
          }) => value.projectKey===edge.projectKey)!.amountMinor);
        partCounts.set(part,Number(edge.amountMinor));
        const rows=groups.get(edge.projectKey)??[];
        rows.push(part);
        groups.set(edge.projectKey,rows);
      }
    }
    else {
      const rows=groups.get(key)??[];
      rows.push(row);
      groups.set(key,rows);
    }
  }
  // Acceptance is an independent fact and can arrive before usage capture.
  for(const row of accepted) {
    if(row.supersededBy===null&&timestamp(row.acceptedAt)>=span.start&&
      timestamp(row.acceptedAt)<span.end&&!groups.has(row.projectKey))
      groups.set(row.projectKey,[]);
  }
  if(!groups.has(null))
    groups.set(null,[]);
  const projects: ProjectEconomics[]=[...groups].map(([projectKey,rows]) => {
    const companies=new Set(rows.map(row => row.companyRef).filter((id): id is string => id!==null));
    if(companies.size>1)
      reject("project_company_conflict");
    const outputs=accepted.filter(row => row.projectKey===projectKey&&row.supersededBy===null&&timestamp(row.acceptedAt)>=span.start&&timestamp(row.acceptedAt)<span.end);
    const usage=summarizeUsage(rows),projectCosts=summarizeCosts(rows);
    usage.events=rows.reduce((sum,row) => sum+(partCounts.get(row)??1),0);
    usage.unknownTokenEvents=rows.filter(row => tokenKeys.some(key => row[key]===null)).reduce((sum,row) => sum+(partCounts.get(row)??1),0);
    projectCosts.pricedEvents=rows.filter(row => row.costUsd!==null).reduce((sum,row) => sum+(partCounts.get(row)??1),0);
    projectCosts.unpricedEvents=rows.filter(row => row.costUsd===null).reduce((sum,row) => sum+(partCounts.get(row)??1),0);
    projectCosts.rateUnboundEvents=rows.filter(row => row.costKind==="estimated"&&(!row.rateVersion||!row.rateEffectiveAt)).reduce((sum,row) => sum+(partCounts.get(row)??1),0);
    return {
      projectKey,companyRef: [...companies][0]??null,workItems: new Set([...rows.map(row => row.workItemId),...outputs.map(row => row.workItemId)].filter(Boolean)).size,
      acceptedOutcomes: outputs.length,matureAcceptedOutcomes: outputs.filter(row => !row.reopenedAt&&timestamp(row.exposureEndsAt)<=nowMs).length,
      usage,costs: projectCosts
    };
  }).sort((a,b) => (a.projectKey??"").localeCompare(b.projectKey??""));
  const costs=summarizeCosts(events);
  if(costs.rateUnboundEvents)
    gaps.add("price_schedule_unbound");
  if(costs.unpricedEvents)
    gaps.add("unpriced_usage");
  if(costs.unclassifiedUsd!==null)
    gaps.add("unknown_cost_provenance");
  if(!input.acceptances)
    gaps.add("acceptance_source_unavailable");
  const linked=events.filter(row => accepted.some(a => a.id===row.acceptedOutcomeId&&a.workItemId===row.workItemId&&a.projectKey===row.projectKey)).length;
  const finance=reconcileFinance(input.tenantId,input.period,input.now,input.finance);
  for(const reason of finance.reasons)
    gaps.add(reason);
  return {
    schemaVersion: ECONOMICS_SCHEMA_VERSION,tenantId: input.tenantId,generatedAt: input.now,period: input.period,
    coverage: {
      state: complete? "complete":events.length? "partial":"unavailable",observedThrough: input.observedThrough,
      capturedEvents: input.events.length,admittedEvents: events.length,duplicateEvents: reconciliation.duplicateEvents,quarantinedEvents: reconciliation.quarantined.length,
      attributedEvents: events.filter(row => row.projectKey!==null&&row.attributionSource!=="unallocated").length,
      unallocatedEvents: events.filter(row => row.projectKey===null||row.attributionSource==="unallocated").length,
      workLinkedEvents: events.filter(row => row.workItemId!==null).length,acceptedOutcomeLinkedEvents: linked,truncated: input.truncated??false
    },
    usage: summarizeUsage(events),costs,projects,finance,forecast: forecastWork(input.tenantId,input.now,accepted,input.forecast),
    experiments: (input.experiments??[]).map(row => evaluateExperiment(input.tenantId,input.now,row)),evidenceGaps: [...gaps].sort()
  };
}
