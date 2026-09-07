import type { AcceptanceFact,ExperimentDecision,ExperimentFact,ForecastInput,ForecastView } from "./contracts";
import { assertTenant,count,identifier,minor,period,reject,timestamp } from "./validation";
export function forecastWork(tenantId: string,now: string,accepted: AcceptanceFact[],input?: ForecastInput|null): ForecastView {
  const base: ForecastView={
    state: "unavailable",method: null,sampleSize: 0,remainingItems: input?.remainingItems??null,
    estimatedCostMinor: null,currency: null,exponent: null,estimatedDurationMs: null,p50At: null,p80At: null,
    reasons: ["comparable_baseline_unavailable"],evidenceRefs: []
  };
  if(!input)
    return base;
  count(input.remainingItems);
  identifier(input.cohort);
  const nowMs=timestamp(now),ids=new Set<string>();
  const samples=input.baseline.filter(sample => {
    assertTenant(tenantId,sample.tenantId);
    identifier(sample.evidenceRef);
    if(ids.has(sample.workItemId))
      reject("duplicate_baseline_work");
    ids.add(sample.workItemId);
    count(sample.fullWorkflowDurationMs);
    const acceptance=accepted.find(row => row.id===sample.acceptanceId&&row.workItemId===sample.workItemId);
    return sample.cohort===input.cohort&&sample.includesReviewAndRepair&&sample.fullWorkflowDurationMs>0&&
      timestamp(sample.startedAt)<timestamp(sample.acceptedAt)&&timestamp(sample.acceptedAt)<=nowMs&&
      sample.fullWorkflowDurationMs<=timestamp(sample.acceptedAt)-timestamp(sample.startedAt)&&
      acceptance&&acceptance.acceptedAt===sample.acceptedAt&&!acceptance.reopenedAt&&!acceptance.supersededBy&&
      timestamp(acceptance.exposureEndsAt)<=nowMs;
  });
  base.sampleSize=samples.length;
  if(!samples.length)
    return base;
  base.state="scenario";
  base.method="comparable_workflow_mean";
  base.estimatedDurationMs=Math.round(samples.reduce((sum,row) => sum+row.fullWorkflowDurationMs,0)/samples.length*input.remainingItems);
  if(!Number.isSafeInteger(base.estimatedDurationMs))
    reject("forecast_duration_overflow");
  base.reasons=["uncalibrated_scenario_no_delivery_date","capacity_and_dependencies_not_modeled"];
  base.evidenceRefs=[...new Set(samples.map(row => row.evidenceRef))].sort();
  const currencies=new Set(samples.map(row => `${row.currency}:${row.exponent}`));
  if(samples.every(row => row.costMinor!==null&&row.currency!==null&&row.exponent!==null)&&currencies.size===1) {
    for(const row of samples)
      if(minor(row.costMinor!)<BigInt(0))
        reject("baseline_negative_cost");
    base.estimatedCostMinor=(samples.reduce((sum,row) => sum+minor(row.costMinor!),BigInt(0))*BigInt(input.remainingItems)/BigInt(samples.length)).toString();
    base.currency=samples[0].currency;
    base.exponent=samples[0].exponent;
  }
  else
    base.reasons.push("baseline_cost_incomplete_or_mixed_currency");
  return base;
}
export function evaluateExperiment(tenantId: string,now: string,experiment: ExperimentFact): ExperimentDecision {
  assertTenant(tenantId,experiment.tenantId);
  for(const v of [experiment.caseId,experiment.ownerRef,experiment.cohort,...experiment.evidenceRefs])
    identifier(v);
  const window=period(experiment.window),reasons: string[]=[];
  for(const v of [experiment.baselineAssigned,experiment.treatmentAssigned,experiment.baselineCompleted,experiment.treatmentCompleted,experiment.baselineAccepted,experiment.treatmentAccepted])
    count(v);
  if(experiment.baselineCompleted>experiment.baselineAssigned||experiment.treatmentCompleted>experiment.treatmentAssigned||experiment.baselineAccepted>experiment.baselineCompleted||experiment.treatmentAccepted>experiment.treatmentCompleted)
    reject("experiment_counts");
  for(const threshold of [experiment.acceptanceThreshold,experiment.practicalEffectThreshold]) {
    if(!Number.isFinite(threshold)||threshold<0||threshold>1)
      reject("experiment_threshold");
  }
  const baseline=minor(experiment.baselineCostMinor),treatment=minor(experiment.treatmentCostMinor),implementation=minor(experiment.implementationCostMinor);
  if(baseline<BigInt(0)||treatment<BigInt(0)||implementation<BigInt(0))
    reject("experiment_cost");
  if(!/^[a-f0-9]{64}$/.test(experiment.changeHash))
    reasons.push("change_hash_unavailable");
  if(timestamp(experiment.prespecifiedAt)>window.start)
    reasons.push("not_prespecified");
  if(window.end>timestamp(now))
    reasons.push("observation_window_open");
  if(!experiment.baselineAssigned||!experiment.treatmentAssigned||experiment.baselineCompleted!==experiment.baselineAssigned||experiment.treatmentCompleted!==experiment.treatmentAssigned)
    reasons.push("assigned_cohort_incomplete");
  if(!experiment.reviewAndRepairIncluded)
    reasons.push("review_or_repair_missing");
  if(!experiment.evidenceRefs.length)
    reasons.push("observation_evidence_missing");
  if(!experiment.independentAcceptanceRef)
    reasons.push("independent_acceptance_missing");
  if(!experiment.rollbackRef)
    reasons.push("rollback_missing");
  if(experiment.confounders.length)
    reasons.push("unresolved_confounders");
  const result: ExperimentDecision={ caseId: experiment.caseId,ownerRef: experiment.ownerRef,decision: "inconclusive",reasons,evidenceRefs: experiment.evidenceRefs,observedEffect: null };
  if(reasons.length)
    return result;
  const baselineQuality=experiment.baselineAccepted/experiment.baselineAssigned;
  const treatmentQuality=experiment.treatmentAccepted/experiment.treatmentAssigned;
  if(treatmentQuality<experiment.acceptanceThreshold||treatmentQuality<baselineQuality) {
    result.decision="revert";
    reasons.push("quality_threshold_failed");
    return result;
  }
  if(baseline===BigInt(0)||!experiment.baselineAccepted||!experiment.treatmentAccepted) {
    reasons.push("accepted_outcome_cost_baseline_unavailable");
    return result;
  }
  const baselinePerOutcome=Number(baseline)/experiment.baselineAccepted;
  const treatmentPerOutcome=Number(treatment+implementation)/experiment.treatmentAccepted;
  result.observedEffect=(baselinePerOutcome-treatmentPerOutcome)/baselinePerOutcome;
  if(result.observedEffect<0) {
    result.decision="revert";
    reasons.push("full_workflow_cost_worse");
  }
  else if(result.observedEffect>=experiment.practicalEffectThreshold) {
    result.decision="adopt";
    reasons.push("prespecified_thresholds_met");
  }
  else
    reasons.push("practical_effect_threshold_not_met");
  return result;
}
