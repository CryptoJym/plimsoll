import type { ExpenseAllocation,ExpenseAllocationPlan,FinanceLine,FinanceSnapshot,FinanceView,Period } from "./contracts";
import { assertTenant,count,digest,identifier,minor,period,reject,timestamp } from "./validation";
/** Deterministic largest remainder; null is the explicit unallocated destination. */
export function allocateMinorUnits(amount: string,weights: Array<{
  projectKey: string|null;
  weight: number;
}>) {
  const total=minor(amount);
  if(total<BigInt(0))
    reject("negative_allocation_use_reversal");
  const normalized=[...weights].sort((a,b) => (a.projectKey??"").localeCompare(b.projectKey??""));
  const seen=new Set<string|null>();
  for(const row of normalized) {
    if(row.projectKey!==null)
      identifier(row.projectKey);
    if(seen.has(row.projectKey))
      reject("duplicate_weight_destination");
    seen.add(row.projectKey);
    count(row.weight);
  }
  const denominator=normalized.reduce((sum,row) => sum+BigInt(row.weight),BigInt(0));
  if(denominator===BigInt(0))
    return [{ projectKey: null,amountMinor: total.toString() }];
  const rows=normalized.map(row => ({
    ...row,amount: total*BigInt(row.weight)/denominator,
    remainder: total*BigInt(row.weight)%denominator
  }));
  let residual=total-rows.reduce((sum,row) => sum+row.amount,BigInt(0));
  for(const row of [...rows].sort((a,b) => a.remainder===b.remainder
    ? (a.projectKey??"").localeCompare(b.projectKey??""):a.remainder>b.remainder? -1:1)) {
    if(residual===BigInt(0))
      break;
    row.amount+=BigInt(1);
    residual-=BigInt(1);
  }
  return rows.map(row => ({ projectKey: row.projectKey,amountMinor: row.amount.toString() }));
}
function allocation(line: FinanceLine,projectKey: string|null,amountMinor: string,kind: ExpenseAllocation["kind"],plan: ExpenseAllocationPlan|null,splitId: string|null): ExpenseAllocation {
  const value={
    sourceLineId: line.sourceLineId,sourceVersion: line.sourceVersion,
    projectKey,companyRef: line.companyRef,currency: line.currency,exponent: line.exponent,
    amountMinor,kind,splitId,policyVersion: plan?.policyVersion??null,generation: plan?.generation??null,
    period: line.period,evidenceRef: plan?.evidenceRef??line.evidenceRef,reversalOf: null
  };
  return { ...value,allocationId: digest(value) };
}
export function reconcileFinance(tenantId: string,selectedPeriod: Period,now: string,snapshot?: FinanceSnapshot|null): FinanceView {
  if(!snapshot)
    return { state: "unavailable",currencies: [],allocations: [],reasons: ["finance_source_unavailable"] };
  assertTenant(tenantId,snapshot.tenantId);
  const selected=period(selectedPeriod),span=period(snapshot.period);
  if(span.start!==selected.start||span.end!==selected.end)
    reject("finance_period_mismatch");
  if(timestamp(snapshot.observedThrough)>timestamp(now)||timestamp(snapshot.observedThrough)<span.start)
    reject("finance_watermark");
  identifier(snapshot.attestationRef);
  if(!/^[a-f0-9]{64}$/.test(snapshot.manifestDigest))
    reject("finance_manifest");
  if(snapshot.lines.length>10000||snapshot.plans.length>10000)
    reject("finance_limit");
  const lines=new Map<string,FinanceLine>();
  for(const line of snapshot.lines) {
    assertTenant(tenantId,line.tenantId);
    if(!["recognized_expense","recognized_revenue","cash_movement"].includes(line.kind))
      reject("finance_line_kind");
    for(const v of [line.companyRef,line.sourceLineId,line.sourceVersion,line.evidenceRef,line.statementRef,line.attestationRef,line.taxTreatment])
      identifier(v);
    if(!/^[A-Z]{3}$/.test(line.currency)||!Number.isInteger(line.exponent)||line.exponent<0||line.exponent>6)
      reject("currency");
    const linePeriod=period(line.period);
    if(linePeriod.start<span.start||linePeriod.end>span.end)
      reject("line_period");
    minor(line.amountMinor);
    if(line.projectKey!==null)
      identifier(line.projectKey);
    const prior=lines.get(line.sourceLineId);
    if(prior&&digest(prior)!==digest(line))
      reject("source_line_conflict");
    lines.set(line.sourceLineId,line);
  }
  const plans=new Map<string,ExpenseAllocationPlan>();
  for(const plan of snapshot.plans) {
    assertTenant(tenantId,plan.tenantId);
    identifier(plan.policyVersion);
    identifier(plan.evidenceRef);
    count(plan.generation);
    if(plan.manifestDigest!==snapshot.manifestDigest)
      reject("allocation_manifest_mismatch");
    const line=lines.get(plan.sourceLineId);
    if(!line||line.kind!=="recognized_expense"||minor(line.amountMinor)<BigInt(0))
      reject("allocation_source_missing");
    if(line.sourceVersion!==plan.sourceVersion||digest(line.period)!==digest(plan.period))
      reject("allocation_source_version_or_period");
    const prior=plans.get(plan.sourceLineId);
    if(prior&&digest(prior)!==digest(plan))
      reject("overlapping_source_claims");
    plans.set(plan.sourceLineId,plan);
  }
  const allocations: ExpenseAllocation[]=[];
  for(const line of lines.values()) {
    if(line.kind!=="recognized_expense"||minor(line.amountMinor)<BigInt(0))
      continue;
    const total=minor(line.amountMinor),plan=plans.get(line.sourceLineId);
    if(!plan) {
      allocations.push(allocation(line,line.projectKey,line.amountMinor,line.projectKey? "direct":"unallocated",null,null));
      continue;
    }
    let claimed=BigInt(0);
    const splitIds=new Set<string>();
    for(const split of plan.splits) {
      identifier(split.id);
      if(splitIds.has(split.id))
        reject("duplicate_split");
      splitIds.add(split.id);
      const amount=minor(split.amountMinor);
      if(amount<BigInt(0))
        reject("negative_split");
      claimed+=amount;
      if(claimed>total)
        reject("source_expense_overallocated");
      if(split.kind==="direct"&&(split.weights.length!==1||split.weights[0].projectKey===null||split.weights[0].weight<=0))
        reject("direct_destination");
      if(line.projectKey!==null&&split.weights.some(row => row.projectKey!==line.projectKey))
        reject("explicit_project_conflict");
      for(const row of allocateMinorUnits(split.amountMinor,split.weights)) {
        allocations.push(allocation(line,row.projectKey,row.amountMinor,row.projectKey===null? "unallocated":split.kind,plan,split.id));
      }
    }
    // Explicit remainder is retained even when the policy lacks a usable weight.
    if(claimed<total||plan.splits.length===0)
      allocations.push(allocation(line,null,(total-claimed).toString(),"unallocated",plan,null));
  }
  const originals=new Map(allocations.map(row => [row.allocationId,row]));
  for(const row of snapshot.priorAllocations??[]) {
    identifier(row.allocationId);
    identifier(row.evidenceRef);
    minor(row.amountMinor);
    if(row.reversalOf!==null||minor(row.amountMinor)<BigInt(0))
      reject("invalid_original_allocation");
    const known=originals.get(row.allocationId);
    if(known&&digest(known)!==digest(row))
      reject("original_allocation_conflict");
    originals.set(row.allocationId,row);
  }
  const reversed=new Set<string>(),credits=new Set<string>();
  for(const reversal of snapshot.reversals) {
    assertTenant(tenantId,reversal.tenantId);
    identifier(reversal.evidenceRef);
    const credit=lines.get(reversal.sourceLineId);
    if(!credit||credit.kind!=="recognized_expense"||minor(credit.amountMinor)>=BigInt(0)||credit.sourceVersion!==reversal.sourceVersion)
      reject("credit_source");
    if(credits.has(credit.sourceLineId))
      reject("duplicate_credit");
    credits.add(credit.sourceLineId);
    let amount=BigInt(0);
    for(const id of reversal.allocationIds) {
      if(reversed.has(id))
        reject("duplicate_reversal");
      const original=originals.get(id);
      if(!original||original.companyRef!==credit.companyRef||original.currency!==credit.currency||original.exponent!==credit.exponent)
        reject("reversal_original_mismatch");
      reversed.add(id);
      amount-=minor(original.amountMinor);
      const value=allocation(credit,original.projectKey,(-minor(original.amountMinor)).toString(),original.kind,null,original.splitId);
      value.reversalOf=id;
      value.policyVersion=original.policyVersion;
      value.generation=original.generation;
      value.evidenceRef=reversal.evidenceRef;
      value.allocationId=digest({ credit: credit.sourceLineId,original: id });
      allocations.push(value);
    }
    if(amount!==minor(credit.amountMinor))
      reject("credit_not_conserved");
  }
  for(const line of lines.values()) {
    if(line.kind!=="recognized_expense")
      continue;
    const sum=allocations.filter(row => row.sourceLineId===line.sourceLineId).reduce((s,row) => s+minor(row.amountMinor),BigInt(0));
    if(sum!==minor(line.amountMinor))
      reject("source_expense_not_conserved");
  }
  const currencyKeys=new Map<string,number>();
  for(const line of lines.values()) {
    if(currencyKeys.has(line.currency)&&currencyKeys.get(line.currency)!==line.exponent)
      reject("currency_exponent_conflict");
    currencyKeys.set(line.currency,line.exponent);
  }
  const complete=snapshot.complete&&timestamp(snapshot.observedThrough)>=span.end;
  const currencies=[...currencyKeys].sort(([a],[b]) => a.localeCompare(b)).map(([currency,exponent]) => {
    const currencyLines=[...lines.values()].filter(line => line.currency===currency);
    const sumKind=(kind: FinanceLine["kind"]) => {
      const rows=currencyLines.filter(line => line.kind===kind);
      return rows.length? rows.reduce((s,row) => s+minor(row.amountMinor),BigInt(0)):null;
    };
    const revenue=sumKind("recognized_revenue"),expense=sumKind("recognized_expense"),cash=sumKind("cash_movement");
    const sums={ direct: BigInt(0),shared: BigInt(0),unallocated: BigInt(0) };
    for(const row of allocations.filter(row => row.currency===currency))
      sums[row.kind]+=minor(row.amountMinor);
    return {
      currency,exponent,recognizedRevenueMinor: revenue?.toString()??null,
      recognizedDirectExpenseMinor: expense===null? null:sums.direct.toString(),
      allocatedSharedExpenseMinor: expense===null? null:sums.shared.toString(),
      unallocatedExpenseMinor: expense===null? null:sums.unallocated.toString(),cashMovementMinor: cash?.toString()??null,
      contributionMinor: complete&&revenue!==null&&expense!==null&&sums.unallocated===BigInt(0)? (revenue-sums.direct-sums.shared).toString():null
    };
  });
  return {
    state: complete? "reconciled":"partial",currencies,allocations,
    reasons: complete? []:["finance_coverage_incomplete"]
  };
}
