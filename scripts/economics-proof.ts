import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { buildWorkspaceEconomics,reconcileUsage } from "../packages/shared/src/economics/service";
import { allocateMinorUnits,reconcileFinance } from "../packages/shared/src/economics/finance";
import { digest } from "../packages/shared/src/economics/validation";
import type { AcceptanceFact,EconomicsInput,FinanceLine,FinanceSnapshot,UsageFact } from "../packages/shared/src/economics/contracts";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { collectSessionSnapshots,collectSessionProjectAllocations } from "../packages/collector-cli/src/session-sync";
import { readProfileIdentities } from "../packages/collector-cli/src/local-identity";
import { appendRootObservation,bindCaptureInventory,validateCaptureRoots,type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { beginAutomaticCaptureBaseline,completeAutomaticCaptureBaseline,captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { usageFactFromEvent } from "../packages/shared/src/economics/event-adapter";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { createProfileCapture } from "../packages/collector-cli/src/profile-capture";
import { createProofCompletion } from "./lib/proof-completion";
const completion=createProofCompletion("economics",37);
if(!process.env.PLIMSOLL_PROOF_HOME||process.env.HOME!==process.env.PLIMSOLL_PROOF_HOME||
  !process.env.PLIMSOLL_HOME?.startsWith(path.resolve(process.env.PLIMSOLL_PROOF_HOME)+path.sep)) {
  throw new Error("isolated_proof_home_required");
}
const results: Array<{
  name: string;
  passed: boolean;
}>=[];
function check(name: string,fn: () => void) { fn(); results.push({ name,passed: true }); completion.check(name); }
const tenant="tenant-fixture",start="2026-09-01T00:00:00.000Z",end="2026-09-07T00:00:00.000Z",now="2026-09-08T00:00:00.000Z";
const period={ start,end },A=`sha256:${"a".repeat(64)}`,B=`sha256:${"b".repeat(64)}`;
function event(id: string,projectKey: string|null,inputTokens=1,extra: Partial<UsageFact>={}): UsageFact {
  return {
    tenantId: tenant,installationEpochId: "epoch-one",source: "codex",sourceEventId: id,sourceVersion: "runtime_v1",schemaVersion: "usage_v1",
    observedAt: "2026-09-02T00:00:00.000Z",receivedAt: "2026-09-02T00:00:01.000Z",timePrecision: "millisecond",
    nativeSessionId: "session-one",attemptId: "attempt-one",parentAttemptId: null,accountId: "account-one",model: "fixture_model",
    logicalEventId: id,identityEvidenceRef: "native_event",payloadDigest: digest({ id,inputTokens }),evidenceRef: `source:${id}`,
    projectKey,companyRef: null,workItemId: "work-one",acceptedOutcomeId: null,attributionSource: projectKey? "workspace":"unallocated",
    costUsd: 0.000001,costKind: "estimated",rateVersion: "fixture_v1",rateEffectiveAt: start,inputTokens,outputTokens: 0,cacheReadTokens: 0,cacheCreationTokens: 0,...extra
  };
}
function input(events: UsageFact[],extra: Partial<EconomicsInput>={}): EconomicsInput { return { tenantId: tenant,period,now,events,complete: true,observedThrough: end,...extra }; }
const output: AcceptanceFact={ tenantId: tenant,id: "accept-one",workItemId: "work-one",projectKey: A,artifactRef: "artifact-one",artifactRevision: "rev-one",criteriaVersion: "criteria_v1",reviewerRef: "reviewer",evidenceRef: "review-receipt",acceptedAt: "2026-09-03T00:00:00.000Z",exposureEndsAt: end,reopenedAt: null,supersededBy: null };
check("accepted_output_without_captured_usage_stays_visible",() => {
  const view=buildWorkspaceEconomics(input([],{ acceptances: [output],complete: false,observedThrough: null }));
  const project=view.projects.find(row => row.projectKey===A)!;
  assert.equal(project.acceptedOutcomes,1);
  assert.equal(project.workItems,1);
  assert.equal(project.usage.events,0);
  assert.equal(project.costs.reportedUsd,null);
  assert.equal(view.coverage.state,"unavailable");
});
check("unattested_dispatch_does_not_override_workspace_evidence",() => {
  const fact=usageFactFromEvent({
    id: "event-one",tenantId: tenant,source: "codex",observedAt: start,receivedAt: start,
    sessionId: null,accountId: null,model: null,projectKey: B,inputTokens: 2,outputTokens: 0,cacheReadTokens: 0,cacheCreationTokens: 0,costUsd: null,
    metadata: { dispatchProjectKey: A,workItemId: "work-one" }
  });
  assert.equal(fact.projectKey,B);
  assert.equal(fact.attributionSource,"workspace");
});
check("2tokensA100B_event_conservation",() => {
  const view=buildWorkspaceEconomics(input([event("a1",A),event("a2",A),event("b1",B,100)]));
  assert.equal(view.projects.find(row => row.projectKey===A)!.usage.inputTokens,2);
  assert.equal(view.projects.find(row => row.projectKey===B)!.usage.inputTokens,100);
  assert.equal(view.usage.inputTokens,102);
  assert.equal(view.projects.reduce((sum,row) => sum+row.usage.inputTokens!,0),102);
});
check("cross_epoch_and_account_failover_duplicate",() => {
  const original=event("native-1",A,9),replay={ ...original,installationEpochId: "epoch-two",accountId: "account-two",attemptId: "attempt-two" };
  const view=buildWorkspaceEconomics(input([original,replay,event("native-2",A,9,{ attemptId: "attempt-two",accountId: "account-two" })]));
  assert.equal(view.usage.inputTokens,18);
  assert.equal(view.coverage.duplicateEvents,1);
  assert.equal(view.coverage.admittedEvents,2);
});
check("unresolved_cross_epoch_overlap_quarantines_both",() => {
  const row=event("overlap",A,9,{ logicalEventId: null,identityEvidenceRef: null });
  const view=buildWorkspaceEconomics(input([row,{ ...row,installationEpochId: "epoch-two" }]));
  assert.equal(view.coverage.quarantinedEvents,2);
  assert.equal(view.usage.inputTokens,0);
  assert.ok(view.evidenceGaps.includes("cross_epoch_identity_unresolved"));
});
check("conflicting_payload_is_not_a_second_expense",() => {
  const row=event("conflict",A),bad={ ...row,inputTokens: 500,payloadDigest: digest("changed") };
  assert.equal(reconcileUsage(tenant,[row,bad],now).quarantined.length,2);
});
check("distinct_identical_native_calls_remain_distinct",() => {
  const row=event("call-1",A),other={ ...row,sourceEventId: "call-2",logicalEventId: "call-2" };
  assert.equal(buildWorkspaceEconomics(input([row,other])).usage.events,2);
});
check("cost_bases_unpriced_and_honest_nulls",() => {
  const view=buildWorkspaceEconomics(input([event("reported",A,2,{ costKind: "reported",costUsd: 5 }),event("estimated",B,4,{ costUsd: 2 }),event("unpriced",null,3,{ costUsd: null }),event("unknown",A,1,{ costUsd: 7,costKind: "unknown" })]));
  assert.deepEqual([view.costs.reportedUsd,view.costs.estimatedUsd,view.costs.unclassifiedUsd,view.costs.unpricedEvents],[5,2,7,1]);
  assert.equal(view.finance.state,"unavailable");
  assert.equal(view.forecast.estimatedCostMinor,null);
  assert.equal(view.forecast.p80At,null);
});
check("explicit_dispatch_precedes_registry_and_workspace",() => {
  const mapping={ projectKey: B,companyRef: null,workItemId: "work-one",source: "registry" as const,effectiveFrom: start,effectiveTo: end,evidenceRef: "registry_receipt" };
  const view=buildWorkspaceEconomics(input([event("explicit",A,4,{ attributionSource: "dispatch" }),event("registry",A,2)],{ projectMappings: [mapping] }));
  assert.equal(view.projects.find(row => row.projectKey===A)!.usage.inputTokens,4);
  assert.equal(view.projects.find(row => row.projectKey===B)!.usage.inputTokens,2);
});
check("weighted_work_edges_conserve_tokens_and_money",() => {
  const view=buildWorkspaceEconomics(input([event("split",null,101,{ costUsd: 0.000101,allocation: { policyVersion: "split_v1",evidenceRef: "assignment",weights: [{ projectKey: A,weight: 1 },{ projectKey: B,weight: 1 },{ projectKey: null,weight: 1 }] } })]));
  assert.equal(view.projects.reduce((sum,row) => sum+row.usage.inputTokens!,0),101);
  assert.equal(view.projects.reduce((sum,row) => sum+Math.round((row.costs.estimatedUsd??0)*1e9),0),101000);
  assert.equal(view.projects.reduce((sum,row) => sum+row.usage.events,0),1);
});
check("unknown_token_class_not_zero",() => assert.equal(buildWorkspaceEconomics(input([event("missing-class",A,1,{ cacheCreationTokens: null })])).usage.cacheCreationTokens,null));
check("cross_tenant_fails_closed",() => assert.throws(() => buildWorkspaceEconomics(input([event("foreign",A,1,{ tenantId: "foreign" })])),/tenant_mismatch/));
check("future_event_quarantined",() => assert.equal(buildWorkspaceEconomics(input([event("future",A,1,{ observedAt: "2027-01-01T00:00:00.000Z" })])).coverage.quarantinedEvents,1));
check("acceptance_requires_source_and_counts_once",() => {
  const view=buildWorkspaceEconomics(input([event("accepted-1",A),event("accepted-2",A)],{ acceptances: [output,output] }));
  assert.equal(view.projects.find(row => row.projectKey===A)!.acceptedOutcomes,1);
  assert.equal(view.projects.find(row => row.projectKey===A)!.matureAcceptedOutcomes,1);
});
check("reopened_outcome_not_mature",() => assert.equal(buildWorkspaceEconomics(input([event("reopen",A)],{ acceptances: [{ ...output,reopenedAt: "2026-09-07T12:00:00.000Z" }] })).projects.find(row => row.projectKey===A)!.matureAcceptedOutcomes,0));
check("forecast_abstains_without_comparable_accepted_baseline",() => {
  const view=buildWorkspaceEconomics(input([],{ forecast: { cohort: "cohort-one",remainingItems: 5,baseline: [] } }));
  assert.equal(view.forecast.estimatedDurationMs,null);
});
check("forecast_actual_sample_is_scenario_without_made_up_confidence",() => {
  const view=buildWorkspaceEconomics(input([],{
    acceptances: [output],forecast: {
      cohort: "cohort-one",remainingItems: 5,baseline: [{
        tenantId: tenant,workItemId: "work-one",acceptanceId: output.id,cohort: "cohort-one",startedAt: start,acceptedAt: output.acceptedAt,
        fullWorkflowDurationMs: 1000,costMinor: "200",currency: "USD",exponent: 2,includesReviewAndRepair: true,evidenceRef: "workflow-receipt"
      }]
    }
  }));
  assert.equal(view.forecast.sampleSize,1);
  assert.equal(view.forecast.estimatedCostMinor,"1000");
  assert.equal(view.forecast.state,"scenario");
  assert.equal(view.forecast.p50At,null);
});
function line(id: string,amountMinor: string,extra: Partial<FinanceLine>={}): FinanceLine { return { tenantId: tenant,companyRef: "company-one",sourceLineId: id,sourceVersion: "bill_v1",evidenceRef: "bill-evidence",statementRef: "statement",attestationRef: "finance-attested",period,currency: "USD",exponent: 2,amountMinor,kind: "recognized_expense",taxTreatment: "net",projectKey: null,...extra }; }
function finance(lines: FinanceLine[]): FinanceSnapshot { return { tenantId: tenant,complete: true,period,observedThrough: end,manifestDigest: "a".repeat(64),attestationRef: "finance-snapshot",lines,plans: [],reversals: [] }; }
check("largest_remainder_exact_and_stable",() => {
  assert.deepEqual(allocateMinorUnits("101",[{ projectKey: B,weight: 1 },{ projectKey: A,weight: 1 }]),[{ projectKey: A,amountMinor: "51" },{ projectKey: B,amountMinor: "50" }]);
  assert.deepEqual(allocateMinorUnits("101",[]),[{ projectKey: null,amountMinor: "101" }]);
});
check("global_source_expense_direct_shared_residual_conservation",() => {
  const source=finance([line("vendor-line","101"),line("revenue","300",{ kind: "recognized_revenue",projectKey: A })]);
  source.plans=[{
    tenantId: tenant,sourceLineId: "vendor-line",sourceVersion: "bill_v1",period,policyVersion: "allocation_v1",generation: 1,manifestDigest: source.manifestDigest,evidenceRef: "allocation-receipt",
    splits: [{ id: "direct",kind: "direct",amountMinor: "20",weights: [{ projectKey: A,weight: 1 }] },{ id: "pool",kind: "shared",amountMinor: "80",weights: [{ projectKey: A,weight: 1 },{ projectKey: B,weight: 3 }] }]
  }];
  const view=reconcileFinance(tenant,period,now,source);
  assert.equal(view.allocations.reduce((sum,row) => sum+BigInt(row.amountMinor),BigInt(0)),BigInt(101));
  assert.equal(view.currencies[0].recognizedDirectExpenseMinor,"20");
  assert.equal(view.currencies[0].allocatedSharedExpenseMinor,"80");
  assert.equal(view.currencies[0].unallocatedExpenseMinor,"1");
  assert.equal(view.currencies[0].contributionMinor,null);
  source.plans[0].splits.push({ id: "double-claim",kind: "direct",amountMinor: "101",weights: [{ projectKey: A,weight: 1 }] });
  assert.throws(() => reconcileFinance(tenant,period,now,source),/source_expense_overallocated/);
});
check("duplicate_bill_and_plan_replay_is_idempotent_conflict_rejected",() => {
  const bill=line("same","99",{ projectKey: A }),source=finance([bill,bill]);
  assert.equal(reconcileFinance(tenant,period,now,source).allocations.length,1);
  source.lines.push({ ...bill,sourceVersion: "bill_v2" });
  assert.throws(() => reconcileFinance(tenant,period,now,source),/source_line_conflict/);
});
check("credit_negates_original_rows_even_after_weights_change",() => {
  const source=finance([line("original","101")]);
  source.plans=[{ tenantId: tenant,sourceLineId: "original",sourceVersion: "bill_v1",period,policyVersion: "policy_v1",generation: 1,manifestDigest: source.manifestDigest,evidenceRef: "policy",splits: [{ id: "pool",kind: "shared",amountMinor: "101",weights: [{ projectKey: A,weight: 1 },{ projectKey: B,weight: 1 }] }] }];
  const originals=reconcileFinance(tenant,period,now,source).allocations;
  const credit=finance([line("credit","-101")]);
  credit.priorAllocations=originals;
  credit.reversals=[{ tenantId: tenant,sourceLineId: "credit",sourceVersion: "bill_v1",evidenceRef: "credit-attestation",allocationIds: originals.map(row => row.allocationId) }];
  const reversed=reconcileFinance(tenant,period,now,credit).allocations;
  assert.deepEqual(reversed.map(row => row.amountMinor),["-51","-50"]);
  credit.reversals.push({ ...credit.reversals[0] });
  assert.throws(() => reconcileFinance(tenant,period,now,credit),/duplicate_credit/);
});
check("currency_separation_and_missing_revenue",() => {
  const view=reconcileFinance(tenant,period,now,finance([line("usd","500"),line("eur","900",{ currency: "EUR" })]));
  assert.equal(view.currencies.length,2);
  assert.ok(view.currencies.every(row => row.recognizedRevenueMinor===null&&row.contributionMinor===null));
});
async function captureProof() {
  const root=fs.mkdtempSync(path.join(process.env.PLIMSOLL_HOME!,"roots-")),buffer=new LocalEventBuffer(path.join(root,"ledger.sqlite"));
  try {
    const dirA=path.join(root,"codex-a"),dirB=path.join(root,"codex-b"),missing=path.join(root,"missing");
    const profiles: CaptureRoot[]=[{ rootId: "root-a",profileId: "profile-a",installationEpochId: "epoch-a",source: "codex",directory: dirA },
    { rootId: "root-b",profileId: "profile-b",installationEpochId: "epoch-b",source: "codex",directory: dirB },
    { rootId: "root-missing",profileId: "profile-missing",installationEpochId: "epoch-a",source: "codex",directory: missing }];
    const write=(directory: string,sessionId: string,tokens: number) => {
      const day=path.join(directory,"2026","09","02");
      fs.mkdirSync(day,{ recursive: true });
      const file=path.join(day,`rollout-${sessionId}.jsonl`);
      fs.writeFileSync(file,[{ type: "session_meta",timestamp: "2026-09-02T00:00:00.000Z",payload: { id: sessionId } },
      { type: "turn_context",payload: { model: "gpt-5.5" } },...[0,tokens].map((n,index) => ({ type: "event_msg",timestamp: `2026-09-02T00:00:0${index}.000Z`,payload: { type: "token_count",info: { total_token_usage: { input_tokens: n,cached_input_tokens: 0,output_tokens: 0,reasoning_output_tokens: 0 } } } }))].map(row => JSON.stringify(row)).join("\n")+"\n");
      return file;
    };
    const idA="019e1111-2222-7333-8444-555555555555",idB="019e2222-3333-7444-8555-666666666666";
    const file=write(dirA,idA,2);
    write(dirB,idB,100);
    let tailer=new RolloutTailer(buffer,undefined,() => [],undefined,profiles);
    const first=await tailer.scan({ scope: "full",now: new Date(now) });
    check("multiple_roots_missing_root_visible",() => { assert.equal(first.tokensAppended.input,102); assert.equal(first.roots!.length,3); assert.equal(first.roots!.find(row => row.rootId==="root-missing")!.state,"missing"); assert.equal(first.exhaustive,false); });
    tailer.close();
    fs.unlinkSync(file);
    write(dirA,"019e3333-4444-7555-8666-777777777777",7);
    // Copying the same native rollout across profiles/epochs must not charge it again.
    write(dirA,idB,100);
    tailer=new RolloutTailer(buffer,undefined,() => [],undefined,profiles.map(row => ({ ...row,installationEpochId: "epoch-after-reinstall" })));
    const second=await tailer.scan({ scope: "full",now: new Date(now) });
    check("multi_root_rotation_and_epoch_replay_conserve",() => {
      assert.equal(second.tokensAppended.input,7); assert.equal((buffer.database.prepare("select sum(input_tokens) as tokens from buffered_events").get() as {
        tokens: number;
      }).tokens,109);
    });
    const third=await tailer.scan({ scope: "full",now: new Date(now) });
    check("unchanged_roots_read_no_bytes",() => assert.equal(third.bytesRead,0));
    tailer.close();
    check("explicit_identity_inventory_no_default_fallback",() => assert.equal(readProfileIdentities([{ rootId: "missing",profileId: "profile",paths: { codexAuthPath: path.join(root,"absent-auth") } }])[0].state,"unavailable"));
    check("overlapping_inventory_rejected",() => assert.throws(() => validateCaptureRoots([profiles[0],{ ...profiles[1],directory: path.join(dirA,"child") }]),/overlap/));
    const claudeDir=path.join(root,"claude"),project=path.join(claudeDir,"project");
    fs.mkdirSync(project,{ recursive: true });
    fs.writeFileSync(path.join(project,"44445555-6666-4777-8888-99990000aaaa.jsonl"),JSON.stringify({ type: "assistant",timestamp: "2026-09-02T00:00:00.000Z",message: { id: "msg-one",model: "claude-opus-5",usage: { input_tokens: 4,output_tokens: 3 } } })+"\n");
    const transcript=new TranscriptTailer(buffer,undefined,undefined,[{ rootId: "claude",profileId: "claude-profile",installationEpochId: "epoch-claude",source: "claude_code",directory: claudeDir }]);
    const captured=await transcript.scan({ scope: "full",now: new Date(now) });
    check("claude_profile_root_capture",() => assert.equal(captured.tokensAppended.input,4));
    transcript.close();
  }
  finally {
    buffer.close();
    fs.rmSync(root,{ recursive: true,force: true });
  }
  const baselineLedger=new LocalEventBuffer(":memory:");
  try {
    const initial: CaptureRoot={ rootId: "root",profileId: "profile",installationEpochId: "epoch",source: "codex",directory: process.env.PLIMSOLL_HOME! };
    bindCaptureInventory(baselineLedger.database,"codex",[initial]);
    const run=beginAutomaticCaptureBaseline(baselineLedger.database,"codex",{ startedAt: start,filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(baselineLedger.database,"codex",{ runId: run.latestRun!.runId,completedAt: end });
    bindCaptureInventory(baselineLedger.database,"codex",[{ ...initial,installationEpochId: "next-epoch" }]);
    check("changed_inventory_reopens_original_baseline_cutoff",() => {
      const current=captureBaselineStatus(baselineLedger.database).sources.find(row => row.source==="codex")!;
      assert.equal(current.status,"in_progress");
      assert.equal(current.latestRun!.startedAt,start);
      assert.equal(current.latestRun!.runId,run.latestRun!.runId);
    });
    const captured=aiInteractionEventSchema.parse({ id: "00000000-0000-4000-8000-000000000099",tenantId: tenant,source: "codex",dataMode: "metadata",eventType: "usage_rollout",observedAt: start,inputTokens: 9,outputTokens: 0,metadata: {} });
    assert.equal(appendRootObservation(baselineLedger,captured,initial),true);
    baselineLedger.database.prepare("delete from buffered_events where id=?").run(captured.id);
    check("retained_source_receipt_prevents_epoch_replay_after_raw_prune",() => {
      assert.equal(appendRootObservation(baselineLedger,captured,{ ...initial,installationEpochId: "next-epoch" }),false);
      assert.equal((baselineLedger.database.prepare("select count(*) as n from buffered_events").get() as {
        n: number;
      }).n,0);
      assert.throws(() => appendRootObservation(baselineLedger,{ ...captured,inputTokens: 90 },initial),/source_conflict/);
    });
  }
  finally {
    baselineLedger.close();
  }
  {
    const emptyLedger = new LocalEventBuffer(":memory:");
    try {
      const capture = createProfileCapture(emptyLedger, { captureRoots: [] });
      const rollout = await capture.rollout.scan({ scope: "full" });
      const transcript = await capture.transcript.scan({ scope: "full" });
      check("explicit_empty_provider_inventory_never_reads_default_roots", () => {
        assert.equal(rollout.eventsAppended, 0); assert.equal(transcript.eventsAppended, 0);
        assert.equal(rollout.filesSeen, 0); assert.equal(transcript.filesSeen, 0);
        assert.deepEqual(rollout.roots, []); assert.deepEqual(transcript.roots, []);
        assert.equal(captureBaselineStatus(emptyLedger.database).status, "complete");
      });
      capture.close();
    } finally { emptyLedger.close(); }
  }
  for(const provider of ["codex","claude_code"] as const) {
    const automaticRoot=fs.mkdtempSync(path.join(process.env.PLIMSOLL_HOME!,"automatic-"));
    const automaticLedger=new LocalEventBuffer(":memory:");
    const healthy=path.join(automaticRoot,"healthy"),missing=path.join(automaticRoot,"missing"),unsafe=path.join(automaticRoot,"unsafe"),target=path.join(automaticRoot,"target");
    fs.mkdirSync(healthy);
    fs.mkdirSync(target);
    fs.symlinkSync(target,unsafe);
    const roots: CaptureRoot[]=[{ rootId: "healthy",profileId: "p1",installationEpochId: "epoch",source: provider,directory: healthy },
    { rootId: "missing",profileId: "p2",installationEpochId: "epoch",source: provider,directory: missing },
    { rootId: "unsafe",profileId: "p3",installationEpochId: "epoch",source: provider,directory: unsafe }];
    const tailer=provider==="codex"? new RolloutTailer(automaticLedger,undefined,() => [],undefined,roots):new TranscriptTailer(automaticLedger,undefined,undefined,roots);
    try {
      const cutoff=new Date(Date.now()-1000);
      // The existing global capture fence also requires the other provider.
      // This fixture's other provider has an explicitly empty inventory.
      const sibling=provider==="codex"? "claude_code":"codex";
      const empty=beginAutomaticCaptureBaseline(automaticLedger.database,sibling,{ startedAt: cutoff.toISOString(),filesDiscovered: 0 });
      completeAutomaticCaptureBaseline(automaticLedger.database,sibling,{ runId: empty.latestRun!.runId,completedAt: new Date().toISOString() });
      for(let iteration=0;iteration<20;iteration++) {
        await tailer.scan({ scope: "recent",now: cutoff,automatic: { phase: "baseline",budget: new CaptureWorkBudget() } });
        if(captureBaselineStatus(automaticLedger.database).sources.find(row => row.source===provider)!.status==="complete")
          break;
      }
      assert.equal(captureBaselineStatus(automaticLedger.database).sources.find(row => row.source===provider)!.status,"complete");
      const timestamp=new Date().toISOString(),session="019eaaaa-bbbb-7ccc-8ddd-eeeeeeeeeeee";
      const relative=provider==="codex"? path.join(...timestamp.slice(0,10).split("-"),`rollout-${session}.jsonl`):path.join("project",`${session}.jsonl`);
      for(const [directory,tokens] of [[healthy,5],[target,999]] as const) {
        const file=path.join(directory,relative);
        fs.mkdirSync(path.dirname(file),{ recursive: true });
        const contents=provider==="codex"? [
          { type: "session_meta",timestamp,payload: { id: session } },{ type: "turn_context",payload: { model: "gpt-5.5" } },
          ...[0,tokens].map(input => ({ type: "event_msg",timestamp,payload: { type: "token_count",info: { total_token_usage: { input_tokens: input,cached_input_tokens: 0,output_tokens: 0,reasoning_output_tokens: 0 } } } })),
        ]:[{ type: "assistant",timestamp,message: { id: "native-one",model: "claude-opus-5",usage: { input_tokens: tokens,output_tokens: 0 } } }];
        fs.writeFileSync(file,contents.map(row => JSON.stringify(row)).join("\n")+"\n");
      }
      let total=0,last: Awaited<ReturnType<typeof tailer.scan>>|undefined;
      for(let iteration=0;iteration<20&&total===0;iteration++) {
        last=await tailer.scan({ scope: "recent",automatic: { phase: "capture",budget: new CaptureWorkBudget() } });
        total+=last.tokensAppended.input;
      }
      check(`${provider}_automatic_healthy_root_survives_missing_and_unsafe_root`,() => {
        assert.equal(total,5,JSON.stringify({ provider,last }));
        assert.equal(last!.exhaustive,false);
        assert.equal(last!.discoveryErrors,2);
        assert.equal(last!.roots!.find(row => row.rootId==="unsafe")!.state,"unsafe");
      });
    }
    finally {
      tailer.close();
      automaticLedger.close();
      fs.rmSync(automaticRoot,{ recursive: true,force: true });
    }
  }
  const ledger=new LocalEventBuffer(":memory:",{ workspaceId: tenant });
  try {
    const session="019e1111-2222-7333-8444-555555555555";
    for(const [index,repo,tokens] of [[1,A,1],[2,A,1],[3,B,100]] as const) {
      const id=`00000000-0000-4000-8000-${String(index).padStart(12,"0")}`;
      ledger.append(aiInteractionEventSchema.parse({ id,tenantId: tenant,source: "codex",dataMode: "metadata",eventType: "usage_rollout",observedAt: "2026-09-02T00:00:00.000Z",sessionId: session,inputTokens: tokens,outputTokens: 0,metadata: {} }),[]);
      ledger.database.prepare("update buffered_events set repo_hash=? where id=?").run(repo,id);
    }
    check("session_summary_does_not_assign_102_tokens_to_A",() => assert.equal(collectSessionSnapshots(ledger.database,{ until: "2099-01-01T00:00:00.000Z" })[0].repoHash,null));
    ledger.projection.runMaintenance(new Date());
    const view=collectSessionProjectAllocations(ledger.database,{ tenantId: tenant,period,now: new Date().toISOString() });
    check("local_event_allocation_reader_conserves_A2_B100",() => { assert.equal(view.projects.find(row => row.projectKey===A)!.usage.inputTokens,2); assert.equal(view.projects.find(row => row.projectKey===B)!.usage.inputTokens,100); });
    check("local_allocation_reader_is_workspace_scoped",() => {
      const other=collectSessionProjectAllocations(ledger.database,{ tenantId: "foreign",period,now: new Date().toISOString() });
      assert.equal(other.usage.events,0);
      assert.equal(other.usage.inputTokens,0);
      assert.throws(() => collectSessionProjectAllocations(ledger.database,{ tenantId: tenant,period,now,limit: 1.5 }),/limit_invalid/);
    });
    for(const [index,at,tokens] of [[81,"2026-08-31T18:00:00-06:00",3],[82,"2026-09-06T18:00:00-06:00",900]] as const) {
      const id=`00000000-0000-4000-8000-${String(index).padStart(12,"0")}`;
      ledger.append(aiInteractionEventSchema.parse({ id,tenantId: tenant,source: "codex",dataMode: "metadata",eventType: "assistant_response",observedAt: at,inputTokens: tokens,outputTokens: 0,projectKey: A,metadata: {} }),[]);
      ledger.database.prepare("update buffered_events set repo_hash=? where id=?").run(B,id);
    }
    ledger.projection.runMaintenance(new Date());
    const offset=collectSessionProjectAllocations(ledger.database,{ tenantId: tenant,period,now: new Date().toISOString() });
    check("equivalent_offset_period_bounds_and_explicit_project_win",() => {
      assert.equal(offset.usage.inputTokens,105);
      assert.equal(offset.projects.find(row => row.projectKey===A)!.usage.inputTokens,5);
      assert.equal(offset.projects.find(row => row.projectKey===B)!.usage.inputTokens,100);
    });
  }
  finally {
    ledger.close();
  }
}
captureProof().then(() => {
  const receipt={ schema: "economics-acceptance/v1",completed: true,assertions: results.length,passed: results.length,failed: 0,checks: results };
  console.log(JSON.stringify(receipt,null,2));
  if(process.env.ECONOMICS_RECEIPT)
    fs.writeFileSync(process.env.ECONOMICS_RECEIPT,JSON.stringify(receipt,null,2)+"\n");
  completion.complete();
}).catch(error => { console.error(error); process.exitCode=1; });
