import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ensureUuidEventId } from "../packages/collector-cli/src/upload-history";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";
import { usageFactFromEvent } from "../packages/shared/src/economics/event-adapter";
import { buildWorkspaceEconomics,reconcileUsage } from "../packages/shared/src/economics/service";
import type { EconomicsInput } from "../packages/shared/src/economics/contracts";
import { collectSessionProjectAllocations } from "../packages/collector-cli/src/session-sync";
import { createProofCompletion } from "./lib/proof-completion";
const completion=createProofCompletion("live-usage-projection",10);

const start="2026-09-07T01:00:00.000Z",end="2026-09-07T02:00:00.000Z";
const tenant="workspace-test";
const project=`sha256:${"a".repeat(64)}`;
const metadata={
  sourceVersion:"codex.app-server.usage.v1",sourceIdentityEvidenceRef:"native_runtime_observed_interval_v1",
  sourceEventId:"live-projection-event",logicalSourceEventId:"live-projection-event",installationEpochId:"epoch-one",
  sourcePayloadDigest:"a".repeat(64),captureRootId:"root-one",captureProfileId:"profile-one",
  liveObservationKind:"observed_interval",liveIntervalStart:start,liveIntervalEnd:end,
  liveAttributionState:"unresolved",liveFinanceEligibility:"unqualified_observer",
  liveTotalTokens:15,liveReasoningOutputTokens:2,
  workItemId:"work-one",workEvidenceRef:"endpoint-only-evidence",dispatchProjectKey:project,
  companyRef:"company-one",captureAccountHash:"endpoint-account",
};
const event=aiInteractionEventSchema.parse({
  id:"live-projection-event",tenantId:"workspace-test",source:"codex",dataMode:"metadata",eventType:"usage_live",
  observedAt:end,sessionId:"live-session",intent:"unknown",actionClass:"other",inputTokens:10,outputTokens:5,
  cacheReadTokens:0,cacheCreationTokens:0,projectKey:project,metadata,
});
const checks:string[]=[];
const check=(name:string,fn:()=>void) => { fn();checks.push(name);completion.check(name); };
const root=fs.mkdtempSync(path.join(os.tmpdir(),"plimsoll-live-projection-"));
const ledger=path.join(root,"events.sqlite");
const options={workspaceId:"workspace-test",delivery:{enabled:false},enrollmentNow:()=>new Date("2026-09-01T00:00:00.000Z")};
let buffer=new LocalEventBuffer(ledger,options);
try {
  // Insert a synthetic raw fixture behind the intake boundary. This test proves
  // projection and retention; it never grants a live producer capability.
  const seed={...event,eventType:"assistant_response" as const,metadata:{}};
  assert.equal(buffer.append(seed),true);
  buffer.database.prepare(`update buffered_events set event_type='usage_live',payload_json=? where id=?`)
    .run(JSON.stringify(event),event.id);
  const raw=buffer.database.prepare(`select rowid as rowid from buffered_events where id=?`).get(event.id) as {rowid:number};
  buffer.projection.tryApplyRawRow(raw.rowid);
  const read=()=>buffer.database.prepare(`select event_type,input_tokens,output_tokens,cost_nanos,model,
    project_key,repo_hash,account_hash,live_usage_json from dashboard_event_facts where raw_rowid=?`).get(raw.rowid) as Record<string,unknown>;
  check("validated_interval_and_counters_persist_without_endpoint_allocation",()=>{
    const fact=read();
    assert.equal(fact.event_type,"usage_live");
    assert.equal(fact.input_tokens,10);assert.equal(fact.output_tokens,5);
    for(const key of ["cost_nanos","model","project_key","repo_hash","account_hash"]) assert.equal(fact[key],null);
    assert.deepEqual(JSON.parse(String(fact.live_usage_json)),{
      intervalStart:start,intervalEnd:end,attributionState:"unresolved",financeEligibility:"unqualified_observer",
      totalTokens:15,reasoningOutputTokens:2,
    });
  });
  check("repair_clears_invalid_interval_but_keeps_live_exclusion",()=>{
    buffer.database.prepare(`update buffered_events set payload_json='broken-private-sentinel' where id=?`).run(event.id);
    assert.ok(buffer.database.prepare(`select 1 from dashboard_projection_repairs where raw_rowid=?`).get(raw.rowid));
    buffer.projection.tryApplyRawRow(raw.rowid);
    assert.equal(read().live_usage_json,null);assert.equal(read().event_type,"usage_live");
    buffer.database.prepare(`update buffered_events set payload_json=? where id=?`).run(JSON.stringify(event),event.id);
    buffer.projection.tryApplyRawRow(raw.rowid);
  });
  check("actual_retention_and_restart_preserve_interval_evidence",()=>{
    buffer.database.prepare(`update buffered_events set uploaded_at=? where id=?`).run(end,event.id);
    const receipt=buffer.prune(0,{maxRows:64,now:new Date(Date.now()+86400000)});
    assert.equal(receipt.events,1);
    assert.equal((buffer.database.prepare(`select count(*) as n from buffered_events`).get() as {n:number}).n,0);
    // Reuse the raw rowid before maintenance can drain the prune receipt.
    assert.equal(buffer.append({...seed,id:"second-live-projection-event",sessionId:"second-session",inputTokens:3}),true);
    buffer.close();buffer=new LocalEventBuffer(ledger,options);
    for(let i=0;i<4;i++)buffer.projection.runMaintenance(new Date());
    assert.equal(read()?.event_type,"assistant_response");
    const retained=buffer.database.prepare(`select usage_fact_json as fact from dashboard_live_usage_retained where event_id=?`).get(event.id) as {fact:string};
    const fact=JSON.parse(retained.fact);
    assert.equal(fact.inputTokens,10);assert.equal(fact.outputTokens,5);
    assert.equal(fact.observedInterval.intervalStart,start);assert.equal(fact.observedInterval.intervalEnd,end);
    assert.equal(fact.observedInterval.financeEligibility,"unqualified_observer");
    const view=collectSessionProjectAllocations(buffer.database,{
      tenantId:tenant,period:{start:"2026-09-07T00:00:00.000Z",end:"2026-09-08T00:00:00.000Z"},
      now:new Date(Date.now()+86400000).toISOString(),
    });
    assert.equal(view.usage.inputTokens,13);assert.equal(view.coverage.state,"partial");
  });
  check("retained_observation_does_not_block_reused_raw_rowid",()=>{
    assert.equal(read()?.input_tokens,3);
    assert.equal((buffer.database.prepare(`select count(*) as n from dashboard_live_usage_retained`).get() as {n:number}).n,1);
  });
  check("late_terminal_privacy_receipt_removes_retained_observation",()=>{
    buffer.database.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead','local_privacy_violation','local',0,?,?)`).run(ensureUuidEventId(event.id).id,end,end);
    assert.equal((buffer.database.prepare(`select count(*) as n from dashboard_live_usage_retained`).get() as {n:number}).n,0);
  });
  check("terminal_delivery_receipt_before_prune_prevents_retained_copy",()=>{
    const id="withdrawn-before-prune";
    const live={...event,id,sessionId:"preprune-session",metadata:{...metadata,sourceEventId:id,logicalSourceEventId:id}};
    assert.equal(buffer.append({...seed,id,sessionId:live.sessionId}),true);
    buffer.database.prepare(`update buffered_events set event_type='usage_live',payload_json=? where id=?`).run(JSON.stringify(live),id);
    const row=buffer.database.prepare(`select rowid from buffered_events where id=?`).get(id) as {rowid:number};
    buffer.projection.tryApplyRawRow(row.rowid);
    buffer.database.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead','local_privacy_violation','local',0,?,?)`).run(ensureUuidEventId(id).id,end,end);
    buffer.database.prepare(`update buffered_events set uploaded_at=? where id=?`).run(end,id);
    buffer.prune(0,{maxRows:64,now:new Date(Date.now()+86400000)});
    assert.equal((buffer.database.prepare(`select count(*) as n from dashboard_live_usage_retained where event_id=?`).get(id) as {n:number}).n,0);
  });
} finally { buffer.close();fs.rmSync(root,{recursive:true,force:true}); }

const stored={...event,tenantId:tenant,sessionId:event.sessionId!,accountId:"endpoint-account",model:"invented-model",
  projectKey:project,inputTokens:10,outputTokens:5,cacheReadTokens:0,cacheCreationTokens:0,costUsd:99,
  receivedAt:"2026-09-07T02:00:01.000Z",metadata};
const fact=usageFactFromEvent(stored);
const input=(events=[fact],overrides:Partial<EconomicsInput>={}):EconomicsInput=>({
  tenantId:tenant,period:{start:"2026-09-07T00:00:00.000Z",end:"2026-09-08T00:00:00.000Z"},
  now:"2026-09-08T01:00:00.000Z",events,complete:true,observedThrough:"2026-09-08T00:00:00.000Z",...overrides,
});
check("economics_keeps_observation_unpriced_unallocated_and_partial",()=>{
  assert.equal(fact.observedInterval?.intervalStart,start);
  for(const key of ["model","costUsd","projectKey","accountId","workItemId","companyRef"] as const) assert.equal(fact[key],null);
  const view=buildWorkspaceEconomics(input());
  assert.equal(view.usage.inputTokens,10);assert.equal(view.coverage.state,"partial");
  assert.equal(view.projects.find(row=>row.projectKey===null)?.usage.outputTokens,5);
  assert.equal(view.costs.unpricedEvents,1);
});
check("period_crossing_is_not_prorated_or_pointized",()=>{
  for(const period of [{start:"2026-09-07T01:30:00.000Z",end:"2026-09-07T03:00:00.000Z"},
    {start:"2026-09-07T00:00:00.000Z",end}]) {
    const view=buildWorkspaceEconomics(input([fact],{period}));
    assert.equal(view.usage.events,0);assert.equal(view.coverage.state,"unavailable");
    assert.ok(view.evidenceGaps.includes("observed_interval_crosses_period"));
    assert.equal(fact.inputTokens,10);
  }
});
check("registry_requires_whole_interval_and_cannot_override_unresolved",()=>{
  const mapping={projectKey:project,companyRef:null,workItemId:"work-one",source:"registry" as const,
    effectiveFrom:"2026-09-07T01:30:00.000Z",effectiveTo:null,evidenceRef:"mapping-evidence"};
  const qualified={...fact,workItemId:"work-one",observedInterval:{...fact.observedInterval!,attributionState:"qualified" as const}};
  assert.equal(buildWorkspaceEconomics(input([qualified],{projectMappings:[mapping]})).coverage.attributedEvents,0);
  assert.equal(buildWorkspaceEconomics(input([qualified],{projectMappings:[{...mapping,effectiveFrom:start}]})).coverage.attributedEvents,1);
  assert.equal(buildWorkspaceEconomics(input([{...fact,workItemId:"work-one"}],{projectMappings:[{...mapping,effectiveFrom:start}]})).coverage.attributedEvents,0);
});
check("missing_markers_cannot_promote_live_fact_and_interval_conflicts_quarantine",()=>{
  const missing=usageFactFromEvent({...stored,metadata:{}});
  const view=buildWorkspaceEconomics(input([missing]));
  assert.equal(view.usage.events,0);assert.ok(view.evidenceGaps.includes("observed_interval_evidence_missing"));
  const changed={...fact,observedInterval:{...fact.observedInterval!,intervalStart:"2026-09-07T01:01:00.000Z"}};
  assert.equal(reconcileUsage(tenant,[fact,changed],input().now).quarantined.length,2);
});
console.log(JSON.stringify({state:"PASS_LIVE_INTERVAL_PROJECTION_ONLY",checks,nativeProof:false}));
completion.complete();
