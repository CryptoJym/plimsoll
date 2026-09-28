/** Advertised B22 capture withdrawal and gap delivery. All persisted gaps
 * are sent by revision; a zero-item withdrawal always goes first so an old
 * complete claim is removed before the next raw event can leave this process. */
import type { LocalEventBuffer } from "../buffer";
import { createHash } from "node:crypto";
import type { CollectorConfig } from "../config";
import { captureSpoolState } from "../capture-spool-state";
import { authenticatedJsonPost } from "../http-transport";
import { faultGapId, rolloutGapScope } from "./capture-gaps";
import { observeActivitySummaryAdvertisement,
  readActivitySummaryAdvertisement } from "./activity-summary-capability";

type GapRow = {
  gapId:string; revision:number; source:string; sessionId:string|null;
  machineHash:string; epochKey:string; startedAtMs:number; endedAtMs:number|null;
  intervalBasis:string; resolvedAtMs:number|null; countBasis:string;
  droppedRows:number|null; droppedUsageRows:number|null; reason:string;
  fileKeyDigest:string|null; unreadBytes:number|null;
};
function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value)
    .filter(([,entry])=>entry!==undefined).sort(([a],[b])=>a.localeCompare(b))
    .map(([name,entry])=>[name,canonical(entry)]));
  return value;
}
function gapDigest(item: unknown): string {
  return createHash("sha256").update(JSON.stringify(canonical(item))).digest("hex").slice(0,32);
}

export async function publishAdvertisedCaptureUncertainty(input: {
  config:CollectorConfig; buffer:LocalEventBuffer; url:string; spoolHome:string;
  ingestKey?:string; signingSecret?:string; fetchImpl:typeof fetch;
  timeoutMs:number; now:()=>Date;
}): Promise<void> {
  const {config,buffer}=input;
  const db=buffer.database;
  const advertisement=readActivitySummaryAdvertisement(db,config.installKey);
  if (!advertisement.enabled) return;
  const scope=rolloutGapScope(db);
  if (scope.workspaceId!==config.tenantId || !scope.installationEpochId ||
      scope.installationEpochId==="unbound") return;
  const pendingCount=(db.prepare(`select count(*) as n from capture_gaps
    where workspace_id=? and installation_epoch_id=? and upload_state!='acked'`)
    .get(scope.workspaceId,scope.installationEpochId) as {n:number}).n;
  const openCount=(db.prepare(`select count(*) as n from capture_gaps
    where workspace_id=? and installation_epoch_id=?
      and ended_at_ms is null and resolved_at_ms is null`)
    .get(scope.workspaceId,scope.installationEpochId) as {n:number}).n;
  const durability=buffer.captureDurability;
  durability.refreshFromDisk();
  const status=durability.status();
  if (pendingCount===0 && openCount===0 && !status.restartUnverified &&
      status.faults.length===0) return;
  const bindingInstall=advertisement.actorBindingInstallHeard ?? config.cloudDeviceId;
  const bindingVersion=advertisement.actorBindingVersion;
  if (!bindingInstall || bindingVersion===null) throw new Error("activity_summary_v2_binding_unavailable");
  const spool=captureSpoolState(input.spoolHome);
  const makeClaim=(declaredGaps: Array<{gapId:string;revision:number}>)=>{
    const v1=buffer.delivery.captureClaim([],spool,input.now());
    if (!v1) throw new Error("activity_summary_v2_claim_unavailable");
    const faults=durability.status().faults.map(({faultId,kind,atMs})=>({faultId,kind,atMs}));
    if (faults.length>500) throw new Error("activity_summary_v2_fault_limit");
    return {...v1,v:2 as const,through:null,
      unattested:status.restartUnverified ? "restart_unverified"
        : faults.length ? "gap_record_unavailable" : v1.unattested ?? "frontier_unknown",
      declaredGaps,faults,tailer:{sources:[]}};
  };
  const send=async(items:unknown[],claim:ReturnType<typeof makeClaim>)=>{
    const body=JSON.stringify({kind:"activity_summary_v2",contractVersion:2,
      tenantId:config.tenantId,installKey:config.installKey,
      actorBindingVersionHeard:bindingVersion,
      actorBindingInstallHeard:bindingInstall,items});
    const response=await authenticatedJsonPost({url:input.url,body,
      installKey:config.installKey,ingestKey:input.ingestKey,
      signingSecret:input.signingSecret,fetchImpl:input.fetchImpl,now:input.now,
      timeoutMs:input.timeoutMs,maxRequestBytes:1_500_000,
      headers:{"x-plimsoll-capture":JSON.stringify(claim)}});
    if (!response.ok) {
      if (response.status===503 && response.body && typeof response.body==="object" &&
          !Array.isArray(response.body) &&
          (response.body as Record<string,unknown>).error==="activity_summary_v2_unavailable") {
        observeActivitySummaryAdvertisement(db,config.installKey,{},input.now().getTime());
        return null;
      }
      throw new Error(`activity_summary_v2_http_${response.status}`);
    }
    const value=response.body && typeof response.body==="object" && !Array.isArray(response.body)
      ? response.body as Record<string,unknown> : {};
    if (value.ok!==true || !Array.isArray(value.receipts) || value.receipts.length!==items.length ||
        !value.capture || typeof value.capture!=="object" || Array.isArray(value.capture)) {
      throw new Error("activity_summary_v2_invalid_receipt");
    }
    observeActivitySummaryAdvertisement(db,config.installKey,value,input.now().getTime());
    return value;
  };
  const withdrawal=await send([],makeClaim([]));
  if (!withdrawal) return;
  if ((withdrawal.capture as Record<string,unknown>).status!=="advanced")
    throw new Error("activity_summary_v2_withdrawal_unconfirmed");
  if (!readActivitySummaryAdvertisement(db,config.installKey).enabled) return;

  const rows=db.prepare(`select gap_id as gapId,revision,source,session_id as sessionId,
      machine_hash as machineHash,epoch_key as epochKey,
      started_at_ms as startedAtMs,ended_at_ms as endedAtMs,
      interval_basis as intervalBasis,resolved_at_ms as resolvedAtMs,
      count_basis as countBasis,dropped_rows as droppedRows,
      dropped_usage_rows as droppedUsageRows,reason,file_key_digest as fileKeyDigest,
      unread_bytes as unreadBytes
    from capture_gaps where workspace_id=? and installation_epoch_id=?
      and upload_state!='acked' order by gap_id limit 100`)
    .all(scope.workspaceId,scope.installationEpochId) as GapRow[];
  if (rows.length===0) return;
  const faultIds=new Map((db.prepare(`select fault_id as faultId from capture_faults`)
    .all() as Array<{faultId:string}>).map(row=>[faultGapId(row.faultId),row.faultId]));
  const items=rows.map(row=>{
    const faultId=faultIds.get(row.gapId)??null;
    if (row.reason==="gap_record_unavailable" && faultId===null)
      throw new Error("activity_summary_v2_fault_link_missing");
    return {kind:"capture_gap",gapId:row.gapId,revision:row.revision,
      source:row.source,faultId,
      ...(row.sessionId===null?{}:{sessionId:row.sessionId}),
      machineHash:row.machineHash,epochKey:row.epochKey,
      startedAtMs:row.startedAtMs,endedAtMs:row.endedAtMs,
      resolvedAt:row.resolvedAtMs===null?null:new Date(row.resolvedAtMs).toISOString(),
      intervalBasis:row.intervalBasis,countBasis:row.countBasis,
      droppedRows:row.droppedRows,droppedUsageRows:row.droppedUsageRows,
      reason:row.reason,fileKeyDigest:row.fileKeyDigest,unreadBytes:row.unreadBytes};
  });
  const declaredGaps=rows.map(row=>({gapId:row.gapId,revision:row.revision}));
  const result=await send(items,makeClaim(declaredGaps));
  if (!result) return;
  const receipts=result.receipts as Array<Record<string,unknown>>;
  const accepted=rows.filter((row,index)=>{
    const receipt=receipts[index];
    return receipt?.kind==="capture_gap" && receipt.key===row.gapId &&
      receipt.revision===row.revision && receipt.digest===gapDigest(items[index]) &&
      (receipt.status==="held" || receipt.status==="duplicate");
  }).map(row=>({gapId:row.gapId,revision:row.revision}));
  if (accepted.length!==rows.length) throw new Error("activity_summary_v2_gap_receipt_incomplete");
  durability.acknowledgeGaps(accepted);
}
