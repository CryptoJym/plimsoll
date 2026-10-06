import crypto from "node:crypto";
import { performance } from "node:perf_hooks";

import { mutateCollectorConfigTransactionally } from "./config";
import {
  dispatchBindingSchema,
  dispatchBindingForSession,
  dispatchBindingMetadata,
  captureRootDigest,
  namespacedWorkItemIdSchema,
  type DispatchBinding,
} from "./capture-root-inventory";
import type { CaptureRoot } from "./capture-root-inventory";
import type { LocalEventBuffer } from "./buffer";
import { aiInteractionEventSchema } from "../../shared/src/schemas";
import { assertNoTerminalDispatchContinuation, dispatchHistoryPressure, historicalDispatchBindings, updateDispatchHistory } from "./dispatch-binding-index";
import { dispatchBindingProofDigest, verifiedDispatchTerminalProof, type DispatchTerminalAuthority } from "./dispatch-binding-lifecycle";

const BIND_FLAGS = new Set([
  "--session-id", "--work-item-id", "--project-key", "--attempt-id", "--parent-attempt-id",
  "--role", "--work-class", "--complexity-band", "--technique-id", "--technique-version",
  "--assignment-id", "--arm", "--launched-by", "--valid-from", "--valid-until",
]);
const CLOSE_FLAGS = new Set(["--attempt-id"]);
const RESTAMP_FLAGS = CLOSE_FLAGS;
// The intersection of the local outcome matcher, outbound work_ref/v1, and
// the router's canonical receipt contract. Legacy bindings remain readable.
const LINKABLE_WORK_ITEM = /^beads:eco-[a-z0-9]+(?:\.[1-9][0-9]*)*$/;
const LINKABLE_ATTEMPT = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export function dispatchBindLinkage(binding: Pick<DispatchBinding, "workItemId" | "attemptId">) {
  const invalid: Array<"work-item-id" | "attempt-id"> = [];
  if (!LINKABLE_WORK_ITEM.test(binding.workItemId) ||
      Buffer.byteLength(binding.workItemId.slice("beads:".length), "utf8") > 128) {
    invalid.push("work-item-id");
  }
  if (!LINKABLE_ATTEMPT.test(binding.attemptId)) invalid.push("attempt-id");
  return { state: invalid.length ? "unlinkable" as const : "linkable" as const, invalid };
}

export function countUnlinkableDispatchBindings(roots: readonly CaptureRoot[], now = new Date()): number {
  if (!Number.isFinite(now.getTime())) throw new Error("dispatch_clock_invalid");
  const seen = new Set<string>();
  let count = 0;
  for (const root of roots) for (const binding of root.dispatch ?? []) {
    if (Date.parse(binding.validFrom) > now.getTime() ||
        (binding.validUntil && Date.parse(binding.validUntil) <= now.getTime())) continue;
    const key = JSON.stringify(binding);
    if (seen.has(key)) continue;
    seen.add(key);
    if (dispatchBindLinkage(binding).state === "unlinkable") count += 1;
  }
  return count;
}

function warnUnlinkableBind(binding: DispatchBinding) {
  const linkage = dispatchBindLinkage(binding);
  if (linkage.invalid.includes("work-item-id")) {
    console.warn(`dispatch bind warning: --work-item-id ${JSON.stringify(binding.workItemId)} cannot link; ` +
      "expected beads:<exact lowercase canonical Beads ID>, eco-... and at most 128 bytes");
  }
  if (linkage.invalid.includes("attempt-id")) {
    console.warn(`dispatch bind warning: --attempt-id ${JSON.stringify(binding.attemptId)} cannot link; ` +
      "expected a lowercase canonical UUIDv4 run ID");
  }
}

function options(args: string[], allowed: Set<string>) {
  const values = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (!allowed.has(flag) || !value || value.startsWith("--") || values.has(flag))
      throw new Error("dispatch_invalid_options");
    values.set(flag, value);
  }
  return (flag: string) => values.get(flag);
}
function required(value: string | undefined, flag: string) {
  if (!value) throw new Error(`dispatch_missing_${flag.slice(2)}`);
  return value;
}
export function bindDispatch(args: string[], now = new Date()) {
  if (!Number.isFinite(now.getTime())) throw new Error("dispatch_clock_invalid");
  const strict = args.filter(arg => arg === "--strict").length;
  if (strict > 1) throw new Error("dispatch_invalid_options");
  const value = options(args.filter(arg => arg !== "--strict"), BIND_FLAGS);
  const technique = [value("--technique-id"), value("--technique-version"),
    value("--assignment-id"), value("--arm")];
  if (technique.some(Boolean) && !technique.every(Boolean)) throw new Error("dispatch_technique_flags_incomplete");
  const workItemId = namespacedWorkItemIdSchema.parse(required(value("--work-item-id"), "--work-item-id"));
  const sessionId = required(value("--session-id"), "--session-id");
  const attemptId = required(value("--attempt-id"), "--attempt-id");
  const binding = dispatchBindingSchema.parse({
    sessionId,workItemId,projectKey: required(value("--project-key"), "--project-key"),
    attemptId,parentAttemptId: value("--parent-attempt-id") ?? null,
    companyRef: null,acceptedOutcomeId: null,
    validFrom: required(value("--valid-from"), "--valid-from"),validUntil: value("--valid-until") ?? null,
    evidenceRef: `dispatch:${crypto.createHash("sha256").update(JSON.stringify([sessionId,attemptId,workItemId])).digest("hex")}`,
    role: value("--role") ?? "author",workClass: value("--work-class") ?? "other",
    complexityBand: value("--complexity-band") ?? "unknown",
    ...(technique.every(Boolean) ? { techniqueId: technique[0],techniqueVersion: technique[1],
      assignmentId: technique[2],arm: technique[3] } : {}),
    ...(value("--launched-by") ? { launchedBy: value("--launched-by") } : {}),
  });
  if (binding.validUntil && Date.parse(binding.validUntil) <= Date.parse(binding.validFrom))
    throw new Error("capture_dispatch_window_invalid");
  if (Date.parse(binding.validFrom) > now.getTime() ||
      (binding.validUntil && Date.parse(binding.validUntil) > now.getTime()))
    throw new Error("dispatch_binding_future_window");
  const linkage = dispatchBindLinkage(binding);
  if (strict && linkage.state === "unlinkable") {
    warnUnlinkableBind(binding);
    throw new Error("dispatch_bind_unlinkable");
  }
  let archived = 0;
  const updated = mutateCollectorConfigTransactionally(current => {
    if (!current.captureRoots?.length) throw new Error("dispatch_capture_roots_missing");
    assertNoTerminalDispatchContinuation(current.captureRoots,binding,dispatchBindingSchema.parse);
    const result = updateDispatchHistory(current.captureRoots, now, (root, bindings) => {
      for(const original of bindings.filter(candidate=>candidate.sessionId===sessionId&&candidate.attemptId===attemptId)) {
        if(original.validFrom===binding.validFrom) {
          if(JSON.stringify(original)!==JSON.stringify(binding))throw new Error("dispatch_binding_replacement_conflict");
        } else {
          const {validFrom:oldFrom,validUntil:oldUntil,...oldIdentity}=original;
          const {validFrom:newFrom,validUntil:newUntil,...newIdentity}=binding;
          if(oldUntil===null||Date.parse(oldUntil)>Date.parse(newFrom)||JSON.stringify(oldIdentity)!==JSON.stringify(newIdentity))
            throw new Error("dispatch_binding_replacement_conflict");
        }
      }
      const prior = bindings.filter(candidate => candidate.sessionId !== sessionId || candidate.attemptId !== attemptId ||
        // Preserve a closed predecessor when this exact attempt continues in a
        // new disjoint interval. Archived windows cannot be rewritten.
        (candidate.validUntil !== null && Date.parse(candidate.validUntil) <= now.getTime() &&
          candidate.validFrom !== binding.validFrom));
      const overlaps = prior.some(candidate => candidate.sessionId === sessionId &&
        Date.parse(candidate.validFrom) < (binding.validUntil ? Date.parse(binding.validUntil) : Infinity) &&
        Date.parse(binding.validFrom) < (candidate.validUntil ? Date.parse(candidate.validUntil) : Infinity));
      if (overlaps) throw new Error("dispatch_session_window_conflict");
      return [...prior,binding];
    }, dispatchBindingSchema.parse);
    archived = result.archived;
    return { ...current,captureRoots: result.roots };
  });
  if (linkage.state === "unlinkable") warnUnlinkableBind(binding);
  return { status: "dispatch_bound" as const,sessionId,workItemId,projectKey: binding.projectKey,
    attemptId,role: binding.role,roots: updated.captureRoots?.length ?? 0,pruned: 0,archived,linkage,
    pressure: dispatchHistoryPressure(updated.captureRoots ?? [], dispatchBindingSchema.parse) };
}

export function closeDispatch(args: string[], now = new Date(), authority?: DispatchTerminalAuthority) {
  if (!Number.isFinite(now.getTime())) throw new Error("dispatch_clock_invalid");
  const value = options(args,CLOSE_FLAGS);
  const attemptId = required(value("--attempt-id"),"--attempt-id");
  let closed = 0,archived = 0;
  mutateCollectorConfigTransactionally((current, context) => {
    if (!current.captureRoots?.length) throw new Error("dispatch_capture_roots_missing");
    const proof = verifiedDispatchTerminalProof(authority, now, context.sourceSha256);
    if (proof.attemptId !== attemptId) throw new Error("dispatch_terminal_proof_attempt_mismatch");
    const targets = current.captureRoots.flatMap(root => (root.dispatch ?? []).filter(binding =>
      binding.attemptId === attemptId && (binding.validUntil === null || Date.parse(binding.validUntil) > now.getTime()))
      .map(binding => ({ root,binding })));
    if (!targets.length || targets.length !== proof.bindings.length) throw new Error("dispatch_terminal_proof_scope_mismatch");
    for (const { root,binding } of targets) {
      if (binding.sessionId !== proof.sessionId || binding.workItemId !== proof.workItemId ||
          Date.parse(binding.validFrom) >= Date.parse(proof.terminalAt) || !proof.bindings.some(row =>
            row.rootDigest === captureRootDigest(root) && row.bindingSha256 === dispatchBindingProofDigest(binding)))
        throw new Error("dispatch_terminal_proof_binding_mismatch");
    }
    const result = updateDispatchHistory(current.captureRoots, now, (root, bindings) => bindings.map(binding => {
      if (!targets.some(target => target.root.rootId === root.rootId && target.binding === binding)) return binding;
      closed++;
      return { ...binding, validUntil: proof.terminalAt };
    }), dispatchBindingSchema.parse,proof);
    archived = result.archived;
    return { ...current,captureRoots: result.roots };
  });
  return { status: "dispatch_closed" as const,attemptId,closed,pruned: 0,archived };
}

/** Correct only local rows that have never entered a delivery attempt. */
export function restampDispatch(args: string[],buffer: LocalEventBuffer,roots: readonly CaptureRoot[]) {
  const value=options(args,RESTAMP_FLAGS);
  const attemptId=dispatchBindingSchema.shape.attemptId.parse(required(value("--attempt-id"),"--attempt-id"));
  const bySession=new Map<string,{ binding: DispatchBinding;source: CaptureRoot["source"];rootId: string|null;rootDigest: string|null }>();
  const historical=historicalDispatchBindings(roots,{attemptId},dispatchBindingSchema.parse);
  const all=roots.flatMap(root => (root.dispatch??[]).map(binding => ({root,binding}))).concat(
    historical.map(({root,binding})=>({root:root as CaptureRoot,binding})));
  for(const {root,binding} of all) {
    if(binding.attemptId!==attemptId) continue;
    const rootId=root.source==="claude_code" ? root.rootId : null;
    const key=`${root.source}\u0000${binding.sessionId}\u0000${rootId??""}\u0000${binding.validFrom}`;
    const prior=bySession.get(key);
    if(prior&&JSON.stringify(prior.binding)!==JSON.stringify(binding)) throw new Error("dispatch_restamp_binding_conflict");
    bySession.set(key,{binding,source:root.source,rootId,
      rootDigest:root.source==="claude_code"?captureRootDigest(root):null});
  }
  if(!bySession.size) throw new Error("dispatch_attempt_not_found");
  const started=performance.now();
  const MAX_ROWS=5000,MAX_BYTES=16*1024*1024,MAX_ROW_BYTES=64*1024,MAX_MS=2000,MAX_QUERIES=128;
  let scanned=0,restamped=0,skipped=0,truncated=false,readBytes=0,queries=0;
  const hasRootObservations=Boolean(buffer.database.prepare(`select 1 from sqlite_master
    where type='table' and name='capture_root_observations'`).get());
  const admittedRoot=hasRootObservations ? buffer.database.prepare(`select 1
    from capture_root_observations where event_id=? and root_digest=? and state='admitted'`) : null;
  for(const {binding,source,rootId,rootDigest} of bySession.values()) {
    if(scanned>=MAX_ROWS||readBytes>=MAX_BYTES||performance.now()-started>MAX_MS||queries>=MAX_QUERIES) {truncated=true;break;}
    const expectedBinding=JSON.stringify(binding);
    let cursor=0;
    while(true) {
    const limit=Math.min(64,MAX_ROWS-scanned+1,Math.floor((MAX_BYTES-readBytes)/MAX_ROW_BYTES));
    if(limit<1||performance.now()-started>MAX_MS||queries>=MAX_QUERIES) {truncated=true;break;}
    queries++;
    // Pin selection and nested delivery writes in one BEGIN IMMEDIATE. No
    // other writer can replace a selected raw incarnation between them.
    const priorCounts={scanned,restamped,skipped,readBytes};
    let batchLength=0;
    try { buffer.database.transaction(()=>{
    const rows=buffer.database.prepare(`select raw.rowid as rowId,raw.id,
      case when length(cast(raw.payload_json as blob))<=${MAX_ROW_BYTES} then raw.payload_json else null end as payloadJson,
      raw.observed_at as observedAt from buffered_events as raw where raw.source=? and raw.session_id=?
        and julianday(raw.observed_at)>=julianday(?)-1.0/86400
        and (? is null or julianday(raw.observed_at)<julianday(?)+1.0/86400)
        and case when length(cast(raw.payload_json as blob))>${MAX_ROW_BYTES} then 1
          when json_valid(raw.payload_json)=1 then
            json_extract(raw.payload_json,'$.metadata.workItemId') is null
            and (? is null or json_extract(raw.payload_json,'$.metadata.captureRootId')=?)
          else 0 end
        and raw.rowid>?
        and raw.uploaded_at is null and raw.privacy_disposition is null
        and not exists (select 1 from upload_replays as replay
          where replay.delivery_id=raw.id
             or (replay.raw_id=raw.id and replay.raw_created_at=raw.created_at
                 and replay.raw_generation is raw.privacy_generation))
        and not exists (select 1 from upload_outbox as queued where queued.raw_rowid=raw.rowid
          and queued.raw_id=raw.id and queued.raw_created_at=raw.created_at
          and queued.raw_generation is raw.privacy_generation
          and (queued.attempt_count>0 or queued.sealed_envelope_json is not null or queued.state<>'pending'))
      order by raw.rowid limit ?`).all(source,binding.sessionId,binding.validFrom,
        binding.validUntil,binding.validUntil,rootId,rootId,cursor,limit) as Array<{ rowId:number;id:string;payloadJson:string|null;observedAt:string }>;
    batchLength=rows.length;
    if(performance.now()-started>MAX_MS) {truncated=true;return;}
    for(const row of rows) {
      if(scanned>=MAX_ROWS||readBytes>=MAX_BYTES||performance.now()-started>MAX_MS) {truncated=true;break;}
      scanned++;
      cursor=row.rowId;
      if(row.payloadJson===null) {skipped++;continue;}
      readBytes+=Buffer.byteLength(row.payloadJson);
      if(readBytes>MAX_BYTES) {truncated=true;skipped++;break;}
      if(!Number.isFinite(Date.parse(row.observedAt))||Date.parse(row.observedAt)<Date.parse(binding.validFrom) ||
          (binding.validUntil&&Date.parse(row.observedAt)>=Date.parse(binding.validUntil))) { skipped++;continue; }
      // A root ID can be reused after a profile, directory or installation
      // changes. Only the admitted receipt proves the row's full root digest.
      if(source==="claude_code"&&(!rootDigest||!admittedRoot?.get(row.id,rootDigest))) {
        skipped++;continue;
      }
      if(JSON.stringify(dispatchBindingForSession(source,binding.sessionId,
        row.observedAt,roots))!==expectedBinding) { skipped++;continue; }
      const event=aiInteractionEventSchema.parse(JSON.parse(row.payloadJson));
      if(event.source!==source||event.sessionId!==binding.sessionId||
          Date.parse(event.observedAt)!==Date.parse(row.observedAt)) {skipped++;continue;}
      if(typeof event.metadata.captureRootId==="string") {
        const eventRoot=roots.find(root=>root.source===source&&root.rootId===event.metadata.captureRootId);
        if(!eventRoot||event.metadata.captureProfileId!==eventRoot.profileId||
            event.metadata.installationEpochId!==eventRoot.installationEpochId||
            !admittedRoot?.get(row.id,captureRootDigest(eventRoot))) {skipped++;continue;}
      }
      const corrected=aiInteractionEventSchema.parse({ ...event,
        metadata: { ...event.metadata,...dispatchBindingMetadata(binding) } });
      if(buffer.delivery.restampUnsentRaw(row.id,JSON.stringify(corrected))) restamped++;
      else skipped++;
      if(performance.now()-started>MAX_MS) throw new Error("dispatch_restamp_deadline_exceeded");
    }
    }).immediate(); }
    catch(error) {
      if(!(error instanceof Error)||error.message!=="dispatch_restamp_deadline_exceeded")throw error;
      ({scanned,restamped,skipped,readBytes}=priorCounts);truncated=true;
    }
    if(truncated||batchLength<limit) break;
    }
  }
  return { status:"dispatch_restamped" as const,attemptId,sessions:bySession.size,scanned,restamped,skipped,truncated,readBytes,queries };
}
