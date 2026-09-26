import crypto from "node:crypto";

import { mutateCollectorConfigTransactionally } from "./config";
import {
  dispatchBindingSchema,
  dispatchBindingMetadata,
  namespacedWorkItemIdSchema,
  type DispatchBinding,
} from "./capture-root-inventory";
import type { CaptureRoot } from "./capture-root-inventory";
import type { LocalEventBuffer } from "./buffer";
import { aiInteractionEventSchema } from "../../shared/src/schemas";

const BIND_FLAGS = new Set([
  "--session-id", "--work-item-id", "--project-key", "--attempt-id", "--parent-attempt-id",
  "--role", "--work-class", "--complexity-band", "--technique-id", "--technique-version",
  "--assignment-id", "--arm", "--launched-by", "--valid-from", "--valid-until",
]);
const CLOSE_FLAGS = new Set(["--attempt-id"]);
const RESTAMP_FLAGS = CLOSE_FLAGS;
const RETENTION_MS = 7 * 24 * 60 * 60 * 1_000;

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
function newestBindings(bindings: DispatchBinding[], now: Date) {
  const cutoff = now.getTime() - RETENTION_MS;
  const retained = bindings.filter(binding => !binding.validUntil || Date.parse(binding.validUntil) >= cutoff)
    .sort((a, b) => Date.parse(b.validFrom) - Date.parse(a.validFrom) ||
      b.attemptId.localeCompare(a.attemptId) || b.sessionId.localeCompare(a.sessionId));
  return { bindings: retained.slice(0, 1000), pruned: bindings.length - Math.min(retained.length, 1000) };
}

export function bindDispatch(args: string[], now = new Date()) {
  const value = options(args, BIND_FLAGS);
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
  let pruned = 0;
  const updated = mutateCollectorConfigTransactionally(current => {
    if (!current.captureRoots?.length) throw new Error("dispatch_capture_roots_missing");
    return { ...current,captureRoots: current.captureRoots.map(root => {
      const prior = (root.dispatch ?? []).filter(candidate =>
        candidate.sessionId !== sessionId || candidate.attemptId !== attemptId);
      const overlaps = prior.some(candidate => candidate.sessionId === sessionId &&
        Date.parse(candidate.validFrom) < (binding.validUntil ? Date.parse(binding.validUntil) : Infinity) &&
        Date.parse(binding.validFrom) < (candidate.validUntil ? Date.parse(candidate.validUntil) : Infinity));
      if (overlaps) throw new Error("dispatch_session_window_conflict");
      const result = newestBindings([...prior,binding],now);
      pruned += result.pruned;
      return { ...root,dispatch: result.bindings };
    }) };
  });
  return { status: "dispatch_bound" as const,sessionId,workItemId,projectKey: binding.projectKey,
    attemptId,role: binding.role,roots: updated.captureRoots?.length ?? 0,pruned };
}

export function closeDispatch(args: string[], now = new Date()) {
  const value = options(args,CLOSE_FLAGS);
  const attemptId = required(value("--attempt-id"),"--attempt-id");
  let closed = 0,pruned = 0;
  mutateCollectorConfigTransactionally(current => {
    if (!current.captureRoots?.length) throw new Error("dispatch_capture_roots_missing");
    return { ...current,captureRoots: current.captureRoots.map(root => {
      const updated = (root.dispatch ?? []).map(binding => {
        if (binding.attemptId !== attemptId ||
            (binding.validUntil && Date.parse(binding.validUntil) <= now.getTime())) return binding;
        closed += 1;
        return Date.parse(binding.validFrom) >= now.getTime() ? null : { ...binding,validUntil: now.toISOString() };
      }).filter((binding): binding is DispatchBinding => binding !== null);
      const result = newestBindings(updated,now);
      pruned += result.pruned;
      return { ...root,dispatch: result.bindings };
    }) };
  });
  return { status: "dispatch_closed" as const,attemptId,closed,pruned };
}

/** Correct only local rows that have never entered a delivery attempt. */
export function restampDispatch(args: string[],buffer: LocalEventBuffer,roots: readonly CaptureRoot[]) {
  const value=options(args,RESTAMP_FLAGS);
  const attemptId=dispatchBindingSchema.shape.attemptId.parse(required(value("--attempt-id"),"--attempt-id"));
  const bySession=new Map<string,{ binding: DispatchBinding;source: CaptureRoot["source"] }>();
  for(const root of roots) for(const binding of root.dispatch??[]) {
    if(binding.attemptId!==attemptId) continue;
    const key=`${root.source}\u0000${binding.sessionId}`;
    const prior=bySession.get(key);
    if(prior&&JSON.stringify(prior.binding)!==JSON.stringify(binding)) throw new Error("dispatch_restamp_binding_conflict");
    bySession.set(key,{binding,source:root.source});
  }
  if(!bySession.size) throw new Error("dispatch_attempt_not_found");
  let scanned=0,restamped=0,skipped=0,truncated=false;
  for(const {binding,source} of bySession.values()) {
    const rows=buffer.database.prepare(`select raw.id,raw.payload_json as payloadJson,raw.observed_at as observedAt
      from buffered_events as raw where raw.source=? and raw.session_id=? and raw.observed_at>=?
        and (? is null or raw.observed_at<?) and json_valid(raw.payload_json)=1
        and json_extract(raw.payload_json,'$.metadata.workItemId') is null
        and raw.uploaded_at is null and raw.privacy_disposition is null
        and not exists (select 1 from upload_outbox as queued where queued.raw_rowid=raw.rowid
          and (queued.attempt_count>0 or queued.sealed_envelope_json is not null or queued.state<>'pending'))
      order by raw.rowid limit 5001`).all(source,binding.sessionId,binding.validFrom,
        binding.validUntil,binding.validUntil) as Array<{ id:string;payloadJson:string;observedAt:string }>;
    if(rows.length>5000) truncated=true;
    for(const row of rows.slice(0,5000)) {
      scanned++;
      if(Date.parse(row.observedAt)<Date.parse(binding.validFrom) ||
          (binding.validUntil&&Date.parse(row.observedAt)>=Date.parse(binding.validUntil))) { skipped++;continue; }
      const event=aiInteractionEventSchema.parse(JSON.parse(row.payloadJson));
      const corrected=aiInteractionEventSchema.parse({ ...event,
        metadata: { ...event.metadata,...dispatchBindingMetadata(binding) } });
      if(buffer.delivery.restampUnsentRaw(row.id,JSON.stringify(corrected))) restamped++;
      else skipped++;
    }
  }
  return { status:"dispatch_restamped" as const,attemptId,sessions:bySession.size,scanned,restamped,skipped,truncated };
}
