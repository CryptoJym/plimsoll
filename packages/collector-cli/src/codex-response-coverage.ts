import type Database from "better-sqlite3";
import { estimateCostUsd, providerAccountKey, usageFieldKeys, type AiInteractionEvent } from "../../shared/src/index";
import { captureCodexModel, codexHasUsage, isCaptureGap } from "./codex-model-capture";
import { frozenCodexCapture } from "./codex-named-capture";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";
import { sealOutboundEnvelope } from "./outbound-envelope";

export const CODEX_RESPONSE_DUPLICATE = "codex_response_covered";
const FIELDS = ["inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens", "costUsd"] as const;
type Row = { rowid: number; id: string; created: string; generation: string; workspace: string;
  device: string; epoch: string; payload: string; duplicate: string | null };
const columns = `e.rowid,e.id,e.created_at as created,e.privacy_generation as generation,
  e.workspace_id as workspace,e.device_id as device,e.installation_epoch_id as epoch,
  e.payload_json as payload,e.usage_duplicate_reason as duplicate`;
const turn = (e: AiInteractionEvent) => e.metadata.codexTurnId ?? e.metadata["turn.id"] ?? e.metadata.turn_id;
const native = (e: AiInteractionEvent) => e.eventType === "usage_rollout" && e.metadata.usageSource === "rollout";
const sse = (e: AiInteractionEvent) => e.metadata.otelEventName === "codex.sse_event";
const table = (db: Database.Database) => Boolean(db.prepare(
  "select 1 from sqlite_master where type='table' and name='codex_response_coverage'").get());

function accounts(e: AiInteractionEvent) {
  const attrs = e.metadata.otelAttributes as Record<string, unknown> | undefined;
  return new Set([e.actorId, ...usageFieldKeys.actorId.flatMap(k =>
    [e.metadata[k], attrs?.[k]])].filter((v): v is string => typeof v === "string" && !!v)
    .map(v => v.startsWith("sha256:") ? v : providerAccountKey(v)));
}

function compatible(a: AiInteractionEvent, b: AiInteractionEvent) {
  if (a.source !== "codex" || b.source !== "codex" || !a.sessionId || a.sessionId !== b.sessionId ||
      a.metadata.stitched === "time_window" || b.metadata.stitched === "time_window" ||
      a.metadata.counterLineage || b.metadata.counterLineage || native(a) === native(b) ||
      !(native(a) && sse(b) || sse(a) && native(b))) return false;
  const at = turn(a), bt = turn(b);
  // A turn can contain several responses. Require the exact native
  // completion timestamp as well; turn IDs, when both reported, must agree.
  // Neither a turn alone nor nearest time/counts can spend a witness.
  if (a.observedAt !== b.observedAt || at && bt && at !== bt) return false;
  const aa = accounts(a), ba = accounts(b);
  if (aa.size > 1 || ba.size > 1 || aa.size && ba.size && ![...aa].some(v => ba.has(v))) return false;
  if (a.metadata.captureAccountHash && b.metadata.captureAccountHash &&
      a.metadata.captureAccountHash !== b.metadata.captureAccountHash) return false;
  const common = FIELDS.filter(k => k !== "costUsd" && a[k] !== undefined && b[k] !== undefined);
  if (!common.length) return a.costUsd !== undefined && a.costUsd === b.costUsd;
  return common.every(k => a[k] === b[k]);
}

/** Only retained accounting for this response may reduce another producer.
 * The mapping consumes a witness at most once per native counter record;
 * a later turn (even with identical amounts) cannot spend it again. */
export function codexResponseCoverage(db: Database.Database, event: AiInteractionEvent,
  commit?: (rawId: string) => boolean, raw?: Row) {
  if (event.source !== "codex" || !codexHasUsage(event) || !(native(event) || sse(event)) ||
      !event.sessionId || event.metadata.counterLineage) return undefined;
  if (native(event) && (!turn(event) || !event.model || event.metadata.modelEvidenceConflict === true)) return undefined;
  let binding: Pick<Row, "workspace" | "device" | "epoch"> | undefined;
  try {
    binding = raw ?? db.prepare(`select current_workspace_id as workspace,current_device_id as device,
      current_installation_epoch_id as epoch from collector_workspace_binding where singleton=1`).get() as typeof binding;
  } catch (error) {
    if (error instanceof Error && /no such (table|column)/.test(error.message)) return undefined;
    throw error;
  }
  if (!binding?.epoch || !binding.device) return undefined;
  const eligible = terminalPrivacyEligibilitySql(db, "e");
  const rows = db.prepare(`select ${columns} from buffered_events e
    where e.source='codex' and e.session_id=? and e.id<>? and e.workspace_id=?
      and e.device_id=? and e.installation_epoch_id=? and e.usage_duplicate_reason is null
      and ${native(event) ? "json_extract(e.payload_json,'$.metadata.otelEventName')='codex.sse_event'"
        : "e.event_type='usage_rollout' and json_extract(e.payload_json,'$.metadata.usageSource')='rollout'"}
      and ${eligible} and (e.observed_at=? or (? is not null and
        coalesce(json_extract(e.payload_json,'$.metadata.codexTurnId'),
          json_extract(e.payload_json,'$.metadata."turn.id"'),json_extract(e.payload_json,'$.metadata.turn_id'))=?))
      and (e.input_tokens is not null or e.output_tokens is not null or e.cache_read_tokens is not null
        or e.cache_creation_tokens is not null or e.cost_usd is not null) limit 129`)
    .all(event.sessionId,event.id,binding.workspace,binding.device,binding.epoch,event.observedAt,
      turn(event) ?? null,turn(event) ?? null) as Row[];
  if (rows.length > 128) return undefined;
  const candidates = rows.flatMap(row => {
    const peer = JSON.parse(row.payload) as AiInteractionEvent;
    if (!compatible(event, peer)) return [];
    const nativeId = native(event) ? event.id : peer.id;
    if (table(db) && db.prepare(`select 1 from codex_response_coverage
      where owner_rowid=? and owner_id=? and owner_created=? and owner_generation=? and native_id<>?`)
      .get(row.rowid,row.id,row.created,row.generation,nativeId)) return [];
    const captured = frozenCodexCapture(db,row.id)?.event ?? captureCodexModel(db,peer,row.id,false,false);
    return !isCaptureGap(captured) && codexHasUsage(captured) && captured.model &&
      (!event.model || event.model === captured.model) ? [{row,captured,nativeId}] : [];
  });
  if (candidates.length !== 1) return undefined;
  const {row,captured,nativeId} = candidates[0]!;
  if (raw) {
    const admitted = captureCodexModel(db,event,event.id,false,false);
    if (isCaptureGap(admitted) || admitted.model !== captured.model) return undefined;
  }
  if (commit && !commit(row.id)) return undefined;
  const remaining = {...event, metadata: {...event.metadata}};
  for (const k of FIELDS) if (event[k] !== undefined && captured[k] !== undefined)
    remaining[k] = Math.max(0,event[k]! - captured[k]!);
  const tokensCovered = FIELDS.slice(0,4).every(k => (remaining[k] ?? 0) === 0);
  // A native estimate prices the remaining counters, never the full twin
  // again. A reported cost keeps its own field-wise accounting.
  if (event.costKind === "estimated" || event.metadata.costEstimated === true) {
    remaining.costUsd = tokensCovered ? undefined : estimateCostUsd({model:event.model,
      inputTokens:remaining.inputTokens,outputTokens:remaining.outputTokens,
      cacheReadTokens:remaining.cacheReadTokens,cacheCreationTokens:remaining.cacheCreationTokens})?.costUsd;
  }
  const covered = tokensCovered && (remaining.costUsd ?? 0) === 0;
  if (commit && raw) {
    db.exec(`create table if not exists codex_response_coverage (
      raw_rowid integer not null,raw_id text not null,raw_created text not null,raw_generation text not null,
      owner_rowid integer not null,owner_id text not null,owner_created text not null,owner_generation text not null,
      native_id text not null,original_event_json text not null,
      primary key(raw_rowid,raw_id,raw_created,raw_generation)
    ); create index if not exists idx_codex_response_owner on codex_response_coverage
      (owner_rowid,owner_id,owner_created,owner_generation,native_id);`);
    db.prepare(`insert or ignore into codex_response_coverage values (?,?,?,?,?,?,?,?,?,?)`)
      .run(raw.rowid,raw.id,raw.created,raw.generation,row.rowid,row.id,row.created,row.generation,
        nativeId,JSON.stringify(event));
  }
  return {covered,remaining,ownerId:row.id};
}

/** Called under append's writer, after exact span pairing. Frozen owners
 * keep their ID/bytes; only the newly admitted twin's unsealed counters move. */
export function applyCodexResponseCoverage(db: Database.Database, rawId: string, freeze: (id: string) => boolean) {
  const raw = db.prepare(`select ${columns} from buffered_events e where e.id=?`).get(rawId) as Row | undefined;
  if (!raw || raw.duplicate || frozenCodexCapture(db,rawId)) return false;
  if (originalCoveredResponse(db,rawId)) return false;
  const event = JSON.parse(raw.payload) as AiInteractionEvent;
  const coverage = codexResponseCoverage(db,event,freeze,raw);
  if (!coverage) return false;
  const e = coverage.remaining;
  db.prepare(`update buffered_events set payload_json=?,event_type=?,usage_duplicate_reason=?,
    input_tokens=?,output_tokens=?,cache_read_tokens=?,cache_creation_tokens=?,cost_usd=? where id=?`)
    .run(coverage.covered ? raw.payload : JSON.stringify(e),coverage.covered ? "otel_span" : event.eventType,
      coverage.covered ? CODEX_RESPONSE_DUPLICATE : null,
      coverage.covered ? null : e.inputTokens ?? null,coverage.covered ? null : e.outputTokens ?? null,
      coverage.covered ? null : e.cacheReadTokens ?? null,coverage.covered ? null : e.cacheCreationTokens ?? null,
      coverage.covered ? null : e.costUsd ?? null,rawId);
  const queues = db.prepare(`select delivery_id as id,base_envelope_json as payload from upload_outbox
    where raw_rowid=? and raw_id=? and raw_created_at=? and raw_generation=? and sealed_envelope_json is null`)
    .all(raw.rowid,raw.id,raw.created,raw.generation) as Array<{id:string;payload:string}>;
  for (const queue of queues) {
    if (coverage.covered) db.prepare("delete from upload_outbox where delivery_id=? and sealed_envelope_json is null").run(queue.id);
    else {
      const base = JSON.parse(queue.payload);
      const sealed = sealOutboundEnvelope({...base,event:{...e,id:base.event.id}});
      if (!sealed.ok) throw new Error("codex_response_remainder_seal_refused");
      const bytes = JSON.stringify(sealed.envelope);
      db.prepare(`update upload_outbox set base_envelope_json=?,base_bytes=?
        where delivery_id=? and sealed_envelope_json is null`).run(bytes,Buffer.byteLength(bytes),queue.id);
    }
  }
  return true;
}

export function originalCoveredResponse(db: Database.Database, rawId: string) {
  if (!table(db)) return undefined;
  const row = db.prepare(`select c.original_event_json as payload from codex_response_coverage c
    join buffered_events e on e.rowid=c.raw_rowid and e.id=c.raw_id and e.created_at=c.raw_created
      and e.privacy_generation=c.raw_generation where e.id=?`).get(rawId) as {payload:string} | undefined;
  return row ? JSON.parse(row.payload) as AiInteractionEvent : undefined;
}
