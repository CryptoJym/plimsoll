import type Database from "better-sqlite3";
import { frozenCodexCapture } from "./codex-named-capture";
import { applyCodexResponseCoverage } from "./codex-response-coverage";
import { estimateCostUsd, codexResponseIdentities, codexResponseIdentityOverlap, providerAccountKey, usageFieldKeys, validatedMetadataAttribute,
  type AiInteractionEvent } from "../../shared/src/index";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";

const WINDOW_MS = 30_000;
const MAX_SPAN_MS = 10 * 60_000;
const MAX_ROWS = 128;
export const CODEX_SPAN_ROLLOUT_DUPLICATE = "codex_span_rollout_duplicate";

export function isCodexResponseSpan(event: AiInteractionEvent) {
  return event.source === "codex" && event.metadata.otelEventName === "handle_responses";
}

// A response span is one request, not evidence that live capture covers every
// request in the conversation. This is only the indexable candidate filter;
// response coverage verifies retained financial fields for each Codex row.
// Other sources keep their existing session rule.
export const CODEX_SESSION_AUTHORITY_SQL = `not (source='codex' and
  coalesce(case when json_valid(payload_json) then
    json_extract(payload_json,'$.metadata.otelEventName') end,'')='handle_responses')`;

type Row = {
  rowid: number; id: string; created: string; generation: string | null;
  workspace: string | null; device: string | null; epoch: string | null;
  at: string; uploaded: string | null; input: number; output: number;
  cache: number | null; write: number | null; paired: string | null;
  payload: string; event: AiInteractionEvent;
};
const COLUMNS = `e.rowid,e.id,e.created_at as created,e.privacy_generation as generation,
  e.workspace_id as workspace,e.device_id as device,e.installation_epoch_id as epoch,
  e.observed_at as at,e.uploaded_at as uploaded,e.input_tokens as input,e.output_tokens as output,
  e.cache_read_tokens as cache,e.cache_creation_tokens as write,
  e.usage_paired_event_id as paired,e.payload_json as payload`;
const decode = (row: Omit<Row, "event">): Row => ({ ...row, event: JSON.parse(row.payload) });
const table = (db: Database.Database, name: string, prepare = (sql: string) => db.prepare(sql)) =>
  Boolean(prepare("select 1 from sqlite_master where type='table' and name=?").get(name));
const turn = (event: AiInteractionEvent) => event.metadata.codexTurnId ?? event.metadata["turn.id"] ?? event.metadata.turn_id;
function kind(row: Row): "span" | "rollout" | null {
  if (isCodexResponseSpan(row.event) && row.event.eventType === "assistant_response") return "span";
  if (row.event.eventType === "usage_rollout" && row.event.metadata.usageSource === "rollout" &&
      row.event.metadata.counterLineage === undefined && row.event.sessionId && turn(row.event) &&
      row.event.model && validatedMetadataAttribute("model", row.event.model).accepted) return "rollout";
  return null;
}
const session = (event: AiInteractionEvent) => event.metadata.stitched === "time_window" ? undefined : event.sessionId;
function nativeValues(event: AiInteractionEvent, keys: readonly string[]) {
  const nested = event.metadata.otelAttributes;
  const attributes = nested && typeof nested === "object" && !Array.isArray(nested)
    ? nested as Record<string, unknown> : {};
  return keys.flatMap(key => [event.metadata[key], attributes[key]])
    .filter((value): value is string => typeof value === "string" && !!value.trim());
}
function account(event: AiInteractionEvent) {
  const reported = nativeValues(event, usageFieldKeys.actorId);
  return new Set([...reported, ...reported.map(providerAccountKey), event.actorId].filter(Boolean));
}
function nativeConflict(event: AiInteractionEvent) {
  const models = new Set(nativeValues(event, usageFieldKeys.model));
  const accounts = new Set(nativeValues(event, usageFieldKeys.actorId));
  const reportedAccount = [...accounts][0];
  return event.metadata.modelEvidenceConflict === true || models.size > 1 || accounts.size > 1 ||
    Boolean(event.model && models.size === 1 && !models.has(event.model)) ||
    Boolean(event.actorId && reportedAccount && event.actorId !== reportedAccount &&
      event.actorId !== providerAccountKey(reportedAccount));
}
function compatible(span: Row, rollout: Row) {
  const end = Date.parse(String(span.event.metadata.otelSpanEndAt ?? span.at));
  const start = Date.parse(span.at), at = Date.parse(rollout.at);
  const a = account(span.event), b = account(rollout.event);
  const spanTurn = turn(span.event), rolloutTurn = turn(rollout.event);
  if(codexResponseIdentities(span.event.metadata).length && codexResponseIdentities(rollout.event.metadata).length &&
    !codexResponseIdentityOverlap(span.event.metadata,rollout.event.metadata))return false;
  const nativeModels = new Set(nativeValues(span.event, usageFieldKeys.model));
  return span.workspace !== null && span.epoch !== null && span.device !== null &&
    span.workspace === rollout.workspace && span.epoch === rollout.epoch && span.device === rollout.device &&
    span.input === rollout.input && span.output === rollout.output &&
    (span.cache ?? 0) === (rollout.cache ?? 0) && (span.write ?? 0) === (rollout.write ?? 0) &&
    span.input + span.output > 0 &&
    Number.isFinite(end) && end >= start && end - start <= MAX_SPAN_MS && Math.abs(end - at) <= WINDOW_MS &&
    (!session(span.event) || session(span.event) === session(rollout.event)) &&
    (!spanTurn || !rolloutTurn || spanTurn === rolloutTurn ||
      codexResponseIdentityOverlap(span.event.metadata,rollout.event.metadata)) &&
    (!span.event.metadata.traceId || !rollout.event.metadata.traceId ||
      span.event.metadata.traceId === rollout.event.metadata.traceId) &&
    (a.size === 0 || b.size === 0 || [...a].some(value => b.has(value))) &&
    !nativeConflict(span.event) && !nativeConflict(rollout.event) &&
    nativeModels.size <= 1 && (nativeModels.size === 0 || nativeModels.has(rollout.event.model ?? ""));
}

function ensureSchema(db: Database.Database) {
  db.exec(`create table if not exists codex_span_usage_emissions (
    raw_id text primary key,raw_rowid integer not null,raw_created_at text not null,
    raw_generation text,model text not null
  );
  create table if not exists codex_span_rollout_pairs (
    span_id text primary key,span_rowid integer not null,span_created_at text not null,span_generation text,
    rollout_id text not null unique,rollout_rowid integer not null,rollout_created_at text not null,rollout_generation text,
    owner_id text not null,model text not null,method text not null
  );`);
}

/** The frozen named span remains the owner even after its upload is ACKed.
 * Called in the lease transaction only after schema/privacy validation, or
 * before an explicit history send. It never changes a frozen envelope. */
export function rememberCodexSpanEmission(db: Database.Database, rawId: string, event: AiInteractionEvent) {
  if (db.readonly || !isCodexResponseSpan(event) || !event.model ||
      (event.inputTokens ?? 0) + (event.outputTokens ?? 0) === 0) return;
  ensureSchema(db);
  db.prepare(`insert into codex_span_usage_emissions
    select id,rowid,created_at,privacy_generation,? from buffered_events where id=?
    on conflict(raw_id) do update set raw_rowid=excluded.raw_rowid,raw_created_at=excluded.raw_created_at,
      raw_generation=excluded.raw_generation,model=excluded.model`).run(event.model, rawId);
}

export function codexSpanRolloutDecision(db: Database.Database, rawId: string, prepare = (sql: string) => db.prepare(sql)) {
  if (!table(db, "codex_span_rollout_pairs", prepare)) return undefined;
  return prepare(`select p.owner_id as ownerId,p.model,e.id as eventId
    from codex_span_rollout_pairs p join buffered_events e on
      (e.id=p.span_id and e.rowid=p.span_rowid and e.created_at=p.span_created_at and e.privacy_generation is p.span_generation)
      or (e.id=p.rollout_id and e.rowid=p.rollout_rowid and e.created_at=p.rollout_created_at and e.privacy_generation is p.rollout_generation)
    where e.id=? and (p.span_id=? or p.rollout_id=?) limit 1`).get(rawId, rawId, rawId) as
      { ownerId: string; model: string; eventId: string } | undefined;
}

function emittedSpanModel(db: Database.Database, row: Row): string | undefined {
  if (table(db, "codex_span_usage_emissions")) {
    const emission = db.prepare(`select model from codex_span_usage_emissions where raw_id=?
      and raw_rowid=? and raw_created_at=? and raw_generation is ?`).get(row.id, row.rowid, row.created, row.generation) as
      { model: string } | undefined;
    if (emission) return emission.model;
  }
  // Also respect a sealed pre-upgrade retry whose attempt may have committed.
  const frozen = db.prepare(`select sealed_envelope_json as payload from upload_outbox where raw_id=?
    and raw_rowid=? and raw_created_at=? and raw_generation is ? and sealed_envelope_json is not null limit 1`)
    .get(row.id, row.rowid, row.created, row.generation) as { payload: string } | undefined;
  if (frozen) try {
    const event = JSON.parse(frozen.payload).event as AiInteractionEvent;
    if (event.inputTokens === row.input && event.outputTokens === row.output && event.model) return event.model;
  } catch { /* Unknown frozen bytes cannot attest usage. */ }
  return undefined;
}

function isCaptureGapEnvelope(payload: string | null | undefined) {
  if (!payload) return false;
  try {
    const event = JSON.parse(payload).event as { metadata?: { usageSource?: unknown; captureGap?: unknown } } | undefined;
    return event?.metadata?.usageSource === "capture_gap" || event?.metadata?.captureGap === true;
  } catch {
    return false;
  }
}

/** A response span that already has a durable gap decision is accounting
 * history, not a new pairing candidate. The raw row deliberately retains its
 * counters for diagnostics, while gap envelopes and replay lineage retain the
 * frozen tokenless result. */
export function hasDurableCaptureGap(db: Database.Database, row: Pick<Row, "rowid" | "id" | "created" | "generation">, prepare = (sql: string) => db.prepare(sql)) {
  if (table(db, "codex_capture_decisions", prepare) && prepare(`select 1
      from codex_capture_decisions where raw_rowid=? and raw_id=? and raw_created_at=?
        and raw_generation is ? limit 1`).get(row.rowid, row.id, row.created, row.generation)) return true;
  try {
    if (table(db, "upload_outbox", prepare)) {
      const envelopes = prepare(`select base_envelope_json as base, sealed_envelope_json as sealed
        from upload_outbox where raw_rowid=? and raw_id=? and raw_created_at=? and raw_generation is ?`)
        .all(row.rowid, row.id, row.created, row.generation) as Array<{ base: string; sealed: string | null }>;
      if (envelopes.some(value => isCaptureGapEnvelope(value.sealed) || isCaptureGapEnvelope(value.base))) return true;
    }
    if (table(db, "upload_replays", prepare)) {
      const replays = prepare(`select frozen_envelope_json as frozen
        from upload_replays where raw_rowid=? and raw_id=? and raw_created_at=? and raw_generation is ?`)
        .all(row.rowid, row.id, row.created, row.generation) as Array<{ frozen: string | null }>;
      if (replays.some(value => isCaptureGapEnvelope(value.frozen))) return true;
    }
  } catch {
    // A pre-migration reader has no complete replay shape. Keep the raw
    // pairing decision conservative and let the normal capture path decide.
    return true;
  }
  return false;
}

function nearby(db: Database.Database, row: Row) {
  const end = Date.parse(String(row.event.metadata.otelSpanEndAt ?? row.at));
  if (!Number.isFinite(end)) return [];
  const eligible = terminalPrivacyEligibilitySql(db, "e");
  const found = db.prepare(`select ${COLUMNS} from buffered_events e indexed by idx_events_observed
    where e.source='codex' and e.observed_at>=? and e.observed_at<=?
      and e.input_tokens=? and e.output_tokens=? and e.workspace_id is ?
      and e.device_id is ? and e.installation_epoch_id is ? and ${eligible}
      and e.usage_duplicate_reason is null and e.usage_paired_event_id is null
      and e.id<>? order by e.observed_at,e.id limit ${MAX_ROWS + 1}`)
    .all(new Date(end - MAX_SPAN_MS - WINDOW_MS).toISOString(), new Date(end + WINDOW_MS).toISOString(),
      row.input, row.output, row.workspace, row.device, row.epoch, row.id) as Array<Omit<Row, "event">>;
  if (found.length > MAX_ROWS) return [];
  return found.flatMap(candidate => {
    try {
      const other = decode(candidate);
      const span = kind(row) === "span" ? row : other;
      const rollout = kind(row) === "rollout" ? row : other;
      return kind(span) === "span" && kind(rollout) === "rollout" && compatible(span, rollout) ? [other] : [];
    } catch { return []; }
  });
}

function nativeTraceCompatible(db: Database.Database, span: Row, rollout: Row) {
  const traceId = span.event.metadata.traceId;
  if (typeof traceId !== "string") return true;
  const eligible = terminalPrivacyEligibilitySql(db, "e", { includeUsageDuplicates: true });
  const rows = db.prepare(`select e.payload_json as payload from buffered_events e
    where e.source='codex' and ${eligible}
      and e.workspace_id is ? and e.device_id is ? and e.installation_epoch_id is ?
      and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.traceId') end=?
      `)
    .iterate(span.workspace, span.device, span.epoch, traceId) as Iterable<{ payload: string }>;
  const models = new Set<string>();
  for (const row of rows) {
    const peer = JSON.parse(row.payload) as AiInteractionEvent;
    // Financially ineligible gap peers can still report contradictory native
    // attributes. They must not hide ambiguity from a later pair.
    if (nativeConflict(peer) ||
        (session(peer) && session(peer) !== session(rollout.event))) return false;
    const a = account(peer), b = account(rollout.event);
    if (a.size && b.size && ![...a].some(value => b.has(value))) return false;
    for (const value of nativeValues(peer, usageFieldKeys.model)) {
      models.add(value);
      if (models.size > 1 || !models.has(rollout.event.model ?? "")) return false;
    }
  }
  return models.size === 0 || models.size === 1 && models.has(rollout.event.model ?? "");
}

/** The append caller owns the SQLite writer transaction. Exact marginal
 * counts and completion time must have a mutually unique match. Repeated
 * counts, lineage-first totals, different devices/epochs and near-twins stay
 * unpaired; time proximity by itself never proves response identity. */
export function pairCodexSpanRolloutEvent(db: Database.Database, eventId: string) {
  const raw = db.prepare(`select ${COLUMNS} from buffered_events e where e.id=?`).get(eventId) as
    Omit<Row, "event"> | undefined;
  if (!raw || raw.paired || raw.input === null || raw.output === null) return null;
  const row = decode(raw);
  if (!kind(row)) return null;
  const candidates = nearby(db, row);
  if (candidates.length !== 1) return null;
  const other = candidates[0]!;
  const reciprocal = nearby(db, other);
  if (reciprocal.length !== 1 || reciprocal[0]!.id !== row.id) return null;
  const span = kind(row) === "span" ? row : other;
  const rollout = kind(row) === "rollout" ? row : other;
  const captured = frozenCodexCapture(db,span.id);
  if ((!captured && hasDurableCaptureGap(db, span)) || hasDurableCaptureGap(db, rollout)) return null;
  if (!table(db, "codex_turn_model_evidence")) return null;
  const nativeTurn = db.prepare(`select model,count(distinct nullif(account_key,'')) as accounts
    from codex_turn_model_evidence where workspace_id=? and device_id is ? and installation_epoch_id=?
      and session_id=? and turn_id=? group by model limit 2`).all(
        rollout.workspace, rollout.device, rollout.epoch, rollout.event.sessionId, turn(rollout.event)) as
      Array<{ model: string; accounts: number }>;
  if (nativeTurn.length !== 1 || nativeTurn[0]!.model !== rollout.event.model || nativeTurn[0]!.accounts > 1 ||
      rollout.event.metadata.modelEvidenceConflict === true) return null;
  // Fresh captures still require all native trace facts. An exact twin of
  // a captured named span is deduplication against accounting history; later
  // facts cannot give that already-counted response another financial owner.
  if (!captured && !nativeTraceCompatible(db, span, rollout)) return null;
  const emitted = captured?.event.model ?? emittedSpanModel(db, span);
  // A historical acknowledged span without an emission witness needs the
  // reviewed cloud correction, rather than an invented local ownership claim.
  if (!emitted && span.uploaded) return null;
  if (emitted && emitted !== rollout.event.model) return null;
  if (captured && ["cacheReadTokens","cacheCreationTokens","costUsd"]
    .some(k => rollout.event[k as keyof AiInteractionEvent] !== undefined &&
      captured.event[k as keyof AiInteractionEvent] === undefined)) {
    // Exact response identity does not mean every field was known. Keep
    // the native zero/cost complement while retaining the captured owner.
    return applyCodexResponseCoverage(db,rollout.id,id => Boolean(frozenCodexCapture(db,id)),span.id)
      ? {ownerId:span.id,duplicateId:rollout.id} : null;
  }
  const owner = emitted ? span : rollout;
  const duplicate = emitted ? rollout : span;
  // Accepted twins retain their real local finance even if their old release
  // did not record a named witness. Never conceal two historical deliveries.
  if(duplicate.uploaded||frozenCodexCapture(db,duplicate.id))return null;
  const ownerCapture=frozenCodexCapture(db,owner.id);
  if(ownerCapture&&["cacheReadTokens","cacheCreationTokens","costUsd"].some(k=>
    duplicate.event[k as keyof AiInteractionEvent]!==undefined&&ownerCapture.event[k as keyof AiInteractionEvent]===undefined))
    return applyCodexResponseCoverage(db,duplicate.id,id=>Boolean(frozenCodexCapture(db,id)),owner.id)
      ?{ownerId:owner.id,duplicateId:duplicate.id}:null;
  ensureSchema(db);
  db.prepare(`insert into codex_span_rollout_pairs values (?,?,?,?,?,?,?,?,?,?,?)`).run(
    span.id, span.rowid, span.created, span.generation, rollout.id, rollout.rowid, rollout.created, rollout.generation,
    owner.id, rollout.event.model!, "unique_exact_marginal_completion/v1");
  db.prepare(`update buffered_events set usage_paired_event_id=?,usage_duplicate_reason=?,
    event_type='otel_span',input_tokens=null,output_tokens=null,cache_read_tokens=null,
    cache_creation_tokens=null,cost_usd=null where id=?`).run(owner.id, CODEX_SPAN_ROLLOUT_DUPLICATE, duplicate.id);
  const price = estimateCostUsd({ model: rollout.event.model, inputTokens: owner.input, outputTokens: owner.output,
    cacheReadTokens: owner.cache ?? 0, cacheCreationTokens: owner.write ?? 0 });
  if(owner.uploaded||ownerCapture)db.prepare("update buffered_events set usage_paired_event_id=? where id=?").run(duplicate.id,owner.id);
  else db.prepare(`update buffered_events set usage_paired_event_id=?,model=?,cost_usd=coalesce(cost_usd,?),
    cost_kind=case when cost_usd is null and ? is not null then 'estimated' else cost_kind end where id=?`)
    .run(duplicate.id, rollout.event.model!, price?.costUsd ?? null, price?.costUsd ?? null, owner.id);
  // An attempted named span was selected as owner above. No frozen usage is
  // rewritten or deleted; a loser that has not been sealed can leave the queue.
  db.prepare(`delete from upload_outbox where raw_id=? and raw_rowid=? and raw_created_at=?
    and raw_generation is ? and sealed_envelope_json is null and state in ('pending','retry')`)
    .run(duplicate.id, duplicate.rowid, duplicate.created, duplicate.generation);
  return { ownerId: owner.id, duplicateId: duplicate.id };
}
