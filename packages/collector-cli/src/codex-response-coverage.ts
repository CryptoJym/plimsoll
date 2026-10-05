import type Database from "better-sqlite3";
import { providerAccountKey, usageFieldKeys, type AiInteractionEvent } from "../../shared/src/index";
import { captureCodexModel, codexHasUsage, isCaptureGap } from "./codex-model-capture";
import { frozenCodexCapture } from "./codex-named-capture";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";
import { sealOutboundEnvelope } from "./outbound-envelope";

export const CODEX_RESPONSE_DUPLICATE = "codex_response_covered";
const FIELDS = ["inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens", "costUsd"] as const;
// A history slice probes the same fixed SQL for each native counter. Keep
// compilation and its allocation off the writer loop; the bounded cache is
// connection-local and SQLite reparses statements after schema changes.
const statementCaches = new WeakMap<Database.Database,Map<string,Database.Statement>>();
function statement(db: Database.Database, sql: string) {
  let cache=statementCaches.get(db);
  if (!cache) {cache=new Map();statementCaches.set(db,cache);}
  const cached=cache.get(sql);if(cached)return cached;
  if(cache.size>=32)cache.clear();
  const prepared=db.prepare(sql);cache.set(sql,prepared);return prepared;
}
type Row = { rowid: number; id: string; created: string; generation: string; workspace: string;
  device: string; epoch: string; payload: string; duplicate: string | null };
const columns = `e.rowid,e.id,e.created_at as created,e.privacy_generation as generation,
  e.workspace_id as workspace,e.device_id as device,e.installation_epoch_id as epoch,
  e.payload_json as payload,e.usage_duplicate_reason as duplicate`;
const turn = (e: AiInteractionEvent) => e.metadata.codexTurnId ?? e.metadata["turn.id"] ?? e.metadata.turn_id;
const request = (e: AiInteractionEvent) => e.metadata.request_id ?? e.metadata.call_id;
const key = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const explicitIdentity = (e: AiInteractionEvent) => key(request(e)) || key(turn(e));
function sameResponse(a: AiInteractionEvent, b: AiInteractionEvent) {
  const ar=request(a),br=request(b),at=turn(a),bt=turn(b);
  return !(key(ar)&&key(br)&&ar!==br || key(at)&&key(bt)&&at!==bt) &&
    (key(ar)&&ar===br || key(at)&&at===bt);
}
const native = (e: AiInteractionEvent) => e.eventType === "usage_rollout" && e.metadata.usageSource === "rollout";
const sse = (e: AiInteractionEvent) => e.metadata.otelEventName === "codex.sse_event";
const span = (e: AiInteractionEvent) => e.metadata.otelEventName === "handle_responses";
const otlp = (e: AiInteractionEvent) => sse(e) || span(e);
const completion = (e: AiInteractionEvent) => span(e) ? e.metadata.otelSpanEndAt : e.observedAt;
const table = (db: Database.Database) => Boolean(statement(db,
  "select 1 from sqlite_master where type='table' and name='codex_response_coverage'").get());

function accounts(e: AiInteractionEvent) {
  const attrs = e.metadata.otelAttributes as Record<string, unknown> | undefined;
  return new Set([e.actorId, ...usageFieldKeys.actorId.flatMap(k =>
    [e.metadata[k], attrs?.[k]])].filter((v): v is string => typeof v === "string" && !!v)
    .map(v => v.startsWith("sha256:") ? v : providerAccountKey(v)));
}

function compatible(a: AiInteractionEvent, b: AiInteractionEvent, exactPair = false) {
  const explicit = sameResponse(a,b);
  // Independently identified responses cannot acquire a common identity
  // merely because their amounts/time happen to match. Anonymous established
  // exact twins remain eligible under the existing boundary checks below.
  if(!explicit&&explicitIdentity(a)&&explicitIdentity(b))return false;
  if (a.source !== "codex" || b.source !== "codex" ||
      (a.sessionId && b.sessionId && a.sessionId !== b.sessionId) ||
      (!exactPair && (!a.sessionId || !b.sessionId)) ||
      a.metadata.stitched === "time_window" || b.metadata.stitched === "time_window" ||
      a.metadata.counterLineage || b.metadata.counterLineage ||
      !(native(a) && otlp(b) || otlp(a) && native(b) || sse(a) && otlp(b) || otlp(a) && sse(b) ||
        explicit && (native(a)&&native(b) || otlp(a)&&otlp(b)))) return false;
  if (!explicit && !exactPair && otlp(a) && otlp(b) && (!a.metadata.traceId || a.metadata.traceId !== b.metadata.traceId)) return false;
  const requestA=request(a),requestB=request(b);
  if (requestA && requestB && requestA !== requestB) return false;
  const at = turn(a), bt = turn(b);
  if (at && bt && at !== bt) return false;
  // Native cumulative counters can retain a complete earlier SSE portion.
  // This is a directional exact-counter match, never a nearest-time choice:
  // a future SSE cannot consume an older native counter. Partial reports
  // without the complete counter signature require the same completion.
  if (!explicit && !exactPair && completion(a) !== completion(b)) {
    if (otlp(a) && otlp(b)) return false;
    const log=otlp(a)?a:b,rollout=native(a)?a:b;
    if (!sse(log) || Date.parse(log.observedAt)>Date.parse(rollout.observedAt) ||
        log.inputTokens===undefined || log.outputTokens===undefined ||
        log.inputTokens!==rollout.inputTokens || log.outputTokens!==rollout.outputTokens ||
        log.inputTokens+log.outputTokens===0) return false;
  }
  const aa = accounts(a), ba = accounts(b);
  if (aa.size > 1 || ba.size > 1 || aa.size && ba.size && ![...aa].some(v => ba.has(v))) return false;
  if (a.metadata.captureAccountHash && b.metadata.captureAccountHash &&
      a.metadata.captureAccountHash !== b.metadata.captureAccountHash) return false;
  // Explicit native response identity precedes amounts and time. An update
  // may report larger, smaller, partial or zero fields at any completion.
  if (explicit) return true;
  const common = FIELDS.filter(k => k !== "costUsd" && a[k] !== undefined && b[k] !== undefined);
  if (!common.length) return otlp(a) && otlp(b) || a.costUsd !== undefined && a.costUsd === b.costUsd;
  return common.every(k => a[k] === b[k]);
}

/** Only retained accounting for this response may reduce another producer.
 * Explicit requests/turns join independently of arrival order and clocks.
 * Each field converges to the maximum response observation, not their sum.
 * Anonymous exact-counter joins still consume a witness once per counter. */
export function codexResponseCoverage(db: Database.Database, event: AiInteractionEvent,
  commit?: (rawId: string) => boolean, raw?: Row, exactPeerId?: string) {
  if (event.source !== "codex" || !codexHasUsage(event) || !(native(event) || otlp(event)) ||
      (!event.sessionId && !exactPeerId) || event.metadata.counterLineage) return undefined;
  let binding: Pick<Row, "workspace" | "device" | "epoch"> | undefined;
  try {
    binding = raw ?? statement(db,`select current_workspace_id as workspace,current_device_id as device,
      current_installation_epoch_id as epoch from collector_workspace_binding where singleton=1`).get() as typeof binding;
  } catch (error) {
    if (error instanceof Error && /no such (table|column)/.test(error.message)) return undefined;
    throw error;
  }
  if (!binding?.epoch || !binding.device) return undefined;
  const eligible = terminalPrivacyEligibilitySql(db, "e");
  const at = completion(event);
  if (typeof at !== "string" || !Number.isFinite(Date.parse(at))) return undefined;
  const peerCompletion = "case when json_extract(e.payload_json,'$.metadata.otelEventName')='handle_responses' then json_extract(e.payload_json,'$.metadata.otelSpanEndAt') else e.observed_at end";
  const otlpPredicate = "json_extract(e.payload_json,'$.metadata.otelEventName') in ('codex.sse_event','handle_responses')";
  const nativePredicate = "json_extract(e.payload_json,'$.metadata.usageSource')='rollout'";
  let rows: Row[];
  if (!exactPeerId && explicitIdentity(event)) {
    // Page the proven identity before consulting mutable residuals. A fixed
    // candidate cap must not turn the 129th update into additional usage.
    const matches: string[]=[],values: string[]=[];
    // Covered native deltas remain part of the response's source prefix.
    // They cannot own another delivery, but excluding them from this read
    // makes the next marginal look like the entire response and loses it.
    const identityEligible=terminalPrivacyEligibilitySql(db,"e",{includeUsageDuplicates:true});
    if(key(request(event))) {matches.push("coalesce(json_extract(e.payload_json,'$.metadata.request_id'),json_extract(e.payload_json,'$.metadata.call_id'))=?");values.push(request(event) as string);}
    if(key(turn(event))) {matches.push("coalesce(json_extract(e.payload_json,'$.metadata.codexTurnId'),json_extract(e.payload_json,'$.metadata.\"turn.id\"'),json_extract(e.payload_json,'$.metadata.turn_id'))=?");values.push(turn(event) as string);}
    const query=`select ${columns} from buffered_events e where e.source='codex' and e.session_id=?
      and e.id<>? and e.workspace_id=? and e.device_id=? and e.installation_epoch_id=? and ${identityEligible}
      and (${nativePredicate} or ${otlpPredicate}) and (${matches.join(' or ')})
      and (e.usage_duplicate_reason is null or ${nativePredicate})
      and (e.input_tokens is not null or e.output_tokens is not null or e.cache_read_tokens is not null
        or e.cache_creation_tokens is not null or e.cost_usd is not null or ${nativePredicate})
      and e.rowid>? order by e.rowid limit 128`;
    rows=[];let cursor=0;
    for(;;) {
      const page=statement(db,query).all(event.sessionId,event.id,binding.workspace,binding.device,binding.epoch,...values,cursor) as Row[];
      rows.push(...page);if(page.length<128)break;cursor=page[page.length-1]!.rowid;
    }
    // Keep the established exact anonymous twin join as well. It cannot
    // supply identity to a different explicit response or an earlier turn.
  } else rows=[];
  const exactRows = statement(db,`select ${columns} from buffered_events e
    where e.source='codex' and ${exactPeerId ? '1' : 'e.session_id=?'} and e.id<>? and e.workspace_id=?
      and e.device_id=? and e.installation_epoch_id=? and e.usage_duplicate_reason is null
      and ${native(event) ? otlpPredicate
        : "((e.event_type='usage_rollout' and json_extract(e.payload_json,'$.metadata.usageSource')='rollout') or " + otlpPredicate + ")"}
      and ${eligible} and ${exactPeerId ? "e.id=?" : native(event)
        ? `${peerCompletion}<=? and (${peerCompletion}=? or (e.input_tokens=? and e.output_tokens=?))`
        : `((e.event_type='usage_rollout' and e.observed_at>=? and (e.observed_at=? or (e.input_tokens=? and e.output_tokens=?))) or (${otlpPredicate} and ${peerCompletion}=?))`}
      and (e.input_tokens is not null or e.output_tokens is not null or e.cache_read_tokens is not null
        or e.cache_creation_tokens is not null or e.cost_usd is not null) limit 129`)
    .all(...(exactPeerId ? [] : [event.sessionId]),event.id,binding.workspace,binding.device,binding.epoch,
      ...(exactPeerId ? [exactPeerId] : [at,at,event.inputTokens ?? null,event.outputTokens ?? null,
        ...(native(event)?[]:[at])])) as Row[];
  if (exactRows.length > 128 && !rows.length) return undefined;
  for(const row of exactRows)if(!rows.some(r=>r.rowid===row.rowid))rows.push(row);
  const originals=new Map(rows.map(row=>[row.id,
    originalCoveredResponse(db,row.id)??JSON.parse(row.payload) as AiInteractionEvent]));
  const nativeObservations=[...(native(event)?[event]:[]),...[...originals.values()].filter(native)];
  const nativeAmount=(basis: AiInteractionEvent,field: typeof FIELDS[number]) =>
    nativeObservations.reduce((sum,observation)=>sum+
      (sameResponse(basis,observation)&&compatible(basis,observation)?observation[field]??0:0),0);
  const observations = rows.flatMap(row => {
    const peer=JSON.parse(row.payload) as AiInteractionEvent;
    const identity=originals.get(row.id)!;
    if(!compatible(event,identity,row.id===exactPeerId))return [];
    if(native(event)&&!sameResponse(event,identity)&&completion(event)!==completion(identity)) {
      // A diagnostic native counter can already identify this anonymous SSE
      // at its exact completion, even before a lease records its reservation.
      // Its missing model cannot let the SSE consume a different future turn.
      const anchored=statement(db,`select e.id,e.payload_json as payload from buffered_events e
        where e.source='codex' and e.session_id=? and e.id<>? and e.observed_at=?
          and e.workspace_id=? and e.device_id=? and e.installation_epoch_id=?
          and json_extract(e.payload_json,'$.metadata.usageSource')='rollout'
          and ${terminalPrivacyEligibilitySql(db,"e",{includeUsageDuplicates:true})} limit 129`)
        .all(event.sessionId,event.id,completion(identity),binding.workspace,binding.device,binding.epoch) as Array<{id:string;payload:string}>;
      if(anchored.length>128||anchored.some(other=>{
        const original=originalCoveredResponse(db,other.id)??JSON.parse(other.payload) as AiInteractionEvent;
        return native(original)&&!sameResponse(event,original)&&compatible(identity,original);
      }))return [];
    }
    const captured=frozenCodexCapture(db,row.id)?.event??captureCodexModel(db,peer,row.id,false,false);
    if(isCaptureGap(captured)||!captured.model||event.model&&event.model!==captured.model&&!sameResponse(event,identity))return [];
    return [{row,identity,captured}];
  });
  const budget={...event};
  if(native(event)&&explicitIdentity(event)) {
    // The tailer/history parser gives marginal file counters. Several native
    // counter records in one explicit response add before competing with its
    // cumulative SSE/span observations. Distinct turns never enter this sum.
    for(const field of FIELDS)if(event[field]!==undefined)
      budget[field]=nativeAmount(event,field);
  }
  const candidates = observations.flatMap(({row,identity,captured}) => {
    if(row.duplicate&&!frozenCodexCapture(db,row.id)||!codexHasUsage(captured))return [];
    const nativeId = native(event) ? event.id : native(identity) ? row.id :
      `otel:${event.sessionId}:${event.metadata.traceId}:${at}`;
    const contributors = [{row,captured}];
    // A partial twin can own only the remainder. Follow its accounting
    // ancestry so a later complete report consumes all retained fields once.
    // The full raw incarnation, boundary and privacy checks hold at each hop.
    while (table(db)) {
      const last = contributors[contributors.length-1]!.row;
      if (native(event) && !sameResponse(event,identity) && statement(db,`select 1 from codex_response_coverage
        where owner_rowid=? and owner_id=? and owner_created=? and owner_generation=?
          and native_id<>? and native_id not like 'otel:%'`)
        .get(last.rowid,last.id,last.created,last.generation,nativeId)) return [];
      const parent = statement(db,`select ${columns} from codex_response_coverage c join buffered_events e
        on e.rowid=c.owner_rowid and e.id=c.owner_id and e.created_at=c.owner_created
          and e.privacy_generation=c.owner_generation
        where c.raw_rowid=? and c.raw_id=? and c.raw_created=? and c.raw_generation=? and ${eligible}
          and e.workspace_id=? and e.device_id=? and e.installation_epoch_id=?`)
        .get(last.rowid,last.id,last.created,last.generation,row.workspace,row.device,row.epoch) as Row|undefined;
      if (!parent) break;
      if(parent.id===event.id)break;
      // Proven identities read every retained owner directly. Its ancestry
      // must not impose the legacy 16-hop bound on a long update sequence.
      const parentIdentity=originalCoveredResponse(db,parent.id)??JSON.parse(parent.payload) as AiInteractionEvent;
      if(sameResponse(event,identity)&&sameResponse(event,parentIdentity))break;
      if (contributors.length>=16 || contributors.some(c=>c.row.rowid===parent.rowid&&c.row.id===parent.id)) return [];
      const value = frozenCodexCapture(db,parent.id)?.event ??
        captureCodexModel(db,JSON.parse(parent.payload),parent.id,false,false);
      if (isCaptureGap(value) || !codexHasUsage(value) || value.model !== captured.model&&!sameResponse(event,identity)) return [];
      contributors.push({row:parent,captured:value});
    }
    return [{row,captured,nativeId,contributors,identity,root:contributors[contributors.length-1]!.row.id}];
  });
  if (!candidates.length) return undefined;
  // Prefer proven response identity, then the established exact completion.
  // A weaker directional equal-counter fallback must not mix a different
  // future turn into either set. This is exact equality, never nearest time.
  const identified=candidates.filter(c=>sameResponse(event,c.identity));
  const aligned=candidates.filter(c=>completion(c.identity)===at);
  let owners=identified.length?identified:aligned.length?aligned:candidates;
  const explicit=identified.length>0;
  if (!explicit && new Set(owners.map(c=>c.root)).size !== 1) {
    // A released writer may leave several still-unsealed producers for one
    // response. They are not separate paid roots. Require mutual native
    // identity, then retain the one already frozen root, or the first raw
    // producer. Each other producer is reconciled before its own lease.
    if (!owners.every(a => owners.every(b => a===b ||
      compatible(originalCoveredResponse(db,a.row.id) ?? JSON.parse(a.row.payload),
        originalCoveredResponse(db,b.row.id) ?? JSON.parse(b.row.payload))))) return undefined;
    const paid = owners.filter(c => c.contributors.some(value => frozenCodexCapture(db,value.row.id)));
    if (new Set(paid.map(c=>c.root)).size > 1) return undefined;
    owners = paid.length ? paid : [...owners].sort((a,b)=>a.row.rowid-b.row.rowid).slice(0,1);
  }
  const {captured,nativeId,contributors: ancestry} = owners.sort((a,b)=>b.contributors.length-a.contributors.length)[0]!;
  let contributors=ancestry;
  if(explicit) {
    // Sum retained deltas across ALL accounting branches of the response.
    // Frozen/ACKed owners are final. Among unsealed released producers retain
    // one first owner; each other row is reconciled before its own lease.
    const unique=new Map<string,typeof ancestry[number]>();
    for(const owner of owners)for(const c of owner.contributors)
      unique.set(JSON.stringify([c.row.rowid,c.row.id,c.row.created,c.row.generation]),c);
    const all=[...unique.values()];
    const frozen=all.filter(c=>frozenCodexCapture(db,c.row.id));
    const unsealed=all.filter(c=>!frozenCodexCapture(db,c.row.id)).sort((a,b)=>a.row.rowid-b.row.rowid);
    const retained=Object.fromEntries(FIELDS.map(k=>[k,frozen.reduce((n,c)=>n+(c.captured[k]??0),0)]));
    const first=unsealed.find(c=>{
      const original=originals.get(c.row.id)??originalCoveredResponse(db,c.row.id)??JSON.parse(c.row.payload) as AiInteractionEvent;
      // A native marginal belongs to its source response, independently of
      // a smaller incoming SSE. Its own prefix decides whether it can freeze.
      return FIELDS.every(k=>c.captured[k]===undefined || original[k]!==undefined&&c.captured[k]!<=
        Math.max(0,(native(original)&&sameResponse(event,original)?nativeAmount(original,k):original[k]!)-retained[k]!));
    });
    contributors=[...frozen,...(first?[first]:[])];
    if(!contributors.length)return undefined;
  }
  let reservationOnly=false;
  if (raw) {
    const admitted = captureCodexModel(db,event,event.id,false,false);
    // A diagnostic native row can reserve an already-paid exact counter
    // portion without acquiring the owner's model or a financial envelope.
    // Its gap decision and raw native facts stay untouched.
    if (isCaptureGap(admitted)) reservationOnly=native(event);
    else if(admitted.model!==captured.model&&!explicit)return undefined;
    if(isCaptureGap(admitted)&&!reservationOnly)return undefined;
  }
  const remaining = {...budget, metadata: {...event.metadata}};
  for (const k of FIELDS) if (budget[k] !== undefined && contributors.some(c=>c.captured[k] !== undefined))
    remaining[k] = Math.max(0,budget[k]! - contributors.reduce((n,c)=>n+(c.captured[k]??0),0));
  // One native counter may emit only its own marginal. Prefix knowledge can
  // deduplicate it against a cumulative observation; it cannot recover an
  // older gap's counters under this counter's delivery ID.
  if(native(event))for(const k of FIELDS)if(event[k]!==undefined&&remaining[k]!==undefined)
    remaining[k]=Math.min(event[k]!,remaining[k]!);
  const tokensCovered = FIELDS.slice(0,4).every(k => (remaining[k] ?? 0) === 0);
  // Reported and previously estimated costs obey the same field maximum.
  // An absent cost remains absent; a remainder does not invent a new price.
  const aliases = {inputTokens:usageFieldKeys.inputTokens,outputTokens:usageFieldKeys.outputTokens,
    cacheReadTokens:usageFieldKeys.cacheReadTokens,cacheCreationTokens:usageFieldKeys.cacheCreationTokens,
    costUsd:[...usageFieldKeys.costUsd,...usageFieldKeys.estimatedCostUsd]};
  const attrs=remaining.metadata.otelAttributes;
  const adjusted: Record<string,unknown>|undefined=attrs&&typeof attrs==='object'&&!Array.isArray(attrs)
    ? {...attrs}:undefined;
  for(const field of FIELDS)if(remaining[field]!==event[field])for(const key of aliases[field]) {
    if(key in remaining.metadata) {
      if(remaining[field]===undefined)delete remaining.metadata[key];else remaining.metadata[key]=remaining[field];
    }
    if(adjusted&&key in adjusted) {
      if(remaining[field]===undefined)delete adjusted[key];else adjusted[key]=remaining[field];
    }
  }
  if(adjusted)remaining.metadata.otelAttributes=adjusted;
  const amountsCovered = tokensCovered && (remaining.costUsd ?? 0) === 0;
  // An absent field has never attested a reported zero. Keep a named zero
  // remainder for any newly known field instead of hiding its completeness.
  const fieldsKnown = FIELDS.every(k => event[k] === undefined ||
    contributors.some(c => c.captured[k] !== undefined));
  const covered = amountsCovered && fieldsKnown;
  if(reservationOnly&&!amountsCovered)return undefined;
  if (commit && contributors.some(c=>!commit(c.row.id))) return undefined;
  // Keep the first retained root visible to the anonymous once-per-counter
  // guard. A newer zero/complement owner must not hide that its older paid
  // SSE root already covered another native counter with equal amounts.
  const accountingOwner=[...contributors].sort((a,b)=>a.row.rowid-b.row.rowid)[0]!.row;
  if (commit && raw) {
    db.exec(`create table if not exists codex_response_coverage (
      raw_rowid integer not null,raw_id text not null,raw_created text not null,raw_generation text not null,
      owner_rowid integer not null,owner_id text not null,owner_created text not null,owner_generation text not null,
      native_id text not null,original_event_json text not null,
      primary key(raw_rowid,raw_id,raw_created,raw_generation)
    ); create index if not exists idx_codex_response_owner on codex_response_coverage
      (owner_rowid,owner_id,owner_created,owner_generation,native_id);`);
    statement(db,`insert or ignore into codex_response_coverage values (?,?,?,?,?,?,?,?,?,?)`)
      .run(raw.rowid,raw.id,raw.created,raw.generation,accountingOwner.rowid,accountingOwner.id,accountingOwner.created,accountingOwner.generation,
        nativeId,JSON.stringify(event));
  }
  return {covered,remaining,ownerId:accountingOwner.id,reservationOnly};
}

/** exactPeerId is supplied only by the two mutually unique exact-counter
 * pairing routines after their native boundary/completion checks. It carries
 * their established response identity; it never bypasses capture admission.
 * Called under append's writer, after exact span pairing. Frozen owners
 * keep their ID/bytes; only the newly admitted twin's unsealed counters move. */
export function applyCodexResponseCoverage(db: Database.Database, rawId: string, freeze: (id: string) => boolean,
  exactPeerId?: string) {
  const raw = statement(db,`select ${columns} from buffered_events e where e.id=?`).get(rawId) as Row | undefined;
  if (!raw || raw.duplicate || frozenCodexCapture(db,rawId)) return false;
  const original=originalCoveredResponse(db,rawId);
  if(original&&!explicitIdentity(original))return false;
  // Unsealed older remainders may overlap a subsequently captured branch.
  // Reconcile their original observation again before freezing; never use
  // the already-reduced payload as a new response amount.
  const event = original??JSON.parse(raw.payload) as AiInteractionEvent;
  const coverage = codexResponseCoverage(db,event,freeze,raw,exactPeerId);
  if (!coverage) return false;
  if (coverage.reservationOnly) return false;
  const e = coverage.remaining;
  statement(db,`update buffered_events set payload_json=?,event_type=?,usage_duplicate_reason=?,
    input_tokens=?,output_tokens=?,cache_read_tokens=?,cache_creation_tokens=?,cost_usd=? where id=?`)
    .run(coverage.covered ? raw.payload : JSON.stringify(e),coverage.covered ? "otel_span" : event.eventType,
      coverage.covered ? CODEX_RESPONSE_DUPLICATE : null,
      coverage.covered ? null : e.inputTokens ?? null,coverage.covered ? null : e.outputTokens ?? null,
      coverage.covered ? null : e.cacheReadTokens ?? null,coverage.covered ? null : e.cacheCreationTokens ?? null,
      coverage.covered ? null : e.costUsd ?? null,rawId);
  const queues = statement(db,`select delivery_id as id,base_envelope_json as payload from upload_outbox
    where raw_rowid=? and raw_id=? and raw_created_at=? and raw_generation=? and sealed_envelope_json is null`)
    .all(raw.rowid,raw.id,raw.created,raw.generation) as Array<{id:string;payload:string}>;
  for (const queue of queues) {
    if (coverage.covered) statement(db,"delete from upload_outbox where delivery_id=? and sealed_envelope_json is null").run(queue.id);
    else {
      const base = JSON.parse(queue.payload);
      const sealed = sealOutboundEnvelope({...base,event:{...e,id:base.event.id}});
      if (!sealed.ok) throw new Error("codex_response_remainder_seal_refused");
      const bytes = JSON.stringify(sealed.envelope);
      statement(db,`update upload_outbox set base_envelope_json=?,base_bytes=?
        where delivery_id=? and sealed_envelope_json is null`).run(bytes,Buffer.byteLength(bytes),queue.id);
    }
  }
  return true;
}

export function originalCoveredResponse(db: Database.Database, rawId: string) {
  if (!table(db)) return undefined;
  const row = statement(db,`select c.original_event_json as payload from codex_response_coverage c
    join buffered_events e on e.rowid=c.raw_rowid and e.id=c.raw_id and e.created_at=c.raw_created
      and e.privacy_generation=c.raw_generation where e.id=?`).get(rawId) as {payload:string} | undefined;
  return row ? JSON.parse(row.payload) as AiInteractionEvent : undefined;
}
