import type Database from "better-sqlite3";
import {
  estimateCostUsd,
  codexResponseIdentities,
  codexResponseIdentityOverlap,
  providerAccountKey,
  usageFieldKeys,
  validatedMetadataAttribute,
  type AiInteractionEvent,
} from "../../shared/src/index";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";
import { codexSpanRolloutDecision, hasDurableCaptureGap, isCodexResponseSpan } from "./codex-span-rollout-pairing";
import { frozenCodexCapture, legacyNativeAcknowledgementsEligible } from "./codex-named-capture";

export const CODEX_MODEL_WAIT_MS = 60_000;
const WINDOW_MS = 10 * 60_000;
const MAX_EVIDENCE_ROWS = 128;
const text = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() ? value : undefined;
// A conservative indexed subset of independentLinkedResponse's native
// conversation boundary. Uncertain representations stay in the zero bucket.
// Keep the expression identical in the index and the qualified query.
const linkedTrace = "json_extract(payload_json,'$.metadata.traceId')";
const linkedSession = "json_extract(payload_json,'$.sessionId')";
const linkedProducer = `coalesce(json_extract(payload_json,'$.metadata."conversation.id"'),
  json_extract(payload_json,'$.metadata.otelAttributes."conversation.id"'))`;
export const CODEX_NATIVE_LINKED_SCOPE_SQL = `case when json_valid(payload_json) then
  case when typeof(${linkedTrace})='text' and ${linkedTrace} glob '*[A-Za-z0-9]*'
    and typeof(${linkedSession})='text' and ${linkedSession} glob '*[A-Za-z0-9]*'
    and ${linkedSession}=session_id and json_extract(payload_json,'$.metadata.stitched') is not 'time_window'
    and typeof(${linkedProducer})='text' and ${linkedProducer}=${linkedSession}
  then 1 else 0 end else 0 end`;
const linkedScopeQuery = CODEX_NATIVE_LINKED_SCOPE_SQL.replaceAll("payload_json","e.payload_json")
  .replaceAll("session_id","e.session_id");
/** A capture gap is a durable accounting decision. Its raw counters remain
 * local diagnostics and must never become usage again on a later read. */
export function isCaptureGap(event: AiInteractionEvent): boolean {
  return event.metadata?.usageSource === "capture_gap" || event.metadata?.captureGap === true;
}
export function codexHasUsage(event: AiInteractionEvent): boolean {
  // Native runtime intervals deliberately carry no per-request model and are
  // unqualified observers; cloud excludes them from financial usage.
  return (
    !isCaptureGap(event) &&
    event.eventType !== "usage_live" &&
    (event.source === "codex" || codexMisfiledUnderClaude(event)) &&
    ([
      event.inputTokens,
      event.outputTokens,
      event.cacheReadTokens,
      event.cacheCreationTokens,
    ].some((value) => value !== undefined) ||
      event.costUsd !== undefined)
  );
}

function nestedOtelAttributes(event: AiInteractionEvent): Record<string, unknown> {
  const value = event.metadata?.otelAttributes;
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function valuesForAliases(
  event: AiInteractionEvent,
  aliases: readonly string[],
): string[] {
  return [
    ...aliases.map((key) => event.metadata?.[key]),
    ...aliases.map((key) => nestedOtelAttributes(event)[key]),
  ].filter(
    (value): value is string => typeof value === "string" && !!value.trim(),
  );
}

/** Model names reported by the native producer attributes.  `event.model` is
 * deliberately excluded: 0.7.48 maintenance could write a nearest model
 * into that field after capture, and that value has no provenance. */
function nativeModels(event: AiInteractionEvent): Set<string> {
  return new Set(valuesForAliases(event, usageFieldKeys.model));
}

function nativeModel(event: AiInteractionEvent): string | undefined {
  const models = nativeModels(event);
  return models.size === 1 ? [...models][0] : undefined;
}

function modelAttributeConflict(event: AiInteractionEvent): boolean {
  const model = text(event.model);
  const native = nativeModel(event);
  return Boolean(model && native && model !== native);
}

function metadataAccounts(event: AiInteractionEvent): Set<string> {
  return new Set(valuesForAliases(event, usageFieldKeys.actorId));
}

/**
 * Producer account metadata is already a protected hash, while actorId may be
 * the protected hash of that metadata value. Keep both representations as
 * evidence for joins, but only call an event internally conflicting when its
 * explicit metadata disagrees or actorId is unrelated to its one account.
 */
function nativeAccounts(event: AiInteractionEvent): Set<string> {
  const metadata = metadataAccounts(event);
  const evidence = new Set(metadata);
  for (const value of metadata) evidence.add(providerAccountKey(value));
  if (text(event.actorId)) evidence.add(event.actorId!);
  return evidence;
}

function accountConflict(event: AiInteractionEvent): boolean {
  const metadata = metadataAccounts(event);
  if (metadata.size > 1) return true;
  const account = [...metadata][0];
  return Boolean(
    event.actorId && account &&
      event.actorId !== account && event.actorId !== providerAccountKey(account),
  );
}

/** A pre-witness binary can freeze a producer's explicit model, or a guess.
 * Only self-contained native provenance can establish the former without
 * re-evaluating later peers. A derived/bare model never qualifies here. */
export function legacyFrozenNativeCapture(raw: AiInteractionEvent, frozen: AiInteractionEvent): boolean {
  if (raw.source !== "codex" || isCaptureGap(raw) || raw.metadata.modelCaptureSource ||
      raw.metadata.modelEvidenceConflict === true || accountConflict(raw) ||
      nativeModels(raw).size > 1 || modelAttributeConflict(raw) || !sameCounts(raw,frozen) ||
      (raw.costUsd !== undefined && raw.costUsd !== frozen.costUsd)) return false;
  const reported = nativeModel(raw);
  const nativeRequest = raw.metadata.otelEventName === "codex.sse_event" ||
    (isCodexResponseSpan(raw) && text(raw.metadata.traceId));
  const nativeTurn = raw.metadata.usageSource === "rollout" && trustedSession(raw) &&
    text(raw.metadata.codexTurnId) && text(raw.model);
  return Boolean(frozen.model && ((nativeRequest && reported === frozen.model) ||
    (nativeTurn && raw.model === frozen.model)));
}
export function codexMisfiledUnderClaude(event: AiInteractionEvent): boolean {
  return (
    event.source === "claude_code" &&
    /^(codex[-_.]|codex$)/i.test(String(event.metadata?.serviceName ?? ""))
  );
}
export function unresolvedCapture(event: AiInteractionEvent): boolean {
  return (
    codexHasUsage(event) &&
    (!text(event.model) ||
      nativeModels(event).size === 0 ||
      codexMisfiledUnderClaude(event) ||
      event.metadata?.modelEvidenceConflict === true ||
      nativeModels(event).size > 1 ||
      modelAttributeConflict(event))
  );
}
function pairedObservation(event: AiInteractionEvent, reason = "paired_sse_event"): AiInteractionEvent {
  const {
    inputTokens,
    outputTokens,
    cacheReadTokens,
    cacheCreationTokens,
    costUsd: _cost,
    costKind: _kind,
    ...rest
  } = event;
  const metadata = { ...event.metadata };
  for (const key of [
    ...usageFieldKeys.inputTokens,
    ...usageFieldKeys.outputTokens,
    ...usageFieldKeys.cacheReadTokens,
    ...usageFieldKeys.cacheCreationTokens,
    ...usageFieldKeys.costUsd,
    ...usageFieldKeys.estimatedCostUsd,
  ])
    delete metadata[key];
  delete metadata.costEstimated;
  // This exact native log already owns these counts. The span records the
  // captured model, but contributes no second observation to cloud usage.
  return {
    ...rest,
    eventType: "otel_span",
    metadata: {
      ...metadata,
      usageDuplicateReason: reason,
      ...(inputTokens !== undefined
        ? { modelCaptureInputTokens: inputTokens }
        : {}),
      ...(outputTokens !== undefined
        ? { modelCaptureOutputTokens: outputTokens }
        : {}),
      ...(cacheReadTokens !== undefined
        ? { modelCaptureCacheReadTokens: cacheReadTokens }
        : {}),
      ...(cacheCreationTokens !== undefined
        ? { modelCaptureCacheCreationTokens: cacheCreationTokens }
        : {}),
    },
  };
}

type Peer = {
  event: AiInteractionEvent;
  pairedId: string | null;
  lineage?: CaptureLineage;
  duplicate?: boolean;
};
function sameCounts(a: AiInteractionEvent, b: AiInteractionEvent) {
  return (
    a.inputTokens === b.inputTokens &&
    a.outputTokens === b.outputTokens &&
    (a.cacheReadTokens ?? null) === (b.cacheReadTokens ?? null) &&
    (a.cacheCreationTokens ?? null) === (b.cacheCreationTokens ?? null)
  );
}
function trustedSession(event: AiInteractionEvent) {
  return event.metadata.stitched === "time_window"
    ? undefined
    : event.sessionId;
}
function compatible(a: AiInteractionEvent, b: AiInteractionEvent) {
  const as = trustedSession(a),
    bs = trustedSession(b);
  const aa = nativeAccounts(a),
    ba = nativeAccounts(b);
  return (
    (!as || !bs || as === bs) &&
    (!a.metadata.traceId ||
      !b.metadata.traceId ||
      a.metadata.traceId === b.metadata.traceId) &&
    (aa.size === 0 || ba.size === 0 ||
      [...aa].some((value) => ba.has(value)))
  );
}
function unique(
  peers: Peer[],
  key: (event: AiInteractionEvent) => string | undefined,
) {
  return [
    ...new Set(
      peers
        .map((peer) => key(peer.event))
        .filter((value): value is string => Boolean(value)),
    ),
  ];
}
function capture(
  event: AiInteractionEvent,
  peers: Peer[],
  source: string,
): AiInteractionEvent {
  // Trace/SSE and exact-pair evidence must be named by a native producer
  // attribute. Local-turn evidence is the one deliberate exception: the
  // native turn table supplies a model on its synthetic context row.
  const model = unique(
    peers,
    (e) => source === "local_session_turn"
      ? nativeModel(e) ?? text(e.model)
      : nativeModel(e),
  )[0]!;
  const eventAccounts = [...metadataAccounts(event)];
  const accounts = [...new Set(peers.flatMap((peer) => [...metadataAccounts(peer.event)]))];
  const sessions = unique(peers, (e) => trustedSession(e));
  const account =
    eventAccounts[0] ??
    (accounts.length === 1 ? accounts[0] : undefined);
  const sessionId =
    trustedSession(event) ?? (sessions.length === 1 ? sessions[0] : undefined);
  const estimate = estimateCostUsd({
    model,
    inputTokens: event.inputTokens,
    outputTokens: event.outputTokens,
    cacheReadTokens: event.cacheReadTokens,
    cacheCreationTokens: event.cacheCreationTokens,
  });
  return {
    ...event,
    model,
    ...(sessionId ? { sessionId } : {}),
    ...(text(event.actorId) || !account ? {} : { actorId: account }),
    ...(estimate && event.costUsd === undefined
      ? { costUsd: estimate.costUsd, costKind: "estimated" as const }
      : {}),
    metadata: {
      ...event.metadata,
      modelCaptureSource: source,
      ...(account
        ? { "user.account_id": account, accountIdentityState: "reported" }
        : { accountIdentityState: "unavailable" }),
      ...(sessionId && !trustedSession(event)
        ? { sessionLinkBasis: source }
        : {}),
      ...(estimate && event.costUsd === undefined
        ? { costEstimated: true }
        : {}),
    },
  };
}
export function codexModelGap(
  db: Database.Database,
  event: AiInteractionEvent,
  reason: string,
  recordDiagnostics = true,
): AiInteractionEvent {
  const epoch = text(event.metadata.installationEpochId),
    at = Date.parse(event.observedAt);
  if (recordDiagnostics && !db.readonly && epoch && Number.isFinite(at)) {
    db.exec(`create table if not exists codex_model_capture_gaps (
      installation_epoch_id text not null, observed_day integer not null,
      from_ms integer not null, to_ms integer not null,
      primary key(installation_epoch_id,observed_day)) without rowid`);
    db.prepare(
      `insert into codex_model_capture_gaps values (?,?,?,?)
      on conflict(installation_epoch_id,observed_day) do update set
        from_ms=min(from_ms,excluded.from_ms),to_ms=max(to_ms,excluded.to_ms)`,
    ).run(epoch, Math.floor(at / 86400000), at, at);
  }
  const {
    model: _model,
    inputTokens,
    outputTokens,
    cacheReadTokens,
    cacheCreationTokens,
    costUsd: _cost,
    costKind: _kind,
    ...rest
  } = event;
  const metadata = { ...event.metadata };
  // Native aliases are evidence, not another billable token observation.
  for (const key of [
    ...usageFieldKeys.inputTokens,
    ...usageFieldKeys.outputTokens,
    ...usageFieldKeys.cacheReadTokens,
    ...usageFieldKeys.cacheCreationTokens,
    ...usageFieldKeys.model,
    ...usageFieldKeys.costUsd,
    ...usageFieldKeys.estimatedCostUsd,
  ])
    delete metadata[key];
  delete metadata.costEstimated;
  return {
    ...rest,
    eventType: "unknown",
    metadata: {
      ...metadata,
      usageSource: "capture_gap",
      captureGap: true,
      modelGapReason: reason,
      ...(inputTokens !== undefined
        ? { modelGapInputTokens: inputTokens }
        : {}),
      ...(outputTokens !== undefined
        ? { modelGapOutputTokens: outputTokens }
        : {}),
      ...(cacheReadTokens !== undefined
        ? { modelGapCacheReadTokens: cacheReadTokens }
        : {}),
      ...(cacheCreationTokens !== undefined
        ? { modelGapCacheCreationTokens: cacheCreationTokens }
        : {}),
      accountIdentityState: text(
        metadata[
          event.source === "claude_code" && !codexMisfiledUnderClaude(event)
            ? "user.account_uuid"
            : "user.account_id"
        ],
      )
        ? "reported"
        : "unavailable",
    },
  };
}

type CaptureLineage = {
  rawRowid: number;
  rawId: string;
  rawCreatedAt: string;
  rawGeneration: string | null;
};

function ensureCaptureDecisionTable(db: Database.Database) {
  if (!db.readonly) {
    db.exec(`create table if not exists codex_capture_decisions (
      decision_id integer primary key,
      raw_rowid integer not null, raw_id text not null,
      raw_created_at text not null, raw_generation text,
      decision text not null check(decision='gap'),
      reason text not null, decided_at text not null
    );
    create unique index if not exists idx_codex_capture_decisions_lineage
      on codex_capture_decisions(raw_rowid,raw_id,raw_created_at,coalesce(raw_generation,''));`);
  }
}

function captureLineage(db: Database.Database, rawId: string): CaptureLineage | undefined {
  try {
    return db.prepare(`select rowid as rawRowid,id as rawId,created_at as rawCreatedAt,
      privacy_generation as rawGeneration from buffered_events where id=?`).get(rawId) as
      CaptureLineage | undefined;
  } catch { return undefined; }
}

export function hasCaptureGapDecision(db: Database.Database, lineage: CaptureLineage,
  prepare = (sql: string) => db.prepare(sql)): boolean {
  return hasDurableCaptureGap(db, { rowid: lineage.rawRowid, id: lineage.rawId,
    created: lineage.rawCreatedAt, generation: lineage.rawGeneration }, prepare);
}

/** Re-derive existing local facts when native trace facts or a gap change
 * authority. Raw diagnostics and all frozen delivery bytes stay untouched. */
export function queueCodexAuthorityProjectionRepairs(db: Database.Database, rawId: string) {
  if (db.readonly || !db.prepare(`select 1 from sqlite_master
    where type='table' and name='dashboard_projection_repairs'`).get()) return;
  const row = db.prepare(`select rowid as rawRowid,source,session_id as session,observed_at as at,
    workspace_id as workspace,device_id as device,installation_epoch_id as epoch,
    case when json_valid(payload_json) then json_extract(payload_json,'$.metadata.traceId') end as trace
    from buffered_events where id=?`).get(rawId) as
    { rawRowid: number; source: string; session: string | null; at: string; workspace: string; device: string; epoch: string; trace: string | null } | undefined;
  if (!row || row.source !== "codex") return;
  const peers = row.trace ? db.prepare(`select rowid,id,session_id as session from buffered_events
    where source='codex' and workspace_id is ? and device_id is ? and installation_epoch_id is ?
      and case when json_valid(payload_json) then json_extract(payload_json,'$.metadata.traceId') end=?
    limit ${MAX_EVIDENCE_ROWS + 1}`).all(row.workspace,row.device,row.epoch,row.trace) as
      Array<{ rowid: number; id: string; session: string | null }> : [];
  const queue = db.prepare(`insert or ignore into dashboard_projection_repairs (raw_rowid,reason,queued_at)
    values (?,'codex_usage_authority_changed',?)`);
  const at = new Date().toISOString();
  queue.run(row.rawRowid,at);
  for (const peer of peers) queue.run(peer.rowid,at);
  const sessions = new Set([row.session,...peers.map(peer=>peer.session)].filter(Boolean));
  for (const session of sessions) db.prepare(`insert or ignore into dashboard_projection_repairs
    (raw_rowid,reason,queued_at) select e.rowid,'codex_usage_authority_changed',? from buffered_events e
      join dashboard_event_facts f on f.raw_rowid=e.rowid
      where e.source='codex' and e.session_id=? and e.event_type in ('usage_rollout','usage_transcript')`)
      .run(at,session);
  db.prepare(`update dashboard_projection_control set dirty=1,parity_ready=0 where singleton=1`).run();
}

export function rememberCaptureGap(
  db: Database.Database,
  rawId: string,
  reason: string,
  at = new Date(),
) {
  const lineage = captureLineage(db, rawId);
  if (!lineage || db.readonly) return false;
  return rememberCaptureGapForLineage(db,lineage,reason,at);
}

/** A retained delivery may outlive its raw row. Persist its old decision
 * against that complete incarnation, never a later reuse of the same ID. */
export function rememberCaptureGapForLineage(
  db: Database.Database, lineage: CaptureLineage, reason: string, at = new Date(),
) {
  if (db.readonly) return false;
  ensureCaptureDecisionTable(db);
  db.prepare(`insert or replace into codex_capture_decisions
    (raw_rowid,raw_id,raw_created_at,raw_generation,decision,reason,decided_at)
    values (?,?,?,?,?,?,?)`).run(
    lineage.rawRowid, lineage.rawId, lineage.rawCreatedAt, lineage.rawGeneration,
    "gap",
    reason, at.toISOString(),
  );
  const current=captureLineage(db,lineage.rawId);
  if(current && current.rawRowid===lineage.rawRowid && current.rawCreatedAt===lineage.rawCreatedAt &&
      current.rawGeneration===lineage.rawGeneration) queueCodexAuthorityProjectionRepairs(db,lineage.rawId);
  return true;
}

/** Called after the durable 60-second pair wait, immediately before sealing.
 * Time alone never supplies a model or account. Evidence never crosses the
 * ledger's workspace/device/install or a conflicting native session/trace.
 * Each tier must name exactly one model; ambiguity fails closed at that tier.
 */
export function captureCodexModel(
  db: Database.Database,
  event: AiInteractionEvent,
  rawId = event.id,
  persistDecision = false,
  recordDiagnostics = true,
  prepare = (sql: string) => db.prepare(sql),
): AiInteractionEvent {
  // Released readers and raw-ledger migrations can leave metadata absent.
  // An absent producer record supplies no model evidence; it must still go
  // through the tokenless-gap decision rather than crash a repair/lease.
  if (!event.metadata || typeof event.metadata !== "object" || Array.isArray(event.metadata))
    event = { ...event, metadata: {} };
  if (!codexHasUsage(event)) return event;
  let row:
    | {
        workspace: string | null;
        device: string | null;
        epoch: string | null;
        pairedId: string | null;
        rawRowid: number;
        rawId: string;
        rawCreatedAt: string;
        rawGeneration: string | null;
        uploadedAt: string | null;
      }
    | undefined;
  try {
    row = prepare(
        `select rowid as rawRowid,id as rawId,created_at as rawCreatedAt,
      privacy_generation as rawGeneration, workspace_id as workspace, device_id as device,
      installation_epoch_id as epoch, usage_paired_event_id as pairedId,uploaded_at as uploadedAt
      from buffered_events where id=?`,
      )
      .get(rawId) as typeof row;
  } catch (error) {
    // Explicit history uploads also support pre-install-identity ledgers.
    // They remain read-only and cannot invent a native join boundary.
    if (!(error instanceof Error) || !/no such column/.test(error.message))
      throw error;
  }
  if (row?.epoch)
    event = {
      ...event,
      metadata: { ...event.metadata, installationEpochId: row.epoch },
      };
  // An earlier validated native capture is an immutable accounting result.
  // Its raw attributes remain available to contradict OTHER new captures.
  const frozen = row && frozenCodexCapture(db,rawId,prepare);
  if (frozen) return frozen.event;
  const gap = (reason: string) => {
    const result = codexModelGap(db, event, reason, recordDiagnostics);
    if (persistDecision && row && !db.readonly) {
      ensureCaptureDecisionTable(db);
      prepare(`insert or replace into codex_capture_decisions
        (raw_rowid,raw_id,raw_created_at,raw_generation,decision,reason,decided_at)
        values (?,?,?,?,?,?,?)`).run(
        row.rawRowid, row.rawId, row.rawCreatedAt, row.rawGeneration,
        "gap",
        reason, new Date().toISOString(),
      );
      queueCodexAuthorityProjectionRepairs(db, rawId);
    }
    return result;
  };
  if (row && hasCaptureGapDecision(db, row, prepare))
    return gap("persisted_capture_gap");
  // A native retry frozen by an older binary is final even before this
  // binary's first lease. Readers must not consume a rollout against a
  // mutable re-evaluation of that same, already-frozen request.
  if (row && prepare("select 1 from sqlite_master where type='table' and name='upload_outbox'").get()) {
    const prior = prepare(`select delivery_id as id,sealed_envelope_json as bytes from upload_outbox
      where raw_rowid=? and raw_id=? and raw_created_at=? and raw_generation is ?
        and sealed_envelope_json is not null limit 1`).get(row.rawRowid,row.rawId,row.rawCreatedAt,row.rawGeneration) as
      {id:string;bytes:string}|undefined;
    if(prior)try {
      const previous=JSON.parse(prior.bytes).event as AiInteractionEvent;
      if(legacyFrozenNativeCapture(event,previous))return {...previous,
        metadata:{...previous.metadata,modelCaptureSource:"legacy_native_frozen"}};
    }catch { /* Malformed frozen bytes cannot attest a native capture. */ }
  }
  // Released .47/.48 readers billed explicit native SSE models and did not
  // write capture gaps. A ledger that already had a capture-contract schema
  // is deliberately ineligible for this inference: an ACK may be for a gap.
  // Bare/proximity models and conflicting native attributes never qualify.
  if (row?.uploadedAt && event.metadata.otelEventName === "codex.sse_event" &&
      legacyNativeAcknowledgementsEligible(db,prepare) && legacyFrozenNativeCapture(event,event))
    return capture(event,[{event,pairedId:null}],"legacy_native_acknowledged");
  if (nativeModels(event).size > 1 || modelAttributeConflict(event) || accountConflict(event) ||
    event.metadata.modelEvidenceConflict === true)
    return gap("conflicting_model_attributes");
  if (codexMisfiledUnderClaude(event))
    return gap("codex_service_under_claude_source");
  // A saved pair cannot override an accounting gap or contradictory native
  // attributes. In particular the raw diagnostics behind a frozen gap still
  // carry their original counters.
  // Some native exporters put the request model directly on the response
  // span. Treat that as trace evidence only when the event carries a bounded
  // trace id and the native model attribute agrees with any normalized model
  // field the producer supplied. A bare model field is still untrusted, which keeps 0.7.48's
  // reconciliation output tokenless. The evidence is joined below with every
  // native row on that trace so a conflicting session, account, or model can
  // never be hidden by this self-attested value.
  const directTraceId = text(event.metadata.traceId);
  const directTraceModels = [
    ...new Set(valuesForAliases(event, usageFieldKeys.model)),
  ];
  const directTraceModelEvidence = Boolean(
    directTraceId &&
    directTraceModels.length === 1 &&
    (!text(event.model) || directTraceModels[0] === event.model) &&
    nativeModels(event).size === 1,
  );
  const nativeSseEvent =
    event.metadata.otelEventName === "codex.sse_event" &&
    nativeModels(event).size === 1 &&
    !modelAttributeConflict(event);
  const turn = text(event.metadata.codexTurnId) ?? text(event.metadata["turn.id"]) ?? text(event.metadata.turn_id);
  // A rollout without a native turn cannot use a trace-free neighbouring
  // response as provenance. Reject it before any bounded peer scan. This is
  // common in old/model-less history and keeps writer work independent of
  // the number of already-imported diagnostic rows.
  if (event.metadata.usageSource === "rollout" && !turn && !directTraceId)
    return gap("model_evidence_missing");
  // A populated model is not provenance. In particular, 0.7.48 could have
  // written a nearest model into this payload. Only the native pair, trace or
  // local-turn branches below may promote it to a billable event; an event
  // whose model has no such evidence falls through to a tokenless gap.
  if (!row) return gap("capture_row_missing");
  if (!row.workspace || !row.epoch) return gap("capture_identity_missing");
  const at = Date.parse(event.observedAt),
    end = Date.parse(String(event.metadata.otelSpanEndAt ?? event.observedAt));
  if (
    !Number.isFinite(at) ||
    !Number.isFinite(end) ||
    end < at ||
    end - at > WINDOW_MS
  )
    return gap("evidence_window_invalid");
  const eligible = terminalPrivacyEligibilitySql(db, "e", { includeUsageDuplicates: true });
  type EvidenceRow = {
    evidenceRowid: number; evidenceId: string; evidenceCreatedAt: string;
    evidenceGeneration: string | null; payload: string; pairedId: string | null;
    duplicateReason: string | null;
  };
  const decodePeers = (rows: EvidenceRow[]): Peer[] => rows.flatMap((r) => {
    try {
      const parsed = JSON.parse(r.payload) as AiInteractionEvent;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return [];
      const event = parsed.metadata && typeof parsed.metadata === "object" && !Array.isArray(parsed.metadata)
        ? parsed : { ...parsed, metadata: {} };
      return [{ event, pairedId: r.pairedId, duplicate: r.duplicateReason !== null,
        lineage: { rawRowid: r.evidenceRowid, rawId: r.evidenceId,
          rawCreatedAt: r.evidenceCreatedAt, rawGeneration: r.evidenceGeneration } }];
    } catch { return []; }
  });
  const mayPromote = (p: Peer) => !p.duplicate && !p.event.metadata.modelCaptureSource && !isCaptureGap(p.event) &&
    !(p.lineage && hasCaptureGapDecision(db, p.lineage, prepare));
  const selectEvidence = `select e.rowid as evidenceRowid, e.id as evidenceId,
    e.created_at as evidenceCreatedAt, e.privacy_generation as evidenceGeneration,
    e.payload_json as payload, e.usage_paired_event_id as pairedId,
    e.usage_duplicate_reason as duplicateReason from buffered_events e`;
  // Financial custody above is immutable. Every FRESH promotion below,
  // including saved span/rollout pairs, first reads contradiction facts. A
  // veto never supplies a model: its state is one model/account/session and
  // at most the existing 128 identity keys. Rows stream through that state;
  // unrelated tools cannot exhaust the promotion budget or hide later facts.
  const conflicts = (p: Peer) =>
    p.event.metadata.modelEvidenceConflict === true ||
    nativeModels(p.event).size > 1 ||
    modelAttributeConflict(p.event) ||
    accountConflict(p.event);
  const nativeTurnModel = (e: AiInteractionEvent) =>
    trustedSession(e) && text(e.metadata.codexTurnId ?? e.metadata["turn.id"] ?? e.metadata.turn_id) &&
      (e.metadata.usageSource === "rollout" || e.metadata.usageSource === "codex_local_turn")
      ? text(e.model) : undefined;
  const targetNativeModel = nativeModel(event) ?? nativeTurnModel(event);
  const traceId = directTraceId;
  const responsePair = codexSpanRolloutDecision(db, rawId,prepare);
  const tracePeers: Peer[] = [];
  let traceOverflow = false;
  let vetoModel = targetNativeModel;
  let vetoAccounts = nativeAccounts(event);
  let vetoSession = trustedSession(event);
  const veto = (p: Peer, domain: "trace" | "linked" | "local") => {
    const internal = domain === "trace" ? "conflicting_trace_model_evidence" :
      domain === "local" ? "conflicting_local_model_evidence" : "conflicting_linked_model_evidence";
    const modelReason = domain === "trace" ? "ambiguous_trace_model" :
      domain === "local" ? "ambiguous_local_turn_model" : "ambiguous_linked_model";
    const identityReason = domain === "trace" ? "ambiguous_trace_identity" :
      domain === "local" ? "ambiguous_local_turn_identity" : "ambiguous_linked_identity";
    if (conflicts(p)) return internal;
    const model = nativeModel(p.event) ?? nativeTurnModel(p.event);
    if (model && vetoModel && model !== vetoModel) return modelReason;
    if (model) vetoModel = model;
    const accounts = nativeAccounts(p.event), session = trustedSession(p.event);
    if (accounts.size && vetoAccounts.size && ![...accounts].some(value => vetoAccounts.has(value)))
      return identityReason;
    if (session && vetoSession && session !== vetoSession) return identityReason;
    if (accounts.size) vetoAccounts = vetoAccounts.size
      ? new Set([...accounts].filter(value => vetoAccounts.has(value))) : accounts;
    if (session) vetoSession = session;
    return undefined;
  };
  const nativeScope = `e.source='codex' and e.id<>? and e.workspace_id is ?
    and e.device_id is ? and e.installation_epoch_id is ? and ${eligible}`;
  const nativeScopeArgs = [rawId,row.workspace,row.device,row.epoch];
  const vetoTraces = new Set<string>(traceId ? [traceId] : []);
  let pairedSpan: Peer | undefined;
  // A mutable saved rollout owner also depends on its exact span's trace.
  // Follow only the saved span incarnation, never a reused ID or another
  // workspace/device/install. That trace remains a veto after pairing.
  if (responsePair && responsePair.spanId !== rawId) {
    const spanRow = prepare(`${selectEvidence} where ${nativeScope}
      and e.rowid=? and e.id=? and e.created_at=? and e.privacy_generation is ?`)
      .get(...nativeScopeArgs,responsePair.spanRowid,responsePair.spanId,
        responsePair.spanCreatedAt,responsePair.spanGeneration) as EvidenceRow | undefined;
    if (spanRow) {
      const span = decodePeers([spanRow])[0];
      if (span) {
        pairedSpan = span;
        const reason = veto(span,"trace");
        if (reason) return gap(reason);
        const spanTrace = text(span.event.metadata.traceId);
        if (spanTrace) vetoTraces.add(spanTrace);
      }
    }
  }
  let vetoReason: string | undefined;
  traceVeto: for (const vetoTrace of vetoTraces) for (const candidate of prepare(`${selectEvidence} where ${nativeScope}
    and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.traceId') end=?`)
    .iterate(...nativeScopeArgs,vetoTrace) as Iterable<EvidenceRow>) {
    const p = decodePeers([candidate])[0];
    if (!p) continue;
    const reason = veto(p,"trace");
    if (reason) { vetoReason = reason; break traceVeto; }
    if (vetoTrace !== traceId) continue; // veto-only partner facts never promote.
    if (tracePeers.length < MAX_EVIDENCE_ROWS + 1) tracePeers.push(p);
    else traceOverflow = true;
  }
  // Close the SQLite iterator before persisting a capture-gap decision.
  if (vetoReason) return gap(vetoReason);
  if (tracePeers.length > MAX_EVIDENCE_ROWS) traceOverflow = true;
  // Request and call namespaces stay distinct. A native turn is local to
  // its conversation. Co-present aliases expand the exact linked component;
  // a counterless/gap/ACK row remains a fact, never a promotable witness.
  // Trace, typed links and native turn context are cumulative veto sources.
  // A trace never makes an otherwise linked native contradiction irrelevant.
  const independentLinkedResponse = (seed: AiInteractionEvent, candidate: AiInteractionEvent) => {
    const seedTrace = text(seed.metadata.traceId), candidateTrace = text(candidate.metadata.traceId);
    // Contradictory native attributes inside one trace always remain vetoes.
    if (seedTrace && seedTrace === candidateTrace) return false;
    const seedSession = trustedSession(seed), candidateSession = trustedSession(candidate);
    const producerSession = (e: AiInteractionEvent) =>
      text(e.metadata["conversation.id"] ?? nestedOtelAttributes(e)["conversation.id"]);
    // Request/call text is scoped by an independently reported native
    // conversation. Two different native conversations AND traces do not
    // become one response merely because their local request text repeats.
    if (seedTrace && candidateTrace && seedSession && candidateSession && seedSession !== candidateSession &&
        producerSession(seed) === seedSession && producerSession(candidate) === candidateSession)
      return true;
    const seedRequest = text(seed.metadata.request_id), candidateRequest = text(candidate.metadata.request_id);
    const seedAccounts = nativeAccounts(seed), candidateAccounts = nativeAccounts(candidate);
    // A shared call/turn alias cannot merge two explicitly different requests
    // across known disjoint accounts. Same-request or unknown-boundary facts
    // still veto; account disagreement alone never excuses a contradiction.
    return Boolean(seedRequest && candidateRequest && seedRequest !== candidateRequest &&
      !accountConflict(seed) && !accountConflict(candidate) && seedAccounts.size && candidateAccounts.size &&
      ![...seedAccounts].some(value => candidateAccounts.has(value)));
  };
  const checkLinkedFacts = (seed: AiInteractionEvent): string | undefined => {
    const seedSession = trustedSession(seed);
    const seedTrace = text(seed.metadata.traceId);
    const producerSession = text(seed.metadata["conversation.id"] ?? nestedOtelAttributes(seed)["conversation.id"]);
    const scopedConversation = Boolean(seedTrace && seedSession && producerSession === seedSession);
    const pending: ReturnType<typeof codexResponseIdentities> = [];
    const seenKeys = new Set<string>();
    const enqueue = (e: AiInteractionEvent) => {
      for (const node of codexResponseIdentities(e.metadata)) {
        if (node.kind === "turn" && !seedSession) continue;
        const key = node.kind + ":" + node.value;
        if (seenKeys.has(key)) continue;
        if (seenKeys.size === MAX_EVIDENCE_ROWS) return false;
        seenKeys.add(key); pending.push(node);
      }
      return true;
    };
    if (!enqueue(seed)) return "linked_identity_overflow";
    if (!pending.length) return undefined;
    let reasonFound: string | undefined;
    linkedVeto: for (let index = 0; index < pending.length; index++) {
      const node = pending[index]!;
      const aliases = node.kind === "request" ? ["request_id"] : node.kind === "call" ? ["call_id"] :
        ["codexTurnId","turn.id","turn_id"];
      const matches = aliases.map(alias => `case when json_valid(e.payload_json) then
        json_extract(e.payload_json,'$.metadata."${alias}"') end=?`);
      const turnScope = node.kind === "turn" ? " and e.session_id is ?" : "";
      const suffix = ` and (${matches.join(" or ")})${turnScope}`;
      const suffixArgs = [...aliases.map(() => node.value),...(node.kind === "turn" ? [seedSession] : [])];
      const branch = (condition: string, args: unknown[]) => ({
        sql: `${selectEvidence} where ${nativeScope}${condition}${suffix}`,
        args: [...nativeScopeArgs,...args,...suffixArgs],
      });
      // Indexed disjoint branches retain ALL uncertain rows, same-session
      // rows and same-trace rows. Only proven independent conversations are
      // omitted. No component or contradiction fact is capped by row count.
      const branches = scopedConversation ? [
        branch(` and ${linkedScopeQuery}=0`,[]),
        branch(` and ${linkedScopeQuery}=1 and e.session_id is ?`,[seedSession]),
        branch(` and ${linkedScopeQuery}=1 and e.session_id is not ? and
          case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.traceId') end=?`,
          [seedSession,seedTrace]),
      ] : [branch("",[])];
      for (const candidate of prepare(branches.map(b => b.sql).join(" union all "))
        .iterate(...branches.flatMap(b => b.args)) as Iterable<EvidenceRow>) {
        const p = decodePeers([candidate])[0];
        if (!p) continue;
        if (independentLinkedResponse(seed,p.event)) continue;
        const reason = veto(p,"linked");
        if (reason) { reasonFound = reason; break linkedVeto; }
        if (!enqueue(p.event)) { reasonFound = "linked_identity_overflow"; break linkedVeto; }
      }
    }
    return reasonFound;
  };
  type TurnName = { model: string; account: string | null; accounts: number };
  const boundary = row;
  const checkLocalFacts = (seed: AiInteractionEvent): { reason?: string; names: TurnName[] } => {
    const seedSession = trustedSession(seed);
    const seedTurn = text(seed.metadata.codexTurnId) ?? text(seed.metadata["turn.id"]) ?? text(seed.metadata.turn_id);
    if (!seedSession || !seedTurn) return { names: [] };
    let reasonFound: string | undefined;
    for (const candidate of prepare(`${selectEvidence} where ${nativeScope} and e.session_id=?
      and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.usageSource') end
        in ('codex_local_turn','rollout') and coalesce(
          case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.codexTurnId') end,
          case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata."turn.id"') end,
          case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.turn_id') end)=?`)
      .iterate(...nativeScopeArgs,seedSession,seedTurn) as Iterable<EvidenceRow>) {
      const p = decodePeers([candidate])[0];
      if (!p) continue;
      const reason = veto(p,"local");
      if (reason) { reasonFound = reason; break; }
    }
    if (reasonFound) return { reason: reasonFound, names: [] };
    let names: TurnName[] = [];
    if (prepare(
    "select 1 from sqlite_master where type='table' and name='codex_turn_model_evidence'",
  ).get()) {
    names = prepare(`select model,count(distinct nullif(account_key,'')) as accounts,
      min(nullif(account_key,'')) as account from codex_turn_model_evidence where
      workspace_id=? and device_id is ? and installation_epoch_id=? and session_id=? and turn_id=?
      group by model limit 2`).all(boundary.workspace,boundary.device,boundary.epoch,seedSession,seedTurn) as TurnName[];
    if (names.some(name => name.accounts > 1)) return { reason: "ambiguous_local_turn_identity", names };
    if (names.length > 1) return { reason: "ambiguous_local_turn_model", names };
    for (const name of names) {
      const reason = veto({event:{...seed,model:name.model,actorId:name.account ?? undefined,
        metadata:{model:name.model}},pairedId:null},"local");
      if (reason) return { reason, names };
    }
  }
    return { names };
  };
  const targetLocalFacts = checkLocalFacts(event);
  if (targetLocalFacts.reason) return gap(targetLocalFacts.reason);
  if (pairedSpan) {
    const spanLocalFacts = checkLocalFacts(pairedSpan.event);
    if (spanLocalFacts.reason) return gap(spanLocalFacts.reason);
  }
  // Preserve the native local-turn diagnostic when it is already decisive.
  // Typed links still veto every otherwise eligible target and saved source.
  vetoReason = checkLinkedFacts(event);
  if (vetoReason) return gap(vetoReason);
  if (pairedSpan) {
    vetoReason = checkLinkedFacts(pairedSpan.event);
    if (vetoReason) return gap(vetoReason);
  }
  const nativeTurnNames = targetLocalFacts.names;
  if (responsePair && vetoModel && responsePair.model !== vetoModel)
    return gap("conflicting_pair_target_model");
  if (responsePair && (responsePair.ownerId !== rawId || isCodexResponseSpan(event))) {
    const captured = { ...event, model: responsePair.model,
      metadata: { ...event.metadata, modelCaptureSource: "paired_rollout_event" } };
    return responsePair.ownerId === rawId ? captured : pairedObservation(captured, "paired_rollout_event");
  }
  if (nativeSseEvent && !traceId)
    return capture(event, [{ event, pairedId: null }], "native_sse_event");
  const scope = `e.source='codex' and e.observed_at>=? and e.observed_at<=? and e.id<>?
    and e.workspace_id is ? and e.device_id is ? and e.installation_epoch_id is ?`;
  const scopeArgs = [new Date(at - WINDOW_MS).toISOString(), new Date(end + WINDOW_MS).toISOString(),
    rawId, row.workspace, row.device, row.epoch];
  const counts = `case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.inputTokens') end is ?
    and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.outputTokens') end is ?
    and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.cacheReadTokens') end is ?
    and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.cacheCreationTokens') end is ?`;
  const countArgs = [event.inputTokens ?? null, event.outputTokens ?? null,
    event.cacheReadTokens ?? null, event.cacheCreationTokens ?? null];
  const peersByRow = new Map<number, Peer>();
  const addPeer = (p: Peer) => {
    peersByRow.set(p.lineage!.rawRowid, p);
    return peersByRow.size > MAX_EVIDENCE_ROWS;
  };
  // Scan indexed request candidates, counting only facts that can really
  // participate in this pair or local-turn tier. Unrelated tools, traces,
  // response identities and accounts cannot spend the evidence budget.
  // Streaming also keeps candidate memory bounded without selecting a model
  // from the first 128 rows and hiding a later relevant contradiction.
  let windowOverflow = false;
  const pairCandidates: Peer[] = [];
  for (const candidate of prepare(`${selectEvidence} where ${scope}
    and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.otelEventName') end
      in ('codex.sse_event','handle_responses')
    and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.otelEventName') end='codex.sse_event'
    and ${counts} and ${eligible}`).iterate(...scopeArgs,...countArgs) as Iterable<EvidenceRow>) {
    const p = decodePeers([candidate])[0];
    if (!p || !compatible(event,p.event) || !sameCounts(event,p.event) ||
      (codexResponseIdentities(event.metadata).length && codexResponseIdentities(p.event.metadata).length &&
        !codexResponseIdentityOverlap(event.metadata,p.event.metadata)) ||
      (p.pairedId && p.pairedId !== rawId) || Math.min(
        Math.abs(Date.parse(p.event.observedAt) - at), Math.abs(Date.parse(p.event.observedAt) - end)) > 30_000) continue;
    if (addPeer(p)) { windowOverflow = true; break; }
    pairCandidates.push(p);
  }
  if (windowOverflow) return gap("evidence_window_overflow");
  const sessionForWindow = trustedSession(event);
  if (sessionForWindow && turn) for (const candidate of prepare(`${selectEvidence} where ${scope}
    and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.usageSource') end
      in ('codex_local_turn','rollout')
    and e.session_id=? and coalesce(
      case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.codexTurnId') end,
      case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata."turn.id"') end,
      case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.turn_id') end)=?
    and ${eligible}`).iterate(...scopeArgs,sessionForWindow,turn) as Iterable<EvidenceRow>) {
    const p = decodePeers([candidate])[0];
    if (!p || trustedSession(p.event) !== sessionForWindow) continue;
    if (addPeer(p)) { windowOverflow = true; break; }
  }
  if (windowOverflow) return gap("evidence_window_overflow");
  const promotablePairs = pairCandidates.filter(mayPromote);
  if (promotablePairs.length) for (const candidate of prepare(`${selectEvidence} where ${scope}
    and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.otelEventName') end
      in ('codex.sse_event','handle_responses')
    and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.otelEventName') end='handle_responses'
    and ${counts} and ${eligible}`).iterate(...scopeArgs,...countArgs) as Iterable<EvidenceRow>) {
    const p = decodePeers([candidate])[0];
    if (!p || !sameCounts(event,p.event) || !promotablePairs.some(log => compatible(p.event,log.event))) continue;
    if (addPeer(p)) { windowOverflow = true; break; }
  }
  if (windowOverflow) return gap("evidence_window_overflow");
  // Native facts survive a financial gap, ACK and replay. Promotion is a
  // separate decision; each exact witness's own provenance must also agree.
  const peers = [...peersByRow.values()];
  const peerEvidence = peers;
  const native = peerEvidence.filter(mayPromote);
  // An internally conflicting peer remains evidence of ambiguity. Dropping
  // it before counting models could leave one clean log and select its model.
  const logs = peerEvidence.filter(
    (p) => p.event.metadata.otelEventName === "codex.sse_event",
  );
  const pairFacts = logs.filter(
    (p) =>
      compatible(event, p.event) &&
      !(codexResponseIdentities(event.metadata).length && codexResponseIdentities(p.event.metadata).length &&
        !codexResponseIdentityOverlap(event.metadata,p.event.metadata)) &&
      sameCounts(event, p.event) &&
      (!p.pairedId || p.pairedId === rawId) &&
      Math.min(
        Math.abs(Date.parse(p.event.observedAt) - at),
        Math.abs(Date.parse(p.event.observedAt) - end),
      ) <= 30_000,
  );
  // A terminal gap cannot name a pair, but its original native attributes
  // remain contradictions. Inspect the full exact-counter candidate set
  // before selecting promotable witnesses, just as the trace tier does.
  if (pairFacts.some((p) => nativeModel(p.event) === undefined || conflicts(p)))
    return gap("conflicting_pair_model_evidence");
  const pair = pairFacts.filter(mayPromote);
  const pairModels = unique(pair, (e) => nativeModel(e));
  if (pairModels.length > 1) return gap("ambiguous_pair_model");
  if (vetoModel && pairModels.length === 1 && pairModels[0] !== vetoModel)
    return gap("conflicting_pair_target_model");
  // A trace-free target can select a traced SSE, or one with request/turn
  // identities absent from the target. Those identities must not conceal a
  // contradiction that would refuse the witness's own fresh capture. Read
  // facts directly, without recursive capture or trusting a frozen peer's
  // financial decision; finalized peers remain facts for a new target.
  for (const source of pairFacts) {
    let reason = veto(source,"trace");
    if (reason) return gap(reason);
    const sourceTrace = text(source.event.metadata.traceId);
    if (sourceTrace && !vetoTraces.has(sourceTrace)) {
      for (const candidate of prepare(`${selectEvidence} where ${nativeScope}
        and case when json_valid(e.payload_json) then json_extract(e.payload_json,'$.metadata.traceId') end=?`)
        .iterate(...nativeScopeArgs,sourceTrace) as Iterable<EvidenceRow>) {
        const fact = decodePeers([candidate])[0];
        if (!fact) continue;
        reason = veto(fact,"trace");
        if (reason) break;
      }
      vetoTraces.add(sourceTrace);
    }
    if (reason) return gap(reason); // every SQLite iterator has closed.
    reason = checkLinkedFacts(source.event);
    if (reason) return gap(reason);
    const local = checkLocalFacts(source.event);
    if (local.reason) return gap(local.reason);
  }
  const competingSpans = peers.filter(
    (p) =>
      p.event.metadata.otelEventName === "handle_responses" &&
      sameCounts(event, p.event) &&
      pair.some((log) => compatible(p.event, log.event)),
  );
  if (
    pair.length === 1 &&
    pairModels.length === 1 &&
    competingSpans.length === 0
  ) {
    const captured=capture(event,pair,"paired_sse_event");
    // A native counter/SSE is not a duplicate span. Its accounting is reduced
    // only by response coverage against retained, capture-qualified owners.
    // The witness's native facts were checked above; paired counters remain
    // owned once and only a true span becomes an observation.
    return isCodexResponseSpan(event)?pairedObservation(captured):captured;
  }
  if (traceOverflow) return gap("trace_evidence_overflow");
  const tracedPeers = tracePeers.filter(mayPromote).filter(p =>
    // A model-less response span is an observation of usage, not a model
    // producer. It remains in tracePeers for EVERY conflict/session/account
    // check, but cannot veto a native SSE's complementary fields. This must
    // hold before and after sealing; otherwise lease order changes capture.
    // A model written by an older binary still supplies no native evidence.
    p.event.metadata.otelEventName !== "handle_responses" || nativeModel(p.event) !== undefined);
  const traced = tracedPeers;
  const traceEvidence =
    traceId && (directTraceModelEvidence || nativeSseEvent)
      ? [{ event, pairedId: null }, ...traced]
      : traced;
  const nativeTraceModels = unique(tracePeers, (e) => nativeModel(e));
  if (nativeTraceModels.length > 1)
    return gap("ambiguous_trace_model");
  if (
    (directTraceModelEvidence || nativeSseEvent) &&
    nativeTraceModels.length === 1 &&
    nativeTraceModels[0] !== nativeModel(event)
  )
    return gap("ambiguous_trace_model");
  // Every peer participating in the trace tier must carry one native
  // producer model. A model written on an old row is never promoted by this
  // branch, even when its counters differ from the response.
  if (tracedPeers.some((p) => nativeModel(p.event) === undefined))
    return gap("missing_trace_model_evidence");
  const traceModels = unique(traceEvidence, (e) => nativeModel(e));
  if (traceModels.length > 1) return gap("ambiguous_trace_model");
  if (
    tracePeers.some((p) => !compatible(event, p.event)) ||
    tracePeers.some((p) => tracePeers.some((other) => !compatible(p.event, other.event))) ||
    unique(tracePeers, (e) => trustedSession(e)).length > 1
  )
    return gap("ambiguous_trace_identity");
  if (
    traceModels.length === 1 &&
    traceEvidence.every((p) => compatible(event, p.event))
  )
    return capture(event, traceEvidence, "unique_trace_sse_event");
  const session = trustedSession(event);
  const local =
    session && turn
      ? [
          // Rollout tailer rows are themselves native turn observations. This
          // preserves a row written by an older reader during an upgrade while
          // still refusing a bare model on a response or SSE row.
          ...(event.metadata.usageSource === "rollout" && text(event.model)
            ? [{ event, pairedId: null }]
            : []),
          ...native.filter(
          (p) =>
            (p.event.metadata.usageSource === "codex_local_turn" ||
              p.event.metadata.usageSource === "rollout") &&
            p.event.sessionId === session &&
            p.event.metadata.codexTurnId === turn,
          ),
        ]
      : [];
  const localFacts = session && turn ? peerEvidence.filter((p) =>
    (p.event.metadata.usageSource === "codex_local_turn" || p.event.metadata.usageSource === "rollout") &&
    trustedSession(p.event) === session &&
    (p.event.metadata.codexTurnId ?? p.event.metadata["turn.id"] ?? p.event.metadata.turn_id) === turn) : [];
  const localFactModels = unique(localFacts, (e) => nativeModel(e) ??
    (e.metadata.usageSource === "rollout" ? text(e.model) : undefined));
  if (localFacts.some(conflicts) || localFactModels.length > 1)
    return gap("conflicting_local_model_evidence");
  if (localFacts.some((p) => !compatible(event, p.event)) ||
      localFacts.some((p) => localFacts.some((other) => !compatible(p.event, other.event))))
    return gap("ambiguous_local_turn_identity");
  if (local.some(conflicts)) return gap("conflicting_local_model_evidence");
  // Keep context-only records already read by the native tailer. Token rows
  // may be suppressed by the existing OTLP deduper without losing this evidence.
  if (
    session &&
    turn &&
    prepare(
        "select 1 from sqlite_master where type='table' and name='codex_turn_model_evidence'",
      )
      .get()
  ) {
    const names = nativeTurnNames;
    if (names.some((name) => name.accounts > 1))
      return gap("ambiguous_local_turn_identity");
    for (const name of names)
      local.push({
        event: {
          ...event,
          ...(name.account ? { actorId: name.account } : { actorId: undefined }),
          model: name.model,
          metadata: {
            ...event.metadata,
            // This attribute is synthesized only from recordCodexTurnModel's
            // native turn_context record, inside the exact install boundary.
            model: name.model,
            ...(name.account ? { "user.account_id": name.account } : {}),
          },
        },
        pairedId: null,
      });
  }
  const localModels = unique(local, (e) => nativeModel(e) ?? text(e.model));
  if (new Set([...localModels,...localFactModels]).size > 1) return gap("ambiguous_local_turn_model");
  if (
    local.some((p) => accountConflict(p.event)) ||
    local.some((p) => local.some((other) => !compatible(p.event, other.event)))
  )
    return gap("ambiguous_local_turn_identity");
  if (
    localModels.length === 1 &&
    !local.every((p) => compatible(event, p.event))
  )
    return gap("local_turn_identity_conflict");
  if (localModels.length === 1)
    return capture(event, local, "local_session_turn");
  return gap("model_evidence_missing");
}

/** Native turn_context only: no new files, credentials, or message content. */
export function recordCodexTurnModel(
  db: Database.Database,
  sessionId: string,
  turnId: string,
  model: string,
  accountKey?: string,
) {
  if (
    !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(sessionId) ||
    !/^[A-Za-z0-9._:-]{1,128}$/.test(turnId) ||
    !validatedMetadataAttribute("model", model).accepted
  )
    return;
  const binding = db
    .prepare(
      `select current_workspace_id as workspace, current_device_id as device,
    current_installation_epoch_id as epoch from collector_workspace_binding where singleton=1`,
    )
    .get() as
    | { workspace: string; device: string | null; epoch: string | null }
    | undefined;
  if (!binding?.epoch) return;
  db.exec(`create table if not exists codex_turn_model_evidence (
    workspace_id text not null,device_id text,installation_epoch_id text not null,
    session_id text not null,turn_id text not null,model text not null,account_key text not null,
    primary key(workspace_id,installation_epoch_id,session_id,turn_id,model,account_key)) without rowid`);
  db.prepare(
    `insert or ignore into codex_turn_model_evidence values (?,?,?,?,?,?,?)`,
  ).run(
    binding.workspace,
    binding.device,
    binding.epoch,
    sessionId,
    turnId,
    model,
    accountKey && /^sha256:[0-9a-f]{16}$/.test(accountKey) ? accountKey : "",
  );
}
