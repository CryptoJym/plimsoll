import type Database from "better-sqlite3";
import {
  estimateCostUsd,
  providerAccountKey,
  usageFieldKeys,
  validatedMetadataAttribute,
  type AiInteractionEvent,
} from "../../shared/src/index";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";
import { codexSpanRolloutDecision, isCodexResponseSpan } from "./codex-span-rollout-pairing";

export const CODEX_MODEL_WAIT_MS = 60_000;
const WINDOW_MS = 10 * 60_000;
const MAX_EVIDENCE_ROWS = 128;
const text = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() ? value : undefined;
/** A capture gap is a durable accounting decision. Its raw counters remain
 * local diagnostics and must never become usage again on a later read. */
export function isCaptureGap(event: AiInteractionEvent): boolean {
  return event.metadata.usageSource === "capture_gap" || event.metadata.captureGap === true;
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
  const value = event.metadata.otelAttributes;
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function valuesForAliases(
  event: AiInteractionEvent,
  aliases: readonly string[],
): string[] {
  return [
    ...aliases.map((key) => event.metadata[key]),
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
export function codexMisfiledUnderClaude(event: AiInteractionEvent): boolean {
  return (
    event.source === "claude_code" &&
    /^(codex[-_.]|codex$)/i.test(String(event.metadata.serviceName ?? ""))
  );
}
export function unresolvedCapture(event: AiInteractionEvent): boolean {
  return (
    codexHasUsage(event) &&
    (!text(event.model) ||
      nativeModels(event).size === 0 ||
      codexMisfiledUnderClaude(event) ||
      event.metadata.modelEvidenceConflict === true ||
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
): AiInteractionEvent {
  const epoch = text(event.metadata.installationEpochId),
    at = Date.parse(event.observedAt);
  if (!db.readonly && epoch && Number.isFinite(at)) {
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

function captureDecision(db: Database.Database, lineage: CaptureLineage) {
  if (!db.prepare("select 1 from sqlite_master where type='table' and name='codex_capture_decisions'").get()) return undefined;
  return db.prepare("select reason from codex_capture_decisions where raw_rowid=? and raw_id=? and raw_created_at=? and raw_generation is ? limit 1").get(
    lineage.rawRowid, lineage.rawId, lineage.rawCreatedAt, lineage.rawGeneration,
  ) as { reason: string } | undefined;
}

function captureLineage(db: Database.Database, rawId: string): CaptureLineage | undefined {
  try {
    return db.prepare(`select rowid as rawRowid,id as rawId,created_at as rawCreatedAt,
      privacy_generation as rawGeneration from buffered_events where id=?`).get(rawId) as
      CaptureLineage | undefined;
  } catch { return undefined; }
}

export function hasCaptureGapDecision(db: Database.Database, lineage: CaptureLineage): boolean {
  return Boolean(captureDecision(db, lineage));
}

export function rememberCaptureGap(
  db: Database.Database,
  rawId: string,
  reason: string,
  at = new Date(),
) {
  const lineage = captureLineage(db, rawId);
  if (!lineage || db.readonly) return false;
  ensureCaptureDecisionTable(db);
  db.prepare(`insert or replace into codex_capture_decisions
    (raw_rowid,raw_id,raw_created_at,raw_generation,decision,reason,decided_at)
    values (?,?,?,?,?,?,?)`).run(
    lineage.rawRowid, lineage.rawId, lineage.rawCreatedAt, lineage.rawGeneration,
    "gap",
    reason, at.toISOString(),
  );
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
): AiInteractionEvent {
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
      }
    | undefined;
  try {
    row = db
      .prepare(
        `select rowid as rawRowid,id as rawId,created_at as rawCreatedAt,
      privacy_generation as rawGeneration, workspace_id as workspace, device_id as device,
      installation_epoch_id as epoch, usage_paired_event_id as pairedId
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
  const gap = (reason: string) => {
    const result = codexModelGap(db, event, reason);
    if (persistDecision && row && !db.readonly) {
      ensureCaptureDecisionTable(db);
      db.prepare(`insert or replace into codex_capture_decisions
        (raw_rowid,raw_id,raw_created_at,raw_generation,decision,reason,decided_at)
        values (?,?,?,?,?,?,?)`).run(
        row.rawRowid, row.rawId, row.rawCreatedAt, row.rawGeneration,
        "gap",
        reason, new Date().toISOString(),
      );
    }
    return result;
  };
  if (row && hasCaptureGapDecision(db, row))
    return gap("persisted_capture_gap");
  if (nativeModels(event).size > 1 || modelAttributeConflict(event) || accountConflict(event) ||
    event.metadata.modelEvidenceConflict === true)
    return gap("conflicting_model_attributes");
  if (codexMisfiledUnderClaude(event))
    return gap("codex_service_under_claude_source");
  // A saved pair cannot override an accounting gap or contradictory native
  // attributes. In particular the raw diagnostics behind a frozen gap still
  // carry their original counters.
  const responsePair = codexSpanRolloutDecision(db, rawId);
  if (responsePair && (responsePair.ownerId !== rawId || isCodexResponseSpan(event))) {
    const captured = { ...event, model: responsePair.model,
      metadata: { ...event.metadata, modelCaptureSource: "paired_rollout_event" } };
    return responsePair.ownerId === rawId ? captured : pairedObservation(captured, "paired_rollout_event");
  }
  // Some native exporters put the request model directly on the response
  // span. Treat that as trace evidence only when the event carries a bounded
  // trace id and the native model attribute agrees with the normalized model
  // field. A bare model field is still untrusted, which keeps 0.7.48's
  // reconciliation output tokenless. The evidence is joined below with every
  // native row on that trace so a conflicting session, account, or model can
  // never be hidden by this self-attested value.
  const directTraceId = text(event.metadata.traceId);
  const directTraceModels = [
    ...new Set(valuesForAliases(event, usageFieldKeys.model)),
  ];
  const directTraceModelEvidence = Boolean(
    directTraceId &&
    text(event.model) &&
    directTraceModels.length === 1 &&
    directTraceModels[0] === event.model &&
    nativeModels(event).size === 1,
  );
  const nativeSseEvent =
    event.metadata.otelEventName === "codex.sse_event" &&
    nativeModels(event).size === 1 &&
    !modelAttributeConflict(event);
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
  const eligible = terminalPrivacyEligibilitySql(db, "e");
  const raw = db
    .prepare(
      `select e.rowid as evidenceRowid, e.id as evidenceId,
       e.created_at as evidenceCreatedAt, e.privacy_generation as evidenceGeneration,
       e.payload_json as payload, e.usage_paired_event_id as pairedId
    from buffered_events e indexed by idx_events_observed
    where e.source='codex' and e.observed_at>=? and e.observed_at<=? and e.id<>?
      and e.workspace_id is ? and e.device_id is ? and e.installation_epoch_id is ?
      and ${eligible} and e.usage_duplicate_reason is null
    order by e.observed_at, e.id limit ${MAX_EVIDENCE_ROWS + 1}`,
    )
    .all(
      new Date(at - WINDOW_MS).toISOString(),
      new Date(end + WINDOW_MS).toISOString(),
      rawId,
      row.workspace,
      row.device,
      row.epoch,
    ) as Array<{
      evidenceRowid: number;
      evidenceId: string;
      evidenceCreatedAt: string;
      evidenceGeneration: string | null;
      payload: string;
      pairedId: string | null;
    }>;
  if (raw.length > MAX_EVIDENCE_ROWS) return gap("evidence_window_overflow");
  const peers: Peer[] = raw.flatMap((r) => {
    try {
      return [
        {
          event: JSON.parse(r.payload) as AiInteractionEvent,
          pairedId: r.pairedId,
          lineage: {
            rawRowid: r.evidenceRowid,
            rawId: r.evidenceId,
            rawCreatedAt: r.evidenceCreatedAt,
            rawGeneration: r.evidenceGeneration,
          },
        },
      ];
    } catch {
      return [];
    }
  });
  // A previously decided capture gap is accounting state, not a native
  // producer peer. Keeping it out of the trace set lets a later genuine SSE
  // evidence row be captured without ever rereading the gap's counters.
  // A peer with a durable gap decision is not allowed to supply a model, but
  // its native attributes remain evidence of ambiguity. Keep the two sets
  // separate: financial eligibility is not permission to erase contradictory
  // producer facts from a later trace capture.
  const peerEvidence = peers.filter((p) => !p.event.metadata.modelCaptureSource);
  const native = peerEvidence.filter((p) =>
    !isCaptureGap(p.event) &&
    !(p.lineage && hasCaptureGapDecision(db, p.lineage)));
  const conflicts = (p: Peer) =>
    p.event.metadata.modelEvidenceConflict === true ||
    nativeModels(p.event).size > 1 ||
    modelAttributeConflict(p.event) ||
    accountConflict(p.event);
  // An internally conflicting peer remains evidence of ambiguity. Dropping
  // it before counting models could leave one clean log and select its model.
  const logs = native.filter(
    (p) => p.event.metadata.otelEventName === "codex.sse_event",
  );
  const pair = logs.filter(
    (p) =>
      compatible(event, p.event) &&
      sameCounts(event, p.event) &&
      (!p.pairedId || p.pairedId === rawId) &&
      Math.min(
        Math.abs(Date.parse(p.event.observedAt) - at),
        Math.abs(Date.parse(p.event.observedAt) - end),
      ) <= 30_000,
  );
  if (pair.some((p) => nativeModel(p.event) === undefined || conflicts(p)))
    return gap("conflicting_pair_model_evidence");
  const pairModels = unique(pair, (e) => nativeModel(e));
  if (pairModels.length > 1) return gap("ambiguous_pair_model");
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
  )
    return pairedObservation(capture(event, pair, "paired_sse_event"));
  const traceId = text(event.metadata.traceId);
  const tracePeers = traceId
    ? peerEvidence.filter((p) => p.event.metadata.traceId === traceId)
    : [];
  if (tracePeers.some(conflicts)) return gap("conflicting_trace_model_evidence");
  const tracedPeers = traceId
    ? native.filter((p) => p.event.metadata.traceId === traceId)
    : [];
  const traced = traceId
    ? logs.filter((p) => p.event.metadata.traceId === traceId)
    : [];
  const traceEvidence =
    traceId && (directTraceModelEvidence || nativeSseEvent)
      ? [{ event, pairedId: null }, ...traced]
      : traced;
  const nativeTraceModels = unique(tracedPeers, (e) => nativeModel(e));
  if (nativeTraceModels.length > 1)
    return gap("ambiguous_trace_model");
  if (
    (directTraceModelEvidence || nativeSseEvent) &&
    nativeTraceModels.length === 1 &&
    nativeTraceModels[0] !== event.model
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
    tracedPeers.some((p) => accountConflict(p.event)) ||
    tracedPeers.some((p) => !compatible(event, p.event)) ||
    tracedPeers.some((p) => tracedPeers.some((other) => !compatible(p.event, other.event))) ||
    unique(tracedPeers, (e) => trustedSession(e)).length > 1
  )
    return gap("ambiguous_trace_identity");
  if (
    traceModels.length === 1 &&
    traceEvidence.every((p) => compatible(event, p.event))
  )
    return capture(event, traceEvidence, "unique_trace_sse_event");
  // A native SSE log without a trace is still an exact named source. There is
  // no trace boundary to join, so only this row's own model can qualify it.
  if (nativeSseEvent && !traceId)
    return capture(event, [{ event, pairedId: null }], "native_sse_event");
  const turn =
    text(event.metadata.codexTurnId) ??
    text(event.metadata["turn.id"]) ??
    text(event.metadata.turn_id);
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
  if (local.some(conflicts)) return gap("conflicting_local_model_evidence");
  // Keep context-only records already read by the native tailer. Token rows
  // may be suppressed by the existing OTLP deduper without losing this evidence.
  if (
    session &&
    turn &&
    db
      .prepare(
        "select 1 from sqlite_master where type='table' and name='codex_turn_model_evidence'",
      )
      .get()
  ) {
    const names = db
      .prepare(
        `select model, count(distinct nullif(account_key,'')) as accounts,
      min(nullif(account_key,'')) as account from codex_turn_model_evidence where
      workspace_id=? and device_id is ? and installation_epoch_id=? and session_id=? and turn_id=? group by model limit 2`,
      )
      .all(row.workspace, row.device, row.epoch, session, turn) as Array<{
      model: string;
      account: string | null;
      accounts: number;
    }>;
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
  if (localModels.length > 1) return gap("ambiguous_local_turn_model");
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
