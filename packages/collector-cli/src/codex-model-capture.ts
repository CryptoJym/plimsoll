import type Database from "better-sqlite3";
import {
  estimateCostUsd,
  usageFieldKeys,
  validatedMetadataAttribute,
  type AiInteractionEvent,
} from "../../shared/src/index";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";

export const CODEX_MODEL_WAIT_MS = 60_000;
const WINDOW_MS = 10 * 60_000;
const MAX_EVIDENCE_ROWS = 128;
const text = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() ? value : undefined;
export function codexHasUsage(event: AiInteractionEvent): boolean {
  // Native runtime intervals deliberately carry no per-request model and are
  // unqualified observers; cloud excludes them from financial usage.
  return (
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

function nativeModels(event: AiInteractionEvent): Set<string> {
  return new Set(
    [
      event.model,
      ...usageFieldKeys.model.map((key) => event.metadata[key]),
    ].filter(
      (value): value is string => typeof value === "string" && !!value.trim(),
    ),
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
      codexMisfiledUnderClaude(event) ||
      event.metadata.modelEvidenceConflict === true ||
      nativeModels(event).size > 1)
  );
}
function pairedObservation(event: AiInteractionEvent): AiInteractionEvent {
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
      usageDuplicateReason: "paired_sse_event",
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

type Peer = { event: AiInteractionEvent; pairedId: string | null };
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
  return (
    (!as || !bs || as === bs) &&
    (!a.metadata.traceId ||
      !b.metadata.traceId ||
      a.metadata.traceId === b.metadata.traceId) &&
    (!a.metadata["user.account_id"] ||
      !b.metadata["user.account_id"] ||
      a.metadata["user.account_id"] === b.metadata["user.account_id"])
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
  const model = unique(peers, (e) => text(e.model))[0]!;
  const accounts = unique(peers, (e) => text(e.metadata["user.account_id"]));
  const sessions = unique(peers, (e) => trustedSession(e));
  const account =
    text(event.metadata["user.account_id"]) ??
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
    ...(account ? { actorId: account } : {}),
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

/** Called after the durable 60-second pair wait, immediately before sealing.
 * Time alone never supplies a model or account. Evidence never crosses the
 * ledger's workspace/device/install or a conflicting native session/trace.
 * Each tier must name exactly one model; ambiguity fails closed at that tier.
 */
export function captureCodexModel(
  db: Database.Database,
  event: AiInteractionEvent,
  rawId = event.id,
): AiInteractionEvent {
  if (!codexHasUsage(event)) return event;
  let row:
    | {
        workspace: string | null;
        device: string | null;
        epoch: string | null;
        pairedId: string | null;
      }
    | undefined;
  try {
    row = db
      .prepare(
        `select workspace_id as workspace, device_id as device,
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
  const gap = (reason: string) => codexModelGap(db, event, reason);
  if (
    nativeModels(event).size > 1 ||
    event.metadata.modelEvidenceConflict === true
  )
    return gap("conflicting_model_attributes");
  const accountKey =
    event.source === "claude_code" ? "user.account_uuid" : "user.account_id";
  if (codexMisfiledUnderClaude(event))
    return gap("codex_service_under_claude_source");
  if (text(event.model))
    return {
      ...event,
      metadata: {
        ...event.metadata,
        accountIdentityState: text(event.metadata[accountKey])
          ? "reported"
          : "unavailable",
      },
    };
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
      `select e.payload_json as payload, e.usage_paired_event_id as pairedId
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
    ) as Array<{ payload: string; pairedId: string | null }>;
  if (raw.length > MAX_EVIDENCE_ROWS) return gap("evidence_window_overflow");
  const peers: Peer[] = raw.flatMap((r) => {
    try {
      return [
        {
          event: JSON.parse(r.payload) as AiInteractionEvent,
          pairedId: r.pairedId,
        },
      ];
    } catch {
      return [];
    }
  });
  const native = peers.filter((p) => !p.event.metadata.modelCaptureSource);
  const conflicts = (p: Peer) =>
    p.event.metadata.modelEvidenceConflict === true ||
    nativeModels(p.event).size > 1;
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
  if (pair.some(conflicts)) return gap("conflicting_pair_model_evidence");
  const pairModels = unique(pair, (e) => text(e.model));
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
  const traced = traceId
    ? logs.filter((p) => p.event.metadata.traceId === traceId)
    : [];
  if (traced.some(conflicts)) return gap("conflicting_trace_model_evidence");
  const traceModels = unique(traced, (e) => text(e.model));
  if (traceModels.length > 1) return gap("ambiguous_trace_model");
  if (
    unique(traced, (e) => text(e.metadata["user.account_id"])).length > 1 ||
    unique(traced, (e) => trustedSession(e)).length > 1
  )
    return gap("ambiguous_trace_identity");
  if (
    traceModels.length === 1 &&
    traced.every((p) => compatible(event, p.event))
  )
    return capture(event, traced, "unique_trace_sse_event");
  const turn =
    text(event.metadata.codexTurnId) ??
    text(event.metadata["turn.id"]) ??
    text(event.metadata.turn_id);
  const session = trustedSession(event);
  const local =
    session && turn
      ? native.filter(
          (p) =>
            (p.event.metadata.usageSource === "codex_local_turn" ||
              p.event.metadata.usageSource === "rollout") &&
            p.event.sessionId === session &&
            p.event.metadata.codexTurnId === turn,
        )
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
          model: name.model,
          metadata: {
            ...event.metadata,
            ...(name.account ? { "user.account_id": name.account } : {}),
          },
        },
        pairedId: null,
      });
  }
  const localModels = unique(local, (e) => text(e.model));
  if (localModels.length > 1) return gap("ambiguous_local_turn_model");
  if (unique(local, (e) => text(e.metadata["user.account_id"])).length > 1)
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
