import type { AiInteractionEvent } from "../../shared/src/index";
import { captureCodexModel, codexModelGap, codexHasUsage, codexMisfiledUnderClaude, hasCaptureGapDecision, isCaptureGap, legacyFrozenNativeCapture, rememberCaptureGap, rememberCaptureGapForLineage, unresolvedCapture, CODEX_MODEL_WAIT_MS } from "./codex-model-capture";
import { frozenCodexCapture, frozenCodexDelivery, installCodexFrozenCompatibility, rememberFrozenCodexCapture } from "./codex-named-capture";
import { rememberCodexSpanEmission } from "./codex-span-rollout-pairing";
import { applyCodexResponseCoverage } from "./codex-response-coverage";
import crypto from "node:crypto";

import type Database from "better-sqlite3";

import {
  aiInteractionEventSchema,
  aiWorkIngestEventSchema,
  type AiWorkIngestEvent,
} from "../../shared/src/index";
import { canonicalLinkage, sealOutboundEnvelope } from "./outbound-envelope";
import {
  markRawPrivacyDisposition,
  terminalPrivacyEligibilitySql,
  type TerminalPrivacyReason,
} from "./privacy-disposition";
import { collisionSafeDeliveryId, ensureUuidEventId, incarnationDeliveryId,
  isCollisionSafeDeliveryId,
  normalizeHistoryEvent } from "./upload-history";
import { applyProjectAttribution, SessionAttributionBatch } from "./session-attribution";
import {
  CAPTURE_WRITE_LAG_MS,
  captureFrontier,
  mergeCaptureGaps,
  type CaptureGap,
} from "./capture-frontier";
import type { CaptureSpoolState } from "./capture-spool-state";
import { captureDeadLetterCensus, type CaptureDeadLetter, type CaptureDeadLetterInterval } from "./capture-dead-letters";

export const DEFAULT_DELIVERY_LIMITS = {
  maxActiveRows: 50_000,
  maxActiveBytes: 512 * 1024 * 1024,
  maxOldestAgeDays: 90,
  maxItemBytes: 256 * 1024,
  migrationBatchRows: 5_000,
  migrationBatchBytes: 32 * 1024 * 1024,
  maxBatchesPerCycle: 20,
  leaseSeconds: 120,
  requestTimeoutSeconds: 30,
  maxBackoffSeconds: 60 * 60,
  maxProbesPerCycle: 31,
} as const;

const REMOTE_REJECTED_MAX_ATTEMPTS = 5;

export type DeliveryLimits = {
  [Key in keyof typeof DEFAULT_DELIVERY_LIMITS]: number;
};

export type DeliveryFailureClass =
  | "none"
  | "local_payload_unparseable"
  | "local_schema_invalid"
  | "local_privacy_violation"
  | "local_item_oversize"
  | "local_request_budget"
  | "remote_validation"
  | "remote_auth"
  | "remote_transient"
  | "remote_rejected"
  | "remote_contract";

export type DeliveryCircuit = "none" | "auth_blocked" | "contract_blocked";
export type DeliveryReceiptReason =
  | "remote_acknowledged"
  | "local_evidence_quarantined"
  | "local_payload_unparseable"
  | "local_schema_invalid"
  | "local_privacy_violation"
  | "local_item_oversize"
  | "local_usage_duplicate"
  | "local_model_capture_gap"
  | "remote_rejected_exhausted"
  | "remote_validation_rejected";

/** Remote terminal reasons are the only replayable dead letters (bead .46):
 * the cloud rejected an envelope the collector prepared correctly, so a fixed
 * remote contract makes the delivery viable again. Local privacy, quarantine,
 * oversize and schema receipts are decisions about the row itself and stay
 * final — replaying them would re-run the same local refusal. */
export const REPLAYABLE_RECEIPT_REASONS: readonly DeliveryReceiptReason[] = [
  "remote_validation_rejected",
  "remote_rejected_exhausted",
];

export function isReplayableReceiptReason(
  reason: string,
): reason is DeliveryReceiptReason {
  return REPLAYABLE_RECEIPT_REASONS.includes(reason as DeliveryReceiptReason);
}

export type DeliveryReplaySummary = {
  reason: string;
  selected: number;
  requeued: number;
  skipped: {
    alreadyActive: number;
    alreadyAcknowledged: number;
    missingRaw: number;
    privacyDisposed: number;
  };
  dryRun: boolean;
  /** Set only when a full `--limit` of candidates re-queued nothing, so an
   * operator reading the JSON is pointed at the narrowing option instead of
   * having to infer it from a large `selected` with `requeued: 0`. */
  hint?: string;
};

/** Bounded like the migration scan: the replay transaction never waits longer
 * than this for a competing writer before rolling back for a later retry. */
const REPLAY_BUSY_TIMEOUT_MS = 5_000;

function isTerminalPrivacyReason(
  reason: DeliveryReceiptReason,
): reason is TerminalPrivacyReason {
  return (
    reason === "local_evidence_quarantined" ||
    reason === "local_privacy_violation"
  );
}

export type DeliveryStatus = {
  enabled: boolean;
  degraded: boolean;
  degradedReasons: Array<
    | "pressure_row_budget"
    | "pressure_byte_budget"
    | "pressure_age_budget"
    | "migration_slice_budget"
    | "remote_rejected"
    | "auth_circuit"
    | "contract_circuit"
  >;
  remainingDelivery: number;
  active: {
    pending: number;
    retry: number;
    inFlight: number;
    bytes: number;
    oldestCreatedAt: string | null;
    oldestAgeSeconds: number | null;
  };
  receipts: { acknowledged: number; dead: number };
  pressure: {
    degraded: boolean;
    reasons: Array<"row_budget" | "byte_budget" | "age_budget">;
    budgets: { rows: number; bytes: number; oldestAgeSeconds: number };
  };
  circuit: {
    kind: DeliveryCircuit;
    openedAt: string | null;
    until: string | null;
  };
  migration: {
    cursorRowid: number;
    complete: boolean;
    pausedReason: "pressure" | "slice_budget_too_small" | "receipt_lineage_pending" | null;
    progressMode: "bounded_rowid_watermark_no_exact_remaining";
    sliceBudget: { rows: number; bytes: number; uploadBatchesPerCycle: number };
    lastSlice: {
      visited: number;
      bytes: number;
      enqueued: number;
      dead: number;
      skippedUploaded: number;
      at: string | null;
    };
  };
  retention: {
    mode: "raw_ttl";
    rawTtlBlockedBy: null;
    pendingDeliverySurvivesRawExpiry: true;
  };
  counters: {
    outboxRowsEnqueued: number;
    outboxAttempts: number;
    deadLettersWritten: number;
  };
  work: {
    controlRowsRead: 1;
    activeRowsScanned: 0;
    receiptRowsScanned: 0;
    rawRowsScanned: 0;
  };
  privacy: {
    mode: "metadata_only";
    evidenceVault: "not_implemented";
    legacyEvidenceDisposition: "local_quarantine_migration_required";
    liveLedgerInspection: "not_performed";
  };
};

export type LeasedDeliveryItem = {
  deliveryId: string;
  rawRowid: number | null;
  rawId: string | null;
  rawCreatedAt: string | null;
  rawGeneration: string | null;
  deviceId?: string | null;
  envelopeJson: string;
  envelope: AiWorkIngestEvent;
  attemptCount: number;
};

export type DeliveryLease = {
  leaseId: string;
  items: LeasedDeliveryItem[];
  locallyDead: number;
  blockedBy: DeliveryCircuit | "none";
};

/**
 * Capture watermark v1 (eco-6hoxj.163.18): what one upload request attests.
 * The cloud contract is docs/capture-watermark-v1.md in the cloud repository.
 */
export type DeliveryCaptureClaim = {
  v: 1;
  epoch: string;
  epochStartedAt: string;
  /** Strictly increasing per ledger; one value per request. */
  cursor: number;
  /**
   * Every event of the epoch observed before this is acknowledged or in this
   * request, apart from `gaps`. Null: not attested, and `unattested` says why.
   */
  through: string | null;
  unattested?: CaptureUnattestedReason;
  /** Deliveries of the epoch queued outside this request, plus spooled push files. */
  pending: number;
  /** Deliveries and spooled push files of the epoch lost for good; each lies in a gap. */
  dead: number;
  /** Deliveries kept local by design (privacy violation, evidence quarantine); not gaps. */
  withheld: number;
  /** At most CAPTURE_CLAIM_MAX_GAPS closed intervals in which data is known to be missing. */
  gaps: Array<{ from: string; to: string; deadLetters?: CaptureDeadLetter[] }>;
};

/** Why a claim attests nothing (review r2 B3): the cloud withdraws what it held. */
export type CaptureUnattestedReason =
  | "migration_incomplete"
  | "over_row_budget"
  | "spool_unreadable"
  | "frontier_unknown";

/** Refusals the collector makes on purpose; the claim counts them as withheld. */
const WITHHELD_RECEIPT_REASONS_SQL = "'local_evidence_quarantined','local_privacy_violation'";

export type DeliveryValidationWitness = {
  contractHash: string;
  acknowledgedAt: string;
  item: LeasedDeliveryItem;
};

type RawDeliveryRow = {
  rawRowid: number;
  rawId: string;
  privacyGeneration: string | null;
  privacyDisposition: TerminalPrivacyReason | null;
  usageDuplicateReason?: string | null;
  dataMode: string;
  createdAt: string;
  uploadedAt: string | null;
  payloadJson: string;
  suppressedFieldsJson: string;
  repoHash: string | null;
  branchHash: string | null;
  workspaceId: string | null;
  deviceId: string | null;
};

type LegacyCandidateRow = Pick<
  RawDeliveryRow,
  "rawRowid" | "rawId" | "createdAt" | "uploadedAt" | "workspaceId" | "deviceId"
> & { dataMode: string; rowBytes: number };

type ReceiptBackfillControl = {
  cursorRowid: number;
};

type ReceiptBackfillCandidate = {
  rawRowid: number;
  rawId: string;
  rawCreatedAt: string;
  rawGeneration: string | null;
};

type ActiveDeliveryRow = {
  deliveryId: string;
  rawRowid: number | null;
  rawId: string | null;
  rawCreatedAt: string | null;
  rawGeneration: string | null;
  baseEnvelopeJson: string;
  rawPayloadJson: string | null;
  sealedEnvelopeJson: string | null;
  repoHash: string | null;
  branchHash: string | null;
  attemptCount: number;
  deviceId: string | null;
};

type RawPrivacyRow = {
  rawId: string;
  createdAt: string;
  privacyGeneration: string | null;
  privacyDisposition: TerminalPrivacyReason | null;
  usageDuplicateReason: string | null;
  dataMode: string;
  uploadedAt: string | null;
};

type RawLineageSnapshot = Pick<
  ActiveDeliveryRow,
  "deliveryId" | "rawRowid" | "rawId" | "rawCreatedAt" | "rawGeneration" | "deviceId"
>;

function rememberDeliveryCaptureGap(db: Database.Database, row: Pick<ActiveDeliveryRow,
  "rawRowid" | "rawId" | "rawCreatedAt" | "rawGeneration">, reason: string) {
  if(row.rawRowid===null || row.rawId===null || row.rawCreatedAt===null)return false;
  return rememberCaptureGapForLineage(db,{rawRowid:row.rawRowid,rawId:row.rawId,
    rawCreatedAt:row.rawCreatedAt,rawGeneration:row.rawGeneration},reason);
}

type PrivacyDecision = DeliveryReceiptReason | "lineage_unresolved" | null;

type PreparedDelivery =
  | {
      ok: true;
      deliveryId: string;
      baseEnvelopeJson: string;
      baseBytes: number;
      repoHash: string | null;
      branchHash: string | null;
    }
  | { ok: false; deliveryId: string; reason: DeliveryReceiptReason };

function prepareDelivery(row: RawDeliveryRow, maxItemBytes: number, resolvedId?: string): PreparedDelivery {
  const fallbackId = resolvedId ?? ensureUuidEventId(row.rawId).id;
  if (row.dataMode === "evidence") {
    return { ok: false, deliveryId: fallbackId, reason: "local_evidence_quarantined" };
  }
  const normalized = normalizeHistoryEvent({
    payloadJson: row.payloadJson,
    suppressedFieldsJson: row.suppressedFieldsJson,
  });
  if (normalized.ok === false) {
    const reason =
      normalized.reason === "payload_unparseable"
        ? "local_payload_unparseable"
        : normalized.reason === "forbidden_content"
          ? "local_privacy_violation"
          : "local_schema_invalid";
    return { ok: false, deliveryId: fallbackId, reason };
  }

  const deliveryId = resolvedId ?? normalized.envelope.event.id;
  // Capture, legacy migration and replay enqueue one row at a time, so no
  // session lookup runs here: lease() applies session inheritance when it
  // seals the envelope, with one bounded lookup per lease batch.
  const attributed = applyProjectAttribution({ ...normalized.envelope.event, id: deliveryId }, {
    repoHash: row.repoHash,
    branchHash: row.branchHash,
  });
  const envelope = sealOutboundEnvelope({
    ...normalized.envelope,
    event: attributed.event,
  });
  if (!envelope.ok) {
    return {
      ok: false,
      deliveryId,
      reason: envelope.reason === "schema" ? "local_schema_invalid" : "local_privacy_violation",
    };
  }
  const baseEnvelopeJson = JSON.stringify(envelope.envelope);
  const baseBytes = Buffer.byteLength(baseEnvelopeJson);
  if (baseBytes > maxItemBytes) {
    return { ok: false, deliveryId, reason: "local_item_oversize" };
  }
  return {
    ok: true,
    deliveryId,
    baseEnvelopeJson,
    baseBytes,
    repoHash: canonicalLinkage(row.repoHash),
    branchHash: canonicalLinkage(row.branchHash),
  };
}

function persistedGapPayload(db: Database.Database, row: RawDeliveryRow) {
  let event: AiInteractionEvent;
  try { event = aiInteractionEventSchema.parse(JSON.parse(row.payloadJson)); }
  catch { return row.payloadJson; }
  const lineage = {
    rawRowid: row.rawRowid, rawId: row.rawId, rawCreatedAt: row.createdAt,
    rawGeneration: row.privacyGeneration,
  };
  if (isCaptureGap(event)) {
    const reason = typeof event.metadata.modelGapReason === "string"
      ? event.metadata.modelGapReason : "legacy_capture_gap";
    rememberCaptureGap(db, row.rawId, reason);
    return row.payloadJson;
  }
  if (!codexHasUsage(event) || !hasCaptureGapDecision(db, lineage)) return row.payloadJson;
  return JSON.stringify(codexModelGap(db, event, "persisted_capture_gap"));
}

function captureGapReason(envelopeJson: string | null | undefined): string | undefined {
  if (!envelopeJson) return undefined;
  try {
    const envelope = aiWorkIngestEventSchema.parse(JSON.parse(envelopeJson));
    if (!isCaptureGap(envelope.event)) return undefined;
    return typeof envelope.event.metadata.modelGapReason === "string"
      ? envelope.event.metadata.modelGapReason
      : "sealed_capture_gap";
  } catch {
    return undefined;
  }
}

/** Refresh only a never-attempted envelope after pairing adds cached tokens. */
export function refreshUnsentRawDelivery(
  db: Database.Database,
  rawId: string,
  maxItemBytes = DEFAULT_DELIVERY_LIMITS.maxItemBytes,
) {
  const refresh = () => {
  const raw = db.prepare(
    `select rowid as rawRowid, id as rawId, created_at as createdAt,
       data_mode as dataMode, uploaded_at as uploadedAt,
       payload_json as payloadJson, suppressed_fields_json as suppressedFieldsJson,
       repo_hash as repoHash, branch_hash as branchHash,
       workspace_id as workspaceId, device_id as deviceId,
       privacy_generation as privacyGeneration,
       privacy_disposition as privacyDisposition,
       usage_duplicate_reason as usageDuplicateReason
     from buffered_events where id = ?`,
  ).get(rawId) as RawDeliveryRow | undefined;
  if (!raw || raw.uploadedAt || raw.usageDuplicateReason) return false;
  const linked = db.prepare(`select delivery_id as id,base_envelope_json as baseEnvelopeJson from upload_outbox
    where raw_rowid=? and raw_id=? and raw_created_at=?
      and raw_generation is ? limit 1`).get(raw.rawRowid, raw.rawId,
    raw.createdAt, raw.privacyGeneration) as { id: string; baseEnvelopeJson: string } | undefined;
  if (!linked) return false;
  if (hasCaptureGapDecision(db, {
    rawRowid: raw.rawRowid, rawId: raw.rawId, rawCreatedAt: raw.createdAt,
    rawGeneration: raw.privacyGeneration,
  })) return false;
  try {
    const base = aiWorkIngestEventSchema.parse(JSON.parse(linked.baseEnvelopeJson));
    if (isCaptureGap(base.event)) return false;
  } catch {
    // A malformed base cannot prove that this delivery is safe to refresh.
    // Leave it for the ordinary lease/schema path instead of deriving a new
    // billable envelope from the raw row.
    return false;
  }
  const prepared = prepareDelivery(raw, maxItemBytes, linked.id);
  if (!prepared.ok) return false;
  return db.prepare(
    `update upload_outbox set base_envelope_json = @baseEnvelopeJson,
       base_bytes = @baseBytes, sealed_envelope_json = null, sealed_bytes = null,
       updated_at = @now
     where delivery_id = @deliveryId and state in ('pending','retry')
       and sealed_envelope_json is null and attempt_count = 0`,
  ).run({ ...prepared, now: new Date().toISOString() }).changes > 0;
  };
  return db.inTransaction ? refresh() : db.transaction(refresh).immediate();
}

/** A paired span stays in the raw ledger but must not enter a new upload. */
export function retirePairedSpanDelivery(db: Database.Database, spanId: string) {
  return db.prepare(
    `delete from upload_outbox where delivery_id = ? and sealed_envelope_json is null and state in ('pending','retry')`,
  ).run(ensureUuidEventId(spanId).id).changes;
}

function attachFillOnlyLinkage(
  envelope: AiWorkIngestEvent,
  repoHash: string | null,
  branchHash: string | null,
  attribution: SessionAttributionBatch,
  disposedRawRowids: ReadonlySet<number>,
): AiWorkIngestEvent {
  const attributed = attribution.attribute(envelope.event, {
    repoHash,
    branchHash,
    excludedRowids: disposedRawRowids,
  });
  return {
    ...envelope,
    event: attributed.event,
  };
}

function terminalStatusClass(reason: DeliveryReceiptReason) {
  if (reason === "remote_acknowledged") return "remote_2xx";
  if (reason === "remote_rejected_exhausted") return "remote_rejected";
  if (reason === "remote_validation_rejected") return "remote_validation";
  return "local_validation";
}

function asLimits(input: Partial<DeliveryLimits> | undefined): DeliveryLimits {
  return { ...DEFAULT_DELIVERY_LIMITS, ...input };
}

export class DeliveryOutbox {
  private enabled: boolean;
  private limits: DeliveryLimits;
  private workspaceId: string | null;
  private deviceId: string | null;
  /** Injectable clock for eligibility/claim bookkeeping (issue 0182). Every
   * timestamp this class writes that later feeds a lease/eligibility
   * comparison — notably enqueueRaw's next_attempt_at — must come from here,
   * never from the wall clock, or an injected caller clock can silently
   * disagree with these stamps and empty a claim. */
  private readonly clock: () => Date;
  private readonly onHoldChange: (() => void) | undefined;

  constructor(
    private readonly db: Database.Database,
    options: {
      enabled?: boolean;
      limits?: Partial<DeliveryLimits>;
      workspaceId?: string;
      deviceId?: string;
      now?: () => Date;
      onHoldChange?: () => void;
    } = {},
  ) {
    this.enabled = options.enabled ?? false;
    this.limits = asLimits(options.limits);
    this.clock = options.now ?? (() => new Date());
    this.onHoldChange = options.onHoldChange;
    this.workspaceId = options.workspaceId?.trim() || null;
    this.deviceId = options.deviceId?.trim() || null;
    this.initializeSchema();
    if (this.enabled) this.reopenMigrationPastWatermark();
  }

  configure(options: {
    enabled: boolean;
    limits?: Partial<DeliveryLimits>;
    workspaceId?: string;
    deviceId?: string;
  }) {
    const enabledChanged = this.enabled !== options.enabled;
    this.enabled = options.enabled;
    if (enabledChanged) this.onHoldChange?.();
    this.limits = asLimits(options.limits);
    if (options.workspaceId) this.setWorkspace(options.workspaceId);
    if (options.deviceId) this.setDevice(options.deviceId);
    if (this.enabled) this.reopenMigrationPastWatermark();
  }

  setWorkspace(workspaceId: string) {
    const value = workspaceId.trim();
    if (!value) throw new Error("Delivery workspace requires a non-empty id.");
    this.workspaceId = value;
  }

  setDevice(deviceId: string) {
    const value = deviceId.trim();
    if (!value) throw new Error("Delivery device requires a non-empty id.");
    this.deviceId = value;
  }

  queueAgeSeconds(now = new Date()) {
    const row = this.db
      .prepare(
        `select min(created_at) as oldestCreatedAt
         from upload_outbox
         where state in ('pending', 'retry', 'in_flight')
           and workspace_id is ? and device_id is ?`,
      )
      .get(this.workspaceId, this.deviceId) as { oldestCreatedAt: string | null };
    if (!row.oldestCreatedAt) return null;
    const createdAt = Date.parse(row.oldestCreatedAt);
    if (!Number.isFinite(createdAt)) return null;
    return Math.max(0, Math.floor((now.getTime() - createdAt) / 1_000));
  }

  bindUnassignedWorkspace(workspaceId: string) {
    const value = workspaceId.trim();
    if (!value) throw new Error("Delivery workspace requires a non-empty id.");
    return this.db
      .prepare(`update upload_outbox set workspace_id = ? where workspace_id is null`)
      .run(value).changes;
  }

  isEnabled() {
    return this.enabled;
  }

  /**
   * Capture watermark v1 (eco-6hoxj.163.18). The claim for ONE request whose
   * delivery ids are `requestDeliveryIds`, computed in one write transaction
   * right before it is sent. `spool` is what the hook and OTLP spools held
   * just before (capture-spool-state.ts); null when it could not be read.
   *
   * - `through` is at most the capture frontier (capture-frontier.ts), earlier
   *   than the observed time of every delivery of the epoch still queued
   *   outside this request, and earlier than every spooled push file's
   *   arrival less the write lag. Never before the epoch start. Null, with
   *   `unattested` saying why, whenever that bound cannot be taken (review r2
   *   B3): the legacy migration is still enqueuing ledger rows, the queue is
   *   over the row budget (the upload path never scans it then), a spool
   *   cannot be read, or the frontier is not known yet.
   * - `gaps` are bounded intervals of known loss: dead deliveries, spooled
   *   push files rejected or expired, and tailed files the frontier moved past
   *   unread. They never hold `through` back (review r2 S2), so one loss marks
   *   only the time it covers. Privacy and evidence refusals are deliberate,
   *   not loss: they are counted in `withheld` and are not gaps.
   * - `cursor` increments once per claim, so the cloud can drop a slower,
   *   older request that commits after a newer one.
   *
   * Scoped to rows appended since the epoch started (append times, which the
   * ledger stamps itself): earlier rows belong to the previous epoch's claims.
   */
  captureClaim(
    requestDeliveryIds: string[],
    spool: CaptureSpoolState | null,
    now = this.clock(),
  ): DeliveryCaptureClaim | null {
    if (!this.enabled || !this.workspaceId) return null;
    const run = this.db.transaction(() => {
      const frontier = captureFrontier(this.db);
      if (!frontier || frontier.workspaceId !== this.workspaceId) return null;
      const epochStartedAt = frontier.epochStartedAt;
      const epochStartMs = Date.parse(epochStartedAt);
      const control = this.db
        .prepare(
          `select migration_complete as migrationComplete,
             active_pending + active_retry + active_in_flight as active
           from upload_control where singleton = 1`,
        )
        .get() as { migrationComplete: number; active: number };
      // Keep the upload path bounded: over the row budget the queue is far
      // behind anyway, so attest nothing instead of reading every queued
      // envelope (about 70 ms per request at 50,000 rows, linear beyond it).
      const overBudget = control.active > this.limits.maxActiveRows;
      const epochStartSeconds = epochStartMs / 1000;
      const requestIds = JSON.stringify(requestDeliveryIds);
      // Earliest observed time of the queued rows the epoch covers. An
      // unreadable envelope or observed time dates its row to the epoch start
      // instead of failing the claim (review r2 S5).
      const pending = overBudget
        ? {
            count: Math.max(0, control.active - (this.db
              .prepare(`select count(*) as count from upload_outbox where delivery_id in (select value from json_each(?))`)
              .get(requestIds) as { count: number }).count),
            earliestSeconds: null,
          }
        : (this.db
            .prepare(
              `select count(*) as count,
                 min(case when observed is null then @epochStartSeconds
                   when observed >= @epochStartSeconds then observed end) as earliestSeconds
               from (
                 select unixepoch(case when json_valid(base_envelope_json)
                   then json_extract(base_envelope_json, '$.event.observedAt') end, 'subsec') as observed
                 from upload_outbox
                 where workspace_id is @workspaceId and device_id is @deviceId
                   and created_at >= @epochStartedAt
                   and delivery_id not in (select value from json_each(@requestIds))
               )`,
            )
            .get({
              workspaceId: this.workspaceId,
              deviceId: this.deviceId,
              epochStartedAt,
              epochStartSeconds,
              requestIds,
            }) as { count: number; earliestSeconds: number | null });
      const lost = this.deadLetterSummary(epochStartedAt, epochStartMs);
      const spoolLosses = (spool?.losses ?? []).filter((loss) => loss.toMs >= epochStartMs);
      const receiptLineageComplete = this.receiptLineageComplete();
      const unattested: CaptureUnattestedReason | null =
        control.migrationComplete !== 1 || !receiptLineageComplete ? "migration_incomplete"
          : overBudget ? "over_row_budget"
            : spool === null || spool.unreadable ? "spool_unreadable"
              : frontier.capturedThrough === null ? "frontier_unknown"
                : null;
      let throughMs = unattested === null ? Date.parse(frontier.capturedThrough!) : null;
      // Floor to the millisecond: rounding can only move the bound earlier.
      const bounds = [
        pending.earliestSeconds === null ? null : Math.floor(pending.earliestSeconds * 1000),
        spool?.oldestPendingMs == null ? null : spool.oldestPendingMs - CAPTURE_WRITE_LAG_MS,
      ];
      for (const bound of bounds) {
        if (throughMs !== null && bound !== null) throughMs = Math.min(throughMs, bound);
      }
      if (throughMs !== null) throughMs = Math.max(throughMs, epochStartMs);
      const gaps = mergeCaptureGaps([
        ...frontier.gaps,
        ...lost.gaps,
        ...spoolLosses.map((loss) => ({
          fromMs: Math.max(epochStartMs, loss.fromMs - CAPTURE_WRITE_LAG_MS),
          toMs: loss.toMs,
        })),
      ]);
      this.db
        .prepare(
          `update upload_control set capture_claim_sequence = capture_claim_sequence + 1,
             updated_at = @now where singleton = 1`,
        )
        .run({ now: now.toISOString() });
      const cursor = (this.db
        .prepare(`select capture_claim_sequence as cursor from upload_control where singleton = 1`)
        .get() as { cursor: number }).cursor;
      const claim: DeliveryCaptureClaim = {
        v: 1 as const,
        epoch: frontier.installationEpochId,
        epochStartedAt,
        cursor,
        through: throughMs === null ? null : new Date(throughMs).toISOString(),
        ...(unattested === null ? {} : { unattested }),
        pending: pending.count + (spool?.pendingFiles ?? 0),
        dead: lost.dead + spoolLosses.reduce((total, loss) => total + loss.count, 0),
        withheld: lost.withheld,
        gaps: gaps.map((gap) => {
          const complete = lost.censuses.find(interval =>
            interval.fromMs === gap.fromMs && interval.toMs === gap.toMs);
          const otherLoss = [...frontier.gaps, ...spoolLosses.map(loss => ({
            fromMs: Math.max(epochStartMs, loss.fromMs - CAPTURE_WRITE_LAG_MS), toMs: loss.toMs,
          }))].some(other => other.fromMs <= gap.toMs && other.toMs >= gap.fromMs);
          return { from: new Date(gap.fromMs).toISOString(), to: new Date(gap.toMs).toISOString(),
            ...(complete && !otherLoss ? { deadLetters: complete.deadLetters } : {}),
          };
        }),
      };
      // The existing v1 header bound is 1024 characters, including the census.
      // Keep known gaps when complete summaries cannot fit that wire contract.
      if (JSON.stringify(claim).length > 1024) claim.gaps = claim.gaps.map(({ from, to }) => ({ from, to }));
      return claim;
    });
    return run.immediate();
  }

  /**
   * The epoch's dead receipts for the capture claim: lost deliveries as one
   * interval per UTC day of their observed time (the epoch start when the
   * ledger row is gone), withheld ones as a count. Recomputed only when a
   * dead receipt changed, so the claim does not rescan them on every request
   * (review r2 N1). Not scoped to the workspace: receipts carry none, and
   * counting more only widens the report.
   */
  private deadLetterSummary(epochStartedAt: string, epochStartMs: number): {
    dead: number;
    withheld: number;
    gaps: CaptureGap[];
    censuses: CaptureDeadLetterInterval[];
  } {
    const version = (this.db
      .prepare(`select capture_dead_version as version from upload_control where singleton = 1`)
      .get() as { version: number }).version;
    const cached = this.db
      .prepare(
        `select dead_version as version, epoch_started_at as epochStartedAt, summary_json as summary
         from capture_dead_summary where singleton = 1`,
      )
      .get() as { version: number; epochStartedAt: string; summary: string } | undefined;
    if (cached && cached.version === version && cached.epochStartedAt === epochStartedAt) {
      const parsed = JSON.parse(cached.summary) as {
        schema?: number; dead: number; withheld: number; gaps: CaptureGap[];
        censuses?: CaptureDeadLetterInterval[];
      };
      if (parsed.schema === 2 && Array.isArray(parsed.censuses)) return { ...parsed, censuses: parsed.censuses };
    }
    const counts = this.db
      .prepare(
        `select count(*) as total,
           coalesce(sum(case when reason in (${WITHHELD_RECEIPT_REASONS_SQL}) then 1 else 0 end), 0) as withheld
         from upload_receipts where terminal_state = 'dead' and created_at >= ?`,
      )
      .get(epochStartedAt) as { total: number; withheld: number };
    const days = this.db
      .prepare(
        `select min(f) as fromSeconds, max(t) as toSeconds from (
           select coalesce(unixepoch(b.observed_at, 'subsec'), @epochStartSeconds) as f,
             coalesce(unixepoch(b.observed_at, 'subsec'), unixepoch(r.created_at, 'subsec'), @epochStartSeconds) as t
           from upload_receipts r left join buffered_events b on b.id = r.delivery_id
           where r.terminal_state = 'dead' and r.created_at >= @epochStartedAt
             and r.reason not in (${WITHHELD_RECEIPT_REASONS_SQL})
         ) group by cast(f / 86400 as integer)`,
      )
      .all({ epochStartedAt, epochStartSeconds: epochStartMs / 1000 }) as Array<{ fromSeconds: number; toSeconds: number }>;
    const gaps: CaptureGap[] = [];
    for (const day of days) {
      const toMs = Math.ceil(day.toSeconds * 1000);
      if (toMs < epochStartMs) continue;
      const fromMs = Math.max(epochStartMs, Math.floor(day.fromSeconds * 1000));
      gaps.push({ fromMs, toMs: Math.max(fromMs, toMs) });
    }
    const merged = mergeCaptureGaps(gaps);
    const censuses = merged.flatMap(interval => {
      const deadLetters = captureDeadLetterCensus(this.db, epochStartedAt, interval);
      return deadLetters ? [{ ...interval, deadLetters }] : [];
    });
    const summary = { schema: 2, dead: counts.total - counts.withheld,
      withheld: counts.withheld, gaps: merged, censuses };
    this.db
      .prepare(
        `insert into capture_dead_summary (singleton, dead_version, epoch_started_at, summary_json)
         values (1, ?, ?, ?)
         on conflict (singleton) do update set dead_version = excluded.dead_version,
           epoch_started_at = excluded.epoch_started_at, summary_json = excluded.summary_json`,
      )
      .run(version, epochStartedAt, JSON.stringify(summary));
    return summary;
  }

  isEvidenceQuarantined(rawId: string) {
    return Boolean(
      this.db
        .prepare(
          `select 1 as quarantined from upload_receipts
           where delivery_id = ? and reason = 'local_evidence_quarantined'
           limit 1`,
        )
        .get(ensureUuidEventId(rawId).id),
    );
  }

  private initializeSchema() {
    // Additive only: no query or index is built against the historical raw
    // ledger here. Large-ledger work happens solely in bounded migration slices.
    this.db.exec(`
      create table if not exists upload_outbox (
        delivery_id text primary key,
        raw_rowid integer,
        raw_id text,
        raw_created_at text,
        raw_generation text,
        workspace_id text,
        device_id text,
        base_envelope_json text not null,
        base_bytes integer not null,
        repo_hash text,
        branch_hash text,
        sealed_envelope_json text,
        sealed_bytes integer,
        state text not null check (state in ('pending','retry','in_flight')),
        attempt_count integer not null default 0,
        next_attempt_at text not null,
        lease_id text,
        lease_expires_at text,
        last_failure_class text not null default 'none',
        created_at text not null,
        updated_at text not null
      );
      create table if not exists upload_receipts (
        delivery_id text primary key,
        raw_rowid integer,
        raw_id text,
        raw_created_at text,
        raw_generation text,
        terminal_state text not null check (terminal_state in ('acknowledged','dead')),
        reason text not null,
        status_class text not null,
        attempt_count integer not null,
        created_at text not null,
        terminal_at text not null
      );
      create table if not exists upload_validation_witness (
        singleton integer primary key check (singleton = 1),
        contract_hash text not null,
        delivery_id text not null,
        envelope_json text not null,
        envelope_bytes integer not null,
        acknowledged_at text not null
      );
      create table if not exists upload_validation_candidates (
        delivery_id text primary key,
        contract_hash text not null,
        failed_at text not null
      );
      -- Bead .46: a dead letter written for a remote reason is only terminal
      -- while the remote contract that rejected it is unchanged. upload-replay
      -- supersedes that receipt and records the supersession here, so a second
      -- replay of the same delivery is a counted no-op rather than a duplicate.
      create table if not exists upload_replays (
        delivery_id text primary key,
        reason text not null,
        original_terminal_at text not null,
        replayed_at text not null,
        replay_count integer not null default 1,
        raw_rowid integer,
        raw_id text,
        raw_created_at text,
        raw_generation text,
        frozen_envelope_json text,
        frozen_bytes integer,
        frozen_attempt_count integer
      );
      create table if not exists upload_control (
        singleton integer primary key check (singleton = 1),
        migration_cursor_rowid integer not null default 0,
        migration_complete integer not null default 0,
        migration_paused_reason text,
        circuit_kind text not null default 'none',
        circuit_opened_at text,
        circuit_until text,
        active_pending integer not null default 0,
        active_retry integer not null default 0,
        active_in_flight integer not null default 0,
        active_bytes integer not null default 0,
        active_oldest_created_at text,
        receipt_acknowledged integer not null default 0,
        receipt_dead integer not null default 0,
        outbox_enqueued_total integer not null default 0,
        outbox_attempts_total integer not null default 0,
        migration_last_visited integer not null default 0,
        migration_last_bytes integer not null default 0,
        migration_last_enqueued integer not null default 0,
        migration_last_dead integer not null default 0,
        migration_last_skipped_uploaded integer not null default 0,
        migration_last_at text,
        privacy_migration_version integer not null default 0,
        validation_probe_rows integer not null default 0,
        active_remote_rejected integer not null default 0,
        updated_at text not null
      );
      insert or ignore into upload_control (singleton, updated_at)
      values (1, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
      create index if not exists idx_upload_outbox_due
        on upload_outbox (state, next_attempt_at, created_at);
      create index if not exists idx_upload_outbox_lease
        on upload_outbox (state, lease_expires_at);
      create index if not exists idx_upload_outbox_created
        on upload_outbox (created_at, delivery_id);
      create index if not exists idx_upload_outbox_raw_rowid
        on upload_outbox (raw_rowid);
      create index if not exists idx_upload_receipts_state
        on upload_receipts (terminal_state);
      create index if not exists idx_upload_validation_candidates_contract_failure
        on upload_validation_candidates (contract_hash, failed_at, delivery_id);

      create trigger if not exists trg_upload_outbox_gauge_insert
      after insert on upload_outbox
      begin
        update upload_control set
          active_pending = active_pending + case when new.state = 'pending' then 1 else 0 end,
          active_retry = active_retry + case when new.state = 'retry' then 1 else 0 end,
          active_in_flight = active_in_flight + case when new.state = 'in_flight' then 1 else 0 end,
          active_bytes = active_bytes + coalesce(new.sealed_bytes, new.base_bytes),
          outbox_enqueued_total = outbox_enqueued_total + 1,
          active_oldest_created_at = case
            when active_oldest_created_at is null or new.created_at < active_oldest_created_at
              then new.created_at else active_oldest_created_at end
        where singleton = 1;
      end;

      create trigger if not exists trg_upload_outbox_gauge_delete
      after delete on upload_outbox
      begin
        update upload_control set
          active_pending = active_pending - case when old.state = 'pending' then 1 else 0 end,
          active_retry = active_retry - case when old.state = 'retry' then 1 else 0 end,
          active_in_flight = active_in_flight - case when old.state = 'in_flight' then 1 else 0 end,
          active_bytes = active_bytes - coalesce(old.sealed_bytes, old.base_bytes),
          active_oldest_created_at = case
            when active_oldest_created_at = old.created_at
              then (select min(created_at) from upload_outbox)
            else active_oldest_created_at end
        where singleton = 1;
      end;

      create trigger if not exists trg_upload_validation_candidate_cleanup
      after delete on upload_outbox
      begin
        delete from upload_validation_candidates where delivery_id = old.delivery_id;
      end;

      create trigger if not exists trg_upload_outbox_gauge_update
      after update of state, sealed_bytes, attempt_count on upload_outbox
      begin
        update upload_control set
          active_pending = active_pending
            - case when old.state = 'pending' then 1 else 0 end
            + case when new.state = 'pending' then 1 else 0 end,
          active_retry = active_retry
            - case when old.state = 'retry' then 1 else 0 end
            + case when new.state = 'retry' then 1 else 0 end,
          active_in_flight = active_in_flight
            - case when old.state = 'in_flight' then 1 else 0 end
            + case when new.state = 'in_flight' then 1 else 0 end,
          active_bytes = active_bytes
            - coalesce(old.sealed_bytes, old.base_bytes)
            + coalesce(new.sealed_bytes, new.base_bytes),
          outbox_attempts_total = outbox_attempts_total
            + max(0, new.attempt_count - old.attempt_count)
        where singleton = 1;
      end;

      create trigger if not exists trg_upload_receipt_gauge_insert
      after insert on upload_receipts
      begin
        update upload_control set
          receipt_acknowledged = receipt_acknowledged
            + case when new.terminal_state = 'acknowledged' then 1 else 0 end,
          receipt_dead = receipt_dead
            + case when new.terminal_state = 'dead' then 1 else 0 end
        where singleton = 1;
      end;
    `);
    const replayColumns = this.db
      .prepare(`pragma table_info(upload_replays)`)
      .all() as Array<{ name: string }>;
    const replayAdditions: Array<[string, string]> = [
      ["raw_rowid", "integer"],
      ["raw_id", "text"],
      ["raw_created_at", "text"],
      ["raw_generation", "text"],
      ["frozen_envelope_json", "text"],
      ["frozen_bytes", "integer"],
      ["frozen_attempt_count", "integer"],
    ];
    for (const [name, type] of replayAdditions) {
      if (!replayColumns.some((column) => column.name === name))
        this.db.exec(`alter table upload_replays add column ${name} ${type}`);
    }
    const controlColumns = this.db
      .prepare(`pragma table_info(upload_control)`)
      .all() as Array<{ name: string }>;
    if (!controlColumns.some((column) => column.name === "validation_probe_rows")) {
      this.db.exec(
        `alter table upload_control
         add column validation_probe_rows integer not null default 0`,
      );
    }
    if (!controlColumns.some((column) => column.name === "privacy_migration_version")) {
      this.db.exec(
        `alter table upload_control
         add column privacy_migration_version integer not null default 0`,
      );
    }
    if (!controlColumns.some((column) => column.name === "capture_claim_sequence")) {
      this.db.exec(
        `alter table upload_control
         add column capture_claim_sequence integer not null default 0`,
      );
    }
    if (!controlColumns.some((column) => column.name === "capture_dead_version")) {
      this.db.exec(
        `alter table upload_control
         add column capture_dead_version integer not null default 0`,
      );
    }
    // eco-6hoxj.163.18 (review r2 N1): every change to a dead receipt bumps a
    // version, so the capture claim summarizes dead receipts once per change.
    this.db.exec(`
      create table if not exists capture_dead_summary (
        singleton integer primary key check (singleton = 1),
        dead_version integer not null,
        epoch_started_at text not null,
        summary_json text not null
      );
      create trigger if not exists trg_capture_dead_version_insert
      after insert on upload_receipts when new.terminal_state = 'dead'
      begin
        update upload_control set capture_dead_version = capture_dead_version + 1 where singleton = 1;
      end;
      create trigger if not exists trg_capture_dead_version_delete
      after delete on upload_receipts when old.terminal_state = 'dead'
      begin
        update upload_control set capture_dead_version = capture_dead_version + 1 where singleton = 1;
      end;
      create trigger if not exists trg_capture_dead_version_update
      after update on upload_receipts when old.terminal_state = 'dead' or new.terminal_state = 'dead'
      begin
        update upload_control set capture_dead_version = capture_dead_version + 1 where singleton = 1;
      end;
    `);
    if (!controlColumns.some((column) => column.name === "active_remote_rejected")) {
      this.db.exec(
        `alter table upload_control
         add column active_remote_rejected integer not null default 0`,
      );
    }
    this.db.exec(`
      create trigger if not exists trg_upload_remote_rejected_gauge_insert
      after insert on upload_outbox
      when new.last_failure_class = 'remote_rejected'
      begin
        update upload_control set active_remote_rejected = active_remote_rejected + 1
        where singleton = 1;
      end;
      create trigger if not exists trg_upload_remote_rejected_gauge_delete
      after delete on upload_outbox
      when old.last_failure_class = 'remote_rejected'
      begin
        update upload_control set active_remote_rejected = active_remote_rejected - 1
        where singleton = 1;
      end;
      create trigger if not exists trg_upload_remote_rejected_gauge_update
      after update of last_failure_class on upload_outbox
      when old.last_failure_class <> new.last_failure_class
      begin
        update upload_control set active_remote_rejected = active_remote_rejected
          - case when old.last_failure_class = 'remote_rejected' then 1 else 0 end
          + case when new.last_failure_class = 'remote_rejected' then 1 else 0 end
        where singleton = 1;
      end;
    `);
    const outboxColumns = this.db
      .prepare(`pragma table_info(upload_outbox)`)
      .all() as Array<{ name: string }>;
    if (!outboxColumns.some((column) => column.name === "workspace_id")) {
      this.db.exec(`alter table upload_outbox add column workspace_id text`);
    }
    if (!outboxColumns.some((column) => column.name === "device_id")) {
      this.db.exec(`alter table upload_outbox add column device_id text`);
    }
    for (const column of ["raw_id", "raw_created_at", "raw_generation"]) {
      if (!outboxColumns.some((existing) => existing.name === column)) {
        this.db.exec(`alter table upload_outbox add column ${column} text`);
      }
    }
    const receiptColumns = this.db.pragma("table_info(upload_receipts)") as Array<{ name: string }>;
    for (const [column, type] of [
      ["raw_rowid", "integer"], ["raw_id", "text"],
      ["raw_created_at", "text"], ["raw_generation", "text"],
    ] as const) {
      if (!receiptColumns.some((existing) => existing.name === column)) {
        this.db.exec(`alter table upload_receipts add column ${column} ${type}`);
      }
    }
    const lineageIndex = this.db.prepare(`select sql from sqlite_master
      where type='index' and name='idx_upload_receipts_raw_lineage'`).get() as
      { sql: string } | undefined;
    if (lineageIndex && !lineageIndex.sql.includes("where terminal_state='dead'")) {
      this.db.exec(`drop index idx_upload_receipts_raw_lineage`);
    }
    this.db.exec(
      `create index if not exists idx_upload_outbox_workspace_due
         on upload_outbox (workspace_id, device_id, state, next_attempt_at, created_at);
       create index if not exists idx_upload_outbox_raw_generation
         on upload_outbox (raw_generation, created_at, delivery_id);
       create index if not exists idx_upload_receipts_raw_lineage
         on upload_receipts (raw_rowid, raw_id) where terminal_state='dead';
       drop trigger if exists trg_upload_receipt_lineage_immutable;
       create trigger trg_upload_receipt_lineage_immutable
       before update of raw_rowid, raw_id, raw_created_at, raw_generation on upload_receipts
       when (new.raw_rowid is not old.raw_rowid
         or new.raw_id is not old.raw_id
         or new.raw_created_at is not old.raw_created_at
         or new.raw_generation is not old.raw_generation)
         and not (old.raw_rowid is null and old.raw_id is null
           and old.raw_created_at is null and old.raw_generation is null
           and new.raw_rowid is not null and new.raw_id is not null
           and new.raw_created_at is not null)
       begin
         select raise(abort, 'upload_receipt_lineage_is_immutable');
       end;
       create trigger if not exists trg_upload_outbox_lineage_immutable
       before update of raw_rowid, raw_id, raw_created_at, raw_generation on upload_outbox
       when new.raw_rowid is not old.raw_rowid
         or new.raw_id is not old.raw_id
         or new.raw_created_at is not old.raw_created_at
         or new.raw_generation is not old.raw_generation
       begin
         select raise(abort, 'upload_outbox_lineage_is_immutable');
       end`,
    );
    // The old raw-scan table was never shipped. A receipt cursor is independent
    // of the already-complete privacy/raw migration on upgraded 0.7.44 ledgers.
    const lineageControlColumns = this.db.pragma("table_info(upload_receipt_lineage_backfill)") as
      Array<{ name: string }>;
    if (lineageControlColumns.some((column) => column.name === "scan_cursor_rowid")) {
      this.db.exec(`drop trigger if exists trg_receipt_lineage_backfill_insert;
        drop table if exists upload_receipt_lineage_candidates;
        drop table upload_receipt_lineage_backfill`);
    }
    this.db.exec(`
      create table if not exists upload_receipt_lineage_backfill (
        singleton integer primary key check (singleton = 1),
        cursor_rowid integer not null,
        updated_at text not null
      );
      insert or ignore into upload_receipt_lineage_backfill
        (singleton,cursor_rowid,updated_at)
        values (1,0,strftime('%Y-%m-%dT%H:%M:%fZ','now'));
      create trigger if not exists trg_receipt_lineage_backfill_insert
      after insert on upload_receipts
      when new.terminal_state='dead'
        and new.raw_rowid is null and new.raw_id is null
        and new.raw_created_at is null and new.raw_generation is null
      begin
        update upload_receipt_lineage_backfill set
          cursor_rowid=min(cursor_rowid,new.rowid-1),
          updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where singleton=1;
      end;
    `);
    this.db.prepare(
      `update upload_control set migration_cursor_rowid = 0,
         migration_complete = 0, migration_paused_reason = null,
         privacy_migration_version = 1, updated_at = @now
       where singleton = 1 and privacy_migration_version < 1`,
     ).run({ now: this.clock().toISOString() });
    installCodexFrozenCompatibility(this.db);
   }

  /** Only dead NULL-lineage receipts ahead of the durable cursor block attestation. */
  private receiptLineageComplete() {
    const cursor = (this.db.prepare(`select cursor_rowid as cursorRowid
      from upload_receipt_lineage_backfill where singleton=1`).get() as ReceiptBackfillControl).cursorRowid;
    return !this.db.prepare(`select 1 from upload_receipts indexed by idx_upload_receipts_raw_lineage
      where terminal_state='dead' and raw_rowid is null and raw_id is null
        and raw_created_at is null and raw_generation is null and rowid>? limit 1`).get(cursor);
  }

  private hasUnprocessedDeadReceipt(rawId: string) {
    return Boolean(this.db.prepare(`select 1 from upload_receipts where delivery_id=?
      and terminal_state='dead' and raw_rowid is null and raw_id is null
      and raw_created_at is null and raw_generation is null
      and rowid>(select cursor_rowid from upload_receipt_lineage_backfill where singleton=1)`)
      .get(ensureUuidEventId(rawId).id));
  }

  /**
   * Receipt-side repair. Primary-key and created-at index probes prove an
   * owner inside the writer transaction. An oversized timestamp cohort stays
   * unresolved; it cannot extend a writer slice or claim false uniqueness.
   */
  backfillLegacyReceiptLineage(options: { maxRows?: number; maxWriterMs?: number } = {}) {
    const maxRows = Math.max(1, Math.min(Math.trunc(options.maxRows ?? 256), 5_000));
    const maxWriterMs = Math.max(1, Math.min(Math.trunc(options.maxWriterMs ?? 100), 500));
    const deadline = performance.now() + maxWriterMs;
    type Candidate = ReceiptBackfillCandidate & { uploadedAt: string | null };
    const rawById = this.db.prepare(`select rowid as rawRowid,id as rawId,
      created_at as rawCreatedAt,privacy_generation as rawGeneration,
      uploaded_at as uploadedAt from buffered_events where id=?`);
    const rawAtTime = this.db.prepare(`select rowid as rawRowid,id as rawId,
      created_at as rawCreatedAt,privacy_generation as rawGeneration,
      uploaded_at as uploadedAt from buffered_events indexed by idx_events_retention
      where created_at=? order by created_at,id limit 1025`);
    const pendingCandidate = this.db.prepare(`select rowid as rawRowid,id as rawId,
      created_at as createdAt,privacy_generation as privacyGeneration,
      uploaded_at as uploadedAt,data_mode as dataMode,payload_json as payloadJson,
      suppressed_fields_json as suppressedFieldsJson,repo_hash as repoHash,
      branch_hash as branchHash,workspace_id as workspaceId,device_id as deviceId,
      privacy_disposition as privacyDisposition,
      usage_duplicate_reason as usageDuplicateReason
      from buffered_events where rowid=? and id=? and created_at=?
        and privacy_generation is ? and uploaded_at is null
        and length(cast(payload_json as blob)) +
          length(cast(suppressed_fields_json as blob)) <= ?`);
    const assignLegacyGeneration = this.db.prepare(`update buffered_events
      set privacy_generation=@generation where rowid=@rawRowid and id=@rawId
        and created_at=@rawCreatedAt and privacy_generation is null
        and not exists (select 1 from buffered_events competing_raw
          where competing_raw.id=@deliveryId and competing_raw.rowid<>@rawRowid)
        and not exists (select 1 from raw_retention_receipts old_raw
          where old_raw.event_id in (@rawId,@deliveryId)
            and (old_raw.raw_rowid is not @rawRowid
              or old_raw.raw_created_at is not @rawCreatedAt
              or old_raw.raw_generation is not @generation))
        and not exists (select 1 from upload_outbox o
          where o.delivery_id=@deliveryId and
            (o.raw_rowid is not @rawRowid or o.raw_id is not @rawId
             or o.raw_created_at is not @rawCreatedAt
             or o.raw_generation is not @generation))`);
    const bind = this.db.prepare(`update upload_receipts set
      raw_rowid=@rawRowid,raw_id=@rawId,raw_created_at=@rawCreatedAt,
      raw_generation=@rawGeneration
      where rowid=@receiptRowid and delivery_id=@deliveryId
        and terminal_state='dead' and created_at=@rawCreatedAt
        and raw_rowid is null and raw_id is null
        and raw_created_at is null and raw_generation is null
        and exists (select 1 from buffered_events b where b.rowid=@rawRowid
          and b.id=@rawId and b.created_at=@rawCreatedAt
          and b.privacy_generation is @rawGeneration)
        and not exists (select 1 from buffered_events competing_raw
          where competing_raw.id=@deliveryId and competing_raw.rowid<>@rawRowid)
        and not exists (select 1 from raw_retention_receipts old_raw
          where old_raw.event_id in (@rawId,@deliveryId)
            and (old_raw.raw_rowid is not @rawRowid
              or old_raw.raw_created_at is not @rawCreatedAt
              or old_raw.raw_generation is not @rawGeneration))
        and not exists (select 1 from upload_outbox o
          where o.delivery_id=@deliveryId and
            (o.raw_rowid is not @rawRowid or o.raw_id is not @rawId
             or o.raw_created_at is not @rawCreatedAt
             or o.raw_generation is not @rawGeneration))`);
    const run = this.db.transaction(() => {
      const cursor = (this.db.prepare(`select cursor_rowid as cursorRowid
        from upload_receipt_lineage_backfill where singleton=1`).get() as ReceiptBackfillControl).cursorRowid;
      const migrationCursor = (this.db.prepare(`select migration_cursor_rowid as n
        from upload_control where singleton=1`).get() as { n: number }).n;
      const receipts = this.db.prepare(`select rowid as receiptRowid,
        delivery_id as deliveryId,created_at as createdAt,
        reason,terminal_at as terminalAt
        from upload_receipts indexed by idx_upload_receipts_raw_lineage
        where terminal_state='dead' and raw_rowid is null and raw_id is null
          and raw_created_at is null and raw_generation is null and rowid>?
        order by rowid limit ?`).all(cursor, maxRows) as Array<{
          receiptRowid: number; deliveryId: string; createdAt: string;
          reason: DeliveryReceiptReason; terminalAt: string;
        }>;
      let visited = 0;
      let bound = 0;
      let lastRowid = cursor;
      for (const receipt of receipts) {
        if (performance.now() >= deadline) break;
        const literal = rawById.get(receipt.deliveryId) as Candidate | undefined;
        const cohort = rawAtTime.all(receipt.createdAt) as Candidate[];
        const candidates = cohort.length > 1024 ? [] :
          cohort.filter((raw) => ensureUuidEventId(raw.rawId).id === receipt.deliveryId);
        if (literal && literal.rawCreatedAt === receipt.createdAt &&
            !candidates.some((raw) => raw.rawRowid === literal.rawRowid)) candidates.push(literal);
        const unique = cohort.length <= 1024 && candidates.length === 1
          ? candidates[0] : undefined;
        // Prefer a stable generation before binding. A conflicting lineage
        // leaves this receipt unbound; a matching old NULL-generation outbox
        // can instead bind NULL and rely on the raw disposition below.
        // All decisions are in the same immediate transaction.
        if (unique?.rawGeneration === null) {
          const generation = crypto.randomUUID();
          if (assignLegacyGeneration.run({
            generation, rawRowid: unique.rawRowid, rawId: unique.rawId,
            rawCreatedAt: unique.rawCreatedAt, deliveryId: receipt.deliveryId,
          }).changes === 1) unique.rawGeneration = generation;
        }
        const changes = unique ? bind.run({
          rawRowid: unique.rawRowid, rawId: unique.rawId,
          rawCreatedAt: unique.rawCreatedAt, rawGeneration: unique.rawGeneration,
          receiptRowid: receipt.receiptRowid, deliveryId: receipt.deliveryId,
        }).changes : 0;
        // Only a successful lineage bind proves that this old privacy decision
        // belongs to the raw. The raw disposition survives later generation
        // assignment if a matching old outbox required a NULL-generation bind.
        if (changes && unique && isTerminalPrivacyReason(receipt.reason)) {
          this.retireLinkedPrivacyDeliveries(unique.rawRowid, unique.rawId,
            unique.rawCreatedAt, unique.rawGeneration, receipt.reason,
            receipt.terminalAt);
        }
        bound += changes;
        if (!changes && candidates.length <= 2 && this.enabled) {
          // A completed raw cursor must not rewind over millions of uploaded
          // rows just because an old receipt is ambiguous. Finish this receipt
          // first, then give only its already-passed eligible raw candidates a
          // collision-safe delivery ID in the same transaction.
          this.db.prepare(`update upload_receipt_lineage_backfill
            set cursor_rowid=?,updated_at=? where singleton=1`)
            .run(receipt.receiptRowid, this.clock().toISOString());
          for (const raw of candidates) {
            if (raw.rawRowid > migrationCursor || raw.uploadedAt !== null) continue;
            const row = pendingCandidate.get(raw.rawRowid, raw.rawId,
              raw.rawCreatedAt, raw.rawGeneration, this.limits.maxItemBytes) as
              RawDeliveryRow | undefined;
            if (!row || row.dataMode !== "metadata" || row.privacyDisposition ||
                row.usageDuplicateReason ||
                (this.workspaceId !== null && row.workspaceId !== this.workspaceId) ||
                (this.deviceId !== null && row.deviceId !== this.deviceId)) continue;
            this.enqueueRaw(row);
          }
        }
        visited += 1;
        lastRowid = receipt.receiptRowid;
      }
      if (visited) this.db.prepare(`update upload_receipt_lineage_backfill
        set cursor_rowid=?,updated_at=? where singleton=1`)
        .run(lastRowid, this.clock().toISOString());
      if (bound && this.db.prepare(`select 1 from sqlite_master
        where type='table' and name='retention_hold_revision'`).get())
        this.db.prepare(`update retention_hold_revision set revision=revision+1
          where singleton=1`).run();
      return { visited, bound };
    });
    const { visited, bound } = run.immediate();
    if (bound) this.onHoldChange?.();
    return { visited, bound, complete: this.receiptLineageComplete() };
  }

  /**
   * Keep the completed legacy watermark truthful without scanning history.
   * `max(rowid)` uses SQLite's integer-primary-key fast path. Buffer appends
   * call noteRawAppend in their own transaction; this reconciliation also
   * catches rollback-compatible/direct raw inserts made while delivery was off.
   */
  private reopenMigrationPastWatermark() {
    this.db
      .prepare(
        `update upload_control set migration_complete = 0,
           migration_paused_reason = null, updated_at = @now
         where singleton = 1 and migration_complete = 1
           and migration_cursor_rowid <
             (select coalesce(max(rowid), 0) from buffered_events)`,
      )
      .run({ now: this.clock().toISOString() });
  }

  noteRawAppend(rawRowid: number) {
    if (!Number.isSafeInteger(rawRowid) || rawRowid <= 0) return;
    const now = this.clock().toISOString();
    if (this.enabled) {
      // A configured append is projected in the same transaction immediately
      // after this call, so an already-complete high-water can advance in O(1).
      this.db
        .prepare(
          `update upload_control set migration_cursor_rowid = max(migration_cursor_rowid, @rawRowid),
             updated_at = @now
           where singleton = 1 and migration_complete = 1`,
        )
        .run({ rawRowid, now });
      return;
    }
    this.db
      .prepare(
        `update upload_control set migration_complete = 0,
           migration_paused_reason = null, updated_at = @now
         where singleton = 1 and migration_complete = 1
           and migration_cursor_rowid < @rawRowid`,
      )
      .run({ rawRowid, now });
  }

  /** Resolve caller-controlled legacy IDs without borrowing another row's receipt. */
  private deliveryIdForRaw(row: RawDeliveryRow) {
    const linked = this.db.prepare(`select delivery_id as id from upload_outbox
      where raw_rowid=? and raw_id=? and raw_created_at=?
        and raw_generation is ? limit 1`).get(
      row.rawRowid, row.rawId, row.createdAt, row.privacyGeneration,
    ) as { id: string } | undefined;
    if (linked) return linked.id;
    const rawById = this.db.prepare(`select id as rawId,created_at as createdAt,
      privacy_generation as privacyGeneration from buffered_events where id=?`);
    const outboxById = this.db.prepare(`select raw_id as rawId,
      raw_created_at as rawCreatedAt,raw_generation as rawGeneration
      from upload_outbox where delivery_id=?`);
    const receiptById = this.db.prepare(`select raw_rowid as rawRowid,raw_id as rawId,
      raw_created_at as rawCreatedAt,raw_generation as rawGeneration
      from upload_receipts where delivery_id=?`);
    const defaultId = ensureUuidEventId(row.rawId).id;
    const ambiguousBaseReceipt = Boolean(this.db.prepare(`select 1 from upload_receipts
      where delivery_id=? and raw_id is null and raw_created_at is null
        and raw_generation is null`).get(defaultId));
    // A bounded per-row probe is resumable with the existing migration cursor.
    // Later incarnations use their generation in the namespace, so repeated
    // reuse does not consume one shared sequence of alternate IDs. The first
    // alternate remains compatible with pre-release legacy-receipt repair.
    // Exhaustion of the collision probes fails closed until a later repair.
    for (let attempt = 0; attempt < 32; attempt++) {
      const id = attempt === 0 ? defaultId :
        ambiguousBaseReceipt && attempt === 1 ? collisionSafeDeliveryId(row.rawId, 1) :
          incarnationDeliveryId(row.rawId, row.createdAt, row.privacyGeneration,
            attempt - (ambiguousBaseReceipt ? 2 : 1));
      const raw = rawById.get(id) as { rawId: string; createdAt: string;
        privacyGeneration: string | null } | undefined;
      if (raw && (raw.rawId !== row.rawId || raw.createdAt !== row.createdAt ||
          raw.privacyGeneration !== row.privacyGeneration)) continue;
      const outbox = outboxById.get(id) as { rawId: string | null;
        rawCreatedAt: string | null; rawGeneration: string | null } | undefined;
      if (outbox && (outbox.rawId !== row.rawId || outbox.rawCreatedAt !== row.createdAt ||
          outbox.rawGeneration !== row.privacyGeneration)) continue;
      const receipt = receiptById.get(id) as {
        rawRowid: number | null; rawId: string | null;
        rawCreatedAt: string | null; rawGeneration: string | null;
      } | undefined;
      if (receipt && (receipt.rawId !== row.rawId ||
          receipt.rawCreatedAt !== row.createdAt || receipt.rawGeneration !== row.privacyGeneration)) {
        // Old receipts have no provable lineage; never let one decide this
        // raw row's privacy or upload fate by a caller-controlled ID alone.
        continue;
      }
      return id;
    }
    return null;
  }

  enqueueRaw(row: RawDeliveryRow) {
    if (!this.enabled || row.uploadedAt) return { enqueued: 0, dead: 0 };
    if (row.privacyDisposition) return { enqueued: 0, dead: 0 };
    if (row.usageDuplicateReason) return { enqueued: 0, dead: 0 };
    this.relocateConflictingPrivacyReceipt({
      deliveryId: ensureUuidEventId(row.rawId).id,
      rawRowid: row.rawRowid, rawId: row.rawId,
      rawCreatedAt: row.createdAt, rawGeneration: row.privacyGeneration,
      deviceId: row.deviceId,
    });
    if (this.hasUnprocessedDeadReceipt(row.rawId))
      return { enqueued: 0, dead: 0 };
    // A pre-upgrade privacy receipt may be ambiguous. It cannot prove raw
    // lineage, but it must still prevent a privacy-rejected raw from escaping.
    if (this.db.prepare(`select 1 from upload_receipts where delivery_id=?
      and raw_rowid is null and raw_id is null and raw_created_at is null
      and raw_generation is null
      and reason in ('local_evidence_quarantined','local_privacy_violation')`)
      .get(ensureUuidEventId(row.rawId).id)) return { enqueued: 0, dead: 0 };
    const deliveryId = this.deliveryIdForRaw(row);
    if (!deliveryId) return { enqueued: 0, dead: 0 };
    const existingReceipt = this.db
      .prepare(`select reason from upload_receipts where delivery_id = ?
        and raw_rowid = ? and raw_id = ? and raw_created_at = ?
        and raw_generation is ?`)
      .get(deliveryId, row.rawRowid, row.rawId, row.createdAt, row.privacyGeneration) as
      { reason: string } | undefined;
    if (
      existingReceipt?.reason === "local_evidence_quarantined" ||
      existingReceipt?.reason === "local_privacy_violation"
    ) {
      const terminalAt = this.clock().toISOString();
      return { enqueued: 0,
        dead: this.retireLinkedPrivacyDeliveries(row.rawRowid, row.rawId,
          row.createdAt, row.privacyGeneration, existingReceipt.reason, terminalAt) };
    }
    const prepared = prepareDelivery({ ...row, payloadJson: persistedGapPayload(this.db, row) }, this.limits.maxItemBytes,
      deliveryId === ensureUuidEventId(row.rawId).id ? undefined : deliveryId);
    if (prepared.ok === false) {
      const terminalAt = this.clock().toISOString();
      let retired = 0;
      if (isTerminalPrivacyReason(prepared.reason)) {
        retired = this.retireLinkedPrivacyDeliveries(row.rawRowid, row.rawId,
          row.createdAt, row.privacyGeneration, prepared.reason, terminalAt);
      }
      return {
        enqueued: 0,
        dead: retired + this.writeReceipt({
          deliveryId: prepared.deliveryId,
          lineage: row,
          state: "dead",
          reason: prepared.reason,
          attemptCount: 0,
          createdAt: row.createdAt,
          terminalAt,
        }),
      };
    }
    if (!row.privacyGeneration) {
      const terminalAt = this.clock().toISOString();
      const retired = this.retireLinkedPrivacyDeliveries(row.rawRowid, row.rawId,
        row.createdAt, row.privacyGeneration, "local_privacy_violation", terminalAt);
      return {
        enqueued: 0,
        dead: retired + this.writeReceipt({
          deliveryId: prepared.deliveryId,
          lineage: row,
          state: "dead",
          reason: "local_privacy_violation",
          attemptCount: 0,
          createdAt: row.createdAt,
          terminalAt,
        }),
      };
    }
    // next_attempt_at is the lease-eligibility gate. It must come from the
    // injectable clock: a wall-clock stamp here can silently fall after a
    // caller's injected lease clock and empty every future claim (issue 0182).
    const nowDate = this.clock();
    const now = nowDate.toISOString();
    // The app server emits the SSE event and response span in separate OTLP
    // batches. Give their first upload one bounded pairing interval; a lone
    // shape remains deliverable when the interval expires.
    let nextAttemptAt = now;
    try {
      const event = (JSON.parse(row.payloadJson) as { source?: string; eventType?: string;
        model?: string; inputTokens?: number; outputTokens?: number; cacheReadTokens?: number; cacheCreationTokens?: number; metadata?: { otelEventName?: string } });
      if ((event.source === "codex" || event.source === "claude_code") && (
          event.source === "codex" && event.eventType === "assistant_response" && (event.metadata?.otelEventName === "codex.sse_event" ||
            event.metadata?.otelEventName === "handle_responses") ||
          unresolvedCapture(event as AiInteractionEvent))) {
        // Repaired/re-enqueued history already waited at capture. Preserve
        // that elapsed wait, while an injected clock bounds a future stamp.
        const capturedAt = Math.min(Date.parse(row.createdAt), nowDate.getTime());
        nextAttemptAt = new Date(Math.max(nowDate.getTime(),
          (Number.isFinite(capturedAt) ? capturedAt : nowDate.getTime()) + CODEX_MODEL_WAIT_MS)).toISOString();
      }
    } catch {
      // prepareDelivery already validates the payload; this is only a grace
      // hint, so an older shape keeps the ordinary upload cadence.
    }
    const inserted = this.db
      .prepare(
        `insert or ignore into upload_outbox
          (delivery_id, raw_rowid, raw_id, raw_created_at, raw_generation, workspace_id, device_id,
           base_envelope_json, base_bytes, repo_hash, branch_hash,
           state, attempt_count, next_attempt_at, last_failure_class, created_at, updated_at)
         select @deliveryId, @rawRowid, @rawId, @createdAt, @privacyGeneration, @workspaceId, @deviceId,
           @baseEnvelopeJson, @baseBytes, @repoHash, @branchHash,
           'pending', 0, @nextAttemptAt, 'none', @createdAt, @now
         where not exists (
           select 1 from upload_receipts where delivery_id = @deliveryId
         )`,
      )
      .run({
        ...prepared,
        rawRowid: row.rawRowid,
        rawId: row.rawId,
        privacyGeneration: row.privacyGeneration,
        workspaceId: row.workspaceId,
        deviceId: row.deviceId,
        createdAt: row.createdAt,
        now,
        nextAttemptAt,
      }).changes;
    return { enqueued: inserted, dead: 0 };
  }

  repairRawById(rawId: string) {
    if (!this.enabled) return { enqueued: 0, dead: 0 };
    const row = this.db
      .prepare(
        `select rowid as rawRowid, id as rawId, created_at as createdAt,
           data_mode as dataMode,
           uploaded_at as uploadedAt, payload_json as payloadJson,
           suppressed_fields_json as suppressedFieldsJson,
           repo_hash as repoHash, branch_hash as branchHash,
           workspace_id as workspaceId,
           device_id as deviceId,
           privacy_generation as privacyGeneration,
           privacy_disposition as privacyDisposition,
           usage_duplicate_reason as usageDuplicateReason
         from buffered_events where id = ?`,
      )
      .get(rawId) as RawDeliveryRow | undefined;
    return row ? this.enqueueRaw(row) : { enqueued: 0, dead: 0 };
  }

  /** Explicit local correction before the first delivery attempt. */
  restampUnsentRaw(rawId: string, payloadJson: string) {
    return this.db.transaction(() => {
      const row = this.db.prepare(`select rowid as rawRowid,id as rawId,created_at as createdAt,
        data_mode as dataMode,uploaded_at as uploadedAt,payload_json as payloadJson,
        suppressed_fields_json as suppressedFieldsJson,repo_hash as repoHash,branch_hash as branchHash,
        workspace_id as workspaceId,device_id as deviceId,privacy_generation as privacyGeneration,
        privacy_disposition as privacyDisposition from buffered_events where id=?`).get(rawId) as RawDeliveryRow|undefined;
      if (!row || row.uploadedAt || row.privacyDisposition || frozenCodexCapture(this.db,rawId)) return false;
      if (hasCaptureGapDecision(this.db, {
        rawRowid: row.rawRowid, rawId: row.rawId, rawCreatedAt: row.createdAt,
        rawGeneration: row.privacyGeneration,
      })) return false;
      const outbox = this.db.prepare(`select delivery_id as deliveryId,state,attempt_count as attemptCount,
        sealed_envelope_json as sealedEnvelopeJson from upload_outbox
        where raw_rowid=? and raw_id=? and raw_created_at=?
          and raw_generation is ?`).get(
        row.rawRowid, row.rawId, row.createdAt, row.privacyGeneration,
      ) as
        { deliveryId: string;state: string;attemptCount: number;sealedEnvelopeJson: string|null }|undefined;
      if (outbox?.sealedEnvelopeJson) {
        const reason = captureGapReason(outbox.sealedEnvelopeJson);
        if (reason) rememberCaptureGap(this.db, rawId, reason);
      }
      // A remote terminal replay must not erase the fact that this delivery
      // was already frozen. Keep restamp conservative even if an old replay
      // reader rebuilt a pending attempt-zero row.
      if (this.db.prepare(`select 1 from upload_replays
        where (delivery_id=? or (raw_id=? and raw_created_at=? and raw_generation is ?))
        limit 1`).get(outbox?.deliveryId ?? ensureUuidEventId(rawId).id,
          row.rawId, row.createdAt, row.privacyGeneration)) return false;
      if (outbox && (outbox.attemptCount !== 0 || outbox.sealedEnvelopeJson !== null || outbox.state !== "pending"))
        return false;
      if (this.db.prepare("select 1 from upload_receipts where delivery_id=?").get(ensureUuidEventId(rawId).id))
        return false;
      const prepared = prepareDelivery({ ...row,payloadJson },this.limits.maxItemBytes,
        outbox?.deliveryId);
      if (!prepared.ok || (outbox && outbox.deliveryId !== prepared.deliveryId))
        throw new Error("dispatch_restamp_envelope_invalid");
      this.db.prepare(`update buffered_events set payload_json=? where rowid=? and id=?
        and created_at=? and privacy_generation is ?`).run(payloadJson,
        row.rawRowid, row.rawId, row.createdAt, row.privacyGeneration);
      if (outbox) this.db.prepare(`update upload_outbox set base_envelope_json=?,base_bytes=?,updated_at=?
        where delivery_id=? and raw_id=? and raw_created_at=?
          and raw_generation is ?`).run(prepared.baseEnvelopeJson,prepared.baseBytes,
        this.clock().toISOString(),outbox.deliveryId,row.rawId,row.createdAt,row.privacyGeneration);
      return true;
    }).immediate();
  }

  /** Replay dead letters written for a remote terminal reason (bead .46).
   *
   * The cloud rejecting an envelope is a statement about the *remote* contract,
   * not about the row: once that contract is fixed the delivery is viable
   * again, but `enqueueRaw` (:720-805) refuses any delivery id that already
   * carries a receipt. Replay makes the supersession explicit — it removes the
   * dead receipt, keeps `upload_control.receipt_dead` exact (the receipt gauge
   * trigger at :583-591 only counts inserts), records the supersession in
   * `upload_replays`, and hands the raw row back to the ordinary enqueue path.
   * Nothing is uploaded here; delivery still happens on normal `upload` cycles,
   * so this is safe to run while a circuit is open.
   *
   * The write transaction is bounded by raw bytes and busy_timeout, not by
   * `--limit`. `--limit` is a budget for the actionable arm only. The inert
   * skip report is unbounded: O(lifetime replays) at ≈2.6 µs/row (measured),
   * four primary-key lookups per inert row (`classified` is referenced twice
   * and SQLite does not materialize it).
   */
  replayDeadLetters(options: {
    reason: string;
    since?: string;
    limit?: number;
    maxBytes?: number;
    dryRun?: boolean;
    now?: Date;
  }): DeliveryReplaySummary {
    const reason = options.reason;
    const dryRun = Boolean(options.dryRun);
    const summary: DeliveryReplaySummary = {
      reason,
      selected: 0,
      requeued: 0,
      skipped: {
        alreadyActive: 0,
        alreadyAcknowledged: 0,
        missingRaw: 0,
        privacyDisposed: 0,
      },
      dryRun,
    };
    if (!isReplayableReceiptReason(reason)) {
      throw new Error(
        `upload-replay refuses reason '${reason}': only remote terminal reasons are replayable ` +
          `(${REPLAYABLE_RECEIPT_REASONS.join(", ")}). Local privacy, quarantine, oversize and ` +
          `schema dead letters are decisions about the row itself and stay final.`,
      );
    }
    if (!this.enabled) return summary;
    const limit = Math.max(1, Math.min(Math.trunc(options.limit ?? 500), 5_000));
    const maxBytes = Math.max(1, Math.trunc(options.maxBytes ?? this.limits.migrationBatchBytes));
    let sinceIso: string | null = null;
    if (options.since !== undefined) {
      const parsed = new Date(options.since);
      if (Number.isNaN(parsed.getTime())) {
        throw new Error(`upload-replay --since expects an ISO-8601 timestamp, got: ${options.since}`);
      }
      sinceIso = parsed.toISOString();
    }
    const nowIso = (options.now ?? this.clock()).toISOString();
    // Fixed internal enum, never caller input: safe to inline as a SQL list.
    const replayableList = REPLAYABLE_RECEIPT_REASONS.map((value) => `'${value}'`).join(", ");

    // How many actionable rows the selection returned, and whether more exist
    // than `--limit`. The hint gates on overflow, never on the inert skip
    // report and never on a pool that is exactly `--limit` rows.
    let candidatesSelected = 0;
    let overflow = false;

    const priorBusyTimeout = this.db.pragma("busy_timeout", { simple: true }) as number;
    this.db.pragma(`busy_timeout = ${REPLAY_BUSY_TIMEOUT_MS}`);
    try {
      // A delivery already replayed no longer has a dead receipt, so the
      // replay ledger is the second half of the candidate set: a repeat run
      // must still see it and count it as skipped rather than report nothing.
      //
      // Those already-replayed rows are inert by construction — the delivery
      // is either acknowledged or already live in the outbox — and they sort
      // by their *first* death, so charging them against the row limit let a
      // lifetime of replays crowd out a dead letter written today and the
      // recovery tool reported a full `selected` while re-queueing nothing
      // (review r1, finding 2). The limit is a budget for work: only the
      // actionable arm is bounded by it. The inert arm is selected unbounded —
      // O(lifetime replays) at ≈2.6 µs/row, four primary-key lookups per row
      // because `classified` is referenced twice and is not materialized — so
      // the skip report is the whole truth rather than a number that silently
      // shrinks with --limit (review r2, finding 2).
      //
      // A delivery that was replayed and then died again under the same reason
      // carries both an `upload_replays.original_terminal_at` and a fresh
      // `upload_receipts.terminal_at`, so the union yields it twice. Grouping
      // the pool by delivery id and keeping the FIRST death collapses it to
      // one row: it costs one slot of the work budget and is counted once
      // (review r2, note 4).
      const rows = this.db
        .prepare(
          `with pool as (
             select deliveryId, min(diedAt) as diedAt from (
               select delivery_id as deliveryId, terminal_at as diedAt
                 from upload_receipts
                where terminal_state = 'dead' and reason = @reason
                  and (@since is null or terminal_at >= @since)
               union all
               select p.delivery_id as deliveryId, p.original_terminal_at as diedAt
                 from upload_replays p
                where p.reason = @reason
                  and (@since is null or p.original_terminal_at >= @since)
                  and not exists (
                    select 1 from upload_receipts r
                     where r.delivery_id = p.delivery_id and r.terminal_state = 'dead'
                       and r.reason not in (${replayableList})
                  )
             )
             group by deliveryId
           ),
           classified as (
             select deliveryId, diedAt,
               case
                 when exists (
                   select 1 from upload_outbox o where o.delivery_id = pool.deliveryId
                 ) then 0
                 when exists (
                   select 1 from upload_receipts r
                    where r.delivery_id = pool.deliveryId
                      and r.terminal_state = 'acknowledged'
                 ) then 0
                 else 1
               end as actionable
             from pool
           )
           select deliveryId, diedAt, actionable from (
             select deliveryId, diedAt, actionable from classified
              where actionable = 1 order by diedAt, deliveryId limit @limit
           )
           union all
           select deliveryId, diedAt, actionable from classified
            where actionable = 0 order by diedAt, deliveryId`,
        )
        .all({ reason, since: sinceIso, limit: limit + 1 }) as Array<{
          deliveryId: string;
          diedAt: string;
          actionable: number;
        }>;
      const selectedActionable = rows.filter((row) => row.actionable === 1);
      overflow = selectedActionable.length > limit;
      const candidates = overflow ? selectedActionable.slice(0, limit) : selectedActionable;
      const inert = rows.filter((row) => row.actionable === 0);
      candidatesSelected = candidates.length;

      const activeStatement = this.db.prepare(
        `select 1 as active from upload_outbox where delivery_id = ?`,
      );
      const receiptStatement = this.db.prepare(
        `select terminal_state as state from upload_receipts where delivery_id = ?`,
      );
      // The delivery id is the normalized event id, which for a captured row is
      // its ledger id. A delivery whose id was derived from a non-UUID raw id
      // (upload-history backfill) cannot be resolved back and is reported as
      // missingRaw rather than guessed at.
      const rawStatement = this.db.prepare(
        `select rowid as rawRowid, id as rawId, created_at as createdAt,
           data_mode as dataMode, uploaded_at as uploadedAt,
           payload_json as payloadJson, suppressed_fields_json as suppressedFieldsJson,
           repo_hash as repoHash, branch_hash as branchHash,
           workspace_id as workspaceId, device_id as deviceId,
           privacy_generation as privacyGeneration, privacy_disposition as privacyDisposition,
           usage_duplicate_reason as usageDuplicateReason,
           length(cast(payload_json as blob)) +
             length(cast(suppressed_fields_json as blob)) as rowBytes
         from buffered_events where id = ?`,
      );

      const classifyAndRequeue = () => {
        let bytes = 0;
        for (const candidate of candidates) {
          if (bytes >= maxBytes) break;
          summary.selected += 1;
          if (activeStatement.get(candidate.deliveryId)) {
            summary.skipped.alreadyActive += 1;
            continue;
          }
          const receipt = receiptStatement.get(candidate.deliveryId) as
            | { state: string }
            | undefined;
          if (receipt?.state === "acknowledged") {
            summary.skipped.alreadyAcknowledged += 1;
            continue;
          }
          const raw = rawStatement.get(candidate.deliveryId) as
            | (RawDeliveryRow & { rowBytes: number })
            | undefined;
          if (!raw) {
            summary.skipped.missingRaw += 1;
            continue;
          }
          if (raw.privacyDisposition || raw.usageDuplicateReason) {
            summary.skipped.privacyDisposed += 1;
            continue;
          }
          if (raw.uploadedAt) {
            summary.skipped.alreadyAcknowledged += 1;
            continue;
          }
          bytes += raw.rowBytes;
          if (dryRun) {
            summary.requeued += 1;
            continue;
          }
          this.supersedeDeadReceipt(candidate.deliveryId, reason, candidate.diedAt, nowIso);
          const outcome = this.enqueueRaw(raw);
          if (outcome.enqueued > 0) {
            const lineage = this.db.prepare(`select frozen_envelope_json as frozenEnvelopeJson,
                frozen_bytes as frozenBytes, frozen_attempt_count as frozenAttemptCount
              from upload_replays where delivery_id=?`).get(candidate.deliveryId) as {
                frozenEnvelopeJson: string | null; frozenBytes: number | null;
                frozenAttemptCount: number | null;
              } | undefined;
            const frozenEnvelopeJson = lineage?.frozenEnvelopeJson;
            if (frozenEnvelopeJson) {
              const restored = this.restoreReplayEnvelope(candidate.deliveryId, {
                ...lineage,
                frozenEnvelopeJson,
              }, nowIso);
              if (!restored) throw new Error("replay_frozen_lineage_missing");
            }
          }
          if (outcome.enqueued > 0) summary.requeued += 1;
          else if (outcome.dead > 0) summary.skipped.privacyDisposed += 1;
          else summary.skipped.missingRaw += 1;
        }
        // Reported so a repeat run is never silent; classified against live
        // state rather than trusting the selection's flag.
        for (const candidate of inert) {
          summary.selected += 1;
          if (activeStatement.get(candidate.deliveryId)) summary.skipped.alreadyActive += 1;
          else summary.skipped.alreadyAcknowledged += 1;
        }
      };
      // A dry run must leave the ledger byte-identical, so it never opens a
      // write transaction.
      if (dryRun) classifyAndRequeue();
      else this.db.transaction(classifyAndRequeue)();
    } finally {
      this.db.pragma(`busy_timeout = ${priorBusyTimeout}`);
    }
    // The hint's advice — narrow with --since, or raise --limit — can only help
    // when the *actionable* arm saturated the row budget *and more rows exist*,
    // so that a different window or a larger limit would reach rows this run
    // could not. Selecting `--limit` of `--limit` (the whole pool) is not
    // saturation. Gating on `selected` instead fired it in the healthy steady
    // state, where the limit was filled by inert rows that the same sentence
    // says never consume it (review r2, finding 1).
    if (summary.requeued === 0 && overflow) {
      summary.hint =
        `selected ${candidatesSelected} actionable candidates and re-queued none at ` +
        `--limit ${limit}: narrow the window with --since <ISO-8601> or raise --limit. ` +
        "Already-replayed deliveries are reported as skipped but never consume the limit.";
    }
    return summary;
  }

  private supersedeDeadReceipt(
    deliveryId: string,
    reason: string,
    diedAt: string,
    nowIso: string,
  ) {
    const removed = this.db
      .prepare(`delete from upload_receipts where delivery_id = ? and terminal_state = 'dead'`)
      .run(deliveryId).changes;
    if (removed > 0) {
      this.db
        .prepare(
          `update upload_control set receipt_dead = max(0, receipt_dead - @removed),
             updated_at = @now
           where singleton = 1`,
        )
        .run({ removed, now: nowIso });
    }
    this.db
      .prepare(
        `insert into upload_replays
           (delivery_id, reason, original_terminal_at, replayed_at, replay_count)
         values (@deliveryId, @reason, @diedAt, @now, 1)
         on conflict(delivery_id) do update set
           reason = excluded.reason,
           replayed_at = excluded.replayed_at,
           replay_count = upload_replays.replay_count + 1`,
      )
      .run({ deliveryId, reason, diedAt, now: nowIso });
  }

  private rememberReplayLineage(input: {
    deliveryId: string;
    rawRowid: number | null;
    rawId: string | null;
    rawCreatedAt: string | null;
    rawGeneration: string | null;
    frozenEnvelopeJson: string;
    frozenAttemptCount: number;
    terminalAt: string;
  }) {
    const frozenBytes = Buffer.byteLength(input.frozenEnvelopeJson);
    this.db.prepare(`insert into upload_replays
      (delivery_id,reason,original_terminal_at,replayed_at,replay_count,
       raw_rowid,raw_id,raw_created_at,raw_generation,frozen_envelope_json,
       frozen_bytes,frozen_attempt_count)
      values (?, 'remote_validation_rejected', ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
      on conflict(delivery_id) do update set
        raw_rowid=coalesce(upload_replays.raw_rowid,excluded.raw_rowid),
        raw_id=coalesce(upload_replays.raw_id,excluded.raw_id),
        raw_created_at=coalesce(upload_replays.raw_created_at,excluded.raw_created_at),
        raw_generation=coalesce(upload_replays.raw_generation,excluded.raw_generation),
        frozen_envelope_json=coalesce(upload_replays.frozen_envelope_json,excluded.frozen_envelope_json),
        frozen_bytes=coalesce(upload_replays.frozen_bytes,excluded.frozen_bytes),
        frozen_attempt_count=coalesce(upload_replays.frozen_attempt_count,excluded.frozen_attempt_count)`).run(
      input.deliveryId, input.terminalAt, input.terminalAt, input.rawRowid, input.rawId,
      input.rawCreatedAt, input.rawGeneration, input.frozenEnvelopeJson, frozenBytes,
      input.frozenAttemptCount,
    );
  }

  private restoreReplayEnvelope(
    deliveryId: string,
    lineage: { frozenEnvelopeJson: string; frozenBytes: number | null; frozenAttemptCount: number | null },
    nowIso: string,
  ) {
    const bytes = lineage.frozenBytes ?? Buffer.byteLength(lineage.frozenEnvelopeJson);
    return this.db.prepare(`update upload_outbox set
      base_envelope_json=?, base_bytes=?, sealed_envelope_json=?, sealed_bytes=?,
      attempt_count=max(attempt_count,?), state='pending', lease_id=null,
      lease_expires_at=null, next_attempt_at=?, updated_at=? where delivery_id=?`).run(
      lineage.frozenEnvelopeJson, bytes, lineage.frozenEnvelopeJson, bytes,
      lineage.frozenAttemptCount ?? 1, nowIso, nowIso, deliveryId,
    ).changes === 1;
  }

  fillLinkageForRawRow(rawRowid: number, repoHash: string | null, branchHash: string | null) {
    if (!this.enabled) return 0;
    const fill = () => {
      const raw = this.db.prepare(`select id as rawId,created_at as rawCreatedAt,
        privacy_generation as rawGeneration from buffered_events where rowid=?`)
        .get(rawRowid) as { rawId: string; rawCreatedAt: string;
          rawGeneration: string | null } | undefined;
      if (!raw) return 0;
      return this.db
      .prepare(
        `update upload_outbox set
           repo_hash = coalesce(repo_hash, @repoHash),
           branch_hash = coalesce(branch_hash, @branchHash),
           updated_at = @now
         where raw_rowid = @rawRowid and raw_id = @rawId
           and raw_created_at = @rawCreatedAt
           and raw_generation is @rawGeneration
           and sealed_envelope_json is null and attempt_count = 0`,
      )
      .run({
        rawRowid,
        rawId: raw.rawId,
        rawCreatedAt: raw.rawCreatedAt,
        rawGeneration: raw.rawGeneration,
        repoHash: canonicalLinkage(repoHash),
        branchHash: canonicalLinkage(branchHash),
        now: this.clock().toISOString(),
      }).changes;
    };
    return this.db.inTransaction ? fill() : this.db.transaction(fill).immediate();
  }

  migrateLegacy(options: { maxRows?: number; maxBytes?: number; maxWriterMs?: number; now?: Date } = {}) {
    const receiptBackfill = this.backfillLegacyReceiptLineage({
      maxRows: Math.min(options.maxRows ?? 256, 256),
      maxWriterMs: Math.min(options.maxWriterMs ?? 100, 100),
    });
    if (!this.enabled) return { visited: 0, enqueued: 0, dead: 0, skippedUploaded: 0, quarantinedEvidence: 0, complete: false, paused: null };
    const now = options.now ?? new Date();
    const nowIso = now.toISOString();
    const maxRows = Math.max(1, Math.min(Math.trunc(options.maxRows ?? this.limits.migrationBatchRows), 5_000));
    // The daemon bounds each writer turn below the OTLP 750 ms retry window.
    // Start the migration turn's clock after the candidate read: a cold large
    // ledger must not spend the whole writer budget on read-only work.
    const writerBudgetMs = options.maxWriterMs === undefined
      ? undefined
      : Math.max(1, Math.min(Math.trunc(options.maxWriterMs), 1_000));
    const lineageDead = this.db.transaction(() =>
      this.quarantineUnprovenLineage(Math.min(maxRows, 500), nowIso,
        writerBudgetMs === undefined ? undefined : performance.now() + writerBudgetMs),
    )();
    const pressure = this.status(now).pressure;
    if (pressure.degraded) {
      this.db
        .prepare(
          `update upload_control set migration_paused_reason = 'pressure', updated_at = ?
           where singleton = 1`,
        )
        .run(nowIso);
      return { visited: 0, enqueued: 0, dead: lineageDead, skippedUploaded: 0, quarantinedEvidence: 0, complete: false, paused: "pressure" as const };
    }

    const control = this.db
      .prepare(
        `select migration_cursor_rowid as cursorRowid, migration_complete as complete
         from upload_control where singleton = 1`,
      )
      .get() as { cursorRowid: number; complete: number };
    if (control.complete) {
      return { visited: 0, enqueued: 0, dead: lineageDead, skippedUploaded: 0,
        quarantinedEvidence: 0, complete: this.receiptLineageComplete(),
        paused: receiptBackfill.complete ? null : "receipt_lineage_pending" as const };
    }

    const maxBytes = Math.max(1, Math.trunc(options.maxBytes ?? this.limits.migrationBatchBytes));
    const rows = this.db
      .prepare(
        `select rowid as rawRowid, id as rawId, created_at as createdAt,
           data_mode as dataMode, uploaded_at as uploadedAt,
           workspace_id as workspaceId, device_id as deviceId,
           length(cast(payload_json as blob)) +
             length(cast(suppressed_fields_json as blob)) as rowBytes
         from buffered_events
         where rowid > ?
         order by rowid asc
         limit ?`,
      )
      .all(control.cursorRowid, maxRows) as LegacyCandidateRow[];

    let visited = 0;
    let bytes = 0;
    let enqueued = 0;
    let dead = lineageDead;
    let skippedUploaded = 0;
    let quarantinedEvidence = 0;
    let cursor = control.cursorRowid;
    let firstDeferredRowid: number | null = null;
    let paused: "slice_budget_too_small" | "receipt_lineage_pending" | null = null;
    let writerBudgetExhausted = false;
    const readRaw = this.db.prepare(
      `select rowid as rawRowid, id as rawId, created_at as createdAt,
         data_mode as dataMode,
         uploaded_at as uploadedAt, payload_json as payloadJson,
         suppressed_fields_json as suppressedFieldsJson,
         repo_hash as repoHash, branch_hash as branchHash,
         workspace_id as workspaceId, device_id as deviceId,
         privacy_generation as privacyGeneration,
         privacy_disposition as privacyDisposition,
         usage_duplicate_reason as usageDuplicateReason,
         length(cast(payload_json as blob)) +
           length(cast(suppressed_fields_json as blob)) as rowBytes
       from buffered_events where rowid = ? and id = ? and created_at = ?`,
    );
    const assignLegacyGeneration = this.db.prepare(
      `update buffered_events set privacy_generation = @privacyGeneration
       where rowid = @rawRowid and id = @rawId and created_at = @createdAt
         and privacy_generation is null`,
    );
    const run = this.db.transaction(() => {
      const writerDeadline = writerBudgetMs === undefined ? undefined : performance.now() + writerBudgetMs;
      for (const candidate of rows) {
        if (writerDeadline !== undefined && performance.now() >= writerDeadline) {
          writerBudgetExhausted = true;
          break;
        }
        visited += 1;
        cursor = candidate.rawRowid;
        assignLegacyGeneration.run({
          rawRowid: candidate.rawRowid,
          rawId: candidate.rawId,
          createdAt: candidate.createdAt,
          privacyGeneration: crypto.randomUUID(),
        });
        const row = readRaw.get(candidate.rawRowid, candidate.rawId,
          candidate.createdAt) as
          | (RawDeliveryRow & { rowBytes: number })
          | undefined;
        if (!row) continue;
        const rowBytes = row.rowBytes ?? 0;
        if (this.workspaceId !== null && row.workspaceId !== this.workspaceId) {
          continue;
        }
        if (this.deviceId !== null && row.deviceId !== this.deviceId) {
          continue;
        }
        if (row.uploadedAt) {
          skippedUploaded += 1;
          continue;
        }
        if (row.dataMode === "evidence") {
          quarantinedEvidence += 1;
          // A stale build may already have cached one or more envelopes for
          // this row under different delivery ids. Retire a bounded indexed
          // slice in the same transaction; any remainder is independently
          // rejected by the lease boundary's raw-row point lookup.
          dead += this.quarantineLinkedEvidence(
            row.rawRowid, row.rawId, row.createdAt,
            row.privacyGeneration,
            nowIso,
          );
          dead += this.writeReceipt({
            deliveryId: ensureUuidEventId(row.rawId).id,
            lineage: row,
            state: "dead",
            reason: "local_evidence_quarantined",
            attemptCount: 0,
            createdAt: row.createdAt,
            terminalAt: nowIso,
          });
          continue;
        }
        if (row.privacyDisposition || row.usageDuplicateReason) continue;
        if (this.hasUnprocessedDeadReceipt(row.rawId)) {
          firstDeferredRowid = Math.min(firstDeferredRowid ?? row.rawRowid, row.rawRowid);
          continue;
        }
        // A pre-outbox legacy row can be arbitrarily large. Classify a row
        // already above the item ceiling from its SQLite length metadata;
        // never materialize it into the migration process merely to reject it.
        if (rowBytes > this.limits.maxItemBytes) {
          dead += this.writeReceipt({
            deliveryId: ensureUuidEventId(row.rawId).id,
            lineage: row,
            state: "dead",
            reason: "local_item_oversize",
            attemptCount: 0,
            createdAt: row.createdAt,
            terminalAt: nowIso,
          });
          continue;
        }
        // A maintenance budget is not an item-validity boundary. Preserve an
        // otherwise deliverable row, expose an actionable degraded pause, and
        // resume once the operator raises the slice budget. The header-only
        // length check keeps repeated paused cycles bounded and never loads it.
        if (rowBytes > maxBytes) {
          visited -= 1;
          cursor = Math.max(control.cursorRowid, candidate.rawRowid - 1);
          paused = "slice_budget_too_small";
          break;
        }
        if (bytes > 0 && bytes + rowBytes > maxBytes) {
          visited -= 1;
          cursor = candidate.rawRowid - 1;
          break;
        }
        bytes += rowBytes;
        const result = this.enqueueRaw(row);
        enqueued += result.enqueued;
        dead += result.dead;
      }
      if (firstDeferredRowid !== null) {
        cursor = Math.min(cursor, firstDeferredRowid - 1);
        paused = "receipt_lineage_pending";
      }
      const complete = !writerBudgetExhausted && paused === null &&
        this.receiptLineageComplete() && rows.length < maxRows && visited === rows.length;
      this.db
        .prepare(
          `update upload_control set
             migration_cursor_rowid = @cursor,
             migration_complete = @complete,
             migration_paused_reason = @paused,
             migration_last_visited = @visited,
             migration_last_bytes = @bytes,
             migration_last_enqueued = @enqueued,
             migration_last_dead = @dead,
             migration_last_skipped_uploaded = @skippedUploaded,
             migration_last_at = @now,
             updated_at = @now
           where singleton = 1`,
        )
        .run({
          cursor,
          complete: complete ? 1 : 0,
          visited,
          bytes,
          enqueued,
          dead,
          skippedUploaded,
          paused,
          now: nowIso,
        });
      return complete;
    });
    const complete = run();
    return { visited, bytes, enqueued, dead, skippedUploaded, quarantinedEvidence, complete, paused };
  }

  /** Pin the admitted live result before a native reader consumes its
   * cumulative counters as covered. This neither leases nor uploads, and
   * leaves next_attempt_at (the 60-second transport hold) unchanged. */
  freezeSessionCoverage(rawId: string): boolean {
    const freeze = () => {
      if (frozenCodexCapture(this.db,rawId)) return true;
      const raw=this.db.prepare(`select payload_json as payload,uploaded_at as uploadedAt,
        suppressed_fields_json as suppressed,repo_hash as repo,branch_hash as branch from buffered_events where id=?`)
        .get(rawId) as {payload:string;uploadedAt:string|null;suppressed:string;repo:string|null;branch:string|null}|undefined;
      if(!raw)return false;
      const captured=captureCodexModel(this.db,aiInteractionEventSchema.parse(JSON.parse(raw.payload)),rawId,false,false);
      if(!codexHasUsage(captured)||isCaptureGap(captured))return false;
      // Coverage retains qualified old ACK fields directly. Their released
      // readers discarded the envelope: acknowledge custody, without making
      // up bytes or a new named witness from the raw diagnostic payload.
      if(raw.uploadedAt)return captured.metadata.modelCaptureSource==="legacy_native_acknowledged";
      const row=this.db.prepare(`select delivery_id as deliveryId,base_envelope_json as base,
        sealed_envelope_json as sealed,repo_hash as repo,branch_hash as branch from upload_outbox
        where raw_id=? and raw_rowid=(select rowid from buffered_events where id=?)
          and raw_created_at=(select created_at from buffered_events where id=?)
          and raw_generation=(select privacy_generation from buffered_events where id=?) limit 1`)
        .get(rawId,rawId,rawId,rawId) as {deliveryId:string;base:string;sealed:string|null;repo:string|null;branch:string|null}|undefined;
      if(!row) {
        // Stateless capture can retain accounting before an outbox exists.
        // Freeze the same native/privacy-validated envelope its snapshot would
        // send. An enabled but pending queue, or an older terminal delivery,
        // cannot acquire fabricated replacement bytes through this fallback.
        if(this.enabled||this.db.prepare(`select 1 from upload_receipts where raw_id=? or delivery_id=? limit 1`)
          .get(rawId,rawId))return false;
        const attribution=new SessionAttributionBatch(this.db,[{event:captured,repoHash:canonicalLinkage(raw.repo)}]);
        const sealed=sealOutboundEnvelope(attachFillOnlyLinkage({event:captured,suppressedFields:JSON.parse(raw.suppressed)},
          canonicalLinkage(raw.repo),canonicalLinkage(raw.branch),attribution,new Set()));
        if(!sealed.ok)throw new Error("codex_usage_coverage_seal_refused");
        const bytes=JSON.stringify(sealed.envelope);
        if(Buffer.byteLength(bytes)>this.limits.maxItemBytes)throw new Error("codex_usage_coverage_item_oversize");
        rememberFrozenCodexCapture(this.db,rawId,sealed.envelope.event.id,bytes,captured);
        return true;
      }
      if(row.sealed) {
        const prior=aiWorkIngestEventSchema.parse(JSON.parse(row.sealed));
        if(isCaptureGap(prior.event))return false;
        rememberFrozenCodexCapture(this.db,rawId,row.deliveryId,row.sealed,captured);
        return true;
      }
      const base=aiWorkIngestEventSchema.parse(JSON.parse(row.base));
      const attribution=new SessionAttributionBatch(this.db,[{event:captured,repoHash:canonicalLinkage(row.repo)}]);
      const sealed=sealOutboundEnvelope(attachFillOnlyLinkage({...base,event:{...captured,id:base.event.id}},
        canonicalLinkage(row.repo),canonicalLinkage(row.branch),attribution,new Set()));
      if(!sealed.ok)throw new Error("codex_usage_coverage_seal_refused");
      const bytes=JSON.stringify(sealed.envelope);
      if(Buffer.byteLength(bytes)>this.limits.maxItemBytes)throw new Error("codex_usage_coverage_item_oversize");
      this.db.prepare(`update upload_outbox set sealed_envelope_json=?,sealed_bytes=?
        where delivery_id=? and sealed_envelope_json is null`).run(bytes,Buffer.byteLength(bytes),row.deliveryId);
      rememberFrozenCodexCapture(this.db,rawId,row.deliveryId,bytes,captured);
      return true;
    };
    return this.db.inTransaction?freeze():this.db.transaction(freeze).immediate();
  }

  reconcileCodexResponse(rawId: string) {
    const reconcile = () => applyCodexResponseCoverage(this.db,rawId,id => this.freezeSessionCoverage(id));
    return this.db.inTransaction ? reconcile() : this.db.transaction(reconcile).immediate();
  }

  lease(options: { maxRows?: number; maxBytes?: number; now?: Date; leaseId?: string } = {}): DeliveryLease {
    if (!this.enabled) return { leaseId: "", items: [], locallyDead: 0, blockedBy: "none" };
    const now = options.now ?? new Date();
    const nowIso = now.toISOString();
    const control = this.db
      .prepare(
        `select circuit_kind as kind, circuit_until as until
         from upload_control where singleton = 1`,
      )
      .get() as { kind: DeliveryCircuit; until: string | null };
    if (control.kind !== "none" && control.until && control.until > nowIso) {
      return { leaseId: "", items: [], locallyDead: 0, blockedBy: control.kind };
    }
    if (control.kind !== "none") this.clearCircuit(now);

    const maxRows = Math.max(1, Math.min(Math.trunc(options.maxRows ?? 500), 500));
    const maxBytes = Math.max(1, Math.trunc(options.maxBytes ?? 1_500_000));
    const leaseId = options.leaseId ?? crypto.randomUUID();
    const leaseExpiresAt = new Date(now.getTime() + this.limits.leaseSeconds * 1_000).toISOString();
    const items: LeasedDeliveryItem[] = [];
    let locallyDead = 0;
    let selectedBytes = 0;

    const run = this.db.transaction(() => {
      this.db
        .prepare(
          `update upload_outbox set state = 'retry', next_attempt_at = @now,
             lease_id = null, lease_expires_at = null, updated_at = @now
           where state = 'in_flight' and lease_expires_at <= @now`,
        )
        .run({ now: nowIso });
      const candidates = this.db
        .prepare(
          `select delivery_id as deliveryId, raw_rowid as rawRowid,
             raw_id as rawId, raw_created_at as rawCreatedAt,
             raw_generation as rawGeneration,
             base_envelope_json as baseEnvelopeJson,
             (select payload_json from buffered_events raw
               where raw.rowid = upload_outbox.raw_rowid and raw.id is upload_outbox.raw_id
                 and raw.created_at is upload_outbox.raw_created_at
                 and raw.privacy_generation is upload_outbox.raw_generation) as rawPayloadJson,
             sealed_envelope_json as sealedEnvelopeJson,
             repo_hash as repoHash, branch_hash as branchHash,
             device_id as deviceId,
             attempt_count as attemptCount
           from upload_outbox
           where state in ('pending','retry') and next_attempt_at <= @now
             and not exists (select 1 from claude_replay_hooks held
               where held.event_id=upload_outbox.raw_id and held.status='pending')
             -- Fail closed (#163 rework): a null workspace binding claims
             -- ONLY unassigned rows, never rows bound to any workspace.
             and workspace_id is @workspaceId
             and device_id is @deviceId
           order by case
               when exists (
                 select 1 from upload_validation_candidates c
                 where c.delivery_id = upload_outbox.delivery_id
               ) then 2
               when last_failure_class in ('remote_validation', 'remote_rejected', 'local_request_budget') then 1
               else 0
             end,
             next_attempt_at, created_at, delivery_id
           limit @maxRows`,
        )
        .all({ now: nowIso, workspaceId: this.workspaceId, deviceId: this.deviceId, maxRows }) as ActiveDeliveryRow[];

      // Session inheritance is applied here, where envelopes are sealed. Parse
      // every unsealed envelope first so the batch plans one bounded lookup
      // per session instead of one ledger scan per token row.
      const unsealed = new Map<string, AiWorkIngestEvent | null>();
      for (const row of candidates) {
        if (row.sealedEnvelopeJson) continue;
        if (row.rawId && row.rawPayloadJson && this.reconcileCodexResponse(row.rawId)) {
          const current = this.db.prepare(`select base_envelope_json as base,sealed_envelope_json as sealed,
            (select payload_json from buffered_events raw where raw.rowid=upload_outbox.raw_rowid
              and raw.id is upload_outbox.raw_id and raw.created_at is upload_outbox.raw_created_at
              and raw.privacy_generation is upload_outbox.raw_generation) as raw
              from upload_outbox where delivery_id=?`)
            .get(row.deliveryId) as {base:string;sealed:string|null;raw:string|null}|undefined;
          if (!current) continue;
          row.baseEnvelopeJson=current.base;row.rawPayloadJson=current.raw;row.sealedEnvelopeJson=current.sealed;
        }
        try {
          unsealed.set(row.deliveryId, aiWorkIngestEventSchema.parse(JSON.parse(row.baseEnvelopeJson)));
        } catch {
          unsealed.set(row.deliveryId, null);
        }
      }
      const attribution = new SessionAttributionBatch(
        this.db,
        candidates.flatMap((row) => {
          const parsed = unsealed.get(row.deliveryId);
          return parsed && parsed.event.dataMode !== "evidence"
            ? [{ event: parsed.event, repoHash: canonicalLinkage(row.repoHash) }]
            : [];
        }),
      );
      // Raw rows this pass privacy-disposes stop counting as session context
      // for the rest of the pass, as a fresh per-row query would see them.
      const disposedRawRowids = new Set<number>();

      for (const row of candidates) {
        if (!this.db.prepare("select 1 from upload_outbox where delivery_id=?").get(row.deliveryId)) continue;
        const authoritativeReason = this.authoritativePrivacyReason(row);
        if (authoritativeReason === "lineage_unresolved") continue;
        if (authoritativeReason) {
          locallyDead += this.deadActive(row.deliveryId, authoritativeReason, nowIso, disposedRawRowids);
          continue;
        }
        const namedWitness = frozenCodexDelivery(this.db,row);
        let envelopeJson = row.sealedEnvelopeJson ??
          namedWitness?.envelopeJson;
        if (!row.sealedEnvelopeJson && envelopeJson) this.db.prepare(`update upload_outbox set
          sealed_envelope_json=?,sealed_bytes=?,updated_at=? where delivery_id=? and sealed_envelope_json is null`)
          .run(envelopeJson,Buffer.byteLength(envelopeJson),nowIso,row.deliveryId);
        if (!envelopeJson) {
          const parsed = unsealed.get(row.deliveryId);
          if (!parsed) {
            locallyDead += this.deadActive(row.deliveryId, "local_schema_invalid", nowIso, disposedRawRowids);
            continue;
          }
          if (parsed.event.dataMode === "evidence") {
            locallyDead += this.deadActive(
              row.deliveryId,
              "local_evidence_quarantined",
              nowIso,
              disposedRawRowids,
            );
            continue;
          }
          // Sealed base envelopes intentionally omit local capture diagnostics
          // for rollback readers. Re-read the immutable raw payload here so
          // turn IDs and nested evidence still participate in capture; only
          // the resulting outbound event is sealed below.
          let captureInput = parsed.event;
          // A retired uncertain delivery is replaced by an explicit gap. Its
          // raw lineage is retained for diagnostics, but rereading it here
          // would resurrect the counters under the replacement ID.
          if (!isCaptureGap(parsed.event) && row.rawPayloadJson) {
            try {
              captureInput = aiInteractionEventSchema.parse(JSON.parse(row.rawPayloadJson));
            } catch {
              captureInput = parsed.event;
            }
          }
          const captured = isCaptureGap(parsed.event)
            ? parsed.event
            : codexHasUsage(captureInput) && !row.rawPayloadJson
            ? codexModelGap(this.db,captureInput,"capture_row_missing")
            : captureCodexModel(this.db, captureInput, row.rawId ?? parsed.event.id, true);
          if (isCaptureGap(parsed.event) && row.rawId) {
            rememberDeliveryCaptureGap(this.db, row,
              String(parsed.event.metadata.modelGapReason ?? "legacy_capture_gap"));
          }
          parsed.event = { ...captured, id: parsed.event.id };
          const sealed = sealOutboundEnvelope(
            attachFillOnlyLinkage(
              parsed,
              canonicalLinkage(row.repoHash),
              canonicalLinkage(row.branchHash),
              attribution,
              disposedRawRowids,
            ),
          );
          if (!sealed.ok) {
            locallyDead += this.deadActive(
              row.deliveryId,
              sealed.reason === "schema" ? "local_schema_invalid" : "local_privacy_violation",
              nowIso,
              disposedRawRowids,
            );
            continue;
          }
          envelopeJson = JSON.stringify(sealed.envelope);
          const envelopeBytes = Buffer.byteLength(envelopeJson);
          if (envelopeBytes > this.limits.maxItemBytes) {
            locallyDead += this.deadActive(row.deliveryId, "local_item_oversize", nowIso, disposedRawRowids);
            continue;
          }
          this.db
            .prepare(
              `update upload_outbox set sealed_envelope_json = @envelopeJson,
                 sealed_bytes = @envelopeBytes, updated_at = @now
               where delivery_id = @deliveryId and sealed_envelope_json is null`,
            )
            .run({ deliveryId: row.deliveryId, envelopeJson, envelopeBytes, now: nowIso });
          rememberFrozenCodexCapture(this.db,row.rawId ?? parsed.event.id,row.deliveryId,envelopeJson,captured);
        }
        // Older builds may already have sealed an evidence-marked item. The
        // sealed copy is not trusted merely because it predates this gate.
        let outboundEnvelope: AiWorkIngestEvent;
        try {
          outboundEnvelope = aiWorkIngestEventSchema.parse(JSON.parse(envelopeJson));
        } catch {
          locallyDead += this.deadActive(row.deliveryId, "local_schema_invalid", nowIso, disposedRawRowids);
          continue;
        }
        // A gap sealed by an older binary has no r4 decision-table row yet.
        // Import the accounting decision from the frozen envelope before any
        // terminal deletion or replay can remove that envelope.
        if (row.rawId) {
          const reason = captureGapReason(envelopeJson);
          if (reason) rememberDeliveryCaptureGap(this.db, row, reason);
        }
        if (outboundEnvelope.event.dataMode === "evidence") {
          locallyDead += this.deadActive(
            row.deliveryId,
            "local_evidence_quarantined",
            nowIso,
            disposedRawRowids,
          );
          continue;
        }
        const revalidated = sealOutboundEnvelope(outboundEnvelope);
        if (!revalidated.ok || JSON.stringify(revalidated.envelope) !== envelopeJson) {
          locallyDead += this.deadActive(row.deliveryId, "local_privacy_violation", nowIso, disposedRawRowids);
          continue;
        }
        let sealedOriginGap = false;
        if (row.sealedEnvelopeJson && codexHasUsage(outboundEnvelope.event)) {
          if (namedWitness?.envelopeJson === envelopeJson) {
            // Complete lineage and frozen bytes identify the admitted result,
            // even if raw retention has since removed its diagnostic row.
          } else if (!row.rawId || !row.rawPayloadJson) {
            sealedOriginGap = true;
          } else {
            try {
              const rawLineage = aiInteractionEventSchema.parse(JSON.parse(row.rawPayloadJson));
              const frozen = frozenCodexCapture(this.db,row.rawId);
              if (frozen?.deliveryId === row.deliveryId && frozen.envelopeJson === envelopeJson) {
                // This exact native result was validated when these bytes
                // froze. Later evidence governs new captures, not this ID.
              } else if (!hasCaptureGapDecision(this.db, {
                rawRowid: row.rawRowid!,rawId: row.rawId,
                rawCreatedAt: row.rawCreatedAt!,rawGeneration: row.rawGeneration,
              }) && legacyFrozenNativeCapture(rawLineage,outboundEnvelope.event)) {
                rememberFrozenCodexCapture(this.db,row.rawId,row.deliveryId,envelopeJson,
                  {...outboundEnvelope.event,metadata:{...outboundEnvelope.event.metadata,modelCaptureSource:"legacy_native_frozen"}});
              } else if (isCaptureGap(rawLineage)) {
                rememberCaptureGap(this.db, row.rawId,
                  String(rawLineage.metadata.modelGapReason ?? "legacy_capture_gap"));
                sealedOriginGap = true;
              } else if (codexHasUsage(rawLineage)) {
                const validated = captureCodexModel(this.db, rawLineage, row.rawId, true);
                sealedOriginGap = isCaptureGap(validated) ||
                  typeof validated.model !== "string" || !validated.model.trim() ||
                  validated.model !== outboundEnvelope.event.model;
                if (!sealedOriginGap) rememberFrozenCodexCapture(this.db,row.rawId,row.deliveryId,envelopeJson,validated);
              } else {
                sealedOriginGap = true;
              }
            } catch {
              sealedOriginGap = true;
            }
          }
        }
        if (row.sealedEnvelopeJson && sealedOriginGap) {
          // An old, once-attempted request may already have committed remotely.
          // Never rewrite its frozen bytes or retry unknown billable usage.
          // Retire it and send a distinct tokenless gap with the same raw lineage.
          const owner = this.db.prepare(`select installation_epoch_id as epoch from buffered_events where
            rowid=? and id is ? and created_at is ? and privacy_generation is ?`).get(
              row.rawRowid,row.rawId,row.rawCreatedAt,row.rawGeneration) as {epoch:string|null}|undefined;
          const prior = {...outboundEnvelope.event,metadata:{...outboundEnvelope.event.metadata,
            ...(owner?.epoch ? {installationEpochId:owner.epoch} : {})}};
          let gapId: string | undefined;
          if (row.rawId && row.rawCreatedAt && row.rawGeneration) for (let attempt=0;attempt<32;attempt++) {
            const id=incarnationDeliveryId(row.rawId,row.rawCreatedAt,row.rawGeneration,attempt);
            if (!this.db.prepare(`select 1 from buffered_events where id=? union all
              select 1 from upload_outbox where delivery_id=? union all
              select 1 from upload_receipts where delivery_id=? union all
              select 1 from upload_replays where delivery_id=? union all
              select 1 from upload_validation_candidates where delivery_id=? limit 1`).get(id,id,id,id,id)) {gapId=id;break;}
          }
          if (!gapId) {
            locallyDead += this.deadActive(row.deliveryId,"local_model_capture_gap",nowIso,disposedRawRowids);
            codexModelGap(this.db,prior,"legacy_gap_identity_unavailable");
            continue;
          }
          const gap = sealOutboundEnvelope({...outboundEnvelope,event:{
            ...codexModelGap(this.db,prior,codexMisfiledUnderClaude(prior) ? "legacy_sealed_source_mismatch" :
              prior.model ? "legacy_sealed_model_evidence_conflict" : "legacy_sealed_model_missing"),id:gapId}});
          if (!gap.ok) {
            locallyDead += this.deadActive(row.deliveryId,"local_schema_invalid",nowIso,disposedRawRowids);
            continue;
          }
          const gapJson=JSON.stringify(gap.envelope),gapBytes=Buffer.byteLength(gapJson);
          locallyDead += this.deadActive(row.deliveryId,"local_model_capture_gap",nowIso,disposedRawRowids);
          if (gapBytes <= this.limits.maxItemBytes) this.db.prepare(`insert or ignore into upload_outbox
            (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,workspace_id,device_id,
             base_envelope_json,base_bytes,repo_hash,branch_hash,state,attempt_count,next_attempt_at,
             last_failure_class,created_at,updated_at)
            select ?,?,?,?,?,?,?,?,?,?,?,'pending',0,?,'none',?,?
            where not exists(select 1 from upload_receipts where delivery_id=?)`).run(
              gapId,row.rawRowid,row.rawId,row.rawCreatedAt,row.rawGeneration,this.workspaceId,row.deviceId,
              gapJson,gapBytes,row.repoHash,row.branchHash,nowIso,row.rawCreatedAt??nowIso,nowIso,gapId);
          continue;
        }
        const envelopeBytes = Buffer.byteLength(envelopeJson);
        const addedBytes = envelopeBytes + (items.length > 0 ? 1 : 0);
        if (items.length > 0 && selectedBytes + addedBytes > maxBytes) break;
        selectedBytes += addedBytes;
        const attemptCount = row.attemptCount + 1;
        if (row.rawId) rememberCodexSpanEmission(this.db, row.rawId, outboundEnvelope.event);
        this.db
          .prepare(
            `update upload_outbox set state = 'in_flight', attempt_count = @attemptCount,
               lease_id = @leaseId, lease_expires_at = @leaseExpiresAt,
               updated_at = @now
             where delivery_id = @deliveryId and state in ('pending','retry')`,
          )
          .run({
            deliveryId: row.deliveryId,
            attemptCount,
            leaseId,
            leaseExpiresAt,
            now: nowIso,
          });
        items.push({
          deliveryId: row.deliveryId,
          rawRowid: row.rawRowid,
          rawId: row.rawId,
          rawCreatedAt: row.rawCreatedAt,
          rawGeneration: row.rawGeneration,
          deviceId: row.deviceId,
          envelopeJson,
          envelope: outboundEnvelope,
          attemptCount,
        });
      }
    });
    run();
    return { leaseId, items, locallyDead, blockedBy: "none" };
  }

  /**
   * Re-check a bounded leased batch immediately before request serialization.
   * The raw row and terminal receipt are authoritative over the cached item.
   * Invalid items are made terminal in the same SQLite transaction and never
   * returned to the caller.
   */
  revalidateLeaseItems(leaseId: string, items: LeasedDeliveryItem[], at = new Date()) {
    if (items.length > 500) throw new Error("Delivery revalidation is bounded to 500 items.");
    const terminalAt = at.toISOString();
    const getActive = this.db.prepare(
      `select delivery_id as deliveryId, raw_rowid as rawRowid,
         raw_id as rawId, raw_created_at as rawCreatedAt,
         raw_generation as rawGeneration, device_id as deviceId
       from upload_outbox
       where delivery_id = ? and state = 'in_flight' and lease_id = ?`,
    );
    return this.db.transaction(() => {
      const deliverable: LeasedDeliveryItem[] = [];
      let locallyDead = 0;
      for (const item of items) {
        const active = getActive.get(item.deliveryId, leaseId) as
          | RawLineageSnapshot
          | undefined;
        if (!active) continue;
        const reason =
          active.rawRowid !== item.rawRowid || active.rawId !== item.rawId ||
          active.rawCreatedAt !== item.rawCreatedAt ||
          active.rawGeneration !== item.rawGeneration ||
          active.deviceId !== (item.deviceId ?? null)
            ? "lineage_unresolved"
            : this.authoritativePrivacyReason(active);
        if (reason === "lineage_unresolved") continue;
        if (reason) {
          locallyDead += this.deadActive(item.deliveryId, reason, terminalAt);
          continue;
        }
        deliverable.push(item);
      }
      return { items: deliverable, locallyDead };
    })();
  }

  validationLeaseRows(requestedRows: number) {
    const requested = Math.max(1, Math.min(Math.trunc(requestedRows), 500));
    const row = this.db
      .prepare(
        `select validation_probe_rows as probeRows
         from upload_control where singleton = 1`,
      )
      .get() as { probeRows: number };
    return row.probeRows > 0 ? Math.min(requested, row.probeRows) : requested;
  }

  boundValidationLeaseRows(rows: number, at = new Date()) {
    const bounded = Math.max(1, Math.min(Math.trunc(rows), 500));
    return this.db
      .prepare(
        `update upload_control set
           validation_probe_rows = case
             when validation_probe_rows = 0 then @bounded
             else min(validation_probe_rows, @bounded)
           end,
           updated_at = @now
         where singleton = 1`,
      )
      .run({ bounded, now: at.toISOString() }).changes;
  }

  growValidationLeaseRows(requestedRows: number, at = new Date()) {
    const requested = Math.max(1, Math.min(Math.trunc(requestedRows), 500));
    return this.db
      .prepare(
        `update upload_control set
           validation_probe_rows = min(@requested, validation_probe_rows * 2),
           updated_at = @now
         where singleton = 1 and validation_probe_rows > 0`,
      )
      .run({ requested, now: at.toISOString() }).changes;
  }

  acknowledge(
    leaseId: string,
    ids: string[],
    at = new Date(),
    validationWitness?: { contractHash: string; item: LeasedDeliveryItem },
  ) {
    const terminalAt = at.toISOString();
    const privacyEligible = terminalPrivacyEligibilitySql(this.db, "buffered_events");
    const get = this.db.prepare(
      `select delivery_id as deliveryId, raw_rowid as rawRowid,
         raw_id as rawId, raw_created_at as rawCreatedAt,
         raw_generation as rawGeneration,
         device_id as deviceId,
         attempt_count as attemptCount, created_at as createdAt
       from upload_outbox
       where delivery_id = ? and state = 'in_flight' and lease_id = ?
         and workspace_id is ? and device_id is ?`,
    );
    const markRaw = this.db.prepare(
      `update buffered_events set uploaded_at = @terminalAt
       where rowid = @rawRowid and id = @rawId and created_at = @rawCreatedAt
         and privacy_generation = @rawGeneration and uploaded_at is null
         and workspace_id is @workspaceId and device_id is @deviceId
         and ${privacyEligible}`,
    );
    // A response span may pair while its already-leased envelope is in
    // flight. A successful remote acknowledgment is still an accepted send;
    // preserve that receipt while keeping the raw row marked as a duplicate.
    const markAcceptedPairedSpan = this.db.prepare(
      `update buffered_events set uploaded_at = @terminalAt
       where rowid = @rawRowid and id = @rawId and created_at = @rawCreatedAt
         and privacy_generation = @rawGeneration and uploaded_at is null
         and workspace_id is @workspaceId and device_id is @deviceId
         and usage_duplicate_reason = 'codex_sse_event_span'
         and privacy_disposition is null and data_mode = 'metadata'
         and not exists (select 1 from upload_receipts where delivery_id = @deliveryId)`,
    );
    const remove = this.db.prepare(`delete from upload_outbox where delivery_id = ? and lease_id = ?`);
    const run = this.db.transaction(() => {
      let acknowledged = 0;
      let markedUploaded = 0;
      let locallyDead = 0;
      const acknowledgedIds: string[] = [];
      for (const id of ids) {
        const row = get.get(id, leaseId, this.workspaceId, this.deviceId) as
          | (RawLineageSnapshot & { attemptCount: number; createdAt: string })
          | undefined;
        if (!row) continue;
        const authoritativeReason = this.authoritativePrivacyReason(row);
        if (authoritativeReason === "lineage_unresolved") continue;
        if (authoritativeReason && authoritativeReason !== "local_usage_duplicate") {
          locallyDead += this.deadActive(id, authoritativeReason, terminalAt);
          continue;
        }
        const markParams = {
          terminalAt,
          deliveryId: id,
          rawRowid: row.rawRowid,
          rawId: row.rawId,
          rawCreatedAt: row.rawCreatedAt,
          rawGeneration: row.rawGeneration,
          workspaceId: this.workspaceId,
          deviceId: this.deviceId,
        };
        const marked = authoritativeReason === "local_usage_duplicate"
          ? markAcceptedPairedSpan.run(markParams).changes
          : markRaw.run(markParams).changes;
        if (marked !== 1 && !this.rawRetentionExpired(row)) {
          locallyDead += this.deadActive(id, authoritativeReason ?? "local_privacy_violation", terminalAt);
          continue;
        }
        const written = this.writeReceipt({
          deliveryId: id,
          state: "acknowledged",
          reason: "remote_acknowledged",
          attemptCount: row.attemptCount,
          createdAt: row.createdAt,
          terminalAt,
        });
        acknowledged += written;
        if (written > 0) acknowledgedIds.push(id);
        markedUploaded += marked;
        remove.run(id, leaseId);
      }
      if (validationWitness && acknowledgedIds.includes(validationWitness.item.deliveryId)) {
        this.writeValidationWitness(validationWitness.contractHash, validationWitness.item, terminalAt);
      }
      this.clearValidationProbeIfEmpty(terminalAt);
      return { acknowledged, acknowledgedIds, markedUploaded, locallyDead };
    });
    return run();
  }

  validationWitness(contractHash: string): DeliveryValidationWitness | null {
    const canonicalContract = canonicalLinkage(contractHash);
    if (!canonicalContract) return null;
    const row = this.db
      .prepare(
        `select contract_hash as contractHash, delivery_id as deliveryId,
           envelope_json as envelopeJson, envelope_bytes as envelopeBytes,
           acknowledged_at as acknowledgedAt
         from upload_validation_witness where singleton = 1 and contract_hash = ?`,
      )
      .get(canonicalContract) as
      | {
          contractHash: string;
          deliveryId: string;
          envelopeJson: string;
          envelopeBytes: number;
          acknowledgedAt: string;
        }
      | undefined;
    if (!row || row.envelopeBytes > this.limits.maxItemBytes || Buffer.byteLength(row.envelopeJson) !== row.envelopeBytes) {
      return null;
    }
    let decoded: unknown;
    try {
      decoded = JSON.parse(row.envelopeJson);
    } catch {
      return null;
    }
    const parsed = sealOutboundEnvelope(decoded);
    if (
      !parsed.ok ||
      parsed.envelope.event.id !== row.deliveryId ||
      JSON.stringify(parsed.envelope) !== row.envelopeJson
    ) {
      return null;
    }
    return {
      contractHash: row.contractHash,
      acknowledgedAt: row.acknowledgedAt,
      item: {
        deliveryId: row.deliveryId,
        rawRowid: null,
        rawId: null,
        rawCreatedAt: null,
        rawGeneration: null,
        envelopeJson: row.envelopeJson,
        envelope: parsed.envelope,
        attemptCount: 0,
      },
    };
  }

  markValidationCandidate(
    leaseId: string,
    deliveryId: string,
    contractHash: string,
    at = new Date(),
  ) {
    const canonicalContract = canonicalLinkage(contractHash);
    if (!canonicalContract) return 0;
    return this.db
      .prepare(
        `insert into upload_validation_candidates (delivery_id, contract_hash, failed_at)
         select delivery_id, @contractHash, @failedAt from upload_outbox
         where delivery_id = @deliveryId and state = 'in_flight' and lease_id = @leaseId
         on conflict(delivery_id) do nothing`,
      )
      .run({
        deliveryId,
        leaseId,
        contractHash: canonicalContract,
        failedAt: at.toISOString(),
      }).changes;
  }

  /** A candidate rejected after the last known-good acknowledgement needs one
   * bounded witness re-probe. This is an O(1) durable decision and never
   * exposes or leases the candidate itself. */
  validationWitnessReprobe(contractHash: string): DeliveryValidationWitness | null {
    const witness = this.validationWitness(contractHash);
    if (!witness) return null;
    const due = this.db
      .prepare(
        `select 1 as due
         from upload_validation_candidates c
         join upload_outbox o on o.delivery_id = c.delivery_id
         where c.contract_hash = ? and c.failed_at >= ?
         limit 1`,
      )
      .get(witness.contractHash, witness.acknowledgedAt) as { due: number } | undefined;
    return due ? witness : null;
  }

  refreshValidationWitness(
    contractHash: string,
    item: LeasedDeliveryItem,
    acknowledgedAt = new Date(),
  ) {
    return this.writeValidationWitness(contractHash, item, acknowledgedAt.toISOString());
  }

  settleProvenValidationCandidates(
    contractHash: string,
    options: { maxRows?: number; now?: Date } = {},
  ) {
    const witness = this.validationWitness(contractHash);
    if (!witness) return 0;
    const maxRows = Math.max(1, Math.min(Math.trunc(options.maxRows ?? 500), 500));
    const rows = this.db
      .prepare(
        `select c.delivery_id as deliveryId
         from upload_validation_candidates c
         join upload_outbox o on o.delivery_id = c.delivery_id
         where c.contract_hash = ? and c.failed_at < ?
         order by c.failed_at, c.delivery_id
         limit ?`,
      )
      .all(witness.contractHash, witness.acknowledgedAt, maxRows) as Array<{ deliveryId: string }>;
    const terminalAt = (options.now ?? new Date()).toISOString();
    return this.db.transaction(() => {
      let dead = 0;
      for (const row of rows) {
        dead += this.deadActive(row.deliveryId, "remote_validation_rejected", terminalAt);
      }
      return dead;
    })();
  }

  deadLetterRemote(leaseId: string, ids: string[], at = new Date()) {
    const terminalAt = at.toISOString();
    const get = this.db.prepare(
      `select attempt_count as attemptCount, created_at as createdAt,
              raw_rowid as rawRowid, raw_created_at as rawCreatedAt,
              raw_generation as rawGeneration,
              raw_id as rawId, sealed_envelope_json as sealedEnvelopeJson,
              base_envelope_json as baseEnvelopeJson
       from upload_outbox where delivery_id = ? and state = 'in_flight' and lease_id = ?`,
    );
    const remove = this.db.prepare(`delete from upload_outbox where delivery_id = ? and lease_id = ?`);
    const run = this.db.transaction(() => {
      let dead = 0;
      for (const id of ids) {
        const row = get.get(id, leaseId) as {
          attemptCount: number; createdAt: string; rawRowid: number | null;
          rawCreatedAt: string | null; rawGeneration: string | null; rawId: string | null;
          sealedEnvelopeJson: string | null; baseEnvelopeJson: string;
        } | undefined;
        if (!row) continue;
        const reason = captureGapReason(row.sealedEnvelopeJson) ??
          captureGapReason(row.baseEnvelopeJson);
        if (row.rawId && reason) rememberDeliveryCaptureGap(this.db, row, reason);
        this.rememberReplayLineage({
          deliveryId: id,
          rawRowid: row.rawRowid,
          rawId: row.rawId,
          rawCreatedAt: row.rawCreatedAt,
          rawGeneration: row.rawGeneration,
          frozenEnvelopeJson: row.sealedEnvelopeJson ?? row.baseEnvelopeJson,
          frozenAttemptCount: row.attemptCount,
          terminalAt,
        });
        dead += this.writeReceipt({
          deliveryId: id,
          state: "dead",
          reason: "remote_validation_rejected",
          attemptCount: row.attemptCount,
          createdAt: row.createdAt,
          terminalAt,
        });
        remove.run(id, leaseId);
      }
      this.clearValidationProbeIfEmpty(terminalAt);
      return dead;
    });
    return run();
  }

  /**
   * A caller-supplied `notBefore` raises the per-item retry date but can never
   * exceed the ledger's own configured ceiling: a server-directed floor is
   * honoured up to `maxBackoffSeconds` and no further, so one malformed
   * `Retry-After` cannot park rows past the maximum this outbox already
   * promised (review r1, F2).
   */
  private flooredAttemptAt(deliveryId: string, attemptCount: number, at: Date, notBefore?: Date) {
    const normal = this.nextAttemptAt(deliveryId, attemptCount, at);
    if (!notBefore || !Number.isFinite(notBefore.getTime())) return normal;
    const ceiling = at.getTime() + this.limits.maxBackoffSeconds * 1_000;
    const floor = Math.min(notBefore.getTime(), ceiling);
    return new Date(Math.max(Date.parse(normal), floor)).toISOString();
  }

  retry(leaseId: string, items: LeasedDeliveryItem[], failure: DeliveryFailureClass, at = new Date(), notBefore?: Date) {
    const update = this.db.prepare(
      `update upload_outbox set state = 'retry', next_attempt_at = @nextAttemptAt,
         lease_id = null, lease_expires_at = null, last_failure_class = @failure,
         updated_at = @now
       where delivery_id = @deliveryId and state = 'in_flight' and lease_id = @leaseId`,
    );
    const run = this.db.transaction(() => {
      let retried = 0;
      for (const item of items) {
        retried += update.run({
          deliveryId: item.deliveryId,
          leaseId,
          failure,
          nextAttemptAt: this.flooredAttemptAt(item.deliveryId, item.attemptCount, at, notBefore),
          now: at.toISOString(),
        }).changes;
      }
      return retried;
    });
    return run();
  }

  settleRemoteRejections(leaseId: string, items: LeasedDeliveryItem[], at = new Date()) {
    const terminalAt = at.toISOString();
    const get = this.db.prepare(
      `select attempt_count as attemptCount, created_at as createdAt
       from upload_outbox where delivery_id = ? and state = 'in_flight' and lease_id = ?`,
    );
    const retry = this.db.prepare(
      `update upload_outbox set state = 'retry', next_attempt_at = @nextAttemptAt,
         lease_id = null, lease_expires_at = null,
         last_failure_class = 'remote_rejected', updated_at = @now
       where delivery_id = @deliveryId and state = 'in_flight' and lease_id = @leaseId`,
    );
    const remove = this.db.prepare(
      `delete from upload_outbox where delivery_id = ? and state = 'in_flight' and lease_id = ?`,
    );
    return this.db.transaction(() => {
      let retried = 0;
      let dead = 0;
      for (const item of items) {
        const row = get.get(item.deliveryId, leaseId) as
          | { attemptCount: number; createdAt: string }
          | undefined;
        if (!row) continue;
        if (row.attemptCount >= REMOTE_REJECTED_MAX_ATTEMPTS) {
          dead += this.writeReceipt({
            deliveryId: item.deliveryId,
            state: "dead",
            reason: "remote_rejected_exhausted",
            attemptCount: row.attemptCount,
            createdAt: row.createdAt,
            terminalAt,
          });
          remove.run(item.deliveryId, leaseId);
          continue;
        }
        retried += retry.run({
          deliveryId: item.deliveryId,
          leaseId,
          nextAttemptAt: this.nextAttemptAt(item.deliveryId, row.attemptCount, at),
          now: terminalAt,
        }).changes;
      }
      return { retried, dead };
    })();
  }

  openCircuit(kind: Exclude<DeliveryCircuit, "none">, at = new Date()) {
    const until = new Date(at.getTime() + this.limits.maxBackoffSeconds * 1_000).toISOString();
    this.db
      .prepare(
        `update upload_control set circuit_kind = ?, circuit_opened_at = ?,
           circuit_until = ?, updated_at = ? where singleton = 1`,
      )
      .run(kind, at.toISOString(), until, at.toISOString());
  }

  clearCircuit(at = new Date()) {
    this.db
      .prepare(
        `update upload_control set circuit_kind = 'none', circuit_opened_at = null,
           circuit_until = null, updated_at = ? where singleton = 1`,
      )
      .run(at.toISOString());
  }

  status(now = new Date()): DeliveryStatus {
    const receiptLineageComplete = this.receiptLineageComplete();
    const control = this.db
      .prepare(
        `select migration_cursor_rowid as cursorRowid,
           migration_complete as complete, migration_paused_reason as pausedReason,
           circuit_kind as circuitKind, circuit_opened_at as circuitOpenedAt,
           circuit_until as circuitUntil,
           active_pending as pending, active_retry as retry,
           active_in_flight as inFlight, active_bytes as bytes,
           active_oldest_created_at as oldestCreatedAt,
           receipt_acknowledged as acknowledged, receipt_dead as dead,
           outbox_enqueued_total as outboxEnqueuedTotal,
           outbox_attempts_total as outboxAttemptsTotal,
           migration_last_visited as lastVisited,
           migration_last_bytes as lastBytes,
           migration_last_enqueued as lastEnqueued,
           migration_last_dead as lastDead,
           migration_last_skipped_uploaded as lastSkippedUploaded,
           migration_last_at as lastAt,
           active_remote_rejected as activeRemoteRejected
         from upload_control where singleton = 1`,
      )
      .get() as {
      cursorRowid: number;
      complete: number;
      pausedReason: "pressure" | "slice_budget_too_small" | null;
      circuitKind: DeliveryCircuit;
      circuitOpenedAt: string | null;
      circuitUntil: string | null;
      pending: number;
      retry: number;
      inFlight: number;
      bytes: number;
      oldestCreatedAt: string | null;
      acknowledged: number;
      dead: number;
      outboxEnqueuedTotal: number;
      outboxAttemptsTotal: number;
      lastVisited: number;
      lastBytes: number;
      lastEnqueued: number;
      lastDead: number;
      lastSkippedUploaded: number;
      lastAt: string | null;
      activeRemoteRejected: number;
    };
    const active = {
      pending: control.pending,
      retry: control.retry,
      inFlight: control.inFlight,
      bytes: control.bytes,
      oldestCreatedAt: control.oldestCreatedAt,
    };
    const receipts = { acknowledged: control.acknowledged, dead: control.dead };
    const remainingDelivery = active.pending + active.retry + active.inFlight;
    const oldestAgeSeconds = active.oldestCreatedAt
      ? Math.max(0, Math.floor((now.getTime() - Date.parse(active.oldestCreatedAt)) / 1_000))
      : null;
    const ageBudgetSeconds = this.limits.maxOldestAgeDays * 24 * 60 * 60;
    const reasons: DeliveryStatus["pressure"]["reasons"] = [];
    if (remainingDelivery > this.limits.maxActiveRows) reasons.push("row_budget");
    if (active.bytes > this.limits.maxActiveBytes) reasons.push("byte_budget");
    if (oldestAgeSeconds !== null && oldestAgeSeconds > ageBudgetSeconds) reasons.push("age_budget");
    const degradedReasons: DeliveryStatus["degradedReasons"] = reasons.map((reason) =>
      reason === "row_budget"
        ? "pressure_row_budget"
        : reason === "byte_budget"
          ? "pressure_byte_budget"
          : "pressure_age_budget",
    );
    if (control.circuitKind === "auth_blocked") degradedReasons.push("auth_circuit");
    if (control.circuitKind === "contract_blocked") degradedReasons.push("contract_circuit");
    if (control.activeRemoteRejected > 0) degradedReasons.push("remote_rejected");
    if (control.pausedReason === "slice_budget_too_small") {
      degradedReasons.push("migration_slice_budget");
    }
    return {
      enabled: this.enabled,
      degraded: degradedReasons.length > 0,
      degradedReasons,
      remainingDelivery,
      active: { ...active, oldestAgeSeconds },
      receipts,
      pressure: {
        degraded: reasons.length > 0,
        reasons,
        budgets: {
          rows: this.limits.maxActiveRows,
          bytes: this.limits.maxActiveBytes,
          oldestAgeSeconds: ageBudgetSeconds,
        },
      },
      circuit: {
        kind: control.circuitKind,
        openedAt: control.circuitOpenedAt,
        until: control.circuitUntil,
      },
      migration: {
        cursorRowid: control.cursorRowid,
        complete: Boolean(control.complete) && receiptLineageComplete,
        pausedReason: receiptLineageComplete ? control.pausedReason : "receipt_lineage_pending",
        progressMode: "bounded_rowid_watermark_no_exact_remaining",
        sliceBudget: {
          rows: this.limits.migrationBatchRows,
          bytes: this.limits.migrationBatchBytes,
          uploadBatchesPerCycle: this.limits.maxBatchesPerCycle,
        },
        lastSlice: {
          visited: control.lastVisited,
          bytes: control.lastBytes,
          enqueued: control.lastEnqueued,
          dead: control.lastDead,
          skippedUploaded: control.lastSkippedUploaded,
          at: control.lastAt,
        },
      },
      retention: {
        mode: "raw_ttl",
        rawTtlBlockedBy: null,
        pendingDeliverySurvivesRawExpiry: true,
      },
      counters: {
        outboxRowsEnqueued: control.outboxEnqueuedTotal,
        outboxAttempts: control.outboxAttemptsTotal,
        deadLettersWritten: control.dead,
      },
      work: {
        controlRowsRead: 1,
        activeRowsScanned: 0,
        receiptRowsScanned: 0,
        rawRowsScanned: 0,
      },
      privacy: {
        mode: "metadata_only",
        evidenceVault: "not_implemented",
        legacyEvidenceDisposition: "local_quarantine_migration_required",
        liveLedgerInspection: "not_performed",
      },
    };
  }

  /** Close one delivery tied to an expired prior-audience raw row.
   * Called inside the prune transaction. The return value requests a later
   * bounded visit if an old ledger has multiple deliveries for one raw row. */
  retirePriorAudienceRaw(rawRowid: number, rawId: string,
    rawCreatedAt: string, rawGeneration: string | null,
    workspaceId: string | null, deviceId: string | null, terminalAt: string): boolean {
    const deliveries = this.db.prepare(`select delivery_id as id from upload_outbox
      where raw_rowid=? and raw_id=? and raw_created_at=?
        and raw_generation is ? order by delivery_id limit 2`).all(
      rawRowid, rawId, rawCreatedAt, rawGeneration,
    ) as Array<{ id: string }>;
    if (deliveries.length === 0) {
      // A fully unlinked legacy envelope carries no raw identity. Retire it
      // for its own prior-audience binding and timestamp; do not use a rowid
      // or let its receipt decide the raw's privacy state.
      const unlinked = this.db.prepare(`select delivery_id as id from upload_outbox
        where raw_rowid is null and raw_id is null and raw_created_at is null
          and raw_generation is null and delivery_id=? and created_at=?
          and workspace_id is ? and device_id is ?`).get(
        ensureUuidEventId(rawId).id, rawCreatedAt, workspaceId, deviceId,
      ) as { id: string } | undefined;
      if (unlinked) this.deadActive(unlinked.id, "local_privacy_violation", terminalAt);
      return this.hasAmbiguousLegacyLink(rawRowid);
    }
    this.deadActive(deliveries[0]!.id, "local_privacy_violation", terminalAt);
    return deliveries.length > 1;
  }

  /** Close stale linked deliveries before local-only raw expires in the same transaction. */
  retireIneligibleRaw(rawRowid: number, rawId: string,
    rawCreatedAt: string, rawGeneration: string | null, dataMode: string,
    privacyDisposition: string | null, usageDuplicateReason: string | null,
    terminalAt: string): boolean {
    const deliveries = this.db.prepare(`select delivery_id as id from upload_outbox
      where raw_rowid=? and raw_id=? and raw_created_at=?
        and raw_generation is ? order by delivery_id limit 2`).all(
      rawRowid, rawId, rawCreatedAt, rawGeneration,
    ) as Array<{ id: string }>;
    if (deliveries.length === 0) return this.hasAmbiguousLegacyLink(rawRowid);
    const reason: DeliveryReceiptReason = dataMode === "evidence" ||
      privacyDisposition === "local_evidence_quarantined"
      ? "local_evidence_quarantined"
      : privacyDisposition ? "local_privacy_violation"
        : usageDuplicateReason ? "local_usage_duplicate" : "local_schema_invalid";
    this.deadActive(deliveries[0]!.id, reason, terminalAt);
    return deliveries.length > 1;
  }

  /** An unbound delivery can delay expiry, but cannot condemn a raw by rowid. */
  private hasAmbiguousLegacyLink(rawRowid: number) {
    return Boolean(this.db.prepare(`select 1 from upload_outbox where
      raw_rowid=? and raw_id is null limit 1`).get(rawRowid));
  }

  /** Persist every terminal privacy decision before an old binary can prune
   * the raw. A historical raw may have multiple collision-safe delivery IDs;
   * leaving one active would let 0.7.44 remove the raw and then re-lease it. */
  private retireLinkedPrivacyDeliveries(rawRowid: number, rawId: string,
    rawCreatedAt: string, rawGeneration: string | null,
    reason: TerminalPrivacyReason, terminalAt: string) {
    const linked = { rawRowid, rawId, rawCreatedAt, rawGeneration, reason,
      statusClass: terminalStatusClass(reason), terminalAt };
    const apply = () => {
      // A rowid reused after retention must never acquire an older copy's
      // terminal decision. The full raw incarnation is required first.
      if (this.db.prepare(`select 1 from buffered_events where rowid=? and id=?
        and created_at=? and privacy_generation is ?`).get(
        rawRowid, rawId, rawCreatedAt, rawGeneration,
      )) markRawPrivacyDisposition(this.db, rawRowid, reason, terminalAt);
      const written = this.db.prepare(`insert or ignore into upload_receipts
        (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,
         terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
        select delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,
          'dead',@reason,@statusClass,attempt_count,created_at,@terminalAt
        from upload_outbox where raw_rowid=@rawRowid and raw_id=@rawId
          and raw_created_at=@rawCreatedAt
          and raw_generation is @rawGeneration`).run(linked).changes;
      this.db.prepare(`delete from upload_outbox where raw_rowid=@rawRowid
        and raw_id=@rawId and raw_created_at=@rawCreatedAt
        and raw_generation is @rawGeneration`).run(linked);
      this.clearValidationProbeIfEmpty(terminalAt);
      return written;
    };
    return this.db.inTransaction ? apply() : this.db.transaction(apply).immediate();
  }

  private deadActive(
    deliveryId: string,
    reason: DeliveryReceiptReason,
    terminalAt: string,
    disposedRawRowids?: Set<number>,
  ) {
    const row = this.db
      .prepare(
        `select raw_rowid as rawRowid,raw_id as rawId,
           raw_created_at as rawCreatedAt,raw_generation as rawGeneration,
           attempt_count as attemptCount,created_at as createdAt,
           base_envelope_json as baseEnvelopeJson,
           sealed_envelope_json as sealedEnvelopeJson
         from upload_outbox where delivery_id = ?`,
      )
      .get(deliveryId) as
        | { rawRowid: number | null; rawId: string | null;
            rawCreatedAt: string | null; rawGeneration: string | null;
            attemptCount: number; createdAt: string;
            baseEnvelopeJson: string; sealedEnvelopeJson: string | null }
        | undefined;
    if (!row) return 0;
    if (isReplayableReceiptReason(reason)) {
      this.rememberReplayLineage({
        deliveryId,
        rawRowid: row.rawRowid,
        rawId: row.rawId,
        rawCreatedAt: row.rawCreatedAt,
        rawGeneration: row.rawGeneration,
        frozenEnvelopeJson: row.sealedEnvelopeJson ?? row.baseEnvelopeJson,
        frozenAttemptCount: row.attemptCount,
        terminalAt,
      });
    }
    const gapReason = captureGapReason(row.sealedEnvelopeJson) ??
      captureGapReason(row.baseEnvelopeJson);
    if (row.rawId && gapReason) rememberDeliveryCaptureGap(this.db, row, gapReason);
    let siblingReceipts = 0;
    if (row.rawRowid !== null && isTerminalPrivacyReason(reason)) {
      const owner = this.db.prepare(`select id from buffered_events
        where rowid=? and id is ? and created_at is ?
          and privacy_generation is ?`).get(
        row.rawRowid, row.rawId, row.rawCreatedAt, row.rawGeneration,
      ) as { id: string } | undefined;
      if (owner) {
        disposedRawRowids?.add(row.rawRowid);
        siblingReceipts = this.retireLinkedPrivacyDeliveries(
          row.rawRowid, owner.id, row.rawCreatedAt!, row.rawGeneration,
          reason, terminalAt);
      }
    }
    const written = this.writeReceipt({
      deliveryId,
      state: "dead",
      reason,
      attemptCount: row.attemptCount,
      createdAt: row.createdAt,
      terminalAt,
    });
    this.db.prepare(`delete from upload_outbox where delivery_id = ?`).run(deliveryId);
    this.clearValidationProbeIfEmpty(terminalAt);
    return siblingReceipts + written;
  }

  /** Move a released binary's privacy receipt off an unrelated delivery ID.
   * The old receipt can be bound only to a different exact raw or expiry
   * incarnation. An ambiguous legacy receipt stays in place and holds work. */
  private relocateConflictingPrivacyReceipt(lineage: RawLineageSnapshot) {
    if (lineage.rawId === null || lineage.rawCreatedAt === null) return false;
    const run = () => {
      const receipt = this.db.prepare(`select raw_rowid as rawRowid,
        raw_id as rawId,raw_created_at as rawCreatedAt,
        raw_generation as rawGeneration,created_at as createdAt,
        terminal_state as state,reason
        from upload_receipts where delivery_id=?`).get(lineage.deliveryId) as {
          rawRowid: number | null; rawId: string | null;
          rawCreatedAt: string | null; rawGeneration: string | null;
          createdAt: string; state: string; reason: DeliveryReceiptReason;
        } | undefined;
      if (!receipt || receipt.state !== "dead" ||
          !isTerminalPrivacyReason(receipt.reason)) return false;
      if (receipt.rawId === lineage.rawId &&
          receipt.rawCreatedAt === lineage.rawCreatedAt &&
          receipt.rawGeneration === lineage.rawGeneration) return false;
      let owner: { rawRowid: number | null; rawId: string;
        rawCreatedAt: string; rawGeneration: string | null } | undefined;
      if (receipt.rawId !== null && receipt.rawCreatedAt !== null) {
        owner = { rawRowid: receipt.rawRowid, rawId: receipt.rawId,
          rawCreatedAt: receipt.rawCreatedAt, rawGeneration: receipt.rawGeneration };
      } else if (receipt.rawRowid === null && receipt.rawId === null &&
          receipt.rawCreatedAt === null && receipt.rawGeneration === null &&
          receipt.createdAt !== lineage.rawCreatedAt) {
        const current = this.db.prepare(`select rowid as rawRowid,id as rawId,
          created_at as rawCreatedAt,privacy_generation as rawGeneration
          from buffered_events where id=? and created_at=?
            and privacy_disposition=?`).get(
          lineage.rawId, receipt.createdAt, receipt.reason,
        ) as typeof owner;
        const expired = this.db.prepare(`select raw_rowid as rawRowid,
          event_id as rawId,raw_created_at as rawCreatedAt,
          raw_generation as rawGeneration from raw_retention_receipts
          where event_id=? and raw_created_at=? limit 2`).all(
          lineage.rawId, receipt.createdAt,
        ) as NonNullable<typeof owner>[];
        if (expired.length <= 1 && (!current || expired.length === 0 ||
            (current.rawId === expired[0]!.rawId &&
              current.rawCreatedAt === expired[0]!.rawCreatedAt &&
              current.rawGeneration === expired[0]!.rawGeneration))) {
          owner = current ?? expired[0];
        }
      }
      if (!owner || (owner.rawId === lineage.rawId &&
          owner.rawCreatedAt === lineage.rawCreatedAt &&
          owner.rawGeneration === lineage.rawGeneration)) return false;
      const occupied = this.db.prepare(`select 1 from buffered_events where id=?
        union all select 1 from upload_outbox where delivery_id=?
        union all select 1 from upload_receipts where delivery_id=?
        union all select 1 from upload_replays where delivery_id=?
        union all select 1 from upload_validation_candidates where delivery_id=?
        limit 1`);
      for (let attempt = 0; attempt < 32; attempt++) {
        const replacement = incarnationDeliveryId(owner.rawId,
          owner.rawCreatedAt, owner.rawGeneration, attempt);
        if (occupied.get(replacement, replacement, replacement, replacement,
          replacement)) continue;
        return this.db.prepare(`update upload_receipts set delivery_id=?,
          raw_rowid=?,raw_id=?,raw_created_at=?,raw_generation=?
          where delivery_id=? and terminal_state='dead'`).run(
          replacement, owner.rawRowid, owner.rawId, owner.rawCreatedAt,
          owner.rawGeneration, lineage.deliveryId,
        ).changes === 1;
      }
      return false;
    };
    return this.db.inTransaction ? run() : this.db.transaction(run).immediate();
  }

  private authoritativePrivacyReason(
    lineage: RawLineageSnapshot,
  ): PrivacyDecision {
    this.relocateConflictingPrivacyReceipt(lineage);
    const receipt = this.db.prepare(`select terminal_state as state,reason,raw_id as rawId,
      raw_created_at as rawCreatedAt,raw_generation as rawGeneration
      from upload_receipts where delivery_id = ?`).get(lineage.deliveryId) as {
        state: string; reason: DeliveryReceiptReason;
        rawId: string | null; rawCreatedAt: string | null;
        rawGeneration: string | null;
    } | undefined;
    if (receipt) {
      if (lineage.rawId === null || lineage.rawCreatedAt === null ||
          receipt.rawId === null || receipt.rawCreatedAt === null)
        return "lineage_unresolved";
      if (receipt.rawId !== lineage.rawId ||
          receipt.rawCreatedAt !== lineage.rawCreatedAt ||
          receipt.rawGeneration !== lineage.rawGeneration) return "lineage_unresolved";
      // A matching acknowledgement or remote rejection is a terminal
      // delivery result, not a new local privacy verdict on the raw.
      return receipt.state === "acknowledged" ? "remote_acknowledged" : receipt.reason;
    }
    if (lineage.rawRowid === null) return "lineage_unresolved";
    const raw = this.db
      .prepare(
        `select id as rawId, created_at as createdAt,
           privacy_generation as privacyGeneration,
           privacy_disposition as privacyDisposition,
           usage_duplicate_reason as usageDuplicateReason,
           data_mode as dataMode, uploaded_at as uploadedAt
         from buffered_events where rowid = ?`,
      )
      .get(lineage.rawRowid) as RawPrivacyRow | undefined;
    if (!raw) {
      // Exact retention expiry leaves the queued copy deliverable. Another
      // missing raw has no proven privacy decision and is a local integrity
      // failure, not a terminal privacy rejection.
      return this.rawRetentionExpired(lineage) ? null : "local_schema_invalid";
    }
    // A recycled rowid is a different raw until all three identity fields
    // agree. Check before consulting any privacy or delivery state on it.
    if (raw.rawId !== lineage.rawId ||
        raw.createdAt !== lineage.rawCreatedAt ||
        raw.privacyGeneration !== lineage.rawGeneration) {
      return this.rawRetentionExpired(lineage) ? null : "lineage_unresolved";
    }
    if (raw.uploadedAt !== null) return "local_privacy_violation";
    if (raw.usageDuplicateReason) return "local_usage_duplicate";
    if (raw.privacyDisposition) return raw.privacyDisposition;
    if (raw.dataMode === "evidence") return "local_evidence_quarantined";
    if (raw.dataMode !== "metadata") return "local_privacy_violation";
    if (
      lineage.rawId === null ||
      lineage.rawCreatedAt === null ||
      lineage.rawGeneration === null
    ) {
      return "local_privacy_violation";
    }
    if (
      raw.privacyGeneration === null ||
      !isCollisionSafeDeliveryId(raw.rawId, lineage.deliveryId,
        raw.createdAt, raw.privacyGeneration)
    ) {
      return "local_privacy_violation";
    }
    return null;
  }

  private rawRetentionExpired(lineage: RawLineageSnapshot) {
    if (
      lineage.rawId === null ||
      lineage.rawCreatedAt === null
    ) {
      return false;
    }
    return Boolean(
      this.db
        .prepare(
          `select 1 as expired from raw_retention_receipts
           where event_id = ? and raw_created_at = ?
             and raw_generation is ? limit 1`,
        )
        .get(
          lineage.rawId,
          lineage.rawCreatedAt,
          lineage.rawGeneration,
        ),
    );
  }

  private quarantineLinkedEvidence(rawRowid: number, rawId: string,
    rawCreatedAt: string, rawGeneration: string | null, terminalAt: string) {
    if (this.db.prepare(`select 1 from buffered_events where rowid=? and id=?
      and created_at=? and privacy_generation is ?`).get(
      rawRowid, rawId, rawCreatedAt, rawGeneration,
    )) markRawPrivacyDisposition(this.db, rawRowid,
      "local_evidence_quarantined", terminalAt);
    const rows = this.db
      .prepare(
        `select delivery_id as deliveryId
         from upload_outbox
         where raw_rowid = ? and raw_id = ? and raw_created_at = ?
           and raw_generation is ?
         order by delivery_id
         limit 500`,
      )
      .all(rawRowid, rawId, rawCreatedAt, rawGeneration) as Array<{ deliveryId: string }>;
    let dead = 0;
    for (const row of rows) {
      dead += this.deadActive(row.deliveryId, "local_evidence_quarantined", terminalAt);
    }
    return dead;
  }

  private quarantineUnprovenLineage(maxRows: number, terminalAt: string, writerDeadline?: number) {
    const rows = this.db
      .prepare(
        `select delivery_id as deliveryId, raw_rowid as rawRowid,
           raw_id as rawId, raw_created_at as rawCreatedAt,
           raw_generation as rawGeneration, device_id as deviceId
         from upload_outbox indexed by idx_upload_outbox_raw_generation
         where raw_generation is null
         order by raw_generation, created_at, delivery_id
         limit ?`,
      )
      .all(maxRows) as RawLineageSnapshot[];
    let dead = 0;
    for (const row of rows) {
      if (writerDeadline !== undefined && performance.now() >= writerDeadline) break;
      const reason = this.authoritativePrivacyReason(row);
      if (reason === "lineage_unresolved") continue;
      dead += this.deadActive(
        row.deliveryId,
        reason ?? "local_privacy_violation",
        terminalAt,
      );
    }
    return dead;
  }

  private clearValidationProbeIfEmpty(nowIso: string) {
    this.db
      .prepare(
        `update upload_control set validation_probe_rows = 0, updated_at = @now
         where singleton = 1 and validation_probe_rows <> 0
           and not exists (select 1 from upload_outbox)`,
      )
      .run({ now: nowIso });
  }

  private writeReceipt(input: {
    deliveryId: string;
    lineage?: Pick<RawDeliveryRow, "rawRowid" | "rawId" | "createdAt" | "privacyGeneration">;
    state: "acknowledged" | "dead";
    reason: DeliveryReceiptReason;
    attemptCount: number;
    createdAt: string;
    terminalAt: string;
  }) {
    const linked = input.lineage ?? this.db.prepare(`select raw_rowid as rawRowid,
      raw_id as rawId,raw_created_at as createdAt,raw_generation as privacyGeneration
      from upload_outbox where delivery_id=?`).get(input.deliveryId) as
      Pick<RawDeliveryRow, "rawRowid" | "rawId" | "createdAt" | "privacyGeneration"> | undefined;
    return this.db
      .prepare(
        `insert or ignore into upload_receipts
          (delivery_id, raw_rowid, raw_id, raw_created_at, raw_generation,
           terminal_state, reason, status_class, attempt_count, created_at, terminal_at)
         values (@deliveryId, @rawRowid, @rawId, @rawCreatedAt, @rawGeneration,
           @state, @reason, @statusClass, @attemptCount, @createdAt, @terminalAt)`,
      )
      .run({ ...input,
        rawRowid: linked?.rawRowid ?? null,
        rawId: linked?.rawId ?? null,
        rawCreatedAt: linked?.createdAt ?? null,
        rawGeneration: linked?.privacyGeneration ?? null,
        statusClass: terminalStatusClass(input.reason) }).changes;
  }

  private writeValidationWitness(
    contractHash: string,
    item: LeasedDeliveryItem,
    acknowledgedAt: string,
  ) {
    const canonicalContract = canonicalLinkage(contractHash);
    if (!canonicalContract) return 0;
    let decoded: unknown;
    try {
      decoded = JSON.parse(item.envelopeJson);
    } catch {
      return 0;
    }
    const envelope = sealOutboundEnvelope(decoded);
    const envelopeBytes = Buffer.byteLength(item.envelopeJson);
    if (
      !envelope.ok ||
      envelope.envelope.event.id !== item.deliveryId ||
      envelopeBytes > this.limits.maxItemBytes ||
      JSON.stringify(envelope.envelope) !== item.envelopeJson
    ) {
      return 0;
    }
    return this.db
      .prepare(
        `insert into upload_validation_witness
          (singleton, contract_hash, delivery_id, envelope_json, envelope_bytes, acknowledged_at)
         values (1, @contractHash, @deliveryId, @envelopeJson, @envelopeBytes, @acknowledgedAt)
         on conflict(singleton) do update set
           contract_hash = excluded.contract_hash,
           delivery_id = excluded.delivery_id,
           envelope_json = excluded.envelope_json,
           envelope_bytes = excluded.envelope_bytes,
           acknowledged_at = excluded.acknowledged_at`,
      )
      .run({
        contractHash: canonicalContract,
        deliveryId: item.deliveryId,
        envelopeJson: item.envelopeJson,
        envelopeBytes,
        acknowledgedAt,
      }).changes;
  }

  private nextAttemptAt(deliveryId: string, attemptCount: number, now: Date) {
    const baseSeconds = Math.min(2 ** Math.max(0, attemptCount - 1), this.limits.maxBackoffSeconds);
    const digest = crypto.createHash("sha256").update(`${deliveryId}:${attemptCount}`).digest();
    const jitter = 0.75 + (digest.readUInt16BE(0) / 65_535) * 0.5;
    const delayMs = Math.min(
      this.limits.maxBackoffSeconds * 1_000,
      Math.max(1_000, Math.round(baseSeconds * jitter * 1_000)),
    );
    return new Date(now.getTime() + delayMs).toISOString();
  }
}
