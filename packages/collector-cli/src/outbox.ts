import crypto from "node:crypto";

import type Database from "better-sqlite3";

import {
  aiWorkIngestEventSchema,
  findForbiddenRawContentFields,
  type AiInteractionEvent,
  type AiWorkIngestEvent,
} from "../../shared/src/index";
import { ensureUuidEventId, normalizeHistoryEvent } from "./upload-history";

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

export type DeliveryLimits = {
  [Key in keyof typeof DEFAULT_DELIVERY_LIMITS]: number;
};

export type DeliveryFailureClass =
  | "none"
  | "local_payload_unparseable"
  | "local_schema_invalid"
  | "local_privacy_violation"
  | "local_item_oversize"
  | "remote_validation"
  | "remote_auth"
  | "remote_transient"
  | "remote_contract";

export type DeliveryCircuit = "none" | "auth_blocked" | "contract_blocked";
export type DeliveryReceiptReason =
  | "remote_acknowledged"
  | "local_payload_unparseable"
  | "local_schema_invalid"
  | "local_privacy_violation"
  | "local_item_oversize"
  | "remote_validation_rejected";

export type DeliveryStatus = {
  enabled: boolean;
  degraded: boolean;
  degradedReasons: Array<
    | "pressure_row_budget"
    | "pressure_byte_budget"
    | "pressure_age_budget"
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
    pausedReason: "pressure" | null;
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
    mode: "compatibility_uploaded_only";
    rawTtlBlockedBy: "projection_parity";
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
};

export type LeasedDeliveryItem = {
  deliveryId: string;
  rawRowid: number | null;
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

type RawDeliveryRow = {
  rawRowid: number;
  rawId: string;
  createdAt: string;
  uploadedAt: string | null;
  payloadJson: string;
  suppressedFieldsJson: string;
  repoHash: string | null;
  branchHash: string | null;
};

type LegacyCandidateRow = Pick<
  RawDeliveryRow,
  "rawRowid" | "rawId" | "createdAt" | "uploadedAt"
> & { rowBytes: number };

type ActiveDeliveryRow = {
  deliveryId: string;
  rawRowid: number | null;
  baseEnvelopeJson: string;
  sealedEnvelopeJson: string | null;
  repoHash: string | null;
  branchHash: string | null;
  attemptCount: number;
};

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

const SAFE_HASH = /^sha256:[a-zA-Z0-9._:-]{8,160}$/;
const SAFE_SUPPRESSED_FIELD = /^[a-zA-Z0-9_.:-]{1,96}$/;
const SENSITIVE_KEY = /(?:^|_)(?:authorization|cookie|credential|email|password|path|prompt|response|secret|token|url)(?:$|_)/i;
const SENSITIVE_STRING =
  /(?:https?:\/\/|\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b|(?:^|[\s"'])\/(?:Users|home|private|var\/folders)\/|\b[A-Za-z]:\\)/i;

function safeLinkage(value: string | null | undefined) {
  return value && SAFE_HASH.test(value) ? value : null;
}

function privacyViolation(value: unknown, key = ""): boolean {
  if (
    key === "transport_path" &&
    typeof value === "string" &&
    ["/v1/logs", "/v1/traces", "/v1/metrics"].includes(value)
  ) {
    return false;
  }
  if (key && SENSITIVE_KEY.test(key)) return true;
  if (typeof value === "string") return SENSITIVE_STRING.test(value);
  if (Array.isArray(value)) return value.some((entry) => privacyViolation(entry));
  if (!value || typeof value !== "object") return false;
  return Object.entries(value as Record<string, unknown>).some(([nestedKey, nested]) =>
    privacyViolation(nested, nestedKey),
  );
}

function sanitizeSuppressedFields(fields: string[]) {
  return [...new Set(fields.map((field) => field.trim()).filter((field) => SAFE_SUPPRESSED_FIELD.test(field)))];
}

function withoutExternalEventId(event: AiInteractionEvent): AiInteractionEvent {
  if (!("externalEventId" in event.metadata)) return event;
  const { externalEventId: _localId, ...metadata } = event.metadata;
  return { ...event, metadata };
}

function prepareDelivery(row: RawDeliveryRow, maxItemBytes: number): PreparedDelivery {
  const fallbackId = ensureUuidEventId(row.rawId).id;
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

  const event = withoutExternalEventId(normalized.envelope.event);
  const envelope = aiWorkIngestEventSchema.safeParse({
    event,
    suppressedFields: sanitizeSuppressedFields(normalized.envelope.suppressedFields),
  });
  const deliveryId = event.id;
  if (!envelope.success) {
    return { ok: false, deliveryId, reason: "local_schema_invalid" };
  }
  if (
    findForbiddenRawContentFields(envelope.data.event.metadata).length > 0 ||
    privacyViolation(envelope.data)
  ) {
    return { ok: false, deliveryId, reason: "local_privacy_violation" };
  }
  const baseEnvelopeJson = JSON.stringify(envelope.data);
  const baseBytes = Buffer.byteLength(baseEnvelopeJson);
  if (baseBytes > maxItemBytes) {
    return { ok: false, deliveryId, reason: "local_item_oversize" };
  }
  return {
    ok: true,
    deliveryId,
    baseEnvelopeJson,
    baseBytes,
    repoHash: safeLinkage(row.repoHash),
    branchHash: safeLinkage(row.branchHash),
  };
}

function attachFillOnlyLinkage(
  envelope: AiWorkIngestEvent,
  repoHash: string | null,
  branchHash: string | null,
): AiWorkIngestEvent {
  if (!repoHash || envelope.event.projectKey) return envelope;
  return {
    ...envelope,
    event: {
      ...envelope.event,
      projectKey: repoHash,
      ...(branchHash
        ? { metadata: { ...envelope.event.metadata, branchHash } }
        : {}),
    },
  };
}

function terminalStatusClass(reason: DeliveryReceiptReason) {
  if (reason === "remote_acknowledged") return "remote_2xx";
  if (reason === "remote_validation_rejected") return "remote_validation";
  return "local_validation";
}

function asLimits(input: Partial<DeliveryLimits> | undefined): DeliveryLimits {
  return { ...DEFAULT_DELIVERY_LIMITS, ...input };
}

export class DeliveryOutbox {
  private enabled: boolean;
  private limits: DeliveryLimits;

  constructor(
    private readonly db: Database.Database,
    options: { enabled?: boolean; limits?: Partial<DeliveryLimits> } = {},
  ) {
    this.enabled = options.enabled ?? false;
    this.limits = asLimits(options.limits);
    this.initializeSchema();
  }

  configure(options: { enabled: boolean; limits?: Partial<DeliveryLimits> }) {
    this.enabled = options.enabled;
    this.limits = asLimits(options.limits);
  }

  isEnabled() {
    return this.enabled;
  }

  private initializeSchema() {
    // Additive only: no query or index is built against the historical raw
    // ledger here. Large-ledger work happens solely in bounded migration slices.
    this.db.exec(`
      create table if not exists upload_outbox (
        delivery_id text primary key,
        raw_rowid integer,
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
        terminal_state text not null check (terminal_state in ('acknowledged','dead')),
        reason text not null,
        status_class text not null,
        attempt_count integer not null,
        created_at text not null,
        terminal_at text not null
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
      create index if not exists idx_upload_receipts_state
        on upload_receipts (terminal_state);

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
  }

  enqueueRaw(row: RawDeliveryRow) {
    if (!this.enabled || row.uploadedAt) return { enqueued: 0, dead: 0 };
    const prepared = prepareDelivery(row, this.limits.maxItemBytes);
    if (prepared.ok === false) {
      return {
        enqueued: 0,
        dead: this.writeReceipt({
          deliveryId: prepared.deliveryId,
          state: "dead",
          reason: prepared.reason,
          attemptCount: 0,
          createdAt: row.createdAt,
          terminalAt: new Date().toISOString(),
        }),
      };
    }
    const now = new Date().toISOString();
    const inserted = this.db
      .prepare(
        `insert or ignore into upload_outbox
          (delivery_id, raw_rowid, base_envelope_json, base_bytes, repo_hash, branch_hash,
           state, attempt_count, next_attempt_at, last_failure_class, created_at, updated_at)
         select @deliveryId, @rawRowid, @baseEnvelopeJson, @baseBytes, @repoHash, @branchHash,
           'pending', 0, @now, 'none', @createdAt, @now
         where not exists (
           select 1 from upload_receipts where delivery_id = @deliveryId
         )`,
      )
      .run({
        ...prepared,
        rawRowid: row.rawRowid,
        createdAt: row.createdAt,
        now,
      }).changes;
    return { enqueued: inserted, dead: 0 };
  }

  repairRawById(rawId: string) {
    if (!this.enabled) return { enqueued: 0, dead: 0 };
    const row = this.db
      .prepare(
        `select rowid as rawRowid, id as rawId, created_at as createdAt,
           uploaded_at as uploadedAt, payload_json as payloadJson,
           suppressed_fields_json as suppressedFieldsJson,
           repo_hash as repoHash, branch_hash as branchHash
         from buffered_events where id = ?`,
      )
      .get(rawId) as RawDeliveryRow | undefined;
    return row ? this.enqueueRaw(row) : { enqueued: 0, dead: 0 };
  }

  fillLinkageForRawRow(rawRowid: number, repoHash: string | null, branchHash: string | null) {
    if (!this.enabled) return 0;
    return this.db
      .prepare(
        `update upload_outbox set
           repo_hash = coalesce(repo_hash, @repoHash),
           branch_hash = coalesce(branch_hash, @branchHash),
           updated_at = @now
         where raw_rowid = @rawRowid
           and sealed_envelope_json is null and attempt_count = 0`,
      )
      .run({
        rawRowid,
        repoHash: safeLinkage(repoHash),
        branchHash: safeLinkage(branchHash),
        now: new Date().toISOString(),
      }).changes;
  }

  migrateLegacy(options: { maxRows?: number; maxBytes?: number; now?: Date } = {}) {
    if (!this.enabled) return { visited: 0, enqueued: 0, dead: 0, skippedUploaded: 0, complete: false, paused: null };
    const now = options.now ?? new Date();
    const pressure = this.status(now).pressure;
    if (pressure.degraded) {
      this.db
        .prepare(
          `update upload_control set migration_paused_reason = 'pressure', updated_at = ?
           where singleton = 1`,
        )
        .run(now.toISOString());
      return { visited: 0, enqueued: 0, dead: 0, skippedUploaded: 0, complete: false, paused: "pressure" as const };
    }

    const control = this.db
      .prepare(
        `select migration_cursor_rowid as cursorRowid, migration_complete as complete
         from upload_control where singleton = 1`,
      )
      .get() as { cursorRowid: number; complete: number };
    if (control.complete) {
      return { visited: 0, enqueued: 0, dead: 0, skippedUploaded: 0, complete: true, paused: null };
    }

    const maxRows = Math.max(1, Math.min(Math.trunc(options.maxRows ?? this.limits.migrationBatchRows), 5_000));
    const maxBytes = Math.max(1, Math.trunc(options.maxBytes ?? this.limits.migrationBatchBytes));
    const rows = this.db
      .prepare(
        `select rowid as rawRowid, id as rawId, created_at as createdAt,
           uploaded_at as uploadedAt,
           length(payload_json) + length(suppressed_fields_json) as rowBytes
         from buffered_events
         where rowid > ?
         order by rowid asc
         limit ?`,
      )
      .all(control.cursorRowid, maxRows) as LegacyCandidateRow[];

    let visited = 0;
    let bytes = 0;
    let enqueued = 0;
    let dead = 0;
    let skippedUploaded = 0;
    let cursor = control.cursorRowid;
    const readRaw = this.db.prepare(
      `select rowid as rawRowid, id as rawId, created_at as createdAt,
         uploaded_at as uploadedAt, payload_json as payloadJson,
         suppressed_fields_json as suppressedFieldsJson,
         repo_hash as repoHash, branch_hash as branchHash
       from buffered_events where rowid = ?`,
    );
    const run = this.db.transaction(() => {
      for (const candidate of rows) {
        const rowBytes = candidate.rowBytes ?? 0;
        visited += 1;
        cursor = candidate.rawRowid;
        if (candidate.uploadedAt) {
          skippedUploaded += 1;
          continue;
        }
        // A pre-outbox legacy row can be arbitrarily large. Classify a row
        // already above the item ceiling from its SQLite length metadata;
        // never materialize it into the migration process merely to reject it.
        if (rowBytes > this.limits.maxItemBytes) {
          dead += this.writeReceipt({
            deliveryId: ensureUuidEventId(candidate.rawId).id,
            state: "dead",
            reason: "local_item_oversize",
            attemptCount: 0,
            createdAt: candidate.createdAt,
            terminalAt: now.toISOString(),
          });
          continue;
        }
        if (bytes > 0 && bytes + rowBytes > maxBytes) {
          visited -= 1;
          cursor = candidate.rawRowid - 1;
          break;
        }
        const row = readRaw.get(candidate.rawRowid) as RawDeliveryRow | undefined;
        if (!row) continue;
        bytes += rowBytes;
        const result = this.enqueueRaw(row);
        enqueued += result.enqueued;
        dead += result.dead;
      }
      const complete = rows.length < maxRows && visited === rows.length;
      this.db
        .prepare(
          `update upload_control set
             migration_cursor_rowid = @cursor,
             migration_complete = @complete,
             migration_paused_reason = null,
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
          now: now.toISOString(),
        });
      return complete;
    });
    const complete = run();
    return { visited, bytes, enqueued, dead, skippedUploaded, complete, paused: null };
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
             base_envelope_json as baseEnvelopeJson,
             sealed_envelope_json as sealedEnvelopeJson,
             repo_hash as repoHash, branch_hash as branchHash,
             attempt_count as attemptCount
           from upload_outbox
           where state in ('pending','retry') and next_attempt_at <= ?
           order by created_at, delivery_id
           limit ?`,
        )
        .all(nowIso, maxRows) as ActiveDeliveryRow[];

      for (const row of candidates) {
        let envelopeJson = row.sealedEnvelopeJson;
        if (!envelopeJson) {
          let parsed: AiWorkIngestEvent;
          try {
            parsed = aiWorkIngestEventSchema.parse(JSON.parse(row.baseEnvelopeJson));
          } catch {
            locallyDead += this.deadActive(row.deliveryId, "local_schema_invalid", nowIso);
            continue;
          }
          const sealed = aiWorkIngestEventSchema.safeParse(
            attachFillOnlyLinkage(parsed, safeLinkage(row.repoHash), safeLinkage(row.branchHash)),
          );
          if (!sealed.success || findForbiddenRawContentFields(sealed.success ? sealed.data.event.metadata : {}).length > 0 || (sealed.success && privacyViolation(sealed.data))) {
            locallyDead += this.deadActive(row.deliveryId, "local_privacy_violation", nowIso);
            continue;
          }
          envelopeJson = JSON.stringify(sealed.data);
          const envelopeBytes = Buffer.byteLength(envelopeJson);
          if (envelopeBytes > this.limits.maxItemBytes) {
            locallyDead += this.deadActive(row.deliveryId, "local_item_oversize", nowIso);
            continue;
          }
          this.db
            .prepare(
              `update upload_outbox set sealed_envelope_json = @envelopeJson,
                 sealed_bytes = @envelopeBytes, updated_at = @now
               where delivery_id = @deliveryId and sealed_envelope_json is null`,
            )
            .run({ deliveryId: row.deliveryId, envelopeJson, envelopeBytes, now: nowIso });
        }
        const envelopeBytes = Buffer.byteLength(envelopeJson);
        if (items.length > 0 && selectedBytes + envelopeBytes > maxBytes) break;
        selectedBytes += envelopeBytes;
        const attemptCount = row.attemptCount + 1;
        this.db
          .prepare(
            `update upload_outbox set state = 'in_flight', attempt_count = @attemptCount,
               lease_id = @leaseId, lease_expires_at = @leaseExpiresAt,
               last_failure_class = 'none', updated_at = @now
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
          envelopeJson,
          envelope: JSON.parse(envelopeJson) as AiWorkIngestEvent,
          attemptCount,
        });
      }
    });
    run();
    return { leaseId, items, locallyDead, blockedBy: "none" };
  }

  acknowledge(leaseId: string, ids: string[], at = new Date()) {
    const terminalAt = at.toISOString();
    const get = this.db.prepare(
      `select raw_rowid as rawRowid, attempt_count as attemptCount, created_at as createdAt
       from upload_outbox where delivery_id = ? and state = 'in_flight' and lease_id = ?`,
    );
    const markRaw = this.db.prepare(
      `update buffered_events set uploaded_at = ? where rowid = ? and uploaded_at is null`,
    );
    const remove = this.db.prepare(`delete from upload_outbox where delivery_id = ? and lease_id = ?`);
    const run = this.db.transaction(() => {
      let acknowledged = 0;
      let markedUploaded = 0;
      for (const id of ids) {
        const row = get.get(id, leaseId) as
          | { rawRowid: number | null; attemptCount: number; createdAt: string }
          | undefined;
        if (!row) continue;
        acknowledged += this.writeReceipt({
          deliveryId: id,
          state: "acknowledged",
          reason: "remote_acknowledged",
          attemptCount: row.attemptCount,
          createdAt: row.createdAt,
          terminalAt,
        });
        if (row.rawRowid !== null) markedUploaded += markRaw.run(terminalAt, row.rawRowid).changes;
        remove.run(id, leaseId);
      }
      return { acknowledged, markedUploaded };
    });
    return run();
  }

  deadLetterRemote(leaseId: string, ids: string[], at = new Date()) {
    const terminalAt = at.toISOString();
    const get = this.db.prepare(
      `select attempt_count as attemptCount, created_at as createdAt
       from upload_outbox where delivery_id = ? and state = 'in_flight' and lease_id = ?`,
    );
    const remove = this.db.prepare(`delete from upload_outbox where delivery_id = ? and lease_id = ?`);
    const run = this.db.transaction(() => {
      let dead = 0;
      for (const id of ids) {
        const row = get.get(id, leaseId) as { attemptCount: number; createdAt: string } | undefined;
        if (!row) continue;
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
      return dead;
    });
    return run();
  }

  retry(leaseId: string, items: LeasedDeliveryItem[], failure: DeliveryFailureClass, at = new Date()) {
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
          nextAttemptAt: this.nextAttemptAt(item.deliveryId, item.attemptCount, at),
          now: at.toISOString(),
        }).changes;
      }
      return retried;
    });
    return run();
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
           migration_last_at as lastAt
         from upload_control where singleton = 1`,
      )
      .get() as {
      cursorRowid: number;
      complete: number;
      pausedReason: "pressure" | null;
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
        complete: Boolean(control.complete),
        pausedReason: control.pausedReason,
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
        mode: "compatibility_uploaded_only",
        rawTtlBlockedBy: "projection_parity",
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
    };
  }

  private deadActive(deliveryId: string, reason: DeliveryReceiptReason, terminalAt: string) {
    const row = this.db
      .prepare(
        `select attempt_count as attemptCount, created_at as createdAt
         from upload_outbox where delivery_id = ?`,
      )
      .get(deliveryId) as { attemptCount: number; createdAt: string } | undefined;
    if (!row) return 0;
    const written = this.writeReceipt({
      deliveryId,
      state: "dead",
      reason,
      attemptCount: row.attemptCount,
      createdAt: row.createdAt,
      terminalAt,
    });
    this.db.prepare(`delete from upload_outbox where delivery_id = ?`).run(deliveryId);
    return written;
  }

  private writeReceipt(input: {
    deliveryId: string;
    state: "acknowledged" | "dead";
    reason: DeliveryReceiptReason;
    attemptCount: number;
    createdAt: string;
    terminalAt: string;
  }) {
    return this.db
      .prepare(
        `insert or ignore into upload_receipts
          (delivery_id, terminal_state, reason, status_class, attempt_count, created_at, terminal_at)
         values (@deliveryId, @state, @reason, @statusClass, @attemptCount, @createdAt, @terminalAt)`,
      )
      .run({ ...input, statusClass: terminalStatusClass(input.reason) }).changes;
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
