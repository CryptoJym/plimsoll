import crypto from "node:crypto";

import type Database from "better-sqlite3";

import {
  aiWorkIngestEventSchema,
  type AiWorkIngestEvent,
} from "../../shared/src/index";
import { canonicalLinkage, sealOutboundEnvelope } from "./outbound-envelope";
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
  | "local_request_budget"
  | "remote_validation"
  | "remote_auth"
  | "remote_transient"
  | "remote_contract";

export type DeliveryCircuit = "none" | "auth_blocked" | "contract_blocked";
export type DeliveryReceiptReason =
  | "remote_acknowledged"
  | "local_evidence_quarantined"
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
    | "migration_slice_budget"
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
    pausedReason: "pressure" | "slice_budget_too_small" | null;
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

export type DeliveryValidationWitness = {
  contractHash: string;
  acknowledgedAt: string;
  item: LeasedDeliveryItem;
};

type RawDeliveryRow = {
  rawRowid: number;
  rawId: string;
  dataMode: string;
  createdAt: string;
  uploadedAt: string | null;
  payloadJson: string;
  suppressedFieldsJson: string;
  repoHash: string | null;
  branchHash: string | null;
  workspaceId: string | null;
};

type LegacyCandidateRow = Pick<
  RawDeliveryRow,
  "rawRowid" | "rawId" | "createdAt" | "uploadedAt" | "workspaceId"
> & { dataMode: string; rowBytes: number };

type ActiveDeliveryRow = {
  deliveryId: string;
  rawRowid: number | null;
  baseEnvelopeJson: string;
  sealedEnvelopeJson: string | null;
  repoHash: string | null;
  branchHash: string | null;
  attemptCount: number;
};

type RawPrivacyRow = {
  rawId: string;
  dataMode: string;
  uploadedAt: string | null;
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

function prepareDelivery(row: RawDeliveryRow, maxItemBytes: number): PreparedDelivery {
  const fallbackId = ensureUuidEventId(row.rawId).id;
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

  const deliveryId = normalized.envelope.event.id;
  const envelope = sealOutboundEnvelope(normalized.envelope);
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
  private workspaceId: string | null;

  constructor(
    private readonly db: Database.Database,
    options: { enabled?: boolean; limits?: Partial<DeliveryLimits>; workspaceId?: string } = {},
  ) {
    this.enabled = options.enabled ?? false;
    this.limits = asLimits(options.limits);
    this.workspaceId = options.workspaceId?.trim() || null;
    this.initializeSchema();
    if (this.enabled) this.reopenMigrationPastWatermark();
  }

  configure(options: {
    enabled: boolean;
    limits?: Partial<DeliveryLimits>;
    workspaceId?: string;
  }) {
    this.enabled = options.enabled;
    this.limits = asLimits(options.limits);
    if (options.workspaceId) this.setWorkspace(options.workspaceId);
    if (this.enabled) this.reopenMigrationPastWatermark();
  }

  setWorkspace(workspaceId: string) {
    const value = workspaceId.trim();
    if (!value) throw new Error("Delivery workspace requires a non-empty id.");
    this.workspaceId = value;
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

  private initializeSchema() {
    // Additive only: no query or index is built against the historical raw
    // ledger here. Large-ledger work happens solely in bounded migration slices.
    this.db.exec(`
      create table if not exists upload_outbox (
        delivery_id text primary key,
        raw_rowid integer,
        workspace_id text,
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
        validation_probe_rows integer not null default 0,
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
    const controlColumns = this.db
      .prepare(`pragma table_info(upload_control)`)
      .all() as Array<{ name: string }>;
    if (!controlColumns.some((column) => column.name === "validation_probe_rows")) {
      this.db.exec(
        `alter table upload_control
         add column validation_probe_rows integer not null default 0`,
      );
    }
    const outboxColumns = this.db
      .prepare(`pragma table_info(upload_outbox)`)
      .all() as Array<{ name: string }>;
    if (!outboxColumns.some((column) => column.name === "workspace_id")) {
      this.db.exec(`alter table upload_outbox add column workspace_id text`);
    }
    this.db.exec(
      `create index if not exists idx_upload_outbox_workspace_due
       on upload_outbox (workspace_id, state, next_attempt_at, created_at)`,
    );
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
      .run({ now: new Date().toISOString() });
  }

  noteRawAppend(rawRowid: number) {
    if (!Number.isSafeInteger(rawRowid) || rawRowid <= 0) return;
    const now = new Date().toISOString();
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
          (delivery_id, raw_rowid, workspace_id, base_envelope_json, base_bytes, repo_hash, branch_hash,
           state, attempt_count, next_attempt_at, last_failure_class, created_at, updated_at)
         select @deliveryId, @rawRowid, @workspaceId, @baseEnvelopeJson, @baseBytes, @repoHash, @branchHash,
           'pending', 0, @now, 'none', @createdAt, @now
         where not exists (
           select 1 from upload_receipts where delivery_id = @deliveryId
         )`,
      )
      .run({
        ...prepared,
        rawRowid: row.rawRowid,
        workspaceId: row.workspaceId,
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
           data_mode as dataMode,
           uploaded_at as uploadedAt, payload_json as payloadJson,
           suppressed_fields_json as suppressedFieldsJson,
           repo_hash as repoHash, branch_hash as branchHash,
           workspace_id as workspaceId
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
        repoHash: canonicalLinkage(repoHash),
        branchHash: canonicalLinkage(branchHash),
        now: new Date().toISOString(),
      }).changes;
  }

  migrateLegacy(options: { maxRows?: number; maxBytes?: number; now?: Date } = {}) {
    if (!this.enabled) return { visited: 0, enqueued: 0, dead: 0, skippedUploaded: 0, quarantinedEvidence: 0, complete: false, paused: null };
    const now = options.now ?? new Date();
    const pressure = this.status(now).pressure;
    if (pressure.degraded) {
      this.db
        .prepare(
          `update upload_control set migration_paused_reason = 'pressure', updated_at = ?
           where singleton = 1`,
        )
        .run(now.toISOString());
      return { visited: 0, enqueued: 0, dead: 0, skippedUploaded: 0, quarantinedEvidence: 0, complete: false, paused: "pressure" as const };
    }

    const control = this.db
      .prepare(
        `select migration_cursor_rowid as cursorRowid, migration_complete as complete
         from upload_control where singleton = 1`,
      )
      .get() as { cursorRowid: number; complete: number };
    if (control.complete) {
      return { visited: 0, enqueued: 0, dead: 0, skippedUploaded: 0, quarantinedEvidence: 0, complete: true, paused: null };
    }

    const maxRows = Math.max(1, Math.min(Math.trunc(options.maxRows ?? this.limits.migrationBatchRows), 5_000));
    const maxBytes = Math.max(1, Math.trunc(options.maxBytes ?? this.limits.migrationBatchBytes));
    const rows = this.db
      .prepare(
        `select rowid as rawRowid, id as rawId, created_at as createdAt,
           data_mode as dataMode, uploaded_at as uploadedAt,
           workspace_id as workspaceId,
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
    let dead = 0;
    let skippedUploaded = 0;
    let quarantinedEvidence = 0;
    let cursor = control.cursorRowid;
    let paused: "slice_budget_too_small" | null = null;
    const readRaw = this.db.prepare(
      `select rowid as rawRowid, id as rawId, created_at as createdAt,
         data_mode as dataMode,
         uploaded_at as uploadedAt, payload_json as payloadJson,
         suppressed_fields_json as suppressedFieldsJson,
         repo_hash as repoHash, branch_hash as branchHash,
         workspace_id as workspaceId
       from buffered_events where rowid = ?`,
    );
    const run = this.db.transaction(() => {
      for (const candidate of rows) {
        const rowBytes = candidate.rowBytes ?? 0;
        visited += 1;
        cursor = candidate.rawRowid;
        if (this.workspaceId !== null && candidate.workspaceId !== this.workspaceId) {
          continue;
        }
        if (candidate.uploadedAt) {
          skippedUploaded += 1;
          continue;
        }
        if (candidate.dataMode === "evidence") {
          quarantinedEvidence += 1;
          // A stale build may already have cached one or more envelopes for
          // this row under different delivery ids. Retire a bounded indexed
          // slice in the same transaction; any remainder is independently
          // rejected by the lease boundary's raw-row point lookup.
          dead += this.quarantineLinkedEvidence(
            candidate.rawRowid,
            now.toISOString(),
          );
          dead += this.writeReceipt({
            deliveryId: ensureUuidEventId(candidate.rawId).id,
            state: "dead",
            reason: "local_evidence_quarantined",
            attemptCount: 0,
            createdAt: candidate.createdAt,
            terminalAt: now.toISOString(),
          });
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
        const row = readRaw.get(candidate.rawRowid) as RawDeliveryRow | undefined;
        if (!row) continue;
        bytes += rowBytes;
        const result = this.enqueueRaw(row);
        enqueued += result.enqueued;
        dead += result.dead;
      }
      const complete = paused === null && rows.length < maxRows && visited === rows.length;
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
          now: now.toISOString(),
        });
      return complete;
    });
    const complete = run();
    return { visited, bytes, enqueued, dead, skippedUploaded, quarantinedEvidence, complete, paused };
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
           where state in ('pending','retry') and next_attempt_at <= @now
             and (@workspaceId is null or workspace_id = @workspaceId)
           order by case
               when exists (
                 select 1 from upload_validation_candidates c
                 where c.delivery_id = upload_outbox.delivery_id
               ) then 2
               when last_failure_class in ('remote_validation', 'local_request_budget') then 1
               else 0
             end,
             next_attempt_at, created_at, delivery_id
           limit @maxRows`,
        )
        .all({ now: nowIso, workspaceId: this.workspaceId, maxRows }) as ActiveDeliveryRow[];

      for (const row of candidates) {
        const authoritativeReason = this.authoritativePrivacyReason(
          row.deliveryId,
          row.rawRowid,
        );
        if (authoritativeReason) {
          locallyDead += this.deadActive(row.deliveryId, authoritativeReason, nowIso);
          continue;
        }
        let envelopeJson = row.sealedEnvelopeJson;
        if (!envelopeJson) {
          let parsed: AiWorkIngestEvent;
          try {
            parsed = aiWorkIngestEventSchema.parse(JSON.parse(row.baseEnvelopeJson));
          } catch {
            locallyDead += this.deadActive(row.deliveryId, "local_schema_invalid", nowIso);
            continue;
          }
          if (parsed.event.dataMode === "evidence") {
            locallyDead += this.deadActive(
              row.deliveryId,
              "local_evidence_quarantined",
              nowIso,
            );
            continue;
          }
          const sealed = sealOutboundEnvelope(
            attachFillOnlyLinkage(
              parsed,
              canonicalLinkage(row.repoHash),
              canonicalLinkage(row.branchHash),
            ),
          );
          if (!sealed.ok) {
            locallyDead += this.deadActive(
              row.deliveryId,
              sealed.reason === "schema" ? "local_schema_invalid" : "local_privacy_violation",
              nowIso,
            );
            continue;
          }
          envelopeJson = JSON.stringify(sealed.envelope);
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
        // Older builds may already have sealed an evidence-marked item. The
        // sealed copy is not trusted merely because it predates this gate.
        let outboundEnvelope: AiWorkIngestEvent;
        try {
          outboundEnvelope = aiWorkIngestEventSchema.parse(JSON.parse(envelopeJson));
        } catch {
          locallyDead += this.deadActive(row.deliveryId, "local_schema_invalid", nowIso);
          continue;
        }
        if (outboundEnvelope.event.dataMode === "evidence") {
          locallyDead += this.deadActive(
            row.deliveryId,
            "local_evidence_quarantined",
            nowIso,
          );
          continue;
        }
        const revalidated = sealOutboundEnvelope(outboundEnvelope);
        if (!revalidated.ok || JSON.stringify(revalidated.envelope) !== envelopeJson) {
          locallyDead += this.deadActive(row.deliveryId, "local_privacy_violation", nowIso);
          continue;
        }
        const envelopeBytes = Buffer.byteLength(envelopeJson);
        const addedBytes = envelopeBytes + (items.length > 0 ? 1 : 0);
        if (items.length > 0 && selectedBytes + addedBytes > maxBytes) break;
        selectedBytes += addedBytes;
        const attemptCount = row.attemptCount + 1;
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
      `select raw_rowid as rawRowid
       from upload_outbox
       where delivery_id = ? and state = 'in_flight' and lease_id = ?`,
    );
    return this.db.transaction(() => {
      const deliverable: LeasedDeliveryItem[] = [];
      let locallyDead = 0;
      for (const item of items) {
        const active = getActive.get(item.deliveryId, leaseId) as
          | { rawRowid: number | null }
          | undefined;
        if (!active) continue;
        const reason =
          active.rawRowid !== item.rawRowid
            ? "local_privacy_violation"
            : this.authoritativePrivacyReason(item.deliveryId, active.rawRowid);
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
      let locallyDead = 0;
      const acknowledgedIds: string[] = [];
      for (const id of ids) {
        const row = get.get(id, leaseId) as
          | { rawRowid: number | null; attemptCount: number; createdAt: string }
          | undefined;
        if (!row) continue;
        const authoritativeReason = this.authoritativePrivacyReason(id, row.rawRowid);
        if (authoritativeReason) {
          locallyDead += this.deadActive(id, authoritativeReason, terminalAt);
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
        if (row.rawRowid !== null) markedUploaded += markRaw.run(terminalAt, row.rawRowid).changes;
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
      this.clearValidationProbeIfEmpty(terminalAt);
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
      privacy: {
        mode: "metadata_only",
        evidenceVault: "not_implemented",
        legacyEvidenceDisposition: "local_quarantine_migration_required",
        liveLedgerInspection: "not_performed",
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
    this.clearValidationProbeIfEmpty(terminalAt);
    return written;
  }

  private authoritativePrivacyReason(
    deliveryId: string,
    rawRowid: number | null,
  ): DeliveryReceiptReason | null {
    const receipt = this.db
      .prepare(`select reason from upload_receipts where delivery_id = ?`)
      .get(deliveryId) as { reason: string } | undefined;
    if (receipt) {
      return receipt.reason === "local_evidence_quarantined"
        ? "local_evidence_quarantined"
        : "local_privacy_violation";
    }
    if (rawRowid === null) return "local_privacy_violation";
    const raw = this.db
      .prepare(
        `select id as rawId, data_mode as dataMode, uploaded_at as uploadedAt
         from buffered_events where rowid = ?`,
      )
      .get(rawRowid) as RawPrivacyRow | undefined;
    if (!raw || raw.uploadedAt !== null) return "local_privacy_violation";
    if (raw.dataMode === "evidence") return "local_evidence_quarantined";
    if (raw.dataMode !== "metadata") return "local_privacy_violation";
    return ensureUuidEventId(raw.rawId).id === deliveryId
      ? null
      : "local_privacy_violation";
  }

  private quarantineLinkedEvidence(rawRowid: number, terminalAt: string) {
    const rows = this.db
      .prepare(
        `select delivery_id as deliveryId
         from upload_outbox
         where raw_rowid = ?
         order by delivery_id
         limit 500`,
      )
      .all(rawRowid) as Array<{ deliveryId: string }>;
    let dead = 0;
    for (const row of rows) {
      dead += this.deadActive(row.deliveryId, "local_evidence_quarantined", terminalAt);
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
