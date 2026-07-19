import crypto from "node:crypto";
import os from "node:os";

import Database from "better-sqlite3";

import {
  canonicalizeSuppressionReceipts,
  type AiInteractionEvent,
} from "../../shared/src/index";
import type { MetricSample } from "./otlp";
import type { OtlpAdmissionDrop, OtlpDropReason } from "./otlp-admission";
import { ensureCodexReconciliationSchema } from "./codex-reconciliation";
import { DeliveryOutbox, type DeliveryLimits } from "./outbox";
import { DashboardProjectionStore } from "./dashboard-projection";
import { LearningFactStore, type LearningFactLimits } from "./learning-facts";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";

export type BufferedEventRow = {
  id: string;
  source: string;
  eventType: string;
  dataMode: string;
  observedAt: string;
  payload: AiInteractionEvent;
  suppressedFields: string[];
  createdAt: string;
  uploadedAt: string | null;
  /** Per-event repo linkage (issue 0008 stitching) — the privacy-preserving
   * key the upload paths forward as event.projectKey (issue 0036). */
  repoHash: string | null;
  branchHash: string | null;
  /** Durable upload audience. Null means legacy/unassigned and is never
   * eligible once a ledger has an initialized workspace binding. */
  workspaceId: string | null;
};

export type BufferStats = {
  count: number;
  oldestCreatedAt: string | null;
  newestCreatedAt: string | null;
  unuploadedCount: number;
  metricSampleCount: number;
  tokenAttributedEvents: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  totalCostUsd: number;
};

export type OtlpAdmissionCounter = {
  source: string;
  reason: OtlpDropReason;
  droppedCount: number;
  firstDroppedAt: string;
  lastDroppedAt: string;
};

const MACHINE = os.hostname();

function gitField(event: AiInteractionEvent, key: string): string | null {
  const git = (event.metadata as Record<string, unknown> | undefined)?.git;
  const value =
    git && typeof git === "object" ? (git as Record<string, unknown>)[key] : undefined;
  return typeof value === "string" && value ? value : null;
}

const EVENT_COLUMNS = [
  "session_id text",
  "action_class text",
  "model text",
  "input_tokens integer",
  "output_tokens integer",
  "cache_read_tokens integer",
  "cache_creation_tokens integer",
  "cost_usd real",
  "uploaded_at text",
  "repo_hash text",
  "branch_hash text",
  "head_sha text",
  "machine text",
  "account_hash text",
  "workspace_id text",
  "privacy_generation text",
  "privacy_disposition text",
  "privacy_disposed_at text",
] as const;

export class LocalEventBuffer {
  private readonly db: Database.Database;
  private workspaceId: string | null = null;
  readonly delivery: DeliveryOutbox;
  readonly projection: DashboardProjectionStore;
  readonly learningFacts: LearningFactStore;

  constructor(
    path: string,
    options: {
      delivery?: { enabled?: boolean; limits?: Partial<DeliveryLimits> };
      workspaceId?: string;
      learningFacts?: { limits?: Partial<LearningFactLimits> };
    } = {},
  ) {
    this.db = new Database(path);
    this.db.pragma("journal_mode = WAL");
    const newLedger = !this.db
      .prepare(`select 1 from sqlite_master where type='table' and name='buffered_events'`)
      .get();
    this.db.exec(`
      create table if not exists buffered_events (
        id text primary key,
        source text not null,
        event_type text not null,
        data_mode text not null,
        observed_at text not null,
        payload_json text not null,
        suppressed_fields_json text not null default '[]',
        created_at text not null
      );
      create table if not exists priority_repos (
        repo_hash text primary key,
        url text not null,
        added_at text not null
      );
      create table if not exists account_labels (
        account_hash text primary key,
        label text not null,
        auto_seeded integer not null default 1,
        first_seen text not null
      );
      create table if not exists account_aliases (
        alias_hash text primary key,
        canonical_hash text not null,
        created_at text not null
      );
      create table if not exists repo_labels (
        repo_hash text primary key,
        label text not null,
        first_seen text not null,
        last_seen text not null
      );
      create table if not exists metric_samples (
        id text primary key,
        source text not null,
        metric_name text not null,
        observed_at text not null,
        session_id text,
        model text,
        sample_type text,
        value real not null,
        attrs_json text not null default '{}',
        suppressed_fields_json text not null default '[]',
        created_at text not null
      );
      create table if not exists otlp_admission_counters (
        source text not null,
        reason text not null,
        dropped_count integer not null default 0,
        first_dropped_at text not null,
        last_dropped_at text not null,
        primary key (source, reason)
      );
      create table if not exists maintenance_state (
        key text primary key,
        value text not null,
        updated_at text not null
      );
      create table if not exists reprice_dirty_events (
        event_id text primary key,
        queued_at text not null
      );
      create table if not exists repo_enrichment_dirty (
        session_id text primary key,
        cursor_rowid integer not null default 0,
        queued_at text not null,
        updated_at text not null
      );
      create table if not exists collector_workspace_binding (
        singleton integer primary key check (singleton = 1),
        current_workspace_id text not null,
        previous_workspace_id text,
        changed_at text not null
      );
      create table if not exists session_usage_authority (
        source text not null,
        session_id text not null,
        authority text not null check (authority in ('tailer','live')),
        claimed_at text not null,
        primary key (source, session_id)
      );
    `);
    this.migrateEventColumns();
    this.db.exec(`
      create index if not exists idx_events_privacy_disposition
        on buffered_events (privacy_disposition, data_mode, created_at, id);
      create trigger if not exists trg_events_privacy_generation_insert
      after insert on buffered_events
      when new.privacy_generation is null
      begin
        update buffered_events
        set privacy_generation = lower(hex(randomblob(16)))
        where rowid = new.rowid and privacy_generation is null;
      end;
      create trigger if not exists trg_events_privacy_generation_immutable
      before update of privacy_generation on buffered_events
      when old.privacy_generation is not null
        and new.privacy_generation is not old.privacy_generation
      begin
        select raise(abort, 'privacy_generation_is_immutable');
      end;
      create trigger if not exists trg_events_privacy_disposition_terminal
      before update of privacy_disposition on buffered_events
      when old.privacy_disposition is not null
        and new.privacy_disposition is not old.privacy_disposition
      begin
        select raise(abort, 'privacy_disposition_is_terminal');
      end;
    `);
    this.delivery = new DeliveryOutbox(this.db, options.delivery);
    if (options.workspaceId) this.useWorkspace(options.workspaceId);
    this.db.exec(`
      create index if not exists idx_events_upload on buffered_events (uploaded_at, created_at);
      create index if not exists idx_events_workspace_upload
        on buffered_events (workspace_id, uploaded_at, created_at);
      create index if not exists idx_events_session on buffered_events (session_id, observed_at);
      create index if not exists idx_events_observed on buffered_events (observed_at);
      create index if not exists idx_events_repo on buffered_events (repo_hash, branch_hash);
      create index if not exists idx_events_account on buffered_events (account_hash, observed_at);
      create index if not exists idx_metrics_name on metric_samples (metric_name, observed_at);
      create index if not exists idx_metrics_session on metric_samples (session_id);
      create index if not exists idx_events_unpriced_usage
        on buffered_events (id)
        where event_type in ('usage_rollout','usage_transcript')
          and cost_usd is null and model is not null;
      create index if not exists idx_events_repo_enrichment_seed
        on buffered_events (id)
        where session_id is not null and (
          repo_hash is not null or input_tokens is not null or
          output_tokens is not null or cost_usd is not null
        );

      -- Dirty-work receipts are written in the same SQLite transaction as
      -- the event mutation. A crash can therefore replay work, but cannot
      -- leave a newly appended token/linkage row invisible to maintenance.
      create trigger if not exists trg_events_reprice_dirty_insert
      after insert on buffered_events
      when new.event_type in ('usage_rollout','usage_transcript')
        and new.cost_usd is null and new.model is not null
      begin
        insert into reprice_dirty_events (event_id, queued_at)
        values (new.id, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        on conflict(event_id) do update set queued_at = excluded.queued_at;
      end;

      create trigger if not exists trg_events_reprice_dirty_update
      after update of event_type, cost_usd, model on buffered_events
      when new.event_type in ('usage_rollout','usage_transcript')
        and new.cost_usd is null and new.model is not null
      begin
        insert into reprice_dirty_events (event_id, queued_at)
        values (new.id, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        on conflict(event_id) do update set queued_at = excluded.queued_at;
      end;

      create trigger if not exists trg_events_repo_dirty_insert
      after insert on buffered_events
      when new.session_id is not null and (
        new.repo_hash is not null or new.input_tokens is not null or
        new.output_tokens is not null or new.cost_usd is not null
      )
      begin
        insert into repo_enrichment_dirty
          (session_id, cursor_rowid, queued_at, updated_at)
        values
          (new.session_id, 0, strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        on conflict(session_id) do update set
          cursor_rowid = 0,
          updated_at = excluded.updated_at;
      end;

      create trigger if not exists trg_events_repo_dirty_update
      after update of session_id, repo_hash, branch_hash, input_tokens, output_tokens, cost_usd
      on buffered_events
      when new.session_id is not null and (
        new.repo_hash is not null or new.input_tokens is not null or
        new.output_tokens is not null or new.cost_usd is not null
      )
      begin
        insert into repo_enrichment_dirty
          (session_id, cursor_rowid, queued_at, updated_at)
        values
          (new.session_id, 0, strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        on conflict(session_id) do update set
          cursor_rowid = 0,
          updated_at = excluded.updated_at;
      end;

      -- Repo enrichment may discover linkage after capture. The delivery
      -- copy accepts fill-only hashes until its first seal; retries never
      -- change the bytes already attempted.
      drop trigger if exists trg_events_outbox_linkage_update;
      create trigger if not exists trg_events_outbox_linkage_update_v2
      after update of repo_hash, branch_hash on buffered_events
      when (
        length(trim(new.repo_hash)) = 71 and
        lower(substr(trim(new.repo_hash), 1, 7)) = 'sha256:' and
        lower(substr(trim(new.repo_hash), 8)) not glob '*[^0-9a-f]*'
      ) or (
        length(trim(new.branch_hash)) = 71 and
        lower(substr(trim(new.branch_hash), 1, 7)) = 'sha256:' and
        lower(substr(trim(new.branch_hash), 8)) not glob '*[^0-9a-f]*'
      )
      begin
        update upload_outbox set
          repo_hash = coalesce(repo_hash,
            case when
              length(trim(new.repo_hash)) = 71 and
              lower(substr(trim(new.repo_hash), 1, 7)) = 'sha256:' and
              lower(substr(trim(new.repo_hash), 8)) not glob '*[^0-9a-f]*'
            then lower(trim(new.repo_hash)) end),
          branch_hash = coalesce(branch_hash,
            case when
              length(trim(new.branch_hash)) = 71 and
              lower(substr(trim(new.branch_hash), 1, 7)) = 'sha256:' and
              lower(substr(trim(new.branch_hash), 8)) not glob '*[^0-9a-f]*'
            then lower(trim(new.branch_hash)) end),
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where raw_rowid = new.rowid
          and sealed_envelope_json is null and attempt_count = 0;
      end;
    `);
    ensureCodexReconciliationSchema(this.db);
    this.learningFacts = new LearningFactStore(
      this.db,
      options.learningFacts?.limits,
    );
    this.projection = new DashboardProjectionStore(this.db, { newLedger });
  }

  /**
   * Select the ledger's upload audience. The first selection migrates legacy
   * unassigned rows. A later mismatch fails closed: only join's explicit
   * transition may change an initialized ledger audience.
   */
  useWorkspace(workspaceId: string) {
    const requested = workspaceId.trim();
    if (!requested) throw new Error("Workspace binding requires a non-empty workspace id.");
    const run = this.db.transaction(() => {
      const binding = this.db
        .prepare(
          `select current_workspace_id as currentWorkspaceId,
             previous_workspace_id as previousWorkspaceId
           from collector_workspace_binding where singleton = 1`,
        )
        .get() as
        | { currentWorkspaceId: string; previousWorkspaceId: string | null }
        | undefined;
      if (binding && binding.currentWorkspaceId !== requested) {
        throw new Error(
          `Ledger workspace binding mismatch: active=${binding.currentWorkspaceId}, requested=${requested}. ` +
            "Use the transactional join/reassign flow; refusing to relabel queued rows.",
        );
      }
      if (!binding) {
        const now = new Date().toISOString();
        this.db
          .prepare(
            `insert into collector_workspace_binding
              (singleton, current_workspace_id, previous_workspace_id, changed_at)
             values (1, @workspaceId, null, @now)`,
          )
          .run({ workspaceId: requested, now });
        this.db
          .prepare(`update buffered_events set workspace_id = ? where workspace_id is null`)
          .run(requested);
        this.delivery.bindUnassignedWorkspace(requested);
      } else if (binding.previousWorkspaceId === null) {
        // Pre-reassignment compatibility: direct/older producers in the same
        // continuous workspace may still append unassigned rows. Once any
        // transition has occurred, null stays quarantined forever.
        this.db
          .prepare(`update buffered_events set workspace_id = ? where workspace_id is null`)
          .run(requested);
        this.delivery.bindUnassignedWorkspace(requested);
      }
    });
    run();
    this.workspaceId = requested;
    this.delivery.setWorkspace(requested);
    return requested;
  }

  /**
   * Transactional reassignment boundary inside the ledger. Existing and
   * legacy-unassigned rows stay bound to `fromWorkspaceId`; only subsequent
   * appends are labeled for `toWorkspaceId`.
   */
  transitionWorkspace(fromWorkspaceId: string, toWorkspaceId: string) {
    const from = fromWorkspaceId.trim();
    const to = toWorkspaceId.trim();
    if (!from || !to) throw new Error("Workspace transition requires non-empty ids.");
    if (from === to) {
      this.useWorkspace(to);
      return { fromWorkspaceId: from, toWorkspaceId: to, boundLegacyRows: 0 };
    }
    let boundLegacyRows = 0;
    const run = this.db.transaction(() => {
      const binding = this.db
        .prepare(
          `select current_workspace_id as currentWorkspaceId
           from collector_workspace_binding where singleton = 1`,
        )
        .get() as { currentWorkspaceId: string } | undefined;
      if (!binding) {
        this.db
          .prepare(
            `insert into collector_workspace_binding
              (singleton, current_workspace_id, previous_workspace_id, changed_at)
             values (1, @from, null, @now)`,
          )
          .run({ from, now: new Date().toISOString() });
      } else if (binding.currentWorkspaceId !== from) {
        throw new Error(
          `Cannot transition ledger from ${from}: it is bound to ${binding.currentWorkspaceId}.`,
        );
      }
      boundLegacyRows += this.db
        .prepare(`update buffered_events set workspace_id = ? where workspace_id is null`)
        .run(from).changes;
      boundLegacyRows += this.delivery.bindUnassignedWorkspace(from);
      this.db
        .prepare(
          `update collector_workspace_binding set
             previous_workspace_id = current_workspace_id,
             current_workspace_id = @to,
             changed_at = @now
           where singleton = 1`,
        )
        .run({ to, now: new Date().toISOString() });
      // Auth/contract circuits describe the prior workspace endpoint and must
      // not block the newly authenticated audience after reassignment.
      this.delivery.clearCircuit();
    });
    run();
    this.workspaceId = to;
    this.delivery.setWorkspace(to);
    return { fromWorkspaceId: from, toWorkspaceId: to, boundLegacyRows };
  }

  workspaceBinding() {
    const row = this.db
      .prepare(
        `select current_workspace_id as currentWorkspaceId,
           previous_workspace_id as previousWorkspaceId, changed_at as changedAt
         from collector_workspace_binding where singleton = 1`,
      )
      .get() as
      | { currentWorkspaceId: string; previousWorkspaceId: string | null; changedAt: string }
      | undefined;
    return row ?? null;
  }

  private migrateEventColumns() {
    const existing = new Set(
      (this.db.pragma("table_info(buffered_events)") as Array<{ name: string }>).map(
        (column) => column.name,
      ),
    );
    for (const definition of EVENT_COLUMNS) {
      const name = definition.split(" ")[0];
      if (!existing.has(name)) {
        this.db.exec(`alter table buffered_events add column ${definition}`);
      }
    }
    // Accounts are tied to emails (issue 0028). LOCAL-ONLY like labels:
    // structurally excluded from uploads, proof-enforced.
    const labelColumns = new Set(
      (this.db.pragma("table_info(account_labels)") as Array<{ name: string }>).map(
        (column) => column.name,
      ),
    );
    if (!labelColumns.has("email")) {
      this.db.exec(`alter table account_labels add column email text`);
    }
    const metricColumns = new Set(
      (this.db.pragma("table_info(metric_samples)") as Array<{ name: string }>).map(
        (column) => column.name,
      ),
    );
    if (!metricColumns.has("suppressed_fields_json")) {
      this.db.exec(
        `alter table metric_samples add column suppressed_fields_json text not null default '[]'`,
      );
    }
  }

  private appendInCurrentTransaction(event: AiInteractionEvent, suppressedFields: string[] = []) {
    if (event.dataMode === "evidence") {
      throw new Error(
        "Raw evidence rows cannot be appended to the ordinary ledger; the encrypted evidence vault is not implemented.",
      );
    }
    const createdAt = new Date().toISOString();
    if (!this.claimSessionUsageAuthority(event, createdAt)) return false;
    const privacyGeneration = crypto.randomUUID();
    const canonicalSuppressedFields = canonicalizeSuppressionReceipts(suppressedFields);
    const result = this.db
      .prepare(
        `insert or ignore into buffered_events
          (id, source, event_type, data_mode, observed_at, payload_json, suppressed_fields_json,
           created_at, session_id, action_class, model, input_tokens, output_tokens,
           cache_read_tokens, cache_creation_tokens, cost_usd, uploaded_at, repo_hash, branch_hash, head_sha,
           machine, account_hash, workspace_id, privacy_generation)
        values
          (@id, @source, @eventType, @dataMode, @observedAt, @payloadJson, @suppressedFieldsJson,
           @createdAt, @sessionId, @actionClass, @model, @inputTokens, @outputTokens,
           @cacheReadTokens, @cacheCreationTokens, @costUsd, null, @repoHash, @branchHash, @headSha,
           @machine, @accountHash, @workspaceId, @privacyGeneration)`,
      )
      .run({
        id: event.id,
        source: event.source,
        eventType: event.eventType,
        dataMode: event.dataMode,
        observedAt: event.observedAt,
        payloadJson: JSON.stringify(event),
        suppressedFieldsJson: JSON.stringify(canonicalSuppressedFields),
        createdAt,
        sessionId: event.sessionId ?? null,
        actionClass: event.actionClass ?? null,
        model: event.model ?? null,
        inputTokens: event.inputTokens ?? null,
        outputTokens: event.outputTokens ?? null,
        cacheReadTokens: event.cacheReadTokens ?? null,
        cacheCreationTokens: event.cacheCreationTokens ?? null,
        costUsd: event.costUsd ?? null,
        repoHash: gitField(event, "remoteUrlHash"),
        branchHash: gitField(event, "branchHash"),
        headSha: gitField(event, "headSha"),
        machine: MACHINE,
        accountHash: event.actorId ?? null,
        workspaceId: this.workspaceId,
        privacyGeneration,
      });
    if (result.changes > 0) {
      this.delivery.noteRawAppend(Number(result.lastInsertRowid));
      this.delivery.enqueueRaw({
        rawRowid: Number(result.lastInsertRowid),
        rawId: event.id,
        dataMode: event.dataMode,
        createdAt,
        uploadedAt: null,
        payloadJson: JSON.stringify(event),
        suppressedFieldsJson: JSON.stringify(canonicalSuppressedFields),
        repoHash: gitField(event, "remoteUrlHash"),
        branchHash: gitField(event, "branchHash"),
        workspaceId: this.workspaceId,
        privacyGeneration,
        privacyDisposition: null,
      });
      if (event.actorId) this.seedAccountLabel(event.actorId);
      // The raw row, privacy-safe fact delta, and delivery envelope share the
      // caller's SQLite transaction. Projection failure is contained as a
      // durable repair receipt so capture remains available.
      this.projection.tryApplyRawRow(Number(result.lastInsertRowid));
      return true;
    }
    // Deterministic replay never rewrites the evidence row or resets its
    // upload marker. It may repair an absent delivery projection from the
    // already-committed raw truth.
    this.delivery.repairRawById(event.id);
    return false;
  }

  /**
   * Token accounting is first-writer-authoritative for an entire session, not
   * merely for one maintenance slice. Without this durable claim, a chunked
   * rollout could commit early deltas, then allow a later OTLP event to make
   * the tailer skip the remainder of that same cumulative stream.
   */
  private claimSessionUsageAuthority(event: AiInteractionEvent, claimedAt: string) {
    if (
      !event.sessionId ||
      (event.inputTokens === undefined && event.outputTokens === undefined) ||
      (event.source !== "codex" && event.source !== "claude_code")
    ) {
      return true;
    }
    const tailerEvent =
      event.eventType === "usage_rollout" || event.eventType === "usage_transcript";
    const desired = tailerEvent ? "tailer" : "live";
    const existing = this.db
      .prepare(
        `select authority from session_usage_authority
         where source = ? and session_id = ?`,
      )
      .get(event.source, event.sessionId) as { authority: "tailer" | "live" } | undefined;
    if (existing) return existing.authority === desired;

    // Upgrade old ledgers deterministically before admitting new work. Live
    // capture wins an already-mixed legacy session; otherwise the existing
    // source that actually has token rows becomes authoritative.
    const legacy = this.db
      .prepare(
        `select
           max(case when event_type in ('usage_rollout','usage_transcript') then 1 else 0 end) as tailer,
           max(case when event_type not in ('usage_rollout','usage_transcript') then 1 else 0 end) as live
         from buffered_events
         where source = ? and session_id = ?
           and (input_tokens is not null or output_tokens is not null)`,
      )
      .get(event.source, event.sessionId) as { tailer: number | null; live: number | null };
    const authority = legacy.live ? "live" : legacy.tailer ? "tailer" : desired;
    this.db
      .prepare(
        `insert into session_usage_authority (source, session_id, authority, claimed_at)
         values (?, ?, ?, ?)
         on conflict(source, session_id) do nothing`,
      )
      .run(event.source, event.sessionId, authority, claimedAt);
    return authority === desired;
  }

  sessionUsageAuthority(source: "codex" | "claude_code", sessionId: string) {
    const row = this.db
      .prepare(
        `select authority from session_usage_authority
         where source = ? and session_id = ?`,
      )
      .get(source, sessionId) as { authority: "tailer" | "live" } | undefined;
    if (row) return row.authority;
    const legacy = this.db
      .prepare(
        `select
           max(case when event_type in ('usage_rollout','usage_transcript') then 1 else 0 end) as tailer,
           max(case when event_type not in ('usage_rollout','usage_transcript') then 1 else 0 end) as live
         from buffered_events
         where source = ? and session_id = ?
           and (input_tokens is not null or output_tokens is not null)`,
      )
      .get(source, sessionId) as { tailer: number | null; live: number | null };
    return legacy.live ? "live" : legacy.tailer ? "tailer" : null;
  }

  append(event: AiInteractionEvent, suppressedFields: string[] = []) {
    return this.db.transaction(() => this.appendInCurrentTransaction(event, suppressedFields))();
  }

  appendMany(
    entries: Array<{ event: AiInteractionEvent; suppressedFields: string[] }>,
    metricSamples: MetricSample[] = [],
    admissionDrops: OtlpAdmissionDrop[] = [],
  ) {
    const run = this.db.transaction(() => {
      for (const entry of entries) {
        this.appendInCurrentTransaction(entry.event, entry.suppressedFields);
      }
      for (const sample of metricSamples) {
        this.appendMetricSample(sample);
      }
      const now = new Date().toISOString();
      for (const drop of admissionDrops) {
        this.recordOtlpAdmissionDrop(drop, now);
      }
    });
    run();
  }

  private recordOtlpAdmissionDrop(drop: OtlpAdmissionDrop, now: string) {
    if (!Number.isSafeInteger(drop.count) || drop.count <= 0) return;
    this.db
      .prepare(
        `insert into otlp_admission_counters
          (source, reason, dropped_count, first_dropped_at, last_dropped_at)
         values (@source, @reason, @count, @now, @now)
         on conflict(source, reason) do update set
           dropped_count = dropped_count + excluded.dropped_count,
           last_dropped_at = excluded.last_dropped_at`,
      )
      .run({ ...drop, now });
  }

  otlpAdmissionCounters(): OtlpAdmissionCounter[] {
    return this.db
      .prepare(
        `select source, reason, dropped_count as droppedCount,
           first_dropped_at as firstDroppedAt, last_dropped_at as lastDroppedAt
         from otlp_admission_counters
         order by source, reason`,
      )
      .all() as OtlpAdmissionCounter[];
  }

  private seededAccounts = new Set<string>();

  /** Auto-seed a friendly label the first time an account hash is seen. */
  private seedAccountLabel(accountHash: string) {
    if (this.seededAccounts.has(accountHash)) return;
    this.seededAccounts.add(accountHash);
    const exists = this.db
      .prepare(`select 1 from account_labels where account_hash = ?`)
      .get(accountHash);
    if (exists) return;
    const base = `${os.userInfo().username}@${MACHINE}`;
    const taken = this.db
      .prepare(`select count(*) as n from account_labels where label like ?`)
      .get(`${base}%`) as { n: number };
    const label = taken.n === 0 ? base : `${base} ·${accountHash.replace("sha256:", "").slice(0, 6)}`;
    this.db
      .prepare(
        `insert or ignore into account_labels (account_hash, label, auto_seeded, first_seen)
         values (?, ?, 1, ?)`,
      )
      .run(accountHash, label, new Date().toISOString());
  }

  setPriorityRepo(repoHash: string, url: string) {
    this.db
      .prepare(
        `insert or replace into priority_repos (repo_hash, url, added_at) values (?, ?, ?)`,
      )
      .run(repoHash, url, new Date().toISOString());
    this.projection.markSnapshotDirty();
  }

  removePriorityRepo(repoHash: string) {
    const changes = this.db.prepare(`delete from priority_repos where repo_hash = ?`).run(repoHash).changes;
    if (changes) this.projection.markSnapshotDirty();
    return changes;
  }

  listPriorityRepos() {
    return this.db
      .prepare(`select repo_hash as repoHash, url, added_at as addedAt from priority_repos order by added_at`)
      .all() as Array<{ repoHash: string; url: string; addedAt: string }>;
  }

  /**
   * Local-only: the email an account is tied to. Never included in upload
   * batches (same boundary as labels). Empty string clears it. Inserts a
   * label row if the account has never been seen, so an email can be staged
   * before its first event.
   */
  setAccountEmail(accountHash: string, email: string) {
    const trimmed = email.trim().slice(0, 120);
    this.db
      .prepare(
        `insert into account_labels (account_hash, label, auto_seeded, first_seen, email)
         values (@accountHash, @accountHash, 1, @now, @email)
         on conflict(account_hash) do update set email = @email`,
      )
      .run({ accountHash, email: trimmed || null, now: new Date().toISOString() });
    this.projection.invalidatePresentation();
  }

  /** Local-only display mapping; never included in upload batches. */
  setAccountLabel(accountHash: string, label: string) {
    this.db
      .prepare(
        `insert into account_labels (account_hash, label, auto_seeded, first_seen)
         values (@accountHash, @label, 0, @now)
         on conflict(account_hash) do update set label = @label, auto_seeded = 0`,
      )
      .run({ accountHash, label, now: new Date().toISOString() });
    this.projection.invalidatePresentation();
  }

  /**
   * Local-only identity merge: events under alias_hash display as
   * canonical_hash (issue 0023 — the v1→v2 sanitizer chain split one human
   * into several hash forms). Read-time only: event rows are immutable
   * history and are never rewritten; the table never enters upload batches.
   * Structure stays flat — a canonical cannot itself be an alias, and
   * aliases pointing at the new alias are repointed to its canonical.
   */
  setAccountAlias(aliasHash: string, canonicalHash: string) {
    if (aliasHash === canonicalHash) {
      throw new Error("alias and canonical must differ");
    }
    const canonicalIsAlias = this.db
      .prepare(`select canonical_hash as c from account_aliases where alias_hash = ?`)
      .get(canonicalHash) as { c: string } | undefined;
    const target = canonicalIsAlias ? canonicalIsAlias.c : canonicalHash;
    if (target === aliasHash) {
      throw new Error("merge would create a cycle");
    }
    const now = new Date().toISOString();
    this.db
      .prepare(
        `insert into account_aliases (alias_hash, canonical_hash, created_at)
         values (@aliasHash, @target, @now)
         on conflict(alias_hash) do update set canonical_hash = @target, created_at = @now`,
      )
      .run({ aliasHash, target, now });
    // Flatten: anything that pointed at the new alias follows it to target.
    this.db
      .prepare(`update account_aliases set canonical_hash = ? where canonical_hash = ?`)
      .run(target, aliasHash);
    this.projection.queueAccountInvalidation(aliasHash);
    this.projection.queueAccountInvalidation(target);
  }

  removeAccountAlias(aliasHash: string) {
    const previous = this.db
      .prepare(`select canonical_hash as canonicalHash from account_aliases where alias_hash = ?`)
      .get(aliasHash) as { canonicalHash: string } | undefined;
    this.db.prepare(`delete from account_aliases where alias_hash = ?`).run(aliasHash);
    this.projection.queueAccountInvalidation(aliasHash);
    if (previous) this.projection.queueAccountInvalidation(previous.canonicalHash);
  }

  listAccountAliases() {
    return this.db
      .prepare(
        `select alias_hash as aliasHash, canonical_hash as canonicalHash, created_at as createdAt
         from account_aliases order by created_at`,
      )
      .all() as Array<{ aliasHash: string; canonicalHash: string; createdAt: string }>;
  }

  /** Local-only display mapping; never included in upload batches. */
  recordRepoLabel(repoHash: string, label: string) {
    const now = new Date().toISOString();
    this.db
      .prepare(
        `insert into repo_labels (repo_hash, label, first_seen, last_seen)
         values (@repoHash, @label, @now, @now)
         on conflict(repo_hash) do update set label = @label, last_seen = @now`,
      )
      .run({ repoHash, label, now });
    this.projection.invalidatePresentation();
  }

  appendMetricSample(sample: MetricSample) {
    this.db
      .prepare(
        `insert into metric_samples
          (id, source, metric_name, observed_at, session_id, model, sample_type, value, attrs_json,
           suppressed_fields_json, created_at)
        values
          (@id, @source, @metricName, @observedAt, @sessionId, @model, @sampleType, @value, @attrsJson,
           @suppressedFieldsJson, @createdAt)
        on conflict(id) do update set source=excluded.source,metric_name=excluded.metric_name,
          observed_at=excluded.observed_at,session_id=excluded.session_id,model=excluded.model,
          sample_type=excluded.sample_type,value=excluded.value,attrs_json=excluded.attrs_json,
          suppressed_fields_json=excluded.suppressed_fields_json,
          created_at=excluded.created_at`,
      )
      .run({
        id: sample.id,
        source: sample.source,
        metricName: sample.metricName,
        observedAt: sample.observedAt,
        sessionId: sample.sessionId ?? null,
        model: sample.model ?? null,
        sampleType: sample.sampleType ?? null,
        value: sample.value,
        attrsJson: JSON.stringify(sample.attrs ?? {}),
        suppressedFieldsJson: JSON.stringify(
          canonicalizeSuppressionReceipts(sample.suppressedFields ?? []),
        ),
        createdAt: new Date().toISOString(),
      });
  }

  private rowToBufferedEvent(row: {
    id: string;
    source: string;
    eventType: string;
    dataMode: string;
    observedAt: string;
    payloadJson: string;
    suppressedFieldsJson: string;
    createdAt: string;
    uploadedAt: string | null;
    repoHash: string | null;
    branchHash: string | null;
    workspaceId: string | null;
  }): BufferedEventRow {
    let storedSuppressedFields: unknown;
    try {
      storedSuppressedFields = JSON.parse(row.suppressedFieldsJson) as unknown;
    } catch {
      storedSuppressedFields = [undefined];
    }
    return {
      id: row.id,
      source: row.source,
      eventType: row.eventType,
      dataMode: row.dataMode,
      observedAt: row.observedAt,
      payload: JSON.parse(row.payloadJson) as AiInteractionEvent,
      suppressedFields: canonicalizeSuppressionReceipts(
        Array.isArray(storedSuppressedFields) ? storedSuppressedFields : [undefined],
      ),
      createdAt: row.createdAt,
      uploadedAt: row.uploadedAt ?? null,
      repoHash: row.repoHash ?? null,
      branchHash: row.branchHash ?? null,
      workspaceId: row.workspaceId ?? null,
    };
  }

  list(limit = 100): BufferedEventRow[] {
    const privacyEligible = terminalPrivacyEligibilitySql(this.db, "buffered_events");
    const rows = this.db
      .prepare(
        `select id, source, event_type as eventType, data_mode as dataMode,
          observed_at as observedAt, payload_json as payloadJson,
          suppressed_fields_json as suppressedFieldsJson, created_at as createdAt,
          uploaded_at as uploadedAt, repo_hash as repoHash, branch_hash as branchHash,
          workspace_id as workspaceId
        from buffered_events
        where ${privacyEligible}
        order by created_at desc
        limit ?`,
      )
      .all(limit) as Parameters<LocalEventBuffer["rowToBufferedEvent"]>[0][];

    return rows.map((row) => this.rowToBufferedEvent(row));
  }

  listUnuploaded(options: { maxRows?: number; maxBytes?: number } = {}): BufferedEventRow[] {
    const maxRows = Math.max(1, Math.min(options.maxRows ?? 500, 500));
    const maxBytes = options.maxBytes ?? 1_500_000;
    const privacyEligible = terminalPrivacyEligibilitySql(this.db, "buffered_events");
    const rows = this.db
      .prepare(
        `select id, source, event_type as eventType, data_mode as dataMode,
          observed_at as observedAt, payload_json as payloadJson,
          suppressed_fields_json as suppressedFieldsJson, created_at as createdAt,
          uploaded_at as uploadedAt, repo_hash as repoHash, branch_hash as branchHash,
          workspace_id as workspaceId,
          length(cast(payload_json as blob)) as payloadBytes
        from buffered_events
        where uploaded_at is null
          and ${privacyEligible}
          and (? is null or workspace_id = ?)
        order by created_at asc
        limit ?`,
      )
      .all(this.workspaceId, this.workspaceId, maxRows) as Array<
      Parameters<LocalEventBuffer["rowToBufferedEvent"]>[0] & { payloadBytes: number }
    >;

    const selected: BufferedEventRow[] = [];
    let bytes = 0;
    for (const row of rows) {
      if (selected.length > 0 && bytes + row.payloadBytes > maxBytes) break;
      bytes += row.payloadBytes;
      selected.push(this.rowToBufferedEvent(row));
    }

    return selected;
  }

  markUploaded(ids: string[], uploadedAt = new Date().toISOString()) {
    if (ids.length === 0) return 0;
    const privacyEligible = terminalPrivacyEligibilitySql(this.db, "buffered_events");
    const mark = this.db.prepare(
      `update buffered_events set uploaded_at = ? where id = ? and ${privacyEligible}`,
    );
    const run = this.db.transaction((eventIds: string[]) => {
      let updated = 0;
      for (const id of eventIds) {
        updated += mark.run(uploadedAt, id).changes;
      }
      return updated;
    });
    return run(ids);
  }

  prune(retentionDays = 90) {
    const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000).toISOString();
    // Compatibility gate until #80 proves projection parity: uploaded raw
    // rows retain the historical cleanup behavior, while pending/dead raw
    // evidence never expires independently. `/status.delivery.retention`
    // names this honestly as compatibility_uploaded_only rather than claiming
    // a raw TTL the current analytics path cannot yet tolerate.
    // An upgrade must not delete the history its initial projection/parity
    // passes have not consumed. This preserves the existing uploaded-only
    // compatibility cleanup without silently activating an independent raw TTL.
    const events = this.projection.status().parityReady
      ? this.db
        .prepare(`delete from buffered_events where created_at < ? and uploaded_at is not null`)
        .run(cutoff).changes
      : 0;
    const metricSamples = this.db
      .prepare(`delete from metric_samples where created_at < ?`)
      .run(cutoff).changes;
    return { cutoff, events, metricSamples };
  }

  stats(): BufferStats {
    const privacyEligible = terminalPrivacyEligibilitySql(this.db, "buffered_events");
    const events = this.db
      .prepare(
        `select count(*) as count,
          min(created_at) as oldestCreatedAt,
          max(created_at) as newestCreatedAt,
          sum(case when uploaded_at is null then 1 else 0 end) as unuploadedCount,
          sum(case when input_tokens is not null or output_tokens is not null then 1 else 0 end) as tokenAttributedEvents,
          coalesce(sum(input_tokens), 0) as totalInputTokens,
          coalesce(sum(output_tokens), 0) as totalOutputTokens,
          coalesce(sum(cost_usd), 0) as totalCostUsd
        from buffered_events
        where ${privacyEligible}`,
      )
      .get() as Omit<BufferStats, "metricSampleCount">;

    const metrics = this.db
      .prepare(`select count(*) as count from metric_samples`)
      .get() as { count: number };

    return {
      count: events.count,
      oldestCreatedAt: events.oldestCreatedAt,
      newestCreatedAt: events.newestCreatedAt,
      unuploadedCount: events.unuploadedCount ?? 0,
      metricSampleCount: metrics.count,
      tokenAttributedEvents: events.tokenAttributedEvents ?? 0,
      totalInputTokens: events.totalInputTokens ?? 0,
      totalOutputTokens: events.totalOutputTokens ?? 0,
      totalCostUsd: events.totalCostUsd ?? 0,
    };
  }

  tokenCoverage(sinceDays = 7) {
    const since = new Date(Date.now() - sinceDays * 24 * 60 * 60 * 1000).toISOString();
    const privacyEligible = terminalPrivacyEligibilitySql(this.db, "buffered_events");
    return this.db
      .prepare(
        `select source,
          count(distinct session_id) as sessions,
          count(distinct case when input_tokens is not null then session_id end) as sessionsWithTokens,
          sum(case when event_type = 'assistant_response' then 1 else 0 end) as assistantResponses,
          sum(case when input_tokens is not null then 1 else 0 end) as tokenAttributedEvents,
          coalesce(sum(input_tokens), 0) as inputTokens,
          coalesce(sum(output_tokens), 0) as outputTokens,
          coalesce(sum(cost_usd), 0) as costUsd
        from buffered_events
        where observed_at >= ? and session_id is not null and ${privacyEligible}
        group by source`,
      )
      .all(since);
  }

  /** Read access for dashboard queries; do not write through this. */
  get database() {
    return this.db;
  }

  close() {
    this.db.close();
  }
}
