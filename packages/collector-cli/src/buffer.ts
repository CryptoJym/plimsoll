import { openLedgerDatabase, openLedgerDatabaseForStartup } from "./ledger-connection";
import { recoverInvalidLedgerPublication } from "./fresh-ledger-cutover";
import { ensureCodexLiveUsageSchema, liveUsageAppendAllowed, liveUsageInstallationEpoch,
  liveUsageMetricAllowed } from "./codex-live-usage-ledger";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import { performance } from "node:perf_hooks";
import type { Worker } from "node:worker_threads";

import Database from "better-sqlite3";
import { z } from "zod";
import { advanceCaptureBaselineEnrollment } from "./capture-baseline";
import { captureRootEventInstallationEpoch, promoteClaudeRootSightingsForRaw,
  promoteLegacyClaudeRootSightings } from "./capture-root-inventory";

import {
  canonicalizeSuppressionReceipts,
  LOCAL_TENANT_ID,
  type AiInteractionEvent,
} from "../../shared/src/index";
import type { MetricSample } from "./otlp";
import type { OtlpAdmissionDrop, OtlpDropReason } from "./otlp-admission";
import { ensureCodexReconciliationSchema } from "./codex-reconciliation";
import {
  CODEX_USAGE_DUPLICATE_REASON,
  buildCodexUsagePairingIndexes,
  ensureCodexUsagePairingSchema,
  pairCodexUsageEvent,
} from "./codex-usage-pairing";
import { ensureSessionContextIndexSchema } from "./session-context-index";
import { ensureSessionSummarySchema } from "./session-summary";
import { DeliveryOutbox, type DeliveryLimits } from "./outbox";
import { registerRetentionDeliveryId } from "./delivery-id";
import { countRetentionHoldsOffThread } from "./retention-hold-count";
import { DashboardProjectionStore } from "./dashboard-projection";
import type { LedgerOpenTimingSink } from "./open-timing";
import { LearningFactStore, type LearningFactLimits } from "./learning-facts";
import { promoteRuntimeLearningFacts } from "./runtime-facts";
import { ensureWeeklyToolStatsSchema } from "./weekly-tool-stats";
import { ensureFinanceProvenanceSchema, initializeFinanceSourceCoverage, markFinancePublicationDirty,
  advanceFinanceRetentionWatermarks, type FinanceCoverageMutationRow } from "./history-coverage";
import { legacyNullLineageReceiptMatchSql, terminalPrivacyEligibilitySql } from "./privacy-disposition";
import { ensureRepoContextLinkDispositionSchema } from "./repo-context-link-dispositions";
import {
  canonicalRepoContextCwd,
  REPO_CONTEXT_CAPTURE_POLICY_GENERATION,
  peekRepoContextSidecar,
  peekRepoContextId,
  REPO_CONTEXT_RESOLVER_VERSION,
  takeRepoContextId,
  takeRepoContextSidecar,
  validRepoContextId,
  validRepoContextOccurrence,
  validRepoContextRequest,
  validRepoContextResult,
  type RepoContextRequest,
  type RepoContextResult,
} from "./repo-context";

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
  /** Stable local device identity. Null is retained only for legacy rows. */
  deviceId: string | null;
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

export type RawRetentionStatus = {
  inspection: "complete";
  policy: { retentionDays: number; cutoffAt: string };
  states: {
    retained: number;
    pendingDelivery: number;
    heldForUpload: number;
    quarantined: number;
    expired: number;
    notInspected: 0;
  };
  lastPass: {
    rowsVisited: number;
    rowsExpired: number;
    hasMore: boolean;
    at: string | null;
  };
};

export type OtlpAdmissionCounter = {
  source: string;
  reason: OtlpDropReason;
  droppedCount: number;
  firstDroppedAt: string;
  lastDroppedAt: string;
};

const MACHINE = os.hostname();
const REPO_CONTEXT_QUEUE_LIMIT = 128;
const REPO_CONTEXT_BATCH_LIMIT = 8;
const REPO_CONTEXT_FILL_LIMIT = 128;
const REPO_CONTEXT_COUNTER_LIMIT = 1_000_000_000;
const REPO_CONTEXT_CONFLICT_ROW_LIMIT = 128;
const REPO_CONTEXT_RESULT_LIMIT = 4_096;
const REPO_CONTEXT_RESULT_GC_LIMIT = 128;
const CANONICAL_PROJECT_KEY = /^sha256:[a-f0-9]{64}$/i;
type CostKind = "reported" | "estimated" | "unknown";

function sha256DigestHex(text: string) {
  return crypto.createHash("sha256").update(text).digest();
}

export type RepoContextDropReason =
  | "queue_overflow"
  | "boundary_unavailable"
  | "worker_crash"
  | "resolution_failed"
  | "result_conflict";

export type RepoContextApplyReceipt = {
  resultsInserted: number;
  resultReplays: number;
  resultConflicts: number;
  unknownResults: number;
  rowsFilled: number;
  rowsVisited: number;
};

type RepoContextHandoffBatch = {
  mode: "handoff" | "child_inflight";
  selected: RepoContextRequest[];
  selectedIds: Set<string>;
  cancelledIds: Set<string>;
  overflowIds: Set<string>;
  overflowCount: number;
  durableCount: number | null;
};

type ChildRepoContextRun = {
  selected: RepoContextRequest[];
  selectedIds: Set<string>;
  overflowIds: Set<string>;
};

function gitField(event: AiInteractionEvent, key: string): string | null {
  const git = (event.metadata as Record<string, unknown> | undefined)?.git;
  const value =
    git && typeof git === "object" ? (git as Record<string, unknown>)[key] : undefined;
  return typeof value === "string" && value ? value : null;
}

function canonicalProjectKey(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  return CANONICAL_PROJECT_KEY.test(normalized) ? normalized : null;
}

function admittedCostKind(event: AiInteractionEvent): CostKind | null {
  if (event.costUsd === undefined) return null;
  const candidate = (event as AiInteractionEvent & { costKind?: unknown }).costKind;
  return candidate === "reported" || candidate === "estimated" || candidate === "unknown"
    ? candidate
    : "unknown";
}

const EVENT_COLUMNS = [
  // Local-only receive clock for producer-ID hook replay. Unlike observed_at,
  // it remains the receipt time when the body supplied its own event time.
  "first_received_at text",
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
  "installation_epoch_id text",
  "project_key text",
  "cost_kind text check (cost_kind is null or cost_kind in ('reported','estimated','unknown'))",
  "device_id text",
  "privacy_generation text",
  "privacy_disposition text",
  "privacy_disposed_at text",
  "usage_duplicate_reason text",
  "usage_paired_event_id text",
] as const;

const enrollmentTimestampSchema = z.string().datetime({ offset: true });
const installationEpochIdSchema = z.string().uuid();

export class LocalEventBuffer {
  private claudeReplayState: "inactive"|"pending"|"ready"|"timed_out"="inactive";
  private claudeReplayRootSetJson="[]";
  private readonly deferredClaudeHookIds=new Set<string>();
  private readonly db: Database.Database;
  private readonly enrollmentNow: () => Date;
  private workspaceId: string | null = null;
  private deviceId: string | null = null;
  private installationEpochId: string | null = null;
  private readonly repoContextQueue: RepoContextRequest[] = [];
  private readonly queuedRepoContextIds = new Set<string>();
  private activeRepoContextCommitScope: RepoContextHandoffBatch | null = null;
  private historyImportEpoch: string | null = null;
  private childRepoContextRun: ChildRepoContextRun | null = null;
  private repoContextPostCommitAmbiguous = 0;
  readonly delivery: DeliveryOutbox;
  readonly projection: DashboardProjectionStore;
  readonly learningFacts: LearningFactStore;
  private insertEventStatement?: Database.Statement;
  private budgetAttemptedRows = 0;
  private retentionHoldCount: {
    count: number; cutoffAt: string; measuredAtMonotonic: number;
    revision: number; generation: number; retentionDays: number;
  } | null = null;
  private retentionHoldTask: { worker: Worker; result: Promise<number>; exited: Promise<void> } | null = null;
  private readonly retentionHoldWorkers = new Set<{ worker: Worker; exited: Promise<void> }>();
  private retentionHoldClose: Promise<void> | null = null;
  private retentionHoldCountDirty = false;
  private retentionHoldCountClosed = false;
  private retentionHoldGeneration = 0;
  private retentionHoldCountChanged: (() => void) | null = null;

  constructor(
    path: string,
    options: {
      delivery?: {
        enabled?: boolean;
        limits?: Partial<DeliveryLimits>;
        /** Injectable clock for delivery eligibility bookkeeping (issue 0182). */
        now?: () => Date;
      };
      workspaceId?: string;
      deviceId?: string;
      /** An epoch-unbound ledger may inherit the agreed epoch of configured
       * capture roots. null means roots disagree and binding must refuse. */
      freshCaptureRootEpoch?: string | null;
      /** Clock seam for deterministic enrollment fixtures; never replaces a persisted cutoff. */
      enrollmentNow?: () => Date;
      learningFacts?: { limits?: Partial<LearningFactLimits> };
      /** HTTP collectors fail fast under child-writer contention; maintenance
       * workers may use a short bounded wait. The better-sqlite3 default is
       * five seconds, which is never appropriate on the listener event loop. */
      databaseBusyTimeoutMs?: number;
      /** Daemon-only deadline for first-open migration and recovery. The
       * connection returns to databaseBusyTimeoutMs before construction ends. */
      startupBusyDeadlineMs?: number;
      /** Optional diagnostic sink used by the copied-ledger rehearsal tool. */
      onOpenStep?: LedgerOpenTimingSink;
    } = {},
  ) {
    const openStarted = performance.now();
    let stepStarted = openStarted;
    const markOpenStep = (step: string) => {
      if (!options.onOpenStep) return;
      const finished = performance.now();
      options.onOpenStep({
        step,
        durationMs: finished - stepStarted,
        elapsedMs: finished - openStarted,
      });
      stepStarted = finished;
    };
    this.enrollmentNow = options.enrollmentNow ?? (() => new Date());
    const timeout = Math.max(0, Math.min(options.databaseBusyTimeoutMs ?? 5_000, 5_000));
    const remainingOpenWait = () => options.startupBusyDeadlineMs === undefined
      ? timeout
      : Math.max(0, Math.min(10_000, Math.ceil(options.startupBusyDeadlineMs - performance.now())));
    if (options.freshCaptureRootEpoch !== undefined && options.freshCaptureRootEpoch !== null &&
        !installationEpochIdSchema.safeParse(options.freshCaptureRootEpoch).success) {
      throw new Error("installation_epoch_id_invalid");
    }
    // A mixed-root replacement must not leave an empty SQLite file or WAL
    // sidecar behind. An existing bound ledger decides from its durable epoch
    // inside useWorkspace's transaction, regardless of stale root config.
    if (options.freshCaptureRootEpoch === null && !fs.existsSync(path)) {
      throw new Error("fresh_ledger_capture_root_epochs_conflict");
    }
    try {
      this.db = options.startupBusyDeadlineMs === undefined
        ? openLedgerDatabase(path, { timeout: remainingOpenWait() })
        : openLedgerDatabaseForStartup(path, { timeout: remainingOpenWait() }, options.startupBusyDeadlineMs);
    }
    catch (error) {
      if ((error as { code?: string }).code === "LEDGER_PUBLICATION_INVALID") {
        // A known foreign handle cannot be made safe by retrying or restoring.
        if ((error as { cause?: { code?: string } }).cause?.code === "LEDGER_ARCHIVE_HANDLE_IN_USE") throw error;
        recoverInvalidLedgerPublication(path);
        throw new Error("replacement_verification_failed; archive restored; collector start refused", { cause: error });
      }
      throw error;
    }
    // An interrupted rename must not admit a first scan before the post-swap
    // file-generation fences are durable. Restore can still open it directly.
    try {
      const replacement = this.db.prepare(`select 1 from sqlite_master
        where type='table' and name='collector_replacement_ledger'`).get();
      if (replacement) {
        const columns = this.db.pragma("table_info(collector_replacement_ledger)") as
          Array<{ name: string }>;
        const pending = columns.some(column => column.name === "post_switch_fence_pending")
          ? this.db.prepare(`select post_switch_fence_pending as pending
            from collector_replacement_ledger where singleton=1`).get() as
              { pending: number } | undefined
          : undefined;
        if (pending?.pending !== 0) throw new Error("replacement_post_switch_fence_pending");
      }
    } catch (error) {
      this.db.close();
      throw error;
    }
    try {
      registerRetentionDeliveryId(this.db);
      // A second first opener can see SQLITE_BUSY while the first changes the
      // file into WAL mode. SQLite does not always apply busy_timeout to this
      // pragma, so retry only that transient result within the startup budget.
      const journalDeadline = options.startupBusyDeadlineMs ?? performance.now() + timeout;
      const busyWait = new Int32Array(new SharedArrayBuffer(4));
      for (;;) {
        try {
          // A DELETE-mode writer may escalate to EXCLUSIVE during this pragma.
          // Limit SQLite's native wait to the budget left for this attempt.
          this.db.pragma(`busy_timeout = ${Math.max(0, Math.min(10_000, Math.ceil(journalDeadline - performance.now())))}`);
          this.db.pragma("journal_mode = WAL");
          break;
        } catch (error) {
          if ((error as { code?: string }).code !== "SQLITE_BUSY" || performance.now() >= journalDeadline)
            throw error;
          Atomics.wait(busyWait, 0, 0, 10);
        }
      }
    } catch (error) {
      this.db.close();
      throw error;
    }
    this.deviceId = options.deviceId?.trim() || null;
    // All open-time schema reads and writes must share one writer lock. On a
    // first open, another process can otherwise read the same missing column
    // and lose its ALTER to this process with "duplicate column name".
    try {
    this.db.pragma(`busy_timeout = ${remainingOpenWait()}`);
    this.db.exec("BEGIN IMMEDIATE");
    const newLedger = !this.db
      .prepare(`select 1 from sqlite_master where type='table' and name='buffered_events'`)
      .get();
    markOpenStep("ledger.sqlite_open");
    this.db.exec(`
      create table if not exists buffered_events (
        id text primary key,
        source text not null,
        event_type text not null,
        data_mode text not null,
        observed_at text not null,
        payload_json text not null,
        suppressed_fields_json text not null default '[]',
        created_at text not null,
        first_received_at text
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
      -- Issue 0056 (#104): same event ID with a different payload digest is a
      -- collision, never a silent overwrite. Receipts carry bounded counters
      -- and content digests only — never raw rejected payload values.
      create table if not exists event_collision_quarantine (
        event_id text primary key,
        conflict_count integer not null check (
          conflict_count >= 1 and conflict_count <= ${REPO_CONTEXT_COUNTER_LIMIT}
        ),
        first_conflict_digest text not null,
        last_conflict_digest text not null,
        first_conflicted_at text not null,
        last_conflicted_at text not null
      );
      create table if not exists maintenance_state (
        key text primary key,
        value text not null,
        updated_at text not null
      );
      create table if not exists claude_replay_hooks (
        event_id text primary key,
        status text not null check(status in ('pending','timed_out')),
        created_at text not null,
        root_set_json text
      );
      create trigger if not exists trg_claude_replay_raw_delete
      after delete on buffered_events begin
        delete from claude_replay_hooks where event_id=old.id;
      end;
      create table if not exists raw_retention_receipts (
        event_id text not null,
        raw_rowid integer not null,
        raw_created_at text not null,
        raw_generation text,
        expired_at text not null,
        reason text not null check (reason = 'retention_window_elapsed')
      );
      create table if not exists raw_retention_control (
        singleton integer primary key check (singleton = 1),
        last_cutoff_at text,
        last_run_at text,
        last_rows_visited integer not null default 0,
        last_rows_expired integer not null default 0,
        last_has_more integer not null default 0,
        expired_total integer not null default 0
      );
      insert or ignore into raw_retention_control (singleton)
      values (1);
      create table if not exists maintenance_stage_cursors (
        stage text primary key check (stage in (
          'retention_deletion', 'wal_checkpoint', 'pending_event_link_fill',
          'enrichment', 'git_context'
        )),
        cursor text,
        rows_total integer not null default 0 check (rows_total >= 0),
        request_json text,
        updated_at text not null
      ) without rowid;
      create table if not exists maintenance_git_context_queue (
        context_id text primary key,
        source text not null check (source in ('codex', 'claude_code')),
        cwd text not null,
        queued_at text not null
      ) without rowid;
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
        changed_at text not null,
        current_installation_epoch_id text,
        current_installation_epoch_started_at text
      );
      create table if not exists session_usage_authority (
        source text not null,
        session_id text not null,
        authority text not null check (authority in ('tailer','live')),
        claimed_at text not null,
        primary key (source, session_id)
      );
      create table if not exists repo_context_identity_key (
        singleton integer primary key check (singleton = 1),
        hmac_key blob not null check (length(hmac_key) = 32),
        created_at text not null
      );
      create table if not exists repo_context_results (
        context_id text primary key,
        repo_hash text not null,
        branch_hash text,
        head_sha text,
        resolved_at text not null,
        resolver_version text not null,
        accepted_at text not null
      );
      create table if not exists repo_context_conflicts (
        context_id text primary key,
        conflict_count integer not null check (
          conflict_count >= 1 and conflict_count <= ${REPO_CONTEXT_COUNTER_LIMIT}
        ),
        first_conflict_digest text not null,
        last_conflict_digest text not null,
        first_conflicted_at text not null,
        last_conflicted_at text not null
      );
      create table if not exists repo_context_suppressions (
        context_id text primary key check (
          length(context_id) = 75 and
          substr(context_id, 1, 11) = 'repoctx:v1:' and
          substr(context_id, 12) not glob '*[^0-9a-f]*'
        ),
        reason text not null check (reason = 'transcript_context_conflict'),
        suppressed_at text not null,
        cleanup_complete integer not null default 0 check (cleanup_complete in (0, 1))
      ) without rowid;
      create table if not exists repo_context_unknown_counters (
        reason text primary key check (reason in (
          'queue_overflow', 'boundary_unavailable', 'worker_crash',
          'resolution_failed', 'result_conflict'
        )),
        dropped_count integer not null check (dropped_count >= 0),
        updated_at text not null
      );
      create table if not exists repo_context_inflight (
        context_id text primary key,
        started_at text not null,
        owner text not null default 'parent' check (owner in ('parent', 'child'))
      );
      create table if not exists repo_context_handoffs (
        context_id text primary key check (
          length(context_id) = 75 and
          substr(context_id, 1, 11) = 'repoctx:v1:' and
          substr(context_id, 12) not glob '*[^0-9a-f]*'
        )
      ) without rowid;
      -- Deliberately new-only: never backfill this table from buffered_events.
      -- Its indexes are therefore created empty on first upgrade instead of
      -- scanning the historical ledger merely to discover legacy NULLs.
      create table if not exists repo_context_event_links (
        event_id text primary key,
        context_id text not null check (
          length(context_id) = 75 and
          substr(context_id, 1, 11) = 'repoctx:v1:' and
          substr(context_id, 12) not glob '*[^0-9a-f]*'
        ),
        fill_pending integer not null default 1 check (fill_pending in (0, 1)),
        context_conflict integer not null default 0 check (context_conflict in (0, 1)),
        suppression_cleaned integer not null default 0 check (
          suppression_cleaned in (0, 1)
        )
      ) without rowid;
    `);
    markOpenStep("ledger.core_schema");
    // Released collectors insert expiry receipts with INSERT OR IGNORE. Keep
    // that statement valid after rollback while allowing a later incarnation
    // of the same caller-controlled event ID to record its own expiry.
    // The fast path avoids taking a writer lock on every subsequent open.
    // Recheck under the writer lock so concurrent upgraders cannot both rebuild.
    const oldReceiptKey = () => (this.db.pragma("table_info(raw_retention_receipts)") as
      Array<{ name: string; pk: number }>).some((column) =>
        column.name === "event_id" && column.pk !== 0);
    const hasIncarnationIndex = () => Boolean(this.db.prepare(`select 1 from sqlite_master
      where type='index' and name='idx_raw_retention_incarnation'`).get());
    if (oldReceiptKey() || !hasIncarnationIndex()) this.db.transaction(() => {
      if (oldReceiptKey()) {
        const dependentTriggers = this.db.prepare(`select name,sql from sqlite_master
          where type='trigger' and sql like '%raw_retention_receipts%'`).all() as
          Array<{ name: string; sql: string }>;
        for (const trigger of dependentTriggers) {
          this.db.exec(`drop trigger "${trigger.name.replaceAll('"', '""')}"`);
        }
        this.db.exec(`
          create table raw_retention_receipts_by_incarnation (
            event_id text not null,
            raw_rowid integer not null,
            raw_created_at text not null,
            raw_generation text,
            expired_at text not null,
            reason text not null check (reason = 'retention_window_elapsed')
          );
          insert into raw_retention_receipts_by_incarnation
            (event_id,raw_rowid,raw_created_at,raw_generation,expired_at,reason)
          select event_id,raw_rowid,raw_created_at,raw_generation,expired_at,reason
          from raw_retention_receipts;
          drop table raw_retention_receipts;
          alter table raw_retention_receipts_by_incarnation rename to raw_retention_receipts;
        `);
        for (const trigger of dependentTriggers) this.db.exec(trigger.sql);
      }
      // The two expression terms distinguish NULL from an empty generation.
      this.db.exec(`create unique index if not exists idx_raw_retention_incarnation
        on raw_retention_receipts
          (event_id,raw_rowid,raw_created_at,(raw_generation is null),coalesce(raw_generation,''))`);
    }).immediate();
    markOpenStep("ledger.retention_receipt_schema");
    const replayHookColumns = new Set(
      (this.db.pragma("table_info(claude_replay_hooks)") as Array<{ name: string }>)
        .map((column) => column.name),
    );
    if (!replayHookColumns.has("root_set_json"))
      this.db.exec("alter table claude_replay_hooks add column root_set_json text");
    const retentionControlColumns = new Set(
      (this.db.pragma("table_info(raw_retention_control)") as Array<{ name: string }>)
        .map((column) => column.name),
    );
    if (!retentionControlColumns.has("expired_total")) {
      this.db.exec(
        `alter table raw_retention_control add column expired_total integer not null default 0`,
      );
    }
    const insertMaintenanceStage = this.db.prepare(
      `insert into maintenance_stage_cursors (stage, updated_at)
       values (?, ?) on conflict(stage) do nothing`,
    );
    const maintenanceStageCreatedAt = new Date().toISOString();
    this.db.transaction(() => {
      for (const stage of [
        "retention_deletion", "wal_checkpoint", "pending_event_link_fill",
        "enrichment", "git_context",
      ]) insertMaintenanceStage.run(stage, maintenanceStageCreatedAt);
    })();
    const repoContextInflightColumns = new Set(
      (this.db.pragma("table_info(repo_context_inflight)") as Array<{ name: string }>)
        .map((column) => column.name),
    );
    if (!repoContextInflightColumns.has("owner")) {
      this.db.exec(
        `alter table repo_context_inflight add column owner text not null default 'parent'`,
      );
    }
    const repoContextSuppressionColumns = new Set(
      (this.db.pragma("table_info(repo_context_suppressions)") as Array<{ name: string }>)
        .map((column) => column.name),
    );
    if (!repoContextSuppressionColumns.has("cleanup_complete")) {
      this.db.exec(
        `alter table repo_context_suppressions
         add column cleanup_complete integer not null default 0`,
      );
    }
    const discardedRepoContextResults = this.migrateRepoContextResultSchema();
    if (discardedRepoContextResults > 0) {
      this.recordRepoContextDrop("resolution_failed", discardedRepoContextResults);
    }
    this.migrateEventColumns();
    this.migrateWorkspaceBindingColumns();
    ensureFinanceProvenanceSchema(this.db);
    ensureCodexLiveUsageSchema(this.db);
    ensureRepoContextLinkDispositionSchema(this.db);
    const bindingColumns = new Set(
      (this.db.pragma("table_info(collector_workspace_binding)") as Array<{ name: string }>)
        .map((column) => column.name),
    );
    if (!bindingColumns.has("current_device_id")) {
      this.db.exec(`alter table collector_workspace_binding add column current_device_id text`);
    }
    if (!bindingColumns.has("previous_device_id")) {
      this.db.exec(`alter table collector_workspace_binding add column previous_device_id text`);
    }
    // Establish the ledger-local winner before the parent begins serving or
    // can spawn a second connection. First admission therefore only reads it.
    this.repoContextHmacKey();
    markOpenStep("ledger.column_migrations");
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
      create trigger if not exists trg_events_installation_epoch_immutable
      before update of installation_epoch_id on buffered_events
      when new.installation_epoch_id is not old.installation_epoch_id
      begin
        select raise(abort, 'installation_epoch_id_is_immutable');
      end;
      create trigger if not exists trg_events_privacy_disposition_terminal
      before update of privacy_disposition on buffered_events
      when old.privacy_disposition is not null
        and new.privacy_disposition is not old.privacy_disposition
      begin
        select raise(abort, 'privacy_disposition_is_terminal');
      end;
    `);
    markOpenStep("ledger.privacy_schema");
    this.delivery = new DeliveryOutbox(this.db, {
      ...(options.delivery ?? {}),
      deviceId: options.deviceId,
      onHoldChange: () => this.invalidateRetentionHoldCount(),
    });
    markOpenStep("ledger.delivery_schema");
    // The revision is durable so every writer, including legacy repair and
    // direct ledger mutations, fences an in-flight read-only count worker.
    // No historical rows are scanned or rewritten when these triggers land.
    this.db.exec(`
      create table if not exists retention_hold_revision (
        singleton integer primary key check (singleton = 1),
        revision integer not null default 0
      );
      insert or ignore into retention_hold_revision (singleton,revision) values (1,0);
      create trigger if not exists trg_retention_hold_raw_insert
      after insert on buffered_events begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      create trigger if not exists trg_retention_hold_raw_delete
      after delete on buffered_events begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      drop trigger if exists trg_retention_hold_raw_update;
      create trigger trg_retention_hold_raw_update
      after update of id,created_at,data_mode,uploaded_at,privacy_disposition,
        privacy_generation,usage_duplicate_reason,workspace_id,device_id on buffered_events begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      create trigger if not exists trg_retention_hold_outbox_insert
      after insert on upload_outbox begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      create trigger if not exists trg_retention_hold_outbox_delete
      after delete on upload_outbox begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      create trigger if not exists trg_retention_hold_outbox_lineage
      after update of delivery_id,raw_rowid,raw_id on upload_outbox begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      create trigger if not exists trg_retention_hold_receipt_insert
      after insert on upload_receipts begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      drop trigger if exists trg_retention_hold_receipt_update;
      create trigger trg_retention_hold_receipt_update
      after update of delivery_id,terminal_state,reason,raw_rowid,raw_id,
        raw_created_at,raw_generation on upload_receipts begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      create trigger if not exists trg_retention_hold_receipt_delete
      after delete on upload_receipts begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      create trigger if not exists trg_retention_hold_binding_insert
      after insert on collector_workspace_binding begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      create trigger if not exists trg_retention_hold_binding_update
      after update of current_workspace_id,current_device_id on collector_workspace_binding begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
      create trigger if not exists trg_retention_hold_binding_delete
      after delete on collector_workspace_binding begin
        update retention_hold_revision set revision=revision+1 where singleton=1;
      end;
    `);
    // A 0.7.40 ledger can still hold an unexpired upload lease. Remove its
    // insert fence before the daemon accepts the first intake request.
    // Other ledgers keep the lazy session-summary schema initialization.
    const oldLeaseInsertTrigger = this.db.prepare(
      "select 1 from sqlite_master where type='trigger' and name='trg_session_sync_upload_lease_insert'",
    ).get();
    if (oldLeaseInsertTrigger) {
      ensureSessionSummarySchema(this.db);
      markOpenStep("ledger.session_summary_schema");
    }
    if (options.workspaceId) this.useWorkspace(options.workspaceId, this.deviceId,
      undefined, options.freshCaptureRootEpoch);
    markOpenStep("ledger.workspace_binding");
    this.db.exec(`
      create index if not exists idx_events_upload on buffered_events (uploaded_at, created_at);
      create index if not exists idx_events_workspace_upload
        on buffered_events (workspace_id, uploaded_at, created_at);
      create index if not exists idx_events_session on buffered_events (session_id, observed_at);
      create index if not exists idx_events_observed on buffered_events (observed_at);
      create index if not exists idx_events_retention on buffered_events (created_at, id);
      create index if not exists idx_events_repo on buffered_events (repo_hash, branch_hash);
      create index if not exists idx_raw_retention_expired
        on raw_retention_receipts (expired_at, event_id);
      create index if not exists idx_repo_context_event_links_pending_context
        on repo_context_event_links (context_id, event_id)
        where fill_pending = 1;
      create index if not exists idx_repo_context_event_links_cleanup
        on repo_context_event_links (context_id, event_id)
        where suppression_cleaned = 0;
      create index if not exists idx_repo_context_suppression_cleanup
        on repo_context_suppressions (cleanup_complete, suppressed_at, context_id);
      create index if not exists idx_repo_context_results_gc
        on repo_context_results (accepted_at, context_id);
      create index if not exists idx_events_account on buffered_events (account_hash, observed_at);
      create index if not exists idx_metrics_name on metric_samples (metric_name, observed_at);
      create index if not exists idx_metrics_session on metric_samples (session_id);
      create index if not exists idx_metrics_observed on metric_samples (created_at);
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

      create trigger if not exists trg_repo_context_event_link_identity_immutable
      before update of event_id, context_id on repo_context_event_links
      when new.event_id is not old.event_id or new.context_id is not old.context_id
      begin
        select raise(abort, 'repo_context_event_link_identity_is_immutable');
      end;

      create trigger if not exists trg_repo_context_event_link_state_monotonic
      before update of fill_pending, context_conflict, suppression_cleaned
      on repo_context_event_links
      when new.fill_pending > old.fill_pending
        or new.context_conflict < old.context_conflict
        or new.suppression_cleaned < old.suppression_cleaned
        or (new.context_conflict = 1 and new.fill_pending <> 0)
        or (new.suppression_cleaned = 1 and new.fill_pending <> 0)
      begin
        select raise(abort, 'repo_context_event_link_state_is_monotonic');
      end;

      create trigger if not exists trg_events_repo_context_link_delete
      after delete on buffered_events
      begin
        delete from repo_context_event_links where event_id = old.id;
      end;

      create trigger if not exists trg_repo_context_result_immutable_update
      before update on repo_context_results
      begin
        select raise(abort, 'repo_context_result_is_immutable');
      end;

      drop trigger if exists trg_repo_context_result_immutable_delete;
      create trigger if not exists trg_repo_context_result_delete_guard
      before delete on repo_context_results
      when exists (
        select 1 from repo_context_event_links l
        where l.context_id = old.context_id
          and l.fill_pending = 1
          and not exists (
            select 1 from repo_context_suppressions s where s.context_id = old.context_id
          )
      ) or exists (
        select 1 from repo_context_handoffs h where h.context_id = old.context_id
      ) or exists (
        select 1 from repo_context_inflight i where i.context_id = old.context_id
      )
      begin
        select raise(abort, 'repo_context_result_still_referenced');
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

    `);
    // Replace the installed rowid-only trigger atomically on upgrade. Repo
    // enrichment may fill an unsealed copy only for this exact raw incarnation.
    const linkageTriggers = this.db.prepare(`select name from sqlite_master
      where type='trigger' and name in ('trg_events_outbox_linkage_update',
        'trg_events_outbox_linkage_update_v2', 'trg_events_outbox_linkage_update_v3')`)
      .all() as Array<{ name: string }>;
    if (linkageTriggers.length !== 1 ||
        linkageTriggers[0].name !== "trg_events_outbox_linkage_update_v3") {
      this.db.transaction(() => this.db.exec(`
      drop trigger if exists trg_events_outbox_linkage_update;
      drop trigger if exists trg_events_outbox_linkage_update_v2;
      create trigger if not exists trg_events_outbox_linkage_update_v3
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
        where raw_rowid = new.rowid and raw_id = new.id
          and raw_created_at = new.created_at
          and raw_generation is new.privacy_generation
          and sealed_envelope_json is null and attempt_count = 0;
      end;
      `)).immediate();
    }
    markOpenStep("ledger.raw_indexes_and_triggers");
    // Empty on first upgrade; maintenance backfills older rows in bounded batches.
    ensureSessionContextIndexSchema(this.db);
    markOpenStep("ledger.session_context_index");
    ensureCodexReconciliationSchema(this.db);
    markOpenStep("ledger.codex_reconciliation_schema");
    ensureCodexUsagePairingSchema(this.db);
    // An empty ledger has no history to scan; existing ledgers get these
    // indexes only in the explicit stopped-service upgrade step.
    if (newLedger) buildCodexUsagePairingIndexes(this.db);
    this.learningFacts = new LearningFactStore(
      this.db,
      options.learningFacts?.limits,
    );
    ensureWeeklyToolStatsSchema(this.db);
    markOpenStep("ledger.learning_schema");
    this.projection = new DashboardProjectionStore(this.db, {
      newLedger,
      onOpenStep: options.onOpenStep
        ? (step) => options.onOpenStep!({
            ...step,
            elapsedMs: performance.now() - openStarted,
          })
        : undefined,
    });
    markOpenStep("ledger.projection_schema");
    this.db.exec("COMMIT");
    } catch (error) {
      try {
        if (this.db.inTransaction) this.db.exec("ROLLBACK");
      } finally {
        this.db.close();
      }
      throw error;
    }
    try {
      // Small upgrades finish at open; large ledgers advance in bounded upload
      // and retention turns, independently of the completed raw cursor.
      if (options.startupBusyDeadlineMs !== undefined)
        this.db.pragma(`busy_timeout = ${remainingOpenWait()}`);
      this.delivery.backfillLegacyReceiptLineage({ maxRows: 16, maxWriterMs: 50 });
      markOpenStep("ledger.receipt_lineage_slice");
      if (options.startupBusyDeadlineMs !== undefined)
        this.db.pragma(`busy_timeout = ${timeout}`);
    } catch (error) {
      this.db.close();
      throw error;
    }
  }

  /**
   * Select the ledger's upload audience (issue 0089). Enrollment is
   * future-only: no selection ever relabels pre-existing rows. Unassigned
   * rows stay in local quarantine until an explicitly confirmed history
   * operation moves them. A mismatch fails closed: only join's explicit
   * transactional activation may change an initialized ledger audience.
   *
   * Single legacy exception (issue #163 rework): binding the unmanaged
   * default LOCAL tenant adopts workspace-unassigned rows left by
   * pre-workspace builds (every install upgraded via migrateEventColumns).
   * Same local owner, no enrollment involved — legacy upload behavior
   * returns. Binding ANY managed/joined workspace must never adopt
   * unassigned rows: that relabel is the exact leak class #163 quarantines.
   */
  useWorkspace(workspaceId: string, deviceId = this.deviceId, installationEpochId?: string,
    freshCaptureRootEpoch?: string | null) {
    const requested = workspaceId.trim();
    if (!requested) throw new Error("Workspace binding requires a non-empty workspace id.");
    let requestedDevice = deviceId?.trim() || null;
    let selectedEpochId: string | null = null;
    const run = this.db.transaction(() => {
      const binding = this.db
        .prepare(
          `select current_workspace_id as currentWorkspaceId,
             previous_workspace_id as previousWorkspaceId,
             current_device_id as currentDeviceId,
             current_installation_epoch_id as currentInstallationEpochId
           from collector_workspace_binding where singleton = 1`,
        )
        .get() as
        | { currentWorkspaceId: string; previousWorkspaceId: string | null; currentDeviceId: string | null;
            currentInstallationEpochId: string | null }
        | undefined;
      if (binding && binding.currentWorkspaceId !== requested) {
        throw new Error(
          `Ledger workspace binding mismatch: active=${binding.currentWorkspaceId}, requested=${requested}. ` +
            "Use the transactional join/reassign flow; refusing to relabel queued rows.",
        );
      }
      if (binding?.currentDeviceId && requestedDevice && binding.currentDeviceId !== requestedDevice) {
        throw new Error(
          `Ledger device binding mismatch: active=${binding.currentDeviceId}, requested=${requestedDevice}. ` +
          "Use the transactional device rotation/reinstall flow; refusing to relabel queued rows.",
        );
      }
      // A reopened ledger may omit the device option. Once its binding has a
      // device, inherit that binding rather than creating new unbound rows.
      if (!requestedDevice && binding?.currentDeviceId) {
        requestedDevice = binding.currentDeviceId;
      }
      // Use the root epoch only until an epoch binding is durable. The check
      // and the binding write share this transaction, so a crash before commit
      // leaves the next open eligible, and a competing binder cannot be
      // overwritten by a stale pre-transaction new-file observation.
      if (!binding?.currentInstallationEpochId && freshCaptureRootEpoch === null) {
        throw new Error("fresh_ledger_capture_root_epochs_conflict");
      }
      if (!binding) {
        this.db
          .prepare(
            `insert into collector_workspace_binding
              (singleton, current_workspace_id, previous_workspace_id, current_device_id, previous_device_id, changed_at)
             values (1, @workspaceId, null, @deviceId, null, @now)`,
          )
          .run({ workspaceId: requested, deviceId: requestedDevice, now: new Date().toISOString() });
      } else if (!binding.currentDeviceId && requestedDevice) {
        this.db
          .prepare(`update collector_workspace_binding set current_device_id = ? where singleton = 1`)
          .run(requestedDevice);
      }
      if (requested === LOCAL_TENANT_ID) {
        // LOCAL-only adoption of pre-workspace rows. A managed/joined
        // selection deliberately leaves `workspace_id is null` rows untouched:
        // they remain quarantined from every managed audience.
        this.db
          .prepare(`update buffered_events set workspace_id = ? where workspace_id is null`)
          .run(LOCAL_TENANT_ID);
        this.db
          .prepare(`update upload_outbox set workspace_id = ? where workspace_id is null`)
          .run(LOCAL_TENANT_ID);
        if (requestedDevice) {
          this.db
            .prepare(
              `update buffered_events set device_id = ?
               where workspace_id = ? and device_id is null`,
            )
            .run(requestedDevice, LOCAL_TENANT_ID);
          this.db
            .prepare(
              `update upload_outbox set device_id = ?
               where workspace_id = ? and device_id is null`,
            )
            .run(requestedDevice, LOCAL_TENANT_ID);
        }
      }
      selectedEpochId = this.ensureCurrentInstallationEpoch(requested,
        installationEpochId ?? (!binding?.currentInstallationEpochId
          ? freshCaptureRootEpoch ?? undefined : undefined));
      // Deliberately no backfill for managed workspaces here or in any later
      // selection: once a managed workspace is selected, unassigned history is
      // permanently ineligible for that audience (lease and list filters are
      // workspace-scoped), which is the quarantine contract.
    });
    run.immediate();
    this.workspaceId = requested;
    this.deviceId = requestedDevice;
    this.installationEpochId = selectedEpochId;
    this.delivery.setWorkspace(requested);
    if (requestedDevice) this.delivery.setDevice(requestedDevice);
    return requested;
  }

  /**
   * Transactional reassignment boundary inside the ledger (issue 0089).
   * Future-only: existing rows — including legacy-unassigned ones — are never
   * relabeled. Prior bound rows stay bound to `fromWorkspaceId`, unassigned
   * rows stay unbound (local quarantine); only subsequent appends are labeled
   * for `toWorkspaceId`.
   */
  transitionWorkspace(fromWorkspaceId: string, toWorkspaceId: string, deviceId = this.deviceId, installationEpochId?: string) {
    const from = fromWorkspaceId.trim();
    const to = toWorkspaceId.trim();
    if (!from || !to) throw new Error("Workspace transition requires non-empty ids.");
    let requestedDevice = deviceId?.trim() || null;
    let selectedEpochId: string | null = null;
    if (from === to) {
      this.useWorkspace(to, requestedDevice, installationEpochId);
      return { fromWorkspaceId: from, toWorkspaceId: to, boundLegacyRows: 0 };
    }
    const run = this.db.transaction(() => {
      const binding = this.db
        .prepare(
          `select current_workspace_id as currentWorkspaceId,
             current_device_id as currentDeviceId
           from collector_workspace_binding where singleton = 1`,
        )
        .get() as { currentWorkspaceId: string; currentDeviceId: string | null } | undefined;
      if (!binding) {
        this.db
          .prepare(
            `insert into collector_workspace_binding
              (singleton, current_workspace_id, previous_workspace_id, current_device_id, previous_device_id, changed_at)
             values (1, @from, null, @deviceId, null, @now)`,
          )
          .run({ from, deviceId: requestedDevice, now: new Date().toISOString() });
      } else if (binding.currentWorkspaceId !== from) {
        throw new Error(
          `Cannot transition ledger from ${from}: it is bound to ${binding.currentWorkspaceId}.`,
        );
      } else if (binding.currentDeviceId && requestedDevice && binding.currentDeviceId !== requestedDevice) {
        throw new Error(
          `Cannot transition ledger device from ${binding.currentDeviceId}: requested ${requestedDevice}.`,
        );
      } else if (binding.currentDeviceId && !requestedDevice) {
        requestedDevice = binding.currentDeviceId;
      }
      this.db
        .prepare(
          `update collector_workspace_binding set
             previous_workspace_id = current_workspace_id,
             previous_device_id = current_device_id,
             current_workspace_id = @to,
             current_device_id = @deviceId,
             changed_at = @now
           where singleton = 1`,
        )
        .run({ to, deviceId: requestedDevice, now: new Date().toISOString() });
      selectedEpochId = this.ensureCurrentInstallationEpoch(to, installationEpochId ?? crypto.randomUUID());
      // Auth/contract circuits describe the prior workspace endpoint and must
      // not block the newly authenticated audience after reassignment.
      this.delivery.clearCircuit();
    });
    run();
    this.workspaceId = to;
    this.deviceId = requestedDevice;
    this.installationEpochId = selectedEpochId;
    this.delivery.setWorkspace(to);
    if (requestedDevice) this.delivery.setDevice(requestedDevice);
    return { fromWorkspaceId: from, toWorkspaceId: to, boundLegacyRows: 0 };
  }

  private ensureCurrentInstallationEpoch(workspaceId: string, installationEpochId?: string) {
    if (installationEpochId !== undefined && !installationEpochIdSchema.safeParse(installationEpochId).success) {
      throw new Error("installation_epoch_id_invalid");
    }
    const row = this.db.prepare(`select current_installation_epoch_id as id,
      current_installation_epoch_started_at as startedAt from collector_workspace_binding where singleton=1`).get() as
      { id: string | null; startedAt: string | null };
    if (!row.id || (installationEpochId !== undefined && row.id !== installationEpochId)) {
      const startedAt = this.enrollmentNow().toISOString();
      if (row.startedAt && Date.parse(startedAt) < Date.parse(row.startedAt)) {
        throw new Error("installation_epoch_clock_regressed");
      }
      row.id = installationEpochId ?? crypto.randomUUID(); row.startedAt = startedAt;
      this.db.prepare(`update collector_workspace_binding set current_installation_epoch_id=?,
        current_installation_epoch_started_at=? where singleton=1`).run(row.id,row.startedAt);
      markFinancePublicationDirty(this.db);
      if (workspaceId !== LOCAL_TENANT_ID) advanceCaptureBaselineEnrollment(this.db, row.startedAt);
    }
    if (!enrollmentTimestampSchema.safeParse(row.startedAt).success) {
      throw new Error("installation_epoch_cutoff_invalid");
    }
    initializeFinanceSourceCoverage(this.db,workspaceId,row.id,row.startedAt!);
    return row.id;
  }

  /** Managed admission uses the durable enrollment binding, never file mtime,
   * observation arrival time, an account label, or a proposed profile epoch. */
  eventAdmissionReason(observedAt: unknown, claimedInstallationEpochId?: unknown, trustedInstallationEpochId?: unknown):
    "stale_connection" | "enrollment_binding_invalid" | "invalid_timestamp" | "before_enrollment" | "epoch_mismatch" | null {
    const binding = this.workspaceBinding();
    if (binding && (binding.currentWorkspaceId !== this.workspaceId ||
        binding.currentDeviceId !== this.deviceId || binding.currentInstallationEpochId !== this.installationEpochId))
      return "stale_connection";
    if (!binding || binding.currentWorkspaceId === LOCAL_TENANT_ID) return null;
    if (!binding.currentInstallationEpochId || !binding.currentInstallationEpochStartedAt ||
        !enrollmentTimestampSchema.safeParse(binding.currentInstallationEpochStartedAt).success)
      return "enrollment_binding_invalid";
    if (!enrollmentTimestampSchema.safeParse(observedAt).success) return "invalid_timestamp";
    // Authenticated live bindings and account-assertion capture roots carry an
    // unforgeable in-process sidecar. That capability permits a source epoch
    // distinct from the workspace-enrollment epoch, including a delayed event
    // from a closed pre-failover interval.
    if (trustedInstallationEpochId !== undefined) {
      if (!installationEpochIdSchema.safeParse(trustedInstallationEpochId).success ||
          claimedInstallationEpochId !== trustedInstallationEpochId) return "epoch_mismatch";
      return null;
    }
    if (Date.parse(observedAt as string) < Date.parse(binding.currentInstallationEpochStartedAt) &&
        !(this.historyImportEpoch && claimedInstallationEpochId === this.historyImportEpoch &&
          this.historyImportEpoch === binding.currentInstallationEpochId)) return "before_enrollment";
    if (claimedInstallationEpochId !== undefined && claimedInstallationEpochId !== binding.currentInstallationEpochId)
      return "epoch_mismatch";
    return null;
  }

  /** Scoped exception for a verified, fenced historical file import. It never
   * changes the persisted epoch or the capture frontier. Only the dedicated
   * importer invokes this around one bounded SQLite transaction. */
  withHistoryImportAdmission<T>(installationEpochId: string, action: () => T): T {
    if (this.historyImportEpoch || this.db.inTransaction ||
        this.workspaceBinding()?.currentInstallationEpochId !== installationEpochId) {
      throw new Error("history_import_epoch_not_current");
    }
    this.historyImportEpoch = installationEpochId;
    try { return action(); }
    finally { this.historyImportEpoch = null; }
  }

  workspaceBinding() {
    const row = this.db
      .prepare(
        `select current_workspace_id as currentWorkspaceId,
           previous_workspace_id as previousWorkspaceId,
           current_device_id as currentDeviceId,
           previous_device_id as previousDeviceId,
           changed_at as changedAt,
           current_installation_epoch_id as currentInstallationEpochId,
           current_installation_epoch_started_at as currentInstallationEpochStartedAt
         from collector_workspace_binding where singleton = 1`,
      )
      .get() as
      | {
          currentWorkspaceId: string;
          previousWorkspaceId: string | null;
          currentDeviceId: string | null;
          previousDeviceId: string | null;
          currentInstallationEpochId: string | null;
          currentInstallationEpochStartedAt: string | null;
          changedAt: string;
        }
      | undefined;
    return row ?? null;
  }

  get currentDeviceId() {
    return this.deviceId;
  }

  /**
   * Payload-free enrollment receipt (issue 0089, #163 rework). Counts only —
   * never event, envelope, or payload bytes. Quarantined rows are the rows
   * WITHHELD FROM THE CURRENT WORKSPACE: rows bound to a different workspace
   * (production pre-join capture binds LOCAL via openBuffer) plus unassigned
   * legacy rows. Before any binding exists, only unassigned rows count.
   * These rows are invisible to every workspace-scoped lease/list filter.
   */
  enrollmentStatus() {
    const binding = this.workspaceBinding();
    const currentWorkspaceId = binding?.currentWorkspaceId ?? null;
    const currentDeviceId = binding?.currentDeviceId ?? null;
    const withheldPredicate = currentWorkspaceId === null
      ? `workspace_id is null`
      : currentDeviceId === null
        ? `(workspace_id is null or workspace_id <> ?)`
        : `(workspace_id is null or workspace_id <> ? or device_id is null or device_id <> ?)`;
    const countParams = currentWorkspaceId === null
      ? []
      : currentDeviceId === null
        ? [currentWorkspaceId]
        : [currentWorkspaceId, currentDeviceId];
    const quarantinedEventRows = (
      this.db
        .prepare(
          `select count(*) as n from buffered_events where ${withheldPredicate}`,
        )
        .get(...countParams) as { n: number }
    ).n;
    const quarantinedOutboxRows = (
      this.db
        .prepare(
          `select count(*) as n from upload_outbox where ${withheldPredicate}`,
        )
        .get(...countParams) as { n: number }
    ).n;
    return {
      futureOnlyEnrollment: true as const,
      currentWorkspaceId,
      currentDeviceId,
      currentInstallationEpochId: binding?.currentInstallationEpochId ?? null,
      currentInstallationEpochStartedAt: binding?.currentInstallationEpochStartedAt ?? null,
      quarantinedHistoryRows: quarantinedEventRows + quarantinedOutboxRows,
      quarantinedEventRows,
      quarantinedOutboxRows,
    };
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

  private migrateWorkspaceBindingColumns() {
    const existing = new Set(
      (this.db.pragma("table_info(collector_workspace_binding)") as Array<{ name: string }>).map(
        (column) => column.name,
      ),
    );
    for (const definition of [
      "current_installation_epoch_id text",
      "current_installation_epoch_started_at text",
    ]) {
      const name = definition.split(" ")[0]!;
      if (!existing.has(name)) this.db.exec(`alter table collector_workspace_binding add column ${definition}`);
    }
  }

  private migrateRepoContextResultSchema() {
    const columns = this.db.pragma("table_info(repo_context_results)") as Array<{
      name: string;
      notnull: number;
    }>;
    const hasResolverVersion = columns.some((column) => column.name === "resolver_version");
    const hasAcceptedAt = columns.some((column) => column.name === "accepted_at");
    const repoHashRequired = columns.some(
      (column) => column.name === "repo_hash" && column.notnull === 1,
    );
    if (hasResolverVersion && hasAcceptedAt && repoHashRequired) return 0;

    const discarded = (this.db
      .prepare(`select count(*) as count from repo_context_results where repo_hash is null`)
      .get() as { count: number }).count;

    const resolverExpression = hasResolverVersion
      ? "coalesce(resolver_version, 'git-context:v1')"
      : "'git-context:v1'";
    const acceptedExpression = hasAcceptedAt ? "accepted_at" : "resolved_at";
    this.db.transaction(() => {
      this.db.exec(`
        drop trigger if exists trg_repo_context_result_immutable_update;
        drop trigger if exists trg_repo_context_result_immutable_delete;
        alter table repo_context_results rename to repo_context_results_legacy;
        create table repo_context_results (
          context_id text primary key,
          repo_hash text not null,
          branch_hash text,
          head_sha text,
          resolved_at text not null,
          resolver_version text not null,
          accepted_at text not null
        );
        insert into repo_context_results
          (context_id, repo_hash, branch_hash, head_sha, resolved_at, resolver_version, accepted_at)
        select context_id, repo_hash, branch_hash, head_sha, resolved_at,
          ${resolverExpression}, ${acceptedExpression}
        from repo_context_results_legacy
        where repo_hash is not null;
        drop table repo_context_results_legacy;
      `);
    })();
    return discarded;
  }

  /**
   * The context key is ledger-local and never leaves SQLite. Two concurrent
   * openers may propose candidates, but every caller re-reads the durable
   * winner before deriving an id.
   */
  private repoContextHmacKey() {
    const readWinner = () => this.db
      .prepare(`select hmac_key as hmacKey from repo_context_identity_key where singleton = 1`)
      .get() as { hmacKey: Buffer } | undefined;
    let winner = readWinner();
    if (!winner) {
      this.db
        .prepare(
          `insert into repo_context_identity_key (singleton, hmac_key, created_at)
           values (1, ?, ?)
           on conflict(singleton) do nothing`,
        )
        .run(crypto.randomBytes(32), new Date().toISOString());
      winner = readWinner();
    }
    if (!winner || !Buffer.isBuffer(winner.hmacKey) || winner.hmacKey.length !== 32) {
      throw new Error("repo_context_identity_key_invalid");
    }
    return winner.hmacKey;
  }

  private deriveRepoContextRequest(
    source: AiInteractionEvent["source"],
    occurrence: string,
    cwd: string,
  ): RepoContextRequest | null {
    const canonicalCwd = canonicalRepoContextCwd(cwd);
    if (!validRepoContextOccurrence(occurrence) || !canonicalCwd) return null;
    const digest = crypto
      .createHmac("sha256", this.repoContextHmacKey())
      .update(`repoctx:v1:capture-policy:${REPO_CONTEXT_CAPTURE_POLICY_GENERATION}\0`, "utf8")
      .update(source, "utf8")
      .update("\0", "utf8")
      .update(occurrence, "utf8")
      .update("\0", "utf8")
      .update(canonicalCwd, "utf8")
      .digest("hex");
    return { contextId: `repoctx:v1:${digest}`, source, cwd: canonicalCwd };
  }

  private repoContextRequestFor(event: AiInteractionEvent): RepoContextRequest | null {
    const sidecar = peekRepoContextSidecar(event);
    return sidecar
      ? this.deriveRepoContextRequest(event.source, sidecar.occurrence, sidecar.cwd)
      : null;
  }

  repoContextOccurrenceRequest(
    source: AiInteractionEvent["source"],
    occurrence: string,
    cwd: string,
  ) {
    return this.deriveRepoContextRequest(source, occurrence, cwd);
  }

  /** A fresh path-free terminal binding; it owns no resolver work or result. */
  repoContextUnknownId(source: AiInteractionEvent["source"], occurrence: string) {
    if (!validRepoContextOccurrence(occurrence)) throw new Error("repo_context_occurrence_invalid");
    return `repoctx:v1:${crypto.createHmac("sha256", this.repoContextHmacKey())
      .update(`repoctx:terminal-unknown:v${REPO_CONTEXT_CAPTURE_POLICY_GENERATION}\0`).update(source).update("\0")
      .update(occurrence).digest("hex")}`;
  }

  stageRepoContextRequest(request: RepoContextRequest) {
    if (!this.activeRepoContextCommitScope) {
      throw new Error("repo_context_commit_scope_required");
    }
    if (!validRepoContextRequest(request)) throw new Error("repo_context_request_invalid");
    const resolved = this.resolvedRepoContext(request.contextId);
    if (resolved.suppressed) return null;
    if (resolved.exists) return this.repoContextHasCurrentCapturePolicy(request.contextId)
      ? request.contextId : null;
    return this.reserveRepoContextHandoff(request, this.activeRepoContextCommitScope)
      ? request.contextId
      : null;
  }

  /** Terminally suppress a transcript occurrence after a different-cwd
   * replay. This transaction writes only the path-free tombstone and cancels
   * exact queue ownership. Bounded maintenance slices perform de-attribution;
   * token truth and sealed/attempted outbound bytes remain immutable. */
  suppressRepoContextId(contextId: string) {
    const scope = this.activeRepoContextCommitScope;
    if (!scope) throw new Error("repo_context_commit_scope_required");
    if (!validRepoContextId(contextId)) return false;
    const now = new Date().toISOString();
    const inserted = this.db.prepare(
      `insert into repo_context_suppressions
         (context_id, reason, suppressed_at, cleanup_complete)
       values (?, 'transcript_context_conflict', ?, 0)
       on conflict(context_id) do nothing`,
    ).run(contextId, now).changes;
    if (inserted > 0) markFinancePublicationDirty(this.db, now);
    const changes = scope.mode === "child_inflight"
      ? this.db.prepare(
          `delete from repo_context_inflight where context_id = ? and owner = 'child'`,
        ).run(contextId).changes
      : this.db.prepare(`delete from repo_context_handoffs where context_id = ?`).run(contextId).changes;
    const selectedHere = scope.selectedIds.has(contextId);
    const selectedEarlier = scope.mode === "child_inflight"
      ? this.childRepoContextRun?.selectedIds.has(contextId) === true
      : this.queuedRepoContextIds.has(contextId);
    if (changes === 0 && inserted === 0 && !selectedHere && !selectedEarlier) {
      return false;
    }
    scope.cancelledIds.add(contextId);
    return true;
  }

  /**
   * A maintenance child has one bounded, run-scoped ownership lane. Requests
   * admitted while this lane is active enter inflight in the same transaction
   * as their event and cursor; they never become indistinguishable from live
   * parent handoffs after a child crash.
   */
  beginChildRepoContextRun() {
    if (this.childRepoContextRun || this.activeRepoContextCommitScope) {
      throw new Error("repo_context_child_run_already_active");
    }
    if (this.repoContextQueue.length > 0 || this.queuedRepoContextIds.size > 0) {
      throw new Error("repo_context_child_run_not_sole_writer");
    }
    this.childRepoContextRun = {
      selected: [],
      selectedIds: new Set<string>(),
      overflowIds: new Set<string>(),
    };
  }

  finishChildRepoContextRun() {
    if (!this.childRepoContextRun || this.activeRepoContextCommitScope) {
      throw new Error("repo_context_child_run_not_active");
    }
    const selected = [...this.childRepoContextRun.selected];
    this.childRepoContextRun = null;
    return selected;
  }

  abandonChildRepoContextRun() {
    if (this.activeRepoContextCommitScope) {
      throw new Error("repo_context_commit_scope_active");
    }
    const selected = this.childRepoContextRun?.selected.length ?? 0;
    this.childRepoContextRun = null;
    return selected;
  }

  transactionWithRepoContextHandoffs<T>(work: () => T): T {
    if (this.activeRepoContextCommitScope) {
      throw new Error("repo_context_commit_scope_nested");
    }
    const handoffs = this.newRepoContextHandoffBatch(
      this.childRepoContextRun ? "child_inflight" : "handoff",
    );
    this.activeRepoContextCommitScope = handoffs;
    try {
      const result = this.db.transaction(() => {
        const value = work();
        if (handoffs.overflowCount > 0) {
          this.recordRepoContextDrop("queue_overflow", handoffs.overflowCount);
        }
        return value;
      }).immediate();
      this.activeRepoContextCommitScope = null;
      this.finalizeRepoContextHandoffs(handoffs);
      return result;
    } catch (error) {
      this.activeRepoContextCommitScope = null;
      throw error;
    }
  }

  /** Backward-compatible local provenance, never a parser checkpoint key.
   * Released readers ignore the raw metadata receipt and tolerate the result
   * version. Exact event IDs keep the raw receipt lookup a bounded PK read. */
  repoContextHasCurrentCapturePolicy(contextId: string, capturedEventIds: readonly string[] = []) {
    if (!validRepoContextId(contextId)) return false;
    const row = this.db.prepare(
      `select resolver_version as resolverVersion from repo_context_results where context_id = ?`,
    ).get(contextId) as { resolverVersion: string } | undefined;
    if (row) return row.resolverVersion === REPO_CONTEXT_RESOLVER_VERSION;
    if (this.activeRepoContextCommitScope?.selectedIds.has(contextId)) return true;
    if (capturedEventIds.length > 2) throw new Error("repo_context_provenance_lookup_unbounded");
    const receipt = this.db.prepare(
      `select 1 from repo_context_event_links l
       join buffered_events e on e.id = l.event_id
       where l.event_id = ? and l.context_id = ?
         and case when json_valid(e.payload_json)
           then json_extract(e.payload_json, '$.metadata.repoContextPolicyGeneration') end = ?
       limit 1`,
    );
    return capturedEventIds.some(eventId => Boolean(receipt.get(eventId, contextId, REPO_CONTEXT_CAPTURE_POLICY_GENERATION)));
  }

  private resolvedRepoContext(contextId: string | null, currentCaptureOnly = false) {
    if (!contextId) {
      return {
        exists: false,
        suppressed: false,
        repoHash: null as string | null,
        branchHash: null as string | null,
        headSha: null as string | null,
      };
    }
    const row = this.db
      .prepare(
        `select r.repo_hash as repoHash, r.branch_hash as branchHash, r.head_sha as headSha,
           r.resolver_version as resolverVersion,
           exists(select 1 from repo_context_suppressions s where s.context_id = ?) as suppressed
         from repo_context_results r where r.context_id = ?`,
      )
      .get(contextId, contextId) as {
        repoHash: string;
        branchHash: string | null;
        headSha: string | null;
        suppressed: number;
        resolverVersion: string;
      } | undefined;
    if (row) {
      const suppressed = row.suppressed === 1;
      const excluded = suppressed ||
        (currentCaptureOnly && row.resolverVersion !== REPO_CONTEXT_RESOLVER_VERSION);
      return {
        exists: true,
        suppressed,
        repoHash: excluded ? null : row.repoHash,
        branchHash: excluded ? null : row.branchHash,
        headSha: excluded ? null : row.headSha,
      };
    }
    const suppressed = Boolean(this.db
      .prepare(`select 1 from repo_context_suppressions where context_id = ?`)
      .get(contextId));
    return {
      exists: false,
      suppressed,
      repoHash: null,
      branchHash: null,
      headSha: null,
    };
  }

  canBindRepoContextId(contextId: string) {
    if (!validRepoContextId(contextId)) return false;
    const resolved = this.resolvedRepoContext(contextId);
    if (resolved.suppressed) return false;
    if (resolved.exists) return true;
    return Boolean(
      this.db.prepare(`select 1 from repo_context_inflight where context_id = ?`).get(contextId) ||
      this.db.prepare(`select 1 from repo_context_handoffs where context_id = ?`).get(contextId),
    );
  }

  private newRepoContextHandoffBatch(
    mode: "handoff" | "child_inflight" = "handoff",
  ): RepoContextHandoffBatch {
    return {
      mode,
      selected: [],
      selectedIds: new Set<string>(),
      cancelledIds: new Set<string>(),
      overflowIds: new Set<string>(),
      overflowCount: 0,
      durableCount: null,
    };
  }

  private reserveRepoContextHandoff(
    request: RepoContextRequest | null,
    batch: RepoContextHandoffBatch,
  ) {
    if (!request) return false;
    if (batch.selectedIds.has(request.contextId)) return true;
    if (batch.overflowIds.has(request.contextId)) return false;
    const resolved = this.resolvedRepoContext(request.contextId);
    if (resolved.suppressed) return false;
    if (resolved.exists) return true;

    if (batch.mode === "child_inflight") {
      const run = this.childRepoContextRun;
      if (!run) throw new Error("repo_context_child_run_not_active");
      if (run.selectedIds.has(request.contextId)) return true;
      if (run.overflowIds.has(request.contextId)) return false;
      const existingInflight = this.db
        .prepare(`select owner from repo_context_inflight where context_id = ?`)
        .get(request.contextId) as { owner: "parent" | "child" } | undefined;
      const existingHandoff = this.db
        .prepare(`select 1 from repo_context_handoffs where context_id = ?`)
        .get(request.contextId);
      if (existingInflight) return true;
      // A handoff proves only a path-free durable id. The child does not own
      // the parent's raw cwd and therefore cannot promise this event will be
      // resolved before that parent handoff is lost or deferred.
      if (existingHandoff) return false;
      if (run.selected.length + batch.selected.length >= REPO_CONTEXT_BATCH_LIMIT) {
        batch.overflowIds.add(request.contextId);
        batch.overflowCount += 1;
        return false;
      }
      const inserted = this.db
        .prepare(
          `insert into repo_context_inflight (context_id, started_at, owner)
           values (?, ?, 'child') on conflict(context_id) do nothing`,
        )
        .run(request.contextId, new Date().toISOString()).changes;
      if (inserted === 0) return Boolean(this.db
        .prepare(`select 1 from repo_context_inflight where context_id = ?`)
        .get(request.contextId));
      batch.selected.push(request);
      batch.selectedIds.add(request.contextId);
      return true;
    }

    if (this.queuedRepoContextIds.has(request.contextId)) return true;
    batch.durableCount ??= (this.db
      .prepare(`select count(*) as count from repo_context_handoffs`)
      .get() as { count: number }).count;
    const alreadyDurable = this.db
      .prepare(`select 1 from repo_context_handoffs where context_id = ?`)
      .get(request.contextId);
    if (alreadyDurable) return true;
    if (
      batch.durableCount >= REPO_CONTEXT_QUEUE_LIMIT ||
      this.repoContextQueue.length + batch.selected.length >= REPO_CONTEXT_QUEUE_LIMIT
    ) {
      batch.overflowCount += 1;
      batch.overflowIds.add(request.contextId);
      return false;
    }
    const inserted = this.db
      .prepare(
        `insert into repo_context_handoffs (context_id) values (?)
         on conflict(context_id) do nothing`,
      )
      .run(request.contextId).changes;
    if (inserted === 0) return Boolean(this.db
      .prepare(`select 1 from repo_context_handoffs where context_id = ?`)
      .get(request.contextId));
    batch.durableCount += 1;
    batch.selected.push(request);
    batch.selectedIds.add(request.contextId);
    return true;
  }

  private finalizeRepoContextHandoffs(batch: RepoContextHandoffBatch) {
    try {
      if (batch.mode === "child_inflight") {
        const run = this.childRepoContextRun;
        if (!run) throw new Error("repo_context_child_run_not_active");
        if (batch.cancelledIds.size > 0) {
          run.selected = run.selected.filter(
            (request) => !batch.cancelledIds.has(request.contextId),
          );
          for (const contextId of batch.cancelledIds) run.selectedIds.delete(contextId);
        }
        for (const request of batch.selected) {
          if (batch.cancelledIds.has(request.contextId)) continue;
          run.selected.push(request);
          run.selectedIds.add(request.contextId);
        }
        for (const contextId of batch.overflowIds) run.overflowIds.add(contextId);
        return;
      }
      if (batch.cancelledIds.size > 0) {
        for (let index = this.repoContextQueue.length - 1; index >= 0; index -= 1) {
          if (batch.cancelledIds.has(this.repoContextQueue[index]!.contextId)) {
            this.repoContextQueue.splice(index, 1);
          }
        }
        for (const contextId of batch.cancelledIds) this.queuedRepoContextIds.delete(contextId);
      }
      for (const request of batch.selected) {
        if (batch.cancelledIds.has(request.contextId)) continue;
        this.repoContextQueue.push(request);
        this.queuedRepoContextIds.add(request.contextId);
      }
    } catch {
      this.repoContextPostCommitAmbiguous = Math.min(
        REPO_CONTEXT_COUNTER_LIMIT,
        this.repoContextPostCommitAmbiguous + batch.selected.length,
      );
      // The event and context-id-only handoff are already durable. Returning
      // the committed append truth prevents an unsafe caller retry.
    }
  }

  takeRepoContextBatch(limit = REPO_CONTEXT_BATCH_LIMIT) {
    const bounded = Math.max(0, Math.min(limit, REPO_CONTEXT_BATCH_LIMIT));
    // This is deliberately a peek. beginRepoContextResolution removes only
    // requests whose durable handoff transition committed successfully, so a
    // SQLITE_BUSY leaves both the in-memory request and durable handoff
    // available to the next cadence.
    return this.repoContextQueue.slice(0, bounded);
  }

  repoContextQueueStatus() {
    return {
      queued: this.repoContextQueue.length,
      limit: REPO_CONTEXT_QUEUE_LIMIT,
      batchLimit: REPO_CONTEXT_BATCH_LIMIT,
      postCommitAmbiguous: this.repoContextPostCommitAmbiguous,
    };
  }

  recordRepoContextDrop(reason: RepoContextDropReason, count = 1) {
    const bounded = Math.max(0, Math.min(Math.trunc(count), REPO_CONTEXT_COUNTER_LIMIT));
    if (bounded === 0) return 0;
    this.db
      .prepare(
        `insert into repo_context_unknown_counters
           (reason, dropped_count, updated_at)
         values (@reason, @count, @now)
         on conflict(reason) do update set
           dropped_count = min(@limit, dropped_count + excluded.dropped_count),
           updated_at = excluded.updated_at`,
      )
      .run({ reason, count: bounded, now: new Date().toISOString(), limit: REPO_CONTEXT_COUNTER_LIMIT });
    return bounded;
  }

  repoContextUnknownCounters() {
    return this.db
      .prepare(
        `select reason, dropped_count as droppedCount, updated_at as updatedAt
         from repo_context_unknown_counters order by reason`,
      )
      .all() as Array<{ reason: RepoContextDropReason; droppedCount: number; updatedAt: string }>;
  }

  beginRepoContextResolution(requests: RepoContextRequest[]) {
    const insert = this.db.prepare(
      `insert into repo_context_inflight (context_id, started_at, owner)
       select ?, ?, 'parent' where not exists (
         select 1 from repo_context_results where context_id = ?
       ) on conflict(context_id) do nothing`,
    );
    const hasResult = this.db.prepare(
      `select 1 from repo_context_results where context_id = ?`,
    );
    const isSuppressed = this.db.prepare(
      `select 1 from repo_context_suppressions where context_id = ?`,
    );
    const clearHandoff = this.db.prepare(
      `delete from repo_context_handoffs where context_id = ?`,
    );
    const hasInflight = this.db.prepare(
      `select 1 from repo_context_inflight where context_id = ?`,
    );
    const now = new Date().toISOString();
    const transition = this.db.transaction(() => {
      const accepted: RepoContextRequest[] = [];
      const completedIds = new Set<string>();
      const seen = new Set<string>();
      for (const request of requests.slice(0, REPO_CONTEXT_BATCH_LIMIT)) {
        if (!validRepoContextRequest(request) || seen.has(request.contextId)) continue;
        seen.add(request.contextId);
        if (isSuppressed.get(request.contextId)) {
          clearHandoff.run(request.contextId);
          completedIds.add(request.contextId);
          continue;
        }
        if (hasResult.get(request.contextId)) {
          clearHandoff.run(request.contextId);
          completedIds.add(request.contextId);
          continue;
        }
        if (insert.run(request.contextId, now, request.contextId).changes > 0) {
          clearHandoff.run(request.contextId);
          accepted.push(request);
          completedIds.add(request.contextId);
          continue;
        }
        if (hasInflight.get(request.contextId)) {
          clearHandoff.run(request.contextId);
          completedIds.add(request.contextId);
        }
      }
      return { accepted, completedIds };
    })();
    if (transition.completedIds.size > 0) {
      for (let index = this.repoContextQueue.length - 1; index >= 0; index -= 1) {
        const queued = this.repoContextQueue[index]!;
        if (transition.completedIds.has(queued.contextId)) this.repoContextQueue.splice(index, 1);
      }
      for (const contextId of transition.completedIds) {
        this.queuedRepoContextIds.delete(contextId);
      }
    }
    return transition.accepted;
  }

  failRepoContextResolution(
    requests: RepoContextRequest[],
    reason: "boundary_unavailable" | "worker_crash",
  ) {
    const unique = new Set(
      requests
        .slice(0, REPO_CONTEXT_BATCH_LIMIT)
        .filter(validRepoContextRequest)
        .map((request) => request.contextId),
    );
    const failed = this.db.transaction(() => {
      const clear = this.db.prepare(`delete from repo_context_inflight where context_id = ?`);
      const clearHandoff = this.db.prepare(`delete from repo_context_handoffs where context_id = ?`);
      const hasResult = this.db.prepare(`select 1 from repo_context_results where context_id = ?`);
      let unresolved = 0;
      for (const contextId of unique) {
        const cleared = clear.run(contextId).changes + clearHandoff.run(contextId).changes;
        if (cleared > 0 && !hasResult.get(contextId)) unresolved += 1;
      }
      this.recordRepoContextDrop(reason, unresolved);
      return unresolved;
    })();
    for (const request of requests) this.queuedRepoContextIds.delete(request.contextId);
    return failed;
  }

  /** One parent-side failure gate: fail the exact parent batch, then recover
   * only residual child-owned inflight work. Durable handoffs are never read
   * or changed here. */
  failRepoContextRun(
    requests: RepoContextRequest[],
    reason: "boundary_unavailable" | "worker_crash",
  ) {
    const parentIds = [...new Set(
      requests
        .slice(0, REPO_CONTEXT_BATCH_LIMIT)
        .filter(validRepoContextRequest)
        .map((request) => request.contextId),
    )];
    return this.db.transaction(() => {
      const hasResult = this.db.prepare(`select 1 from repo_context_results where context_id = ?`);
      const clearParent = this.db.prepare(`delete from repo_context_inflight where context_id = ?`);
      let parentUnknown = 0;
      for (const contextId of parentIds) {
        const cleared = clearParent.run(contextId).changes;
        if (cleared > 0 && !hasResult.get(contextId)) parentUnknown += 1;
      }
      const childUnknown = (this.db.prepare(
        `select count(*) as count from repo_context_inflight i
         where owner = 'child' and not exists (
           select 1 from repo_context_results r where r.context_id = i.context_id
         )`,
      ).get() as { count: number }).count;
      this.db.prepare(`delete from repo_context_inflight where owner = 'child'`).run();
      this.recordRepoContextDrop(reason, parentUnknown);
      this.recordRepoContextDrop("worker_crash", childUnknown);
      return { parentUnknown, childUnknown };
    })();
  }

  repoContextInflightCount() {
    return (this.db
      .prepare(`select count(*) as count from repo_context_inflight`)
      .get() as { count: number }).count;
  }

  recoverRepoContextState() {
    // Recovery reads a count before deleting rows. Begin as a writer so a
    // concurrent hook cannot turn that read snapshot into SQLITE_BUSY when
    // the deferred transaction tries to upgrade at the first delete.
    const recovered = this.db.transaction(() => {
      const unresolved = this.db
        .prepare(
          `select count(*) as count from (
             select context_id from repo_context_handoffs
             union
             select context_id from repo_context_inflight
           ) stale
           where not exists (
             select 1 from repo_context_results r where r.context_id = stale.context_id
           )`,
        )
        .get() as { count: number };
      if (unresolved.count > 0) this.recordRepoContextDrop("worker_crash", unresolved.count);
      this.db.prepare(`delete from repo_context_handoffs`).run();
      this.db.prepare(`delete from repo_context_inflight`).run();
      return unresolved.count;
    }).immediate();
    this.repoContextQueue.splice(0);
    this.queuedRepoContextIds.clear();
    return recovered;
  }

  recoverRepoContextInflight() {
    const recovered = this.db.transaction(() => {
      const unresolved = (this.db
        .prepare(
          `select count(*) as count from repo_context_inflight i
           where not exists (
             select 1 from repo_context_results r where r.context_id = i.context_id
           )`,
        )
        .get() as { count: number }).count;
      if (unresolved > 0) this.recordRepoContextDrop("worker_crash", unresolved);
      this.db.prepare(`delete from repo_context_inflight`).run();
      return unresolved;
    })();
    return recovered;
  }

  dropQueuedRepoContextRemainder(reason: "resolution_failed" = "resolution_failed") {
    const contextIds = [...new Set(this.repoContextQueue.map((request) => request.contextId))];
    if (contextIds.length === 0) return 0;
    const dropped = this.db.transaction(() => {
      const clear = this.db.prepare(`delete from repo_context_handoffs where context_id = ?`);
      let cleared = 0;
      for (const contextId of contextIds) cleared += clear.run(contextId).changes;
      this.recordRepoContextDrop(reason, cleared);
      return cleared;
    })();
    const clearedIds = new Set(contextIds);
    for (let index = this.repoContextQueue.length - 1; index >= 0; index -= 1) {
      if (clearedIds.has(this.repoContextQueue[index]!.contextId)) {
        this.repoContextQueue.splice(index, 1);
      }
    }
    for (const contextId of clearedIds) this.queuedRepoContextIds.delete(contextId);
    return dropped;
  }

  private repoContextResultAtCapacity() {
    return Boolean(this.db
      .prepare(
        `select 1 from repo_context_results
         order by accepted_at, context_id
         limit 1 offset ?`,
      )
      .get(REPO_CONTEXT_RESULT_LIMIT - 1));
  }

  private gcRepoContextResultsInCurrentTransaction(limit: number, requiredFree = 0) {
    const bounded = Math.max(0, Math.min(Math.trunc(limit), REPO_CONTEXT_RESULT_GC_LIMIT));
    if (bounded === 0) return { visited: 0, deleted: 0, blocked: 0 };
    const windowLimit = REPO_CONTEXT_RESULT_LIMIT + bounded;
    const window = this.db
      .prepare(
        `select context_id as contextId from repo_context_results
         order by accepted_at, context_id limit ?`,
      )
      .all(windowLimit) as Array<{ contextId: string }>;
    const excess = Math.min(
      bounded,
      Math.max(Math.min(requiredFree, bounded), window.length - REPO_CONTEXT_RESULT_LIMIT, 0),
    );
    if (excess === 0) return { visited: 0, deleted: 0, blocked: 0 };
    const candidates = this.db
      .prepare(
        `with oldest as (
           select context_id, accepted_at from repo_context_results
           order by accepted_at, context_id limit @windowLimit
         )
         select r.context_id as contextId
         from oldest o join repo_context_results r on r.context_id = o.context_id
         where not exists (
           select 1 from repo_context_event_links l
           where l.context_id = r.context_id
             and l.fill_pending = 1
             and not exists (
               select 1 from repo_context_suppressions s where s.context_id = r.context_id
             )
         ) and not exists (
           select 1 from repo_context_handoffs h where h.context_id = r.context_id
         ) and not exists (
           select 1 from repo_context_inflight i where i.context_id = r.context_id
         )
         order by o.accepted_at, o.context_id limit @excess`,
      )
      .all({ windowLimit, excess }) as Array<{ contextId: string }>;
    const remove = this.db.prepare(
      `delete from repo_context_results
       where context_id = @contextId
         and not exists (
           select 1 from repo_context_event_links l
           where l.context_id = @contextId
             and l.fill_pending = 1
             and not exists (
               select 1 from repo_context_suppressions s where s.context_id = @contextId
             )
         )
         and not exists (
           select 1 from repo_context_handoffs h where h.context_id = @contextId
         )
         and not exists (
           select 1 from repo_context_inflight i where i.context_id = @contextId
         )`,
    );
    const removeConflict = this.db.prepare(
      `delete from repo_context_conflicts where context_id = ?`,
    );
    let deleted = 0;
    for (const candidate of candidates) {
      const changes = remove.run(candidate).changes;
      if (changes > 0) {
        removeConflict.run(candidate.contextId);
        deleted += changes;
      }
    }
    return {
      visited: candidates.length,
      deleted,
      blocked: Math.max(0, excess - deleted),
    };
  }

  runRepoContextResultGc(limit = REPO_CONTEXT_RESULT_GC_LIMIT) {
    return this.db.transaction(() => this.gcRepoContextResultsInCurrentTransaction(limit))();
  }

  repoContextResultRetentionStatus() {
    const rows = this.db
      .prepare(
        `select context_id from repo_context_results
         order by accepted_at, context_id limit ?`,
      )
      .all(REPO_CONTEXT_RESULT_LIMIT + 1) as Array<{ context_id: string }>;
    return {
      count: rows.length,
      capped: rows.length > REPO_CONTEXT_RESULT_LIMIT,
      limit: REPO_CONTEXT_RESULT_LIMIT,
      gcLimit: REPO_CONTEXT_RESULT_GC_LIMIT,
    };
  }

  private drainRepoContextSuppressionsInCurrentTransaction(limit: number) {
    const bounded = Math.max(0, Math.min(Math.trunc(limit), REPO_CONTEXT_FILL_LIMIT));
    if (bounded === 0) return { contextsVisited: 0, rowsVisited: 0, rowsCleared: 0 };
    const nextContext = this.db.prepare(
      `select context_id as contextId from repo_context_suppressions
       where cleanup_complete = 0
       order by suppressed_at, context_id limit 1`,
    );
    const selectRows = this.db.prepare(
      `select l.event_id as eventId, e.rowid,
         e.created_at as createdAt,e.privacy_generation as generation
       from repo_context_event_links l indexed by idx_repo_context_event_links_cleanup
       left join buffered_events e on e.id = l.event_id
       where l.context_id = ? and l.suppression_cleaned = 0
       order by l.event_id limit ?`,
    );
    const clearOutbox = this.db.prepare(
      `update upload_outbox set repo_hash = null, branch_hash = null, updated_at = ?
       where raw_rowid = ? and raw_id = ? and raw_created_at = ?
         and raw_generation is ?
         and sealed_envelope_json is null and attempt_count = 0`,
    );
    const clearRow = this.db.prepare(
      `update buffered_events set repo_hash = null, branch_hash = null, head_sha = null
       where id = ? and (
         repo_hash is not null or branch_hash is not null or head_sha is not null
       )`,
    );
    const markCleaned = this.db.prepare(
      `update repo_context_event_links
       set fill_pending = 0, suppression_cleaned = 1
       where event_id = ? and context_id = ? and suppression_cleaned = 0`,
    );
    const hasRemaining = this.db.prepare(
      `select 1
       from repo_context_event_links indexed by idx_repo_context_event_links_cleanup
       where context_id = ? and suppression_cleaned = 0 limit 1`,
    );
    const complete = this.db.prepare(
      `update repo_context_suppressions set cleanup_complete = 1
       where context_id = ? and cleanup_complete = 0`,
    );
    const now = new Date().toISOString();
    let contextsVisited = 0;
    let rowsVisited = 0;
    let rowsCleared = 0;
    while (rowsVisited < bounded && contextsVisited < REPO_CONTEXT_FILL_LIMIT) {
      const context = nextContext.get() as { contextId: string } | undefined;
      if (!context) break;
      contextsVisited += 1;
      const rows = selectRows.all(context.contextId, bounded - rowsVisited) as Array<{
        eventId: string;
        rowid: number | null;
        createdAt: string | null;
        generation: string | null;
      }>;
      for (const row of rows) {
        rowsVisited += 1;
        if (row.rowid !== null) clearOutbox.run(now, row.rowid,
          row.eventId, row.createdAt, row.generation);
        rowsCleared += clearRow.run(row.eventId).changes;
        markCleaned.run(row.eventId, context.contextId);
      }
      if (!hasRemaining.get(context.contextId)) complete.run(context.contextId);
      if (rows.length === 0 && hasRemaining.get(context.contextId)) break;
    }
    return { contextsVisited, rowsVisited, rowsCleared };
  }

  /** One fixed, indexed de-attribution slice for terminal transcript
   * conflicts. A tombstone remains after cleanup so future rows stay UNKNOWN. */
  drainRepoContextSuppressions(limit = REPO_CONTEXT_FILL_LIMIT) {
    return this.db.transaction(() =>
      this.drainRepoContextSuppressionsInCurrentTransaction(limit)
    ).immediate();
  }

  private fillRepoContextRowsInCurrentTransaction(
    limit: number,
    contextIds?: readonly string[],
  ) {
    const bounded = Math.max(0, Math.min(Math.trunc(limit), REPO_CONTEXT_FILL_LIMIT));
    if (bounded === 0) return { rowsVisited: 0, rowsFilled: 0 };
    // Maintenance is result-driven, never pending-link-driven. Queue overflow,
    // worker crash, and failed resolution may leave exact new-row receipts,
    // but no cadence scans them merely to discover that no result exists.
    const scopedIds = contextIds
      ? [...new Set(contextIds)].slice(0, REPO_CONTEXT_BATCH_LIMIT)
      : (this.db.prepare(
          `select r.context_id as contextId
           from repo_context_results r
           where not exists (
             select 1 from repo_context_suppressions s where s.context_id = r.context_id
           ) and exists (
             select 1
             from repo_context_event_links l
               indexed by idx_repo_context_event_links_pending_context
             where l.context_id = r.context_id and l.fill_pending = 1
           )
           order by r.accepted_at, r.context_id
           limit ?`,
        ).all(REPO_CONTEXT_BATCH_LIMIT) as Array<{ contextId: string }>).map(
          (row) => row.contextId,
        );
    if (scopedIds.length === 0) return { rowsVisited: 0, rowsFilled: 0 };
    const contextFilter = `and l.context_id in (${scopedIds.map(() => "?").join(",")})`;
    const pending = this.db.prepare(
      `select e.rowid, l.event_id as eventId, l.context_id as contextId,
         e.repo_hash as existingRepoHash, r.repo_hash as repoHash,
         r.branch_hash as branchHash, r.head_sha as headSha
       from repo_context_event_links l indexed by idx_repo_context_event_links_pending_context
       join buffered_events e on e.id = l.event_id
       join repo_context_results r on r.context_id = l.context_id
       where l.fill_pending = 1
         and l.context_conflict = 0
         ${contextFilter}
         and not exists (
           select 1 from repo_context_suppressions s where s.context_id = l.context_id
         )
       order by l.event_id
       limit ?`,
    ).all(...(scopedIds ?? []), bounded) as Array<{
      rowid: number;
      eventId: string;
      contextId: string;
      existingRepoHash: string | null;
      repoHash: string;
      branchHash: string | null;
      headSha: string | null;
    }>;
    const fill = this.db.prepare(
      `update buffered_events set
         repo_hash = coalesce(repo_hash, @repoHash),
         branch_hash = coalesce(branch_hash, @branchHash),
         head_sha = coalesce(head_sha, @headSha)
       where rowid = @rowid and id = @eventId
         and not exists (
           select 1 from repo_context_suppressions s where s.context_id = @contextId
         )
         and (
           repo_hash is null or
           (@branchHash is not null and branch_hash is null) or
           (@headSha is not null and head_sha is null)
         )`,
    );
    const markConflict = this.db.prepare(
      `update repo_context_event_links
       set context_conflict = 1, fill_pending = 0
       where event_id = ? and context_id = ?
         and context_conflict = 0 and fill_pending = 1`,
    );
    const completeLink = this.db.prepare(
      `update repo_context_event_links set fill_pending = 0
       where event_id = ? and context_id = ? and fill_pending = 1`,
    );
    let rowsFilled = 0;
    for (const row of pending) {
      if (row.existingRepoHash && row.existingRepoHash !== row.repoHash) {
        if (markConflict.run(row.eventId, row.contextId).changes > 0) {
          this.recordRepoContextRowConflict(
            row.contextId,
            row.existingRepoHash,
            row.repoHash,
          );
          this.recordRepoContextDrop("result_conflict");
        }
        continue;
      }
      rowsFilled += fill.run(row).changes;
      completeLink.run(row.eventId, row.contextId);
    }
    return { rowsVisited: pending.length, rowsFilled };
  }

  /** One fixed maintenance slice over the new-only sidecar work index. Legacy
   * event history is never searched for context work. */
  drainRepoContextFills(limit = REPO_CONTEXT_FILL_LIMIT) {
    return this.db.transaction(() => this.fillRepoContextRowsInCurrentTransaction(limit)).immediate();
  }

  applyRepoContextResults(
    results: RepoContextResult[],
    fillLimit = REPO_CONTEXT_FILL_LIMIT,
  ): RepoContextApplyReceipt {
    const receipt: RepoContextApplyReceipt = {
      resultsInserted: 0,
      resultReplays: 0,
      resultConflicts: 0,
      unknownResults: 0,
      rowsFilled: 0,
      rowsVisited: 0,
    };
    const boundedFill = Math.max(0, Math.min(fillLimit, REPO_CONTEXT_FILL_LIMIT));
    return this.db.transaction(() => {
      if (results.length > REPO_CONTEXT_BATCH_LIMIT) {
        throw new Error("repo_context_result_batch_too_large");
      }
      const existingResult = this.db.prepare(
        `select repo_hash as repoHash, branch_hash as branchHash, head_sha as headSha,
           resolver_version as resolverVersion
         from repo_context_results where context_id = ?`,
      );
      const hasInflight = this.db.prepare(
        `select 1 from repo_context_inflight where context_id = ?`,
      );
      const insertResult = this.db.prepare(
        `insert into repo_context_results
           (context_id, repo_hash, branch_hash, head_sha, resolved_at, resolver_version, accepted_at)
         values (
           @contextId, @repoHash, @branchHash, @headSha, @resolvedAt, @resolverVersion, @acceptedAt
         )
         on conflict(context_id) do nothing`,
      );
      const clearInflight = this.db.prepare(
        `delete from repo_context_inflight where context_id = ?`,
      );
      const clearHandoff = this.db.prepare(
        `delete from repo_context_handoffs where context_id = ?`,
      );
      type PreparedResult = {
        result: RepoContextResult;
        existing: {
          repoHash: string | null;
          branchHash: string | null;
          headSha: string | null;
          resolverVersion: string;
        } | undefined;
        same: boolean;
        inflight: boolean;
        suppressed: boolean;
      };
      const prepared: PreparedResult[] = [];
      const seen = new Set<string>();
      // Preflight the complete batch before counters, conflicts, GC, markers,
      // results, or event rows can change. Only an identical retained replay
      // may arrive without the exact inflight authority row.
      for (const result of results) {
        if (!validRepoContextResult(result)) {
          throw new Error("repo_context_result_invalid");
        }
        if (seen.has(result.contextId)) {
          throw new Error("repo_context_result_duplicate");
        }
        seen.add(result.contextId);
        const existing = existingResult.get(result.contextId) as PreparedResult["existing"];
        const same = Boolean(existing) &&
          existing!.repoHash === result.repoHash &&
          existing!.branchHash === result.branchHash &&
          existing!.headSha === result.headSha &&
          existing!.resolverVersion === result.resolverVersion;
        const inflight = Boolean(hasInflight.get(result.contextId));
        const suppressed = Boolean(this.db
          .prepare(`select 1 from repo_context_suppressions where context_id = ?`)
          .get(result.contextId));
        if (!same && !inflight && !suppressed) {
          throw new Error("repo_context_result_not_inflight");
        }
        prepared.push({ result, existing, same, inflight, suppressed });
      }

      const fillableContextIds: string[] = [];
      for (const item of prepared) {
        const { result, existing, same, inflight, suppressed } = item;
        if (suppressed) {
          if (same) receipt.resultReplays += 1;
          else receipt.unknownResults += 1;
          clearInflight.run(result.contextId);
          clearHandoff.run(result.contextId);
          continue;
        }
        if (same && !inflight) {
          receipt.resultReplays += 1;
          continue;
        }
        if (!result.repoHash) {
          receipt.unknownResults += 1;
          this.recordRepoContextDrop("resolution_failed");
          clearInflight.run(result.contextId);
          clearHandoff.run(result.contextId);
          continue;
        }
        if (existing) {
          if (same) {
            receipt.resultReplays += 1;
            fillableContextIds.push(result.contextId);
          } else {
            receipt.resultConflicts += 1;
            if (this.recordRepoContextConflict(result)) {
              this.recordRepoContextDrop("result_conflict");
            }
          }
          clearInflight.run(result.contextId);
          clearHandoff.run(result.contextId);
          continue;
        }
        if (this.repoContextResultAtCapacity()) {
          const gc = this.gcRepoContextResultsInCurrentTransaction(1, 1);
          if (gc.deleted === 0) {
            receipt.unknownResults += 1;
            this.recordRepoContextDrop("resolution_failed");
            clearInflight.run(result.contextId);
            clearHandoff.run(result.contextId);
            continue;
          }
        }
        const inserted = insertResult.run({
          ...result,
          acceptedAt: new Date().toISOString(),
        }).changes;
        if (inserted > 0) {
          receipt.resultsInserted += 1;
          fillableContextIds.push(result.contextId);
        }
        clearInflight.run(result.contextId);
        clearHandoff.run(result.contextId);
      }

      const fill = this.fillRepoContextRowsInCurrentTransaction(
        boundedFill,
        fillableContextIds,
      );
      receipt.rowsVisited += fill.rowsVisited;
      receipt.rowsFilled += fill.rowsFilled;
      return receipt;
    }).immediate();
  }

  private recordRepoContextConflict(result: RepoContextResult) {
    const digest = `sha256:${crypto
      .createHash("sha256")
      .update(JSON.stringify([
        result.contextId,
        result.repoHash,
        result.branchHash,
        result.headSha,
        result.resolverVersion,
      ]))
      .digest("hex")}`;
    return this.recordRepoContextConflictDigest(result.contextId, digest);
  }

  private recordRepoContextRowConflict(
    contextId: string,
    existingRepoHash: string,
    resolvedRepoHash: string,
  ) {
    const digest = `sha256:${crypto
      .createHash("sha256")
      .update(JSON.stringify([
        "event_repo_mismatch",
        contextId,
        existingRepoHash,
        resolvedRepoHash,
      ]))
      .digest("hex")}`;
    return this.recordRepoContextConflictDigest(contextId, digest);
  }

  private recordEventCollisionDigest(eventId: string, digest: string) {
    const now = new Date().toISOString();
    const existing = this.db
      .prepare(
        `select last_conflict_digest as lastDigest
         from event_collision_quarantine where event_id = ?`,
      )
      .get(eventId) as { lastDigest: string } | undefined;
    if (existing?.lastDigest === digest) return;
    if (existing) {
      this.db
        .prepare(
          `update event_collision_quarantine set
             conflict_count = min(@limit, conflict_count + 1),
             last_conflict_digest = @digest,
             last_conflicted_at = @now
           where event_id = @eventId`,
        )
        .run({
          eventId,
          digest,
          now,
          limit: REPO_CONTEXT_COUNTER_LIMIT,
        });
      return;
    }
    this.db
      .prepare(
        `insert into event_collision_quarantine
           (event_id, conflict_count, first_conflict_digest, last_conflict_digest,
            first_conflicted_at, last_conflicted_at)
         values (@eventId, 1, @digest, @digest, @now, @now)`,
      )
      .run({ eventId, digest, now });
  }

  /**
   * Bounded ingest-integrity summary for the management status surface:
   * counts only — no event IDs, digests, or payload material.
   */
  eventCollisionSummary() {
    const row = this.db
      .prepare(
        `select count(*) as quarantinedEventIds,
                coalesce(sum(conflict_count), 0) as totalConflicts
         from event_collision_quarantine`,
      )
      .get() as { quarantinedEventIds: number; totalConflicts: number };
    return {
      quarantinedEventIds: Number(row.quarantinedEventIds),
      totalConflicts: Math.min(Number(row.totalConflicts), REPO_CONTEXT_COUNTER_LIMIT),
    };
  }

  private recordRepoContextConflictDigest(contextId: string, digest: string) {
    const now = new Date().toISOString();
    const existing = this.db
      .prepare(
        `select last_conflict_digest as lastDigest
         from repo_context_conflicts where context_id = ?`,
      )
      .get(contextId) as { lastDigest: string } | undefined;
    if (existing?.lastDigest === digest) return false;
    if (existing) {
      this.db
        .prepare(
          `update repo_context_conflicts set
             conflict_count = min(@limit, conflict_count + 1),
             last_conflict_digest = @digest,
             last_conflicted_at = @now
           where context_id = @contextId`,
        )
        .run({
          contextId,
          digest,
          now,
          limit: REPO_CONTEXT_COUNTER_LIMIT,
        });
      return true;
    }
    const inserted = this.db
      .prepare(
        `insert into repo_context_conflicts
           (context_id, conflict_count, first_conflict_digest, last_conflict_digest,
            first_conflicted_at, last_conflicted_at)
         select @contextId, 1, @digest, @digest, @now, @now
         where (select count(*) from repo_context_conflicts) < @rowLimit
         on conflict(context_id) do nothing`,
      )
      .run({
        contextId,
        digest,
        now,
        rowLimit: REPO_CONTEXT_CONFLICT_ROW_LIMIT,
      }).changes;
    // The global bounded counter remains the only evidence once the
    // context-keyed quarantine reaches its fixed cardinality limit.
    return inserted > 0 || !existing;
  }

  private appendInCurrentTransaction(
    event: AiInteractionEvent, suppressedFields: string[] = [], project = true,
    firstReceivedAt?: string, historyImportNoLiveSibling = false,
  ) {
    if (!liveUsageAppendAllowed(this.db, event)) return { appended: false, repoContextRequest: null };
    if (event.dataMode === "evidence") {
      throw new Error(
        "Raw evidence rows cannot be appended to the ordinary ledger; the encrypted evidence vault is not implemented.",
      );
    }
    const trustedInstallationEpochId = liveUsageInstallationEpoch(this.db, event) ??
      captureRootEventInstallationEpoch(event);
    const enrollmentRejected = this.eventAdmissionReason(event.observedAt, event.metadata?.installationEpochId,
      trustedInstallationEpochId);
    if (enrollmentRejected) return { appended: false, repoContextRequest: null, enrollmentRejected };
    const createdAt = new Date().toISOString();
    if (!this.claimSessionUsageAuthority(event, createdAt)) {
      return { appended: false, repoContextRequest: null };
    }
    const boundRepoContextId = peekRepoContextId(event);
    const repoContextRequest = boundRepoContextId ? null : this.repoContextRequestFor(event);
    const repoContextId = boundRepoContextId ?? repoContextRequest?.contextId ?? null;
    const boundRepoContextBindable = boundRepoContextId
      ? this.canBindRepoContextId(boundRepoContextId)
      : true;
    const existingRepoHash = gitField(event, "remoteUrlHash");
    const resolvedRepoContext = this.resolvedRepoContext(repoContextId, true);
    const repoContextConflict = Boolean(
      existingRepoHash && resolvedRepoContext.repoHash &&
      existingRepoHash !== resolvedRepoContext.repoHash,
    );
    const repoHash = existingRepoHash ?? resolvedRepoContext.repoHash;
    const branchHash = gitField(event, "branchHash") ??
      (repoContextConflict ? null : resolvedRepoContext.branchHash);
    const headSha = gitField(event, "headSha") ??
      (repoContextConflict ? null : resolvedRepoContext.headSha);
    const privacyGeneration = crypto.randomUUID();
    const currentBinding = this.workspaceBinding();
    const workspaceId = currentBinding?.currentWorkspaceId ?? this.workspaceId;
    const installationEpochId = trustedInstallationEpochId ?? currentBinding?.currentInstallationEpochId ?? null;
    const projectKey = canonicalProjectKey(event.projectKey);
    const costKind = admittedCostKind(event);
    const canonicalSuppressedFields = canonicalizeSuppressionReceipts(suppressedFields);
    // Only new context-bound captures carry this local-only policy receipt.
    // Sealing reads it from the exact raw lineage; it is not an outbound field.
    const payloadJson = JSON.stringify(repoContextId ? {
      ...event, metadata: { ...event.metadata, repoContextPolicyGeneration: REPO_CONTEXT_CAPTURE_POLICY_GENERATION },
    } : event);
    const insert = this.insertEventStatement ??= this.db.prepare(
        `insert or ignore into buffered_events
          (id, source, event_type, data_mode, observed_at, payload_json, suppressed_fields_json,
           created_at, first_received_at, session_id, action_class, model, input_tokens, output_tokens,
           cache_read_tokens, cache_creation_tokens, cost_usd, uploaded_at, repo_hash, branch_hash, head_sha,
           machine, account_hash, workspace_id, device_id, installation_epoch_id, project_key, cost_kind, privacy_generation)
        values
          (@id, @source, @eventType, @dataMode, @observedAt, @payloadJson, @suppressedFieldsJson,
           @createdAt, @firstReceivedAt, @sessionId, @actionClass, @model, @inputTokens, @outputTokens,
           @cacheReadTokens, @cacheCreationTokens, @costUsd, null, @repoHash, @branchHash, @headSha,
           @machine, @accountHash, @workspaceId, @deviceId, @installationEpochId, @projectKey, @costKind, @privacyGeneration)`,
      );
    const result = insert
      .run({
        id: event.id,
        source: event.source,
        eventType: event.eventType,
        dataMode: event.dataMode,
        observedAt: event.observedAt,
        payloadJson,
        suppressedFieldsJson: JSON.stringify(canonicalSuppressedFields),
        createdAt,
        firstReceivedAt: firstReceivedAt ?? null,
        sessionId: event.sessionId ?? null,
        actionClass: event.actionClass ?? null,
        model: event.model ?? null,
        inputTokens: event.inputTokens ?? null,
        outputTokens: event.outputTokens ?? null,
        cacheReadTokens: event.cacheReadTokens ?? null,
        cacheCreationTokens: event.cacheCreationTokens ?? null,
        costUsd: event.costUsd ?? null,
        repoHash,
        branchHash,
        headSha,
        machine: MACHINE,
        accountHash: event.actorId ?? null,
        workspaceId,
        installationEpochId,
        projectKey,
        costKind,
        deviceId: this.deviceId,
        privacyGeneration,
      });
    if (result.changes > 0) {
      this.budgetAttemptedRows += 1;
      const usagePair = pairCodexUsageEvent(this.db, event.id);
      const pairedLogPayload = usagePair?.logId === event.id
        ? (this.db.prepare(`select payload_json as payloadJson from buffered_events where id = ?`)
            .get(event.id) as { payloadJson: string }).payloadJson
        : payloadJson;
      if (repoContextId) {
        this.db.prepare(
          `insert into repo_context_event_links
             (event_id, context_id, fill_pending, context_conflict, suppression_cleaned)
           values (@eventId, @contextId, @fillPending, @contextConflict, @suppressionCleaned)`,
        ).run({
          eventId: event.id,
          contextId: repoContextId,
          fillPending: resolvedRepoContext.exists || resolvedRepoContext.suppressed ||
              !boundRepoContextBindable
            ? 0
            : 1,
          contextConflict: repoContextConflict ? 1 : 0,
          suppressionCleaned: resolvedRepoContext.suppressed ? 1 : 0,
        });
      }
      this.delivery.noteRawAppend(Number(result.lastInsertRowid));
      this.delivery.enqueueRaw({
        rawRowid: Number(result.lastInsertRowid),
        rawId: event.id,
        dataMode: event.dataMode,
        createdAt,
        uploadedAt: null,
        payloadJson: pairedLogPayload,
        suppressedFieldsJson: JSON.stringify(canonicalSuppressedFields),
        repoHash,
        branchHash,
        workspaceId: this.workspaceId,
        privacyGeneration,
        privacyDisposition: null,
        usageDuplicateReason: usagePair?.spanId === event.id
          ? CODEX_USAGE_DUPLICATE_REASON : null,
        deviceId: this.deviceId,
      });
      if (repoContextConflict && repoContextId && existingRepoHash && resolvedRepoContext.repoHash) {
        this.recordRepoContextRowConflict(
          repoContextId,
          existingRepoHash,
          resolvedRepoContext.repoHash,
        );
        this.recordRepoContextDrop("result_conflict");
      }
      if (event.actorId) this.seedAccountLabel(event.actorId);
      // The raw row, privacy-safe fact delta, and delivery envelope share the
      // caller's SQLite transaction. Projection failure is contained as a
      // durable repair receipt so capture remains available.
      if (project) this.projection.tryApplyRawRow(Number(result.lastInsertRowid), new Date(),
        historyImportNoLiveSibling && this.historyImportEpoch !== null &&
        (event.eventType === "usage_rollout" || event.eventType === "usage_transcript"));
      // Otherwise the insert trigger's durable repair receipt remains queued.
      // Capture admission never depends on finishing all derived aggregates.
      // Runtime learning facts (#156) promote from the same durable moment.
      // The promoter is total (it records bounded drop receipts instead of
      // throwing); the guard keeps fact promotion structurally unable to
      // fail an event append even if that invariant regresses.
      try {
        promoteRuntimeLearningFacts(this, event);
      } catch {
        /* capture availability outranks learning-fact promotion */
      }
      return {
        appended: true,
        repoContextRequest: resolvedRepoContext.exists ? null : repoContextRequest,
      };
    }
    // Deterministic replay never rewrites the evidence row or resets its
    // upload marker. It may repair an absent delivery projection from the
    // already-committed raw truth. Issue 0056 (#104): a repeated ID is then
    // classified — identical payload digest dedupes; a different digest is a
    // collision and earns a bounded quarantine receipt (digests only, never
    // raw rejected values).
    this.delivery.repairRawById(event.id);
    const existingRow = this.db
      .prepare(`select payload_json as payloadJson from buffered_events where id = ?`)
      .get(event.id) as { payloadJson: string } | undefined;
    if (!existingRow) return { appended: false, repoContextRequest: null };
    const incomingDigest = sha256DigestHex(payloadJson);
    const storedDigest = sha256DigestHex(existingRow.payloadJson);
    if (incomingDigest.equals(storedDigest)) {
      return { appended: false, deduplicated: true, repoContextRequest: null };
    }
    this.recordEventCollisionDigest(event.id, `sha256:${storedDigest.toString("hex")}`);
    return { appended: false, collisionQuarantined: true, repoContextRequest: null };
  }

  private closeUnreservedRepoContextLink(
    eventId: string,
    request: RepoContextRequest | null,
    reserved: boolean,
  ) {
    if (!request || reserved) return;
    this.db.prepare(
      `update repo_context_event_links set fill_pending = 0
       where event_id = ? and context_id = ? and fill_pending = 1`,
    ).run(eventId, request.contextId);
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
    // Another connection can win between the compatibility lookup and the
    // insert. Always re-read the durable winner; returning our proposed value
    // would allow both the live receiver and maintenance tailer to append.
    const winner = this.db
      .prepare(
        `select authority from session_usage_authority
         where source = ? and session_id = ?`,
      )
      .get(event.source, event.sessionId) as { authority: "tailer" | "live" } | undefined;
    return winner?.authority === desired;
  }

  sessionUsageAuthority(source: "codex" | "claude_code" | "grok", sessionId: string) {
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

  append(
    event: AiInteractionEvent,
    suppressedFields?: string[],
    options?: { integrityReceipt?: false | undefined; historyImportNoLiveSibling?: boolean },
  ): boolean;
  append(
    event: AiInteractionEvent,
    suppressedFields: string[] | undefined,
    options: { integrityReceipt: true; firstReceivedAt?: string; historyImportNoLiveSibling?: boolean },
  ): {
    appended: boolean;
    deduplicated?: true;
    collisionQuarantined?: true;
    enrollmentRejected?: NonNullable<ReturnType<LocalEventBuffer["eventAdmissionReason"]>>;
  };
  append(
    event: AiInteractionEvent,
    suppressedFields: string[] = [],
    options: { integrityReceipt?: boolean; firstReceivedAt?: string;
      historyImportNoLiveSibling?: boolean } = {},
  ) {
    const ownsHandoffs = this.activeRepoContextCommitScope === null;
    const handoffs = this.activeRepoContextCommitScope ?? this.newRepoContextHandoffBatch();
    let result: ReturnType<LocalEventBuffer["appendInCurrentTransaction"]>;
    try {
      const run = () => {
        const appended = this.appendInCurrentTransaction(event, suppressedFields, true,
          options.firstReceivedAt, options.historyImportNoLiveSibling);
        if(appended.appended && this.deferredClaudeHookIds.has(event.id))
          this.db.prepare(`insert or ignore into claude_replay_hooks(event_id,status,created_at,root_set_json)
            values(?,'pending',?,?)`).run(event.id,new Date().toISOString(),this.claudeReplayRootSetJson);
        const reserved = this.reserveRepoContextHandoff(
          appended.repoContextRequest,
          handoffs,
        );
        this.closeUnreservedRepoContextLink(
          event.id,
          appended.repoContextRequest,
          reserved,
        );
        if (ownsHandoffs && handoffs.overflowCount > 0) {
          this.recordRepoContextDrop("queue_overflow", handoffs.overflowCount);
        }
        return appended;
      };
      result = ownsHandoffs ? this.db.transaction(run).immediate() : run();
    } finally {
      this.deferredClaudeHookIds.delete(event.id);
      takeRepoContextSidecar(event);
      takeRepoContextId(event);
    }
    if (ownsHandoffs) this.finalizeRepoContextHandoffs(handoffs);
    if (!options.integrityReceipt) return result.appended;
    return {
      appended: result.appended,
      ...(result.deduplicated ? { deduplicated: true as const } : {}),
      ...(result.collisionQuarantined
        ? { collisionQuarantined: true as const }
        : {}),
      ...(result.enrollmentRejected ? { enrollmentRejected: result.enrollmentRejected } : {}),
    };
  }

  /** The daemon activates this before accepting hooks. Fixture buffers remain
   * on the ordinary hot path unless a replay is explicitly started. */
  beginClaudeReplayBarrier(rootDigests:readonly string[]=[]) {
    this.claudeReplayRootSetJson=JSON.stringify([...rootDigests].sort());
    this.claudeReplayState="pending";
  }
  finishClaudeReplayBarrier(reached: boolean) {
    this.claudeReplayState=reached ? "ready":"timed_out";
  }
  claudeReplayBarrierState() { return this.claudeReplayState; }
  deferClaudeHookUntilReplay(eventId: string) {
    if(this.claudeReplayState!=="pending") return false;
    this.deferredClaudeHookIds.add(eventId);
    return true;
  }

  appendMany(
    entries: Array<{ event: AiInteractionEvent; suppressedFields: string[] }>,
    metricSamples: MetricSample[] = [],
    admissionDrops: OtlpAdmissionDrop[] = [],
    options: { projectionDeadlineMs?: number } = {},
  ) {
    const projectionDeadlineMs = options.projectionDeadlineMs ?? performance.now() + 25;
    const appended: Array<ReturnType<LocalEventBuffer["appendInCurrentTransaction"]>> = [];
    let enrollmentRejectedMetricCount = 0;
    const ownsHandoffs = this.activeRepoContextCommitScope === null;
    const handoffs = this.activeRepoContextCommitScope ?? this.newRepoContextHandoffBatch();
    const closeUnreservedLink = this.db.prepare(
      `update repo_context_event_links set fill_pending = 0
       where event_id = ? and context_id = ? and fill_pending = 1`,
    );
    let workStarted = false;
    const work = () => {
      workStarted = true;
      for (const entry of entries) {
        const result = this.appendInCurrentTransaction(entry.event, entry.suppressedFields, performance.now() < projectionDeadlineMs);
        appended.push(result);
        const reserved = this.reserveRepoContextHandoff(result.repoContextRequest, handoffs);
        if (result.repoContextRequest && !reserved) {
          closeUnreservedLink.run(entry.event.id, result.repoContextRequest.contextId);
        }
      }
      for (const sample of metricSamples) {
        if (!this.appendMetricSample(sample)) enrollmentRejectedMetricCount += 1;
      }
      const now = new Date().toISOString();
      for (const drop of admissionDrops) {
        this.recordOtlpAdmissionDrop(drop, now);
      }
      if (ownsHandoffs && handoffs.overflowCount > 0) {
        this.recordRepoContextDrop("queue_overflow", handoffs.overflowCount);
      }
    };
    try {
      if (ownsHandoffs) this.db.transaction(work).immediate();
      else work();
    } finally {
      // BEGIN IMMEDIATE can lose to maintenance before the callback starts.
      // Preserve sidecars for the listener's bounded retry in that case.
      if (!ownsHandoffs || workStarted) {
        for (const entry of entries) {
          takeRepoContextSidecar(entry.event);
          takeRepoContextId(entry.event);
        }
      }
    }
    if (ownsHandoffs) this.finalizeRepoContextHandoffs(handoffs);
    return {
      deduplicatedCount: appended.filter((entry) => entry.deduplicated).length,
      collisionQuarantinedCount: appended.filter((entry) => entry.collisionQuarantined).length,
      enrollmentRejectedEventCount: appended.filter((entry) => entry.enrollmentRejected).length,
      enrollmentRejectedMetricCount,
    };
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
    // The label is cosmetic; a missing passwd entry (uv_os_get_passwd ENOENT
    // under unavailable directory services) must never fail a capture append.
    let username: string;
    try {
      username = os.userInfo().username;
    } catch {
      username = "local";
    }
    const base = `${username}@${MACHINE}`;
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
    const run = () => {
      if (!liveUsageMetricAllowed(this.db, sample)) return false;
      if (this.eventAdmissionReason(sample.observedAt)) return false;
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
      return true;
    };
    return this.db.inTransaction ? run() : this.db.transaction(run)();
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
    deviceId: string | null;
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
      deviceId: row.deviceId ?? null,
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
          workspace_id as workspaceId, device_id as deviceId
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
          workspace_id as workspaceId, device_id as deviceId,
          length(cast(payload_json as blob)) as payloadBytes
        from buffered_events
        where uploaded_at is null
          and ${privacyEligible}
          -- Fail closed (#163 rework): a null workspace binding sees ONLY
          -- unassigned rows, never rows bound to any workspace.
          and workspace_id is ?
          and device_id is ?
        order by created_at asc
        limit ?`,
      )
      .all(this.workspaceId, this.deviceId, maxRows) as Array<
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
      `update buffered_events set uploaded_at = ? where id = ?
         and workspace_id is ? and device_id is ? and ${privacyEligible}`,
    );
    const run = this.db.transaction((eventIds: string[]) => {
      let updated = 0;
      for (const id of eventIds) {
        updated += mark.run(uploadedAt, id, this.workspaceId, this.deviceId).changes;
      }
      return updated;
    });
    return run(ids);
  }

  /** Keep active and unacknowledged deliveries; old uploaded_at-only rows may expire. */
  private rawRetentionUploadHoldSql() {
    // Lease uses this same durable audience. A previous audience cannot make
    // progress after a binding change and must not hold raw retention open.
    const activeAudience = `(e.workspace_id is (
      select current_workspace_id from collector_workspace_binding where singleton = 1
    ) and e.device_id is (
      select current_device_id from collector_workspace_binding where singleton = 1
    ))`;
    // Pre-raw_id rows retain raw_rowid; only fully unlinked legacy rows use
    // the derived delivery id. A linked row can share that id with another raw.
    const activeOutbox = `(exists (
      select 1 from upload_outbox o
      where o.raw_rowid = e.rowid and (o.raw_id is null or
        (o.raw_id = e.id and o.raw_created_at = e.created_at
          and o.raw_generation is e.privacy_generation))
    ) or exists (
      select 1 from upload_outbox o where o.delivery_id = retention_delivery_id(e.id)
        and o.raw_rowid is null and o.raw_id is null
    ))`;
    const outboxEligible = this.rawRetentionOutboxEligibleSql();
    if (!this.delivery.isEnabled()) return `(${activeAudience} and ${outboxEligible} and ${activeOutbox})`;
    return `(${activeAudience} and ((${outboxEligible} and ${activeOutbox}) or (
      e.data_mode = 'metadata'
      and e.privacy_disposition is null
      and e.usage_duplicate_reason is null
      and not exists (
        select 1 from upload_receipts local_receipt
        where local_receipt.raw_rowid = e.rowid
          and local_receipt.raw_id = e.id
          and local_receipt.raw_created_at = e.created_at
          and local_receipt.raw_generation is e.privacy_generation
          and local_receipt.terminal_state = 'dead'
          and local_receipt.reason in (
            'local_evidence_quarantined', 'local_payload_unparseable',
            'local_schema_invalid', 'local_privacy_violation',
            'local_item_oversize', 'local_usage_duplicate'
          )
      )
      and (e.uploaded_at is null or exists (
        select 1 from upload_receipts unacknowledged
        where unacknowledged.raw_rowid = e.rowid
          and unacknowledged.raw_id = e.id
          and unacknowledged.raw_created_at = e.created_at
          and unacknowledged.raw_generation is e.privacy_generation
          and unacknowledged.terminal_state = 'dead'
      ) or exists (
        -- Until the bounded lineage repair proves an owner, a legacy remote
        -- rejection can only add a hold. It must never waive one.
        select 1 from upload_receipts legacy_unacknowledged
        where ${legacyNullLineageReceiptMatchSql("e", "legacy_unacknowledged")}
          and legacy_unacknowledged.terminal_state = 'dead'
          and legacy_unacknowledged.reason in
            ('remote_validation_rejected', 'remote_rejected_exhausted')
      ))
    )))`;
  }

  private rawRetentionOutboxEligibleSql() {
    return `(e.data_mode = 'metadata' and e.privacy_disposition is null
      and e.usage_duplicate_reason is null)`;
  }

  prune(
    retentionDays = 90,
    options: { maxRows?: number; now?: Date } = {},
  ) {
    const requestedRows = Number.isFinite(options.maxRows) ? Math.trunc(options.maxRows!) : 1_000;
    const maxRows = Math.max(1, Math.min(requestedRows, 10_000));
    const now = options.now ?? new Date();
    const cutoff = new Date(now.getTime() - retentionDays * 24 * 60 * 60 * 1_000).toISOString();
    // The ordinary retention cadence starts immediately and follows hasMore.
    // Keep repairing dead receipts between upload intervals without extending
    // any one writer turn beyond its existing 100 ms limit.
    const lineageRepair = this.delivery.backfillLegacyReceiptLineage({ maxRows: 256, maxWriterMs: 100 });
    // A bounded upgrade page runs before the first raw deletion. If it has
    // more observations, defer this pass; the next call resumes at its durable
    // cursor. Every deletion also promotes its own observation atomically.
    const promotion=promoteLegacyClaudeRootSightings(this.db,maxRows);
    if(!promotion.complete) return {
      cutoff,events:0,metricSamples:0,eventRowsVisited:0,
      migrationProtectedRows:0,metricRowsVisited:0,hasMore:true,
      madeProgress:promotion.visited>0 || lineageRepair.visited>0,
    };
    // Seek through a bounded raw candidate page BEFORE checking migration.
    // A protected prefix must neither cause a full scan nor hide later rows.
    const scanKey = "raw_retention_scan_v1";
    const deliveryProtection = `case when ${this.rawRetentionUploadHoldSql()} then 1 else 0 end`;
    const run = this.db.transaction(() => {
      const stored = this.db.prepare(`select value from maintenance_state where key=?`).get(scanKey) as {value:string}|undefined;
      const scan = stored ? JSON.parse(stored.value) as {at:string;id:string;metricsFirst:boolean} :
        {at:"",id:"",metricsFirst:false};
      const metricsPending = Boolean(this.db.prepare(
        `select 1 from metric_samples indexed by idx_metrics_observed where created_at < ? limit 1`,
      ).get(cutoff));
      const rawLimit = !metricsPending ? maxRows : maxRows === 1 ? (scan.metricsFirst ? 0 : 1) : Math.ceil(maxRows / 2);
      const candidates = rawLimit === 0 ? [] : this.db.prepare(
        `select e.rowid as rawRowid, e.id as eventId,
           e.created_at as rawCreatedAt, e.privacy_generation as rawGeneration,
           e.source, e.workspace_id as workspaceId, e.device_id as deviceId,
           e.data_mode as dataMode, e.privacy_disposition as privacyDisposition,
           e.usage_duplicate_reason as usageDuplicateReason,
           case when ${this.rawRetentionOutboxEligibleSql()} then 1 else 0 end as uploadEligible,
           e.installation_epoch_id as installationEpochId, e.observed_at as observedAt,
           ${deliveryProtection} as migrationProtected
         from buffered_events e indexed by idx_events_retention
         where e.created_at < ? and (e.created_at,e.id) > (?,?)
         order by e.created_at,e.id limit ?`,
      ).all(cutoff,scan.at,scan.id,rawLimit) as Array<{
        rawRowid:number;eventId:string;rawCreatedAt:string;rawGeneration:string|null;migrationProtected:number;
        source:string;workspaceId:string|null;deviceId:string|null;
        dataMode:string;privacyDisposition:string|null;usageDuplicateReason:string|null;
        uploadEligible:number;
        installationEpochId:string|null;observedAt:string;
      }>;
      let migrationProtectedRows = 0;
      const recordExpiry = this.db.prepare(
        `insert or ignore into raw_retention_receipts
           (event_id, raw_rowid, raw_created_at, raw_generation, expired_at, reason)
         values (@eventId, @rawRowid, @rawCreatedAt, @rawGeneration, @expiredAt,
           'retention_window_elapsed')`,
      );
      const removeRaw = this.db.prepare(`delete from buffered_events where rowid = ?`);
      let events = 0;
      let retirementPending = false;
      const expiredCandidates: typeof candidates = [];
      for (const row of candidates) {
        if (row.migrationProtected) { migrationProtectedRows += 1; continue; }
        if (row.workspaceId !== this.workspaceId || row.deviceId !== this.deviceId) {
          // Close one linked delivery per visit. A raw row with more than one
          // legacy delivery is revisited by the bounded retention cursor.
          if (this.delivery.retirePriorAudienceRaw(row.rawRowid, row.eventId,
            row.rawCreatedAt, row.rawGeneration, row.workspaceId, row.deviceId,
            now.toISOString())) {
            retirementPending = true;
            continue;
          }
        }
        if (!row.uploadEligible) {
          if (this.delivery.retireIneligibleRaw(row.rawRowid, row.eventId,
            row.rawCreatedAt, row.rawGeneration, row.dataMode,
            row.privacyDisposition, row.usageDuplicateReason, now.toISOString())) {
            retirementPending = true;
            continue;
          }
        }
        if (row.source === "claude_code")
          promoteClaudeRootSightingsForRaw(this.db, row.eventId);
        recordExpiry.run({
          eventId: row.eventId,
          rawRowid: row.rawRowid,
          rawCreatedAt: row.rawCreatedAt,
          rawGeneration: row.rawGeneration,
          expiredAt: now.toISOString(),
        });
        events += removeRaw.run(row.rawRowid).changes;
        expiredCandidates.push(row);
      }
      advanceFinanceRetentionWatermarks(this.db, expiredCandidates as FinanceCoverageMutationRow[], now.toISOString());
      const remainingBudget = Math.max(0, maxRows - candidates.length);
      const metricRows = remainingBudget === 0
        ? []
        : this.db.prepare(
          `select rowid as rowid from metric_samples
           where created_at < ? order by created_at, rowid limit ?`,
        ).all(cutoff, remainingBudget) as Array<{ rowid: number }>;
      let metricSamples = 0;
      const removeMetric = this.db.prepare(`delete from metric_samples where rowid = ?`);
      for (const row of metricRows) metricSamples += removeMetric.run(row.rowid).changes;
      const last = candidates.at(-1);
      // A full page alone does not prove more work. Probe just beyond its
      // cursor so an idle ledger does not enter the five-second cadence.
      const rawHasMore = (rawLimit === 0 || candidates.length === rawLimit) &&
        Boolean(this.db.prepare(`select 1 from buffered_events e indexed by idx_events_retention
          where e.created_at < ? and (e.created_at,e.id) > (?,?) limit 1`)
          .get(cutoff, last?.rawCreatedAt ?? scan.at, last?.eventId ?? scan.id));
      const metricHasMore = metricsPending && Boolean(this.db.prepare(
        `select 1 from metric_samples indexed by idx_metrics_observed
         where created_at < ? limit 1`,
      ).get(cutoff));
      const hasMore = !lineageRepair.complete || retirementPending || rawHasMore || metricHasMore;
      const madeProgress = lineageRepair.visited > 0 || retirementPending ||
        candidates.length > 0 || metricRows.length > 0;
      const next = rawLimit === 0 ? scan : rawHasMore && last
        ? {at:last.rawCreatedAt,id:last.eventId,metricsFirst:scan.metricsFirst}
        : {at:"",id:"",metricsFirst:scan.metricsFirst};
      this.db.prepare(`insert into maintenance_state(key,value,updated_at) values (?,?,?)
        on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at`)
        .run(scanKey,JSON.stringify({...next,metricsFirst:!scan.metricsFirst,migrationProtectedRows}),now.toISOString());
      this.db.prepare(
        `update raw_retention_control set last_cutoff_at=?,last_run_at=?,
           last_rows_visited=?,last_rows_expired=?,last_has_more=?,
           expired_total=expired_total+? where singleton=1`,
      ).run(
        cutoff,
        now.toISOString(),
        candidates.length + metricRows.length,
        events + metricSamples,
        hasMore ? 1 : 0,
        events,
      );
      return {
        events,
        eventRowsVisited: candidates.length,
        migrationProtectedRows,
        metricSamples,
        metricRowsVisited: metricRows.length,
        hasMore,
        madeProgress,
      };
    }).immediate();
    if (run.events > 0) this.invalidateRetentionHoldCount();
    return {
      cutoff,
      events: run.events,
      metricSamples: run.metricSamples,
      eventRowsVisited: run.eventRowsVisited,
      migrationProtectedRows: run.migrationProtectedRows,
      metricRowsVisited: run.metricRowsVisited,
      hasMore: run.hasMore,
      madeProgress: run.madeProgress,
    };
  }

  private invalidateRetentionHoldCount() {
    this.retentionHoldGeneration += 1;
    this.retentionHoldCountDirty = true;
  }

  private retentionHoldRevision() {
    return (this.db.prepare(
      `select revision from retention_hold_revision where singleton=1`,
    ).get() as { revision: number }).revision;
  }

  /** Bound the synchronous sample; a worker supplies the exact large-ledger count. */
  retentionProgressStatus(retentionDays = 90, now = new Date()) {
    const pass = this.db.prepare(`select last_rows_visited as rowsVisited,
      last_rows_expired as rowsExpired,last_has_more as hasMore,last_run_at as at,
      expired_total as expired from raw_retention_control where singleton=1`).get() as {
        rowsVisited:number;rowsExpired:number;hasMore:number;at:string|null;expired:number;
      };
    const scan = this.db.prepare(`select value from maintenance_state where key='raw_retention_scan_v1'`).get() as {value:string}|undefined;
    const cutoffAt = new Date(now.getTime()-retentionDays*86_400_000).toISOString();
    const revision = this.retentionHoldRevision();
    const generation = this.retentionHoldGeneration;
    const cached = this.retentionHoldCount;
    const fresh = cached && !this.retentionHoldCountDirty &&
      cached.revision === revision && cached.generation === generation &&
      cached.retentionDays === retentionDays && cutoffAt >= cached.cutoffAt &&
      performance.now() >= cached.measuredAtMonotonic &&
      performance.now() - cached.measuredAtMonotonic < 60_000;
    let heldForUpload: number;
    let heldForUploadExact: boolean;
    if (fresh) {
      heldForUpload = cached.count;
      // Exactness describes the completed count at its own cutoff, even when
      // this status refresh has advanced the policy cutoff by milliseconds.
      heldForUploadExact = true;
    } else {
      // Never walk an offline backlog on the collector's event loop. A small
      // ledger is exact here; the read-only worker counts a larger one.
      const sample = this.db.prepare(
        `select case when ${this.rawRetentionUploadHoldSql()} then 1 else 0 end as held
         from buffered_events e indexed by idx_events_retention
         where e.created_at < ? order by e.created_at,e.id limit 513`,
      ).all(cutoffAt) as Array<{ held: number }>;
      heldForUpload = sample.reduce((count, row) => count + row.held, 0);
      heldForUploadExact = sample.length < 513 &&
        revision === this.retentionHoldRevision() && generation === this.retentionHoldGeneration;
      if (heldForUploadExact) {
        this.retentionHoldCount = { count: heldForUpload, cutoffAt, measuredAtMonotonic: performance.now(),
          revision, generation, retentionDays };
        this.retentionHoldCountDirty = false;
      } else {
        if (cached?.retentionDays === retentionDays && cutoffAt >= cached.cutoffAt)
          heldForUpload = cached.count;
        if (!this.db.memory) {
          try { void this.refreshRetentionHoldCount(retentionDays, now).catch(() => undefined); }
          catch { /* retain the bounded observation for this refresh */ }
        }
      }
    }
    return {
      inspection: "bounded" as const,
      policy: {retentionDays,cutoffAt},
      states: {retained:null,pendingDelivery:null,heldForUpload,quarantined:null,expired:pass.expired,notInspected:1},
      lastPass: {rowsVisited:pass.rowsVisited,rowsExpired:pass.rowsExpired,
        hasMore:Boolean(pass.hasMore),at:pass.at,
        heldForUploadExact,
        heldForUploadAsOfCutoff: fresh ? cached.cutoffAt :
          heldForUploadExact ? cutoffAt :
            cached?.retentionDays === retentionDays && cutoffAt >= cached.cutoffAt ? cached.cutoffAt : null,
        migrationProtectedRows:scan ? Number(JSON.parse(scan.value).migrationProtectedRows ?? 0) : 0},
    };
  }

  /** Exact large-ledger count on a read-only worker; callers never wait on the intake loop. */
  refreshRetentionHoldCount(retentionDays = 90, now = new Date()): Promise<number> {
    if (this.retentionHoldCountClosed) return Promise.reject(new Error("retention_hold_count_closed"));
    if (this.retentionHoldTask) return this.retentionHoldTask.result;
    const cutoffAt = new Date(now.getTime()-retentionDays*86_400_000).toISOString();
    const revision = this.retentionHoldRevision();
    const generation = this.retentionHoldGeneration;
    if (this.db.memory) {
      const count = (this.db.prepare(
        `select count(*) as n from buffered_events e indexed by idx_events_retention
         where e.created_at < ? and ${this.rawRetentionUploadHoldSql()}`,
      ).get(cutoffAt) as { n: number }).n;
      if (revision === this.retentionHoldRevision() && generation === this.retentionHoldGeneration) {
        this.retentionHoldCount = { count, cutoffAt, measuredAtMonotonic: performance.now(),
          revision, generation, retentionDays };
        this.retentionHoldCountDirty = false;
      }
      return Promise.resolve(count);
    }
    const { worker, result, exited } = countRetentionHoldsOffThread(this.db, cutoffAt, this.rawRetentionUploadHoldSql());
    const liveWorker = { worker, exited };
    this.retentionHoldWorkers.add(liveWorker);
    void exited.then(() => this.retentionHoldWorkers.delete(liveWorker));
    const settled = result.then((count) => {
      if (!this.retentionHoldCountClosed && generation === this.retentionHoldGeneration &&
          revision === this.retentionHoldRevision()) {
        this.retentionHoldCount = { count, cutoffAt, measuredAtMonotonic: performance.now(),
          revision, generation, retentionDays };
        this.retentionHoldCountDirty = false;
        this.retentionHoldCountChanged?.();
      }
      return count;
    }).finally(() => {
      if (this.retentionHoldTask?.worker === worker) this.retentionHoldTask = null;
    });
    this.retentionHoldTask = { worker, result: settled, exited };
    return settled;
  }

  onRetentionHoldCountChanged(listener: () => void) {
    this.retentionHoldCountChanged = listener;
    return () => {
      if (this.retentionHoldCountChanged === listener) this.retentionHoldCountChanged = null;
    };
  }

  retentionStatus(retentionDays = 90, now = new Date()): RawRetentionStatus {
    const cutoffAt = new Date(
      now.getTime() - retentionDays * 24 * 60 * 60 * 1_000,
    ).toISOString();
    const binding = this.workspaceBinding();
    const quarantinePredicate = binding
      ? `(data_mode = 'evidence' or privacy_disposition is not null or
          workspace_id is null or workspace_id <> ?)`
      : `(data_mode = 'evidence' or privacy_disposition is not null)`;
    const quarantineParams = binding ? [binding.currentWorkspaceId] : [];
    const retained = (
      this.db.prepare(
        `select count(*) as n from buffered_events
         where not ${quarantinePredicate}`,
      ).get(...quarantineParams) as { n: number }
    ).n;
    const quarantined = (
      this.db.prepare(
        `select count(*) as n from buffered_events where ${quarantinePredicate}`,
      ).get(...quarantineParams) as { n: number }
    ).n;
    const pendingDelivery = (
      this.db.prepare(
        `select count(*) as n from upload_outbox
         where state in ('pending','retry','in_flight')`,
      ).get() as { n: number }
    ).n;
    const heldForUpload = (this.db.prepare(
      `select count(*) as n from buffered_events e indexed by idx_events_retention
       where e.created_at < ? and ${this.rawRetentionUploadHoldSql()}`,
    ).get(cutoffAt) as { n: number }).n;
    const pass = this.db.prepare(
      `select last_rows_visited as rowsVisited,last_rows_expired as rowsExpired,
         last_has_more as hasMore,last_run_at as at,expired_total as expired
       from raw_retention_control
       where singleton=1`,
    ).get() as {
      rowsVisited: number;
      rowsExpired: number;
      hasMore: number;
      at: string | null;
      expired: number;
    };
    return {
      inspection: "complete",
      policy: { retentionDays, cutoffAt },
      states: {
        retained,
        pendingDelivery,
        heldForUpload,
        quarantined,
        expired: pass.expired,
        notInspected: 0,
      },
      lastPass: {
        rowsVisited: pass.rowsVisited,
        rowsExpired: pass.rowsExpired,
        hasMore: Boolean(pass.hasMore),
        at: pass.at,
      },
    };
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

  /** Cheap intake counter read by the advisory footprint sampler once a minute. */
  budgetAttemptedTotal() {
    return this.budgetAttemptedRows;
  }

  close() {
    if (this.retentionHoldCountClosed) return this.retentionHoldClose ?? undefined;
    this.retentionHoldCountClosed = true;
    this.retentionHoldCountChanged = null;
    const workers = [...this.retentionHoldWorkers];
    if (workers.length === 0) {
      this.db.close();
      return;
    }
    // A count can settle before its worker exits, and a later recount can start
    // another worker. Keep the parent barrier until all of them have exited.
    this.retentionHoldClose = (async () => {
      await Promise.all(workers.map(async (task) => {
        let timeout: ReturnType<typeof setTimeout> | undefined;
        try {
          const outcome = await Promise.race([
            task.exited.then(() => "exited" as const),
            new Promise<"timeout">((resolve) => {
              timeout = setTimeout(() => resolve("timeout"), 1_000);
            }),
          ]);
          if (outcome === "timeout") {
            await task.worker.terminate();
            await task.exited;
          }
        } finally {
          if (timeout) clearTimeout(timeout);
        }
      }));
      this.db.close();
    })();
    // Legacy callers may ignore close's return value; still observe failures.
    void this.retentionHoldClose.catch(() => undefined);
    return this.retentionHoldClose;
  }
}
