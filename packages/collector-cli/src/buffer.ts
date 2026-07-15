import os from "node:os";

import Database from "better-sqlite3";

import type { AiInteractionEvent } from "../../shared/src/index";
import type { MetricSample } from "./otlp";
import type { OtlpAdmissionDrop, OtlpDropReason } from "./otlp-admission";
import { DeliveryOutbox, type DeliveryLimits } from "./outbox";

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
] as const;

export class LocalEventBuffer {
  private readonly db: Database.Database;
  readonly delivery: DeliveryOutbox;

  constructor(
    path: string,
    options: {
      delivery?: { enabled?: boolean; limits?: Partial<DeliveryLimits> };
    } = {},
  ) {
    this.db = new Database(path);
    this.db.pragma("journal_mode = WAL");
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
    `);
    this.migrateEventColumns();
    this.delivery = new DeliveryOutbox(this.db, options.delivery);
    this.db.exec(`
      create index if not exists idx_events_upload on buffered_events (uploaded_at, created_at);
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
  }

  private appendInCurrentTransaction(event: AiInteractionEvent, suppressedFields: string[] = []) {
    const createdAt = new Date().toISOString();
    const result = this.db
      .prepare(
        `insert or ignore into buffered_events
          (id, source, event_type, data_mode, observed_at, payload_json, suppressed_fields_json,
           created_at, session_id, action_class, model, input_tokens, output_tokens,
           cache_read_tokens, cache_creation_tokens, cost_usd, uploaded_at, repo_hash, branch_hash, head_sha,
           machine, account_hash)
        values
          (@id, @source, @eventType, @dataMode, @observedAt, @payloadJson, @suppressedFieldsJson,
           @createdAt, @sessionId, @actionClass, @model, @inputTokens, @outputTokens,
           @cacheReadTokens, @cacheCreationTokens, @costUsd, null, @repoHash, @branchHash, @headSha,
           @machine, @accountHash)`,
      )
      .run({
        id: event.id,
        source: event.source,
        eventType: event.eventType,
        dataMode: event.dataMode,
        observedAt: event.observedAt,
        payloadJson: JSON.stringify(event),
        suppressedFieldsJson: JSON.stringify(suppressedFields),
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
      });
    if (result.changes > 0) {
      this.delivery.noteRawAppend(Number(result.lastInsertRowid));
      this.delivery.enqueueRaw({
        rawRowid: Number(result.lastInsertRowid),
        rawId: event.id,
        createdAt,
        uploadedAt: null,
        payloadJson: JSON.stringify(event),
        suppressedFieldsJson: JSON.stringify(suppressedFields),
        repoHash: gitField(event, "remoteUrlHash"),
        branchHash: gitField(event, "branchHash"),
      });
      if (event.actorId) this.seedAccountLabel(event.actorId);
      return true;
    }
    // Deterministic replay never rewrites the evidence row or resets its
    // upload marker. It may repair an absent delivery projection from the
    // already-committed raw truth.
    this.delivery.repairRawById(event.id);
    return false;
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
  }

  removePriorityRepo(repoHash: string) {
    return this.db.prepare(`delete from priority_repos where repo_hash = ?`).run(repoHash).changes;
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
  }

  removeAccountAlias(aliasHash: string) {
    this.db.prepare(`delete from account_aliases where alias_hash = ?`).run(aliasHash);
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
  }

  /**
   * Codex usage spans arrive sessionless and modelless (live 0.137 shape).
   * Adopt session/model from the nearest codex row that has them (±10 min,
   * single-machine serialization makes this reliable), then price the usage.
   * Updates promoted columns AND payload_json so uploads carry the fix.
   * Stitch provenance lands in metadata.stitched / metadata.costEstimated.
   */
  reconcileCodexUsage(estimate: (input: {
    model?: string;
    inputTokens?: number;
    outputTokens?: number;
    cacheReadTokens?: number;
  }) => { costUsd: number } | undefined) {
    const candidates = this.db
      .prepare(
        `select id, observed_at as observedAt, session_id as sessionId, model,
           input_tokens as inputTokens, output_tokens as outputTokens,
           cache_read_tokens as cacheReadTokens, cost_usd as costUsd
         from buffered_events
         where source = 'codex' and event_type = 'assistant_response'
           and (session_id is null or model is null or cost_usd is null)
           and (input_tokens is not null or output_tokens is not null)`,
      )
      .all() as Array<{
      id: string;
      observedAt: string;
      sessionId: string | null;
      model: string | null;
      inputTokens: number | null;
      outputTokens: number | null;
      cacheReadTokens: number | null;
      costUsd: number | null;
    }>;
    if (candidates.length === 0) return { examined: 0, stitched: 0, priced: 0 };

    const nearest = this.db.prepare(
      `select session_id as sessionId, model from buffered_events
       where source = 'codex' and id != ?
         and (session_id is not null or model is not null)
         and abs(strftime('%s', observed_at) - strftime('%s', ?)) <= 600
       order by abs(strftime('%s', observed_at) - strftime('%s', ?)) asc
       limit 12`,
    );
    const apply = this.db.prepare(
      `update buffered_events set
         session_id = coalesce(@sessionId, session_id),
         model = coalesce(@model, model),
         cost_usd = coalesce(@costUsd, cost_usd),
         payload_json = json_set(payload_json,
           '$.sessionId', coalesce(@sessionId, json_extract(payload_json, '$.sessionId')),
           '$.model', coalesce(@model, json_extract(payload_json, '$.model')),
           '$.costUsd', coalesce(@costUsd, json_extract(payload_json, '$.costUsd')),
           '$.metadata.stitched', @stitched,
           '$.metadata.costEstimated', json(@costEstimated))
       where id = @id`,
    );

    let stitched = 0;
    let priced = 0;
    const run = this.db.transaction(() => {
      for (const row of candidates) {
        const neighbors = nearest.all(row.id, row.observedAt, row.observedAt) as Array<{
          sessionId: string | null;
          model: string | null;
        }>;
        const sessionId = row.sessionId ?? neighbors.find((n) => n.sessionId)?.sessionId ?? null;
        const model = row.model ?? neighbors.find((n) => n.model)?.model ?? null;
        let costUsd: number | null = row.costUsd;
        let costEstimated = false;
        if (costUsd === null && model) {
          const estimated = estimate({
            model,
            inputTokens: row.inputTokens ?? undefined,
            outputTokens: row.outputTokens ?? undefined,
            cacheReadTokens: row.cacheReadTokens ?? undefined,
          });
          if (estimated) {
            costUsd = estimated.costUsd;
            costEstimated = true;
          }
        }
        if (sessionId !== row.sessionId || model !== row.model || costUsd !== row.costUsd) {
          apply.run({
            id: row.id,
            sessionId,
            model,
            costUsd,
            stitched: sessionId !== row.sessionId ? "time_window" : null,
            costEstimated: costEstimated ? "true" : "false",
          });
          if (sessionId !== row.sessionId) stitched += 1;
          if (costEstimated) priced += 1;
        }
      }
    });
    run();
    return { examined: candidates.length, stitched, priced };
  }

  appendMetricSample(sample: MetricSample) {
    this.db
      .prepare(
        `insert or replace into metric_samples
          (id, source, metric_name, observed_at, session_id, model, sample_type, value, attrs_json, created_at)
        values
          (@id, @source, @metricName, @observedAt, @sessionId, @model, @sampleType, @value, @attrsJson, @createdAt)`,
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
  }): BufferedEventRow {
    return {
      id: row.id,
      source: row.source,
      eventType: row.eventType,
      dataMode: row.dataMode,
      observedAt: row.observedAt,
      payload: JSON.parse(row.payloadJson) as AiInteractionEvent,
      suppressedFields: JSON.parse(row.suppressedFieldsJson) as string[],
      createdAt: row.createdAt,
      uploadedAt: row.uploadedAt ?? null,
      repoHash: row.repoHash ?? null,
      branchHash: row.branchHash ?? null,
    };
  }

  list(limit = 100): BufferedEventRow[] {
    const rows = this.db
      .prepare(
        `select id, source, event_type as eventType, data_mode as dataMode,
          observed_at as observedAt, payload_json as payloadJson,
          suppressed_fields_json as suppressedFieldsJson, created_at as createdAt,
          uploaded_at as uploadedAt, repo_hash as repoHash, branch_hash as branchHash
        from buffered_events
        order by created_at desc
        limit ?`,
      )
      .all(limit) as Parameters<LocalEventBuffer["rowToBufferedEvent"]>[0][];

    return rows.map((row) => this.rowToBufferedEvent(row));
  }

  listUnuploaded(options: { maxRows?: number; maxBytes?: number } = {}): BufferedEventRow[] {
    const maxRows = Math.max(1, Math.min(options.maxRows ?? 500, 500));
    const maxBytes = options.maxBytes ?? 1_500_000;
    const rows = this.db
      .prepare(
        `select id, source, event_type as eventType, data_mode as dataMode,
          observed_at as observedAt, payload_json as payloadJson,
          suppressed_fields_json as suppressedFieldsJson, created_at as createdAt,
          uploaded_at as uploadedAt, repo_hash as repoHash, branch_hash as branchHash,
          length(payload_json) as payloadBytes
        from buffered_events
        where uploaded_at is null
        order by created_at asc
        limit ?`,
      )
      .all(maxRows) as Array<
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
    const mark = this.db.prepare(`update buffered_events set uploaded_at = ? where id = ?`);
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
    const events = this.db
      .prepare(`delete from buffered_events where created_at < ? and uploaded_at is not null`)
      .run(cutoff).changes;
    const metricSamples = this.db
      .prepare(`delete from metric_samples where created_at < ?`)
      .run(cutoff).changes;
    return { cutoff, events, metricSamples };
  }

  stats(): BufferStats {
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
        from buffered_events`,
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
        where observed_at >= ? and session_id is not null
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
