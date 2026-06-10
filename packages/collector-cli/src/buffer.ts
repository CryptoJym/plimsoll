import os from "node:os";

import Database from "better-sqlite3";

import type { AiInteractionEvent } from "../../shared/src/index";
import type { MetricSample } from "./otlp";

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

  constructor(path: string) {
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
    `);
    this.migrateEventColumns();
    this.db.exec(`
      create index if not exists idx_events_upload on buffered_events (uploaded_at, created_at);
      create index if not exists idx_events_session on buffered_events (session_id, observed_at);
      create index if not exists idx_events_observed on buffered_events (observed_at);
      create index if not exists idx_events_repo on buffered_events (repo_hash, branch_hash);
      create index if not exists idx_events_account on buffered_events (account_hash, observed_at);
      create index if not exists idx_metrics_name on metric_samples (metric_name, observed_at);
      create index if not exists idx_metrics_session on metric_samples (session_id);
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
  }

  append(event: AiInteractionEvent, suppressedFields: string[] = []) {
    this.db
      .prepare(
        `insert or replace into buffered_events
          (id, source, event_type, data_mode, observed_at, payload_json, suppressed_fields_json,
           created_at, session_id, action_class, model, input_tokens, output_tokens,
           cache_read_tokens, cost_usd, uploaded_at, repo_hash, branch_hash, head_sha,
           machine, account_hash)
        values
          (@id, @source, @eventType, @dataMode, @observedAt, @payloadJson, @suppressedFieldsJson,
           @createdAt, @sessionId, @actionClass, @model, @inputTokens, @outputTokens,
           @cacheReadTokens, @costUsd, null, @repoHash, @branchHash, @headSha,
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
        createdAt: new Date().toISOString(),
        sessionId: event.sessionId ?? null,
        actionClass: event.actionClass ?? null,
        model: event.model ?? null,
        inputTokens: event.inputTokens ?? null,
        outputTokens: event.outputTokens ?? null,
        cacheReadTokens: event.cacheReadTokens ?? null,
        costUsd: event.costUsd ?? null,
        repoHash: gitField(event, "remoteUrlHash"),
        branchHash: gitField(event, "branchHash"),
        headSha: gitField(event, "headSha"),
        machine: MACHINE,
        accountHash: event.actorId ?? null,
      });
    if (event.actorId) this.seedAccountLabel(event.actorId);
  }

  appendMany(
    entries: Array<{ event: AiInteractionEvent; suppressedFields: string[] }>,
    metricSamples: MetricSample[] = [],
  ) {
    const run = this.db.transaction(() => {
      for (const entry of entries) {
        this.append(entry.event, entry.suppressedFields);
      }
      for (const sample of metricSamples) {
        this.appendMetricSample(sample);
      }
    });
    run();
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
    };
  }

  list(limit = 100): BufferedEventRow[] {
    const rows = this.db
      .prepare(
        `select id, source, event_type as eventType, data_mode as dataMode,
          observed_at as observedAt, payload_json as payloadJson,
          suppressed_fields_json as suppressedFieldsJson, created_at as createdAt,
          uploaded_at as uploadedAt
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
          uploaded_at as uploadedAt, length(payload_json) as payloadBytes
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
    const events = this.db
      .prepare(`delete from buffered_events where created_at < ?`)
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
