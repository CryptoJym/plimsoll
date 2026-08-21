import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import Database from "better-sqlite3";

import {
  outcomeTimelineBackfillStateSchema,
  outcomeTimelineCoverageSchema,
  pullTimelineFactSchema,
  type OutcomeTimelineBackfillState,
  type OutcomeTimelineCoverage,
  type PullTimelineFact,
} from "../../shared/src/index";
import {
  deriveOutcomePerformanceRecords,
  parseOutcomePerformanceRecord,
  summarizeOutcomePerformance,
  type OutcomePerformanceRecord,
} from "./performance-layer";

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, nested]) => [key, canonicalValue(nested)]),
  );
}

export function canonicalTimelineJson(value: unknown): string {
  return JSON.stringify(canonicalValue(value));
}

export class ImmutableTimelineConflictError extends Error {
  constructor(externalId: string) {
    super(`Immutable outcome fact conflict for external id ${externalId}`);
    this.name = "ImmutableTimelineConflictError";
  }
}

export type AppendFactResult = { inserted: number; duplicates: number };

/** Dedicated local outcome database. This is intentionally not the collector
 * buffer/telemetry ledger and has no upload path. */
export class OutcomeTimelineStore {
  readonly database: Database.Database;

  constructor(databasePath: string) {
    fs.mkdirSync(path.dirname(databasePath), { recursive: true });
    this.database = new Database(databasePath);
    this.database.pragma("journal_mode = WAL");
    this.database.exec(`
      create table if not exists outcome_timeline_facts (
        external_id text primary key,
        repository_external_id text not null,
        pull_external_id text not null,
        pull_number integer not null,
        kind text not null,
        canonical_json text not null,
        inserted_at text not null
      );
      create index if not exists outcome_timeline_facts_pull
        on outcome_timeline_facts(repository_external_id, pull_external_id, kind);

      create table if not exists outcome_timeline_backfill_state (
        repository_external_id text primary key,
        canonical_json text not null,
        updated_at text not null
      );

      create table if not exists outcome_timeline_coverage (
        id text primary key,
        run_id text not null,
        repository_external_id text not null,
        pull_external_id text,
        status text not null,
        dimension text not null,
        reason text not null,
        canonical_json text not null,
        recorded_at text not null
      );
      create index if not exists outcome_timeline_coverage_run
        on outcome_timeline_coverage(run_id, repository_external_id, pull_external_id);

      create table if not exists outcome_timeline_runs (
        run_id text primary key,
        canonical_json text not null,
        recorded_at text not null
      );

      -- Derived-only local read model. Immutable facts remain the source of
      -- truth; this table makes old facts usable by dashboard/report callers
      -- without any GitHub request or background work.
      create table if not exists outcome_timeline_performance (
        repository_external_id text not null,
        pull_external_id text not null,
        pull_number integer not null,
        occurred_at text,
        canonical_json text not null,
        materialized_at text not null,
        primary key (repository_external_id, pull_external_id)
      );
      create index if not exists outcome_timeline_performance_time
        on outcome_timeline_performance(repository_external_id, occurred_at, pull_number);
      create table if not exists outcome_timeline_performance_state (
        singleton integer primary key check (singleton = 1),
        generation integer not null
      );
      insert or ignore into outcome_timeline_performance_state (singleton, generation) values (1, 0);
    `);
  }

  private appendFactsInternal(facts: PullTimelineFact[], now: string): AppendFactResult {
    const select = this.database.prepare(
      "select canonical_json as canonicalJson from outcome_timeline_facts where external_id = ?",
    );
    const insert = this.database.prepare(`
      insert into outcome_timeline_facts (
        external_id, repository_external_id, pull_external_id, pull_number, kind, canonical_json, inserted_at
      ) values (?, ?, ?, ?, ?, ?, ?)
    `);
    let inserted = 0;
    let duplicates = 0;
    for (const candidate of [...facts].sort((a, b) => a.externalId.localeCompare(b.externalId))) {
      const fact = pullTimelineFactSchema.parse(candidate);
      const canonicalJson = canonicalTimelineJson(fact);
      const existing = select.get(fact.externalId) as { canonicalJson: string } | undefined;
      if (existing) {
        if (existing.canonicalJson !== canonicalJson) throw new ImmutableTimelineConflictError(fact.externalId);
        duplicates += 1;
        continue;
      }
      insert.run(
        fact.externalId,
        fact.repositoryExternalId,
        fact.pullExternalId,
        fact.pullNumber,
        fact.kind,
        canonicalJson,
        now,
      );
      inserted += 1;
    }
    return { inserted, duplicates };
  }

  appendFacts(facts: PullTimelineFact[], now = new Date().toISOString()): AppendFactResult {
    return this.database.transaction(() => this.appendFactsInternal(facts, now))();
  }

  private saveStateInternal(state: OutcomeTimelineBackfillState, now: string): void {
    const parsed = outcomeTimelineBackfillStateSchema.parse(state);
    this.database
      .prepare(`
        insert into outcome_timeline_backfill_state (repository_external_id, canonical_json, updated_at)
        values (?, ?, ?)
        on conflict(repository_external_id) do update set
          canonical_json = excluded.canonical_json,
          updated_at = excluded.updated_at
      `)
      .run(parsed.repositoryExternalId, canonicalTimelineJson(parsed), now);
  }

  saveState(state: OutcomeTimelineBackfillState, now = new Date().toISOString()): void {
    this.database.transaction(() => this.saveStateInternal(state, now))();
  }

  readState(repositoryExternalId: string): OutcomeTimelineBackfillState | null {
    const row = this.database
      .prepare("select canonical_json as canonicalJson from outcome_timeline_backfill_state where repository_external_id = ?")
      .get(repositoryExternalId) as { canonicalJson: string } | undefined;
    return row ? outcomeTimelineBackfillStateSchema.parse(JSON.parse(row.canonicalJson)) : null;
  }

  private recordCoverageInternal(rows: OutcomeTimelineCoverage[], now: string): void {
    const insert = this.database.prepare(`
      insert into outcome_timeline_coverage (
        id, run_id, repository_external_id, pull_external_id, status, dimension, reason, canonical_json, recorded_at
      ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
      on conflict(id) do update set canonical_json = excluded.canonical_json, recorded_at = excluded.recorded_at
    `);
    for (const candidate of rows) {
      const coverage = outcomeTimelineCoverageSchema.parse(candidate);
      const canonicalJson = canonicalTimelineJson(coverage);
      const id = crypto.createHash("sha256").update(canonicalJson).digest("hex");
      insert.run(
        id,
        coverage.runId,
        coverage.repositoryExternalId,
        coverage.pullExternalId ?? null,
        coverage.status,
        coverage.dimension,
        coverage.reason,
        canonicalJson,
        now,
      );
    }
  }

  recordCoverage(rows: OutcomeTimelineCoverage[], now = new Date().toISOString()): void {
    this.database.transaction(() => this.recordCoverageInternal(rows, now))();
  }

  commitPullCollection(input: {
    facts: PullTimelineFact[];
    coverage: OutcomeTimelineCoverage[];
    state: OutcomeTimelineBackfillState;
    now?: string;
  }): AppendFactResult {
    const now = input.now ?? new Date().toISOString();
    return this.database.transaction(() => {
      const result = this.appendFactsInternal(input.facts, now);
      this.recordCoverageInternal(input.coverage, now);
      this.saveStateInternal(input.state, now);
      return result;
    })();
  }

  recordRun(runId: string, receipt: Record<string, unknown>, now = new Date().toISOString()): void {
    const canonicalJson = canonicalTimelineJson(receipt);
    this.database
      .prepare(`
        insert into outcome_timeline_runs (run_id, canonical_json, recorded_at)
        values (?, ?, ?)
        on conflict(run_id) do update set canonical_json = excluded.canonical_json, recorded_at = excluded.recorded_at
      `)
      .run(runId, canonicalJson, now);
  }

  facts(repositoryExternalId?: string): PullTimelineFact[] {
    const rows = repositoryExternalId
      ? (this.database
          .prepare(
            "select canonical_json as canonicalJson from outcome_timeline_facts where repository_external_id = ? order by external_id",
          )
          .all(repositoryExternalId) as Array<{ canonicalJson: string }>)
      : (this.database
          .prepare("select canonical_json as canonicalJson from outcome_timeline_facts order by external_id")
          .all() as Array<{ canonicalJson: string }>);
    return rows.map((row) => pullTimelineFactSchema.parse(JSON.parse(row.canonicalJson)));
  }

  coverage(runId?: string): OutcomeTimelineCoverage[] {
    const rows = runId
      ? (this.database
          .prepare("select canonical_json as canonicalJson from outcome_timeline_coverage where run_id = ? order by id")
          .all(runId) as Array<{ canonicalJson: string }>)
      : (this.database
          .prepare("select canonical_json as canonicalJson from outcome_timeline_coverage order by id")
          .all() as Array<{ canonicalJson: string }>);
    return rows.map((row) => outcomeTimelineCoverageSchema.parse(JSON.parse(row.canonicalJson)));
  }

  /**
   * Deterministically materialize all persisted timeline facts. Replays only
   * update rows whose canonical derivation changed, so an already backfilled
   * store is a no-op. This never fetches or fabricates outcome evidence.
   */
  materializePerformance(input: {
    repositoryExternalId?: string;
    requiredChecks?: { names: string[] };
    reworkWindowDays?: number;
    now?: string;
  } = {}): { inserted: number; updated: number; unchanged: number; generation: number } {
    const now = input.now ?? new Date().toISOString();
    const facts = this.facts(input.repositoryExternalId);
    const coverage = this.coverage().filter(
      (row) => !input.repositoryExternalId || row.repositoryExternalId === input.repositoryExternalId,
    );
    const records = deriveOutcomePerformanceRecords({
      facts,
      coverage,
      requiredChecks: input.requiredChecks,
      reworkWindowDays: input.reworkWindowDays,
    });
    return this.database.transaction(() => {
      const existing = this.database.prepare(
        `select canonical_json as canonicalJson from outcome_timeline_performance
         where repository_external_id = ? and pull_external_id = ?`,
      );
      const insert = this.database.prepare(
        `insert into outcome_timeline_performance
         (repository_external_id, pull_external_id, pull_number, occurred_at, canonical_json, materialized_at)
         values (?, ?, ?, ?, ?, ?)`,
      );
      const update = this.database.prepare(
        `update outcome_timeline_performance set pull_number = ?, occurred_at = ?, canonical_json = ?, materialized_at = ?
         where repository_external_id = ? and pull_external_id = ?`,
      );
      let inserted = 0;
      let updated = 0;
      let unchanged = 0;
      for (const record of records) {
        const canonicalJson = canonicalTimelineJson(record);
        const prior = existing.get(record.repositoryExternalId, record.pullExternalId) as
          | { canonicalJson: string }
          | undefined;
        if (!prior) {
          insert.run(
            record.repositoryExternalId,
            record.pullExternalId,
            record.pullNumber,
            record.occurredAt,
            canonicalJson,
            now,
          );
          inserted += 1;
        } else if (prior.canonicalJson !== canonicalJson) {
          update.run(
            record.pullNumber,
            record.occurredAt,
            canonicalJson,
            now,
            record.repositoryExternalId,
            record.pullExternalId,
          );
          updated += 1;
        } else {
          unchanged += 1;
        }
      }
      if (inserted || updated) {
        this.database.prepare(
          `update outcome_timeline_performance_state set generation = generation + 1 where singleton = 1`,
        ).run();
      }
      const generation = (this.database.prepare(
        `select generation from outcome_timeline_performance_state where singleton = 1`,
      ).get() as { generation: number }).generation;
      return { inserted, updated, unchanged, generation };
    })();
  }

  performanceRecords(repositoryExternalId?: string): OutcomePerformanceRecord[] {
    const rows = repositoryExternalId
      ? (this.database.prepare(
          `select canonical_json as canonicalJson from outcome_timeline_performance
           where repository_external_id = ? order by occurred_at, pull_number, pull_external_id`,
        ).all(repositoryExternalId) as Array<{ canonicalJson: string }>)
      : (this.database.prepare(
          `select canonical_json as canonicalJson from outcome_timeline_performance
           order by occurred_at, pull_number, pull_external_id`,
        ).all() as Array<{ canonicalJson: string }>);
    return rows.map((row) => parseOutcomePerformanceRecord(JSON.parse(row.canonicalJson)));
  }

  performanceGeneration(): number {
    return (this.database.prepare(
      `select generation from outcome_timeline_performance_state where singleton = 1`,
    ).get() as { generation: number }).generation;
  }

  performanceSummary(days: number, asOf?: string) {
    return summarizeOutcomePerformance({
      records: this.performanceRecords(),
      days,
      generation: this.performanceGeneration(),
      asOf,
    });
  }

  close(): void {
    this.database.close();
  }
}
