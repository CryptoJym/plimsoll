/**
 * Local Capacity Rail store (issue #170).
 *
 * A self-view of provider capacity that lives in the ordinary local ledger,
 * is coherent, strictly bounded, and independent of interaction telemetry:
 *
 * - Separate tables: account/profile registry, exact-change snapshots,
 *   15-minute window history, self-only daily summaries, the latest compact
 *   projection, retention policies, and probe state.
 * - Manual refresh only. Nothing here probes a provider on a cadence; the
 *   owner applies a bounded provider-facts document by hand until probe cost
 *   is measured. There is no network code in this module.
 * - Retention (#166 is the canonical maintenance owner; these are the
 *   capacity-table policies it owns): exact changes 7 days, 15-minute history
 *   30 days, self-only daily summaries 90 days, latest until profile removal.
 * - Provider failure preserves the last coherent state: validation and
 *   transaction errors roll back completely, record an honest FAILED probe
 *   receipt, and never touch the projection.
 * - Freshness fails closed: future-dated evidence is UNKNOWN (never fresh,
 *   never clamped), stale evidence stays visible as STALE, and headroom
 *   exists only on fresh, complete evidence — UNKNOWN is never a zero.
 *
 * Doctrine note: this file deliberately shares no symbols with
 * packages/shared/src/capacity.ts so each module remains independently
 * scannable by scripts/capacity-dependency-reachability.ts.
 */

import fs from "node:fs";
import path from "node:path";

import type Database from "better-sqlite3";

export const CAPACITY_RAIL_SCHEMA = "plimsoll.capacity-rail.v1" as const;
export const CAPACITY_RAIL_PROVIDER_FACTS_SCHEMA =
  "plimsoll.capacity-provider-facts.v1" as const;

/** Retention policies owned by the bounded-maintenance lane (#166). */
export const CAPACITY_RAIL_RETENTION_POLICIES = {
  exactChangesDays: 7,
  windowHistoryDays: 30,
  dailySummariesDays: 90,
} as const;

export const CAPACITY_RAIL_WINDOW_MINUTES = 15 as const;
const WINDOW_BUCKET_MS = CAPACITY_RAIL_WINDOW_MINUTES * 60 * 1000;

/** Fresh-evidence horizon for headroom math; older evidence stays STALE. */
export const CAPACITY_RAIL_FRESHNESS_MAX_AGE_MS = 6 * 60 * 60 * 1000;

/** Hard bounds. The rail is compact by construction, not by hope. */
export const CAPACITY_RAIL_MAX_PROFILES = 32 as const;
export const CAPACITY_RAIL_MAX_INGEST_OBSERVATIONS = 512 as const;
export const CAPACITY_RAIL_SURFACE_MAX_ROWS = 64 as const;
export const CAPACITY_RAIL_IDENTITY_MAX_LENGTH = 128 as const;
// Same bounded plain-identity class as the planning module: labels like
// "anthropic.max.owner" or "five_hour_window". Free text (spaces, leading
// punctuation, non-ascii, >128 chars) can never become an identity.
const CAPACITY_RAIL_IDENTITY_REGEX =
  /^[A-Za-z0-9](?:[A-Za-z0-9._:@/-]{0,126}[A-Za-z0-9])?$/;
const CAPACITY_RAIL_ERROR_MESSAGE_MAX_LENGTH = 200;

export const CAPACITY_RAIL_UNITS = ["tokens", "requests", "usd", "percent"] as const;
export type CapacityRailUnit = (typeof CAPACITY_RAIL_UNITS)[number];

export const CAPACITY_RAIL_SOURCES = ["provider_report", "local_telemetry"] as const;
export type CapacityRailSource = (typeof CAPACITY_RAIL_SOURCES)[number];

export type CapacityRailFreshness = "fresh" | "STALE" | "UNKNOWN";

export type CapacityRailProviderFacts = {
  schema: typeof CAPACITY_RAIL_PROVIDER_FACTS_SCHEMA;
  capturedAt: string;
  profiles: Array<{ profileId: string; provider: string }>;
  observations: Array<{
    profileId: string;
    dimension: string;
    unit: CapacityRailUnit;
    limit: number | null;
    used: number | null;
    source: CapacityRailSource;
    observedAt: string;
  }>;
};

export type CapacityRailConstraint = {
  profileId: string;
  dimension: string;
  unit: CapacityRailUnit;
  source: CapacityRailSource;
  freshness: CapacityRailFreshness;
  ageMs: number | null;
  observedAt: string;
  limit: number | null;
  used: number | null;
  remaining: number | null;
  remainingFraction: number | null;
};

export type CapacityRailStatus = {
  schema: typeof CAPACITY_RAIL_SCHEMA;
  generatedAt: string;
  manualRefreshOnly: true;
  freshnessWindowMs: number;
  profiles: Array<{ profileId: string; provider: string; registeredAt: string }>;
  constraints: CapacityRailConstraint[];
  constraintsTruncated: boolean;
  storedCounts: {
    exactChanges: number;
    windowBuckets: number;
    dailySummaries: number;
  };
  probe: {
    state: "NEVER_RUN" | "OK" | "FAILED";
    lastAttemptAt: string | null;
    lastSuccessAt: string | null;
    consecutiveFailures: number;
    lastErrorKind: string | null;
    lastErrorMessage: string | null;
  };
  retention: {
    exactChangesDays: number;
    windowHistoryDays: number;
    dailySummariesDays: number;
    latestUntilProfileRemoval: true;
    lastSweptAt: string | null;
  };
  unknownLegend: string;
};

export type CapacityRailIngestReceipt = {
  schema: typeof CAPACITY_RAIL_PROVIDER_FACTS_SCHEMA;
  accepted: boolean;
  appliedAt: string;
  profilesRegistered: number;
  snapshotsRecorded: number;
  windowBucketsTouched: number;
  dailySummariesTouched: number;
  latestRowsReplaced: number;
  retentionDeleted: {
    exactChanges: number;
    windowBuckets: number;
    dailySummaries: number;
  };
};

function railNow(): string {
  return new Date().toISOString();
}

function requireRailIdentity(field: string, value: unknown): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > CAPACITY_RAIL_IDENTITY_MAX_LENGTH ||
    !CAPACITY_RAIL_IDENTITY_REGEX.test(value)
  ) {
    throw Object.assign(
      new Error(`capacity rail rejects ${field}: expected a bounded plain identity`),
      { errorKind: "invalid_identity" },
    );
  }
  return value;
}

function requireRailIso(field: string, value: unknown): string {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value))) {
    throw Object.assign(
      new Error(`capacity rail rejects ${field}: expected an ISO timestamp`),
      { errorKind: "invalid_timestamp" },
    );
  }
  return value;
}

function optionalRailNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw Object.assign(
      new Error("capacity rail rejects non-null measurements outside finite >= 0"),
      { errorKind: "invalid_measurement" },
    );
  }
  return value;
}

function railError(kind: string, message: string): Error {
  return Object.assign(new Error(message), { errorKind: kind });
}

function sanitizeRailErrorDetail(message: unknown): string {
  const text = String(message ?? "").replace(/[\r\n\t]+/g, " ");
  return text.slice(0, CAPACITY_RAIL_ERROR_MESSAGE_MAX_LENGTH);
}

/** Strictly-older-than cutoff deletion; rows exactly at the boundary stay. */
function railCutoffIso(nowIso: string, days: number): string {
  return new Date(Date.parse(nowIso) - days * 24 * 60 * 60 * 1000).toISOString();
}

function railBucketStart(observedAtIso: string): string {
  const ms = Date.parse(observedAtIso);
  return new Date(Math.floor(ms / WINDOW_BUCKET_MS) * WINDOW_BUCKET_MS).toISOString();
}

type RailProbeRow = {
  state: "NEVER_RUN" | "OK" | "FAILED";
  lastAttemptAt: string | null;
  lastSuccessAt: string | null;
  consecutiveFailures: number;
  lastErrorKind: string | null;
  lastErrorMessage: string | null;
};

export class CapacityRailStore {
  private readonly statements: {
    activeProfiles: Database.Statement;
    allProfiles: Database.Statement;
    latestRows: Database.Statement;
    probe: Database.Statement;
    retentionKey: Database.Statement;
    countExact: Database.Statement;
    countWindows: Database.Statement;
    countDaily: Database.Statement;
  };
  private readonly surfacePath: string | null;
  private readonly clock: () => string;

  constructor(
    private readonly database: Database.Database,
    options: { surfacePath?: string | null; now?: () => string } = {},
  ) {
    this.database.exec(`
      create table if not exists capacity_rail_profiles (
        profile_id text primary key,
        provider text not null,
        registered_at text not null,
        removed_at text
      ) without rowid;
      create table if not exists capacity_rail_snapshots (
        snapshot_id integer primary key autoincrement,
        profile_id text not null references capacity_rail_profiles(profile_id),
        dimension text not null,
        unit text not null,
        limit_value real check (limit_value is null or limit_value >= 0),
        used_value real check (used_value is null or used_value >= 0),
        source text not null,
        observed_at text not null,
        captured_at text not null
      );
      create index if not exists idx_capacity_rail_snapshots_ttl
        on capacity_rail_snapshots (captured_at);
      create table if not exists capacity_rail_window_history (
        bucket_start text not null,
        profile_id text not null references capacity_rail_profiles(profile_id),
        dimension text not null,
        unit text not null,
        sample_count integer not null check (sample_count > 0),
        min_used real check (min_used is null or min_used >= 0),
        max_used real check (max_used is null or max_used >= 0),
        last_used real check (last_used is null or last_used >= 0),
        last_limit real check (last_limit is null or last_limit >= 0),
        last_observed_at text not null,
        primary key (bucket_start, profile_id, dimension)
      ) without rowid;
      create index if not exists idx_capacity_rail_windows_ttl
        on capacity_rail_window_history (bucket_start);
      create table if not exists capacity_rail_daily_summaries (
        day text not null,
        profile_id text not null references capacity_rail_profiles(profile_id),
        dimension text not null,
        unit text not null,
        sample_count integer not null check (sample_count > 0),
        max_used real check (max_used is null or max_used >= 0),
        last_used real check (last_used is null or last_used >= 0),
        last_limit real check (last_limit is null or last_limit >= 0),
        last_observed_at text not null,
        visibility text not null default 'self_only' check (visibility = 'self_only'),
        primary key (day, profile_id, dimension)
      ) without rowid;
      create index if not exists idx_capacity_rail_daily_ttl
        on capacity_rail_daily_summaries (day);
      create table if not exists capacity_rail_latest (
        profile_id text not null references capacity_rail_profiles(profile_id),
        dimension text not null,
        unit text not null,
        limit_value real check (limit_value is null or limit_value >= 0),
        used_value real check (used_value is null or used_value >= 0),
        source text not null,
        observed_at text not null,
        updated_at text not null,
        primary key (profile_id, dimension)
      ) without rowid;
      create table if not exists capacity_rail_retention (
        policy_key text primary key,
        policy_value text not null,
        updated_at text not null
      ) without rowid;
      create table if not exists capacity_rail_probe_state (
        probe_key text primary key check (probe_key = 'manual_refresh'),
        state text not null check (state in ('NEVER_RUN', 'OK', 'FAILED')),
        last_attempt_at text,
        last_success_at text,
        consecutive_failures integer not null default 0 check (consecutive_failures >= 0),
        last_error_kind text,
        last_error_message text
      ) without rowid;
    `);
    this.surfacePath = options.surfacePath ?? null;
    this.clock = options.now ?? railNow;
    this.statements = {
      activeProfiles: this.database.prepare(
        `select profile_id as profileId, provider, registered_at as registeredAt
         from capacity_rail_profiles where removed_at is null order by profile_id`,
      ),
      allProfiles: this.database.prepare(
        `select count(*) as n from capacity_rail_profiles`,
      ),
      latestRows: this.database.prepare(
        `select l.profile_id as profileId, l.dimension, l.unit, l.limit_value as limitValue,
                l.used_value as usedValue, l.source, l.observed_at as observedAt
         from capacity_rail_latest l
         join capacity_rail_profiles p on p.profile_id = l.profile_id
         where p.removed_at is null
         order by l.profile_id, l.dimension`,
      ),
      probe: this.database.prepare(
        `select state, last_attempt_at as lastAttemptAt, last_success_at as lastSuccessAt,
                consecutive_failures as consecutiveFailures, last_error_kind as lastErrorKind,
                last_error_message as lastErrorMessage
         from capacity_rail_probe_state where probe_key = 'manual_refresh'`,
      ),
      retentionKey: this.database.prepare(
        `select policy_value as value from capacity_rail_retention where policy_key = ?`,
      ),
      countExact: this.database.prepare(
        `select count(*) as n from capacity_rail_snapshots`,
      ),
      countWindows: this.database.prepare(
        `select count(*) as n from capacity_rail_window_history`,
      ),
      countDaily: this.database.prepare(
        `select count(*) as n from capacity_rail_daily_summaries`,
      ),
    };
    this.seedRetentionPolicies();
    // Bounded-without-cadence: the TTL sweep runs opportunistically on open
    // and after every successful refresh, so no background timer is needed
    // while the tables still cannot grow without bound.
    this.runRetention();
  }

  private seedRetentionPolicies(): void {
    const stamped = this.clock();
    const seed = this.database.prepare(
      `insert into capacity_rail_retention (policy_key, policy_value, updated_at)
       values (?, ?, ?)
       on conflict(policy_key) do nothing`,
    );
    seed.run("exact_changes_days", String(CAPACITY_RAIL_RETENTION_POLICIES.exactChangesDays), stamped);
    seed.run("window_history_days", String(CAPACITY_RAIL_RETENTION_POLICIES.windowHistoryDays), stamped);
    seed.run("daily_summaries_days", String(CAPACITY_RAIL_RETENTION_POLICIES.dailySummariesDays), stamped);
    seed.run("latest_until_profile_removal", "true", stamped);
    seed.run("refresh_mode", "manual_only_no_background_cadence", stamped);
  }

  /**
   * The compact projection. Reads ONLY the rail's own tables — never raw
   * interaction history or any other ledger surface — so a dashboard request
   * can never scan events or touch a provider.
   */
  railProjection(nowOverride?: string): CapacityRailStatus {
    const now = nowOverride ?? this.clock();
    const nowMs = Date.parse(requireRailIso("now", now));
    const constraints: CapacityRailConstraint[] = [];
    const latestRows = this.statements.latestRows.all() as Array<{
      profileId: string;
      dimension: string;
      unit: CapacityRailUnit;
      limitValue: number | null;
      usedValue: number | null;
      source: CapacityRailSource;
      observedAt: string;
    }>;
    for (const row of latestRows) {
      const observedMs = Date.parse(row.observedAt);
      let freshness: CapacityRailFreshness;
      let ageMs: number | null;
      if (!Number.isFinite(observedMs) || observedMs > nowMs) {
        freshness = "UNKNOWN";
        ageMs = null;
      } else {
        ageMs = nowMs - observedMs;
        freshness = ageMs <= CAPACITY_RAIL_FRESHNESS_MAX_AGE_MS ? "fresh" : "STALE";
      }
      const usable =
        freshness === "fresh" && row.limitValue !== null && row.usedValue !== null;
      const remaining = usable ? Math.max(0, row.limitValue! - row.usedValue!) : null;
      const remainingFraction =
        usable && row.limitValue! > 0
          ? Math.max(0, Math.min(1, remaining! / row.limitValue!))
          : null;
      constraints.push({
        profileId: row.profileId,
        dimension: row.dimension,
        unit: row.unit,
        source: row.source,
        freshness,
        ageMs,
        observedAt: row.observedAt,
        limit: row.limitValue,
        used: row.usedValue,
        remaining,
        remainingFraction,
      });
    }
    constraints.sort(
      (left, right) =>
        left.profileId.localeCompare(right.profileId) ||
        left.dimension.localeCompare(right.dimension),
    );
    const truncated = constraints.length > CAPACITY_RAIL_SURFACE_MAX_ROWS;
    const probeRow = this.statements.probe.get() as RailProbeRow | undefined;
    const swept = this.statements.retentionKey.get("last_sweep_at") as
      | { value: string }
      | undefined;
    return {
      schema: CAPACITY_RAIL_SCHEMA,
      generatedAt: now,
      manualRefreshOnly: true,
      freshnessWindowMs: CAPACITY_RAIL_FRESHNESS_MAX_AGE_MS,
      profiles: this.statements.activeProfiles.all() as Array<{
        profileId: string;
        provider: string;
        registeredAt: string;
      }>,
      constraints: truncated
        ? constraints.slice(0, CAPACITY_RAIL_SURFACE_MAX_ROWS)
        : constraints,
      constraintsTruncated: truncated,
      storedCounts: {
        exactChanges: (this.statements.countExact.get() as { n: number }).n,
        windowBuckets: (this.statements.countWindows.get() as { n: number }).n,
        dailySummaries: (this.statements.countDaily.get() as { n: number }).n,
      },
      probe: probeRow
        ? {
            state: probeRow.state,
            lastAttemptAt: probeRow.lastAttemptAt,
            lastSuccessAt: probeRow.lastSuccessAt,
            consecutiveFailures: probeRow.consecutiveFailures,
            lastErrorKind: probeRow.lastErrorKind,
            lastErrorMessage: probeRow.lastErrorMessage,
          }
        : {
            state: "NEVER_RUN",
            lastAttemptAt: null,
            lastSuccessAt: null,
            consecutiveFailures: 0,
            lastErrorKind: null,
            lastErrorMessage: null,
          },
      retention: {
        ...CAPACITY_RAIL_RETENTION_POLICIES,
        latestUntilProfileRemoval: true,
        lastSweptAt: swept?.value ?? null,
      },
      unknownLegend:
        "UNKNOWN means evidence is missing, stale, or future-dated; it is never a zero. " +
        "Headroom exists only where fresh evidence supports both sides.",
    };
  }

  /**
   * Manual refresh: validate the whole provider-facts document first, then
   * apply everything in ONE transaction. Any rejection records an honest
   * FAILED probe receipt and rethrows with the previous state untouched.
   */
  applyProviderFacts(input: unknown): CapacityRailIngestReceipt {
    const now = this.clock();
    let facts: CapacityRailProviderFacts;
    try {
      facts = parseRailProviderFacts(input);
      this.assertIngestBounds(facts);
    } catch (error) {
      this.recordRefreshFailure(
        (error as { errorKind?: string }).errorKind ?? "invalid_document",
        error,
        now,
      );
      throw error;
    }
    try {
      const receipt = this.transaction(facts, now);
      this.markProbeOk(now);
      const { sweptAt: _sweptAt, ...deleted } = this.runRetention(now);
      return {
        schema: CAPACITY_RAIL_PROVIDER_FACTS_SCHEMA,
        accepted: true,
        ...receipt,
        retentionDeleted: deleted,
      };
    } catch (error) {
      this.recordRefreshFailure("apply_failed", error, now);
      throw error;
    }
  }

  private assertIngestBounds(facts: CapacityRailProviderFacts): void {
    const hasProfile = this.database.prepare(
      `select 1 from capacity_rail_profiles where profile_id = ?`,
    );
    const existing = (this.statements.allProfiles.get() as { n: number }).n;
    const fresh = facts.profiles.filter((profile) => !hasProfile.get(profile.profileId)).length;
    if (existing + fresh > CAPACITY_RAIL_MAX_PROFILES) {
      throw railError(
        "profile_bound_exceeded",
        `capacity rail caps registries at ${CAPACITY_RAIL_MAX_PROFILES} profiles`,
      );
    }
  }

  private transaction(
    facts: CapacityRailProviderFacts,
    now: string,
  ): Omit<CapacityRailIngestReceipt, "accepted" | "schema" | "retentionDeleted"> {
    const registerProfile = this.database.prepare(
      `insert into capacity_rail_profiles (profile_id, provider, registered_at)
       values (?, ?, ?)
       on conflict(profile_id) do update set removed_at = null`,
    );
    const insertSnapshot = this.database.prepare(
      `insert into capacity_rail_snapshots
         (profile_id, dimension, unit, limit_value, used_value, source, observed_at, captured_at)
       values (?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    const upsertWindow = this.database.prepare(
      `insert into capacity_rail_window_history
         (bucket_start, profile_id, dimension, unit, sample_count,
          min_used, max_used, last_used, last_limit, last_observed_at)
       values (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
       on conflict(bucket_start, profile_id, dimension) do update set
         sample_count = sample_count + 1,
         min_used = min(coalesce(min_used, excluded.min_used), coalesce(excluded.min_used, min_used)),
         max_used = max(coalesce(max_used, excluded.max_used), coalesce(excluded.max_used, max_used)),
         last_used = case when excluded.last_observed_at >= last_observed_at
                          then excluded.last_used else last_used end,
         last_limit = case when excluded.last_observed_at >= last_observed_at
                           then excluded.last_limit else last_limit end,
         last_observed_at = case when excluded.last_observed_at >= last_observed_at
                                 then excluded.last_observed_at else last_observed_at end`,
    );
    const upsertDaily = this.database.prepare(
      `insert into capacity_rail_daily_summaries
         (day, profile_id, dimension, unit, sample_count, max_used, last_used, last_limit,
          last_observed_at)
       values (?, ?, ?, ?, 1, ?, ?, ?, ?)
       on conflict(day, profile_id, dimension) do update set
         sample_count = sample_count + 1,
         max_used = max(coalesce(max_used, excluded.max_used), coalesce(excluded.max_used, max_used)),
         last_used = case when excluded.last_observed_at >= last_observed_at
                          then excluded.last_used else last_used end,
         last_limit = case when excluded.last_observed_at >= last_observed_at
                           then excluded.last_limit else last_limit end,
         last_observed_at = case when excluded.last_observed_at >= last_observed_at
                                 then excluded.last_observed_at else last_observed_at end`,
    );
    const replaceLatest = this.database.prepare(
      `insert into capacity_rail_latest
         (profile_id, dimension, unit, limit_value, used_value, source, observed_at, updated_at)
       values (?, ?, ?, ?, ?, ?, ?, ?)
       on conflict(profile_id, dimension) do update set
         unit = excluded.unit, limit_value = excluded.limit_value,
         used_value = excluded.used_value, source = excluded.source,
         observed_at = excluded.observed_at, updated_at = excluded.updated_at`,
    );
    // A provider may replay the same observation when an owner refreshes a
    // cached report. The observation identity is its profile, dimension, and
    // observed timestamp; replaying it must not grow the exact-change log or
    // inflate the window/daily sample counts.
    const existingSnapshot = this.database.prepare(
      `select 1 from capacity_rail_snapshots
       where profile_id = ? and dimension = ? and observed_at = ? limit 1`,
    );
    const ordered = [...facts.observations].sort((left, right) =>
      left.observedAt.localeCompare(right.observedAt),
    );
    const apply = this.database.transaction(() => {
      for (const profile of facts.profiles) {
        registerProfile.run(profile.profileId, profile.provider, now);
      }
      const touchedWindows = new Set<string>();
      const touchedDays = new Set<string>();
      const appliedObservations: typeof ordered = [];
      for (const observation of ordered) {
        if (
          existingSnapshot.get(
            observation.profileId,
            observation.dimension,
            observation.observedAt,
          )
        ) {
          continue;
        }
        insertSnapshot.run(
          observation.profileId,
          observation.dimension,
          observation.unit,
          observation.limit,
          observation.used,
          observation.source,
          observation.observedAt,
          now,
        );
        appliedObservations.push(observation);
        const bucketStart = railBucketStart(observation.observedAt);
        upsertWindow.run(
          bucketStart,
          observation.profileId,
          observation.dimension,
          observation.unit,
          observation.used,
          observation.used,
          observation.used,
          observation.limit,
          observation.observedAt,
        );
        touchedWindows.add(`${bucketStart}\u0000${observation.profileId}\u0000${observation.dimension}`);
        const day = observation.observedAt.slice(0, 10);
        upsertDaily.run(
          day,
          observation.profileId,
          observation.dimension,
          observation.unit,
          observation.used,
          observation.used,
          observation.limit,
          observation.observedAt,
        );
        touchedDays.add(day);
        replaceLatest.run(
          observation.profileId,
          observation.dimension,
          observation.unit,
          observation.limit,
          observation.used,
          observation.source,
          observation.observedAt,
          now,
        );
      }
      return {
        appliedAt: now,
        profilesRegistered: facts.profiles.length,
        snapshotsRecorded: appliedObservations.length,
        windowBucketsTouched: touchedWindows.size,
        dailySummariesTouched: touchedDays.size,
        latestRowsReplaced: new Set(
          appliedObservations.map((observation) => `${observation.profileId}\u0000${observation.dimension}`),
        ).size,
      };
    });
    return apply();
  }

  /** Honest failure bookkeeping. Touches ONLY the probe-state table. */
  recordRefreshFailure(errorKind: string, error: unknown, nowOverride?: string): void {
    const now = nowOverride ?? this.clock();
    this.database
      .prepare(
        `insert into capacity_rail_probe_state
           (probe_key, state, last_attempt_at, last_success_at, consecutive_failures,
            last_error_kind, last_error_message)
         values ('manual_refresh', 'FAILED', ?, null, 1, ?, ?)
         on conflict(probe_key) do update set
           state = 'FAILED',
           last_attempt_at = excluded.last_attempt_at,
           consecutive_failures = consecutive_failures + 1,
           last_error_kind = excluded.last_error_kind,
           last_error_message = excluded.last_error_message`,
      )
      .run(now, sanitizeRailErrorKind(errorKind), sanitizeRailErrorDetail(
        error instanceof Error ? error.message : error,
      ));
  }

  private markProbeOk(now: string): void {
    this.database
      .prepare(
        `insert into capacity_rail_probe_state
           (probe_key, state, last_attempt_at, last_success_at, consecutive_failures,
            last_error_kind, last_error_message)
         values ('manual_refresh', 'OK', ?, ?, 0, null, null)
         on conflict(probe_key) do update set
           state = 'OK',
           last_attempt_at = excluded.last_attempt_at,
           last_success_at = excluded.last_success_at,
           consecutive_failures = 0,
           last_error_kind = null,
           last_error_message = null`,
      )
      .run(now, now);
  }

  /** Profile removal exit path: latest dies immediately; history ages out. */
  forgetProfile(profileIdRaw: unknown, nowOverride?: string): { removed: boolean } {
    const profileId = requireRailIdentity("profileId", profileIdRaw);
    const now = nowOverride ?? this.clock();
    const claimed = this.database.transaction(() => {
      const claim = this.database
        .prepare(
          `update capacity_rail_profiles set removed_at = ?
           where profile_id = ? and removed_at is null`,
        )
        .run(now, profileId);
      if (claim.changes === 0) return false;
      this.database
        .prepare(`delete from capacity_rail_latest where profile_id = ?`)
        .run(profileId);
      return true;
    });
    return { removed: claimed() };
  }

  /**
   * TTL sweep over exactly the three bounded-history tables. Latest rows are
   * NEVER swept here — they leave only via explicit profile removal.
   */
  runRetention(nowOverride?: string): {
    exactChanges: number;
    windowBuckets: number;
    dailySummaries: number;
    sweptAt: string;
  } {
    const now = nowOverride ?? this.clock();
    const exactCutoff = railCutoffIso(now, CAPACITY_RAIL_RETENTION_POLICIES.exactChangesDays);
    const windowCutoff = railCutoffIso(now, CAPACITY_RAIL_RETENTION_POLICIES.windowHistoryDays);
    // Daily rows are keyed by calendar day; the cutoff is date-granular so a
    // row aged exactly policy-days keeps its full final day before dying.
    const dailyCutoff = railCutoffIso(now, CAPACITY_RAIL_RETENTION_POLICIES.dailySummariesDays).slice(0, 10);
    const sweep = this.database.transaction(() => {
      const exact = this.database
        .prepare(`delete from capacity_rail_snapshots where captured_at < ?`)
        .run(exactCutoff).changes;
      const windows = this.database
        .prepare(`delete from capacity_rail_window_history where bucket_start < ?`)
        .run(windowCutoff).changes;
      const daily = this.database
        .prepare(`delete from capacity_rail_daily_summaries where day < ?`)
        .run(dailyCutoff).changes;
      this.database
        .prepare(
          `insert into capacity_rail_retention (policy_key, policy_value, updated_at)
           values ('last_sweep_at', ?, ?)
           on conflict(policy_key) do update set
             policy_value = excluded.policy_value, updated_at = excluded.updated_at`,
        )
        .run(now, now);
      return { exactChanges: exact, windowBuckets: windows, dailySummaries: daily };
    });
    return { ...sweep(), sweptAt: now };
  }

  /**
   * Atomic cached status surface for SQLite-free readers (doctor). Best
   * effort: a failed cache write never fails the caller's actual operation.
   */
  writeStatusSurface(statusOverride?: CapacityRailStatus): string | null {
    if (!this.surfacePath) return null;
    try {
      const payload = statusOverride ?? this.railProjection();
      const directory = path.dirname(this.surfacePath);
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      const temporary = `${this.surfacePath}.tmp-${process.pid}`;
      fs.writeFileSync(temporary, `${JSON.stringify(payload, null, 2)}\n`, { mode: 0o600 });
      fs.renameSync(temporary, this.surfacePath);
      return this.surfacePath;
    } catch {
      return null;
    }
  }
}

function sanitizeRailErrorKind(kind: string): string {
  return kind.replace(/[^a-z0-9_.-]/gi, "_").slice(0, 64);
}

/**
 * Fail-closed parser for operator-supplied provider facts. Exact key sets:
 * unknown fields are rejected rather than ignored, so a document can never
 * smuggle free text (prompt bodies, paths, credentials) into the rail.
 */
export function parseRailProviderFacts(input: unknown): CapacityRailProviderFacts {
  if (typeof input === "string") {
    try {
      input = JSON.parse(input);
    } catch {
      throw railError("malformed_json", "capacity rail provider facts are not valid JSON");
    }
  }
  if (input === null || typeof input !== "object" || Array.isArray(input)) {
    throw railError("invalid_document", "capacity rail provider facts must be a JSON object");
  }
  const document = input as Record<string, unknown>;
  for (const key of Object.keys(document)) {
    if (!["schema", "capturedAt", "profiles", "observations"].includes(key)) {
      throw railError("unknown_field", `capacity rail rejects unknown field ${JSON.stringify(key)}`);
    }
  }
  if (document.schema !== CAPACITY_RAIL_PROVIDER_FACTS_SCHEMA) {
    throw railError(
      "schema_mismatch",
      `capacity rail expects schema ${CAPACITY_RAIL_PROVIDER_FACTS_SCHEMA}`,
    );
  }
  const capturedAt = requireRailIso("capturedAt", document.capturedAt);
  if (!Array.isArray(document.profiles) || !Array.isArray(document.observations)) {
    throw railError(
      "invalid_document",
      "capacity rail provider facts need profiles[] and observations[]",
    );
  }
  if (document.profiles.length > CAPACITY_RAIL_MAX_PROFILES) {
    throw railError(
      "profile_bound_exceeded",
      `capacity rail accepts at most ${CAPACITY_RAIL_MAX_PROFILES} profiles per document`,
    );
  }
  if (document.observations.length > CAPACITY_RAIL_MAX_INGEST_OBSERVATIONS) {
    throw railError(
      "observation_bound_exceeded",
      `capacity rail accepts at most ${CAPACITY_RAIL_MAX_INGEST_OBSERVATIONS} observations per document`,
    );
  }
  const profiles: CapacityRailProviderFacts["profiles"] = [];
  const seenProfiles = new Set<string>();
  for (const entry of document.profiles) {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) {
      throw railError("invalid_document", "each capacity profile must be an object");
    }
    const record = entry as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      if (!["profileId", "provider"].includes(key)) {
        throw railError("unknown_field", `capacity rail rejects unknown field ${JSON.stringify(key)}`);
      }
    }
    const profileId = requireRailIdentity("profileId", record.profileId);
    const provider = requireRailIdentity("provider", record.provider);
    if (seenProfiles.has(profileId)) {
      throw railError("duplicate_profile", `duplicate capacity profile ${profileId}`);
    }
    seenProfiles.add(profileId);
    profiles.push({ profileId, provider });
  }
  const observations: CapacityRailProviderFacts["observations"] = [];
  const seenObservations = new Set<string>();
  for (const entry of document.observations) {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) {
      throw railError("invalid_document", "each capacity observation must be an object");
    }
    const record = entry as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      if (!["profileId", "dimension", "unit", "limit", "used", "source", "observedAt"].includes(key)) {
        throw railError("unknown_field", `capacity rail rejects unknown field ${JSON.stringify(key)}`);
      }
    }
    const profileId = requireRailIdentity("profileId", record.profileId);
    if (!seenProfiles.has(profileId)) {
      throw railError(
        "unregistered_profile_reference",
        `observation references profile ${profileId} absent from the document's profiles[]`,
      );
    }
    const dimension = requireRailIdentity("dimension", record.dimension);
    const observedAt = requireRailIso("observedAt", record.observedAt);
    // Only EXACT restatements (same profile, dimension, AND timestamp) are
    // duplicates; several observations of one dimension over time are the
    // exact-change history this table exists to keep.
    const observationKey = `${profileId}\u0000${dimension}\u0000${observedAt}`;
    if (seenObservations.has(observationKey)) {
      throw railError(
        "duplicate_observation",
        `duplicate capacity observation for ${profileId}/${dimension} at ${observedAt}`,
      );
    }
    seenObservations.add(observationKey);
    if (typeof record.unit !== "string" || !CAPACITY_RAIL_UNITS.includes(record.unit as never)) {
      throw railError("invalid_unit", `capacity rail rejects unit ${String(record.unit)}`);
    }
    if (typeof record.source !== "string" || !CAPACITY_RAIL_SOURCES.includes(record.source as never)) {
      throw railError("invalid_source", `capacity rail rejects source ${String(record.source)}`);
    }
    observations.push({
      profileId,
      dimension,
      unit: record.unit as CapacityRailUnit,
      limit: optionalRailNumber(record.limit),
      used: optionalRailNumber(record.used),
      source: record.source as CapacityRailSource,
      observedAt,
    });
  }
  return { schema: CAPACITY_RAIL_PROVIDER_FACTS_SCHEMA, capturedAt, profiles, observations };
}

/**
 * Read the cached status surface WITHOUT opening SQLite. Returns the parsed
 * bounded payload plus why nothing was available; readers (doctor) must stay
 * honest about absence instead of inventing zeros.
 */
export function readRailStatusSurface(surfacePath: string): {
  status: "absent" | "unreadable" | "available";
  payload: CapacityRailStatus | null;
} {
  let text: string;
  try {
    text = fs.readFileSync(surfacePath, "utf8");
  } catch (error) {
    return {
      status: (error as NodeJS.ErrnoException).code === "ENOENT" ? "absent" : "unreadable",
      payload: null,
    };
  }
  try {
    const parsed = JSON.parse(text) as CapacityRailStatus;
    if (parsed?.schema !== CAPACITY_RAIL_SCHEMA) {
      return { status: "unreadable", payload: null };
    }
    return { status: "available", payload: parsed };
  } catch {
    return { status: "unreadable", payload: null };
  }
}
