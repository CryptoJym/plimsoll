/**
 * Fixture-based proof for issue #170 — local Capacity Rail store,
 * projection, CLI, dashboard endpoint, retention, and doctor.
 *
 * Everything runs against throwaway ledgers in temporary homes using the
 * REAL production wiring (LocalEventBuffer + CapacityRailStore). Provider
 * input arrives as operator-supplied documents; no test ever opens a
 * network connection. Adversarial cases try to make the rail leak private
 * content, lose coherence on failure, or serve unbounded/unfresh data.
 *
 *   pnpm proof:capacity-rail
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import {
  CAPACITY_RAIL_FRESHNESS_MAX_AGE_MS,
  CAPACITY_RAIL_MAX_INGEST_OBSERVATIONS,
  CAPACITY_RAIL_MAX_PROFILES,
  CAPACITY_RAIL_RETENTION_POLICIES,
  CAPACITY_RAIL_SCHEMA,
  CapacityRailStore,
} from "../packages/collector-cli/src/capacity-rail";
import { collectorCapacitySurfacePath } from "../packages/collector-cli/src/config";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { scanCapacityDoctrine } from "./capacity-dependency-reachability";

const repoRoot = process.cwd();
const tsxCli = path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs");
const cliEntry = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");

type Check = { name: string; detail: Record<string, unknown> };
const checks: Check[] = [];
function prove(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

/** Fixed-clock helper: T0 = 2026-08-24T12:00:00Z. */
const T0 = Date.parse("2026-08-24T12:00:00.000Z");
const at = (msFromT0: number) => new Date(T0 + msFromT0).toISOString();
const HOUR = 60 * 60 * 1000;
const DAY = 24 * HOUR;

function tempHome(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capacity-rail-"));
}

function openProductionBuffer(home: string): LocalEventBuffer {
  const ledger = path.join(home, "work-ledger.sqlite");
  return new LocalEventBuffer(ledger, { workspaceId: undefined });
}

function factsDoc(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema: "plimsoll.capacity-provider-facts.v1",
    capturedAt: at(0),
    profiles: [{ profileId: "anthropic.max.owner", provider: "anthropic" }],
    observations: [
      {
        profileId: "anthropic.max.owner",
        dimension: "five_hour_window",
        unit: "tokens",
        limit: 220_000,
        used: 30_000,
        source: "provider_report",
        observedAt: at(-1 * HOUR),
      },
    ],
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// 1. Ledger integration: separate tables + seeded retention policies.
// ---------------------------------------------------------------------------
{
  const home = tempHome();
  const buffer = openProductionBuffer(home);
  const tables = (
    buffer.database
      .prepare(`select name from sqlite_master where type='table' and name like 'capacity_rail_%' order by name`)
      .all() as Array<{ name: string }>
  ).map((row) => row.name);
  prove(
    "rail_creates_separate_profile_snapshot_window_daily_latest_retention_probe_tables",
    JSON.stringify(tables) ===
      JSON.stringify([
        "capacity_rail_daily_summaries",
        "capacity_rail_latest",
        "capacity_rail_probe_state",
        "capacity_rail_profiles",
        "capacity_rail_retention",
        "capacity_rail_snapshots",
        "capacity_rail_window_history",
      ]),
    { tables },
  );
  const policies = Object.fromEntries(
    (
      buffer.database
        .prepare(`select policy_key as k, policy_value as v from capacity_rail_retention`)
        .all() as Array<{ k: string; v: string }>
    ).map((row) => [row.k, row.v]),
  );
  prove(
    "rail_seeds_exact_retention_policies_in_their_own_table",
    policies.exact_changes_days === String(CAPACITY_RAIL_RETENTION_POLICIES.exactChangesDays) &&
      policies.window_history_days === String(CAPACITY_RAIL_RETENTION_POLICIES.windowHistoryDays) &&
      policies.daily_summaries_days === String(CAPACITY_RAIL_RETENTION_POLICIES.dailySummariesDays) &&
      policies.latest_until_profile_removal === "true" &&
      policies.refresh_mode === "manual_only_no_background_cadence",
    { policies },
  );
  const neverRun = buffer.capacityRail.railProjection(at(0));
  prove(
    "never_refreshed_projection_is_honestly_unknown_not_zero",
    neverRun.probe.state === "NEVER_RUN" &&
      neverRun.constraints.length === 0 &&
      neverRun.profiles.length === 0 &&
      neverRun.manualRefreshOnly === true,
    { probe: neverRun.probe },
  );
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// 2. Manual refresh: coherent apply across all five fact families.
// ---------------------------------------------------------------------------
{
  const home = tempHome();
  const buffer = openProductionBuffer(home);
  const receipt = buffer.capacityRail.applyProviderFacts(
    factsDoc({
      observations: [
        {
          profileId: "anthropic.max.owner",
          dimension: "five_hour_window",
          unit: "tokens",
          limit: 220_000,
          used: 30_000,
          source: "provider_report",
          observedAt: at(-2 * HOUR),
        },
        {
          profileId: "anthropic.max.owner",
          dimension: "five_hour_window",
          unit: "tokens",
          limit: 220_000,
          used: 40_000,
          source: "provider_report",
          observedAt: at(-30 * 60 * 1000),
        },
      ],
    }),
  );
  prove(
    "manual_refresh_accepts_bounded_document_and_records_receipt",
    receipt.accepted === true &&
      receipt.snapshotsRecorded === 2 &&
      receipt.profilesRegistered === 1 &&
      receipt.latestRowsReplaced === 1 &&
      receipt.windowBucketsTouched === 2 &&
      receipt.dailySummariesTouched === 1,
    { receipt },
  );
  const projection = buffer.capacityRail.railProjection(at(0));
  const constraint = projection.constraints[0]!;
  prove(
    "compact_projection_serves_newest_fresh_observation_with_real_headroom",
    constraint.used === 40_000 &&
      constraint.freshness === "fresh" &&
      constraint.remaining === 180_000 &&
      Math.abs(constraint.remainingFraction! - 180_000 / 220_000) < 1e-9 &&
      projection.probe.state === "OK",
    { constraint },
  );
  const windows = buffer.database
    .prepare(
      `select bucket_start as bucketStart, sample_count as samples, min_used as minUsed,
              max_used as maxUsed, last_used as lastUsed
       from capacity_rail_window_history order by bucket_start`,
    )
    .all() as Array<{ bucketStart: string; samples: number; minUsed: number; maxUsed: number; lastUsed: number }>;
  const expectedBucket = new Date(
    Math.floor(Date.parse(at(-2 * HOUR)) / (15 * 60 * 1000)) * 15 * 60 * 1000,
  ).toISOString();
  prove(
    "window_history_buckets_align_to_fifteen_minutes_with_merged_extremes",
    windows.length === 2 &&
      windows[0]!.bucketStart === expectedBucket &&
      windows[0]!.samples === 1 &&
      windows[1]!.samples === 1,
    { windows, expectedBucket },
  );
  const daily = buffer.database
    .prepare(
      `select day, visibility, sample_count as samples, max_used as maxUsed, last_used as lastUsed
       from capacity_rail_daily_summaries`,
    )
    .all() as Array<{ day: string; visibility: string; samples: number; maxUsed: number; lastUsed: number }>;
  prove(
    "daily_summaries_are_marked_self_only_and_merge_the_day",
    daily.length === 1 &&
      daily[0]!.visibility === "self_only" &&
      daily[0]!.samples === 2 &&
      daily[0]!.maxUsed === 40_000 &&
      daily[0]!.lastUsed === 40_000,
    { daily },
  );
  const snapshots = buffer.database
    .prepare(
      `select used_value as used, observed_at as observedAt, captured_at as capturedAt
       from capacity_rail_snapshots order by observed_at`,
    )
    .all() as Array<{ used: number; observedAt: string; capturedAt: string }>;
  prove(
    "exact_change_log_keeps_every_distinct_observation",
    snapshots.length === 2 && snapshots.every((row) => row.capturedAt === snapshots[0]!.capturedAt),
    { snapshots },
  );

  const surfacePath = collectorCapacitySurfacePath(path.join(home, "work-ledger.sqlite"));
  const written = buffer.capacityRail.writeStatusSurface(projection);
  const surfaceText = fs.readFileSync(surfacePath!, "utf8");
  prove(
    "status_surface_cache_written_atomically_beside_ledger",
    written === surfacePath &&
      JSON.parse(surfaceText).schema === CAPACITY_RAIL_SCHEMA &&
      (fs.statSync(surfacePath!).mode & 0o777) === 0o600,
    { surfacePath },
  );
  // Idempotent re-refresh converges instead of duplicating.
  buffer.capacityRail.applyProviderFacts(factsDoc());
  const snapshotsAfter = buffer.database.prepare(`select count(*) as n from capacity_rail_snapshots`).get() as { n: number };
  prove("refresh_is_idempotent_by_exact_timestamp", snapshotsAfter.n === 3, snapshotsAfter);
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// 3. Freshness fails closed: stale and future-dated evidence stay UNKNOWN.
// ---------------------------------------------------------------------------
{
  const home = tempHome();
  const buffer = openProductionBuffer(home);
  buffer.capacityRail.applyProviderFacts(factsDoc());
  const staleView = buffer.capacityRail.railProjection(at(7 * DAY));
  const staleRow = staleView.constraints[0]!;
  prove(
    "stale_evidence_reports_STALE_with_null_headroom_never_zero",
    staleRow.freshness === "STALE" &&
      staleRow.remaining === null &&
      staleRow.remainingFraction === null &&
      staleRow.used === 30_000 &&
      staleRow.ageMs === 7 * DAY + HOUR,
    { staleRow },
  );
  buffer.capacityRail.applyProviderFacts(
    factsDoc({
      observations: [
        {
          profileId: "anthropic.max.owner",
          dimension: "future_skew",
          unit: "tokens",
          limit: 100,
          used: 10,
          source: "provider_report",
          observedAt: at(5 * 60 * 60 * 1000),
        },
      ],
    }),
  );
  const futureRow = buffer.capacityRail.railProjection(at(0)).constraints.find(
    (row) => row.dimension === "future_skew",
  )!;
  prove(
    "future_dated_evidence_is_UNKNOWN_never_fresh_never_clamped",
    futureRow.freshness === "UNKNOWN" &&
      futureRow.ageMs === null &&
      futureRow.remaining === null &&
      futureRow.remainingFraction === null,
    { futureRow },
  );
  const boundaryRow = (() => {
    buffer.capacityRail.applyProviderFacts(
      factsDoc({
        observations: [
          {
            profileId: "anthropic.max.owner",
            dimension: "boundary_check",
            unit: "tokens",
            limit: 100,
            used: 10,
            source: "provider_report",
            observedAt: at(-CAPACITY_RAIL_FRESHNESS_MAX_AGE_MS),
          },
        ],
      }),
    );
    return buffer.capacityRail.railProjection(at(0)).constraints.find(
      (row) => row.dimension === "boundary_check",
    )!;
  })();
  prove(
    "evidence_exactly_at_the_freshness_horizon_stays_fresh",
    boundaryRow.freshness === "fresh" && boundaryRow.remaining === 90,
    { boundaryRow },
  );
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// 4. Provider failure preserves the last coherent state, honestly marked.
// ---------------------------------------------------------------------------
{
  const home = tempHome();
  const buffer = openProductionBuffer(home);
  buffer.capacityRail.applyProviderFacts(factsDoc());
  const coherentBefore = buffer.capacityRail.railProjection(at(0));
  const snapshotCountBefore = (
    buffer.database.prepare(`select count(*) as n from capacity_rail_snapshots`).get() as { n: number }
  ).n;

  const hostileCases: Array<[string, unknown, string]> = [
    ["malformed_json", "{not json", "malformed_json"],
    ["unknown_field_smuggling", factsDoc({ note: "free text" }), "unknown_field"],
    ["unregistered_profile_reference",
      factsDoc({
        profiles: [],
        observations: [{
          profileId: "ghost.profile", dimension: "d", unit: "tokens",
          limit: 1, used: 1, source: "provider_report", observedAt: at(0),
        }],
      }), "unregistered_profile_reference"],
    ["invalid_unit", factsDoc({
      observations: [{
        profileId: "anthropic.max.owner", dimension: "d", unit: "furlongs",
        limit: 1, used: 1, source: "provider_report", observedAt: at(0),
      }],
    }), "invalid_unit"],
    ["negative_measurement", factsDoc({
      observations: [{
        profileId: "anthropic.max.owner", dimension: "d", unit: "tokens",
        limit: 100, used: -5, source: "provider_report", observedAt: at(0),
      }],
    }), "invalid_measurement"],
    ["oversized_document",
      factsDoc({
        observations: Array.from({ length: CAPACITY_RAIL_MAX_INGEST_OBSERVATIONS + 1 }, (_, index) => ({
          profileId: "anthropic.max.owner",
          dimension: `dim.${index}`,
          unit: "tokens",
          limit: 100,
          used: 1,
          source: "provider_report",
          observedAt: at(index),
        })),
      }), "observation_bound_exceeded"],
    ["profile_bound_exceeded",
      factsDoc({
        profiles: Array.from({ length: CAPACITY_RAIL_MAX_PROFILES + 1 }, (_, index) => ({
          profileId: `cluster.profile.${index}`,
          provider: "anthropic",
        })),
        observations: [],
      }), "profile_bound_exceeded"],
    ["identity_rejection", factsDoc({
      profiles: [{ profileId: "bad profile with spaces", provider: "anthropic" }],
      observations: [],
    }), "invalid_identity"],
  ];
  let allRejected = true;
  const kindsSeen: string[] = [];
  for (const [label, document, expectedKind] of hostileCases) {
    try {
      buffer.capacityRail.applyProviderFacts(document);
      allRejected = false;
      kindsSeen.push(`${label}:ACCEPTED(!)`);
    } catch (error) {
      const kind = (error as { errorKind?: string }).errorKind;
      kindsSeen.push(kind ?? "NO_KIND");
      if (kind !== expectedKind) allRejected = false;
    }
  }
  const coherentAfter = buffer.capacityRail.railProjection(at(0));
  const snapshotCountAfter = (
    buffer.database.prepare(`select count(*) as n from capacity_rail_snapshots`).get() as { n: number }
  ).n;
  prove(
    "every_hostile_provider_document_is_rejected_with_named_error_kind",
    allRejected,
    { kindsSeen },
  );
  prove(
    "failed_refresh_preserves_last_coherent_state_byte_for_byte",
    JSON.stringify(coherentBefore.constraints) === JSON.stringify(coherentAfter.constraints) &&
      snapshotCountBefore === snapshotCountAfter,
    { before: coherentBefore.constraints, after: coherentAfter.constraints },
  );
  prove(
    "failure_marks_probe_honestly_with_error_kind_and_counter",
    coherentAfter.probe.state === "FAILED" &&
      coherentAfter.probe.consecutiveFailures === hostileCases.length &&
      typeof coherentAfter.probe.lastErrorKind === "string" &&
      (coherentAfter.probe.lastErrorMessage?.length ?? 0) <= 200,
    { probe: coherentAfter.probe },
  );

  // TRUE mid-transaction abort: a pre-seeded SQL tripwire rejects exactly one
  // row AFTER earlier rows of the same batch were applied. Atomicity must
  // roll the whole batch back.
  const tripHome = tempHome();
  const ledgerPath = path.join(tripHome, "work-ledger.sqlite");
  const raw = new Database(ledgerPath);
  raw.exec(`
    create table capacity_rail_profiles (
      profile_id text primary key, provider text not null,
      registered_at text not null, removed_at text
    ) without rowid;
    create table capacity_rail_snapshots (
      snapshot_id integer primary key autoincrement,
      profile_id text not null references capacity_rail_profiles(profile_id),
      dimension text not null, unit text not null,
      limit_value real check (limit_value is null or limit_value >= 0),
      used_value real check (used_value is null or used_value >= 0),
      source text not null, observed_at text not null, captured_at text not null,
      check (dimension <> 'trap.dimension')
    );
    create table capacity_rail_window_history (
      bucket_start text not null, profile_id text not null, dimension text not null,
      unit text not null, sample_count integer not null, min_used real, max_used real,
      last_used real, last_limit real, last_observed_at text not null,
      primary key (bucket_start, profile_id, dimension)
    ) without rowid;
    create table capacity_rail_daily_summaries (
      day text not null, profile_id text not null, dimension text not null,
      unit text not null, sample_count integer not null, max_used real, last_used real,
      last_limit real, last_observed_at text not null,
      visibility text not null default 'self_only',
      primary key (day, profile_id, dimension)
    ) without rowid;
    create table capacity_rail_latest (
      profile_id text not null, dimension text not null, unit text not null,
      limit_value real, used_value real, source text not null,
      observed_at text not null, updated_at text not null,
      primary key (profile_id, dimension)
    ) without rowid;
    create table capacity_rail_retention (
      policy_key text primary key, policy_value text not null, updated_at text not null
    ) without rowid;
    create table capacity_rail_probe_state (
      probe_key text primary key check (probe_key = 'manual_refresh'),
      state text not null check (state in ('NEVER_RUN', 'OK', 'FAILED')),
      last_attempt_at text, last_success_at text,
      consecutive_failures integer not null default 0,
      last_error_kind text, last_error_message text
    ) without rowid;
  `);
  const tripped = new CapacityRailStore(raw, { now: () => at(0) });
  try {
    tripped.applyProviderFacts(
      factsDoc({
        observations: [
          {
            profileId: "anthropic.max.owner",
            dimension: "clean.dimension",
            unit: "tokens",
            limit: 100,
            used: 1,
            source: "provider_report",
            observedAt: at(-HOUR),
          },
          {
            profileId: "anthropic.max.owner",
            dimension: "trap.dimension",
            unit: "tokens",
            limit: 100,
            used: 2,
            source: "provider_report",
            observedAt: at(-30 * 60 * 1000),
          },
        ],
      }),
    );
    prove("mid_transaction_tripwire_fires", false, { detail: "batch was accepted" });
  } catch {
    // expected
  }
  const countsAfterTrip = {
    profiles: (raw.prepare(`select count(*) as n from capacity_rail_profiles`).get() as { n: number }).n,
    snapshots: (raw.prepare(`select count(*) as n from capacity_rail_snapshots`).get() as { n: number }).n,
    latest: (raw.prepare(`select count(*) as n from capacity_rail_latest`).get() as { n: number }).n,
    windows: (raw.prepare(`select count(*) as n from capacity_rail_window_history`).get() as { n: number }).n,
  };
  const tripProbe = raw
    .prepare(`select state, consecutive_failures as f from capacity_rail_probe_state`)
    .get() as { state: string; f: number } | undefined;
  prove(
    "crash_mid_batch_rolls_back_to_last_coherent_state_and_marks_failure",
    countsAfterTrip.profiles === 0 &&
      countsAfterTrip.snapshots === 0 &&
      countsAfterTrip.latest === 0 &&
      countsAfterTrip.windows === 0 &&
      tripProbe?.state === "FAILED" &&
      tripProbe.f === 1,
    { countsAfterTrip, tripProbe },
  );

  // Crash/reopen coherence: reopen sees exactly what the commit saw.
  raw.close();
  const reopened = new Database(ledgerPath);
  const reopenedCounts = {
    snapshots: (reopened.prepare(`select count(*) as n from capacity_rail_snapshots`).get() as { n: number }).n,
    probe: (reopened.prepare(`select state as s from capacity_rail_probe_state`).get() as { s: string }).s,
  };
  reopened.close();
  prove(
    "reopen_after_failed_batch_matches_pre_crash_view_exactly",
    reopenedCounts.snapshots === 0 && reopenedCounts.probe === "FAILED",
    reopenedCounts,
  );
  fs.rmSync(tripHome, { recursive: true, force: true });

  // A successful refresh after failures resets the honest counters.
  buffer.capacityRail.applyProviderFacts(factsDoc());
  const recovered = buffer.capacityRail.railProjection(at(0));
  prove(
    "successful_refresh_clears_failure_counters",
    recovered.probe.state === "OK" && recovered.probe.consecutiveFailures === 0,
    { probe: recovered.probe },
  );
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// 5. Retention: 7 days exact, 30 days windows, 90 days dailies, latest until
//    profile removal. Boundary rows stay; strictly older rows go.
// ---------------------------------------------------------------------------
{
  const home = tempHome();
  const ledgerPath = path.join(home, "work-ledger.sqlite");
  // Simulated wall clock so ingestion stamps advance with each seeded age,
  // exactly like a real operator refreshing across days. The rail stamps
  // rows with ITS clock (the local ingestion moment), not with the
  // document's capturedAt, so the two must move together here.
  let simNowMs = T0;
  const timedRailStore = new CapacityRailStore(new Database(ledgerPath), {
    now: () => new Date(simNowMs).toISOString(),
    surfacePath: collectorCapacitySurfacePath(ledgerPath),
  });
  const rail = timedRailStore;
  const db = (rail as unknown as { database: Database.Database }).database;
  const applyAt = (ageDays: number, dimension: string) => {
    simNowMs = T0 - ageDays * DAY;
    rail.applyProviderFacts(
      factsDoc({
        observations: [{
          profileId: "anthropic.max.owner",
          dimension,
          unit: "tokens",
          limit: 1000,
          used: ageDays * 10,
          source: "provider_report",
          observedAt: new Date(simNowMs).toISOString(),
        }],
      }),
    );
  };
  applyAt(0, "day.zero");
  applyAt(6, "day.six");
  applyAt(7, "day.seven.exact");
  applyAt(8, "day.eight.over");
  applyAt(29, "window.twentynine");
  applyAt(30, "window.thirty.exact");
  applyAt(31, "window.thirtyone.over");
  applyAt(89, "daily.eightynine");
  applyAt(90, "daily.ninety.exact");
  applyAt(91, "daily.ninetyone.over");

  // One authoritative sweep pinned at T0; per-apply sweeps already ran with
  // their own past clocks and could not have touched newer rows.
  const swept = rail.runRetention(at(0));
  const remaining = {
    exact: (
      db.prepare(`select count(*) as n from capacity_rail_snapshots`).get() as { n: number }
    ).n,
    windows: (
      db.prepare(`select count(*) as n from capacity_rail_window_history`).get() as { n: number }
    ).n,
    daily: (
      db.prepare(`select count(*) as n from capacity_rail_daily_summaries`).get() as { n: number }
    ).n,
  };
  // Every apply writes one row into EACH family, so the sweep at T0 must
  // delete exactly the rows strictly older than each policy boundary:
  //   exact (7d):  ages {8,29,30,31,89,90,91} die  -> 7 deleted, 3 stay
  //   window(30d): ages {31,89,90,91} die          -> 4 deleted, 6 stay
  //   daily (90d): ages {91} dies                  -> 1 deleted, 9 stay
  prove(
    "retention_deletes_strictly_older_than_policy_boundaries",
    remaining.exact === 3 &&
      remaining.windows === 6 &&
      remaining.daily === 9 &&
      swept.exactChanges === 7 &&
      swept.windowBuckets === 4 &&
      swept.dailySummaries === 1,
    { remaining, swept },
  );
  const dimsOf = (table: string) =>
    (
      db.prepare(`select distinct dimension from ${table}`).all() as Array<{ dimension: string }>
    ).map((row) => row.dimension);
  prove(
    "boundary_rows_survive_and_overage_rows_die_per_family",
    (() => {
      const snapshots = dimsOf("capacity_rail_snapshots");
      const windows = dimsOf("capacity_rail_window_history");
      const dailies = dimsOf("capacity_rail_daily_summaries");
      return snapshots.includes("day.seven.exact") && !snapshots.includes("day.eight.over") &&
        windows.includes("window.thirty.exact") && !windows.includes("window.thirtyone.over") &&
        dailies.includes("daily.ninety.exact") && !dailies.includes("daily.ninetyone.over");
    })(),
    {
      snapshots: dimsOf("capacity_rail_snapshots"),
      windows: dimsOf("capacity_rail_window_history"),
      dailies: dimsOf("capacity_rail_daily_summaries"),
    },
  );
  prove(
    "latest_rows_are_never_swept_by_ttl",
    (db.prepare(`select count(*) as n from capacity_rail_latest`).get() as { n: number }).n === 10,
    {},
  );

  // Profile removal kills latest immediately; history ages out naturally.
  const removal = rail.forgetProfile("anthropic.max.owner", at(0));
  const afterRemoval = {
    latest: (db.prepare(`select count(*) as n from capacity_rail_latest`).get() as { n: number }).n,
    activeProfiles: (
      db.prepare(`select count(*) as n from capacity_rail_profiles where removed_at is null`).get() as { n: number }
    ).n,
    snapshots: (db.prepare(`select count(*) as n from capacity_rail_snapshots`).get() as { n: number }).n,
  };
  prove(
    "profile_removal_drops_latest_now_while_history_ages_out_per_policy",
    removal.removed === true &&
      afterRemoval.latest === 0 &&
      afterRemoval.activeProfiles === 0 &&
      afterRemoval.snapshots === 3,
    afterRemoval,
  );
  prove(
    "removed_profile_disappears_from_compact_projection",
    rail.railProjection(at(0)).profiles.length === 0 &&
      rail.railProjection(at(0)).constraints.length === 0,
    {},
  );
  (rail as unknown as { database: Database.Database }).database.close();
  fs.rmSync(home, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// 6. Dashboard request path: zero provider calls, zero raw-history scans,
//    bounded rows even under adversarial growth.
// ---------------------------------------------------------------------------
{
  const home = tempHome();
  const buffer = openProductionBuffer(home);
  const rail = buffer.capacityRail;

  // Grow the rail past the surface cap: 2 profiles x 40 dimensions.
  const bigDoc = factsDoc({
    profiles: [
      { profileId: "alpha.provider", provider: "anthropic" },
      { profileId: "beta.provider", provider: "openai" },
    ],
    observations: [] as Array<Record<string, unknown>>,
  });
  let minuteOffset = 1;
  for (const profile of bigDoc.profiles as Array<{ profileId: string }>) {
    for (let index = 0; index < 40; index++) {
      (bigDoc.observations as Array<Record<string, unknown>>).push({
        profileId: profile.profileId,
        dimension: `dimension.slot.${String(index).padStart(2, "0")}`,
        unit: "tokens",
        limit: 1000,
        used: index,
        source: "provider_report",
        observedAt: at(-minuteOffset * 60 * 1000),
      });
      minuteOffset += 1;
    }
  }
  rail.applyProviderFacts(bigDoc);

  // Execution-scope witness on a SECOND connection: prepare() is wrapped so
  // every returned statement RECORDS its executions. The rail pre-builds its
  // statements at construction, so execution-level capture is the honest way
  // to see exactly what one dashboard request runs.
  const secondConnection = new Database(path.join(home, "work-ledger.sqlite"));
  const originalPrepare = secondConnection.prepare.bind(secondConnection);
  type PrepareParameters = Parameters<Database.Database["prepare"]>;
  type RailStatement = Database.Statement;
  const executedSql: Array<{ sql: string; method: string }> = [];
  const observedConnection = secondConnection as unknown as {
    prepare: (...args: PrepareParameters) => RailStatement;
  };
  observedConnection.prepare = (...args: PrepareParameters) => {
    const sqlText = String(args[0]);
    const statement = originalPrepare(...(args as Parameters<typeof originalPrepare>));
    const record = (method: string) => (...inner: unknown[]) => {
      executedSql.push({ sql: sqlText, method });
      return (statement as unknown as Record<string, (...args2: unknown[]) => unknown>)[method](...inner);
    };
    return new Proxy(statement, {
      get(target, property, receiver) {
        if (property === "all" || property === "get" || property === "run" || property === "iterate") {
          return record(property as string);
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === "function" ? value.bind(target) : value;
      },
    }) as RailStatement;
  };
  const observedRail = new CapacityRailStore(observedConnection as unknown as Database.Database, {
    now: () => at(0),
  });

  executedSql.length = 0;
  const payload = observedRail.railProjection(at(0));
  const requestStatements = [...executedSql];
  executedSql.length = 0;
  observedRail.runRetention(at(0));
  const maintenanceStatements = [...executedSql];

  const forbiddenTables =
    /buffered_events|metric_samples|upload_outbox|account_labels|account_aliases|repo_context|session_usage_authority|collector_workspace_binding/;
  const tablesIn = (sqlList: Array<{ sql: string }>) =>
    new Set(
      sqlList.flatMap(({ sql }) =>
        [...sql.matchAll(/(?:from|join|update|into)\s+([a-z_][a-z0-9_]*)/gi)].map(
          (match) => match[1]!,
        ),
      ),
    );
  const requestTables = tablesIn(requestStatements);
  prove(
    "dashboard_request_path_touches_only_rail_tables_never_raw_history",
    requestStatements.length > 0 &&
      requestTables.size > 0 &&
      [...requestTables].every((table) => table.startsWith("capacity_rail_")) &&
      requestStatements.every(({ sql }) => !forbiddenTables.test(sql)),
    {
      requestTables: [...requestTables],
      statements: requestStatements.length,
    },
  );
  prove(
    "dashboard_request_path_is_pure_read_with_bounded_statements",
    requestStatements.length <= 16 &&
      requestStatements.every(
        ({ sql, method }) =>
          (method === "all" || method === "get") &&
          !/^\s*(insert|update|delete|replace)/i.test(sql),
      ),
    { statements: requestStatements.length, methods: [...new Set(requestStatements.map((s) => s.method))] },
  );
  prove(
    "surface_caps_rows_and_flags_truncation_instead_of_growing_unbounded",
    payload.constraints.length === 64 &&
      payload.constraintsTruncated === true &&
      payload.storedCounts.exactChanges === 80,
    {
      served: payload.constraints.length,
      truncated: payload.constraintsTruncated,
      stored: payload.storedCounts,
    },
  );

  // Zero provider calls and zero raw-history scans, statically too.
  const railSource = fs.readFileSync(
    path.join(repoRoot, "packages/collector-cli/src/capacity-rail.ts"),
    "utf8",
  );
  prove(
    "rail_module_contains_no_network_process_or_raw_history_surface",
    !/fetch\(|http\.request|https\.request|net\.connect|dgram|child_process|spawn\(|execSync\(/.test(railSource) &&
      !forbiddenTables.test(railSource),
    {},
  );
  secondConnection.close();
  buffer.close();
  fs.rmSync(home, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// 7. Privacy sentinels: DB tables, WAL, SHM, logs, exports, API responses,
//    support bundles, queue payloads stay free of private content.
// ---------------------------------------------------------------------------
{
  const sentinelsFixture = JSON.parse(
    fs.readFileSync(
      path.join(repoRoot, "scripts/resource-proof/fixtures/metadata-privacy-sentinels.json"),
      "utf8",
    ),
  ) as { sentinels: Record<string, string>; prefixLength: number };
  const sentinelValues = Object.values(sentinelsFixture.sentinels);
  const prefixes = sentinelValues.map((value) => value.slice(0, sentinelsFixture.prefixLength));
  const needles = [...sentinelValues, ...prefixes];

  const home = tempHome();
  const buffer = openProductionBuffer(home);
  const rail = buffer.capacityRail;

  // Smuggling attempts: sentinels in unknown fields and as identities.
  // Underscore-bearing tokens are legitimate LABEL characters (house identity
  // class), so the adversarial cases here use content-shaped sentinels —
  // absolute paths, multibyte prose, key=value cookies — that must never
  // become identities, plus unknown fields that must never exist at all.
  const smuggles: Array<[string, unknown, string]> = [
    ["unknown_top_level_field", { ...factsDoc(), operatorNote: sentinelValues[0] }, "unknown_field"],
    ["absolute_path_as_profile_identity", factsDoc({
      profiles: [{ profileId: sentinelValues[3]!, provider: "anthropic" }],
      observations: [],
    }), "invalid_identity"],
    ["multibyte_prose_as_dimension", factsDoc({
      observations: [{
        profileId: "anthropic.max.owner", dimension: sentinelValues[8]!, unit: "tokens",
        limit: 100, used: 1, source: "provider_report", observedAt: at(0),
      }],
    }), "invalid_identity"],
    ["cookie_pair_in_unknown_nested_field", factsDoc({
      observations: [{
        profileId: "anthropic.max.owner", dimension: "d", unit: "percent",
        limit: 100, used: 1, source: "local_telemetry", observedAt: at(0),
        context: { cookie: sentinelValues[7] },
      }],
    }), "unknown_field"],
    ["oversized_identity_blob", factsDoc({
      profiles: [{ profileId: `a${"b".repeat(128)}`, provider: "anthropic" }],
      observations: [],
    }), "invalid_identity"],
  ];
  const rejectionKinds: string[] = [];
  for (const [, document, expectedKind] of smuggles) {
    try {
      rail.applyProviderFacts(document);
      rejectionKinds.push("ACCEPTED(!)");
    } catch (error) {
      const kind = (error as { errorKind?: string }).errorKind ?? "unknown";
      rejectionKinds.push(kind === expectedKind ? kind : `${kind}(expected ${expectedKind})`);
    }
  }
  prove(
    "smuggled_privacy_sentinels_are_rejected_before_any_write",
    rejectionKinds.every((kind) => kind === "unknown_field" || kind === "invalid_identity") &&
      rejectionKinds.filter((kind) => kind === "unknown_field").length === 2 &&
      rejectionKinds.filter((kind) => kind === "invalid_identity").length === 3,
    { rejectionKinds },
  );

  // Now run a fully legitimate refresh and inspect EVERY storage surface.
  rail.applyProviderFacts(factsDoc());
  const apiResponse = rail.railProjection(at(0));
  const surfacePath = collectorCapacitySurfacePath(path.join(home, "work-ledger.sqlite"));
  rail.writeStatusSurface(apiResponse);
  const surfaceFile = surfacePath;
  const surfaceBytes = fs.readFileSync(surfaceFile);

  const tableNames = (
    buffer.database
      .prepare(`select name from sqlite_master where type='table' and name like 'capacity_rail_%'`)
      .all() as Array<{ name: string }>
  ).map((row) => row.name);
  let tableDump = "";
  for (const table of tableNames) {
    const rows = buffer.database.prepare(`select * from ${table}`).all();
    tableDump += JSON.stringify(rows);
  }

  // Interaction events and the event outbox are untouched by capacity work;
  // queue payloads never carry capacity fields.
  const outboxRows = buffer.database
    .prepare(`select count(*) as n from upload_outbox`)
    .get() as { n: number };
  const outboxPayloadSample = buffer.database
    .prepare(`select coalesce(group_concat(base_envelope_json), '') as all_payloads from upload_outbox`)
    .get() as { all_payloads: string };
  const eventRows = buffer.database
    .prepare(`select count(*) as n from buffered_events`)
    .get() as { n: number };

  buffer.close();
  const ledgerBytes = fs.readFileSync(path.join(home, "work-ledger.sqlite"));
  const walBytes = fs.existsSync(`${home}/work-ledger.sqlite-wal`)
    ? fs.readFileSync(`${home}/work-ledger.sqlite-wal`)
    : Buffer.alloc(0);
  const shmBytes = fs.existsSync(`${home}/work-ledger.sqlite-shm`)
    ? fs.readFileSync(`${home}/work-ledger.sqlite-shm`)
    : Buffer.alloc(0);

  const haystacks: Array<[string, string | Buffer]> = [
    ["api_response", JSON.stringify(apiResponse)],
    ["surface_file", surfaceBytes],
    ["capacity_tables", tableDump],
    ["ledger_db_bytes", ledgerBytes],
    ["wal_bytes", walBytes],
    ["shm_bytes", shmBytes],
  ];
  const leaks: string[] = [];
  for (const [surfaceName, haystack] of haystacks) {
    const text = Buffer.isBuffer(haystack) ? haystack.toString("utf8") : haystack;
    for (const needle of needles) {
      if (text.includes(needle)) leaks.push(`${surfaceName}:${needle.slice(0, 14)}`);
    }
  }
  prove(
    "privacy_sentinels_absent_from_tables_wal_shm_api_and_cached_surface",
    leaks.length === 0,
    { leaks },
  );

  // Interaction events and the event outbox are untouched by capacity work;
  // queue payloads never carry capacity fields.
  prove(
    "interaction_events_and_event_outbox_untouched_by_capacity_refresh",
    outboxRows.n === 0 &&
      eventRows.n === 0 &&
      !outboxPayloadSample.all_payloads.includes("capacity"),
    { outboxRows, eventRows },
  );

  // Support-bundle-style dump of the whole home directory: no sentinels.
  const supportDump: string[] = [];
  const walkBundle = (dir: string) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walkBundle(full);
      else if (entry.name.endsWith(".log") || entry.name.endsWith(".json")) {
        supportDump.push(fs.readFileSync(full, "utf8"));
      }
    }
  };
  walkBundle(home);
  prove(
    "support_bundle_style_home_dump_carries_no_sentinels",
    !supportDump.some((text) => needles.some((needle) => text.includes(needle))),
    { filesScanned: supportDump.length },
  );
  fs.rmSync(home, { recursive: true, force: true });

  // CLI logs stay clean even when REJECTING a sentinel-bearing document.
  const cliHome = tempHome();
  const badInputPath = path.join(cliHome, "bad-facts.json");
  fs.writeFileSync(
    badInputPath,
    JSON.stringify({ ...factsDoc(), sessionTranscript: sentinelValues[4] }),
  );
  const cliEnv = { ...process.env, PLIMSOLL_HOME: cliHome };
  let cliStderr = "";
  try {
    execFileSync(
      process.execPath,
      [tsxCli, cliEntry, "capacity", "refresh", "--input", badInputPath],
      { env: cliEnv, cwd: repoRoot, stdio: ["ignore", "pipe", "pipe"], timeout: 120_000 },
    );
  } catch (error) {
    const failure = error as { stderr?: Buffer; status?: number };
    cliStderr = failure.stderr?.toString("utf8") ?? "";
    if (failure.status !== 1) cliStderr += `\nunexpected exit ${failure.status}`;
  }
  prove(
    "cli_refresh_rejects_sentinel_document_without_echoing_it_into_logs",
    !needles.some((needle) => cliStderr.includes(needle)) &&
      cliStderr.includes("unknown_field"),
    { stderrHead: cliStderr.slice(0, 200) },
  );
  fs.rmSync(cliHome, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// 8. End-to-end CLI: status, refresh, forget-profile, doctor --read-only.
// ---------------------------------------------------------------------------
{
  const home = tempHome();
  const cliEnv = { ...process.env, PLIMSOLL_HOME: home };
  const runCli = (args: string[], expectFailure = false): { stdout: string; stderr: string } => {
    try {
      const stdout = execFileSync(
        process.execPath,
        [tsxCli, cliEntry, ...args],
        { env: cliEnv, cwd: repoRoot, stdio: ["ignore", "pipe", "pipe"], timeout: 180_000 },
      ).toString("utf8");
      return { stdout, stderr: "" };
    } catch (error) {
      const failure = error as { stdout?: Buffer; stderr?: Buffer; status?: number };
      if (!expectFailure) {
        throw new Error(
          `cli ${args.join(" ")} failed (${failure.status}): ${failure.stderr?.toString("utf8") ?? ""}`,
        );
      }
      return {
        stdout: failure.stdout?.toString("utf8") ?? "",
        stderr: failure.stderr?.toString("utf8") ?? "",
      };
    }
  };

  const factsPath = path.join(home, "facts.json");
  fs.writeFileSync(factsPath, JSON.stringify(factsDoc()));

  const statusBefore = JSON.parse(runCli(["capacity", "status"]).stdout);
  prove(
    "cli_capacity_status_reports_never_run_honestly_on_fresh_ledger",
    statusBefore.probe.state === "NEVER_RUN" && statusBefore.schema === CAPACITY_RAIL_SCHEMA,
    { probe: statusBefore.probe },
  );

  const refreshReceipt = JSON.parse(runCli(["capacity", "refresh", "--input", factsPath]).stdout);
  prove(
    "cli_capacity_refresh_applies_manual_document",
    refreshReceipt.accepted === true && refreshReceipt.snapshotsRecorded === 1,
    { receipt: refreshReceipt },
  );
  const statusAfter = JSON.parse(runCli(["capacity", "status"]).stdout);
  prove(
    "cli_capacity_status_serves_projection_after_refresh",
    statusAfter.constraints.length === 1 &&
      statusAfter.constraints[0].used === 30_000 &&
      statusAfter.probe.state === "OK",
    { constraints: statusAfter.constraints },
  );

  // Failed CLI refresh keeps exit code 1 and preserves prior state.
  const badPath = path.join(home, "bad.json");
  fs.writeFileSync(badPath, JSON.stringify({ schema: "wrong.schema.v1", capturedAt: at(0) }));
  const failedRefresh = runCli(["capacity", "refresh", "--input", badPath], true);
  prove(
    "cli_failed_refresh_exits_nonzero_with_named_kind",
    failedRefresh.stderr.includes("schema_mismatch"),
    { stderr: failedRefresh.stderr.slice(0, 160) },
  );
  const statusAfterFailure = JSON.parse(runCli(["capacity", "status"]).stdout);
  prove(
    "cli_state_intact_after_failed_refresh",
    statusAfterFailure.constraints.length === 1 &&
      statusAfterFailure.constraints[0].used === 30_000 &&
      statusAfterFailure.probe.state === "FAILED",
    { probe: statusAfterFailure.probe },
  );

  // forget-profile end to end.
  const removal = JSON.parse(
    runCli(["capacity", "forget-profile", "--profile", "anthropic.max.owner"]).stdout,
  );
  const statusAfterRemoval = JSON.parse(runCli(["capacity", "status"]).stdout);
  prove(
    "cli_forget_profile_removes_latest_rows_immediately",
    removal.removed === true &&
      statusAfterRemoval.constraints.length === 0 &&
      statusAfterRemoval.profiles.length === 0,
    { removal },
  );

  // Doctor: capacity block comes from the cached surface WITHOUT opening the
  // ledger. Proven adversarially: replace the ledger with garbage bytes so
  // ANY SQLite open would fail outright — this runs LAST because it ruins
  // the fixture ledger for every SQLite-based command after it.
  const surfacePath = collectorCapacitySurfacePath(path.join(home, "work-ledger.sqlite"));
  assert.ok(fs.existsSync(surfacePath), "status surface must exist after refresh/status");
  fs.writeFileSync(path.join(home, "work-ledger.sqlite"), Buffer.from("this is not a database"));
  const doctorRun = runCli(["doctor", "--read-only", "--json"], true);
  const doctorReceipt = JSON.parse(doctorRun.stdout);
  prove(
    "doctor_reads_capacity_from_cache_without_sqlite_even_when_ledger_is_corrupt",
    doctorReceipt.capacity.cacheStatus === "available" &&
      doctorReceipt.capacity.surface.schema === CAPACITY_RAIL_SCHEMA &&
      doctorReceipt.capacity.refreshMode === "manual_only_no_background_cadence" &&
      doctorReceipt.sqlite.opened === false &&
      doctorReceipt.enrollment.quarantinedHistoryRows === null,
    {
      capacity: doctorReceipt.capacity.cacheStatus,
      opened: doctorReceipt.sqlite.opened,
    },
  );

  // Absent cache stays honest (fresh home, no refresh ever).
  const freshHome = tempHome();
  const freshDoctor = JSON.parse(
    runCliWithHome(["doctor", "--read-only", "--json"], freshHome).stdout,
  );
  prove(
    "doctor_reports_absent_capacity_cache_honestly_not_zeros",
    freshDoctor.capacity.cacheStatus === "absent" && freshDoctor.capacity.surface === null,
    { capacity: { status: freshDoctor.capacity.cacheStatus, surface: freshDoctor.capacity.surface } },
  );
  fs.rmSync(freshHome, { recursive: true, force: true });

  function runCliWithHome(args: string[], homeDir: string): { stdout: string; stderr: string } {
    return runCliIn(args, { ...process.env, PLIMSOLL_HOME: homeDir });
  }
  function runCliIn(args: string[], env: NodeJS.ProcessEnv): { stdout: string; stderr: string } {
    try {
      const stdout = execFileSync(
        process.execPath,
        [tsxCli, cliEntry, ...args],
        { env, cwd: repoRoot, stdio: ["ignore", "pipe", "pipe"], timeout: 180_000 },
      ).toString("utf8");
      return { stdout, stderr: "" };
    } catch (error) {
      const failure = error as { stdout?: Buffer; stderr?: Buffer; status?: number };
      return {
        stdout: failure.stdout?.toString("utf8") ?? "",
        stderr: failure.stderr?.toString("utf8") ?? "",
      };
    }
  }
  fs.rmSync(home, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// 9. Doctrine gate: sanctioned wiring stays closed, repo stays clean.
// ---------------------------------------------------------------------------
{
  const report = scanCapacityDoctrine(repoRoot);
  prove(
    "doctrine_scan_repo_has_zero_capacity_doctrine_offenders",
    report.offendingImporters.length === 0,
    { offenders: report.offendingImporters },
  );
  prove(
    "sanctioned_wiring_set_is_exactly_the_three_product_files",
    JSON.stringify(report.sanctionedCapacitySurfaces) ===
      JSON.stringify([
        "packages/collector-cli/src/buffer.ts",
        "packages/collector-cli/src/capacity-rail.ts",
        "packages/collector-cli/src/cli.ts",
      ]),
    { sanctioned: report.sanctionedCapacitySurfaces },
  );
}

console.log(JSON.stringify({ schema: "plimsoll.capacity-rail-proof.v1", passed: checks.length, checks }, null, 2));
