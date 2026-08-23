/**
 * Fixture-based proof for issue #173 capacity planning intelligence.
 *
 * Covers the reset calendar, stale-signal alerts, gated linear pace
 * estimates, fact-separation doctrine, the explicit-merge gate, and a STATIC
 * DEPENDENCY PROOF that blocks capacity from automatic routing, coaching,
 * ranking, compensation, discipline, interventions, D3/D4 hosted work, or
 * individual performance verdicts.
 *
 * Uses only repository fixtures and in-memory data. Run with Node 22:
 *   pnpm exec tsx scripts/capacity-proof.ts
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { scanCapacityDoctrine } from "./capacity-dependency-reachability";

import {
  CAPACITY_PLAN_SCHEMA,
  assertCapacityMergeApproval,
  buildCapacityPlan,
  buildCapacityResetCalendar,
  classifyCapacitySignalFreshness,
  estimateCapacityLinearPace,
  type CapacityPaceEstimate,
} from "../packages/shared/src/index";

const root = process.cwd();
const fixtures = path.join(root, "packages/shared/fixtures/capacity");
type Check = { name: string; detail: Record<string, unknown> };
const checks: Check[] = [];

function prove(name: string, condition: unknown, detail: Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

// ---------------------------------------------------------------------------
// 1. Reset calendar — kinds stay separate, sorted, horizon-bounded.
// ---------------------------------------------------------------------------
{
  const fixture = JSON.parse(
    fs.readFileSync(path.join(fixtures, "reset-calendar.json"), "utf8"),
  ) as {
    now: string;
    horizonDays: number;
    profiles: Array<{ profileId: string; provider: string }>;
    resets: Array<{ eventId: string; profileId: string; kind: string; at: string }>;
  };
  const calendar = buildCapacityResetCalendar({
    events: fixture.resets as never,
    now: fixture.now,
    horizonDays: fixture.horizonDays,
  });
  prove(
    "calendar_lists_upcoming_events_sorted_within_horizon",
    calendar.length === 3 &&
      calendar[0]!.eventId === "reset.anthropic.weekly.aug24" &&
      calendar[1]!.eventId === "reset.anthropic.billing.sep" &&
      calendar[2]!.eventId === "reset.openai.earned.expire",
    { calendar },
  );
  const kinds = new Set(calendar.map((entry) => entry.kind));
  prove(
    "calendar_keeps_billing_quota_and_earned_kinds_separate",
    kinds.size === 3 &&
      calendar.some((entry) => entry.kind === "quota_reset") &&
      calendar.some((entry) => entry.kind === "earned_reset_expiry") &&
      calendar.some((entry) => entry.kind === "billing_renewal"),
    { kinds: [...kinds] },
  );
  const pastBilling = buildCapacityResetCalendar({
    events: [
      { eventId: "past.renewal", profileId: "anthropic.max.james", kind: "billing_renewal", at: "2026-07-01T00:00:00.000Z" },
      ...fixture.resets.filter((event) => event.profileId === "anthropic.max.james") as never,
    ],
    now: fixture.now,
    horizonDays: fixture.horizonDays,
  });
  prove(
    "calendar_excludes_past_and_out_of_horizon_events_without_zeroing_them",
    !pastBilling.some((entry) => entry.eventId === "past.renewal") &&
      pastBilling.every((entry) => entry.at >= fixture.now!),
    { pastBilling },
  );
  assert.throws(() =>
    buildCapacityResetCalendar({
      events: [{ ...fixture.resets[0]!, kind: "performance_score" }] as never,
      now: fixture.now,
    }),
  );
  prove("calendar_rejects_unknown_event_kinds", true, {});
}

// ---------------------------------------------------------------------------
// 2. Stale signals — STALE/UNKNOWN stay literal UNKNOWN, never zero.
// ---------------------------------------------------------------------------
{
  const fixture = JSON.parse(
    fs.readFileSync(path.join(fixtures, "stale-signals.json"), "utf8"),
  ) as {
    now: string;
    maxAgeMs: number;
    profiles: Array<{ profileId: string; provider: string }>;
    signalsByProfile: Record<string, Array<{ signalId: string; dimension: string; limit: number | null; used: number | null; observedAt: string | null }>>;
  };

  const staleFreshness = classifyCapacitySignalFreshness({
    observedAt: "2026-08-19T00:00:00.000Z",
    now: fixture.now,
    maxAgeMs: fixture.maxAgeMs,
  });
  const missingFreshness = classifyCapacitySignalFreshness({
    observedAt: null,
    now: fixture.now,
    maxAgeMs: fixture.maxAgeMs,
  });
  // Issue #195: a future-dated observation (clock skew, bad input) must fail
  // CLOSED — it is UNKNOWN, never fresh, and never clamped to age zero.
  const futureFreshness = classifyCapacitySignalFreshness({
    observedAt: "2026-08-22T00:00:00.000Z",
    now: fixture.now,
    maxAgeMs: fixture.maxAgeMs,
  });
  prove(
    "freshness_classifies_future_dated_observation_as_unknown_never_fresh",
    futureFreshness.status === "UNKNOWN" && futureFreshness.ageMs === null,
    { futureFreshness },
  );
  prove(
    "freshness_classifies_stale_and_missing_without_inventing_values",
    staleFreshness.status === "STALE" && staleFreshness.ageMs === 2 * 24 * 60 * 60 * 1000 &&
      missingFreshness.status === "UNKNOWN" && missingFreshness.ageMs === null,
    { staleFreshness, missingFreshness },
  );

  const plan = buildCapacityPlan({
    profiles: fixture.profiles,
    resets: [],
    signalsByProfile: fixture.signalsByProfile as never,
    now: fixture.now,
    maxAgeMs: fixture.maxAgeMs,
  });
  const summary = plan.summaries[0]!;
  const weekly = summary.constraints.find((row) => row.dimension === "weekly_all_models")!;
  const reported = summary.constraints.find((row) => row.dimension === "provider_reported_share")!;
  const fresh = summary.constraints.find((row) => row.dimension === "five_hour_window")!;
  prove(
    "stale_signal_headroom_stays_null_never_zero",
    weekly.freshness === "STALE" && weekly.remaining === null && weekly.remainingFraction === null &&
      weekly.used === 1_000_000,
    { weekly },
  );
  prove(
    "unknown_limit_signal_stays_unknown_even_when_recently_observed",
    reported.freshness === "UNKNOWN" && reported.limit === null &&
      reported.remaining === null && reported.remainingFraction === null,
    { reported },
  );
  prove(
    "fresh_evidence_yields_real_headroom",
    fresh.freshness === "fresh" && fresh.remaining === 40_000 &&
      Math.abs(fresh.remainingFraction! - 40_000 / 220_000) < 1e-9,
    { fresh },
  );

  const futureDatedPlan = buildCapacityPlan({
    profiles: [{ profileId: "anthropic.max.james", provider: "anthropic" }],
    resets: [],
    signalsByProfile: {
      "anthropic.max.james": [
        {
          signalId: "signal.future.skewed_clock",
          profileId: "anthropic.max.james",
          dimension: "five_hour_window",
          unit: "tokens",
          limit: 220_000,
          used: 10_000,
          source: "local_telemetry",
          observedAt: "2026-08-22T00:00:00.000Z",
        },
      ],
    },
    now: fixture.now,
    maxAgeMs: fixture.maxAgeMs,
  });
  const futureRow = futureDatedPlan.summaries[0]!.constraints[0]!;
  prove(
    "future_dated_signal_yields_no_headroom_and_raises_unknown_alert",
    futureRow.freshness === "UNKNOWN" && futureRow.remaining === null &&
      futureRow.remainingFraction === null && futureRow.ageMs === null &&
      futureDatedPlan.staleAlerts.length === 1 &&
      futureDatedPlan.staleAlerts[0]!.severity === "MISSING_SIGNAL",
    { row: futureRow, alerts: futureDatedPlan.staleAlerts },
  );

  const severities = plan.staleAlerts.map((alert) => alert.severity);
  prove(
    "stale_alerts_name_stale_and_missing_signals",
    severities.includes("STALE_SIGNAL") && severities.includes("MISSING_SIGNAL") &&
      plan.staleAlerts.every((alert) => alert.message.includes("not zero")),
    { alerts: plan.staleAlerts },
  );
  prove(
    "low_headroom_notification_fires_only_from_fresh_evidence",
    summary.lowHeadroomNotifications.length === 1 &&
      summary.lowHeadroomNotifications[0]!.dimension === "five_hour_window" &&
      !summary.lowHeadroomNotifications.some((notification) =>
        ["weekly_all_models", "provider_reported_share"].includes(notification.dimension),
      ),
    { notifications: summary.lowHeadroomNotifications },
  );

  const lowHeadroomPlan = buildCapacityPlan({
    profiles: fixture.profiles,
    resets: [],
    signalsByProfile: {
      "anthropic.max.james": [
        {
          signalId: "signal.nearly_full",
          profileId: "anthropic.max.james",
          dimension: "five_hour_window",
          unit: "tokens",
          limit: 220_000,
          used: 215_000,
          source: "local_telemetry",
          observedAt: fixture.now,
        },
      ],
    },
    now: fixture.now,
    maxAgeMs: fixture.maxAgeMs,
  });
  const lowHeadroom = lowHeadroomPlan.summaries[0]!.lowHeadroomNotifications;
  prove(
    "low_headroom_notification_fires_only_on_fresh_evidence",
    lowHeadroom.length === 1 && lowHeadroom[0]!.dimension === "five_hour_window" &&
      Math.abs(lowHeadroom[0]!.remainingFraction - 5_000 / 220_000) < 1e-9,
    { lowHeadroom },
  );
}

// ---------------------------------------------------------------------------
// 3. Gated linear pace estimates — derived, uncertain, windowed, freshness.
// ---------------------------------------------------------------------------
{
  const fixture = JSON.parse(
    fs.readFileSync(path.join(fixtures, "pace-gates.json"), "utf8"),
  ) as {
    now: string;
    minElapsedMs: number;
    minObservations: number;
    cases: Array<{
      case: string;
      expect: CapacityPaceEstimate["state"];
      expectedReasonPrefix?: string;
      knownLimitTokens?: number;
      limitObservedAt?: string;
      observations: Array<{ at: string; cumulativeTokens: number }>;
    }>;
  };

  for (const paceCase of fixture.cases) {
    const estimate = estimateCapacityLinearPace({
      observations: paceCase.observations,
      now: fixture.now,
      minElapsedMs: fixture.minElapsedMs,
      minObservations: fixture.minObservations,
      knownLimitTokens: paceCase.knownLimitTokens ?? null,
      limitObservedAt: paceCase.limitObservedAt ?? null,
    });
    if (paceCase.expect === "UNKNOWN") {
      prove(
        `pace_gate_blocks_${paceCase.case}_as_unknown_not_zero`,
        estimate.state === "UNKNOWN" &&
          typeof estimate.reason === "string" &&
          estimate.reason.startsWith(paceCase.expectedReasonPrefix!) &&
          estimate.tokensPerDay === null &&
          estimate.observationWindow === null,
        { estimate },
      );
    } else {
      // 200000 tokens over 44h → ~109090.91 tokens/day.
      prove(
        `pace_estimate_${paceCase.case}_is_derived_uncertain_with_window_and_freshness`,
        estimate.state === "ESTIMATED" &&
          estimate.claimClass === "derived" &&
          estimate.certainty === "uncertain" &&
          estimate.observationWindow!.since === "2026-08-19T00:00:00.000Z" &&
          estimate.observationWindow!.until === "2026-08-20T20:00:00.000Z" &&
          estimate.freshness!.asOf === fixture.now &&
          Math.abs(estimate.tokensPerDay! - 109_090.91) < 0.01,
        { estimate },
      );
      const projected = estimate.projectedExhaustion;
      // Remaining 20000 tokens at that pace ≈ 4.4h after last observation.
      const projectedMs = projected.projectedAt === null ? NaN : Date.parse(projected.projectedAt);
      prove(
        `pace_projection_${paceCase.case}_derived_from_fresh_limit`,
        projected.state === "PROJECTED" &&
          Math.abs(projectedMs - Date.parse("2026-08-21T00:24:00.000Z")) < 5_000 &&
          projected.limitTokens === 220_000,
        { projected },
      );
      prove(
        `pace_limitations_${paceCase.case}_name_doctrine`,
        estimate.limitations.join(" ").includes("never a routing, coaching, ranking, or verdict input"),
        { limitations: estimate.limitations },
      );
    }
  }

  const staleLimitEstimate = estimateCapacityLinearPace({
    observations: [
      { at: "2026-08-18T00:00:00.000Z", cumulativeTokens: 0 },
      { at: "2026-08-19T00:00:00.000Z", cumulativeTokens: 50_000 },
      { at: "2026-08-20T00:00:00.000Z", cumulativeTokens: 100_000 },
    ],
    now: fixture.now,
    knownLimitTokens: 220_000,
    limitObservedAt: "2026-08-10T00:00:00.000Z",
  });
  prove(
    "pace_projection_refuses_stale_limit_but_keeps_labeled_pace",
    staleLimitEstimate.state === "ESTIMATED" &&
      staleLimitEstimate.claimClass === "derived" &&
      staleLimitEstimate.projectedExhaustion.state === "UNKNOWN",
    { staleLimitEstimate },
  );

  // Issue #195: a cumulative counter that DROPS mid-window is a quota reset;
  // linear interpolation across it previously returned a negative
  // "stale-valid" pace instead of failing closed to UNKNOWN.
  const resetWindowEstimate = estimateCapacityLinearPace({
    observations: [
      { at: "2026-08-18T00:00:00.000Z", cumulativeTokens: 200_000 },
      { at: "2026-08-19T00:00:00.000Z", cumulativeTokens: 210_000 },
      { at: "2026-08-20T20:00:00.000Z", cumulativeTokens: 5_000 },
    ],
    now: fixture.now,
  });
  prove(
    "pace_gate_blocks_quota_reset_negative_delta_as_unknown_never_negative_pace",
    resetWindowEstimate.state === "UNKNOWN" &&
      typeof resetWindowEstimate.reason === "string" &&
      resetWindowEstimate.reason.startsWith("quota_reset_detected_in_window") &&
      resetWindowEstimate.tokensPerDay === null &&
      resetWindowEstimate.observationWindow === null &&
      resetWindowEstimate.claimClass === "observed",
    { resetWindowEstimate },
  );
}

// ---------------------------------------------------------------------------
// 4. Fact separation and explicit merge approval.
// ---------------------------------------------------------------------------
{
  const plan = buildCapacityPlan({
    profiles: [
      { profileId: "a.one", provider: "anthropic" },
      { profileId: "b.two", provider: "anthropic" },
    ],
    resets: [],
    signalsByProfile: {
      "a.one": [{
        signalId: "s.a", profileId: "a.one", dimension: "five_hour_window",
        unit: "tokens", limit: 100, used: 50, source: "local_telemetry",
        observedAt: "2026-08-21T00:00:00.000Z",
      }],
      "b.two": [{
        signalId: "s.b", profileId: "b.two", dimension: "five_hour_window",
        unit: "tokens", limit: 100, used: 90, source: "local_telemetry",
        observedAt: "2026-08-21T00:00:00.000Z",
      }],
    },
    costsByProfile: {
      "a.one": { profileId: "a.one", subscriptionUsdPerMonth: 200, apiEquivalentUsd: 1840, observedAt: "2026-08-21T00:00:00.000Z" },
    },
    now: "2026-08-21T00:00:00.000Z",
  });
  prove(
    "profiles_remain_distinct_no_cross_profile_blending",
    plan.profilesDistinct === true && plan.mergeApproval === "none" &&
      plan.summaries.length === 2 &&
      plan.summaries.every((summary) =>
        summary.constraints.every((row) => row.profileId === summary.profileId),
      ),
    { summaries: plan.summaries.map((summary) => summary.profileId) },
  );

  let mergedThrew = false;
  try {
    assertCapacityMergeApproval(undefined);
  } catch {
    mergedThrew = true;
  }
  let unapprovedThrew = false;
  try {
    assertCapacityMergeApproval({ approved: false, approvedBy: "owner", approvedAt: "2026-08-21T00:00:00.000Z" });
  } catch {
    unapprovedThrew = true;
  }
  assertCapacityMergeApproval({ approved: true, approvedBy: "owner", approvedAt: "2026-08-21T00:00:00.000Z" });
  prove(
    "explicit_merge_guard_requires_approved_true",
    mergedThrew && unapprovedThrew,
    { mergedThrew, unapprovedThrew },
  );

  assert.throws(() =>
    buildCapacityPlan({
      profiles: [{ profileId: "a.one", provider: "anthropic" }],
      resets: [],
      signalsByProfile: {},
      costsByProfile: {
        "ghost.profile": { profileId: "ghost.profile", subscriptionUsdPerMonth: 1, apiEquivalentUsd: 2, observedAt: "2026-08-21T00:00:00.000Z" },
      },
      now: "2026-08-21T00:00:00.000Z",
    }),
  );
  prove("cost_facts_must_reference_known_profiles", true, {});

  // Issue #195: a signal stored under one profile's key but stamped with
  // another profile's profileId must be rejected, not silently summarized.
  let mismatchedSignalThrew = false;
  try {
    buildCapacityPlan({
      profiles: [
        { profileId: "a.one", provider: "anthropic" },
        { profileId: "b.two", provider: "anthropic" },
      ],
      resets: [],
      signalsByProfile: {
        "a.one": [{
          signalId: "s.mismatch", profileId: "b.two", dimension: "five_hour_window",
          unit: "tokens", limit: 100, used: 50, source: "local_telemetry",
          observedAt: "2026-08-21T00:00:00.000Z",
        }],
      },
      now: "2026-08-21T00:00:00.000Z",
    });
  } catch {
    mismatchedSignalThrew = true;
  }
  let unknownKeyThrew = false;
  try {
    buildCapacityPlan({
      profiles: [{ profileId: "a.one", provider: "anthropic" }],
      resets: [],
      signalsByProfile: {
        "ghost.profile": [{
          signalId: "s.ghost", profileId: "ghost.profile", dimension: "five_hour_window",
          unit: "tokens", limit: 100, used: 50, source: "local_telemetry",
          observedAt: "2026-08-21T00:00:00.000Z",
        }],
      },
      now: "2026-08-21T00:00:00.000Z",
    });
  } catch {
    unknownKeyThrew = true;
  }
  let mismatchedCostKeyThrew = false;
  try {
    buildCapacityPlan({
      profiles: [
        { profileId: "a.one", provider: "anthropic" },
        { profileId: "b.two", provider: "anthropic" },
      ],
      resets: [],
      signalsByProfile: {},
      costsByProfile: {
        "a.one": { profileId: "b.two", subscriptionUsdPerMonth: 1, apiEquivalentUsd: 2, observedAt: "2026-08-21T00:00:00.000Z" },
      },
      now: "2026-08-21T00:00:00.000Z",
    });
  } catch {
    mismatchedCostKeyThrew = true;
  }
  prove(
    "profile_id_map_key_mismatch_rejected_for_signals_keys_and_costs",
    mismatchedSignalThrew && unknownKeyThrew && mismatchedCostKeyThrew,
    { mismatchedSignalThrew, unknownKeyThrew, mismatchedCostKeyThrew },
  );

  prove("plan_schema_is_versioned", plan.schema === CAPACITY_PLAN_SCHEMA, { schema: plan.schema });
}

// ---------------------------------------------------------------------------
// 5. STATIC DEPENDENCY PROOF — capacity must not feed decision surfaces.
//
// Reachability-based (issue #195): relative import specifiers are resolved
// and barrel/index re-export chains are followed, so `from "./index"` can no
// longer smuggle capacity into a decision surface. Decision surfaces are
// detected by CONTENT as well as by file name.
// ---------------------------------------------------------------------------
{
  const report = scanCapacityDoctrine(root);
  prove(
    "static_proof_no_module_reaches_and_consumes_capacity_from_decision_surface",
    report.offendingImporters.length === 0,
    { offenders: report.offendingImporters, scannedFiles: report.scannedFiles.length },
  );

  // FALSIFICATION FIXTURES — the exact bypass shapes from issue #195, planted
  // in a throwaway sandbox tree. The gate must turn RED naming each offender.
  // Fixture declarations are assembled from parts so THIS file's own source
  // does not itself read as a decision surface (issue #199).
  const exportFn = (name: string, body: string): string =>
    ["export", "function"].join(" ") + ` ${name}(): ${body}`;
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capacity-bypass-"));
  try {
    const sharedSrc = path.join(sandbox, "packages/shared/src");
    fs.mkdirSync(sharedSrc, { recursive: true });
    fs.writeFileSync(
      path.join(sharedSrc, "capacity.ts"),
      [
        "export const CAPACITY_PLAN_SCHEMA = \"plimsoll.capacity-plan.v1\" as const;",
        "export function buildCapacityPlan(): string { return CAPACITY_PLAN_SCHEMA; }",
        "",
      ].join("\n"),
    );
    fs.writeFileSync(
      path.join(sharedSrc, "index.ts"),
      [
        'export * from "./capacity";',
        'export * from "./harmless";',
        "",
      ].join("\n"),
    );
    fs.writeFileSync(
      path.join(sharedSrc, "harmless.ts"),
      "export function harmlessHelper(): number { return 7; }\n",
    );
    // Offender 1: decision surface by FILE NAME, bypassing via the barrel.
    fs.writeFileSync(
      path.join(sharedSrc, "metric-registry.ts"),
      [
        'import { buildCapacityPlan } from "./index";',
        exportFn("registerMetric", `string { return buildCapacityPlan(); }`),
        "",
      ].join("\n"),
    );
    // Offender 2: decision surface by CONTENT ONLY (innocuous filename).
    fs.writeFileSync(
      path.join(sharedSrc, "summarizer.ts"),
      [
        'import { buildCapacityPlan } from "./index";',
        exportFn("scoreTechnique", `string { return buildCapacityPlan(); }`),
        "",
      ].join("\n"),
    );
    // Offender 3: transitive consumer that never names a capacity symbol.
    fs.mkdirSync(path.join(sandbox, "packages/cli"), { recursive: true });
    fs.writeFileSync(
      path.join(sandbox, "packages/cli/dispatcher.ts"),
      [
        'import { scoreTechnique } from "../shared/src/summarizer";',
        exportFn("routeTask", `string { return scoreTechnique(); }`),
        "",
      ].join("\n"),
    );
    // Negative control: a file that REACHES capacity through the barrel
    // without consuming any capacity symbol stays green — reachability alone
    // does not condemn, actual capacity-symbol consumption does.
    fs.writeFileSync(
      path.join(sharedSrc, "innocent-bystander.ts"),
      [
        'import { harmlessHelper } from "./index";',
        "export function summarizeInnocently(): number { return harmlessHelper(); }",
        "",
      ].join("\n"),
    );

    const bypass = scanCapacityDoctrine(sandbox);
    const offenderPaths = bypass.offendingImporters.map((offense) => offense.file);

    prove(
      "falsification_barrel_import_in_named_decision_surface_turns_red",
      offenderPaths.some((file) => file.endsWith("metric-registry.ts")),
      { offenders: bypass.offendingImporters },
    );
    prove(
      "falsification_barrel_import_in_content_detected_decision_surface_turns_red",
      offenderPaths.some((file) => file.endsWith("summarizer.ts")),
      { offenders: bypass.offendingImporters },
    );
    prove(
      "falsification_transitive_consumer_without_capacity_symbols_turns_red",
      offenderPaths.some((file) => file.endsWith("dispatcher.ts")),
      { offenders: bypass.offendingImporters },
    );
    prove(
      "falsification_negative_control_non_decision_surface_stays_green",
      !offenderPaths.some((file) => file.endsWith("innocent-bystander.ts")) &&
        bypass.filesReachingCapacity.some((file) => file.endsWith("innocent-bystander.ts")),
      {
        offenders: bypass.offendingImporters,
        reaching: bypass.filesReachingCapacity.map((file) => path.basename(file)),
      },
    );
  } finally {
    fs.rmSync(sandbox, { recursive: true, force: true });
  }

  // -----------------------------------------------------------------------
  // FALSIFICATION FIXTURES for issue #199 — the two residuals measured on
  // the #195-hardened gate. Each exploit is planted in a throwaway sandbox
  // and MUST turn the scan RED.
  // -----------------------------------------------------------------------
  {
    const writeShared = (
      sandbox: string,
      extra: Array<{ name: string; lines: string[] }>,
    ): void => {
      const sharedSrc = path.join(sandbox, "packages/shared/src");
      fs.mkdirSync(sharedSrc, { recursive: true });
      fs.mkdirSync(path.join(sandbox, "packages/cli"), { recursive: true });
      fs.writeFileSync(
        path.join(sharedSrc, "capacity.ts"),
        [
          "export const CAPACITY_PLAN_SCHEMA = \"plimsoll.capacity-plan.v1\" as const;",
          "export function buildCapacityPlan(): string { return CAPACITY_PLAN_SCHEMA; }",
          "",
        ].join("\n"),
      );
      fs.writeFileSync(
        path.join(sharedSrc, "index.ts"),
        'export * from "./capacity";\n',
      );
      for (const file of extra) {
        fs.writeFileSync(path.join(sharedSrc, file.name), [...file.lines, ""].join("\n"));
      }
    };

    // Issue #199 P2: obfuscated namespace access. The namespace binding is
    // used with computed member access keyed by a string-split symbol name,
    // so no identifier scan can see it. Both the helper and the decision
    // surface that consumes it must be named.
    {
      const sandboxP2 = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capacity-p199-ns-"));
      try {
        writeShared(sandboxP2, [
          {
            name: "obfuscated-helper.ts",
            lines: [
              'import * as deep from "./index";',
              "",
              'const KEY = "build" + "Capacity" + "Plan";',
              "",
              "export function obfuscatedCapacityAccess(): string {",
              "  const surface = deep as unknown as Record<string, () => string>;",
              "  return surface[KEY]();",
              "}",
            ],
          },
          {
            name: "task-dispatcher.ts",
            lines: [
              'import { obfuscatedCapacityAccess } from "./obfuscated-helper";',
              "",
              exportFn("scoreTaskPlacement", `string { return obfuscatedCapacityAccess(); }`),
            ],
          },
        ]);
        const bypass = scanCapacityDoctrine(sandboxP2);
        const offenderPaths = bypass.offendingImporters.map((offense) => offense.file);
        prove(
          "falsification_p199_obfuscated_namespace_helper_turns_red",
          offenderPaths.some((file) => file.endsWith("obfuscated-helper.ts")),
          { offenders: bypass.offendingImporters },
        );
        prove(
          "falsification_p199_decision_surface_consuming_obfuscated_helper_turns_red",
          offenderPaths.some((file) => file.endsWith("task-dispatcher.ts")),
          { offenders: bypass.offendingImporters },
        );
      } finally {
        fs.rmSync(sandboxP2, { recursive: true, force: true });
      }
    }

    // Issue #199 P3: a decision surface NAMED to match /capacity/i must stay
    // scanned. It consumes capacity through the sanctioned barrel and is a
    // decision surface BY NAME, so it must turn RED. Negative control: a
    // capacity-named DEFINITION module that only uses its own exports stays
    // green — definition and consumption are separate roles.
    {
      const sandboxP3 = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capacity-p199-name-"));
      try {
        writeShared(sandboxP3, [
          {
            name: "capacity-router.ts",
            lines: [
              'import { buildCapacityPlan } from "./index";',
              "",
              exportFn("routeThroughCapacityGate", `string { return buildCapacityPlan(); }`),
            ],
          },
          {
            name: "capacity-notes.ts",
            lines: [
              "export const CAPACITY_NOTE_FLOOR = 3;",
              "",
              "export function describeCapacityNoteFloor(): number { return CAPACITY_NOTE_FLOOR; }",
            ],
          },
        ]);
        const bypass = scanCapacityDoctrine(sandboxP3);
        const offenderPaths = bypass.offendingImporters.map((offense) => offense.file);
        prove(
          "falsification_p199_capacity_named_decision_surface_turns_red",
          offenderPaths.some((file) => file.endsWith("capacity-router.ts")),
          { offenders: bypass.offendingImporters },
        );
        prove(
          "falsification_p199_capacity_named_definition_module_stays_green",
          !offenderPaths.some((file) => file.endsWith("capacity-notes.ts")) &&
            bypass.capacityModules.some((file) => file.endsWith("capacity-notes.ts")) &&
            bypass.scannedFiles.some((file) => file.endsWith("capacity-notes.ts")),
          { offenders: bypass.offendingImporters },
        );
      } finally {
        fs.rmSync(sandboxP3, { recursive: true, force: true });
      }
    }

    // Issue #199 TOOLING RULE. The only offense exemption is the gate's own
    // enforcement code, keyed to exact repo-relative paths. These three
    // checks pin that rule from both sides so it cannot decay back into the
    // P3 basename exemption:
    //   (a) a capacity-named consumer that is NOT a decision surface still
    //       offends — naming a module after capacity buys no exemption;
    //   (b) a file carrying an enforcement-tooling BASENAME in a different
    //       directory is still policed — the allowlist is path-keyed;
    //   (c) the real allowlisted path is exempt and is not a symbol PROVIDER.
    {
      const sandboxTool = fs.mkdtempSync(
        path.join(os.tmpdir(), "plimsoll-capacity-p199-tooling-"),
      );
      try {
        writeShared(sandboxTool, [
          {
            // Capacity-named, consumes capacity, carries no decision verb in
            // its name or its exports. Must still be RED.
            name: "capacity-dispatch.ts",
            lines: [
              'import { buildCapacityPlan } from "./index";',
              "",
              exportFn("dispatchQueuedWork", `string { return buildCapacityPlan(); }`),
            ],
          },
          {
            // Same BASENAME as allowlisted tooling, different directory.
            name: "capacity-proof.ts",
            lines: [
              'import { buildCapacityPlan } from "./index";',
              "",
              exportFn("exerciseCapacityFixture", `string { return buildCapacityPlan(); }`),
            ],
          },
        ]);
        // The genuinely allowlisted path, consuming capacity exactly as the
        // real gate tooling does.
        fs.mkdirSync(path.join(sandboxTool, "scripts"), { recursive: true });
        fs.writeFileSync(
          path.join(sandboxTool, "scripts/capacity-dependency-reachability.ts"),
          [
            'import { buildCapacityPlan } from "../packages/shared/src/index";',
            "",
            exportFn("scanFixtureDoctrine", `string { return buildCapacityPlan(); }`),
            "",
          ].join("\n"),
        );
        const bypass = scanCapacityDoctrine(sandboxTool);
        const offenderPaths = bypass.offendingImporters.map((offense) => offense.file);
        prove(
          "falsification_p199_capacity_named_non_decision_consumer_turns_red",
          offenderPaths.some((file) => file.endsWith("capacity-dispatch.ts")),
          { offenders: bypass.offendingImporters },
        );
        prove(
          "falsification_p199_tooling_basename_outside_allowlisted_path_turns_red",
          offenderPaths.some((file) =>
            file.endsWith(path.join("packages/shared/src", "capacity-proof.ts")),
          ),
          { offenders: bypass.offendingImporters },
        );
        prove(
          "p199_enforcement_tooling_is_exempt_and_is_not_a_capacity_symbol_provider",
          !offenderPaths.some((file) =>
            file.endsWith(path.join("scripts", "capacity-dependency-reachability.ts")),
          ) &&
            !bypass.capacityModules.some((file) =>
              file.endsWith(path.join("scripts", "capacity-dependency-reachability.ts")),
            ) &&
            bypass.scannedFiles.some((file) =>
              file.endsWith(path.join("scripts", "capacity-dependency-reachability.ts")),
            ),
          { offenders: bypass.offendingImporters, capacityModules: bypass.capacityModules },
        );
      } finally {
        fs.rmSync(sandboxTool, { recursive: true, force: true });
      }
    }
  }

  const capacitySource = report.capacityModules
    .filter((file) => file.includes(`${path.sep}src${path.sep}`))
    .map((file) => fs.readFileSync(file, "utf8"))
    .join("\n");
  const importsDecisionSurface =
    /from\s+"\.[^"]*(?:performance-layer|metric-registry|learning-evidence)"/.test(capacitySource);
  prove(
    "static_proof_capacity_module_imports_no_scoring_or_performance_layer",
    !importsDecisionSurface,
    { importsDecisionSurface },
  );

  const exportedFunctionNames = [...capacitySource.matchAll(/export (?:async )?function ([A-Za-z0-9]+)\(/g)]
    .map((match) => match[1]!);
  const forbiddenExports = exportedFunctionNames.filter((name) =>
    /(?:route|coach|rank|compensat|disciplin|intervent|verdict)/i.test(name),
  );
  prove(
    "static_proof_capacity_exports_no_routing_coaching_ranking_verdict_verbs",
    forbiddenExports.length === 0,
    { exportedFunctionNames, forbiddenExports },
  );

  const dashboard = fs.readFileSync(
    path.join(root, "packages/collector-cli/src/dashboard.html"),
    "utf8",
  );
  const server = fs.readFileSync(path.join(root, "packages/collector-cli/src/server.ts"), "utf8");
  prove(
    "static_proof_dashboard_and_server_have_no_capacity_to_performance_wiring",
    !/(?:capacity[A-Za-z]*\s*=>\s*.*outcomePerformance)|(?:outcomePerformance\s*=.*[Cc]apacity)/.test(dashboard) &&
      !/[Cc]apacity.*outcomePerformance/.test(server),
    {},
  );
}

console.log(JSON.stringify({ schema: "plimsoll.capacity-proof.v1", passed: checks.length, checks }, null, 2));
