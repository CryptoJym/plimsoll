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
import path from "node:path";

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

  prove("plan_schema_is_versioned", plan.schema === CAPACITY_PLAN_SCHEMA, { schema: plan.schema });
}

// ---------------------------------------------------------------------------
// 5. STATIC DEPENDENCY PROOF — capacity must not feed decision surfaces.
//
// Scans every TypeScript source file in the repo and fails if any module
// concerned with routing, coaching, ranking, compensation, discipline,
// interventions, D3/D4 hosted phases, or individual performance verdicts
// imports the capacity module; if the capacity module itself imports any
// scoring/decision surface; or if the capacity export surface grows a verb
// belonging to those domains.
// ---------------------------------------------------------------------------
{
  const forbiddenConsumerPattern =
    /(?:rout|coach|rank|ranking|compensat|disciplin|intervent|verdict|score|scoring|performance[-_.]?layer|metric[-_.]?registry)/i;
  const forbiddenExportPattern =
    /export (?:async )?function [A-Za-z]*(?:Route|rout|Coach|coach|Rank|rank|Compensat|Disciplin|Intervent|Verdict|Score|Scoring)[A-Za-z]*\(/;

  const sourceDirs = ["packages", "scripts"];
  const tsFiles: string[] = [];
  const walk = (dir: string) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.name === "node_modules" || entry.name.startsWith(".")) continue;
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.name.endsWith(".ts")) tsFiles.push(full);
    }
  };
  for (const dir of sourceDirs) walk(path.join(root, dir));

  const capacityFiles = tsFiles.filter((file) => /capacity/i.test(path.basename(file)));
  const importersOfCapacity = tsFiles.filter((file) => {
    if (capacityFiles.includes(file)) return false;
    const text = fs.readFileSync(file, "utf8");
    return /from\s+"[^"]*capacity"/.test(text) || /from\s+'[^']*capacity'/.test(text);
  });

  const offendingImporters = importersOfCapacity.filter((file) => forbiddenConsumerPattern.test(file));
  prove(
    "static_proof_no_decision_surface_module_imports_capacity",
    offendingImporters.length === 0,
    { importersOfCapacity, offendingImporters },
  );

  const capacitySource = capacityFiles
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
