/**
 * Fixture-based proof for issue #174 matched-outcome capacity research
 * guardrails.
 *
 * Proves that capacity can be studied ONLY as a supporting context variable
 * under the separate scientific protocol in
 * packages/shared/src/capacity-research.ts:
 *
 * 1. Pre-registration fails closed without matched work type AND time
 *    window, all six required evidence dimensions, a declared minimum
 *    sample plus the UNKNOWN missing-evidence rule, six separate fact-stream
 *    join keys, explicit three-machine-compatibility exclusion, and recorded
 *    human review.
 * 2. Readiness gates leave insufficient evidence literal UNKNOWN — never a
 *    zero, never a small effect estimate — and machine-compatibility
 *    observations never enter statistical team conclusions.
 * 3. Findings disclose scope, source dates, confidence, limitations, and
 *    the decisions they can and cannot support; causal claims and every
 *    forbidden decision (rankings, coaching scores, compensation,
 *    discipline, interventions, D3/D4 hosted analytics, individual
 *    performance verdicts) fail closed.
 * 4. STATIC DOCTRINE INTEGRATION: the reachability gate covers the new
 *    module — decision surfaces consuming capacity-research symbols turn
 *    RED, and the real tree stays clean.
 *
 * Uses only in-memory data and throwaway sandboxes. Run with Node 22:
 *   pnpm exec tsx scripts/capacity-research-proof.ts
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { scanCapacityDoctrine } from "./capacity-dependency-reachability";

import {
  CAPACITY_RESEARCH_FACT_STREAMS,
  CAPACITY_RESEARCH_FINDING_SCHEMA,
  CAPACITY_RESEARCH_FORBIDDEN_DECISIONS,
  CAPACITY_RESEARCH_LIMITATIONS,
  CAPACITY_RESEARCH_POPULATION_EVENTS,
  CAPACITY_RESEARCH_PROTOCOL_SCHEMA,
  CAPACITY_RESEARCH_REQUIRED_EVIDENCE,
  buildCapacityResearchFinding,
  evaluateCapacityResearchReadiness,
  validateCapacityResearchProtocol,
  type CapacityResearchFindingInput,
  type CapacityResearchProtocolInput,
  type CapacityResearchSampleCounts,
} from "../packages/shared/src/index";

const root = process.cwd();
type Check = { name: string; detail: Record<string, unknown> };
const checks: Check[] = [];

function prove(name: string, condition: unknown, detail: Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function throws(action: () => void): boolean {
  try {
    action();
    return false;
  } catch {
    return true;
  }
}

const NOW = "2026-08-24T00:00:00.000Z";

function validProtocolInput(
  overrides: Partial<CapacityResearchProtocolInput> = {},
): CapacityResearchProtocolInput {
  return {
    studyId: "study.capacity-context.pilot1",
    matching: {
      workType: "bugfix_pr",
      window: {
        since: "2026-08-01T00:00:00.000Z",
        until: "2026-08-21T00:00:00.000Z",
      },
      populationEvent: "merged",
    },
    requiredEvidence: [...CAPACITY_RESEARCH_REQUIRED_EVIDENCE],
    minimumSample: { minMatchedPairs: 12, missingEvidencePolicy: "unknown" },
    factStreamJoins: [
      { stream: "identity", joinKey: "actor_cluster_id" },
      { stream: "activity", joinKey: "episode_id" },
      { stream: "time", joinKey: "session_window_id" },
      { stream: "cost", joinKey: "cost_record_id" },
      { stream: "capacity", joinKey: "profile_dimension_slot" },
      { stream: "outcomes", joinKey: "delivery_outcome_id" },
    ],
    exclusions: ["three_machine_compatibility"],
    humanReview: { approvedBy: "owner.james", approvedAt: "2026-07-28T00:00:00.000Z" },
    now: NOW,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// 1. Pre-registration gate — fail-closed protocol validation.
// ---------------------------------------------------------------------------
{
  const protocol = validateCapacityResearchProtocol(validProtocolInput());
  prove(
    "protocol_accepts_fully_pre_registered_study",
    protocol.schema === CAPACITY_RESEARCH_PROTOCOL_SCHEMA &&
      protocol.matching.workType === "bugfix_pr" &&
      protocol.matching.populationEvent === "merged" &&
      protocol.requiredEvidence.length === CAPACITY_RESEARCH_REQUIRED_EVIDENCE.length &&
      protocol.exclusions[0] === "three_machine_compatibility" &&
      protocol.factStreamJoins.length === CAPACITY_RESEARCH_FACT_STREAMS.length &&
      protocol.capacityRole === "supporting_context_variable",
    { studyId: protocol.studyId, exclusions: protocol.exclusions },
  );

  const distinctJoinKeys =
    new Set(protocol.factStreamJoins.map((join) => join.joinKey)).size ===
    CAPACITY_RESEARCH_FACT_STREAMS.length;
  prove("protocol_keeps_all_six_fact_stream_join_keys_distinct", distinctJoinKeys, {
    joins: protocol.factStreamJoins,
  });

  // Deliverable 1: comparison is impossible before work type AND window match.
  prove(
    "gate_rejects_missing_work_type",
    throws(() =>
      validateCapacityResearchProtocol(
        validProtocolInput({
          matching: {
            workType: "",
            window: { since: "2026-08-01T00:00:00.000Z", until: "2026-08-21T00:00:00.000Z" },
            populationEvent: "merged",
          },
        }),
      ),
    ),
    {},
  );
  prove(
    "gate_rejects_inverted_or_missing_time_window",
    throws(() =>
      validateCapacityResearchProtocol(
        validProtocolInput({
          matching: {
            workType: "bugfix_pr",
            window: { since: "2026-08-21T00:00:00.000Z", until: "2026-08-01T00:00:00.000Z" },
            populationEvent: "merged",
          },
        }),
      ),
    ) &&
      throws(() =>
        validateCapacityResearchProtocol(
          validProtocolInput({
            matching: {
              workType: "bugfix_pr",
              window: { since: "", until: "2026-08-21T00:00:00.000Z" },
              populationEvent: "merged",
            },
          }),
        ),
      ),
    {},
  );
  prove(
    "gate_rejects_unknown_population_event",
    throws(() =>
      validateCapacityResearchProtocol(
        validProtocolInput({
          matching: {
            workType: "bugfix_pr",
            window: { since: "2026-08-01T00:00:00.000Z", until: "2026-08-21T00:00:00.000Z" },
            populationEvent: "committed" as never,
          },
        }),
      ),
    ),
    { allowed: [...CAPACITY_RESEARCH_POPULATION_EVENTS] },
  );

  // Deliverable 2: each of the six evidence dimensions is individually
  // required — dropping any one fails pre-registration.
  const droppedDimensions = CAPACITY_RESEARCH_REQUIRED_EVIDENCE.filter(
    (dimension) =>
      throws(() =>
        validateCapacityResearchProtocol(
          validProtocolInput({
            requiredEvidence: CAPACITY_RESEARCH_REQUIRED_EVIDENCE.filter(
              (kept) => kept !== dimension,
            ),
          }),
        ),
      ),
  );
  prove(
    "gate_requires_every_evidence_dimension_individually",
    droppedDimensions.length === CAPACITY_RESEARCH_REQUIRED_EVIDENCE.length,
    {
      rejectedWhenDropped: droppedDimensions,
      required: [...CAPACITY_RESEARCH_REQUIRED_EVIDENCE],
    },
  );

  // Deliverable 4: minimum sample and missing-evidence rules are declared
  // BEFORE inference, and the only legal policy is UNKNOWN.
  prove(
    "gate_rejects_undeclared_or_zero_minimum_sample",
    throws(() =>
      validateCapacityResearchProtocol(
        validProtocolInput({
          minimumSample: { minMatchedPairs: 0, missingEvidencePolicy: "unknown" },
        }),
      ),
    ) &&
      throws(() =>
        validateCapacityResearchProtocol(
          validProtocolInput({
            minimumSample: undefined as never,
          }),
        ),
      ),
    {},
  );
  const hostilePolicies = ["drop", "zero", "impute_mean", "drop_silently"];
  const rejectedPolicies = hostilePolicies.filter((policy) =>
    throws(() =>
      validateCapacityResearchProtocol(
        validProtocolInput({
          minimumSample: {
            minMatchedPairs: 12,
            missingEvidencePolicy: policy as never,
          },
        }),
      ),
    ),
  );
  prove(
    "gate_rejects_every_missing_evidence_policy_except_unknown",
    rejectedPolicies.length === hostilePolicies.length,
    { rejectedPolicies, attempted: hostilePolicies },
  );

  // Deliverable 3: six separate joins; duplicates, gaps, and shared keys
  // (stream collapse) all fail.
  const baseJoins = validProtocolInput().factStreamJoins;
  prove(
    "gate_rejects_duplicate_missing_and_collapsed_fact_stream_joins",
    throws(() =>
      validateCapacityResearchProtocol(
        validProtocolInput({
          factStreamJoins: [
            ...baseJoins,
            { stream: "cost", joinKey: "cost_record_id_duplicate" },
          ],
        }),
      ),
    ) &&
      throws(() =>
        validateCapacityResearchProtocol(
          validProtocolInput({
            factStreamJoins: baseJoins.filter((join) => join.stream !== "capacity"),
          }),
        ),
      ) &&
      throws(() =>
        validateCapacityResearchProtocol(
          validProtocolInput({
            // outcomes reusing the activity key collapses two streams into one join.
            factStreamJoins: baseJoins.map((join) =>
              join.stream === "outcomes"
                ? { stream: join.stream, joinKey: "episode_id" }
                : join,
            ),
          }),
        ),
      ),
    {},
  );

  // Acceptance: three-machine compatibility data must be explicitly excluded.
  prove(
    "gate_refuses_protocol_without_three_machine_compatibility_exclusion",
    throws(() =>
      validateCapacityResearchProtocol(validProtocolInput({ exclusions: [] })),
    ),
    {},
  );

  // Human review is mandatory and cannot be future-dated.
  prove(
    "gate_requires_recorded_human_review",
    throws(() =>
      validateCapacityResearchProtocol(
        validProtocolInput({ humanReview: undefined as never }),
      ),
    ) &&
      throws(() =>
        validateCapacityResearchProtocol(
          validProtocolInput({
            humanReview: { approvedBy: "owner.james", approvedAt: "2027-01-01T00:00:00.000Z" },
          }),
        ),
      ),
    {},
  );

  // Identity grammar holds (whitespace, NFC/fullwidth aliases reject).
  prove(
    "gate_enforces_identity_grammar_on_study_and_reviewer_ids",
    throws(() =>
      validateCapacityResearchProtocol(validProtocolInput({ studyId: "study capacity pilot" })),
    ) &&
      throws(() =>
        validateCapacityResearchProtocol(
          validProtocolInput({
            humanReview: { approvedBy: "ｏｗｎｅｒ", approvedAt: "2026-07-28T00:00:00.000Z" },
          }),
        ),
      ),
    {},
  );
}

// ---------------------------------------------------------------------------
// 2. Readiness gates — insufficient evidence stays UNKNOWN, never zero.
// ---------------------------------------------------------------------------
{
  const protocol = validateCapacityResearchProtocol(validProtocolInput());
  const counts: CapacityResearchSampleCounts = {
    observations: 40,
    machineCompatibilityObservations: 6,
    matchedPairs: 14,
    pairsMissingRequiredEvidence: 2,
  };

  const estimable = evaluateCapacityResearchReadiness(protocol, counts);
  prove(
    "readiness_estimable_at_exact_declared_minimum_with_compatibility_excluded",
    estimable.state === "ESTIMABLE" &&
      estimable.usablePairs === 12 &&
      estimable.excludedMachineCompatibilityObservations === 6 &&
      estimable.pairsUnknownEvidence === 2,
    { readiness: estimable },
  );

  const belowMinimum = evaluateCapacityResearchReadiness(protocol, {
    ...counts,
    matchedPairs: 13,
  });
  prove(
    "readiness_below_minimum_is_unknown_with_reason_never_an_estimate",
    belowMinimum.state === "UNKNOWN" &&
      typeof belowMinimum.reason === "string" &&
      belowMinimum.reason.startsWith("below_declared_minimum_sample") &&
      belowMinimum.usablePairs === 11,
    { readiness: belowMinimum },
  );

  const unknownEvidenceSwallowsUsable = evaluateCapacityResearchReadiness(protocol, {
    ...counts,
    matchedPairs: 13,
    pairsMissingRequiredEvidence: 3,
  });
  prove(
    "readiness_counts_pairs_with_unknown_evidence_as_unusable_not_usable",
    unknownEvidenceSwallowsUsable.state === "UNKNOWN" &&
      unknownEvidenceSwallowsUsable.usablePairs === 10 &&
      unknownEvidenceSwallowsUsable.pairsUnknownEvidence === 3,
    { readiness: unknownEvidenceSwallowsUsable },
  );

  const noPairs = evaluateCapacityResearchReadiness(protocol, {
    ...counts,
    matchedPairs: 0,
    pairsMissingRequiredEvidence: 0,
  });
  prove(
    "readiness_without_matched_pairs_is_unknown_no_matched_pairs",
    noPairs.state === "UNKNOWN" && noPairs.reason === "no_matched_pairs_within_declared_matching_cell",
    { readiness: noPairs },
  );

  const compatibilityOnly = evaluateCapacityResearchReadiness(protocol, {
    observations: 9,
    machineCompatibilityObservations: 9,
    matchedPairs: 0,
    pairsMissingRequiredEvidence: 0,
  });
  prove(
    "compatibility_only_data_cannot_support_team_statistics",
    compatibilityOnly.state === "UNKNOWN" &&
      compatibilityOnly.reason.startsWith("only_machine_compatibility_observations"),
    { readiness: compatibilityOnly },
  );

  prove(
    "readiness_validates_count_shapes_fail_closed",
    throws(() =>
      evaluateCapacityResearchReadiness(protocol, {
        ...counts,
        machineCompatibilityObservations: 41,
      }),
    ) &&
      throws(() =>
        evaluateCapacityResearchReadiness(protocol, {
          ...counts,
          pairsMissingRequiredEvidence: 15,
        }),
      ) &&
      throws(() =>
        evaluateCapacityResearchReadiness(protocol, {
          ...counts,
          matchedPairs: 14.5,
        }),
      ),
    {},
  );
}

// ---------------------------------------------------------------------------
// 3. Finding envelope — disclosure-complete output, forbidden decisions and
//    causal claims fail closed, UNKNOWN findings carry no invented numbers.
// ---------------------------------------------------------------------------
{
  const protocol = validateCapacityResearchProtocol(validProtocolInput());
  const counts: CapacityResearchSampleCounts = {
    observations: 40,
    machineCompatibilityObservations: 6,
    matchedPairs: 14,
    pairsMissingRequiredEvidence: 2,
  };
  const findingInput: CapacityResearchFindingInput = {
    claimClass: "suggestive",
    confidence: "low",
    confidenceBasis: "observational pairing inside one work-type cell; unmeasured confounders remain",
    supportedDecisions: ["hypothesis_generation", "study_design_followup"],
    asOf: NOW,
  };

  const finding = buildCapacityResearchFinding(protocol, counts, findingInput);
  prove(
    "finding_discloses_scope_source_dates_confidence_limitations",
    finding.schema === CAPACITY_RESEARCH_FINDING_SCHEMA &&
      finding.status === "estimated" &&
      finding.claimClass === "suggestive" &&
      finding.scope.workType === "bugfix_pr" &&
      finding.sourceDates.windowSince === "2026-08-01T00:00:00.000Z" &&
      finding.sourceDates.generatedAsOf === NOW &&
      finding.confidence!.level === "low" &&
      finding.limitations === CAPACITY_RESEARCH_LIMITATIONS,
    { finding },
  );
  prove(
    "finding_sample_reports_compatibility_exclusions_and_unknown_evidence_visibly",
    finding.sample !== null &&
      finding.sample.usablePairs === 12 &&
      finding.sample.excludedMachineCompatibilityObservations === 6 &&
      finding.sample.pairsUnknownEvidence === 2,
    { sample: finding.sample },
  );

  // Acceptance: forbidden decisions are structurally unsupported — even when
  // the caller tries to sneak one in, and even when they try to trim the
  // unsupported list.
  const forbiddenAttempts = [...CAPACITY_RESEARCH_FORBIDDEN_DECISIONS];
  const rejectedForbidden = forbiddenAttempts.filter((decision) =>
    throws(() =>
      buildCapacityResearchFinding(protocol, counts, {
        ...findingInput,
        supportedDecisions: [decision],
      }),
    ),
  );
  prove(
    "finding_rejects_every_forbidden_decision_as_supported",
    rejectedForbidden.length === forbiddenAttempts.length,
    { rejectedForbidden, attempted: forbiddenAttempts },
  );
  prove(
    "finding_unsupported_decisions_are_complete_and_caller_proof",
    finding.unsupportedDecisions.includes("rankings") &&
      finding.unsupportedDecisions.includes("coaching_scores") &&
      finding.unsupportedDecisions.includes("compensation") &&
      finding.unsupportedDecisions.includes("discipline") &&
      finding.unsupportedDecisions.includes("interventions") &&
      finding.unsupportedDecisions.includes("d3_d4_hosted_analytics") &&
      finding.unsupportedDecisions.includes("individual_performance_verdicts") &&
      finding.unsupportedDecisions.includes("team_statistical_conclusions_from_compatibility_data") &&
      finding.supportedDecisions.every(
        (decision) => !finding.unsupportedDecisions.includes(decision),
      ),
    { unsupported: finding.unsupportedDecisions },
  );

  prove(
    "finding_rejects_causal_and_overconfident_claims",
    throws(() =>
      buildCapacityResearchFinding(protocol, counts, {
        ...findingInput,
        claimClass: "causal" as never,
      }),
    ) &&
      throws(() =>
        buildCapacityResearchFinding(protocol, counts, {
          ...findingInput,
          claimClass: "associated" as never,
        }),
      ) &&
      throws(() =>
        buildCapacityResearchFinding(protocol, counts, {
          ...findingInput,
          confidence: "high" as never,
        }),
      ),
    {},
  );

  // Acceptance: insufficient evidence remains UNKNOWN with no numbers.
  const starved = buildCapacityResearchFinding(
    protocol,
    { observations: 9, machineCompatibilityObservations: 9, matchedPairs: 0, pairsMissingRequiredEvidence: 0 },
    findingInput,
  );
  prove(
    "insufficient_evidence_finding_is_unknown_with_null_claim_and_empty_support",
    starved.status === "UNKNOWN" &&
      starved.claimClass === null &&
      starved.confidence === null &&
      starved.sample === null &&
      starved.supportedDecisions.length === 0 &&
      typeof starved.reason === "string" &&
      starved.reason.startsWith("only_machine_compatibility_observations") &&
      starved.unsupportedDecisions.length === finding.unsupportedDecisions.length &&
      starved.scope.workType === "bugfix_pr",
    { finding: starved },
  );
}

// ---------------------------------------------------------------------------
// 4. Static doctrine integration — the reachability gate covers the new
//    module; the real tree stays clean.
// ---------------------------------------------------------------------------
{
  const report = scanCapacityDoctrine(root);
  prove(
    "real_tree_has_zero_capacity_doctrine_offenders_with_research_module_present",
    report.offendingImporters.length === 0,
    { offenders: report.offendingImporters, scannedFiles: report.scannedFiles.length },
  );
  prove(
    "research_module_is_a_capacity_symbol_provider_under_the_gate",
    report.capacityModules.some((file) => file.endsWith("capacity-research.ts")) &&
      report.capacityModules.some((file) => file.endsWith(path.join("src", "capacity.ts"))),
    { capacityModules: report.capacityModules.map((file) => path.basename(file)) },
  );

  // FALSIFICATION FIXTURES — decision surfaces consuming capacity-research
  // symbols must turn the scan RED, by direct specifier, through the barrel,
  // and transitively. Negative control: an unrelated helper consumer stays
  // green while merely reaching the module.
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capacity-research-"));
  try {
    const sharedSrc = path.join(sandbox, "packages/shared/src");
    fs.mkdirSync(sharedSrc, { recursive: true });
    fs.writeFileSync(
      path.join(sharedSrc, "capacity-research.ts"),
      [
        'export const CAPACITY_RESEARCH_PROTOCOL_SCHEMA = "plimsoll.capacity-research-protocol.v1" as const;',
        "export function validateCapacityResearchProtocol(): string { return CAPACITY_RESEARCH_PROTOCOL_SCHEMA; }",
        "",
      ].join("\n"),
    );
    fs.writeFileSync(
      path.join(sharedSrc, "index.ts"),
      ['export * from "./capacity-research";', ""].join("\n"),
    );
    fs.writeFileSync(
      path.join(sharedSrc, "harmless.ts"),
      "export function harmlessHelper(): number { return 7; }\n",
    );
    const exportFn = (name: string, body: string): string =>
      ["export", "function"].join(" ") + ` ${name}(): ${body}`;
    // Offender A: decision surface by FILE NAME, direct specifier import.
    fs.writeFileSync(
      path.join(sharedSrc, "metric-registry.ts"),
      [
        'import { validateCapacityResearchProtocol } from "./capacity-research";',
        exportFn("registerMetric", `string { return validateCapacityResearchProtocol(); }`),
        "",
      ].join("\n"),
    );
    // Offender B: decision surface by CONTENT ONLY, smuggled via the barrel.
    fs.writeFileSync(
      path.join(sharedSrc, "summarizer.ts"),
      [
        'import { validateCapacityResearchProtocol } from "./index";',
        exportFn("scoreTechnique", `string { return validateCapacityResearchProtocol(); }`),
        "",
      ].join("\n"),
    );
    // Offender C: transitive consumer that never names a capacity symbol.
    fs.mkdirSync(path.join(sandbox, "packages/cli"), { recursive: true });
    fs.writeFileSync(
      path.join(sandbox, "packages/cli/dispatcher.ts"),
      [
        'import { scoreTechnique } from "../shared/src/summarizer";',
        exportFn("routeTask", `string { return scoreTechnique(); }`),
        "",
      ].join("\n"),
    );
    // Negative control: reaches the module through the barrel but consumes
    // only an unrelated helper.
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
      "falsification_decision_surface_importing_research_module_directly_turns_red",
      offenderPaths.some((file) => file.endsWith("metric-registry.ts")),
      { offenders: bypass.offendingImporters },
    );
    prove(
      "falsification_content_detected_decision_surface_via_barrel_turns_red",
      offenderPaths.some((file) => file.endsWith("summarizer.ts")),
      { offenders: bypass.offendingImporters },
    );
    prove(
      "falsification_transitive_consumer_of_research_symbols_turns_red",
      offenderPaths.some((file) => file.endsWith("dispatcher.ts")),
      { offenders: bypass.offendingImporters },
    );
    prove(
      "falsification_negative_control_unrelated_helper_consumer_stays_green",
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

  // No capacity doctrine module may import scoring/performance/learning
  // surfaces, and no capacity module may export decision verbs.
  const capacitySource = report.capacityModules
    .filter((file) => file.includes(`${path.sep}src${path.sep}`))
    .map((file) => fs.readFileSync(file, "utf8"))
    .join("\n");
  const importsDecisionSurface =
    /from\s+"\.[^"]*(?:performance-layer|metric-registry|learning-evidence)"/.test(capacitySource);
  prove(
    "static_proof_capacity_modules_import_no_scoring_or_performance_layer",
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
}

console.log(
  JSON.stringify({ schema: "plimsoll.capacity-research-proof.v1", passed: checks.length, checks }, null, 2),
);
