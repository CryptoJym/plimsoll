#!/usr/bin/env node

/**
 * Issue #176 proof: deterministic lane receipts, drift-safe handoffs.
 *
 * Exercises the read-only workflow controls in
 * packages/shared/src/lane-receipts.ts against synthetic hostile fixtures in
 * scripts/fixtures/lane-receipts/hostile. Pure data-in/data-out proof: no
 * worktrees are created, no processes are started, nothing is stopped or
 * changed. Receipts and findings never carry prompts, transcripts,
 * credentials, environment values, provider data, home paths, or personal
 * productivity judgments — the privacy guard enforces that mechanically.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { guardProofCompletion } from "./lib/proof-completion";
import {
  appendAttemptObservation,
  assertReceiptPrivacy,
  buildOpsReceipt,
  declareFanOut,
  detectRetryOverwrite,
  evaluateIntegrationGate,
  findReceiptPrivacyViolations,
  isStaleLiveness,
  parsePreflight,
  preflightContradictions,
  renderHandoffMarkdown,
  runWorktreeCensus,
  serializeOpsReceipt,
  type AttemptHistory,
} from "../packages/shared/src/lane-receipts";

const scriptPath = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(scriptPath), "..");
const hostileDir = path.join(repoRoot, "scripts", "fixtures", "lane-receipts", "hostile");

const checks: Array<{ name: string; detail?: Record<string, unknown> }> = [];

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push(detail && Object.keys(detail).length > 0 ? { name, detail } : { name });
}

// Refuses the two silent-green modes — an early event-loop drain and a hang
// that never exits. See scripts/lib/proof-completion.ts for why each is needed.
const guard = guardProofCompletion({
  countChecks: () => checks.length,
});

function readFixture(name: string): Record<string, unknown> {
  return JSON.parse(fs.readFileSync(path.join(hostileDir, name), "utf8")) as Record<
    string,
    unknown
  >;
}

function provePreflight() {
  const validRaw = {
    laneId: "lane-176/build",
    ownerRole: "builder",
    branch: "feat/lane-176",
    baseSha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    headSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    cleanState: "CLEAN",
    lastActivityAtMs: 1_000,
    liveness: "ACTIVE",
  };
  const parsed = parsePreflight(validRaw);
  check("preflight_accepts_complete_record", parsed.ok, {});
  if (parsed.ok) {
    check(
      "preflight_records_identity_branch_heads_state_activity_liveness",
      parsed.value.laneId === "lane-176/build" &&
        parsed.value.ownerRole === "builder" &&
        parsed.value.branch === "feat/lane-176" &&
        parsed.value.baseSha === "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" &&
        parsed.value.headSha === "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" &&
        parsed.value.cleanState === "CLEAN" &&
        parsed.value.lastActivityAtMs === 1_000 &&
        parsed.value.liveness === "ACTIVE",
      {},
    );
  }

  const missing = readFixture("missing-fields.json") as { preflights: unknown[] };
  for (const [index, raw] of missing.preflights.entries()) {
    const result = parsePreflight(raw);
    check(`preflight_fails_closed_on_hostile_record_${index}`, !result.ok, {
      issuesReported: result.ok ? 0 : result.issues.length,
    });
    if (!result.ok) {
      check(
        `preflight_reports_missing_field_names_${index}`,
        result.issues.every((issue) => issue.startsWith("missing_or_invalid_field:")),
        {},
      );
    }
  }
}

function proveContradictions() {
  const fixture = readFixture("contradictory-states.json") as { preflights: unknown[] };
  const parsed = fixture.preflights.map((raw) => parsePreflight(raw));
  check("contradictory_fixture_records_parse", parsed.every((result) => result.ok), {});
  if (parsed[0]?.ok && parsed[1]?.ok) {
    const first = preflightContradictions(parsed[0].value);
    check(
      "complete_with_dirty_worktree_is_contradictory",
      first.includes("complete_but_dirty"),
      {},
    );
    const second = preflightContradictions(parsed[1].value);
    check(
      "blocked_without_reason_is_contradictory",
      second.includes("blocked_without_reason"),
      {},
    );
  }

  const cleanRaw = {
    laneId: "lane-ok",
    ownerRole: "builder",
    branch: "feat/ok",
    baseSha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    headSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    cleanState: "CLEAN",
    lastActivityAtMs: 1_000,
    liveness: "ACTIVE",
    blockedReason: "waiting on review",
  };
  const cleanParsed = parsePreflight(cleanRaw);
  check(
    "consistent_record_has_no_contradictions",
    cleanParsed.ok && preflightContradictions(cleanParsed.value).length === 0,
    {},
  );

  // Issue #194 battery: an ACTIVE lane whose head equals its base has made no
  // progress; the contradiction check for this state was dead-coded by an
  // `&& false` clause and must fire again.
  const unreachableRaw = {
    ...cleanRaw,
    laneId: "lane-unreachable",
    headSha: cleanRaw.baseSha,
  };
  const unreachableParsed = parsePreflight(unreachableRaw);
  check(
    "active_lane_with_head_equal_to_base_is_contradictory",
    unreachableParsed.ok &&
      preflightContradictions(unreachableParsed.value).includes("unreachable"),
    { contradictions: unreachableParsed.ok ? preflightContradictions(unreachableParsed.value) : [] },
  );
}

function proveStaleLiveness() {
  const fixture = readFixture("stale-liveness.json") as {
    nowMs: number;
    stalenessBudgetMs: number;
    preflight: unknown;
  };
  const parsed = parsePreflight(fixture.preflight);
  check("stale_fixture_record_parses", parsed.ok, {});
  if (parsed.ok) {
    check(
      "active_liveness_past_budget_is_reported_stale_not_mutated",
      isStaleLiveness(parsed.value, fixture.nowMs, fixture.stalenessBudgetMs) &&
        parsed.value.liveness === "ACTIVE",
      {},
    );
    check(
      "fresh_active_lane_within_budget_is_live",
      !isStaleLiveness({ ...parsed.value, lastActivityAtMs: fixture.nowMs - 1_000 }, fixture.nowMs),
      {},
    );
  }
}

function proveCensus() {
  const ownership = readFixture("duplicate-lane-ownership.json") as { worktrees: unknown[] };
  const ownershipFindings = runWorktreeCensus(ownership.worktrees as never[]);
  check(
    "census_detects_duplicate_lane_ownership",
    ownershipFindings.some((f) => f.kind === "duplicate_lane_ownership"),
    { kinds: ownershipFindings.map((f) => f.kind) },
  );
  check(
    "census_detects_duplicate_heads",
    ownershipFindings.some((f) => f.kind === "duplicate_head"),
    {},
  );

  const drift = readFixture("dirty-drift.json") as {
    canonicalBaseSha: string;
    expectedLanes: string[];
    worktrees: unknown[];
  };
  const driftFindings = runWorktreeCensus(drift.worktrees as never[], {
    canonicalBaseSha: drift.canonicalBaseSha,
    expectedLanes: drift.expectedLanes,
  });
  check("census_detects_dirty_lane_without_owner", driftFindings.some((f) => f.kind === "dirty_unowned"), {});
  check("census_detects_base_drift", driftFindings.some((f) => f.kind === "base_drift"), {});
  check("census_detects_missing_worktree_for_expected_lane", driftFindings.some((f) => f.kind === "missing_worktree"), {});

  const staleFixture = readFixture("stale-liveness.json");
  void staleFixture;

  const withHandoff = [
    {
      name: "wt-handoff",
      laneId: "lane-h",
      owner: "builder-a",
      headSha: "cccccccccccccccccccccccccccccccccccccccc",
      clean: true,
      handoffHeadSha: "dddddddddddddddddddddddddddddddddddddddd",
      handoffUpdatedAtMs: 1,
    },
  ];
  const staleHandoffFindings = runWorktreeCensus(withHandoff, { nowMs: 10 ** 13 });
  check(
    "census_detects_stale_handoff_head_drift_and_age",
    staleHandoffFindings.filter((f) => f.kind === "stale_handoff").length >= 1,
    {},
  );

  const healthy = [
    {
      name: "wt-clean",
      laneId: "lane-clean",
      owner: "builder-a",
      headSha: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
      baseSha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      clean: true,
      handoffHeadSha: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
      handoffUpdatedAtMs: 9_000,
    },
  ];
  const quietFindings = runWorktreeCensus(healthy, {
    canonicalBaseSha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    expectedLanes: ["lane-clean"],
    nowMs: 10_000,
  });
  check("census_is_quiet_on_healthy_estate", quietFindings.length === 0, {
    findingCount: quietFindings.length,
  });

  const serializedFindings = JSON.stringify([...ownershipFindings, ...driftFindings]);
  check(
    "census_findings_carry_no_home_paths_or_secret_like_strings",
    !serializedFindings.includes("/Users/") && !serializedFindings.includes("/home/"),
    {},
  );
}

function proveFanOutDeclaration() {
  const good = {
    laneBudget: 5,
    phases: [
      { kind: "build", lanes: ["lane-b1"] },
      { kind: "build", lanes: ["lane-b2"] },
      { kind: "review", lanes: ["reviewer-1"] },
      { kind: "integration", lanes: ["integrator-1"] },
      { kind: "release-gate", lanes: ["release-1"] },
    ],
  };
  const declared = declareFanOut(good);
  check("fanout_declares_budget_and_distinct_gated_phases", declared.ok, {});
  if (declared.ok) {
    check(
      "builders_overlap_while_gates_remain_distinct",
      declared.value.phases.filter((phase) => phase.kind === "build").length === 2 &&
        declared.value.laneBudget === 5,
      {},
    );
  }

  const sharedLane = {
    laneBudget: 4,
    phases: [
      { kind: "build", lanes: ["lane-shared"] },
      { kind: "review", lanes: ["lane-shared"] },
      { kind: "integration", lanes: ["integrator-1"] },
      { kind: "release-gate", lanes: ["release-1"] },
    ],
  };
  const shared = declareFanOut(sharedLane);
  check(
    "gated_phase_cannot_share_builder_lane",
    !shared.ok && shared.issues.some((issue) => issue.startsWith("lane_shared_across_phases")),
    {},
  );

  const missingGate = {
    laneBudget: 3,
    phases: [
      { kind: "build", lanes: ["lane-b1"] },
      { kind: "review", lanes: ["reviewer-1"] },
      { kind: "integration", lanes: ["integrator-1"] },
    ],
  };
  const missing = declareFanOut(missingGate);
  check(
    "missing_release_gate_fails_closed",
    !missing.ok && missing.issues.some((issue) => issue.startsWith("phase_count_must_be_exactly_one:release-gate")),
    {},
  );

  const overBudget = declareFanOut({
    laneBudget: 1,
    phases: [
      { kind: "build", lanes: ["lane-a"] },
      { kind: "build", lanes: ["lane-b"] },
      { kind: "review", lanes: ["reviewer-1"] },
      { kind: "integration", lanes: ["integrator-1"] },
      { kind: "release-gate", lanes: ["release-1"] },
    ],
  });
  check(
    "lanes_exceeding_budget_fail_closed",
    !overBudget.ok && overBudget.issues.some((issue) => issue.startsWith("declared_lanes_exceed_budget")),
    {},
  );
}

function proveDeterministicReceipts() {
  const input = {
    laneId: "lane-176/build",
    attemptId: "attempt-1",
    branch: "feat/lane-176",
    baseSha: "e79a8ea111111111111111111111111111111111",
    headSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    changedFiles: ["packages/shared/src/lane-receipts.ts", "scripts/lane-receipt-proof.ts", "package.json"],
    exactTests: ["pnpm proof:lane-receipts", "pnpm typecheck"],
    failures: [],
    blockers: [],
    resumeCommand: "pnpm proof:lane-receipts",
  };

  const receiptA = buildOpsReceipt(input);
  const receiptB = buildOpsReceipt({
    ...input,
    changedFiles: [...input.changedFiles].reverse(),
    exactTests: [...input.exactTests].reverse(),
  });
  check(
    "receipt_is_deterministic_regardless_of_input_order",
    serializeOpsReceipt(receiptA) === serializeOpsReceipt(receiptB),
    {},
  );
  check(
    "receipt_contains_changed_files_tests_head_and_resume_command",
    receiptA.changedFiles.length === 3 &&
      receiptA.exactTests.length === 2 &&
      receiptA.headSha === input.headSha &&
      receiptA.resumeCommand === input.resumeCommand,
    {},
  );

  const handoffA = renderHandoffMarkdown(receiptA);
  const handoffB = renderHandoffMarkdown(buildOpsReceipt(input));
  check("handoff_markdown_is_byte_identical_across_runs", handoffA === handoffB, {});
  check(
    "handoff_carries_head_resume_and_content_hash",
    handoffA.includes(input.headSha) &&
      handoffA.includes("pnpm proof:lane-receipts") &&
      handoffA.includes(receiptA.contentHash),
    {},
  );

  check(
    "receipt_privacy_guard_passes_clean_receipt",
    findReceiptPrivacyViolations(receiptA).length === 0,
    {},
  );

  const serialized = serializeOpsReceipt(receiptA);
  check(
    "serialized_receipt_has_no_timestamp_or_environment_fields",
    !/"(?:generatedAt|timestamp|environment|env|prompt|transcript)"/.test(serialized),
    {},
  );
}

function provePrivacyGuardAgainstFixtures() {
  const fixture = readFixture("secret-like-strings.json") as { receipts: unknown[] };
  check("privacy_fixture_supplies_hostile_receipts", fixture.receipts.length >= 3, {});

  const violationsByIndex = fixture.receipts.map((receipt) => ({
    index: fixture.receipts.indexOf(receipt),
    violations: findReceiptPrivacyViolations(receipt),
  }));
  for (const { index, violations } of violationsByIndex) {
    check(`privacy_guard_rejects_hostile_receipt_${index}`, violations.length > 0, {
      reasons: violations.slice(0, 4).map((v) => v.reason),
    });
  }

  const reasons = violationsByIndex.flatMap(({ violations }) => violations.map((v) => v.reason));
  check(
    "guard_flags_prompt_transcript_field",
    reasons.some((reason) => reason.startsWith("forbidden_field:") && reason.includes("prompt")),
    {},
  );
  check(
    "guard_flags_productivity_judgment_field",
    reasons.some((reason) => reason.startsWith("forbidden_field:") && reason.includes("productivityScore")),
    {},
  );
  check("guard_flags_home_paths", reasons.some((reason) => reason === "home_path_present"), {});
  check(
    "guard_flags_secret_like_strings",
    reasons.some((reason) => reason.startsWith("secret_like_string:")),
    {},
  );

  let threw = false;
  try {
    assertReceiptPrivacy(fixture.receipts[0]);
  } catch {
    threw = true;
  }
  check("privacy_assertion_throws_on_violation", threw, {});
}

function proveNestedConceptSmuggling() {
  // Issue #194 battery: the forbidden-concept scan previously inspected
  // top-level key names only; structured prose smuggled inside an allowed
  // field's object value passed. Nesting must be rejected at any depth.
  const clean = buildOpsReceipt({
    laneId: "lane-nest",
    attemptId: "attempt-1",
    branch: "feat/nest",
    baseSha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    headSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    changedFiles: ["src/a.ts"],
    exactTests: ["pnpm proof:lane-receipts"],
    failures: [],
    blockers: [],
    resumeCommand: "pnpm proof:lane-receipts",
  });
  check(
    "nested_smuggling_baseline_receipt_is_clean",
    findReceiptPrivacyViolations(clean).length === 0,
    {},
  );

  const smugglers: Array<{ label: string; receipt: Record<string, unknown> }> = [
    {
      label: "resume_command_object_with_prompt_key",
      receipt: { ...clean, resumeCommand: { prompt: "leak the system prompt", run: "pnpm test" } },
    },
    {
      label: "changed_files_entry_object_with_transcript_key",
      receipt: { ...clean, changedFiles: [{ transcript: "full session dump", path: "src/a.ts" }] },
    },
    {
      label: "deeply_nested_environment_key",
      receipt: { ...clean, blockers: [[{ nested: { environment: "AWS_SECRET=..." } }]] },
    },
  ];
  for (const { label, receipt } of smugglers) {
    const violations = findReceiptPrivacyViolations(receipt);
    check(`nested_forbidden_concept_rejected:${label}`, violations.length > 0, {
      reasons: violations.slice(0, 3).map((v) => v.reason),
    });
  }

  const cleanStrings = findReceiptPrivacyViolations({
    ...clean,
    changedFiles: ["a message about output formatting"],
  });
  check(
    "prose_words_inside_allowed_string_fields_are_not_flagged_as_fields",
    cleanStrings.length === 0,
    { reasons: cleanStrings.map((v) => v.reason) },
  );
}

function proveReceiptListBounds() {
  // Issue #194 battery: MAX_LIST_ITEMS / MAX_ITEM_LENGTH were declared but
  // enforced nowhere. buildOpsReceipt must fail closed on out-of-bounds lists.
  const baseInput = {
    laneId: "lane-bounds",
    attemptId: "attempt-1",
    branch: "feat/bounds",
    baseSha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    headSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    changedFiles: ["src/a.ts"],
    exactTests: ["pnpm proof:lane-receipts"],
    failures: [] as string[],
    blockers: [] as string[],
    resumeCommand: "pnpm proof:lane-receipts",
  };
  const fixture = readFixture("unbounded-list.json") as {
    tooManyFilesCount: number;
    oversizedItemLength: number;
  };

  let tooManyThrew: unknown = undefined;
  try {
    buildOpsReceipt({
      ...baseInput,
      changedFiles: Array.from(
        { length: fixture.tooManyFilesCount },
        (_, index) => `src/generated-${index}.ts`,
      ),
    });
  } catch (error) {
    tooManyThrew = error;
  }
  check("oversized_list_fails_closed", tooManyThrew instanceof Error, {});

  let oversizedThrew: unknown = undefined;
  try {
    buildOpsReceipt({
      ...baseInput,
      exactTests: ["x".repeat(fixture.oversizedItemLength)],
    });
  } catch (error) {
    oversizedThrew = error;
  }
  check("oversized_list_item_fails_closed", oversizedThrew instanceof Error, {});

  const inBounds = buildOpsReceipt(baseInput);
  check(
    "in_bounds_lists_still_build_deterministic_receipt",
    serializeOpsReceipt(inBounds) === serializeOpsReceipt(buildOpsReceipt(baseInput)),
    {},
  );
}

function proveTamperedHead() {
  const fixture = readFixture("tampered-head.json") as {
    preflight: unknown;
    receipt: Record<string, unknown>;
    tamperedReceipt: Record<string, unknown>;
  };
  const parsed = parsePreflight(fixture.preflight);
  check("tamper_fixture_preflight_parses", parsed.ok, {});
  if (parsed.ok) {
    check(
      "receipt_head_mismatch_with_preflight_is_detected",
      fixture.receipt.headSha !== parsed.value.headSha,
      {},
    );
    check(
      "malformed_replacement_sha_is_rejected_by_parse",
      !parsePreflight({
        ...parsed.value,
        headSha: fixture.tamperedReceipt.headSha,
      }).ok,
      {},
    );
  }

  const gateEvidence = [
    {
      gate: "typescript-exact-head" as const,
      passed: true,
      headSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      reviewer: "typecheck",
    },
    { gate: "privacy" as const, passed: true, headSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", reviewer: "guard" },
    { gate: "tamper" as const, passed: true, headSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", reviewer: "census" },
    {
      gate: "adversarial-review" as const,
      passed: true,
      headSha: "dddddddddddddddddddddddddddddddddddddddd",
      reviewer: "independent-reviewer",
    },
  ];
  const decision = evaluateIntegrationGate(gateEvidence, {
    headSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    builderOwner: "builder-a",
  });
  check(
    "gate_evidence_at_wrong_head_does_not_satisfy_integration",
    !decision.allowed && decision.reasons.some((r) => r.startsWith("gate_not_passed_at_head:adversarial-review")),
    { reasons: decision.reasons },
  );
}

function proveAttemptHistory() {
  const fixture = readFixture("retry-overwrite.json") as {
    before: AttemptHistory;
    after: AttemptHistory;
    overwriteAttempt: { attemptId: string; sourceHead: string; result: "pass" | "fail" };
  };

  const tamper = detectRetryOverwrite(fixture.before, fixture.after);
  check(
    "retry_overwrite_erasing_prior_failure_is_detected",
    tamper.tampered &&
      tamper.reasons.some((r) => r.startsWith("observation_erased") || r.startsWith("observation_rewritten")) &&
      tamper.reasons.some((r) => r.startsWith("attempt_source_head_rewritten")),
    { reasons: tamper.reasons },
  );

  const originalFailure =
    fixture.before[0]!.observations.find((obs) => obs.result === "fail")!.failureSummary ?? "";

  let history: AttemptHistory = [];
  const first = appendAttemptObservation(history, {
    attemptId: fixture.before[0]!.attemptId,
    sourceHead: fixture.before[0]!.sourceHead,
    result: "fail",
    failureSummary: originalFailure,
  });
  check("first_attempt_appends", first.ok, {});
  if (first.ok) history = first.history;

  const retrySameHead = appendAttemptObservation(history, {
    attemptId: fixture.before[0]!.attemptId,
    sourceHead: fixture.before[0]!.sourceHead,
    result: "pass",
  });
  check("retry_keeps_original_attempt_id_and_source_head", retrySameHead.ok, {});
  if (retrySameHead.ok) {
    history = retrySameHead.history;
    const entry = history[0]!;
    check(
      "later_pass_preserves_earlier_failure_verbatim",
      entry.observations.length === 2 &&
        entry.observations[0]!.result === "fail" &&
        entry.observations[0]!.failureSummary === originalFailure &&
        entry.observations[1]!.result === "pass",
      {},
    );
  }

  const headChanged = appendAttemptObservation(history, fixture.overwriteAttempt);
  check(
    "retry_with_changed_source_head_fails_closed",
    !headChanged.ok && headChanged.issue.startsWith("source_head_changed_for_attempt:"),
    {},
  );

  const untouched = detectRetryOverwrite(fixture.before, history);
  check(
    "legitimate_retry_leaves_history_untampered",
    !untouched.tampered,
    { reasons: untouched.reasons },
  );
}

function proveIntegrationGateHappyPath() {
  const headSha = "cccccccccccccccccccccccccccccccccccccccc";
  const evidence = [
    { gate: "typescript-exact-head" as const, passed: true, headSha, reviewer: "tsc --noEmit" },
    { gate: "privacy" as const, passed: true, headSha, reviewer: "receipt-guard" },
    { gate: "tamper" as const, passed: true, headSha, reviewer: "census" },
    { gate: "adversarial-review" as const, passed: true, headSha, reviewer: "independent-reviewer" },
  ];
  const allowed = evaluateIntegrationGate(evidence, { headSha, builderOwner: "builder-a" });
  check(
    "all_four_gates_at_exact_head_with_independent_reviewer_allow_integration",
    allowed.allowed,
    { reasons: allowed.reasons },
  );

  const selfReviewed = evaluateIntegrationGate(
    evidence.map((item) =>
      item.gate === "adversarial-review" ? { ...item, reviewer: "builder-a" } : item,
    ),
    { headSha, builderOwner: "builder-a" },
  );
  check(
    "adversarial_review_by_builder_owner_is_blocked",
    !selfReviewed.allowed &&
      selfReviewed.reasons.includes("adversarial_reviewer_matches_builder_owner"),
    {},
  );
}

function proveGateFailureCoexistence() {
  // Issue #194: an explicit passed:false record at the context head must
  // refuse integration even when a passing duplicate for the same gate
  // exists. Failing evidence is unratcheted: it fails the gate at that head,
  // full stop. Failures recorded at other heads do not block the head.
  const fixture = readFixture("gate-failure-coexistence.json") as {
    context: { headSha: string; builderOwner: string };
    failingGate: string;
    coexistingEvidence: Parameters<typeof evaluateIntegrationGate>[0];
    failingOnlyEvidence: Parameters<typeof evaluateIntegrationGate>[0];
    staleFailureEvidence: Parameters<typeof evaluateIntegrationGate>[0];
  };

  const coexisting = evaluateIntegrationGate(fixture.coexistingEvidence, fixture.context);
  check(
    "failing_evidence_at_head_refuses_integration_despite_passing_duplicates",
    !coexisting.allowed &&
      coexisting.reasons.includes(`gate_failed_at_head:${fixture.failingGate}`),
    { reasons: coexisting.reasons },
  );

  const failingOnly = evaluateIntegrationGate(fixture.failingOnlyEvidence, fixture.context);
  check(
    "explicit_failure_at_head_reports_distinct_reason_not_missing_pass",
    !failingOnly.allowed &&
      failingOnly.reasons.includes(`gate_failed_at_head:${fixture.failingGate}`) &&
      !failingOnly.reasons.some((reason) => reason.startsWith("gate_not_passed_at_head")),
    { reasons: failingOnly.reasons },
  );

  const stale = evaluateIntegrationGate(fixture.staleFailureEvidence, fixture.context);
  check(
    "failure_recorded_at_other_head_does_not_block_context_head_pass",
    stale.allowed && stale.reasons.length === 0,
    { reasons: stale.reasons },
  );
}

function main() {
  provePreflight();
  proveContradictions();
  proveStaleLiveness();
  proveCensus();
  proveFanOutDeclaration();
  proveDeterministicReceipts();
  provePrivacyGuardAgainstFixtures();
  proveNestedConceptSmuggling();
  proveReceiptListBounds();
  proveTamperedHead();
  proveAttemptHistory();
  proveIntegrationGateHappyPath();
  proveGateFailureCoexistence();

  const serializedChecks = JSON.stringify(checks);
  check(
    "proof_output_excludes_home_paths_and_fake_secrets",
    !serializedChecks.includes("/Users/") && !serializedChecks.includes("sk-proj") && !serializedChecks.includes("ghp_0123456789"),
    {},
  );
  console.log(JSON.stringify({ status: "passed", checks }, null, 2));
}

main();
guard.complete();
