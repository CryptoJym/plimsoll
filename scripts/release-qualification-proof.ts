/**
 * Proof for issue #171 — exact-artifact release qualification and canary
 * receipts.
 *
 * Covers: builder handoff validation and determinism; the four-stage chain
 * (handoff → independent review → integration rerun → owner approval of ONE
 * rollout); reset-on-any-change semantics for code/config/dependency/schema/
 * digest drift; canary isolated-home read-only compatibility receipts
 * (attestations must be literally true, machine identity pseudonymous);
 * Studio 3 preflight staying BLOCKED_MISSING_SSH_MAPPING without an SSH
 * mapping; the canary-vs-stage-gated scope gate; and the performance-
 * evidence doctrine guard (SSH/reachability/quota/event-volume/working-hours
 * stay operational context, never employee-performance evidence).
 *
 * Uses only repository fixtures and in-memory data. Run:
 *   pnpm proof:release-qualification
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import { guardProofCompletion } from "./lib/proof-completion";
import {
  CANARY_ATTESTATIONS,
  CANARY_COMPATIBILITY_RECEIPT_SCHEMA,
  CANARY_SCOPES,
  GATED_FLEET_STAGE_ISSUES,
  STUDIO3_PREFLIGHT_SCHEMA,
  assertNoPerformanceEvidence,
  attachIntegrationRerun,
  attachReview,
  canaryProgressAllowed,
  canonicalJson,
  consumeOwnerRollout,
  evaluateArtifactReleaseGate,
  evaluateScopeClaim,
  findPerformanceEvidenceViolations,
  grantOwnerApproval,
  parseIntegrationRerun,
  parseBuilderHandoff,
  parseCompatibilityReceipt,
  parseReviewApproval,
  parseStudio3Preflight,
  startReleaseChain,
  type BuilderHandoff,
} from "../packages/shared/src/index";

const root = process.cwd();
const fixtureRoot = path.join(root, "scripts/fixtures/release-qualification");

function readFixture(rel: string): Record<string, unknown> {
  return JSON.parse(fs.readFileSync(path.join(fixtureRoot, rel), "utf8")) as Record<string, unknown>;
}

type Check = { name: string; detail: Record<string, unknown> };
const checks: Check[] = [];

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}): void {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

// Refuses the two silent-green modes — an early event-loop drain and a hang
// that never exits. See scripts/lib/proof-completion.ts for why each is needed.
const guard = guardProofCompletion({
  countChecks: () => checks.length,
});

const DIGEST_B = "sha256:907d0d1224d6155ee6ed4d0d324a16a58539839915a46179c592a3fa2bb4dd08";
const CONFIG_FP_B = "sha256:5b9f51661179d78ccb1335a3076d0a4a9724556444e325fad7d8754ec93ed75e";
const OTHER_HEAD = "dddddddddddddddddddddddddddddddddddddddd";

// ---------------------------------------------------------------------------
// 1. Builder handoff — validation, closed vocabulary, deterministic digest.
// ---------------------------------------------------------------------------
{
  const fixture = readFixture("valid/builder-handoff.json");
  const parsed = parseBuilderHandoff(fixture.handoff);
  check("valid_builder_handoff_parses", parsed.ok === true, { issues: parsed.ok ? [] : parsed.issues });
  if (!parsed.ok) throw new Error("fixture handoff must parse");
  const handoff = parsed.value;

  const reparsed = parseBuilderHandoff(JSON.parse(JSON.stringify(fixture.handoff)));
  check(
    "handoff_digest_deterministic_across_reparses",
    reparsed.ok && reparsed.value.handoffDigest === handoff.handoffDigest,
    {},
  );

  const reorderedInput = {
    ...(fixture.handoff as Record<string, unknown>),
    changedFiles: [
      "scripts/release-qualification-proof.ts",
      "packages/shared/src/release-qualification.ts",
    ],
  };
  const reordered = parseBuilderHandoff(reorderedInput);
  check(
    "changed_file_order_does_not_change_handoff_digest",
    reordered.ok && reordered.value.handoffDigest === handoff.handoffDigest,
    {},
  );

  const tamperedInput = { ...(fixture.handoff as Record<string, unknown>), unresolvedFindings: ["late"] };
  const tampered = parseBuilderHandoff(tamperedInput);
  check(
    "edited_unresolved_findings_change_handoff_digest",
    tampered.ok && tampered.value.handoffDigest !== handoff.handoffDigest,
    {},
  );

  for (const [label, mutate] of [
    ["bad_head_sha", (h: Record<string, unknown>) => ({ ...h, headSha: "nothex" })],
    ["bad_artifact_digest", (h: Record<string, unknown>) => ({ ...h, artifactDigest: "md5:zz" })],
    ["unknown_field", (h: Record<string, unknown>) => ({ ...h, secretNote: "smuggled" })],
    ["unbounded_list", (h: Record<string, unknown>) => ({
      ...h,
      changedFiles: Array.from({ length: 513 }, (_, i) => `file-${i}`),
    })],
    ["test_receipt_missing_suite", (h: Record<string, unknown>) => ({
      ...h,
      testReceipt: { passed: true },
    })],
  ] as const) {
    const result = parseBuilderHandoff(mutate({ ...(fixture.handoff as Record<string, unknown>) }));
    check(`hostile_handoff_${label}_fails_closed`, !result.ok, { issues: result.ok ? [] : result.issues });
  }
}

// ---------------------------------------------------------------------------
// 2. Happy path — all four stages bind to the exact head+digest, gate allows,
//    owner approval is spent by exactly one rollout.
// ---------------------------------------------------------------------------
{
  const fixture = readFixture("valid/builder-handoff.json");
  const parsed = parseBuilderHandoff(fixture.handoff);
  if (!parsed.ok) throw new Error("fixture handoff must parse");
  const handoff: BuilderHandoff = parsed.value;

  const reviewParsed = parseReviewApproval({
    reviewerActor: "independent-reviewer",
    headSha: handoff.headSha,
    artifactDigest: handoff.artifactDigest,
    verdict: "approve",
  });
  const rerunParsed = parseIntegrationRerun({
    integratorActor: "integration-lead",
    headSha: handoff.headSha,
    suite: "pnpm-proof-battery-exact-head",
    passed: true,
  });
  check("review_and_rerun_parse", reviewParsed.ok && rerunParsed.ok, {});
  if (!reviewParsed.ok || !rerunParsed.ok) throw new Error("fixtures must parse");

  let finalChain = startReleaseChain(handoff);
  const reviewAttach = attachReview(finalChain, reviewParsed.value);
  check("review_attaches", reviewAttach.ok, { issue: reviewAttach.ok ? "" : reviewAttach.issue });
  if (!reviewAttach.ok) throw new Error("review must attach");
  finalChain = reviewAttach.chain;

  const rerunAttach = attachIntegrationRerun(finalChain, rerunParsed.value);
  check("integration_rerun_attaches", rerunAttach.ok, {});
  if (!rerunAttach.ok) throw new Error("rerun must attach");
  finalChain = rerunAttach.chain;

  const ownerGrant = grantOwnerApproval(finalChain, {
    ownerActor: "james",
    artifactDigest: handoff.artifactDigest,
  });
  check("owner_grants_exact_digest", ownerGrant.ok, {});
  if (!ownerGrant.ok) throw new Error("owner grant must succeed");
  finalChain = ownerGrant.chain;

  const allowed = evaluateArtifactReleaseGate(finalChain, {
    currentHeadSha: handoff.headSha,
    currentArtifactDigest: handoff.artifactDigest,
    currentConfigFingerprint: handoff.configFingerprint,
    currentDependencyFingerprint: handoff.dependencyFingerprint,
    currentSchemaFingerprint: handoff.schemaFingerprint,
  });
  check(
    "fully_qualified_chain_allows_exactly_one_rollout",
    allowed.allowed && allowed.reasons.length === 0,
    { reasons: allowed.reasons },
  );

  const consumed = consumeOwnerRollout(finalChain);
  check("first_rollout_consumes_approval", consumed.ok, {});
  if (!consumed.ok) throw new Error("first consumption must succeed");
  finalChain = consumed.chain;

  const spent = evaluateArtifactReleaseGate(finalChain, {
    currentHeadSha: handoff.headSha,
    currentArtifactDigest: handoff.artifactDigest,
  });
  check(
    "gate_blocks_after_single_rollout_spent",
    !spent.allowed && spent.reasons.includes("owner_rollout_already_used"),
    { reasons: spent.reasons },
  );
  const secondConsume = consumeOwnerRollout(finalChain);
  check("second_rollout_refused", !secondConsume.ok && secondConsume.issue === "owner_rollout_already_used", {});

  const wrongDigestGrant = grantOwnerApproval(rerunAttach.chain, {
    ownerActor: "james",
    artifactDigest: DIGEST_B,
  });
  check(
    "owner_cannot_approve_a_different_digest",
    !wrongDigestGrant.ok && wrongDigestGrant.issue.startsWith("owner_approval_digest_mismatch"),
    {},
  );
}

// ---------------------------------------------------------------------------
// 3. Reset semantics — ANY change (code/config/deps/schema/digest) resets.
// ---------------------------------------------------------------------------
{
  const fixture = readFixture("valid/builder-handoff.json");
  const parsed = parseBuilderHandoff(fixture.handoff);
  if (!parsed.ok) throw new Error("fixture handoff must parse");
  const handoff = parsed.value;
  const base = () =>
    startReleaseChain(handoff);

  const evaluateDrift = (
    mutateCurrent: (current: {
      currentHeadSha: string;
      currentArtifactDigest: string;
      currentConfigFingerprint?: string;
      currentDependencyFingerprint?: string;
      currentSchemaFingerprint?: string;
    }) => Parameters<typeof evaluateArtifactReleaseGate>[1],
    expectReason: string,
  ): void => {
    const current = {
      currentHeadSha: handoff.headSha,
      currentArtifactDigest: handoff.artifactDigest,
      currentConfigFingerprint: handoff.configFingerprint,
      currentDependencyFingerprint: handoff.dependencyFingerprint,
      currentSchemaFingerprint: handoff.schemaFingerprint,
    };
    const decision = evaluateArtifactReleaseGate(base(), mutateCurrent(current));
    check(
      `reset_on_${expectReason}`,
      !decision.allowed && decision.reasons.includes(expectReason),
      { reasons: decision.reasons },
    );
  };

  // Code change: head moved.
  evaluateDrift((c) => ({ ...c, currentHeadSha: OTHER_HEAD }), "head_changed_since_handoff");
  // Artifact rebuilt differently.
  evaluateDrift((c) => ({ ...c, currentArtifactDigest: DIGEST_B }), "artifact_digest_changed_since_handoff");
  // Configuration changed.
  evaluateDrift((c) => ({ ...c, currentConfigFingerprint: CONFIG_FP_B }), "configFingerprint_drift_resets_approval");
  // Dependency set changed.
  evaluateDrift((c) => ({ ...c, currentDependencyFingerprint: CONFIG_FP_B }), "dependencyFingerprint_drift_resets_approval");
  // Schema changed.
  evaluateDrift((c) => ({ ...c, currentSchemaFingerprint: CONFIG_FP_B }), "schemaFingerprint_drift_resets_approval");
  // Pinned at build time but current state cannot supply it: fail closed.
  evaluateDrift((c) => {
    const { currentConfigFingerprint: _drop, ...rest } = c;
    void _drop;
    return rest;
  }, "current_state_missing:configFingerprint");

  // Not pinned at build time but a fingerprint now exists: unpinned drift fails closed.
  const unpinnedHandoff = parseBuilderHandoff({
    ...(fixture.handoff as Record<string, unknown>),
    configFingerprint: undefined,
  });
  check("handoff_without_optional_fingerprint_parses", unpinnedHandoff.ok, {});
  if (!unpinnedHandoff.ok) throw new Error("unpinned handoff must parse");
  const unpinnedDecision = evaluateArtifactReleaseGate(startReleaseChain(unpinnedHandoff.value), {
    currentHeadSha: unpinnedHandoff.value.headSha,
    currentArtifactDigest: unpinnedHandoff.value.artifactDigest,
    currentConfigFingerprint: CONFIG_FP_B,
  });
  check(
    "unpinned_current_fingerprint_fails_closed_not_ignored",
    !unpinnedDecision.allowed && unpinnedDecision.reasons.includes("unpinned_current_configFingerprint"),
    { reasons: unpinnedDecision.reasons },
  );

  // Missing stages each block independently.
  const emptyReasons = evaluateArtifactReleaseGate(base(), {
    currentHeadSha: handoff.headSha,
    currentArtifactDigest: handoff.artifactDigest,
  }).reasons;
  for (const expected of [
    "review_missing",
    "integration_missing",
    "owner_approval_missing",
  ] as const) {
    check(`missing_stage_blocks:${expected}`, emptyReasons.includes(expected), { emptyReasons });
  }

  // Digest drift ALSO invalidates the owner approval binding even before evaluation.
  const driftedOwnerEvaluate = evaluateArtifactReleaseGate(base(), {
    currentHeadSha: handoff.headSha,
    currentArtifactDigest: DIGEST_B,
  });
  check(
    "digest_drift_lists_owner_binding_and_reset_reasons_together",
    driftedOwnerEvaluate.reasons.includes("artifact_digest_changed_since_handoff"),
    { reasons: driftedOwnerEvaluate.reasons },
  );
}

// ---------------------------------------------------------------------------
// 4. Review independence and binding guards; integration guards.
// ---------------------------------------------------------------------------
{
  const fixture = readFixture("valid/builder-handoff.json");
  const parsed = parseBuilderHandoff(fixture.handoff);
  if (!parsed.ok) throw new Error("fixture handoff must parse");
  const handoff = parsed.value;
  const chain = startReleaseChain(handoff);

  const selfReview = attachReview(chain, {
    reviewerActor: handoff.builderActor,
    headSha: handoff.headSha,
    artifactDigest: handoff.artifactDigest,
    verdict: "approve",
  });
  check(
    "reviewer_must_be_independent_of_builder",
    !selfReview.ok && selfReview.issue === "reviewer_not_independent",
    {},
  );

  const wrongHeadReview = attachReview(chain, {
    reviewerActor: "independent-reviewer",
    headSha: OTHER_HEAD,
    artifactDigest: handoff.artifactDigest,
    verdict: "approve",
  });
  check(
    "review_of_different_source_head_refused_at_attach",
    !wrongHeadReview.ok && wrongHeadReview.issue.startsWith("review_head_mismatch"),
    {},
  );

  const wrongDigestReview = attachReview(chain, {
    reviewerActor: "independent-reviewer",
    headSha: handoff.headSha,
    artifactDigest: DIGEST_B,
    verdict: "approve",
  });
  check(
    "review_of_different_digest_refused_at_attach",
    !wrongDigestReview.ok && wrongDigestReview.issue.startsWith("review_digest_mismatch"),
    {},
  );

  const rejectParsed = parseReviewApproval({
    reviewerActor: "independent-reviewer",
    headSha: handoff.headSha,
    artifactDigest: handoff.artifactDigest,
    verdict: "reject",
  });
  check("reject_verdict_parses", rejectParsed.ok, {});
  if (!rejectParsed.ok) throw new Error("reject verdict must parse");
  const rejectedChain = attachReview(chain, rejectParsed.value);
  if (!rejectedChain.ok) throw new Error("reject must still attach as evidence");
  const rejectedDecision = evaluateArtifactReleaseGate(rejectedChain.chain, {
    currentHeadSha: handoff.headSha,
    currentArtifactDigest: handoff.artifactDigest,
  });
  check(
    "rejected_review_blocks_release_with_distinct_reason",
    !rejectedDecision.allowed && rejectedDecision.reasons.includes("review_verdict_reject"),
    { reasons: rejectedDecision.reasons },
  );

  const wrongHeadRerun = attachIntegrationRerun(chain, {
    integratorActor: "integration-lead",
    headSha: OTHER_HEAD,
    suite: "pnpm-proof-battery-exact-head",
    passed: true,
  });
  check(
    "integration_rerun_at_other_head_refused_at_attach",
    !wrongHeadRerun.ok && wrongHeadRerun.issue.startsWith("integration_head_mismatch"),
    {},
  );

  const failingSuiteRerun = attachIntegrationRerun(chain, {
    integratorActor: "integration-lead",
    headSha: handoff.headSha,
    suite: "pnpm-proof-battery-exact-head",
    passed: false,
  });
  check("failing_suite_attaches_as_evidence", failingSuiteRerun.ok, {});
  if (!failingSuiteRerun.ok) throw new Error("failing rerun must attach as evidence");
  const failedDecision = evaluateArtifactReleaseGate(failingSuiteRerun.chain, {
    currentHeadSha: handoff.headSha,
    currentArtifactDigest: handoff.artifactDigest,
  });
  check(
    "failed_integration_suite_blocks_release",
    !failedDecision.allowed && failedDecision.reasons.includes("integration_suite_failed"),
    { reasons: failedDecision.reasons },
  );

  const badVerdict = parseReviewApproval({
    reviewerActor: "r",
    headSha: handoff.headSha,
    artifactDigest: handoff.artifactDigest,
    verdict: "rubber-stamp",
  });
  check("unknown_review_verdict_fails_closed", !badVerdict.ok, {});
}

// ---------------------------------------------------------------------------
// 5. Canary compatibility receipts (Studio0 + authorized MacBook).
// ---------------------------------------------------------------------------
{
  const studio0 = readFixture("valid/studio0-compatibility-receipt.json");
  const macbook = readFixture("valid/macbook-compatibility-receipt.json");

  const studio0Parsed = parseCompatibilityReceipt(studio0.receipt);
  const macbookParsed = parseCompatibilityReceipt(macbook.receipt);
  check(
    "studio0_and_macbook_isolated_home_receipts_parse",
    studio0Parsed.ok && macbookParsed.ok,
    { s0: studio0Parsed.ok ? [] : studio0Parsed.issues, mb: macbookParsed.ok ? [] : macbookParsed.issues },
  );
  if (studio0Parsed.ok) {
    check(
      "receipt_machine_identity_is_pseudonymous_hash_never_raw",
      /^[0-9a-f]{64}$/.test(studio0Parsed.value.machineKeyHash) &&
        Object.values(studio0Parsed.value.attestations).every((value) => value === true) &&
        Object.keys(studio0Parsed.value.attestations).length === CANARY_ATTESTATIONS.length &&
        studio0Parsed.value.schema === CANARY_COMPATIBILITY_RECEIPT_SCHEMA,
      {},
    );
  }

  // Deterministic serialization of receipts for stable storage.
  if (studio0Parsed.ok && macbookParsed.ok) {
    check(
      "receipt_serialization_canonical_and_order_insensitive",
      canonicalJson(studio0Parsed.value) ===
        canonicalJson(JSON.parse(JSON.stringify(studio0Parsed.value))),
      {},
    );
  }

  // Hostile fixtures — every one refuses whole.
  const nonisolated = readFixture("hostile/nonisolated-attestation.json");
  const nonisolatedParsed = parseCompatibilityReceipt(nonisolated.receipt);
  check(
    "hostile_nonisolated_receipt_refused_attestation_must_be_true",
    !nonisolatedParsed.ok &&
      nonisolatedParsed.issues.some((issue) => issue === "attestation_not_true:noInstall"),
    { issues: nonisolatedParsed.ok ? [] : nonisolatedParsed.issues },
  );

  const rawKey = readFixture("hostile/raw-machine-key.json");
  const rawKeyParsed = parseCompatibilityReceipt(rawKey.receipt);
  check(
    "hostile_raw_machine_key_refused_identity_stays_pseudonymous",
    !rawKeyParsed.ok && rawKeyParsed.issues.includes("machine_key_must_be_pseudonymous_sha256_hex"),
    { issues: rawKeyParsed.ok ? [] : rawKeyParsed.issues },
  );

  const credentialed = readFixture("hostile/credential-provider-version.json");
  const credentialedParsed = parseCompatibilityReceipt(credentialed.receipt);
  const credentialedError = credentialedParsed.ok ? "" : JSON.stringify(credentialedParsed.issues);
  check(
    "hostile_credential_shaped_provider_version_refused_and_redacted",
    !credentialedParsed.ok &&
      credentialedError.includes("privacy_material_rejected_redacted") &&
      !credentialedError.includes("sk-proj-FAKESENTINEL"),
    { error: credentialedError },
  );

  const perfFields = readFixture("hostile/performance-fields.json");
  const perfParsed = parseCompatibilityReceipt(perfFields.receipt);
  check(
    "hostile_performance_evidence_field_refused_by_closed_vocabulary",
    !perfParsed.ok && perfParsed.issues.some((issue) => issue.startsWith("unknown_field:employeeProductivityScore")),
    { issues: perfParsed.ok ? [] : perfParsed.issues },
  );
  const perfScan = findPerformanceEvidenceViolations(perfFields.receipt);
  check(
    "doctrine_scanner_names_productivity_concept_in_foreign_record",
    perfScan.some((violation) => violation.reason === "performance_evidence_field:productivity"),
    { scan: perfScan },
  );

  const homePath = readFixture("hostile/home-path-osplatform.json");
  const homePathParsed = parseCompatibilityReceipt(homePath.receipt);
  const homePathError = homePathParsed.ok ? "" : JSON.stringify(homePathParsed.issues);
  check(
    "hostile_home_path_refused_redacted",
    !homePathParsed.ok &&
      homePathError.includes("home_path_present") &&
      !homePathError.includes("/Users/james"),
    { error: homePathError },
  );
}

// ---------------------------------------------------------------------------
// 6. Studio 3 preflight — blocked until SSH mapping exists.
// ---------------------------------------------------------------------------
{
  const blocked = readFixture("valid/studio3-preflight-blocked.json");
  const blockedParsed = parseStudio3Preflight(blocked.preflight);
  check(
    "studio3_preflight_parses_while_ssh_mapping_missing",
    blockedParsed.ok && blockedParsed.value.status === "BLOCKED_MISSING_SSH_MAPPING",
    { issues: blockedParsed.ok ? [] : blockedParsed.issues },
  );
  if (blockedParsed.ok) {
    const progress = canaryProgressAllowed(blockedParsed.value);
    check(
      "canary_progression_blocked_missing_ssh_mapping",
      !progress.allowed && progress.reasons.includes("blocked_missing_ssh_mapping"),
      { reasons: progress.reasons },
    );
    check(
      "studio3_preflight_records_pseudonymous_key_os_versions",
      /^[0-9a-f]{64}$/.test(blockedParsed.value.machineKeyHash) &&
        blockedParsed.value.osPlatform.length > 0 &&
        typeof blockedParsed.value.providerVersions === "object" &&
        blockedParsed.value.schema === STUDIO3_PREFLIGHT_SCHEMA,
      {},
    );
  }

  const mapped = parseStudio3Preflight({
    ...(blocked.preflight as Record<string, unknown>),
    sshMapping: "present",
  });
  check(
    "studio3_preflight_with_mapping_records_and_progresses",
    mapped.ok && mapped.value.status === "PREFLIGHT_RECORDED" && canaryProgressAllowed(mapped.value).allowed,
    {},
  );

  const smuggledKey = parseStudio3Preflight({
    ...(blocked.preflight as Record<string, unknown>),
    providerVersions: { codex_cli: "xoxb-FAKESENTINEL0000000000" },
  });
  const smuggledError = smuggledKey.ok ? "" : JSON.stringify(smuggledKey.issues);
  check(
    "studio3_preflight_refuses_credential_material_redacted",
    !smuggledKey.ok && smuggledError.includes("credential_value_shape") && !smuggledError.includes("xoxb-FAKESENTINEL"),
    { error: smuggledError },
  );

  const unknownField = parseStudio3Preflight({
    ...(blocked.preflight as Record<string, unknown>),
    sshUsername: "mateo",
  });
  check(
    "studio3_preflight_closed_vocabulary_no_usernames",
    !unknownField.ok && unknownField.issues.some((issue) => issue === "unknown_field:sshUsername"),
    {},
  );
}

// ---------------------------------------------------------------------------
// 7. Scope gate — canary open, stage-gated scopes refused with issue set.
// ---------------------------------------------------------------------------
{
  for (const scope of CANARY_SCOPES) {
    const claim = evaluateScopeClaim(scope);
    check(`canary_scope_open:${scope}`, claim.allowed, {});
  }
  for (const scope of ["packaging", "automatic_update", "stable_fleet_promotion"]) {
    const claim = evaluateScopeClaim(scope);
    check(
      `stage_gated_scope_refused:${scope}`,
      !claim.allowed &&
        claim.reason === "scope_gated_by_stage_issues" &&
        claim.gatedBy.length === GATED_FLEET_STAGE_ISSUES.length &&
        GATED_FLEET_STAGE_ISSUES.every((issue) => claim.gatedBy.includes(issue)),
      { gatedBy: claim.allowed ? [] : claim.gatedBy },
    );
  }
}

// ---------------------------------------------------------------------------
// 8. Performance-evidence doctrine — static self-scan + scanner controls.
// ---------------------------------------------------------------------------
{
  const source = fs.readFileSync(path.join(root, "packages/shared/src/release-qualification.ts"), "utf8");
  const exportedNames = [...source.matchAll(/export (?:async )?function ([A-Za-z0-9]+)\(/g)].map(
    (match) => match[1]!,
  );
  const forbiddenExports = exportedNames.filter((name) =>
    /(?:route|coach|rank|compensat|disciplin|intervent|verdict|score)/i.test(name),
  );
  check(
    "static_proof_release_module_exports_no_decision_or_scoring_verbs",
    forbiddenExports.length === 0,
    { exportedNames, forbiddenExports },
  );
  const importsDecisionSurface =
    /from\s+"[^"]*(?:performance-layer|metric-registry|learning-evidence|outcome-timeline)"/.test(source);
  check(
    "static_proof_release_module_imports_no_scoring_or_performance_surface",
    !importsDecisionSurface,
    {},
  );

  let threw = false;
  try {
    assertNoPerformanceEvidence({ nested: { deep: { productivityScore: 10 } } });
  } catch (error) {
    threw = error instanceof Error && error.message.includes("performance_evidence_rejected");
  }
  check("assert_throws_on_nested_performance_evidence_field", threw, {});

  let negativeControlPassed = false;
  try {
    assertNoPerformanceEvidence({
      machineRole: "studio0",
      quotaReadings: "operational-context-only",
      eventVolume: { count: 12 },
    });
    negativeControlPassed = true;
  } catch {
    negativeControlPassed = false;
  }
  check(
    "negative_control_operational_context_fields_are_not_performance_evidence",
    negativeControlPassed,
    {},
  );

  const hoursVerdict = readFixture("hostile/working-hours-verdict-handoff.json");
  const hoursParsed = parseBuilderHandoff(hoursVerdict.handoff);
  check(
    "working_hours_verdict_field_refused_by_closed_handoff_vocabulary",
    !hoursParsed.ok && hoursParsed.issues.some((issue) => issue === "unknown_field:workingHoursVerdict"),
    { issues: hoursParsed.ok ? [] : hoursParsed.issues },
  );
}

const serializedChecks = JSON.stringify(checks);
check(
  "proof_output_excludes_home_paths_and_fake_secrets",
  !serializedChecks.includes("/Users/") &&
    !serializedChecks.includes("sk-proj-FAKESENTINEL") &&
    !serializedChecks.includes("xoxb-FAKESENTINEL"),
  {},
);

console.log(JSON.stringify({ status: "passed", checks }, null, 2));
guard.complete();
