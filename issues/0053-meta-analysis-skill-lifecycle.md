# 0053 — Learning: evidence packets, cohort analysis, and owner-gated skill lifecycle

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/101

## TL;DR
- Build versioned, reviewable analysis packets from deterministic facts; let the model summarize and nominate hypotheses only downstream.
- Compare like with like (project, work type, complexity, actor where allowed, model/version, time epoch) and label observational results as association.
- A candidate can become a durable skill only after dedupe, counterexamples, prospective validation, independent verification, and owner approval.

## Scope
Open schemas/export for own-data evidence packets plus hosted comparative/prescriptive implementation. No direct writes to installed skill trees.

## Acceptance Criteria
- [ ] Analysis run records source snapshot/query hash, metric versions, window/as-of, exposed/control counts, coverage, attribution mix, effect estimate/uncertainty, confounders, and counterexamples.
- [ ] Cohort minimums are statistical gates separate from the existing privacy minimum; insufficient samples return `not_estimable`.
- [ ] Matching/stratification controls project, work type, complexity, model/tool version, actor/repo clustering, and calendar changes; multiple-hypothesis checks prevent chance winners.
- [ ] Candidate lifecycle is `observed → candidate → reviewed playbook → owner-approved pilot → evaluated → skill proposal → owner-approved publication → monitored → stale/deprecated/rolled back`.
- [ ] Existing-skill inventory produces `new_skill`, `enhance_existing`, `compose_existing`, `duplicate`, `conflict`, `quarantine`, or `insufficient_evidence`.
- [ ] Skill proposal includes applicability, contraindications, tests, privacy/security/doctrine review, negative cases, expiry, rollback target, and independent verifier.
- [ ] No process writes `~/.codex/skills`, `~/.claude/skills`, global memory, or team machines; output is a review artifact/PR proposal only.
- [ ] Open-web/model-generated text is tainted and cannot gate or enter executable skill instructions without human review.

## Operational Boundaries
- Local/open layer stays descriptive about the operator's own data; organization comparisons and prescriptions live in the hosted product.
- No individual ranking below policy thresholds; no composite developer score.

## Local/open implementation slice

The bounded own-data compiler is documented in
`docs/learning-evidence-packets.md`. It supplies a versioned runtime schema,
deterministic fingerprint/no-op behavior, paired observational association
packet, and review-artifact-only skill lifecycle. It does not satisfy the
hosted comparison, durable skill proposal, pilot, publication, or monitoring
scope, so this parent issue remains open.
