# 0049 — Learning metrics: versioned truth, coverage, maturity, and claim lineage

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/97

## TL;DR
- Define the small metric dictionary the learning system may use before adding new dashboards.
- Every result carries formula version, numerator/denominator, window/as-of time, sample, source coverage, maturity/censoring, attribution method, and claim class.
- `UNKNOWN` and incomplete evidence never count as success or zero.

## Scope
Shared schemas, deterministic definitions, analysis manifest, and proof fixtures. No recommendations or hosted cohort engine.

## Problem / Task
Current VDY counts merged PRs with `none`/`unknown` checks as non-failing, young PRs can look stable before the rework window matures, and GitHub fetch failures are swallowed. Those states cannot gate learning or skill promotion.

## Acceptance Criteria
- [ ] Registry defines project allocation coverage, first-pass yield, correction loop, time/tokens to first green, mature stable delivery, post-merge rework, known-cost coverage, and technique exposure.
- [ ] Each metric emits `definitionVersion`, window, as-of, eligible/sample counts, numerator, denominator, coverage, evidence state, and analytical claim class.
- [ ] Evidence states include `verified`, `partial`, `inferred`, `blocked`, and `excluded`; claim classes include `observed`, `suggestive`, `associated`, `causal`, and `not_estimable`.
- [ ] Required checks must be known and explicitly passed; `none`, `unknown`, missing pages, and failed fetches never inflate validation.
- [ ] Stability metrics exclude right-censored work until the configured horizon matures.
- [ ] Missing cost is unknown/floor, never `$0.00`; input/output/cache-read/cache-write remain separate token dimensions.
- [ ] Golden fixtures pin formulas and reject look-ahead leakage, denominator drift, and mixed token/dollar weighting.

## Operational Boundaries
- Definitions are deterministic code, not LLM judgments.
- Existing legacy VDY can remain for continuity only when labeled `legacy` beside the strict metric.
