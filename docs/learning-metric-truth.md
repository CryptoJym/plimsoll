# Learning metric truth contract

`packages/shared/src/metric-registry.ts` is the only versioned definition surface for the first learning metrics. It is pure: callers provide a bounded manifest, and the module returns deterministic descriptive results without fetching, recommending, or changing collector state.

## Versions

- Analysis manifest: `1.0.0`
- Metric definitions: `1.0.0`

Both versions are required in every input. An unknown version fails closed instead of silently changing a formula.

## Registry

| Metric | Numerator | Denominator / censoring |
| --- | --- | --- |
| Project allocation coverage | Deliveries with a named project and attribution | All cohort deliveries; unknown allocation remains in the denominator |
| First-pass yield | Verified explicit pass on the first required-check observation | All cohort deliveries; `none`, `unknown`, missing pages, and failed fetches cannot pass |
| Correction loop | Verified first failures that later become green by `asOf` | All verified first failures |
| Time/tokens to first green | Separate elapsed-time and input/output/cache-read/cache-write sums | A separate known-observation count for every dimension |
| Mature stable delivery | Mature, check-passing deliveries with complete rework evidence and no rework in the horizon | Only deliveries whose full stability horizon has elapsed |
| Post-merge rework | Mature deliveries with observed rework in the horizon | All mature deliveries; incomplete evidence makes the rate a floor |
| Known-cost coverage | Deliveries with reported or explicitly estimated cost | All cohort deliveries; missing cost is unknown |
| Technique exposure | Deliveries with one or more attributed technique ids | All cohort deliveries; exposure is descriptive, not an effect estimate |

## Required result envelope

Every metric result names:

- `definitionVersion`, `window`, and `asOf`
- keyed `numerator`, `denominator`, and calculated `measures`
- `sample` counts: eligible, sampled, excluded, censored, unknown, and exclusion reasons
- verified/inferred/partial/blocked/excluded source `coverage`
- configured horizon and mature/censored counts under `maturity`
- attribution methods and attribution coverage
- evidence state and analytical claim class
- limitations and any deterministic breakdown rows

Evidence states are `verified`, `partial`, `inferred`, `blocked`, and `excluded`. Claim classes are `observed`, `suggestive`, `associated`, `causal`, and `not_estimable`. This single-cohort registry emits only observed or suggestive descriptions; blocked, excluded, and fully censored results are `not_estimable`. Association and causality require a separate comparative design and are never inferred from technique exposure.

## Fail-closed rules

- Check success requires `checks.evidenceState = verified` and an explicit `passed` observation. A visible pass on an incomplete page does not count.
- Events after `asOf` are invisible. Stability and rework also ignore events after the configured horizon.
- Right-censored deliveries remain visible in `eligibleCount` but do not enter stability numerators or denominators until mature.
- Missing cost yields `null`/`unknown`; a subtotal with incomplete coverage is a `floor`. An explicitly reported `$0` remains distinguishable from missing cost.
- Input, output, cache-read, cache-write, time, and USD are distinct units. `sumLikeQuantities` rejects unlike units, so token dimensions cannot be collapsed or mixed with dollars.

Run the adversarial golden fixtures with:

```sh
pnpm proof:metric-truth
```
