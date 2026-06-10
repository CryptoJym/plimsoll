# 0025 — Pricing: unpriced models must not display as $0.00

GitHub: #32

## TL;DR
- Post-rollout-backfill (2026-06-10), By-model showed `gpt-5.2` with 12,565,581 input /
  77,305 output tokens and **$0.00** — the model was absent from shared/pricing.ts, so cost
  was null and a null-cost sum rendered as $0.00. On a money surface, "unpriceable" must
  never look like "free".
- Fixed three ways: (1) gpt-5.2 rate added from OpenAI's own model page ($1.75 / $0.175 /
  $14.00 per 1M, snapshot gpt-5.2-2025-12-11, fetched 2026-06-10 — sourced, not guessed);
  (2) the tailer reprices null-cost usage_rollout rows whenever their model becomes
  priceable (runs at the top of every scan; only null-cost rows are touched); (3) byModel
  exposes `unpricedCalls` and the dashboard renders "unpriced" (or a `+` floor marker on
  partially priced rows) instead of $0.00.

## Acceptance Criteria
- [x] Proof `unpriced_model_distinguished_from_free`: an event with tokens and an unknown
      model yields costUsd 0 with unpricedCalls ≥ 1 in the byModel payload.
- [x] Proof `rate_table_update_reprices_null_cost_rows`: a rate landing after ingestion
      fills cost on existing rows and flags `costEstimated`.
- [x] Live: no by-model row with >1M tokens displays $0.00 unless its priced cost is zero.

## Operational Boundaries
- Honest-history doctrine: rates are sourced from vendor pages with as-of dates, never
  invented; vendor-reported and previously estimated costs are never rewritten.

## Notes For Future Agents
- `MODEL_PRICING` uses longest-prefix matching, so dated snapshots (gpt-5.5-2026-01-01)
  price via their family entry. When a new model shows up unpriced, the capture watch
  stays green (tokens flow) — check By-model for the "unpriced" marker after big backfills.
