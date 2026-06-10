# 0010 — Reports: descriptive pattern summaries (local, free tier)

## TL;DR
- Extend the local report with descriptive practice summaries over the user's own data: model mix, cache-hit ratio, action-class mix, session shapes (delegate-heavy vs monolithic), cost by hour-of-day.
- Descriptive only — comparative/prescriptive analytics (cohorts, benchmarks, recommendations) are hosted-product scope and stay out of this repo.

## Scope
New `pnpm report -- --patterns` section. SQL over existing columns; no new capture, no scoring, no advice.

## Context
- Columns already promoted for cheap aggregation: action_class, model, tokens, cost, session_id (indexed).
- metric_samples carries claude_code.active_time, lines_of_code, commit counts for enrichment.

## Acceptance Criteria
- [ ] `--patterns` outputs: tokens/cost by model; cache-read ratio per model; action-class distribution; top-10 sessions by cost with their action mix.
- [ ] Zero recommendations language — numbers and definitions only (the open/paid boundary is explicit in CONTRIBUTING.md).
- [ ] Runs in <5s on a 90-day ledger (use the indexes; no payload_json parsing).
