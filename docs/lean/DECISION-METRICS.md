# Decision metrics for a lean Plimsoll (round 6)

**Bead:** eco-6hoxj.164.2 · **Round:** 6 (revised after `input/review-r5/REVIEW.md`, verdict CHANGES_REQUIRED) · **As of:** 2026-09-25 MDT · Planning only; nothing was deployed or measured live in this lane.

**Sources and citation style.** `VALUE-MAP.md:27`, `decision-metrics.md:8`, `footprint-budgets.md:12`, `local-inventory.md:44` point into `input/value-map/`; `HOSTS.md:6`, `PROD-FACTS.md:22`, `PROD-FACTS-2.md:12` into `input/footprint/` (shipped again in `out/evidence/footprint/`); `UNCOVERED-EVIDENCE.txt:16` and `LANE-ROOT-CAUSE.md:3` into `input/capture-gaps/` (shipped in `out/evidence/capture-gaps/`); `DESIGN.md §4.2` and `LANES.md:62` into `input/economics-design/`; `REVIEW.md:NN` into `input/review-r5/` (the round-4 claim ids `Q01`–`Q21` into `input/review-r4/`). Source citations of the form `collector-cli/src/buffer.ts:3008-3013` were re-validated **in this round** against collector commit `92c33bf` (tree `67fd5a8`) and `cloud src/lib/ingest.ts:427` against cloud commit **`067a8a4`** (existence and range for every citation, content for the load-bearing ones: `out/checks/validate-citations.log`) (`4954995` plus PR #151, which changed four UI files, two tests and the capture-watermark active-window module; `out/checks/source-pins.log`). Every number that is not a measurement is labelled an estimate.

**What changed in round 5 (summary; the full map is in `CHANGES.md`).** Every admitted record carries a **token volume state**, and no dollar or token-weighted verdict is computed while a cohort holds a record whose volume is unknown or whose cache semantics are ambiguous (blocker B6, Q11/Q12; §0, G2, G3, M7, M8, M9; fixture `out/fixtures/b6_unknown_volume_gates.py`). G1 treats an unresolved file's gap as **open-ended** until the file is parsed (should-fix 5, Q21; ARCHITECTURE.md §3.6; fixture `sf5_unresolved_file_open_gap.py`). Round 6: a non-zero cache column on a `no_cache_columns` source is `ambiguous_semantics` (round-5 should-fix; §0, G3; fixture `sf_no_cache_columns_guard.py`), and every timestamp the schema accepts reaches the day facts (ARCHITECTURE.md §3.5; fixture `b5_non_iso_day_facts.py`). Everything else is as in round 5.

## 0. How to read the numbers

- **Usage record** means a row that satisfies the economics predicate `<UR>` byte-for-byte: `event_type IN ('usage_rollout','usage_transcript','usage_live') OR input_tokens IS NOT NULL OR output_tokens IS NOT NULL OR cache_read_tokens IS NOT NULL OR cache_creation_tokens IS NOT NULL OR cost_usd IS NOT NULL` (`cloud src/lib/economics/loader.ts:72-79` at `067a8a4`; `DESIGN.md §3`; the lane-2 export `USAGE_RECORD_PREDICATE_SQL`, `LANES.md:62`, does not exist yet, ARCHITECTURE.md §10). The predicate is amount-aware, not type-aware: the shared event schema allows token and cost columns on **every** event type, including `otel_span` (`collector-cli/packages/shared/src/schemas.ts:273-326`). Everything that fails `<UR>` is an **activity record** and is never counted in an amount.
- **Admitted** means the record passed the real `reconcileUsage` judgement (admitted, not duplicate, not quarantined, not invalid), carried into `usage_facts` by economics lane 2 (`DESIGN.md §4.2-4.3`; `cloud src/lib/economics/service.ts:22-27`).
- **Known cost** is a floor: the sum over admitted records that carry a cost, by basis (`reported`, `estimated`, `unknown`; `cloud src/lib/economics/service.ts:101-116`). A null cost is an unpriced observation, never zero (`VALUE-MAP.md:9`). No page shows "total spend" while any admitted record in the period is unpriced; it shows "known cost $X, plus N tokens unpriced". Reported and estimated cost are never added into one number without the basis named.
- **Normalised tokens (blocker 6).** Every token count in a share, ratio or bound is first normalised under the record's `token_rule` (ARCHITECTURE.md §3.1 "Token rules"): under `openai_cached_subset` the record's distinct input tokens are `input_tokens` (cached tokens are a subset of it, `collector-cli/packages/shared/src/pricing.ts:7-9,77-83`), and cached = `min(cache_read, input)`; under `anthropic_cache_exclusive` distinct input is `input + cache_read + cache_creation` (`pricing.ts:30-35`); under `no_cache_columns` and `unknown` it is `input`. **Normalised tokens** = distinct input + output. The reviewer's fixture (85 priced input tokens that are all cached plus 15 unpriced input tokens) is 85% priced by tokens, not 91.9% (`out/checks/counterexamples.log`, case 9).
- **Price versus bound (blocker 6, R15).** The collector's catalog is a dated list-price table looked up by exact key or **longest prefix** (`pricing.ts:22-52,54-65`; fetched 2026-06-10 and 2026-08-20, `:7-8,30,37`). It *prices* an estimate; a prefix match on a suffixed model id says nothing about that model's maximum rate, tier or period. A rate is a **bound** only when the cloud rate catalog (ARCHITECTURE.md §8 "Rate catalog") holds an **attested ceiling entry** for the record's exact `model_key` (or an admin-attested alias), whose effective interval contains the record's instant, with `ceiling_complete = 1` (every published tier and channel enumerated by the attester). Such a record has `rate_bound_kind = 'attested_ceiling'` and its bounded cost is normalised tokens per column × the ceiling per column. Every other unpriced record has `rate_bound_kind = 'none'` and **no bound**: it belongs to the **unbounded** remainder. **Observed rates are never bounds.** The page shows two remainders: "unpriced but bounded: N tokens, up to $Y (ceiling attested by … on …)" and "unpriced, unbounded: M tokens on models {…}". Every gate below that needs a bound requires the unbounded remainder to be **zero**; otherwise it withholds the dollar figure and compares normalised tokens. Seeding the cloud catalog from the collector's table creates **price** entries only (`provenance = 'collector_static_list'`, `ceiling_complete = 0`), so on day one no dollar comparison can pass: that is the honest state until an admin attests ceilings (`out/checks/counterexamples.log`, case 16).
- **Token volume state (blocker B6).** Every admitted record has `token_volume_state ∈ {known, unknown_null, ambiguous_semantics, live_interval}` (ARCHITECTURE.md §3.1): `known` only when every column its `token_rule` reads is non-null and, under `unknown` or `no_cache_columns`, no cache column is non-zero; `unknown_null` when a needed column is null (the shared schema makes every amount optional, `collector-cli/packages/shared/src/schemas.ts:299-303`, so a record admitted by type or by cost alone has no measured volume); `ambiguous_semantics` when the rule is `unknown` or `no_cache_columns` and a cache column is non-zero (an exclusive-cache reading and a subset reading differ by the cache count: the reviewer's record with input 1 and cache-read 999 is 98.9% or 8.3% covered depending on the reading; a source that declares no cache semantics yet reports 900 cached tokens is read under no guessed rule either, `out/fixtures/sf_no_cache_columns_guard.py`); `live_interval` for a `usage_live` placeholder whose amounts arrive on a later record. **Rule:** before any share, ratio or bound is computed, the cohort's non-`known` records are counted; a **dollar** comparison requires all three non-`known` counts to be zero in every period compared, and a **token-weighted** share, ranking or verdict requires them to be zero in the cohort; otherwise the verdict is withheld, the record-count shares (which need no volume) are shown, and the page says "N records have no measured token volume; M records have ambiguous cache semantics; K live intervals await their usage record". Null is never zero and an ambiguous record is never read under a guessed rule (`out/fixtures/b6_unknown_volume_gates.py`: the reviewer's two cases pass round 4's formula and are withheld under this rule; an all-known cohort at 95% still passes).
- **Coverage** has three independent axes on separate lines: capture ("observed through", from the watermark, withdrawn to unknown while a durability fault or an unverified restart is open, ARCHITECTURE.md §6.3), tailer completeness ("read to the end", from the coverage walk and deferred bytes; ARCHITECTURE.md §6.4) and reconciliation ("totals through", from the projection) (`DESIGN.md §4.8`; `cloud src/lib/capture-watermark/contract.ts:407-424`).
- Every metric states the minimum coverage it needs before it is displayed as a number. Below that threshold the page shows the coverage sentence, never a zero and never a dash.

## 1. Prerequisite gates (shown first, always)

Measured today: 15% of usage records carry a project key, 0% a work item, 16% an attested logical identity, 34% a cost (`PROD-FACTS-2.md:7-12`; `PROD-FACTS.md:22-27`: 974 of 2,867 usage rows with cost in one hour). Of the priced share, `costKind` tells reported from estimated; estimated records are priced from the catalog with `rateVersion` = its fingerprint (`cloud src/lib/economics/event-adapter.ts:31,51`).

### G1. Capture, tailer completeness and reconciliation coverage

| | |
|---|---|
| Decision it drives | Whether to act on any number for this period at all, and which machine or source to fix first. |
| Exact definition | Four separate groups, never merged into one count. (a) **Observed**: usage records captured in the period by state: admitted, duplicate, quarantined, invalid, with reason counts (`usage_facts.state`, `usage_rollup_hourly` counts, `DESIGN.md §4.2`). (b) **Unobserved but bounded**: declared capture gaps per machine and source with interval, reason and `dropped_rows` where the collector counted them (`interval_basis = 'counted_interval'`). (c) **Unobserved and unbounded**: gaps whose row count is `unknown` because a tailer never parsed the input (`collector-cli/src/capture-frontier.ts:7-45`), because the gap record itself could not be written durably (`gap_record_unavailable`, with its `fault_interval`), because the collector restarted after a crash or an unresolved fault and has not re-verified coverage (`restart_unverified`), or because a file is unread or unresolved, in which case the gap is **open-ended from the epoch start** (`interval_basis = 'epoch_open'`, `ended_at_ms` null, ARCHITECTURE.md §3.6; should-fix 5) and stays open until the file is parsed to its end. (d) **Tailer completeness per machine and source**: `bytesDeferred`, `deferredGenerations`, unread files and bytes by class, the age and completeness of the last whole-root coverage walk, roots covered out of roots configured, and open durability faults (ARCHITECTURE.md §6.3-6.4; `collector-cli/src/rollout-tailer.ts:1263-1289`; `capture-frontier.ts:499-522`). Plus `reconciledThrough`, `observedThrough`, and machines reporting in the last 24 h out of enrolled. |
| Minimum data | Lane-2 states and counts; the capture watermark (`cloud src/lib/capture-watermark/store.ts:24-58`); the versioned `capture_gap` items and the cloud `capture_gaps` table; claim v2's tailer and fault fields; install last-contact. |
| Completeness rule | Always shown. It is the status line. `complete` is false while any declared gap overlaps the period (`cloud src/lib/capture-watermark/contract.ts:407-424`), any gap is unbounded, any fault is open, any machine's `bytesDeferred > 0`, or any machine's last whole-root walk is incomplete or older than 24 h. Seven green days with zero declared gaps do **not** make a period complete while a machine reports unread bytes. An unresolved file makes the machine's **whole epoch, through the present**, incomplete until it is parsed: its gap has no end, so a period after the file's last write is never complete either (`out/fixtures/sf5_unresolved_file_open_gap.py`). |
| Wording when short | The state table in `DESIGN.md §4.8` plus: "Studio3 did not capture between 14:02 and 14:31 UTC (footprint limit): at least 1,240 events; totals for that machine are a floor."; for the unbounded case, "Studio3 stopped capturing between 14:02 and 14:31 UTC; how much was missed is unknown."; for a durability fault, "Studio3 could not record a capture gap at 14:02 UTC; capture since then is not confirmed."; for tailer completeness, "Studio1 has 5.0 GB of session files not yet read (174 files, oldest 2026-09-07); totals for that machine are a floor until the catch-up finishes" and "Studio3 skipped 716 MB of session files it cannot parse (oversized records, ambiguous rewrites); usage they hold could belong to any time since 2026-09-07; totals for Studio3 are a floor for the whole period." Never "N events dropped" for an unbounded gap. |

### G2. Attribution coverage

| | |
|---|---|
| Decision it drives | Whether per-project and per-workflow figures can be compared at all; which unmapped keys to assign first. |
| Exact definition | `attributedShareRecords` = admitted records with `project_key` / admitted. `attributedShareSpend` = known cost of attributed records / known cost of all admitted records. `attributedShareTokens` = **normalised** tokens (§0) of attributed records / normalised tokens of all admitted, computed per model and as a total. `workLinkedShare` in the same three forms for `work_item_id`. Unallocated row: record count, known cost, unpriced-bounded tokens with their bound, unpriced-unbounded tokens with their models, and the top five unmapped keys (repo label when one exists, else the hashed key, plus source and machine) with counts. |
| Minimum data | `project_key`, `work_item_id`, `attribution_source`, `token_rule` on `usage_facts` (`DESIGN.md §4.2`; the `token_rule` column is additive, B15); rollup columns `work_linked`, `work_identity_missing`; the daily dimensional rollup for the token form (B15) carrying normalised tokens; repo labels from the collector's `repo_labels`; the rate catalog (B15). |
| Completeness rule | Always shown. **Volume gate first (B6):** the cohort's `unknown_null`, `ambiguous_semantics` and `live_interval` counts must be zero before any token share is computed; otherwise the token forms are withheld and only `attributedShareRecords` is shown with the counts. A **ranking** of projects or workflows, or any per-project verdict, requires all of: the volume gate; `attributedShareSpend ≥ 0.80`; `attributedShareTokens ≥ 0.80` for every model that carries ≥ 5% of the period's normalised tokens; the unallocated **unbounded** remainder = 0 tokens; and the unallocated **bounded** remainder's bound below 10% of known cost. When the unbounded remainder is not zero the dollar ranking is withheld and a **token ranking** is shown, labelled "by tokens: N tokens on {models} have no attested price ceiling". The cohort is the exact set of admitted records in the period; nothing is sampled. The Unallocated row is always pinned first (UX review PJ-01). |
| Wording when short | "85% of known spend has no project, and unpriced usage could add up to $2,900 more (ceiling attested by J. on 2026-10-01 for gpt-5.5). Comparisons between projects are withheld until both are under the thresholds. Map the top keys: <five keys with counts> →" and, when unbounded tokens exist, "2.1 M tokens on grok-4-7 have no attested ceiling; ranking by tokens only." Today this sentence would show on every workspace (`PROD-FACTS-2.md:12`). |

### G3. Pricing coverage

| | |
|---|---|
| Decision it drives | Whether dollar figures can be trusted; which models need a price or a reported-cost source; which models need an attested ceiling. |
| Exact definition | `unknownVolumeRecords` = admitted records by `token_volume_state` other than `known` (three counts, shown first); `pricedShareRecords` = priced / admitted; `pricedShareTokens` = normalised tokens of priced records / normalised tokens of all admitted, per model and total, **computed only when the three counts are zero**; split reported / estimated / unknown (`cost_kind`); `rateUnbound` count (estimated records whose `rate_catalog_ref` does not resolve); unpriced normalised tokens by model, each marked bounded (with its bound, the ceiling's attester and interval) or unbounded; the list of models without an attested ceiling, separately from the list without any price. |
| Minimum data | `cost_kind`, `rate_bound_kind`, `rate_catalog_ref`, `rate_ceiling_ref`, `rate_version`, `token_rule`, `token_volume_state`, token columns on `usage_facts`; rollup `priced`, `unpriced`, `rate_unbound`, `unknown_n` (`DESIGN.md §4.2`); the daily dimensional rollup by model for the token form (B15); the rate catalog with price, ceiling and alias entries (B15). |
| Completeness rule | Always shown. A dollar **comparison** (versus previous period, versus budget, per outcome) requires, in every period compared: `unknownVolumeRecords` = 0 on all three counts (B6; checked before anything else); `pricedShareTokens ≥ 0.90` for every model carrying ≥ 5% of that period's normalised tokens; `rateUnbound` = 0 among estimated records; the unbounded remainder = 0 tokens; and the bounded remainder's bound below 10% of known cost. Below that the comparison is made in normalised tokens and says so. Dollar **floors** are always shown with both remainders. |
| Wording when short | "Known cost $4,120 (floor; up to $1,300 more unpriced, bounded by ceilings attested for gpt-5.5 and claude-opus-5). 31 M tokens on 3 models have no price: gpt-6-sol (22 M, priced by prefix, no attested ceiling), grok-4-7 (9 M, no price). Attest a ceiling →"; and, when the volume gate fails, "Token shares withheld: 41 records carry no token counts and 12 Grok records have ambiguous cache semantics; record shares shown instead." |

## 2. The decision metrics

Nine metrics. Each is computed from admitted usage records, session and turn summaries, the daily dimensional rollup and the acceptance journal; none needs raw activity rows (ARCHITECTURE.md §3). "Page" assumes the UX proposal (admin-only Financials, Overview, Projects, Improve); §4 says where each lives if James keeps today's pages.

### M1. Known spend and its change

| | |
|---|---|
| Decision | Are we on plan this period; did spend move, and on which basis. |
| Definition | Known cost over admitted, non-live facts in `[S, E)` by basis (reported, estimated, unclassified), live rows applied with the service's interval rules (`DESIGN.md §4.4` step 5); the same for the previous equal-length period `[S − (E − S), S)`. **Dollar change** = (current − previous) / previous per basis, shown only when both periods pass G1 (capture complete or partial with a named window) and G3's comparison rule. Otherwise the page shows a **volume change** in normalised tokens, labelled "usage volume change, not spend", and never a dollar percentage. Reported and estimated are never summed into one change figure. |
| Minimum data | `usage_rollup_hourly` interior + `usage_facts` edges (lane 2/3); nothing new. |
| Completeness | Freshness `current` or `catching_up` with the "totals through" instant; never labelled complete with pending, dirty, building or quarantined non-zero (`DESIGN.md §4.8`). |
| Short-coverage wording | "Known cost $X through 04:10 UTC (floor; N tokens unpriced, up to $Y bounded; M tokens unbounded). Spend change versus the previous 28 days is not shown: 3 of those days have unconfirmed capture. Usage volume was up 12% in tokens." |
| Page | Financials headline; one tile on Overview with the same figure and basis. |

### M2. Spend by project, model, account and source, with share

| | |
|---|---|
| Decision | Where to focus; what to reallocate; which model, account or source is the cost driver. |
| Definition | Group admitted facts by one dimension at a time: project (`project_key` → label), model, account (server-bound actor, pseudonymous), **source**. Columns: known cost by basis, share of known cost, records, input / output / cache-read / cache-creation tokens (raw columns, with the `token_rule` named), normalised tokens, unpriced normalised tokens split bounded (with bound) and unbounded. Total row. Unallocated pinned first with its share. |
| Minimum data | Project and company are rollup dimensions today (`DESIGN.md §4.2`). Model, account and source are **not**; they need a second rollup keyed by `(day, project_key, model_key, account, source)` with the same count and null columns plus normalised tokens (bead B15, `fact_version` 2 where `DESIGN.md §7` item 6 leaves room). Group cardinality: at most 9 (project, company) groups per hour today (`PROD-FACTS-2.md:15`), 57 accounts, about a dozen models (UX review CT-02) and four sources, so the daily grain keeps the read bounded (estimate: under 5,000 rows per 28 days). |
| Completeness | Tokens always. Dollar columns per G3. Project ranking per G2 (below the gate the table still shows; the verdict and "top project" callouts do not). |
| Short-coverage wording | The G2 and G3 sentences, placed above the table. |
| Page | Financials (the Controls tables with a cost column, UX review CT-01); the per-project rows on Projects. Nothing on the member screen. |

### M3. Budget burn and period-end projection

| | |
|---|---|
| Decision | Slow down, reallocate, or raise the budget this period. |
| Definition | Inputs: budget amount, currency, period boundaries in the workspace time zone, owner, scope (workspace or project). `spentKnown` = M1 to date. Burn rate = known cost over the last 7 **capture-complete** days / 7. Projection = `spentKnown` + burn rate × remaining days, shown as a range. **Range rule:** with 14 or more complete days in the trailing 14, the range uses the minimum and maximum daily known cost of those 14; with 7 to 13 complete days it uses the minimum and maximum of the complete days available and says "range from N complete days"; with fewer than 7 the projection is withheld. Unknown remainder = unpriced tokens with their bound where one exists (shown separately, never converted into the projection). |
| Minimum data | Daily sums from the rollup; a new `workspace_budgets` record (cloud; admin-only write); capture completeness per day from the watermark, the fault state and the tailer fields. |
| Completeness | At least 7 capture-complete days in the trailing 14 and G3's comparison rule; otherwise the projection is withheld. |
| Short-coverage wording | "No budget set. Set one →" / "Projection withheld: capture is unconfirmed for 8 of the last 14 days" / "Projection withheld: 40% of tokens on gpt-6-sol are unpriced" / "Projection withheld: Studio1 has 5.0 GB of session files not yet read". |
| Page | Financials. Admin-only in either roles answer. |

### M4. Cost per accepted outcome, and rework cost

| | |
|---|---|
| Decision | Is this workflow, model or project worth its full cost; where is quality failing. |
| Definition | For a project or workflow and a period. **Accepted items** = distinct `work_item_id` values whose acceptance head (the latest `accept` or `supersede` receipt for that item, `cloud src/lib/acceptance/repository.ts:52-57`) has `acceptedAt` in the period, is not superseded by a later receipt at the period end, and has no `reopen` recorded against it at the period end (`repository.ts:110-137`). A supersede chain counts once, as its head. A reopened item is excluded until it is accepted again; when it is, its whole-item cost includes the rework. **Whole-item cost** = Σ known cost of admitted facts carrying that `work_item_id`, from the first fact to the head's `acceptedAt`, including facts observed before the period start. Cost per accepted item = Σ whole-item cost / accepted items. **Rework cost** = Σ known cost of facts on an item observed after its first `reopen` and before its next acceptance. Items still open are censored, not counted as zero. The cohort's linked but unpriced tokens, bounded and unbounded, are shown beside the figure. |
| Minimum data | `work_item_id` and `accepted_outcome_id` on usage facts (`DESIGN.md §4.2`), which requires **dispatch tagging** at the source (bead B18); the acceptance journal with reopen and supersede events. |
| Completeness | For the project or workflow: `workLinkedShare` by spend ≥ 0.50 **and** by normalised tokens ≥ 0.50 for every model carrying ≥ 5% of the cohort's tokens; ≥ 10 accepted items in the window; the unlinked cost is shown on its own line ("$Z of this project's spend is not linked to any item and is excluded"). |
| Short-coverage wording | "Not measurable yet: 0% of usage carries a work item (`PROD-FACTS-2.md:10`). Turn on dispatch tagging →". The current manager "cost per accepted artifact", which divides a 1,000-event sample by a separate 500-artifact count, is retired (`decision-metrics.md:8`). |
| Page | Projects (per project) and Financials (per workflow). |

### M5. Spend without an accepted result

| | |
|---|---|
| Decision | Which agent workflow or source burns spend without producing a result. |
| Definition | Over sessions that **ended** in the period, disposition ∈ {`completed`, `stopped`, `abandoned_timeout`, `error`} (the exact enum in ARCHITECTURE.md §3.3; there is no review-pending disposition): Σ known cost of ended sessions whose `work_item_id` has no acceptance head at the period end, by source, workflow and model. The acceptance join happens on the cloud at read time (session summaries × acceptance journal), so a later acceptance changes the figure on the next read without a collector revision. Active sessions are censored. Sessions with dispatch intent `explore` are shown on their own line, not as waste. A stopped session alone is not an abandonment fact (`decision-metrics.md:12`); the disposition rule (`session_stop` seen → `stopped`; no activity for 30 minutes and no stop → `abandoned_timeout`; terminal error marker → `error`; explicit completion marker or accepted outcome → `completed`) is stated on the page with its rule id. Sessions whose coverage relation is `partial` or `unknown` are listed separately as "coverage conflict", never summed (ARCHITECTURE.md §5.3). A session's cost is the sum over its segments (sealed and open, ARCHITECTURE.md §3.3); a segment with post-seal mutations is flagged on the drill-down. |
| Minimum data | Session summaries and segments (start, end, disposition, disposition rule, known cost, work item, dispatch intent, coverage relation; ARCHITECTURE.md §3.3, §5.3); the acceptance journal. |
| Completeness | Disposition recorded for ≥ 90% of ended sessions and `workLinkedShare` by spend ≥ 0.50 for the scope; otherwise the count of sessions with an unknown outcome is shown as unknown. |
| Short-coverage wording | "412 sessions ended without a recorded outcome (unknown, not wasted). Link work items to see unproductive spend." |
| Page | Improve (Work patterns) and Projects. |

### M6. Retry loops and failed tool attempts, with attached spend by tool class and model (blocker 6, R17)

| | |
|---|---|
| Decision | Fix a stuck tool or workflow; stop a loop that is burning tokens; see which model the loop ran on. |
| Definition | Per session and tool class, from counters derived **at capture** (ARCHITECTURE.md §3.2-3.3, rule `m6_v3`): attempts, failures, unknown results, retries (`retry_of` chains, `collector-cli/src/runtime-facts.ts:161-230`), longest chain, and a loop flag when ≥ 3 retries of the same tool class fall within a 10-minute window. **Spend after failure** is accumulated **per class and per (class, model)** at capture: from a failed attempt of class C, every following turn's known cost is added to C's bucket and to the (C, turn's `model_key`) bucket, up to and including the next turn with a successful result of C (or the session end); stored as `spend_after_failure_by_class_json` `{class: {cost_nanos, turns, cost_unknown_turns}}` and `spend_after_failure_by_class_model_json` `{class: {model_key: {cost_nanos, turns, cost_unknown_turns}}}` (bounded 8 classes × 8 models + "other"), with the session total in `spend_after_failure_nanos` (which is not the sum of the classes when a turn follows failures of two classes; the page says so). Labelled an estimate because tool calls carry no cost of their own (`decision-metrics.md:7`). Aggregated for the period by tool class, source **and model** by summing the per-(class, model) buckets: two allocations of one class total across two models are now different stored fields (`out/checks/counterexamples.log`, case 8). |
| Minimum data | `tool_class_counts_json`, `spend_after_failure_by_class_json` and `spend_after_failure_by_class_model_json` on session summaries; per-turn cost, `model_key` and `failed_then_recovered_json` on turn summaries. |
| Completeness | Attempt-to-result correlation ≥ 90% for the source; the unknown-result rate is always shown; turns with unknown cost shown as a count, never as zero; the "other" model bucket's share shown when non-zero. |
| Short-coverage wording | "Tool results unmatched for 23% of attempts on source X; loop counts are a floor." |
| Better than today | Studio0 refuses learning facts beyond the 100,000-attempt cap (292,934 refused, `HOSTS.md:19`; `collector-cli/src/learning-facts.ts:40-45`); per-session counters have no such cap. |
| Page | Improve. |

### M7. Cache efficiency

| | |
|---|---|
| Decision | Which prompts, workflows and models reuse context well; where to enable or restructure caching. |
| Definition | Per model, source and project, under the record's `token_rule` (§0): cache-hit share = cached / distinct input, where cached = `min(cache_read, input)` under `openai_cached_subset` and `cache_read` under `anthropic_cache_exclusive`; cache-creation share = `cache_creation / distinct input` where the source bills it. The rule (Codex rollouts report cumulative counters turned into marginal deltas, `collector-cli/src/rollout-tailer.ts:1713-1755`; Claude transcripts report per-message cache read and creation; Grok reports no cache columns; the catalog states which semantics it prices, `pricing.ts:8-9,30-35`) is stored as `token_rule` on the fact and shown on the page. Estimated saving = cached × (input rate − cached-input rate) at the record's **price** entry (an estimate of what was saved at list price, labelled as such); it is never a bound. Records with null cache columns count as "not captured". |
| Minimum data | The four token columns, `rate_catalog_ref`, `rate_bound_kind` and `token_rule` on usage facts. |
| Completeness | The cohort passes the volume gate (§0; records with `ambiguous_semantics` are excluded from the hit share and counted); cache columns non-null for ≥ 90% of the source's normalised tokens; dollar saving only with a price entry on ≥ 90% of the cohort's cached tokens. |
| Short-coverage wording | "Cache tokens not captured for 62% of Grok records; hit share shown for the rest. Saving not shown: no catalog price for 40% of cache reads." |
| Page | Financials (by model) and Improve. |

### M8. Context growth within sessions, by model

| | |
|---|---|
| Decision | Shorten or restart context; set a compaction policy; pick a model with a larger effective cache. |
| Definition | From turn summaries: `context_tokens(t)` = distinct input under the turn's `token_rule` (`context_rule` per source names it). Per session: median, p90, max; slope over the last 20 turns (tokens per turn); compaction or reset markers. **Above-threshold cost** is computed at capture, not from the bounded turn series: for the fixed thresholds 100k, 150k and 200k tokens the session summary stores `context_thresholds_json` (totals) and `context_thresholds_by_model_json` (per `model_key`, the turn's dominant model, rule `m8_v2`), updated on every revision from the full turn set while turn rows exist (ARCHITECTURE.md §3.3). The page shows the 150k figure by default and says which threshold it uses. Aggregated: sessions over threshold and the cost share above threshold by source and **by model**, summing the per-model buckets. Percentiles, never one large turn (`decision-metrics.md:14`). |
| Minimum data | Turn index, per-turn token deltas, `token_rule` and `model_key` on turn summaries; `context_thresholds_json` and `context_thresholds_by_model_json` on the session summary. Codex rollouts carry a native `turnIndex` (`collector-cli/src/rollout-tailer.ts:1713-1755`); other sources get an ordered index from usage-record order within the session, with `turn_order_basis` recorded (ARCHITECTURE.md §3.2). |
| Completeness | The session's records pass the volume gate (§0; a turn with an unknown-volume record has `context_tokens` null and is counted, never zero); `turn_order_basis` native or ordered for ≥ 90% of the session's usage records; otherwise session-level only. A mixed-model turn is attributed to its dominant model and counted once; the share of mixed turns is shown. |
| Short-coverage wording | "Turn order not captured for source X; showing session totals only." |
| Page | Improve; per-session drill-down (the bounded `turn_series_json` serves the drill-down only, never an aggregate). |

### M9. Anomaly against baseline (should-fix 2: zero baseline defined)

| | |
|---|---|
| Decision | Investigate now, or not. |
| Definition | Daily known cost and normalised tokens versus a baseline = median of the same weekday over the previous four capture-complete weeks. Deviation decomposed into volume (records) and mix (cost per record). **Dollar rule** (used when the day and every baseline day pass G3's comparison rule): alert iff **both** `|Δ$| ≥ $X` **and** `|Δ$| / baseline$ ≥ 0.30`, with `$X = max($25, 1% of the trailing 28-day known cost)`. **Token rule** (otherwise): alert iff both `|Δtok| ≥ T_X` and `|Δtok| / baseline_tok ≥ 0.30`, with `T_X = 1% of the trailing 28-day admitted normalised tokens` (no absolute default; an admin may set one). **Zero baseline:** when the baseline is 0 (the median of the four weekday values is zero) the ratio is undefined and is **not** treated as satisfied; the rule degrades to the absolute test alone, alert iff `|Δ| ≥ $X` (or `T_X`), and the page says "no baseline for this weekday (zero in the last four weeks); absolute threshold only". When both the baseline and the day are zero there is no alert and no deviation is shown. When fewer than four baseline weeks are capture-complete the baseline is "forming" and no alert fires. Which rule fired is stated on the page; both thresholds are overridable per workspace by an admin. Never on a raw span spike (`decision-metrics.md:16`). |
| Minimum data | Daily rollup; watermark, fault state and tailer completeness per day. |
| Completeness | Four complete weeks of history; both the day and its baseline days capture-complete; the token rule fires only when the day and its baseline days pass the volume gate (§0). |
| Short-coverage wording | "Baseline forming: 9 of 28 days complete." |
| Page | Overview verdict line and Financials. |

### Named future input: allowance-window burn (not a metric yet)

Subscription users manage token capacity against the provider's native window, which usage records cannot substitute for (`decision-metrics.md:10`). It needs a separately attested allowance snapshot feed (source, account, window, used, limit, reset, attested at). Until that feed exists the page shows an explicit state, "Allowance: not connected (usage below is spend, not remaining quota)", never zero and never a derived remaining share. Bead B19 specifies the feed and its attestation; nothing here implies remaining allowance from known spend.

**Before/after experiments** stay on Improve as a decision procedure (prespecified window, cohort, threshold), fed by M1, M4 and M5 for the cohort; not a standing metric.

## 3. Rejected from the main surface, and why

| Rejected | Why |
|---|---|
| Raw event count, span count, tool calls per minute, average payload size | Activity volume says nothing about whether spend bought a result; collector diagnostics (`decision-metrics.md:24-26`). They stay on Settings/Connections as health. |
| "Tokens per message" without task or context | No denominator a manager can act on. |
| People ranked by total tokens | Encourages the wrong action and leaks pseudonymous identity into a league table; spend by account stays available as a table with cost, not a ranking. |
| The manager sample ratios (validation/continue over the latest 1,000 events) | Sensitive to which rows happen to be in the sample (`decision-metrics.md:24-26`; `cloud src/lib/dashboards/mutations.ts:26-40`). Replaced by M5/M6 over session summaries. |
| "Cost per accepted artifact" as computed today | Divides an unlinked cost sample by an unlinked artifact count (`decision-metrics.md:8`). Replaced by M4. |
| Any total that converts null to zero | Null is an unpriced or incomplete observation (`VALUE-MAP.md:9`). |
| Exact cost per tool derived from tool-call counts | Tool calls carry no amount; M6 labels its spend-after-failure figure as an estimate (`decision-metrics.md:7`). |
| A model-shaming score by price alone | No reliable "routine work" label exists (`decision-metrics.md:15`). M2 shows spend by model; M4 adds outcome quality once work items are linked. |
| "Abandoned" from `session_stop` alone | Not an abandonment fact (`decision-metrics.md:12`); M5 uses the explicit disposition rule. |
| A dollar change computed from a token change | A volume change is not a spend comparison; M1 labels it. |
| **An "upper bound" from the highest observed rate, or from a prefix-matched list price** | The highest rate seen in a window is a scenario, not a maximum; a prefix match prices an estimate for a model whose exact rate, tier and period the table does not attest (`pricing.ts:54-65`; R15). Only an attested ceiling for the exact model and interval bounds anything. |
| A token share that adds cache columns to input for every source | OpenAI's cached tokens are inside its input tokens (`pricing.ts:8-9`); adding them counts the same tokens twice (R16). Shares use normalised tokens. |
| Remaining allowance derived from known spend | Usage is not the provider's quota window; the allowance feed is a named future input. |
| The local dashboard's action mix as a management figure | An activity count; it remains a local health view. |

## 4. Where each item lives under either product answer

| Item | If James adopts admin-only Financials + admin/member | If today's pages stay |
|---|---|---|
| G1 status line (capture, faults, tailer completeness, reconciliation) | Every data page, first line | Same |
| G2, G3 | Financials, above the tables; the Unallocated row on Projects | Cost basis and Controls tables |
| M1 | Financials headline; one Overview tile | Overview/Cost basis headline tile (basis with a value first, UX OV-01) |
| M2 | Financials tables | Controls tables with a cost column (UX CT-01) |
| M3 | Financials | Cost basis |
| M4, M5 | Projects and Improve | Projects and Work patterns |
| M6, M7, M8 | Improve (M7 also Financials by model) | Work patterns |
| M9 | Overview verdict line, Financials | Overview |
| Allowance state | Financials, one line | Cost basis, one line |
| Member role | Nothing financial; the member screen names the admin | Same: a plain member already sees only the welcome card |

The contract is the same either way: additive fields on `plimsoll.workspace-economics.v1` (`PROD-FACTS-2.md:18`) for the gates and M1-M3, M7, M9; a new read model for M4-M6 and M8 from session and turn summaries (ARCHITECTURE.md §8). Page placement is a routing decision, not a data decision.

## 5. What each metric needs from the collector and the cloud

| Metric | Usage facts (unchanged envelope) | Session summary (at capture) | Turn summary | Rollups | New input |
|---|---|---|---|---|---|
| G1 | states, receipts | | | hourly counts | versioned capture gaps (bounded, unbounded, epoch-wide for unresolved files); faults and the restart state; claim v2 tailer fields (`.163.81`/`.163.82`, B22) |
| G2 | project_key, work_item_id, token_rule, tokens by model | | | daily dimensional rollup with normalised tokens (B15) | dispatch tagging (B18); rate catalog (B15) |
| G3 | cost_kind, rate_bound_kind, rate_catalog_ref, rate_ceiling_ref, token_rule, token_volume_state, tokens by model | | | daily dimensional rollup with the three unknown-volume counts (B15) | rate catalog with price, ceiling and alias entries (B15); attested ceilings (admin) |
| M1, M9 | amounts by basis | | | hourly + daily | budget record (M3) |
| M2 | amounts, model, account, source, token_rule | | | daily (project, model, account, source) | |
| M3 | amounts | | | daily; completeness per day | budget record |
| M4 | work_item_id, accepted_outcome_id | | | | dispatch tagging, acceptance journal |
| M5 | | disposition + rule, cost per segment, work item, intent, coverage relation | | | acceptance journal (joined at read) |
| M6 | | tool_class_counts_json, spend_after_failure_by_class_json, spend_after_failure_by_class_model_json, loop flag | per-turn cost, model_key, failed_then_recovered_json | | |
| M7 | four token columns, rate_catalog_ref, token_rule | | | | |
| M8 | | context percentiles, slope, context_thresholds_by_model_json | turn index, context tokens, token_rule, model_key | | |
| Allowance | | | | | attested allowance feed (B19) |
