# Agent-economics join audit — 2026-08-20

Read-only audit of the production ledger on Studio 0
(`~/Library/Application Support/plimsoll/work-ledger.sqlite`, 7.6 GB, live WAL),
plus the source tree at merge `ebef4d5`. Every database read in this audit used
`file:...?mode=ro` with `PRAGMA query_only=ON`. The running collector
(launchd `com.plimsoll.collector`, PID 25551) was not stopped, restarted, or
reconfigured.

Companion change in this branch: `claude-opus-5` and `claude-sonnet-5` added to
`packages/shared/src/pricing.ts`, with a static proof in
`scripts/maintenance-proof.ts` (`claude_5_family_models_price`).

## Bottom line

1. The two "missing" routes are not broken and not local. They are paths on a
   **hosted workspace server** that the CLI *calls*; the local collector never
   served them. Nothing to fix.
2. The three empty fact tables are a **separate, unfinished learning-evidence
   program**. Schema and writers exist; no production code path calls the
   writers. They are not needed for cost-per-merged-PR.
3. Snapshot vs live: **the live projection tables are authoritative**; the
   snapshot is a cache that only republishes when the projection backlog is
   zero. Both surfaces were verified against the raw ledger.
4. The two defects that actually block cost-per-merged-PR are different from
   all three questions:
   - **Pricing gap** — `claude-opus-5` / `claude-sonnet-5` were absent from
     `MODEL_PRICING`, so 46,704 usage events (75% of the window's opus-5
     calls) carry `cost_usd NULL` ≈ **$6,134 invisible**. Fixed in this
     branch; the catalog-fingerprint sweep auto-reprices after deploy.
   - **Linkage starvation** — only **13 of 2,423 sessions** carry any
     repo/branch/sha linkage because the maintenance child that runs
     enrichment has been deadline-killed **652 times**. The join logic itself
     works (proven below with real rows). Filed as a follow-up issue.

## (a) Why `/api/work-intelligence/join` and `/api/work-intelligence/github-outcomes` return 404

They are **outbound cloud paths, not local routes**:

- `packages/collector-cli/src/join.ts:25` — `CLOUD_JOIN_PATH =
  "/api/work-intelligence/join"`. `plimsoll join "<url>#<token>"` POSTs a
  workspace join token to this path **on the cloud base URL**
  (`new URL(CLOUD_JOIN_PATH, joinBase.origin)`, join.ts:634). This "join" is
  fleet membership, not the data join.
- `packages/collector-cli/src/outcomes-sync.ts:55` — `OUTCOMES_PATH =
  "/api/work-intelligence/github-outcomes"`. `plimsoll sync-outcomes
  --repository owner/repo` computes the session⋈PR join **locally** and POSTs
  the batch to this path on the joined workspace ("the hosted workspace has
  the receiving route (github-outcomes, cloud C8)", outcomes-sync.ts:24).
- The local server (`packages/collector-cli/src/server.ts`) registers only
  `/status`, `/`, `/api/settings|snapshot|summary|sessions|repos|accounts|repo|session`,
  the `/api/settings/*` writes, `/hooks/*`, and `/v1/{logs,traces,metrics}`.
  Every other `/api/*` GET falls through to `{"error":"not_found"}`
  (server.ts:413). Not a feature flag; not an unbuilt package.

The README's promised session→merged-PR join (README.md:9) is implemented as
**local reports**: `scripts/efficiency-report.ts` (`pnpm report`) and the pure
join functions in `outcomes-sync.ts` (`collectSessionLinks`,
`joinSessionsToPulls`). This machine has never joined a workspace, so the
cloud push path has never run. `plimsoll backfill-outcome-timeline` (local
outcome store `outcome-timeline-v1.sqlite`) has also never run here — the file
does not exist in the collector home.

## (b) The three empty fact tables

`work_episode_facts`, `tool_attempt_facts`, `technique_exposure_facts` — all
0 rows (verified), plus their sibling `technique_identity_registry`, also 0.

- Schema is created by `LearningFactStore.ensureSchema()`
  (`packages/collector-cli/src/learning-facts.ts:274`), instantiated by every
  `LocalEventBuffer` (`buffer.ts:550`). That is why the tables exist.
- Writers exist and are fully guarded (`recordToolSignal`,
  `recordWorkEpisode`, `recordTechniqueExposure`), and an adapter
  (`adaptToolInteractionEvent`) converts normalized events into signals.
- **No production caller exists.** The only callers in the repo are
  `scripts/learning-facts-proof.ts` and `scripts/system-e2e-proof.ts`. The
  capture pipeline (forwarder, OTLP explode, tailers) never invokes the
  store. The pipeline was built and proven (PR #118 lineage) but never wired
  to live capture.
- Not a blocker for agent economics: cost-per-merged-PR reads
  `buffered_events` + GitHub state, not these tables.

## (c) Snapshot $7,370.04 vs live $17,988.16 — which was right

Mechanism (`packages/collector-cli/src/dashboard-projection.ts`):

- `/api/summary` serves the stored `dashboard_snapshots` row.
  `publishSnapshots()` (line ~2487) refuses to build a new generation unless
  backfill is complete **and** every projection backlog (repairs, compact
  mutations, compact GC, dirty sessions, account invalidations) is zero. While
  a backlog drains, the snapshot stays frozen at its last generation and the
  live aggregate tables (`dashboard_window_totals`, `dashboard_model_window`,
  `dashboard_daily_window`, …) keep moving.
- Timeline on 2026-08-20 (times MDT): generation 132 published 09:56 with
  $7,370.04 / 2,090,894 events — a coherent picture of a ledger that still
  had most of the morning's backfill unpriced/unapplied. The post-#178
  unwedge then drained reprice + repair work; by 10:40 the live tables read
  $17,988.16 / 2,177,476 while the snapshot was still gen 132. Generation
  133 published at 11:08:30 (the daemon restart) with $18,122.90 / 2,187,139.
- Parity check against the raw ledger (11:5x MDT): direct
  `buffered_events` recompute over the projection's own 30-day cutoff
  (`2026-07-21T17:08:31.046Z`) = **2,197,028 events, $18,321.02 priced
  cost**; `dashboard_window_totals` days=30 read minutes earlier =
  2,195,717 / $18,305.32. Deltas match the still-draining backlog (control:
  `dirty=1`, `degraded_reason=projection_repair_backlog`, 850 repairs, 180
  dirty sessions at read time).

**Ruling: the live projection tables (and behind them `buffered_events`) are
the surface of record. The snapshot is a freshness-gated cache.** Any consumer
of `/api/summary` must treat `x-plimsoll-projection-generation` +
`created_at` as staleness signals, per §5.3's adversarial check: read both
surfaces, print both when they disagree.

**Both surfaces still undercount.** 48,062 usage events carry `cost_usd NULL`
with a known model. Breakdown of the Claude portion (the priced remainder of
opus-5 came from Claude Code's vendor-reported cost):

| model | NULL-cost events | input | output | cache read | cache write | est. missing (catalog rates) |
|---|---:|---:|---:|---:|---:|---:|
| claude-opus-5 | 44,599 | 267,134 | 29,272,886 | 8,022,405,034 | 204,923,243 | **$6,025.13** |
| claude-sonnet-5 | 2,105 | 207,082 | 1,575,656 | 186,010,543 | 7,802,786 | **$109.31** |

gpt-5.6-sol/luna/terra (1,352 events), `qwen3.8:27b` (3) and
`codex-auto-review` (3) also sit at NULL. The gpt-5.6 family is
subscription-routed; §5.3 already treats it as $0 marginal — but note the
dashboards render this as `$0.00`, which reads as "free" rather than
"not metered". Leaving them unpriced is deliberate.

Corrected 30-day picture once the backfill prices the backlog (estimates
flagged `costEstimated` by design): total ≈ **$24.4k**, claude-opus-5 ≈
**$8,986** (not $2,757). The §5.3 line "Fable ran less than half the calls of
Opus 5 and cost five times more" becomes **≈1.6x**, not 5x.

## Root causes that actually block cost-per-merged-PR

### 1. Pricing catalog gap (fixed in this branch)

`priceForModel()` exact-matches, then longest-prefix-matches. No catalog key
was a prefix of `claude-opus-5` or `claude-sonnet-5`, so `estimateCostUsd()`
returned undefined and the reprice drain discarded those queue rows as
unknown-model ("wait for a catalog-fingerprint change",
`maintenance.ts:292`). The catalog fingerprint has not changed since install
day (`maintenance_state.pricing_catalog_applied`, 2026-07-19).

This branch adds both entries (rates per platform pricing, 2026-08-20:
opus-5 $5/$25, cache read 0.1x, cache write 1.25x; sonnet-5 standard $3/$15 —
intro $2/$10 runs through 2026-08-31 and is noted in the code comment).
**Deploy note:** merging + restarting the collector changes the catalog
fingerprint, which triggers the built-in bounded legacy sweep
(`idx_events_unpriced_usage`) and reprices all 48k NULL-cost events
automatically. No manual backfill step.

### 2. Linkage starvation (follow-up issue — the join denominator)

The join works; almost nothing is joinable:

- Events with linkage: claude_code **32 of 306,147**; codex **184 of
  1,891,989**. Sessions with any linkage key: **13 of 2,423**.
- Enrichment state: 93 `repo_context_results` ever; **48,280
  `repo_context_event_links` with `fill_pending=1`**; 709 sessions in
  `repo_enrichment_dirty`; 0 inflight.
- Cause: the maintenance child (capture scan → repo/git context → reprice)
  is killed at its hardcoded 30s deadline (`cli.ts:1116`;
  `maintenance_deadline_exceeded` × **652** in `collector.err.log`), so the
  enrichment and reprice stages starve. At audit time the circuit was open
  (31 skipped jobs) and the boundary blamed stage `git_context`, candidate
  `sha256:e03c5460…`, which brute-force matching identified as cwd
  `~/Documents/Codex/Projects/new-reward/namaste-poodles-visibility-report`
  — but direct probes of that path (stat/list/read `.git/HEAD`) all return
  in milliseconds. The quarantine attributes the kill to whatever candidate
  is on stage when the deadline lands, it holds only one candidate, and it
  expires after 15 minutes — so the loop repeats. The real 30s consumer
  inside `runRecent()` is not yet pinned; per-stage timing in the receipt
  would answer it.

## The join, proven with real rows

Read-only reimplementation of `collectSessionLinks` + `joinSessionsToPulls`
(same hashes: `sha256(github.com/cryptojym/plimsoll)`, sha256 of branch name,
plain shas) against the production ledger × the repo's 60 most recent merged
PRs via the GitHub API:

```
ledger sessions total:               2,423
sessions with any linkage key:          13
sessions with repo_hash == plimsoll:     2
merged PRs examined:                    60
join rows (session × PR):                2

PR    merged_at             sessions  events  priced_cost  via
#138  2026-07-19T21:03:42Z         1      17        $0.00  merge_sha
#178  2026-08-20T15:53:04Z         1  27,358      $774.68  merge_sha

join denominator: 2 joined / 2 sessions with this repo's hash (ledger life)
```

So: the hashes line up, sessions join to merged PRs, and PR #178 (the
projection unwedge) cost **$774.68** in priced tokens. Per §5.3's rule the
denominator must ride with any such number: with linkage on 13/2,423
sessions, fleet-wide cost-per-merged-PR is **not yet publishable**. The
pricing fix (this branch) repairs the numerator; the enrichment issue repairs
the denominator.

## State of record after this audit

| Surface | Value at audit | Status |
|---|---|---|
| `buffered_events` (raw, priced) | 30d: 2,197,028 events / $18,321.02 | source of truth for priced cost |
| `dashboard_window_totals` days=30 | 2,195,717 / $18,305.32 | authoritative live surface, small lag |
| `dashboard_snapshots` days=30 | gen 133, 11:08:30 MDT, $18,122.90 | cache; republish gated on zero backlog |
| Unpriced backlog | 48,062 events (46,457 queued) | ≈$6,134; auto-reprices after this branch deploys |
| Learning-fact tables | 0 / 0 / 0 / 0 rows | unwired program, out of scope here |
| Session linkage | 13 / 2,423 sessions | blocker for fleet economics; follow-up issue |
