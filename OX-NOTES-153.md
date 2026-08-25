# OX-NOTES — Issue #153: 0081 Token truth — classify nonzero-first cumulative counter lineage

## Root cause

`RolloutTailer` (packages/collector-cli/src/rollout-tailer.ts) computes per-event
deltas by telescoping cumulative `total_token_usage` values. A fresh file lineage
starts from an ASSUMED zero baseline (`initialParserState` → `previous: ZERO`), so
the FIRST observed cumulative total entered validated usage whole. The Studio0
incident (2026-08): one tailer-authority session whose first row carried
961282526 input / 953284608 cached / 2008151 output — ~99% of the ledger's entire
increase — counted as if this session had consumed it, because the counter may be
inherited/forked/resumed/global and the aggregate evidence cannot distinguish
that from legitimate long-session usage.

## Fix (smallest diff that satisfies #153)

`packages/collector-cli/src/rollout-tailer.ts` only:

- Classification rule (derived from durable parser state, no checkpoint-schema
  change): at diff time, `tokenCountIndex === 0 && isZeroTotals(previous) &&
  !isZeroTotals(totals)` marks the row `lineageFirstUnknown`. An OBSERVED
  all-zero first total legitimately anchors the baseline — later deltas from it
  remain validated marginals.
- Classified rows still append (raw evidence preserved, deterministic id
  unchanged) but with typed token columns zeroed, no pricing
  (`estimateCostUsd` skipped, so reprice maintenance can never price them), and
  metadata: `counterLineage: "unknown_nonzero_first"` plus raw source totals
  `sourceCumulativeInput/CachedInput/Output/ReasoningOutput`.
- Scan honesty counters added to `RolloutScanResult`: `unvalidatedFirstRows`
  and `tokensUnvalidated {input, cachedInput, output}` (typed optional like
  `baselinePendingMetadataPeak`; always set by scan()). Both are included in
  `resultMutationSnapshot`/`restoreResultMutationSnapshot` so a failed slice
  rolls back its counters too. They surface via the existing
  `scan-rollouts` JSON output and `rollout_scan` cadence logs for free.

Outbound boundary untouched: unknown/local-only metadata keys are omitted with
suppression receipts by outbound-envelope.ts (same precedent as
`reasoningOutputTokens`). No allowlist expansion, no downstream SQL changes,
no schema migration, no checkpoint version bump.

## Deliberate proof-expectation changes

Old expectations encoded the defect ("diffing from ZERO"):

- scripts/incremental-capture-proof.ts: first-counter row now asserts zeroed
  typed columns + classification + raw preservation; DB sums 250→150,
  400→300 (stateless rebuild replays deterministically; duplicate ids keep
  original classified rows); deferred repo-context fixture expects row0=0 with
  linkage intact, marginals 10 each.
- scripts/signal-fidelity-proof.ts: rollout sums 3000/600/130→2000/400/80;
  rescan-idempotent 3000→2000; new checks `rollout_lineage_first_counter_preserved_raw`;
  unpriced-model reprice fixture gains an observed-zero anchor line so the
  rate-table-heal behavior it exists to test remains exercised.

## New adversarial proof

scripts/token-lineage-proof.ts (`pnpm proof:token-lineage`):

1. Studio0 incident shape (961M lump, model UNKNOWN, 2ms timestamps):
   classified, preserved raw, excluded from validated sums, counters exact,
   rescan does not double-report, outbound seal omits lineage fields with receipts.
2. Mid-life FORK (truncate+regrow from inflated counter): forced rebuild replays
   deterministically; fork delta attributable; replay cannot inflate validated sums.
3. Slice-boundary smuggling: first counter arriving in a LATER committed slice
   (after a meta-only cursor commit) is still classified.
4. Observed-zero anchoring: later deltas stay VALIDATED (no over-classification),
   both rows priced normally under a known model.

Pre-fix vs post-fix: proof fails on stashed tree with actual validated input
961302526 (lump swallowed); passes on fixed tree.

## Verification

- `npx tsc --noEmit` exit 0.
- Green after fix: incremental-capture, token-lineage, signal-fidelity (108
  checks), usage-dedupe, metric-truth.
- Pre-existing/environmental failures, byte-identical on clean tree:
  codex-reconciliation (cadence projection), outbox (CLI self-test needs packaged
  build), allocation, join-isolation, privacy-mode.
- maintenance-proof flaked once under a loaded sequential battery
  (`codex_jsonl_open_soft_cap_retains_candidate_batch_until_admitted`, capFrames
  113 vs 112) then passed 3/3 isolated on BOTH clean and fixed trees — timing-
  sensitive cadence budgeting, unrelated to this diff.
