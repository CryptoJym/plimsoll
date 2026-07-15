# 0044 — Dashboard projections: bounded refresh and constant-work health

GitHub: https://github.com/CryptoJym/plimsoll/issues/80

## TL;DR

- Dashboard refresh reads one coherent incremental snapshot instead of regrouping millions of raw events across five endpoints.
- Capture health reads path-free tailer activity facts rather than recursively walking local session trees on request.
- Exact attribution, tail, unpriced, cache, subscription, and drill-down receipts survive the read-model migration.

## Scope

Add a compact privacy-safe analytics fact layer, bounded dirty-grain repair and legacy backfill, exact fixed-window read models, one versioned snapshot endpoint, compatibility slices, projection-backed detail routes, path-free capture activity state, and an adversarial resource proof. Keep one collector process and one SQLite database.

This sounding does not activate the installed collector, rewrite the live ledger, enable destructive raw retention, deploy, or change hosted cloud behavior.

## Context

Parent architecture: #75. Stacked implementation base: issue #79 head `0169dd828f74ba0311c356819b859a8a5e0d3e99`. Trace: `46be3ad1-514a-42d2-9f14-2212fdab14dc`.

The existing dashboard requests summary, sessions, repositories, accounts, and status every 30 seconds. Those routes aggregate `buffered_events`; capture health recursively scans local artifacts. The selected windows are 30, 90, 182, 365, and 1825 days.

## Evidence

Read-only live benchmark recorded under #75:

```text
summary 4923ms
sessions 847ms
repos 1402ms
accounts 1229ms
statusStats 1492ms
captureHealth 2998ms
bundle wall 13.15s / CPU 12.26s / RSS 256MB
```

## Problem / Task

Maintain bounded analytics read models from promoted privacy-safe facts and publish one atomic snapshot generation per supported window. Requests must never repair, aggregate raw history, or discover filesystem entries. Projection failure must preserve capture and the last coherent snapshot with an explicit stale/degraded receipt.

## Acceptance Criteria

- [ ] Facts contain only safe hashed identities/linkage and promoted numeric/classification fields; no raw IDs, hostname, payload, content, path, URL, email, label, or credential.
- [ ] All five supported windows reconcile against raw reference fixtures, including cutoff equality, dominant repo/account ties, aliases, multi-repo fallback, unlinked rows, cache fields, unpriced calls, multiple subscriptions, and the 11-row plus aggregate tail.
- [ ] Raw insert/update, repricing, reconciliation, repo enrichment, aliases, priority, labels/email, and subscriptions have a transactional delta, bounded repair, or presentation-only invalidation seam.
- [ ] Legacy backfill and independent parity scan use restart-safe rowid high-watermarks and at most 1,000 raw rows per maintenance slice; requests return `projection_backfilling` until a coherent generation exists.
- [ ] Initial migration uses bounded cooperative follow-ups (40 slices / 2 seconds active work / one event-loop yield between slices), exposes remaining-rowid and ETA counters, and targets about four hours for the observed 4.81M-row ledger rather than 80 hours.
- [ ] A 250,001-row session repairs across multiple slices and crash/reopen without exposing partial aggregates; generic zero-value sessionless spans use compressed segments with measured SQLite file growth no greater than 32 bytes/raw row.
- [ ] Raw delete and compact-to-fact correction receipts survive restart and restore exact parity; metric sample count is null while bounded legacy backfill is incomplete and exact afterward.
- [ ] `/api/snapshot?days=` serves summary, sessions, repositories, accounts, status, generation, freshness, degraded state, and ETag; compatibility routes slice the same generation.
- [ ] The dashboard makes one snapshot request per 30 seconds; resize only redraws browser cache.
- [ ] `/status`, CLI status, and detail routes read projections/facts only; filesystem methods can be forced to throw while snapshot/status remain available.
- [ ] Retention exposes projection parity/readiness but raw-TTL activation remains explicit and proof-gated.
- [ ] Warm snapshot p95 is at most 500 ms with deterministic raw/filesystem request scan counters equal to zero; raw-history growth with fixed projection cardinality does not change request work.
- [ ] `pnpm proof`, issue #77/#78/#79 focused proofs, TypeScript, CLI build, projection proof, and the resource dashboard scenario pass.

## Operational Boundaries

- No live ledger, service, LaunchAgent, provider, cloud, or deployment mutation.
- One process and one SQLite database; no worker service, second database, external cache, or raw-query TTL cache.
- Projection and outbox schemas remain structurally separate.
- Tests use temporary databases, homes, roots, and loopback ports only.

## Notes For Future Agents

- Repository list attribution is event-grain with dominant-session fallback; account attribution is whole-session after alias resolution. They intentionally differ.
- The repository tail is 11 head rows plus `__tail__`; tail session counts are summed per repository and cost is rounded to four decimals.
- Raw event/session/machine/account/linkage strings are not inherently safe. Derive safe hashes and require canonical `sha256:` repo/branch linkage.
- Integer nano-USD is the durable additive representation. Proof pins its allowed difference from legacy SQLite REAL sums.
