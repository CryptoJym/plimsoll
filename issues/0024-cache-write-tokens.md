# 0024 — Capture cache-write tokens as a first-class column

## TL;DR
- `claude_code.token.usage` metrics record 10,484,404 `cacheCreation` tokens (2026-06-10 ledger)
  but `buffered_events` has no cache-write column, so per-event token accounting silently omits
  the most expensive input class (cache writes price ≈1.25× input).
- Cost is currently still correct (Claude reports `cost_usd` per request); this is about token
  receipts being complete, and about codex/other adapters where cost must be computed from tokens.

## Problem / Task
`cache_creation_tokens` column on `buffered_events`, extracted from api_request attributes
(`cache_creation_input_tokens` / `cache_creation.input_tokens` key variants), surfaced in
session/model rollups and receipts.

## Acceptance Criteria
- Proof fixture api_request with cache-creation attr lands in the column and in
  `dashboardSessionDetail` receipts.
- Migration is additive (`alter table add column`), old rows stay NULL — honest absence.
- Efficiency report includes cache-write tokens where present.

## Operational Boundaries
- `pnpm proof` stays green; upload schema change coordinated with ingest (header alias work in
  plimsoll-cloud#1) before any cloud sync of the new column.
