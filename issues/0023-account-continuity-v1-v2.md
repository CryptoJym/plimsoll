# 0023 — Account continuity across the v1→v2 hash boundary

## TL;DR
- One human currently renders as 4 account hashes + a NULL row. The v1→v2 swap re-hashed the
  same wire identity through different sanitizer chains: `d007d307…` = v2-rehash(v1-sanitized
  value) on backfilled rows; `406685df…` = v2-rehash(wire value) on live rows. Codex pair
  `6fc28c7d…`/`d4351f41…` mirrors it.
- Outcome: the accounts panel shows one row per real identity, with the merge auditable and
  local-only.

## Context
- v2 re-hash is unsalted `sha256(value)[:16]` (packages/shared/src/policy.ts).
- The v1 archive (`~/Library/Application Support/CFO of One/work-ledger-v1-archive-20260610.sqlite`)
  still holds the v1-sanitized pre-images the backfill re-hashed — so archive-hash → live-hash
  linkage is computable IF v1's sanitizer form of the wire value can be reproduced (v1 impl
  lives in ai-costs-review). Naive forms already ruled out (sha256 of the live stored string
  does not produce d007d3…).

## Problem / Task
James (and later, teammates) must never have to mentally sum split identities. Either:
(a) computed alias table `account_aliases(old_hash → canonical_hash, reason, created_at)`
    derived from the archive pre-images, or
(b) explicit local merge in Settings ("these rows are the same person"), stored in the same
    alias table, applied at query time in dashboardAccounts.
(a) where provable, (b) as the human fallback. Both local-only.

## Acceptance Criteria
- Accounts panel shows one row for the owner across backfilled + live history (screenshot).
- Aliases never appear in upload bodies (proof check, same pattern as labels).
- Merge is reversible (alias rows deletable from Settings).

## Operational Boundaries
- Hashes in `buffered_events` are immutable history — aliasing happens at read time; no
  destructive rewrite of event rows.

## Open Questions
- Was v1's sanitizer salted? If yes, (a) is dead and (b) is the path.
