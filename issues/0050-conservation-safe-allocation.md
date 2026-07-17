# 0050 — Allocation: conserve every token across project and work-unit joins

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/98

## TL;DR
- Replace session-wide `max(repo_hash)` and whole-session-per-PR charging with one allocation spine: exact event HEAD/branch/repo first, bounded segment inference second, explicit unallocated remainder.
- Fix tailers so each token delta receives the Git context active at that delta rather than the final context of a scanned chunk.
- Make totals reconcile exactly across all token classes and known cost.

## Scope
Local collector/tailers, efficiency report, allocation receipts, and deterministic fixtures. No semantic prompt analysis.

## Evidence
- Live audit: 225 sessions span multiple repositories and carry 19,771,520,024 direct input+output tokens.
- `scripts/efficiency-report.ts` groups sessions and uses `max(repo_hash)`/`max(branch_hash)`; a long session may then be charged in full to multiple PRs.
- Codex and Claude tailers can attach the last context in a chunk to earlier usage deltas.

## Acceptance Criteria
- [ ] Each token/cost event has zero or one allocation edge; weights per event sum to at most 1.
- [ ] For every window: direct + inferred + unallocated equals captured totals exactly for input, output, cache-read, cache-write, and known cost.
- [ ] Allocation hierarchy and confidence are explicit: exact HEAD membership; time-bounded branch/repo; bounded same-session segment; unallocated.
- [ ] Multi-repo session, repository-switch-in-one-chunk, reused branch, force-push, and one-session/three-PR fixtures cannot duplicate or misplace tokens.
- [ ] Report exposes direct/inferred/unallocated coverage and never hides a dominant-repo fallback.
- [ ] Historical labels may change display names without rewriting allocation truth.
- [ ] Performance is incremental/bounded; no dashboard request scans raw history.

## Operational Boundaries
- Repo/branch privacy-safe hashes and public commit SHAs only; no raw path or remote leaves the local boundary without deliberate label disclosure.
