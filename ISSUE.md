Sounding: [issues/0061-enrichment-starvation-join-denominator.md](https://github.com/CryptoJym/plimsoll/blob/claude/goofy-dubinsky-06050e/issues/0061-enrichment-starvation-join-denominator.md) (lands on main with PR #180).

**TL;DR**
- Only 13 of 2,423 ledger sessions carry any repo/branch/sha linkage (2 for this repo), so fleet cost-per-merged-PR has no honest denominator.
- The maintenance child is deadline-killed at the hardcoded 30s (`cli.ts:1116`) — `maintenance_deadline_exceeded` × 652 in `collector.err.log` — starving `git_context` enrichment (48,280 event-links `fill_pending`, 709 dirty sessions, 93 contexts ever resolved) and the reprice drain.
- The quarantine misattributes: it blames whatever candidate is on stage at kill time (probes of the blamed cwd return in milliseconds), holds one candidate, expires in 15 min — loop repeats.
- The join itself is proven with real rows (PR #178 ↔ session, $774.68 priced, via merge-sha) — see docs/agent-economics-join-audit-2026-08-20.md in PR #180.

**First step per the sounding's acceptance criteria:** per-stage elapsed timings in the maintenance receipt, so the real 30s consumer inside `runRecent()` is named with numbers before any deadline/architecture change.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
