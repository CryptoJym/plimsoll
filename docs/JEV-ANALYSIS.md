# Jev analysis in Plimsoll

The local collector dashboard reads saved Jev automatic-decision receipts. Opening or refreshing the page does not call a model or change tasks, routing, hooks, or memories.

The first integration makes rescue, reuse, readiness, change, waste and calibration recommendations inspectable beside Plimsoll's existing telemetry. Expand a finding to inspect its native session, event and input hashes, inference usage, cost estimate, and reported owner actions. A prepared offer does not establish delivery or use. An owner-reported `accepted` action is not independent proof of successful work. Verified acceptance, task attribution and measured savings remain unknown.

## Source and access

- Source contract: Jev `AUTO-RECEIPTS.md`, automatic receipt interface v1 (`jev-auto-v1`).
- Default source: `~/.local/state/jev-decisions/inference.sqlite3`; operators can set `PLIMSOLL_JEV_DB` in the collector process environment for an existing authorized receipt store.
- Set `PLIMSOLL_JEV_DISABLED=1` in that environment to disable this read path. The dashboard shows the disabled state explicitly.
- `GET /api/jev-analysis?days=30` requires the existing `x-plimsoll-token` management credential. Producer credentials do not grant access. Without provisioned management auth the endpoint is unavailable.
- Reads use one read-only SQLite transaction. The projection excludes task text, provider answers, tool arguments, arbitrary paths and private outcome references. No source writes or new persistent ledger.
- Coverage is this machine only. The reader never guesses a Beads work ID, Inbox owner, current-running state or fleet completeness from a session or workspace hash.

## Bounds and accounting

The reader accepts a 1–90 day window and returns the newest 50 decisions. Larger dashboard windows show the latest 90 days for Jev. Each decision includes at most 10 recent owner actions; truncation is explicit. Malformed, mismatched, future-dated and out-of-order records are excluded with partial coverage. A malformed outcome does not erase its valid parent decision. Missing, busy, disabled and unreadable stores have distinct explanations. An empty window does not establish whether hooks are active, and rejected records are distinguished from no observed records.

Reads reject symlinked stores/sidecars, combined database/journal files above 64 MiB and JSON records above 64 KB. The cap remains explicit because the current source does not promise timestamp/outcome indexes; row limits alone do not bound those scans. The dashboard explains a source outside the read limit. Outcomes for the selected decisions are fetched in one pass. A busy store has a 100 ms lock timeout. The server retains one result for up to 15 seconds; changing windows within that interval returns an explicit retry state and cannot trigger another scan. It creates no background job. `/status` continues to use its existing cached data and does not read Jev.

Costs cover the distinct request hashes referenced by the displayed decisions. They are not a complete provider bill. Repeated decisions, offers and cache references do not multiply request cost. Missing or conflicting costs remain unknown. Estimates and billed costs are separate.

## Next integrations, in value order

1. **Failure and waste triage.** Join saved rescue/waste advice to verified native task ownership and subsequent outcome evidence. Measure fewer repeated failures or repeated work, including Jev overhead, against comparable runs.
2. **Completion evidence review.** Use readiness/change advice to surface unsupported completion claims while retaining the original checks and accountable owner. Track false reassurance and missed failures.
3. **Related telemetry.** Classify ambiguous event groups only after deterministic joins and filters. Start with a bounded initial classification and at most one evidence-driven refinement; keep unknown as a valid outcome.

Memory retention and relationship changes belong to the memory system's own provenance and retention policy. They should consume verified telemetry signals later, rather than letting this dashboard delete or rewrite memory.

Jev's official [hierarchical classification](https://docs.typesafe.ai/cookbooks/hierarchical_classification) and [parallel questions](https://docs.typesafe.ai/cookbooks/parallel_questions) patterns support structured classification and independent questions. Extra passes require a measurable accuracy benefit; a loop alone does not establish greater reliability. The present product slice adds no inference loop.

## Verification

`pnpm proof:jev-analysis` checks native joins, privacy, cost deduplication, uncertain outcomes, malformed/missing/bounded sources, source immutability and the authenticated HTTP boundary. `pnpm proof:dashboard-security` exercises the actual dashboard in desktop/mobile Chrome with hostile Jev field contents and checks text rendering, partial coverage, network confinement and existing settings behavior.

Native canary snapshots are integration-test inputs only. Never import them as production activity or represent their classifications as measured savings. Run `pnpm typecheck` and the repository's required CI checks before release.
