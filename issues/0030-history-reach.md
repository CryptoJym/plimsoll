# 0030 — History reach: the ledger holds everything that exists on disk

## TL;DR
- Owner (2026-06-10): "it isn't tracking far enough in the past — should be a lot more if
  the data exists." It did: ~/.claude/projects transcripts reach 2026-04-06 (368 files)
  while the ledger's claude history started 2026-05-18 and was v1-thin before 06-10.
- Shipped: Claude transcript tailer (mirror of the codex rollout tailer — per-message
  usage, dedupe by message id, sourced Anthropic estimates flagged costEstimated, repo
  linkage from cwd, live-covered sessions skipped, content never parsed beyond assistant
  usage lines, never persisted); dashboard window selector (30d → all history, clamp 5y);
  prune semantics fixed — retention now ages out UPLOADED rows only (the backfills were
  all created the same day; a created_at prune would have erased months in one sweep).
- Codex side audited: 2026/03 rollouts dir is EMPTY on disk (pre-plimsoll cleanup), so
  April-23-onward IS everything that exists. Honest floor, documented.
- Anthropic rates added (platform.claude.com pricing, fetched 2026-06-10) with correct
  Anthropic semantics (input excludes cache reads — no OpenAI-style clamp). Cache writes
  still excluded from estimates (no column, issue 0024) → estimates are floors.

## Acceptance (proof, 60 checks)
- [x] transcript_usage_ingested_exact_and_deduped (exact $0.2395 fable estimate)
- [x] transcript_live_covered_session_skipped · transcript_rescan_idempotent_and_content_free
- [x] retention_prune_spares_unuploaded_history
- [ ] Live: claude ledger reach ≈ 2026-04-06; "all history" window renders; capture watch green.
