# 0035 — Workspace backfill: the hosted workspace holds the machine's full ledger history, idempotently, with a reconciliation audit

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/62

## TL;DR
- `plimsoll upload-history` pushes the machine's ENTIRE local ledger history to the joined workspace ingest — batched ≤500, HMAC-signed, paced, resumable — then prints a per-source × per-month reconciliation audit (local events + tokens vs accepted, skips itemized with reasons).
- Idempotency is the invariant: ledger event ids are cloud-conformant UUIDs (deterministic from the tailers; the normalizer guarantees UUIDs at append) and the cloud dedupes by id (bulk `createMany(skipDuplicates)` since cloud PR #19; per-event upserts before that), so re-runs cannot duplicate. Non-UUID ids derive the SAME UUID every run (`deterministicEventId(["workspace-backfill", id])`), original preserved in `metadata.externalEventId`. The server's additive `inserted` response field reports genuinely-new rows — run 2 of the idempotency proof shows `inserted: 0`.
- The live 5-minute sync drains the same backlog slowly (~7k events/hour observed); `upload-history` is the accelerator and runs alongside the daemon — ledger opened strictly READ-ONLY, daemon never touched, rows never marked uploaded.

## Scope
A history-upload command in collector-cli (sibling of `upload`): full-history walk → workspace push → audit. Does NOT mark rows uploaded, does NOT upload `metric_samples` (parity: the sync path never does either), changes nothing cloud-side, descriptive own-data only.

## Context
- Ledger: `~/Library/Application Support/Plimsoll/work-ledger.sqlite` (WAL, live daemon writing); ~821k events at survey (2026-06-11), all ids pass the cloud UUID regex; max payload 12.4 KB.
- Wire shape: exactly `upload.ts` — `{tenantId, installKey, appVersion, events:[{event, suppressedFields}]}` with `x-plimsoll-install-key` / `x-plimsoll-upload-timestamp` / `x-plimsoll-upload-signature: sha256=<hmac of "${ts}.${body}">`; cloud dedupes by event id.
- Measured ingest latency at build time: ~300 ms/event server-side (per-event upserts) → bounded client concurrency (1–8). Historical: the cloud's bulk-ingest fast lane (cloud PR #19, one `createMany(skipDuplicates)` per batch) landed mid-rollout and cut a 500-event batch from ~150 s to ~1 s (~15 evt/s → ~525 evt/s observed live).
- 5 ledger rows failed the strict event schema at survey: `sessionId: null` json_set artifacts from `reconcileCodexUsage` (buffer.ts). `upload-history` repairs them (top-level null-strip); the daemon's own `buildIngestBatch` will WEDGE on them when its oldest-first queue arrives there — separate fix needed (see Open Questions).

## Design decisions
- **Naming**: "backfill" in this repo means INTO the ledger (`scan-rollouts`, `backfill-v1-archive`); cloud direction is "upload" → `upload-history`.
- **Read-only ledger**: opened `{ readonly: true, fileMustExist: true }`; rowid-keyed pagination (indexed, stable; the repo never VACUUMs, and the watermark id is re-verified on resume).
- **Resume watermark** (`workspace-backfill-state.json`, 0600, target-fingerprinted with sha256(url|tenant) — no secrets): advances only over the contiguous frontier of succeeded batches; the audit folds in at the same frontier so resumed runs never double-count. Correctness never depends on it — ids are idempotent.
- **Default scope**: rows with `created_at <=` run start; resumed runs keep their stored scope; `--until` pins it explicitly (run 2 of the idempotency proof reuses run 1's value); `--full` re-walks everything.
- **Fail closed**: 400/401/403 abort immediately with guidance; 429/5xx/network retry ≤5 with backoff + Retry-After.
- **Redaction**: server ingest responses are never echoed (they contain the install key); only `accepted` counts are read.

## Acceptance Criteria
- [x] `upload-history --dry-run` audits with zero network (proofed via loopback fetch counter on the unjoined path + dry-run mode).
- [x] Live run completed 2026-06-11 (T1 scope = 2026-06-11T20:18:04Z): 832,886/832,886 accepted, 0 skipped, audit per source × month with honest cost (unpriced never $0.00); evidence on the GitHub issue.
- [x] Server-side verification (read-only Prisma): every source × month cell matches the audit EXACTLY (codex 04: 2,930 / claude 05: 1,009 / codex 05: 213,294 / claude 06: 98,061 / codex 06: 517,592); `count(distinct id) = count(*)` in every cell; observedAt span 2026-04-23T23:57:38Z → 2026-06-11T20:17:47Z.
- [x] Second full run over the same `--until`: identical audit, accepted 832,886, server-reported `inserted: 0` across all 1,666 batches, cloud count delta EXACTLY 0 (832,886 before and after; 0 rows created in the run-2 window).
- [x] `pnpm proof` green: 66 → 76 checks; `history_batches_obey_ingest_contract` demonstrably fails when the ≤500 cap is removed.

## Operational Boundaries
- Ledger read-only; LaunchAgent daemon untouched; `collector.config.json` untouched; install key / signing secret never printed.
- Privacy: upload.ts envelope parity; client-side `findForbiddenRawContentFields` gate before send (proofed with a raw-prompt sentinel).
- `pnpm proof` stays green; cloud rows only via the public ingest API.

## Notes For Future Agents
- The ingest response echoes `installKey` — anything that prints a response must redact.
- Cloud DB growth: ~820k rows ≈ 700 MB+ table+index growth. The daemon's drain was already heading there; plan headroom is a deploy-owner call.
- The ~300 ms/event sequential-upsert bottleneck was fixed mid-rollout by the cloud's bulk fast lane (plimsoll-cloud PR #19); observed live throughput went ~15 → ~525 → ~1,560 evt/s. First-writer-wins: re-sends no longer refresh enriched payloads on existing rows.
- The resume watermark proved itself live twice: the run-1 process was killed mid-flight (54,500 events in) and resumed exactly, zero loss, zero double-count.

## Open Questions
- Should `buildIngestBatch` get the same null-strip normalization so the daemon can't wedge on stitch artifacts (follow-up sounding)?
