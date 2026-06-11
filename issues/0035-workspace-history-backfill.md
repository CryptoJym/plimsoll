# 0035 — Workspace backfill: the hosted workspace holds the machine's full ledger history, idempotently, with a reconciliation audit

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/62

## TL;DR
- `plimsoll upload-history` pushes the machine's ENTIRE local ledger history to the joined workspace ingest — batched ≤500, HMAC-signed, paced, resumable — then prints a per-source × per-month reconciliation audit (local events + tokens vs accepted, skips itemized with reasons).
- Idempotency is the invariant: ledger event ids are cloud-conformant UUIDs (deterministic from the tailers; the normalizer guarantees UUIDs at append) and the cloud upserts by id, so re-runs cannot duplicate. Non-UUID ids derive the SAME UUID every run (`deterministicEventId(["workspace-backfill", id])`), original preserved in `metadata.externalEventId`.
- The live 5-minute sync drains the same backlog slowly (~7k events/hour observed); `upload-history` is the accelerator and runs alongside the daemon — ledger opened strictly READ-ONLY, daemon never touched, rows never marked uploaded.

## Scope
A history-upload command in collector-cli (sibling of `upload`): full-history walk → workspace push → audit. Does NOT mark rows uploaded, does NOT upload `metric_samples` (parity: the sync path never does either), changes nothing cloud-side, descriptive own-data only.

## Context
- Ledger: `~/Library/Application Support/Plimsoll/work-ledger.sqlite` (WAL, live daemon writing); ~821k events at survey (2026-06-11), all ids pass the cloud UUID regex; max payload 12.4 KB.
- Wire shape: exactly `upload.ts` — `{tenantId, installKey, appVersion, events:[{event, suppressedFields}]}` with `x-plimsoll-install-key` / `x-plimsoll-upload-timestamp` / `x-plimsoll-upload-signature: sha256=<hmac of "${ts}.${body}">`; cloud upserts by event id.
- Measured ingest latency: ~300 ms/event server-side (sequential upserts) → bounded client concurrency (1–8) instead of a sequential ~68 h walk.
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
- [ ] Live run completes; audit table per source × month with honest cost (unpriced never $0.00), skips itemized.
- [ ] Server-side verification (read-only Prisma): per-source counts match accepted; `count(distinct id) = count(*)`; observedAt span covers the ledger's history.
- [ ] Second full run over the same `--until`: same accepted totals, cloud row count UNCHANGED (delta 0).
- [x] `pnpm proof` green: 66 → 76 checks; `history_batches_obey_ingest_contract` demonstrably fails when the ≤500 cap is removed.

## Operational Boundaries
- Ledger read-only; LaunchAgent daemon untouched; `collector.config.json` untouched; install key / signing secret never printed.
- Privacy: upload.ts envelope parity; client-side `findForbiddenRawContentFields` gate before send (proofed with a raw-prompt sentinel).
- `pnpm proof` stays green; cloud rows only via the public ingest API.

## Notes For Future Agents
- The ingest response echoes `installKey` — anything that prints a response must redact.
- Cloud DB growth: ~820k rows ≈ 700 MB+ table+index growth. The daemon's drain was already heading there; plan headroom is a deploy-owner call.
- Per-event ~300 ms is the cloud's sequential upsert loop; a server-side batched upsert would remove it (private repo).

## Open Questions
- Should `buildIngestBatch` get the same null-strip normalization so the daemon can't wedge on stitch artifacts (follow-up sounding)?
- Cloud-side batch upsert to cut ~300 ms/event — private-repo issue?
