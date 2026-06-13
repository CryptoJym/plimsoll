# 0037 — Session sync: the workspace holds REAL session rows that join to their events

## TL;DR
- Hosted `AiWorkSession` was 0 rows while the ledger stitched sessions for every event — per-session and per-person analytics (cloud #24 Phase D3/D4) had nothing to stand on. Now the collector pushes one SNAPSHOT per stitched session (`kind: "session_sync"` on the existing ingest route) and the cloud upserts them grow-only by deterministic session id.
- Live run on the real workspace: **4,185 sessions** (4,066 codex / 119 claude_code) created in 5.96s through the full signed HTTP path; idempotent re-run inserted 0 with a byte-identical content fingerprint; joins verified in all three id forms with token totals reconciling exactly.
- The hosted admin "Sessions" KPI switches from the event-derived placeholder (733,923 — every sessionless event counted itself) to real rows (4,185), with the provenance labeled either way.
- Two surfaces ship it: the daemon's 5-minute sync refreshes the sessions its uploads touched (zero new state — failure carries ids to the next cycle), and `upload-history --sessions` is the full backfill + post-restart recovery tool.

## Scope
A session push lane in collector-cli (sibling of `upload-history --repair-attribution`): ledger session snapshots → workspace upsert → reconciliation audit. Cloud half (plimsoll-cloud `session-ingest` branch): the discriminated batch lane, the grow-only tenant-guarded upsert, the event-lane UUIDv7 session-id fix, dashboards preferring real rows. It does NOT backfill `session_id` onto already-uploaded event rows (those join via `metadata.externalSessionId` — see Notes), does NOT push sessionless events as sessions, and stays descriptive own-data sync (open/paid boundary).

## Context
- Ledger at run time: 1,501,255 events; 742,822 carry a session id across 4,185 distinct sessions (codex ids are **UUIDv7**, claude ids UUIDv4, plus one literal `hook-fix-verify` test id).
- How session ids crossed historically: the cloud event lane accepted only UUID v1–5 into the `session_id` uuid column — claude v4 ids landed in the column (99,067 events / 104 sessions on the cloud), every codex v7 id was exiled to `metadata.externalSessionId` with a NULL column (443,273 events / 3,881 sessions). Postgres' uuid type accepts ANY version — the strictness was inherited, not designed.
- The join contract that fixes it: session row id = ledger session id VERBATIM-lowercased when Postgres-uuid-shaped (any version), else `deterministicEventId(["session-sync", raw])` with the raw id kept in `session.metadata.externalSessionId`. The cloud event lane now accepts the full Postgres uuid shape for `sessionId` (`classifySessionId`), so future codex events join via the column too.
- Update semantics (cloud, documented in `src/lib/session-sync.ts`): sessions GROW (endedAt advances, totals climb) and every upload is a full ledger-recomputed snapshot — so **grow-only last-writer-wins**: the latest snapshot wins only when at least as complete (`ended_at` later, or tied with `totals.events` not lower). Replayed stale snapshots and ledger-prune shrinkage become no-ops (`skippedStale`); a cross-tenant id forge is refused by the tenant guard in the conflict WHERE.
- Wire shape: `{kind:"session_sync", tenantId, installKey, appVersion, sessions:[{session, totals}]}` ≤500/batch, same install-key + HMAC transport (`postHistoryBatch`). Totals are TYPED (events/input/output/cacheRead/cacheCreation/pricedEvents/costUsd) so reconciliation is checkable; only linkage hashes cross (dominant repo/branch pair, dominant account hash); machine hostnames never do.

## Evidence (live run, 2026-06-13, tenant 753a5a4f…)
Backfill run 1 (`upload-history --sessions --until 2026-06-13T00:55:06.453Z`):
```
{"status":"session_sync_done","ledgerSessions":4185,"eligibleSessions":4185,"skippedSessions":0,"sentSessions":4185,"acceptedSessions":4185,"insertedSessions":4185,"updatedSessions":0,"batches":9,"durationMs":5957}
TOTAL  4185 sessions  742822 events  $22860.78 (180924/742822 priced)  sent 4185  accepted 4185
session ids deterministically re-derived to UUID: 1
```
Idempotent re-run (same `--until`): `insertedSessions: 0, updatedSessions: 4185` — and the DB content fingerprint (md5 over id|started|ended|projectKey|totals of all rows) is **byte-identical** before/after: `ab23814cbe944af805fd47be1b959adb`, sessions 4185, totals events 742,822.

Join coverage (read-only Prisma on the live DB): of 832,886 cloud events, 99,067 join via `e.session_id = s.id` (**100%** of column-bearing events), 443,272 of 443,273 via `lower(e.metadata->>'externalSessionId') = s.id::text`, and the 1 remaining (raw id `hook-fix-verify`) joins via matching `externalSessionId` on both rows. Sample reconciliations — exact:
```
claude (column form)  01b3d796-1501-4eda-a296-9b4ccdaa1ec9: 534 event rows = totals.events 534; input 58140=58140; output 283399=283399
codex (metadata form) 019dbcc6-d26c-7c82-84cb-a211da747e46: 12 = 12; input 1514101=1514101; output 4954=4954
```
Dashboard data path on the live DB: `counts = {totalEvents: 832886, sessions: 733923, sessionRows: 4185}` → KPI renders `Sessions · synced 4,185` (provenance `session_rows`); a tenant with no session rows keeps the event-derived count labeled `event-derived`.

## Acceptance Criteria
- [x] Cloud ingest accepts a `session_sync` batch behind the same install-key+HMAC auth; event fast lane untouched (one createMany, unchanged).
- [x] Upsert is ONE set-based statement per batch, tenant-guarded, grow-only; stale replay and cross-tenant forge are no-ops (proof + live smoke on a `d1-` fixture tenant, cleaned after).
- [x] Deterministic session ids: uuid-shaped pass through lowercased; non-uuid derive stably in a namespace distinct from event ids (proof-pinned).
- [x] The join works for BOTH historical forms and the derived form, proven with live join queries; token totals reconcile exactly on fully-drained sessions.
- [x] 5-minute sync pushes touched sessions; failure isolated from the event backoff; ids carry over in memory.
- [x] `upload-history --sessions` full backfill with dry-run, `--until` scoping, reconciliation audit (unpriced never $0.00), skips itemized by reason.
- [x] Idempotency: second run over the same `--until` reports inserted 0 and the cloud row content is unchanged (fingerprint-verified).
- [x] `pnpm proof` green both repos with new checks: public 80 → 85, cloud 103 → 108.

## Operational Boundaries
- Ledger opened READ-ONLY (or the daemon's own live handle, reads only); the LaunchAgent daemon never stopped, restarted, or reconfigured. `collector.config.json` untouched; install key never printed.
- Metadata mode only: hashes + counters cross, raw content never; client-side forbidden-field gate before send; cloud gate re-checks.
- The daemon half activates on the next collector restart/release — until then `upload-history --sessions` covers new sessions (note: owner restart implication).

## Notes For Future Agents
- Pre-D1 codex event rows keep `session_id` NULL (events are immutable, first-writer-wins). Their join runs through `metadata.externalSessionId` — works, but unindexed. If D3/D4 want column-grade joins on history, build a session-id repair lane exactly like attribution repair (`{id, sessionId}` pairs, set-based fill-only) — the ledger has every pair.
- Session totals are LEDGER truth; event-row sums on the cloud lag the drain for sessions whose events are still queued (ledger 1.50M vs cloud 832,886 at run time). Reconcile only on fully-drained sessions.
- The cloud's `skippedStale` counter is the grow-only guard working: replayed stale snapshot, or (only under id forgery) the tenant guard refusing a cross-tenant write.
- `kind` on the shared route now discriminates THREE lanes: `attribution_repair`, `session_sync`, default events. An old cloud rejects `session_sync` batches 400 fail-closed (schema drift), which is the correct failure mode.

## Open Questions
- Should `AiWorkSession.deviceInstallId` be filled from the authorized install (machine-bound sessions for multi-Mac tenants), now or with D4 actor binding?
- Should the dashboards also surface per-session rollups (events/cost per session) now that rows exist, or wait for D3's hosted VDY?
