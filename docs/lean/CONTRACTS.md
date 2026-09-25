# Lean Plimsoll contract register (round 7 = B0, eco-6hoxj.164.4)

**As of:** 2026-09-25 MDT · **Owner:** B0 (contract owner; documents and failing tests only). This file is normative for the
five items the round-6 review left to B0: the two blockers and the four `b0Carries` items (`input/review-r6/VERDICT.json`). Where
it and a round-6 document disagree, this file wins and the round-6 document carries a "round 7" note pointing here. Every rule
below has (a) a runnable fixture in `fixtures/` (red under `--rule r6`, green under `--rule r7`; `checks/fixtures-summary.json`)
and (b) a **pending test** in the repository that owns the rule (§C6), which fails today and names the bead that turns it green.
Nothing here is implemented; nothing here is frozen until the lead signs the freeze list (`FREEZE.md`).

## C1. One actor-ownership predicate (blocker 1; B6 cloud, B2a collector)

**The defect.** Round 6 wrote two validators. A delivered stamp had to satisfy `V exists, V ≤ actorBindingVersionHeard ≤ current`;
an undelivered stamp only `V exists, V ≤ current` plus a rowid-monotonic list check. For a version the cloud has issued but the
collector has not yet echoed (version 1 supplied by the 300 response, first echoed by the 650 request) a row stamped 1 was null on
the delivered path and actor B on the undelivered path (`input/review-r6/checks/reviewer-new-counterexamples.log`).

**The rule.** `actor_for_stamp(install, stamp)` is the only ownership function, and both paths call it:

| Stamp | Delivered raw row (ingest) | Undelivered member (part) |
|---|---|---|
| `V` such that `0 ≤ V ≤ install.binding_version` at judgment (`V = 0` is the registration binding, `V ≥ 1` the audit row that created version `V`) | `actor_id = actor_of(install, V)`; basis `raw_ingest` (identical to `binding_at_capture` by construction) | `actor_id = actor_of(install, V)`, basis `binding_at_capture` |
| `V` the cloud never issued for that install (`V > binding_version`, or no audit row for `V ≥ 1`) | `actor_id = null`, `metadata.actorStampInvalid = true`, listed in the S4 certify | `actor_id = null`, basis `unallocated_stamp_invalid`, candidates = every actor in the install's history plus its current actor |
| `null` (a collector older than B2a, or a B2a collector before its first response, C4) | today's ingest binding (the install's actor at ingest); basis `ingest_current` | `single_binding` (the install has no audit row and a non-null actor) else `unallocated_no_stamp` |

The stamped rows are therefore owned by the same actor whether or not they were delivered, for every ordering of capture,
rebind, contact and delivery, and a dead delivery replayed after a later rebind resolves as its first attempt would have. The
**null-stamp row remains the one disclosed exception**, now stated as confined to `stamp = null` on a rebound install, whichever
collector version wrote it (C4).

**What the echo is for.** `actorBindingVersionHeard` (every request) and the rowid order of the versions in a segment are
**diagnostics, never ownership**: `stamp_ahead_of_echo` (a request carries a stamp above its own echo) and
`stamp_sequence_regressed` (a segment's versions are not non-decreasing in rowid order) are recorded on the request receipt and
the segment, shown in `/status`, the S4 certify report and the impact report, and change no actor. `heard_at` on the audit row is
the instant of the **first request that echoes** the version: a disclosure fact for the export's `changed_at → heard_at` window.
A collector's echo is `max(persisted version, highest stamp in the request)`, so an honest collector never trips the diagnostic.

**The corrected timeline** (fixture `b4_offline_rebind.py`): registration supplies version 0; contact 50 echoes 0; contact 300
echoes 0 and its response first supplies 1; contact 650 first echoes 1 (`heard_at(1) = 650`) and its response supplies 2, which
has no `heard_at` until the next contact. A row captured at 350 is stamped 1 (the collector persisted 1 at 300) and belongs to B
on both paths whether delivered at 400 (before the echo) or at 700. Cases covered: A-H in the fixture: before the rebind,
across it, the reviewer's offline capture (stamp 0 after the rebind), the issued-but-unheard stamp, a delivery after a second
rebind, an undelivered row, a never-issued stamp (5), and a pre-first-response null stamp on a never-rebound install; plus the
replay and the rebound-install null-stamp exception.

**Judgment state.** Validity is decided against the install's audit table at the time the event or the item is judged; a
version that does not exist then is invalid on both paths. Parts are computed per received revision and frozen at the seal; a
raw row's actor is bound once at ingest. A stamp that the cloud never issued is a collector or cloud defect (`stamp_not_issued`
on the certify), not an expected state.

**Tests.** Cloud `tests/contracts/lean/actor-binding-stamp.contract.test.ts` (B6): the predicate table above, the eight
orderings, the reviewer's counterexample, the replay, `firstEchoHeardAt`, the diagnostics, `eventRowsForStorage` binding by
stamp, and `acknowledgedResponse` carrying `actorBindingVersion`. Collector `tests/contracts/lean/actor-stamp.contract.ts`
(B2a): the stamp on the identity row and on the wire, the echo on every batch, null before the first response.

## C2. S1b runway: G per host, one G, G remaining (blocker 2 and should-fix 5; B1 and B10a collector)

**The defect.** `G_host = usage_rows × 1,632 B + raw_rows × 309 B` embeds Studio1's session, day and segment density in the
1,632 B. At 333 sessions per 1,000 usage rows the same row widths need 507 MB against a 457 MB gate, and a host exactly at
the 63-day start threshold has 58.06 real days. The multiplier used the unfactored `G_host` while the numerator used `1.25 ×
G_host`, and the continuous runway kept subtracting full G after conversion had already consumed part of it.

**Evidence that the mix is host-specific.** A read-only census on a `VACUUM INTO` copy of this host's (Studio4's) ledger
(`checks/host_cardinality_census.py`, `checks/studio4-cardinality-census.json`, 2026-09-25 14:21 MDT, 2.4 s on the copy): 79,259
usage rows, 352,885 raw rows (3.45 activity rows per usage row; Studio1 has 0.61), 5,141 sessions (64.9 per 1,000 usage rows;
Studio1 47), 5,654 session-day rows, 455 rollup buckets, 34 day targets, raw table 833 MB, ledger 1,504 MB. Round 6 assumed
174,820 usage rows and 281,249 raw rows for Studio4 by scaling Studio1's counts by ledger size: more than double the real
usage count and a fifth of the real activity ratio. Scaling is not measuring.

**The rule.**

1. **G from counted cardinalities.** `G_host = usage_rows × (F + T) + sessions × S + session_day_rows × SD + (model_day_rows +
   activity_day_rows) × DR + rollup_buckets × RB + (1.2 × sessions + rollup_buckets + day_targets) × SEG + raw_rows × I`, with the
   round-5 row widths (`F` 1,050, `T` 364, `S` 1,300, `SD` 390, `DR` 390, `RB` 400, `SEG` 830, `I` 309 bytes, estimates from
   column widths) and **every cardinality counted on the host itself**: on a light host by B1's one-time read-only census on a
   `VACUUM INTO` copy of the ledger (the query in `checks/host_cardinality_census.py`; 2.4 s on Studio4's copy; never a scan on
   Studio0), on a busy host and on Studio0 by the converted copy's measured peak (unchanged). A host whose counts were scaled from
   another host, or not counted, has **no gate value** and cannot start a hold. The 1.25 factor covers the **row widths** until
   S2 has measured them on a copy; it no longer covers the mix, which is measured.
2. **One G.** `G_gate = 1.25 × G_host` (or the copy's measured peak) is subtracted in the runway numerator **and** sizes the
   multiplier: `g_host = G_gate / raw_bytes`, `hold_growth_per_day = p95_7d(gross growth) × (1 + g_host × raw_share)`.
3. **G remaining.** While conversion runs, `G_remaining = max(0, G_gate − new_table_bytes_now)` where `new_table_bytes_now` is the
   bytes the lean tables occupy (their pages, from the daily `dbstat` sample on light hosts or the converter's own page counter
   elsewhere); `runway_days = (free_disk − reserve − G_remaining − rebuild_headroom) / hold_growth_per_day`. Bytes the converter
   has already written are in `free_disk` and are never subtracted twice; after S3 completes `G_remaining = 0`. The abort
   ladder's rungs read this runway. A `/status` field `runway.g = {gate, remaining, basis: census | copy_peak}` discloses both.
4. **Preflight receipt.** The S1b receipt records the census (counts, when, on which copy), `G_gate`, its basis, the measured
   conversion size and abort peak where a copy exists, and the runway series.

**Tests.** Collector `tests/contracts/lean/runway.contract.ts` (B1, B10a): the reviewer's 333-session counterexample (the gate's
G bounds the host's real need; at the gate's 63-day threshold the host really has ≥ 63 days), the Studio4 census as a second
host, an unmeasured host has no gate value, the multiplier uses `G_gate`, G remaining mid-conversion and the < 5-day rung, and the
round-6 `s1b_runway_geometry.py` assertions. Fixture: `fixtures/s1b_runway_host_bound.py`.

## C3. `conversion_rejects` DDL (b0Carries 3a; B2a collector)

```sql
create table if not exists conversion_rejects (     -- one row per stored raw row the converter or the live writer could not fold
  event_id text not null, raw_generation text not null default '',   -- identity: the raw row's id and privacy generation (never rowid)
  raw_rowid integer,                                -- live pointer while the raw row exists; null after its deletion
  epoch_key text not null default '', source text not null,
  reason text not null check (reason in ('contract_violation','payload_unreadable','day_key_unresolvable')),
  detail text,                                      -- bounded, value-blind (the parser's error class; never the stored string)
  observed_at_raw_digest text not null,             -- sha256 of the stored observed_at string, so the census can match it without copying it
  gap_id text not null references capture_gaps(gap_id),   -- the counted capture_gap (reason contract_violation) that carries it in G1
  first_seen_at_ms integer not null, last_seen_at_ms integer not null, attempts integer not null default 1,
  resolved_at_ms integer,                           -- set when a later pass folds the row (a parser fix); the row is then retained, not deleted
  primary key (event_id, raw_generation)
);
create index if not exists idx_cr_open on conversion_rejects (resolved_at_ms) where resolved_at_ms is null;
```

Retention: **never deleted by age or by budget pressure** (it is in the never-delete set of ARCHITECTURE.md §2.3); a resolved
row keeps its record with `resolved_at_ms`; the table is compacted only by B14 with the same proof as `capture_root_observations`.
`/status` lists `conversionRejects = {open, resolved, byReason}` and the S3 certify (f) lists every open row by id. Its raw row is
in the never-delete set while the reject is open. Test: collector `tests/contracts/lean/schema.contract.ts` (DDL) and
`converter.contract.ts` (a NaN string becomes one counted gap and one reject; nothing vanishes; `b5_non_iso_day_facts.py`).

## C4. Null stamps before the first response (b0Carries 3b; B2a collector, B6 cloud)

A B2a collector stamps every raw row with the highest `actorBindingVersion` it has **persisted**. It persists a version from any
authenticated response (the registration or enrollment response included, where the route exists; B6 verifies each route).
Until the first such response it writes **null**, exactly like a pre-B2a collector: a fresh install's rows before its first
contact, and an upgraded install's rows before its first post-upgrade response, carry `actor_binding_version = null`. The cloud
treats every null stamp by the C1 exception, so the exception is stated as "stamp = null on a rebound install", not as
"older collectors". The collector's `/status` shows `actorBinding = {version, heardAt, stampedRows, nullStampedRows}` so the size
of the exception is visible per host, and B2a's acceptance counts the null-stamped rows admitted before the first response on
the Studio5 copy. Test: collector `actor-stamp.contract.ts` (null until the first response; stamped after); cloud
`actor-binding-stamp.contract.test.ts` (case H, the rebound-install exception).

## C5. `b22_false_complete.py` with repository-relative paths (b0Carries 4; B22 collector, B6 cloud)

The document half reads `BEADS.md`, `PROOF.md` and `ARCHITECTURE.md` from `--docs DIR`, else from the directory beside the
fixture set, else from `docs/lean/` at the enclosing repository root (`fixtures/b22_false_complete.py`). The collector branch
carries the documents under `docs/lean/` and runs the document half as `tests/contracts/lean/b22-documents.contract.ts` (green
today: a guard, not pending). The **behavioural half** is ported beside sf5: collector `capture-gaps.contract.ts` (an
`unresolved:*` or never-read file is declared with `interval_basis = 'epoch_open'`, `started_at_ms` = the epoch start,
`ended_at_ms = null`; a period after the file's last write is not complete while the file is unparsed; the gap resolves on parse)
and cloud `capture-coverage.contract.test.ts` (`captureCoverageForPeriod` treats a persisted open gap as overlapping every later
period, and a later claim cannot erase it).

## C6. Pending contract tests: convention and surfaces

**Convention.** Both repositories use `node:test`. A pending test is declared `test(name, pending("B6"), fn)`; `pending` sets
`todo: "pending until B6 lands"`, so node runs it, prints its failure, counts it under `# todo`, and **exits 0**. The suites run in
CI as their own step (`pnpm test:contracts:lean` in plimsoll-cloud, `pnpm contracts:lean` in plimsoll); a test whose `pending`
marker is removed becomes blocking. The implementer of a bead removes the marker in the same change that turns the test green.
Every test names its bead in its title and cites the CONTRACTS.md section it binds. Test files live outside the default test
globs and, in the collector, outside `tsconfig` `include`, so they never break `tsc --noEmit`; in the cloud they are typechecked
and linted, so a missing surface is loaded at run time through `loadSurface()` and fails as a test, not as a type error.

**Surfaces the cloud tests bind (plimsoll-cloud, `tests/contracts/lean/`).**
- `src/lib/actor-binding-stamp.ts`: `actorForStamp({ stamp, path, binding })` → `{ actorId, basis, stampInvalid? }`;
  `firstEchoHeardAt(contacts)`; `stampDiagnostics({ stamp, echoed, currentVersion })`. (B6, C1, C4)
- `src/lib/ingest.ts`: `eventRowsForStorage(batch, tenantId, authorizedActorId, { binding })` binds `metadata.actorBindingVersion`
  through `actorForStamp`; sets `metadata.actorStampInvalid`. (B6, C1)
- `src/lib/delivery-ack-response.ts`: `acknowledgedResponse` emits `actorBindingVersion` for a registered install
  (`CollectorUploadAuthorization` gains `actorBindingVersion: number`). (B6)
- `src/lib/activity-summary/contract.ts`: `ACTIVITY_SUMMARY_PAYLOAD_KIND`, `isActivitySummaryPayload`, `activitySummaryBatchSchema`,
  `SUMMARY_ITEM_KINDS` (with `day_summary`); `src/lib/activity-summary/judge.ts`: `judgeSummaryItems(items, stored)`;
  `src/lib/activity-summary/actor-parts.ts`: `actorPartsForSegment({ members, install })`. (B6)
- `src/lib/capture-watermark/contract.ts`: `captureCoverageForPeriod` with `until: null` open gaps and `resolvedAt`. (B6, C5)
- `src/lib/economics/token-volume.ts`: `tokenVolumeState`, `normalisedTokens`, `pricingGate`. (B15)
- `prisma/schema.prisma`: `DeviceInstall.bindingVersion`, `DeviceInstall.activityLaneClosedAt`, `DeviceInstallActorBindingAudit.bindingVersion`
  and `.heardAt`, models `AiSummaryActorPart` and `CaptureGap`. (B6)

**Surfaces the collector tests bind (plimsoll, `tests/contracts/lean/`).**
- `packages/collector-cli/src/lean/schema.ts`: `ensureLeanSchema(db)` (ARCHITECTURE.md §3 DDL v3 plus C3) on the ledger connection
  (or one exposed as `buffer.leanDatabase`), which must keep `foreign_keys = ON`: **finding (B0):** better-sqlite3 opens every connection
  with foreign keys enforced, so at `03445d3a` the ledger connection already has `PRAGMA foreign_keys = 1` (the guard test
  `schema.contract.ts` last case is green today); the round-6 note that B2a must "set" it becomes "must not turn it off";
  `raw_retention_control.hold_reason`; `collector_workspace_binding.actor_binding_version`; `raw_retention_receipts` accepting the four
  reasons. (B2a, B10a, B10b)
- `packages/collector-cli/src/buffer.ts`: `prune` deletes nothing while `hold_reason` is set; `retentionProgressStatus().hold`;
  `recordActorBindingVersion(version)`; `workspaceBinding().actorBindingVersion`; `summary_members` written at `append` with the stamp
  and two edges when `lean.write` is on (`new LocalEventBuffer(path, { lean: { write: true } })`). (B10a, B2a)
- `packages/collector-cli/src/upload.ts`: `buildIngestBatch(...).batch.actorBindingVersionHeard`; `packages/shared/src/schemas.ts`:
  `aiWorkIngestBatchSchema` accepts it; `analytical-metadata.ts`: `metadataKeyDisposition("actorBindingVersion")` is an identifier;
  `outbound-envelope.ts`: `sealOutboundEnvelope` keeps it. (B2a)
- `packages/collector-cli/src/lean/runway.ts`: `LEAN_ROW_WIDTHS`, `estimateHostG(census)`, `holdRunway(input)`. (B1, B10a; C2)
- `packages/collector-cli/src/lean/day-key.ts`: `utcDayOf`, `censusClass`, `dashboardWindowSince`; `DASHBOARD_SCHEMA_VERSION = 3`. (B5)
- `packages/collector-cli/src/lean/converter.ts`: `convertLedgerHistory(buffer, options)`. (B2a; C3)
- `packages/collector-cli/src/lean/capture-gaps.ts`: `declareUnresolvedFileGap`, `resolveCaptureGap`, `coverageCompleteForPeriod`. (B22; C5)
- `packages/collector-cli/src/lean/retention.ts`: `applyUploadReceipts`, `releaseUnderLadder`, `retireSegment`. (B2a, B10b)
- `packages/collector-cli/src/lean/rebuild.ts`: `abortRebuildBound`. (B13)

A bead may rename a surface only by updating its test in the same change; the contract is the behaviour, the name is the handle.

## C7. What the tests do not prove

They are contract tests against shipped code that does not implement the contract, so today every pending test fails for the
reason "surface missing" or "old behaviour", never for a subtle reason; the fixtures in `fixtures/` are the in-memory rule models
that show each rule is self-consistent. Row widths in C2 remain estimates until S2 measures them on a copy; the Studio4 census
counts are measured, the other hosts' are not (Studio1's sessions were measured in round 5; its day and segment counts are the
plan's geometry). No live collector, hosted service or production database was written to; the Studio4 ledger was copied with
`VACUUM INTO` from a read-only connection and the copy was deleted after the census.
