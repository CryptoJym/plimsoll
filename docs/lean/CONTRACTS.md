# Lean Plimsoll contract register (round 8 = B0 round 2, eco-6hoxj.164.4)

**As of:** 2026-09-25 MDT · **Owner:** B0 (contract owner; documents and failing tests only). This file is normative for the
items the round-6 review left to B0 (two blockers, four `b0Carries`; `input/review-r6/VERDICT.json`) and for the repairs the
round-1 review of B0 required (`input/review-r1/VERDICT.json`: C1/C4 judged at judgment time, C2 rule 3 overstating the runway,
the test helper; and its should-fixes). Where it and another document disagree, this file wins and the other carries a "round 7"
or "round 8" note pointing here. Every rule below has (a) a runnable fixture in `fixtures/` (the two round-8 fixtures are red
under `--rule r6` and `--rule r7` and green under `--rule r8`; `checks/fixtures-summary.json`) and (b) a **pending test** in the
repository that owns the rule (§C6), which fails today and names the bead that turns it green. Nothing here is implemented;
nothing here is frozen until the lead signs the freeze list (`FREEZE.md`).

## C1. One actor-ownership predicate, fixed at the first sighting (review-r6 blocker 1, review-r1 blocker 1; B6 cloud, B2a collector)

**The defect (review r6).** Round 6 wrote two validators. A delivered stamp had to satisfy `V exists, V ≤ actorBindingVersionHeard
≤ current`; an undelivered stamp only `V exists, V ≤ current`. For a version the cloud had issued but the collector had not yet
echoed, a row stamped 1 was null on the delivered path and B on the undelivered path.

**The defect (review r1).** Round 7 replaced them with one predicate, valid iff `0 ≤ V ≤ binding_version` **at judgment**. That
still depends on time: a delivered row is judged once at ingest, an undelivered member when its summary arrives, so a stamp for a
version the cloud issues *later* was null on one path and a real actor on the other (stamp 2 delivered at 400: null; the same row
judged in its summary at 1000: C; a dead delivery replayed at 700: C). An honest collector reached such a stamp on **re-join**:
the cloud creates a new `DeviceInstall` per join (`plimsoll-cloud@4954995 src/lib/auth/join-token-store.ts:95-104`), B6 starts it
at `binding_version` 0, and the round-7 B2a kept the highest version ever persisted in the surviving ledger (`plimsoll@03445d3a
packages/collector-cli/src/join.ts:205-232,603-625` keeps the ledger and its binding row across a join) and never lowered it, so
rows captured under the new install's first actor D were null when delivered and F's when exported
(`input/review-r1/checks/review_c1_orderings.log` R1, R2).

**The rule.** A stamp is the **pair** `(install, V)`: the `DeviceInstall` id of the response that supplied `V` and the version. The
collector persists the pair, stamps it on the identity row (`summary_members.actor_binding_version`, `.actor_binding_install`) and
sends it on the wire (`metadata.actorBindingVersion`, `metadata.actorBindingInstall`); the summary item's per-segment parts are
keyed by the pair. `actor_for_stamp(stamp)` is the only ownership function and both paths call it; it judges `V` against the
install the pair names, never against the uploading install:

| Stamp | Delivered raw row (ingest) | Undelivered member (part) |
|---|---|---|
| `(install, V)` **issued at its first sighting**: `V = 0`, or the audit row for `V` existed when the cloud first saw `(install, V)` in any request echo, delivered row or summary member | `actor_id = actor_of(install, V)`; basis `raw_ingest` (identical to `binding_at_capture` by construction) | `actor_id = actor_of(install, V)`, basis `binding_at_capture` |
| `(install, V)` **not issued at its first sighting** (`V > binding_version` then), recorded once as the durable fact `stamp_not_issued(install, V)` in the same transaction; or a pair naming an install the tenant does not have | `actor_id = null`, `metadata.actorStampInvalid = true`, listed in the S4 certify; **stays null after the cloud issues `V`**, at ingest, on replay and in every summary revision | `actor_id = null`, basis `unallocated_stamp_invalid`, candidates = every actor in the install's history plus its current actor |
| `null` (a collector older than B2a, or a B2a collector before the first versioned response of its **current** install, C4) | today's ingest binding (the uploading install's actor at ingest); basis `ingest_current` | `single_binding` (the install has no audit row and a non-null actor) else `unallocated_no_stamp` |

**The guarantee (what `fixtures/b4_offline_rebind.py` and the tests prove).**
1. **Fixed at the first sighting.** For every install and every `V`, the answer for `(install, V)` is decided the first time the
   cloud sees the pair and is never revisited: `actor_for_stamp` reads only the audit table and the `stamp_not_issued` facts, an
   audit row created after the fact does not revive it, and no fact can be created once the audit row exists. Every judgment is a
   sighting, so every judgment of a row stamped `(install, V)` (ingest, a dead delivery's replay, any summary revision, either
   path) returns the same actor or the same null. The fixture probes every sighted pair at every instant from its first sighting
   to the end of the timeline and finds one answer each (round 7 flipped for `(W, 2)` and `(Zf, 2)`).
2. **Honest collectors are exact.** A collector that stamps only versions it persisted from its current install's responses, and
   clears the pair at a join, re-join or workspace transition (C4), never produces a not-issued pair: every stamped row is owned
   by `actor_of(install, V)` on both paths for every ordering of capture, rebind, contact, delivery and re-join, including a row
   still in the outbox at a re-join (it keeps the old install's pair and is judged against the old install, whoever delivers it).
   The echo, `heard_at`, the delivery instant and the summary instant are not inputs.
3. **A not-issued pair fails closed, permanently and visibly.** Rows carrying it are unallocated on both paths (never anyone's),
   and so are rows the same install later stamps legitimately with that version: `(install, V)` is poisoned, listed on the
   certify (`stamp_not_issued` with the first sighting, the binding version then and the affected row count) and on the impact
   report as unallocated with candidates; the repair is an admin rebind, which issues a fresh version. The cloud cannot tell a
   faulty stamp from a stamp issued later except by the order of sightings, which is why the fact is recorded durably at the first one.
4. **The null-stamp row remains the one disclosed exception**, confined to `stamp = null` on a rebound install, whichever collector
   version wrote it (C4).

**Sightings and the durable fact.** `device_install_stamp_not_issued (tenant_id, device_install_id, version, first_seen_at,
binding_version_then, source ∈ {echo, row, member})`, unique per `(device_install_id, version)`, written in the transaction of the
request receipt, the ingest or the summary judgment that first saw the pair above the install's `binding_version`; never deleted
except with the install by a tenant erasure. A refused batch (schema violation, 400) judges nothing and records nothing.

**What the echo is for.** `actorBindingVersionHeard` on every request, **upload deliveries included**, is the collector's
`max(persisted version of the current install, highest stamp carried for the current install)`. It is a diagnostic and a sighting,
never ownership: `stamp_ahead_of_echo` (a request carries a stamp above its own echo), `echo_ahead_of_binding` (an echo above the
install's `binding_version`; it records the not-issued fact), `stamp_sequence_regressed` (a segment's versions are not
non-decreasing in rowid order) and `stamp_from_earlier_install` (a pair naming an install other than the uploader: expected after a
re-join, counted) are recorded on the request receipt and the segment, shown in `/status`, the S4 certify and the impact report,
and change no actor. `heard_at` on the audit row is the instant of the **first request that echoes** the version, a disclosure
fact for the export's `changed_at → heard_at` window; a response that supplies a version sets nothing, and the registration
response is not an echo.

**The corrected timeline** (`fixtures/b4_offline_rebind.py`, round 8; should-fixes 1-2 of review r1). Install X: registration
supplies 0; requests at 50 (heartbeat, echo 0: `heard_at(0) = 50`), 150 (delivery), 300 (heartbeat, echo 0; its response first
supplies 1), 400 (the delivery that carries the row stamped 1: echo 1, `heard_at(1) = 400`, not 650), 650 (heartbeat, echo 1;
response supplies 2), 700 (delivery, echo 2: `heard_at(2) = 700`), 710 (a delivery carrying a never-issued stamp 5: echo 5,
`echo_ahead_of_binding`, `stamp_not_issued(X, 5)`). The fixture's `--rule r6` mode is round 6 **as written** (two validators, first-echo
`heard_at`), so case D splits null/B under it; `--rule r7` is round 7 as written with the round-1 fixture's modelling (deliveries are
non-echoing instants, `heard_at(0)` = the registration), so cases I and J split under it; `--rule r8` is this rule. Cases: A-H
(round 7), I (a stamp from a version issued later, on install W), J0 (a pre-re-join outbox row), J1/J3 (an honest re-join), J1' (a
join whose response carries no version), Jf (a faulty collector carrying a stale version into the new install), K (an A→B→A
reversal), the over-time probe and the timeline facts: 36 checks, r6 12/36, r7 20/36, r8 36/36.

**Judgment state.** Validity is decided at the first sighting of `(install, V)` and recorded; a version that did not exist then is
invalid on both paths for ever. Parts are computed per received revision and frozen at the seal; a raw row's actor is bound once at
ingest; because the fact precedes both, neither can differ from the other.

**Tests.** Cloud `tests/contracts/lean/actor-binding-stamp.contract.test.ts` (B6, 10 cases): the reviewer's issued-but-unheard row,
the honest orderings and the A→B→A reversal, the never-issued stamp, the first-sighting rule with the replay and the repair, the
re-join (honest, pre-re-join outbox row, faulty, null after the join), C4, `firstEchoHeardAt` with deliveries as requests and the
diagnostics, `eventRowsForStorage` binding by the pair (unknown install fails closed), `acknowledgedResponse`, the per-actor parts;
`schema-additions.contract.test.ts` (the `stamp_not_issued` fact). Collector `tests/contracts/lean/actor-stamp.contract.ts` (B2a,
6 cases): null before the first response, the pair on the identity row and never lowered within one install, the echo and the pair
on the wire, both keys allowlisted through the seal, the scope cleared by a re-join and a transition with the new install's 0
accepted, and a pre-re-join row keeping its pair while the batch echoes the current install's version.

## C2. S1b runway: G per host, one G, G owed (review-r6 blocker 2 and should-fix 5; review-r1 blocker 2 and should-fix 5; B1 and B10a collector)

**The defect (review r6).** `G_host = usage_rows × 1,632 B + raw_rows × 309 B` embeds Studio1's session, day and segment density in the
1,632 B. At 333 sessions per 1,000 usage rows the same row widths need 507 MB against a 457 MB gate, and a host exactly at
the 63-day start threshold has 58.06 real days. The multiplier used the unfactored `G_host` while the numerator used `1.25 ×
G_host`, and the continuous runway kept subtracting full G after conversion had already consumed part of it.

**The defect (review r1).** Round 7's rule 3, `G_remaining = max(0, G_gate − new_table_bytes_now)`, subtracted **every** page of the
lean tables. S2 dual-write precedes S3 conversion (MIGRATION.md §2), so the live writer's bytes were counted as conversion progress,
and the one-time census omitted rows admitted after it, which still need converting. Both overstate the runway: on Studio4's census
by 0.39-2.56 days in the reviewer's scenarios, and the rule showed 5.0 d where the truth was 2.45 d and the 2.0 d abort point where
the truth was −0.55 d (`input/review-r1/checks/review_c2_runway.log`). The round-7 fixture could not see it: its "true runway" was
the rule's own `G − C`.

**Evidence that the mix is host-specific.** A read-only census on a `VACUUM INTO` copy of this host's (Studio4's) ledger
(`fixtures/host_cardinality_census.py`, `checks/studio4-cardinality-census.json`, 2026-09-25 14:21 MDT, 2.4 s on the copy): 79,259
usage rows, 352,885 raw rows (3.45 activity rows per usage row; Studio1 has 0.61), 5,141 sessions (64.9 per 1,000 usage rows;
Studio1 47), 5,654 session-day rows, 455 rollup buckets, 34 day targets, raw table 833 MB, ledger 1,504 MB. Round 6 assumed
174,820 usage rows and 281,249 raw rows for Studio4 by scaling Studio1's counts by ledger size: more than double the real
usage count and a fifth of the real activity ratio. Scaling is not measuring. (The census was not re-run in round 8; Studio4's
segment proxy is therefore unmeasured and the fixture falls back to the 1.2 × sessions geometry for it.)

**The rule.**

1. **G from counted cardinalities.** `G_host = usage_rows × (F + T) + sessions × S + session_day_rows × SD + (model_day_rows +
   activity_day_rows) × DR + rollup_buckets × RB + (segments + rollup_buckets + day_targets) × SEG + raw_rows × I`, with the
   round-5 row widths (`F` 1,050, `T` 364, `S` 1,300, `SD` 390, `DR` 390, `RB` 400, `SEG` 830, `I` 309 bytes, estimates from
   column widths) and **every cardinality counted on the host itself**: on a light host by B1's read-only census on a `VACUUM INTO`
   copy of the ledger (the query in `fixtures/host_cardinality_census.py`; 2.4 s on Studio4's copy; never a scan on Studio0), on a
   busy host and on Studio0 by the converted copy's measured peak (unchanged). `segments = max(1.2 × sessions, segment_proxy)`, where
   `segment_proxy` is the census's count of sessions split at 7 days (Σ over sessions of `1 + ⌊span_days / 7⌋`); the terminal-pause
   splits it does not count are declared covered by the 1.25 factor (the whole segment term is 2.3% of G on Studio4; a 2× error
   in it is inside the 25%). A host whose counts were scaled from another host, or not counted, has **no gate value** and cannot
   start a hold. **The census obligation (round 8):** the census is taken **after the catch-up** (S1b (b)), **at most one day before
   the S1b decision**, and **re-taken before S3** (its counts are the conversion's denominator); it gives no gate value otherwise.
   The `VACUUM INTO` copy is made only when free space on the volume is at least the ledger's bytes plus the reserve (the copy is a
   full live-page copy), and it is deleted after the count. The 1.25 factor covers the **row widths** until S2 has measured them on
   a copy; it no longer covers the mix, which is measured.
2. **One G.** `G_gate = 1.25 × G_host` (or the copy's measured peak) is subtracted in the runway numerator **and** sizes the
   multiplier: `g_gate = G_gate / raw_bytes_at_census`, `hold_growth_per_day = p95_7d(gross growth) × (1 + g_gate × raw_share)`.
3. **G owed (round 8).** While conversion runs,
   `G_owed = max(0, G_gate − C_conv) + g_gate × U`,
   where `C_conv` is the bytes the **converter itself has written** for census-era history (its own counter, carried in its
   checkpoint; never a page count of the lean tables, which from S2 on also holds the live writer's rows) and `U` is the bytes of
   the raw rows admitted **after the census** that no lean row covers yet (neither converted nor dual-written; rows admitted after
   S2 are dual-written at admission and are growth, already in `free_disk` and in the multiplier, not conversion work). `G_owed = 0`
   once every census-era and post-census row is folded. `runway_days = (free_disk − reserve − G_owed − rebuild_headroom) /
   hold_growth_per_day`. The abort ladder's rungs read this runway. A `/status` field `runway.g = {gate, owed, converterWritten,
   unfoldedRawBytes, leanTableBytes, basis: census | copy_peak}` discloses the terms; `leanTableBytes` is disclosure only.
   **What this guarantees** (`fixtures/s1b_runway_host_bound.py`, round 8, whose truth is a byte-level simulation of the ledger that
   never calls the rule): with true widths at or below 1.25 × the estimates the published runway never exceeds the true runway in
   any of the reviewer's dual-write and post-census scenarios, no rung fires later than the truth, and the only conservatism is the
   1.25 allowance itself (0.25 × the census-era G until completion plus 0.25 × the post-census owed bytes); with widths above 1.25 ×
   the estimates the rule overstates, which is why the **numbers** stay open until S2 measures the widths (FREEZE.md).
4. **Preflight receipt.** The S1b receipt records the census (counts, the segment proxy, when, on which copy, the free space at the
   copy, that it followed the catch-up), `G_gate`, its basis, the measured conversion size and abort peak where a copy exists, and
   the runway series.

**Tests.** Collector `tests/contracts/lean/runway.contract.ts` (B1, B10a; 5 cases): the reviewer's 333-session counterexample (the
gate's G bounds the host's real need; the segment proxy raises G; an unmeasured host has no gate value), at the gate's 63-day
threshold the host really has ≥ 63 days with the multiplier on `G_gate`, G owed against an independent simulation (dual-write pages
ignored, the two terms, never overstating in the six reviewer scenarios, the 5-day and 2-day rungs never late, 0 at completion),
the round-6 `s1b_runway_geometry.py` assertion, and `censusPreflight` (after the catch-up, at most a day old, room for the copy).
Fixture: `fixtures/s1b_runway_host_bound.py` (19 checks: r6 9/19, r7 9/19, r8 19/19).

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

Retention: **never deleted by age or by budget pressure**: the table and any raw row with an open reject are listed in the
never-delete set of ARCHITECTURE.md §2.3 (round 8); a resolved row keeps its record with `resolved_at_ms`; the table is compacted
only by B14 with the same proof as `capture_root_observations`. The prune and the ladder's release (`releaseUnderLadder`) refuse a raw
row with an open reject; a resolved reject releases its raw row under the ordinary rules. **`raw_rowid` is a live pointer only:** the
raw-delete trigger of ARCHITECTURE.md §2.2 ("Every raw delete, by any path, tombstones") also sets `conversion_rejects.raw_rowid =
null` for `old.id` in the same statement, so a rowid SQLite reuses after the delete can never alias the reject (its identity is
`(event_id, raw_generation)`, as for `target_ref`, B3). `/status` lists `conversionRejects = {open, resolved, byReason}` and the S3
certify (f) lists every open row by id. Tests: collector `tests/contracts/lean/schema.contract.ts` (DDL), `converter.contract.ts`
(a NaN string becomes one counted gap and one reject; nothing vanishes; `b5_non_iso_day_facts.py`) and
`conversion-rejects.contract.ts` (round 8: B2a, the trigger nulls the pointer and a reused rowid does not alias; B10b, an open
reject is never released by the prune or the ladder, the reject row survives both, a resolved reject releases its raw row).

## C4. Null stamps before the first response, and the install scope of the pair (b0Carries 3b; review-r1 blocker 1; B2a collector, B6 cloud)

A B2a collector stamps every raw row with the pair `(install, version)` it has **persisted**: the `DeviceInstall` id and the
`actorBindingVersion` of the latest authenticated response (the registration or enrollment response included, where the route
exists; B6 verifies each route). The pair is **scoped to the install it names**: a response from a different install replaces it
(no cross-install comparison); a lower version from the same install never lowers it; a join, re-join or workspace transition
(`buffer.ts` `useWorkspace` with a new installation epoch, `transitionWorkspace`) clears it to null. Until the first versioned
response of the **current** install the collector writes **null**, exactly like a pre-B2a collector: a fresh install's rows before
its first contact, an upgraded install's rows before its first post-upgrade response, and a re-joined ledger's rows between the
join and the new install's first versioned response carry `actor_binding_version = null`; the join response supplies the new
install's version 0 where the route carries it, so that window is normally empty. Rows already in the outbox at a re-join keep the
pair they were stamped with and are judged against that install (C1). The cloud treats every null stamp by the C1 exception, so
the exception is stated as "stamp = null on a rebound install", not as "older collectors". The collector's `/status` shows
`actorBinding = {install, version, heardAt, stampedRows, nullStampedRows, earlierInstallRows}` so the size of the exception and of
the re-join tail is visible per host, and B2a's acceptance counts the null-stamped rows admitted before the first response on the
Studio5 copy. Test: collector `actor-stamp.contract.ts` (null until the first response; stamped after; cleared by a re-join and a
transition; the pre-re-join row keeps its pair); cloud `actor-binding-stamp.contract.test.ts` (C4 case, the re-join cases).

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
- `src/lib/actor-binding-stamp.ts`: `actorForStamp({ stamp, path, binding })` → `{ actorId, basis, stampInvalid? }` where `binding =
  { installId, currentVersion, currentActorId, actorByVersion, hasAuditRows, notIssued: number[] }` is the state of the install the
  stamp's pair names; `sightStamp(binding, stamp)` → the binding with `notIssued` extended when the stamp is above `currentVersion`
  (pure; persisting the fact is B6's); `firstEchoHeardAt(requests)` over every request kind; `stampDiagnostics({ stamps, echoed,
  currentVersion })` → `{ stampAheadOfEcho, echoAheadOfBinding, ownershipChanged }`. (B6, C1, C4)
- `src/lib/ingest.ts`: `eventRowsForStorage(batch, tenantId, authorizedActorId, { uploadingInstallId, bindings })` binds the pair
  `metadata.actorBindingVersion` + `metadata.actorBindingInstall` through `actorForStamp` against `bindings[actorBindingInstall]`
  (a pair naming an install outside `bindings` fails closed); sets `metadata.actorStampInvalid`. (B6, C1)
- `src/lib/delivery-ack-response.ts`: `acknowledgedResponse` emits `actorBindingVersion` for a registered install
  (`CollectorUploadAuthorization` gains `actorBindingVersion: number`). (B6)
- `src/lib/activity-summary/contract.ts`: `ACTIVITY_SUMMARY_PAYLOAD_KIND`, `isActivitySummaryPayload`, `activitySummaryBatchSchema`,
  `SUMMARY_ITEM_KINDS` (with `day_summary`); `src/lib/activity-summary/judge.ts`: `judgeSummaryItems(items, stored)`;
  `src/lib/activity-summary/actor-parts.ts`: `actorPartsForSegment({ members, install })`. (B6)
- `src/lib/capture-watermark/contract.ts`: `captureCoverageForPeriod` with `until: null` open gaps and `resolvedAt`. (B6, C5)
- `src/lib/economics/token-volume.ts`: `tokenVolumeState`, `normalisedTokens`, `pricingGate`. (B15)
- `prisma/schema.prisma`: `DeviceInstall.bindingVersion`, `DeviceInstall.activityLaneClosedAt`, `DeviceInstallActorBindingAudit.bindingVersion`
  and `.heardAt`, models `AiSummaryActorPart`, `CaptureGap` and `DeviceInstallStampNotIssued` (`deviceInstallId`, `version`,
  `firstSeenAt`, `bindingVersionThen`, `source`, unique per install and version). (B6, C1)

**Surfaces the collector tests bind (plimsoll, `tests/contracts/lean/`).**
- `packages/collector-cli/src/lean/schema.ts`: `ensureLeanSchema(db)` (ARCHITECTURE.md §3 DDL v3 plus C3) on the ledger connection
  (or one exposed as `buffer.leanDatabase`), which must keep `foreign_keys = ON`: **finding (B0):** better-sqlite3 opens every connection
  with foreign keys enforced, so at `03445d3a` the ledger connection already has `PRAGMA foreign_keys = 1` (the guard test
  `schema.contract.ts` last case is green today); the round-6 note that B2a must "set" it becomes "must not turn it off";
  `raw_retention_control.hold_reason`; `collector_workspace_binding.actor_binding_version`; `raw_retention_receipts` accepting the four
  reasons. (B2a, B10a, B10b)
- `packages/collector-cli/src/buffer.ts`: `prune` deletes nothing while `hold_reason` is set; `retentionProgressStatus().hold`;
  `recordActorBindingVersion(version, installId)` (a different install replaces the pair; a lower version of the same install is
  ignored); `workspaceBinding().actorBindingVersion` and `.actorBindingInstall`, both cleared by `useWorkspace` with a new
  installation epoch and by `transitionWorkspace`; `summary_members` written at `append` with the pair
  (`actor_binding_version`, `actor_binding_install`) and two edges when `lean.write` is on (`new LocalEventBuffer(path, { lean:
  { write: true } })`). (B10a, B2a; C1, C4)
- `packages/collector-cli/src/upload.ts`: `buildIngestBatch(...).batch.actorBindingVersionHeard` (the current install's version;
  earlier installs' stamps do not raise it) and the pair on every event's metadata; `packages/shared/src/schemas.ts`:
  `aiWorkIngestBatchSchema` accepts it; `analytical-metadata.ts`: `metadataKeyDisposition("actorBindingVersion")` and
  `("actorBindingInstall")` are identifiers; `outbound-envelope.ts`: `sealOutboundEnvelope` keeps both. (B2a)
- `packages/collector-cli/src/lean/runway.ts`: `LEAN_ROW_WIDTHS`; `estimateHostG(census)` (with the optional `segmentProxy`;
  `null` for an unmeasured census); `holdRunway({ freeBytes, reserveBytes, rebuildHeadroomBytes, gGateBytes, rawBytesAtCensus,
  converterWrittenBytes, rawUnfoldedBytesSinceCensus, conversionComplete, leanTableBytesNow?, grossGrowthP95PerDay, rawBytes,
  ledgerBytes })` → `{ runwayDays, gOwedBytes, holdGrowthPerDay, rung ∈ none | converter_paused | release_acked_only | abort }`;
  `censusPreflight({ censusAtMs, catchUpCompleteAtMs, decisionAtMs, freeBytes, ledgerBytes, reserveBytes })` → `{ ok, reasons ⊆
  {census_before_catchup, census_stale, no_space_for_vacuum_into} }`. (B1, B10a; C2)
- `packages/collector-cli/src/lean/day-key.ts`: `utcDayOf`, `censusClass`, `dashboardWindowSince`; `DASHBOARD_SCHEMA_VERSION = 3`. (B5)
- `packages/collector-cli/src/lean/converter.ts`: `convertLedgerHistory(buffer, options)`. (B2a; C3)
- `packages/collector-cli/src/lean/capture-gaps.ts`: `declareUnresolvedFileGap`, `resolveCaptureGap`, `coverageCompleteForPeriod`. (B22; C5)
- `packages/collector-cli/src/lean/retention.ts`: `applyUploadReceipts`, `releaseUnderLadder` (refuses a raw row with an open
  `conversion_rejects` row), `retireSegment`; the raw-delete trigger on `buffered_events` nulls `conversion_rejects.raw_rowid`. (B2a, B10b; C3)
- `packages/collector-cli/src/lean/rebuild.ts`: `abortRebuildBound`. (B13)

A bead may rename a surface only by updating its test in the same change; the contract is the behaviour, the name is the handle.

## C7. What the tests do not prove

They are contract tests against shipped code that does not implement the contract, so today every pending test fails for the
reason "surface missing" or "old behaviour", never for a subtle reason; the fixtures in `fixtures/` are the in-memory rule models
that show each rule is self-consistent. Row widths in C2 remain estimates until S2 measures them on a copy; the Studio4 census
counts are measured, the other hosts' are not (Studio1's sessions were measured in round 5; its day and segment counts are the
plan's geometry). No live collector, hosted service or production database was written to; the Studio4 ledger was copied with
`VACUUM INTO` from a read-only connection and the copy was deleted after the census.

## C8. The `<UR>` usage-record predicate pin (BEADS.md B0, MIGRATION.md S1; review-r1 should-fix 4)

The predicate that decides which rows are usage records is the inline text of `plimsoll-cloud src/lib/economics/loader.ts`
(`usageRecordPredicate`, unchanged since `067a8a4`, re-read at `4954995`):

```sql
( event_type IN ('usage_rollout','usage_transcript','usage_live')
  OR input_tokens IS NOT NULL OR output_tokens IS NOT NULL
  OR cache_read_tokens IS NOT NULL OR cache_creation_tokens IS NOT NULL
  OR cost_usd IS NOT NULL )
```

It is pinned in **both** repositories as `tests/contracts/lean/fixtures/usage_record_predicate.sql` with the pin
`USAGE_RECORD_PREDICATE_PIN` = sha256 of the text after comment lines are dropped, whitespace is collapsed (none inside the outer
parentheses) and case is folded. Guards (green today): the fixture text hashes to the pin in both repositories; the cloud's inline
loader text hashes to it; the collector's cardinality census (`fixtures/host_cardinality_census.py`) counts usage rows with it.
Pending: cloud `usage-record-pin.contract.test.ts` (lane 2: `src/lib/economics/usage-record-predicate.ts` exports
`USAGE_RECORD_PREDICATE_SQL` equal to the pin and `loader.ts` uses it; B0's successor re-pins on merge); collector
`usage-record-pin.contract.ts` (B2a: `lean/usage-record.ts` exports `USAGE_RECORD_PREDICATE_SQL` equal to the pin and
`isUsageRecord(row)`, the converter's rule, so the S3 fold and the cloud agree row for row).
