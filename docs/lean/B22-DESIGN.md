# B22 implementation design: durable capture unknownness

Base: collector `375f277b` (0.7.44 with the stop-window release fix). All
`packages/collector-cli/src/...:line` references below are at that base, before
the separate rollout spike commit. The fixed rules are `CONTRACTS.md` C5,
`ARCHITECTURE.md` §3.6 and §6.1–6.4, and `PROOF.md` §8. This document is an
implementation map, not a new upload or cloud contract.

## 1. Invariant and boundary

One source unit has only two commit outcomes: **admitted** (including a proven
deduplicate) or **declared unknown/lost** in `capture_gaps`. Its durable capture
cursor may advance only in the SQLite transaction that commits that outcome.
Preparation (file read, generation check, JSON parse, record classification,
normalization and deterministic gap ID) stays outside the write transaction;
there is no filesystem or network I/O while holding the SQLite writer lock.
`BEGIN IMMEDIATE` is the existing transaction mode for event admission
(`buffer.ts:1327-1345,2611-2635`). On failure, the whole unit rolls back, its
cursor stays at its former value, its unread bytes remain visible to the next
walk, and a fault is raised **before** another claim is built. Do not swallow a
gap-write error as a parse error or call `recordCaptureRecordLoss` alone a gap.

The source unit is a JSONL slice for Codex/Claude, a bounded Grok document turn
slice, one OTLP replay chunk (16 entries/samples), or one hook spool file. A
refusal within a partly admitted unit must either write its counted gap in that
unit's transaction or leave the unit uncommitted. A rejected/deduplicated raw
event is not proof of admission when the reason means capture was discarded.

## 2. Complete writer census and exact target transactions

| Path at `375f277b` | Current commit/advance | Required boundary |
| --- | --- | --- |
| Codex rollout JSONL, `rollout-tailer.ts:1107-1133,1173-1235`; cursor SQL `jsonl-byte-tailer.ts:360-424` | `transactionWithRepoContextHandoffs` already wraps continuation removal, `capture_record_losses`, `ingestLines`, event/outbox writes and `rememberJsonlScanCursor`; the `checkpoint` and `unresolvedRecord` early returns are inside it. | In that **same** immediate transaction, insert a counted gap before a skipped record's offset advances, or upsert an `epoch_open` unknown gap before an unresolved checkpoint is saved; resolve an open gap only in the final EOF transaction after all lines are admitted and counted refusals are written. A continuation-only checkpoint writes no capture cursor and needs no new gap. File snapshot validation stays before `BEGIN`. |
| Claude transcript JSONL, `transcript-tailer.ts:1068,1128-1182`; shared cursor SQL `jsonl-byte-tailer.ts:360-424` | Same transaction shape as Codex; transcript revision state/event/outbox writes are inside `ingestLines`. | Same as Codex, using `source='claude_code'`, including the unresolved early return, oversized skip, revision refusal and EOF resolution. |
| Grok usage file, `grok-usage-tailer.ts:1467-1477,1512-1535,1600-1723,1727-1745` | `applyDocument` wraps emitted events, `grok_usage_turn_state` and `grok_usage_file_state` in one immediate transaction. Oversized and parse-error file-state writes at 1476/1534, and same-digest state at 1514, are separate autocommit writes. | Move **every** file-state transition that makes the generation read or refused into a short immediate transaction containing the matching open/counted gap or admitted turns. The existing `applyDocument` transaction gains gaps for invalid turns and refused rewrites before advancing `turn_state`, and resolves an open file gap only at complete committed EOF. `partial` retains the open gap. Oversized/parse-error states upsert `epoch_open` before the state row; identical-digest state may resolve only if the prior generation was fully committed. |
| Grok discovery cursor, `grok-usage-tailer.ts:1122-1140,1762-1784` | `writeState` saves the name-hash cursor in `maintenance_state` after processing; `finishSweep` saves marker and reset cursor together. | A discovery cursor is **not** a proof that file bytes were captured. Before moving it past a candidate, either its file state and gap/admission commit has succeeded or the candidate remains pending and the sweep is marked unclean. Persist `unclean` with the cursor when a candidate could not be recorded. Never set a clean sweep/coverage receipt from the reset transaction while any candidate lacks that proof. The 0.7.44 separate walk-state write is acceptable only under this ordering; no large directory walk enters a writer transaction. |
| Codex/Claude automatic discovery and baseline resume, `capture-baseline.ts:133-157,1200-1258,1265-1358,1361-1415,1539-1615`; callers `rollout-tailer.ts:696,765,821-901,1394`, `transcript-tailer.ts:646,711,767-847,1309` | `maintenance_state` holds the bounded sweep resume, while `automatic_capture_baseline_pending_generations` and `automatic_capture_baseline_state` hold staged metadata and progress; today these are separate small commits. They are discovery progress, not a byte offset. | For each bounded group that moves its resume position past a candidate, commit the corresponding admission or `epoch_open` gap **with that resume write** in one immediate transaction; refactor the existing helpers to execute inside the caller's transaction. Staged stat-only metadata is not byte admission: keep its file gap open and the baseline incomplete until the exact generation is parsed to EOF. The completion-arm transaction checks that no candidate in the run remains pending or lacks gap/admission proof. Never use a completed metadata baseline by itself to close a capture gap. |
| OTLP live POST, `server.ts:1889-1938`; shared append transaction `buffer.ts:2648-2687` | Each 16-entry chunk commits admission/drop counters independently; failed remainder may be spooled. | In each chunk's existing immediate transaction write counted gaps for actual refused records (where the contract calls them loss) with admission/drop counters, before its successful 202 acknowledgement. A failed gap insert rolls back that chunk. For a gap-write failure return 503, even if an intake spool is writable; ordinary busy/deadline handling retains its existing spool path. |
| OTLP spool replay, `otlp-spool.ts:964-966,1008-1053`, cursor SQL at `:389-407` | `transactionWithRepoContextHandoffs` already wraps `appendMany` and the `maintenance_state` `nextChunk` write; the file is unlinked after ledger flush (`:1208-1235`). | Add gap inserts to that same transaction **before** `writeCursor`. On any failure, `nextChunk` and the file stay; replay retries. Existing ledger flush precedes unlink, and removal of a stale cursor after unlink remains housekeeping, not capture advancement. Expiry/quarantine of a still-unread file is a separate loss transaction: persist its gap before unlink/quarantine. |
| Hook live POST and spool replay, `server.ts:197-224,400-523,1741-1803`; `buffer.ts:2611-2635` | Live admission uses `append`'s immediate transaction. Drain replays via the same callable, then unlinks the file; 503 leaves it in place. | A hook has no SQLite byte cursor. The durable spool file is its pending cursor. A refused/lost normalized hook writes its counted/unknown gap in `append`'s transaction or remains pending. Drain unlinks only after the admission/gap commit and, for durability parity with OTLP, a ledger flush; 503 leaves it in place. The live route acknowledges 202 only after ledger admission or an ordinary busy-case durable intake-spool write; a gap-write failure is always 503. Expiry/quarantine writes a gap before removing the last replayable file. |
| Spool loss on expiry/quarantine, `otlp-spool.ts:942-959,1137-1152`; `hook-spool.ts:914-934,937-974`; v1 diagnostic `spool-losses.ts:78-111` | OTLP currently unlinks an expired file before `recordSpoolLoss`; hook reject records a best-effort v1 loss log before moving or deleting the file. Neither v1 log is a `capture_gaps` row. | First commit the counted or unknown `capture_gaps` row and any SQLite replay cursor/state change in one immediate transaction, then flush the ledger, then remove/move the last replayable file. If SQL commit or flush fails, retain the file and cursor. Call `recordSpoolLoss` outside the SQL transaction for existing v1 diagnostics, but its best-effort result is not the durability proof. Filesystem removal cannot be inside SQLite, so ordering, idempotent gap IDs and keeping the file on failure make the irreversible step safe. |
| Capture coverage frontier, `capture-frontier.ts:809-875,885-915,967-1008` | `applyCaptureCoverage` upserts/deletes `capture_uncovered_files`; `finishCaptureCoverage` advances `capture_coverage_state.complete_through` in a second transaction. Today `captureFrontier` converts old uncovered rows into **closed**, capped v1 claim gaps. | In each 256-file `applyCaptureCoverage` transaction, upsert a durable `epoch_open` gap for `no_tailer_row` or `unresolved:*` **before** the uncovered-file state can let a frontier pass it. Resolve it only when the tailer has parsed that exact generation to EOF. `finishCaptureCoverage` checks that all new/open gaps for the epoch exist before moving `complete_through`; if any insert fails it does not advance. A file once proved covered may use the narrower `file_write_interval`. Coverage-walk cursor/receipt can be saved only after all batches commit. |
| JSONL continuation state, `jsonl-continuation.ts:34-40,418-423`; rollout/transcript `:1186-1204` / `:1140-1158` | `applyCheckpoint/remove` already execute inside the tailer's transaction. | A continuation checkpoint advances **scan work**, not `rollout_scan_state.committed_offset`; keep it inside the existing transaction. If an unresolved checkpoint is also remembered in the scan cursor, its open gap is written first. Do not treat a parked continuation as read coverage. |

Other buffered writers (`buffer.append` and `appendMany`) have no file cursor;
they use the same admission/gap rule when they refuse a pushed capture. The
v1 upload `capture_claim_sequence` in `outbox.ts:614-633` is an upload ordering
cursor, not a capture cursor, and must keep incrementing once per claim. This
census includes the spool filesystem removal boundaries because deleting the
last replayable file is consumption even without a SQLite offset.
Declaring a gap does not lift the existing frontier hold at the oldest unread
byte of a parseable file. `complete_through` remains bounded by that hold;
`declaredGaps` independently makes even a later claim incomplete over an
unresolved or never-read generation.
The two current OTLP `admissionDrops` reasons,
`generic_zero_value_span` and `app_server_internal_span`
(`otlp-admission.ts:3-14,97-125`), are proved zero-retained-dimension policy
exclusions, not lost capture; keep their counters in the chunk transaction
without inventing a gap. A future budget or backpressure refusal of otherwise
admissible data uses `footprint_cap` or `backpressure` respectively. Spool
expiry/cap uses `footprint_cap`; a quarantined unparseable body uses
`contract_violation`. For a known batch use counted row totals; for an opaque
or corrupt body leave both counts null with `count_basis='unknown'`.

## 3. Local schema and row operations

Create these tables during ledger open, before a tailer can advance. Keep
`capture_gaps` as the shared table: `capture-gaps.contract.ts:24`,
`conversion-rejects.contract.ts:17,27,47` and `converter.contract.ts:35`
query/insert it. The planned DDL (`ARCHITECTURE.md:454-477`) has no
`workspace_id` or `installation_epoch_id`, while the pending converter test
inserts both; include them now. Defaults on `machine_hash`, `epoch_key` and
`upload_state` let that fixed test's deliberately minimal insert run. Production
writers supply the bound values, never the defaults. No raw path or payload is
stored. The table is append/resolve only; no age-based prune.

```sql
create table if not exists capture_gaps (
  gap_id text primary key,
  workspace_id text not null default '',
  installation_epoch_id text not null default '',
  source text not null,
  session_id text,
  machine_hash text not null default '',
  epoch_key text not null default '',
  started_at_ms integer not null,
  ended_at_ms integer,
  interval_basis text not null check (interval_basis in
    ('counted_interval','file_write_interval','epoch_open','fault_interval')),
  resolved_at_ms integer,
  dropped_rows integer,
  dropped_usage_rows integer,
  count_basis text not null check (count_basis in ('counted','unknown')),
  reason text not null check (reason in
    ('footprint_cap','backpressure','unknown_type','contract_violation',
     'gap_record_unavailable','tailer_unread','record_exceeds_byte_budget',
     'generation_rewrite_ambiguous','coverage_walk_incomplete','restart_unverified')),
  file_key_digest text,
  unread_bytes integer,
  upload_state text not null default 'pending'
    check (upload_state in ('pending','in_flight','acked')),
  revision integer not null default 1 check (revision > 0),
  check (ended_at_ms is null or ended_at_ms >= started_at_ms),
  check (resolved_at_ms is null or resolved_at_ms >= started_at_ms),
  check ((count_basis = 'unknown' and dropped_rows is null and dropped_usage_rows is null)
      or (count_basis = 'counted' and dropped_rows is not null and dropped_rows >= 0))
);
create index if not exists capture_gaps_open_epoch
  on capture_gaps(workspace_id, installation_epoch_id, source, file_key_digest)
  where resolved_at_ms is null;
create index if not exists capture_gaps_upload
  on capture_gaps(upload_state, gap_id, revision);
create table if not exists capture_faults (
  fault_id text primary key,
  kind text not null check (kind in
    ('gap_write_failed','cursor_advance_failed','storage_full')),
  source text,
  file_key_digest text,
  at_ms integer not null,
  resolved_at_ms integer,
  detail text,
  check (resolved_at_ms is null or resolved_at_ms >= at_ms)
);
```

`gap_id` is SHA-256 of a versioned, length-delimited tuple. For a file gap the
tuple is `(installation_epoch_id, source, digest(cursor key), generation
identity, 'unread')`; for a skipped record it includes its byte offset and
fingerprint (`capture-record-loss.ts:203-212`); for a fault it includes the
fault ID. `file_key_digest` is SHA-256 of the tailer cursor key, never the key
itself (JSONL keys can contain a path). Production `machine_hash` is the
canonical `sha256:<hex>` form of the collector's machine identity
(`dashboard-projection.ts:474-482,592`), never a hostname.
`capture_faults.detail` is a bounded error code only, with no exception text,
path or source bytes. A binding change while a local fault is unresolved is
held until its gap is durably scoped to the old installation epoch; do not
relabel that fault under a later epoch. For an oversized JSONL skip,
`reason='record_exceeds_byte_budget'`, `dropped_rows=1`, and
`dropped_usage_rows=1` only for a proved usage kind (`codex_token_count` or
`claude_assistant`). An `unknown` prefix is usage-possible but its usage count
is unknown, so store `dropped_usage_rows=NULL`; a proved non-usage skip does
not create a usage gap. The existing `capture_record_losses` receipt is
written in the same transaction. Upsert the same gap ID idempotently;
change any cloud-visible content by `revision = revision + 1` and reset
`upload_state='pending'`. Never lower a revision or erase an acknowledged
gap. A resolve sets `resolved_at_ms` after the full file is parsed and all
counted refusals from it are written in that same transaction. Coverage
overlap is `resolved_at_ms is null AND started_at_ms < period.endMs AND
(ended_at_ms is null OR ended_at_ms > period.startMs)` for half-open periods;
a claim additionally filters `workspace_id` and `installation_epoch_id` to
its bound scope, so another epoch's gap cannot bleed into it. A resolved gap
overlaps nothing. The C5 fixture's first eleven hours therefore
stay incomplete even when `through` reaches 11:00.

The frontier must carry the **same generation identity** as the JSONL tailer
in `CaptureCoverageFile`: `dev:ino:birthtimeNs` from a precise bigint stat
(`jsonl-byte-tailer.ts:839-865`), captured before its short SQL transaction.
The frontier's existing `birthtimeMs` alone is too coarse to make the same
gap ID. A stat failure uses the explicit absent-identity sentinel; it cannot
later be resolved by parsing a different generation. A rewrite that destroys
an unread generation leaves that generation's open gap durable; parsing the
replacement resolves only the replacement's ID. Grok uses the exact
`(device,inode,size,mtimeNs,ctimeNs)` tuple from `identityOf`
(`grok-usage-tailer.ts:652-666`) for the same purpose. These identities are
inputs to the ID hash, not stored paths or new ledger columns.

Capture before workspace enrollment stays in the local `unbound` scope. A
first binding sets `through=null`, `unattested='coverage_walk_incomplete'`
until a bounded whole-root walk has materialized epoch-scoped gaps for every
still-unread generation in its existing per-batch transactions. Those rows
start at the new epoch's start. An unbound receipt is never uploaded as if it
belonged to the new epoch, and no non-null claim is sent before the new walk
commits its last batch.

For `unresolved:*` and `no_tailer_row`, write `count_basis='unknown'`, both
counts null, `interval_basis='epoch_open'`, `started_at_ms` equal to the
installation epoch start, and `ended_at_ms=NULL`. `last_write_at` is only an
observation of filesystem mtime, not an upper bound on event time; a 10:05
event in a file last written at 10:00 and parsed at 11:00 survives the intake
clamp (`normalizer.ts:203-214`). Keep the gap open across a later claim, file
rewrite, process restart and source-mtime change. For a once-covered file with
a proved cursor bound use `file_write_interval`; for a parsed refusal use
`counted_interval`. A file's `unread_bytes` is its observed extent minus the
durable committed offset; update it with the same gap ID and higher revision
on later walks. The `no_row_pre_epoch` class cannot be silently discarded just
because mtime precedes enrollment (`capture-frontier.ts:850`): if the source
can still contain an in-epoch stamp, declare the same open gap until parse.
Map `no_tailer_row`, `behind`, `work_remaining` and `no_row_pre_epoch` to
`reason='tailer_unread'`; map the JSONL unresolved kinds to their identically
named allowed reasons `record_exceeds_byte_budget` and
`generation_rewrite_ambiguous`. Grok oversized uses
`record_exceeds_byte_budget`; a Grok parse error uses `contract_violation`
with unknown counts until a later parse proves the exact losses.

## 4. Fault and restart state machine

The marker paths are beside the actual ledger path, never in a provider home:
`capture-fault.json` and `capture-clean-shutdown`. The fault marker is a small
versioned JSON object `{v:1,faultId,kind,atMs,source,fileKeyDigest}` with no
path, payload or error text. Write a same-directory temporary file with 0600,
fsync it, rename, then fsync the directory. Catch marker-write failure and
keep the in-memory fault. The database row mirrors it as soon as SQLite is
writable. A retry cannot clear the in-memory bit until the gap row and fault
row commit and the gap item is acknowledged. Remove `capture-fault.json` and
fsync its directory only after every fault row is resolved, every fault gap
is acknowledged, and the post-fault whole-root walk completes; a failed
unlink keeps the process unverified. If the marker could not be written, the
missing clean marker still forces restart verification.

| State | Entry / exit | `/status` and claim |
| --- | --- | --- |
| `startup_checking` | A prior clean marker was present and unlinked, with no fault marker/row, but this process has not completed its first whole-root walk. | `/status`: `restartUnverified=false`, current walk incomplete; claim `through=null`, `unattested='coverage_walk_incomplete'` and any persisted `declaredGaps`. A clean prior exit alone does not certify a later file generation. |
| `verified` | Clean marker was present at startup, was unlinked, no fault marker/row; a completed, fresh whole-root walk covers every configured root. | `/status`: `captureDurability.faults=[]`, `restartUnverified=false`, walk and per-source/root deferred/unread fields. Claim v2 may have non-null `through` only when its other existing bounds allow it; it carries `declaredGaps` for persisted gaps. An open gap still makes affected periods incomplete. |
| `fault_live` | A gap/cursor transaction fails; immediately set process fault, attempt marker, then row when possible. Stay until the retry commits the gap and its upload is acknowledged. | `/status`: open fault, marker persistence result, `storage_full` when applicable, unread bytes and deferred counts. Every new claim has `through=null`, `unattested='gap_record_unavailable'`, and `faults` contains `{faultId,kind,atMs}`. |
| `restart_unverified` | On startup, read whether clean marker existed **before** unlink. If absent, fault marker exists, any unresolved fault row exists, or unlink failed, enter this state before network work. A clean exit with an unresolved fault deliberately leaves the clean marker absent. | First claim and every later claim while this state holds: `through=null`, `unattested='restart_unverified'` (higher priority than the live fault reason), `faults` and `declaredGaps` as available. `/status` says `restartUnverified=true` and names incomplete/stale walk. Publish the first withdrawal in a zero-item summary batch even with no new raw events. |
| `repairing` | Retry the failed source unit. Persist one `capture_faults` row and one `capture_gap` item with `reason='gap_record_unavailable'`, `count_basis='unknown'`, `interval_basis='fault_interval'`, from `max(epochStart, fault.atMs - CAPTURE_WRITE_LAG_MS)` to the repair time. Set the gap's `ended_at_ms` to repair time but leave its `resolved_at_ms=NULL`: the historical unknown interval remains a declared loss. Upload and acknowledge that gap; only then set `capture_faults.resolved_at_ms`. Re-run a whole-root coverage walk after startup. | `through` stays null with the state's reason until **both** fault resolution/ack and the new complete walk succeed. `/status` exposes the pending fault/gap and per-root progress. |
| `verified_with_gap` | All faults are resolved and acknowledged, and the post-start whole-root walk completed with `rootsCovered=rootsConfigured`, no deferred bytes/generations, less than 24 h old. | `restartUnverified=false`; claim may resume a bounded `through`, but the persisted fault/open gaps stay in `declaredGaps` and make overlapping coverage incomplete. |

At startup unlink `capture-clean-shutdown` before opening intake and sync its
directory; only a fully clean exit with no unresolved fault recreates and
syncs it. On any failed unlink or marker read, stay `restart_unverified`. A
`SIGKILL` or crash cannot run the exit handler. When a crash left neither a
fault marker nor a row, persist a conservative `restart_unverified` gap for the
unknown interval from the epoch start through post-start verification; this
uses the already allowed reason and prevents a later complete claim from
erasing the unobserved crash window. A restart state is never cleared by a
timer or a successful single tailer pass. A walk older than 24 h or incomplete
sets `through=null`, `unattested='coverage_walk_incomplete'` even without a
fault (`ARCHITECTURE.md:580-588`). Reason priority is
`restart_unverified` > `gap_record_unavailable` >
`coverage_walk_incomplete` > the existing v1 reasons in `outbox.ts:590-595`.

Expose the local state as `/status.captureDurability = {state,
restartUnverified, cleanMarkerObservedAtStart, faultMarkerPersisted,
storageFull, faults}`. `faults` is an array of
`{faultId,kind,atMs,source,fileKeyDigest}` with optional source/digest and no
private detail; `faultMarkerPersisted` is `null` with no fault, `true` after a
successful fsynced marker write, and `false` if it failed. The claim's fixed
v2 `faults` array has only `{faultId,kind,atMs}`. The state object and the
per-source/root completeness fields are read from the same cached coherent
status generation, so a stale status cache is marked stale by the existing
status path rather than falsely reporting a fresh clean walk.

`/status` and claim v2 carry the same per-source/per-root
`bytesDeferred`, `deferredGenerations`, `coverageWalk` and `unreadFiles`
objects specified in `ARCHITECTURE.md:582-588`; the status route reads its
existing cached snapshot, not a filesystem walk on the request path
(`server.ts:1099-1186`). Build the claim from an atomic local snapshot of
the fault rows, marker-derived memory state, gaps and frontier. Store gap
items ahead of a claim that references them. A cloud summary batch with
`items: []` is permitted and carries only the withdrawing claim; if a batch
has items, `conflict`/`stale` receipts prevent claim advancement while
`held`/`duplicate` allow it (`ARCHITECTURE.md:562`). A withdrawal must not
wait for an event outbox lease. This is a collector scheduling change, not an
upload contract change.

## 5. Pushed requests and spool ordering

After authentication and validation, any `POST /hooks/codex` (including the
live-usage producer-header branch), `POST /hooks/grok`, or OTLP POST accepted
by `isOtlpPath` whose admission/gap transaction fails **because its required
gap cannot be recorded** answers exactly HTTP 503,
JSON `{"error":"collector_request_rejected","reason":"gap_record_unavailable"}`,
`Retry-After: 1`, `Content-Type: application/json`, `Connection: close`.
Add `gap_record_unavailable` to `HttpBoundaryReason` (`http-boundary.ts:75-126`)
and use the existing catch/response writer (`server.ts:2025-2028,2095-2108`),
adding this reason to its `Retry-After` predicate. Wrap the failed gap DML in
a typed error **before** `asHttpBoundaryRejection` can mistake a SQLite `BUSY`
gap failure for ordinary `storage_busy_retry`. Do not turn authentication,
schema, privacy or source refusals into 503s.

For this **specific** gap failure, bypass the collector's intake-spool-to-202
branches (`server.ts:1761-1803,1930-1936`): the request must return 503 even
if that spool is writable. The existing local hook client treats 503 as a
known non-admission and durably spools the body (`local-hook-client.ts:83-112,
139-149`); an OTLP exporter receives `Retry-After` and retries. The existing
collector intake spools still handle their ordinary busy/deadline cases and
may answer 202 only when their own durable write succeeds. If an OTLP request
had already committed earlier chunks, the 503 makes the client retry the
whole request; deterministic event IDs deduplicate those chunks, and only the
failed chunk remains uncommitted. During replay, `gap_record_unavailable` is
a deferred 503: keep the hook file (`server.ts:510-517`) and keep the OTLP
file and `nextChunk` (`otlp-spool.ts:1033-1048`). No spool deletion or expiry
may precede the durable gap/admission proof. A client that receives 503 owns
a retry; no `accepted` count, outbox ack or capture cursor may move for its
uncommitted unit.

## 6. Verification and cost

| Rule | Gate to turn green in implementation bead |
| --- | --- |
| `epoch_open`, null end, overlap after mtime, resolve at EOF | Remove `pending("B22")` from `tests/contracts/lean/capture-gaps.contract.ts:15`; keep `b22-documents.contract.ts` green; run `fixtures/b22_false_complete.py` and `sf5_unresolved_file_open_gap.py`. The rollout spike demonstrates this one surface only. |
| DDL and compatibility with conversion | Remove B22 pending marker from `schema.contract.ts:41`; B2a later un-pends `conversion-rejects.contract.ts:20` and `converter.contract.ts:18` without a second gap table. |
| Forced gap-insert failure, no cursor move/unread-byte change | `PROOF.md:133` §8 item 6: inject `RAISE(ABORT)` on `capture_gaps`, compare `rollout_scan_state.committed_offset`, `capture_uncovered_files`/walk unread bytes and source file before/after. Repeat for transcript, Grok state, OTLP chunk and hook spool. |
| Fault/marker/crash/claim withdrawal | `PROOF.md:83` watermark-v2 row: failed marker write, kill -9 before next claim, first zero-item withdrawal, clean exit with unresolved fault, post-start walk and fault-gap ack. Run the existing capture-claim proofs as regression gates. |
| Full volume and 503 | `PROOF.md:120` §7 item 4: full-volume failure, marker failure, free only permitted `error_samples` and old `metric_samples`, retry gap, then recovery; HTTP tests assert exact 503 body and `Retry-After` on every pushed route, hook-client 503 spool, ordinary busy/deadline intake-spool 202, failed replay retaining its file, no ack. |
| Tailer completeness | `PROOF.md:128-133` §8 items 1–5: per-source/root deferred and walk fields, old-day never-read file, no `complete` while bytes deferred/unresolved/faulted. |

Normal fully admitted JSONL slice: **zero** new gap writes, one indexed open-gap
lookup at EOF; counted skip: one `capture_gaps` insert beside the existing
`capture_record_losses` insert; unresolved transition: one gap upsert beside
the cursor; resolution: one gap update only at EOF. Grok adds at most one gap
upsert per refused turn and one per unresolved file transition; OTLP adds one
per actual refusal, never one per admitted event; hook adds one on actual
refusal/loss. The coverage walk adds one indexed gap upsert per uncovered file
inside its existing 256-file short transaction. Indexes add the corresponding
SQLite B-tree writes. Fault repair adds one fault row and one gap row per
episode. No per-record filesystem fsync is added to healthy tailer batches.

The 0.7.44 rollout watch's contention proof requires each bounded writer
slice to fit below **750 ms** (`studio0-writer-contention-proof.ts:104-116`).
The new DML is O(log gap rows), prepared once per slice; no parse, directory
walk, marker fsync or upload is added under that lock. The wall-clock worst
case cannot be stated as a deterministic number for SQLite/WAL on an arbitrary
full disk; the operational bound is the existing ≤128 KiB/64-record JSONL
slice (`rollout-tailer.ts:1103-1105`) plus one gap insert/update, measured by
the unchanged 750 ms proof. If it exceeds 750 ms on the accepted copy, reduce
the slice limit (not the assertion) before shipping. Never put a whole-root
walk or a spool file's entire replay into one transaction.

## 7. Scope fence

The implementation must not change the upload contract: it keeps the v1 event
batch semantics, delivery IDs and receipts, and uses the already fixed
`activity_summary_v2` item/claim schema for the claim-only batch. It does not
rewrite other ledger tables or indexes
outside the `capture_gaps` and `capture_faults` DDL above and the necessary
local transaction wiring. It does not edit cloud code or cloud schema (B6
owns that side), retention/ledger migration policy, or the planned meaning
of the open gap. Any evidence outside this scope belongs in the implementation
bead's “Also found” section.
