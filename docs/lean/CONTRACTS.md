# Lean Plimsoll contract register (round 11 = B0 round 5, eco-6hoxj.164.4)

**As of:** 2026-09-25 MDT · **Owner:** B0 (contract owner; documents and failing tests only). This file is normative for the
items the round-6 review left to B0 (two blockers, four `b0Carries`; `input/review-r6/VERDICT.json`) and for the repairs the
round-1 review of B0 required (`input/review-r1/VERDICT.json`: C1/C4 judged at judgment time, C2 rule 3 overstating the runway,
the test helper; and its should-fixes) and the round-2 review required (`input/review-r2/VERDICT.json`: the old install's late
answer replacing the stamp, the first sighting racing the rebind, two pending tests that could not pass; and its nine should-fixes)
and the independent read of C1/C4 after round 9 required (`input/read-c1c4/VERDICT.json`: a sighting must take the lock before it
reads the facts, and one ledger uploaded by two installs must read one fact; and its should-fixes) and the independent read of C1
after round 10 required (`input/read-c1-r10/VERDICT.json`: a pair naming an install of another ledger must never own the uploader's
row, and a re-join that cannot prove the previous key must not split an old row; and its should-fixes). Where it and another
document disagree, this file wins and the other carries a "round 7", "round 8", "round 9", "round 10" or "round 11" note pointing
here. Every rule below has (a) a runnable fixture in `fixtures/` (the round-11 fixture `b4_offline_rebind.py` is red under `--rule
r6`, `r7`, `r8`, `r9` and `r10` and green under `--rule r11`; `s1b_runway_host_bound.py` is red under `r6`, `r7` and `r8` and green
under `r9`;
`checks/fixtures-summary.json`) and (b) a **pending test** in the repository that owns the rule (§C6), which fails today and names
the bead that turns it green. Nothing here is implemented;
nothing here is frozen until the lead signs the freeze list (`FREEZE.md`).

## C1. One actor-ownership predicate, fixed at the first sighting (review-r6 blocker 1, review-r1 blocker 1, review-r2 blockers 1-2 and should-fixes 1-4, read-c1c4 blockers 1-2 and should-fixes 1-5, read-c1-r10 blockers 1-2 and should-fixes 1-4; B6 cloud, B2a collector)

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

**The defect (review r2).** Round 8 fixed the answer at the first sighting but left three gaps (`input/review-r2/VERDICT.json`,
`checks/review_c1_orderings_r2.log`). (1) C4 said "a response from a different install replaces the pair". After a re-join the
**old** install's answer can still arrive: to a request that was in flight, or from a collector process that loaded its config
before the join (`plimsoll@03445d3a packages/collector-cli/src/cli.ts:2479` reads it once; `join` only prints "restart a running
collector", `:2375`; the cloud keeps the old install valid, `plimsoll-cloud@4954995 src/lib/auth/join-token-store.ts:83-104`). It
replaced the new install's pair, and rows captured under the new install's actor D were exported as C's on both paths (R4: 3 of 3
rows). (2) Under READ COMMITTED a sighting that read `binding_version` could interleave with the rebind that issued the version: T1
reads 1, the rebind commits 2, T3 judges the pair issued and exports C, T1 records the fact, every later judgment is null (R5: 1 of
12 interleavings flipped). (3) The fact was keyed by the named install, so any install of the tenant could poison another install's
next version by naming it (R7). Round 9 closes all three below: the collector accepts a version only from the install its ledger is
**joined** with (C4), the sighting is **serialized** against the rebind, and the fact is keyed by the **uploader**.

**The defect (the read of C1/C4 after round 9).** Round 9 left two gaps, both reached only by a faulty stamp (a pair naming a
version the cloud had not issued when it first saw it) and both breaking the promise that a row gets one answer whenever and on
whichever path it is judged (`input/read-c1c4/VERDICT.json`, `checks/c1-sql-orderings.log`). (1) The text locked the read of
`binding_version` but did not say when the facts and the audit rows are read, and C6's ingest surface judged from a preloaded view;
with the facts read before the lock, two first sightings of one pair on the ingest and summary paths at the same instant, around the
rebind that issues the version, split in 8 of 566 schedules (the summary read "no fact", the ingest recorded the fact and committed,
the rebind committed, the summary locked, read the issued version and exported C; every later judgment was null). (2) Keyed by the
uploader, one row judged by two installs of the same ledger got two answers: after a re-join the old install X judges the row's
summary member (a fact under X) and the new install Z delivers the row itself, and if X was rebound up to the version in between the
row was unallocated in X's part and someone's when Z delivered it. Round 10 closes both below: a sighting takes the lock **first** and
reads everything under it, and the fact is keyed by the **ledger** (the chain of installs of one Mac), not the uploader.

**The defect (the read of C1 after round 10).** Round 10 left two gaps, both about a row whose pair names an install of **another
ledger**, and both breaking the promise that a row gets one answer whichever path judges it (`input/read-c1-r10/VERDICT.json`,
`checks/round10-sql-orderings-unlinked.log` S8, S9). (1) The predicate judged a pair against the install it names and read only the
uploader's ledger's facts, so a same-tenant install P (server-bound to B) that uploaded a row stamped `(X, 2)` after another Mac's
install X had been rebound to C as version 2 got its row assigned to C: an actor attribution across Macs, which today's ingest (the
server-bound actor, `src/lib/ingest.ts:397-427`) never allows. (2) A re-join that cannot prove the previous install's key starts its
own ledger, which cannot see the old install's not-issued fact: X's summary judged the faulty `(X, 2)` nobody's, X was rebound up to
2, and the new unlinked install's delivery of the same row was C's. Round 11 closes both below: **ownership never crosses a
ledger**: a pair naming an install outside the uploader's ledger is refused before any judgment and records nothing, the collector
parks such rows, and the only release into a judgment is the ledger's link (the join's proof, or an admin's audited link), after
which the rows are judged in that ledger, where they read the same facts and audit rows the old install's summary did.

**The rule.** A stamp is the **pair** `(install, V)`: the `DeviceInstall` id of the response that supplied `V` and the version. The
collector persists the pair, stamps it on the identity row (`summary_members.actor_binding_version`, `.actor_binding_install`) and
sends it on the wire (`metadata.actorBindingVersion`, `metadata.actorBindingInstall`); the summary item's per-segment parts are
keyed by the pair. `actor_for_stamp(stamp)` is the only ownership function and both paths call it; it judges `V` against the
install the pair names, never against the uploading install:

| Stamp | Delivered raw row (ingest) | Undelivered member (part) |
|---|---|---|
| `(install, V)` **issued at its first sighting**: `V = 0`, or the audit row for `V` existed when the cloud first saw `(install, V)` in any request echo, delivered row or summary member | `actor_id = actor_of(install, V)`; basis `raw_ingest` (identical to `binding_at_capture` by construction) | `actor_id = actor_of(install, V)`, basis `binding_at_capture` |
| `(install, V)` **not issued at its first sighting** (`V > binding_version` then), recorded once as the durable fact `stamp_not_issued(ledger, install, V)` in the same transaction, keyed by the uploader's **ledger** (round 10: the first install of the uploader's chain of installs on one Mac, so the old install judging a summary member and the new install delivering the row after a re-join read one fact; round 9 keyed it by the uploader); or a pair naming an install the tenant does not have (judged closed, **nothing recorded**) | `actor_id = null`, `metadata.actorStampInvalid = true`, listed in the S4 certify; **stays null after the cloud issues `V`**, at ingest, on replay and in every summary revision | `actor_id = null`, basis `unallocated_stamp_invalid`, candidates = every actor in the install's history plus its current actor |
| `(install, V)` naming an install **outside the uploader's ledger** (round 11: another Mac of the tenant, or the previous chain of a re-join that could not prove the previous install's key) | **not judged**: the batch is refused (400 `stamp_from_other_ledger`, its body listing the refused pairs), **nothing recorded** in any ledger; the collector parks the rows (`outbox_row_parked`: undelivered, retained, never leased while parked) until the ledger is linked, or an admin releases them undelivered and never judged | **not judged**: the segment is parked (`summary_segment_parked`, exactly as for the flood refusal) and the rest of the batch is resubmitted |
| `null` (a collector older than B2a, or a B2a collector before the first versioned response of its **current** install, C4) | today's ingest binding (the uploading install's actor at ingest); basis `ingest_current` | `single_binding` (the install has no audit row and a non-null actor) else `unallocated_no_stamp` |

**The guarantee (what `fixtures/b4_offline_rebind.py` and the tests prove).**
1. **Fixed at the first sighting.** For every install and every `V`, the answer for `(install, V)` is decided the first time the
   cloud sees the pair and is never revisited: `actor_for_stamp` reads only the audit table and the `stamp_not_issued` facts, an
   audit row created after the fact does not revive it, and no fact can be created once the audit row exists **because the sighting
   is serialized against the rebind** (below, round 9): a rebind cannot commit between a sighting's read of `binding_version` and
   its fact, and (round 10) every input of the judgment, `binding_version`, the audit rows and the facts, is read **after** the
   sighting took the lock, never from a view loaded before it. Every judgment is a
   sighting, so every judgment of a row stamped `(install, V)` (ingest, a dead delivery's replay, any summary revision, either
   path, whichever install of the ledger uploads it, round 10) returns the same actor or the same null. The fixture probes every sighted pair at every instant from its first sighting
   to the end of the timeline and finds one answer each (round 7 flipped for `(W, 2)` and `(Zf, 2)`).
2. **Honest collectors are exact.** An honest collector is one that stamps only versions it persisted from responses of the
   install its ledger is **joined** with (C4: the joined install is recorded at join activation; a response from any other install
   is ignored and counted, so the old install's late answer after a re-join changes nothing) and clears the pair at a join, re-join
   or workspace transition. It never produces a not-issued pair: every stamped row is owned by `actor_of(install, V)` on both paths
   for every ordering of capture, rebind, contact, delivery, re-join and late answer, including a row still in the outbox at a
   re-join (it keeps the old install's pair and is judged against the old install, whoever delivers it). The echo, `heard_at`, the
   delivery instant and the summary instant are not inputs.
3. **A not-issued pair fails closed, permanently and visibly.** Rows carrying it are unallocated on both paths and under whichever
   install of the ledger judges them (never anyone's), and so are rows the same install later stamps legitimately with that version: `(install, V)` is poisoned, listed on the
   certify (`stamp_not_issued` with the first sighting, the binding version then and the affected row count) and on the impact
   report as unallocated with candidates; the repair is an admin rebind, which issues a fresh version. The cloud cannot tell a
   faulty stamp from a stamp issued later except by the order of sightings, which is why the fact is recorded durably at the first one.
4. **The null-stamp row remains the one disclosed exception**, confined to `stamp = null` on an install **rebound before the part
   is sealed**, whichever collector version wrote it (C4): its undelivered answer is `single_binding` while the install has no audit
   row and `unallocated_no_stamp` once it has one, so a part sealed before the install's first rebind and a part sealed after it
   differ, and that is disclosed (review r2 R9).
5. **Ownership never crosses a ledger (round 11).** A row uploaded by an install of ledger L is owned by an actor of L's binding
   history or by nobody: `actor_for_stamp` refuses, before any judgment, a pair naming an install outside L (the fixture's S8 probes
   every uploader, pair and instant and finds no crossing; round 10 crossed for every issued pair named across Macs), and a refused
   row is never judged until L is the named install's ledger. The first-sighting guarantee is therefore **per ledger**: for every
   ledger L, install in L and V, the answer for `(install, V)` in L is decided at L's first sighting of the pair and never revisited,
   and no other ledger ever judges that pair. What the hold costs an honest collector is a delay, never an answer: a pre-re-join row
   delivered through an unlinked ledger waits (S9) and, once the ledger is linked, is judged in that ledger, where it reads the facts
   and audit rows the old install's summary read, so it agrees with the member (null for a faulty pair, the version's actor for an
   honest one); a row an admin releases instead is acknowledged undelivered and never judged, so the summary's answer stands alone.

**Sightings and the durable fact.** `device_install_stamp_not_issued (tenant_id, ledger_install_id, recorded_by_install_id,
device_install_id, version, first_seen_at, binding_version_then, source ∈ {echo, row, member})`, unique per `(ledger_install_id,
device_install_id, version)` (round 10; `recorded_by_install_id` is the install whose request first saw the pair, for the certify),
`device_install_id` a foreign key to `device_installs` without cascade, written in the transaction of the request receipt, the ingest
or the summary judgment that first saw the pair above the install's `binding_version`; never deleted except by a tenant erasure
(below). A refused batch (schema violation, 400) judges nothing and records nothing. **Serialization (round 9, review r2 R5; round
10, read c1c4 blocking 1; round 11).** A sighting **first takes the named install's row `FOR SHARE`** in the transaction that records
the fact, and **only then reads the install's `ledger_install_id` (round 11), `binding_version`, the audit rows and the facts**, all
under that lock; a pair whose install's ledger is not the uploader's ledger is refused right there, before any further read (round
11, "Scope"); a view of any of them loaded before the
lock (at request start, or by a caller that hands `eventRowsForStorage` a preloaded view, C6) is not a judgment input and is re-read
under the lock. The rebind (`bindDeviceInstalls`, `reverseDeviceInstallActorBindings`, `plimsoll-cloud@4954995
src/lib/device-install-actor-binding.ts:128-134,151,258`) first locks the install rows `FOR UPDATE` (`lockInstalls`), then updates
the row (`binding_version + 1`, the actor) and inserts the audit row, in one transaction; `FOR UPDATE` conflicts with `FOR SHARE`, so
either the rebind waits until every sighting that holds the row commits (the fact then precedes the audit row and the pair is not
issued for ever) or the sighting waits for a rebind that holds the row and reads the issued version with its audit row (no fact).
PostgreSQL grants a `FOR SHARE` at once while a `FOR UPDATE` is merely waiting (compatible row locks do not queue), so a sighting
never waits for a waiting rebind, only for one that holds the row (`checks/round10-sql-orderings.log` S5c). A sighting or a view
that names several installs locks them in **ascending id order**, and B6 gives `lockInstalls` the same `ORDER BY`, so two lockers of
the same installs cannot deadlock (round 10, low). An implementation may instead run both at `SERIALIZABLE` with retry. The fact is
inserted `ON CONFLICT (ledger_install_id, device_install_id, version) DO NOTHING` and re-read, so two first sightings of the same pair
on two paths at once (R6) leave one fact and both judge null; neither batch is refused. Of the 12 interleavings of a faulty sighting
with the rebind that issues its version, 0 flip under this rule and the rebind waits in 4 (`b4_offline_rebind.py` R5; round 8 flipped
in 1); of the statement-level schedules of two first sightings of one pair on the ingest and summary paths around the rebind, 0 of 102
split with the lock taken first, while a view of the facts loaded before the lock, which round 9's text did not exclude, splits in 8
of 566 (`b4_offline_rebind.py` R5b, red under r9; reproduced on the real migrations in `checks/c1-sql-orderings.log` S2 and
`checks/round10-sql-orderings.log` S2, S5a-b). The durable proof is pending in the cloud: `actor-binding-stamp-postgres.contract.test.ts`
(B6): the R5 wait with the rebind observed waiting, the concurrent mirror order (the rebind holds its lock first; the sighting waits,
reads the issued version, answers the actor and records nothing: without `FOR SHARE` it reads the old version, records a fact after the
rebind commits and a judgment in between says the actor), and `loadSightingView` waiting for a held `FOR UPDATE` and returning the
committed version with its audit row and every committed fact. **Scope (round 9, review r2 R7; round 10, read c1c4 blocking 2).** Every
install belongs to a **ledger**: `device_installs.ledger_install_id`, the first install of the chain of installs on one Mac. A fresh
install is its own ledger. At a re-join the collector still holds the previous install's credentials (`join.ts:201-238` drops them
only when it stages the grant), so the join request carries `previousInstall = { deviceId, proof }` with `proof =
HMAC-SHA256(previous install's installKey, join token)`; the cloud's join route links the new install to the previous install's
ledger **only when the proof verifies** against that install in the token's tenant, whatever that install's lifecycle, and otherwise
gives the new install its own ledger and answers `lineage: unlinked` (disclosed on the join receipt, in `/status` and on the certify;
the pre-re-join rows such a ledger delivers are **refused and parked**, `stamp_from_other_ledger`, until the ledger is linked, round
11 below; an honest B2a collector links whenever it has a previous install). `actor_for_stamp` for a row or member uploaded by install U requires, first, that the install the pair
names is in U's ledger, `ledger(install) = ledger(U)`, and refuses the pair otherwise (round 11, below); it then reads the facts with
`ledger_install_id = ledger(U)`: the old install X judging a summary member and the new install Z delivering the same row after a re-join read one fact
(whichever recorded it first), so a faulty pair judged null in X's summary stays null when Z delivers it after X is rebound up to the
version (round 9 keyed the fact by the uploader and gave that row to the person bound to X then; `b4_offline_rebind.py` R10, red
under r9; `checks/round10-sql-orderings.log` S3, S3b). **Outside the ledger (round 11, read c1 r10 blockers 1-2).** A pair naming an
install of the tenant that is **not in the uploader's ledger** is **refused before any judgment**: the sighting reads the named
install's `ledger_install_id` under its lock, and when it differs from the uploader's ledger the batch is refused (400
`stamp_from_other_ledger`, its body listing the refused pairs), nothing is judged and **nothing is recorded** in any ledger; on a
summary batch the collector parks the segments whose parts name a refused pair and resubmits the rest exactly as for the flood
refusal (C4's `partitionFloodRefusal` takes either reason); on a delivery it parks the refused rows in the outbox
(`outbox_row_parked`, `partitionRefusedRows`: undelivered, retained under the ordinary rules, never leased while parked, listed in
`/status` as `parkedRows` beside the `lineage` and on the certify) and resubmits the rest at once. Round 10 judged such a pair
against the install it named and read only the uploader's ledger's facts, so another Mac's issued pair assigned the uploader's row to
that Mac's actor (S8), and an unlinked re-join's delivery of an old faulty row was someone's while the old install's summary said
nobody (S9). So one Mac's faulty or hostile traffic can neither poison another Mac's next version (R7 stays closed, now by
construction: no install can sight another ledger's pairs at all; the poisoning of guarantee 3 is confined to the ledger's own
pairs) nor take another Mac's actor, and the server-bound actor today's ingest protects (`src/lib/ingest.ts:397-427`) is never
overridden across Macs (`b4_offline_rebind.py` S8, red under r10; `checks/round11-sql-orderings.log` S8). **The link is the release
(round 11, blocking 2).** A parked row is judged only once its uploader's ledger **is** the named install's ledger: at join, by the
verified proof (`lineage: linked`, `ledger_linked_by = join_proof`); later, by an **admin's audited link** (`linkLedgerByAdmin`:
allowed only for an install that is still its own ledger, so a chain is linked once; it takes the install row `FOR UPDATE` like a
rebind, sets `ledger_install_id` with `ledger_linked_at` and `ledger_linked_by`, and **re-keys the facts of the install's old
ledger** to the new one in the same transaction, so a pair poisoned in the unlinked ledger stays poisoned after the link,
`checks/round11-sql-orderings.log` S10, and a sighting never reads a half-linked ledger, S11). The next authenticated response then
reports `lineage: linked` and the collector resubmits the parked rows, which are judged in that ledger, where they read the facts
and audit rows the old install's summary read: the same answer (`b4_offline_rebind.py` S9, red under r10;
`checks/round11-sql-orderings.log` S9, S9h). A row an admin releases without a link is acknowledged `undeliverable_unlinked_ledger`
under the admin's receipt: never delivered, never judged, listed on the certify, so the old install's summary answer stands alone.
An honest B2a collector links at join whenever it still holds the previous install's key, so the hold is reached only when that key
is gone (a lost config, a rotated key) or by a faulty or hostile collector, and it costs an honest collector a delay, never an
answer. **What "another Mac never enters the ledger" means (round 11, read c1 r10 should-fix 4).** The ledger is the chain of
installs that proved possession of one another's keys; the possession proof cannot tell a copied key on another physical Mac from
the same Mac, so the separation between Macs holds **while the previous install's key stays secret** (the ordinary uncompromised-key
assumption of the install key itself), and lineage cannot be claimed without that key. Within that assumption the first-sighting
guarantee is per ledger (guarantee 5), and the intentional R7 separation is exactly the ledger boundary: nothing an install of
another ledger sends is ever judged in, or recorded for, this ledger. A pair naming an install of another tenant still fails closed
and records nothing (the cloud test asserts the absence of the fact, not only the null). **Cap (round 9, low).** A request may create at
most 8 new facts; a request that would create more is refused whole (400, `stamp_not_issued_flood`, its body listing the refused
pairs), judges nothing and records nothing, so an authenticated collector cannot write one fact per distinct version per batch. Every
new fact a request creates is listed on its receipt. **On a summary batch (round 10, read c1c4 low)** the collector splits the item:
the segments whose parts name a refused pair are **parked** (`summary_segment_parked`, listed in `/status` and on the certify, retried
after the next versioned response of the joined install is heard or released by an admin) and the rest of the batch, the claim and
the other segments' receipts included, is resubmitted at once, so one faulty segment never stalls the claim and receipt lane
(collector `actor-stamp.contract.ts` test 11).

**Retention and erasure (round 9, review r2 should-fix 2; round 10, read c1c4 should-fixes 3-5).** The answer for every pair is
read from the audit rows and from every install a pair can name, the **old** install after a re-join included, **whatever that
install's lifecycle**: `device_installs.lifecycle` (`active`, `suspended`, `revoked`; `src/lib/fleet-registry/contracts.ts:38-64`)
gates the authentication of requests, never the audit rows, so revoking the old identity after a re-join (the natural clean-up) turns
no pair into null, and the view a sighting reads is built from every install of the tenant, never from the active ones only
(`b4_offline_rebind.py` section 12, red under r9; `checks/round10-sql-orderings.log` S6). They are therefore kept **until tenant
erasure**: audit rows, superseded installs and the `stamp_not_issued` facts are outside every retention job and every future
device-removal path (a path that deleted an install would turn its pairs from an actor into null). The fact's foreign key to
`device_installs` (no cascade) refuses the delete of an install some fact names, that is of faulty-stamp installs only; every other
install, the old install after an honest re-join included, is guarded by an **erasure-only delete trigger** on `device_installs` like
the audit table's (`prisma/migrations/20260923170000_…:24-41`): a delete is allowed only while `plimsoll.erasing_tenant` names the
row's tenant, so `capture_watermarks`' cascade (`20260923180000_…:45`) can fire under erasure alone. At `plimsoll-cloud@4954995` only
tenant erasure deletes installs or audit rows (`src/lib/work-intelligence/subject-rights.ts:500,506`). Tenant erasure deletes
`device_install_stamp_not_issued` (tenant-scoped) **before** `device_installs`, in the same transaction, and the residue check counts
it (ARCHITECTURE.md §7); the order that matters is the one erasure runs, `TENANT_COLUMN_MODELS` in
`src/lib/work-intelligence/tenant-erasure-plan.ts:15-49` (`subject-rights.ts:549` iterates it; the deleter object literal's key order
decides nothing), so the fact model is listed there before `DeviceInstall`; the `tenant_id` column exists for exactly that. Tests:
cloud `schema-additions.contract.test.ts` (the model with `tenant_id`, `ledger_install_id`, `recorded_by_install_id`, the install
relation without cascade; the plan's order and the residue check; the guard migration), `actor-binding-stamp-postgres.contract.test.ts`
(the refused delete of an install a fact names and of one no fact names; erasure through the erasure-only path with zero residue; a
revoked install's issued pair still the version's actor); cloud `actor-binding-stamp.contract.test.ts` test 13 (lifecycle).

**What the echo is for.** `actorBindingVersionHeard` on every request, **upload deliveries included**, is the collector's
`max(persisted version of the current install, highest stamp carried for the current install)`, and (round 10, read c1c4 low) the
request names the install it is for, `actorBindingInstallHeard`, the ledger's joined install: a daemon still running on the config
of the old install X computes its echo for the ledger's joined install Z, and the cloud attributes an echo only to the install it
names, ignoring and counting one whose named install is not the authenticated install (`echo_from_other_install`), so no `heard_at`
and no fact can arise in X's ledger from Z's echo. It is a diagnostic and a sighting,
never ownership: `stamp_ahead_of_echo` (a request carries a stamp above its own echo), `echo_ahead_of_binding` (an echo above the
install's `binding_version`; it records the not-issued fact), `stamp_sequence_regressed` (a segment's versions are not
non-decreasing in rowid order) and `stamp_from_earlier_install` (a pair naming an install other than the uploader **but in its
ledger**: expected after a linked re-join, counted; a not-issued one records a fact for the uploader's ledger, round 10; a pair
naming an install **outside** the ledger is `stamp_from_other_ledger` and refuses the batch, round 11) are recorded on the request receipt and the segment, shown in `/status`, the S4 certify and the impact report,
and change no actor. `heard_at` on the audit row is the instant of the **first request that echoes** the version, a disclosure
fact for the export's `changed_at → heard_at` window; a response that supplies a version sets nothing, and the registration
response is not an echo. `heard_at` exists only for an **issued** version, because it lives on the audit row: the instant of an
echo above `binding_version` is the fact's `first_seen_at`, never a `heard_at` (round 9; the round-8 fixture kept one for the
never-issued 5).

**The corrected timeline** (`fixtures/b4_offline_rebind.py`, rounds 8-9; should-fixes 1-2 of review r1). Install X: registration
supplies 0; requests at 50 (heartbeat, echo 0: `heard_at(0) = 50`), 150 (delivery), 300 (heartbeat, echo 0; its response first
supplies 1), 400 (the delivery that carries the row stamped 1: echo 1, `heard_at(1) = 400`, not 650), 650 (heartbeat, echo 1;
response supplies 2), 700 (delivery, echo 2: `heard_at(2) = 700`), 710 (a delivery carrying a never-issued stamp 5: echo 5,
`echo_ahead_of_binding`, `stamp_not_issued(X, 5)`). The fixture's `--rule r6` mode is round 6 **as written** (two validators, first-echo
`heard_at`), so case D splits null/B under it; `--rule r7` is round 7 as written with the round-1 fixture's modelling (deliveries are
non-echoing instants, `heard_at(0)` = the registration), so cases I and J split under it; `--rule r8` is this rule. Cases: A-H
(round 7), I (a stamp from a version issued later, on install W), J0 (a pre-re-join outbox row), J1/J3 (an honest re-join), J1' (a
join whose response carries no version), Jf (a faulty collector carrying a stale version into the new install), K (an A→B→A
reversal), the over-time probe and the timeline facts; **round 9** adds L (the old install's late answer after the re-join:
ignored and counted, the rows stay D's), R5 (the 12 interleavings of a sighting with the rebind: 0 flips with the row lock, the
rebind waits in 4) and R7 (a fact keyed by the uploader never enters the named install's own view; a foreign tenant's install
records nothing), and `heard_at` only for issued versions; **round 10** adds R5b (two first sightings of one pair on the ingest and
summary paths at the same instant around the rebind, as statement-level schedules with PostgreSQL's waits: every order round 9's text
permitted, the facts read before the lock included, must agree; 8 of 566 split under r9, 0 of 102 under r10), R10 (two installs of one
ledger sight the same pair around the rebind: 2 of 142 schedules split under r9's uploader key, 0 under the ledger key; the sequential
re-join case, X's summary at 300, the re-join at 500, X rebound to G as version 5 at 600, Z's replay at 700: null throughout under r10,
G's under r9; the key separates Macs and joins installs of one Mac; a join whose proof fails starts its own ledger and is disclosed)
and the lifecycle case (an install revoked at 900 still names its version's actor at 950); **round 11** adds S8 (another Mac's
install P, bound to B, uploads X's issued pair `(X, 2)`: held, never C's, nothing recorded; and for every uploader, pair and instant
the answer is an actor of the uploader's own ledger or nobody) and S9 (the unlinked re-join: X's summary judges the faulty `(X, 2)`
nobody's at 300, U re-joins without proof at 500, X is rebound to C as 2 at 600, U's delivery at 700 is held, the admin links U at
800 and the delivery at 850 is nobody's like the member; the honest `(X, 1)` is B's on both paths; the link's re-key keeps U's own
poisoned `(U, 1)` nobody's after U is rebound to 1; a linked re-join is judged at once as in round 10; a copied key passes the
possession proof): 60 checks, r6 21/60, r7 29/60, r8 44/60, r9 47/60, **r10 54/60**, r11 60/60 (`--rule r8` is round 8 as written:
the late answer replaces the pair, no lock, the fact keyed by the named install; `--rule r9` is round 9 as written: the lock on the
read of `binding_version` only, the fact keyed by the uploader, no lifecycle rule; `--rule r10` is round 10 as written: a pair judged
against the install it names in the uploader's ledger's view, whichever ledger uploads it).

**Judgment state.** Validity is decided at the first sighting of `(install, V)` and recorded; a version that did not exist then is
invalid on both paths for ever. Parts are computed per received revision and frozen at the seal; a raw row's actor is bound once at
ingest; because the fact precedes both, the rebind cannot slip between a sighting's read and its fact, every input is read after the
lock, every install of the ledger reads the same fact (round 10), and a pair naming an install outside the ledger is never judged
until the ledger is linked (round 11), neither can differ from the other.

**Tests.** Cloud `tests/contracts/lean/actor-binding-stamp.contract.test.ts` (B6, 15 cases): the reviewer's issued-but-unheard row,
the honest orderings and the A→B→A reversal, the never-issued stamp, the first-sighting rule with the replay and the repair, the
re-join (honest, pre-re-join outbox row, faulty, null after the join), C4, `firstEchoHeardAt` with deliveries as requests, an echo
attributed only to the install it names, and the diagnostics, `eventRowsForStorage` binding by the pair against the view loaded under
the lock (a not-issued pair yields one new fact for the uploader's ledger, an unknown install fails closed and records **no** fact),
the ledger-keyed fact across a re-join (X's fact binds Z and Z's binds X, before and after X is rebound up to the version), lifecycle,
`acknowledgedResponse` (with `lineage`, round 11), the per-actor parts, and (round 11) another Mac's install refused before any
judgment with no fact anywhere (R7, test 11), S8 on both paths and through `eventRowsForStorage` (test 14), S9 held and then judged
after the link with the null-stamp exception untouched (test 15); `actor-binding-stamp-postgres.contract.test.ts` (B6, 9 cases on the
repository's disposable Postgres cluster, set up as `checks/postgres-harness-repaired-probe.log` proves): the fact in the sighting's own
transaction, replay after the version is issued and a revoked install's issued pair, the R5 row lock with the rebind observed waiting
and the concurrent mirror order, R6 and R7 by the ledger, `loadSightingView` lock-first with the reviewer's ordering replayed through
it, one ledger under two installs with the join's lineage proof, retention with the erasure-only guard and erasure with zero residue,
and (round 11) `cluster()` stopping the cluster on any set-up failure, R7 refusing (case 4), S8, S9 with the admin link, its re-key
and the linked-once rule (case 8), and the join route driven through `fixtures/join-route-child.ts` (case 9: a reused token refused
with no install created, a proof replayed under a fresh token and a previous install of another tenant joining unlinked, the linked
grant with `lineage`); `schema-additions.contract.test.ts` (5 cases: the columns with the link's audit columns (round 11), the fact with its tenant, ledger, recording install and install reference,
the plan's erasure order and the residue check, the guard migration, the other additions). Collector
`tests/contracts/lean/actor-stamp.contract.ts` (B2a, 12 cases): null before the first response, the pair on the identity row and never
lowered within one install, the echo with its install and the pair on the wire, both keys allowlisted through the seal, the scope
cleared by a re-join and a transition with the new install's 0 accepted, join activation recording the new install with the
handshake's version while a pre-re-join row keeps its pair, the old install's late answer ignored and counted, join activation driven
through `join.ts` with a fake cloud (the handshake's version recorded only when its `deviceId` is the grant's install; the join request's
`previousInstall` proof), the pre-B2a seed and `joined_install_unknown`, the partial join reported as `join_incomplete` with the echo
naming the ledger's install, the flood refusal splitting a summary batch, and (round 11) a delivery refused as
`stamp_from_other_ledger` split the same way, its rows parked and never judged, retried on `lineage: linked` or released by an
admin as undeliverable (test 12); `helper.contract.ts` gains a green guard proving that `join.ts` activation completes on today's
code with a fake cloud.

## C2. S1b runway: G per host, one G, G owed (review-r6 blocker 2 and should-fix 5; review-r1 blocker 2 and should-fix 5; review-r2 should-fixes 5-6; B1, B10a and B2a collector)

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
   full live-page copy), and it is deleted after the count. **The re-census before S3 runs during the hold (round 9, review r2):**
   its copy is made only when `free − ledger ≥ reserve + rebuild_headroom + G_owed`, so the copy itself can never put the runway
   under a rung (`censusPreflight` reason `copy_would_trip_rung`; a copy that fits `ledger + reserve` alone may not). The 1.25 factor covers the **row widths** until S2 has measured them on
   a copy; it no longer covers the mix, which is measured.
2. **One G.** `G_gate = 1.25 × G_host` (or the copy's measured peak) is subtracted in the runway numerator **and** sizes the
   multiplier: `g_gate = G_gate / raw_bytes_at_census`, `hold_growth_per_day = p95_7d(gross growth) × (1 + g_gate × raw_share)`.
3. **G owed (round 8).** While conversion runs,
   `G_owed = max(0, G_gate − C_conv) + g_gate × U`,
   where `C_conv` is the bytes the **converter itself has written** for census-era history, **measured** (round 9, review r2) as
   its own page allocation: inside each converter write transaction, `Δ(page_count − freelist_count) × page_size` from `PRAGMA
   page_count` and `PRAGMA freelist_count` read at the transaction's start and again before its commit (SQLite has one writer at a
   time, so the delta is the converter's alone), summed into the converter's checkpoint row and committed with the chunk
   (`withMeasuredWrite`, §C6); never a page count of the lean tables, which from S2 on also holds the live writer's rows, and never
   the rows written × the estimated widths (which misses by the width factor); and `U` is the bytes of the raw rows admitted **after
   the census** that no lean row covers yet (neither converted nor dual-written; rows admitted after S2 are dual-written at
   admission and are growth, already in `free_disk` and in the multiplier, not conversion work), in the **census's raw-bytes
   basis**: `U = unfolded_rows × raw_bytes_at_census / raw_rows_at_census`, so `g_gate × U` is `G_gate` per raw row × rows. `G_owed = 0`
   once every census-era and post-census row is folded. `runway_days = (free_disk − reserve − G_owed − rebuild_headroom) /
   hold_growth_per_day`. The abort ladder's rungs read this runway. A `/status` field `runway.g = {gate, owed, converterWritten,
   unfoldedRawBytes, leanTableBytes, basis: census | copy_peak}` discloses the terms; `leanTableBytes` is disclosure only.
   **What this guarantees** (`fixtures/s1b_runway_host_bound.py`, round 8, whose truth is a byte-level simulation of the ledger that
   never calls the rule): with true widths at or below 1.25 × the estimates the published runway never exceeds the true runway in
   any of the reviewer's dual-write and post-census scenarios, no rung fires later than the truth, and the only conservatism is the
   1.25 allowance itself (0.25 × the census-era G until completion plus 0.25 × the post-census owed bytes); with widths above 1.25 ×
   the estimates the rule overstates, which is why the **numbers** stay open until S2 measures the widths (FREEZE.md). The rule does
   **not** self-correct to real widths (round 9 corrects the round-8 CHANGES.md): with `max(0, G_gate − C_conv)`, real widths above
   the estimate drive the first term to 0 early, so it is exact at 1.25 × and safe only up to it. The fixture's truth divides by
   rule 2's own growth figure, so it tests the owed bytes, not the growth rate (review r2, noted).
4. **Preflight receipt.** The S1b receipt records the census (counts, the segment proxy, when, on which copy, the free space at the
   copy, that it followed the catch-up), `G_gate`, its basis, the measured conversion size and abort peak where a copy exists, and
   the runway series.

**Tests.** Collector `tests/contracts/lean/runway.contract.ts` (B1, B10a, B2a; 6 cases): the reviewer's 333-session counterexample
(the gate's G bounds the host's real need; the segment proxy raises G; an unmeasured host has no gate value), at the gate's 63-day
threshold the host really has ≥ 63 days with the multiplier on `G_gate`, G owed against an independent simulation (dual-write pages
ignored, the two terms, never overstating in the six reviewer scenarios, the 5-day and 2-day rungs never late, 0 at completion),
the round-6 `s1b_runway_geometry.py` assertion, `censusPreflight` (after the catch-up, at most a day old, room for the copy, and
during the hold room for the ladder's terms too), and `withMeasuredWrite` (round 9: `C_conv` equals the page delta around the
converter's own transaction and excludes another writer's pages between two of them). Fixture: `fixtures/s1b_runway_host_bound.py`
(25 checks: r6 10/25, r7 10/25, **r8 22/25**, r9 25/25; the round-9 checks: a measured `C_conv` exact at width factors 1.25 and
0.8 where rows × estimates is not, the live writer's pages outside it, `U` in the census's basis, and the during-hold copy gate).

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

## C4. Null stamps before the first response, and the joined-install scope of the pair (b0Carries 3b; review-r1 blocker 1; review-r2 blocker 1; read-c1c4 should-fixes 6-7; B2a collector, B6 cloud)

A B2a collector stamps every raw row with the pair `(install, version)` it has **persisted**: the `DeviceInstall` id and the
`actorBindingVersion` of the latest authenticated response **of the install its ledger is joined with**. **The joined install
(round 9).** Join activation records the grant's `DeviceInstall` id (`stagedConfig.cloudDeviceId`, `plimsoll@03445d3a
packages/collector-cli/src/join.ts:225,660`) in the active ledger's binding row (`collector_workspace_binding.joined_install`) in
the same step as `useWorkspace`/`transitionWorkspace` with the new installation epoch (`join.ts:605-621`), together with the
handshake response's `actorBindingVersion` where the response carried one **and named the grant's install** (`deviceId` equal to
`stagedConfig.cloudDeviceId`; a handshake response naming another install leaves the pair null and is counted, round 10; the handshake
ran on a temporary ledger, `join.ts:537-583`, so the active ledger never saw it): `recordJoinedInstall(installId, version | null)`.
The join request carries the previous install's id with proof of possession of its key, so the cloud can link the new install to the
ledger's lineage (C1 "Scope", round 10). A versioned response is
**accepted only when its `deviceId` equals the ledger's `joined_install`** and only upward (a lower version from the joined install
never lowers the pair); a response from any other install is **ignored and counted** (`actorBinding.ignoredResponses`), never a
replacement: so the old install's answer to a request that was in flight at the re-join, or from a collector process that loaded its
config before the join (`cli.ts:2479`; the cloud keeps the old install valid, `join-token-store.ts:83-104`), changes nothing, and
the daemon reads the joined install from the ledger, never from its start-up config. Round 8's "a response from a different install
replaces it" is withdrawn (review r2 R4). A ledger joined before B2a and not re-joined since has no `joined_install`: B2a's first
start seeds it from the config's `cloudDeviceId` when the config carries one (`seedJoinedInstall`; config and ledger are written
together at a join, `join.ts:605-648`, so at start they agree; a recorded joined install is never overwritten by a seed); otherwise
every response is ignored and counted, rows are null-stamped, `/status` says `joined_install_unknown`, and the next join records it.
**The partial join (round 10, read c1c4 should-fix 6).** Activation changes the ledger (`useWorkspace` or `transitionWorkspace` at
`join.ts:605/:618`, and with B2a `joined_install`) before it writes the config (`:648`) and does not roll the ledger back if the
config write fails: the daemon then authenticates as the old install while the ledger's joined install is the new one, every
response is ignored and counted, and `/status` reports `actorBinding.state = join_incomplete` (the ledger's joined install and the
config's `cloudDeviceId` differ) until `join --resume` completes the activation; the batch's echo names the ledger's joined install
(C1), so the cloud never attributes it to the config's install. A join, re-join or workspace transition (`buffer.ts` `useWorkspace` with a
new installation epoch, `transitionWorkspace`) clears the pair **and** the joined install to null; the join that follows records
the new one. Until the first versioned response of the **joined** install the collector writes **null**, exactly like a pre-B2a
collector: a fresh install's rows before its first contact, an upgraded install's rows before its first post-upgrade response, and a
re-joined ledger's rows between the join and the new install's first versioned response carry `actor_binding_version = null`; the
join records the new install's version 0 where the handshake response carried it, so that window is normally empty. Rows already
in the outbox at a re-join keep the pair they were stamped with and are judged against that install (C1; through a ledger the join
could not link they are held until the ledger is linked, C1 "Scope", round 11: the collector's promise here, keep the pair, is
unchanged). The cloud treats every
null stamp by the C1 exception, so the exception is stated as "stamp = null on an install rebound before the seal", not as "older
collectors". The collector's `/status` shows `actorBinding = {state ∈ ok | joined_install_unknown | join_incomplete,
joinedInstall, install, version, heardAt, stampedRows, nullStampedRows, earlierInstallRows, ignoredResponses, lineage ∈ linked |
unlinked, parkedSegments, parkedRows (round 11)}` so the size of the exception, of the re-join tail, of the ignored late answers and of the degraded states
is visible per host, and B2a's acceptance counts the null-stamped rows admitted before the first response on the Studio5 copy. Test:
collector `actor-stamp.contract.ts` (12 cases: null until the first response; stamped after; the joined install recorded and cleared by
a re-join and a transition; the pre-re-join row keeps its pair; the old install's late answer ignored and counted, the echo the joined
install's; round 10: join activation driven through `join.ts` with a fake cloud, the handshake's version recorded only when its
`deviceId` is the grant's install, the join request's lineage proof; the pre-B2a seed and `joined_install_unknown`; the partial join
as `join_incomplete` with the echo naming the ledger's install; the flood split; round 11: the parked rows) with `helper.contract.ts` proving the `join.ts` premise;
cloud `actor-binding-stamp.contract.test.ts` (C4 case, the re-join cases). Fixture: `b4_offline_rebind.py` case L (red under r8) and
the join-lineage check of section 11 (red under r9).

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
- `src/lib/actor-binding-stamp.ts`: `actorForStamp({ stamp, path, binding, ledgerInstallId })` → `{ actorId, basis, stampInvalid? }`,
  `ledgerInstallId` being the uploader's ledger (round 11: the call **throws** `StampFromOtherLedgerError`, message
  `stamp_from_other_ledger`, when `binding.ledgerInstallId` differs, before any judgment, so both paths refuse at the one ownership
  function), where `binding = { installId, ledgerInstallId, currentVersion, currentActorId, actorByVersion, hasAuditRows, notIssued:
  number[], lifecycle }` is the state of the install the stamp's pair names as `loadSightingView` returned it under the lock, `notIssued` being the facts of the
  reading ledger's view; `lifecycle` is carried, never judged (round 10); `sightStamp(binding, stamp)` → the binding with `notIssued`
  extended when the stamp is above `currentVersion` (pure; persisting the fact is B6's); `firstEchoHeardAt(requests)` over every
  request kind, a request `{ at, echoed, install?, echoedInstall? }` counted only when `echoedInstall` is absent or equal to `install`
  (round 10); `stampDiagnostics({ stamps, echoed, currentVersion })` → `{ stampAheadOfEcho, echoAheadOfBinding, ownershipChanged }`.
  (B6, C1, C4)
- `src/lib/ingest.ts`: `eventRowsForStorage(batch, tenantId, authorizedActorId, { uploadingInstallId, ledgerInstallId, bindings,
  sightings })` binds the pair `metadata.actorBindingVersion` + `metadata.actorBindingInstall` through `actorForStamp` against
  `bindings[actorBindingInstall]`; `bindings` **is the record `loadSightingView` returned in the ingest transaction, after the lock,
  for the installs the batch's pairs name** (round 10; the function stays pure, the route may not hand it a view loaded earlier); a pair
  naming an install outside `bindings` fails closed and records nothing; a pair naming an install whose `ledgerInstallId` is not the
  option's `ledgerInstallId` makes the whole call throw `StampFromOtherLedgerError { status: 400, reason: "stamp_from_other_ledger",
  pairs }` before any row is bound, and the route answers that 400 with the pairs, judging nothing and recording nothing (round 11);
  sets `metadata.actorStampInvalid`; appends every new
  not-issued fact `{ ledgerInstallId, recordedByInstallId, deviceInstallId, version, source }` to `sightings`, which the route persists
  in the same transaction (round 9). The summary path (`actorPartsForSegment`) takes its `install` view from the same call in the
  judgment's transaction. (B6, C1)
- `src/lib/actor-binding-stamp-store.ts` (rounds 9-11): `loadSightingView(tx, { tenantId, ledgerInstallId, installIds })` locks the
  named installs' rows `FOR SHARE` in ascending id order **first**, then reads each install's `ledger_install_id` (round 11),
  `binding_version`, the audit rows and the ledger's facts under the lock → `Record<installId, binding>` (an install the tenant does
  not have is absent; a revoked or suspended one is present; one of another ledger is present with its own `ledgerInstallId`, so the
  predicate refuses it); `sightPairInTransaction(tx, { tenantId, ledgerInstallId, recordedByInstallId, deviceInstallId, version,
  source, at })` takes the row `FOR SHARE`, then reads the install's ledger and rejects `StampFromOtherLedgerError` when it is not
  `ledgerInstallId` (round 11), else reads and judges, inserts the fact `ON CONFLICT DO NOTHING` when the version is above
  `binding_version` and re-reads → `{ issued, actorId, recorded, bindingVersionThen }`; `linkLedgerByAdmin(tx, { tenantId, installId,
  ledgerInstallId, changedBy })` → `{ linked, ledgerInstallId, rekeyedFacts }` (round 11: the install row `FOR UPDATE`, allowed only
  for an install still its own ledger, `ledger_install_id` with `ledger_linked_at` and `ledger_linked_by`, and the re-key of the
  install's old ledger's facts, in one transaction; the join route sets `ledger_linked_by = join_proof` on a verified proof and the
  grant carries `lineage`); `issueBindingVersionInTransaction(tx,
  { tenantId, deviceInstallId, actorId, changedBy })` is the rebind's write (`binding_version + 1` on the install row, the audit row
  carrying it) that `bindDeviceInstalls` and `reverseDeviceInstallActorBindings` call after `lockInstalls` (which gains `ORDER BY id`);
  `stampFactsFor(tx, { ledgerInstallId, deviceInstallId })` lists the recorded versions; `linkLedgerAtJoin(tx, { tenantId,
  newInstallId, previousInstallId, token, proof })` is the join route's lineage step → `{ linked, ledgerInstallId }` (linked only when
  `proof = HMAC-SHA256(previous install's install_key, token)` verifies; the join route reads `previousInstall` from the request and
  answers `lineage`). Proved on the repository's disposable Postgres cluster by `actor-binding-stamp-postgres.contract.test.ts`
  (needs `PLIMSOLL_PROOF_PG_BIN` as `ci.yml`'s usage-projection step has; the harness applies `0_init` first, connects as the cluster's
  superuser through the socket with the port in the authority, inserts `work_tenants.updated_at`, and runs the erasure in a child
  process bound to the cluster, `tests/contracts/lean/fixtures/erase-tenant-child.ts`; `checks/postgres-harness-repaired-probe.log`;
  round 11: `cluster()` stops the cluster on any set-up failure, the join route is driven in a child the same way,
  `tests/contracts/lean/fixtures/join-route-child.ts`, proved runnable against today's route by `checks/join-route-child-probe.log`,
  and on macOS PostgreSQL 17 needs a valid `LC_ALL` to start, `checks/round11-sql-orderings.log`). (B6, C1)
- `src/lib/delivery-ack-response.ts`: `acknowledgedResponse` emits `actorBindingVersion` and (round 11) `lineage` for a registered
  install (`CollectorUploadAuthorization` gains `actorBindingVersion: number` and `lineage: "linked" | "unlinked"`), so a collector
  whose rows are parked learns when its ledger was linked. (B6)
- `src/lib/activity-summary/contract.ts`: `ACTIVITY_SUMMARY_PAYLOAD_KIND`, `isActivitySummaryPayload`, `activitySummaryBatchSchema`,
  `SUMMARY_ITEM_KINDS` (with `day_summary`); `src/lib/activity-summary/judge.ts`: `judgeSummaryItems(items, stored)`;
  `src/lib/activity-summary/actor-parts.ts`: `actorPartsForSegment({ members, install })`. (B6)
- `src/lib/capture-watermark/contract.ts`: `captureCoverageForPeriod` with `until: null` open gaps and `resolvedAt`. (B6, C5)
- `src/lib/economics/token-volume.ts`: `tokenVolumeState`, `normalisedTokens`, `pricingGate`. (B15)
- `src/lib/work-intelligence/tenant-erasure-plan.ts`: `TENANT_COLUMN_MODELS` lists `DeviceInstallStampNotIssued` before
  `DeviceInstall` (the order erasure runs, `subject-rights.ts:549`; the CI proof holds the list against the schema);
  `src/lib/work-intelligence/subject-rights.ts`: the tenant-scoped delete list names it (bound to the plan by `satisfies`) and
  `countTenantResidue` counts it (rounds 9-10). (B6, C1)
- `prisma/migrations/<B6>/migration.sql`: the erasure-only delete trigger on `device_installs` (`BEFORE DELETE`, allowed only while
  `current_setting('plimsoll.erasing_tenant', true)` is the row's tenant, else `RAISE EXCEPTION … erasure`), the `ledger_install_id`
  column backfilled with the row's own id, `lockInstalls`' `ORDER BY`. (B6, C1)
- `prisma/schema.prisma`: `DeviceInstall.bindingVersion`, `DeviceInstall.ledgerInstallId`, `DeviceInstall.ledgerLinkedAt` and
  `.ledgerLinkedBy` (round 11: how and when the install was linked into a ledger other than its own, `join_proof` or the admin),
  `DeviceInstall.activityLaneClosedAt`,
  `DeviceInstallActorBindingAudit.bindingVersion` and `.heardAt`, models `AiSummaryActorPart`, `CaptureGap` and
  `DeviceInstallStampNotIssued` (`tenantId`, `ledgerInstallId`, `recordedByInstallId`, `deviceInstallId`, `version`, `firstSeenAt`,
  `bindingVersionThen`, `source`, unique per `(ledgerInstallId, deviceInstallId, version)`, a relation to `DeviceInstall` without
  cascade; round 10). (B6, C1)

**Surfaces the collector tests bind (plimsoll, `tests/contracts/lean/`).**
- `packages/collector-cli/src/lean/schema.ts`: `ensureLeanSchema(db)` (ARCHITECTURE.md §3 DDL v3 plus C3) on the ledger connection
  (or one exposed as `buffer.leanDatabase`), which must keep `foreign_keys = ON`: **finding (B0):** better-sqlite3 opens every connection
  with foreign keys enforced, so at `03445d3a` the ledger connection already has `PRAGMA foreign_keys = 1` (the guard test
  `schema.contract.ts` last case is green today); the round-6 note that B2a must "set" it becomes "must not turn it off";
  `raw_retention_control.hold_reason`; `collector_workspace_binding.actor_binding_version`, `.actor_binding_install` and
  `.joined_install` (round 9); `raw_retention_receipts` accepting the four reasons. (B2a, B10a, B10b)
- `packages/collector-cli/src/buffer.ts`: `prune` deletes nothing while `hold_reason` is set; `retentionProgressStatus().hold`;
  `recordJoinedInstall(installId, version | null)` (join activation: the grant's install becomes `collector_workspace_binding.
  joined_install`, the handshake's version the pair, round 9; the version only when the handshake response's `deviceId` is the grant's
  install, round 10); `seedJoinedInstall(cloudDeviceId | undefined)` → `"seeded"` | `"kept"` | `"unknown"` (B2a's first start on a
  pre-B2a ledger, round 10); `recordActorBindingVersion(version, installId)` → `"accepted"` (the joined install, upward) | `"kept"`
  (the joined install, not above the persisted version) | `"ignored"` (any other install, or no joined install known; counted in
  `workspaceBinding().ignoredResponses`); `workspaceBinding().actorBindingVersion`, `.actorBindingInstall` and `.joinedInstall`, all
  cleared by `useWorkspace` with a new installation epoch and by `transitionWorkspace`; `summary_members` written at `append` with the
  pair (`actor_binding_version`, `actor_binding_install`) and two edges when `lean.write` is on (`new LocalEventBuffer(path, { lean:
  { write: true } })`). (B10a, B2a; C1, C4)
- `packages/collector-cli/src/join.ts` (round 10): activation calls `recordJoinedInstall` in the same step as `useWorkspace` /
  `transitionWorkspace` (`:605-621`) with the handshake response's `actorBindingVersion` when its `deviceId` equals the grant's
  install; the join request body carries `previousInstall = { deviceId: existing cloudDeviceId, proof: HMAC-SHA256(existing installKey,
  token) }` when the existing config has both; `lean/actor-binding-status.ts`: `actorBindingStatus({ binding, config })` → `{ state ∈
  ok | joined_install_unknown | join_incomplete, joinedInstall, configInstall }` for `/status`; `lean/summary-upload.ts`:
  `partitionFloodRefusal(items, { pairs })` → `{ resubmit, parked }` (the segments whose parts name a refused pair parked, everything
  else resubmitted; it takes a `stamp_from_other_ledger` refusal the same way, round 11); `lean/upload-park.ts` (round 11):
  `partitionRefusedRows(events, { reason, pairs })` → `{ resubmit, parked }` for a delivery refused as `stamp_from_other_ledger`;
  `buffer.ts` gains `parkOutboxRows(ids, reason)`, `parkedRows()` and `releaseParkedRows({ reason: "lineage_linked" } | { reason:
  "admin_release", receipt })` (a parked row is never leased while parked; `lineage_linked` returns it to the outbox; `admin_release`
  acknowledges it `undeliverable_unlinked_ledger` under the receipt, never delivered, never judged); `actorBindingStatus` gains
  `lineage` and `parkedRows`. (B2a; C1, C4)
- `packages/collector-cli/src/upload.ts`: `buildIngestBatch(...).batch.actorBindingVersionHeard` (the current install's version;
  earlier installs' stamps do not raise it) and `.actorBindingInstallHeard` (the ledger's joined install the echo is for, round 10),
  and the pair on every event's metadata; `packages/shared/src/schemas.ts`: `aiWorkIngestBatchSchema` accepts both;
  `analytical-metadata.ts`: `metadataKeyDisposition("actorBindingVersion")` and `("actorBindingInstall")` are identifiers;
  `outbound-envelope.ts`: `sealOutboundEnvelope` keeps both. (B2a)
- `packages/collector-cli/src/lean/runway.ts`: `LEAN_ROW_WIDTHS`; `estimateHostG(census)` (with the optional `segmentProxy`;
  `null` for an unmeasured census); `holdRunway({ freeBytes, reserveBytes, rebuildHeadroomBytes, gGateBytes, rawBytesAtCensus,
  converterWrittenBytes, rawUnfoldedBytesSinceCensus, conversionComplete, leanTableBytesNow?, grossGrowthP95PerDay, rawBytes,
  ledgerBytes })` → `{ runwayDays, gOwedBytes, holdGrowthPerDay, rung ∈ none | converter_paused | release_acked_only | abort }`;
  `censusPreflight({ censusAtMs, catchUpCompleteAtMs, decisionAtMs, freeBytes, ledgerBytes, reserveBytes, duringHold?,
  rebuildHeadroomBytes?, gOwedBytes? })` → `{ ok, reasons ⊆ {census_before_catchup, census_stale, no_space_for_vacuum_into,
  copy_would_trip_rung} }` (round 9: `copy_would_trip_rung` when `duringHold` and `freeBytes − ledgerBytes < reserveBytes +
  rebuildHeadroomBytes + gOwedBytes`). (B1, B10a; C2)
- `packages/collector-cli/src/lean/converter.ts`: `withMeasuredWrite(db, write)` runs `write` in one transaction and returns
  `{ writtenBytes }` = `Δ(page_count − freelist_count) × page_size` across it, the converter's `C_conv` increment, committed with the
  chunk into its checkpoint (round 9; C2).
- `packages/collector-cli/src/lean/day-key.ts`: `utcDayOf`, `censusClass`, `dashboardWindowSince`; `DASHBOARD_SCHEMA_VERSION = 3`. (B5)
- `packages/collector-cli/src/lean/converter.ts`: `convertLedgerHistory(buffer, options)`. (B2a; C3)
- `packages/collector-cli/src/lean/capture-gaps.ts`: `declareUnresolvedFileGap`, `resolveCaptureGap`, `coverageCompleteForPeriod`. (B22; C5)
- `packages/collector-cli/src/lean/retention.ts`: `applyUploadReceipts`, `releaseUnderLadder` (refuses a raw row with an open
  `conversion_rejects` row), `retireSegment`; the raw-delete trigger on `buffered_events` nulls `conversion_rejects.raw_rowid`. (B2a, B10b; C3)
- `packages/collector-cli/src/lean/rebuild.ts`: `abortRebuildBound`. (B13)

**Test premises (round 9, review-r2 blocker 3).** Two pending tests could not pass against a correct implementation for reasons of
their own set-up, not of the rule. `receipts-and-ladder.contract.ts` test 2 rewrote `buffered_events.created_at` after the appends,
which broke the outbox lineage (`upload_outbox.raw_created_at` is copied from it and is immutable, `outbox.ts` trigger
`trg_upload_outbox_lineage_immutable`), so the lease dead-lettered the rows: it now appends the three old rows under a `Date` mocked
at the epoch start (`node:test` `mock.timers`, `apis: ["Date"]`), so `created_at` and the lineage are old together.
`conversion-rejects.contract.ts` test 2 inserted its raw row by SQL, so it had no outbox row and could never be acknowledged: it now
appends the row through the buffer, leases and acknowledges it (the reject row, not the stored timestamp string, is what the
retention rule reads). `helper.contract.ts` gains two green guards for exactly these premises (all four ladder rows lease and three
acknowledge; the reject row is acknowledged and today's prune deletes it at age), beside the eight append guards of round 8.

A bead may rename a surface only by updating its test in the same change; the contract is the behaviour, the name is the handle.

## C7. What the tests do not prove

They are contract tests against shipped code that does not implement the contract, so today every pending test fails for the
reason "surface missing" or "old behaviour", never for a subtle reason; the fixtures in `fixtures/` are the in-memory rule models
that show each rule is self-consistent. The cloud's Postgres proof (`actor-binding-stamp-postgres.contract.test.ts`) fails today at
its missing surface before any cluster starts; once B6 removes its marker it needs the private cluster's binaries
(`PLIMSOLL_PROOF_PG_BIN`) in the lean contract step; its harness was run step by step against today's schema in round 10
(`checks/postgres-harness-repaired-probe.log`: the migrations with `0_init`, the Prisma connection, the tenant row, the erasure child;
only B6's own columns are missing), so the five set-up faults the read found are gone. The R5 interleaving was reproduced in round 9
in a local PostgreSQL 16 cluster at the SQL level (`checks/r5-postgres-reproduction.log`), and the round-10 rules (the facts read before
the lock, the two installs of one ledger, the mirror order, the view under a held lock, PostgreSQL's grant of a `FOR SHARE` while a
`FOR UPDATE` waits, lifecycle, the erasure-only guard) in round 10 (`checks/round10-sql-orderings.log`), and the round-11 rules (a
pair outside the ledger refused with nothing recorded, the hold across an unlinked re-join, the admin link with its re-key, the link
waiting for a sighting) in round 11 on PostgreSQL 17.11 (`checks/round11-sql-orderings.log`: S8 and S9 red as written in round 10,
green under round 11), not through B6's code; the join route was driven through `fixtures/join-route-child.ts` against today's
route on the disposable cluster (`checks/join-route-child-probe.log`: 201 for a fresh token, 409 `used` for a reuse with no second
install; `previousInstall` and `lineage` are absent today, which is where the pending case fails). What
no test can bind is where a future route *calls* `loadSightingView`: the pending proof shows the call itself locks first and returns
a consistent view, and C6 requires the call in the judging transaction; a route that loaded the view earlier would pass the proof
and break C1, so B6's review must read the route. Row widths in C2 remain estimates until S2 measures them on a copy; the Studio4 census
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
parentheses) and case is folded **outside single-quoted literals only** (round 9, review r2: `'usage_live'` → `'USAGE_LIVE'` is a
different predicate and changes the pin; `OR` → `or` is the same predicate and does not; the pinned value is unchanged because the
pinned literals are lower case). Guards (green today): the fixture text hashes to the pin in both repositories; a literal's case
change breaks the pin and a keyword's does not; the cloud's inline loader text hashes to it; the collector's cardinality census
(`fixtures/host_cardinality_census.py`) counts usage rows with it.
Pending: cloud `usage-record-pin.contract.test.ts` (lane 2: `src/lib/economics/usage-record-predicate.ts` exports
`USAGE_RECORD_PREDICATE_SQL` equal to the pin and `loader.ts` uses it; B0's successor re-pins on merge); collector
`usage-record-pin.contract.ts` (B2a: `lean/usage-record.ts` exports `USAGE_RECORD_PREDICATE_SQL` equal to the pin and
`isUsageRecord(row)`, the converter's rule, so the S3 fold and the cloud agree row for row).
