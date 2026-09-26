#!/usr/bin/env python3
"""B4 residual, round 8 (B0 round 2, review-r1 blocker 1): ONE actor-ownership predicate whose answer does not depend on WHEN a
row is judged, for every stamp, rebind and re-join ordering, including a stamp from a version the cloud had not issued when the
row was stamped; and the corrected request/response timeline (review-r1 should-fixes 1 and 2).

Round 6 (ARCHITECTURE.md r6 §5.2) wrote two validators: a delivered stamp had to satisfy `V exists, V <= actorBindingVersionHeard
<= current`, an undelivered stamp only `V exists, V <= current`. With round 6's WRITTEN acknowledgement boundary (`heard_at` = the
first request that echoes a version; the round-1 fixture's r6 mode wrongly recorded the response instead, review-r1 should-fix 1)
the reviewer's issued-but-unheard row (version 1 supplied by the 300 response, first echoed later; a row stamped 1 delivered at
400) is null on the delivered path and B on the undelivered path (review-r6 blocker 1).

Round 7 (B0 round 1, CONTRACTS.md r7 C1) made `actor_for_stamp(install, stamp)` the only ownership function on both paths, valid
iff `0 <= V <= binding_version` AT JUDGMENT. That still depends on time: a delivered row is judged once at ingest, an undelivered
member when its summary arrives, so a stamp for a version the cloud issues LATER is null on one path and a real actor on the
other (review-r1 blocker 1: stamp 2 delivered at 400 is null, the same row judged in its summary at 1000 is C, a dead delivery
replayed at 700 is C). An honest collector reaches such a stamp on RE-JOIN: the cloud creates a new DeviceInstall per join
(plimsoll-cloud@4954995 src/lib/auth/join-token-store.ts:95-104) and B6 starts it at binding_version 0, while the round-7 B2a
kept the highest version ever persisted in the surviving ledger and never lowered it, so rows captured under the new install's
first actor D were null when delivered and F's (the third rebind) when exported (review-r1 checks/review_c1_orderings.log R2).

Round 8 rule (CONTRACTS.md r8 C1, C4):
  (a) The answer for (install, V) is FIXED AT THE FIRST SIGHTING of V from that install. A sighting is any request echo,
      delivered row or summary member that carries V. If V > binding_version at the first sighting the cloud records the durable
      fact stamp_not_issued(install, V) in the same transaction, and (install, V) is invalid on every path at every later
      judgment, even after the cloud issues V; otherwise V is issued (V = 0, or its audit row exists) and stays valid. Every
      judgment is a sighting, so every judgment of a row stamped V returns the same answer.
  (b) The stamp is the PAIR (install, version): the collector persists a version together with the DeviceInstall id of the
      response that supplied it, stamps both on the identity row and on the wire, and the cloud judges the stamp against the
      install the pair names (an unknown install fails closed). A join, re-join or workspace transition clears the persisted
      pair to null; "never lower" holds within one install only; the join/enrollment response supplies the new install's
      version (0) where the route carries one, else the rows are null-stamped until the first versioned response (C4, the
      disclosed exception). Rows still in the outbox at a re-join keep the old pair and are judged against the old install.
  (c) The echo (`actorBindingVersionHeard`) is a diagnostic and a sighting, never ownership; every request, upload deliveries
      included, echoes max(persisted version, highest stamp carried), so `heard_at(V)` is the first such request: the delivery at
      400 that carries the stamp-1 row is the first echo of 1 (heard_at(1) = 400, not 650) and heard_at(0) = 50 (the first
      request, not the registration response).
Round 9 rule (B0 round 3, review-r2 blockers 1-2 and should-fixes 1, 4; CONTRACTS.md r9 C1, C4):
  (d) The collector accepts a versioned response only from the install its ledger is JOINED with (recorded at join activation,
      `collector_workspace_binding.joined_install`); a response from any other install, e.g. the OLD install's answer to an
      in-flight request or from a process that loaded its config before the re-join (cli.ts:2479), is ignored and counted and
      never replaces the pair (round 8 said "a response from a different install replaces it": review-r2 R4, 3 of 3 rows
      exported as the old install's person).
  (e) A sighting that may record a fact reads `device_installs.binding_version` with the install row locked FOR SHARE (the
      rebind's UPDATE takes the row lock and waits), then inserts the fact ON CONFLICT DO NOTHING and re-reads it, so a rebind
      can never commit between the read and the fact (review-r2 R5: 1 of 12 READ COMMITTED interleavings flipped a pair).
  (f) The fact is keyed by the UPLOADER as well: (uploader_install, install, V); an install's own view is never poisoned by
      another install's traffic naming it (review-r2 R7); a pair naming an install of another tenant records nothing.
  (g) `heard_at` exists only for an issued version (it lives on the audit row); a never-issued echo's instant is the fact's
      `first_seen_at`.
Round 10 rule (B0 round 4, the independent read of C1/C4 after round 9, blockers 1-2 and should-fixes; CONTRACTS.md r10 C1, C4):
  (h) LOCK, THEN READ. A sighting takes the named install's row FOR SHARE FIRST and only then reads binding_version, the audit
      rows and the facts, inside the transaction that records the fact; a view of the facts loaded before the lock is never a
      judgment input. Round 9 locked the read of binding_version but did not say when the facts are read, so a judgment from a
      view of the facts loaded BEFORE the lock (C6's preloaded eventRowsForStorage view) was a permitted order: two first
      sightings of one pair on the ingest and summary paths around the rebind split in 8 of 566 schedules (read c1c4 blocker 1).
  (i) THE FACT IS KEYED BY THE LEDGER: stamp_not_issued(ledger, install, V), where `ledger` is the first install of the
      uploader's chain of installs on one Mac (the cloud records at join, with proof of possession of the previous install's
      key, which install a new install replaced). One ledger uploaded by two installs across a re-join (the old install X judging
      a summary member, the new install Z delivering the row) reads ONE fact; another Mac's install (its own ledger) still never
      enters this ledger's view (round 9's R7 stays closed); a foreign tenant's install still records nothing; a re-join whose
      link could not be verified starts a new ledger and is DISCLOSED. Round 9 keyed the fact by the uploader, so X's fact did
      not bind Z: if X was rebound up to V in between, the row was null in X's summary and someone's when Z delivered it
      (read c1c4 blocker 2).
  (j) A pair is judged against the install it names WHATEVER that install's lifecycle (active, suspended, revoked): lifecycle
      gates authentication, never the audit rows, so revoking the old identity after a re-join turns no pair into null.
Round 11 rule (B0 round 5, the independent read of C1 after round 10, blockers 1-2 and should-fix 4; CONTRACTS.md r11 C1):
  (k) OWNERSHIP NEVER CROSSES A LEDGER. A pair naming an install OUTSIDE the uploader's ledger (another Mac of the tenant, or the
      previous chain of a re-join that could not prove the previous install's key) is never judged: the batch is refused (400
      stamp_from_other_ledger, the pairs listed), nothing is judged and nothing is recorded, and the collector parks the rows (or
      the summary segments) until the ledger is linked, or an admin releases them undelivered, never judged. Round 10 read the
      uploader's ledger's facts (none) and judged the pair against the named install: an ISSUED pair of another Mac gave the
      uploader's row to that Mac's actor (read c1 r10 blocking 1, S8), and an unlinked re-join's delivery of an old faulty row was
      someone's while the old install's summary said nobody (blocking 2, S9).
  (l) THE LINK IS THE RELEASE. The join's verified proof links a ledger at join; an admin may link an unlinked install into a
      ledger later (audited: ledger_linked_at, ledger_linked_by), which re-keys the facts of its old ledger in the same
      transaction, so a pair poisoned before the link stays poisoned after it; the parked rows are then resubmitted and judged
      in that ledger, where they read the same facts and audit rows as the old install's summary did: one answer. The
      first-sighting guarantee is per ledger, and the ledger separation holds while the previous install's key stays secret (a
      copied key passes the possession proof from another Mac).
Round 12 rule (B0 round 6, the independent read of C1 after round 11, its one blocking item; CONTRACTS.md r12 C1):
  (m) THE LINK MOVES A LEDGER, NEVER AN INSTALL. Round 11's admin link moved ONE install (allowed while it was still its own
      ledger) but re-keyed EVERY fact of its old ledger, so an unlinked root U that already had a proof-linked child V could be
      linked alone: V stayed in ledger U while V's not-issued fact went to X, and V's poisoned (V, 1) flipped to the actor of a
      later rebind (the read's CHAIN_COUNTEREXAMPLE on PostgreSQL 17.10); and the link checked nothing about its destination, so
      a non-root destination made a chain with two ledger ids. Round 12: linkLedgerByAdmin takes a source LEDGER (an install that
      is its own ledger) and a destination that is the ROOT of its own ledger in the same tenant, and moves every install whose
      ledger is the source, with the source ledger's facts, in one transaction under one set of locks, recording one audit row
      (who, when, source, destination, installs moved, facts re-keyed) and stamping every moved install; a non-root or foreign
      destination is refused (naming the root); a source that is not a root is refused (a chain is linked whole, once per link);
      links compose (the merged root may be linked on, and the whole chain moves again).
Round 13 rule (B0 round 7, the independent read of C1 after round 12, its two blocking items; CONTRACTS.md r13 C1):
  (n) A DEADLOCKED C1 TRANSACTION IS RETRIED FROM ITS START, AT MOST THREE ATTEMPTS. A sighting that names several installs locks
      them FOR SHARE in ascending id order; the merge locks the two roots FOR UPDATE first and only then the source ledger's
      members: two orders, so a batch naming a member that sorts before its root and the root itself can hold the member and wait
      for the root while the merge holds the root and waits for the member. PostgreSQL detects the cycle and aborts one transaction
      (SQLSTATE 40P01; 40001 where an implementation runs at SERIALIZABLE). Round 12 said nothing about the aborted one: the reader's
      probe saw the merge aborted and the parked rows never released. Round 13: the aborted operation (a sighting on either path,
      the rebind, the join's lineage step, the admin's merge) is rolled back and run again as a fresh transaction from its first
      statement, fresh locks and every input re-read under them, at most 3 attempts in all; nothing is acknowledged, recorded or
      receipted before the transaction that did the work commits; when the third attempt is aborted too the caller sees 503
      transaction_retry_exhausted with Retry-After (a sighting: the collector's ordinary transient retry re-delivers the batch, no
      row parked or acknowledged; the admin's link: nothing moved, no audit row, the rows still parked, the admin re-issues it). A
      retried link that commits releases the parked rows exactly as a first-attempt link does.
  (o) THE 400 NAMES THE LEDGER IT JUDGED. The stamp_from_other_ledger refusal carries ledgerInstallId, the uploader's ledger the
      request was authorized with (read before the sighting's lock), which is the ledger the sighting compared the named install's
      ledger against; the collector parks the refused rows under that wire value, never one it supplied or learned itself, and
      releases them when a response names a different ledger. Round 12 released rows parked "under the ledger the refusing response
      named" but put no such field on the 400, so an implementation could only park under a ledger it learned later, the current
      one, under which a row refused against a stale view (the merge committed between the request's authorization and its lock)
      stays parked on every later response.
Modes: --rule r6 (round 6 as written), r7 (round-1 C1 as written, with the round-1 fixture's modelling: deliveries are non-echoing
ingest instants and heard_at(0) is the registration), r8 (round-2 C1/C4 as written), r9 (round-3 C1/C4 as written: the lock on the
read of binding_version only, the fact keyed by the uploader, no lifecycle rule), r10 (round-4 C1/C4 as written: the ledger key,
a pair judged against the install it names whichever ledger uploads it), r11 (round-5 C1 as written: the admin link moves one
install and re-keys its old ledger's facts, with no destination check), r12 (round-6 C1 as written: no rule for a deadlocked
transaction, and a 400 that names no ledger), r13 (this rule). Red under r6, r7, r8, r9, r10, r11 and r12, green under r13.
"""
import itertools
import json
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("b4_offline_rebind", rule)
R8 = rule in ("r8", "r9", "r10", "r11", "r12", "r13")   # the round-8 machinery (the pair, the first-sighting fact) is kept by rounds 9-13
R9 = rule in ("r9", "r10", "r11", "r12", "r13")         # the round-9 machinery (the joined install, the row lock, the fact scoped to an uploader) is kept by rounds 10-13
R10 = rule in ("r10", "r11", "r12", "r13")      # the round-10 machinery (lock then read, the ledger key, lifecycle) is kept by rounds 11-13
R11 = rule in ("r11", "r12", "r13")             # the round-11 machinery (the refusal across ledgers, the hold, the admin link with its re-key) is kept by rounds 12-13
R12 = rule in ("r12", "r13")                    # the round-12 machinery (the whole-ledger merge into a canonical destination) is kept by round 13
R13 = rule == "r13"
# Round 10: the ledger each install uploads for (the first install of its chain on one Mac). Z re-joined X's Mac with a verified
# link to X; Zu re-joined a Mac whose previous install could not be proved (no link: its own ledger, disclosed); P and Q are other Macs.
LEDGER = {"X": "X", "Y": "Y", "W": "W", "R": "R", "Z": "X", "Zf": "X", "Zu": "Zu", "P": "P", "Q": "Q", "Xr": "Xr"}
def fact_key(uploader, install, v):
    """The key of the durable fact: round 8 the named install; round 9 the uploader as well; round 10 the uploader's LEDGER."""
    if R10: return (LEDGER[uploader], install, v)
    if R9: return (uploader, install, v)
    return (install, v)

# ---- installs: version -> (actor bound, changed_at on the cloud clock); version 0 = the registration or join binding ---------
INSTALLS = {
    "X": {0: ("A", 0), 1: ("B", 200), 2: ("C", 600)},     # registered bound to A; an admin rebinds to B at 200 (v1) and to C at 600 (v2)
    "Y": {0: ("A", 0)},                                     # registered bound to A, never rebound (no audit row)
    "W": {0: ("A", 0), 1: ("B", 200), 2: ("C", 600)},     # X's twin for the FUTURE-STAMP case, so its sighting fact cannot touch X's rows
    "R": {0: ("A", 0), 1: ("B", 200), 2: ("A", 600)},     # A -> B -> A reversal (the reviewer's honest orderings R3)
    "Z": {0: ("D", 800), 1: ("E", 900), 2: ("F", 950)},   # the NEW install the cloud creates when X's Mac re-joins at 800: bound to D by the grant, rebound to E and F
    "Zf": {0: ("D", 800), 1: ("E", 900), 2: ("F", 950)},  # Z's twin for a FAULTY collector that carries X's stale version across the re-join
}
def current_version(at, install): return max(v for v, (_, t) in INSTALLS[install].items() if t <= at)
def current_actor(at, install): return INSTALLS[install][current_version(at, install)][0]
def actor_of_version(v, install): return INSTALLS[install][v][0] if v in INSTALLS[install] else None
def audits(install): return len(INSTALLS[install]) - 1

# ---- the collector side: what each install's collector had PERSISTED (from responses) at each instant --------------------------
# Every request gets a response that supplies the version current at that instant; the collector persists it. Round 8 (C4): the
# persisted version is scoped to the install, so the re-join at 800 clears X's version and the join response supplies Z's 0.
REGISTRATION = {"X": 0, "Y": 0, "W": 0, "R": 0, "Z": 800, "Zf": 800}
HEARTBEATS = {"X": [50, 300, 650], "Y": [50, 300, 650], "W": [50, 300, 650], "R": [50, 300, 650], "Z": [910, 960], "Zf": [910, 960]}
# Deliveries (upload requests) per install, with the case names whose rows they carry; filled from CASES below.
DELIVERIES = {k: {} for k in INSTALLS}
def response_instants(install):
    """Instants at which the collector received a response supplying the current version (round 8: deliveries respond too)."""
    return sorted({REGISTRATION[install], *HEARTBEATS[install], *(DELIVERIES[install] if R8 else [])})
def persisted_version_at(t, install, join_supplies_version=True):
    """Highest version the collector had persisted from THIS install's responses strictly before instant t (its stamp for a row captured at t)."""
    instants = [at for at in response_instants(install) if at < t and (join_supplies_version or at != REGISTRATION[install] or install in ("X", "Y", "W", "R"))]
    return max((current_version(at, install) for at in instants), default=None)
STALE_FROM_X = 2       # the highest version X's collector ever persisted (from the 650 response), which survives the re-join under the round-7 B2a
def stamp_at(capture_at, install, collector="honest", join_supplies_version=True):
    persisted = persisted_version_at(capture_at, install, join_supplies_version)
    if install in ("Z", "Zf") and (not R8 or collector == "faulty"):
        return max(STALE_FROM_X, persisted if persisted is not None else -1)     # round-7 B2a: the highest version ever persisted, never lowered
    return persisted

# ---- cases (install, capture_at, delivered_at, stamp) ------------------------------------------------------------------------
SUMMARY_AT = 1000
CASES = [
    # name, install, capture_at, delivered_at (None = never delivered), stamp ("auto" = what the honest collector had persisted)
    ("A: captured 100 (before the rebind), delivered 150", "X", 100, 150, "auto"),
    ("B: captured 100 (before the rebind), delivered 400 (after the collector persisted v1)", "X", 100, 400, "auto"),
    ("C (reviewer r5): rebind at 200, OFFLINE capture at 250 with stamp 0, delivered 400", "X", 250, 400, "auto"),
    ("D (reviewer r6, issued-but-unheard): captured 350 with stamp 1 (supplied at 300), delivered 400", "X", 350, 400, "auto"),
    ("E: captured 350 (stamp 1), delivered 700 after the second rebind (v2 at 600)", "X", 350, 700, "auto"),
    ("F: captured 250 with stamp 0, never delivered (exported from the summary at 1000)", "X", 250, None, "auto"),
    ("G: a stamp the cloud never issued (5) on a delivered and an undelivered row", "X", 350, 710, 5),
    ("H: a B2a collector's row before its FIRST response (stamp null) on the never-rebound install Y", "Y", 20, 400, None),
]
for name, install, captured, delivered_at, stamp in CASES:
    if delivered_at is not None: DELIVERIES[install].setdefault(delivered_at, []).append(name[0])
DELIVERIES["W"][400] = ["I"]; DELIVERIES["W"][700] = ["I-replay", "I-legit"]          # the future-stamp case and its consequences on W
DELIVERIES["R"].update({400: ["K"], 640: ["K"], 710: ["K"]})
DELIVERIES["Z"].update({850: ["J1"], 980: ["J3"]}); DELIVERIES["Zf"].update({850: ["J1"], 980: ["J3"]})

# ---- the request timeline: echoes, heard_at, diagnostics and (round 8) sightings ---------------------------------------------
SIGHT = {}                     # (install, V) -> the durable first-sighting fact (round 8 only)
def sight(install, v, at, source):
    if v is None or not R8 or (install, v) in SIGHT: return
    SIGHT[(install, v)] = {"first_seen_at": at, "binding_version_then": current_version(at, install), "not_issued": v > current_version(at, install), "source": source}
def stamps_carried(install, at):
    if at not in DELIVERIES[install]: return []                                 # a heartbeat carries no rows
    out = []
    for name, inst, captured, delivered_at, stamp in CASES:
        if inst == install and delivered_at == at: out.append(stamp_at(captured, inst) if stamp == "auto" else stamp)
    if install == "W" and at == 400: out.append(2)                               # case I: the future-version stamp
    if install == "W" and at == 700: out += [2, 2]                               # I replay and the legitimately stamped-2 row
    if install == "R": out.append({400: 0, 640: 1, 710: 2}[at])
    if install in ("Z", "Zf"): out.append(stamp_at({850: 820, 980: 970}[at], install, "faulty" if install == "Zf" else "honest"))
    return [s for s in out if s is not None]
REQUESTS = {k: [] for k in INSTALLS}          # per install: (at, kind, echo)
heard_at = {k: {} for k in INSTALLS}
diagnostics = {k: [] for k in INSTALLS}
for install in INSTALLS:
    if not R8: heard_at[install][0] = REGISTRATION[install]                       # round-1 modelling: the registration response counted as heard
    instants = sorted({*HEARTBEATS[install], *DELIVERIES[install]})
    for at in instants:
        if at in HEARTBEATS[install]:
            echo = persisted_version_at(at, install)
            kind = "heartbeat"
        else:
            kind = "delivery"
            if not R8: continue                                                   # rounds 6-7 (round-1 modelling): a delivery is a non-echoing ingest instant
            carried = stamps_carried(install, at)
            persisted = persisted_version_at(at, install)
            if install == "Zf": persisted = STALE_FROM_X
            echo = max([v for v in [persisted, *carried] if v is not None], default=None)
        if echo is None: continue
        REQUESTS[install].append((at, kind, echo))
        if not R9 or echo <= current_version(at, install):        # round 9 (g): no audit row exists for a never-issued version
            heard_at[install].setdefault(echo, at)
        if R8:
            sight(install, echo, at, "echo")
            diagnostics[install].append({"at": at, "echo": echo, "echo_ahead_of_binding": echo > current_version(at, install),
                                         "stamp_ahead_of_echo": any(s > echo for s in stamps_carried(install, at))})
def acknowledged_version_at(at, install):
    return max((v for v, t in heard_at[install].items() if t <= at), default=0)
print("    X timeline: " + json.dumps(REQUESTS["X"]) + f"; heard_at({rule}) = {heard_at['X']}")

# ---- the predicate ------------------------------------------------------------------------------------------------------------
def actor_for_stamp(stamp, at, path, install="X"):
    """Round 8: ONE function for both paths whose answer for (install, V) is fixed at the first sighting. Round 7: valid iff issued
    at judgment. Round 6 as written: the delivered path also needs V <= the acknowledged version <= current."""
    if stamp is None:                                     # the one disclosed exception: pre-stamp history / pre-first-response rows
        if path == "delivered": return current_actor(at, install), "ingest_current"
        return (INSTALLS[install][0][0], "single_binding") if audits(install) == 0 else (None, "unallocated_no_stamp")
    invalid = (None, "actor_stamp_invalid" if path == "delivered" else "unallocated_stamp_invalid")
    issued_now = stamp in INSTALLS[install] and stamp <= current_version(at, install)
    if R8:
        sight(install, stamp, at, path)                   # every judgment is a sighting
        if SIGHT[(install, stamp)]["not_issued"]: return invalid
        return actor_of_version(stamp, install), "binding_at_capture"
    if rule == "r7":
        return (actor_of_version(stamp, install), "binding_at_capture") if issued_now else invalid
    if path == "delivered":                               # round 6 as written
        if not issued_now or stamp > acknowledged_version_at(at, install): return invalid
        return actor_of_version(stamp, install), "binding_at_capture"
    return (actor_of_version(stamp, install), "binding_at_capture") if issued_now else invalid

def judge(install, capture_at, delivered_at, stamp="auto", collector="honest", join_supplies_version=True):
    st = stamp_at(capture_at, install, collector, join_supplies_version) if stamp == "auto" else stamp
    delivered = actor_for_stamp(st, delivered_at, "delivered", install) if delivered_at is not None else None
    undelivered = actor_for_stamp(st, SUMMARY_AT, "undelivered", install)
    twin = delivered if delivered is not None else actor_for_stamp(st, SUMMARY_AT, "delivered", install)
    return dict(stamp=st, delivered=delivered, undelivered=undelivered, twin=twin)

# ---- 1. the round-7 cases A-H still hold ------------------------------------------------------------------------------------------
results = {}
for name, install, captured, delivered_at, stamp in CASES:
    k = results[name[0]] = judge(install, captured, delivered_at, stamp)
    c.expect(k["twin"][0] == k["undelivered"][0], f"{name}: delivered ingest and undelivered part name the same actor",
             f"stamp={k['stamp']} delivered={k['twin']} undelivered={k['undelivered']}")
D = results["D"]
c.expect(D["twin"][0] == "B" and D["undelivered"][0] == "B", "the issued-but-unheard stamp resolves to the SAME historical actor (B) on both paths, not null on one of them",
         f"delivered={D['twin']} undelivered={D['undelivered']} acknowledged_at_400={acknowledged_version_at(400, 'X')}")
c.expect(not any(k["undelivered"][1] == "summary_ingest" for k in results.values()), "no part is attributed by the binding in force at summary ingest")
first, replay = actor_for_stamp(1, 400, "delivered"), actor_for_stamp(1, 700, "delivered")
c.expect(first[0] == replay[0], "a dead delivery of the stamp-1 row replayed after a later rebind resolves to the same actor as its first attempt", f"first={first} replay={replay}")
G = results["G"]
c.expect(G["twin"] == (None, "actor_stamp_invalid") and G["undelivered"] == (None, "unallocated_stamp_invalid"),
         "a never-issued stamp is unallocated on both paths (actor_stamp_invalid on the raw row, unallocated_stamp_invalid on the part)", f"delivered={G['twin']} undelivered={G['undelivered']}")
pre_d, pre_u = actor_for_stamp(None, 400, "delivered"), actor_for_stamp(None, SUMMARY_AT, "undelivered")
c.expect(pre_d == ("B", "ingest_current") and pre_u == (None, "unallocated_no_stamp"),
         "null-stamp rows on a rebound install keep today's ingest binding when delivered and are unallocated when not: the exception is confined to stamp = null", f"delivered={pre_d} undelivered={pre_u}")

# ---- 2. blocker 1 (review r1): a stamp from a version the cloud had NOT issued when the row was stamped (install W) ----------------
I_del = actor_for_stamp(2, 400, "delivered", "W")            # the cloud issues 2 at 600; the row is delivered at 400
I_replay = actor_for_stamp(2, 700, "delivered", "W")         # a dead delivery of the same row replayed after 2 was issued
I_und = actor_for_stamp(2, SUMMARY_AT, "undelivered", "W")   # the same row judged in its summary at 1000
print("    I (future stamp 2 on W): " + json.dumps({"delivered_400": I_del, "replayed_700": I_replay, "undelivered_1000": I_und, "sighting": SIGHT.get(("W", 2))}))
c.expect(I_del[0] == I_replay[0] == I_und[0], "I: a stamp from a not-yet-issued version gives ONE answer at ingest (400), on replay (700) and on the undelivered path (1000)",
         f"delivered={I_del} replay={I_replay} undelivered={I_und}")
c.expect(I_del == (None, "actor_stamp_invalid") and I_und == (None, "unallocated_stamp_invalid"), "I: that answer is unallocated on both paths (fail closed, disclosed), never C's",
         f"delivered={I_del} undelivered={I_und}")
c.expect(SIGHT.get(("W", 2)) == {"first_seen_at": 400, "binding_version_then": 1, "not_issued": True, "source": "echo"},
         "I: the cloud recorded stamp_not_issued(W, 2) at the first sighting (the 400 request that echoed and carried it, binding_version 1), so no later rebind can flip it", str(SIGHT.get(("W", 2))))
legit_d, legit_u = actor_for_stamp(2, 700, "delivered", "W"), actor_for_stamp(2, SUMMARY_AT, "undelivered", "W")
c.expect(legit_d[0] is None and legit_u[0] is None, "I: after that sighting a row legitimately stamped 2 on W (captured 680, delivered 700) is ALSO unallocated on both paths: (W, 2) is poisoned, disclosed, and repaired only by a rebind to a fresh version",
         f"delivered={legit_d} undelivered={legit_u}")
INSTALLS["W"][3] = ("C", 720)                                                  # the repair: the admin rebinds W to C again as a fresh version 3
fresh_d, fresh_u = actor_for_stamp(3, 800, "delivered", "W"), actor_for_stamp(3, SUMMARY_AT, "undelivered", "W")
c.expect(fresh_d == ("C", "binding_at_capture") and fresh_u == ("C", "binding_at_capture"), "I: rows stamped with the fresh version 3 are C's on both paths (the repair works)", f"delivered={fresh_d} undelivered={fresh_u}")

# ---- 3. blocker 1 (review r1): the RE-JOIN. X's Mac re-joins at 800; the cloud creates install Z (binding_version 0, bound to D). --
# The stamp is the PAIR (install, version): the identity row and the wire carry the install id the version was issued to, so a row
# still in the outbox at the re-join (captured under X's v2 at 700, delivered by Z's credentials at 850) is judged against X.
J0 = judge("X", 700, 850)
c.expect(J0["stamp"] == 2 and J0["twin"] == ("C", "binding_at_capture") and J0["undelivered"] == ("C", "binding_at_capture"),
         "J0: a pre-re-join outbox row keeps its (X, 2) pair and is C's on both paths when Z delivers it at 850, never judged against Z", f"{J0}")
J1 = judge("Z", 820, 850)                                   # captured under D, delivered while Z is still at v0
J3 = judge("Z", 970, 980)                                   # captured after the collector persisted Z's v2 (the 960 response), delivered at 980
print("    J (re-join, honest collector): " + json.dumps({"J1": J1, "J3": J3, "Z_heard_at": heard_at["Z"]}))
c.expect(J1["stamp"] == 0, "J1: after the re-join the honest collector stamps the NEW install's version 0 (the join response supplied it), not X's stale 2", f"stamp={J1['stamp']}")
c.expect(J1["twin"] == ("D", "binding_at_capture") and J1["undelivered"] == ("D", "binding_at_capture"), "J1: rows captured under D are D's on both paths (never null, never F's)", f"delivered={J1['twin']} undelivered={J1['undelivered']}")
c.expect(J3["twin"] == ("F", "binding_at_capture") and J3["undelivered"] == ("F", "binding_at_capture"), "J3: rows captured after the collector persisted Z's v2 are F's on both paths", f"delivered={J3['twin']} undelivered={J3['undelivered']}")
J1n = judge("Z", 820, 850, join_supplies_version=False)     # the join response carries no version (route not upgraded): null until the first versioned response
c.expect(J1n["stamp"] is None and J1n["twin"] == ("D", "ingest_current") and J1n["undelivered"] == (None, "unallocated_no_stamp"),
         "J1': if the join response carries no version the row is null-stamped, i.e. inside the DISCLOSED exception (ingest_current D when delivered, unallocated_no_stamp when not), never a stale stamp", f"{J1n}")
Jf1 = judge("Zf", 820, 850, collector="faulty")             # a faulty collector keeps X's stale 2 across the re-join
Jf_replay = actor_for_stamp(Jf1["stamp"], 990, "delivered", "Zf")
Jf3 = judge("Zf", 970, 980, collector="faulty")
print("    J (re-join, FAULTY collector keeps stale 2): " + json.dumps({"Jf1": Jf1, "replay_990": Jf_replay, "Jf3": Jf3, "sighting": SIGHT.get(("Zf", 2))}))
c.expect(Jf1["stamp"] == 2 and Jf1["twin"][0] is None and Jf1["undelivered"][0] is None and Jf_replay[0] is None,
         "Jf: a stale version carried across the re-join is stamp_not_issued(Z, 2) from its first sighting (850, binding_version 0): unallocated at ingest, on replay at 990 and in the summary at 1000; never F's", f"{Jf1} replay={Jf_replay}")
c.expect(Jf3["twin"][0] is None and Jf3["undelivered"][0] is None, "Jf: Z's legitimate v2 rows are then unallocated too (disclosed; repaired by a fresh rebind), not silently F's", f"{Jf3}")

# ---- 4. honest orderings across an A -> B -> A reversal (the reviewer's R3) keep agreeing ------------------------------------------
K = [(1, 620, 640), (1, 620, None), (2, 700, 710), (0, 250, 400)]
for stamp, captured, delivered_at in K:
    k = judge("R", captured, delivered_at, stamp)
    c.expect(k["twin"][0] == k["undelivered"][0] == actor_of_version(stamp, "R"), f"K: stamp {stamp} captured {captured} delivered {delivered_at} on the A->B->A install names the version's actor on both paths", f"{k}")

# ---- 5. the theorem itself: the answer is independent of the judgment instant ---------------------------------------------------
def answers_over_time(install, stamp, first):
    out = set()
    for at in range(first, 1101, 10):
        for path in ("delivered", "undelivered"):
            out.add(actor_for_stamp(stamp, at, path, install)[0])
    return out
probes = {("X", 1): 400, ("X", 0): 150, ("X", 2): 700, ("X", 5): 710, ("W", 2): 400, ("Zf", 2): 850, ("Z", 0): 850, ("R", 2): 710}
flips = {f"{inst}:{v}": sorted(answers_over_time(inst, v, first), key=str) for (inst, v), first in probes.items()}
print("    answers over time from the first sighting: " + json.dumps(flips))
c.expect(all(len(a) == 1 for a in flips.values()), "for every (install, V) the predicate gives ONE answer at every instant from its first sighting onward, on both paths (independent of when it is judged)",
         json.dumps({k: v for k, v in flips.items() if len(v) != 1}))

# ---- 6. the timeline facts the export discloses (should-fix 2) ----------------------------------------------------------------------
c.expect(heard_at["X"].get(0) == 50, "heard_at(v0) = 50: the first REQUEST that echoes 0, not the registration response at 0", str(heard_at["X"]))
c.expect(heard_at["X"].get(1) == 400, "heard_at(v1) = 400: the delivery at 400 carries the stamp-1 row and echoes max(persisted 1, highest stamp 1) = 1; the 300 response that supplied 1 sets nothing", str(heard_at["X"]))
c.expect(heard_at["X"].get(2) == 700, "heard_at(v2) = 700: the E delivery echoes 2 (supplied by the 650 response)", str(heard_at["X"]))
c.expect([r for r in REQUESTS["X"] if r[0] in (300, 400)] == [(300, "heartbeat", 0), (400, "delivery", 1)], "contact 300 echoes 0 and its response first supplies 1; the 400 delivery is the first request that echoes 1", json.dumps(REQUESTS["X"]))
diag = {d["at"]: d for d in diagnostics["X"]}
c.expect(diag.get(400, {}).get("stamp_ahead_of_echo") is False and diag.get(710, {}).get("echo_ahead_of_binding") is True,
         "diagnostics: the honest 400 delivery never trips stamp_ahead_of_echo; G's delivery echoes 5 > binding_version 2 (echo_ahead_of_binding) and its sighting is stamp_not_issued(X, 5), ownership unchanged", json.dumps(diagnostics["X"]))
note = f"captured under binding v{D['stamp']} at 350, before the cloud heard v{D['stamp']} (heard_at {heard_at['X'].get(D['stamp'])}); rebind changed_at {INSTALLS['X'][1][1]}"
c.expect("heard_at 400" in note and "changed_at 200" in note, "the export names the changed_at -> heard_at window (200 -> 400) for the reviewer's row from stored facts", note)
c.expect(5 not in heard_at["X"] and (not R8 or SIGHT.get(("X", 5), {}).get("first_seen_at") == 710),
         "heard_at exists only for issued versions (it lives on the audit row): the never-issued 5 has no heard_at; its echo instant 710 is the fact's first_seen_at", f"heard_at={heard_at['X']} fact={SIGHT.get(('X', 5))}")

# ---- 7. blocker 1 (review r2): the OLD install's late answer after a re-join must never overwrite the current install's pair ---------
class CollectorPair:
    """C4: the persisted pair. Round 8 as written: a response from a different install replaces it. Round 9: only a response
    from the install the ledger is joined with is accepted; the rest are ignored and counted."""
    def __init__(self): self.pair, self.joined, self.ignored = None, None, 0
    def join(self, install, version):                 # join activation: clears the pair, records the joined install and the handshake's version
        self.pair, self.joined = None, install
        if version is not None: self.pair = (install, version)
    def response(self, install, version):
        if R9 and install != self.joined:
            self.ignored += 1; return "ignored"
        if self.pair is None or self.pair[0] != install:
            self.pair = (install, version); return "replaced"
        if version > self.pair[1]:
            self.pair = (install, version); return "raised"
        return "kept"
    def echo(self): return self.pair[1] if self.pair else None
col = CollectorPair()
col.join("X", 0); col.response("X", 1); col.response("X", 2)          # X's collector reached v2 (the 650 response)
col.join("Z", 0)                                                       # re-join at 800: the grant's install Z, the handshake supplied Z's 0
straggler = col.response("X", 2)                                       # 805: X's answer to an in-flight request (or a stale-config daemon) arrives
L_rows = []
for captured in (810, 850, 890):
    inst, v = col.pair
    d, u = actor_for_stamp(v, captured + 5, "delivered", inst), actor_for_stamp(v, SUMMARY_AT, "undelivered", inst)
    L_rows.append({"captured": captured, "stamp": (inst, v), "delivered": d, "undelivered": u, "truth": current_actor(captured, "Z")})
print("    L (re-join, the old install's late answer): " + json.dumps({"straggler": straggler, "ignored": col.ignored, "rows": L_rows}))
c.expect(straggler == "ignored" and col.ignored == 1, "L: after the re-join a response from the OLD install X is ignored and counted, never accepted (round 8 replaced the pair with (X, 2))", f"straggler={straggler} ignored={col.ignored}")
c.expect(all(r["delivered"][0] == r["undelivered"][0] == r["truth"] for r in L_rows), "L: rows captured under Z's actor D after the late answer are D's on both paths (round 8 exported all three as C's, the wrong person)", json.dumps(L_rows))
c.expect(col.pair == ("Z", 0) and col.echo() == 0, "L: the persisted pair stays (Z, 0) and the echo is the CURRENT install's version 0, not the old install's 2", f"pair={col.pair} echo={col.echo()}")

# ---- 8. blocker 2 (review r2): the first-sighting fact must be serialized against the rebind that issues the version ---------------
# T1: a faulty row stamped (X, 2) is judged: reads binding_version (1), writes the fact, commits. T2: the admin rebind that issues
# v2 and commits. T3: another row stamped (X, 2), judged in one step. Round 8 (READ COMMITTED, no lock): T1 decides from what it
# read. Round 9: T1 reads the install row FOR SHARE, so T2's UPDATE waits until T1 commits; an order that puts T2.commit between
# T1.read and T1.commit is re-serialized with T2.commit after T1.commit (the lock is what the database does).
def run_schedule(order):
    order = list(order)
    serialized = False
    if R9 and order.index("T1.read") < order.index("T2.commit") < order.index("T1.commit"):
        order.remove("T2.commit"); order.insert(order.index("T1.commit") + 1, "T2.commit"); serialized = True
    committed, t1_seen, answers = {"bv": 1, "fact": False}, None, {}
    for step in order:
        if step == "T1.read": t1_seen = dict(committed)
        elif step == "T1.commit":
            if t1_seen["fact"]: answers["T1"] = None
            elif t1_seen["bv"] >= 2: answers["T1"] = "C"
            else: committed["fact"] = True; answers["T1"] = None          # insert ... on conflict do nothing
        elif step == "T2.commit": committed["bv"] = 2
        elif step == "T3":
            if committed["fact"]: answers["T3"] = None
            elif committed["bv"] >= 2: answers["T3"] = "C"
            else: committed["fact"] = True; answers["T3"] = None
    answers["later"] = None if committed["fact"] else ("C" if committed["bv"] >= 2 else None)
    return tuple(order), answers, serialized
schedules = [run_schedule(o) for o in itertools.permutations(["T1.read", "T1.commit", "T2.commit", "T3"]) if o.index("T1.read") < o.index("T1.commit")]
flips = [(o, a) for o, a, _ in schedules if len(set(a.values())) > 1]
serialized = sum(1 for _, _, s in schedules if s)
print("    R5 (sighting vs rebind): " + json.dumps({"orderings": len(schedules), "flips": [[list(o), a] for o, a in flips], "serialized_by_the_row_lock": serialized}))
c.expect(len(schedules) == 12 and not flips, "R5: in all 12 interleavings of a faulty sighting with the rebind that issues its version, every judgment of (X, 2) agrees (round 8 flipped in 1 of 12: T3 exported C, then T1 recorded the fact)", json.dumps([[list(o), a] for o, a in flips]))
c.expect(serialized == 4, "R5: the FOR SHARE read makes the rebind wait in exactly the 4 orderings where it would otherwise commit between the sighting's read and its fact", f"serialized={serialized}")

# ---- 9. should-fix 1 (review r2): the fact is keyed by the uploader, so one install's traffic cannot poison another's version --------
class Cloud:
    def __init__(self, audit): self.audit, self.facts = {k: dict(v) for k, v in audit.items()}, {}
    def bv(self, install, at): return max(v for v, (_, t) in self.audit[install].items() if t <= at)
    def sight(self, install, v, at, uploader, tenant_installs=("P", "Q", "X", "Z", "Zu")):
        if install not in tenant_installs: return (None, "stamp_invalid:unknown_install")   # another tenant's install: judged closed, NO fact
        key = fact_key(uploader, install, v)
        if key in self.facts: return (None, "stamp_invalid:stamp_not_issued")
        if v <= self.bv(install, at): return (self.audit[install][v][0], "binding_at_capture")
        self.facts[key] = {"first_seen_at": at, "binding_version_then": self.bv(install, at)}
        return (None, "stamp_invalid:stamp_not_issued")
cloud = Cloud({"P": {0: ("A", 0)}, "Q": {0: ("A2", 0), 1: ("B2", 200)}})
p_row = cloud.sight("Q", 1, 100, uploader="P")                               # P's collector names (Q, 1) while Q is at v0
q_rows = [cloud.sight("Q", 1, t, uploader="Q") for t in (260, 400)]        # Q's honest rows after Q's rebind to B2 (v1)
foreign = cloud.sight("F", 1, 300, uploader="P")                             # a pair naming an install the tenant does not have
print("    R7 (fact scope): " + json.dumps({"P_row": p_row, "Q_rows": q_rows, "foreign": foreign, "facts": {"|".join(map(str, k)): v for k, v in cloud.facts.items()}}))
c.expect(p_row[0] is None and all(r == ("B2", "binding_at_capture") for r in q_rows), "R7: another install's traffic naming (Q, 1) before Q issues 1 records a fact for THAT uploader only; Q's own rows stamped (Q, 1) after the rebind are B2's (round 8 keyed the fact by the named install and poisoned Q)", f"P={p_row} Q={q_rows}")
c.expect(foreign == (None, "stamp_invalid:unknown_install") and not any(k[-2] == "F" for k in cloud.facts), "R7: a pair naming an install of another tenant fails closed and records no fact", json.dumps({"|".join(map(str, k)): v for k, v in cloud.facts.items()}))

# ---- 10. blocking 1 (read c1c4): a sighting takes the lock FIRST, then reads binding_version, the audit rows and the facts ----------
# Two FIRST sightings of the faulty pair (X, 2) on the two paths at the same instant (Ti: the delivered row at ingest; Ts: its
# member in a summary) around the rebind R that issues 2, as statement-level schedules under READ COMMITTED with PostgreSQL's
# waits: FOR SHARE waits for the rebind's FOR UPDATE (lockInstalls) and the rebind waits for every share lock; an insert of the
# fact waits on another transaction's uncommitted insert of the same key. A step that must wait cannot run until the transaction
# it waits on has committed, so the enumeration visits exactly the schedules the database can produce (the reviewer's model,
# checks/review_c1c4_r9.py section B, re-implemented here from the round-10 text). The rule decides which statement orders a
# sighting MAY use: r6-r8 no lock at all; r9 the lock on the read of binding_version, the facts read before OR after it (the text
# did not say); r10 the lock first, then binding_version and the facts under it.
V2, ACTOR_OF_V2 = 2, "C"
def sighting_steps(name, uploader, order):
    key = fact_key(uploader, "X", V2)
    def lock(s, t):                                   # SELECT ... FOR SHARE: waits while the rebind holds FOR UPDATE; the locked read sees the latest committed version
        if s["U"] not in (None, name): return False
        s["S"].add(name); t["bv"] = s["bv"]; return True
    def read_unlocked(s, t):                          # rounds 6-8: no lock; binding_version and the facts read as committed at that instant
        t["bv"] = s["bv"]; t["fact"] = key in s["facts"]; return True
    def facts(s, t):                                  # the facts as committed when this statement runs
        t["fact"] = key in s["facts"]; return True
    def judge(s, t):
        if t["fact"]: t["answer"] = None; return True
        if V2 <= t["bv"]: t["answer"] = ACTOR_OF_V2; return True
        owner = s["pending"].get(key)
        if owner not in (None, name): return False    # the unique key: wait for the other transaction's uncommitted insert
        if key in s["facts"]: t["answer"] = None; return True    # ON CONFLICT DO NOTHING, re-read: the fact exists
        s["pending"][key] = name; t["answer"] = None; return True
    def commit(s, t):
        s["S"].discard(name)
        for k, owner in list(s["pending"].items()):
            if owner == name: s["facts"].add(k); del s["pending"][k]
        return True
    steps = {"no_lock": [read_unlocked, judge, commit], "lock_then_read": [lock, facts, judge, commit], "facts_then_lock": [facts, lock, judge, commit]}[order]
    return [(f"{name}.{f.__name__}", f) for f in steps]
def rebind_steps(name="R"):
    def lock_for_update(s, t):
        if (s["S"] - {name}) or s["U"] not in (None, name): return False
        s["U"] = name; return True
    def update(s, t): t["new_bv"] = s["bv"] + 1; return True
    def commit(s, t): s["bv"] = t["new_bv"]; s["U"] = None; return True
    return [(f"{name}.{f.__name__}", f) for f in (lock_for_update, update, commit)]
def later_answer(s, uploader):
    if fact_key(uploader, "X", V2) in s["facts"]: return None
    return ACTOR_OF_V2 if V2 <= s["bv"] else None
def enumerate_schedules(txs):
    import copy
    results, deadlocks, names = [], [0], list(txs)
    def dfs(state, pcs, locals_, trace):
        if all(pcs[n] == len(txs[n]) for n in names):
            results.append((tuple(trace), state, locals_)); return
        progressed = False
        for n in names:
            if pcs[n] == len(txs[n]): continue
            label, step = txs[n][pcs[n]]
            s2, l2 = copy.deepcopy(state), copy.deepcopy(locals_)
            if step(s2, l2[n]):
                progressed = True
                p2 = dict(pcs); p2[n] += 1
                dfs(s2, p2, l2, trace + [label])
        if not progressed: deadlocks[0] += 1
    dfs({"bv": 1, "facts": set(), "pending": {}, "S": set(), "U": None}, {n: 0 for n in names}, {n: {} for n in names}, [])
    return results, deadlocks[0]
def run_variant(order, uploaders):
    txs = {"Ti": sighting_steps("Ti", uploaders[0], order), "Ts": sighting_steps("Ts", uploaders[1], order), "R": rebind_steps()}
    results, deadlocks = enumerate_schedules(txs)
    splits, example = 0, None
    for trace, state, loc in results:
        answers = {f"ingest({uploaders[0]})": loc["Ti"]["answer"], f"summary({uploaders[1]})": loc["Ts"]["answer"],
                   f"later({uploaders[0]})": later_answer(state, uploaders[0]), f"later({uploaders[1]})": later_answer(state, uploaders[1])}
        if len(set(answers.values())) > 1:
            splits += 1
            if example is None: example = {"schedule": list(trace), "answers": answers}
    return {"order": order, "uploaders": list(uploaders), "schedules": len(results), "deadlocks": deadlocks, "splits": splits, "example": example}
PERMITTED_ORDERS = {"r9": ["lock_then_read", "facts_then_lock"], "r10": ["lock_then_read"], "r11": ["lock_then_read"], "r12": ["lock_then_read"], "r13": ["lock_then_read"]}.get(rule, ["no_lock"])
same_uploader = {order: run_variant(order, ("X", "X")) for order in ("no_lock", "lock_then_read", "facts_then_lock")}
print("    R5b (two paths, one uploader, statement-level): " + json.dumps({k: {kk: vv for kk, vv in v.items() if kk != "example"} for k, v in same_uploader.items()}))
print("    R5b facts-first example: " + json.dumps(same_uploader["facts_then_lock"]["example"]))
c.expect(same_uploader["facts_then_lock"]["splits"] > 0 and same_uploader["lock_then_read"]["splits"] == 0,
         "R5b: the defect is real: a summary sighting that reads the facts BEFORE taking the lock splits from the ingest sighting of the same pair (the summary reads 'no fact', the ingest records the fact and commits, the rebind commits, the summary locks, reads the issued version and exports C; every later judgment is null), while lock-then-read never splits",
         json.dumps({k: (v["splits"], v["schedules"]) for k, v in same_uploader.items()}))
c.expect(all(same_uploader[o]["splits"] == 0 for o in PERMITTED_ORDERS),
         f"R5b: every statement order the rule permits agrees in every schedule (permitted under {rule}: {PERMITTED_ORDERS}); round 9's text locked only the read of binding_version and so permitted a view of the facts loaded before the lock; round 10 takes the lock first and reads binding_version, the audit rows and the facts under it",
         json.dumps({o: (same_uploader[o]["splits"], same_uploader[o]["schedules"]) for o in PERMITTED_ORDERS}))

# ---- 11. blocking 2 (read c1c4): one ledger uploaded by two installs must read ONE fact -------------------------------------------
# After a re-join the old install X still judges the row's summary member (X's summary) while the new install Z delivers the row
# itself (C1: a pre-re-join outbox row is judged against the old install, whoever delivers it; today's collector batches a
# pre-re-join row with a post-re-join row). The fact must bind both.
two_uploaders = run_variant("lock_then_read", ("Z", "X"))
print("    R10 (two uploaders of one ledger, lock then read): " + json.dumps({k: v for k, v in two_uploaders.items() if k != "example"}) + " example=" + json.dumps(two_uploaders["example"]))
c.expect(two_uploaders["splits"] == 0,
         "R10: two first sightings of the faulty (X, 2) by the two installs of one ledger (Z delivers the row, X judges its member) around the rebind agree in every schedule (round 9 keyed the fact by the uploader: 2 of 142 schedules split, Z null and X C; round 10 keys it by the ledger, so both read one fact)",
         json.dumps({"splits": two_uploaders["splits"], "schedules": two_uploaders["schedules"], "example": two_uploaders["example"]}))
# the same history with no concurrency at all (the reviewer's section C): X's summary judges the faulty (X, 5) at 300 (X at v4);
# the Mac re-joins as Z at 500; an admin rebinds the OLD install X (still valid) to G as version 5 at 600; Z replays the row's
# dead delivery at 700.
cloud_c = Cloud({"X": {0: ("A", 0), 1: ("B", 100), 2: ("C", 150), 3: ("C", 200), 4: ("B", 250)}, "Z": {0: ("D", 500)}})
member_300 = cloud_c.sight("X", 5, 300, uploader="X")
cloud_c.audit["X"][5] = ("G", 600)
replay_700 = cloud_c.sight("X", 5, 700, uploader="Z")
print("    R10 sequential (X summary at 300, re-join at 500, X rebound to G as v5 at 600, Z replays at 700): " + json.dumps({"member": member_300, "replay": replay_700, "facts": {"|".join(map(str, k)): v for k, v in cloud_c.facts.items()}}))
c.expect(member_300[0] is None and replay_700[0] is None and member_300[0] == replay_700[0],
         "R10: the faulty (X, 5) judged null in X's summary at 300 is still null when Z delivers the same row at 700 after the old install was rebound up to 5 (round 9: G's, a person who may never have used the Mac; the fact keyed by the ledger binds every install of the ledger)",
         f"member={member_300} replay={replay_700}")
# the ledger view stays per Mac: P's fact naming (Q, 1) (section 9) is in P's ledger only, and a foreign tenant records nothing (unchanged)
c.expect(fact_key("P", "Q", 1) != fact_key("Q", "Q", 1) and fact_key("Z", "X", 5) == fact_key("X", "X", 5),
         "R10: the key separates Macs and joins installs of one Mac: P's view of (Q, 1) is not Q's (R7 stays closed), Z's view of (X, 5) is X's (one answer across the re-join)",
         f"P={fact_key('P', 'Q', 1)} Q={fact_key('Q', 'Q', 1)} Z={fact_key('Z', 'X', 5)} X={fact_key('X', 'X', 5)}")
# a re-join whose link to the previous install could not be verified starts its own ledger and is disclosed
def join_ledger(new_install, previous, proof_verified):
    """The cloud's join route (round 10): the new install inherits the previous install's ledger only when the join request proves possession of that install's key."""
    if not R10: return None
    linked = previous is not None and proof_verified
    return {"ledger": LEDGER[previous] if linked else new_install, "lineage": "linked" if linked else "unlinked"}
linked, unlinked = join_ledger("Z", "X", True), join_ledger("Zu", "X", False)
print("    R10 join lineage: " + json.dumps({"Z": linked, "Zu": unlinked}))
c.expect(linked == {"ledger": "X", "lineage": "linked"} and unlinked == {"ledger": "Zu", "lineage": "unlinked"},
         "R10: a re-join that proves possession of the previous install's key joins its ledger; one that cannot starts a new ledger and the join receipt says lineage_unlinked (disclosed; its pre-re-join rows are counted as stamp_from_other_ledger on the certify)",
         f"linked={linked} unlinked={unlinked}")

# ---- 12. should-fix (read c1c4): a pair is judged against the install it names whatever that install's lifecycle --------------------
# Xr is X's twin whose identity an admin REVOKES at 900 (the natural clean-up after the re-join at 800). A pre-re-join row stamped
# (Xr, 2) (issued at 600, C) is delivered by Z at 950. Round 10 says lifecycle gates authentication only; round 9's text did not
# say, so a B6 that builds its install view from ACTIVE installs (a natural implementation) turns the pair into 'nobody'.
INSTALLS["Xr"] = dict(INSTALLS["X"])
LIFECYCLE = {"Xr": ("revoked", 900)}
def lifecycle_at(install, at):
    change = LIFECYCLE.get(install)
    return change[0] if change and at >= change[1] else "active"
def actor_for_stamp_lifecycle(stamp, at, path, install):
    if not R10 and lifecycle_at(install, at) != "active":      # rounds <= 9 as permitted: the view holds active installs only
        return (None, "actor_stamp_invalid:unknown_install" if path == "delivered" else "unallocated_stamp_invalid:unknown_install")
    return actor_for_stamp(stamp, at, path, install)
revoked_d, revoked_u = actor_for_stamp_lifecycle(2, 950, "delivered", "Xr"), actor_for_stamp_lifecycle(2, SUMMARY_AT, "undelivered", "Xr")
print("    lifecycle (Xr revoked at 900; (Xr, 2) delivered at 950): " + json.dumps({"delivered": revoked_d, "undelivered": revoked_u}))
c.expect(revoked_d == ("C", "binding_at_capture") and revoked_u == ("C", "binding_at_capture"),
         "lifecycle: a row stamped (Xr, 2), issued at 600, is C's on both paths after Xr's identity is revoked at 900 (round 9's text let an implementation build its view from active installs and turn the pair into null)",
         f"delivered={revoked_d} undelivered={revoked_u}")

# ---- 13. blocking 1 (read c1 r10, S8): a pair naming an install of ANOTHER ledger is never judged ---------------------------------
# P (its own ledger, server-bound actor B) uploads a row stamped (X, 2) after another Mac's install X was rebound to C as version 2.
# Round 10 read the facts of P's ledger (none) and judged the pair against X: issued, so C's: an actor attribution across Macs (the
# reviewer's S8 on PostgreSQL). Round 11: actor_for_stamp refuses a pair whose install is outside the uploader's ledger before any
# judgment: the batch is refused (400 stamp_from_other_ledger, the pairs listed), nothing is judged and nothing is recorded; the
# collector parks the rows until the ledger is linked or an admin releases them (undelivered, never judged).
class Ledgers:
    """The cloud with explicit ledgers. `sight` is actor_for_stamp for a pair uploaded by `uploader`; `link` is the admin's audited
    link (round 11: of an unlinked install into another ledger, re-keying the facts of its old ledger in the same transaction; round
    12: of a whole LEDGER into a root of the same tenant, moving every install of the source ledger with its facts, one audit row).
    `tenant` maps an install to its tenant (one tenant unless given); `linked_by` records how and when each install came to be in a
    ledger other than its own."""
    def __init__(self, audit, ledger, tenant=None):
        self.audit, self.ledger, self.facts, self.refused, self.links = {k: dict(v) for k, v in audit.items()}, dict(ledger), {}, [], []
        self.wire = {}                                                   # the body of the last 400 refusal (round 13: it names the ledger it judged)
        self.tenant = {k: (tenant or {}).get(k, "t1") for k in self.audit}
        self.linked_by = {k: {"by": "join_proof", "at": None} for k in self.audit if self.ledger[k] != k}
    def bv(self, install, at): return max(v for v, (_, t) in self.audit[install].items() if t <= at)
    def key(self, uploader, install, v, view=None):
        if R10: return (view or self.ledger[uploader], install, v)  # rounds 10-13: the uploader's ledger (the one the request was authorized with)
        if R9: return (uploader, install, v)                        # round 9: the uploader
        return (install, v)                                         # round 8: the named install
    def sight(self, install, v, at, uploader, view=None):
        """`view` is the uploader's ledger as the request was authorized with it, read before the sighting's lock (by default the current one;
        round 13, section 17: a merge that commits between that read and the lock leaves it stale, and the refusal is judged against it)."""
        view = view or self.ledger[uploader]
        if install not in self.audit: return (None, "stamp_invalid:unknown_install")            # another tenant: judged closed, no fact
        if R11 and self.ledger[install] != view:                                                 # round 11: outside the uploader's ledger
            self.refused.append((uploader, install, v, at))
            self.wire = {"reason": "stamp_from_other_ledger", "pairs": [[install, v]], **({"ledgerInstallId": view} if R13 else {})}
            return ("held", "refused:stamp_from_other_ledger")
        key = self.key(uploader, install, v, view)
        if key in self.facts: return (None, "stamp_invalid:stamp_not_issued")
        if v <= self.bv(install, at): return (self.audit[install][v][0], "binding_at_capture")
        self.facts[key] = {"first_seen_at": at, "binding_version_then": self.bv(install, at), "recorded_by": uploader}
        return (None, "stamp_invalid:stamp_not_issued")
    def members(self, ledger): return sorted(i for i in self.ledger if self.ledger[i] == ledger)
    def link(self, install, ledger, at, by="admin", rekey=True):
        """Round 11: an UNLINKED install (its own ledger) is linked into `ledger`; its old ledger's facts are re-keyed with it. Round 12:
        `install` names a source LEDGER (it must be its own ledger) and `ledger` a destination that is the ROOT of its own ledger in the
        same tenant; every install of the source ledger moves with the source ledger's facts, one audit row records the link and every
        moved install is stamped. Returns True, or the refusal reason (round 12; False under round 11)."""
        if not R11 or self.ledger[install] != install: return "source_not_root" if R12 else False
        if R12:
            if ledger not in self.ledger or self.tenant[ledger] != self.tenant[install]: return "target_not_found"
            if self.ledger[ledger] != ledger: return f"target_not_root:{self.ledger[ledger]}"      # the refusal names the destination's root
            if ledger == install: return "source_is_target"
        old = install
        moved = self.members(old) if R12 else [install]                                            # round 11 moved the one install only
        for i in moved:
            self.ledger[i] = ledger; self.linked_by[i] = {"by": by, "at": at}
        rekeyed = 0
        if rekey:
            for k in [k for k in self.facts if k[0] == old]: self.facts[(ledger, k[1], k[2])] = self.facts.pop(k); rekeyed += 1
        self.links.append({"source": old, "ledger": ledger, "at": at, "by": by, "installs_moved": len(moved), "facts_rekeyed": rekeyed})
        return True
    def actors_of_ledger(self, ledger):
        return {actor for inst, hist in self.audit.items() if self.ledger[inst] == ledger for actor, _ in hist.values()}
INST11 = {"X": {0: ("A", 0), 1: ("B", 200), 2: ("C", 600)},    # one Mac's first install, rebound to B (v1) and C (v2)
          "P": {0: ("B", 0)},                                    # ANOTHER Mac, server-bound to B (the same person X had at v1)
          "U": {0: ("D", 500)},                                  # X's Mac re-joined at 500 WITHOUT proof of X's key: its own ledger (lineage: unlinked)
          "Zl": {0: ("D", 500)}}                                 # the same re-join WITH proof: X's ledger (lineage: linked)
LEDGER11 = {"X": "X", "P": "P", "U": "U", "Zl": "X"}
cloud13 = Ledgers(INST11, LEDGER11)
s8 = cloud13.sight("X", 2, 700, uploader="P")                                  # P's row stamped (X, 2), delivered at 700, after X issued 2 (C) at 600
p_own = cloud13.sight("P", 0, 710, uploader="P")                               # P's honest row stamped with its own pair
print("    S8 (another Mac's issued pair, uploaded by P bound to B): " + json.dumps({"P_row_X2": s8, "P_own_row": p_own, "facts": {"|".join(map(str, k)): v for k, v in cloud13.facts.items()}, "refused": cloud13.refused}))
c.expect(s8[0] != "C" and s8[0] == "held",
         "S8: a row P uploads stamped (X, 2), another Mac's ISSUED pair, is never C's: the pair names an install outside P's ledger, so the batch is refused (stamp_from_other_ledger) and the row is held, not judged (round 10 read P's empty ledger view, judged the pair against X and gave P's row to C, the person bound to another Mac; P's server-bound actor is B)",
         f"P_row={s8}")
c.expect(not cloud13.facts and p_own == ("B", "binding_at_capture"), "S8: nothing is recorded for the refused pair (no fact in any ledger) and P's own rows are unaffected", f"facts={cloud13.facts} own={p_own}")
def crossings(cloud):
    """Guarantee 5 (round 11): a row uploaded by ledger L is owned by an actor of L's binding history or by nobody, whoever is named and whenever it is judged."""
    out = []
    for uploader in cloud.ledger:
        allowed = cloud.actors_of_ledger(cloud.ledger[uploader]) | {None, "held"}
        for install in cloud.audit:
            for v in cloud.audit[install]:
                for at in range(min(t for _, t in cloud.audit[install].values()), 1001, 50):     # from the install's registration on
                    a = Ledgers(INST11, LEDGER11).sight(install, v, at, uploader)[0]
                    if a not in allowed: out.append({"uploader": uploader, "pair": [install, v], "at": at, "answer": a})
    return out
crossed = crossings(cloud13)
print("    S8 ownership crossings (uploader, pair, instant -> an actor outside the uploader's ledger): " + json.dumps(crossed[:6]) + (" ..." if len(crossed) > 6 else ""))
c.expect(not crossed, "S8/guarantee 5: for every uploader, every pair of the tenant and every instant, the answer is an actor of the uploader's own ledger or nobody: ownership never crosses a ledger (round 10 crossed for every issued pair named across Macs)", f"{len(crossed)} crossings, e.g. {crossed[:2]}")

# ---- 14. blocking 2 (read c1 r10, S9): a re-join without proof of the previous key; the old path and the new path ---------------
# X's summary at 300 judges the faulty (X, 2) (X at v1): not issued, a fact in X's ledger, nobody. At 500 the Mac re-joins WITHOUT
# proof of X's key (the config was lost, or the key had been rotated): U is its own ledger, lineage: unlinked. At 600 an admin
# rebinds the old install X to C as version 2. At 700 U delivers the pre-re-join outbox row stamped (X, 2). Round 10 judged it in
# U's ledger's view (no fact) against X: issued, C's, while the member was nobody's. Round 11 holds it: the pair names an install
# outside U's ledger. At 800 an admin links U into X's ledger (audited); the parked row is resubmitted at 850 and judged in X's
# ledger, where it reads the fact X's summary recorded: nobody, like the member. One row, one answer; the hold is not a judgment.
cloud14 = Ledgers(INST11, LEDGER11)
member_300 = cloud14.sight("X", 2, 300, uploader="X")
honest_member_300 = cloud14.sight("X", 1, 300, uploader="X")                    # an honest pre-re-join row stamped (X, 1): issued, B's
deliver_700 = cloud14.sight("X", 2, 700, uploader="U")
honest_deliver_700 = cloud14.sight("X", 1, 700, uploader="U")
u_own_550 = cloud14.sight("U", 1, 550, uploader="U")                            # U's own faulty pair (U, 1) at U's v0: a fact in U's ledger
linked = cloud14.link("U", "X", 800)
deliver_850 = cloud14.sight("X", 2, 850, uploader="U")
honest_deliver_850 = cloud14.sight("X", 1, 850, uploader="U")
cloud14.audit["U"][1] = ("E", 900)                                               # U is rebound to E as version 1 after the link
u_own_950 = cloud14.sight("U", 1, 950, uploader="U")
print("    S9 (unlinked re-join; the old path, the new path, the link): " + json.dumps({"member_300": member_300, "deliver_700_unlinked": deliver_700, "linked_at_800": linked, "deliver_850_linked": deliver_850,
      "honest": {"member_300": honest_member_300, "deliver_700": honest_deliver_700, "deliver_850": honest_deliver_850}, "U_own_pair": {"sighted_550": u_own_550, "judged_950_after_link_and_rebind": u_own_950},
      "facts": {"|".join(map(str, k)): v["recorded_by"] for k, v in cloud14.facts.items()}, "links": cloud14.links}))
judgments = {a[0] for a in (member_300, deliver_700, deliver_850) if a[0] != "held"}
c.expect(judgments == {None}, "S9: the old faulty row gets ONE answer: nobody in X's summary (the old path), and nobody when the unlinked new install's delivery is finally judged (the new path, after the link); no judgment ever says C (round 10 judged the delivery in the unlinked ledger's empty view and gave the row to C)", f"member={member_300} deliver_700={deliver_700} deliver_850={deliver_850}")
c.expect(deliver_700[0] == "held" and honest_deliver_700[0] == "held", "S9: through an UNLINKED ledger a pre-re-join row is held, not judged (the pair names an install outside that ledger), whether its pair is faulty or honest: the batch is refused as stamp_from_other_ledger and the collector parks the rows (round 10 judged both)", f"faulty={deliver_700} honest={honest_deliver_700}")
c.expect(linked and deliver_850 == member_300 and honest_deliver_850 == honest_member_300 == ("B", "binding_at_capture"), "S9: after the admin links the install into X's ledger, the parked rows are judged in that ledger and agree with X's summary: the faulty pair nobody's, the honest pair B's (the link is the only release into a judgment)", f"linked={linked} faulty={deliver_850} honest={honest_deliver_850}")
c.expect(u_own_950[0] is None and ("X", "U", 1) in cloud14.facts, "S9/link: the link re-keys the facts of the linked ledger, so U's own pair (U, 1), poisoned before the link, stays nobody's after it and after U is rebound up to 1 (without the re-key it would be E's: a flip)", f"judged={u_own_950} facts={list(cloud14.facts)}")
no_rekey = Ledgers(INST11, LEDGER11); no_rekey.sight("U", 1, 550, uploader="U"); no_rekey.link("U", "X", 800, rekey=False); no_rekey.audit["U"][1] = ("E", 900)
print("    S9 counterexample, a link WITHOUT the re-key: (U, 1) judged at 950 -> " + json.dumps(no_rekey.sight("U", 1, 950, uploader="U")) + " (E's under r11 without the re-key: the flip the re-key prevents)")
cloud14b = Ledgers(INST11, LEDGER11)
zl_member, zl_deliver = cloud14b.sight("X", 2, 300, uploader="X"), None
cloud14b.audit["X"][2] = ("C", 600); zl_deliver = cloud14b.sight("X", 2, 700, uploader="Zl")
c.expect(zl_member[0] is None and zl_deliver == zl_member, "S9/linked: the same history through a re-join WITH proof (Zl in X's ledger) is judged at once and agrees, as in round 10: the hold applies only to an unlinked ledger", f"member={zl_member} deliver={zl_deliver}")
copied_key = join_ledger("U2", "X", True)                                        # a copied previous-install key passes the possession proof from any Mac
c.expect(copied_key == {"ledger": "X", "lineage": "linked"} or not R10, "S9/should-fix 4: the ledger separation is per KEY, not per hardware: a join that proves possession of X's key joins X's ledger wherever it runs, so 'another Mac never enters the ledger' holds while the previous install's key stays secret; the first-sighting guarantee is per ledger", f"{copied_key}")

# ---- 15. blocking (read c1 r11): the admin link must move a whole ledger, into a canonical destination -------------------------------
# The read's CHAIN_COUNTEREXAMPLE. X is a Mac's first install (v1 = B at 200). At 500 the Mac re-joins WITHOUT proof of X's key: U is its
# own ledger (lineage: unlinked). At 520 the Mac re-joins again, this time WITH proof of U's key: V joins U's ledger (a proof-linked
# child of the unlinked root). At 550 V sights its own future pair (V, 1) (V at v0): a fact in ledger U, nobody's. At 800 an admin
# links U into X's ledger. Round 11 moved the ONE install U and re-keyed EVERY fact of ledger U, so V stayed in ledger U while V's fact
# went to X: at 900 V is rebound to E as version 1, and at 950 V's own judgment of (V, 1) reads ledger U (no fact): issued, E's: the
# flip fixed-first-sighting ownership forbids; and U's later upload of V's pair was refused as another ledger's. Round 12 moves the
# whole ledger: U and V are both in X, V's fact is X's, (V, 1) stays nobody's, and nothing is stranded.
INST15 = {"X": {0: ("A", 0), 1: ("B", 200)}, "U": {0: ("D", 500)}, "V": {0: ("D", 520)}, "Zl": {0: ("D", 300)},  # Zl: X's proof-linked child (in X)
          "F": {0: ("D", 0)}, "F2": {0: ("D", 0)}, "Y": {0: ("A", 0)}}                                            # F: a fresh root; F2: another TENANT's root; Y: another root
LEDGER15 = {"X": "X", "U": "U", "V": "U", "Zl": "X", "F": "F", "F2": "F2", "Y": "Y"}
cloud15 = Ledgers(INST15, LEDGER15, tenant={"F2": "t2"})
v_own_550 = cloud15.sight("V", 1, 550, uploader="V")                              # V's own future pair at V's v0: a fact in ledger U
direct_child_link = cloud15.link("V", "X", 700)                                    # a member cannot be linked alone (both rounds refuse: V is not its own ledger)
link_800 = cloud15.link("U", "X", 800)
cloud15.audit["V"][1] = ("E", 900)                                                 # V is rebound to E as version 1 after the link
v_own_950 = cloud15.sight("V", 1, 950, uploader="V")                              # V judges its own (V, 1) in its ledger
u_uploads_v_960 = cloud15.sight("V", 1, 960, uploader="U")                        # U delivers a row of the chain stamped (V, 1)
print("    CHAIN (U unlinked root, V its proof-linked child; the admin links U -> X at 800; V rebound to E as 1 at 900): " + json.dumps({"V_own_550": v_own_550, "direct_child_link": direct_child_link, "link_800": link_800, "ledgers_after": {k: cloud15.ledger[k] for k in ("U", "V", "X", "Zl")}, "V_own_950": v_own_950, "U_uploads_V1_960": u_uploads_v_960, "facts": {"|".join(map(str, k)): v["recorded_by"] for k, v in cloud15.facts.items()}, "links": cloud15.links, "linked_by": {k: cloud15.linked_by.get(k) for k in ("U", "V")}}))
c.expect(v_own_550[0] is None and v_own_950[0] is None and direct_child_link in (False, "source_not_root"),
         "CHAIN: V's own pair (V, 1), poisoned in ledger U before the link, is still nobody's after the admin links U into X and V is rebound up to 1 (round 11 moved U alone and re-keyed V's fact away from V's ledger: E's, a flip; the direct link of the child V is refused in both rounds, so round 11 had no way to keep the chain together)",
         f"sighted_550={v_own_550} judged_950={v_own_950} direct_child_link={direct_child_link}")
c.expect(link_800 is True and cloud15.members("U") == [] and {cloud15.ledger[i] for i in ("U", "V")} == {"X"} and not [k for k in cloud15.facts if k[0] == "U"],
         "CHAIN: the link moves the whole ledger: after it no install and no fact is left in ledger U, U and V are both in X (round 11 left V in the emptied ledger U: a stranded member whose facts had moved without it)",
         f"link={link_800} members_of_U={cloud15.members('U')} ledgers={ {k: cloud15.ledger[k] for k in ('U', 'V')} } facts={list(cloud15.facts)}")
c.expect(u_uploads_v_960 == v_own_950 == (None, "stamp_invalid:stamp_not_issued"),
         "CHAIN: after the link U's upload of a chain row stamped (V, 1) is judged in X's ledger, not held, and agrees with V's own judgment: nobody's (round 11: U in X and V in U, so U's upload named an install outside U's ledger and was held for ever)",
         f"U_uploads={u_uploads_v_960} V_own={v_own_950}")
c.expect(len(cloud15.links) == 1 and cloud15.links[0] == {"source": "U", "ledger": "X", "at": 800, "by": "admin", "installs_moved": 2, "facts_rekeyed": 1} and all(cloud15.linked_by.get(i) == {"by": "admin", "at": 800} for i in ("U", "V")),
         "CHAIN/audit: one audit row per link (who, when, the source ledger, the destination, 2 installs moved, 1 fact re-keyed) and every moved install stamped with who and when (round 11 stamped and moved one install)",
         f"links={cloud15.links} linked_by={ {k: cloud15.linked_by.get(k) for k in ('U', 'V')} }")
second_u, second_v = cloud15.link("U", "Y", 970), cloud15.link("V", "Y", 971)
c.expect(second_u in (False, "source_not_root") and second_v in (False, "source_not_root") and {cloud15.ledger[i] for i in ("U", "V")} == {"X"},
         "CHAIN/linked once: after the link neither U nor V is its own ledger, so a second link of either is refused and the chain stays whole in X (both rounds)", f"U={second_u} V={second_v}")
# the destination must be the ROOT of its own ledger, in the same tenant. Round 11 checked only the source: linking a fresh root F
# into Zl (a member of X's ledger) made F's ledger id Zl while Zl's is X: one chain, two ledger ids (the read's NONROOT_TARGET
# counterexample), so F's rows naming X's pairs and X's naming F's were refused as another ledger's for ever.
nonroot = cloud15.link("F", "Zl", 980); f_after_nonroot = cloud15.ledger["F"]
f_names_x = cloud15.sight("X", 1, 985, uploader="F")
foreign = cloud15.link("F", "F2", 986); f_after_foreign = cloud15.ledger["F"]
canonical = cloud15.link("F", "X", 990)
f_names_x_after = cloud15.sight("X", 1, 995, uploader="F")
print("    NON-ROOT DESTINATION (F -> Zl, a member of X's ledger; F -> F2, another tenant's root; F -> X, the root): " + json.dumps({"F_into_Zl": nonroot, "F_ledger_after_it": f_after_nonroot, "Zl_ledger": cloud15.ledger["Zl"], "F_names_X1": f_names_x, "F_into_F2": foreign, "F_ledger_after_it": f_after_foreign, "F_into_X": canonical, "F_ledger_after": cloud15.ledger["F"], "F_names_X1_after": f_names_x_after}))
c.expect(nonroot == "target_not_root:X" and f_after_nonroot == "F",
         "NON-ROOT DESTINATION: a link into an install that is not the root of its own ledger is refused, naming the root the admin should use, and nothing moves (round 11 accepted it: F's ledger id became Zl while Zl's ledger is X, one chain with two ledger ids)",
         f"result={nonroot} F_ledger={f_after_nonroot} Zl_ledger={cloud15.ledger['Zl']}")
c.expect(foreign == "target_not_found" and f_after_foreign == "F",
         "NON-ROOT DESTINATION/tenant: a destination of another tenant is refused as not found and nothing moves (round 11 checked nothing about the destination)", f"result={foreign} F_ledger={f_after_foreign}")
c.expect(canonical is True and cloud15.ledger["F"] == "X" and f_names_x_after == ("B", "binding_at_capture"),
         "NON-ROOT DESTINATION/repair: the same link into the root X succeeds, and F's rows naming X's pairs are judged in X's ledger (under round 11's non-root link they were held for ever: F's ledger id was Zl, not X)",
         f"result={canonical} F_ledger={cloud15.ledger['F']} F_names_X1_after={f_names_x_after}")
# links compose: the merged root X may itself be linked into another root Y later, and the whole chain (X, Zl, U, V, F) moves again
compose = cloud15.link("X", "Y", 1000)
c.expect(compose is True and all(cloud15.ledger[i] == "Y" for i in ("X", "Zl", "U", "V", "F")) and not [k for k in cloud15.facts if k[0] != "Y"] and (cloud15.links[-1]["installs_moved"] if cloud15.links else None) == 5,
         "COMPOSE: linking the merged root X into Y moves every install of X's ledger (X, Zl, U, V, F) with every fact, so a chain never has two ledger ids (round 11 moved X alone: four members stranded in the emptied ledger X)",
         f"result={compose} ledgers={ {i: cloud15.ledger[i] for i in ('X', 'Zl', 'U', 'V', 'F')} } facts={list(cloud15.facts)} moved={(cloud15.links[-1].get('installs_moved') if cloud15.links else None)}")

# ---- 16. blocking 1 (read c1 r12): a two-pair sighting and the merge take their locks in two orders and can deadlock ----------------------
# The sighting locks the installs its pairs name FOR SHARE in ascending id order (C6 loadSightingView); the merge locks the two roots FOR
# UPDATE first and only then the source ledger's members (C1 "The link is the release"): when a member V sorts before its root U, a batch
# naming V and U can hold V and wait for U while the merge U -> X holds U (and X) and waits for V. PostgreSQL detects the cycle and aborts
# one transaction (40P01). Round 12 said nothing about the aborted one (the reader's pg-lock-order.log: the merge aborted, the parked rows
# never released); round 13 retries the aborted transaction from its start, at most 3 attempts, and acknowledges nothing before commit.
# Every statement-level interleaving of the two transactions is enumerated, with EITHER transaction as PostgreSQL's victim at a deadlock.
ORDER16 = {"V": 1, "U": 2, "X": 3}                                  # ascending id: the member V sorts before its root U; X is the destination root
RETRY_CAP = 3 if R13 else None                                       # round 13: at most 3 attempts; rounds 6-12 as written: no rule, the victim stays aborted
def state16():
    return {"ledger": {"V": "U", "U": "U", "X": "X"}, "facts": set(), "audit": [], "share": {r: set() for r in ORDER16}, "update": {r: None for r in ORDER16}}
def lock_share(s, t, row):                                          # SELECT ... FOR SHARE: waits while another transaction holds the row FOR UPDATE
    if s["update"][row] not in (None, t): return False
    s["share"][row].add(t); return True
def lock_update(s, t, row):                                         # SELECT ... FOR UPDATE: waits while any other transaction holds the row
    if (s["share"][row] - {t}) or s["update"][row] not in (None, t): return False
    s["update"][row] = t; return True
def release16(s, t):                                                # commit or rollback: every lock of t released
    for row in ORDER16:
        s["share"][row].discard(t)
        if s["update"][row] == t: s["update"][row] = None
def sighting16(view="U"):
    """V delivers a batch stamped (V, 1) and (U, 1), both at version 0 (not issued); the request was authorized with V's ledger read as `view`."""
    named = sorted(("V", "U"), key=ORDER16.get)
    def lock_first(s, t): return lock_share(s, "S", named[0])
    def lock_second(s, t): return lock_share(s, "S", named[1])
    def read(s, t):                                                 # under both locks: the named installs' ledgers, then the judgment
        if any(s["ledger"][r] != view for r in named): t["answer"] = ("held", view)      # 400 stamp_from_other_ledger, judged against `view`
        else: t["answer"] = ("judged", view); t["facts"] = {(view, r, 1) for r in named}
        return True
    def commit(s, t): s["facts"] |= t.get("facts", set()); release16(s, "S"); t["committed"] = True; return True
    return [("S.lock_" + named[0], lock_first), ("S.lock_" + named[1], lock_second), ("S.read", read), ("S.commit", commit)]
def merge16(source="U", target="X"):
    roots = sorted((source, target), key=ORDER16.get)
    def lock_root_a(s, t): return lock_update(s, "M", roots[0])
    def lock_root_b(s, t): return lock_update(s, "M", roots[1])
    def lock_members(s, t):                                         # a later statement: every install whose ledger is the source, FOR UPDATE, ascending
        for r in sorted((r for r in ORDER16 if s["ledger"][r] == source and r != source), key=ORDER16.get):
            if not lock_update(s, "M", r): return False
        return True
    def commit(s, t):                                               # the moves, the re-key and the audit row are visible at commit only
        moved = [r for r in ORDER16 if s["ledger"][r] == source]
        for r in moved: s["ledger"][r] = target
        s["facts"] = {(target if k[0] == source else k[0], k[1], k[2]) for k in s["facts"]}
        s["audit"].append({"source": source, "target": target, "installs_moved": len(moved)})
        release16(s, "M"); t["committed"] = True; t["moved"] = len(moved); return True
    return [("M.lock_" + roots[0], lock_root_a), ("M.lock_" + roots[1], lock_root_b), ("M.lock_members", lock_members), ("M.commit", commit)]
def enumerate16(retry_cap, handoff=True):
    """`handoff`: a lock the victim releases is granted to the transaction already waiting for it before anyone else runs (PostgreSQL's lock
    manager wakes the waiter at the release and it re-checks the row at once); without it an adversarial scheduler lets the victim's retry
    re-take its first lock before the waiter is granted, which is how the retry cap can be reached at all with two lockers."""
    import copy
    txs = {"S": sighting16(), "M": merge16()}
    runs, deadlocks = [], []
    def dfs(state, pcs, locals_, attempts, trace):
        active = [n for n in txs if pcs[n] < len(txs[n]) and "status" not in locals_[n]]
        if not active:
            runs.append({"trace": trace, "state": state, "locals": locals_, "attempts": attempts}); return
        progressed = False
        for n in active:
            label, step = txs[n][pcs[n]]
            s2, l2 = copy.deepcopy(state), copy.deepcopy(locals_)
            if step(s2, l2[n]):
                progressed = True
                p2 = dict(pcs); p2[n] += 1
                dfs(s2, p2, l2, dict(attempts), trace + [label])
        if progressed: return
        # every active transaction waits on another: a deadlock; PostgreSQL aborts one (40P01), and the model takes each in turn as the victim
        deadlocks.append({"trace": trace, "holds": {n: [r for r in ORDER16 if n in state["share"][r] or state["update"][r] == n] for n in active}})
        for victim in active:
            s2, l2, p2, a2 = copy.deepcopy(state), copy.deepcopy(locals_), dict(pcs), dict(attempts)
            release16(s2, victim); l2[victim] = {}; p2[victim] = 0                        # rolled back: no lock, no fact, no move, no answer survives
            if retry_cap is None: l2[victim] = {"status": "aborted"}                       # rounds 6-12 as written: no rule for the aborted transaction
            elif a2[victim] >= retry_cap: l2[victim] = {"status": "exhausted"}            # the cap: 503 transaction_retry_exhausted
            else: a2[victim] += 1                                                          # round 13: run again from its first statement
            trace2 = trace + [f"DEADLOCK: {victim} aborted (40P01)" + (", retried from its start" if "status" not in l2[victim] else "")]
            if handoff:
                for n in active:                                                            # the waiter is granted the released row at the abort
                    if n != victim and step_of(txs, n, p2)(s2, l2[n]): trace2 = trace2 + [txs[n][p2[n]][0] + " (granted at the abort)"]; p2[n] += 1
            dfs(s2, p2, l2, a2, trace2)
    dfs(state16(), {n: 0 for n in txs}, {n: {} for n in txs}, {n: 1 for n in txs}, [])
    return runs, deadlocks
def step_of(txs, n, pcs): return txs[n][pcs[n]][1]
runs16, deadlocks16 = enumerate16(RETRY_CAP)
runs16_adversarial, _ = enumerate16(RETRY_CAP, handoff=False)
def outcome16(run):
    s, l = run["state"], run["locals"]
    answer = l["S"].get("answer")
    merge_ok = bool(l["M"].get("committed")) and s["ledger"]["V"] == "X" and s["ledger"]["U"] == "X" and len(s["audit"]) == 1 and s["audit"][0]["installs_moved"] == 2
    sight_ok = bool(l["S"].get("committed")) and answer in (("judged", "U"), ("held", "U"))
    keyed = "X" if l["M"].get("committed") else "U"                                   # a committed merge re-keyed the source ledger's facts
    facts_ok = s["facts"] == ({(keyed, "V", 1), (keyed, "U", 1)} if answer == ("judged", "U") and l["S"].get("committed") else set())
    return {"merge": "committed" if l["M"].get("committed") else l["M"].get("status", "incomplete"), "sighting": l["S"].get("status") or (answer[0] if answer else "incomplete"),
            "merge_ok": merge_ok, "sight_ok": sight_ok, "facts_ok": facts_ok, "attempts": run["attempts"], "trace": run["trace"]}
outcomes16 = [outcome16(r) for r in runs16]
adversarial16 = [outcome16(r) for r in runs16_adversarial]
victims16 = {}
for r in runs16:
    for step in r["trace"]:
        if step.startswith("DEADLOCK"): victims16[step] = victims16.get(step, 0) + 1
merge_failed = [o for o in outcomes16 if not o["merge_ok"]]
sight_failed = [o for o in outcomes16 if not (o["sight_ok"] and o["facts_ok"])]
max_attempts = max(max(o["attempts"].values()) for o in outcomes16)
# the two named interleavings: who waits first decides whose deadlock_timeout fires first, i.e. the victim PostgreSQL picks (the SQL log's S15a, S15b)
reader_order = [d for d in deadlocks16 if d["trace"][:1] == ["S.lock_V"]]        # the sighting holds V; the merge holds U and X and waits for V; the sighting asks for U
mirror_order = [d for d in deadlocks16 if d["trace"][:2] == ["M.lock_U", "M.lock_X"]]   # the merge holds the roots; the sighting holds V and waits for U; the merge asks for V
print("    S15 (a two-pair sighting vs the merge, statement-level, either transaction the victim): " + json.dumps({"schedules": len(runs16), "deadlocks": len(deadlocks16), "victims": victims16,
      "merge_not_committed": len(merge_failed), "sighting_not_answered": len(sight_failed), "max_attempts": max_attempts, "reader_order_deadlocks": len(reader_order), "mirror_order_deadlocks": len(mirror_order),
      "example": deadlocks16[0] if deadlocks16 else None, "outcomes": sorted({(o["merge"], o["sighting"]) for o in outcomes16})}))
c.expect(reader_order and mirror_order and all(d["holds"] == {"S": ["V"], "M": ["U", "X"]} for d in deadlocks16) and any("M aborted" in k for k in victims16) and any("S aborted" in k for k in victims16),
         "S15: the defect is real in both interleavings: the sighting (V then U, FOR SHARE) and the merge (the roots U and X FOR UPDATE, then the member V) deadlock with the sighting holding V and the merge holding U and X, whether the sighting or the merge locked first (the reader's pg-lock-order.log), and PostgreSQL may abort either",
         json.dumps({"reader_order": len(reader_order), "mirror_order": len(mirror_order), "holds": deadlocks16[0]["holds"] if deadlocks16 else None, "victims": victims16}))
c.expect(not merge_failed and max_attempts <= 3,
         "S15: in every interleaving, whichever transaction PostgreSQL aborts, the admin's merge commits within the retry cap (at most 3 attempts; 2 are ever needed with two lockers): U and V are in X with one audit row, so the link releases the parked rows (round 12 as written: the aborted merge stays aborted, nothing moves, and the rows it was to release stay parked, as the reader observed)",
         json.dumps({"merge_not_committed": len(merge_failed), "example": (merge_failed[0]["trace"] if merge_failed else None), "max_attempts": max_attempts}))
c.expect(not sight_failed,
         "S15: in every interleaving the sighting is answered by one committed transaction within the cap: judged in U before the merge (its facts then re-keyed to X by the merge) or refused naming U, the ledger the request was authorized with (a stale view: section 17 releases it on the next response naming X); nothing is acknowledged or recorded by an aborted attempt (round 12 as written: the aborted sighting's batch has no rule and no answer)",
         json.dumps({"sighting_not_answered": len(sight_failed), "example": (sight_failed[0]["trace"] if sight_failed else None)}))
# the cap: without PostgreSQL's hand-off (an adversarial scheduler that lets the victim's retry re-take its first lock before the waiter is granted,
# or a third concurrent locker) a transaction can be aborted again; the third abort is the caller's 503, never a silent abort and never a half-result
def ended16(o, tx): return o["merge" if tx == "M" else "sighting"]
exhausted16 = [o for o in adversarial16 if "exhausted" in (o["merge"], o["sighting"])]
unruled16 = [o for o in adversarial16 if "aborted" in (o["merge"], o["sighting"]) or "incomplete" in (o["merge"], o["sighting"])]
half16 = [o for o in adversarial16 if (o["merge"] == "committed") != o["merge_ok"] or (o["sighting"] in ("judged", "held")) != (o["sight_ok"] and o["facts_ok"])]
over16 = [o for o in adversarial16 if any(n > 3 for n in o["attempts"].values()) or ("exhausted" in (o["merge"], o["sighting"]) and 3 not in o["attempts"].values())]
print("    S15 adversarial (no hand-off): " + json.dumps({"schedules": len(adversarial16), "reach_the_cap": len(exhausted16), "no_rule": len(unruled16), "half_results": len(half16), "over_the_cap": len(over16), "outcomes": sorted({(o["merge"], o["sighting"]) for o in adversarial16})}))
c.expect(exhausted16 and not unruled16 and not half16 and not over16,
         "S15/cap: at most 3 attempts: in the adversarial schedules a transaction aborted three times ends as transaction_retry_exhausted (the caller's 503, after exactly 3 attempts) with nothing recorded, moved or acknowledged, and a committed transaction's result is whole; no transaction is ever left aborted without a rule (round 12 as written: every victim is)",
         json.dumps({"reach_the_cap": len(exhausted16), "no_rule": len(unruled16), "half_results": len(half16), "over_the_cap": len(over16), "example": (unruled16 or half16 or over16 or [{"trace": None}])[0]["trace"]}))

# ---- 17. blocking 2 (read c1 r12): the 400 refusal names the ledger it judged, and the collector parks under that wire value ---------------
# The uploader's ledger is read for the request's authorization, before the sighting's lock; a merge U -> X can commit in between, so the
# sighting reads the named install's ledger as X, compares it with U and refuses (the reader's MERGE_THEN_SIGHTING). Round 12 released a
# parked row when a later response named a ledger OTHER than the one the row was parked under, but its 400 carried reason and pairs only:
# no wire field named the ledger the refusal was judged against, so the only ledger a collector could park under was one it learned later,
# the current ledger X, under which the row stays parked on every later X response. Round 13: the 400 carries ledgerInstallId, the
# uploader's ledger the refusal was judged against (U); the collector parks under that wire value and the next response naming X
# releases the row, which is resubmitted, judged in X, and agrees with every other judgment of its pair.
class ParkedRows:
    """The collector's park (C6 parkOutboxRows / releaseParkedRows). Round 13: a row is parked under the ledger the 400 named. Rounds 11-12 as
    written: the 400 names no ledger, so the model fills the missing value from the next authenticated response, the current ledger: a
    reading the round-12 text permits, since it names no source for the value."""
    def __init__(self): self.rows = []
    def park(self, row_id, body): self.rows.append([row_id, body.get("ledgerInstallId")])
    def on_response(self, ledger):                                      # every acknowledged response names the install's ledger (round 12)
        for row in self.rows:
            if row[1] is None: row[1] = ledger
        released = [row[0] for row in self.rows if row[1] != ledger]    # released: parked under a ledger other than the one the response names
        self.rows = [row for row in self.rows if row[1] == ledger]
        return released
INST17 = {"X": {0: ("A", 0), 1: ("B", 200)}, "U": {0: ("D", 500)}, "V": {0: ("D", 520)}, "P": {0: ("B", 0)}}   # U: unlinked root; V: its proof-linked child; P: another Mac
LEDGER17 = {"X": "X", "U": "U", "V": "U", "P": "P"}
cloud17 = Ledgers(INST17, LEDGER17)
view_700 = cloud17.ledger["V"]                                          # 700: V's delivery is authorized; the uploader's ledger is read as U
link_701 = cloud17.link("U", "X", 701)                                  # 701: the admin merges U into X before the sighting locks V
refused_702 = cloud17.sight("V", 1, 702, uploader="V", view=view_700)   # 702: the sighting locks V, reads its ledger (X now), compares with U: refused
body_702 = dict(cloud17.wire)                                           # the 400 body on the wire
park17 = ParkedRows(); park17.park("row-v1", body_702)
released_720 = park17.on_response(cloud17.ledger["V"])                  # 720: the next acknowledged response names V's ledger, X
retry_730 = cloud17.sight("V", 1, 730, uploader="V") if released_720 else None   # the resubmission is authorized with V's current ledger, X
released_800 = park17.on_response(cloud17.ledger["V"])                  # 800: another response naming X
print("    S16 (the refusal's ledger; the merge committed between authorization and the lock): " + json.dumps({"view_at_authorization": view_700, "link_701": link_701, "refused_702": refused_702, "wire_body": body_702,
      "parked_under": [r[1] for r in park17.rows] or "released", "released_by_the_720_response": released_720, "retry_730": retry_730, "released_by_the_800_response": released_800, "still_parked": park17.rows, "facts": sorted("|".join(map(str, k)) for k in cloud17.facts)}))
c.expect(refused_702[0] == "held" and body_702.get("ledgerInstallId") == "U",
         "S16: the 400 stamp_from_other_ledger carries ledgerInstallId, the uploader's ledger the refusal was judged against (U, the request's authorization view, not X, the current ledger): the collector parks from the wire value (round 12: the body carried reason and pairs only, and the test supplied the ledger by hand)",
         f"refused={refused_702} body={body_702}")
c.expect(released_720 == ["row-v1"] and retry_730 == (None, "stamp_invalid:stamp_not_issued") and ("X", "V", 1) in cloud17.facts and not park17.rows and released_800 == [],
         "S16: parked under the refusal's ledger U, the row is released by the first response naming X and judged in X (nobody's: its pair was not issued; one answer with every later judgment); parked under the current ledger X, the only value round 12's text let a collector find, it stays parked on the 720 response and on every later X response",
         f"released_720={released_720} retry_730={retry_730} still_parked={park17.rows} released_800={released_800}")
p_refused = cloud17.sight("X", 1, 810, uploader="P"); p_body = dict(cloud17.wire); park_p = ParkedRows(); park_p.park("row-p", p_body)
p_same = park_p.on_response(cloud17.ledger["P"]); p_same_again = park_p.on_response(cloud17.ledger["P"])
p_link = cloud17.link("P", "X", 900); p_released = park_p.on_response(cloud17.ledger["P"]); p_retry = cloud17.sight("X", 1, 910, uploader="P") if p_released else None
c.expect(p_refused[0] == "held" and p_same == [] and p_same_again == [] and p_released == ["row-p"] and p_retry == ("B", "binding_at_capture"),
         "S16: the release compares two server-supplied values: a response naming the ledger the refusal named (P's, unchanged) releases nothing, and the response after the admin links P into X names X and releases the row, which is then judged in X (B's, like X's own judgment of (X, 1))",
         f"refused={p_refused} same={p_same},{p_same_again} link={p_link} released={p_released} retry={p_retry}")
c.finish()
