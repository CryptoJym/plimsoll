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
  (g) `heard_at` exists only for an issued version (it lives on the audit row); a never-issued echo's instant is the fact's
      `first_seen_at`.
Modes: --rule r6 (round 6 as written), r7 (round-1 C1 as written, with the round-1 fixture's modelling: deliveries are non-echoing
ingest instants and heard_at(0) is the registration), r8 (round-2 C1/C4 as written), r9 (this rule). Red under r6, r7 and r8,
green under r9.
"""
import itertools
import json
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("b4_offline_rebind", rule)
R8 = rule in ("r8", "r9")          # the round-8 machinery (the pair, the first-sighting fact) is kept by round 9
R9 = rule == "r9"

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

c.finish()
