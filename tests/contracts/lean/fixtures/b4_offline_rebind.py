#!/usr/bin/env python3
"""B4 residual, round 7 (B0 blocker 1, review-r6 `blocking[0]`): ONE actor-ownership predicate for a delivered raw row and
an undelivered summary member, and the corrected request/response timeline.

Round 6 wrote two validators (ARCHITECTURE.md r6 §5.2): a delivered stamp had to satisfy `V exists, V <= actorBindingVersionHeard
<= current`, an undelivered stamp only `V exists, V <= current` (plus a rowid-monotonic list check). For a version the cloud has
ISSUED but the collector has not yet ECHOED (version 1 supplied by the 300 response, first echoed by the 650 request), a row
stamped 1 and delivered at 400 was null on the delivered path and B on the undelivered path (reviewer-new-counterexamples.log:
actor_valid_but_unheard). The round-6 fixture also recorded a response's version as heard at that same contact (heard_at(1)=300)
although the first request that echoes 1 is at 650 (`shouldFix[0]`).

Round-7 rule (CONTRACTS.md C1; ARCHITECTURE.md r7 §5.2 "One actor predicate"): `actor_for_stamp(install, stamp)` is the only
ownership function on BOTH paths: a stamped row belongs to the actor its version bound (`actor_of(install, V)`) iff V is a
version the cloud issued for that install (0 <= V <= current binding version at judgment); otherwise it is unallocated on both
paths (`actor_id = null`, `actorStampInvalid` on the raw row, `unallocated_stamp_invalid` on the part). The echo
(`actorBindingVersionHeard`) and the rowid-monotonic list are DIAGNOSTICS (`stamp_ahead_of_echo`, `stamp_sequence_regressed`),
disclosed and never ownership. `heard_at` is the first REQUEST that echoes a version, a disclosure fact for the export. A null
stamp (a collector older than B2a, or a B2a collector before its first response) keeps the one disclosed exception.
"""
import json
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("b4_offline_rebind", rule)

# Install X: registered bound to A (version 0); an admin rebinds it to B at cloud time 200 (version 1) and to C at 600 (version 2).
AUDIT = {0: ("A", 0), 1: ("B", 200), 2: ("C", 600)}      # version -> (actor bound, changed_at); 0 = the registration binding
# Install Y: registered bound to A and never rebound (no audit row at all).
INSTALLS = {"X": AUDIT, "Y": {0: ("A", 0)}}
def current_version(at, install="X"): return max(v for v, (_, t) in INSTALLS[install].items() if t <= at)
def current_actor(at, install="X"): return INSTALLS[install][current_version(at, install)][0]
def actor_of_version(v, install="X"): return INSTALLS[install][v][0] if v in INSTALLS[install] else None

# --- the explicit request/response timeline (shouldFix[0]) ---------------------------------------------------------------
# Every contact is one REQUEST (echoing the highest version the collector has PERSISTED, which is the version supplied by the
# last response before it) and one RESPONSE (supplying the version current at that instant). The scenario deliveries below are
# modelled as ingest instants on the cloud clock; they add no echoing request of their own, so `heard_at` comes from this list
# alone (the reviewer's reading: contact 300 echoes 0 and first supplies 1; contact 650 first echoes 1 and supplies 2).
REGISTRATION_AT = 0
CONTACTS = [50, 300, 650]
persisted = {REGISTRATION_AT: 0}                          # instant -> version the collector persisted from that response
def persisted_version_at(t):
    """Highest version the collector had persisted strictly before instant t (= its stamp for a row captured at t)."""
    return max(v for at, v in persisted.items() if at < t)
timeline, heard_at_echo, heard_at_response = [], {0: REGISTRATION_AT}, {0: REGISTRATION_AT}
for ct in CONTACTS:
    echo, supplied = persisted_version_at(ct), current_version(ct)
    heard_at_echo.setdefault(echo, ct)                    # round 7: the cloud records the first request that ECHOES a version
    heard_at_response.setdefault(supplied, ct)            # round 6 as written: the version a response SUPPLIED was recorded as heard there
    persisted[ct] = supplied
    timeline.append({"contact": ct, "request_echo": echo, "response_version": supplied})
heard_at = heard_at_echo if rule == "r7" else heard_at_response
def acknowledged_version_at(t):
    """Highest version whose heard_at is at or before t (the cloud-stored acknowledgement boundary)."""
    return max(v for v, at in heard_at.items() if at <= t)
def stamp_at(capture_at): return persisted_version_at(capture_at)
print("    timeline: " + json.dumps(timeline) + f"; heard_at({rule}) = {heard_at}")

# --- the predicate(s) ------------------------------------------------------------------------------------------------------
def actor_for_stamp(stamp, at, path, install="X"):
    """Round 7: ONE function for both paths. Round 6: the two written validators (delivered needs V <= acknowledged <= current)."""
    audits = len(INSTALLS[install]) - 1                    # audit rows = versions after the registration binding
    if stamp is None:                                     # the one disclosed exception: pre-stamp history / pre-first-response rows
        if path == "delivered": return current_actor(at, install), "ingest_current"
        return (INSTALLS[install][0][0], "single_binding") if audits == 0 else (None, "unallocated_no_stamp")
    issued = stamp in INSTALLS[install] and stamp <= current_version(at, install)
    if rule == "r7":
        if not issued: return None, ("actor_stamp_invalid" if path == "delivered" else "unallocated_stamp_invalid")
        return actor_of_version(stamp, install), "binding_at_capture"
    # round 6 as written
    if path == "delivered":
        if not issued or stamp > acknowledged_version_at(at): return None, "actor_stamp_invalid"
        return actor_of_version(stamp, install), "binding_at_capture"
    if not issued: return None, "unallocated_stamp_invalid"
    return actor_of_version(stamp, install), "binding_at_capture"

SUMMARY_AT = 1000
def scenario(name, capture_at, delivered_at, stamp="auto", install="X"):
    st = stamp_at(capture_at) if stamp == "auto" else stamp
    undelivered = actor_for_stamp(st, SUMMARY_AT, "undelivered", install)
    delivered = actor_for_stamp(st, delivered_at, "delivered", install) if delivered_at is not None else None
    twin = delivered if delivered is not None else actor_for_stamp(st, SUMMARY_AT, "delivered", install)
    return dict(name=name, capture_at=capture_at, stamp=st, delivered_at=delivered_at, delivered=delivered, undelivered=undelivered, twin=twin)

CASES = [
    scenario("A: captured 100 (before the rebind), delivered 150", 100, 150),
    scenario("B: captured 100 (before the rebind), delivered 400 (after the collector persisted v1)", 100, 400),
    scenario("C (reviewer r5): rebind at 200, OFFLINE capture at 250 with stamp 0, delivered 400", 250, 400),
    scenario("D (reviewer r6, issued-but-unheard): captured 350 with stamp 1 (supplied at 300, first echoed at 650), delivered 400", 350, 400),
    scenario("E: captured 350 (stamp 1), delivered 700 after the second rebind (v2 at 600)", 350, 700),
    scenario("F: captured 250 with stamp 0, never delivered (exported from the summary at 1000)", 250, None),
    scenario("G: a stamp the cloud never issued (5) on a delivered and an undelivered row", 350, 700, stamp=5),
    scenario("H: a B2a collector's row before its FIRST response (stamp null) on the never-rebound install Y", 20, 400, stamp=None, install="Y"),
]
for k in CASES:
    same = k["twin"][0] == k["undelivered"][0]
    c.expect(same, f"{k['name']}: delivered ingest and undelivered part name the same actor",
             f"stamp={k['stamp']} delivered={k['twin']} undelivered={k['undelivered']}")
D = CASES[3]
print("    " + json.dumps({"actor_valid_but_unheard": {"stamp": D["stamp"], "acknowledged_at_delivery": acknowledged_version_at(400) if rule == "r7" else acknowledged_version_at(400),
                                                       "current_version": current_version(400), "delivered_actor": D["twin"][0], "undelivered_actor": D["undelivered"][0]}}))
c.expect(D["twin"][0] == "B" and D["undelivered"][0] == "B", "the issued-but-unheard stamp resolves to the SAME historical actor (B) on both paths, not null on one of them",
         f"delivered={D['twin']} undelivered={D['undelivered']}")
c.expect(not any(k["undelivered"][1] == "summary_ingest" for k in CASES), "no part is attributed by the binding in force at summary ingest")
# replay stability: a dead delivery replayed after a later rebind (and after the version was finally echoed) binds as its first attempt would have
first, replay = actor_for_stamp(1, 400, "delivered"), actor_for_stamp(1, 700, "delivered")
c.expect(first[0] == replay[0], "a dead delivery replayed after a later rebind resolves to the same actor as its first attempt", f"first={first} replay={replay}")
# invalid stamp: fails closed on both paths, disclosed with a candidate set, never exported as anyone's
G = CASES[6]
c.expect(G["twin"] == (None, "actor_stamp_invalid") and G["undelivered"] == (None, "unallocated_stamp_invalid"),
         "a never-issued stamp is unallocated on both paths (actor_stamp_invalid on the raw row, unallocated_stamp_invalid on the part)", f"delivered={G['twin']} undelivered={G['undelivered']}")
# the one disclosed exception: stamp null on a REBOUND install (pre-stamp history or a pre-first-response B2a row on such an install)
pre_d, pre_u = actor_for_stamp(None, 400, "delivered"), actor_for_stamp(None, SUMMARY_AT, "undelivered")
c.expect(pre_d == ("B", "ingest_current") and pre_u == (None, "unallocated_no_stamp"),
         "null-stamp rows on a rebound install keep today's ingest binding when delivered and are unallocated when not: the exception is confined to stamp = null", f"delivered={pre_d} undelivered={pre_u}")
# the timeline facts the export discloses (shouldFix[0]): heard_at is the first ECHO, version 2 is supplied but not yet heard
c.expect(heard_at.get(1) == 650 and 2 not in heard_at, "heard_at(v1) = 650 (the first request that echoes 1), not 300 (the response that supplied it); v2, supplied at 650, has no heard_at yet", str(heard_at))
c.expect(timeline[1] == {"contact": 300, "request_echo": 0, "response_version": 1} and timeline[2] == {"contact": 650, "request_echo": 1, "response_version": 2},
         "contact 300 echoes 0 and first supplies 1; contact 650 first echoes 1 and supplies 2", json.dumps(timeline))
note = f"captured under binding v{D['stamp']} at {D['capture_at']}, before the collector acknowledged v{D['stamp']} (heard_at {heard_at.get(D['stamp'])}); rebind changed_at {AUDIT[1][1]}"
c.expect("heard_at 650" in note and "changed_at 200" in note, "the export names the changed_at -> heard_at window for the reviewer's row from stored facts", note)
c.finish()
