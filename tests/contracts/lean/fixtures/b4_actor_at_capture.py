#!/usr/bin/env python3
"""B4: round 4 attributed an undelivered (`collector_superset`) member to the install's binding at SUMMARY
INGEST (`actor_basis = 'summary_ingest'`), so a member captured while the install was bound to A can
appear in B's export (VERDICT B4, N09/R08/Q09; reviewer-counterexamples.log 'undelivered A-era member is
exported to B by summary_ingest fallback').

Round-5 rule (ARCHITECTURE.md §5.2 'Binding versions and per-actor parts'): the cloud numbers every binding
change of an install (`binding_version`, in the same transaction as the audit row; historical audit rows
numbered in (changed_at, id) order by the B6 migration); every authenticated response to the collector
carries the install's current `actorBindingVersion`; the collector persists the highest version it has
been told and stamps it on every raw row at admission (`summary_members.actor_binding_version`). Parts:
  raw_ingest          delivered member: the cloud row's own actor_id (unchanged from today, ingest.ts:427)
  binding_at_capture  undelivered member with a stamp V: the actor the audit row for V bound
  single_binding      undelivered member without a stamp on an install whose audit has no change at all
  unallocated_*       everything else: actor_id NULL, listed with its candidate actors, never exported as an actor's
Nothing is attributed by the binding in force when the summary arrives.
"""
import sqlite3
from _common import Checks, H, hexd, sum_digest, rule_arg, MOD

rule = rule_arg()
c = Checks("b4_actor_at_capture", rule)
db = sqlite3.connect(":memory:")
db.executescript("""
-- cloud
create table device_installs(id text primary key, actor_id text, binding_version integer not null default 0);
create table device_install_actor_binding_audits(id text primary key, install_id text not null, binding_version integer not null,
  changed_at integer not null, before_actor_id text, after_actor_id text);
create table ai_interaction_events(id text primary key, install_id text not null, actor_id text, created_at integer not null);
-- collector (what the summary item carries per member: id, delivered?, stamp)
create table summary_members(event_id text primary key, raw_rowid integer not null, actor_binding_version integer, delivered integer not null);
""")
# Install X: registered bound to A (version 0 = the registration binding); an admin binds it to B at cloud time 200 (version 1).
db.execute("insert into device_installs values ('X','A',0)")
db.execute("insert into device_install_actor_binding_audits values ('audit-1','X',1,200,'A','B')")
db.execute("update device_installs set actor_id='B', binding_version=1 where id='X'")
# Install Y: bound to A at registration, never changed (no audit rows).
db.execute("insert into device_installs values ('Y','A',0)")

# Members of one session on install X (local rowid order = capture order):
#  m1 captured at 100 and DELIVERED at 150 (cloud row actor A, bound at ingest)
#  m2 captured at 100, stamp 0 (the collector had been told version 0), delivery DEAD -> undelivered
#  m3 captured at 300 after the collector learned version 1 (stamp 1), withheld locally -> undelivered
#  m4 captured before stamps existed (stamp NULL), undelivered
db.execute("insert into ai_interaction_events values ('m1','X','A',150)")
db.executemany("insert into summary_members values (?,?,?,?)", [("m1", 1, 0, 1), ("m2", 2, 0, 0), ("m3", 3, 1, 0), ("m4", 0, None, 0)])
# The session summary arrives at cloud time 400, when X is bound to B.
SUMMARY_INGEST_AT = 400

def actor_bound_by_version(install_id, version):
    if version == 0:
        row = db.execute("select before_actor_id from device_install_actor_binding_audits where install_id=? order by binding_version limit 1", (install_id,)).fetchone()
        return row[0] if row else db.execute("select actor_id from device_installs where id=?", (install_id,)).fetchone()[0]
    row = db.execute("select after_actor_id from device_install_actor_binding_audits where install_id=? and binding_version=?", (install_id, version)).fetchone()
    return row[0] if row else None

def parts(install_id):
    """Returns {(actor_id, basis): [member ids]} for the session's members."""
    out = {}
    current_actor, current_version = db.execute("select actor_id, binding_version from device_installs where id=?", (install_id,)).fetchone()
    audits = db.execute("select count(*) from device_install_actor_binding_audits where install_id=? and changed_at <= ?", (install_id, SUMMARY_INGEST_AT)).fetchone()[0]
    for event_id, rowid, stamp, delivered in db.execute("select event_id, raw_rowid, actor_binding_version, delivered from summary_members order by raw_rowid"):
        if delivered:
            actor = db.execute("select actor_id from ai_interaction_events where id=?", (event_id,)).fetchone()[0]
            key = (actor, "raw_ingest")
        elif rule == "r4":
            key = (current_actor, "summary_ingest")           # round 4: the binding in force when the summary arrived
        elif stamp is not None:
            if stamp > current_version: key = (None, "unallocated_stamp_invalid")
            else: key = (actor_bound_by_version(install_id, stamp), "binding_at_capture")
        elif audits == 0:
            key = (current_actor, "single_binding")
        else:
            key = (None, "unallocated_no_stamp")
        out.setdefault(key, []).append(event_id)
    return out

p = parts("X")
by_member = {m: k for k, ms in p.items() for m in ms}
c.expect(by_member["m1"] == ("A", "raw_ingest"), "m1 (delivered under A) is A's, basis raw_ingest", str(by_member["m1"]))
c.expect(by_member["m2"][0] == "A", "m2 (captured while bound to A, never delivered) is attributed to A, not to B", str(by_member["m2"]))
c.expect(by_member["m3"][0] == "B", "m3 (captured after the collector learned the B binding) is B's", str(by_member["m3"]))
c.expect(by_member["m4"] == (None, "unallocated_no_stamp"), "m4 (pre-stamp, install with a binding change) is unallocated, attributed to nobody", str(by_member["m4"]))
c.expect(not any(k[1] == "summary_ingest" for k in p), "no part is attributed by the binding in force at summary ingest", str(sorted(p, key=str)))
# exports: A's export holds exactly m1 and m2; B's holds exactly m3; the unallocated part is listed, not exported as anyone's
export = {a: sorted(m for k, ms in p.items() if k[0] == a for m in ms) for a in ("A", "B")}
c.expect(export == {"A": ["m1", "m2"], "B": ["m3"]}, "A's export = {m1, m2}; B's export = {m3}", str(export))
# part digests sum to the segment digest, unallocated part included
seg = sum_digest(["m1", "m2", "m3", "m4"]); parts_sum = sum(sum_digest(ms) for ms in p.values()) % MOD
c.expect(parts_sum == seg, "part digests (unallocated part included) sum to the segment digest")
# a binding change AFTER the summary was ingested moves nothing (parts are sealed with the segment)
frozen = dict(p)
db.execute("insert into device_install_actor_binding_audits values ('audit-2','X',2,500,'B','C')"); db.execute("update device_installs set actor_id='C', binding_version=2 where id='X'")
c.expect(frozen == p, "a binding change after ingest moves no part (parts frozen at the seal)")
# install Y (never rebound): a pre-stamp undelivered member is exactly its single actor
db.execute("delete from summary_members"); db.execute("insert into summary_members values ('y1',1,null,0)")
py = parts("Y")
c.expect(list(py) == [("A", "single_binding")] if rule == "r5" else False, "an install with no binding change attributes a pre-stamp member to its single actor exactly", str(py))
c.finish()
