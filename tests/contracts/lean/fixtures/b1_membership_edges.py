#!/usr/bin/env python3
"""B1: one membership row per (event_id, raw_generation) cannot record the session AND day targets that the
usage deletion proof needs, and a sessionless usage row has no session target at all (VERDICT B1, Q06;
reviewer-counterexamples.log 'one event cannot be member of session and day targets').

Round-5 rule (ARCHITECTURE.md §2.2, §3.3): membership is an identity row in `summary_members` plus one
`summary_member_edges` row per covering target. Every raw row has exactly two covering targets: its BASE
target (the session segment when it carries a session id, else the hourly rollup bucket segment) and its
DAY target (one per epoch, source and UTC day; the parent of the three day-fact tables). The deletion
proof walks every edge and refuses while any covering target is open or unacknowledged.
"""
import sqlite3
from _common import Checks, H, hexd, sum_digest, digest16, rule_arg

rule = rule_arg()
c = Checks("b1_membership_edges", rule)
db = sqlite3.connect(":memory:")
db.execute("pragma foreign_keys = on")
db.executescript("""
create table buffered_events(id text primary key, session_id text, is_usage integer not null, payload text not null);
create table summary_segments(
  target_ref integer primary key autoincrement,
  target_kind text not null check (target_kind in ('session','rollup','day')),
  base_key text not null, segment_seq integer not null default 1,
  state text not null default 'open' check (state in ('open','sealed','retired')),
  member_count integer not null default 0, live_member_count integer not null default 0, tombstone_count integer not null default 0,
  member_sum_digest text not null default '00000000000000000000000000000000',
  residue_sum_digest text not null default '00000000000000000000000000000000',
  sealed_revision integer, acked_revision integer, acked_member_sum_digest text, post_seal_mutations integer not null default 0,
  unique (target_kind, base_key, segment_seq));
""")
if rule == "r4":
    # ARCHITECTURE.md round 4 §3.3: one target per (event_id, raw_generation).
    db.executescript("""
    create table summary_membership(
      event_id text not null, raw_generation text not null default '', raw_rowid integer,
      target_ref integer not null references summary_segments(target_ref), folded_revision integer not null,
      state text not null default 'live' check (state in ('live','tombstone')), payload_digest16 blob not null,
      deleted_at_ms integer, readmissions integer not null default 0,
      primary key (event_id, raw_generation));
    """)
else:
    # ARCHITECTURE.md round 5 §3.3: identity row + one edge per covering target.
    db.executescript("""
    create table summary_members(
      event_id text not null, raw_generation text not null default '', raw_rowid integer,
      row_class text not null check (row_class in ('usage_session','usage_sessionless','activity_session','activity_sessionless')),
      state text not null default 'live' check (state in ('live','tombstone')), payload_digest16 blob not null,
      actor_binding_version integer, deleted_at_ms integer, readmissions integer not null default 0,
      primary key (event_id, raw_generation));
    create unique index idx_smm_live_rowid on summary_members(raw_rowid) where raw_rowid is not null;
    create table summary_member_edges(
      event_id text not null, raw_generation text not null default '',
      target_ref integer not null references summary_segments(target_ref), folded_revision integer not null,
      primary key (event_id, raw_generation, target_ref),
      foreign key (event_id, raw_generation) references summary_members(event_id, raw_generation)) without rowid;
    create index idx_sme_target on summary_member_edges(target_ref, event_id);
    """)

def segment(kind, base_key):
    db.execute("insert into summary_segments(target_kind, base_key) values (?,?)", (kind, base_key))
    return db.execute("select target_ref from summary_segments where target_kind=? and base_key=?", (kind, base_key)).fetchone()[0]

EPOCH, SRC, DAY = "epoch-1", "codex", "2026-09-25"
S = segment("session", f"{EPOCH}|{SRC}|sess-1")          # base target for rows with a session id
B = segment("rollup", f"{EPOCH}|{SRC}|2026-09-25T14|m:gpt|otel_span|mach")   # base target for sessionless rows
D = segment("day", f"{EPOCH}|{SRC}|{DAY}")               # day target: parent of session_day/model_day/activity_day rows
KIND = {S: "session", B: "rollup", D: "day"}

REQUIRED = {  # ARCHITECTURE.md §3.3 "Covering targets by row class"
    "usage_session": {"session", "day"}, "usage_sessionless": {"rollup", "day"},
    "activity_session": {"session", "day"}, "activity_sessionless": {"rollup", "day"},
}
def row_class(is_usage, session_id):
    return ("usage" if is_usage else "activity") + ("_session" if session_id else "_sessionless")

recorded = {}   # event_id -> set of target kinds the membership tables hold
def fold(event_id, session_id, is_usage, payload):
    db.execute("insert into buffered_events values (?,?,?,?)", (event_id, session_id, is_usage, payload))
    rowid = db.execute("select rowid from buffered_events where id=?", (event_id,)).fetchone()[0]
    targets = [S if session_id else B, D]
    kinds = set()
    if rule == "r4":
        for t in targets:
            try:
                db.execute("insert into summary_membership(event_id, raw_rowid, target_ref, folded_revision, payload_digest16) values (?,?,?,1,?)",
                           (event_id, rowid, t, digest16(payload)))
                kinds.add(KIND[t])
            except sqlite3.IntegrityError as exc:   # the reviewer's counterexample: UNIQUE constraint failed
                print(f"    r4: second membership for {event_id} refused: {exc}")
    else:
        db.execute("insert into summary_members(event_id, raw_rowid, row_class, payload_digest16) values (?,?,?,?)",
                   (event_id, rowid, row_class(is_usage, session_id), digest16(payload)))
        for t in targets:
            db.execute("insert into summary_member_edges(event_id, target_ref, folded_revision) values (?,?,1)", (event_id, t))
            kinds.add(KIND[t])
    for t in targets if rule == "r5" else [t for t in targets if KIND[t] in kinds]:
        db.execute("update summary_segments set member_count=member_count+1, live_member_count=live_member_count+1 where target_ref=?", (t,))
    recorded[event_id] = kinds

fold("u1", "sess-1", 1, "usage with a session")
fold("u2", None, 1, "sessionless usage (an amount-bearing otel_span)")
fold("a1", "sess-1", 0, "activity with a session")
fold("a2", None, 0, "sessionless activity")

# (1) every row's covering set is exactly the required set for its class
for event_id, sid, usage in (("u1", "sess-1", 1), ("u2", None, 1), ("a1", "sess-1", 0), ("a2", None, 0)):
    req = REQUIRED[row_class(usage, sid)]
    c.expect(recorded[event_id] == req, f"{event_id} ({row_class(usage, sid)}) covering targets == {sorted(req)}", f"recorded {sorted(recorded[event_id])}")
c.expect("session" not in recorded["u2"] and recorded["u2"] == {"rollup", "day"}, "sessionless usage has a defined covering set (rollup + day), no session target")

# (2) seal and acknowledge the base targets but leave the DAY target open: the proof must refuse.
def seal_and_ack(t):
    if rule == "r4":
        ids = [r[0] for r in db.execute("select event_id from summary_membership where target_ref=? and state='live'", (t,))]
    else:
        ids = [r[0] for r in db.execute("select e.event_id from summary_member_edges e join summary_members m using(event_id, raw_generation) where e.target_ref=? and m.state='live'", (t,))]
    d = hexd(sum_digest(ids))
    db.execute("update summary_segments set state='sealed', sealed_revision=2, acked_revision=2, member_sum_digest=?, residue_sum_digest=?, acked_member_sum_digest=? where target_ref=?", (d, d, d, t))

def covering_targets(event_id):
    if rule == "r4":
        return [r[0] for r in db.execute("select target_ref from summary_membership where event_id=?", (event_id,))]
    return [r[0] for r in db.execute("select target_ref from summary_member_edges where event_id=?", (event_id,))]

def proof(event_id):
    """The round-5 deletion proof, steps 1-2 and 6 (ARCHITECTURE.md §2.2), over EVERY covering target."""
    for t in covering_targets(event_id):
        state, acked, sealed_rev, acked_d, d, psm = db.execute("select state, acked_revision, sealed_revision, acked_member_sum_digest, member_sum_digest, post_seal_mutations from summary_segments where target_ref=?", (t,)).fetchone()
        if state != "sealed" or acked is None or acked < sealed_rev or acked_d != d or psm:
            return f"refused: covering target {t} ({KIND[t]}) is {state}/unacknowledged"
    table = "summary_membership" if rule == "r4" else "summary_members"
    db.execute(f"update {table} set state='tombstone', raw_rowid=null, deleted_at_ms=1 where event_id=?", (event_id,))
    for t in covering_targets(event_id):
        db.execute("update summary_segments set tombstone_count=tombstone_count+1, live_member_count=live_member_count-1 where target_ref=?", (t,))
    db.execute("delete from buffered_events where id=?", (event_id,))
    return "deleted"

seal_and_ack(S); seal_and_ack(B)
r = proof("u1")
c.expect(r.startswith("refused"), "u1 is NOT deletable while its day target is open although its session target is sealed", r)
seal_and_ack(D)
results = {e: proof(e) for e in ("u1", "u2", "a1", "a2")}
c.expect(all(v == "deleted" for v in results.values()), "every row deletable once BOTH covering targets are sealed and acknowledged", str(results))
tomb = db.execute("select tombstone_count, live_member_count from summary_segments where target_ref=?", (D,)).fetchone()
c.expect(tomb == (4, 0), "the day target's counters saw all four deletions (tombstones=4, live=0)", str(tomb))
if rule == "r5":
    edges_left = db.execute("select count(*) from summary_member_edges").fetchone()[0]
    c.expect(edges_left == 8, "tombstoned members keep their edges until the target's last-chunk audit (8 edges)", str(edges_left))
c.finish()
