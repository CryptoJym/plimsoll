#!/usr/bin/env python3
"""B3: round 4 retired sealed `summary_segments` after 90 days while indefinite tombstones and 1,825-day day
facts still referenced `target_ref`; with foreign keys on the delete fails, with them off SQLite reuses the
integer rowid and an old tombstone aliases a new segment (VERDICT B3, Q08; reviewer-counterexamples.log
'without FK enforcement, old tombstone target_ref=1 aliases a new segment after rowid reuse').

Round-5 rule (ARCHITECTURE.md §2.1, §2.2 'Retirement', §3.3): `target_ref` is `integer primary key
autoincrement` (never reused, even after the highest row is deleted); the lean writer's connection runs
with `PRAGMA foreign_keys = ON`; tombstone EDGES are dropped at the target's last-chunk audit while the
tombstone IDENTITY rows stay; a segment row is deleted only by the retirement proof: zero edges, zero
mutation records, zero day-fact children, and the session/rollup row past its own retention.
"""
import sqlite3
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("b3_durable_target_refs", rule)
db = sqlite3.connect(":memory:")
autoinc = "" if rule == "r4" else " autoincrement"
db.executescript(f"""
create table summary_segments(target_ref integer primary key{autoinc}, target_kind text not null, base_key text not null,
  state text not null default 'open', live_member_count integer not null default 0);
create table summary_members(event_id text primary key, state text not null default 'live', raw_rowid integer, deleted_at_ms integer);
create table summary_member_edges(event_id text not null references summary_members(event_id), target_ref integer not null references summary_segments(target_ref),
  primary key (event_id, target_ref)) without rowid;
create table sealed_member_mutations(target_ref integer not null references summary_segments(target_ref), event_id text not null, kind text not null, at_ms integer not null);
create table activity_day_facts(epoch_key text not null default '', day text not null, target_ref integer not null references summary_segments(target_ref));
""")
db.execute("pragma foreign_keys = " + ("off" if rule == "r4" else "on"))

# A day target with a 1,825-day child, then a session segment (the HIGHEST rowid, as in the reviewer's case) with one
# member that was deleted long ago (a tombstone).
db.execute("insert into summary_segments(target_kind, base_key, state) values ('day','e|codex|2026-06-01','sealed')")
db.execute("insert into summary_segments(target_kind, base_key, state) values ('session','e|codex|old-session','sealed')")
old_session = db.execute("select target_ref from summary_segments where target_kind='session'").fetchone()[0]
day_target = db.execute("select target_ref from summary_segments where target_kind='day'").fetchone()[0]
db.execute("insert into summary_members values ('old-event','tombstone',null,1)")
db.execute("insert into summary_member_edges values ('old-event',?)", (old_session,))
db.execute("insert into summary_member_edges values ('old-event',?)", (day_target,))
db.execute("insert into activity_day_facts values ('e','2026-06-01',?)", (day_target,))

def references(t):
    return sum(db.execute(f"select count(*) from {tbl} where target_ref=?", (t,)).fetchone()[0]
               for tbl in ("summary_member_edges", "sealed_member_mutations", "activity_day_facts"))

def retire(t):
    """Round 4: delete the sealed segment 90 days after the session; round 5: only with a zero-reference proof."""
    if rule == "r5" and references(t) != 0:
        return f"refused: {references(t)} references"
    try:
        db.execute("delete from summary_segments where target_ref=?", (t,)); return "deleted"
    except sqlite3.IntegrityError as exc:
        return f"refused by FK: {exc}"

# 1. The 90-day retirement of the old session segment while a tombstone edge still references it.
r1 = retire(old_session)
if rule == "r4":
    # The round-4 retention rule deletes it (foreign keys are off on the collector's connection at the pin: no
    # `foreign_keys` pragma in collector-cli/src at 92c33bf). Then a new segment is created.
    db.execute("insert into summary_segments(target_kind, base_key) values ('session','e|codex|new-session')")
else:
    db.execute("insert into summary_segments(target_kind, base_key) values ('session','e|codex|new-session')")
new_session = db.execute("select target_ref from summary_segments where base_key='e|codex|new-session'").fetchone()[0]
alias = db.execute("select count(*) from summary_member_edges e join summary_segments s on s.target_ref=e.target_ref where e.event_id='old-event' and s.base_key='e|codex|new-session'").fetchone()[0]
c.expect(alias == 0, "the old tombstone's edge never points at a newer segment (no rowid aliasing)", f"retire={r1!r}; new segment target_ref={new_session}; aliasing edges={alias}")
c.expect(new_session != old_session, "a new segment never receives a retired segment's target_ref", f"old={old_session} new={new_session}")

# 2. Lifetime consistency: a referenced segment cannot be retired; after the last-chunk audit drops the tombstone
#    edges and the child rows are gone, the retirement proof allows it; the number is still never reused.
if rule == "r5":
    c.expect(r1.startswith("refused"), "retirement of a referenced segment is refused by the proof", r1)
    db.execute("delete from summary_member_edges where event_id='old-event'")      # last-chunk audit passed: tombstone edges dropped, identity row stays
    still_tomb = db.execute("select state from summary_members where event_id='old-event'").fetchone()[0]
    c.expect(still_tomb == "tombstone", "the tombstone identity row survives the edge drop (re-admission still decided by it)")
    r2 = retire(old_session)
    c.expect(r2 == "deleted", "zero references -> the session segment retires", r2)
    r3 = retire(day_target)
    c.expect(r3.startswith("refused"), "the day target with a 1,825-day child cannot retire", r3)
    # delete the highest-numbered segment and insert again: autoincrement never hands out an old number
    top = db.execute("select max(target_ref) from summary_segments").fetchone()[0]
    db.execute("delete from summary_segments where target_ref=?", (top,))
    db.execute("insert into summary_segments(target_kind, base_key) values ('session','e|codex|another')")
    fresh = db.execute("select target_ref from summary_segments where base_key='e|codex|another'").fetchone()[0]
    c.expect(fresh > top, "after deleting the highest row the next target_ref is still fresh (sqlite_sequence)", f"deleted {top}, next {fresh}")
    # foreign keys on: a raw delete of the segment under a live edge is refused by SQLite itself (belt and braces)
    db.execute("insert into summary_segments(target_kind, base_key) values ('rollup','e|codex|bucket')")
    b = db.execute("select target_ref from summary_segments where base_key='e|codex|bucket'").fetchone()[0]
    db.execute("insert into summary_members values ('live-event','live',7,null)")
    db.execute("insert into summary_member_edges values ('live-event',?)", (b,))
    try:
        db.execute("delete from summary_segments where target_ref=?", (b,)); fk = "deleted"
    except sqlite3.IntegrityError as exc:
        fk = f"refused: {exc}"
    c.expect(fk.startswith("refused"), "PRAGMA foreign_keys=ON refuses deleting a segment that an edge references", fk)
else:
    c.expect(False, "round-4 retirement (90 days) is consistent with indefinite tombstones and 1,825-day day facts", f"segment deleted under a tombstone reference: {r1}")
c.finish()
