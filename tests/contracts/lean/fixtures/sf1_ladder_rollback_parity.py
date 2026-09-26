#!/usr/bin/env python3
"""Should-fix 1: MIGRATION.md round 4 said S7/S8/F can be flagged back "with all raw rows present". After the
runway ladder's five-day rung has released acknowledged raw rows older than retentionDays (with receipts and
tombstones), that is false: the old reader path rebuilt from raw rows lacks exactly the released rows.

Round-5 rule (MIGRATION.md §8 'After a rung-2 release', PROOF.md §5 item 2b): a flag rollback after rung 2 is
qualified: the old path's reconstruction equals the pre-release old path MINUS the released set enumerated by
`raw_retention_receipts` (reason `lean_hold_release_acked`), and the new path (sealed segments, tombstones)
still holds the full count; the difference is disclosed, never called byte parity.
"""
import sqlite3
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("sf1_ladder_rollback_parity", rule)
db = sqlite3.connect(":memory:")
db.executescript("""
create table buffered_events(id text primary key, created_at text, uploaded_at text);
create table upload_receipts(delivery_id text primary key, terminal_state text);
create table raw_retention_receipts(event_id text primary key, reason text not null);
create table summary_segments(target_ref integer primary key autoincrement, member_count integer, tombstone_count integer default 0, state text);
create table summary_members(event_id text primary key, state text default 'live');
insert into buffered_events values ('old-acked-1','2026-01-01','2026-01-02'),('old-acked-2','2026-01-01','2026-01-02'),('old-pending','2026-01-01',null),('recent','2026-09-20','2026-09-21');
insert into upload_receipts values ('old-acked-1','acknowledged'),('old-acked-2','acknowledged'),('recent','acknowledged');
insert into summary_segments(member_count, state) values (4,'sealed');
insert into summary_members(event_id) values ('old-acked-1'),('old-acked-2'),('old-pending'),('recent');
""")
old_path_before = db.execute("select count(*) from buffered_events").fetchone()[0]     # dashboard_event_facts rebuilt from raw rows
# rung 2: release acknowledged rows older than retentionDays (90 d) only (ARCHITECTURE.md §2.4)
released = [r[0] for r in db.execute("""select e.id from buffered_events e where e.created_at < '2026-04-01' and e.uploaded_at is not null
  and exists (select 1 from upload_receipts r where r.delivery_id = e.id and r.terminal_state='acknowledged') order by 1""")]
for event_id in released:
    db.execute("insert into raw_retention_receipts values (?, 'lean_hold_release_acked')", (event_id,))
    db.execute("update summary_members set state='tombstone' where event_id=?", (event_id,))
    db.execute("update summary_segments set tombstone_count=tombstone_count+1 where target_ref=1")
    db.execute("delete from buffered_events where id=?", (event_id,))
old_path_after = db.execute("select count(*) from buffered_events").fetchone()[0]       # lean.facts=on rebuilds facts from what is left
new_path = db.execute("select member_count from summary_segments where target_ref=1").fetchone()[0]
receipts = db.execute("select count(*) from raw_retention_receipts where reason='lean_hold_release_acked'").fetchone()[0]
if rule == "r4":
    c.expect(old_path_after == old_path_before, "round-4 claim: flag rollback after the ladder still sees all raw rows", f"before={old_path_before} after={old_path_after}")
else:
    c.expect(old_path_after == old_path_before - receipts, "old path after rollback == pre-release old path minus the receipted release set", f"{old_path_after} == {old_path_before} - {receipts}")
    c.expect(new_path == old_path_before, "the new path (sealed segment) still holds the full pre-release count", f"{new_path}")
    c.expect(sorted(released) == ["old-acked-1", "old-acked-2"], "only acknowledged rows older than retentionDays were released; the pending row stays", str(released))
c.finish()
