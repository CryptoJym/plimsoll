#!/usr/bin/env python3
"""B1 residual (round 6): every raw row's global day target must be sealed AND acknowledged before the row can be deleted
(ARCHITECTURE.md §2.2 step 1), but protocol v2 in round 5 had no item kind identified by the day target and no receipt that
could set its `acked_revision` / `acked_member_sum_digest`; the round-5 fixture wrote those fields directly (VERDICT r5
'B1 residual', R5-05; reviewer-r5-counterexamples.log: day_ack.possible = false). So S9 could never delete a row.

Round-6 rule (ARCHITECTURE.md §5.2 'day_summary'): a `day_summary` item carries the day target (epoch, source, UTC day,
segmentSeq) with its revision, digest, memberSumDigest, deliveredMemberSumDigest and counters; the cloud judges it with the
same monotone-revision, seal-once and digest rules as the other segment items, inside the same one-transaction batch, and
returns the same receipt shape with the coverage relation of the day base key. The collector sets the day target's ack
fields ONLY from a `held` or `duplicate` receipt whose memberSumDigest equals its own; `conflict` and `stale` receipts
acknowledge nothing. This fixture never writes an ack field directly: every acknowledgement comes through a batch round trip.
"""
import hashlib, sqlite3
from pathlib import Path
from _common import Checks, hexd, sum_digest, digest16, rule_arg

rule = rule_arg()
c = Checks("b1_day_target_receipt", rule)

R5_KINDS = ["event", "session_summary", "session_day", "activity_rollup", "capture_gap", "membership_page"]
KINDS = R5_KINDS + (["day_summary"] if rule == "r6" else [])          # protocol v2 item table (ARCHITECTURE.md §5.2)
ITEM_KIND_FOR_TARGET = {"session": "session_summary", "rollup": "activity_rollup", "day": "day_summary"}
TARGET_KIND_FOR_ITEM = {v: k for k, v in ITEM_KIND_FOR_TARGET.items()}

db = sqlite3.connect(":memory:")
db.execute("pragma foreign_keys = on")
db.executescript("""
-- collector
create table buffered_events(id text primary key, session_id text, is_usage integer not null, payload text not null, delivered integer not null default 1);
create table summary_segments(
  target_ref integer primary key autoincrement,
  target_kind text not null check (target_kind in ('session','rollup','day')),
  base_key text not null, segment_seq integer not null default 1,
  state text not null default 'open' check (state in ('open','sealed','retired')),
  member_count integer not null default 0, live_member_count integer not null default 0, tombstone_count integer not null default 0,
  delivered_member_count integer not null default 0,
  member_sum_digest text not null default '00000000000000000000000000000000',
  delivered_member_sum_digest text not null default '00000000000000000000000000000000',
  residue_sum_digest text not null default '00000000000000000000000000000000',
  revision integer not null default 1, digest text not null default '',
  sealed_revision integer, acked_revision integer, acked_member_sum_digest text, post_seal_mutations integer not null default 0,
  unique (target_kind, base_key, segment_seq));
create table summary_members(event_id text not null, raw_generation text not null default '', raw_rowid integer,
  row_class text not null check (row_class in ('usage_session','usage_sessionless','activity_session','activity_sessionless')),
  state text not null default 'live' check (state in ('live','tombstone')), payload_digest16 blob not null,
  actor_binding_version integer, deleted_at_ms integer, readmissions integer not null default 0,
  primary key (event_id, raw_generation));
create unique index idx_smm_live_rowid on summary_members(raw_rowid) where raw_rowid is not null;
create table summary_member_edges(event_id text not null, raw_generation text not null default '',
  target_ref integer not null references summary_segments(target_ref), folded_revision integer not null,
  primary key (event_id, raw_generation, target_ref),
  foreign key (event_id, raw_generation) references summary_members(event_id, raw_generation)) without rowid;
create table upload_log(batch_no integer, item_kind text, key text, revision integer, status text, coverage text);
-- cloud
create table cloud_raw_keys(event_id text not null, base_key text not null, primary key (event_id, base_key));  -- the cloud's own knowledge of each delivered row's session / bucket / day
create table cloud_segments(item_kind text not null, key text not null, segment_seq integer not null, revision integer not null,
  digest text not null, member_sum_digest text not null, sealed integer not null, primary key (item_kind, key, segment_seq));
create table cloud_quarantine(item_kind text, key text, revision integer, reason text);
""")

EPOCH, SRC = "epoch-1", "codex"
def segment(kind, base_key):
    db.execute("insert into summary_segments(target_kind, base_key) values (?,?)", (kind, base_key))
    return db.execute("select target_ref from summary_segments where target_kind=? and base_key=?", (kind, base_key)).fetchone()[0]
def day_key(day): return f"{EPOCH}|{SRC}|{day}"
S  = segment("session", f"{EPOCH}|{SRC}|sess-1")
B  = segment("rollup",  f"{EPOCH}|{SRC}|2026-09-25T14|m:gpt|otel_span|mach")
D1 = segment("day", day_key("2026-09-25"))
D2 = segment("day", day_key("2026-09-26"))
KIND = {t: k for t, k in db.execute("select target_ref, target_kind from summary_segments")}
BASE = {t: k for t, k in db.execute("select target_ref, base_key from summary_segments")}

def fold(event_id, session_id, is_usage, payload, day_target, delivered=1):
    db.execute("insert into buffered_events(id, session_id, is_usage, payload, delivered) values (?,?,?,?,?)", (event_id, session_id, is_usage, payload, delivered))
    rowid = db.execute("select rowid from buffered_events where id=?", (event_id,)).fetchone()[0]
    row_class = ("usage" if is_usage else "activity") + ("_session" if session_id else "_sessionless")
    db.execute("insert into summary_members(event_id, raw_rowid, row_class, payload_digest16) values (?,?,?,?)", (event_id, rowid, row_class, digest16(payload)))
    for t in ((S if session_id else B), day_target):
        db.execute("insert into summary_member_edges(event_id, target_ref, folded_revision) values (?,?,1)", (event_id, t))
        db.execute("update summary_segments set member_count=member_count+1, live_member_count=live_member_count+1, delivered_member_count=delivered_member_count+? where target_ref=?", (delivered, t))
        if delivered:
            db.execute("insert into cloud_raw_keys values (?,?)", (event_id, BASE[t]))

def seal(t):
    """Sealing: exhaustive recompute from every live member (all present), digests frozen, revision cut."""
    ids = [r[0] for r in db.execute("select e.event_id from summary_member_edges e join summary_members m using(event_id, raw_generation) where e.target_ref=? and m.state='live'", (t,))]
    dids = [r[0] for r in db.execute("select e.event_id from summary_member_edges e join buffered_events b on b.id=e.event_id where e.target_ref=? and b.delivered=1", (t,))]
    msd, dmsd = hexd(sum_digest(ids)), hexd(sum_digest(dids))
    rev = db.execute("select revision from summary_segments where target_ref=?", (t,)).fetchone()[0] + 1
    dig = hashlib.sha256(f"{KIND[t]}|{BASE[t]}|{rev}|{msd}|{len(ids)}".encode()).hexdigest()[:32]
    db.execute("update summary_segments set state='sealed', sealed_revision=?, revision=?, digest=?, member_sum_digest=?, residue_sum_digest=?, delivered_member_sum_digest=? where target_ref=?",
               (rev, rev, dig, msd, msd, dmsd, t))

skipped = []
def build_batch():
    """The collector's batch: one item per segment whose current revision is not acknowledged; only kinds the protocol has."""
    items = []
    for t, kind, base_key, seq, state, rev, dig, msd, dmsd, mc, dmc, acked in db.execute(
            "select target_ref, target_kind, base_key, segment_seq, state, revision, digest, member_sum_digest, delivered_member_sum_digest, member_count, delivered_member_count, acked_revision from summary_segments order by target_ref"):
        if acked is not None and acked >= rev:
            continue
        item_kind = ITEM_KIND_FOR_TARGET[kind]
        if item_kind not in KINDS:                      # round 5: no item kind can carry a day target, so it is never sent
            skipped.append((t, kind)); continue
        items.append(dict(kind=item_kind, key=base_key, segmentSeq=seq, revision=rev, digest=dig, memberSumDigest=msd,
                          deliveredMemberSumDigest=dmsd, memberCount=mc, deliveredMemberCount=dmc, sealed=(state == "sealed")))
    return items

batch_no = 0
def judge(items):
    """The cloud: one transaction; every item judged; receipts for all; an unknown kind refuses the whole batch (400)."""
    global batch_no
    batch_no += 1
    if any(i["kind"] not in KINDS for i in items):
        return None
    receipts = []
    for it in items:
        stored = db.execute("select revision, digest, member_sum_digest, sealed from cloud_segments where item_kind=? and key=? and segment_seq=?", (it["kind"], it["key"], it["segmentSeq"])).fetchone()
        cloud_ids = [r[0] for r in db.execute("select event_id from cloud_raw_keys where base_key=?", (it["key"],))]
        cd = hexd(sum_digest(cloud_ids))
        if cd == it["memberSumDigest"] and len(cloud_ids) == it["memberCount"]: coverage = "equal"
        elif cd == it["deliveredMemberSumDigest"] and it["memberCount"] > it["deliveredMemberCount"]: coverage = "collector_superset"
        else: coverage = "unknown"
        if stored is None or it["revision"] > stored[0]:
            if stored is not None and stored[3] and stored[2] != it["memberSumDigest"]:
                status, echo = "conflict", stored[2]        # a sealed segment is stored once and never replaced
                db.execute("insert into cloud_quarantine values (?,?,?,?)", (it["kind"], it["key"], it["revision"], "segment_seal_conflict"))
            else:
                status, echo = "held", it["memberSumDigest"]
                db.execute("insert or replace into cloud_segments values (?,?,?,?,?,?,?)", (it["kind"], it["key"], it["segmentSeq"], it["revision"], it["digest"], it["memberSumDigest"], int(it["sealed"])))
        elif it["revision"] == stored[0]:
            if it["digest"] == stored[1]:
                status, echo = "duplicate", stored[2]
            else:
                status, echo = "conflict", stored[2]
                db.execute("insert into cloud_quarantine values (?,?,?,?)", (it["kind"], it["key"], it["revision"], f"{it['kind']}_revision_conflict"))
        else:
            status, echo = "stale", stored[2]
        db.execute("insert into upload_log values (?,?,?,?,?,?)", (batch_no, it["kind"], it["key"], it["revision"], status, coverage))
        receipts.append(dict(kind=it["kind"], key=it["key"], revision=it["revision"], digest=it["digest"], status=status, coverage=coverage,
                             segments=[dict(seq=it["segmentSeq"], sealed=it["sealed"], memberSumDigest=echo, status=status)]))
    return receipts

def apply_receipts(receipts):
    """The collector: ack fields are set ONLY here, only from held/duplicate receipts whose digest equals the local one."""
    acked = []
    for r in receipts or []:
        if r["status"] not in ("held", "duplicate"):
            continue
        kind = TARGET_KIND_FOR_ITEM[r["kind"]]
        for seg in r["segments"]:
            row = db.execute("select target_ref, member_sum_digest, revision from summary_segments where target_kind=? and base_key=? and segment_seq=?", (kind, r["key"], seg["seq"])).fetchone()
            if row and seg["memberSumDigest"] == row[1] and r["revision"] == row[2]:
                db.execute("update summary_segments set acked_revision=?, acked_member_sum_digest=? where target_ref=?", (r["revision"], seg["memberSumDigest"], row[0]))
                acked.append(row[0])
    return acked

def covering_targets(event_id):
    return [r[0] for r in db.execute("select target_ref from summary_member_edges where event_id=?", (event_id,))]

def proof(event_id):
    """The round-5/6 deletion proof, steps 1-2, 5 and 6 (ARCHITECTURE.md §2.2), over EVERY covering target."""
    delivered = db.execute("select delivered from buffered_events where id=?", (event_id,)).fetchone()[0]
    if not delivered:
        return "refused: the row's own delivery is not acknowledged (never-delete set, §2.3)"
    for t in covering_targets(event_id):
        state, acked, sealed_rev, acked_d, d, psm = db.execute("select state, acked_revision, sealed_revision, acked_member_sum_digest, member_sum_digest, post_seal_mutations from summary_segments where target_ref=?", (t,)).fetchone()
        if state != "sealed" or acked is None or acked < sealed_rev or acked_d != d or psm:
            return f"refused: covering target {t} ({KIND[t]}) is {state}/unacknowledged"
    db.execute("update summary_members set state='tombstone', raw_rowid=null, deleted_at_ms=1 where event_id=?", (event_id,))
    for t in covering_targets(event_id):
        db.execute("update summary_segments set tombstone_count=tombstone_count+1, live_member_count=live_member_count-1 where target_ref=?", (t,))
    db.execute("delete from buffered_events where id=?", (event_id,))
    return "deleted"

# --- scenario: one day with a session, a sessionless bucket and one dead delivery -----------------------------------------
fold("u1", "sess-1", 1, "usage with a session", D1)
fold("u2", None, 1, "sessionless usage (an amount-bearing otel_span)", D1)
fold("a1", "sess-1", 0, "activity with a session", D1)
fold("a2", None, 0, "sessionless activity whose delivery is dead", D1, delivered=0)
for t in (S, B, D1):
    seal(t)
items = build_batch()
receipts = judge(items)
acked = apply_receipts(receipts)

# (1) the protocol can carry the global day target at all
c.expect("day_summary" in KINDS, "protocol v2 has an item kind identified by the global day target (day_summary)", f"kinds={KINDS}")
carried = db.execute("select count(*) from upload_log where item_kind='day_summary' and key=?", (day_key("2026-09-25"),)).fetchone()[0]
c.expect(carried == 1, "the sealed day target was carried by a batch item and judged with the session and bucket items", f"day items uploaded={carried}; targets with no item kind={skipped}")
c.expect(receipts is not None and len(receipts) == len(items) and len(items) > 0, "one transaction: a receipt for every item in the batch", f"items={len(items)} receipts={0 if receipts is None else len(receipts)}")
# (2) the acknowledgement reaches the day target through the receipt, never through a direct write
row = db.execute("select acked_revision, sealed_revision, acked_member_sum_digest, member_sum_digest from summary_segments where target_ref=?", (D1,)).fetchone()
c.expect(row[0] is not None and row[0] == row[1] and row[2] == row[3] and D1 in acked, "the day target's acked_revision and acked_member_sum_digest were set by a held receipt (apply_receipts), not by a direct write", f"acked_revision={row[0]} sealed_revision={row[1]} digests equal={row[2] == row[3]}")
src = Path(__file__).read_text(); needle = "set acked_revision=?" + ", acked_member_sum_digest=?"
c.expect(src.count(needle) == 1, "the only statement that writes ack fields in this fixture is the one inside apply_receipts", f"occurrences={src.count(needle)}")
if rule == "r6":
    cov = db.execute("select coverage from upload_log where item_kind='day_summary' and key=?", (day_key("2026-09-25"),)).fetchone()[0]
    c.expect(cov == "collector_superset", "the day receipt carries the day base key's coverage relation (a2 is undelivered: collector_superset)", cov)
# (3) the deletion proof now passes for every delivered row of the day, and still refuses the undelivered one
results = {e: proof(e) for e in ("u1", "u2", "a1")}
c.expect(all(v == "deleted" for v in results.values()), "every delivered row of the day is deletable after the receipt round trip (both covering targets sealed AND acknowledged)", str(results))
r_a2 = proof("a2")
c.expect(r_a2.startswith("refused") and "never-delete" in r_a2, "a day-level acknowledgement never makes a row with a dead delivery deletable", r_a2)

if rule == "r6":
    # (4) a conflict receipt acknowledges nothing: the cloud already holds a SEALED day segment for D2 at the same revision with a different digest
    fold("u3", "sess-1", 1, "usage on the next day", D2)
    db.execute("insert or replace into cloud_segments values ('day_summary', ?, 1, 2, ?, ?, 1)", (day_key("2026-09-26"), "0" * 32, "f" * 32))   # the cloud already holds a sealed revision 2 with a different digest
    seal(D2)                                                       # revision becomes 2: equal revision, different digest
    r2 = judge(build_batch()); apply_receipts(r2)
    st = [r["status"] for r in r2 if r["kind"] == "day_summary"]
    q = db.execute("select reason from cloud_quarantine where key=?", (day_key("2026-09-26"),)).fetchone()
    d2, d2_sealed = db.execute("select acked_revision, sealed_revision from summary_segments where target_ref=?", (D2,)).fetchone()
    c.expect(st == ["conflict"] and q is not None and (d2 is None or d2 < d2_sealed), "a conflict receipt (equal revision, different digest) is quarantined and acknowledges nothing: the sealed revision stays unacknowledged", f"statuses={st} quarantine={q} acked_revision={d2} sealed_revision={d2_sealed}")
    c.expect(proof("u3").startswith("refused") and "day" in proof("u3"), "the member of the conflicting day target stays undeletable and the refusal names the day target", proof("u3"))
    # (5) a duplicate receipt is idempotent; a stale receipt changes nothing
    d1_item = next(i for i in items if i["kind"] == "day_summary")
    r3 = judge([d1_item]); apply_receipts(r3)
    stale_item = dict(d1_item, revision=d1_item["revision"] - 1, digest="1" * 32)
    r4 = judge([stale_item]); apply_receipts(r4)
    row2 = db.execute("select acked_revision, acked_member_sum_digest from summary_segments where target_ref=?", (D1,)).fetchone()
    c.expect(r3[0]["status"] == "duplicate" and r4[0]["status"] == "stale" and (row2[0], row2[1]) == (row[1], row[3]), "a re-sent day item is a duplicate (ack unchanged) and a lower revision is stale (ack unchanged)", f"duplicate={r3[0]['status']} stale={r4[0]['status']} ack={row2}")
c.finish()
