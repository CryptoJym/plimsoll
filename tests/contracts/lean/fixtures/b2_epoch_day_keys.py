#!/usr/bin/env python3
"""B2: `model_day_facts` and `activity_day_facts` omit `epoch_key` from their primary keys although the day
target's base key is epoch-scoped, so two installation epochs with the same day, source, model/kind and
segment sequence collide (VERDICT B2, Q07; reviewer-counterexamples.log 'model-day rows of two epochs collide').

Round-5 rule (ARCHITECTURE.md §3.5): both tables carry `epoch_key` first in the primary key and in every
index; each row's `target_ref` is its epoch's own day target; the reader and the oracle sum over epochs
and segments.
"""
import sqlite3
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("b2_epoch_day_keys", rule)
db = sqlite3.connect(":memory:")
db.execute("pragma foreign_keys = on")
db.executescript("""
create table summary_segments(target_ref integer primary key autoincrement, target_kind text not null, base_key text not null,
  segment_seq integer not null default 1, unique (target_kind, base_key, segment_seq));
insert into summary_segments(target_kind, base_key) values ('day','epoch-1|codex|2026-09-25'), ('day','epoch-2|codex|2026-09-25');
""")
if rule == "r4":
    db.executescript("""
    create table model_day_facts(day text not null, source text not null, model_key text not null, model text, segment_seq integer not null default 1,
      calls integer not null default 0, target_ref integer not null references summary_segments(target_ref),
      primary key (day, source, model_key, segment_seq));
    create table activity_day_facts(day text not null, source text not null, kind text not null, action_class text not null default '',
      machine_hash text not null default '', segment_seq integer not null default 1, events integer not null default 0,
      target_ref integer not null references summary_segments(target_ref),
      primary key (day, source, kind, action_class, machine_hash, segment_seq));
    """)
    ins_model = "insert into model_day_facts(day, source, model_key, model, segment_seq, calls, target_ref) values (?,?,?,?,1,?,?)"
    ins_act = "insert into activity_day_facts(day, source, kind, action_class, machine_hash, segment_seq, events, target_ref) values (?,?,?,?,?,1,?,?)"
else:
    db.executescript("""
    create table model_day_facts(epoch_key text not null default '', day text not null, source text not null, model_key text not null, model text,
      segment_seq integer not null default 1, calls integer not null default 0, target_ref integer not null references summary_segments(target_ref),
      primary key (epoch_key, day, source, model_key, segment_seq));
    create index idx_mdf_day on model_day_facts(day, source, model_key);
    create table activity_day_facts(epoch_key text not null default '', day text not null, source text not null, kind text not null,
      action_class text not null default '', machine_hash text not null default '', segment_seq integer not null default 1,
      events integer not null default 0, target_ref integer not null references summary_segments(target_ref),
      primary key (epoch_key, day, source, kind, action_class, machine_hash, segment_seq));
    create index idx_adf_day on activity_day_facts(day, source, kind);
    """)
    ins_model = "insert into model_day_facts(epoch_key, day, source, model_key, model, segment_seq, calls, target_ref) values (?,?,?,?,?,1,?,?)"
    ins_act = "insert into activity_day_facts(epoch_key, day, source, kind, action_class, machine_hash, segment_seq, events, target_ref) values (?,?,?,?,?,?,1,?,?)"

def insert(sql, params):
    try:
        db.execute(sql, params); return "inserted"
    except sqlite3.IntegrityError as exc:
        return f"refused: {exc}"

# The reviewer's rows: same day, source, model / kind, machine and segment sequence in two epochs.
if rule == "r4":
    m = [insert(ins_model, ("2026-09-25", "codex", "m:gpt", "gpt", 3, 1)), insert(ins_model, ("2026-09-25", "codex", "m:gpt", "gpt", 5, 2))]
    a = [insert(ins_act, ("2026-09-25", "codex", "tool_use", "", "machine", 7, 1)), insert(ins_act, ("2026-09-25", "codex", "tool_use", "", "machine", 11, 2))]
else:
    m = [insert(ins_model, ("epoch-1", "2026-09-25", "codex", "m:gpt", "gpt", 3, 1)), insert(ins_model, ("epoch-2", "2026-09-25", "codex", "m:gpt", "gpt", 5, 2))]
    a = [insert(ins_act, ("epoch-1", "2026-09-25", "codex", "tool_use", "", "machine", 7, 1)), insert(ins_act, ("epoch-2", "2026-09-25", "codex", "tool_use", "", "machine", 11, 2))]
c.expect(m == ["inserted", "inserted"], "model_day_facts holds the same (day, source, model, seq) for two epochs", str(m))
c.expect(a == ["inserted", "inserted"], "activity_day_facts holds the same (day, source, kind, machine, seq) for two epochs", str(a))
calls = db.execute("select coalesce(sum(calls),0) from model_day_facts where day='2026-09-25' and source='codex' and model_key='m:gpt'").fetchone()[0]
events = db.execute("select coalesce(sum(events),0) from activity_day_facts where day='2026-09-25' and source='codex' and kind='tool_use'").fetchone()[0]
c.expect(calls == 8, "reader sums both epochs' model-day rows (3 + 5 = 8 calls)", str(calls))
c.expect(events == 18, "reader sums both epochs' activity-day rows (7 + 11 = 18 events)", str(events))
if rule == "r5":
    # every day row's target is its own epoch's day target (base_key starts with the row's epoch)
    bad = db.execute("select count(*) from model_day_facts f join summary_segments s on s.target_ref=f.target_ref where s.base_key not like f.epoch_key||'|%'").fetchone()[0]
    c.expect(bad == 0, "every model-day row references its own epoch's day target", f"{bad} mismatches")
c.finish()
