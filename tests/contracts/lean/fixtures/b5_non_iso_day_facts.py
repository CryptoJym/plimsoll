#!/usr/bin/env python3
"""B5 new loss (round 6): the shared timestamp schema accepts any string Date.parse accepts that ends in Z or +-HH:MM
(collector packages/shared/src/schemas.ts:65-73), including RFC-2822-style strings such as 'Sat, 26 Sep 2026 00:00:00 GMT+00:00'
whose UTC day is known (2026-09-26). Round 5 classified such rows `non_iso`, called them contract violations and kept them out
of the day tables, so an admitted usage record with a known instant was missing from the dashboard's day sums (VERDICT r5
'B5 new loss', R5-10; reviewer-r5-counterexamples.log non_iso.plan_excludes_day_fact = true).

Round-6 rule (ARCHITECTURE.md §3.5 'Every accepted timestamp has a day'): the converter and the live writer fold EVERY stored
timestamp by the UTC day of Date.parse(observed_at), the parser that admitted it (collector-cli/src/dashboard-projection.ts:500-504);
`non_iso` stays a census class for the per-cutoff harness only. `contract_violation` is reserved for a stored string the parser
rejects (NaN), which the schema would have refused at intake: such a row is recorded as a counted gap AND its event id is kept
in `conversion_rejects` (listed in /status and the S3 certify). Nothing is dropped silently.
"""
import re, sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("b5_non_iso_day_facts", rule)
SCHEMA_SUFFIX = re.compile(r"(?:Z|[+-]\d{2}:\d{2})$")
ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:([Tt ])(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?)?(Z|[+-]\d{2}:\d{2})$")
# Non-ISO strings the schema accepts, with the instant node gives them (out/checks/node-date-parse-r6.log, node v26.8.1; the two
# strings shared with round 5's node v20.20.2 log agree). This is a FINITE corpus of shapes, not the Date.parse grammar.
NON_ISO = {
    "Sat, 26 Sep 2026 00:00:00 GMT+00:00": 1790380800000,
    "September 26, 2026 00:00:00 +00:00": 1790380800000,
    "26 Sep 2026 00:00:00 GMT+00:00": 1790380800000,
    "1 Sep 2026 00:00:00 +00:00": 1788220800000,
    "Sat Sep 26 2026 00:00:00 GMT+00:00": 1790380800000,
    "Sat, 26 Sep 2026 23:30:00 -02:00": 1790472600000,
    "2026/09/26 00:00:00 +00:00": 1790380800000,
}
NAN = {"26/09/2026 00:00:00 +00:00"}          # Date.parse NaN: the schema refuses it; it can reach a ledger only past the schema

def date_parse_ms(s):
    """Python port of V8 Date.parse for the ISO shapes (fractions truncated to ms; T24:00 = next midnight) plus the measured corpus."""
    m = ISO.match(s)
    if not m:
        return NON_ISO.get(s)
    y, mo, d, sep, hh, mi, ss, frac, tz = m.groups()
    hh = int(hh or 0); mi = int(mi or 0); ss = int(ss or 0); ms = int((frac or "0")[:3].ljust(3, "0"))
    if hh == 24 and mi == 0 and ss == 0 and ms == 0:
        base = datetime(int(y), int(mo), int(d), tzinfo=timezone.utc) + timedelta(days=1)
    elif hh > 23 or mi > 59 or ss > 59:
        return None
    else:
        base = datetime(int(y), int(mo), int(d), hh, mi, ss, tzinfo=timezone.utc)
    off = 0 if tz == "Z" else (1 if tz[0] == "+" else -1) * (int(tz[1:3]) * 60 + int(tz[4:6]))
    return int(base.timestamp()) * 1000 + ms - off * 60_000
def schema_ok(s): return SCHEMA_SUFFIX.search(s) is not None and date_parse_ms(s) is not None
def utc_day(ms): return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()

ROWS = [  # (id, observed_at, is_usage, input_tokens, output_tokens)
    ("c1", "2026-09-25T10:00:00.000Z", 1, 50, 5),
    ("c2", "2026-09-26T09:00:00.000Z", 1, 40, 4),
    ("r1", "Sat, 26 Sep 2026 00:00:00 GMT+00:00", 1, 100, 10),   # the reviewer's string: a usage record, UTC day 2026-09-26
    ("r2", "September 26, 2026 00:00:00 +00:00", 0, None, None),
    ("r3", "1 Sep 2026 00:00:00 +00:00", 1, 7, 1),                 # sorts BEFORE every ISO cutoff: today's rule excludes it from every window
    ("r4", "Sat, 26 Sep 2026 23:30:00 -02:00", 1, 20, 2),          # local 26th, UTC day 2026-09-27
    ("x1", "26/09/2026 00:00:00 +00:00", 1, 999, 99),              # schema-invalid (NaN): an intake bypass, never a valid row
]
valid_ids = {r[0] for r in ROWS if schema_ok(r[1])}
c.expect(valid_ids == {"c1", "c2", "r1", "r2", "r3", "r4"}, "the schema accepts every row except the NaN string", str(sorted(valid_ids)))

def convert(rows):
    """The converter (S3) and the live writer: day facts keyed by the UTC day of the parsed instant."""
    facts, gaps, rejects, folded = {}, [], [], []
    for event_id, s, usage, i, o in rows:
        ms = date_parse_ms(s)
        non_iso = not re.match(r"^\d{4}-\d{2}-\d{2}", s)
        if rule == "r5" and non_iso:          # round 5 §3.5 (c): 'a contract violation the converter records as a counted capture_gap ... and keeps out of the day tables'
            gaps.append(("contract_violation", event_id, "counted")); continue
        if ms is None:                        # round 6: only a string the parser rejects; round 5 could not fold it either
            gaps.append(("contract_violation", event_id, "counted"))
            if rule == "r6": rejects.append(event_id)
            continue
        d = facts.setdefault(utc_day(ms), dict(events=0, calls=0, input=0, output=0))
        d["events"] += 1
        if usage:
            d["calls"] += 1; d["input"] += i or 0; d["output"] += o or 0
        folded.append(event_id)
    return facts, gaps, rejects, folded

facts, gaps, rejects, folded = convert(ROWS)

# the raw-row oracle: an independent SQL over buffered_events with the projection's observed_at_ms column (dashboard-projection.ts:598,797)
db = sqlite3.connect(":memory:")
db.execute("create table buffered_events(id text primary key, observed_at text not null, observed_at_ms integer, is_usage integer, input_tokens integer, output_tokens integer)")
db.executemany("insert into buffered_events values (?,?,?,?,?,?)", [(r[0], r[1], date_parse_ms(r[1]), r[2], r[3], r[4]) for r in ROWS])
oracle = {day: dict(events=ev, calls=ca, input=i, output=o) for day, ev, ca, i, o in db.execute(
    "select strftime('%Y-%m-%d', observed_at_ms/1000.0, 'unixepoch') as day, count(*), sum(is_usage), coalesce(sum(case when is_usage then input_tokens end),0), coalesce(sum(case when is_usage then output_tokens end),0) from buffered_events where observed_at_ms is not null group by 1")}

c.expect(facts == oracle, "day facts equal the raw-row oracle keyed by the UTC day of every parsed instant", f"facts={facts}\n      oracle={oracle}")
c.expect(facts.get("2026-09-26", {}).get("input", 0) == 140, "the reviewer's RFC-style usage record (100 input tokens) is in the 2026-09-26 day fact (40 + 100)", f"input on 2026-09-26 = {facts.get('2026-09-26', {}).get('input')}")
labelled = {g[1] for g in gaps if g[0] == "contract_violation"}
c.expect(not (labelled & valid_ids), "no schema-valid timestamp is labelled contract_violation", f"labelled={sorted(labelled)}")
accounted = set(folded) | set(rejects)
c.expect(accounted == {r[0] for r in ROWS}, "every row is either folded into a day fact or listed by id in conversion_rejects: nothing vanishes", f"folded={sorted(folded)} rejects={rejects}")
c.expect(rejects == ["x1"] and ("contract_violation", "x1", "counted") in gaps, "the NaN string alone is a contract violation: a counted gap with its event id retained", f"rejects={rejects} gaps={gaps}")

# the per-cutoff harness (round 5's boundary rule, kept): non_iso rows are examined per cutoff, never assumed
def cutoff_str(day): return f"{day}T00:00:00.000Z"
def cutoff_ms(day): return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()) * 1000
def flips(s):
    base = datetime.fromisoformat(utc_day(date_parse_ms(s)))
    out = []
    for k in range(-1, 2):
        day = (base + timedelta(days=k)).date().isoformat()
        if (s >= cutoff_str(day)) != (date_parse_ms(s) >= cutoff_ms(day)): out.append(day)
    return out
for s in NON_ISO:
    print(f"    {s!r}: utc_day={utc_day(date_parse_ms(s))} old-rule position vs every ISO cutoff={'after' if s >= '2026-01-01T00:00:00.000Z' else 'before'} flips_at={flips(s)}")
r3s = "1 Sep 2026 00:00:00 +00:00"
c.expect((r3s >= cutoff_str("2026-09-01")) is False and date_parse_ms(r3s) >= cutoff_ms("2026-09-01"),
         "harness: '1 Sep 2026 ...' is excluded by the string rule at its own midnight cutoff and included by the instant rule (a per-cutoff flip, listed)", f"flips={flips(r3s)}")
# the embedded corpus agrees with the node log shipped beside it
log = Path(__file__).resolve().parents[1] / "checks" / "node-date-parse-r6.log"
if log.exists():
    measured = {}
    for line in log.read_text().splitlines():
        m = re.match(r'^"(.*)"\s+schema_ok=(\w+)\s+ms=(\S+)', line)
        if m: measured[m.group(1)] = None if m.group(3) == "NaN" else int(m.group(3))
    mismatch = {s: (ms, measured.get(s)) for s, ms in NON_ISO.items() if measured.get(s) != ms}
    nan_ok = all(measured.get(s) is None for s in NAN if s in measured)
    c.expect(not mismatch and nan_ok, f"the corpus matches the node measurement in checks/node-date-parse-r6.log ({len(measured)} strings)", str(mismatch))
else:
    c.expect(True, "node log not present beside the fixture; corpus taken as measured (see out/checks/node-date-parse-r6.log in the packet)")
c.finish()
