#!/usr/bin/env python3
"""B5: a `+00:00` event exactly at a UTC-midnight cutoff is excluded by today's lexical window rule
(`fact.observedAt >= cutoffAt`, dashboard-projection.ts:2185-2196, cutoff = `toISOString()`, `:545-547`)
and included by the round-4 instant rule, without its day moving; round 4's census counted only rows whose
string-slice day differs from their UTC day, so it missed that row and the byte-parity promise for
`Z`/`±00:00` rows was false (VERDICT B5, C21/Q10; reviewer-counterexamples.log 'NEW: +00:00 exact-midnight
event has zero day shift ...').

Round-5 rule (ARCHITECTURE.md §3.5 'The boundary, exactly'): for a midnight cutoff C the two rules differ
for a row s iff (s >= C_str) != (observed_at_ms(s) >= C_ms), where observed_at_ms is Date.parse
(collector-cli/src/dashboard-projection.ts:500-504; V8 truncates fractions beyond 3 digits,
out/checks/node-date-parse.log). Rows that can differ at SOME midnight cutoff are exactly the union of
three census classes, each computable per row without knowing the cutoff:
  day_move             substr(s,1,10) <> UTC day of observed_at_ms
  before_own_midnight  s < substr(s,1,10) || 'T00:00:00.000Z'   (explicit-offset forms at 00:00:00, fractions
                       of 4+ digits starting '000' at 00:00:00, a space separator anywhere in the day)
  non_iso              s does not start with YYYY-MM-DD (schema-valid RFC-2822-style strings)
A row in none of the classes is byte-identical under both rules at every midnight cutoff (proved below
by enumeration over the schema's accepted grammar); the harness evaluates census rows exactly per cutoff.
"""
import re, sqlite3
from datetime import datetime, timezone, timedelta
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("b5_lexical_boundary", rule)

# --- Date.parse as verified with node v20.20.2 (out/checks/node-date-parse.log) ------------------------------------------
NODE_VERIFIED = {  # string -> ms (or None when Date.parse gives NaN); the subset of that log this fixture reproduces
    "2026-09-26T00:00:00.000Z": 1790380800000, "2026-09-26T00:00:00Z": 1790380800000, "2026-09-26T00:00:00+00:00": 1790380800000,
    "2026-09-26T00:00:00-00:00": 1790380800000, "2026-09-26T00:00:00.000+00:00": 1790380800000, "2026-09-26T00:00:00.0Z": 1790380800000,
    "2026-09-26T00:00:00.00Z": 1790380800000, "2026-09-26T00:00:00.0000Z": 1790380800000, "2026-09-26T00:00:00.0009Z": 1790380800000,
    "2026-09-26T00:00:00.000500Z": 1790380800000, "2026-09-26T00:00:00.0010Z": 1790380800001, "2026-09-25T23:59:59.9999Z": 1790380799999,
    "2026-09-26T00:00:00.5+00:00": 1790380800500, "2026-09-26T00:00:00-02:00": 1790388000000, "2026-09-26T00:00:00+02:00": 1790373600000,
    "2026-09-25T23:30:00-02:00": 1790386200000, "2026-09-26T00:00Z": 1790380800000, "2026-09-26T00:00+00:00": 1790380800000,
    "2026-09-26 00:00:00Z": 1790380800000, "2026-09-26 12:00:00Z": 1790424000000, "2026-09-26t00:00:00Z": 1790380800000,
    "2026-09-26T00:00:00.123456789Z": 1790380800123, "2026-09-26Z": 1790380800000, "2026-09-26T24:00:00Z": 1790467200000,
    "2026-09-26T00:00:00+14:00": 1790330400000, "2026-09-26T00:00:00-12:00": 1790424000000,
    "Sat, 26 Sep 2026 00:00:00 GMT+00:00": 1790380800000, "September 26, 2026 00:00:00 +00:00": 1790380800000,
    "2026-09-26T24:00:00.000Z": 1790467200000, "2026-09-26T24:00:00.5Z": None, "2026-09-26T24:00:01Z": None, "2026-09-26 24:00:00Z": 1790467200000, "2026-09-26T24:00Z": 1790467200000,
}
SCHEMA_SUFFIX = re.compile(r"(?:Z|[+-]\d{2}:\d{2})$")          # packages/shared/src/schemas.ts:64-73 (plus Date.parse != NaN)
ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:([Tt ])(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?)?(Z|[+-]\d{2}:\d{2})$")
NON_ISO_INSTANT = {"Sat, 26 Sep 2026 00:00:00 GMT+00:00": 1790380800000, "September 26, 2026 00:00:00 +00:00": 1790380800000}

def date_parse_ms(s):
    """Python port of V8 Date.parse for the shapes enumerated here (fractions truncated to ms; T24:00 = next midnight)."""
    m = ISO.match(s)
    if not m:
        return NON_ISO_INSTANT.get(s)
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

def schema_ok(s):
    return SCHEMA_SUFFIX.search(s) is not None and date_parse_ms(s) is not None

mismatch = {s: (date_parse_ms(s), ms) for s, ms in NODE_VERIFIED.items() if date_parse_ms(s) != ms}
c.expect(not mismatch, f"the fixture's Date.parse port reproduces node v20 on all {len(NODE_VERIFIED)} verified strings", str(mismatch))

# --- the two window rules and the census classes -------------------------------------------------------------------------
def cutoff_str(day):   # sinceIso: new Date(ms).toISOString()  -> always 'YYYY-MM-DDT00:00:00.000Z' at a midnight cutoff
    return f"{day.isoformat()}T00:00:00.000Z"
def cutoff_ms(day):
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()) * 1000
def old_rule(s, day):  return s >= cutoff_str(day)                      # factIncludedInWindow, string compare
def new_rule(s, day):  return date_parse_ms(s) >= cutoff_ms(day)        # round 4/5: by instant
def utc_day(s):        return datetime.fromtimestamp(date_parse_ms(s) / 1000, tz=timezone.utc).date().isoformat()
def census_classes(s):
    classes = set()
    if not re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return {"non_iso"}
    if s[:10] != utc_day(s): classes.add("day_move")
    if rule == "r5" and s < s[:10] + "T00:00:00.000Z": classes.add("before_own_midnight")
    return classes

D = datetime(2026, 9, 26, tzinfo=timezone.utc).date()
CUTOFFS = [D + timedelta(days=k) for k in range(-3, 4)]
def differing_cutoffs(s):
    return [day.isoformat() for day in CUTOFFS if old_rule(s, day) != new_rule(s, day)]

# 1. The reviewer's exact case.
s0 = "2026-09-26T00:00:00+00:00"
c.expect(s0[:10] == utc_day(s0) and old_rule(s0, D) is False and new_rule(s0, D) is True,
         "reviewer's case: +00:00 at midnight has zero day shift, old rule excludes, instant rule includes")
c.expect(bool(census_classes(s0)), "the census counts the reviewer's case", f"classes={sorted(census_classes(s0))}")

# 2. Enumerate the schema's accepted grammar around the cutoff and prove the census classes are exactly the rows that can differ.
days = ["2026-09-25", "2026-09-26", "2026-09-27"]
seps = ["T", "t", " "]
times = ["", "00:00", "00:00:00", "00:00:01", "12:00:00", "23:59:59", "24:00:00"]
fracs = ["", ".0", ".00", ".000", ".001", ".0000", ".0009", ".0010", ".9999", ".5"]
tzs = ["Z", "+00:00", "-00:00", "+02:00", "-02:00", "+14:00", "-12:00", "+00:30", "-00:30"]
strings = set()
for day in days:
    strings.add(day + "Z")
    for sep in seps:
        for t in times:
            if not t: continue
            for f in fracs:
                if f and len(t) < 8: continue
                for tz in tzs:
                    strings.add(f"{day}{sep}{t}{f}{tz}")
strings |= set(NON_ISO_INSTANT)
valid = sorted(s for s in strings if schema_ok(s))
differ = {s: differing_cutoffs(s) for s in valid}
in_census = {s for s in valid if census_classes(s)}
missed = sorted(s for s, cuts in differ.items() if cuts and s not in in_census)
never = sorted(s for s in valid if not census_classes(s) and differ[s])
c.expect(len(valid) > 1000, f"grammar enumeration produced {len(valid)} schema-valid strings")
c.expect(not missed, "every string that differs at some midnight cutoff is in a census class", f"{len(missed)} missed, e.g. {missed[:6]}")
c.expect(not never, "no string outside the census classes differs at any midnight cutoff (byte-parity set is exact)", str(never[:6]))
by_class = {}
for s in valid:
    for k in census_classes(s): by_class[k] = by_class.get(k, 0) + 1
n_differ = sum(1 for s in valid if differ[s])
CANON = re.compile(r"\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d\.\d{3}Z")
canonical_never = all(not differ[s] for s in valid if CANON.fullmatch(s))
print(f"    census classes over the grammar: {by_class}; strings differing at >=1 cutoff: {n_differ}; canonical 'T..sssZ' strings never differ: {canonical_never}")
# the enumerated case families (each shown once)
examples = {
    "explicit zero offset at 00:00:00 (reviewer)": "2026-09-26T00:00:00+00:00",
    "negative offset at local 00:00:00, same UTC day": "2026-09-26T00:00:00-02:00",
    "4+ digit fraction starting 000 at 00:00:00": "2026-09-26T00:00:00.0009Z",
    "space separator, any time of the day": "2026-09-26 12:00:00Z",
    "no-seconds form with an offset": "2026-09-26T00:00+00:00",
    "T24:00:00 (day move by the clock)": "2026-09-26T24:00:00Z",
    "positive offset, local date ahead of UTC (day move)": "2026-09-26T01:00:00+02:00",
    "negative offset, local date behind UTC (day move, round-4 case)": "2026-09-25T23:30:00-02:00",
    "positive offset at local 00:00:00 (day move class, yet agrees at every midnight cutoff)": "2026-09-26T00:00:00+02:00",
    "RFC-2822-style string the schema accepts (non_iso)": "Sat, 26 Sep 2026 00:00:00 GMT+00:00",
    "canonical toISOString shape (never differs)": "2026-09-26T00:00:00.000Z",
    "Z without fraction (never differs)": "2026-09-26T00:00:00Z",
}
for label, s in examples.items():
    print(f"    {label}: {s!r} classes={sorted(census_classes(s))} differs_at={differing_cutoffs(s)}")
c.expect(differing_cutoffs("2026-09-26T00:00:00+02:00") == [] and "day_move" in census_classes("2026-09-26T00:00:00+02:00"),
         "the census is a superset: a day_move row can still agree at every cutoff (harness decides per cutoff)")

# 3. The SQL census (MIGRATION.md S0) over the same strings: canonical rows are provably identical; every other row is examined in code.
db = sqlite3.connect(":memory:")
db.execute("create table buffered_events(observed_at text)")
db.executemany("insert into buffered_events values (?)", [(s,) for s in valid])
sql = """select observed_at,
  case when observed_at glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[01][0-9]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z'
         or observed_at glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T2[0-3]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z' then 'canonical'
       when observed_at not glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*' then 'non_iso'
       when strftime('%Y-%m-%d', observed_at) is null then 'unparsed_by_sqlite'
       when substr(observed_at,1,10) <> strftime('%Y-%m-%d', observed_at) then 'day_move'
       when observed_at < substr(observed_at,1,10)||'T00:00:00.000Z' then 'before_own_midnight'
       else 'examine' end as census_class from buffered_events"""
sql_class = dict(db.execute(sql).fetchall())
canonical_differs = [s for s, k in sql_class.items() if k == "canonical" and differ[s]]
c.expect(not canonical_differs, "SQL census: every 'canonical' row is identical under both rules at every cutoff", str(canonical_differs[:4]))
sql_missed = [s for s in valid if differ[s] and sql_class[s] not in ("day_move", "before_own_midnight", "non_iso", "unparsed_by_sqlite", "examine")]
c.expect(not sql_missed, "SQL census: every differing row lands in a class that the code pass examines", str(sql_missed[:4]))
print(f"    SQL census classes: { {k: sum(1 for v in sql_class.values() if v == k) for k in sorted(set(sql_class.values()))} }")
c.finish()
