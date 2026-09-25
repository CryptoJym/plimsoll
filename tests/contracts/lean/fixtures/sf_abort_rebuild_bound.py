#!/usr/bin/env python3
"""Should-fix (round 6): MIGRATION.md §4 step 8 and PROOF.md §3 / §6 item 7 accepted an abort rebuild only if "the file returns
to at most its pre-S2 live size". Under the hold nothing is deleted while capture keeps appending raw rows, so a correct abort
(new tables dropped, then rebuilt) is legitimately LARGER than the pre-S2 file by the raw bytes captured since S2; the round-5
wording would fail a correct abort (VERDICT r5 shouldFix 5).

Round-6 rule: the bound is pre-S2 live bytes PLUS the raw capture since S2 (raw rows admitted since S2, counted by B1, times
the host's measured bytes per raw row), with the new tables removed; the bound stays tight enough to catch an abort that left
the new tables in place.
"""
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("sf_abort_rebuild_bound", rule)
MB = 1_000_000
L0 = 1_342_111_744                 # pre-S2 live bytes (Studio1-shaped host)
PER_RAW = 2_262                    # measured bytes per raw row incl. indexes (plan-arithmetic.log)
ROWS_SINCE_S2 = 6 * 20_000         # raw rows admitted during six days of S2-S3 under the hold (B1 counts them)
R = ROWS_SINCE_S2 * PER_RAW        # the raw capture since S2
G = 365_900_000                    # the new tables at S3 (round-6 geometry, s1b_runway_geometry.py)

def rebuilt_size(new_tables_dropped=True):
    return L0 + R + (0 if new_tables_dropped else G)

bound = L0 if rule == "r5" else L0 + R
size = rebuilt_size()
print(f"    pre-S2 live {L0/MB:.0f} MB; raw capture since S2 {R/MB:.0f} MB ({ROWS_SINCE_S2:,} rows x {PER_RAW} B); new tables {G/MB:.0f} MB; bound under {rule} = {bound/MB:.0f} MB")
c.expect(size <= bound, "a correct abort (new tables dropped; the hold deleted nothing) passes the acceptance bound", f"rebuilt {size/MB:.0f} MB vs bound {bound/MB:.0f} MB")
c.expect(rebuilt_size(new_tables_dropped=False) > bound, "an abort that left the new tables in place fails the bound (the bound is tight)", f"{rebuilt_size(False)/MB:.0f} MB > {bound/MB:.0f} MB")
c.expect(bound - size < 0.01 * L0, "the bound is not loose: it exceeds the correct size by less than 1% of the pre-S2 file", f"slack {(bound-size)/MB:.0f} MB")
c.finish()
