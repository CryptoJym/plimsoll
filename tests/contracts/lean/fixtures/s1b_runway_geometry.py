#!/usr/bin/env python3
"""S1b runway (round 6): the round-5 preflight gate still used G = 0.42 x raw bytes and hold_growth = gross x (1 + 0.42 x raw_share),
both from plan_arithmetic.py's old 20 KiB-per-1,000-rows membership line, although round 5 added a 125 B identity row and two
92 B edges for EVERY raw row and about 19 MB of segment rows on Studio1. The reviewer's Studio1 lower bound is 346.8 MB
(55.6% of raw bytes), about 365.7 MB (58.6%) with the specified segments (VERDICT r5 'S1b runway', R5-12).

Round-6 rule (BUDGETS.md §3, §4.2; ARCHITECTURE.md §2.4; MIGRATION.md S1b): G is computed per host from its MEASURED row counts
(B1 records usage rows and raw rows daily) and the round-5 row constants, G = usage_rows x U + raw_rows x I + segments, never
from a fixed share of raw bytes; the hold-growth multiplier uses the host's own G ratio; a host without a copy measurement
applies a 1.25 safety factor to the estimate; busy hosts and Studio0 use the copy's measured peak. The hold may start only
under the corrected figure.
"""
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("s1b_runway_geometry", rule)
MB = 1_000_000; GiB = 1024 ** 3; KiB = 1024
# Studio1, measured (value-map dbstat and counts via out/checks/plan_arithmetic.py)
RAW_BYTES = 623_939_584; USAGE_ROWS = 171_840; ACTIVITY_ROWS = 104_614; RAW_ROWS = USAGE_ROWS + ACTIVITY_ROWS; LEDGER = 1_342_111_744
# per 1,000 usage rows, the lean lines that form the pre-deletion new-table set (plan-arithmetic.log)
LEAN_PER_1000 = {"usage_facts_local": 1000 * 700 * 1.5, "turn_summaries": 1000 * 280 * 1.3, "session_summaries": 47 * 1300,
                 "session_day_facts": 47 * 1.5 * 260 * 1.5, "day_tables": 8 * KiB, "rollup": 10 * KiB}
OLD_MEMBERSHIP_PER_1000 = 20 * KiB                    # round 3/4: 'summary_membership (only while raw rows exist; est. 20 KiB steady)'
IDENT_B, EDGE_B, EDGES = 125, 92, 2                  # round 5 identity row and edges, per RAW row
SEG_PER_1000 = 108 * KiB                             # segment rows per 1,000 usage rows at Studio1's geometry (plan-arithmetic.log 'Round 5')
U = sum(LEAN_PER_1000.values()) / 1000 + SEG_PER_1000 / 1000   # bytes per usage row (facts, turns, sessions, day rows, rollups, segments)
I = IDENT_B + EDGES * EDGE_B                                    # bytes per raw row (identity row + two edges)

def G_bytes(usage_rows, raw_rows, raw_bytes):
    if rule == "r5":
        ratio = (sum(LEAN_PER_1000.values()) + OLD_MEMBERSHIP_PER_1000) * (USAGE_ROWS / 1000) / RAW_BYTES   # 0.425 on Studio1, applied as a fixed share everywhere
        return ratio * raw_bytes
    return usage_rows * U + raw_rows * I

G = G_bytes(USAGE_ROWS, RAW_ROWS, RAW_BYTES); ratio = G / RAW_BYTES
print(f"    row constants: U = {U:,.0f} B per usage row (incl. {SEG_PER_1000/1000:.0f} B of segments), I = {I} B per raw row; Studio1 G = {G/MB:.1f} MB = {ratio:.1%} of raw bytes (rule {rule})")
c.expect(G >= 346.8 * MB, "Studio1 G is at or above the reviewer's lower bound of 346.8 MB", f"G={G/MB:.1f} MB")
c.expect(ratio >= 0.556, "G as a share of Studio1's raw bytes is at or above 55.6%", f"{ratio:.1%}")

# the S1b start decision on a Studio1-shaped host: 50 GiB free on a 500 GiB volume, WAL_24h 0.2 GiB, gross growth 0.30 GiB/day, planned hold 42 days
raw_share = RAW_BYTES / LEDGER
mult = 1 + (0.42 if rule == "r5" else ratio) * raw_share
reserve = max(0.05 * 500 * GiB, 2 * 0.2 * GiB, 2 * GiB); rebuild = 1.2 * LEDGER
free = 50 * GiB; gross = 0.30 * GiB; planned = 42
runway = (free - reserve - G - rebuild) / (gross * mult)
allowed = runway >= max(30, 1.5 * planned)
G_ref = USAGE_ROWS * U + RAW_ROWS * I; mult_ref = 1 + (G_ref / RAW_BYTES) * raw_share
runway_ref = (free - reserve - G_ref - rebuild) / (gross * mult_ref); allowed_ref = runway_ref >= max(30, 1.5 * planned)
c.expect(allowed == allowed_ref, "the S1b start decision equals the decision under the corrected geometry (the hold is not started on the stale figure)",
         f"this rule: runway {runway:.1f} d, hold allowed={allowed}; corrected: runway {runway_ref:.1f} d, allowed={allowed_ref}")
c.expect(mult >= 1 + 0.556 * raw_share, "the hold-growth multiplier uses the host's G ratio, not 0.42", f"multiplier={mult:.3f} (raw_share {raw_share:.3f})")

if rule == "r6":
    # per-host table: counts scaled from Studio1's mix where no count exists (an estimate); the gate applies 1.25x until the copy measures
    alloc = {"studio1": (1_342_111_744, 623_939_584), "studio3": (915_918_848, 462_868_480), "studio4": (1_365_389_312, 741_421_056), "studio5": (403_279_872, 232_103_936)}
    for h, (t, r) in alloc.items():
        u = USAGE_ROWS * t / alloc["studio1"][0]; rr = RAW_ROWS * t / alloc["studio1"][0]
        g = G_bytes(u, rr, r)
        print(f"    {h}: est. usage rows {u:,.0f}, raw rows {rr:,.0f} -> G {g/MB:.0f} MB ({g/r:.1%} of raw); gate value 1.25 x G = {1.25*g/MB:.0f} MB until the copy measures; ledger {t/1e9:.2f} -> {(t+g)/1e9:.2f} GB")
    c.expect(1.25 * G >= G_ref, "the gate value for an unmeasured light host is at least the corrected estimate", f"1.25 x G = {1.25*G/MB:.0f} MB")
c.finish()
