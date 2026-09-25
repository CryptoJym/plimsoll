#!/usr/bin/env python3
"""S1b runway, round 7 (B0 blocker 2, review-r6 `blocking[1]`, `shouldFix[4]`): G per host from that host's OWN measured
cardinalities, the same conservative G in the runway numerator and in the hold-growth multiplier, and G REMAINING (not full G)
while conversion runs.

Round 6 (BUDGETS.md r6 §3, §4.2) computed `G_host = usage_rows x 1,632 B + raw_rows x 309 B`, x 1.25 until a copy measures it.
The 1,632 B per usage row embeds Studio1's mix (47 sessions and 1.5 session-day rows per 1,000 usage rows, 108 KiB of segment
rows per 1,000); a host with 333 sessions per 1,000 usage rows and the SAME row widths needs 507 MB against a 457 MB gate, and
a host exactly at the 63-day start threshold has 58.06 real days (reviewer-new-counterexamples.log: runway_session_mix). The
multiplier used the unfactored G while the numerator used 1.25 x G, and the continuous runway subtracted full G after
conversion had already consumed part of it, so free space fell twice for the same bytes and the < 5-day release could fire early.

Round-7 rule (CONTRACTS.md C2; BUDGETS.md r7 §3, §4.2; ARCHITECTURE.md r7 §2.4; MIGRATION.md r7 S1b):
  G_host = usage_rows x (F + T) + sessions x S + session_day_rows x SD + (model_day_rows + activity_day_rows) x DR
         + rollup_buckets x RB + (1.2 x sessions + rollup_buckets + day_targets) x SEG + raw_rows x I
  with every cardinality COUNTED on the host (light hosts: B1's one-time read-only census on a ledger COPY, never a scan on
  Studio0; busy hosts and Studio0: the converted copy's measured peak) and the row widths the round-5 geometry estimates,
  x 1.25 until S2 has measured the widths on a copy. A host whose census is scaled or missing has NO gate value. `G_gate` is
  used in the numerator AND in `g_host = G_gate / raw_bytes`. While conversion runs, `G_remaining = max(0, G_gate -
  new_table_bytes_now)`; the runway subtracts G_remaining, never full G, so converted bytes are not counted twice.
"""
import json
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("s1b_runway_host_bound", rule)
MB = 1_000_000; GiB = 1024 ** 3; KiB = 1024
# round-5 row widths incl. indexes (ESTIMATES until S2 measures them on a copy; plan-arithmetic.log "Round 5"/"Round 6")
F, T, S, SD, DR, RB, SEG, I = 700 * 1.5, 280 * 1.3, 1300, 260 * 1.5, 260 * 1.5, 400, 830, 125 + 2 * 92
U_R6 = (1000 * F + 1000 * T + 47 * S + 47 * 1.5 * SD + 8 * KiB + 10 * KiB + 108 * KiB) / 1000    # 1,631 B: Studio1's mix baked in

# Hosts. Studio1: the plan's measured counts (sessions 8,070; the rest at the plan's Studio1 geometry: 1.5 session-day rows per
# session, 144 rollup buckets/day x 90 days, 2 day targets/day/source). Studio4: MEASURED by out/checks/host_cardinality_census.py
# on a VACUUM INTO copy of this host's ledger, 2026-09-25 14:21 MDT (studio4-cardinality-census.json). Reviewer-333: Studio1's
# counts with 333 sessions per 1,000 usage rows (the reviewer's sensitivity case). Studio4-scaled: what round 6 assumed for
# Studio4 by scaling Studio1's counts by ledger size (plan-arithmetic.log "Round 6"), kept to show why scaling is not measuring.
HOSTS = {
    "studio1": dict(measured=True, usage=171_840, raw=276_454, sessions=8_070, session_days=12_105, day_rows=1_400, rollup_buckets=144 * 90, day_targets=180,
                    raw_bytes=623_939_584, ledger=1_342_111_744),
    "studio4 (census on copy)": dict(measured=True, usage=79_259, raw=352_885, sessions=5_141, session_days=5_654, day_rows=64 + 268, rollup_buckets=455, day_targets=34,
                    raw_bytes=833_286_144, ledger=1_504_444_416),
    "reviewer-333": dict(measured=True, usage=171_840, raw=276_454, sessions=57_223, session_days=85_834, day_rows=1_400, rollup_buckets=144 * 90, day_targets=180,
                    raw_bytes=623_939_584, ledger=1_342_111_744),
    "studio4 (r6 scaled from studio1)": dict(measured=False, usage=174_820, raw=281_249, sessions=None, session_days=None, day_rows=None, rollup_buckets=None, day_targets=None,
                    raw_bytes=741_421_056, ledger=1_365_389_312),
}

def G_actual(h):
    """The reviewer's full geometry at factor 1.0: what the new tables really need for this host's mix (unknown widths aside)."""
    if not h["measured"]: return None
    return (h["usage"] * (F + T) + h["sessions"] * S + h["session_days"] * SD + h["day_rows"] * DR + h["rollup_buckets"] * RB
            + (1.2 * h["sessions"] + h["rollup_buckets"] + h["day_targets"]) * SEG + h["raw"] * I)

def G_gate(h):
    """The figure the S1b gate subtracts (and, round 7, also feeds into the multiplier)."""
    if rule == "r6":
        return 1.25 * (h["usage"] * U_R6 + h["raw"] * I)          # mix-blind; accepts scaled counts
    if not h["measured"]: return None                            # round 7: no census, no gate value, no hold
    return 1.25 * G_actual(h)

def multiplier_G(h):
    """The G that sizes the hold-growth multiplier 1 + g_host x raw_share."""
    return (h["usage"] * U_R6 + h["raw"] * I) if rule == "r6" else G_gate(h)

# 1. Per host: the gate figure bounds the host's real need, and only for a measured host.
for name, h in HOSTS.items():
    g, a = G_gate(h), G_actual(h)
    print(f"    {name}: G_gate={None if g is None else round(g / MB, 1)} MB  G_actual={None if a is None else round(a / MB, 1)} MB  measured={h['measured']}")
    if h["measured"]:
        c.expect(g is not None and g >= a, f"{name}: the gate's G is at least the host's real new-table bytes for its own session/day/segment mix", f"gate={g / MB:.1f} MB actual={a / MB:.1f} MB")
    else:
        c.expect(g is None, f"{name}: a host whose counts were scaled from another host (not measured) has NO gate value, so no hold can start on it", f"gate={None if g is None else round(g / MB)} MB")
rev = HOSTS["reviewer-333"]
c.expect(G_actual(rev) > 457.3 * MB, "sanity: the reviewer's 333-session host needs more than round 6's 457 MB gate", f"actual={G_actual(rev) / MB:.1f} MB")

# 2. The runway at the exact 63-day start threshold: the gate must never claim more days than the host really has.
gross, reserve = 0.30 * GiB, 25 * GiB
def runway(h, G_num, G_mult, free, converted=0):
    raw_share = h["raw_bytes"] / h["ledger"]
    growth = gross * (1 + (G_mult / h["raw_bytes"]) * raw_share)
    g_rem = max(0, G_num - converted) if rule == "r7" else G_num
    return (free - reserve - 1.2 * h["ledger"] - g_rem) / growth, growth
free_at_threshold = reserve + 1.2 * rev["ledger"] + G_gate(rev) + 63 * runway(rev, G_gate(rev), multiplier_G(rev), 10 * GiB)[1]
days_gate = runway(rev, G_gate(rev), multiplier_G(rev), free_at_threshold)[0]
days_true = runway(rev, G_actual(rev), G_actual(rev), free_at_threshold)[0]
print(f"    reviewer-333 at the gate's 63-day threshold: gate says {days_gate:.2f} d, the host really has {days_true:.2f} d")
c.expect(days_gate >= 63 - 1e-9 and days_true >= 63 - 1e-9, "when the gate says 63 days the host really has at least 63 (the gate never overstates the runway)", f"gate={days_gate:.2f} true={days_true:.2f}")

# 3. One G: the numerator and the multiplier use the same conservative figure.
c.expect(multiplier_G(rev) == G_gate(rev), "the hold-growth multiplier uses the same G_gate as the numerator (not the unfactored estimate)", f"multiplier G={multiplier_G(rev) / MB:.1f} MB numerator G={G_gate(rev) / MB:.1f} MB")

# 4. G remaining: after the converter has written C bytes (already reflected in free space), the runway subtracts G - C, not G.
h4 = HOSTS["studio4 (census on copy)"]; Gg = G_gate(h4); C = 0.6 * Gg
free0 = reserve + 1.2 * h4["ledger"] + Gg + 5.4 * runway(h4, Gg, Gg, 10 * GiB)[1]     # 5.4 days of runway before conversion started
free_now = free0 - C                                                                   # conversion consumed C of the free space
true_days = (free_now - reserve - 1.2 * h4["ledger"] - (Gg - C)) / runway(h4, Gg, Gg, free_now)[1]
rule_days = runway(h4, Gg, Gg, free_now, converted=C)[0]
print(f"    studio4 mid-conversion (C = {C / MB:.0f} MB of G_gate {Gg / MB:.0f} MB written): true runway {true_days:.2f} d, the rule says {rule_days:.2f} d")
c.expect(abs(rule_days - true_days) < 0.01, "mid-conversion the runway subtracts G REMAINING, so converted bytes are not counted twice", f"rule={rule_days:.2f} true={true_days:.2f}")
c.expect((rule_days < 5) == (true_days < 5), "the < 5-day acknowledged-only release rung fires only when the true runway is under 5 days", f"rule={rule_days:.2f} true={true_days:.2f}")
c.expect(runway(h4, Gg, Gg, free0 - Gg, converted=Gg)[0] == runway(h4, 0, Gg, free0 - Gg)[0], "once conversion is complete G_remaining is 0 and only the rebuild headroom is still subtracted")
c.finish()
