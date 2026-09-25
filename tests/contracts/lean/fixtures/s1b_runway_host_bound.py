#!/usr/bin/env python3
"""S1b runway, round 8 (B0 round 2, review-r1 blocker 2 and should-fix 5): G OWED while conversion runs, counted from the
converter's own bytes plus the raw rows admitted after the census; a "true runway" computed from a byte-level simulation that is
INDEPENDENT of the rule under test; the census freshness and space gate; the segment proxy. Round-7 checks 1-7 (G per host from
counted cardinalities, one G) are kept.

Round 6 (BUDGETS.md r6 §3, §4.2) computed `G_host = usage_rows x 1,632 B + raw_rows x 309 B`, x 1.25 until a copy measures it.
The 1,632 B per usage row embeds Studio1's mix; a host with 333 sessions per 1,000 usage rows needs 507 MB against a 457 MB gate
and has 58.06 real days at the gate's 63-day threshold (review-r6 blocker 2). Round 7 (CONTRACTS.md r7 C2 rules 1-2) fixed that
with G from the host's own counted cardinalities and one G_gate in the numerator and the multiplier (checks 1-7 below).

Round 7's rule 3 set `G_remaining = max(0, G_gate - new_table_bytes_now)` over EVERY page of the lean tables. S2 dual-write
precedes S3 conversion (MIGRATION.md §2), so the live writer's bytes were counted as conversion progress, and the one-time census
omitted rows admitted after it, which still need converting. Both overstate the runway: on Studio4's census by 0.39-2.56 days in
the reviewer's scenarios; the rule showed 5.0 d where the truth was 2.45 d and the 2.0 d abort point where the truth was -0.55 d
(review-r1 checks/review_c2_runway.log). The round-7 fixture could not see this because its "true runway" was the rule's own G - C.

Round 8 rule (CONTRACTS.md r8 C2 rule 3):
  G_owed = max(0, G_gate - C_conv) + g_gate x U,   g_gate = G_gate / raw_bytes_at_census
  C_conv = bytes the CONVERTER wrote for census-era history (its own counter; never a page count of the lean tables, which
           includes the live writer's rows); U = raw bytes of rows admitted after the census that no lean row covers yet
           (neither converted nor dual-written); G_owed = 0 once every census-era and post-census row is folded.
  runway_days = (free_disk - reserve - G_owed - rebuild_headroom) / hold_growth_per_day.
  Census obligation (rule 1): taken after the catch-up, at most one day before the S1b decision, re-taken before S3; the
  `VACUUM INTO` copy is refused unless free space >= ledger bytes + reserve. Segments: the census counts a proxy (sessions split
  at 7 days) where it has one, and the terminal-pause splits are declared covered by the 1.25 factor (the segment term is about
  2% of G on Studio4).
Round 9 (B0 round 3, review-r2 should-fixes 5-6; CONTRACTS.md r9 C2): the FORMULA is unchanged. Two definitions are added:
  C_conv is a MEASUREMENT, the converter's own page allocation: inside each of its write transactions
           delta(page_count - freelist_count) x page_size, summed into its checkpoint and committed with the chunk (SQLite has one
           writer at a time, so those pages are the converter's alone); never rows x estimated widths (round 8 said "its own
           counter" without a basis) and never a lean-table page count (round 7). U is in the census's raw-bytes basis:
           unfolded post-census raw rows x (raw_bytes_at_census / raw_rows_at_census).
  The re-census before S3 runs DURING the hold: its VACUUM INTO copy is made only when free - ledger >= reserve + rebuild
           headroom + G_owed (the copy must leave the ladder's terms intact and cannot trip a rung), on top of the first census's
           free >= ledger + reserve.
Modes: --rule r6 (round 6), r7 (round-1 C2 as written), r8 (round-2 C2 as written), r9 (this rule). Red under r6, r7 and r8,
green under r9.
"""
import json
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("s1b_runway_host_bound", rule)
R8 = rule in ("r8", "r9")          # the round-8 formula is kept by round 9
R9 = rule == "r9"
MB = 1_000_000; GiB = 1024 ** 3; KiB = 1024
# round-5 row widths incl. indexes (ESTIMATES until S2 measures them on a copy; plan-arithmetic.log "Round 5"/"Round 6")
F, T, S, SD, DR, RB, SEG, I = 700 * 1.5, 280 * 1.3, 1300, 260 * 1.5, 260 * 1.5, 400, 830, 125 + 2 * 92
U_R6 = (1000 * F + 1000 * T + 47 * S + 47 * 1.5 * SD + 8 * KiB + 10 * KiB + 108 * KiB) / 1000    # 1,631 B: Studio1's mix baked in

# Hosts (unchanged from round 7). Studio4: MEASURED by checks/host_cardinality_census.py on a VACUUM INTO copy of this host's
# ledger, 2026-09-25 14:21 MDT (studio4-cardinality-census.json); it carries no segment proxy (the census was not re-run in round 8).
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

def segment_rows(h):
    """Round 8: the counted 7-day-split proxy when the census carries one, else the plan's 1.2 x sessions geometry."""
    proxy = h.get("segment_proxy")
    return max(1.2 * h["sessions"], proxy) if (R8 and proxy is not None) else 1.2 * h["sessions"]
def G_actual(h):
    """The reviewer's full geometry at factor 1.0: what the new tables really need for this host's mix (unknown widths aside)."""
    if not h["measured"]: return None
    return (h["usage"] * (F + T) + h["sessions"] * S + h["session_days"] * SD + h["day_rows"] * DR + h["rollup_buckets"] * RB
            + (segment_rows(h) + h["rollup_buckets"] + h["day_targets"]) * SEG + h["raw"] * I)
def G_gate(h):
    if rule == "r6": return 1.25 * (h["usage"] * U_R6 + h["raw"] * I)          # mix-blind; accepts scaled counts
    if not h["measured"]: return None                                            # rounds 7-8: no census, no gate value, no hold
    return 1.25 * G_actual(h)
def multiplier_G(h): return (h["usage"] * U_R6 + h["raw"] * I) if rule == "r6" else G_gate(h)

# ---- 1-3. G per host from counted cardinalities, the 63-day threshold, one G (round 7, unchanged) ----------------------------------
for name, h in HOSTS.items():
    g, a = G_gate(h), G_actual(h)
    print(f"    {name}: G_gate={None if g is None else round(g / MB, 1)} MB  G_actual={None if a is None else round(a / MB, 1)} MB  measured={h['measured']}")
    if h["measured"]:
        c.expect(g is not None and g >= a, f"{name}: the gate's G is at least the host's real new-table bytes for its own session/day/segment mix", f"gate={g / MB:.1f} MB actual={a / MB:.1f} MB")
    else:
        c.expect(g is None, f"{name}: a host whose counts were scaled from another host (not measured) has NO gate value, so no hold can start on it", f"gate={None if g is None else round(g / MB)} MB")
rev = HOSTS["reviewer-333"]
c.expect(G_actual(rev) > 457.3 * MB, "sanity: the reviewer's 333-session host needs more than round 6's 457 MB gate", f"actual={G_actual(rev) / MB:.1f} MB")
gross, reserve = 0.30 * GiB, 25 * GiB
def growth_rate(h, G_mult):
    return gross * (1 + (G_mult / h["raw_bytes"]) * (h["raw_bytes"] / h["ledger"]))
def runway_r7(h, G_num, G_mult, free, converted=0):
    g_rem = max(0, G_num - converted) if rule != "r6" else G_num
    return (free - reserve - 1.2 * h["ledger"] - g_rem) / growth_rate(h, G_mult)
free_at_threshold = reserve + 1.2 * rev["ledger"] + G_gate(rev) + 63 * growth_rate(rev, multiplier_G(rev))
days_gate = runway_r7(rev, G_gate(rev), multiplier_G(rev), free_at_threshold)
days_true = runway_r7(rev, G_actual(rev), G_actual(rev), free_at_threshold)
print(f"    reviewer-333 at the gate's 63-day threshold: gate says {days_gate:.2f} d, the host really has {days_true:.2f} d")
c.expect(days_gate >= 63 - 1e-9 and days_true >= 63 - 1e-9, "when the gate says 63 days the host really has at least 63 (the gate never overstates the runway)", f"gate={days_gate:.2f} true={days_true:.2f}")
c.expect(multiplier_G(rev) == G_gate(rev), "the hold-growth multiplier uses the same G_gate as the numerator (not the unfactored estimate)", f"multiplier G={multiplier_G(rev) / MB:.1f} MB numerator G={G_gate(rev) / MB:.1f} MB")

# ---- 4. G owed: a byte-level simulation whose truth does not use the rule under test (blocker 2, review r1) ------------------------
h4 = HOSTS["studio4 (census on copy)"]
def rule_owed(G_gate_bytes, raw_bytes_census, converter_written, lean_pages_now, unfolded_raw_bytes, folded_everything):
    """The rule under test. Round 7: G_gate minus EVERY lean-table page. Round 8: G_gate minus the converter's own bytes, plus the
    gate's bytes-per-raw-byte times the raw bytes admitted after the census that nothing has folded yet; 0 once everything is folded."""
    if rule == "r6": return G_gate_bytes                                        # round 6 subtracted full G throughout
    if rule == "r7": return max(0.0, G_gate_bytes - lean_pages_now)
    if folded_everything: return 0.0
    return max(0.0, G_gate_bytes - converter_written) + (G_gate_bytes / raw_bytes_census) * unfolded_raw_bytes
def simulate(h, gross_per_day, days_census_to_s2, days_s2_to_now, converted_fraction, width_factor=1.0):
    """Truth, independent of the rule: the host's ledger from the census instant. Free space starts where the gate allows exactly
    63 days at the census. Raw bytes grow by gross x raw_share per day. Before S2 no lean bytes are written; from S2 the live writer
    adds lean bytes for every admitted raw row at the TRUE widths (width_factor x the estimates). The converter has written
    converted_fraction of the census-era history at the true widths. Rows admitted between the census and S2 are neither converted
    nor dual-written yet. The true remaining conversion work is what the converter has still to write."""
    Gg = G_gate(h); g_gate = Gg / h["raw_bytes"]
    G_true = width_factor * G_actual(h); g_true = G_true / h["raw_bytes"]
    raw_share = h["raw_bytes"] / h["ledger"]
    growth = gross_per_day * (1 + g_gate * raw_share)                           # the rule's hold growth (C2 rule 2)
    rebuild = 1.2 * h["ledger"]
    free0 = reserve + rebuild + Gg + 63 * growth
    raw_growth = gross_per_day * raw_share
    dual = g_true * raw_growth * days_s2_to_now                                  # live-writer bytes since S2 (growth, not conversion)
    conv = converted_fraction * G_true                                           # converter bytes for census-era history
    unfolded = raw_growth * days_census_to_s2                                    # raw bytes admitted between the census and S2: still owed
    free_now = free0 - gross_per_day * (days_census_to_s2 + days_s2_to_now) - dual - conv
    true_owed = (1 - converted_fraction) * G_true + g_true * unfolded
    owed = rule_owed(Gg, h["raw_bytes"], conv, dual + conv, unfolded, converted_fraction >= 1 and unfolded == 0)
    return dict(gross_MB_per_day=round(gross_per_day / MB, 1), days_census_to_S2=days_census_to_s2, days_S2_to_now=days_s2_to_now,
                converted_fraction=converted_fraction, width_factor=width_factor, G_gate_MB=round(Gg / MB, 1),
                dual_write_MB=round(dual / MB, 1), converter_MB=round(conv / MB, 1), unfolded_raw_MB=round(unfolded / MB, 1),
                rule_owed_MB=round(owed / MB, 1), true_owed_MB=round(true_owed / MB, 1),
                rule_days=(free_now - reserve - rebuild - owed) / growth, true_days=(free_now - reserve - rebuild - true_owed) / growth,
                margin_days=(owed - true_owed) / growth, growth=growth, free_now=free_now, rebuild=rebuild, owed=owed, true_owed=true_owed, unfolded=unfolded)
SCENARIOS = [  # the reviewer's six (review_c2_runway.py §2-3) plus the rung scenario
    (0.30 * GiB, 0, 3, 0.0), (40 * MB, 0, 7, 0.0), (40 * MB, 0, 7, 0.6),
    (0.30 * GiB, 7, 0, 0.0), (40 * MB, 14, 0, 0.0), (40 * MB, 14, 2, 0.6),
]
worst = 0.0
for sc in SCENARIOS:
    s = simulate(h4, *sc)
    over = s["rule_days"] - s["true_days"]; worst = max(worst, over)
    print(f"    studio4 gross {s['gross_MB_per_day']} MB/d, census->S2 {s['days_census_to_S2']} d, S2->now {s['days_S2_to_now']} d, {int(100 * s['converted_fraction'])}% converted: rule {s['rule_days']:.2f} d, truth {s['true_days']:.2f} d, rule - truth {over:+.2f} d (owed rule {s['rule_owed_MB']} / true {s['true_owed_MB']} MB)")
c.expect(worst <= 1e-9, "the published runway never overstates the truth while dual-write and post-census rows exist (dual-write bytes are growth, not conversion; rows admitted after the census are still owed)", f"worst overstatement {worst:.2f} d")
s = simulate(h4, 40 * MB, 14, 2, 0.6)
expected_margin = 0.25 * (G_actual(h4) + (G_actual(h4) / h4["raw_bytes"]) * s["unfolded"]) / s["growth"]   # the 1.25 allowance on the census G (retained until completion) and on the post-census owed bytes
c.expect(abs((s["true_days"] - s["rule_days"]) - expected_margin) < 1e-6,
         "the rule's only conservatism is the 1.25 width allowance (0.25 x G_actual on the census-era G until completion, plus 0.25 x the post-census owed bytes) / growth: exactly that, nothing padded", f"truth - rule = {s['true_days'] - s['rule_days']:.2f} d, expected {expected_margin:.2f} d")

# ---- 5. the rungs: the reviewer's slow host (40 MB/d, census 14 d before S2, S2 2 d, 60% converted) -------------------------------
def rung_view(target):
    """Shift free space so the RULE shows exactly `target` days; report what the truth is then, and whether the rung is late."""
    base = simulate(h4, 40 * MB, 14, 2, 0.6)
    shift = base["rule_days"] - target
    return {"rule_shows": target, "true_days": round(base["true_days"] - shift, 2)}
five, two = rung_view(5.0), rung_view(2.0)
print(f"    rungs on the slow host: rule 5.0 d -> truth {five['true_days']} d; rule 2.0 d (abort) -> truth {two['true_days']} d")
c.expect(five["true_days"] >= 5 - 1e-9, "the < 5-day acknowledged-only release fires no later than the truth: when the rule shows 5.0 days the host really has at least 5.0", f"truth {five['true_days']} d")
c.expect(two["true_days"] >= 2 - 1e-9, "the < 2-day abort fires no later than the truth: when the rule shows 2.0 days the host really has at least 2.0 (not -0.55)", f"truth {two['true_days']} d")
sweep_ok = True
for offset_days in [x / 4 for x in range(0, 60)]:
    base = simulate(h4, 40 * MB, 14, 2, 0.6)
    shift = base["rule_days"] - offset_days
    true_days, rule_days = base["true_days"] - shift, offset_days
    if (true_days < 5) and not (rule_days < 5): sweep_ok = False
    if (true_days < 2) and not (rule_days < 2): sweep_ok = False
c.expect(sweep_ok, "sweeping the free space: whenever the true runway is under a rung's threshold the rule's runway is under it too (no rung fires late)")

# ---- 6. width sensitivity and completion -----------------------------------------------------------------------------------------
w125 = simulate(h4, 40 * MB, 14, 2, 0.6, width_factor=1.25); w150 = simulate(h4, 40 * MB, 14, 2, 0.6, width_factor=1.5)
print(f"    width factor 1.25 (the worst the factor covers): rule {w125['rule_days']:.2f} d, truth {w125['true_days']:.2f} d; width factor 1.5: rule {w150['rule_days']:.2f} d, truth {w150['true_days']:.2f} d (outside the factor: why S2 measures widths before the numbers freeze)")
c.expect(w125["rule_days"] <= w125["true_days"] + 1e-6, "with true widths 1.25 x the estimates (the most the factor covers) the rule still never overstates", f"rule {w125['rule_days']:.2f} truth {w125['true_days']:.2f}")
c.expect(rule_owed(G_gate(h4), h4["raw_bytes"], G_actual(h4), G_actual(h4) + 50 * MB, 0, True) == 0, "once every census-era and post-census row is folded G_owed is 0 and only the rebuild headroom is still subtracted")

# ---- 7. the census obligation (should-fix 5): fresh, after the catch-up, and only with room for the VACUUM INTO copy ---------------
def census_preflight(census_day, catchup_complete_day, decision_day, free_bytes, ledger_bytes, reserve_bytes, during_hold=False, rebuild_headroom=0, g_owed=0):
    if not R8: return {"ok": True, "reasons": []}                                # round 7: a one-time census with no freshness or space rule
    reasons = []
    if census_day < catchup_complete_day: reasons.append("census_before_catchup")
    if decision_day - census_day > 1: reasons.append("census_stale")
    if free_bytes < ledger_bytes + reserve_bytes: reasons.append("no_space_for_vacuum_into")
    if R9 and during_hold and free_bytes - ledger_bytes < reserve_bytes + rebuild_headroom + g_owed:
        reasons.append("copy_would_trip_rung")                                    # round 9: the re-census copy must leave the ladder's terms intact
    return {"ok": not reasons, "reasons": reasons}
L = h4["ledger"]
stale = census_preflight(census_day=0, catchup_complete_day=3, decision_day=7, free_bytes=40 * GiB, ledger_bytes=L, reserve_bytes=reserve)
tight = census_preflight(census_day=7, catchup_complete_day=3, decision_day=7, free_bytes=L + reserve - 1, ledger_bytes=L, reserve_bytes=reserve)
fresh = census_preflight(census_day=7, catchup_complete_day=3, decision_day=7, free_bytes=L + reserve, ledger_bytes=L, reserve_bytes=reserve)
c.expect(not stale["ok"] and set(stale["reasons"]) == {"census_before_catchup", "census_stale"}, "a census taken before the catch-up or more than a day before the S1b decision gives no gate value", str(stale))
c.expect(not tight["ok"] and tight["reasons"] == ["no_space_for_vacuum_into"], "the VACUUM INTO copy is refused unless free space >= ledger bytes + reserve", str(tight))
c.expect(fresh["ok"], "a fresh post-catch-up census with room for the copy is accepted", str(fresh))

# ---- 8. segments (should-fix 5): a counted proxy when the census has one; else 1.2 x sessions, declared covered by the 1.25 --------
seg_share = (segment_rows(h4) + h4["rollup_buckets"] + h4["day_targets"]) * SEG / G_actual(h4)
print(f"    studio4 segment term: {100 * seg_share:.1f}% of G at 1.2 x sessions (a 2x split error is {100 * seg_share:.1f}% of G, inside the 25% factor)")
with_proxy = dict(h4, segment_proxy=2.0 * h4["sessions"])       # a census that counted sessions split at 7 days: 2 segments per session
c.expect(G_actual(with_proxy) > G_actual(h4) and G_gate(with_proxy) > G_gate(h4), "a census that carries a segment proxy above 1.2 x sessions raises G_host and G_gate; the geometry ratio is only the fallback", f"G with proxy {G_actual(with_proxy) / MB:.1f} MB vs {G_actual(h4) / MB:.1f} MB")
c.expect(seg_share < 0.03, "sanity: on Studio4 the segment term is under 3% of G, so the terminal-pause splits the proxy does not count are covered by the 1.25 factor", f"{100 * seg_share:.1f}%")

# ---- 9. should-fix 5 (review r2): C_conv is MEASURED as the converter's own page allocation, not inferred --------------------------
# A page-level model of the ledger file: the converter and the live writer take turns as SQLite's single writer. Each write
# transaction allocates whole pages (freelist pages first, then new pages). The converter measures its own transaction as
# delta(page_count - freelist_count) x page_size and sums it into its checkpoint. Round 8's text ("its own counter") had no
# basis: an implementer could sum the ESTIMATED widths of the rows it wrote (the only byte figure the plan gives), which is off by
# the width factor; round 7 used the lean tables' page count, which holds the live writer's pages too.
PAGE = 4096
class LedgerFile:
    def __init__(self, pages, freelist): self.page_count, self.freelist = pages, freelist
    def live_pages(self): return self.page_count - self.freelist
    def write(self, bytes_):
        need = -(-bytes_ // PAGE)                                                 # whole pages, ceiling
        reuse = min(need, self.freelist); self.freelist -= reuse; self.page_count += need - reuse
def convert_with_live_writer(width_factor, chunks=40, rows_per_chunk=5000, live_rows_between=800):
    ledger, est_row, true_row = LedgerFile(pages=367_296, freelist=1_200), (F + T) * 0.4 + I, ((F + T) * 0.4 + I) * width_factor
    checkpoint, estimated_counter, lean_pages_before, true_bytes = 0, 0, ledger.live_pages(), 0
    for _ in range(chunks):
        before = ledger.live_pages()                                              # PRAGMA page_count, freelist_count at BEGIN
        ledger.write(rows_per_chunk * true_row); true_bytes += rows_per_chunk * true_row
        checkpoint += (ledger.live_pages() - before) * PAGE                       # committed with the chunk
        estimated_counter += rows_per_chunk * est_row                            # round 8 reading: rows x estimated widths
        ledger.write(live_rows_between * true_row)                                # the live writer's own transactions (dual-write), between the converter's
    lean_pages_now = (ledger.live_pages() - lean_pages_before) * PAGE            # round 7: every lean-table page, both writers
    return {"true_bytes": true_bytes, "measured": checkpoint, "estimated_counter": estimated_counter, "lean_pages_now": lean_pages_now, "chunks": chunks}
def c_conv_basis(m):
    if rule == "r7" or rule == "r6": return m["lean_pages_now"]                  # rounds 6-7: the lean tables' page count
    if not R9: return m["estimated_counter"]                                      # round 8: a counter with no stated basis (rows x estimated widths)
    return m["measured"]                                                          # round 9: the converter's own page allocation
m125 = convert_with_live_writer(1.25)
basis = c_conv_basis(m125)
print(f"    C_conv at width factor 1.25: true {m125['true_bytes'] / MB:.1f} MB, converter's page allocation {m125['measured'] / MB:.1f} MB, rows x estimates {m125['estimated_counter'] / MB:.1f} MB, lean pages {m125['lean_pages_now'] / MB:.1f} MB; rule uses {basis / MB:.1f} MB")
c.expect(abs(basis - m125["true_bytes"]) <= m125["chunks"] * PAGE, "C_conv equals the bytes the converter really wrote (within one page per transaction) when true widths are 1.25 x the estimates: it is measured from the converter's own page allocation, not inferred from row widths or read from the lean tables", f"rule {basis / MB:.2f} MB vs true {m125['true_bytes'] / MB:.2f} MB")
c.expect(basis < m125["lean_pages_now"] - 1, "the live writer's interleaved dual-write pages are not in C_conv (a lean-table page count would include them)", f"C_conv {basis / MB:.1f} MB, lean pages {m125['lean_pages_now'] / MB:.1f} MB")
m080 = convert_with_live_writer(0.8)
c.expect(abs(c_conv_basis(m080) - m080["true_bytes"]) <= m080["chunks"] * PAGE, "the same measurement is exact when true widths are BELOW the estimates (0.8 x): the basis does not depend on the width factor", f"rule {c_conv_basis(m080) / MB:.2f} MB vs true {m080['true_bytes'] / MB:.2f} MB")
# U in the census's raw-bytes basis (definitional; green under r8 and r9 because the fixture already used bytes per raw row)
raw_bytes_per_row = h4["raw_bytes"] / h4["raw"]
U_rows = 12_345
c.expect(abs(rule_owed(G_gate(h4), h4["raw_bytes"], G_gate(h4), 0, U_rows * raw_bytes_per_row, False) - (G_gate(h4) / h4["raw"]) * U_rows) < 1e-6,
         "U is the unfolded post-census raw rows x the census's bytes per raw row, so g_gate x U equals G_gate per raw row x rows (the census's basis, not payload bytes)", f"g_gate x U = {rule_owed(G_gate(h4), h4['raw_bytes'], G_gate(h4), 0, U_rows * raw_bytes_per_row, False) / MB:.2f} MB")

# ---- 10. should-fix 6 (review r2): the re-census copy during the hold must not trip a rung ---------------------------------------
sc = simulate(h4, 40 * MB, 14, 2, 0.6)
rebuild_hd, g_owed_now = sc["rebuild"], sc["owed"]
room_for_copy_only = L + reserve + rebuild_hd / 2                      # passes round 8's free >= ledger + reserve; the copy would eat into the rebuild headroom
mid_hold = census_preflight(census_day=20, catchup_complete_day=3, decision_day=20, free_bytes=room_for_copy_only, ledger_bytes=L, reserve_bytes=reserve, during_hold=True, rebuild_headroom=rebuild_hd, g_owed=g_owed_now)
runway_after_copy = (room_for_copy_only - L - reserve - rebuild_hd - g_owed_now) / sc["growth"]
print(f"    re-census during the hold with free = ledger + reserve + rebuild/2: runway after the copy would be {runway_after_copy:.2f} d; preflight {mid_hold}")
c.expect(not mid_hold["ok"] and "copy_would_trip_rung" in mid_hold["reasons"], "the re-census copy before S3 is refused when free - ledger < reserve + rebuild headroom + G_owed (the copy itself would put the runway under a rung: here negative), even though ledger + reserve is free", str(mid_hold))
enough = census_preflight(census_day=20, catchup_complete_day=3, decision_day=20, free_bytes=L + reserve + rebuild_hd + g_owed_now + 30 * sc["growth"], ledger_bytes=L, reserve_bytes=reserve, during_hold=True, rebuild_headroom=rebuild_hd, g_owed=g_owed_now)
c.expect(enough["ok"], "a re-census copy that leaves reserve + rebuild headroom + G_owed and 30 days of growth free is accepted", str(enough))
c.finish()
