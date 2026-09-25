#!/usr/bin/env python3
"""B6: G2/G3 token coverage could pass while an admitted, unpriced usage record has NULL token columns
(null contributed 0 to the denominator), and the `unknown` token rule ignored cache columns so an
exclusive-cache record looked 98.9% covered when it might be 8.3% (VERDICT B6, Q11/Q12;
reviewer-counterexamples.log 'G3 token coverage can show 100% ...' and 'unknown token_rule coverage=98.9% ...').

Round-5 rule (DECISION-METRICS.md §0 'Token volume state', G2, G3): every admitted record gets a
`token_volume_state`: `known` (rule known and every column the rule reads non-null), `unknown_null` (a
needed column is null), `ambiguous_semantics` (rule `unknown` with a non-zero cache column), or
`live_interval` (a `usage_live` placeholder). Any dollar or token-weighted verdict requires
unknown_null = ambiguous_semantics = live_interval = 0 in the cohort BEFORE any share is computed;
otherwise the verdict is withheld and the counts are shown.
"""
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("b6_unknown_volume_gates", rule)

def normalised(rec):
    """Distinct input + output under the record's token_rule (ARCHITECTURE.md §3.1 'Token rules'). Returns None when unknown."""
    r, i, o, cr, cc = rec["rule"], rec["input"], rec["output"], rec["cache_read"], rec["cache_creation"]
    if rule == "r4":
        i = i or 0; o = o or 0; cr = cr or 0; cc = cc or 0            # round 4: null contributed 0
        if r == "openai_cached_subset": return i + o
        if r == "anthropic_cache_exclusive": return i + cr + cc + o
        return i + o                                                  # no_cache_columns and unknown: input only
    if r == "openai_cached_subset": return None if None in (i, o, cr) else i + o
    if r == "anthropic_cache_exclusive": return None if None in (i, o, cr, cc) else i + cr + cc + o
    if r == "no_cache_columns": return None if None in (i, o) else i + o
    return None if None in (i, o) else i + o                          # unknown rule; ambiguity handled by volume_state

def volume_state(rec):
    if rec["event_type"] == "usage_live": return "live_interval"
    if rec["rule"] == "unknown" and ((rec["cache_read"] or 0) > 0 or (rec["cache_creation"] or 0) > 0): return "ambiguous_semantics"
    return "known" if normalised(rec) is not None else "unknown_null"

def g3(records):
    """Returns (verdict, detail). verdict: 'dollar_comparison_allowed' | 'withheld'."""
    if rule == "r5":
        states = {}
        for r in records: states[volume_state(r)] = states.get(volume_state(r), 0) + 1
        bad = {k: v for k, v in states.items() if k != "known"}
        if bad: return "withheld", f"unknown-volume records {bad}; shown as counts, no token share computed"
    priced = sum(normalised(r) for r in records if r["priced"])
    total = sum(normalised(r) for r in records)
    share = priced / total if total else 0.0
    unbounded = sum(normalised(r) for r in records if not r["priced"] and not r["ceiling"])
    ok = share >= 0.90 and unbounded == 0
    return ("dollar_comparison_allowed" if ok else "withheld"), f"pricedShareTokens={share:.1%} unbounded={unbounded}"

def rec(**kw):
    base = {"event_type": "usage_rollout", "rule": "openai_cached_subset", "input": None, "output": None, "cache_read": None, "cache_creation": None, "priced": False, "ceiling": True}
    base.update(kw); return base

# Reviewer case 1: 90 priced tokens plus one admitted unpriced record whose token columns are all null (admitted by type).
case1 = [rec(input=60, output=30, cache_read=0, priced=True), rec(event_type="usage_transcript", rule="anthropic_cache_exclusive")]
v1 = g3(case1)
c.expect(v1[0] == "withheld", "case 1: an unpriced record with null token columns withholds the dollar comparison", f"{v1}")
# Reviewer case 2: 90 priced tokens plus an unpriced `unknown`-rule record with input 1 and cache_read 999.
case2 = [rec(input=60, output=30, cache_read=0, priced=True), rec(rule="unknown", input=1, output=0, cache_read=999, cache_creation=0)]
v2 = g3(case2)
c.expect(v2[0] == "withheld", "case 2: an unknown-rule record with a cache column withholds (98.9% vs 8.3% ambiguity)", f"{v2}")
# No over-blocking: all-known OpenAI records at 95% priced with attested ceilings pass.
case3 = [rec(input=90, output=5, cache_read=85, priced=True), rec(input=5, output=0, cache_read=0)]
v3 = g3(case3)
c.expect(v3[0] == "dollar_comparison_allowed", "case 3: all volumes known, 95% priced, bounded remainder -> allowed", f"{v3}")
# The reviewer's round-3 fixture still computes 85% (not 91.9%) once every column is known.
case4 = [rec(input=85, output=0, cache_read=85, priced=True), rec(input=15, output=0, cache_read=0)]
v4 = g3(case4)
c.expect("pricedShareTokens=85.0%" in v4[1], "case 4: known OpenAI records give 85.0% priced (cached tokens inside input)", f"{v4}")
# A live interval placeholder is not a token measurement and blocks the dollar verdict until its usage record arrives.
case5 = [rec(input=90, output=10, cache_read=0, priced=True), rec(event_type="usage_live", rule="openai_cached_subset")]
v5 = g3(case5)
c.expect(v5[0] == "withheld", "case 5: a usage_live placeholder in the period withholds the dollar comparison", f"{v5}")
c.finish()
