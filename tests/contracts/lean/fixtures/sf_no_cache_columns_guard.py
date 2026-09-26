#!/usr/bin/env python3
"""Should-fix (round 6): the shared event schema admits cache columns on every event (packages/shared/src/schemas.ts:299-305),
but round 5's token volume state read only input and output under `no_cache_columns` and called the volume `known`, so a
record from a source that declares no cache semantics could carry a non-zero cache column into a token share unread
(VERDICT r5 shouldFix 1; ARCHITECTURE r5 §3.1; the round-5 B6 fixture tested non-zero cache only under `unknown`).

Round-6 rule (ARCHITECTURE.md §3.1 'Token volume state', DECISION-METRICS.md §0): under `no_cache_columns` a non-zero cache
column is `ambiguous_semantics` exactly as under `unknown`; null or zero cache columns stay `known`. The cohort's non-`known`
counts withhold every dollar and token-weighted verdict before any share is computed.
"""
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("sf_no_cache_columns_guard", rule)

def normalised(rec):
    r, i, o, cr, cc = rec["rule"], rec["input"], rec["output"], rec["cache_read"], rec["cache_creation"]
    if r == "openai_cached_subset": return None if None in (i, o, cr) else i + o
    if r == "anthropic_cache_exclusive": return None if None in (i, o, cr, cc) else i + cr + cc + o
    return None if None in (i, o) else i + o                      # no_cache_columns and unknown: distinct input = input

def volume_state(rec):
    if rec["event_type"] == "usage_live": return "live_interval"
    cache_nonzero = (rec["cache_read"] or 0) > 0 or (rec["cache_creation"] or 0) > 0
    if rec["rule"] == "unknown" and cache_nonzero: return "ambiguous_semantics"
    if rule == "r6" and rec["rule"] == "no_cache_columns" and cache_nonzero: return "ambiguous_semantics"   # the round-6 guard
    return "known" if normalised(rec) is not None else "unknown_null"

def g3(records):
    states = {}
    for r in records: states[volume_state(r)] = states.get(volume_state(r), 0) + 1
    bad = {k: v for k, v in states.items() if k != "known"}
    if bad: return "withheld", f"unknown-volume records {bad}; record shares only"
    priced = sum(normalised(r) for r in records if r["priced"]); total = sum(normalised(r) for r in records)
    share = priced / total if total else 0.0
    unbounded = sum(normalised(r) for r in records if not r["priced"] and not r["ceiling"])
    return ("dollar_comparison_allowed" if share >= 0.90 and unbounded == 0 else "withheld"), f"pricedShareTokens={share:.1%} unbounded={unbounded}"

def rec(**kw):
    base = {"event_type": "usage_rollout", "rule": "no_cache_columns", "input": None, "output": None, "cache_read": None, "cache_creation": None, "priced": False, "ceiling": True}
    base.update(kw); return base

priced = rec(rule="openai_cached_subset", input=3000, output=0, cache_read=0, priced=True)
grok_nonzero = rec(input=100, output=10, cache_read=900, cache_creation=0)      # a no_cache_columns source reporting 900 cached tokens
v = g3([priced, grok_nonzero])
c.expect(volume_state(grok_nonzero) != "known", "a non-zero cache column on a no_cache_columns source is not a known volume", volume_state(grok_nonzero))
c.expect(v[0] == "withheld", "the cohort withholds the dollar comparison and every token share while that record is present", str(v))
grok_null = rec(input=100, output=10); grok_zero = rec(input=100, output=10, cache_read=0, cache_creation=0)
v2 = g3([priced, grok_null, grok_zero])
c.expect(volume_state(grok_null) == "known" and volume_state(grok_zero) == "known", "null or zero cache columns under no_cache_columns stay known (no over-blocking)")
c.expect(v2[0] == "dollar_comparison_allowed", "an all-known cohort at 90%+ priced with bounded remainders still passes", str(v2))
c.finish()
