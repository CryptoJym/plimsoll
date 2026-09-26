#!/usr/bin/env python3
"""Should-fix 5 (Q21): round 4 declared an unresolved file's gap as [epoch start, last_write_at], asserting no
event in the file can be later than the file's last write. A writer can stamp an event 10:05 into a file
last written at 10:00; when the file is first parsed at 11:00 the intake clamp (normalizer.ts:203-214) only
tests the stamp against the read time and keeps it. So a cloud period after 10:00 was called complete while
an unparsed event lay inside it (reviewer-counterexamples.log 'unresolved file last-write can precede ...').

Round-5 rule (ARCHITECTURE.md §3.6, §6.2): an unresolved or never-read file's gap is OPEN-ENDED
(`interval_basis = 'epoch_open'`, `ended_at_ms = null`); `captureCoverageForPeriod` treats an open gap as
overlapping every period from its start onward; the gap closes only when the file is parsed to its end
(then it is resolved, or replaced by counted gaps for the records that were refused).
"""
from _common import Checks, rule_arg

rule = rule_arg()
c = Checks("sf5_unresolved_file_open_gap", rule)
EPOCH_START, LAST_WRITE, FIRST_PARSE = 0, 10_00, 11_00        # minutes-as-integers: 10:00, 11:00
STAMPED_EVENT = 10_05

def declare_gap():
    return {"started_at": EPOCH_START, "ended_at": LAST_WRITE if rule == "r4" else None, "resolved": False}

def overlaps(gap, start, end):
    """Closed interval overlap (contract.ts:407-424); an open gap extends to +infinity."""
    if gap["resolved"]: return False
    gap_end = gap["ended_at"] if gap["ended_at"] is not None else float("inf")
    return gap["started_at"] < end and gap_end >= start

def coverage_complete(gaps, start, end, through):
    return through >= end and not any(overlaps(g, start, end) for g in gaps)

gap = declare_gap()
# The cloud claim advanced to 11:00 on the strength of other files; the unresolved file is declared as a gap.
period = (10_01, 11_00)     # a period after the file's last write, containing the stamped-but-unparsed event
complete_before_parse = coverage_complete([gap], *period, through=11_00)
c.expect(not complete_before_parse, "a period after last_write_at is NOT complete while the file is unparsed", f"gap={gap} period={period} complete={complete_before_parse}")
c.expect(EPOCH_START <= STAMPED_EVENT and (gap["ended_at"] is None or STAMPED_EVENT <= gap["ended_at"]),
         "the 10:05 event lies inside the declared gap", f"gap end={gap['ended_at']}")
# whole-epoch state for G1: every period from the epoch start is incomplete until the parse
c.expect(all(not coverage_complete([gap], s, s + 60, through=11_00) for s in range(0, 11_00, 60)), "G1: every period of the epoch is incomplete until the file parses")
# After the parse at 11:00 the file's records are admitted (the clamp keeps 10:05, normalizer.ts:203-214) and the gap resolves.
gap["resolved"] = True
c.expect(coverage_complete([gap], *period, through=11_00), "after the file is parsed to its end the gap resolves and the period can be complete")
c.finish()
