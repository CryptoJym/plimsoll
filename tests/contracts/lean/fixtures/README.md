# Rule models (Python) behind the pending contract tests

The in-memory rule models of the lean-Plimsoll plan (rounds 5-7), copied from the program's contract bundle so an implementer
can replay a rule before writing code: `python3 <fixture>.py --rule r6|r7|r8` (each is red under the round it corrects and green
under the round that fixed it; the two round-8 fixtures, `b4_offline_rebind.py` and `s1b_runway_host_bound.py`, are red under r6
and r7 and green under r8; `_common.py` is the shared helper). `b22_false_complete.py` reads `docs/lean/` at the repository root
by default. `host_cardinality_census.py` is B1's read-only census tool (run on a `VACUUM INTO` copy only) and
`usage_record_predicate.sql` the pinned `<UR>` text. These are not run by CI; the TypeScript tests one directory up are the
contract tests.
