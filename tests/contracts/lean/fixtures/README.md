# Rule models (Python) behind the pending contract tests

The in-memory rule models of the lean-Plimsoll plan (rounds 5-14), copied from the program's contract bundle so an implementer
can replay a rule before writing code: `python3 <fixture>.py --rule r6|r7|r8|r9|r10|r11|r12|r13|r14` (each is red under the round it corrects and
green under the round that fixed it; the round-14 fixture `b4_offline_rebind.py` is red under r6, r7, r8, r9, r10, r11, r12 and r13 and green
under r14, the round-9 fixture `s1b_runway_host_bound.py` red under r6, r7 and r8 and green under r9; `_common.py` is the shared helper). `b22_false_complete.py` reads `docs/lean/` at the repository root
by default. `host_cardinality_census.py` is B1's read-only census tool (run on a `VACUUM INTO` copy only) and
`usage_record_predicate.sql` the pinned `<UR>` text. These are not run by CI; the TypeScript tests one directory up are the
contract tests.
