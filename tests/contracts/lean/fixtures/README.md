# Rule models (Python) behind the pending contract tests

The in-memory rule models of the lean-Plimsoll plan (rounds 5-7), copied from the program's contract bundle so an implementer
can replay a rule before writing code: `python3 <fixture>.py --rule r6|r7` (each is red under the round it corrects and green
under the round that fixed it; `_common.py` is the shared helper). `b22_false_complete.py` reads `docs/lean/` at the repository
root by default. These are not run by CI; the TypeScript tests one directory up are the contract tests.
