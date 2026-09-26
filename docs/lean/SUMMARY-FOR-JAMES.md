# Lean, decision-grade Plimsoll: what changed in B0's eighth round (round 14)

**2026-09-26.** Still a plan, not a change; nothing was deployed, pushed or written anywhere, and no live ledger was read. You signed
eight contracts and C4. An independent reader re-checked the ownership rule (C1): last round's two repairs hold in the
replays, but neither promise was pinned on the wire by a test. Both gaps are closed here, with the reader's two smaller items.

## What the reader found, and what changed

- **The refusal's shape was not nailed down.** The cloud's "this identity belongs to another chain" answer names the chain it judged,
  but the two contracts named its first field differently, with no translation: a collector built to the letter would not have
  recognised the refusal. Now both sides use one name, a pending cloud test checks the exact answer the route sends, and a pending
  collector test feeds that same answer, byte for byte, through the collector's real upload path: set aside under the chain named,
  released when the cloud next names a different chain, delivered once. Two edge cases are tested: a refusal naming no chain sets
  nothing aside (the row is retried as usual), and an unknown chain is taken at face value.
- **The "give up after three tries" answer was not pinned either.** The cloud's tests checked only a one-try override, and the route's
  generic error handler would have flattened the answer to "unavailable". Now a pending test forces three head-on collisions
  in a row and checks that the third is the final answer and there is never a fourth try; another checks the exact answer the route
  sends with its "try again in N seconds" header; and a collector test checks that on it nothing is acknowledged or set aside, and
  the batch goes again when the header says.
- **One clarification.** A retried operation reads everything afresh except the chain the request was checked in under, which stays
  fixed, so a retry after an admin's merge refuses cautiously and the next response resolves it.

## Ready to freeze, and not

Signed: the eight round-9 contracts and C4. **Ready after one more independent read:** the ownership rule (C1), because the wire
shapes, the pinned retry limit and the fixed-context rule are new text no reviewer has read. **Not yet:** the runway numbers and any
hold on any Mac, the deletion proof as a whole, the future claim and fault rules, the fleet half of dispatch tagging.

## Test state

Collector 62 tests (16 green guards, 46 pending); cloud 44 (3 green, 41 pending); both exit 0, every pending test failing at the
surface its bead will build. Typecheck, lint and the coverage gate pass locally. The rule model has 81 checks:
green under this round's rule, red under every earlier one. The collector's answers to both error bodies were checked against
today's code; no database cluster was run. One commit per item; no product code.

## The one decision for you (unchanged)

How much history each machine keeps locally: today 90 days of raw rows; recommended 30 days of usage facts, with the cloud as
the system of record.
