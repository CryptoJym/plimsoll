# Lean, decision-grade Plimsoll: what changed in B0's seventh round (round 13)

**2026-09-26.** Still a plan, not a change; nothing was deployed, pushed or written anywhere, and no live ledger was read. You signed
eight contracts and C4. An independent reader re-checked the ownership rule (C1) once more: last round's
repair of the admin's link holds (the reader's own database replay agrees), and two smaller gaps appeared around it. Both are closed
here; the reader had no smaller items.

## What the reader found, and what changed

- **An upload and the admin's link could jam each other.** An upload naming two identities takes its locks in one order; the link
  takes them in another. In the reader's database the two met head-on, the database cancelled the link, and nothing said what
  happens next: the link silently did not happen, and the rows it should have released stayed parked. Now an operation the database
  cancels this way is run again from the beginning, at most three times, and nothing is answered, recorded or receipted until the
  attempt that did the work has committed. If the third attempt is cancelled too, the caller gets an ordinary
  "try again later" answer: a collector already retries those on its own, and an admin re-issues the link. A link that
  succeeds on its second try releases the parked rows exactly like a first-try link.
- **The refusal did not say which chain it judged.** When the cloud refuses a row because it names an identity from another chain, the
  collector sets the row aside and resends it only once the cloud later names a different chain. But the refusal never said which
  chain it had judged, and in one race (the admin merges the chain between the request's check-in and its judgment) the natural guess,
  the current chain, would leave the row set aside for ever. Now the refusal names the chain it judged, and the collector sets the row
  aside under exactly that value. The reader's race then resolves on the next response.

## Ready to freeze, and not

Signed: the eight round-9 contracts and C4. **Ready after one more independent read:** the ownership rule (C1), because the retry rule
and the refusal's chain are new text no reviewer has read. **Not yet:** the runway numbers and any hold on any Mac, the deletion proof
as a whole, the future claim and fault rules, the fleet half of dispatch tagging.

## Test state

Collector 58 tests (16 green guards, 42 pending); cloud 41 (3 green, 38 pending); both exit 0, every pending test failing at the surface
its bead will build. Typecheck, lint and the coverage gate pass locally, not in GitHub Actions. The rule model has 76 checks: green under this
round's rule, red under every earlier one. Both gaps were reproduced in a real PostgreSQL 17 database, in both orders of the
head-on meeting, and closed under this round's rule. One commit per item; no product code.

## The one decision for you (unchanged)

How much history each machine keeps locally: today 90 days of raw rows; recommended 30 days of usage facts, with the cloud as
the system of record.
