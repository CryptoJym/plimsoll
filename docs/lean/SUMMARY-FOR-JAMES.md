# Lean, decision-grade Plimsoll: what changed in B0's sixth round (round 12)

**2026-09-26.** Still a plan, not a change; nothing was deployed, pushed or written anywhere, and no live ledger was read. You signed
eight contracts and the joined-install rule (C4). An independent reader re-checked the ownership rule (C1) once more: both of last
round's fixes hold (the reader's own database replay and a 576-case enumeration found no row handed across Macs), and one new gap
appeared, in the admin's repair tool. It is closed in this round, with the reader's one smaller item.

## What the reader found, and what changed

- **The admin's link could split a Mac's chain of identities.** When a Mac re-joins without proving its old key, the cloud treats it as
  a new chain and holds its old rows until an admin links the new chain into the old one. Last round's link moved only the one
  identity the admin named, but moved every refusal record of its chain. If the Mac had re-joined again since, this time proving its
  key, so that a newer identity hung off the unlinked one, the newer identity was left behind in an emptied chain while its refusal
  record moved away, and a row the cloud had marked "nobody's" could later become someone's. Now the link moves the whole chain:
  every identity in it and every record, in one step, under one set of locks, with one audit row saying who moved what and when. A
  link into an identity that is not the head of its own chain, or into another workspace, is refused, and the refusal names the
  right head. A join that arrives while a chain is being moved waits and lands in the merged chain.
- **Also done:** the database test's second Mac is now seeded to the person the reader's scenario describes, so the pending test
  matches that scenario exactly; and every response now names the Mac's chain, so a Mac whose chain was merged knows to resend the
  rows it had set aside.

## Ready to freeze, and not

Signed: the eight round-9 contracts and C4. **Ready after one more independent read:** the ownership rule (C1), because the
whole-chain link is new text no reviewer has read. **Not yet:** the runway numbers and any hold on any Mac, the deletion proof as a
whole, the future claim and fault rules, the fleet half of dispatch tagging.

## Test state

Collector 57 tests (16 green guards, 41 pending); cloud 40 (3 green, 37 pending); both exit 0, every pending test failing at the
surface its bead will build. Typecheck, lint and the coverage gate pass locally, not in GitHub Actions. The rule model has 69 checks:
green under this round's rule, red under every earlier one. The reader's two counterexamples were reproduced in a real PostgreSQL 17
database and shown closed under this round's rule, including a join racing the move in both orders. One commit per item; no product
code.

## The one decision for you (unchanged)

How much history each machine keeps locally: today 90 days of raw rows; recommended 30 days of usage facts, with the cloud as
the system of record.
