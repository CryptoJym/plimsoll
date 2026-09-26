# Lean, decision-grade Plimsoll: what changed in B0's fourth round (round 10)

**2026-09-25.** Still a plan, not a change; nothing was deployed, pushed or written anywhere, and no live ledger was read. You signed
eight contracts and then the joined-install rule (C4). An independent reader then checked the ownership rule (C1) and found two gaps.
Both are closed in this round, with the reader's smaller items.

## What the reader found, and what changed

- **The rule said which lock to take but not what to read under it.** Deciding a stamp needs three facts: the Mac's current binding
  version, its binding history, and the record of stamps already refused. Last round locked only the first. If a server read the
  refusals *before* taking the lock, one row could be "nobody" on one path and a person on the other (8 of 566 possible schedules, and
  reproduced in a real database). Now the lock comes first, then all three reads, and the cloud's ingest surface must use the view
  loaded that way, never one loaded earlier.
- **A refusal recorded by a Mac's old identity did not bind its new identity.** After a Mac re-joins, its old identity still judges
  activity summaries while the new one delivers the rows. The refusal record was kept per uploading identity, so the same row could be
  "nobody" in the summary and someone's when delivered, if an admin re-bound the old identity in between. Now the record is kept per
  **ledger**: the chain of identities of one Mac. When a Mac re-joins, it proves it held the previous identity's key, and the cloud links
  the two. Another Mac still cannot poison this one's future (last round's protection stays), and a re-join that cannot prove the link
  is shown as such.

Also done, all from the reader's list: the database proof that was promised last round could not have run (five set-up faults) and
would have passed without the lock; both are fixed, and the set-up was run step by step against today's schema. The erasure-order
check now looks where erasure actually orders its steps. The claim that a database key "refuses" deleting a Mac's identity was
overstated; a real guard now allows such deletes only during workspace erasure. A stamp is judged against the identity it names even
after an admin revokes that identity. The join test now drives the real join code with a fake cloud, and the two half-joined states are
named in `/status`. The echo every request carries now names the identity it is for.

## Ready to freeze, and not

Signed: the eight round-9 contracts and C4. **Ready after one more independent read:** the ownership rule (C1), because the lock order
and the ledger key are new text no reviewer has read, and the reader asked for exactly that. **Not yet:** the runway numbers and any
hold on any Mac, the deletion proof as a whole, the future claim and fault rules, the fleet half of dispatch tagging.

## Test state

Collector 56 tests (16 green guards, 40 pending); cloud 36 (3 green, 33 pending); both exit 0, every pending test failing at the
surface its bead will build. Typecheck, lint and the coverage gate pass locally, not in GitHub Actions. The rule model has 51 checks:
green under this round's rule, red under every earlier one. One commit per item; no product code.

## The one decision for you (unchanged)

How much history each machine keeps locally: today 90 days of raw rows; recommended 30 days of usage facts, with the cloud as
the system of record.
