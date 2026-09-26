# Lean, decision-grade Plimsoll: what changed in B0's fifth round (round 11)

**2026-09-25.** Still a plan, not a change; nothing was deployed, pushed or written anywhere, and no live ledger was read. You signed
eight contracts and the joined-install rule (C4). An independent reader re-checked the ownership rule (C1): last round's fixes hold, and
two new gaps appeared, both about a stamp that names a different Mac's identity. Both are closed in this
round, with the reader's smaller items.

## What the reader found, and what changed

- **A stamp could hand a row to another Mac's person.** Every uploaded row carries a stamp: the identity and binding version at
  capture. Last round judged that stamp against the identity it named, whichever Mac uploaded it. So a Mac bound to person
  B could upload a row stamped with another Mac's identity, and if an admin had since bound that identity to person C, the row became
  C's. Now ownership never crosses a Mac's chain of identities: a stamp naming an identity outside the uploader's own chain is refused
  outright, nothing is recorded, and the Mac keeps those rows aside. A row belongs to someone who was bound to that Mac, or
  to nobody.
- **A Mac that re-joined without proving its old key could split one row's answer.** When a Mac re-joins and cannot prove it held its
  previous identity's key, the cloud treats it as a new chain, while its old rows still carry the old identity's stamp. Last round
  judged those rows in the new chain's empty view, so a row the old identity's summary had marked "nobody" could become someone's on
  delivery. Now such rows are held, not judged. An admin can link the new identity into the old chain; the held rows are then
  judged in that chain and agree with the summary. Rows released without a link are marked undelivered and never judged. An
  honest Mac that still holds its key links itself at join, so the hold costs it nothing.

Also done: the implementation note that said the refusal record is kept per uploading identity now says per
chain; the database proof stops its test cluster if its set-up fails part way; the join test drives the real join route (reused
tokens, replayed proofs, another workspace's identity); and "another Mac never enters the chain"
now carries its condition: it holds while the previous identity's key stays secret, since a copied key passes the proof.

## Ready to freeze, and not

Signed: the eight round-9 contracts and C4. **Ready after one more independent read:** the ownership rule (C1), because the refusal
across chains, the hold and the admin link are new text no reviewer has read. **Not yet:** the runway numbers and any hold on any
Mac, the deletion proof as a whole, the future claim and fault rules, the fleet half of dispatch tagging.

## Test state

Collector 57 tests (16 green guards, 41 pending); cloud 40 (3 green, 37 pending); both exit 0, every pending test failing at the
surface its bead will build. Typecheck, lint and the coverage gate pass locally, not in GitHub Actions. The rule model has 60
checks: green under this round's rule, red under every earlier one. Both new gaps were reproduced in a real PostgreSQL 17 database and
shown closed under this round's rule. One commit per item; no product code.

## The one decision for you (unchanged)

How much history each machine keeps locally: today 90 days of raw rows; recommended 30 days of usage facts, with the cloud as
the system of record.
