# Lean, decision-grade Plimsoll: what changed in B0's third round (round 9)

**2026-09-25.** Still a plan, not a change; nothing was deployed, pushed or written anywhere, and no live ledger was read. The
independent review of B0's second round said: the first wave can continue and most contracts can be signed, but the ownership rule
has three gaps and two promised tests still could not pass. This round closes those and the review's nine smaller items.

## What the review found, and what changed

- **A Mac's old identity could still speak for it after a re-join.** A reply from the *old* install can arrive after a re-join (an
  answer already in flight, or a collector process that read its settings before the re-join); last round's rule let it overwrite
  the new stamp, so the new person's rows were exported as the old person's. Now the ledger records at join time which install it
  belongs to; only that install's replies count, and any other reply is ignored and counted.
- **A rare race could flip a verdict.** The cloud decides each stamp the first time it sees it; if an admin re-bound the Mac at
  that instant, one path could say "issued" while the record said "not issued". The rule now locks the install's row while it
  decides, so the re-bind waits: 12 interleavings, none flips (last round's rule flipped in 1), reproduced in a real PostgreSQL
  database on Studio4, with a database proof pending in the cloud repository.
- **One install could poison another's future.** Any install of a workspace could name another install's next version before it
  existed and make that version worthless. The "not issued" record is now kept per uploading install.
- **Two promised tests could never pass.** Both were set-up defects (a timestamp rewritten after the fact; a row inserted by hand
  with no delivery record). Both are repaired, with two green guards proving their set-up. Last round's summary said all eight
  tests landed; that was true for seven.

Also done: "not issued" records, binding history and old installs are kept until a workspace is erased, and erasure removes them
in order; the runway rule says how the converter measures its own bytes (the pages it allocates, not estimated row sizes) and refuses
a census copy that would itself trip an alarm; the usage-record hash no longer ignores the case of a quoted value; the claim that the
runway rule "self-corrects" to real row sizes is withdrawn (exact at the 25% allowance, safe only up to it).

## Ready to freeze, and not

Ready for your signature (`FREEZE.md`): the runway formula and census obligation, the reject table, the open-gap rule, the
usage-record pin, the day acknowledgement, the day keys, the token-volume gate, the abort size check. **Ready after one more
independent read:** the ownership rule, because this round changes which install's replies count and how the "not issued" record is
keyed. **Not yet:** the runway numbers and any hold on any Mac, the deletion proof as a whole, the future claim and fault rules, the
fleet half of dispatch tagging.

## Test state

Collector 51 tests (15 green guards, 36 pending); cloud 31 (3 green, 28 pending); both exit 0, every pending test failing at the
surface its bead will build. Typecheck, lint and the coverage gate pass locally, not in GitHub Actions. One commit per item; no
product code.

## The one decision for you (unchanged)

How much history each machine keeps locally: today 90 days of raw rows; recommended 30 days of usage facts, with the cloud as
the system of record.
