# Lean, decision-grade Plimsoll: what changed in B0's second round (round 8)

**2026-09-25.** Still a plan, not a change. Nothing was deployed, pushed or written to any machine or the cloud; the round was
read-only toward every live collector and hosted service and did not re-read any live ledger. The independent review of B0's
first round said: start the first wave, but repair two contracts and the test helper before signature. This round makes those
repairs and the smaller items on the review's list.

## What the review found, and what changed

- **Who owns a row could still change with time.** Last round's rule checked a row's binding-version stamp against the versions
  the cloud had issued *at the moment of checking*. A row is checked when it reaches the cloud and again when its session summary
  arrives, so a stamp for a version issued in between was "nobody's" on one path and a real person's on the other; an honest Mac
  could produce one after re-joining the workspace. Now the stamp names the install and the version, and the cloud decides each
  pair **once, the first time it sees it**, and writes the decision down, so every later check agrees. A Mac clears its stamp when
  it re-joins; rows still queued at the re-join keep their old stamp; a stamp the cloud had not issued when it first saw it stays
  "nobody's" for good, is listed, and is repaired by re-binding the Mac. Test: `b4_offline_rebind` (36 checks, red under both
  earlier rules, green now), ten cloud and six collector cases.
- **The disk-runway figure was too optimistic during history conversion.** The rule counted every byte in the new tables as
  conversion done, but the live writer fills those tables before the conversion starts, and rows added after the one-time census
  were never counted as work owed: the display could say 5.0 days when the truth was 2.45. Now only the converter's own bytes count
  as progress, rows admitted after the census count as work owed, and the census is taken after the catch-up, at most a day before
  the decision, again before conversion, and only with room for its copy. The fixture's "truth" is a separate byte-level model, so
  the check can fail; it shows the rule never overstates the runway while real row sizes stay within the 25% allowance, which is
  why the **numbers** still wait for the Studio5 measurement.
- **Eight tests could never pass.** The helper stamped test events at a fixed 2026-09-25 time while the temporary ledger's
  enrollment started when the test ran, so every append was refused. The helper now pins the enrollment at 2026-01-01, and a
  green guard proves every configuration the tests use stores its row.

Also done: the reject table is in the never-delete set with a test; the usage-record rule the cloud and the collector share is
pinned by one hash in both repositories; the census counts sessions split at seven days; the hygiene items are fixed. Last
round's summary is corrected here: it did **not** close the two gaps, and its runway rule under-counted the work owed.

## Ready to freeze, and not

Ready for your signature (`FREEZE.md`): the ownership rule (after one more independent read, because the repair changes what
travels on the wire), the runway formula and census obligation, the reject table, the open-gap rule, the usage-record pin, the
day acknowledgement, the day keys, the token-volume gate and the abort size check. **Not yet:** the runway numbers and any hold on
any Mac, the deletion proof as a whole, the future claim and fault rules, the fleet half of dispatch tagging.

## Test state

Collector 46 tests (12 green guards, 34 pending); cloud 23 (2 green, 21 pending); both exit 0, every pending test failing at the
surface its bead will build. Typecheck, lint and the coverage gate pass locally, not in GitHub Actions. Ten commits, one per item,
no product code.

## The one decision for you (unchanged)

How much history each machine keeps locally: today 90 days of raw rows; the recommendation is 30 days of usage facts, with the
cloud as the system of record.
