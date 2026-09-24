# Busy-host capture: the admission ceiling, service order and Grok walk

eco-6hoxj.163.42. How automatic maintenance shares one cadence among repairs,
Codex, Claude and Grok on a busy host, and what each bound does and does not
promise. Proven by `pnpm proof:busy-host-capture`,
`pnpm proof:grok-worker-replacement` and
`pnpm proof:grok-starvation -- --expect=green`, all on virtual time
(`scripts/lib/virtual-clock.ts`), so they give the same result on any
machine.

## 200 ms is an admission ceiling

`CaptureWorkBudget` (`packages/collector-cli/src/capture-work-budget.ts`)
gives a cadence 200 ms of wall time, 512 KiB, 512 records and 512 events.
The wall is an **admission** ceiling, not a limit on how long a cadence runs:

- Once 200 ms have passed, neither the cadence's budget nor any source's
  scope of it admits another unit of work. A source scope can narrow the
  cadence's clock, never extend it.
- One exception: when the previous cadence's capture leader never started
  because the clock was already spent, this cadence's leader starts its
  first unit even if the clock is spent again (`pastSpentWall`). The byte,
  record and event ceilings still hold. See the service order below.
- A unit that has started finishes. Its reads stop at `unitDeadline()`,
  except for the first unit of a source's turn, which has **no wall
  deadline**: it is bounded only by its byte and record slice (one JSONL
  slice of at most 64 KiB, or one Grok usage file of at most 512 KiB).
  Abandoning it after a slow synchronous call has already been paid for
  would pay that call again next cadence and never commit.
- So a cadence ends when its last admitted unit returns, which one slow
  synchronous call can put far past 200 ms. The finite outer guard in
  production is the maintenance child's process boundary: a 30 s job
  deadline with a 1 s teardown margin, then TERM (250 ms grace) and KILL
  (750 ms grace).

## Service order: every source leads within 2S cadences

Capture sources (Codex, Claude and, with a Grok tailer, Grok) lead in a fixed
rotation (`maintenance.ts`, `automatic_capture_source_turn`):

- The lead passes on once its holder has had its turn: it started with the
  cadence clock open, whether it then committed, failed, or was killed with
  its worker. Only a leader the clock never let start keeps the lead.
- Repairs and capture alternate which goes first. A cadence whose leader
  never started, because repairs or the bookkeeping before capture spent the
  allowance, hands the next cadence to capture whatever its parity, and that
  cadence's leader starts its first unit whatever the clock
  (`automatic_capture_fairness_v1`: `captureFirst`, `leaderDenied`). Both are
  used up by that cadence; `deniedCadences` counts the denials.

The rotation therefore moves in at least one of every two cadences, and each
of S sources leads within **2S cadences** (6 with Grok), however long the
bookkeeping before capture takes. As the leader, its first unit is admitted
and allowed to finish. A source that keeps failing cannot keep the lead, so
it cannot keep the others out. Every cadence records how much of the clock
was gone when the leader's turn came (`captureTurn.preCaptureMs`) and
whether the leader started past a spent clock (`captureTurn.leaderOverride`).

The bookkeeping that matters is `captureBaselineStatus`, which aggregates
the capture baseline's generation table once per JSONL source. Measured on
Studio1 (load average near 9) with a completed baseline of N files per
source: 5 ms at N = 10,000, 48 ms at 50,000, and about 600 ms at 200,000 (a
216 MB ledger). At that size every cadence spends its allowance before
capture, and the leader starts on every second cadence.

## Grok walk: every session within a stated number of passes

A Grok sweep is a **round** over every group and session that existed when it
began (`grok-usage-tailer.ts`, `grok_usage_walk_round_v1`):

- **Walk first.** Every discovery pass starts with the walk and runs it
  until it has covered one group or session, whatever the pass's wall or
  the cadence clock. That unit opens at most the root and one group and
  examines at most one session, within the pass's step and read
  allowances. A worker replaced after every pass therefore still moves the
  round.
- **Recent lane.** Next, a round ranks the groups modified within 48 hours
  and queues changed usage files written in that window. It takes at most
  half of a pass's wall and of its steps, and never replaces the walk. When
  it last started is durable (`recentAtMs`), so a replaced worker does not
  start it again until 10 minutes have passed.
- **Walk.** It streams `sessions/` and each unfinished group in directory
  order. A step opens a directory or examines one entry the round has not
  covered (at most two lstats). Every covered session and finished group is
  recorded in `grok_usage_walk_visits` as hashes of their names, written at
  the end of each pass, so a restarted worker skips them and continues.
  Reading past covered entries costs no step; it is bounded by the pass's
  wall and 32 reads per step.
- **Churn.** A group or session created after the round began waits for the
  next round (`groupsDeferred`, `sessionsDeferred`), so entries added ahead
  of an old session cannot keep taking its place.
- **Caps.** `maxGroups` and `maxSessionsPerGroup` limit what one pass opens
  and observes. The rest moves to a later pass of the same round and is
  counted (`groupsOverLimit`, `sessionsOverLimit`); nothing is dropped.
- **Bounded state.** A finished group keeps one row; its session rows are
  deleted with the pass that finishes it. The state therefore holds one row
  per finished group plus the covered sessions of the groups still being
  walked (`walkStateRows`). A round whose state passes `maxWalkStateRows`
  (200,000) ends early, reported incomplete (`walkStateLimitReached`), and
  the next round starts over with every entry present then.
- **Honest completion.** Whether a round saw an error, or had a worker close
  with a queued usage file unread, is durable (`unclean`), so a round
  finished by a later worker is not reported clean. The next round reads
  what was missed.

Bounds:

- Every pass covers at least one group or session, so a round of N sessions
  in G groups is finished within N + G passes whatever the worker's
  lifetime (for a worker replaced every pass, within the read allowance
  below). With the reviewer's Studio0-scale tree (4,000 sessions in 20
  groups, 12 ms per group lstat, a 50 ms pass), a worker replaced every
  pass finishes one group beside the recent lane in the first pass and at
  least ⌊50 / 12⌋ = 4 in every later one: at most 1 + ⌈19 / 4⌉ = 6 passes.
- A session present when a round begins is observed during that round. A
  session created during a round is observed in the next round, so every
  session is reached within two rounds.
- For a worker replaced every pass, the walk's first unit also has to read
  past the covered entries ahead of the next uncovered one within the
  pass's 65,536 reads; the wall does not stop it. On Studio1 (load average
  near 9), a new worker read past 10,000 covered entries in 20–29 ms and
  past 40,000 in 90–107 ms, and could not get past 70,000. The production
  maintenance child is persistent (`maintenance-boundary.ts`), and a
  persistent worker keeps its directory streams open across passes, so it
  has no such limit.
- The walk state never holds more than `maxWalkStateRows` plus one pass's
  rows. A round that needs more (a single group larger than the limit, or
  churn that adds that many entries within one round) is reported
  incomplete every time and cannot finish; that is beyond the collector's
  stated scale (`lifetimeEntryLimit` 200,000).
- The durable walk state holds only name hashes and numbers; it names no
  path.
