# Busy-host capture: the admission ceiling, service order and Grok walk

eco-6hoxj.163.42. How automatic maintenance shares one cadence among repairs,
Codex, Claude and Grok on a busy host, and what each bound does and does not
promise. Proven by `pnpm proof:busy-host-capture` and
`pnpm proof:grok-starvation -- --expect=green`, both on virtual time
(`scripts/lib/virtual-clock.ts`), so they give the same result on any
machine.

## 200 ms is an admission ceiling

`CaptureWorkBudget` (`packages/collector-cli/src/capture-work-budget.ts`)
gives a cadence 200 ms of wall time, 512 KiB, 512 records and 512 events.
The wall is an **admission** ceiling, not a limit on how long a cadence runs:

- Once 200 ms have passed, neither the cadence's budget nor any source's
  scope of it admits another unit of work. A source scope can narrow the
  cadence's clock, never extend it.
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
- Repairs and capture alternate which goes first. A repair-first cadence
  that spent the allowance before the capture leader could start hands the
  next cadence to capture (`automatic_capture_fairness_v1`), whatever its
  parity. The hand-off is used up by that cadence.

The rotation therefore moves in at least one of every two cadences, and each
of S sources leads within **2S cadences** (6 with Grok). As the leader, its
first unit is admitted and allowed to finish. A source that keeps failing
cannot keep the lead, so it cannot keep the others out. The bound assumes
that the bookkeeping before capture fits inside the allowance.

## Grok walk: every session within a stated number of passes

A Grok sweep is a **round** over every group and session that existed when it
began (`grok-usage-tailer.ts`, `grok_usage_walk_round_v1`):

- **Recent lane.** A round first ranks the groups modified within 48 hours
  and queues changed usage files written in that window, ahead of the walk.
  It takes at most half of any pass, runs again every 10 minutes of a long
  round, keeps no durable state and never replaces the walk.
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

Bounds:

- A session present when a round begins is observed during that round. A
  pass spends at most two steps reopening directories (after the recent
  lane's share), so each walk step budget of more than two examines at least
  one uncovered entry. When the wall does not bind first, N entries take at
  most ⌈N / (steps − 2)⌉ passes. The production budget is 2,048 steps a pass
  and 1,024 for the walk while the recent lane runs.
- A session created during a round is observed in the next round, so every
  session is reached within two rounds.
- These hold when a new worker replaces the old one every pass, as long as
  one pass can read past a group's covered entries within its wall and its
  65,536 reads. Reading past a covered entry took about 1 µs on Studio1 at a
  load average of 50–80, so about 40,000 fit in a 50 ms pass. A persistent
  worker keeps its directory streams open across passes and has no such
  limit.
- The durable walk state holds only name hashes and numbers; it names no
  path.
