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

The bookkeeping that matters is `captureBaselineStatus`, which totals the
capture baseline's generation table once per JSONL source, before every
capture leader and on every status refresh of the collector parent. It used
to read every generation row: on Studio1, with a completed baseline of
200,000 files per source (a 216 MB ledger), one call took about 0.55–0.6 s.
A covering index (`idx_capture_baseline_generation_status`) answers it from
the index alone: about 36 ms at 200,000 per source. Building that index on
such a ledger takes about a quarter of a second, once, on the first open
after the upgrade.

## Grok walk: every session within a stated number of passes

A Grok sweep walks every group of `sessions/`, then every session of each
group, in the order of the hashes of their names (`grok-usage-tailer.ts`).
Its only durable walk state is one cursor in `grok_usage_walk_round_v1`: the
hash of the group it is in and of the last session it examined there.

- **Walk first.** Every discovery pass starts with the walk and runs it
  until the cursor has moved past one group or session, whatever the pass's
  wall, step and read allowances or the cadence clock. That unit lists at
  most `sessions/` and one group and examines at most one session. So every
  pass moves the sweep, however often its worker is replaced.
- **Restarts.** A new worker lists `sessions/` and the cursor's group again
  and goes on right after the cursor. It never reads past what an earlier
  worker walked, so a group of any size costs a new worker one listing, not
  a replay. A worker that stays keeps its listings for the sweep.
- **Recent lane.** Next, a sweep ranks the groups modified within 48 hours
  and queues changed usage files written in that window. It takes at most
  half of a pass's wall and of its steps, and never replaces the walk. When
  it last started is durable (`recentAtMs`), so a replaced worker does not
  start it again until 10 minutes have passed, or the clock has moved back.
- **Churn.** An entry created during a sweep is examined in that sweep if it
  sorts after the cursor, and otherwise in the next. Each entry the cursor
  passes costs one step, so churn slower than the pass's steps cannot keep
  an old session out.
- **No clock in coverage.** No entry is deferred by its creation time, so a
  wrong or corrected clock can delay nothing and mark nothing clean that
  was not read.
- **Caps.** `maxGroups` and `maxSessionsPerGroup` limit what one pass opens
  and observes. The rest waits for a later pass of the same sweep and is
  counted (`groupsOverLimit`, `sessionsOverLimit`); nothing is dropped.
- **Honest completion.** Whether a sweep saw an error, or had a worker close
  with a queued usage file unread, is durable (`unclean`), so a sweep
  finished by a later worker is not reported clean. The next sweep reads
  what was missed.

Bounds:

- Every pass moves the cursor past at least one group or session, so a
  sweep of N sessions in G groups is finished within N + G passes, whatever
  the worker's lifetime and however many entries it has already walked.
- With the reviewer's Studio0-scale tree (4,000 sessions in 20 groups, 12 ms
  per group lstat, a 50 ms pass), a worker replaced every pass finishes one
  group beside the recent lane in the first pass and at least ⌊50 / 12⌋ = 4
  in every later one: at most 1 + ⌈19 / 4⌉ = 6 passes. With the clock 24
  hours behind the file system it is the same 6; with a clock that jumps
  forward 11 minutes every pass, the recent lane runs every pass and it is
  at most G + 1 = 21.
- The durable walk state is one cursor, under 200 bytes, whatever the size
  of the tree or the churn. It holds only name hashes and numbers; it names
  no path.
- A new worker's first unit lists the cursor's whole group, whatever its
  size: a group of 100,000 sessions costs it one listing of about that many
  directory entries, then the pass goes on as usual.
