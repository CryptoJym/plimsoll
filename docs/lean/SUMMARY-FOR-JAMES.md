# Lean, decision-grade Plimsoll: the plan in one page (round 7: the B0 contract repair)

**2026-09-25.** This is still a plan, not a change. Nothing was deployed or touched on any machine or the cloud, and nothing was pushed. The sixth independent review confirmed the day-acknowledgement, timestamp and unread-file fixes and left **two gaps** open. This round (B0, the contract-repair bead) closes both, settles four smaller items the review handed to B0, writes each rule down once (`CONTRACTS.md`), and turns every rule into a test that sits in the real code repositories and **fails today on purpose**, so the people who build each piece know exactly which test they have to turn green. Those tests are marked "pending", which means they run in every build, show their failure, and never block a release until someone removes the marker.

## What B0 changed

- **Who owns a row is decided by one rule, whether or not the row reached the cloud.** The previous round still had two slightly different checks for the two paths, and for one timing (the cloud has told the Mac about a re-binding, but the Mac's next request has not yet confirmed it) they gave different answers: "nobody" on one path and "person B" on the other. Now one function decides: a row stamped with a version the cloud really issued belongs to the person that version bound; a stamp the cloud never issued belongs to nobody, on both paths. The Mac's confirmation is recorded for disclosure only. The test timeline was also corrected: the first confirmation of a re-binding is the next request after it, not the reply that announced it. Test: `b4_offline_rebind` (eight orderings), plus the reviewer's exact case, in both repositories.
- **The disk-safety check before a Mac's no-delete hold now uses that Mac's own numbers.** The old check assumed every Mac looks like Studio1 (47 sessions per 1,000 usage records). Measured on a copy of Studio4's own ledger today: 65 sessions per 1,000, 3.5 activity rows per usage row against Studio1's 0.6, and about half the usage records the plan had assumed by scaling. The check now counts each Mac's sessions, days and buckets on a copy before any hold, refuses to run on guessed counts, uses the same safety figure in both halves of its arithmetic, and stops double-counting bytes the conversion has already written (which could have triggered an early release of old rows). Test: `s1b_runway_host_bound` with the reviewer's 333-session example, in the collector repository.
- **Records the converter cannot read are kept in a durable list**, with the reason and a link to the counted gap; never deleted by age or disk pressure. Test: the converter test in the collector repository.
- **A freshly installed or freshly upgraded collector writes "no stamp" until its first reply from the cloud**, exactly like an old collector, and the plan now says so and counts those rows per Mac.
- **The "unread file" test now finds its documents inside the repository**, and its behavioural half sits beside the open-gap test in both repositories.

## What is ready to freeze, and what is not (`FREEZE.md`)

Ready for your signature: the usage-record rule, the day keys and segment numbers, the day acknowledgement, the ownership rule above, the reject list, the open-gap rule, the token-volume gate and the abort size check. **Not yet:** the disk-safety numbers (the formula is ready; the per-row sizes are still estimates until the Studio5 copy is converted and each light Mac's counts are taken), and therefore no hold on any Mac; the future upload-claim and fault rules; the fleet half of dispatch tagging.

## What changed after the fifth review (round 6, unchanged)

- **A summary of each day can now be acknowledged by the cloud.** Deleting a raw row requires the cloud to have acknowledged every summary that used it, and one of those (the per-day summary) had no way to be sent or acknowledged, so in practice nothing would ever have been deleted. The upload protocol now carries it and acknowledges it like the others. Test: `b1_day_target_receipt`.
- **Who owns activity captured while a Mac was offline is now the same answer whether or not it reached the cloud.** If an admin re-binds a Mac from person A to person B while it is offline, last round's rules could export the same activity as A's if it never uploaded and as B's if it did. Now both paths use the same stamp the collector wrote at capture, the cloud records when the Mac acknowledged the change, and the answer no longer depends on delivery. Test: `b4_offline_rebind` (seven orderings, the reviewer's included).
- **Every timestamp the collector accepts now reaches the day totals.** The collector accepts some unusual but valid timestamp formats; last round's plan would have dropped those records from the dashboard's day sums as "contract violations". They are now folded into their real UTC day, and "contract violation" is reserved for a value the parser truly cannot read, which is then listed by id, never dropped silently. Test: `b5_non_iso_day_facts`.
- **The disk-runway check before a Mac's no-delete hold used a stale growth figure.** The temporary tables the migration adds are about **59% of the raw table on Studio1, not 42%**, once the per-row identity and link rows are counted. The gate now computes the figure per Mac from its measured row counts, with a safety margin, and a rehearsal copy replaces the estimate on the busy Macs and Studio0. Test: `s1b_runway_geometry` (a Mac whose hold the old figure would have started and the corrected one refuses).
- **The "unread file" rule is now the same in the bead and the acceptance drill as in the design.** An unparsed session file keeps a Mac's coverage "unknown" through the present, not just up to the file's last write. Test: `b22_false_complete`.
- Also: a source that reports no cache tokens but sends some anyway is treated as ambiguous rather than counted (`sf_no_cache_columns_guard`); the size check after an aborted migration allows for the raw rows captured meanwhile (`sf_abort_rebuild_bound`); the two tailer fixes are tracked in collector 0.7.42; deleted usage rows count as tombstones too; the fleet half of dispatch tagging stays parked until its owner accepts the interface; and the timestamp proof is described honestly as a large finite corpus with a conservative census.

## What the manager sees (three gates, nine metrics)

Gates first, always on screen:
- **G1 Coverage.** What was captured, what was read to the end, what was reconciled, which machine dropped anything, whether the loss is counted or unknown, and whether the collector has re-verified itself since its last fault or crash.
- **G2 Attribution.** Share of spend with a project and with a work item, weighted by cost and by normalised tokens per model. Today: 15% and 0%. Rankings are withheld until 80% of spend has a project and the unpriced remainder is bounded and small.
- **G3 Pricing.** Share of records and tokens with a cost, which models have a price, and which have an attested ceiling. Dollars are always a floor, with the bounded and unbounded remainders next to them.

Metrics:
1. **Known spend and its change** versus the previous period, by basis; a token-only change is labelled volume, never spend.
2. **Spend by project, model, account and source**, with share and an Unallocated row pinned first.
3. **Budget burn and period-end projection**, as a range, withheld when capture is unconfirmed or unread.
4. **Cost per accepted outcome and rework cost**, counting distinct accepted items, once work items are tagged at dispatch.
5. **Spend without an accepted result**, by workflow, with active sessions excluded.
6. **Retry loops and failed tool attempts**, by tool class and model, with the spend that followed them.
7. **Cache efficiency** per model and workflow, with each provider's token rules stated.
8. **Context growth** inside sessions, by model: how much of the spend goes to turns with bloated context.
9. **Anomaly against a four-week baseline**, in dollars when ceilings are attested, otherwise in tokens; a zero baseline uses the absolute threshold only and says so.

Named for later, shown as "not connected" until it exists: **allowance-window burn** from an attested provider feed. Rejected from the main surface: raw event and span counts, tool calls per minute, tokens per message, people ranked by tokens, the current sampled ratios, any total that turns "unknown" into zero, a dollar change computed from tokens, an "upper bound" from the highest price we happened to see or from a prefix-matched list price, and a model-shaming score by price alone.

## What gets smaller

| | Today | After (est.) | How |
|---|---|---|---|
| Light Macs (Studio3/5; Studio1/4 after their backlog is read) | 0.4-1.4 GB | 0.15-0.55 GB before backlog rows | Drop the per-event dashboard copy (26-36% of each ledger); keep raw activity rows only 3 days after their summary is sealed and acknowledged; keep usage facts with full lineage, small sealed summaries, identity records and tombstones (about 5.5% of today's raw bytes on Studio1 after the audits; about 14% while the links exist) and a few MB of per-day history |
| Busy Macs (MacBook, Studio6) | 6-11 GB | measured on their own copies first; 4 GB is the target | Same |
| Studio0 | 73 GB | 9-12 GB est. before receipt compaction; 2-6 GB of that is usage facts, turns and the replay window; 16 GB is the target set after the rehearsal | Same, plus ending the 31.4 M-row projection backlog outright. Rows the cloud never acknowledged cannot be deleted; receipts, identity records and tombstones are kept until their compaction is proved, and tombstones grow by an estimated 4-25 MB a day on Studio0 until then, so the rehearsal measures that rate and, above 10 MB a day, the compaction proof comes before Studio0's live window. During the migration the ledger first grows by an estimated **21-26 GB** (up from last round's 15-19 GB, now that the per-row identity and link rows are counted); the rehearsal measures the real peak and Studio0's own seven days of growth set its runway before it starts |
| Collector memory on Studio0 | 1,354 MB | under 1,024 MB target | No per-event projection, bounded batches; each process profiled first |
| Cloud rows per day | ~398k (one measured hour × 24) | ~80k est. | Spans, tool rows, prompts and markers (about 83%) become sealed per-session summaries and hourly counts; usage records are unchanged |

The economics work already running (lanes 1-3) is untouched; this builds on its usage-fact projection and shares its exact usage-record rule.

## The three biggest risks

1. **Studio0 itself.** 73 GB, a stuck projection backlog, an unknown share of unacknowledged rows, an unread-file backlog nobody has measured yet, a migration that grows the file before it shrinks it (now estimated at 21-26 GB of temporary growth), and a rebuild that needs disk headroom and a full pause. Mitigation: it goes last, after a full rehearsal on a copy on another machine with every failure drill run there first, with the measured peak and seven days of its own growth as its runway, and a fresh free-space check on the day.
2. **The unread session files.** Until the two tailer fixes (now in collector 0.7.42) are accepted and each Mac's backlog is read, every coverage figure is a floor, an unparseable file marks a Mac's whole period as unknown, and we cannot classify Studio1 or Studio4 as light. This is the first bottleneck, and it sits outside this program's code.
3. **Attribution, not volume, blocks most of the metrics.** Lean storage does not fix 0% work items and 15% projects, and no dollar comparison can pass until someone attests price ceilings. The collector already knows how to carry a work item; nothing tells it one at dispatch. Wiring the fleet launcher to the collector's existing binding is its own bead, owned by the fleet dispatch tooling owner through you, and the single highest-value item; its fleet half is parked until that owner accepts the interface.

## What I would do first (this week): the reviewer's first wave, in order

- **B0** (done this round, eco-6hoxj.164.4): the repaired contracts and their pending tests exist in both repositories (17 on the cloud, 28 in the collector, each naming its bead, plus two guards that are green today); sign the freeze list in `FREEZE.md`; the runway numbers and any live hold wait for the Studio5 copy and each Mac's census.
- **B1** telemetry (already running): keep it observe-only, and switch its runway arithmetic to the per-Mac growth figure before any Mac's hold is considered.
- **B9a, B10a, B13**: the read-only parity oracle and timestamp census, the no-delete hold built and tested on a ledger copy but switched on nowhere, and the rebuild tool with its copy drills.
- **B19, B21, B22, B15, B18**: the allowance-feed spec, coordination with the two tailer lanes in 0.7.42, the open-gap design and tests, the catalog draft, and the collector half of dispatch tagging.
- Then the writers (B2a local, B6 cloud), proved on copies, then each Mac's preflight (hold with runway, catch-up, rebuild drills), then convert and certify history everywhere before any screen switches.
- Migrate Studio5 end to end as the proof host, then Studio6, Studio3, Studio4, Studio1, the MacBook, then the Studio0 rehearsal, then Studio0. Roughly 195-270 worker-hours and 9-12 weeks of calendar, **both uncalibrated** until the first copy drills on Studio5 and the membership-exchange drill report measured times (with a 30% contingency the honest calendar range is 9-16 weeks); the seven-day growth measurement, the catch-up, the seven-day observation, the seven-day rollback window and the 14-day retirement wait are fixed elapsed time.

## One decision for you

**How much history should each machine keep locally for its own dashboard?** Today's default is 90 days of raw rows. Recommendation: **30 days of usage facts locally**; the cloud holds the full history and is the system of record, and the local dashboard now keeps its own per-day history for five years regardless (a few megabytes), so this choice only affects the per-record drill-down and the local finance publication's reach. On Studio0 this is roughly 1 GB versus 3-4 GB in the estimate. The roles and Financials-page questions do not change this plan: the metrics and contract are the same either way, only the routing differs.
