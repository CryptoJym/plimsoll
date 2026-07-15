# ADR-0001: Resource-bounded local collector with isolated internal stages

## Status

Accepted

Accepted for incremental delivery under [Plimsoll issue #75](https://github.com/CryptoJym/plimsoll/issues/75). The target is accepted; individual migration gates remain incomplete until their linked issues merge and the integrated resource proof passes.

## Date

2026-07-15

## Decision owners

James is the product and release decision-maker. Contributors may implement and verify the target in independently reviewable lanes, but they do not deploy, activate the installed LaunchAgent, mutate the live ledger, or close owner decisions.

## Context

Plimsoll is a local-first telemetry collector. It receives hooks and OTLP, tails local Claude Code and Codex artifacts, retains privacy-safe evidence, calculates token/cost and outcome views, presents a loopback dashboard, and optionally uploads to the hosted workspace.

The useful signal is real, but one synchronous Node process and one SQLite event table currently carry unrelated responsibilities:

- capture admission and raw evidence;
- retry queue state through `buffered_events.uploaded_at`;
- repeated repricing and repository-enrichment history sweeps;
- dashboard aggregation over raw events;
- recursive activity discovery for capture health; and
- upload/session synchronization.

The 2026-07-15 read-only survey found 4,810,030 event rows in a 6.8 GB ledger. Generic Codex OTLP spans accounted for 4,130,123 rows and about 3,296.4 MiB of payload while carrying no tokens or cost. A five-surface dashboard refresh consumed 12.26 CPU-seconds and 13.15 seconds wall time. The collector logs contained 16,079 port-conflict failures, 624 sync failures, and a maximum sync-failure streak of 107. A deterministic invalid upload row could block later valid rows because retry state and evidence were the same rows.

These are scaling-shape failures, not a reason to distribute the system. The expected deployment remains one operator, one machine, one collector, and one local ledger.

## Requirements

### Functional

1. Receive local hook, OTLP, rollout, and transcript signals without persisting raw content in metadata mode.
2. Sanitize first, then admit events through a documented value predicate and deterministic dedupe key.
3. Preserve useful local evidence for the configured retention window.
4. Maintain exact, incremental projections for status, health, sessions, repositories, accounts, cost, and upload work.
5. Deliver accepted events through a durable outbox with bounded retries and deterministic poison quarantine.
6. Serve a coherent loopback dashboard snapshot without scanning raw history or the session filesystem on refresh.
7. Expose freshness, drops, backlog pressure, dead letters, and degraded states honestly.
8. Recover after crash or file rotation without event loss or duplicate accounting.

### Non-functional

- **Privacy:** metadata mode persists and transmits no prompt, response, tool arguments, absolute paths, repository URLs, emails, tokens, or credentials. Hashes and explicitly approved linkage fields retain their existing boundary.
- **Resource use:** unchanged inputs cause zero raw-event writes, zero full-history file reads, and zero overlapping maintenance jobs. Dashboard work scales with projection size, not raw-ledger size.
- **Reliability:** exactly one collector owns a configured port/ledger. Upload failure does not block capture, dashboard reads, or later valid delivery.
- **Durability:** offsets, projection watermarks, outbox state, and acknowledgements commit transactionally with the corresponding local change or replay safely through deterministic identifiers.
- **Operability:** every bounded queue or maintenance lane reports its watermark, backlog, work counters, last success, and explicit degraded reason.
- **Compatibility:** existing local rows and configuration remain readable throughout an additive migration. No live-ledger rewrite is required for the first-value path.
- **Testability:** release gates use temporary homes, databases, session trees, and loopback ports. Deterministic work counters are primary; wall-clock observations are secondary.

The executable budget matrix and integrated gate are specified in [Resource budget gates](./resource-budget-gates.md).

## Decision

Keep one deployable collector process and one local SQLite database, but make the internal data flow an explicitly staged modular monolith:

```text
hooks / OTLP / rollout tail / transcript tail
                    |
                    v
       privacy sanitizer (existing policy wall)
                    |
                    v
    admission + deterministic dedupe + drop counters
                    |
          +---------+---------+
          |                   |
          v                   v
 short-retention raw     incremental projection updates
 evidence ledger              |
          |                   +------> coherent dashboard/status snapshot
          |
          v
 durable outbox -- retry/backoff --> hosted ingest acknowledgement
          |
          +--> dead letter (redacted deterministic reason)
```

SQLite remains the transaction boundary. Logical tables separate ownership:

| Logical component | Responsibility | Must not do |
|---|---|---|
| Admission | sanitized value predicate, deterministic ID, bounded drop counters | write rejected payloads |
| Raw ledger | retained privacy-safe evidence | act as a retry state machine or dashboard query engine |
| Projections | incremental, exact read models and activity facts | parse raw content or trigger filesystem scans on read |
| Outbox | immutable delivery envelope reference/copy, attempts, next-attempt, acknowledgement | rewrite raw evidence during retry |
| Dead letter | one quarantined record plus bounded/redacted classification | retain provider response bodies, secrets, or raw content |
| Scheduler | one in-flight job per named lane, dirty-work queues, backoff | launch overlapping interval work |

### Failure isolation inside one process

Each internal lane has its own state and error boundary:

- Admission failures reject/count the single input; they do not stop the server.
- Tailer failures retain the last committed byte offset and retry that file without replaying all history.
- Projection failures mark projections stale and queue bounded repair; raw capture continues.
- Upload failures advance retry state only. Deterministic poison is quarantined once; later valid outbox items remain eligible.
- Dashboard reads return the last coherent snapshot with freshness/degraded metadata; they never repair projections synchronously.
- Scheduler lanes use an in-flight guard and coalesce triggers into one subsequent run.

Process-level failure still restarts the one collector. A healthy existing owner causes a successful `already_running` result, not an error/restart storm.

### Transaction boundaries

For a newly admitted event, one SQLite transaction should:

1. insert the raw evidence row if its deterministic ID is new;
2. apply the projection delta or record a projection-repair watermark; and
3. enqueue the outbox item when cloud delivery is configured.

Crash before commit changes nothing. Crash after commit leaves all three recoverable. Tail offsets advance only after every complete line represented by that offset has committed. Partial JSONL framing remains outside the committed offset until the line completes.

Large pre-existing ledgers are migrated additively:

- new events use the staged path immediately;
- existing `uploaded_at` state seeds outbox rows in bounded batches;
- projections backfill behind a watermark without blocking capture;
- reads switch only after reconciliation proves projection totals against raw reference queries; and
- old compatibility columns are retained until a later, separately approved cleanup.

## Consequences

### Positive

- Removes the highest-value waste without introducing a fleet: noise is rejected once, files are tailed once, and dashboard refresh reads compact projections.
- Gives capture, analytics, and delivery independent failure states while preserving one-process installation and one local transaction boundary.
- Makes retention truthful because upload work no longer exempts raw evidence from configured retention indefinitely.
- Turns performance into an executable contract using work counters that remain stable across CI machines.
- Maintains local-first privacy and allows offline operation.

### Negative

- Projection and outbox schemas add migration, reconciliation, and repair code.
- Read models create temporary dual-path complexity until raw-query parity is proven.
- A single process still shares an event loop; synchronous SQLite work must be bounded and scheduled away from request handling.
- Dead-letter and budget policies need operator-facing semantics; silent deletion is not acceptable.

### Neutral

- SQLite remains the storage engine and deployment artifact.
- The hosted API contract need not change for the 80/20 local improvement.
- Historical raw events are not automatically rewritten or deleted by adopting this ADR.

## Alternatives considered

### Microservices

Rejected. Independent services would add process supervision, IPC/network failure, version skew, observability, installation, and upgrade burden for a single-user local tool. The current problem is unbounded work inside components, not independent scaling across teams or hosts. Internal modules and queues deliver the required failure isolation at a fraction of the operational cost.

### A second database for analytics or delivery

Rejected for the first migration. A second SQLite file weakens atomicity between evidence, projections, and outbox, creates backup/recovery ordering, and doubles corruption and lifecycle surfaces. Separate logical tables in one WAL database provide isolation and transactional consistency. A second database should be reconsidered only if measured write-lock contention remains above budget after bounded transactions and incremental reads ship.

### Kafka, NATS, Redis, or another event-streaming service

Rejected. These systems solve multi-producer/multi-consumer distributed throughput and replay. Plimsoll has one local producer process, modest event rates, strict offline behavior, and a packaged CLI install. SQLite is already a durable local queue. Adding a broker would cost more CPU, memory, ports, installation support, and failure handling than the collector itself should consume.

### Keep the raw event table as the dashboard and retry queue, then add indexes

Rejected as an end state. Targeted indexes help individual queries but cannot make five repeated history aggregations constant-work, cannot prevent retries from rewriting evidence, and cannot isolate poison. Indexes are still useful inside the staged design.

### Batch full-history scans less frequently

Rejected. Lower frequency reduces average CPU but preserves pathological spikes, stale results, overlapping intervals, and work proportional to total history. Incremental watermarks and dirty queues remove the unnecessary work entirely.

### Split the collector into multiple OS processes

Deferred. Worker processes could isolate synchronous database CPU, but they add lifecycle and coordination risk before the work has been made bounded. First bound the work and measure event-loop delay. Introduce a worker only for a proven residual hot lane with a stable message boundary.

## Security and privacy analysis

- Admission runs **after** existing privacy sanitization. Rejected-event telemetry is low-cardinality source/reason counts only; rejected payloads are never retained.
- Projections operate on promoted, sanitized columns and approved hashes. They do not rehydrate raw request bodies.
- Outbox rows carry only fields already allowed by the ingest schema. Credentials remain in the existing local config boundary and never enter the ledger, receipt, dead letter, or logs.
- Dead-letter reasons are enumerated classifications plus bounded identifiers. Provider response bodies and validation payloads are not stored verbatim.
- Dashboard remains loopback-only and keeps the existing same-origin/custom-header protection for writes.
- Resource-proof receipts contain counts, statuses, versions, and durations only. They omit absolute user paths, event payloads, account identifiers, and credentials.

## Failure modes and recovery

| Failure | Visible state | Recovery / containment |
|---|---|---|
| Duplicate collector start | `already_running`; owner pid unchanged | candidate exits successfully; supervisor does not thrash |
| Stale pid record | explicit stale-owner receipt | validate process identity/port before replacing record; never kill an unrelated PID |
| File append ends in partial JSONL | tailer pending bytes > 0 | keep prior committed offset; commit when completed |
| File truncation/rotation | reset/rotation counter increments | identify the new file generation and replay only from its safe boundary |
| Projection update fails | snapshot `stale`, repair backlog > 0 | retain raw evidence; bounded repair from watermark |
| Transient upload failure | retryable backlog/next-attempt visible | bounded exponential backoff with jitter; capture continues |
| Deterministic invalid upload | one dead letter with redacted reason | quarantine once and continue later valid items |
| Outbox/raw byte budget exceeded | collector health degraded, exact pressure visible | preserve newest useful signal according to documented policy; never call it healthy or silently extend retention |
| SQLite busy/corrupt | explicit local-storage failure | short transactions/busy timeout; stop writes on integrity failure and preserve the file for operator recovery |
| Proof clock drifts past fixed fixture windows | deterministic-proof failure | inject a fixture clock; delivered by #82, never widen assertions until they pass by accident |

## 80/20 migration order

1. **Stop restart storms (#76).** Establish one owner and a stable packaged executable before measuring idle cost.
2. **Stop repeated work (#77).** Commit byte offsets, serialize lanes, and replace unconditional sweeps with dirty work.
3. **Stop low-value storage (#78).** Apply the sanitized admission predicate and expose drop counters.
4. **Isolate delivery (#79).** Add outbox/dead-letter state and honest budgets without rewriting live history.
5. **Bound reads (#80).** Build/reconcile projections, switch dashboard/status, and remove read-triggered filesystem walks.
6. **Enforce the contract (#81).** Wire the integrated temporary-environment resource proof and only then make it a mandatory CI/release gate.

This order attacks the measured CPU/storage/restart drivers first while keeping every PR independently reviewable. Cloud D3/D4 work is outside this ADR.

## Release and rollback

- Every stage lands behind compatibility with the existing schema/read path.
- No stage activates or restarts the installed LaunchAgent.
- Projection reads switch only after exact fixture reconciliation and freshness reporting pass.
- Outbox migration is idempotent and retains the legacy upload marker until rollback is no longer required.
- Rollback returns reads/sync to the legacy path without deleting new tables. Captured evidence remains intact.

## Review gate

Architecture acceptance does not equal release completion. Integrated release requires the machine-readable resource receipt to report `gateReady: true`, no failed or unwired required scenarios, existing proof green with the deterministic-clock repair delivered in #82, collector build green, and a source-of-truth readback from merged `main`.

## References

- [Parent architecture issue #75](https://github.com/CryptoJym/plimsoll/issues/75)
- [Resource-proof issue #81](https://github.com/CryptoJym/plimsoll/issues/81)
- [Deterministic proof clock issue #82](https://github.com/CryptoJym/plimsoll/issues/82)
- [Resource budget gates](./resource-budget-gates.md)
