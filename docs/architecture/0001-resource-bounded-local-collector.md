# ADR-0001: Resource-bounded local collector with isolated internal stages

## Status

Accepted

Accepted by James on 2026-07-17 after the child migration lanes merged and the integrated resource, privacy, rollback, and current-`main` proofs passed. This acceptance adopts the architecture described here. It does not authorize collector activation, live-ledger migration or storage reclamation, provider/cloud mutation, or production rollout; those remain separate operational decisions.

## Date

2026-07-15

## Decision owners

James is the product, architecture, and release decision-maker. James explicitly accepted this ADR on 2026-07-17. Contributors may implement and verify the accepted design in independently reviewable lanes, but they do not infer rollout approval, deploy, activate the installed LaunchAgent, mutate the live ledger, or close later owner decisions.

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
5. Copy each accepted event's bounded, sanitized delivery envelope into a durable outbox with bounded retries and deterministic poison quarantine.
6. Serve a coherent loopback dashboard snapshot without scanning raw history or the session filesystem on refresh.
7. Expose freshness, drops, backlog pressure, dead letters, and degraded states honestly.
8. Recover after crash or file rotation without event loss or duplicate accounting.

### Non-functional

- **Privacy:** metadata mode persists and transmits no prompt, response, tool arguments, absolute paths, repository URLs, emails, tokens, or credentials. Hashes and explicitly approved linkage fields retain their existing boundary.
- **Resource use:** unchanged inputs cause zero raw-event writes, zero full-history file reads, and zero overlapping maintenance jobs. Dashboard work scales with projection size, not raw-ledger size.
- **Reliability:** exactly one collector owns a configured port/ledger. Upload failure does not block capture, dashboard reads, or later valid delivery.
- **Durability:** offsets, projection watermarks, sanitized outbox-envelope copies, and acknowledgements commit transactionally with the corresponding local change or replay safely through deterministic identifiers. Raw-evidence TTL is independent of delivery state.
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
 durable sanitized envelope-copy outbox -- retry/backoff --> hosted ingest acknowledgement
          |
          +--> dead letter (redacted deterministic reason)
```

SQLite remains the transaction boundary. Logical tables separate ownership:

| Logical component | Responsibility | Must not do |
|---|---|---|
| Admission | sanitized value predicate, deterministic ID, bounded drop counters | write rejected payloads |
| Raw ledger | retained privacy-safe evidence | act as a retry state machine or dashboard query engine |
| Projections | incremental, exact read models and activity facts | parse raw content or trigger filesystem scans on read |
| Outbox | immutable, bounded copy of the sanitized delivery envelope; attempts, next-attempt, acknowledgement | depend on the raw row remaining after its TTL or rewrite raw evidence during retry |
| Dead letter | one quarantined record plus bounded/redacted classification | retain provider response bodies, secrets, or raw content |
| Scheduler | one in-flight job per named lane, dirty-work queues, backoff | launch overlapping interval work |

### Failure isolation inside one process

Each internal lane has its own state and error boundary:

- Admission failures reject/count the single input; they do not stop the server.
- Tailer failures retain the last committed byte offset and retry that file without replaying all history.
- Projection failures mark projections stale and queue bounded repair; raw capture continues.
- Upload failures advance retry state on the envelope copy only. Deterministic poison is quarantined once; later valid outbox items remain eligible.
- Dashboard reads return the last coherent snapshot with freshness/degraded metadata; they never repair projections synchronously.
- Scheduler lanes use an in-flight guard and coalesce triggers into one subsequent run.

Process-level failure still restarts the one collector. A healthy existing owner causes a successful `already_running` result, not an error/restart storm.

### Transaction boundaries

For a newly admitted event, one SQLite transaction should:

1. insert the raw evidence row if its deterministic ID is new;
2. apply the projection delta or record a projection-repair watermark; and
3. enqueue a bounded copy of the already-sanitized delivery envelope when cloud delivery is configured.

Crash before commit changes nothing. Crash after commit leaves all three recoverable. Tail offsets advance only after every complete line represented by that offset has committed. Partial JSONL framing remains outside the committed offset until the line completes.

### Outbox envelope-copy and retention semantics

The outbox stores a copy, not a foreign-key-only reference to raw evidence. Its immutable delivery body is the exact event shape already accepted by the outbound ingest schema after the privacy gate, plus the existing bounded `suppressedFields` names and privacy-safe linkage fields. It does not contain collector credentials, provider response bodies, local labels/emails, raw paths, prompts, responses, or tool arguments. Each item also has a stable idempotency/event ID, creation time, attempt count, next-attempt time, acknowledgement time, and enumerated terminal reason.

The delivery body has an explicit per-item byte ceiling below the upload batch limit. Every byte boundary uses UTF-8 bytes (`Buffer.byteLength` or SQLite BLOB length), never JavaScript character count or SQLite TEXT character length. An oversized item is classified deterministically and never grows the retry queue unboundedly. Batch construction reads these copies under row and envelope-byte caps, then applies the hard network boundary to the exact serialized `RequestInit.body`, including the batch wrapper and separators. An item that fits the persistent item ceiling but not a caller's transient request cap remains retryable as `local_request_budget`; it neither opens a remote circuit nor prevents a later eligible item from being attempted. A legacy raw row that fits the item ceiling but exceeds the configured migration-slice budget is preserved and reported as `slice_budget_too_small`; raising that maintenance budget resumes the same watermark. A maintenance tuning value never turns otherwise deliverable evidence into a dead letter.

Atomic enqueue means a configured cloud delivery cannot observe an event without a durable local outbox copy, and replay of the deterministic event ID cannot duplicate that copy. After commit, the raw evidence row and the outbox item have independent lifecycle policies:

- raw evidence expires under the configured raw age/byte policy whether the delivery item is pending, acknowledged, or dead-lettered;
- the sanitized envelope copy survives raw expiry until acknowledgement, deterministic quarantine, or the separately documented outbox age/byte policy applies;
- acknowledgement deletes or tombstones the active envelope copy without rewriting raw evidence. One most-recent acknowledged, sanitized envelope may remain in a singleton validation-witness slot, scoped by a SHA-256 hash of the upload contract; it is bounded by the same item ceiling and exists only to distinguish an item-specific 400/422 from a globally broken contract; and
- deterministic quarantine stores the stable item/event ID and a bounded redacted classification, never a provider response body.

This bounded duplication is deliberate: it isolates upload availability from raw-retention truth. During migration, legacy unuploaded rows are copied into the outbox in bounded, idempotent batches before legacy retention protection is removed; status exposes the remaining migration watermark.

Every delivery entry point, including stateless `upload --no-mark`, calls one sealed-envelope boundary before persistence, witness storage, signing, or HTTP. That boundary constructs metadata from an explicit typed allowlist rather than copying input and searching for bad values. Unknown keys, including Unicode homoglyph keys, and legacy local/raw fields are local-only: their values are omitted. Capture, durable storage, and outbound sealing use one shared suppression-receipt formatter and validator. The shared policy sanitizer applies it before returning an evaluation to normalizers, successful HTTP responses, CLI output, logs, or persistence; syntactically unsafe container and semantic keys are omitted before they can survive as empty local metadata. OTLP admission is key-first rather than type-first: log records, spans, metric data points, resource attributes, scope attributes, and outbound sealing consult one shared exact-key disposition before any value-type fallback. Each disposition declares its exact case, value kind (typed string or analytical scalar), permitted OTLP surface, and outbound eligibility. Plimsoll follows the exact lowercase semantic-convention spellings it explicitly lists, while retaining the exact camel-case legacy aliases already observed from supported producers; case-folded, camel/dot/underscore/hyphen variations never borrow admission. Variant fingerprints exist only to genericize suppression receipts, not to admit data. Exact string dispositions cover bounded model, session, service, signal, classification, component, timestamp, version, trace, and HTTP-method values; exact analytical dispositions cover the production token/cost families plus explicit duration, sequence, status, and boolean fields. Arbitrary scalar values, unknown token-like names, and credential/raw/path/PII concepts are omitted for string, integer, double, and boolean inputs. Resource and scope omissions join each child record's canonical receipt set rather than disappearing silently. Forwarded hook/fallback results also precompute the same local-only field omissions the sealer will add, while successful OTLP responses aggregate the same event and metric receipts, so canonical arrays have byte-for-byte parity from response through raw storage, list/reopen, outbox, wire, and acknowledgement witness. Metric samples retain canonical receipts in a backward-compatible `suppressed_fields_json` column so local-only data-point omissions are not silent. A receipt is at most 96 total ASCII characters from `[A-Za-z0-9_.:+-]`; the `attributes.` namespace therefore leaves an 85-character source-key budget. Unsafe or sensitive attribute names and unapproved scalar keys collapse to the grammar-valid `attributes.non_ascii_or_unbounded_key`, repeated category receipts deduplicate in first-seen order, and the canonical array is capped at 128 entries with `suppression.additional_fields` recording overflow. Existing valid bounded receipts remain compatible. Unsafe legacy names are reduced to `suppression.non_ascii_or_unbounded_key` when read or sealed, so their values and names do not cross and list/readback never rewrites the stored row. Exact approved fields still fail closed when their values contain credential, email, raw path/URL, multibyte, or other disallowed shapes; typed low-cardinality ASCII spaces are normalized only after that unsafe-value gate. Approved numeric input/output/cache token counters, bounded analytical identifiers, known OTLP signal names, and the three exact OTLP transport paths remain valid. Repo and branch linkage enter delivery only as a canonical `sha256:` prefix plus exactly 64 hexadecimal characters; compound sensitive names cannot borrow that exception.

Remote 400/422 isolation is bounded within a cycle and resumable across cycles. When the probe budget expires before a batch is isolated, the outbox durably shrinks the next lease width instead of declaring a global contract failure. Clean validation-free successes double that width up to the caller's requested cap, restoring backlog throughput after the poison region without returning to repeated full-batch failure. The raw-row linkage repair path is independently indexed by `upload_outbox(raw_rowid)` so attribution fill does not scan the active outbox.

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
- Pending delivery temporarily duplicates a bounded sanitized envelope until acknowledgement or quarantine.
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
- Outbox rows copy only explicitly allowlisted fields already accepted by the ingest schema, with per-item byte limits. Credentials remain in the existing local config boundary and never enter the ledger, receipt, dead letter, or logs.
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
| Outbox/raw byte budget exceeded | collector health degraded, exact pressure visible for each independent budget | apply each documented age/byte policy independently; never call it healthy, silently extend raw TTL, or retain a raw row merely because delivery is pending |
| SQLite busy/corrupt | explicit local-storage failure | short transactions/busy timeout; stop writes on integrity failure and preserve the file for operator recovery |
| Proof clock drifts past fixed fixture windows | deterministic-proof failure | inject a fixture clock; delivered by #82, never widen assertions until they pass by accident |

## 80/20 migration order

1. **Stop restart storms (#76).** Establish one owner and a stable packaged executable before measuring idle cost.
2. **Stop repeated work (#77).** Commit byte offsets, serialize lanes, and replace unconditional sweeps with dirty work.
3. **Bound Codex reconciliation (#91).** Remove full reconciliation from the OTLP request; store only unresolved usage, prioritized candidates, revisioned pending windows, and a singleton control receipt. Resolve context through bounded raw `idx_events_observed` searches, reserve service for post-high-water work, and advance the legacy rowid high-water through hard 500-row chunks under an honestly reported soft deadline.
4. **Stop low-value storage (#78).** Apply the sanitized admission predicate and expose drop counters.
5. **Isolate delivery (#79).** Add outbox/dead-letter state and honest budgets without rewriting live history.
6. **Bound reads (#80).** Build/reconcile projections, switch dashboard/status, and remove read-triggered filesystem walks.
7. **Enforce the contract (#81).** Wire the integrated temporary-environment resource proof and only then make it a mandatory CI/release gate.

This order attacks the measured CPU/storage/restart drivers first while keeping every PR independently reviewable. Cloud D3/D4 work is outside this ADR.

## Release and rollback

- Every stage lands behind compatibility with the existing schema/read path.
- No stage activates or restarts the installed LaunchAgent.
- Projection reads switch only after exact fixture reconciliation and freshness reporting pass.
- Outbox migration is idempotent and retains the legacy upload marker until rollback is no longer required.
- Rollback returns reads/sync to the legacy path without deleting new tables. Captured evidence remains intact.

## Review gate

Owner acceptance is recorded above. The source integration gate also passed: the machine-readable resource receipt reported `gateReady: true` with no failed or unwired required scenarios; existing proof was green with the deterministic-clock repair delivered in #82; the collector build passed; and merged `main` matched the independently verified tree. Live rollout remains a separate authorization and must retain the release and rollback controls in this ADR.

## References

- [Parent architecture issue #75](https://github.com/CryptoJym/plimsoll/issues/75)
- [Resource-proof issue #81](https://github.com/CryptoJym/plimsoll/issues/81)
- [Deterministic proof clock issue #82](https://github.com/CryptoJym/plimsoll/issues/82)
- [Resource budget gates](./resource-budget-gates.md)
