# Delivery acknowledgement v1

Deploy the additive cloud change before the strict collector. No database migration is required.

## Wire contract

Strict collectors send `x-plimsoll-ack-version: 1` on event/history, session, attribution-repair and outcome POSTs. Requests retain their existing body schemas and HMAC over `timestamp.rawBody`. A server rejects an unsupported requested version before storage. Requests without the header retain the legacy response shape.

After storage completes, the server adds `ack` to its existing JSON response:

```json
{
  "version": 1,
  "kind": "events",
  "requestDigest": "sha256:<SHA256 of exact UTF-8 request body>",
  "scopeDigest": "sha256:<SHA256 of JSON.stringify([tenantId, installKey])>",
  "acceptedIds": ["sha256:<item identity>"],
  "rejectedIds": []
}
```

Kinds are `events`, `sessions`, `attribution_repair`, and `outcomes`. An item identity is SHA256 of `JSON.stringify([type, id])`, prefixed `sha256:`. Types are `event`, `session`, `artifact`, or `outcome`; UUID-shaped IDs are lowercased, other IDs remain exact. Outcome IDs use the collector's submitted IDs, before tenant-salted storage conversion. Digests avoid echoing keys or arbitrary external identifiers. They bind the response to a request; HTTPS and existing authentication remain the trust boundary. They are not an independent server signature.

The accepted set includes newly stored facts and verified duplicates. Legacy `inserted: 0` distinguishes a duplicate event replay from new inserts. Immutable event duplicates must match the stored columns, including usage and metadata, in the authenticated tenant. Cost comparison uses the existing database's six-decimal precision. A missing optional project/customer/workflow linkage may already have been filled by the dedicated repair path. Conflicting facts and foreign-tenant UUID collisions are rejected, without rewriting the held event. Mutable session snapshots retain the existing grow-only semantics; a same-tenant superseding snapshot satisfies an older request. Outcomes retain deterministic tenant-scoped upserts.

The collector requires version, kind, request digest and scope digest to match, and every submitted item identity exactly once across the accepted and rejected sets. Duplicate identities within one v1 request fail before send/storage. Missing/malformed/uncovered/foreign/repeated IDs or contradictory legacy counters/`ok`/`error` fields fail the entire client batch. A well-formed partial acknowledgement is valid: accepted outbox items settle independently, while rejected items remain durable for bounded per-item handling. HTTP 2xx alone never authorizes local progress.

## Retry and privacy behavior

The durable outbox settles accepted items immediately. A rejected item retries up to five total acknowledgement attempts with the existing deterministic exponential-jitter backoff capped by `delivery.maxBackoffSeconds`; exhaustion writes a `remote_rejected_exhausted` dead-letter receipt. A well-formed partial acknowledgement never opens the contract circuit. Malformed acknowledgements retain the whole batch and keep the existing contract circuit behavior. History and session sync still advance only after a validated acknowledgement with no rejected items because those paths do not have per-item settlement state. An uncertain response can follow a committed remote transaction, so retries retain deterministic IDs. A new collector talking to an old server therefore retains work until the server is upgraded; it never downgrades acknowledgement checks.

Every collector authenticated POST, including join and repo labels, uses the shared transport. It allows HTTPS and explicit loopback HTTP (`localhost`, loopback IP literals); other HTTP, embedded credentials, redirects and escaped response origins fail. Request bodies are capped at 1,500,000 UTF-8 bytes; response bodies at 262,144 bytes, measured while streaming after decompression. The default deadline is 30 seconds, capped at 120 seconds; delivery callers pass their configured request timeout. The deadline remains active through response consumption/parsing. Cancellation and discarded bodies do not hold a retry cycle open. All transport/acknowledgement failures expose symbolic diagnostics, never remote error text or thrown network messages.

## Rollout and recovery

1. Deploy the server and verify legacy-client and v1 fixture/canary responses at the hosted boundary.
2. Deploy the strict collector after the server receipt. Verify one new event and its exact replay, plus history, session and outcome acknowledgements.
3. Preserve the outbox, history watermark and installation identity across rollback. Rolling the server back while strict clients run will defer deliveries; restoring v1 service permits replay after the existing circuit/backoff. Do not clear local work to make delivery look healthy.

### One source's validation rejections do not open the host circuit

The wire contract is unchanged — no new remote fields — but the collector's
reading of a rejection is scoped, because a 400/422 names one envelope, not the
endpoint.

A cycle that ends with zero acceptances and only singleton validation
rejections used to be read as a broken contract and opened the whole-host
`contract_blocked` circuit. On 2026-09-12 a hosted cloud whose `AiToolSource`
enum had no `GROK` rejected every Grok envelope per item; studio4 bisected one
cycle down to 16 Grok singletons, inferred a broken contract, and held 33
deliverable `claude_code` events for an hour.

That inference is now gated on the durable validation witness — a previously
acknowledged sanitized envelope under the current contract hash
(`upload_validation_witness`):

- **Witness accepted (2xx).** The contract is proven, so the singletons are
  proven candidates: they are dead-lettered per delivery with
  `remote_validation_rejected`, the circuit stays `none`, and every other item
  remains deliverable in this or the next cycle.
- **Witness rejected (400/422).** Global contract evidence: `contract_blocked`
  opens exactly as before, with zero dead letters.
- **Witness probe does not fit the cycle's `delivery.maxProbesPerCycle`
  budget.** The inference is deferred — the cycle retries with
  `remote_validation` and does not open the circuit. The next cycle's
  start-of-cycle witness reprobe settles it. The budget is never exceeded.
- **No witness.** A host that has never had an acknowledgement under this
  contract hash has nothing that proves the contract, so the conservative
  `contract_blocked` behaviour stands.

### Replaying dead letters after a contract fix

A dead letter written for a *remote* reason records the cloud rejecting an
envelope the collector prepared correctly. Once that contract is fixed the
delivery is viable again, but `enqueueRaw` refuses any delivery id that already
carries a receipt, so those receipts were terminal.

```
plimsoll upload-replay --reason <receipt reason> [--since <ISO-8601>] [--limit N] [--dry-run]
```

Replayable reasons are the remote terminal ones — `remote_validation_rejected`
and `remote_rejected_exhausted`. Local privacy, quarantine, oversize and schema
receipts are decisions about the row itself and are refused with a clear error.

The command selects dead receipts with that reason whose raw row still exists in
`buffered_events` and carries no privacy disposition, supersedes the dead
receipt (recorded in `upload_replays` with a `replay_count`, with
`upload_control.receipt_dead` kept exact), and re-enqueues the raw row through
the ordinary enqueue path. It never uploads: delivery happens on the normal
`upload` cycles, so it is safe to run while a circuit is open. A replayed
delivery that is later acknowledged ends with exactly one `upload_receipts` row
in state `acknowledged`; a second replay of the same delivery is a counted
no-op. `--limit` defaults to 500 and is capped at 5000, and the transaction is
bounded by rows, raw bytes and `busy_timeout` like the migration scan.

Output is one JSON object:

```json
{ "reason": "...", "selected": 0, "requeued": 0,
  "skipped": { "alreadyActive": 0, "alreadyAcknowledged": 0, "missingRaw": 0, "privacyDisposed": 0 },
  "dryRun": false }
```

`delivery-ack.ts` is intentionally byte-identical in the two separately versioned repositories. Keep both copies and contract fixtures aligned. Collector proofs are `scripts/delivery-transport-proof.ts`, `scripts/outbox-proof.ts`, `scripts/upload-replay-proof.ts`, and `scripts/join-isolation-proof.ts`. Cloud `scripts/delivery-ack-proof.ts` requires `PLIMSOLL_DELIVERY_FIXTURE=1` and a disposable loopback database named `plimsoll_delivery_fixture`. Hosted deployment, independent integration review and fleet acceptance remain release-owner work.
