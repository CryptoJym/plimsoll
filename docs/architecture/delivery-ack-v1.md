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

The collector requires version, kind, request digest and scope digest to match, every submitted item identity exactly once, and no rejected IDs. Duplicate identities within one v1 request fail before send/storage. Missing/malformed/partial/foreign/repeated IDs or contradictory legacy counters/`ok`/`error` fields fail the entire client batch. HTTP 2xx alone never authorizes local progress.

## Retry and privacy behavior

The existing durable outbox records a contract failure and retains pending envelopes, using its current bounded circuit/backoff. History only advances its contiguous watermark after a validated complete acknowledgement. Session/outcome failures return failure without reporting acceptance; source facts remain available for replay. An uncertain response can follow a committed remote transaction, so retries retain deterministic IDs. A new collector talking to an old server therefore retains work until the server is upgraded; it never downgrades acknowledgement checks.

Every collector authenticated POST, including join and repo labels, uses the shared transport. It allows HTTPS and explicit loopback HTTP (`localhost`, loopback IP literals); other HTTP, embedded credentials, redirects and escaped response origins fail. Request bodies are capped at 1,500,000 UTF-8 bytes; response bodies at 262,144 bytes, measured while streaming after decompression. The default deadline is 30 seconds, capped at 120 seconds; delivery callers pass their configured request timeout. The deadline remains active through response consumption/parsing. Cancellation and discarded bodies do not hold a retry cycle open. All transport/acknowledgement failures expose symbolic diagnostics, never remote error text or thrown network messages.

## Rollout and recovery

1. Deploy the server and verify legacy-client and v1 fixture/canary responses at the hosted boundary.
2. Deploy the strict collector after the server receipt. Verify one new event and its exact replay, plus history, session and outcome acknowledgements.
3. Preserve the outbox, history watermark and installation identity across rollback. Rolling the server back while strict clients run will defer deliveries; restoring v1 service permits replay after the existing circuit/backoff. Do not clear local work to make delivery look healthy.

`delivery-ack.ts` is intentionally byte-identical in the two separately versioned repositories. Keep both copies and contract fixtures aligned. Collector proofs are `scripts/delivery-transport-proof.ts`, `scripts/outbox-proof.ts`, and `scripts/join-isolation-proof.ts`. Cloud `scripts/delivery-ack-proof.ts` requires `PLIMSOLL_DELIVERY_FIXTURE=1` and a disposable loopback database named `plimsoll_delivery_fixture`. Hosted deployment, independent integration review and fleet acceptance remain release-owner work.
