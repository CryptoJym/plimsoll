# Provider Capacity Snapshot V1 fixtures (issue #168)

Golden fixtures for the frozen Capacity Rail contract
(`plimsoll.provider-capacity.protocol.v1`). These bytes are hash-bound into
the protocol fingerprint recorded in `protocol-receipt.json`; ANY edit here
flips the fingerprint and stales the receipt until it is regenerated
deliberately (`buildProviderCapacityProtocolReceipt`).

| Path | Contents |
|---|---|
| `valid/snapshot-fresh.json` | One valid local-rich `ProviderCapacitySnapshotV1` |
| `valid/sync-batch.json` | Valid sanitized `provider_capacity_sync` batch (envelope + 2 body rows; one row is fresh FULL depletion — a legitimate derived zero) |
| `invalid/*.json` | Must-reject wrappers: `{ mustReject, target, because, payload }`. `target` names the enforcing layer: `providerCapacitySnapshotV1` / `providerCapacitySyncBatch` (Zod schema parse must fail) or `sealedSyncBatch` (`sealProviderCapacitySyncBatch` must refuse) |
| `protocol-receipt.json` | Committed compatibility receipt: contract-material digest + per-fixture sha256 digests + protocol fingerprint. Cross-repo compatibility = exact fingerprint match |

Contract source: `packages/shared/src/provider-capacity-snapshot.ts`.
Proof: `pnpm proof:capacity-contract`.

The scientific unit of measurement across every fixture is
`device + provider profile + quota window + adapter version` — never a
person. Capacity facts here are operational context only; nothing in these
fixtures represents prompts, commands, transcript paths, emails,
credentials, raw provider bodies, billing details, or productivity fields,
and none of those are representable in the schemas.
