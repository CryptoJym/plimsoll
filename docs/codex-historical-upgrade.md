# Historical raw boundary

The first opener records the existing maximum raw rowid under its writer lock.
That boundary is durable and cannot be moved by ordinary open, migration,
maintenance or restart. Existing raw observations keep every column, including
their upload mark. New appends allocate above the boundary and retain current
capture, pairing, delivery and ACK behavior.

Response coverage and exact pairing for historical rows live in derived state
bound to rowid, ID, creation time and privacy generation. Projection uses the
derived remainder and keeps the accounting owner/frozen bytes. It does not
reprice, rename a model or rewrite the archived observation. An ACK for an
existing historical queue item is recorded in its receipt and removes that
delivery; it does not restamp the archival raw upload mark. New-row ACKs still
set their raw mark. Released rollback readers keep their existing behavior.

Before constructor receipt recovery runs, the collector holds unprocessed
NULL-lineage dead receipts. Migration with a cursor below the historical
boundary also holds, without advancing that cursor or queueing old raw rows.
Delivery status exposes `historical_receipt_recovery_requires_opt_in` or
`historical_migration_requires_opt_in`, with `historical_repair_hold` degradation.
Existing queue work and new captures can continue; a repeated historical raw ID
does not authorize new delivery repair.

There is no automatic or environment-variable override. A historical repair
apply command requires separate review, an explicit opt-in and a dry run that
reports affected raw incarnations, deliveries and known/unknown counters.
This version deliberately provides no such apply command. Do not reset the
boundary/cursor or manufacture upload marks/ACK receipts to clear a hold.

Use the read-only SQL in `scripts/fixtures/codex-upgrade-preflight/` on the
released ledger before opening the upgrade. Require known unchanged binding,
actual queue drain through released ACK, completed/current migration and no
pending receipt-lineage recovery. Fence intake and historical producer retries
during the cut. SQL cannot establish the absence of a future operator command.
The scoped financial query is a sufficient superset, and the sealed bare-rollout
query is only a subset, of native-classifier retirement candidates. Production
plans and timing must be checked by the operator; tiny-fixture timing is not a
100 GB performance certification.

`upload --no-mark` is explicitly historical-touching: it selects NULL raw marks
without the outbox/migration boundary, can persist capture and coverage decisions
and sends historical named events or tokenless gaps. It leaves raw columns
protected and does not record ordinary event ACK or prove a queue drain.
`upload-history` and `upload-replay` likewise require an explicit operator
decision. Hold these commands during a normal upgrade cut.
