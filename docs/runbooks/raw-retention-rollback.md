# Raw retention after a 0.7.44 rollback

Rolling back to 0.7.44 restores its retention: expired rows may be pruned locally before upload; their queued copies still upload.

The 0.7.44 prune writes a `raw_retention_receipts` row when it removes a raw
row. It leaves the linked `upload_outbox` row intact. That outbox row has a
complete base envelope, its workspace and device binding, and any sealed
envelope. On re-upgrade, the expiry receipt lets the pending copy lease,
upload and acknowledge even though the raw row is gone. A copy awaiting its
first seal or a re-seal uses the stored base envelope. A terminal privacy
decision writes receipts for every linked legacy delivery ID and retires all
those copies while the raw still exists. With no queued copy, 0.7.44 keeps
that unuploaded raw; the newer pruner can expire it after re-upgrade.

After re-upgrade, check `/status`: `delivery.remainingDelivery` should fall to
zero after upload, and `retention.states.heldForUpload` should reflect only
expired raw rows still waiting for delivery. A row with no outbox copy remains
held by 0.7.44 while delivery is enabled; the newer collector can enqueue it.
If delivery remains nonzero, keep the ledger and retry delivery rather than
clearing the outbox. The fixture proof `pnpm proof:retention-upgrade-downgrade`
executes the exact 0.7.44 code against an upgraded ledger and checks the
upload, acknowledgement, cleanup and status path.
