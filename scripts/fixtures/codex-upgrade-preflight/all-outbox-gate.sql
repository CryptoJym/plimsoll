-- Strong sufficient queue gate for the whole ledger, including rows with
-- unknown/foreign audience and NULL generations that a migration quarantine
-- can inspect. Zero removes the need for a native-model classifier.
-- Require actual released ACKs for drained financial delivery IDs. A later
-- local retirement is not an ACK and must not be used to clear this gate.
SELECT count(*) AS active_deliveries
FROM upload_outbox INDEXED BY idx_upload_outbox_due
WHERE state IN ('pending','retry','in_flight');
