-- Zero excludes automatic receipt-lineage candidate recovery at upgrade.
-- Includes all unprocessed all-NULL-lineage DEAD receipts, including ones
-- which would ultimately bind safely. Existing ACK receipts are not scanned.
SELECT count(*) AS unprocessed_null_lineage_dead_receipts
FROM upload_receipts INDEXED BY idx_upload_receipts_raw_lineage
WHERE terminal_state='dead' AND raw_rowid IS NULL AND raw_id IS NULL
 AND raw_created_at IS NULL AND raw_generation IS NULL
 AND rowid > (SELECT cursor_rowid FROM upload_receipt_lineage_backfill WHERE singleton=1);
