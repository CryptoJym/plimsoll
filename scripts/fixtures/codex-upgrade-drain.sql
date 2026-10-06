-- Upgrade gate: run read-only on the released collector BEFORE upgrading.
-- Zero requires a real ACK, not a locally retired named delivery. Include
-- pending, in-flight, retry, and terminal unuploaded diagnostics conservatively.
SELECT count(*) AS codex_raw_unacknowledged
FROM buffered_events
WHERE source = 'codex' AND uploaded_at IS NULL;
