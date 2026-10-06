-- Diagnostics only. This is NOT the upgrade gate: retiring a named envelope
-- can make it zero while its raw diagnostics and tokenless gap remain unsent.
WITH active AS (
  SELECT coalesce(o.sealed_envelope_json, o.base_envelope_json) AS envelope
  FROM upload_outbox AS o
  JOIN buffered_events AS r
    ON r.rowid = o.raw_rowid AND r.id IS o.raw_id
   AND r.created_at IS o.raw_created_at
   AND r.privacy_generation IS o.raw_generation
  WHERE r.source = 'codex' AND r.uploaded_at IS NULL
    AND o.state IN ('pending', 'retry', 'in_flight')
), valid AS (
  SELECT envelope FROM active WHERE json_valid(envelope)
)
SELECT count(*) AS codex_named_financial_exposure
FROM valid
WHERE json_type(envelope, '$.event.model') = 'text'
  AND trim(json_extract(envelope, '$.event.model')) <> ''
  AND coalesce(json_extract(envelope, '$.event.metadata.usageSource'), '') <> 'capture_gap'
  AND (json_type(envelope, '$.event.inputTokens') IN ('integer', 'real')
    OR json_type(envelope, '$.event.outputTokens') IN ('integer', 'real')
    OR json_type(envelope, '$.event.cacheReadTokens') IN ('integer', 'real')
    OR json_type(envelope, '$.event.cacheCreationTokens') IN ('integer', 'real')
    OR json_type(envelope, '$.event.costUsd') IN ('integer', 'real'));
