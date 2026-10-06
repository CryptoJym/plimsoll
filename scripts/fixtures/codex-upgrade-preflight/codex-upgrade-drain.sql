-- Read-only sufficient gate, evaluated BEFORE upgrade on the .51 ledger.
-- This counts a CONSERVATIVE outbox set, not the exact retirements: genuine
-- native usage can pass the head. Zero means there are no queued financial
-- Codex envelopes to retire, including explicit zero counters and cost-only.
-- Use with migration/recovery/binding gates and the operational exclusions.
-- Does not consult uploaded_at, the raw ledger, or the driftable gauges.
WITH active AS MATERIALIZED (
 SELECT coalesce(sealed_envelope_json,base_envelope_json) AS envelope
 FROM upload_outbox INDEXED BY idx_upload_outbox_workspace_due
 WHERE workspace_id IS (SELECT current_workspace_id FROM collector_workspace_binding WHERE singleton=1)
   AND device_id IS (SELECT current_device_id FROM collector_workspace_binding WHERE singleton=1)
   AND state IN ('pending','retry','in_flight')
), parsed AS MATERIALIZED (
 SELECT envelope, CASE WHEN json_valid(envelope) THEN 1 ELSE 0 END AS valid
 FROM active
)
SELECT count(*) AS active_codex_financial_deliveries
FROM parsed
WHERE CASE WHEN valid=0 THEN 1
 ELSE
  (json_extract(envelope,'$.event.source')='codex'
    OR (json_extract(envelope,'$.event.source')='claude_code' AND
        (lower(json_extract(envelope,'$.event.metadata.serviceName'))='codex' OR
         (substr(lower(json_extract(envelope,'$.event.metadata.serviceName')),1,5)='codex' AND
          substr(json_extract(envelope,'$.event.metadata.serviceName'),6,1) IN ('-','_','.')))))
  AND coalesce(json_extract(envelope,'$.event.eventType'),'') <> 'usage_live'
  AND coalesce(json_extract(envelope,'$.event.metadata.usageSource'),'') <> 'capture_gap'
  AND coalesce(json_extract(envelope,'$.event.metadata.captureGap'),0) <> 1
  AND (json_type(envelope,'$.event.inputTokens') IN ('integer','real')
    OR json_type(envelope,'$.event.outputTokens') IN ('integer','real')
    OR json_type(envelope,'$.event.cacheReadTokens') IN ('integer','real')
    OR json_type(envelope,'$.event.cacheCreationTokens') IN ('integer','real')
    OR json_type(envelope,'$.event.costUsd') IN ('integer','real'))
 END;
