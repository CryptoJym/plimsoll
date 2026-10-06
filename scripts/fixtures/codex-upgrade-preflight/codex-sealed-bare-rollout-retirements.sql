-- Exact ONLY for this proven .51 retirement class, not every candidate.
-- No capture-witness/decision schemas may already exist on the released DB.
-- Other shapes require the pinned head native classifier, not this query.
-- Captures the fixture's 1→0 ACK witness without counting good native SSE.
WITH candidates AS MATERIALIZED (
 SELECT o.sealed_envelope_json AS frozen,r.payload_json AS raw
 FROM upload_outbox AS o INDEXED BY idx_upload_outbox_workspace_due
 CROSS JOIN buffered_events AS r
 WHERE o.workspace_id IS (SELECT current_workspace_id FROM collector_workspace_binding WHERE singleton=1)
   AND o.device_id IS (SELECT current_device_id FROM collector_workspace_binding WHERE singleton=1)
   AND o.state IN ('pending','retry','in_flight')
   AND o.sealed_envelope_json IS NOT NULL
   AND r.rowid=o.raw_rowid AND r.id IS o.raw_id
   AND r.created_at IS o.raw_created_at AND r.privacy_generation IS o.raw_generation
   AND r.source='codex' AND r.uploaded_at IS NULL
   AND r.privacy_disposition IS NULL AND r.usage_duplicate_reason IS NULL
   AND r.data_mode='metadata'
)
SELECT count(*) AS sealed_bare_rollout_retirements
FROM candidates
WHERE CASE WHEN json_valid(frozen) AND json_valid(raw) THEN
 json_extract(frozen,'$.event.source')='codex'
 AND json_extract(raw,'$.source')='codex'
 AND coalesce(json_extract(frozen,'$.event.eventType'),'') <> 'usage_live'
 AND coalesce(json_extract(frozen,'$.event.metadata.usageSource'),'') <> 'capture_gap'
 AND coalesce(json_extract(frozen,'$.event.metadata.captureGap'),0) <> 1
 AND json_extract(raw,'$.metadata.usageSource')='rollout'
 AND coalesce(json_extract(raw,'$.metadata.captureGap'),0) <> 1
 AND coalesce(trim(json_extract(raw,'$.metadata.traceId')),'')=''
 AND coalesce(trim(json_extract(raw,'$.metadata.codexTurnId')),'')=''
 AND coalesce(trim(json_extract(raw,'$.metadata."turn.id"')),'')=''
 AND coalesce(trim(json_extract(raw,'$.metadata.turn_id')),'')=''
 AND (json_type(frozen,'$.event.inputTokens') IN ('integer','real')
   OR json_type(frozen,'$.event.outputTokens') IN ('integer','real')
   OR json_type(frozen,'$.event.cacheReadTokens') IN ('integer','real')
   OR json_type(frozen,'$.event.cacheCreationTokens') IN ('integer','real')
   OR json_type(frozen,'$.event.costUsd') IN ('integer','real'))
 AND (json_type(raw,'$.inputTokens') IN ('integer','real')
   OR json_type(raw,'$.outputTokens') IN ('integer','real')
   OR json_type(raw,'$.cacheReadTokens') IN ('integer','real')
   OR json_type(raw,'$.cacheCreationTokens') IN ('integer','real')
   OR json_type(raw,'$.costUsd') IN ('integer','real'))
 ELSE 0 END;
