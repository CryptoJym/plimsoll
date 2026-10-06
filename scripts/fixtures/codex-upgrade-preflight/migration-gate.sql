-- All three values must match: complete=1, privacy_version>=1, cursor=max.
SELECT migration_complete, privacy_migration_version,
 migration_cursor_rowid,
 (SELECT coalesce(max(rowid),0) FROM buffered_events) AS raw_high_water,
 CASE WHEN migration_complete=1 AND privacy_migration_version>=1
  AND migration_cursor_rowid=(SELECT coalesce(max(rowid),0) FROM buffered_events)
 THEN 1 ELSE 0 END AS migration_gate_pass
FROM upload_control WHERE singleton=1;
