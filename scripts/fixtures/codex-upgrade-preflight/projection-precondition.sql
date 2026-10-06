-- Diagnostic precondition only; old complete=1 does NOT disable the head's
-- new authority scan. On first .51 upgrade, the new version is absent and
-- head initialization resets that scan. Queue/migration gates cannot prove
-- raw historical immutability through automatic projection maintenance.
SELECT c.complete AS current_scan_complete,
 EXISTS(SELECT 1 FROM pragma_table_info('codex_duplicate_fact_scan')
        WHERE name='authority_version') AS head_authority_rule_column_present,
 (SELECT backfill_complete FROM dashboard_projection_control WHERE singleton=1) AS projection_backfill_complete,
 EXISTS(SELECT 1 FROM dashboard_projection_repairs LIMIT 1) AS projection_repairs_present
FROM codex_duplicate_fact_scan c WHERE c.singleton=1;
