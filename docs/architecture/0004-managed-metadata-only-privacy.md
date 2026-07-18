# ADR-0004: Managed installs are metadata-only

## Status

Proposed source boundary for issue #117. Live rollout and legacy-data
migration remain separate owner-gated work.

## Decision

The ordinary Plimsoll ledger has one supported privacy mode for managed or
upload-enabled installs: `metadata_only`.

- `policy.dataMode=evidence` is rejected before config write or collector
  start. CLI/environment enable attempts, setup, and join fail closed; none
  silently downgrade to metadata.
- Joining marks the collector `managed: true`. Older joined configs are still
  recognized from their tenant/install/upload credentials.
- Status, doctor, collector status, and delivery readiness report
  `metadata_only`, `evidenceVault: not_implemented`, and
  `legacyEvidenceDisposition: local_quarantine_migration_required` literally.
- Evidence-marked rows are excluded at every ordinary upload boundary. The
  bounded outbox migration classifies them from `data_mode` without reading
  their payload. Full-history upload also skips from that header before
  payload normalization.
- Capture sanitization remains before ordinary event/WAL/outbox persistence.
  Export, dashboard, logs, receipts, and request bodies never become an
  alternate raw-content route.

## Legacy-data boundary

The readiness state is a policy declaration, not a live-ledger inventory.
Status/doctor do not scan for evidence rows. When an already-running bounded
delivery migration encounters an evidence-marked row, it leaves the raw row
unuploaded and writes only a content-free terminal reason. `upload-history`
reports the same exclusion without sending the row.

No code in this slice scans, migrates, decrypts, copies, deletes, or prints a
live evidence payload. Operators need a separately designed migration before
legacy evidence can leave local quarantine.

## Not implemented

There is no encrypted evidence vault, vault key lifecycle, evidence upload,
or evidence viewer. Adding one requires a separate security/privacy review;
the historical evidence schemas are not proof that such a runtime exists.

## Proof

`pnpm proof:privacy-mode` uses only fixed temporary homes and loopback/fake
transports. It covers environment/config/CLI enable attempts, join, setup,
start/restart, malformed config, legacy evidence exclusion, literal readiness,
metadata-only capture/upload/dashboard receipts, SQLite/WAL/SHM surface scans,
and fixed time/disk budgets. CI runs it on Node 22.
