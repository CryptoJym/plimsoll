# Changelog

## Unreleased

### Fixed

- Automatic maintenance now keeps committed progress across deadlines, bounds
  cursor and enrichment work, and reaps disposable workers before replacement.
- Maintenance worker startup is separated from ledger initialization and has a
  45-second process-readiness deadline for hosts under heavy disk load.
- `maintenance_failed` log receipts now include a path-free error class and
  message (at most 200 characters), failure stage, elapsed milliseconds, and
  whether a progress frame was acknowledged.

