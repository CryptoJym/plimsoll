# Hostile fixtures — lane receipt controls

Synthetic, self-contained JSON used by `pnpm proof:lane-receipts` to exercise
the issue #176 workflow controls against adversarial inputs. Nothing here is
real: every SHA, path, token-shaped string, and lane ID is invented. The
credential-looking strings are deliberately fake sentinels and must never be
copied into real receipts.

| Fixture | Control exercised |
| --- | --- |
| `missing-fields.json` | preflight parse fails closed on missing/invalid fields |
| `contradictory-states.json` | contradiction detection (COMPLETE+DIRTY, BLOCKED without reason) |
| `tampered-head.json` | receipt head vs preflight head mismatch; malformed SHA |
| `duplicate-lane-ownership.json` | census duplicate lane ownership + duplicate heads |
| `dirty-drift.json` | census dirty-unowned lanes, base drift, missing worktrees |
| `stale-liveness.json` | ACTIVE liveness older than the staleness budget is not trusted |
| `retry-overwrite.json` | attempt history tamper: erased failure, rewritten source head |
| `secret-like-strings.json` | privacy guard: secrets, home paths, forbidden fields, judgments |
