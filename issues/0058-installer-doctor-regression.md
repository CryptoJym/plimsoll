# 0058 — Installer regression: source setup and truthful read-only doctor

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/107

## TL;DR
- Fix the accepted-main source installer so it uses the explicit development LaunchAgent path instead of failing before service installation.
- Add a genuinely read-only doctor mode and make `ok`/exit status reflect service, configuration, version, and connectivity truth.
- This is the bounded first slice of 0055 / #103; it does not publish a package or activate the live collector.

## Scope
`install.sh`, CLI doctor/install preflight, generated config verification, focused proof, and documentation. Full update/rollback/uninstall/provenance remains #103.

## Evidence
- `install.sh` calls `install-launch-agent` without `--dev`; accepted-main CLI rejects source installs unless `--dev` is present.
- Current doctor hardcodes `ok:true` even with a missing LaunchAgent and unreachable collector, and creates config/ledger while diagnosing.

## Acceptance Criteria
- [ ] Source installer calls the supported `--dev` path, pins Node `<25`, fails closed on doctor, and supports a no-mutation dry-run in an isolated HOME.
- [ ] `doctor --read-only --json` does not create config, ledger, plist, logs, WAL/SHM, or directories.
- [ ] Doctor returns `ok:false` and nonzero when required config, full Claude/Codex telemetry wiring, expected runtime identity/version, LaunchAgent, or connectivity is absent/conflicted.
- [ ] Readiness levels distinguish `not_installed`, `configured`, `service_ready`, and `signal_verified`; a cold ledger is not signal-verified.
- [ ] Existing config preview/backups remain idempotent and no account credentials are read or copied.
- [ ] Packaged and source fixtures use neutral cwd/HOME and Node 22; proof catches the exact current regressions.
- [ ] README/package README/Product Gate stop describing `doctor` alone as successful install/capture proof.

## Operational Boundaries
- Tests use isolated temporary homes only; no real LaunchAgent registration, tool config, or collector start.
- No npm publish, tag, hosted join, or live rollout.
