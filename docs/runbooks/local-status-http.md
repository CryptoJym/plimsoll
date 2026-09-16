# Local HTTP status rollout (eco-6hoxj.154)

Decision date: 2026-09-16 (America/Denver).

## Decision

**Keep `/status` closed.** When local ingest credentials are provisioned, an
unauthenticated `GET /status` continues to answer `401` with
`management_credential_required`. Do not restore a raw status payload for
monitors. Do not put version, runtime identity, capture health, delivery, or
ledger state on the unauthenticated surface.

Unauthenticated liveness already exists and stays minimal:

```http
GET /healthz HTTP/1.1
Host: 127.0.0.1:<port>

HTTP/1.1 200
{"ok":true}
```

That body is the leak-gate: `scripts/authenticated-ingestion-proof.ts` requires
the key set to be exactly `ok`. This rollout does not add `version` or any
other field. Package version is on `plimsoll status` (`appVersion`) and
`plimsoll doctor --read-only --json` (`version`).

Full status remains one of:

- `plimsoll status` (CLI; presents the management credential to the daemon)
- `GET /status` with `x-plimsoll-token: <managementRead>`

`plimsoll doctor --read-only --json` is the same credentialed path for
readiness.

## Why not reopen `/status`

Issues 0056 / 0059 (#104 / #108) already required: loopback is not
authentication; only minimal health is unauthenticated; management reads use a
separate credential. `/status` carries runtime identity, home identity hash,
capture health, retention, delivery, and sync scheduling. That is operator
state, not a liveness probe.

Fleet acceptance on 2026-09-15 (MacBook, collector 0.7.30) observed the
intended gate: `curl http://127.0.0.1:<port>/status` →
`management_credential_required`, while `plimsoll status --json` worked.

## Readers

| Reader | Was | Now |
|---|---|---|
| Fleet `native-status-read.py` (raw `GET /status`) | Unauthenticated `/status` | In-repo `scripts/native-status-read.py`: `GET /healthz` for liveness; `plimsoll status` for payload |
| Older curl / HTTP monitors | Raw `/status` | `GET /healthz` if they only need up/down; otherwise `plimsoll status` |
| `plimsoll status` / `plimsoll doctor` | Already credentialed | Unchanged: `readDaemonState` / `checkCollectorConnectivity` send `x-plimsoll-token` when provisioned |
| LaunchAgent load readiness / `observeCollectorListener` | Already credentialed | Unchanged: `lifecycleProbeHeaders()` presents the management credential |
| `scripts/install-artifact-proof.ts`, `scripts/packaged-runtime-proof.ts` | Already `/healthz` | Unchanged |
| Isolated proofs without `localAuth` | Legacy unauthenticated `/status` | Unchanged on purpose: credentials absent means the legacy loopback boundary |

## Operator commands

```bash
# Liveness only (no credential). The collector sets keepAliveTimeout=0;
# curl is fine; raw HTTP clients should send Connection: close (the in-repo
# reader uses HTTP/1.0).
curl -sS --max-time 3 http://127.0.0.1:48271/healthz
python3 scripts/native-status-read.py --liveness-only --port 48271

# Full status (credentialed CLI; never prints the token)
plimsoll status
plimsoll doctor --read-only --json
python3 scripts/native-status-read.py --port 48271
```

Default collector port is `48271`. Override with `--port` or `PLIMSOLL_PORT`.

## Proof

- `pnpm proof:authenticated-ingestion` — `/healthz` is the only minimal
  unauthenticated surface; `/status` and `/api/*` require the management
  credential.
- `pnpm proof:status-http-rollout` — repeats that contract and runs the
  migrated fleet reader against `/healthz` only.
