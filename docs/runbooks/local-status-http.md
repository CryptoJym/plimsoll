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
{"ok":true,"instanceId":"<random v4 UUID, new on every collector start>"}
```

That body is the leak-gate: `scripts/authenticated-ingestion-proof.ts` requires
the key set to be exactly `instanceId` and `ok`. `instanceId`
(eco-6hoxj.163.34) is drawn at random once per collector run. It names no
host, user, path or credential, and is not uploaded. The collector writes the
same value to its private `status-summary.json`. No `version` or any other
field is added. Package version is on `plimsoll status` (`appVersion`) and
`plimsoll doctor --read-only --json` (`version`).

A public id can be replayed by any process that later holds the port, so a
local reader proves it is talking to the collector run with a challenge
(eco-6hoxj.163.34, round 4):

```http
GET /healthz?challenge=<43 base64url characters: 32 fresh random bytes> HTTP/1.1

HTTP/1.1 200
{"ok":true,"instanceId":"<id>","proof":"<43 base64url characters>"}
```

`proof` is the unpadded base64url HMAC-SHA256, under this run's
`healthzKey`, of `plimsoll.healthz-proof/v1`, the port the request arrived
on, the `instanceId` and the challenge, joined by newlines. The key is 32
random bytes drawn once per run; it is written only to the 0600
`status-summary.json` and is in no HTTP response, so only the collector run
and a reader of that file can compute or check a proof. A fresh challenge
per check stops replay; the port stops a relay from another listener. Any
other query (a malformed or repeated challenge, another parameter) is `400
{"ok":false,"reason":"invalid_challenge"}`. The macOS menubar calls the
collector running only on a proof that verifies (constant-time compare).

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
| macOS menubar (`packages/mac-menubar`) | Ran `plimsoll status` per refresh | Reads `status-summary.json` (below) and verifies a fresh `GET /healthz?challenge=` proof with its `healthzKey`; runs no `plimsoll` command |

## Status summary file

The running collector writes `status-summary.json` in its home every 15 s
and once as soon as it listens (eco-6hoxj.163.34). It writes a temp file,
sets it to mode 0600 whatever the umask, fsyncs it and renames it into
place, so a reader never sees a partial file. Every step is asynchronous,
off the event loop that serves intake, and a write still in progress when
the next one is due makes that one skip. The writer pins its home by
device and inode when it starts and checks it again before each temp file
and each rename; if the home was replaced (renamed, or swapped for a
symlink), it stops writing and warns once (`home_changed`). Shutdown waits
for a write in progress, so no temp file is left. It holds exactly:

```json
{"schema":"plimsoll.status-summary/v1","instanceId":"<the /healthz value>",
 "healthzKey":"<this run's /healthz proof key, 43 base64url characters>",
 "collectorVersion":"<package version>","port":48271,"updatedAt":"<ISO time>",
 "stats":{"count":0,"tokenAttributedEvents":0,"totalInputTokens":0,"totalOutputTokens":0}}
```

`stats` are the lifetime counters from the daemon's `/status` cache; the
write reads no ledger row, so it costs the same on any ledger size. It is
`null` until the projection is ready. The file names no collector
credential, path, account or event; `healthzKey` only answers `/healthz`
challenges for this run and unlocks nothing else. It stays after the
collector stops (until `plimsoll lifecycle purge`), so a reader must check a
fresh `/healthz` challenge proof before calling the collector running.

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

# Sweep-boundary capture-health rule (eco-6hoxj.155; no collector required)
python3 scripts/native-status-read.py --self-test
```

Default collector port is `48271`. Override with `--port` or `PLIMSOLL_PORT`.

A `0 this sweep, N this tick` capture-health sample is a transient unless it
persists across two reads at least 10 s apart.

## Proof

- `pnpm proof:authenticated-ingestion` — `/healthz` is the only minimal
  unauthenticated surface; `/status` and `/api/*` require the management
  credential.
- `pnpm proof:status-http-rollout` — repeats that contract and runs the
  migrated fleet reader against `/healthz` only.
- `pnpm proof:status-summary` — the summary file is private, exactly shaped,
  atomic for a concurrent reader, written without a SQL statement, free of
  credentials and paths, and names the same run as `/healthz`; a real
  `plimsoll start` daemon writes it and leaves no temp file when it stops.
  With fsync delayed 250 ms, a zero-delay timer and `/healthz` stay prompt;
  a write is one exclusive create, chmod, write, fsync, close and rename
  (no read, no synchronous call) per 15 s; the file is 0600 under umask
  0777; a swapped home is refused; a failed or interrupted write leaves no
  temp file. `/healthz?challenge=` answers an HMAC that verifies with the
  file's key and matches the test vector the menubar tests pin; a responder
  that knows the `instanceId` but not the key (the round-3 reply, a wrong
  key, a replayed proof, a proof relayed from another port) is refused; the
  key is in no HTTP response and no daemon output.
