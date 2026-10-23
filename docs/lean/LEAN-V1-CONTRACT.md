# Plimsoll Lean v1 contract

Owner: James. Orchestrator: Claude session d96c6a97 (2026-10-22). Status: shadow build.

Plimsoll Lean tracks token spend by project with the smallest system that answers the question:

1. A reader on each Mac reads the usage lines Claude Code and Codex already write, adds them up by hour, and sends the totals.
2. The cloud keeps those hourly totals in one table.
3. Prices, project names and the subscription split are worked out when a page reads the numbers.

The old collector and pipeline keep running untouched until a week of shadow numbers match by project.
Evidence for the design: `plimsoll-simplify-20261022/` (prototype `spendlite.py`, one day of Studio 0 = 159 rows, 57 KB).

## 1. Upload: reader to cloud

`POST <origin>/api/work-intelligence/lean/usage`

`<origin>` is the origin of the collector's configured `uploadUrl`. The `/api/work-intelligence/*` prefix is already outside the Clerk matcher.

Headers, the same scheme the collector uses (`packages/collector-cli/src/http-transport.ts`) and the cloud verifies (`src/lib/ingest-signature.ts`):

- `content-type: application/json`
- `x-plimsoll-install-key: <installKey>`
- `x-plimsoll-upload-timestamp: <ISO-8601 time>`
- `x-plimsoll-upload-signature: sha256=<hex HMAC-SHA256(uploadSigningSecret, "${timestamp}.${rawBody}")>`

Limits: raw body at most 1 MiB, at most 2,000 `hours` rows, 500 `sessions`, 50 `seats`. The reader splits larger sends.

```json
{
  "schema": "plimsoll.lean.usage.v1",
  "tenantId": "00000000-0000-0000-0000-000000000000",
  "reader": { "version": "0.1.0", "machineName": "Studio0" },
  "sentAt": "2026-10-22T13:40:00.000Z",
  "hours": [{
    "hour": "2026-10-21T14:00:00.000Z",
    "tool": "claude_code",
    "accountKey": "a1b2c3d4e5f6a7b8",
    "model": "claude-opus-5-5",
    "sessionKey": "d96c6a97-64bd-48fd-97ec-13c939908aeb",
    "projectHint": "Second String",
    "projectSource": "git_repo",
    "requests": 12,
    "freshInput": 40,
    "cacheRead": 5000000,
    "cacheWrite": 66758,
    "output": 21650
  }],
  "sessions": [{
    "tool": "claude_code",
    "sessionKey": "d96c6a97-64bd-48fd-97ec-13c939908aeb",
    "title": null,
    "folderLabel": "Projects",
    "projectHint": null,
    "firstSeen": "2026-10-22T12:41:21.000Z",
    "lastSeen": "2026-10-22T13:39:02.000Z"
  }],
  "seats": [{
    "tool": "codex",
    "accountKey": "1bfc416f00000000",
    "plan": "promax",
    "usedPercent": 100,
    "windowMinutes": 10080,
    "resetsAt": "2026-10-28T16:10:16.000Z",
    "creditsBalance": "20153.957075",
    "observedAt": "2026-10-22T13:10:35.529Z"
  }]
}
```

Field rules:

- `tool`: `claude_code` or `codex`. More tools come later; the server stores any lowercase `[a-z_]{2,32}`.
- `hour`: the UTC start of the hour.
- `accountKey`: the same account key the collector uploads today, so existing account labels and seat links keep matching. If the collector's derivation cannot be reused, the first 16 hex of `sha256("plimsoll-lean-v1:" + tool + ":" + rawAccountId)`. Raw account ids and emails never leave the machine. `"unknown"` when the file names no account.
- `model`: as the tool wrote it; `"unknown"` when absent.
- `sessionKey`: Claude Code `sessionId`; Codex `session_meta.payload.id`.
- `projectHint` and `projectSource`: a launch tag (`launch_tag`), else the git repository that holds the working folder (`git_repo`; a worktree names its main repository; a repository at the home folder or a disk root names no project), else `null` with `none`.
- Token fields are non-negative integers and are the **total** for the row's key and hour, never a delta. `freshInput` excludes cache reads and cache writes for both vendors (Codex: `input_tokens - cached_input_tokens - cache_write_input_tokens`, floor 0). `output` includes reasoning or thinking tokens.
- `sessions[].title`: only when the reader's config turns titles on (Claude Code `summary` lines); otherwise `null`. `folderLabel`: the working folder relative to the home folder, at most 3 parts, sent only when `projectHint` is `null`.
- `seats[]`: the newest reading per account. Codex writes `rate_limits` into `token_count` events (plan, percent used, window, reset time, credit balance). Claude seats are not read in v1.

Server semantics:

- `hours` upsert on `(tenant, deviceInstallId, tool, accountKey, model, sessionKey, hour)`. The new row **replaces** the stored row. Nothing is ever added to a stored total, so a re-send can never double count.
- `sessions` upsert on `(tenant, tool, sessionKey)`: keep the earliest `firstSeen`, the latest `lastSeen`, and the newest non-null `title`, `folderLabel` and `projectHint`.
- `seats` upsert on `(tenant, tool, accountKey)`: keep the reading with the newest `observedAt`.
- Auth: a registered install with an HMAC signature (`mode === "registered_install"` and `signature === "hmac_sha256"`), bound to `body.tenantId`, as the integrations route requires.
- Responses: `200 {"ok":true,"hours":{"inserted":n,"replaced":m},"sessions":n,"seats":n}`; `400 {"error":"invalid_lean_upload"}`; `401`/`403` with the machine-auth error codes; `413 {"error":"body_too_large"}`; `5xx` means retry.

## 2. Read model

The cloud side of the contract (tables, pricing, project names and the subscription split) lives in the private cloud repository under `docs/lean/LEAN-V1-CONTRACT.md`.
