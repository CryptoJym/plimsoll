# 0038 — Outcomes sync: the workspace holds the session↔PR outcome join

## TL;DR
- The local efficiency report (issues 0002/0009) already joins ledger sessions to GitHub PRs via linkage hashes and derives merge status, check results, and short-horizon rework — but the hosted workspace's `WorkArtifact`/`ReviewOutcome` tables held **0 rows**, so hosted VDY (cloud #24 Phase D3) and per-person efficiency (D4) had no outcome data. Now `plimsoll sync-outcomes --repository owner/repo` pushes that same join to the workspace's existing github-outcomes route (cloud C8).
- Live run on the real workspace (tenant `753a5a4f…`): **28 pull-request artifacts + 56 review outcomes** across `cryptojym/plimsoll` (17/34) and `cryptojym/plimsoll-cloud` (11/22); idempotent re-run grew the tables by exactly **zero** rows; the full chain `ai_work_sessions → work_repositories (remote_url_hash == project_key) → work_artifacts → review_outcomes` returns real rows.
- Deterministic ids end to end: `artifact:<externalId>` / `outcome:<externalId>:<kind>` strings → tenant-salted UUIDs on the cloud → upserts converge instead of duplicating (a check status that flips updates the one `:check` row in place).
- Found-and-fixed on the cloud half: the C8 lane's session/actor reference resolution used the inherited v1–5-only uuid test — every **codex UUIDv7** session link would have been dropped silently. Same fix shape as D1's event-lane `classifySessionId` (`postgresUuidOrNull`).

## Scope
A `sync-outcomes` CLI command (sibling of `push-repo-labels` / `upload-history --sessions`): ledger sessions (read-only) ⋈ GitHub PR state for ONE explicitly named repository → one signed batch to `/api/work-intelligence/github-outcomes`. Cloud half (plimsoll-cloud `outcomes-ingest` branch): the v7 reference fix + D2 proof section pinning the contract. It does NOT add new GitHub endpoint families or scopes beyond the local report's fetch surface (pull list, check-runs ≤20 joined PRs, revert scan ≤3 pages, reopen events ≤20 PRs), does NOT push PR titles/bodies/diffs/paths (sha + URL evidence only — the report's title-based revert matching is deliberately dropped wire-side), and is deliberately NOT in the daemon's 5-minute cycle: the join needs a named repo + a GitHub token from the operator's environment, and a background GitHub poll would be new scraping cadence. Own-data sync — open/paid boundary unchanged.

## Context
- Wire contract: `githubOutcomeIngestBatchSchema` (shared on both sides since C8) — `{tenantId, repository:{provider,owner,name,remoteUrlHash}, artifacts:[…≤500], outcomes:[…≤1000]}`, strict, one repository per batch, install-key + HMAC machine auth (same transport contract as `push-repo-labels`).
- Linkage spine the D3/D4 joins stand on: `repository.remoteUrlHash == session.projectKey == event projectKey` (hash of the normalized remote), `artifact.sessionId` = the dominant linked session through D1's `ensureUuidSessionId` mapping, every linked session id in `artifact.metadata.linkedSessionIds`, plus `branchHash`/`headSha`/`mergeCommitSha`/`joinedVia` in metadata.
- Rework windows: revert/reopen signals are filtered with `validatedDeliveryYieldV2`'s exact window semantics (issue 0009); the proof pins parity, so pushed rework flags can never drift from what the local yield math excludes. In-window rework → artifact status `reverted`/`reopened` + a dedicated outcome row with sha/url evidence.
- Honest counters: `acceptedArtifacts`/`acceptedOutcomes`/`detachedSessionRefs`/`detachedActorRefs` come from the server response, never assumed.

## Evidence (live runs, 2026-06-13, tenant 753a5a4f…)
Run 1 (`sync-outcomes --repository CryptoJym/plimsoll --until 2026-06-13T01:44:20.315Z`):
```
{"status":"outcomes_sync_done","repository":"CryptoJym/plimsoll","pullsExamined":34,"pullsJoined":17,"sessionsLinked":2,"artifactsSent":17,"artifactsAccepted":17,"outcomesSent":34,"outcomesAccepted":34,"detachedSessionRefs":0,"detachedActorRefs":0,"durationMs":31016}
```
Same for `CryptoJym/plimsoll-cloud`: 13 examined, 11 joined, 11/11 artifacts, 22/22 outcomes. Idempotent re-run (same `--until`): accepted 17 again, DB counts unchanged — `work_artifacts` 28 → 28, `review_outcomes` 56 → 56 (`work_repositories` stayed 50: the batches matched the rows `push-repo-labels` disclosed). Statuses all `merged`; outcomes 28 `merged` + 24 `passed_check` + 4 `unknown_check`; no rework signals inside the 14d window.

Full-chain sample (read-only Prisma on the live DB), session column join:
```
session ec1cafd2-74ba-4297-aabd-4af470128591 (CLAUDE_CODE) → cryptojym/plimsoll-cloud
  → github.com/cryptojym/plimsoll-cloud/pull/11 (merged): merged, passed_check
  → …/pull/12 (merged): merged, passed_check
  → …/pull/13 (merged): merged, passed_check
```
All 28 artifacts carry a resolved `session_id`; `metadata.linkedSessionIds` resolve 1/1 to real `ai_work_sessions` rows; the hash spine (`session.project_key == repository.remote_url_hash`) returns rows for both repos.

## Acceptance Criteria
- [x] `pnpm proof` green: 85 → **90** checks (join parity + watermark + repo scope; rework-window parity with yield v2; deterministic ids + shape + linkage; signed e2e with byte-identical re-run + dry-run sends nothing; CLI/disclosure/no-raw-content wiring).
- [x] Cloud proof green: 108 → **112** (tenant-salted storage ids + v7 reference resolution; the CLI's exact batch validates / poisoned metadata fails closed; linkage spine columns; cross-tenant refs fail closed as 403).
- [x] Live: `WorkArtifact` 0 → 28, `ReviewOutcome` 0 → 56 on the real tenant; idempotent re-run inserts 0; chain join returns rows.

## Operational Boundaries
- `pnpm proof` stays green; no raw content in metadata mode (the forbidden-fields gate runs client-side before send AND server-side on ingest).
- Ledger opened strictly read-only; nothing marks rows; the daemon is untouched.
- Naming a repo is the same deliberate disclosure as `push-repo-labels` (owner/name + remoteUrlHash cross the wire — nothing else new).

## Notes For Future Agents
- The cloud's v7 fix ships on the `outcomes-ingest` branch; until it deploys, a codex-v7-dominant artifact would store `session_id` NULL (silently — the pre-fix code didn't even count the detach). This live dataset's dominant sessions are all claude v4, so current rows are complete; **re-run `sync-outcomes` once after the cloud deploy** and the upsert fills any missing links in place.
- Data-thinness for D3 (hosted VDY): session granularity is coarse — stitched daemon sessions are long-lived, so 2 distinct sessions cover all 17 plimsoll PRs in the window. Tokens-per-PR via `artifact.sessionId` alone will mis-attribute; D3 should divide through the per-PR session SETS (`linkedSessionIds`) or event-level branch hashes, and say so on the surface.
- GitHub state is as-of-run (no watermark possible on the GitHub side); `--until` watermarks only the ledger half. Convergence comes from deterministic ids + upserts.
- The check outcome id is `outcome:<externalId>:check` (not `:passed_check`) so a status flip updates one row instead of accumulating contradictory siblings.

## Open Questions
- Should `sync-outcomes` learn a `--all-labeled` mode (walk every locally labeled github.com repo) once more of the owner's repos need coverage? Per-repo explicitness was chosen for the disclosure semantics.
