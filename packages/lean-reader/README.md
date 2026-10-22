# Plimsoll Lean reader

A single Node 22.20+ ESM executable that reads local Claude Code and Codex session
files, aggregates usage by UTC hour, and sends replacement totals to Plimsoll.
It has no runtime packages, database, local server, or resident daemon. A macOS
LaunchAgent starts one short pass every five minutes.

## Build and check

From the repository root:

```sh
pnpm install --frozen-lockfile
pnpm proof:lean-reader
```

The proof builds `dist/plimsoll-lean.mjs`, verifies that every bundled import is
a Node built-in, and runs the synthetic `node:test` fixtures. Individual commands
are `pnpm --dir packages/lean-reader build` and
`pnpm --dir packages/lean-reader test` after building. The package uses the root
dev toolchain without changing the collector's frozen dependency lockfile.

## Run

```sh
node packages/lean-reader/dist/plimsoll-lean.mjs backfill \
  --since 2026-10-20T00:00:00Z --until 2026-10-22T00:00:00Z --dry-run --json
node packages/lean-reader/dist/plimsoll-lean.mjs run
node packages/lean-reader/dist/plimsoll-lean.mjs status
node packages/lean-reader/dist/plimsoll-lean.mjs print-config
```

`run` reads appended complete lines, updates totals, and attempts delivery once.
The initial pass covers eight days. `backfill` re-reads files modified at or
after the beginning of the requested window and recomputes full totals for its
hours. The end is exclusive. Partial-hour bounds expand to complete UTC hours
so replacing a cloud row cannot erase the rest of that hour. A dry run updates
local state and leaves rows pending; it never reads upload credentials or sends.
`--json` includes the computed rows and allowlisted session/seat metadata.

For an isolated measurement, use `--state-dir /tmp/plimsoll-lean-check`; this
option also works with `run` and `status`. `PLIMSOLL_LEAN_STATE_DIR` is the
equivalent environment override. `--config /path/to/config.json` selects an
alternate configuration. Neither override changes the session roots.

## What it reads

All roots are optional, recursive, and deduplicated through filesystem identity:

- Claude: `~/.claude/projects`, `~/.claude-seats/*/projects`,
  `~/.claude-accounts/*/projects`, and `$CLAUDE_CONFIG_DIR/projects`.
- Codex: `~/.codex/sessions`, `~/.codex-profiles/*/sessions`,
  `~/.codex-accounts/*/sessions`, `~/.codex-lanepool/*/sessions`, and
  `$CODEX_HOME/sessions`.
- Additional roots in the configuration. Claude reads `*.jsonl`, including
  subagents; Codex reads `rollout-*.jsonl`.

Claude reply IDs are counted once across files, including copied resumptions;
synthetic replies are skipped. Codex response IDs are counted once across files.
A rollout containing named usage records never also adds cumulative usage.
Older rollouts use the difference between cumulative readings, with the first
reading as baseline and negative differences treated as counter resets. Cached
and cache-write tokens are subtracted from Codex's inclusive input count.
Output already includes reasoning tokens; they are never added a second time.

Account keys use the collector's exact `providerAccountKey` derivation:
`sha256:` plus the first 16 hex characters of SHA-256 of the serialized OTLP
`{stringValue: rawId}`. That function needs no running collector or salt.
Claude account UUIDs come from the profile's `.claude.json` (`~/.claude.json`
for the default profile without `CLAUDE_CONFIG_DIR`); Codex IDs come from `creator_account_id`. Missing
identity and model fields remain `unknown`.

Project attribution prefers the newest matching line in
`~/.local/state/plimsoll-lean/launch-tags.jsonl`, then the nearest Git repository.
Worktrees use the main repository name. A repository at home or a filesystem
root does not name a project. Later launch tags also update open totals without
re-reading transcripts. Unassigned folders may send up to three relative path
components within home. No Git command or remote is needed for attribution.

## What it sends

Only the fields in `docs/lean/LEAN-V1-CONTRACT.md`:

- Machine name, tool, hashed account key, model, session key, UTC hour,
  request count, fresh input, cache reads, cache writes, and output totals.
- Project hint/source and session first/last activity. Folder labels are sent
  only when a project is absent. Session titles are off by default.
- The newest Codex rate-limit reading per account, including plan, used
  percentage, reset/window, and credit balance when present.

It never sends prompts, responses, instruction text, tool output, file contents,
full working paths, raw account IDs, emails, provider credentials, or Git remotes.
It does not calculate prices: unknown pricing stays unknown in the cloud.

Credentials are loaded only at send time from
`~/Library/Application Support/Plimsoll/collector.config.json`:
`uploadUrl`, `installKey`, `uploadSigningSecret`, and `tenantId`.
The reader posts to the upload URL's origin at
`/api/work-intelligence/lean/usage`, with the collector's timestamp/body
HMAC-SHA256 signature. HTTPS is required except on loopback. Redirects are
refused, and an endpoint override must keep the credential origin. Batches
respect the 1 MiB, 2,000-hour, 500-session, and 50-seat limits. Delivery has one
30-second deadline across all batches. Totals replace cloud rows; retries and
repeated backfills never add to a stored total.

## Configuration and local footprint

The optional `~/.config/plimsoll-lean/config.json` contains:

```json
{
  "credentialsFrom": "~/Library/Application Support/Plimsoll/collector.config.json",
  "roots": [{ "tool": "claude_code", "path": "~/other-profile/projects" }],
  "sendSessionTitles": false,
  "sendFolderLabels": true,
  "dryRun": false
}
```

`endpoint` may supply an override on the same origin. String roots are accepted
as well; paths containing `codex` or `sessions` select Codex, and other paths
select Claude. Explicit `{tool, path}` entries avoid ambiguity. `print-config`
prints only recognized settings and strips endpoint userinfo, query and fragment;
it never opens the credentials file. Credentials do not belong in this config.

State lives in `~/Library/Application Support/Plimsoll Lean/state.json`, with
owner-only permissions and atomic, fsynced temporary-file replacement. It keeps
file device/inode/size/offset, parse context, eight days of open totals and hashed
deduplication IDs, plus unsent rows. Truncation or inode changes restart a file;
counted IDs prevent repeating usage. Partial lines remain at their starting byte
until completed. One PID/token directory lock excludes concurrent runs and recovers dead
owners. Source-read failures abort a pass before publishing incomplete totals.

Reads use 64 KiB chunks. Lines above 1 MiB are discarded unless their first
512 bytes mark relevant usage/context; relevant oversized records are projected
onto a bounded field allowlist without retaining their content. Closed delivered
hours and expired IDs leave state. Pending data is never silently evicted: a
9 MiB serialized state ceiling stops cursor advancement with `state_capacity`.
Delivery failures retain pending data and retry with exponential backoff from
one minute to one hour. `status` reports the last send/error and measured run
footprint. Resolve a persistent capacity error before leaving the reader unattended.

Each pass writes one JSON summary to
`~/Library/Logs/plimsoll-lean/reader.log`, capped at 5 MB with one rotation.
Logs contain counts, bytes, timing, peak RSS and fixed error codes, never usage
rows, project names, provider data, credential values or remote response bodies.

## Install and remove on macOS

Keep the built executable in a permanent location before installing:

```sh
node /absolute/path/plimsoll-lean.mjs install
node /absolute/path/plimsoll-lean.mjs uninstall
```

Installation creates and bootstraps
`~/Library/LaunchAgents/com.plimsoll.lean-reader.plist`, using the Node executable
from the existing Plimsoll LaunchAgent when available, otherwise the current
Node. It sets `StartInterval=300`, `RunAtLoad`, `ProcessType=Background`,
`LowPriorityIO`, and `Nice=10`. Provider-root environment overrides are preserved.
Use the default config/state locations for installed runs. Uninstall unloads and
removes this reader's plist; local state and logs remain available for inspection.
The existing collector continues independently during shadow comparison.
