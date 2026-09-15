# Plimsoll

**The load line for your AI spend.**

Plimsoll is a local-first telemetry collector for AI coding agents (Claude Code, Gemini CLI, Grok, and Codex today) that answers the question every team is guessing at:

> **What did we actually get for those tokens?**

It captures token usage, cost, tool behavior, and session structure on your machine, joins sessions to shipped outcomes (merged PRs, passing checks), and computes the economics — tokens per merged PR, cost per validated outcome, and where your spend is producing nothing.

```
$ pnpm report -- --repository your-org/your-repo

  PR #28 — merged ✓ checks passed ✓
  1 session · 41,799 in / 188,834 out tokens · 30.4M cache reads
  cost: $48.46
```

That's a real number from Plimsoll measuring the pull request that built Plimsoll.

## Why "Plimsoll"

In 1876 Samuel Plimsoll forced shipowners to paint a load line on every hull. Overloading deaths collapsed — not because anyone wrote a smarter regulation, but because the limit became **visible to anyone standing on the dock**.

AI spend today is a hull with no line: invisible loading, vendor dashboards that stop at org-level totals, and no connection to what shipped. Plimsoll paints the line — and like the original, it's painted on the *outside*: every byte that this collector records, suppresses, hashes, or uploads is open source and inspectable in this repository.

## How it works

```
Claude Code ── hooks (HTTP) ─────────────┐
Claude Code ── OTLP logs + metrics ──────┤
Codex ──────── OTLP logs/traces/metrics ─┤
                                         ▼
                          Plimsoll collector (localhost:48271)
                          · explodes OTLP per record — every API call's
                            tokens, cost, model, session captured
                          · derives action classes from tool names
                            (shell / edit / read / mcp / browser)
                          · suppresses raw content BEFORE persistence:
                            prompts, outputs, commands, file bodies,
                            tool arguments — never stored in metadata mode
                          · hashes identifiers (emails, paths, branches)
                          · resolves git linkage keys (hashed remote,
                            hashed branch, plain commit sha)
                                         ▼
                          local SQLite ledger (~/Library/Application
                          Support/Plimsoll, 90-day retention, indexed)
                                         ▼
            ┌────────────────────────────┴───────────────────┐
   local reports (free, forever)              optional hosted sync
   tokens/cost per repo, per PR,              (team rollups, benchmarks —
   per model, per session;                    watermark-based, signed,
   Validated Delivery Yield                   off by default)
```

The outcome join uses **linkage keys**: both Plimsoll and the GitHub side hash the same normalized inputs (remote URL, branch name), so sessions and pull requests join by construction while the raw strings never leave your machine. Commit shas stay plain — they're already public on GitHub.

A hook event the collector cannot accept right now is **spooled, not dropped**.
The spool is one private file per event under `hook-spool/` in the Plimsoll
home, carrying no credentials, which the collector drains back through the
ordinary hook path every five seconds once the ledger is free — so a recovered
event lands in the ledger attributed exactly like a live one.

Spooling happens in **two** places, because hooks reach the collector in two
ways:

- **The collector's own intake**, for every hook path. The managed hooks post
  straight to `http://127.0.0.1:<port>/hooks/<source>` — Claude Code's is an
  `http` hook, the Codex and Grok hooks are `curl` commands — and never run a
  Plimsoll command. When such a post is authorized and admitted but the local
  ledger stays busy past its retry budget, the collector writes the spool file
  itself and answers `202 {"status":"hook_spooled"}` instead of the old
  `503 storage_busy_retry`. It answers 202 only after flushing the private
  temporary file, publishing it by rename, and flushing the containing directory
  (including the home entry for a newly created spool directory). A failed flush
  is not acknowledged; if the spool cannot be written (its bounds are reached,
  the disk refuses, or `PLIMSOLL_HOOK_SPOOL=off`), the answer stays exactly
  today's 503 so the loss stays visible.
- **The `forward-hook-http` client**, for a host whose hook runs that command.
  It spools when the collector answers 503, answers 408
  (`request_deadline_exceeded`), or is not listening at all (connection refused
  during a managed update window).

What is still lost: a request whose **body never finished arriving** (the 408 —
there is nothing whole to spool), and, for the `http`/`curl` hooks, a post that
never reaches a listening collector at all (connection refused — the collector
is not there to spool it, and those hooks have no Plimsoll process of their own
to do it for them; `forward-hook-http` hosts do spool that case).

The spool is **at-most-once**: every one of those outcomes proves the collector
stored nothing, so a spooled event is never a duplicate — with one documented exception: if the publication flush fails and the rollback of the unacknowledged envelope is also refused (the double-fault residual described below), a visible envelope can remain behind a 503 and be replayed. At the intake, the 503
class is raised only when the ledger write did not commit — the durable append
runs in a single `BEGIN IMMEDIATE` transaction that SQLite has rolled back by
the time the busy error escapes it — and it is the only outcome that is spooled
there. The window the spool does not close is the collector dying mid-request —
a connection reset or a closed socket after the body was sent may mean the row
was already written, so that event still fails loudly and is lost rather than
risk double-counting it in cost and usage. Closing that window needs idempotent
replay (a client-minted event id) and is tracked separately.

What is on disk is **not** the raw body. Before the file is written — by either
writer, through the same function — the collector's own pre-write suppression
rule is applied to it: every key the collector would strip — raw prompt, output
and tool content, credential-like names, file and transcript paths — keeps its
name and loses its value. So
`hook-spool/` holds no more than the local ledger is allowed to hold, and the
collector's own suppression produces exactly the same receipts when the event is
replayed. The deliberate exceptions are two short lists of keys. The first
is what the collector reads *before* suppressing it to derive something it
stores — the working directory it turns into repository linkage, and the hook
event name it turns into the event type. The second is the protected identity
names (`username`, `user_id`, `account_id`, `user.id`, `workspace_root`, …):
the ledger keeps the *hash* of those values rather than dropping them, so an
emptied one would persist the hash of an empty string instead of the hash of the
real identity — the spool file holds the raw value until the collector applies
the event, and holds it for that reason. Blanking either list would quietly make
a recovered event worse than a live one, so their values stay; every one of them
is listed by name, with the reason, in
[docs/privacy-spec.md](docs/privacy-spec.md) under *Where captured data rests on
disk*, which is generated from the code that enforces it.

A recovered event carries the time it **arrived**, not the time the drain got to
it: the file is stamped when it is spooled — with the hook process's own time
when the client spools it, with the daemon's request receive time when the
intake spools it — and the drain hands that stamp to the collector as the
event's timestamp, so a spooled event lands in the same cost and usage window
it would have live. A body that carries a
timestamp the collector can actually use keeps it, untouched — usable is the
collector's own test, so a numeric epoch, an empty string or a future-dated
time is not a timestamp for this purpose and the hook's stamp is used instead,
exactly as it would be live.

`deferred` counts failed drain attempts, not unique events: the same queued
file can add one on multiple ticks. Use `pendingFiles` and its age for the
current backlog. The counter is cumulative and does not reset when it drains.

File/directory synchronization uses the same OS primitives as the other durable
local writers. The primitive is Node's `fs.fsyncSync`, which on macOS is not a plain `fsync(2)`: libuv's `uv_fs_fsync` asks for `fcntl(F_FULLFSYNC)` there and falls back to `F_BARRIERFSYNC`, then to `fsync(2)`, whenever that `fcntl` returns non-zero — a filesystem that refuses it, but also a transient error such as `EIO` or `EINTR` — with no signal to the caller that a weaker flush was used. On a volume that honours `F_FULLFSYNC` — an internal APFS disk is the normal case — the drive is told to flush its own volatile write cache, so the power-loss window is closed as well as the process-crash one, with no native addon. Where the filesystem refuses it and libuv falls back, the process-crash window is still closed and the power-loss one is only narrowed; that is the case on external or virtualised volumes with a writeback cache. Either way this is not a hardware power-cut certification. If publication's
directory flush fails, the writer tries to hide its unacknowledged envelope as a
bounded orphan temporary and returns failure. If the filesystem also refuses
that rollback, a visible unacknowledged file can remain; no universal exactly-once
claim is made for storage failures or interrupted delivery.

The counters (`recovered`, `rejected`, `deferred`, `spooledAtIntake` — how many
events the collector's own intake spooled — `refused` — how many it tried to
spool and could not — pending files and their age) are in
`plimsoll status`, `plimsoll doctor`, and the collector's `/status` under
`hookSpool`; `enabled` there is the running collector's kill-switch state, read
from the collector itself — `plimsoll status` asks the daemon for it in one
request bounded by `PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS` (3 s by default), and
is otherwise a purely local read. It reads `null` with `enabledSource:
"collector_unreachable"` when the collector cannot be asked, and `null` with
`enabledSource: "collector_too_old"` when a collector older than 0.7.22 answers
— reachable and healthy, but with no drain, so the spool holds its events until
that collector is updated. Doctor says so plainly if anything has been pending
for more than ten minutes. The counters are written once per drain tick, after
the tick has applied its files, so `status`/`doctor` can lag the ledger by the
remainder of a 5 s tick: an event can be queryable in the ledger a moment before
`recovered` counts it. The collector's `/status` answers from the drain's
in-memory copy, which every tick refreshes — including a tick that found nothing
to drain, so a host whose spool is refusing every event (a refusal writes no
file, so it gives the drain no work) still shows `refused` rising there, within
that same one tick. Rolling back to a collector that predates `refused` (0.7.24
and older) is safe — it ignores the key when it reads the counters file — but
its first counters write — an intake spool or a drain tick with work — rewrites
the file without it, so an accumulated refusal count is lost at that point; the
other counters are unaffected.
Contract rejections are **not** spooled: a body the collector refuses on its merits (4xx other than 408)
still fails the hook loudly, and a spooled file the drain cannot apply is
quarantined under `hook-spool/rejected/` rather than retried forever. An
operator watching `collector.err.log` sees intake spooling happen without any
body: the first spool in each minute prints `hook_spooled_at_intake` with its
source and client class, and the rest of that minute is reported by one
`hook_spooled_at_intake_summary` line with the count — while the
`storage_busy_retry` rejection summary now counts only the posts that were
really refused, the ones whose spool write failed. Set
`PLIMSOLL_HOOK_SPOOL=off` in the collector's environment to restore the previous
drop-and-log behaviour on both paths.

Busy rejections are classified by route inside that same minute. The first
`storage_busy_retry` rejection of a 60 s window prints
`{"error":"collector_request_rejected","reason":"storage_busy_retry","clientClass":"claude_code","route":"/hooks/claude-code","spoolAttempted":…,"spoolRefused":…,"spoolRefusedReason":…}`
— one line, naming the one route that opened the window — and the window's
closing summary splits its total across every route it saw:
`{"error":"collector_request_rejected_summary","reason":"storage_busy_retry","clientClass":"claude_code","count":3,"suppressed":2,"intervalMs":60000,"action":"retry_after_backoff","routes":{"/hooks/claude-code":2,"otlp":1}}`.
`count` is still the window total for the `(reason, clientClass)` pair; in
unseeded production windows the `routes` values sum to it (proof/recovery
`initialByReason` counts have no route attribution and are excluded from that
split), and `routes` follows every key the older collector printed, so every
busy line production emits today is still the older collector's line byte for
byte with `routes` appended. Route-classified windows never carry record
statistics; if a caller ever hands a record diagnostic to a busy rejection (no
production caller does today), its statistics are dropped at ingest and the drop
is counted in `recordDiagnosticsDiscarded` — emitted only when non-zero,
between `action` and `routes` — so parsers must treat that key as optional and
not read the key set above as closed. Route
names come from a closed vocabulary — `/hooks/claude-code`, `/hooks/codex`,
`/hooks/grok`, `otlp`, `other` — computed from the request path alone, so no
query string, path segment or header value ever reaches the log. Only the busy
class carries `route`/`routes`; every other rejection line is unchanged.

Residual behaviour worth knowing:

- A working directory reported **only** inside `args`/`arguments`/`tool_arguments`
  is not recovered: those keys are raw command content and are blanked before the
  write, so such an event replays without repository linkage. The event itself is
  not lost. Report the working directory at the top level (`cwd`) to keep it.
  `pnpm proof:hook-spool` measures this divergence on every run rather than
  leaving it to be discovered.
- A body carrying a live-usage protocol string under a suppressed key is refused
  live (403) but admitted on replay, because the string is inside the value the
  spool blanked. No live-usage figure is ever admitted — only the event.
- Spool counters in `status`/`doctor` can lag the ledger by the remainder of a
  5 s drain tick, as above. `spooledAtIntake` is written to the counters file
  the moment the intake spools, so `plimsoll status`/`doctor`, which read that
  file, are immediate; the collector's own `/status` serves the drain's cached
  snapshot and so lags by the same tick.
- An `http`/`curl` hook whose post never reaches a listening collector is still
  lost: there is no Plimsoll process on that path to spool it.

### What the capture-health label means

`captureHealth` in `plimsoll status --json`, the collector's `/status`, and
`doctor` enumerates **every configured source** — Claude Code, Codex and Grok —
and answers one question per source: *is what ran on this machine reaching the
ledger?*

- **green** — capture is current. Either the source's newest ledger event is
  inside its expected cadence (60 minutes) and not ahead of the clock, or a
  completed local scan agrees with what the ledger holds, and the session-count
  projection does not show a conflicting watermark.
- **amber** — capture cannot be confirmed, with the exact reason: local activity
  that outran token attribution, a local activity scan that could not finish, a
  scan receipt too old to confirm quiet, a lagging session-count projection,
  or a newest event dated in the future.
  A future-dated event never earns the freshness credit: `last_event_at` only
  ever moves forward, so a clock-skewed or future-dated producer would otherwise
  hold a dead source green for the whole skew interval. The reason carries the
  signed age (`newest event is 1500m in the future …`), as does `lastEventAgeMs`.
- **red** — local activity is demonstrably *not* reaching the ledger.
- **no_events** — the source is configured and enumerated, and has captured
  nothing yet. It is never absent and never reads as healthy, and it does not
  make the overall label amber.

The status snapshot's session count is projection evidence, not a fresh raw-ledger
count. `tokenSessionsToday` counts projected token-bearing sessions whose latest
event falls inside the current **UTC day**; a token event may belong to an earlier
day of that session. The label says "projected token-bearing sessions ending
today (UTC)" accordingly. `sessionCountProjection` publishes the UTC date,
separate ledger-session and token-session watermarks, their source-event
watermarks, signed lags, and the underlying projected counts. A recent non-token
session cannot advance the token-session watermark.

Each count uses its corresponding source clock: `lastEventAt` for all sessions,
`lastTokenEventAt` for token-bearing sessions. Today's source activity with no
matching projected session, a prior-UTC-day watermark, or a lag greater than the
**10-minute session-count budget** makes the exposed counts **null**, not zero.
The count budget is separate from the local-activity capture-lag policy. The
reason names the actual condition: missing projection, prior UTC day, or over
budget; durations retain seconds and use hours/days rather than huge minute totals.

Invalid/future timestamps, a projection ahead of its source clock, or projected
session evidence without a corresponding source timestamp also withhold counts.
Absent timestamps with no projected sessions and no current source activity do
not invent a problem: zero remains distinguishable from unknown. Underlying
projected values remain visible with their freshness states.

Each watermark is compared against the source clock that matches it. A session's
end advances on its last event of **any** kind, and sessions ordinarily sign off
with a non-token event, so the token-session watermark reads each session's own
last token event rather than its end. `latestTokenSessionAt` is therefore token
evidence: a healthy source whose newest token-bearing session ended on a
`session_stop` or a tool call reads **green** with both counts published, and a
negative token lag means the projection really does claim token evidence the
event ledger does not hold. Such a pair is withheld with its sign intact, never
relabelled as zero lag or complete linkage. A session row written before that
per-session clock existed is filled from the facts still held when the ledger is
opened, and falls back to the session's end only if those facts have aged out.

A count that is not vouched for cannot support a red assertion based on that
count. That case becomes amber and, **when the projection is lagging**, says a
linkage fault cannot yet be distinguished from projection lag or unlinked events.
An invalid, future, or projection-ahead pair raises no such question and will not
resolve by waiting, so it is reported without that sentence. This does **not** clear
the earlier red condition for recent local activity not reaching the event ledger;
that event-recency check remains first. Future-event checks also remain intact.
No additional raw-ledger query is made. The standalone ledger-side health query
retains its existing meaning. Even a `projected` count is not proof that every
event has a session link; this change does not repair the projection backlog.

### Rolling the collector back: the projection schema version

The derived dashboard projection carries a `schema_version`, and that version
moves whenever the derived tables change shape. It exists for the **downgrade**
direction: a collector that opens a projection written by a newer binary cannot
know what the extra shape means, so it **fails closed** rather than half-read
it. The projection is marked not ready with `degradedReason:
"projection_schema_newer"`, maintenance does no derived work, and the status
snapshot serves no session count at all. It never serves the last count it had.
The stored version is left exactly as the newer binary published it, so
re-installing that binary and restarting the collector is the whole recovery.

That guard can only protect a rollback to a binary that carries it. A collector
older than the guard reads no version, and one older than **#360**
(eco-6hoxj.80 r3) also predates `last_token_event_at` on
`dashboard_session_source_window` and `dashboard_session_repair_source`. Its
positional session insert-select fails to compile against the wider tables —
`table dashboard_session_source_window has 13 columns but 12 values were
supplied` — on every maintenance tick, which is what made such a rollback go on
serving a frozen, green session count. Rolling back that far is a manual
operation. With the collector **stopped**, against the ledger
(`~/.plimsoll/work-ledger.sqlite` unless `PLIMSOLL_HOME` moves it):

```sql
alter table dashboard_session_repair_source drop column last_token_event_at;
alter table dashboard_session_source_window drop column last_token_event_at;
```

The columns are additive and the old binary re-derives everything it needs, so
dropping them restores session materialization exactly. Re-upgrading adds them
back on open and refills them from the facts the ledger still holds. Rebuilding
the projection from the raw ledger is the equivalent heavier alternative.

The local activity scan is **bounded**: one cadence enumerates at most 256
directory entries within 50 ms, keeps its cursor, and resumes on the next tick.
On a host with many capture roots one sweep therefore spans many cadences, and
`activityState.truncated` stays true for all of them. That is the normal state of
a converging scan, not a capture fault, so it is reported as a `diagnostics`
entry and in `activityState.scan` — naming the roots enumerated, the entries
visited this sweep and this tick, the per-tick budget and the candidates still
pending — and it only becomes the source's `reason` when capture truth cannot
answer. A host whose events are flowing and whose session-count projection is
not lagging reads green with the sweep reported alongside it (bead eco-6hoxj.73).

`activityState.scan` is an operator field set, published for reading rather than
consumed by the label:

- `rootsTotal` / `rootsEligible` / `rootsStarted` — capture roots configured for
  the source (the count `plimsoll status` reports for roots elsewhere), how many
  of them are currently `ready` and so eligible for a sweep, and how many this
  sweep has begun. All three are capture roots: the codex sweep enumerates a day
  partition per root per day, and those are converted back before they are
  published, so `rootsStarted` can never exceed `rootsEligible` and
  `rootsEligible` can never exceed `rootsTotal`. When the two totals differ the
  reason says so explicitly
  (`4/22 eligible of 25 configured capture root(s) enumerated`).
- `entriesThisSweep` / `entriesThisTick` / `pendingFiles` — enumeration progress
  since the sweep began and in this cadence, and the candidates still awaiting
  metadata. The first two count directory *entries* stepped over, never the
  files those entries matched; `pendingFiles` is the field that counts files.
  These advance during the first-install baseline sweep too.
- `entryBudgetPerTick` / `wallBudgetMsPerTick` / `lifetimeEntryLimit` — the
  budget that ended the cadence (256 entries, 50 ms) and the entries one cursor
  may visit before it restarts instead of resuming (100000).
- `converging` — a cursor exists and will resume on the next cadence; it stays
  true for a cadence deferred before any filesystem work, which keeps its cursor,
  and for a cadence that retired a finished cursor and installed a successor on
  the same attempt — that successor is the cursor that resumes. False once the
  sweep finished, hit `limitReached`, or was retired leaving no cursor behind.
  `converging: true` beside `sweepComplete: true` is therefore a same-cadence
  restart, not a contradiction: this cadence's sweep finished and the next one
  begins from the successor, which is what "still sweeping" in the reason says.
- `sweepComplete` — this cadence's cursor finished a full sweep of every eligible
  root. A sweep normally ends by being retired the moment it finishes — drained
  or restarted — and the receipt reports the numbers that cursor held when it
  was retired, so a completed sweep is reported as complete. A cursor discarded
  rather than retired publishes no receipt at all: `close()`, a capture-inventory
  rebind and the "baseline already complete" early return each drop the cursor
  without one. A cadence with no cursor at all reports false: no cursor is no
  receipt. A cursor that hit `limitReached` also reports false — it restarts,
  it did not finish.
- `limitReached` / `deferredBeforeIo` — why the cadence ended, when it was not
  the per-tick budget.

`historyCoverage` answers a different question — has an explicit full backfill
covered this source's retained history? — and stays independent of capture
health. Grok is enumerated there with status `hook_delivered`: its history
arrives by hook, so there is no local transcript to backfill, and it never
participates in the completeness verdict.

## Delivery retry scheduling

The daemon distinguishes a completely failed upload cycle from one that already
acknowledged useful work. A later failed batch in a partly successful cycle no
longer escalates the whole host into a 10–60-minute exponential pause. The next
regular upload cadence can retry eligible work; the outbox still owns each
item's identity, attempt count, retry time and acknowledgement checks.

Completely failing remote cycles retain exponential backoff with a one-hour
ceiling. Direct SQLite contention, or a network failure while the maintenance
circuit is open, does not add an exponential pause. This is a scheduling decision,
not proof that every network failure originated on the host. A real remote HTTP
refusal is not dismissed because local maintenance is unhealthy.

A valid `Retry-After` on a transient remote refusal is a lower bound. Deferred
outbox rows persist that lower bound, so reopening the ledger does not retry them
early. A partly accepted lease can report both its acknowledged siblings and the
remaining server-directed delay. Session follow-ups to that same endpoint are
carried to a later cycle rather than sent inside the cooldown. Malformed delays
are ignored; valid server
cooldowns are not shortened to the normal cadence. A server delay is honoured
only up to a ceiling: the scheduler waits at most one hour, and the persisted
outbox floor never exceeds the configured `delivery.maxBackoffSeconds`, so a
single overlong or mistyped `Retry-After` cannot park delivery indefinitely. The
HTTP-date form is measured against the response's own `Date` header when it has
one, so a skewed local clock does not inflate the wait. Witness-only probes have
no leased event row: their extra scheduler cooldown remains process-local.

Authenticated `/status` exposes `sync.failureStreak`, `sync.nextAttemptAt`,
`sync.notBefore`, `sync.lastError` and `sync.lastCycleUploadedEvents`, and
`plimsoll status` prints the same block by asking the daemon that owns it. A
cycle that acknowledged work is not a failure: its server-directed wait is
reported through `sync.notBefore`, with `sync.lastError` null. The next
attempt is the earliest eligible cadence tick, not a promise of network traffic:
per-item retry dates, open circuits, shutdown and an in-flight cycle still apply.
The scheduling snapshot is process-local; only outbox retry dates survive a restart.
Failure logs include a timestamp and an allowlisted code such as `ETIMEDOUT` or
`ECONNRESET`, not arbitrary exception text, URLs or credentials.

`pnpm proof:sync-backoff` exercises partial cycles, genuine outages, local
pressure, real loopback HTTP refusal, persistent cooldowns, and cache-only status.
The existing delivery, outbox and storage-retry proofs remain required.

## Quickstart

Requirements: macOS, Node >=20 <25.

```bash
# wire Claude Code, Gemini CLI, Grok, and Codex telemetry (independent targets;
# idempotent, takes backups; --dry-run to preview)
npx -y @plimsoll/cli setup

# run the collector + dashboard → http://127.0.0.1:48271
npx -y @plimsoll/cli start

# inspect readiness without creating config, a ledger, or service files
npx -y @plimsoll/cli doctor --read-only --json
```

`doctor` is a diagnostic gate, not an installer and not capture proof by
itself. Its readiness progresses through `not_installed` → `configured` →
`service_ready` → `signal_verified`; only `signal_verified` returns `ok:true`
and exit 0. A cold ledger therefore fails honestly until a real token-bearing
Claude Code or Codex event reaches the collector. Background LaunchAgent mode
for npm installs is still being fitted — until then `start` runs in a terminal.

The packaged CLI also ships transactional lifecycle commands backed by an
immutable, version-pinned runtime
([docs/local-lifecycle.md](docs/local-lifecycle.md)):

```bash
# Pin the running packaged bundle as the immutable runtime and repoint the
# owned LaunchAgent manifest at it; any readiness failure restores the
# previous runtime, config, database, and manifest automatically.
npx -y @plimsoll/cli@<version> lifecycle update --operation-id <id> --artifact self

# Restart the daemon on the new immutable runtime (explicit, never automatic):
npx -y @plimsoll/cli@<version> load-launch-agent

# Preview-default uninstall of ONLY owned targets; data purge is a separate,
# exact-confirmation operation. Support bundles are sanitized and bounded.
```

These commands never invoke `launchctl`, never run from a source checkout via
`self`, and print one JSON receipt each. Release signing, npm publication,
and live-fleet rollout remain gated under
[#103](https://github.com/CryptoJym/plimsoll/issues/103).

**Contributors / running from source** (adds pnpm + git):

```bash
git clone https://github.com/CryptoJym/plimsoll.git
cd plimsoll
./install.sh --dry-run                        # preflight and exact mutation plan only
./install.sh                                  # setup + development LaunchAgent + strict gate

# Equivalent manual source commands:
pnpm install
pnpm collector setup --yes                    # idempotent; backs up changed tool configs
pnpm collector install-launch-agent --dev --repo-root "$PWD" --pnpm "$(command -v pnpm)" --load
pnpm collector doctor --read-only --json      # exits 0 only after a real token signal
pnpm report -- --repository your-org/your-repo   # after a few sessions: the economics
```

`setup` applies the tool configs for you (idempotent, takes backups,
`--dry-run` to preview); `generate-config` prints exactly what to add to
`~/.claude/settings.json`, `~/.gemini/settings.json`,
`${GROK_HOME:-~/.grok}/hooks/plimsoll.json`, and `~/.codex/config.toml` if
you'd rather paste by hand. Managed hooks/exporters point only at
`127.0.0.1:48271`. Nothing is configured behind your back.

Claude Code can also run against a separate config home
(`CLAUDE_CONFIG_DIR=~/.claude-seats/<slug>`), which fleet lanes use to keep one
seat per account. Those sessions read the seat's own `settings.json`, not
`~/.claude/settings.json`, so `setup` manages each of them as its own target,
reported as `claudeSeat[<slug>]`: it discovers every
`~/.claude-seats/*/settings.json` and merges exactly the same exporter
environment and Plimsoll hooks it merges into `~/.claude/settings.json`, with
the same additive merge, the same backups and the same second-run no-op. Your
seat's own hooks and keys are preserved byte-for-byte, a seat directory without
a `settings.json` is skipped rather than created, and a seat added later is
picked up by the next `setup` run. `plimsoll doctor --read-only --json` lists
each discovered seat under `telemetry.claudeSeats` and flags an unmanaged one
with `claude_seat_settings_unmanaged` — a coverage diagnostic that does not
change doctor's readiness verdict.

Codex has the same split: fleet lanes run with
`CODEX_HOME=~/.codex-profiles/<slug>`, so those sessions read the profile's own
`config.toml` and got neither the `[otel]` exporters nor the Plimsoll hooks. As
of 0.7.20 `setup` manages each discovered `~/.codex-profiles/*/config.toml` as
its own target, reported as `codexProfile[<slug>]`, merging exactly the content
it merges into `~/.codex/config.toml` through the same additive TOML
reconciliation: your own hooks, tables, comments and unknown keys stay
byte-identical, backups are written before any change, and the second run is a
no-op. The profile's hook commands reference the same mode-0600
`~/.codex/plimsoll.headers` file the default target references, so no profile
config carries a token in a hook command, and no per-profile header file is
created. A profile directory without a `config.toml` is skipped rather than
created, and a malformed profile config is reported and left alone — it never
fails `setup`, whose own declared targets still decide the exit code.
`plimsoll doctor --read-only --json` lists each discovered profile under
`telemetry.codexProfiles` and flags an unmanaged or unreadable one with
`codex_profile_config_unmanaged`, again without changing the readiness verdict.

The seat and conductor tooling rewrites those files whenever a seat or profile
churns, and a rewritten file silently loses the managed block. The collector
heals that itself: `plimsoll setup --reconcile` re-runs exactly the
Claude and Codex half of setup's target composition — `~/.claude/settings.json`,
`~/.codex/config.toml` and every discovered seat and profile — and applies only
where the plan says `added` or `updated`. It is strictly weaker than
`setup --yes`: it never creates a file that is not there, never mints a
credential, never rewrites a file it cannot parse, and stands down on any file
another writer changes while the plan is being computed, or that the
transactional write itself finds changed underneath it. A seat or profile
directory that exists with no config file in it yet is reported as
`skipped: absent` rather than created. A run that applied or
refused something writes `<collector home>/receipts/managed-config-reconcile-<ts>.json`
with the per-target status, plan lines and backups; a healthy home plans every
target `unchanged` and writes nothing at all. The running collector calls the
same reconcile in-process every `managedConfig.reconcile.intervalSeconds`
(default 600), and only when its own doctor readback reports at least one
drifted target, so a healthy host does zero writes; the tick yields to the event
loop between targets, so a fully churned fleet-scale host never blocks the
collector's HTTP loop for a whole reconcile. A file Plimsoll could not parse is
not retried for an hour, unless it changes on disk first — fixing it heals the
target on the next tick rather than after the hour. Losing a race with another
writer is not a refusal and arms no backoff.

The cadence keeps its own litter bounded: at most five `.plimsoll-backup-*`
files per managed file and at most twenty reconcile receipts, oldest first. It
prunes only the backups it wrote itself — each one is recorded in its state file
when it is written — and it never deletes the oldest backup a managed file has,
so the copy of a host's pre-Plimsoll bytes that `setup --yes` wrote is never a
pruning candidate. `setup --yes` keeps its existing backup policy and prunes
nothing. The count bound binds only once a backup is more than 24 hours old: a
younger one is live rollback material and is kept whatever the count says, so a
file some other writer churns every tick carries the five older backups plus one
day of young ones rather than five in total.

Set `managedConfig.reconcile.enabled` to `false` in `collector.config.json` to
turn the schedule off — the collector re-reads that flag and the interval from
the file on every tick, so the kill-switch takes effect without a restart, and a
disabled collector then performs no managed-config read or write of its own.
`plimsoll doctor --read-only --json` reports the schedule under
`managedConfig.reconcile` (`enabled`, `intervalSeconds`, `lastRunAt`,
`lastResult`, `lastApplied`, `lastRefused`, `lastAbsent`, `nextEligibleAt`).
`lastRunAt` advances on every tick that ran its readback, including one that
found nothing to do (`lastResult: "unchanged"`), so a clean cadence is
distinguishable from one that never ran. A host with no Plimsoll-local
credentials manages no targets at all and stamps `lastResult: "unavailable"`
instead, so it does not read as a healthy host. The state file is read and
written under the same cross-process mutation lock the collector config uses, so
an operator's `setup --reconcile` and a daemon tick cannot drop each other's
backoff entries or run stamp.

Telemetry `setup` manages a seat's *config*; what the collector *captures* from
is its capture-root inventory (`collector.config.json` → `captureRoots[]`),
which is minted once, at enrollment. A host that gains a native root later — a
new Claude seat's `projects/`, a new Codex profile's `sessions/`, or a
`~/.claude/projects` an older enrollment never registered — keeps emitting
spans while its transcripts and rollouts go uncaptured. `capture-roots` closes
that gap without touching enrollment:

```bash
# what native roots exist under $HOME, and which are already registered
plimsoll capture-roots discover --json     # state: registered | candidate | missing

# preview, then append one (repeat --directory to add several at once)
plimsoll capture-roots add --source claude_code --directory ~/.claude/projects \
  --machine <fleet label> --dry-run
plimsoll capture-roots add --source claude_code --directory ~/.claude/projects \
  --machine <fleet label>
```

`add` is append-only. It derives the new root's identity exactly as the
enrolled roots' identities derive, and refuses — with a reason code and no
write — a duplicate directory or id, a path that is not a physical directory,
a path outside `$HOME`, an unknown source, a new directory whose contents
cannot be enumerated unambiguously (`capture_root_scan_ambiguous`), a config
that would lose a top-level field this schema does not know
(`append_only_violation`), or a config whose existing roots do not reproduce
their own ids under the label given (`identity_derivation_mismatch`).

`capture_root_scan_ambiguous` names the entries it could not resolve — a
symlinked or non-regular `.jsonl`, an unreadable subdirectory — because the
fence would not be provable over them. `--allow-scan-errors` registers the
root anyway: the same entries are listed in the receipt under
`scanAmbiguities` and simply left out of the fence, which means they are
**not** excluded and the tailer will capture them like any other file it
finds. Use it when the ambiguous entries are files you are content to have
captured; without it the default refusal is unchanged.

A `$HOME` that is itself a symlink is refused by the repo-wide no-follow
LaunchAgent path guard (`launch_agent_manifest_invalid`, detail
`LAUNCH_AGENT_UNSAFE_HOME`) before anything is written — spell the directory
through the physical home when adding a root.

The identity is derived from the host's **fleet machine label**, which is
stored nowhere — only its digests are. It is not derived from the hostname, so
on a fleet host **`--machine <fleet label>` is the expected form**: `add`
recovers the label from the roots already configured only when a hostname
candidate happens to reproduce their ids, and otherwise refuses
`identity_machine_unresolved` (distinct from `identity_derivation_mismatch`,
which means a label *was* given and this config contradicts it).

A real run stops the collector through the same path as `unload-launch-agent`,
writes a timestamped backup, fences the new root by recording the files it
already holds as pre-existing generations — so those transcripts and rollouts
are excluded rather than replayed as today's work — writes the config
transactionally, starts the collector again and verifies it, then writes a
receipt (before/after config sha256, the roots added, the generations fenced,
restart result) under `<collector home>/receipts/`. The fence is scoped to the
new directory: the provider's baseline cutoff is **not** moved, so every root
already registered keeps capturing exactly what it was capturing. Top-level
config fields this schema does not know are carried through the write
untouched and listed in the receipt as `carriedUnknownKeys`.

Nothing is left half-applied, and the receipt never claims a recovery that
did not happen. Whatever fails from the unload onwards — including a throw
after the bootout, when the daemon is already down — `add` starts the
collector again, records the failed step and the resulting state, and exits 1.
Every failed receipt carries a `recovery`, which is one of:

| `recovery` | What it means |
|---|---|
| `config_applied_collector_restarted` | The write completed. The config names the new roots, the fence belongs to them, and the restart, where one was attempted, came back verified. On a host with no LaunchAgent installed there is no service to cycle, so no restart is attempted and the same value is emitted with `restart.skipped: true` and `reason: "launch_agent_not_installed"`: the write and the fence are what this value speaks to, and `restart.skipped` / `restart.verified` is the authority on the daemon. |
| `config_applied_collector_not_running` | The write completed and the fence belongs to the new roots, but the collector this command stopped did not come back. `restart.failedStep` names where it stopped: start the daemon again. |
| `config_unchanged_fence_rolled_back` | The config is byte-identical to the backup, and every generation row this run fenced was removed again. |
| `ledger_fence_retained` | The config was **not** written and a fence for those roots is still in the ledger: either this run could not roll its own rows back, or an earlier run's rows are still in place and are not this run's to remove (`fenceRollback.generationsRetainedFromEarlierRun`). `fenceRollback.retainedFiles` lists the files fenced under the new roots (a superset of what is still excluded) — remove their rows or re-run the add to register the root they belong to. |
| `config_unchanged_no_backup_written` | Nothing was written at all: the failure was at or before the backup step. `backupPath` is then `null`. |
| `config_unchanged_restored_state_matches_backup` | The config is byte-identical to the backup and no fence for these roots is in the ledger. |

The three `config_unchanged_*` values speak to the config and the ledger only;
`restart.skipped` / `restart.verified` is the authority on whether the daemon
came back — and on whether one was ever asked to.

`backupPath` is only ever a backup that exists on disk; `backupWritten` says
whether the backup step completed. A retry whose fence is already in place
reports `baseline.seals[].reason: "already_sealed"` with the count found and
`baseline.seededAt: null` — the fence is real, this run did not write it, and
if that retry fails at the write too the receipt still reports the fence as
`ledger_fence_retained` rather than a clean ledger. A
restart or daemon verification that does not come back also exits 1 with the
failed step named. Without an installed LaunchAgent the restart is skipped and
the receipt says so. Existing roots, the installation epoch and every other enrollment field
are never changed. `plimsoll doctor --read-only --json` reports what is still
unregistered under `captureRoots.unregisteredCandidates` — like the seat
diagnostic, it does not change doctor's readiness verdict.

The Grok and Codex hook commands carry no secret: each reads its producer
token from a mode-0600 `plimsoll.headers` file beside its own config
(`${GROK_HOME:-~/.grok}/hooks/plimsoll.headers` and `~/.codex/plimsoll.headers`),
so a config search or a pasted command string never exposes the token. Codex's
own OTLP exporter has no file or environment source for a header value, so
`[otel.*_exporter."otlp-http"] headers` keeps the token inline; rotate it with
`plimsoll rotate-producer-token --source codex`, which rewrites the header file,
`config.toml` and every discovered `~/.codex-profiles/<slug>/config.toml` that
already carries the managed block — with a backup per file, inside the same
grace window — and accepts the superseded token only until that window closes
(`--grace-seconds`, default 900). A profile without the managed block is left
untouched (`setup` owns provisioning it) and a malformed one is reported under
`profilesSkipped` and never rewritten, neither of them failing the rotation.
`--dry-run` lists the `codexProfile[<slug>].otel.<exporter>.headers updated`
lines it would write. `plimsoll doctor` reports the rotation deadline and never
prints a token.

The source install script's `--dry-run` does not clone, install dependencies,
write Claude/Gemini/Grok/Codex or Plimsoll files, register a LaunchAgent, or start a
collector. The real install fails closed if the final doctor gate is below
`signal_verified`; the JSON report names the incomplete readiness level and
each missing/conflicted requirement.

> **Codex note:** Codex records token usage on *trace spans* (`gen_ai.usage.*`), not log events. The generated config enables logs, traces, and metrics — if you disable the trace exporter, codex token attribution silently drops to zero. We learned this the hard way (see "The audit story" below).

**Zero-value span admission.** Codex's `codex-app-server` exports its whole
internal tracing tree over OTLP: on one conductor host that was 95,739 of
106,668 ingested events in a single hour, spread over 147 span names
(`realtime_conversation.running_state`, `environments.snapshot`,
`mcp.runtime.refresh_wait`, …) that carry no tokens, no cost, no tool name and
no session or actor linkage. Plimsoll discards those spans at admission,
*before* the ledger write, so neither the local buffer nor the upload queue
grows from them. An app-server span that does carry a retained dimension —
usage, an error or exception, an explicit action class, a tool name, or
analytical linkage (session, actor, project, customer, workflow, git, request
or call id) — is admitted exactly as before; a model name with no usage is not
a retained dimension, because it joins to nothing. The typed Codex events the
reports are built from (`user_prompt_submit`, `assistant_response`, `tool_use`,
`tool_result`, `session_*`, usage events) are never `otel_span` and are
untouched, and every other service and source still fails open. Each drop is
counted durably in the ledger by source and reason and is readable on the
collector's local status endpoint under `otlpAdmission.dropped`
(`reason: "app_server_internal_span"`, alongside the older
`generic_zero_value_span`).

## Local outcome performance

`backfill-outcome-timeline` remains the explicit, bounded GitHub collection
command. After each run, Plimsoll deterministically materializes its immutable
pull facts into local performance rows for the dashboard. This is not a new
feed and does not fetch in the dashboard request path. `MERGED`,
`FIRST_PASS`, and `REWORK_OBSERVED` are only emitted when the timeline proves
them; missing evidence or required-check policy coverage is the literal
`UNKNOWN`, never a fabricated failure, zero, or no-rework result.

```bash
# Re-derive an existing local outcome store without making a provider request.
pnpm backfill:outcome-performance -- --required-checks ./required-checks.json

# Create a local seven-day Markdown + JSON summary on demand (no scheduler).
pnpm weekly:performance -- --out-dir ./performance-rollups
```

The dashboard's **Outcome performance** panel uses the same materialized rows
as the weekly rollup. Its time buckets use the observed merge timestamp when
available, otherwise the observed pull-creation timestamp; rows without either
stay undated rather than being assigned an invented date.

## What gets collected — and what never does

Plimsoll's default is **metadata mode**. In metadata mode, these are *removed before the local database write* — not redacted later, never stored:

- prompts and model outputs
- tool inputs/outputs, command bodies, file contents, diffs/patches
- codex tool `arguments` (raw command lines, workdir paths)
- clipboard, screenshots, keystrokes, browser history (never collected in any mode)

These are **hashed** before storage: emails, user/account IDs, file paths, working directories, repo remotes, branch names.

These are stored plain: timestamps, event types, tool *names*, action classes, models, token counts, costs, durations, session IDs, commit shas.

Managed or upload-enabled installs are locked to the literal
`metadata_only` privacy mode. Attempts to enable raw evidence through the
environment, collector config, CLI config generation, setup, join, or start
fail before the collector/config write; there is no silent downgrade. Existing
legacy rows marked `evidence` stay local and are excluded from both ordinary
sync and `upload-history`, which report
`local_quarantine_migration_required`. Plimsoll does not scan, migrate, or
delete those rows automatically. An encrypted evidence vault is **not
implemented**.

The suppression engine is [`packages/shared/src/policy.ts`](packages/shared/src/policy.ts) and the forbidden-field list is [`packages/shared/src/schemas.ts`](packages/shared/src/schemas.ts). The signal-fidelity proof plants sentinel commands, paths, and prompts and fails if any survive to disk:

```bash
pnpm proof   # 14 checks, writes evidence to evidence/
pnpm proof:privacy-mode # managed-mode, legacy quarantine, temp-home surface proof
pnpm proof:system-e2e   # isolated two-machine, cross-stage source release gate
```

See [`docs/source-system-e2e.md`](docs/source-system-e2e.md) for the exact
source boundary and the hosted/controlled-Mac gates that remain not run.

## The audit story (why this exists in this shape)

Plimsoll's first incarnation ran for five weeks inside our own company and silently captured **0% of codex tokens and ~1% of claude tokens** — OTLP envelopes were flattened into single events, metric datapoints were never parsed, and a 7.5 GB local spool grew with almost no signal in it. The contract tests all passed the entire time; nothing asserted that a *real session* produced attributed tokens.

The rebuild produced this architecture — parse-at-ingest, per-record events, deterministic IDs, retention, watermark sync — plus the rule the project now lives by: **a capture pipeline is only as good as its signal-fidelity proof.** Re-parsing the old spool recovered $141 of attributed spend and 142k behavioral events, so the lesson cost us nothing but pride.

## Free vs. hosted — where the line is

Everything in this repository is free, Apache-2.0, and complete for an individual or a single machine — and features here never migrate to paid:

- the collector, suppression engine, local ledger, schemas
- local reports: tokens/cost per repo, per PR, per model, per session; join rates; Validated Delivery Yield
- backfill, proofs, config generation

The hosted product (separate, commercial) is the **comparative and prescriptive** layer: multi-machine team rollups, manager pattern views, finance cost-to-serve allocation, cross-company benchmarks, and the weekly Efficiency Brief. Descriptive analytics about your own data stay open; "how do we compare and what should we change" is what we sell.

## Status

| Area | State |
|---|---|
| Claude Code capture (hooks + OTLP logs/metrics) | ✅ verified on real sessions |
| Codex capture (hooks + OTLP logs/traces/metrics) | ⚠️ wired; live span-shape verification pending ([issue 0004](issues/0004-codex-live-span-verification.md)) |
| Action-class derivation, privacy suppression | ✅ proof-gated (14 checks) |
| Git linkage + PR efficiency report | ✅ verified on a real PR |
| Retention, watermark sync, backfill | ✅ |
| Linux / Windows | 🚧 [issue 0007](issues/0007-linux-windows-support.md) |
| More agents (Cursor, Gemini CLI, …) | 🚧 community lanes ([0005](issues/0005-cursor-adapter.md), [0006](issues/0006-gemini-cli-adapter.md)) |

## Roadmap

1. **v0.1 — Open release** (now): macOS collector, Claude Code + Gemini CLI + Codex, local efficiency reports, signal-fidelity CI. ([issue 0001](issues/0001-v0.1-release-readiness.md))
2. **v0.2 — Coverage**: codex live verification, per-event repo attribution for multi-repo sessions, rework-window detection for true Validated Delivery Yield, Linux support.
3. **v0.3 — Reach**: adapters for more agents (Cursor, Gemini CLI, Copilot CLI), npx one-line install, menubar app, signed standalone binary.
4. **Hosted beta** (commercial, separate repo): team rollups, benchmarks, Efficiency Brief.

The full backlog lives in [`issues/`](issues/) as operating records — each one is written so a human can scan it on a board and an agent can pick it up without rediscovering the work. See [ISSUE_GUIDE.md](ISSUE_GUIDE.md).

## Contributing

The highest-leverage contributions are **adapters** (get a new agent's telemetry into the normalizer — the source enum and action-class table are built for it) and **signal-fidelity checks** (find a way real telemetry breaks the capture path, encode it as a proof check). See [CONTRIBUTING.md](CONTRIBUTING.md).

Every PR must keep `pnpm proof` green. If your change touches capture, add a check that would have caught its absence.

## License

[Apache-2.0](LICENSE). The collector that watches your work should be software you can read.

---

*Plimsoll is built by [Utlyze](https://utlyze.com). The hosted analytics product is separate and commercial; this collector is complete, free, and will stay that way.*
