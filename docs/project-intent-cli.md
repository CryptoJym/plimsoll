# Choose a project when starting a CLI session

`plimsoll launch claude --project KEY` and `plimsoll launch codex --project KEY`
start in the current folder, including home or a folder without Git. A terminal
picker lists registered projects when the flag is absent; `0` chooses **Needs a
project**. Noninteractive launches without a flag also choose that explicit null.
The helper saves a pending receipt before spawning and preserves the provider's
exit code. Provider arguments follow `--`. No prompt, title, repo or payer chooses
the served project. The observed repository, when provided to the producer,
stays separate.

Run `plimsoll intent choices` for signed install-authenticated project keys and
registry revisions. `--offline` permits a previously authenticated registry copy
for 24 hours, scoped to tenant, install, upload audience and local ledger epoch.
It applies only to network/deadline failures; auth/refusal/invalid responses never
use the copy. Credentials come from the existing private joined collector config,
including its registered `cloudDeviceId`; no credential flags or environment
project stamps are accepted. `choices` must succeed before a key can be declared.

A trusted folder suggestion is explicit:

```sh
plimsoll intent folder-default --project KEY
plimsoll launch claude --use-folder-default
plimsoll intent folder-default --clear
```

It applies only in that exact real folder with the same install/registry revision,
is displayed as **Default (unconfirmed)** and uses `trusted_folder_default`.
Choosing a project number instead declares `hand_start`. Exit appends a null
cutover for the suggestion, leaving the old receipt immutable. Its environment
stamp is never kept in the parent or carried into another launch; there is no
global last project. Folder trust remains a local preference for that folder.

## Shared producer for routed launchers

```sh
plimsoll intent declare --source codex --source-root /absolute/provider/state \
  --project KEY --basis routed_launch --launch-id UUID \
  --work-authority beads --work-namespace eco-6hoxj --work-item eco-6hoxj.165.123
plimsoll intent bind --launch-id UUID --native-session ACTUAL_NATIVE_ID --queue-only
plimsoll intent sync
```

`declare` requires `--source`, `--source-root`, and exactly one of `--project KEY`
or `--needs-project`. Optional arguments: `--basis` (`routed_launch`, `hand_start`,
`trusted_folder_default`, `repo_observation`), `--launch-id UUID` (otherwise random),
`--observed-repo-key HASH`, `--principal OPAQUE_STABLE_PROVIDER_ID`, all three
`--work-authority/--work-namespace/--work-item` arguments, `--offline`, and
`--native-session ID [--continuation] [--queue-only]`. Realm follows source;
unknown account/work identity stays null. `repo_observation` requires a null
project. Source choices include `codex`, `claude_code`, `grok` and `gemini_cli`;
the P02 cloud refuses Gemini with 422, retained for review.

`bind` requires the saved launch ID and actual native ID. It accepts
`--continuation`, `--source-root DIR`, `--principal OPAQUE_ID` and `--queue-only`.
Only pass `--continuation` for a proved resume with its durable local binding.
Root/account rotation creates a child attempt while keeping epoch/root attempt.
A repeated binding to one launch is idempotent. Clear with a new native ID starts
another incarnation; old intervals remain unchanged. A known reused native ID
without continuation stays pending for a fresh existing collector join. This
adapter cannot invent an alternate session UUID: it uses the collector's existing
`ensureUuidSessionId` mapping. Lost binding or unavailable/replaced ledger state
is a visible gap. Missing source state also stays pending.

The producer persists all canonical v1 facts/evidence, derives the six P02 digest
domains, and signs the same upload audience through the existing bounded HTTP
transport. Per-session leases serialize sends across processes. HTTP 202 is
required with the exact matching ACK; it is atomically saved with delivered state
and a monotonic revision before the receipt ceases to be pending. Local receipt
history/evidence is retained. Unknown ACKs and known projects with company-mapping
gaps retain their review reasons.

Timeouts replay identical receipt IDs/facts. A matching stale 409 advances only
the separately persisted `expectedRevision`; three stale recoveries exhaust the
durable retry budget and remain for review. Other permanent refusals do not loop
on upload cycles. Registry stale refetches choices but retains refused evidence;
a new explicit declaration can supersede that refused queue item with a new ID
and evidence reference. A refused, never-admitted proposal cannot anchor the new
attempt: renewal uses the last admissible accepted or ambiguous receipt. With no
such prefix, the renewed receipt has a fresh root/attempt and null parent, even
after root/account rotation. Native identity, session epoch and old receipt facts
remain unchanged. Only a new declaration renews authority; rebinding the refused
launch cannot do so. Ambiguous delivery is never superseded. Superseded refusals
remain in review and the separate `retainedRefusals` count; `queued` counts active
pending receipts. Existing invalid lineage remains pending with
`intent_lineage_unproved`, rather than rewriting earlier facts. Revocation invalidates
cached authority and stops sending. No refusal switches project or adapter.

`intent sync` examines up to eight session documents, round-robin, with at most
three requests per session. It reports partial coverage when documents remain
unexamined or a delivery lease is busy. The daemon and normal `upload` drain one
bounded request after the session path; remote session-row absence remains queued.
`intent status [--launch-id UUID]` displays pending/bound launches, project state,
queue/review counts and observed revisions; its local inventory has a 128-row
limit and reports partial coverage/unexamined rows. Local state lives under private `project-intents/`, bounded to 1 MiB per
JSON document and 4096 documents per kind. Both purge paths erase it.

Exit codes for `intent`: **0** choices/saved default/status or fully acknowledged
send, **2** invalid input/refusal/authority/binding error, **3** saved pending
native/root/ledger binding, **4** safely queued/offline/review or partial replay.
`--queue-only` binds and persists but makes no network send and returns 4.
`declare` prints its `launchId` before an optional binding/send result. It is a
creation operation: repeating its launch ID is refused; retry `bind` or `sync`,
not `declare`. `launch` returns the child's exit status once spawning starts.

## Claude command SessionStart

The helper adds a per-invocation `--settings` command hook for
`startup|resume|clear|compact`. It consumes only native session ID/start reason,
never exports its raw input, and performs local queueing only. Before spawning,
it pins a native UUID and supplies Claude's `--session-id`, or pins an explicit
`--session-id` / UUID `--resume` target. Initial hooks must match that exact native
ID and launched provider PID/start fingerprint through command shells. Every
unrelated startup, including one beneath an active parent with its inherited
launch ID, prints Needs a project. Repeated startup, resume and compact reuse
the binding. Once the matching session has been seen, a clear from that exact
provider process may bind its next native ID as a new incarnation. Unknown
interactive resume/continue/fork targets stay pending for explicit `intent bind`;
a hook cannot claim whichever ID arrives first. No MCP hook, user/seat settings
installation or exporter env-file change is required.

Reviewed rollout snippet (replace the script path with an absolute installed
path; **this implementation lane does not install it**):

```json
{"hooks":{"SessionStart":[{"matcher":"startup|resume|clear|compact","hooks":[{"type":"command","command":"/absolute/path/claude-project-intent-session-start.sh","timeout":10}]}]}}
```

The shipped script invokes `plimsoll intent hook --source claude_code`. Outside an
owned helper launch it prints Needs a project, without borrowing a parent choice.
[Claude's command hook reference](https://code.claude.com/docs/en/hooks) specifies
stdin JSON and SessionStart lifecycle events. The installed Claude 2.1.280 help
was read in an isolated home; provider execution here uses synthetic binaries.

## Codex version and binding coverage

Installed PATH Codex is **0.153.2**. Its isolated `--help` exposes per-invocation
config and hook trust, and `features list` reports hooks stable/enabled. The inline
SessionStart config was accepted by that command. `--strict-config` is unavailable
for `features`; that failed capability probe is retained in lane evidence.
[OpenAI's current Hooks documentation](https://learn.chatgpt.com/docs/hooks)
describes command SessionStart and review of non-managed hooks. Config/feature
acceptance is verified; actual provider hook firing is not verified here.

Codex launches save the same pending receipt and leave existing hooks/config
intact. Pin the actual native ID through the producer's `intent bind` first; the
helper does not know that ID before launch. A hook with an inherited launch ID
cannot supply the first arbitrary native ID. After the explicit bind, this reviewed
snippet can maintain the exact active provider binding during the release owner's
later rollout:

```toml
[[hooks.SessionStart]]
matcher = "startup|resume|clear|compact"
[[hooks.SessionStart.hooks]]
type = "command"
command = "/absolute/path/codex-project-intent-session-start.sh"
timeout = 10
```

Non-managed hooks require Codex review/trust. The helper never bypasses trust,
uses legacy profiles, relies on repo-local OTel settings, or assumes an env file
reconfigures a parent exporter. There is no claim of automatic initial Codex
native binding. Actual Claude/Codex hook process ancestry and live firing remain
unverified by these synthetic provider fixtures; unsupported ancestry fails
closed as Needs a project. Codex desktop coverage and live rollout belong to
P08/P15. The [collector lint profile](collector-lint.md) documents the reproducible
changed-file check, including the transport's `_label` argument.
