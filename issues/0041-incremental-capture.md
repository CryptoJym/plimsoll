Status: In progress

## TL;DR
- Growing rollout and transcript JSONL files now have a crash-safe byte cursor; completed scans read only the suffix plus a 512-byte rewrite probe.
- Partial final lines remain in the source file until newline completion. No raw carry, content, or cwd path is copied into SQLite.
- This sounding remains open for serialized scheduling and dirty-only repricing/repo enrichment.

## Scope
Deliver incremental rollout/transcript file capture, then serialize scan triggers
and replace recurring history sweeps with dirty-work maintenance. This does not
change OTLP capture, upload behavior, retention, or the live collector service.

## Context
GitHub: https://github.com/CryptoJym/plimsoll/issues/77

Parent: #75. Implementation baseline: `origin/main@9fc0af4`. Shared trace:
`46be3ad1-514a-42d2-9f14-2212fdab14dc`.

Legacy `rollout_scan_state` rows contain only the last observed size. Unchanged
legacy files stay skipped; a legacy file rebuilds once, deterministically, when
it next grows, then uses the byte cursor.

## Problem / Task
An appended line must not reread and rewrite an entire growing session file.
Crashes, restarts, partial writes, truncation, and rotation must not lose or
duplicate usage. An unchanged maintenance cycle must not sweep event history.

## Evidence
Focused proof command:

`pnpm exec tsx scripts/incremental-capture-proof.ts`

The proof uses temporary session trees and SQLite only. It pins:

- partial final line: zero events and zero parse errors until completion;
- restart with unchanged input: `filesRead=0`, `bytesRead=0`, `eventsAppended=0`;
- append: exact telescoped rollout delta and suffix plus at most 512-byte probe;
- truncation: `filesReset=1` and the replacement session is ingested;
- legacy growth: `legacyRebuilds=1`, followed by incremental checkpoints;
- deterministic replay and metadata-only content/path persistence.

## Acceptance Criteria
- [x] Appending one JSONL line reads only the new suffix within a 512-byte framing probe.
- [x] A partial final line is not committed until completed; restart neither loses nor duplicates it.
- [x] Truncation/rotation is detected by size, file identity, and a bounded head fingerprint.
- [ ] Two scan triggers cannot overlap.
- [ ] An unchanged-input scan reports zero repricing/enrichment row visits.
- [x] Tailer capture parity and deterministic ids stay green inside the focused proof and `pnpm proof`.
- [x] Metadata mode persists neither raw content nor raw cwd paths in parser checkpoints.

## Operational Boundaries
- `pnpm proof` stays the parity gate. Its time-window baseline failure on
  `origin/main@9fc0af4` is tracked separately in #82 and is not changed here.
- Tests use temporary session trees and databases only; never scan or mutate
  the live ledger or real `~/.codex` / `~/.claude` trees.
- The byte checkpoint and event writes share one SQLite transaction. A crash
  before commit replays deterministic ids; a crash after commit resumes at the
  next complete line.

## Notes For Future Agents
- Keep `rollout_scan_state` as the migration surface; renaming it would strand
  old ledgers.
- `deferred_bytes` is an integer only. Persisting a partial-line carry would
  copy potentially sensitive content into the ledger.
- Remaining owner lane: add an in-flight scan guard, move repricing and repo
  enrichment to indexed dirty work, and make no-work counters visible from the
  scheduler without restoring noisy per-minute history scans.
