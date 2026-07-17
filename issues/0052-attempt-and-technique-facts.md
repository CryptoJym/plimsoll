# 0052 — Facts: tool attempts, failures/retries, work episodes, and technique exposure

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/100

## TL;DR
- Activate the dormant typed facts needed to measure redo and strategy: paired tool attempts/results, bounded errors, work episodes, and explicit skill/technique version exposure.
- Do not infer techniques from prompts, commit messages, or action mix.
- Preserve low-cardinality, privacy-safe, source-authenticated dimensions suitable for incremental projections.

## Scope
Shared schemas, local promoted columns/facts, capture adapters, and proof. Comparative analysis is out of scope.

## Acceptance Criteria
- [ ] Attempt fact includes deterministic operation ID, source, session/episode, tool class/name, start/end or duration, result status, bounded error category, and optional retry-of ID.
- [ ] Explicit exposure fact includes technique/skill ID, version or content digest, assignment/intervention ID, work/task class, complexity band, exposure time, and control/treatment mode.
- [ ] A fail/fail/pass fixture yields three attempts, two failed attempts, and explicit retry relationships without double-counting tool-use/tool-result events.
- [ ] Unknown/missing result is separate from success; producer-claimed favorable metadata cannot become verified outcome truth.
- [ ] Technique absence is never inferred from tool mix; retrospective exposure after outcome time is rejected.
- [ ] Promoted facts are bounded, indexed, cardinality-limited, and included in resource/privacy proofs.
- [ ] Raw error text, stack, prompt, command, arguments, file content/path, secret, and PII sentinels never persist or upload.

## Operational Boundaries
- Company rollout remains metadata-only.
- Technique facts describe exposure; they do not assert effectiveness.
