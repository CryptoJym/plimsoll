# 0028 — Account emails (local-only) + codex identity for rollout events

Parent: 0027 (#37)

## TL;DR
- Accounts are tied to emails. Emails now live in `account_labels.email` — LOCAL-ONLY,
  same boundary as labels, proof-enforced absent from upload bodies.
- Sources: settings drawer input per identity (+ "detected on this machine" hints read
  from `~/.claude.json` oauthAccount and `~/.codex/auth.json`). No hash chain links local
  config values to telemetry-derived hashes (verified — all candidate chains miss), so
  nothing is auto-attached; humans link, tools suggest.
- Codex rollout events gain identity going forward: actorId = hashProtectedValue(
  chatgpt_account_id) — stamped ONLY for sessions that started at/after the current
  login's `last_refresh` (provably this account; history stays unattributed — we
  under-attribute, never mis-attribute). The identity's email auto-records locally.

## Acceptance (proof, 52 checks)
- [x] `local_identities_read_from_tool_configs` — emails/plan/account id from fixture configs
- [x] `codex_identity_stamped_within_honest_window` — post-window session stamped, pre-window not
- [x] `codex_identity_email_recorded_locally` — email lands in account_labels
- [x] `account_email_never_uploaded` — raw email sentinel absent from every upload body
- [x] `account_email_settings_roundtrip` — POST /api/settings/account-email + GET round-trips

## Notes For Future Agents
- chatgpt_plan_type rides the same auth claims — Lane B (0029) uses it for plan suggestions.
- The honest window shrinks when tokens refresh (last_refresh moves forward) — safe direction.
