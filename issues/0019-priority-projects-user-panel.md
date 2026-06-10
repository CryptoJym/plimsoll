# 0019 — Priority projects + user panel

## TL;DR
- A managed list of priority repo URLs (hashed on save); every session classifies priority / non-priority / unlinked. Panel: spend by user × priority bucket — "are people burning tokens on what matters?"
- Auto-import candidates: GitHub org repo list (gh api, opt-in) or the hosted workspace pushes the list to collectors (fleet path).

## Scope
Local: `priority_repos` registry (URL → hash via shared linkage normalizer), summary bucketing, user panel (depends 0018). Hosted distribution of the list = private-repo issue.

## Acceptance Criteria
- [ ] `plimsoll priority add <repo-url>` + dashboard editor; buckets shown top-level (priority / other / unlinked $).
- [ ] User panel: per-account spend split by bucket, with coverage caveats displayed.
- [ ] Unlinked spend is its own honest bucket, never folded into "other".
