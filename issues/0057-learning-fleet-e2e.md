# 0057 — Release gate: bounded learning job and two-member/two-machine E2E

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/105

## TL;DR
- Prove the integrated system, not just schemas: two clean Macs, two members, unique devices, truthful allocation/outcomes, bounded offline learning, revocation, upgrade rollback, and privacy.
- Learning analysis runs weekly or when its source fingerprint changes; it is never a continuous LLM background loop.
- Receipts distinguish source-complete, package-published, enrolled, synced, analyzed, skill-candidate, and owner-approved/published.

## Scope
Cross-repo E2E fixtures and controlled operator evidence. Live rollout steps remain separately authorized.

## Acceptance Criteria
- [ ] Synthetic integrated E2E covers install → config → start → join → token capture → project allocation → PR/check/review timeline → metrics → candidate evidence packet.
- [ ] Two members/two machines remain distinct; one actor may map across own devices only through workspace-scoped identity; no credentials are copied.
- [ ] Offline capture remains bounded and reconnect drains exactly once; poisoned rows do not starve valid work.
- [ ] Revocation blocks the next upload; rotation restores only the intended device; reinstall cannot resurrect revoked credentials.
- [ ] Upgrade N→N+1 preserves receipts; injected failure rolls back runtime/config/database and resumes capture on N.
- [ ] One 100-token multi-project/multi-PR fixture allocates at most 100 tokens; failed/unpriced/unmapped/rework effort remains visible.
- [ ] Technique association fixture handles confounding, Simpson's paradox, small sample, model-version change, and quality non-inferiority; none can auto-publish a skill.
- [ ] Candidate packet contains no raw/privacy sentinel and global skill/memory directories remain untouched.
- [ ] Warm dashboard requests scan no raw ledger/filesystem; weekly job has fixed rows/time/RSS/CPU/I/O budgets and no-op fingerprint skips all analysis work.
- [ ] Controlled real-Mac proof records exact package/version/checksum, device IDs, health, first real token-bearing session, sync, revoke, and uninstall states without exposing secrets.

## Operational Boundaries
- Synthetic/source E2E may ship without activating the live collector.
- Real two-machine enrollment, hosted writes, package publish, and production rollout require explicit owner authorization and serial proof.
