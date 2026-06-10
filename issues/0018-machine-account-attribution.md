# 0018 — Attribution: machine + account labels (local registry)

## TL;DR
- Teams swap between accounts (Max vs API, multiple ChatGPT plans) and machines; spend must answer "who/where".
- Machine: collector stamps os.hostname() per event (promoted column). Account: telemetry carries hashed user ids (user.id, user.account_uuid, codex user.account_id) — add a LOCAL labels registry (hash → "James · Max", auto-seeded `$USER@host`), same pattern as repo_labels: displayed locally, NEVER uploaded raw; hosted tenants map hashes their own way.
- IP addresses rejected: low-signal on DHCP/VPN; hostname + install key identify a device better.

## Acceptance Criteria
- [ ] Events carry machine column; dashboard sessions/filters show machine + account label.
- [ ] Labels registry editable via CLI (`plimsoll label account <hash> "<name>"`) and dashboard.
- [ ] Proof: account label visible locally, absent from upload bodies (mirror repo_label_never_uploaded).

## Operational Boundaries
- Hash-and-label-locally is the pattern; raw emails/usernames never persist or upload.
