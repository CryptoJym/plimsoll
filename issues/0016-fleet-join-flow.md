# 0016 — Fleet: one-command workspace join for teammate Macs

## TL;DR
- Goal (G6): a teammate runs one command containing a join token → their collector installs, registers with the workspace, and starts syncing (signed, watermark-drained) to the hosted ingest.
- Server side exists (install-key bootstrap + HMAC at /api/.../ingest in the hosted product); missing: token issuance UX, collector `join <token>` command, and the hosted deploy accepting x-plimsoll-* headers.

## Scope
Collector-side: `plimsoll join <token-or-url>` sets uploadUrl/installKey/signingSecret in collector.config.json and verifies with a handshake upload. Hosted-side work tracked in the private repo.

## Context
- Sync engine already drains oldest-first with backoff when uploadUrl is set (cli.ts runSync).
- collector.config.json schema: uploadUrl, ingestKey, uploadSigningSecret, installKey.

## Acceptance Criteria
- [ ] `pnpm collector join "<url>#<key>"` → config written, handshake batch accepted, status shows syncConfigured:true with last-sync timestamp.
- [ ] Bad/expired token = clear error, config untouched.
- [ ] Fresh teammate Mac end-to-end: install.sh + join + a session → rows visible in hosted tenant. Evidence transcript attached here.
