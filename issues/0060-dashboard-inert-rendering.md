# 0060 — Dashboard security: inert rendering and strict browser headers

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/109

## TL;DR
- Remove stored-XSS paths from local labels and analytical values by using DOM/text rendering rather than HTML interpolation or inline handlers.
- Add strict CSP, frame denial, MIME protection, and no-CORS defaults.
- Bounded slice of 0056 / #104.

## Scope
`dashboard.html`, dashboard response headers, label validation, and browser/static adversarial proof.

## Acceptance Criteria
- [ ] Account/email/repo/model/tool/error/analytical values render with `textContent` or equivalent safe DOM APIs; no untrusted `innerHTML`, attribute-string interpolation, or inline event handler.
- [ ] CSP disallows inline/eval and external network by default, includes `frame-ancestors 'none'`; responses include `X-Content-Type-Options: nosniff` and no permissive CORS.
- [ ] Malicious HTML/SVG/script/event-handler/URL/Unicode labels remain inert in desktop and mobile dashboard flows.
- [ ] Existing dashboard behavior, accessibility, drilldowns, settings edits, and constant-work snapshot reads remain green.
- [ ] Static proof fails on forbidden sinks and browser E2E records zero console/page errors and zero network exfiltration attempts.

## Operational Boundaries
- No live dashboard/collector activation; fixtures only.
