# 0026 — Dashboard hardening: drill-down freeze + accessibility pass

## TL;DR
- Owner-reported: clicking a repository row "takes a bit to load, then stops loading."
  Two real defects: (1) the rollout scan ran synchronously on the collector's event
  loop — during the boot backfill every dashboard fetch froze for minutes; (2) the
  detail drawers failed silently (`if(!r.ok)return`, unhandled json errors, no loading
  state), so any hiccup looked like a dead click.
- Plus a WCAG pass: the instrument-panel aesthetic was carrying ~2:1 contrast on its
  label ink, mouse-only rows and drawers, and no dialog semantics.

## What shipped
- RolloutTailer.scan() is async and yields the event loop between files; dashboard
  stays responsive during scans (cli + proof await it).
- Drawers: immediate "sounding…" state on open, explicit error rendering, shared
  open/close manager — `role="dialog" aria-modal`, Escape closes, focus moves to the
  close button on open and returns to the invoker on close, Tab cycles inside.
- Keyboard: repo/session rows are tabbable (`tabindex=0`, Enter/Space activate, visible
  focus state); the accounts relabel affordance is a real button with an aria-label.
- Contrast tokens (kept the steel/brass ramp, all text ≥4.5:1 on plates):
  foam-dim #73848f→#8b9aa4 · foam-faint #3e4e5a→#73848f · brass-dim #8a6c2f→#a07f3a.
- Semantics: h1 wordmark, `th scope="col"`, per-table aria-labels, lamp text is a
  `role="status"` live region updated only on real change, daily-spend SVG gets a
  computed aria-label, gauge marked decorative (legend carries the data), settings
  inputs/selects all named, `prefers-reduced-motion` disables animation.

## Acceptance
- [x] `pnpm proof` 47/47 with awaited scans; tsc clean.
- [x] Live: repo drawer shows loading then content; API errors render in-drawer.
- [x] Keyboard walkthrough: Tab reaches rows/buttons with visible ring, Enter opens
      receipts, Escape closes, focus restored (screenshots on PR).

## Notes For Future Agents
- The collector is single-threaded: any new bulk work (imports, repricing sweeps)
  must yield or run before listen — never block the loop while serving.
- Title-attribute tooltips are supplementary only; anything load-bearing needs text.
