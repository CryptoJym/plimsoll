// Race-safe DOM readiness waiting for the dashboard browser-security proof.
//
// Issue #146: the previous inline waitForText evaluated
// `document.body.textContent.includes(marker)` immediately after Page.navigate.
// While a navigation is in flight `document.body` can be null (transient
// TypeError) and the pre-commit document still shows the prior page's content
// (stale-marker acceptance). This module makes both cases deterministic:
//
//  1. A missing node is "missing" — never dereferenced, always pending until
//     the explicit bounded deadline.
//  2. Timeout receipts distinguish: node never mounted, node mounted with the
//     wrong text, marker observed but the navigation generation had not yet
//     committed (stale), while page/browser failures and overall-watchdog
//     cancellation are thrown as their own errors. Receipts carry only
//     booleans/counters/stage names — never page content.
//  3. Readiness is gated on the current navigation generation so stale nodes
//     from a prior page cannot satisfy a later phase.
//  4. All timing is injectable so fixtures run deterministically with fake
//     clocks; no retry loop or CI rerun is part of any acceptance claim.

export type DashboardReadinessStatus = "missing" | "mounted" | "ready";

export type DomWaitTimeoutDetail =
  | "node_never_mounted"
  | "node_mounted_text_mismatch"
  | "generation_not_committed";

export type DomWaitReceipt =
  | { ok: true; elapsedMs: number; polls: number }
  | {
      ok: false;
      stage: "dashboard_readiness";
      detail: DomWaitTimeoutDetail;
      sawNode: boolean;
      markerObserved: boolean;
      elapsedMs: number;
      polls: number;
    };

export interface DomWaitEnvironment {
  /** Returns current readiness. Missing nodes must map to "missing", never throw for absence. Throwing models a real page/browser failure and propagates. */
  probe(): Promise<DashboardReadinessStatus>;
  /** Settles its timer when the signal aborts or the wait finishes. */
  delay(ms: number): Promise<void>;
  now(): number;
  signal: AbortSignal;
  /** True only once the current navigation generation has committed. */
  generationCommitted(): boolean;
  /** Error thrown when the overall watchdog aborts mid-wait. */
  abortError(): Error;
}

export type DomWaitOptions = {
  timeoutMs: number;
  pollMs: number;
};

const RECEIPT_KEYS = new Set([
  "ok",
  "elapsedMs",
  "polls",
  "stage",
  "detail",
  "sawNode",
  "markerObserved",
]);

/** Receipts are content-free: only whitelisted scalar fields, never page text. */
export function receiptIsContentFree(receipt: DomWaitReceipt): boolean {
  return Object.keys(receipt).every((key) => RECEIPT_KEYS.has(key));
}

/**
 * Expression evaluated inside the page. Optional-chained/null-guarded: a body
 * that has not mounted yields "missing" instead of throwing a TypeError.
 */
export function domTextReadinessExpression(marker: string): string {
  return (
    `(function(){var b=document.body;if(!b||typeof b.textContent!=="string")return"missing";` +
    `return b.textContent.indexOf(${JSON.stringify(marker)})!==-1?"ready":"mounted";})()`
  );
}

/**
 * Tracks which (frameId, loaderId) generations have actually committed, as
 * reported by CDP Page.frameNavigated events. A phase arms the gate with the
 * loaderId returned by its own Page.navigate call; observations made before
 * that generation commits belong to the stale prior page and cannot satisfy it.
 */
export class LoaderGenerationGate {
  private readonly committed = new Map<string, string>();
  private expected?: { frameId: string; loaderId: string };

  observe(frameId: string, loaderId: string) {
    if (!frameId || !loaderId) return;
    this.committed.set(frameId, loaderId);
  }

  arm(frameId: string, loaderId: string) {
    if (!frameId || !loaderId) throw new Error("navigation_generation_unavailable");
    this.expected = { frameId, loaderId };
  }

  get armed() {
    return this.expected !== undefined;
  }

  isGenerationCommitted(): boolean {
    if (!this.expected) return false;
    return this.committed.get(this.expected.frameId) === this.expected.loaderId;
  }
}

/**
 * Polls until the probe reports "ready" AND the armed navigation generation
 * has committed, bounded by timeoutMs.
 *
 * Outcomes:
 *  - resolves with { ok: true } once ready in the current generation;
 *  - resolves with { ok: false, detail } on deadline expiry (see DomWaitTimeoutDetail);
 *  - rejects with env.abortError() if the overall watchdog aborts;
 *  - rejects with whatever the probe threw (page/browser failure).
 *
 * No silent retries: every poll outcome is counted into the final receipt.
 */
export async function waitForDomText(
  env: DomWaitEnvironment,
  options: DomWaitOptions,
): Promise<DomWaitReceipt> {
  const startedAt = env.now();
  let polls = 0;
  let sawNode = false;
  let markerObserved = false;
  while (env.now() - startedAt < options.timeoutMs) {
    if (env.signal.aborted) throw env.abortError();
    const status = await env.probe();
    polls += 1;
    if (status !== "missing") sawNode = true;
    if (status === "ready") markerObserved = true;
    if (status === "ready" && env.generationCommitted()) {
      return { ok: true, elapsedMs: env.now() - startedAt, polls };
    }
    await env.delay(options.pollMs);
  }
  if (env.signal.aborted) throw env.abortError();
  const detail: DomWaitTimeoutDetail =
    markerObserved && !env.generationCommitted()
      ? "generation_not_committed"
      : !sawNode
        ? "node_never_mounted"
        : "node_mounted_text_mismatch";
  return {
    ok: false,
    stage: "dashboard_readiness",
    detail,
    sawNode,
    markerObserved,
    elapsedMs: env.now() - startedAt,
    polls,
  };
}
