import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import { CollectorMaintenance } from "../../packages/collector-cli/src/maintenance";
import type { RolloutTailer } from "../../packages/collector-cli/src/rollout-tailer";
import type { TranscriptTailer } from "../../packages/collector-cli/src/transcript-tailer";
import type { GrokUsageTailer } from "../../packages/collector-cli/src/grok-usage-tailer";
import { CaptureCoverageWalk } from "../../packages/collector-cli/src/capture-frontier";

type Check = (name: string, passed: boolean, detail: Record<string, unknown>) => void;

/** Called by the existing r5 CI proof with a blocked synchronous first source. */
export function fairnessCoverageChecks(check: Check) {
const root = fs.mkdtempSync(path.join(os.tmpdir(), "coverage-fairness-"));
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
  workspaceId: "00000000-0000-4000-8000-0000000000c1",
  delivery: { enabled: true, limits: { maxOldestAgeDays: 3650 } },
});
const order: string[] = [];
const progress = { codex: 0, claude_code: 0, grok: 0 };
type Source = keyof typeof progress;
let currentTurn = 0;
const seen = new Map<Source, number>();
const touch = (source: Source) => {
  if (seen.get(source) !== currentTurn) {
    seen.set(source, currentTurn);
    order.push(source);
    progress[source] += 1;
  }
};
const fake = (source: Source) => ({
  coverageWalk: () => new CaptureCoverageWalk({
    roots: [source],
    open: () => {
      touch(source);
      let next = 0;
      let slowedTurn = -1;
      return {
        read: () => {
          touch(source);
          if (source === "codex" && slowedTurn !== currentTurn) {
            slowedTurn = currentTurn;
            const until = performance.now() + 80;
            while (performance.now() < until) { /* synchronous slow directory read */ }
          }
          return next < 10_000 ? { path: `${source}-${next++}`, kind: "file" as const } : null;
        },
        unchanged: () => true,
        close: () => undefined,
      };
    },
    check: () => { touch(source); return null; },
    checkLink: () => null,
  }),
  close: () => undefined,
});
const maintenance = new CollectorMaintenance(buffer,
  fake("codex") as unknown as RolloutTailer,
  fake("claude_code") as unknown as TranscriptTailer,
  undefined,
  fake("grok") as unknown as GrokUsageTailer,
  { captureCoverageTurnMs: 90, captureCoverageIntervalMs: 0 });
const turns: Array<{ order: string[]; progress: typeof progress; wallMs: number }> = [];
for (let index = 0; index < 3; index += 1) {
  currentTurn = index;
  const from = order.length;
  const started = performance.now();
  (maintenance as unknown as { checkCaptureCoverage(): void }).checkCaptureCoverage();
  turns.push({ order: order.slice(from), progress: { ...progress }, wallMs: performance.now() - started });
}
check("slow_first_source_does_not_skip_other_sources_for_a_cadence",
  turns.every((turn, index) => turn.progress.codex === index + 1 &&
    turn.progress.claude_code === index + 1 && turn.progress.grok === index + 1), { turns });
check("first_source_rotates_each_coverage_turn",
  turns.map((turn) => turn.order[0]).join(",") === "codex,claude_code,grok", { turns });
const sorted = turns.map((turn) => turn.wallMs).sort((a, b) => a - b);
console.log(JSON.stringify({ turns, wallMs: { max: sorted.at(-1), p95: sorted[Math.ceil(sorted.length * .95) - 1] } }));
maintenance.close();
buffer.close();
fs.rmSync(root, { recursive: true, force: true });

const singleRoot = fs.mkdtempSync(path.join(os.tmpdir(), "coverage-single-source-"));
const singleBuffer = new LocalEventBuffer(path.join(singleRoot, "ledger.sqlite"), {
  workspaceId: "00000000-0000-4000-8000-0000000000c1",
  delivery: { enabled: true, limits: { maxOldestAgeDays: 3650 } },
});
let steps = 0;
let work = 0;
const onlyWalk = new CaptureCoverageWalk({
  roots: ["codex"],
  open: () => {
    let next = 0;
    return {
      read: () => {
        const until = performance.now() + 0.04;
        while (performance.now() < until) { /* slow active directory */ }
        return next < 20_000 ? { path: `file-${next++}`, kind: "file" as const } : null;
      },
      unchanged: () => true,
      close: () => undefined,
    };
  },
  check: () => null,
  checkLink: () => null,
});
const originalStep = onlyWalk.step.bind(onlyWalk);
onlyWalk.step = (...args) => { steps += 1; const used = originalStep(...args); work += used; return used; };
const single = new CollectorMaintenance(singleBuffer,
  { coverageWalk: () => onlyWalk, close: () => undefined } as unknown as RolloutTailer,
  { coverageWalk: () => CaptureCoverageWalk.empty(), close: () => undefined } as unknown as TranscriptTailer,
  undefined,
  { coverageWalk: () => CaptureCoverageWalk.empty(), close: () => undefined } as unknown as GrokUsageTailer,
  { captureCoverageTurnMs: 250, captureCoverageIntervalMs: 0 });
(single as unknown as { checkCaptureCoverage(): void }).checkCaptureCoverage();
check("unused_source_shares_are_returned_without_spending_cap_twice",
  steps >= 2 && work <= 4_096, { steps, work, done: onlyWalk.done });
single.close();
singleBuffer.close();
fs.rmSync(singleRoot, { recursive: true, force: true });
}
