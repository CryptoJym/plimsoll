import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import {
  CAPTURE_COVERAGE_MAX_ENTRIES,
  CAPTURE_COVERAGE_MAX_WORK_PER_TURN,
  CaptureCoverageWalk,
  openCaptureCoverageDirectory,
} from "../../packages/collector-cli/src/capture-frontier";

const RELEASE_MAX_WORK_PER_TURN = 4_096;
type Check = (name: string, passed: boolean, detail: Record<string, unknown>) => void;

/** Called by the existing r4 CI proof; the release ceiling is deliberately literal here. */
export function incrementalCoverageChecks(check: Check) {
const root = fs.mkdtempSync(path.join(os.tmpdir(), "coverage-incremental-"));
const large = path.join(root, "large");
const changing = path.join(root, "changing");
fs.mkdirSync(large);
fs.mkdirSync(changing);
const total = 10_050;
for (let index = 0; index < total; index += 1) {
  fs.writeFileSync(path.join(large, `session-${String(index).padStart(5, "0")}.jsonl`), "");
}
for (let index = 0; index < 40; index += 1) fs.writeFileSync(path.join(changing, `file-${index}`), "");
const handles = () => fs.readdirSync("/dev/fd").length;
const beforeHandles = handles();
const originalReaddir = fs.readdirSync;
const originalOpen = fs.opendirSync;
let wholeListingCalls = 0;
let largestReadBuffer = 0;
fs.readdirSync = ((directory: fs.PathLike, ...args: unknown[]) => {
  if (String(directory) === large || String(directory) === changing) {
    wholeListingCalls += 1;
    throw new Error("whole directory listing is forbidden in coverage");
  }
  return (originalReaddir as (...args: unknown[]) => unknown)(directory, ...args);
}) as typeof fs.readdirSync;
fs.opendirSync = ((directory: fs.PathLike, options?: { bufferSize?: number }) => {
  if (String(directory) === large || String(directory) === changing) {
    largestReadBuffer = Math.max(largestReadBuffer, options?.bufferSize ?? 32);
  }
  return originalOpen(directory, options);
}) as typeof fs.opendirSync;

const makeWalk = (directory: string, maxEntries?: number) => {
  let checked = 0;
  const walk = new CaptureCoverageWalk({
    roots: [directory],
    maxEntries,
    open: (target) => openCaptureCoverageDirectory(target, (entry) =>
      ({ path: path.join(target, entry.name), kind: "file" })),
    check: () => { checked += 1; return null; },
    checkLink: () => null,
  });
  return { walk, checked: () => checked };
};

try {
  const { walk, checked } = makeWalk(large);
  const times: number[] = [];
  const units: number[] = [];
  for (let turn = 0; turn < 20 && !walk.done; turn += 1) {
    const started = performance.now();
    units.push(walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0));
    times.push(performance.now() - started);
  }
  const sorted = [...times].sort((a, b) => a - b);
  check("release_ceiling_is_literal_and_large_directory_obeys_it",
    CAPTURE_COVERAGE_MAX_WORK_PER_TURN === RELEASE_MAX_WORK_PER_TURN &&
    times.length > 2 && units.every((work) => work <= RELEASE_MAX_WORK_PER_TURN) &&
    largestReadBuffer > 0 && largestReadBuffer <= 32, { units, largestReadBuffer });
  check("large_directory_finishes_with_each_file_checked_once_and_no_whole_listing",
    walk.done && walk.complete && checked() === total && wholeListingCalls === 0,
    { checked: checked(), total, wholeListingCalls });
  console.log(JSON.stringify({ total, turns: times.length, workPerTurn: units,
    wallMs: { max: Math.max(...times), p95: sorted[Math.ceil(sorted.length * .95) - 1] },
    wholeListingCalls, largestReadBuffer }));

  const mutable = makeWalk(changing);
  mutable.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0, 5);
  fs.writeFileSync(path.join(changing, "file-new"), "");
  mutable.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0, 5);
  check("directory_mutation_between_turns_fails_closed",
    mutable.walk.done && !mutable.walk.complete, { done: mutable.walk.done, complete: mutable.walk.complete });

  const capped = makeWalk(changing, 4);
  for (let turn = 0; turn < 20 && !capped.walk.done; turn += 1) {
    capped.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0);
  }
  check("entry_ceiling_still_fails_closed",
    CAPTURE_COVERAGE_MAX_ENTRIES === 200_000 && capped.walk.done && !capped.walk.complete,
    { maxEntries: CAPTURE_COVERAGE_MAX_ENTRIES, done: capped.walk.done, complete: capped.walk.complete });

  const closing = makeWalk(changing);
  closing.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0, 5);
  closing.walk.close();
  const afterHandles = handles();
  check("open_directory_handles_close_on_completion_error_and_shutdown",
    afterHandles === beforeHandles, { beforeHandles, afterHandles });
  console.log(JSON.stringify({ fileHandles: { before: beforeHandles, after: afterHandles } }));
} finally {
  fs.readdirSync = originalReaddir;
  fs.opendirSync = originalOpen;
  fs.rmSync(root, { recursive: true, force: true });
}
}
