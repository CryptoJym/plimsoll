import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import {
  CAPTURE_COVERAGE_MAX_ENTRIES,
  CAPTURE_COVERAGE_MAX_WORK_PER_TURN,
  CaptureCoverageDirectoryCache,
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
const active = path.join(root, "active");
const stable = path.join(root, "stable");
const shrinking = path.join(root, "shrinking");
const ignored = path.join(root, "ignored");
const removed = path.join(root, "removed");
fs.mkdirSync(large);
fs.mkdirSync(changing);
fs.mkdirSync(active);
fs.mkdirSync(stable);
fs.mkdirSync(shrinking);
fs.mkdirSync(ignored);
fs.mkdirSync(removed);
const total = 10_050;
for (let index = 0; index < total; index += 1) {
  fs.writeFileSync(path.join(large, `session-${String(index).padStart(5, "0")}.jsonl`), "");
}
for (let index = 0; index < 40; index += 1) fs.writeFileSync(path.join(changing, `file-${index}`), "");
for (let index = 0; index < 3_000; index += 1) {
  fs.writeFileSync(path.join(active, `file-${String(index).padStart(5, "0")}`), "");
}
for (let index = 0; index < 200; index += 1) fs.writeFileSync(path.join(stable, `file-${index}`), "");
for (let index = 0; index < 3; index += 1) fs.writeFileSync(path.join(shrinking, `file-${index}`), "");
for (let index = 0; index < 30; index += 1) fs.writeFileSync(path.join(ignored, `other-${index}`), "");
for (let index = 0; index < 5; index += 1) fs.writeFileSync(path.join(ignored, `keep-${index}`), "");
for (let index = 0; index < 100; index += 1) fs.writeFileSync(path.join(removed, `file-${index}`), "");
const handles = () => fs.readdirSync("/dev/fd").length;
const beforeHandles = handles();
const originalReaddir = fs.readdirSync;
const originalOpen = fs.opendirSync;
let wholeListingCalls = 0;
let largestReadBuffer = 0;
let realReads = 0;
let realOpens = 0;
fs.readdirSync = ((directory: fs.PathLike, ...args: unknown[]) => {
  if ([large, changing, active, stable, shrinking, ignored, removed].includes(String(directory))) {
    wholeListingCalls += 1;
    throw new Error("whole directory listing is forbidden in coverage");
  }
  return (originalReaddir as (...args: unknown[]) => unknown)(directory, ...args);
}) as typeof fs.readdirSync;
fs.opendirSync = ((directory: fs.PathLike, options?: { bufferSize?: number }) => {
  if ([large, changing, active, stable, shrinking, ignored, removed].includes(String(directory))) {
    largestReadBuffer = Math.max(largestReadBuffer, options?.bufferSize ?? 32);
    realOpens += 1;
  }
  const handle = originalOpen(directory, options);
  const read = handle.readSync.bind(handle);
  handle.readSync = () => {
    if ([large, changing, active, stable, shrinking, ignored, removed].includes(String(directory))) realReads += 1;
    return read();
  };
  return handle;
}) as typeof fs.opendirSync;

const makeWalk = (directory: string, maxEntries?: number, cache?: CaptureCoverageDirectoryCache) => {
  let checked = 0;
  const paths = new Map<string, number>();
  const walk = new CaptureCoverageWalk({
    roots: [directory],
    maxEntries,
    open: (target) => openCaptureCoverageDirectory(target, (entry) =>
      ({ path: path.join(target, entry.name), kind: "file" }), cache),
    check: (file) => { checked += 1; paths.set(file, (paths.get(file) ?? 0) + 1); return null; },
    checkLink: () => null,
  });
  return { walk, checked: () => checked, paths };
};

try {
  const { walk, checked } = makeWalk(large);
  const times: number[] = [];
  const units: number[] = [];
  const actualUnits: number[] = [];
  for (let turn = 0; turn < 20 && !walk.done; turn += 1) {
    const beforeReads = realReads;
    const beforeOpens = realOpens;
    const beforeChecks = checked();
    const started = performance.now();
    units.push(walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0));
    actualUnits.push(realReads - beforeReads + realOpens - beforeOpens + checked() - beforeChecks);
    times.push(performance.now() - started);
  }
  const sorted = [...times].sort((a, b) => a - b);
  check("release_ceiling_is_literal_and_large_directory_obeys_it",
    CAPTURE_COVERAGE_MAX_WORK_PER_TURN === RELEASE_MAX_WORK_PER_TURN &&
    times.length > 2 && units.every((work) => work <= RELEASE_MAX_WORK_PER_TURN) &&
    actualUnits.every((work) => work <= RELEASE_MAX_WORK_PER_TURN) &&
    largestReadBuffer > 0 && largestReadBuffer <= 32, { units, actualUnits, largestReadBuffer });
  check("large_directory_finishes_with_each_file_checked_once_and_no_whole_listing",
    walk.done && walk.complete && checked() === total && wholeListingCalls === 0,
    { checked: checked(), total, wholeListingCalls });
  console.log(JSON.stringify({ total, turns: times.length, workPerTurn: units, actualUnits,
    wallMs: { max: Math.max(...times), p95: sorted[Math.ceil(sorted.length * .95) - 1] },
    wholeListingCalls, largestReadBuffer }));

  const mutable = makeWalk(changing);
  const restartUnits: number[] = [];
  const stepRestart = (subject: ReturnType<typeof makeWalk>, limit = RELEASE_MAX_WORK_PER_TURN) => {
    const beforeReads = realReads;
    const beforeOpens = realOpens;
    const beforeChecks = subject.checked();
    const charged = subject.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0, limit);
    const actual = realReads - beforeReads + realOpens - beforeOpens + subject.checked() - beforeChecks;
    restartUnits.push(actual);
    return { charged, actual };
  };
  const smallUnits: number[] = [];
  smallUnits.push(stepRestart(mutable, 5).actual);
  fs.writeFileSync(path.join(changing, "file-new"), "");
  for (let turn = 0; turn < 20 && !mutable.walk.done; turn += 1) {
    smallUnits.push(stepRestart(mutable, 5).actual);
  }
  check("directory_addition_between_turns_resumes_the_same_folder",
    mutable.walk.done && mutable.walk.complete && mutable.checked() === 41,
    { done: mutable.walk.done, complete: mutable.walk.complete, checked: mutable.checked() });

  const capped = makeWalk(changing, 4);
  for (let turn = 0; turn < 20 && !capped.walk.done; turn += 1) {
    capped.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0);
  }
  check("entry_ceiling_still_fails_closed",
    CAPTURE_COVERAGE_MAX_ENTRIES === 200_000 && capped.walk.done && !capped.walk.complete,
    { maxEntries: CAPTURE_COVERAGE_MAX_ENTRIES, done: capped.walk.done, complete: capped.walk.complete });

  let kept = 0;
  const ignoredWalk = new CaptureCoverageWalk({
    roots: [ignored], maxEntries: 20,
    open: (target) => openCaptureCoverageDirectory(target, (entry) =>
      entry.name.startsWith("keep-") ? { path: path.join(target, entry.name), kind: "file" } : null),
    check: () => { kept += 1; return null; },
    checkLink: () => null,
  });
  for (let turn = 0; turn < 20 && !ignoredWalk.done; turn += 1) {
    ignoredWalk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0);
  }
  check("ignored_entries_cost_work_but_not_the_classified_entry_ceiling",
    ignoredWalk.done && ignoredWalk.complete && kept === 5,
    { done: ignoredWalk.done, complete: ignoredWalk.complete, kept });

  const growing = makeWalk(active);
  let additions = 0;
  for (let turn = 0; turn < 20 && !growing.walk.done; turn += 1) {
    growing.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0);
    if (!growing.walk.done) {
      fs.writeFileSync(path.join(active, `new-${additions++}`), "");
    }
  }
  const next = makeWalk(active);
  for (let turn = 0; turn < 20 && !next.walk.done; turn += 1) {
    next.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0);
  }
  check("growing_directory_completes_and_next_check_sees_new_files",
    growing.walk.done && growing.walk.complete &&
    [...growing.paths.values()].every((count) => count === 1) &&
    next.walk.done && next.walk.complete && next.checked() === 3_000 + additions,
    { firstComplete: growing.walk.complete, firstChecked: growing.checked(),
      additions, nextComplete: next.walk.complete, nextChecked: next.checked() });

  const interrupted = makeWalk(active);
  stepRestart(interrupted, 500);
  fs.writeFileSync(path.join(active, "new-while-open"), "");
  for (let turn = 0; turn < 20 && !interrupted.walk.done; turn += 1) {
    stepRestart(interrupted);
  }
  check("busy_folder_growing_while_its_cursor_is_open_completes",
    interrupted.walk.done && interrupted.walk.complete &&
    interrupted.checked() === 3_000 + additions + 1 &&
    [...interrupted.paths.values()].every((count) => count === 1),
    { done: interrupted.walk.done, complete: interrupted.walk.complete,
      checked: interrupted.checked(), expected: 3_000 + additions + 1 });
  check("directory_restarts_charge_every_real_read_within_each_turn_cap",
    smallUnits.every((units) => units <= 5) &&
    restartUnits.every((units) => units <= RELEASE_MAX_WORK_PER_TURN),
    { smallUnits, restartUnits, ceiling: RELEASE_MAX_WORK_PER_TURN });

  const shrink = makeWalk(shrinking);
  shrink.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0, 2);
  for (let index = 0; index < 3; index += 1) fs.unlinkSync(path.join(shrinking, `file-${index}`));
  shrink.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0);
  check("removed_entry_during_restart_fails_closed",
    shrink.walk.done && !shrink.walk.complete,
    { done: shrink.walk.done, complete: shrink.walk.complete });

  const cache = new CaptureCoverageDirectoryCache();
  const cachedChecks: Array<{ complete: boolean; checked: number; reads: number }> = [];
  for (let pass = 0; pass < 3; pass += 1) {
    if (pass === 2) fs.writeFileSync(path.join(stable, "file-new"), "");
    const subject = makeWalk(stable, undefined, cache);
    const beforeReads = realReads;
    for (let turn = 0; turn < 20 && !subject.walk.done; turn += 1) {
      subject.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0);
    }
    cachedChecks.push({ complete: subject.walk.complete, checked: subject.checked(), reads: realReads - beforeReads });
  }
  check("unchanged_directory_reuses_bounded_listing_and_addition_invalidates_it",
    cachedChecks.every((pass) => pass.complete) &&
    cachedChecks[0]!.checked === 200 && cachedChecks[0]!.reads >= 200 &&
    cachedChecks[1]!.checked === 200 && cachedChecks[1]!.reads === 0 &&
    cachedChecks[2]!.checked === 201 && cachedChecks[2]!.reads >= 201,
    { cachedChecks });

  const renamed = makeWalk(removed);
  renamed.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0, 150);
  fs.renameSync(removed, `${removed}-renamed`);
  renamed.walk.step(Number.POSITIVE_INFINITY, () => undefined, () => 0);
  check("renamed_directory_after_listing_fails_closed_before_files_finish",
    renamed.walk.done && !renamed.walk.complete,
    { done: renamed.walk.done, complete: renamed.walk.complete, checked: renamed.checked() });

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
