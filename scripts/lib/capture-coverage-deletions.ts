import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import { captureFrontier } from "../../packages/collector-cli/src/capture-frontier";
import { rootCursorKey } from "../../packages/collector-cli/src/capture-root-inventory";
import { DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey } from "../../packages/collector-cli/src/jsonl-byte-tailer";
import { CollectorMaintenance } from "../../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../../packages/collector-cli/src/transcript-tailer";

type Check = (name: string, passed: boolean, detail: Record<string, unknown>) => void;
const TENANT = "00000000-0000-4000-8000-0000000000c1";
const DAY = 86_400_000;
const name = (i: number) => `rollout-2026-09-01T00-00-00-${String(i).padStart(12, "0")}.jsonl`;
const pause = () => new Promise((resolve) => setTimeout(resolve, 2));

function world(label: string, beforeLstat?: (file: string) => void) {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), `coverage-${label}-`)));
  const buffer = new LocalEventBuffer(path.join(base, "ledger.sqlite"), {
    workspaceId: TENANT, delivery: { enabled: true, limits: { maxOldestAgeDays: 3650 } },
    enrollmentNow: () => new Date(Date.now() - 10 * DAY),
  });
  const epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
  const codex = path.join(base, "codex");
  const claude = path.join(base, "claude");
  fs.mkdirSync(codex, { recursive: true });
  fs.mkdirSync(claude, { recursive: true });
  const day = path.join(codex, "2026", "09", "25");
  const listed = new Set<string>();
  const checked = new Set<string>();
  const originalOpen = fs.opendirSync;
  fs.opendirSync = ((directory: fs.PathLike, options?: fs.OpenDirOptions) => {
    const dir = originalOpen(directory, options);
    if (String(directory) === day) {
      const read = dir.readSync.bind(dir);
      (dir as { readSync: () => fs.Dirent | null }).readSync = () => {
        const entry = read();
        if (entry) listed.add(path.join(day, entry.name));
        return entry;
      };
    }
    return dir;
  }) as typeof fs.opendirSync;
  const io = {
    ...DEFAULT_JSONL_TAILER_IO,
    lstat: (file: string) => {
      if (file.endsWith(".jsonl")) checked.add(file);
      beforeLstat?.(file);
      return fs.lstatSync(file);
    },
  };
  const codexRoot = { source: "codex" as const, rootId: "codex-0", profileId: "profile-codex-0",
    directory: codex, installationEpochId: epoch };
  const rollout = new RolloutTailer(buffer, undefined, () => [], io, [codexRoot]);
  const transcript = new TranscriptTailer(buffer, undefined, io, [
    { source: "claude_code", rootId: "claude-0", profileId: "profile-claude-0", directory: claude, installationEpochId: epoch },
  ]);
  const maintenance = new CollectorMaintenance(buffer, rollout, transcript, undefined, undefined,
    { captureCoverageIntervalMs: 0 });
  const turn = () => (maintenance as unknown as { checkCaptureCoverage(): void }).checkCaptureCoverage();
  const state = () => {
    const table = buffer.database.prepare(
      `select 1 from sqlite_master where type = 'table' and name = 'capture_coverage_state'`,
    ).get();
    return table ? buffer.database.prepare(
      `select checked_at as checkedAt, complete_through as completeThrough
       from capture_coverage_state where source = 'codex'`,
    ).get() as { checkedAt: string; completeThrough: string | null } | undefined : undefined;
  };
  const losses = () => (buffer.database.prepare(
    `select count(*) as total from capture_uncovered_files where source = 'codex'`,
  ).get() as { total: number }).total;
  const hasLoss = (key: string) => Boolean(buffer.database.prepare(
    `select 1 from capture_uncovered_files where source = 'codex' and file_key = ?`,
  ).get(key));
  return {
    base, buffer, codex, day, listed, checked, maintenance, turn, state, losses, hasLoss,
    fileKey: (file: string) => jsonlScanStateKey(rootCursorKey([codexRoot], file)),
    close: () => {
      maintenance.close();
      buffer.close();
      fs.opendirSync = originalOpen;
      fs.rmSync(base, { recursive: true, force: true });
    },
  };
}

/** A listed file whose saved cursor reached its end contributes no loss. */
async function finishedCaptureRemoved(check: Check) {
  let victim = "";
  let removed = false;
  const w = world("finished-file", (file) => {
    if (file === victim && !removed) { fs.unlinkSync(file); removed = true; }
  });
  try {
    victim = seed(w.day, 1, 900_000)[0]!;
    w.buffer.database.prepare(
      `insert into rollout_scan_state
         (file, size, scanned_at, committed_offset, deferred_bytes, work_remaining)
       values (?, ?, ?, ?, 0, 0)`,
    ).run(w.fileKey(victim), 3, new Date().toISOString(), 3);
    for (let turn = 0; turn < 5 && !w.state(); turn += 1) { w.turn(); await pause(); }
    const state = w.state() ?? null;
    check("listed_file_removed_after_finished_capture_does_not_make_a_gap",
      removed && state?.completeThrough !== null && !w.hasLoss(w.fileKey(victim)) && w.losses() === 0,
      { removed, state, victimLoss: w.hasLoss(w.fileKey(victim)), losses: w.losses() });
  } finally { w.close(); }
}

function seed(day: string, count: number, offset: number) {
  fs.mkdirSync(day, { recursive: true });
  const files: string[] = [];
  for (let i = 0; i < count; i += 1) {
    const file = path.join(day, name(offset + i));
    fs.writeFileSync(file, "{}\n");
    files.push(file);
  }
  return files;
}

/** Reviewer r3's unlisted start-file deletion, through the persisted frontier. */
async function unlistedStartFile(check: Check) {
  const w = world("unlisted-start");
  try {
    const old = seed(w.day, 10_000, 900_000);
    w.turn();
    const before = w.state() ?? null;
    const listedAtMutation = w.listed.size;
    const victim = old.find((file) => !w.listed.has(file));
    if (!victim) throw new Error("no unlisted start file after the first capped turn");
    fs.unlinkSync(victim);
    let turns = 0;
    while (!w.state() && turns < 30) { w.turn(); turns += 1; await pause(); }
    const after = w.state() ?? null;
    const gaps = captureFrontier(w.buffer.database)?.gaps ?? [];
    const lost = w.losses();
    const uncertainty = w.hasLoss(jsonlScanStateKey(`codex\0${w.day}\0coverage-uncertain`));
    check("unlisted_start_file_cannot_advance_a_gap_free_frontier",
      before === null && !w.checked.has(victim) &&
        (after === null || (after.completeThrough !== null && uncertainty && gaps.length > 0)),
      { before, after, victimChecked: w.checked.has(victim), listedAtMutation,
        turns, losses: lost, uncertainty, gaps: gaps.length });
  } finally { w.close(); }
}

/** Reviewer r3's sustained listed deletions, with one new session per turn. */
async function sustainedDeletion(check: Check) {
  const w = world("deletion-growth");
  try {
    for (let d = 1; d <= 20; d += 1) {
      seed(path.join(w.codex, "2026", "09", String(d).padStart(2, "0")), 200, d * 1_000);
    }
    const old = seed(w.day, 10_000, 900_000);
    let completed = 0;
    let last: string | null = null;
    let targeted = 0;
    let firstTargeted: string | null = null;
    const walls: number[] = [];
    for (let turn = 0; turn < 120; turn += 1) {
      if (!(w.maintenance as unknown as { coverageWalks: unknown }).coverageWalks) {
        w.listed.clear();
        w.checked.clear();
      }
      const started = performance.now();
      w.turn();
      walls.push(performance.now() - started);
      const now = w.state()?.checkedAt ?? null;
      if (now && now !== last) { completed += 1; last = now; }
      fs.writeFileSync(path.join(w.day, name(990_000 + turn)), "{}\n");
      const candidate = old.find((file) => w.listed.has(file) && !w.checked.has(file) && fs.existsSync(file));
      const victim = candidate ?? old.find((file) => fs.existsSync(file));
      if (victim) {
        fs.unlinkSync(victim);
        if (candidate) { targeted += 1; firstTargeted ??= candidate; }
      }
      await pause();
    }
    const state = w.state() ?? null;
    const gaps = captureFrontier(w.buffer.database)?.gaps ?? [];
    const losses = w.losses();
    const targetedLoss = firstTargeted !== null && w.hasLoss(w.fileKey(firstTargeted));
    const sorted = [...walls].sort((a, b) => a - b);
    check("growing_folder_with_sustained_listed_deletions_advances_with_gaps",
      completed > 0 && targeted > 0 && state?.completeThrough !== null && targetedLoss && gaps.length > 0,
      { completed, targeted, targetedLoss, state, losses, gaps: gaps.length,
        maxWallMs: Number(sorted.at(-1)!.toFixed(2)), p95WallMs: Number(sorted[Math.ceil(sorted.length * 0.95) - 1]!.toFixed(2)) });
  } finally { w.close(); }
}

export async function deletionCoverageChecks(check: Check) {
  await finishedCaptureRemoved(check);
  await unlistedStartFile(check);
  await sustainedDeletion(check);
}
