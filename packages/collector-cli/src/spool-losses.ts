import fs from "node:fs";
import path from "node:path";

/**
 * Capture watermark v1 (eco-6hoxj.163.18, review S4): spooled push events the
 * collector accepted and later lost — a hook or OTLP spool file rejected on
 * replay, or an OTLP file deleted at its age limit — must reach the upload
 * capture claim as a dated gap. Each spool appends one line per lost file to
 * its own `.losses.jsonl`, beside its counters, at the moment of loss: the
 * spools keep their bookkeeping out of the ledger on purpose (it must survive
 * a ledger nobody can write), and a deleted file's arrival time is otherwise
 * gone. A line holds the file's arrival time and a reason code, never content.
 *
 * The log never forgets a loss (review r3, S2): past its line limit, the
 * oldest losses are merged into one interval per UTC day, and merged further
 * (closest neighbours first) if even that does not fit. A merged interval
 * only widens the gap the claim reports.
 */
export const SPOOL_LOSS_LOG = ".losses.jsonl";
/** Lines the log holds at most. */
export const SPOOL_LOSS_LOG_MAX_ENTRIES = 1_000;
/** Newest losses kept one per line when the log is compacted. */
const SPOOL_LOSS_LOG_RECENT_ENTRIES = 500;
const DAY_MS = 24 * 60 * 60 * 1000;

/** Spooled files lost between two arrival times: one file, or `count` merged. */
export type SpoolLoss = { fromMs: number; toMs: number; count: number };

function lossLogPath(directory: string) {
  return path.join(directory, SPOOL_LOSS_LOG);
}

const time = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value > 0;

function parseLoss(line: string): SpoolLoss | null {
  try {
    const value = JSON.parse(line) as Record<string, unknown>;
    if (time(value.atMs) && typeof value.reason === "string" && /^[a-z0-9_]{1,64}$/.test(value.reason)) {
      return { fromMs: value.atMs, toMs: value.atMs, count: 1 };
    }
    if (time(value.fromMs) && time(value.toMs) && value.fromMs <= value.toMs &&
      typeof value.count === "number" && Number.isSafeInteger(value.count) && value.count > 0) {
      return { fromMs: value.fromMs, toMs: value.toMs, count: value.count };
    }
    return null;
  } catch {
    return null;
  }
}

/** Merge losses into at most `max` intervals: per UTC day first, then closest neighbours. */
function mergeLosses(losses: readonly SpoolLoss[], max: number): SpoolLoss[] {
  const merged: SpoolLoss[] = [];
  for (const loss of [...losses].sort((a, b) => a.fromMs - b.fromMs || a.toMs - b.toMs)) {
    const last = merged.at(-1);
    if (last && Math.floor(loss.fromMs / DAY_MS) <= Math.floor(last.toMs / DAY_MS)) {
      last.toMs = Math.max(last.toMs, loss.toMs);
      last.count += loss.count;
    } else {
      merged.push({ ...loss });
    }
  }
  while (merged.length > Math.max(1, max)) {
    let closest = 0;
    for (let index = 1; index < merged.length - 1; index += 1) {
      if (merged[index + 1]!.fromMs - merged[index]!.toMs < merged[closest + 1]!.fromMs - merged[closest]!.toMs) {
        closest = index;
      }
    }
    const next = merged[closest + 1]!;
    merged[closest]!.toMs = Math.max(merged[closest]!.toMs, next.toMs);
    merged[closest]!.count += next.count;
    merged.splice(closest + 1, 1);
  }
  return merged;
}

/**
 * Append one loss. Best effort by contract: a failure here must never stop the
 * spool from moving on, so it is swallowed and the caller carries on.
 */
export function recordSpoolLoss(directory: string, loss: { atMs: number; reason: string }) {
  try {
    const reason = /^[a-z0-9_]{1,64}$/.test(loss.reason) ? loss.reason : "spool_lost";
    const file = lossLogPath(directory);
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    fs.appendFileSync(file, `${JSON.stringify({ atMs: Math.trunc(loss.atMs), reason })}\n`, { mode: 0o600 });
    const lines = fs.readFileSync(file, "utf8").split("\n").filter(Boolean);
    if (lines.length > SPOOL_LOSS_LOG_MAX_ENTRIES) {
      // Keep the newest losses as they are; fold every older one into
      // intervals. Nothing is dropped.
      const parsed = lines
        .map((line) => ({ line, loss: parseLoss(line) }))
        .filter((entry): entry is { line: string; loss: SpoolLoss } => entry.loss !== null)
        .sort((a, b) => a.loss.toMs - b.loss.toMs);
      const recent = parsed.slice(-SPOOL_LOSS_LOG_RECENT_ENTRIES);
      const older = mergeLosses(
        parsed.slice(0, -SPOOL_LOSS_LOG_RECENT_ENTRIES).map((entry) => entry.loss),
        SPOOL_LOSS_LOG_MAX_ENTRIES - SPOOL_LOSS_LOG_RECENT_ENTRIES,
      );
      const temporary = `${file}.tmp`;
      fs.writeFileSync(temporary, [
        ...older.map((interval) => JSON.stringify(interval)),
        ...recent.map((entry) => entry.line),
      ].join("\n") + "\n", { mode: 0o600 });
      fs.renameSync(temporary, file);
    }
  } catch {
    /* the loss stays counted by the spool's own counters */
  }
}

/** Every recorded loss; `null` when the log exists but cannot be read. */
export function readSpoolLosses(directory: string): SpoolLoss[] | null {
  let text: string;
  try {
    text = fs.readFileSync(lossLogPath(directory), "utf8");
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT" ? [] : null;
  }
  return text.split("\n").filter(Boolean).map(parseLoss).filter((loss): loss is SpoolLoss => loss !== null);
}
