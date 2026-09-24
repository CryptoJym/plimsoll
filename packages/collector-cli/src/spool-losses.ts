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
 */
export const SPOOL_LOSS_LOG = ".losses.jsonl";
/** Newest entries kept; older losses fall outside any live epoch long before this fills. */
export const SPOOL_LOSS_LOG_MAX_ENTRIES = 1_000;

export type SpoolLoss = { atMs: number; reason: string };

function lossLogPath(directory: string) {
  return path.join(directory, SPOOL_LOSS_LOG);
}

function parseLoss(line: string): SpoolLoss | null {
  try {
    const value = JSON.parse(line) as Record<string, unknown>;
    const atMs = value.atMs;
    const reason = value.reason;
    if (typeof atMs !== "number" || !Number.isSafeInteger(atMs) || atMs <= 0) return null;
    if (typeof reason !== "string" || !/^[a-z0-9_]{1,64}$/.test(reason)) return null;
    return { atMs, reason };
  } catch {
    return null;
  }
}

/**
 * Append one loss. Best effort by contract: a failure here must never stop the
 * spool from moving on, so it is swallowed and the caller carries on.
 */
export function recordSpoolLoss(directory: string, loss: SpoolLoss) {
  try {
    const reason = /^[a-z0-9_]{1,64}$/.test(loss.reason) ? loss.reason : "spool_lost";
    const file = lossLogPath(directory);
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    fs.appendFileSync(file, `${JSON.stringify({ atMs: Math.trunc(loss.atMs), reason })}\n`, { mode: 0o600 });
    const lines = fs.readFileSync(file, "utf8").split("\n").filter(Boolean);
    if (lines.length > SPOOL_LOSS_LOG_MAX_ENTRIES) {
      const temporary = `${file}.tmp`;
      fs.writeFileSync(temporary, `${lines.slice(-SPOOL_LOSS_LOG_MAX_ENTRIES).join("\n")}\n`, { mode: 0o600 });
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
