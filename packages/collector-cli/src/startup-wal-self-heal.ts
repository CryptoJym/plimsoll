import { spawn } from "node:child_process";
import fs from "node:fs";

export const DEFAULT_STARTUP_WAL_CHECKPOINT_BYTES = 1024 * 1024 * 1024;
export const STARTUP_WAL_CHECKPOINT_TIMEOUT_MS = 5_000;
export type WalCheckpointCounts = { busy: number; log: number; checkpointed: number };
export type StartupWalSelfHealReceipt = {
  status: "startup_wal_checkpoint";
  attempted: boolean;
  outcome: "skipped_below_threshold" | "completed" | "busy" | "timed_out" | "failed";
  thresholdBytes: number;
  bytesBefore: number;
  bytesAfter: number;
  busy: number | null;
  log: number | null;
  checkpointed: number | null;
  timeoutMs: number;
};

function fileBytes(path: string) {
  try { return fs.statSync(path).size; } catch { return 0; }
}

export async function runStartupWalSelfHeal(options: {
  ledgerPath: string;
  thresholdBytes?: number;
  timeoutMs?: number;
  runCheckpoint: (timeoutMs: number) => Promise<WalCheckpointCounts>;
}): Promise<StartupWalSelfHealReceipt> {
  const thresholdBytes = Math.max(1, options.thresholdBytes ?? DEFAULT_STARTUP_WAL_CHECKPOINT_BYTES);
  const timeoutMs = Math.max(1, Math.min(options.timeoutMs ?? STARTUP_WAL_CHECKPOINT_TIMEOUT_MS, 30_000));
  const walPath = `${options.ledgerPath}-wal`;
  const bytesBefore = fileBytes(walPath);
  const base = { status: "startup_wal_checkpoint" as const, thresholdBytes, bytesBefore, timeoutMs };
  if (bytesBefore <= thresholdBytes) {
    return { ...base, attempted: false, outcome: "skipped_below_threshold", bytesAfter: bytesBefore,
      busy: null, log: null, checkpointed: null };
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const timed = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("startup_wal_checkpoint_timed_out")), timeoutMs);
      timer.unref();
    });
    const counts = await Promise.race([options.runCheckpoint(timeoutMs), timed]);
    return { ...base, attempted: true, outcome: counts.busy === 0 ? "completed" : "busy",
      bytesAfter: fileBytes(walPath), ...counts };
  } catch (error) {
    return { ...base, attempted: true,
      outcome: error instanceof Error && error.message === "startup_wal_checkpoint_timed_out" ? "timed_out" : "failed",
      bytesAfter: fileBytes(walPath), busy: null, log: null, checkpointed: null };
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export function checkpointWalInBoundedChild(options: {
  entryPath: string; execArgv: string[]; nonce: string; timeoutMs: number;
}): Promise<WalCheckpointCounts> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath,
      [...options.execArgv, options.entryPath, "__startup_wal_checkpoint", options.nonce, String(options.timeoutMs)],
      { env: { ...process.env, PLIMSOLL_STARTUP_WAL_NONCE: options.nonce }, stdio: ["ignore", "pipe", "ignore"] });
    let output = "";
    child.stdout.on("data", (chunk) => {
      if (output.length < 4096) output += String(chunk).slice(0, 4096 - output.length);
    });
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, Math.max(1, options.timeoutMs - 100));
    timer.unref();
    child.once("error", (error) => { clearTimeout(timer); reject(error); });
    child.once("close", (code) => {
      clearTimeout(timer);
      if (timedOut) return reject(new Error("startup_wal_checkpoint_timed_out"));
      if (code !== 0) return reject(new Error("startup_wal_checkpoint_failed"));
      try {
        const parsed = JSON.parse(output) as WalCheckpointCounts;
        if (![parsed.busy, parsed.log, parsed.checkpointed].every(Number.isSafeInteger)) throw new Error();
        resolve(parsed);
      } catch { reject(new Error("startup_wal_checkpoint_invalid_receipt")); }
    });
  });
}
