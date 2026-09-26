import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { performance } from "node:perf_hooks";

import { collectorBufferPath } from "./config";
import type { LifecyclePairingIndexesRecord } from "./lifecycle";

const WINDOW_MS = 180_000;
const RETRY_DELAY_MS = 5_000;

/** Run the existing explicit upgrade in its own process after update readiness. */
export async function buildPairingIndexesAfterUpdate(): Promise<LifecyclePairingIndexesRecord> {
  const started = performance.now();
  const elapsedMs = () => Math.min(WINDOW_MS, Math.max(0, Math.floor(performance.now() - started)));
  const skipped = (reason: Exclude<LifecyclePairingIndexesRecord["reason"], null>, attempts: number) =>
    ({ status: "skipped" as const, reason, attempts, elapsedMs: elapsedMs() });
  const ledger = collectorBufferPath();
  try {
    if (!fs.lstatSync(ledger).isFile()) return skipped("upgrade_failed", 0);
  } catch (error) {
    return skipped((error as NodeJS.ErrnoException).code === "ENOENT" ? "ledger_missing" : "upgrade_failed", 0);
  }
  const script = process.argv[1];
  if (!script || !fs.existsSync(script)) return skipped("upgrade_failed", 0);

  let lastReason: Exclude<LifecyclePairingIndexesRecord["reason"], null> = "upgrade_failed";
  for (let attempts = 1; attempts <= 2; attempts += 1) {
    const remaining = WINDOW_MS - elapsedMs();
    if (remaining <= 0) return skipped("timeout", attempts - 1);
    const result = spawnSync(process.execPath,
      [...process.execArgv, script, "lifecycle", "pairing-indexes", "--apply"], {
        encoding: "utf8", timeout: remaining, killSignal: "SIGKILL", maxBuffer: 64 * 1024,
        stdio: ["ignore", "pipe", "pipe"],
      });
    if (result.status === 0) {
      try {
        const receipt = JSON.parse(result.stdout ?? "") as { applied?: boolean; after?: { enabled?: boolean } };
        if (receipt.applied === true && receipt.after?.enabled === true) {
          return { status: "applied", reason: null, attempts, elapsedMs: elapsedMs() };
        }
      } catch { /* A malformed child receipt is a nonfatal upgrade failure. */ }
    }
    const diagnostic = result.stderr ?? "";
    lastReason = (result.error as NodeJS.ErrnoException | undefined)?.code === "ETIMEDOUT" ? "timeout"
      : /requires every other ledger connection to be stopped|lost exclusive ledger ownership|database is locked|SQLITE_BUSY/.test(diagnostic)
        ? "ledger_in_use"
        : /cannot prove ledger quiescence/.test(diagnostic) ? "quiescence_unproven" : "upgrade_failed";
    if (attempts === 2 || lastReason !== "ledger_in_use") return skipped(lastReason, attempts);
    if (WINDOW_MS - elapsedMs() <= RETRY_DELAY_MS) return skipped("timeout", attempts);
    await new Promise<void>((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
  }
  return skipped(lastReason, 2);
}
