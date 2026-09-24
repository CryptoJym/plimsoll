import { hookSpoolDirectory, listHookSpoolArrivals } from "./hook-spool";
import { listOtlpSpoolArrivals, otlpSpoolDirectory } from "./otlp-spool";
import { readSpoolLosses, type SpoolLoss } from "./spool-losses";

/**
 * What the hook and OTLP spools hold or lost, for the upload capture claim
 * (eco-6hoxj.163.18, review r2 S4). File names and the spools' own loss logs
 * only; never a spooled file's contents. Kept out of capture-frontier.ts so
 * the ledger's import graph never reaches the spool modules.
 */
export type CaptureSpoolState = {
  /** Accepted push files not yet in the ledger. */
  pendingFiles: number;
  /** Arrival time of the oldest of them, from its file name. */
  oldestPendingMs: number | null;
  /** Accepted push files that will never reach the ledger, by arrival time (merged when old). */
  losses: SpoolLoss[];
  /** A spool directory or loss log exists but cannot be read. */
  unreadable: boolean;
};

export function captureSpoolState(home: string): CaptureSpoolState {
  const listings = [listHookSpoolArrivals(home), listOtlpSpoolArrivals(home)];
  const logs = [readSpoolLosses(hookSpoolDirectory(home)), readSpoolLosses(otlpSpoolDirectory(home))];
  const arrivals = listings.flatMap((listing) => listing ?? []);
  return {
    pendingFiles: arrivals.length,
    oldestPendingMs: arrivals.reduce<number | null>((oldest, at) => (oldest === null || at < oldest ? at : oldest), null),
    losses: logs.flatMap((log) => log ?? []),
    unreadable: [...listings, ...logs].some((value) => value === null),
  };
}
