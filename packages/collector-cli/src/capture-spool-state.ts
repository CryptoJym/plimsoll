import { hookSpoolDirectory, listHookSpoolArrivalDetails } from "./hook-spool";
import { listOtlpSpoolArrivals, otlpSpoolDirectory } from "./otlp-spool";
import { readSpoolLosses, type SpoolLoss } from "./spool-losses";
import { clearMaintenanceRebuildPause, settleInterruptedMaintenanceRebuildPause } from "./maintenance-rebuild-pause-state";

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
  /** An earlier rebuild pause is still waiting for its admitted pushes. */
  maintenanceRebuildPending?: boolean;
};

export function captureSpoolState(home: string): CaptureSpoolState {
  const hookArrivals = listHookSpoolArrivalDetails(home);
  const listings = [hookArrivals?.map((arrival) => arrival.atMs) ?? null, listOtlpSpoolArrivals(home)];
  const logs = [readSpoolLosses(hookSpoolDirectory(home)), readSpoolLosses(otlpSpoolDirectory(home))];
  const arrivals = listings.flatMap((listing) => listing ?? []);
  const unreadable = [...listings, ...logs].some((value) => value === null);
  // A client may write its retry file after the listener stamps endedAt and
  // the empty marker is cleared. The filename's maintenance cause survives
  // that interval and keeps the capture claim unattested until drain.
  const taggedPending = hookArrivals?.some((arrival) => arrival.maintenanceRebuild) ?? false;
  let maintenanceRebuildPending = taggedPending;
  try {
    const marker = settleInterruptedMaintenanceRebuildPause(home);
    if (marker && !unreadable) {
      const start = Date.parse(marker.at);
      const end = marker.endedAt ? Date.parse(marker.endedAt) : Infinity;
      maintenanceRebuildPending = taggedPending || arrivals.some((at) => at >= start && at <= end);
      if (marker.endedAt && !maintenanceRebuildPending) clearMaintenanceRebuildPause(home);
    } else if (marker) maintenanceRebuildPending = taggedPending || arrivals.length > 0;
  } catch { maintenanceRebuildPending = taggedPending || arrivals.length > 0; }
  return {
    pendingFiles: arrivals.length,
    oldestPendingMs: arrivals.reduce<number | null>((oldest, at) => (oldest === null || at < oldest ? at : oldest), null),
    losses: logs.flatMap((log) => log ?? []),
    unreadable,
    maintenanceRebuildPending,
  };
}
