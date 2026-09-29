import { hookSpoolDirectory, listHookSpoolArrivalDetails } from "./hook-spool";
import { listOtlpSpoolArrivals, otlpSpoolDirectory } from "./otlp-spool";
import { readSpoolLosses, type SpoolLoss } from "./spool-losses";
import { clearMaintenanceRebuildPause, reconcileMaintenanceRebuildRefusals,
  settleInterruptedMaintenanceRebuildPause } from "./maintenance-rebuild-pause-state";

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
  /** Older-binary hook retries consumed without an immutable body digest. */
  unverifiedHookRetries?: number;
  /** Retired old-format refusals whose exact tenant/body outcome is unknowable. */
  unknownHookReceiptFormats?: number;
};

export function captureSpoolState(home: string): CaptureSpoolState {
  const hookArrivals = listHookSpoolArrivalDetails(home);
  const listings = [hookArrivals?.map((arrival) => arrival.atMs) ?? null, listOtlpSpoolArrivals(home)];
  const logs = [readSpoolLosses(hookSpoolDirectory(home)), readSpoolLosses(otlpSpoolDirectory(home))];
  const arrivals = listings.flatMap((listing) => listing ?? []);
  const refusalState = reconcileMaintenanceRebuildRefusals(home);
  const refused = refusalState.count;
  const unreadable = [...listings, ...logs, refused, refusalState.unverifiedHookRetries,
    refusalState.unknownHookReceiptFormats]
    .some((value) => value === null);
  // Current client retries use the 0.7.44-compatible filename and a durable
  // refusal receipt until drain. Recognize previously tagged files as well.
  const taggedPending = hookArrivals?.some((arrival) => arrival.maintenanceRebuild) ?? false;
  let maintenanceRebuildPending = taggedPending || refused === null || refused > 0;
  try {
    const marker = settleInterruptedMaintenanceRebuildPause(home);
    if (marker && !unreadable) {
      const start = Date.parse(marker.at);
      const end = marker.endedAt ? Date.parse(marker.endedAt) : Infinity;
      maintenanceRebuildPending = maintenanceRebuildPending || arrivals.some((at) => at >= start && at <= end);
      if (marker.endedAt && !maintenanceRebuildPending) clearMaintenanceRebuildPause(home);
    } else if (marker) maintenanceRebuildPending = maintenanceRebuildPending || arrivals.length > 0;
  } catch { maintenanceRebuildPending = true; }
  return {
    pendingFiles: arrivals.length,
    oldestPendingMs: arrivals.reduce<number | null>((oldest, at) => (oldest === null || at < oldest ? at : oldest), null),
    // A missing client retry past the spool's stale threshold is a durable
    // known unknown, carried by its retained receipt and visible as a claim
    // gap/dead count. A later exact ledger match retires that gap.
    losses: [...logs.flatMap((log) => log ?? []), ...refusalState.lost],
    unreadable,
    maintenanceRebuildPending,
    unverifiedHookRetries: refusalState.unverifiedHookRetries ?? 0,
    unknownHookReceiptFormats: refusalState.unknownHookReceiptFormats ?? 0,
  };
}
