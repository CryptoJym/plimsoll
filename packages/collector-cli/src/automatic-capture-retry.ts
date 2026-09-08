import fs from "node:fs";
import type { DiscoveryChunk } from "./incremental-jsonl-discovery";

// Initial service plus four follow-up cadences, independent of file size.
// This bounds the extra service before a slot returns to directory discovery.
const PARTIAL_RETRY_CADENCES = 4;

export type AutomaticCapturePendingFile = DiscoveryChunk["files"][number] & {
  servicedCadences?: number;
};

/** Refresh an admitted retry's metadata without changing its generation or allowance. */
export function refreshAutomaticCaptureFile(file: string, previous: fs.BigIntStats, stat: fs.Stats) {
  const precise = fs.lstatSync(file, { bigint: true });
  if (precise.isSymbolicLink() || !precise.isFile() ||
      precise.dev !== previous.dev || precise.ino !== previous.ino ||
      precise.birthtimeNs !== previous.birthtimeNs || precise.size < previous.size ||
      BigInt(stat.dev) !== precise.dev || BigInt(stat.ino) !== precise.ino ||
      BigInt(stat.size) !== precise.size) {
    throw new Error("capture_retry_generation_changed");
  }
  return { stat, precise };
}

/** Bound service even when the same physical file keeps growing. */
export function advanceAutomaticCaptureFiles(
  pending: AutomaticCapturePendingFile[],
  consumed: ReadonlySet<string>,
  partial: ReadonlySet<string>,
): AutomaticCapturePendingFile[] {
  return pending.filter((file) => {
    if (consumed.has(file.file)) return false;
    // Budget/progress deferral before a durable commit is not a retry.
    if (!partial.has(file.file)) return true;
    file.servicedCadences = (file.servicedCadences ?? 0) + 1;
    return file.servicedCadences <= PARTIAL_RETRY_CADENCES;
  });
}
