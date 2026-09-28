import { classifyProcessIdentity, readUtcProcessStartFingerprint,
  UTC_PROCESS_START_ALGORITHM, type ProcessIdentityLiveness } from "./runtime-ownership";

export type RebuildWriterIdentity = Readonly<{
  pid: number;
  processStartFingerprint: string;
  processStartFingerprintAlgorithm: typeof UTC_PROCESS_START_ALGORITHM;
}>;

let current: RebuildWriterIdentity | null = null;

/** Both the opener token and durable SQLite row name the same process run. */
export function currentRebuildWriterIdentity(): RebuildWriterIdentity {
  if (current) return current;
  const processStartFingerprint = readUtcProcessStartFingerprint(process.pid);
  if (!processStartFingerprint) throw new Error("writer_identity_unavailable");
  current = { pid: process.pid, processStartFingerprint,
    processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM };
  return current;
}

/** Only a known-dead process run permits retirement. Old PID-only records
 * can be retired when the PID is absent; a reused live PID stays fenced. */
export function rebuildWriterIdentityLiveness(value: unknown): ProcessIdentityLiveness {
  if (!value || typeof value !== "object" || Array.isArray(value)) return "indeterminate";
  const record = value as Record<string, unknown>;
  const pid = record.pid;
  if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) return "indeterminate";
  if (record.processStartFingerprint === undefined &&
      record.processStartFingerprintAlgorithm === undefined) {
    try { process.kill(pid, 0); return "live"; }
    catch (error) { return (error as NodeJS.ErrnoException).code === "ESRCH" ? "stale" : "indeterminate"; }
  }
  if (typeof record.processStartFingerprint !== "string" ||
      !/^sha256:[0-9a-f]{64}$/.test(record.processStartFingerprint) ||
      record.processStartFingerprintAlgorithm !== UTC_PROCESS_START_ALGORITHM) return "indeterminate";
  return classifyProcessIdentity({ pid, processStartFingerprint: record.processStartFingerprint,
    processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM });
}
