import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

/**
 * A small private summary the running daemon keeps for local readers such as
 * the macOS menubar (eco-6hoxj.163.34), so they never run `plimsoll status`,
 * which opens the ledger read-write, reads the management credential and
 * scans every row.
 *
 * It holds the four lifetime counters the daemon already caches for /status
 * (no ledger read at all), this run's random instanceId (the value GET
 * /healthz returns), the collector version, the port and the write time. It
 * names no credential, path, account or event, and each counter is one
 * `plimsoll status` already prints.
 */
export const STATUS_SUMMARY_FILE = "status-summary.json";
export const STATUS_SUMMARY_SCHEMA = "plimsoll.status-summary/v1";
/** Readers treat a summary several intervals old as stale. */
export const STATUS_SUMMARY_INTERVAL_MS = 15_000;

export type StatusSummaryStats = {
  count: number | null;
  tokenAttributedEvents: number | null;
  totalInputTokens: number | null;
  totalOutputTokens: number | null;
};

export type StatusSummary = {
  schema: typeof STATUS_SUMMARY_SCHEMA;
  instanceId: string;
  collectorVersion: string;
  port: number;
  updatedAt: string;
  stats: StatusSummaryStats | null;
};

/** The four menu counters from the cached lifetime stats; every other field is dropped. */
export function statusSummaryStats(stats: unknown): StatusSummaryStats | null {
  if (!stats || typeof stats !== "object" || Array.isArray(stats)) return null;
  const record = stats as Record<string, unknown>;
  const counter = (key: string) => {
    const value = record[key];
    return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
  };
  return {
    count: counter("count"),
    tokenAttributedEvents: counter("tokenAttributedEvents"),
    totalInputTokens: counter("totalInputTokens"),
    totalOutputTokens: counter("totalOutputTokens"),
  };
}

/** Temp file (exclusive, 0600), fsync, rename: a reader never sees a partial file. */
export function writeStatusSummary(home: string, summary: StatusSummary) {
  const file = path.join(home, STATUS_SUMMARY_FILE);
  const temporary = `${file}.${process.pid}.${crypto.randomBytes(8).toString("hex")}.tmp`;
  let descriptor: number | undefined;
  try {
    descriptor = fs.openSync(temporary, "wx", 0o600);
    fs.writeFileSync(descriptor, `${JSON.stringify(summary)}\n`, "utf8");
    fs.fsyncSync(descriptor);
    fs.closeSync(descriptor);
    descriptor = undefined;
    fs.renameSync(temporary, file);
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
    fs.rmSync(temporary, { force: true });
  }
}

export type StatusSummaryWriterOptions = {
  home: string;
  instanceId: string;
  collectorVersion: string;
  port: number;
  /** The daemon's cached lifetime stats. Must not read the ledger. */
  stats: () => unknown;
  intervalMs?: number;
  now?: () => Date;
};

/** Writes the summary now and every interval after; returns the unref'd timer. */
export function startStatusSummaryWriter(options: StatusSummaryWriterOptions): NodeJS.Timeout {
  let lastFailure: string | null = null;
  const write = () => {
    try {
      writeStatusSummary(options.home, {
        schema: STATUS_SUMMARY_SCHEMA,
        instanceId: options.instanceId,
        collectorVersion: options.collectorVersion,
        port: options.port,
        updatedAt: (options.now?.() ?? new Date()).toISOString(),
        stats: statusSummaryStats(options.stats()),
      });
      lastFailure = null;
    } catch (error) {
      // One line per distinct failure; readers see the summary go stale.
      const code = String((error as NodeJS.ErrnoException | undefined)?.code ?? "unknown");
      if (code !== lastFailure) console.warn(JSON.stringify({ warning: "status_summary_write_failed", code }));
      lastFailure = code;
    }
  };
  write();
  const timer = setInterval(write, options.intervalMs ?? STATUS_SUMMARY_INTERVAL_MS);
  timer.unref();
  return timer;
}
