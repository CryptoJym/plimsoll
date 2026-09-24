import crypto from "node:crypto";
import fs from "node:fs";
import type { FileHandle } from "node:fs/promises";
import path from "node:path";

/**
 * A small private summary the running daemon keeps for local readers such as
 * the macOS menubar (eco-6hoxj.163.34), so they never run `plimsoll status`,
 * which opens the ledger read-write, reads the management credential and
 * scans every row.
 *
 * It holds the four lifetime counters the daemon already caches for /status
 * (no ledger read at all), this run's random instanceId (the value GET
 * /healthz returns), this run's random healthzKey, the collector version,
 * the port and the write time. It names no collector credential, path,
 * account or event, and each counter is one `plimsoll status` already prints.
 *
 * The healthzKey lets a reader of this 0600 file tell the collector from any
 * other process on its port: GET /healthz?challenge=<fresh random> answers
 * with an HMAC of the challenge under the key (healthzProof). Only the
 * collector run and a reader of this file hold the key; no HTTP response
 * carries it, and it unlocks nothing else.
 */
export const STATUS_SUMMARY_FILE = "status-summary.json";
export const STATUS_SUMMARY_SCHEMA = "plimsoll.status-summary/v1";
/** Readers treat a summary several intervals old as stale. */
export const STATUS_SUMMARY_INTERVAL_MS = 15_000;
/** How long shutdown waits for a write in progress, so it leaves no temp file. */
export const STATUS_SUMMARY_STOP_WAIT_MS = 2_000;
/** Names what the /healthz proof authenticates, so it can mean nothing else. */
export const HEALTHZ_PROOF_CONTEXT = "plimsoll.healthz-proof/v1";

/** A challenge is 32 random bytes as unpadded base64url (43 characters). */
export function isHealthzChallenge(value: string) {
  return /^[A-Za-z0-9_-]{43}$/.test(value);
}

/**
 * The /healthz proof: base64url HMAC-SHA256 under this run's key of the
 * context, the port the request arrived on, the run's instanceId and the
 * client's challenge, one per line. A fresh challenge per check means an
 * earlier answer cannot be replayed, and the port means it cannot be relayed
 * from another listener.
 */
export function healthzProof(key: Buffer, port: number, instanceId: string, challenge: string) {
  return crypto.createHmac("sha256", key)
    .update(`${HEALTHZ_PROOF_CONTEXT}\n${port}\n${instanceId}\n${challenge}`)
    .digest("base64url");
}

export type StatusSummaryStats = {
  count: number | null;
  tokenAttributedEvents: number | null;
  totalInputTokens: number | null;
  totalOutputTokens: number | null;
};

export type StatusSummary = {
  schema: typeof STATUS_SUMMARY_SCHEMA;
  instanceId: string;
  /** This run's /healthz proof key, unpadded base64url of 32 random bytes. */
  healthzKey: string;
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

/** The collector home a writer started in, pinned by device and inode. */
export type StatusSummaryHome = { path: string; dev: number; ino: number };

/** A path-free reason a write was refused. */
class StatusSummaryError extends Error {
  constructor(readonly code: "home_not_directory" | "home_changed") {
    super(code);
  }
}

/** Pins the collector home; a later rename or symlink swap of that path is refused. */
export async function anchorStatusSummaryHome(home: string): Promise<StatusSummaryHome> {
  const stat = await fs.promises.lstat(home);
  if (!stat.isDirectory()) throw new StatusSummaryError("home_not_directory");
  return { path: home, dev: stat.dev, ino: stat.ino };
}

async function assertSameHome(home: StatusSummaryHome) {
  const stat = await fs.promises.lstat(home.path);
  if (!stat.isDirectory() || stat.dev !== home.dev || stat.ino !== home.ino) {
    throw new StatusSummaryError("home_changed");
  }
}

/**
 * Temp file (exclusive, no-follow, then forced to 0600 whatever the umask),
 * write, fsync, rename: a reader never sees a partial file. The home is
 * checked again before the temp file is created and before the rename. Every
 * step is asynchronous, so a slow disk delays the summary, never the event
 * loop that serves intake.
 */
export async function writeStatusSummary(home: StatusSummaryHome, summary: StatusSummary) {
  await assertSameHome(home);
  const file = path.join(home.path, STATUS_SUMMARY_FILE);
  const temporary = `${file}.${process.pid}.${crypto.randomBytes(8).toString("hex")}.tmp`;
  let handle: FileHandle | undefined;
  let renamed = false;
  try {
    handle = await fs.promises.open(
      temporary,
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW,
      0o600,
    );
    await handle.chmod(0o600);
    await handle.writeFile(`${JSON.stringify(summary)}\n`, "utf8");
    await handle.sync();
    await handle.close();
    handle = undefined;
    await assertSameHome(home);
    await fs.promises.rename(temporary, file);
    renamed = true;
  } finally {
    if (handle) await handle.close().catch(() => undefined);
    // Only a failed write leaves a temp file; a renamed one is already gone.
    if (!renamed) await fs.promises.rm(temporary, { force: true }).catch(() => undefined);
  }
}

export type StatusSummaryWriterOptions = {
  home: string;
  instanceId: string;
  healthzKey: string;
  collectorVersion: string;
  port: number;
  /** The daemon's cached lifetime stats. Must not read the ledger. */
  stats: () => unknown;
  intervalMs?: number;
  now?: () => Date;
};

export type StatusSummaryWriter = {
  /** Settles when the first write has finished or failed. */
  readonly firstWrite: Promise<void>;
  /** Stops the timer, then waits (at most STATUS_SUMMARY_STOP_WAIT_MS) for a write in progress. */
  stop(): Promise<void>;
};

/**
 * Writes the summary now and every interval after, one write at a time: a
 * tick that finds the previous write still in progress is skipped, so a slow
 * disk never piles writes up. The timer is unref'd.
 */
export function startStatusSummaryWriter(options: StatusSummaryWriterOptions): StatusSummaryWriter {
  const home = anchorStatusSummaryHome(options.home);
  home.catch(() => undefined);
  let lastFailure: string | null = null;
  let inFlight: Promise<void> | null = null;
  let stopped = false;
  const write = async () => {
    try {
      // Sampled at the tick, from memory only.
      const summary: StatusSummary = {
        schema: STATUS_SUMMARY_SCHEMA,
        instanceId: options.instanceId,
        healthzKey: options.healthzKey,
        collectorVersion: options.collectorVersion,
        port: options.port,
        updatedAt: (options.now?.() ?? new Date()).toISOString(),
        stats: statusSummaryStats(options.stats()),
      };
      await writeStatusSummary(await home, summary);
      lastFailure = null;
    } catch (error) {
      // One line per distinct failure; readers see the summary go stale.
      const code = error instanceof StatusSummaryError
        ? error.code
        : String((error as NodeJS.ErrnoException | undefined)?.code ?? "unknown");
      if (code !== lastFailure) console.warn(JSON.stringify({ warning: "status_summary_write_failed", code }));
      lastFailure = code;
    }
  };
  const tick = () => {
    if (stopped || inFlight) return;
    inFlight = write().finally(() => {
      inFlight = null;
    });
  };
  tick();
  const firstWrite = inFlight ?? Promise.resolve();
  const timer = setInterval(tick, options.intervalMs ?? STATUS_SUMMARY_INTERVAL_MS);
  timer.unref();
  return {
    firstWrite,
    async stop() {
      stopped = true;
      clearInterval(timer);
      const pending = inFlight;
      if (!pending) return;
      let bound: NodeJS.Timeout | undefined;
      await Promise.race([
        pending,
        new Promise<void>((resolve) => {
          bound = setTimeout(resolve, STATUS_SUMMARY_STOP_WAIT_MS);
        }),
      ]);
      clearTimeout(bound);
    },
  };
}
