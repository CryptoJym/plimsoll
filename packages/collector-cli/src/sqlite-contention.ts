export const SQLITE_SYNC_RETRY_LIMITS = Object.freeze({
  budgetMs: 31_000,
  initialDelayMs: 25,
  maxDelayMs: 100,
});

function sqliteCode(error: unknown): string | number | null {
  if (!error || typeof error !== "object") return null;
  const source = error as Record<string, unknown>;
  for (const key of ["code", "sqliteCode", "extendedCode", "errno"]) {
    const value = source[key];
    if (typeof value === "string" || typeof value === "number") return value;
  }
  return null;
}

/** SQLite primary and extended BUSY/LOCKED codes, in string or numeric form. */
export function isSqliteContentionError(error: unknown) {
  const code = sqliteCode(error);
  if (typeof code === "string") {
    const canonical = code.toUpperCase();
    return canonical.startsWith("SQLITE_BUSY") || canonical.startsWith("SQLITE_LOCKED");
  }
  return typeof code === "number" && ((code & 0xff) === 5 || (code & 0xff) === 6);
}

export class SyncStorageBusyError extends Error {
  readonly code = "SYNC_STORAGE_BUSY";
  readonly sqliteCode: string | number | null;

  constructor(
    readonly waitMs: number,
    readonly retries: number,
    cause: unknown,
  ) {
    super("sync_storage_busy");
    this.name = "SyncStorageBusyError";
    this.sqliteCode = sqliteCode(cause);
    (this as Error & { cause?: unknown }).cause = cause;
  }
}

type SyncStorageRetryOptions = {
  budgetMs?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  sleep?: (milliseconds: number) => Promise<void>;
};

/** One daemon-sync-cycle wait budget shared by every atomic SQLite operation. */
export class SyncStorageRetryController {
  private readonly budgetMs: number;
  private readonly initialDelayMs: number;
  private readonly maxDelayMs: number;
  private readonly sleep: (milliseconds: number) => Promise<void>;
  private waitMs = 0;
  private retries = 0;

  constructor(options: SyncStorageRetryOptions = {}) {
    this.budgetMs = Math.max(0, Math.trunc(options.budgetMs ?? SQLITE_SYNC_RETRY_LIMITS.budgetMs));
    this.initialDelayMs = Math.max(
      1,
      Math.trunc(options.initialDelayMs ?? SQLITE_SYNC_RETRY_LIMITS.initialDelayMs),
    );
    this.maxDelayMs = Math.max(
      this.initialDelayMs,
      Math.trunc(options.maxDelayMs ?? SQLITE_SYNC_RETRY_LIMITS.maxDelayMs),
    );
    this.sleep = options.sleep ?? ((milliseconds) =>
      new Promise<void>((resolve) => setTimeout(resolve, milliseconds)));
  }

  receipt() {
    return { waitMs: this.waitMs, retries: this.retries, budgetMs: this.budgetMs };
  }

  async run<T>(operation: () => T | Promise<T>): Promise<T> {
    let delayMs = this.initialDelayMs;
    for (;;) {
      try {
        return await operation();
      } catch (error) {
        if (!isSqliteContentionError(error)) throw error;
        const remainingMs = this.budgetMs - this.waitMs;
        if (remainingMs <= 0) {
          throw new SyncStorageBusyError(this.waitMs, this.retries, error);
        }
        const waitMs = Math.min(delayMs, remainingMs);
        this.retries += 1;
        await this.sleep(waitMs);
        this.waitMs += waitMs;
        delayMs = Math.min(delayMs * 2, this.maxDelayMs);
      }
    }
  }
}
