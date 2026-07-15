import fs from "node:fs";
import path from "node:path";

export type CollectorPidRecord = {
  command: string[];
  cwd: string;
  label: string;
  pid: number;
  startedAt: string;
  version: 1;
};

type StartLockRecord = {
  label: string;
  pid: number;
  token: string;
  version: 1;
};

export type CollectorStartOwnership =
  | {
      kind: "already_running";
      ownerPid: number;
      pidPath: string;
      port: number;
    }
  | {
      kind: "owner";
      lockPath: string;
      pidPath: string;
      release: () => void;
      writePidFile: (record: CollectorPidRecord) => void;
    };

export class CollectorStartOwnershipError extends Error {
  constructor(
    readonly code: "start_in_progress",
    message: string,
  ) {
    super(message);
    this.name = "CollectorStartOwnershipError";
  }
}

function processIsRunning(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function parsePidRecord(raw: string, label: string): CollectorPidRecord | null {
  try {
    const parsed = JSON.parse(raw) as Partial<CollectorPidRecord>;
    if (
      parsed.version === 1 &&
      parsed.label === label &&
      Number.isInteger(parsed.pid) &&
      (parsed.pid ?? 0) > 0 &&
      Array.isArray(parsed.command) &&
      typeof parsed.cwd === "string" &&
      typeof parsed.startedAt === "string"
    ) {
      return parsed as CollectorPidRecord;
    }
  } catch {
    // Invalid ownership records are stale state, not authority.
  }
  return null;
}

function readPidRecord(pidPath: string, label: string) {
  if (!fs.existsSync(pidPath)) return null;
  return parsePidRecord(fs.readFileSync(pidPath, "utf8").trim(), label);
}

function removeFileIfUnchanged(filePath: string, expected: string) {
  try {
    if (fs.readFileSync(filePath, "utf8") === expected) {
      fs.unlinkSync(filePath);
      return true;
    }
  } catch {
    // Another process already changed or removed the file.
  }
  return false;
}

function removePidRecordIfOwned(pidPath: string, pid: number, label: string) {
  try {
    const raw = fs.readFileSync(pidPath, "utf8");
    const record = parsePidRecord(raw.trim(), label);
    return record?.pid === pid ? removeFileIfUnchanged(pidPath, raw) : false;
  } catch {
    return false;
  }
}

export function removeCollectorPidFileIfOwned(pidPath: string, pid: number, label: string) {
  return removePidRecordIfOwned(pidPath, pid, label);
}

function sleep(milliseconds: number) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function collectorStatusIsHealthy(port: number, timeoutMs: number) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("http://127.0.0.1:" + port + "/status", {
      signal: controller.signal,
    });
    if (!response.ok) return false;
    const body = (await response.json()) as Record<string, unknown>;
    return (
      body.ok === true &&
      typeof body.dataMode === "string" &&
      typeof body.retentionDays === "number" &&
      typeof body.stats === "object" &&
      body.stats !== null
    );
  } catch {
    return false;
  } finally {
    clearTimeout(timer);
  }
}

function lockRecord(raw: string, label: string): StartLockRecord | null {
  try {
    const parsed = JSON.parse(raw) as Partial<StartLockRecord>;
    if (
      parsed.version === 1 &&
      parsed.label === label &&
      Number.isInteger(parsed.pid) &&
      (parsed.pid ?? 0) > 0 &&
      typeof parsed.token === "string" &&
      parsed.token.length > 0
    ) {
      return parsed as StartLockRecord;
    }
  } catch {
    // Invalid lock records are recoverable stale state.
  }
  return null;
}

function releaseLock(lockPath: string, serializedLock: string) {
  removeFileIfUnchanged(lockPath, serializedLock);
}

export async function acquireCollectorStartOwnership(options: {
  label: string;
  pidPath: string;
  port: number;
  probeTimeoutMs?: number;
  waitTimeoutMs?: number;
}): Promise<CollectorStartOwnership> {
  const probeTimeoutMs = options.probeTimeoutMs ?? 5_000;
  const waitTimeoutMs = options.waitTimeoutMs ?? 10_000;
  const lockPath = options.pidPath + ".start.lock";
  const deadline = Date.now() + waitTimeoutMs;
  let knownUnhealthyPid: number | null = null;
  fs.mkdirSync(path.dirname(options.pidPath), { recursive: true, mode: 0o700 });

  while (Date.now() <= deadline) {
    const existing = readPidRecord(options.pidPath, options.label);
    if (existing && processIsRunning(existing.pid) && knownUnhealthyPid !== existing.pid) {
      if (await collectorStatusIsHealthy(options.port, probeTimeoutMs)) {
        return {
          kind: "already_running",
          ownerPid: existing.pid,
          pidPath: options.pidPath,
          port: options.port,
        };
      }
      knownUnhealthyPid = existing.pid;
    }

    const token = [
      process.pid,
      Date.now(),
      Math.random().toString(16).slice(2),
    ].join("-");
    const serializedLock =
      JSON.stringify({
        label: options.label,
        pid: process.pid,
        token,
        version: 1,
      } satisfies StartLockRecord) + "\n";

    try {
      fs.writeFileSync(lockPath, serializedLock, { flag: "wx", mode: 0o600 });

      const rechecked = readPidRecord(options.pidPath, options.label);
      if (
        rechecked &&
        processIsRunning(rechecked.pid) &&
        knownUnhealthyPid !== rechecked.pid
      ) {
        if (await collectorStatusIsHealthy(options.port, probeTimeoutMs)) {
          releaseLock(lockPath, serializedLock);
          return {
            kind: "already_running",
            ownerPid: rechecked.pid,
            pidPath: options.pidPath,
            port: options.port,
          };
        }
        knownUnhealthyPid = rechecked.pid;
      }

      if (rechecked) {
        removePidRecordIfOwned(options.pidPath, rechecked.pid, options.label);
      } else if (fs.existsSync(options.pidPath)) {
        const invalid = fs.readFileSync(options.pidPath, "utf8");
        removeFileIfUnchanged(options.pidPath, invalid);
      }

      let released = false;
      return {
        kind: "owner",
        lockPath,
        pidPath: options.pidPath,
        release: () => {
          if (released) return;
          released = true;
          releaseLock(lockPath, serializedLock);
        },
        writePidFile: (record) => {
          fs.writeFileSync(options.pidPath, JSON.stringify(record, null, 2) + "\n", {
            flag: "wx",
            mode: 0o600,
          });
        },
      };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      try {
        const raw = fs.readFileSync(lockPath, "utf8");
        const currentLock = lockRecord(raw, options.label);
        if (!currentLock || !processIsRunning(currentLock.pid)) {
          removeFileIfUnchanged(lockPath, raw);
          continue;
        }
      } catch {
        continue;
      }
      await sleep(100);
    }
  }

  throw new CollectorStartOwnershipError(
    "start_in_progress",
    "Another collector start still owns " + lockPath + " after " + waitTimeoutMs + "ms.",
  );
}
