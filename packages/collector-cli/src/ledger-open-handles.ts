import { spawnSync } from "node:child_process";
import fs from "node:fs";

function lstatIfPresent(file: string) {
  try { return fs.lstatSync(file); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

const LSOF = "/usr/sbin/lsof";
/** A handle probe is advisory; a hung system utility must fail closed quickly. */
const LSOF_TIMEOUT_MS = 60_000;

/**
 * Other processes that have any of `files` open; null when that cannot be
 * established. SQLite's locks only show connections that have used the
 * ledger: one opened but not yet used holds no lock, and after a swap it
 * would pair the replaced file with the new ledger's -wal/-shm by name.
 */
export type OpenHandleCheck = (files: readonly string[]) => number[] | null;

/** lsof over the ledger and its sidecars, ignoring this process. */
export const otherProcessesWithFilesOpen: OpenHandleCheck = (files) => {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const present = files.filter((file) => lstatIfPresent(file));
    if (present.length === 0) return [];
    let result = spawnSync(LSOF, ["-S", "2", "-t", "-w", "--", ...present], {
      encoding: "utf8",
      env: { PATH: "/usr/bin:/bin:/usr/sbin" },
      stdio: ["ignore", "pipe", "pipe"],
      timeout: LSOF_TIMEOUT_MS,
      killSignal: "SIGKILL",
      maxBuffer: 1024 * 1024,
    });
    if (result.error || (result.status !== 0 && result.status !== 1)) return null;
    const pids = (result.stdout ?? "").split("\n").map((line) => line.trim()).filter(Boolean).map(Number);
    if (pids.some((pid) => !Number.isSafeInteger(pid) || pid <= 0)) return null;
    const others = [...new Set(pids)].filter((pid) => pid !== process.pid);
    if (others.length > 0) return others;
    // lsof exits 1 both when nobody has the files open and on errors, and
    // this process always has the ledger open while it checks: an error about
    // any file means it is unknown who has that file open.
    if ((result.stderr ?? "").trim() === "") return [];
    // A sidecar that vanished between listing and lsof: look again once.
    if (!present.every((file) => lstatIfPresent(file))) continue;
    return null;
  }
  return null;
};

