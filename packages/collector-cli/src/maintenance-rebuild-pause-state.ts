import fs from "node:fs";
import path from "node:path";

const MARKER = "maintenance-rebuild-pause.json";
type PauseMarker = { version: 1; at: string; pid?: number; endedAt?: string };

function writeMarker(home: string, marker: PauseMarker) {
  const file = path.join(home, MARKER);
  const temporary = `${file}.${process.pid}.tmp`;
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try { fs.writeFileSync(descriptor, `${JSON.stringify(marker)}\n`); fs.fsyncSync(descriptor); }
  finally { fs.closeSync(descriptor); }
  fs.renameSync(temporary, file);
  const directory = fs.openSync(home, "r");
  try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
}

export function markMaintenanceRebuildPause(home: string) {
  writeMarker(home, { version: 1, at: new Date().toISOString(), pid: process.pid });
}

export function readMaintenanceRebuildPause(home: string): PauseMarker | null {
  try {
    const file = path.join(home, MARKER);
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("maintenance_pause_marker_invalid");
    const value = JSON.parse(fs.readFileSync(file, "utf8")) as PauseMarker;
    if (value.version !== 1 || !Number.isFinite(Date.parse(value.at)) ||
      (value.endedAt !== undefined && !Number.isFinite(Date.parse(value.endedAt)))) {
      throw new Error("maintenance_pause_marker_invalid");
    }
    return value;
  }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

export function finishMaintenanceRebuildPause(home: string) {
  const marker = readMaintenanceRebuildPause(home);
  if (marker && !marker.endedAt) writeMarker(home, { ...marker, endedAt: new Date().toISOString() });
}

/** A SIGKILL cannot stamp endedAt. A later daemon may settle its dead
 * listener's marker before deciding whether any of its arrivals remain. */
export function settleInterruptedMaintenanceRebuildPause(home: string) {
  const marker = readMaintenanceRebuildPause(home);
  if (!marker || marker.endedAt) return marker;
  // Markers written by the first B13 version have no PID. Only that version
  // wrote this shape, so a current daemon reading it is after its pause.
  if (!marker.pid) {
    finishMaintenanceRebuildPause(home);
    return readMaintenanceRebuildPause(home);
  }
  try { process.kill(marker.pid, 0); return marker; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") return marker;
    finishMaintenanceRebuildPause(home);
    return readMaintenanceRebuildPause(home);
  }
}

export function clearMaintenanceRebuildPause(home: string) {
  try { fs.unlinkSync(path.join(home, MARKER)); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
}

export function maintenanceRebuildPauseSeen(home: string) {
  return readMaintenanceRebuildPause(home) !== null;
}
