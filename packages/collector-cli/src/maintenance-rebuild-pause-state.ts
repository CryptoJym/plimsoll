import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";

const MARKER = "maintenance-rebuild-pause.json";
const REFUSALS = "maintenance-rebuild-refusals";
type PauseMarker = { version: 1; at: string; pid?: number; endedAt?: string };

type RefusalRoute = "otlp" | "live";
function refusalDirectory(home: string) { return path.join(home, REFUSALS); }
function refusalPath(home: string, route: RefusalRoute, source: string, body: string | Buffer) {
  const digest = createHash("sha256").update(`${route}\0${source}\0`).update(body).digest("hex");
  return path.join(refusalDirectory(home), `${digest}.receipt`);
}
function fsyncDirectory(directory: string) {
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
}

/** A 503 has no server spool, so its route and body identity must survive the
 * listener's exit. Repeated refusals of the same payload share one receipt. */
export function recordMaintenanceRebuildRefusal(home: string, route: RefusalRoute,
  source: string, body: string | Buffer) {
  if (!readMaintenanceRebuildPause(home)) throw new Error("maintenance_pause_marker_missing");
  const directory = refusalDirectory(home);
  try {
    fs.mkdirSync(directory, { mode: 0o700 });
    fsyncDirectory(home);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
  const dirStat = fs.lstatSync(directory);
  if (!dirStat.isDirectory() || dirStat.isSymbolicLink()) throw new Error("maintenance_refusals_unsafe");
  const file = refusalPath(home, route, source, body);
  let descriptor: number;
  try { descriptor = fs.openSync(file, "wx", 0o600); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("maintenance_refusal_unsafe");
    return;
  }
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify({ version: 1, route, at: new Date().toISOString() })}\n`);
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
  fsyncDirectory(directory);
}

/** Only a matching retry whose normal route committed may retire this file. */
export function resolveMaintenanceRebuildRefusal(home: string, route: RefusalRoute,
  source: string, body: string | Buffer) {
  const file = refusalPath(home, route, source, body);
  try {
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("maintenance_refusal_unsafe");
    fs.unlinkSync(file);
    fsyncDirectory(refusalDirectory(home));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}

/** null means the receipt inventory is unsafe or unreadable: hold attestation. */
export function countMaintenanceRebuildRefusals(home: string): number | null {
  const directory = refusalDirectory(home);
  try {
    const stat = fs.lstatSync(directory);
    if (!stat.isDirectory() || stat.isSymbolicLink()) return null;
    const entries = fs.readdirSync(directory, { withFileTypes: true });
    if (entries.some((entry) => !/^[a-f0-9]{64}\.receipt$/.test(entry.name) || !entry.isFile() ||
      entry.isSymbolicLink())) return null;
    return entries.length;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT" ? 0 : null;
  }
}

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
