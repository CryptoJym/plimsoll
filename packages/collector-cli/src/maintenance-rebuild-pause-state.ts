import fs from "node:fs";
import path from "node:path";

const MARKER = "maintenance-rebuild-pause.json";

export function markMaintenanceRebuildPause(home: string) {
  const marker = path.join(home, MARKER);
  const temporary = `${marker}.${process.pid}.tmp`;
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify({ version: 1, at: new Date().toISOString() })}\n`);
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
  fs.renameSync(temporary, marker);
  const directory = fs.openSync(home, "r");
  try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
}

/** A prior rebuild with an undrained push spool withholds a complete claim. */
export function maintenanceRebuildPauseSeen(home: string) {
  try { fs.lstatSync(path.join(home, MARKER)); return true; }
  catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ENOENT";
  }
}
