/** Defer growth of a fenced file until its verified import parser state exists. */
import crypto from "node:crypto";
import type Database from "better-sqlite3";
import type { CaptureRoot } from "./capture-root-inventory";

export function historyGrowthNeedsHandoff(db: Database.Database,
  root: CaptureRoot | undefined, file: string): boolean {
  if (!root) return false;
  const stateTable = db.prepare(`select 1 from sqlite_master
    where type='table' and name='capture_history_file_state'`).get();
  if (stateTable) {
    const key = crypto.createHash("sha256").update(`${root.rootId}\0${file}`).digest("hex");
    const state = db.prepare(`select handoff_ready as ready from capture_history_file_state
      where file_key=?`).get(key) as { ready: number } | undefined;
    if (state && state.ready !== 1) return true;
    if (state?.ready === 1) return false;
  }
  const lockTable = db.prepare(`select 1 from sqlite_master
    where type='table' and name='capture_history_import_lock'`).get();
  if (!lockTable) return false;
  return Boolean(db.prepare(`select 1 from capture_history_import_lock
    where singleton=1 and root_id=?`).get(root.rootId));
}
