import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

/** SQLite owns the cross-process lock. The coordination database is never
 * unlinked: replacing its inode would split publishers and recoverers into
 * independent lock domains. SQLite releases BEGIN EXCLUSIVE on process death. */
export function withRebuildCoordination<T>(ledgerPath: string, action: () => T): T {
  if (!path.isAbsolute(ledgerPath) || path.normalize(ledgerPath) !== ledgerPath ||
    fs.realpathSync(path.dirname(ledgerPath)) !== path.dirname(ledgerPath)) {
    throw new Error("ledger_path_not_canonical");
  }
  const file = `${ledgerPath}.maintenance-rebuild-coordination.sqlite`;
  try {
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("rebuild_coordination_unsafe");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const db = new Database(file, { timeout: 30_000 });
  try {
    fs.chmodSync(file, 0o600);
    db.pragma("journal_mode = DELETE");
    db.exec("BEGIN EXCLUSIVE");
    try {
      const result = action();
      db.exec("COMMIT");
      return result;
    } catch (error) {
      db.exec("ROLLBACK");
      throw error;
    }
  } finally { db.close(); }
}
