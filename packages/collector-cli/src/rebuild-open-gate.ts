import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

export function rebuildOpenLeaseDirectory(ledgerPath: string) { return `${ledgerPath}.rebuild-open-leases`; }
export function rebuildLockPath(ledgerPath: string) { return `${ledgerPath}.maintenance-rebuild.lock`; }

function canonicalGatePath(inputPath: string) {
  if (inputPath === ":memory:") return inputPath;
  try { return fs.realpathSync(inputPath); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    // A dangling file symlink could otherwise acquire a token beside the
    // alias while SQLite follows it to the fenced target after the rename.
    try { if (fs.lstatSync(inputPath).isSymbolicLink()) throw new Error("writer_ledger_path_unavailable"); }
    catch (lstatError) {
      if ((lstatError as NodeJS.ErrnoException).code !== "ENOENT") throw lstatError;
    }
    return path.join(fs.realpathSync(path.dirname(path.resolve(inputPath))), path.basename(inputPath));
  }
}

export function assertRebuildWriterGateOpen(ledgerPath: string) {
  if (fs.existsSync(rebuildLockPath(canonicalGatePath(ledgerPath)))) throw new Error("maintenance_rebuild_paused");
}

/** Create the token before SQLite opens. The rebuild holds its lock before
 * checking tokens and lsof, closing the check-to-rename race. */
export function acquireRebuildOpenToken(ledgerPath: string) {
  if (ledgerPath === ":memory:") return null;
  const canonical = canonicalGatePath(ledgerPath);
  const directory = rebuildOpenLeaseDirectory(canonical);
  try { fs.mkdirSync(directory, { mode: 0o700 }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  const stat = fs.lstatSync(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("writer_lease_directory_invalid");
  const token = path.join(directory, `${process.pid}.${randomUUID()}.lease`);
  const descriptor = fs.openSync(token, "wx", 0o600);
  try { fs.writeFileSync(descriptor, `${process.pid}\n`); fs.fsyncSync(descriptor); }
  finally { fs.closeSync(descriptor); }
  try { assertRebuildWriterGateOpen(canonical); }
  catch (error) { fs.unlinkSync(token); throw error; }
  return token;
}

export function releaseRebuildOpenToken(token: string | null) {
  if (!token) return;
  try { fs.unlinkSync(token); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
}

export function assertNoRebuildOpenTokens(ledgerPath: string) {
  let entries: string[];
  try { entries = fs.readdirSync(rebuildOpenLeaseDirectory(ledgerPath)); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw new Error("writer_lease_check_unavailable", { cause: error });
  }
  if (entries.some((entry) => entry.endsWith(".lease"))) throw new Error("writer_not_quiesced");
}

/** All separate write-capable ledger connections use this opener. Its token
 * lives exactly as long as the SQLite connection, including thrown opens. */
export function openRebuildFencedDatabase(ledgerPath: string, options?: Database.Options) {
  const token = acquireRebuildOpenToken(ledgerPath);
  let db: Database.Database | null = null;
  try {
    db = new Database(ledgerPath, options);
    const close = db.close.bind(db);
    Object.defineProperty(db, "close", { value: () => {
      try { close(); } finally { releaseRebuildOpenToken(token); }
    } });
    return db;
  } catch (error) {
    try { db?.close(); } finally { releaseRebuildOpenToken(token); }
    throw error;
  }
}
