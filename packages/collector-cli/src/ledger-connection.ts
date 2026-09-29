import fs from "node:fs";
import path from "node:path";
import * as childProcess from "node:child_process";
import Database from "better-sqlite3";

/** This factory is also embedded in the two plain-JavaScript worker threads.
 * Keep its runtime dependencies explicit so packaged workers use the same
 * opener and inode checks as the daemon and CLI. */
function ledgerConnectionRuntime(Sqlite: typeof Database, files: typeof fs, paths: typeof path,
  children: typeof childProcess) {
  const lockPath = (file: string) => {
    const absolute = paths.resolve(file);
    const canonical = files.existsSync(absolute) ? files.realpathSync(absolute)
      : paths.join(files.realpathSync(paths.dirname(absolute)), paths.basename(absolute));
    return `${canonical}.connections.lock.sqlite`;
  };
  const acquire = (file: string, mode: "shared" | "exclusive" = "shared") => {
    const sidecar = lockPath(file);
    try { files.closeSync(files.openSync(sidecar, "wx", 0o600)); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    const before = files.lstatSync(sidecar);
    if (!before.isFile() || before.isSymbolicLink() || before.nlink !== 1 ||
        (process.getuid && before.uid !== process.getuid())) {
      throw new Error("ledger_connection_lock_unsafe");
    }
    const database = new Sqlite(sidecar, { fileMustExist: true, timeout: 0 });
    try {
      const after = files.lstatSync(sidecar);
      if (before.dev !== after.dev || before.ino !== after.ino) throw new Error("ledger_connection_lock_changed");
      // No busy wait: even a collector launched by supervision must refuse
      // before opening the ledger. A read forces a SHARED rollback-file lock.
      if (mode === "exclusive") database.pragma("locking_mode = EXCLUSIVE");
      database.exec(mode === "exclusive" ? "BEGIN EXCLUSIVE" : "BEGIN");
      database.prepare("select name from sqlite_master limit 1").get();
    } catch (error) {
      database.close();
      if (/^SQLITE_(BUSY|LOCKED)/.test(String((error as { code?: string }).code))) {
        throw Object.assign(new Error(mode === "shared" ? "ledger switch in progress"
          : "ledger_quiescence_unproven: collector connections are still open"),
        { code: "LEDGER_SWITCH_IN_PROGRESS" });
      }
      throw error;
    }
    let released = false;
    return { database, sidecar, mode, release() {
      if (released) return;
      released = true;
      try { if (database.inTransaction) database.exec("ROLLBACK"); }
      finally { database.close(); }
      // Never unlink this file: an opener must always lock the same inode.
    } };
  };

  const open = (file: string, options: Database.Options = {},
    intentionalRename = false): Database.Database => {
    if (file === ":memory:" || file === "") return new Sqlite(file, options);
    file = paths.resolve(file);
    const lease = acquire(file);
    let database: Database.Database;
    let publication: { state: string; device: number; inode: number;
      freshAttemptPath?: string; marker: { archiveIdentity: string; archivePath: string } } | undefined;
    let beforeOpen: fs.Stats | undefined;
    try {
      if (lease.database.prepare(`select 1 from sqlite_master
          where name='ledger_publication'`).get()) {
        const row = lease.database.prepare("select value from ledger_publication where singleton=1")
          .get() as { value: string } | undefined;
        if (row) publication = JSON.parse(row.value);
        if (publication && !["ready", "restored"].includes(publication.state)) {
          if (publication.state === "publishing") throw new Error("replacement_post_switch_fence_pending");
          throw new Error(`ledger switch in progress; archive recovery required; archive=${publication.marker.archivePath}; save-fresh=${publication.freshAttemptPath}`);
        }
      }
      try { beforeOpen = files.statSync(file); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      database = new Sqlite(file, options);
    }
    catch (error) { lease.release(); throw error; }
    let opened: fs.Stats;
    try {
      opened = files.statSync(file);
      if (beforeOpen && (beforeOpen.dev !== opened.dev || beforeOpen.ino !== opened.ino)) {
        throw new Error("ledger path changed while opening; collector restart required");
      }
      if (publication) {
        if (opened.dev !== publication.device || opened.ino !== publication.inode) {
          throw new Error("replacement_verification_failed: published ledger inode changed");
        }
      }
      if (publication?.state === "ready") {
        const marker = database.prepare(`select archive_identity as archiveIdentity,
          archive_path as archivePath, post_switch_fence_pending as pending
          from collector_replacement_ledger where singleton=1`).get() as
          { archiveIdentity: string; archivePath: string; pending: number } | undefined;
        if (!marker || marker.pending !== 0 || marker.archiveIdentity !== publication.marker.archiveIdentity ||
            marker.archivePath !== publication.marker.archivePath ||
            database.pragma("integrity_check", { simple: true }) !== "ok") {
          throw new Error("replacement_verification_failed: integrity or replacement marker");
        }
        const oldFiles = [publication.marker.archivePath, `${publication.marker.archivePath}-wal`,
          `${publication.marker.archivePath}-shm`].filter(name => files.existsSync(name));
        if (!oldFiles.includes(publication.marker.archivePath)) throw new Error("replacement archive is missing");
        const handles = children.spawnSync("/usr/sbin/lsof", ["-S", "2", "-t", "-w", "--", ...oldFiles],
          { encoding: "utf8", timeout: 10_000, maxBuffer: 1024 * 1024,
            env: { PATH: "/usr/bin:/bin:/usr/sbin" }, stdio: ["ignore", "pipe", "pipe"] });
        const pids = handles.stdout?.trim().split(/\s+/).filter(Boolean).map(Number) ?? [];
        if (handles.error || (handles.status !== 0 && handles.status !== 1) || handles.stderr?.trim() ||
            pids.some(pid => !Number.isSafeInteger(pid) || pid !== process.pid)) {
          throw new Error("replacement_verification_failed: old inode or sidecar handle");
        }
      }
    } catch (error) {
      database.close();
      lease.release();
      if (publication?.state === "ready" && !/^SQLITE_(BUSY|LOCKED)/.test(String((error as { code?: string }).code))) {
        throw Object.assign(new Error("replacement_verification_failed: collector start refused", { cause: error }),
          { code: "LEDGER_PUBLICATION_INVALID", ledgerPath: file });
      }
      throw error;
    }
    const nativeClose = database.close.bind(database);
    database.close = () => {
      try { if (database.open) nativeClose(); }
      finally { lease.release(); }
      return database;
    };
    // Intentional lifecycle mutations own their explicit SQLite lock and
    // retarget files themselves. They still hold the shared opener barrier.
    if (intentionalRename) return database;
    let retired = false;
    const current = () => {
      if (retired) throw new Error("ledger path changed; collector restart required");
      let present: fs.Stats | undefined;
      try { present = files.statSync(file); } catch { /* disappearance is a replacement */ }
      if (present?.dev === opened.dev && present?.ino === opened.ino) return;
      retired = true;
      // SQLite's pager checks HAS_MOVED before its close-time checkpoint.
      // Close without attempting any further SQL on the moved connection.
      try { database.close(); } finally {
        process.exitCode = 75;
        setImmediate(() => process.exit(75)).unref();
      }
      throw Object.assign(new Error("ledger path changed; collector restart required"),
        { code: "LEDGER_REPLACED" });
    };
    const prepare = database.prepare.bind(database);
    database.prepare = ((...args: Parameters<typeof prepare>) => {
      if (!database.inTransaction) current();
      const statement = prepare(...args);
      for (const method of ["run", "get", "all", "iterate"] as const) {
        const invoke = statement[method].bind(statement);
        Object.defineProperty(statement, method, { configurable: true, value: (...values: unknown[]) => {
          if (!database.inTransaction) current();
          return Reflect.apply(invoke, statement, values);
        } });
      }
      return statement;
    }) as typeof database.prepare;
    for (const method of ["exec", "pragma"] as const) {
      const invoke = database[method].bind(database);
      Object.defineProperty(database, method, { configurable: true, value: (...args: unknown[]) => {
        current();
        return Reflect.apply(invoke, database, args);
      } });
    }
    const transaction = database.transaction.bind(database);
    database.transaction = ((action: (...args: unknown[]) => unknown) => {
      const original = transaction(action);
      const wrap = (fn: (...args: unknown[]) => unknown) => function (this: unknown, ...args: unknown[]) {
        current();
        return Reflect.apply(fn, this, args);
      };
      const properties = {
        default: { value: wrap(original) },
        deferred: { value: wrap(original.deferred) },
        immediate: { value: wrap(original.immediate) },
        exclusive: { value: wrap(original.exclusive) },
        database: { value: database, enumerable: true },
      };
      for (const key of ["default", "deferred", "immediate", "exclusive"] as const) {
        Object.defineProperties(properties[key].value, properties);
      }
      return properties.default.value as Database.Transaction;
    }) as typeof database.transaction;
    return database;
  };
  return { acquire, open, lockPath };
}

const runtime = ledgerConnectionRuntime(Database, fs, path, childProcess);
export const acquireLedgerConnectionLock = runtime.acquire;
export const openLedgerDatabase = runtime.open;
export const ledgerConnectionLockPath = runtime.lockPath;
export type LedgerConnectionLock = ReturnType<typeof acquireLedgerConnectionLock>;

export type LedgerPublication = {
  state: "publishing" | "failed" | "ready" | "restored";
  device: number;
  inode: number;
  marker: { archiveIdentity: string; archivePath: string; minCollectorVersion: "0.7.46";
    switchedAt: string; renameToSampleDelayMs: number | null;
    inventoryToRenameDelayMs: number | null; cursorRows: number };
  freshAttemptPath?: string;
};

export function readLedgerPublication(lock: LedgerConnectionLock): LedgerPublication | null {
  if (!lock.database.prepare("select 1 from sqlite_master where name='ledger_publication'").get()) return null;
  const row = lock.database.prepare("select value from ledger_publication where singleton=1")
    .get() as { value: string } | undefined;
  return row ? JSON.parse(row.value) as LedgerPublication : null;
}

/** EXCLUSIVE locking mode keeps the opener barrier across this durable commit. */
export function writeLedgerPublication(lock: LedgerConnectionLock, value: LedgerPublication | null) {
  if (lock.mode !== "exclusive") throw new Error("ledger_publication_requires_exclusive_lock");
  lock.database.exec("create table if not exists ledger_publication (singleton integer primary key check(singleton=1), value text not null)");
  if (value) lock.database.prepare("insert or replace into ledger_publication values (1,?)").run(JSON.stringify(value));
  else lock.database.exec("delete from ledger_publication");
  lock.database.exec("COMMIT");
  const fd = fs.openSync(lock.sidecar, "r");
  try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  const directory = fs.openSync(path.dirname(lock.sidecar), "r");
  try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  lock.database.exec("BEGIN EXCLUSIVE");
}

/** esbuild's optional function-name helper must also exist in eval workers. */
export const ledgerConnectionWorkerSource = `
  const __name = (fn) => fn;
  const openLedgerDatabase = (${ledgerConnectionRuntime.toString()})(
    Database, require('node:fs'), require('node:path'), require('node:child_process')).open;
`;
