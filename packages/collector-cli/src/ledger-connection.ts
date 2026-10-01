import fs from "node:fs";
import path from "node:path";
import * as childProcess from "node:child_process";
import Database from "better-sqlite3";

/** This factory is also embedded in the two plain-JavaScript worker threads.
 * Keep its runtime dependencies explicit so packaged workers use the same
 * opener and inode checks as the daemon and CLI. */
function ledgerConnectionRuntime(Sqlite: typeof Database, files: typeof fs, paths: typeof path,
  children: typeof childProcess) {
  // Keep this single value-free allowlist inside the factory so serialized
  // workers have exactly the same probe diagnostics and retries as the daemon.
  const PROBE_ERROR_CODES = new Set(["ETIMEDOUT", "ENOENT", "EACCES", "EPERM", "ENOBUFS", "EAGAIN", "EINTR",
    "EMFILE", "ENFILE", "ENOMEM"]);
  const lockPath = (file: string) => {
    const absolute = paths.resolve(file);
    const canonical = files.existsSync(absolute) ? files.realpathSync(absolute)
      : paths.join(files.realpathSync(paths.dirname(absolute)), paths.basename(absolute));
    return `${canonical}.connections.lock.sqlite`;
  };
  const acquire = (file: string, mode: "shared" | "exclusive" = "shared", waitMs = 0) => {
    const sidecar = lockPath(file);
    try { files.closeSync(files.openSync(sidecar, "wx", 0o600)); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    const before = files.lstatSync(sidecar);
    if (!before.isFile() || before.isSymbolicLink() || before.nlink !== 1 ||
        (process.getuid && before.uid !== process.getuid())) {
      throw new Error("ledger_connection_lock_unsafe");
    }
    const database = new Sqlite(sidecar, { fileMustExist: true, timeout: waitMs });
    try {
      const after = files.lstatSync(sidecar);
      if (before.dev !== after.dev || before.ino !== after.ino) throw new Error("ledger_connection_lock_changed");
      // No busy wait: even a collector launched by supervision must refuse
      // before opening the ledger. A read forces a SHARED rollback-file lock.
      if (mode === "exclusive") {
        database.pragma("fullfsync = ON");
        database.pragma("synchronous = EXTRA");
      }
      database.exec(mode === "exclusive" ? "BEGIN EXCLUSIVE" : "BEGIN");
      if (mode === "exclusive") database.pragma("locking_mode = EXCLUSIVE");
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
    intentionalRename = false, barrierFile = file, startupDeadlineMs?: number): Database.Database => {
    if (file === ":memory:" || file === "") return new Sqlite(file, options);
    file = paths.resolve(file);
    barrierFile = paths.resolve(barrierFile);
    const privateCopy = barrierFile !== file;
    const lease = acquire(barrierFile);
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
      // A verified lifecycle snapshot restore intentionally changes the inode.
      // Validate its marker and content below; bind this connection's write
      // guard to the inode it actually opened.
      if (publication?.state === "ready" && !privateCopy) {
        const marker = database.prepare(`select archive_identity as archiveIdentity,
          archive_path as archivePath, post_switch_fence_pending as pending
          from collector_replacement_ledger where singleton=1`).get() as
          { archiveIdentity: string; archivePath: string; pending: number } | undefined;
        if (!marker || marker.pending !== 0 || marker.archiveIdentity !== publication.marker.archiveIdentity ||
            marker.archivePath !== publication.marker.archivePath ||
            database.pragma("integrity_check", { simple: true }) !== "ok") {
          throw new Error("replacement_verification_failed: integrity or replacement marker");
        }
        let archiveClear = false;
        let diagnostic: { stage: "archive_handle_probe"; attempts: number; exitStatus: number | null;
          signal: string | null; stderr: boolean; errorCode: string | null } | null = null;
        // Startup gives each probe its whole remaining deadline, preserving a
        // slow but conclusive first probe. Only quick uncertainty is retried:
        // at most one second spent, with more than one second still available.
        // Startup timeouts never retry; foreign PIDs always refuse immediately.
        // Lifecycle mutations retain three 10-second probes; ordinary opens one.
        const probeAttempts = intentionalRename || startupDeadlineMs !== undefined ? 3 : 1;
        for (let attempt = 0; attempt < probeAttempts; attempt += 1) {
          const oldFiles = [publication.marker.archivePath, `${publication.marker.archivePath}-wal`,
            `${publication.marker.archivePath}-shm`].filter(name => files.existsSync(name));
          if (!oldFiles.includes(publication.marker.archivePath)) throw new Error("replacement archive is missing");
          const probeStarted = performance.now();
          const remainingMs = startupDeadlineMs === undefined ? 10_000
            : Math.floor(startupDeadlineMs - probeStarted);
          if (remainingMs <= 0) break;
          const handles = children.spawnSync("/usr/sbin/lsof", ["-S", "2", "-t", "-w", "--", ...oldFiles],
            { encoding: "utf8", timeout: remainingMs,
              ...(startupDeadlineMs === undefined ? {} : { killSignal: "SIGKILL" as const }), maxBuffer: 1024 * 1024,
              env: { PATH: "/usr/bin:/bin:/usr/sbin" }, stdio: ["ignore", "pipe", "pipe"] });
          const lines = (handles.stdout ?? "").trim().split(/\s+/).filter(Boolean);
          const invalidPid = lines.some(pid => !/^[0-9]+$/.test(pid) || !Number.isSafeInteger(Number(pid)) || Number(pid) <= 0);
          if (lines.some(pid => /^[0-9]+$/.test(pid) && Number.isSafeInteger(Number(pid)) &&
              Number(pid) > 0 && Number(pid) !== process.pid)) {
            throw Object.assign(new Error("replacement_verification_failed: old inode or sidecar handle"),
              { code: "LEDGER_ARCHIVE_HANDLE_IN_USE" });
          }
          if (!handles.error && (handles.status === 0 || handles.status === 1) &&
              !(handles.stderr ?? "").trim() && !invalidPid &&
              (startupDeadlineMs === undefined || performance.now() < startupDeadlineMs)) {
            archiveClear = true;
            break;
          }
          const code = (handles.error as NodeJS.ErrnoException | undefined)?.code;
          diagnostic = { stage: "archive_handle_probe", attempts: attempt + 1,
            exitStatus: handles.status, signal: handles.signal,
            stderr: Boolean((handles.stderr ?? "").trim()),
            errorCode: typeof code === "string" ? (PROBE_ERROR_CODES.has(code) ? code : "OTHER") : null };
          if (attempt < probeAttempts - 1) {
            if (startupDeadlineMs !== undefined) {
              const finished = performance.now();
              if (code === "ETIMEDOUT" || finished - probeStarted > 1_000 ||
                  startupDeadlineMs - finished <= 1_000) break;
            }
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
          }
        }
        if (!archiveClear) {
          throw Object.assign(new Error("replacement_verification_failed: archive handle probe inconclusive"),
            { code: "LEDGER_ARCHIVE_HANDLE_UNPROVEN", diagnostic });
        }
      }
    } catch (error) {
      database.close();
      lease.release();
      if (!privateCopy && publication?.state === "ready" && !/^SQLITE_(BUSY|LOCKED)/.test(String((error as { code?: string }).code))) {
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
      const statement = prepare(...args);
      for (const method of ["run", "get", "all", "iterate"] as const) {
        const invoke = statement[method].bind(statement);
        // Keep the native API writable for instrumentation that wraps a method.
        Object.defineProperty(statement, method, { configurable: true, writable: true, value: (...values: unknown[]) => {
          // Read-only dashboard requests stay free of filesystem work. SQLite
          // classifies writes, including INSERT ... RETURNING used via get().
          if (!statement.readonly && !database.inTransaction) current();
          return Reflect.apply(invoke, statement, values);
        } });
      }
      return statement;
    }) as typeof database.prepare;
    for (const method of ["exec", "pragma"] as const) {
      const invoke = database[method].bind(database);
      Object.defineProperty(database, method, { configurable: true, writable: true, value: (...args: unknown[]) => {
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
  // A private restore copy is guarded by the destination's stable lock. Its
  // integrity is checked by the restore caller; it has no publication witness
  // of its own and must not leave a temporary lock inode behind.
  const openCopy = (file: string, ledgerPath: string, options: Database.Options = {}) =>
    open(file, options, true, ledgerPath);
  // Only the daemon's first open may spend its remaining startup budget on
  // inconclusive admission probes before LocalEventBuffer restores an archive.
  const openForStartup = (file: string, options: Database.Options, deadlineMs: number) =>
    open(file, options, false, file, deadlineMs);
  return { acquire, open, openCopy, openForStartup, lockPath };
}

const runtime = ledgerConnectionRuntime(Database, fs, path, childProcess);
export const acquireLedgerConnectionLock = runtime.acquire;
export const openLedgerDatabase = runtime.open;
export const openLedgerDatabaseForStartup = runtime.openForStartup;
export const openLedgerCopyDatabase = runtime.openCopy;
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
  // SQLite's EXTRA + fullfsync commit flushes its own descriptor. Opening
  // and closing another descriptor on this inode would drop this process's
  // POSIX locks, even though SQLite still believes it holds EXCLUSIVE.
  const directory = fs.openSync(path.dirname(lock.sidecar), "r");
  try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  lock.database.exec("BEGIN EXCLUSIVE");
}

/** esbuild's optional function-name helper must also exist in eval workers. */
export const ledgerConnectionWorkerSource = `
  const __name = (fn) => fn;
  const { open: openLedgerDatabase, openCopy: openLedgerCopyDatabase } = (${ledgerConnectionRuntime.toString()})(
    Database, require('node:fs'), require('node:path'), require('node:child_process'));
`;
