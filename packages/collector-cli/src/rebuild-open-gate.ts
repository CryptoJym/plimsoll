import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { processIdentityIsLive, UTC_PROCESS_START_ALGORITHM } from "./runtime-ownership";

export function rebuildOpenLeaseDirectory(ledgerPath: string) { return `${ledgerPath}.rebuild-open-leases`; }
export function rebuildLockPath(ledgerPath: string) { return `${ledgerPath}.maintenance-rebuild.lock`; }
export function rebuildResumeClaimPath(ledgerPath: string, nonce: string) {
  return `${ledgerPath}.maintenance-rebuild-resume-claim.${nonce}`;
}

type ResumeIdentity = Readonly<{ pid: number; instanceId: string; processStartFingerprint: string }>;
type ResumeClaim = ResumeIdentity & Readonly<{ nonce: string }>;
const localResumeClaims = new Map<string, ResumeClaim>();

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

function resumeState(canonical: string): { phase?: unknown; nonce?: unknown } | null {
  try { return JSON.parse(fs.readFileSync(`${canonical}.maintenance-rebuild.json`, "utf8")); }
  catch { return null; }
}

/** Called only after the daemon owns its start lock. The nonce-specific wx
 * claim grants this process, and no second process, the resume-stage opens. */
export function claimRebuildResumePermit(ledgerPath: string, startLockPath: string,
  identity: ResumeIdentity) {
  const canonical = canonicalGatePath(ledgerPath);
  if (!fs.existsSync(rebuildLockPath(canonical))) return false;
  const state = resumeState(canonical);
  if (state?.phase !== "resume_started" || typeof state.nonce !== "string" ||
    !/^[0-9a-f-]{36}$/i.test(state.nonce) || identity.pid !== process.pid ||
    !processIdentityIsLive({ ...identity, processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM }) ||
    localResumeClaims.has(canonical)) throw new Error("maintenance_rebuild_paused");
  let owner: Partial<ResumeIdentity> & { version?: number; label?: string };
  try {
    const stat = fs.lstatSync(startLockPath);
    if (!stat.isFile() || stat.isSymbolicLink() ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid()) ||
      (stat.mode & 0o077) !== 0) throw new Error("start_lock_untrusted");
    const parsed = JSON.parse(fs.readFileSync(startLockPath, "utf8"));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("start_lock_untrusted");
    owner = parsed;
  } catch { throw new Error("maintenance_rebuild_paused"); }
  if (owner.version !== 3 || owner.label !== "com.plimsoll.collector" ||
    owner.pid !== identity.pid || owner.instanceId !== identity.instanceId ||
    owner.processStartFingerprint !== identity.processStartFingerprint) {
    throw new Error("maintenance_rebuild_paused");
  }
  const claim: ResumeClaim = { nonce: state.nonce, pid: identity.pid,
    instanceId: identity.instanceId, processStartFingerprint: identity.processStartFingerprint };
  const claimPath = rebuildResumeClaimPath(canonical, state.nonce);
  let descriptor: number;
  try { descriptor = fs.openSync(claimPath, "wx", 0o600); }
  catch { throw new Error("maintenance_rebuild_paused"); }
  try { fs.writeFileSync(descriptor, `${JSON.stringify(claim)}\n`); fs.fsyncSync(descriptor); }
  finally { fs.closeSync(descriptor); }
  const directory = fs.openSync(path.dirname(claimPath), "r");
  try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  localResumeClaims.set(canonical, claim);
  return true;
}

export function assertRebuildWriterGateOpen(ledgerPath: string) {
  const canonical = canonicalGatePath(ledgerPath);
  if (!fs.existsSync(rebuildLockPath(canonical))) return;
  const claim = localResumeClaims.get(canonical);
  if (claim?.pid === process.pid) {
    try {
      const state = resumeState(canonical);
      if ((state?.phase === "resume_started" || state?.phase === "complete") &&
        state.nonce === claim.nonce &&
        fs.readFileSync(rebuildResumeClaimPath(canonical, claim.nonce), "utf8") ===
          `${JSON.stringify(claim)}\n`) return;
    } catch { /* A missing or unreadable state never grants an opener. */ }
  }
  throw new Error("maintenance_rebuild_paused");
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
