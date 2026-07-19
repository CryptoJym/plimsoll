import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { parse as parseToml } from "smol-toml";

/**
 * Config APPLY mode (issue 0003): idempotent, surgical merges of Plimsoll's
 * telemetry settings into the user's existing tool configs. Never clobbers:
 * unknown keys/hooks/sections are preserved byte-for-byte where possible, a
 * timestamped backup is written before any change, and a second run reports
 * no-op. Callers render the change list and own the confirm step.
 */

export type ApplyResult = {
  path: string;
  changed: boolean;
  changes: string[];
  backupPath?: string;
  /** Set when an existing conflicting config blocks a safe merge. */
  conflict?: string;
};

type ClaudeFileIdentity = {
  device: number;
  inode: number;
  mode: number;
  links: number;
  uid: number;
};

type ClaudePathIdentity = ClaudeFileIdentity & { path: string };

type ClaudePathSnapshot = {
  absolutePath: string;
  ancestors: ClaudePathIdentity[];
  exists: boolean;
  leaf?: ClaudeFileIdentity;
};

export type ClaudeApplyOptions = {
  dryRun?: boolean;
  /** Deterministic synthetic-proof seam; production callers must leave this unset. */
  transactionHooks?: {
    afterParentCreate?: () => void;
    afterPrepare?: () => void;
    afterBackup?: () => void;
    beforeCommit?: () => void;
    afterCommit?: () => void;
  };
};

class ClaudeConfigError extends Error {
  constructor(readonly code: string) {
    super(`CLAUDE_CONFIG_${code}`);
    this.name = "ClaudeConfigError";
  }
}

function claudeFail(code: string): never {
  throw new ClaudeConfigError(code);
}

function runClaudeHook(hook: (() => void) | undefined) {
  if (!hook) return;
  try {
    hook();
  } catch {
    claudeFail("TRANSACTION_ABORTED");
  }
}

function claudeLstat(file: string): fs.Stats | null {
  try {
    return fs.lstatSync(file);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

function claudeIdentity(stat: fs.Stats): ClaudeFileIdentity {
  return {
    device: stat.dev,
    inode: stat.ino,
    mode: stat.mode,
    links: stat.nlink,
    uid: stat.uid,
  };
}

function sameClaudeIdentity(left: ClaudeFileIdentity, right: ClaudeFileIdentity) {
  return left.device === right.device &&
    left.inode === right.inode &&
    left.mode === right.mode &&
    left.links === right.links &&
    left.uid === right.uid;
}

function currentUid() {
  return typeof process.getuid === "function" ? process.getuid() : undefined;
}

function assertSafeClaudeLeaf(stat: fs.Stats) {
  if (stat.isSymbolicLink()) claudeFail("UNSAFE_LEAF_SYMLINK");
  if (!stat.isFile()) claudeFail("UNSAFE_LEAF_TYPE");
  if (stat.nlink !== 1) claudeFail("UNSAFE_LEAF_LINK_COUNT");
  const permissions = stat.mode & 0o777;
  if ((permissions & 0o111) !== 0 || (permissions & 0o022) !== 0) {
    claudeFail("UNSAFE_LEAF_MODE");
  }
  const uid = currentUid();
  if (uid !== undefined && stat.uid !== uid) claudeFail("UNSAFE_LEAF_OWNER");
}

function inspectClaudePath(file: string): ClaudePathSnapshot {
  const absolutePath = path.resolve(file);
  const parsed = path.parse(absolutePath);
  const segments = absolutePath.slice(parsed.root.length).split(path.sep).filter(Boolean);
  const ancestors: ClaudePathIdentity[] = [];
  let cursor = parsed.root;
  let missingAncestor = false;
  for (const segment of segments.slice(0, -1)) {
    cursor = path.join(cursor, segment);
    if (missingAncestor) continue;
    const stat = claudeLstat(cursor);
    if (!stat) {
      missingAncestor = true;
      continue;
    }
    if (stat.isSymbolicLink()) {
      // macOS exposes root-owned compatibility aliases such as /var. They are
      // outside the operator-writable subtree; all lower symlinks fail closed.
      if (path.dirname(cursor) === parsed.root && stat.uid === 0) continue;
      claudeFail("UNSAFE_ANCESTOR_SYMLINK");
    }
    if (!stat.isDirectory()) claudeFail("UNSAFE_ANCESTOR_TYPE");
    ancestors.push({ path: cursor, ...claudeIdentity(stat) });
  }

  const leafStat = claudeLstat(absolutePath);
  if (!leafStat) return { absolutePath, ancestors, exists: false };
  if (missingAncestor) claudeFail("PATH_CHANGED");
  assertSafeClaudeLeaf(leafStat);
  return { absolutePath, ancestors, exists: true, leaf: claudeIdentity(leafStat) };
}

function sameClaudeAncestors(left: ClaudePathIdentity[], right: ClaudePathIdentity[]) {
  return left.length === right.length && left.every((entry, index) =>
    entry.path === right[index]?.path &&
      entry.device === right[index]?.device &&
      entry.inode === right[index]?.inode &&
      entry.mode === right[index]?.mode &&
      entry.uid === right[index]?.uid,
  );
}

function assertStableClaudePath(expected: ClaudePathSnapshot) {
  const current = inspectClaudePath(expected.absolutePath);
  if (current.absolutePath !== expected.absolutePath || current.exists !== expected.exists) {
    claudeFail("PATH_CHANGED");
  }
  if (!sameClaudeAncestors(current.ancestors, expected.ancestors)) {
    claudeFail("ANCESTOR_CHANGED");
  }
  if (expected.exists && (!current.leaf || !expected.leaf || !sameClaudeIdentity(current.leaf, expected.leaf))) {
    claudeFail("LEAF_CHANGED");
  }
}

function claudeOpenNoFollow(file: string, flags: number) {
  try {
    return fs.openSync(file, flags | fs.constants.O_NOFOLLOW);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ELOOP" || code === "EMLINK") claudeFail("UNSAFE_SYMLINK");
    throw error;
  }
}

function claudeReadDescriptor(descriptor: number) {
  const chunks: Buffer[] = [];
  let position = 0;
  while (true) {
    const chunk = Buffer.allocUnsafe(64 * 1024);
    const bytesRead = fs.readSync(descriptor, chunk, 0, chunk.length, position);
    if (bytesRead === 0) break;
    chunks.push(chunk.subarray(0, bytesRead));
    position += bytesRead;
  }
  return Buffer.concat(chunks).toString("utf8");
}

function claudeWriteDescriptor(descriptor: number, content: string) {
  const bytes = Buffer.from(content);
  let offset = 0;
  while (offset < bytes.length) {
    offset += fs.writeSync(descriptor, bytes, offset, bytes.length - offset, offset);
  }
  fs.fsyncSync(descriptor);
}

function assertClaudeBoundContent(
  descriptor: number,
  expectedIdentity: ClaudeFileIdentity,
  expectedContent: string,
) {
  const before = claudeIdentity(fs.fstatSync(descriptor));
  if (!sameClaudeIdentity(before, expectedIdentity)) claudeFail("BOUND_IDENTITY_CHANGED");
  const content = claudeReadDescriptor(descriptor);
  const after = claudeIdentity(fs.fstatSync(descriptor));
  if (!sameClaudeIdentity(after, expectedIdentity) || content !== expectedContent) {
    claudeFail("BOUND_CONTENT_CHANGED");
  }
}

function readClaudePreimage(file: string) {
  const snapshot = inspectClaudePath(file);
  if (!snapshot.exists) return { snapshot, current: "" };
  const descriptor = claudeOpenNoFollow(snapshot.absolutePath, fs.constants.O_RDONLY);
  try {
    const opened = claudeIdentity(fs.fstatSync(descriptor));
    assertSafeClaudeLeaf(fs.fstatSync(descriptor));
    if (!snapshot.leaf || !sameClaudeIdentity(opened, snapshot.leaf)) claudeFail("LEAF_CHANGED");
    const current = claudeReadDescriptor(descriptor);
    assertClaudeBoundContent(descriptor, snapshot.leaf, current);
    assertStableClaudePath(snapshot);
    return { snapshot, current };
  } finally {
    fs.closeSync(descriptor);
  }
}

function fsyncClaudeDirectory(directory: string) {
  const descriptor = claudeOpenNoFollow(directory, fs.constants.O_RDONLY);
  try {
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
}

function ensureClaudeParent(
  snapshot: ClaudePathSnapshot,
  hook: (() => void) | undefined,
) {
  const directory = path.dirname(snapshot.absolutePath);
  const parsed = path.parse(directory);
  const segments = directory.slice(parsed.root.length).split(path.sep).filter(Boolean);
  let cursor = parsed.root;
  for (const segment of segments) {
    const parent = cursor;
    cursor = path.join(cursor, segment);
    const before = claudeLstat(cursor);
    if (!before) {
      try {
        fs.mkdirSync(cursor, { mode: 0o700 });
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      }
      const created = claudeLstat(cursor);
      if (!created || !created.isDirectory() || created.isSymbolicLink()) {
        claudeFail("PARENT_CREATE_RACE");
      }
      const uid = currentUid();
      if ((created.mode & 0o777) !== 0o700 || (uid !== undefined && created.uid !== uid)) {
        claudeFail("UNSAFE_PARENT_MODE");
      }
      fsyncClaudeDirectory(parent);
    } else {
      if (before.isSymbolicLink()) {
        if (path.dirname(cursor) !== parsed.root || before.uid !== 0) {
          claudeFail("UNSAFE_ANCESTOR_SYMLINK");
        }
      } else if (!before.isDirectory()) {
        claudeFail("UNSAFE_ANCESTOR_TYPE");
      }
    }
  }

  const parentStat = claudeLstat(directory);
  if (!parentStat || !parentStat.isDirectory() || parentStat.isSymbolicLink()) {
    claudeFail("UNSAFE_PARENT");
  }
  const uid = currentUid();
  if ((uid !== undefined && parentStat.uid !== uid) || (parentStat.mode & 0o022) !== 0) {
    claudeFail("UNSAFE_PARENT_MODE");
  }

  const createdReady = inspectClaudePath(snapshot.absolutePath);
  if (createdReady.exists !== snapshot.exists) claudeFail("PATH_CHANGED");
  for (const ancestor of snapshot.ancestors) {
    const current = createdReady.ancestors.find((entry) => entry.path === ancestor.path);
    if (
      !current ||
      current.device !== ancestor.device ||
      current.inode !== ancestor.inode ||
      current.mode !== ancestor.mode ||
      current.uid !== ancestor.uid
    ) {
      claudeFail("ANCESTOR_CHANGED");
    }
  }
  runClaudeHook(hook);
  const ready = inspectClaudePath(snapshot.absolutePath);
  if (
    ready.exists !== createdReady.exists ||
    !sameClaudeAncestors(ready.ancestors, createdReady.ancestors) ||
    (createdReady.exists &&
      (!ready.leaf || !createdReady.leaf || !sameClaudeIdentity(ready.leaf, createdReady.leaf)))
  ) {
    claudeFail("ANCESTOR_CHANGED");
  }
  return ready;
}

function assertVisibleClaudeContent(
  snapshot: ClaudePathSnapshot,
  expectedIdentity: ClaudeFileIdentity,
  expectedContent: string,
) {
  const visible = inspectClaudePath(snapshot.absolutePath);
  if (
    !visible.exists ||
    !visible.leaf ||
    !sameClaudeAncestors(visible.ancestors, snapshot.ancestors) ||
    !sameClaudeIdentity(visible.leaf, expectedIdentity)
  ) {
    claudeFail("VISIBLE_IDENTITY_MISMATCH");
  }
  const descriptor = claudeOpenNoFollow(visible.absolutePath, fs.constants.O_RDONLY);
  try {
    assertClaudeBoundContent(descriptor, expectedIdentity, expectedContent);
  } finally {
    fs.closeSync(descriptor);
  }
  const after = inspectClaudePath(snapshot.absolutePath);
  if (
    !after.exists ||
    !after.leaf ||
    !sameClaudeAncestors(after.ancestors, snapshot.ancestors) ||
    !sameClaudeIdentity(after.leaf, expectedIdentity)
  ) {
    claudeFail("VISIBLE_POSTCONDITION_CHANGED");
  }
}

function unlinkClaudeObject(file: string, identity: ClaudeFileIdentity | undefined) {
  const current = claudeLstat(file);
  if (
    identity &&
    current &&
    !current.isSymbolicLink() &&
    current.isFile() &&
    current.dev === identity.device &&
    current.ino === identity.inode
  ) {
    fs.unlinkSync(file);
  }
}

function prepareClaudeFile(snapshot: ClaudePathSnapshot, next: string) {
  const tempPath = path.join(
    path.dirname(snapshot.absolutePath),
    `.${path.basename(snapshot.absolutePath)}.plimsoll-tmp-${randomUUID()}`,
  );
  const descriptor = claudeOpenNoFollow(
    tempPath,
    fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL,
  );
  const identity = claudeIdentity(fs.fstatSync(descriptor));
  try {
    if (identity.links !== 1) claudeFail("PREPARED_LINK_COUNT");
    const mode = snapshot.leaf ? snapshot.leaf.mode & 0o777 : 0o600;
    fs.fchmodSync(descriptor, mode);
    claudeWriteDescriptor(descriptor, next);
  } catch (error) {
    unlinkClaudeObject(tempPath, identity);
    throw error;
  } finally {
    fs.closeSync(descriptor);
  }
  assertVisibleClaudeContent(
    { ...snapshot, absolutePath: tempPath, exists: true, leaf: identity },
    { ...identity, mode: (identity.mode & ~0o777) | (snapshot.leaf ? snapshot.leaf.mode & 0o777 : 0o600) },
    next,
  );
  return {
    tempPath,
    identity: claudeIdentity(fs.lstatSync(tempPath)),
  };
}

function backupClaudePreimage(
  snapshot: ClaudePathSnapshot,
  current: string,
  boundDescriptor: number,
) {
  if (!snapshot.leaf) claudeFail("MISSING_PREIMAGE_IDENTITY");
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const backupPath = `${snapshot.absolutePath}.plimsoll-backup-${stamp}-${randomUUID()}`;
  const descriptor = claudeOpenNoFollow(
    backupPath,
    fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_EXCL,
  );
  let created = claudeIdentity(fs.fstatSync(descriptor));
  try {
    if (created.links !== 1) claudeFail("BACKUP_LINK_COUNT");
    assertClaudeBoundContent(boundDescriptor, snapshot.leaf, current);
    assertVisibleClaudeContent(snapshot, snapshot.leaf, current);
    fs.fchmodSync(descriptor, snapshot.leaf.mode & 0o777);
    claudeWriteDescriptor(descriptor, current);
    created = claudeIdentity(fs.fstatSync(descriptor));
    assertClaudeBoundContent(descriptor, created, current);
    assertClaudeBoundContent(boundDescriptor, snapshot.leaf, current);
    assertVisibleClaudeContent(snapshot, snapshot.leaf, current);
  } catch (error) {
    fs.closeSync(descriptor);
    unlinkClaudeObject(backupPath, created);
    throw error;
  }
  fs.closeSync(descriptor);
  assertVisibleClaudeContent(
    { ...snapshot, absolutePath: backupPath, exists: true, leaf: created },
    created,
    current,
  );
  fsyncClaudeDirectory(path.dirname(snapshot.absolutePath));
  return backupPath;
}

function writeClaudePlan(
  initial: ClaudePathSnapshot,
  current: string,
  next: string,
  hooks: NonNullable<ClaudeApplyOptions["transactionHooks"]> = {},
) {
  const snapshot = ensureClaudeParent(initial, hooks.afterParentCreate);
  assertStableClaudePath(snapshot);
  const boundDescriptor = snapshot.exists
    ? claudeOpenNoFollow(snapshot.absolutePath, fs.constants.O_RDONLY)
    : undefined;
  let prepared: ReturnType<typeof prepareClaudeFile> | undefined;
  try {
    if (snapshot.exists) {
      if (boundDescriptor === undefined || !snapshot.leaf) claudeFail("MISSING_PREIMAGE_IDENTITY");
      assertClaudeBoundContent(boundDescriptor, snapshot.leaf, current);
      assertVisibleClaudeContent(snapshot, snapshot.leaf, current);
    }

    prepared = prepareClaudeFile(snapshot, next);
    runClaudeHook(hooks.afterPrepare);
    assertStableClaudePath(snapshot);
    assertVisibleClaudeContent(
      { ...snapshot, absolutePath: prepared.tempPath, exists: true, leaf: prepared.identity },
      prepared.identity,
      next,
    );

    const backupPath = snapshot.exists
      ? backupClaudePreimage(snapshot, current, boundDescriptor!)
      : undefined;
    runClaudeHook(hooks.afterBackup);

    if (snapshot.exists) {
      assertClaudeBoundContent(boundDescriptor!, snapshot.leaf!, current);
      assertVisibleClaudeContent(snapshot, snapshot.leaf!, current);
    } else {
      assertStableClaudePath(snapshot);
    }
    assertVisibleClaudeContent(
      { ...snapshot, absolutePath: prepared.tempPath, exists: true, leaf: prepared.identity },
      prepared.identity,
      next,
    );
    runClaudeHook(hooks.beforeCommit);

    if (snapshot.exists) {
      assertClaudeBoundContent(boundDescriptor!, snapshot.leaf!, current);
      assertVisibleClaudeContent(snapshot, snapshot.leaf!, current);
      fs.renameSync(prepared.tempPath, snapshot.absolutePath);
    } else {
      assertStableClaudePath(snapshot);
      // No-clobber publication for a fresh file. A plain rename could replace
      // a concurrently-created operator file between the final check and commit.
      fs.linkSync(prepared.tempPath, snapshot.absolutePath);
      fs.unlinkSync(prepared.tempPath);
    }

    runClaudeHook(hooks.afterCommit);
    assertVisibleClaudeContent(snapshot, prepared.identity, next);
    fsyncClaudeDirectory(path.dirname(snapshot.absolutePath));
    assertVisibleClaudeContent(snapshot, prepared.identity, next);
    return backupPath;
  } finally {
    if (boundDescriptor !== undefined) fs.closeSync(boundDescriptor);
    if (prepared) unlinkClaudeObject(prepared.tempPath, prepared.identity);
  }
}

function isJsonRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function aliasFold(value: string) {
  return value.normalize("NFKC").toLowerCase();
}

function rejectManagedAliases(keys: string[], managed: string[], code: string) {
  const managedFolded = new Map(managed.map((key) => [aliasFold(key), key]));
  for (const key of keys) {
    const canonical = managedFolded.get(aliasFold(key));
    if (canonical && key !== canonical) claudeFail(code);
  }
}

function hookUrlKinds(value: unknown): Array<"owned" | "alias"> {
  const kinds: Array<"owned" | "alias"> = [];
  const visit = (node: unknown) => {
    if (Array.isArray(node)) {
      for (const entry of node) visit(entry);
      return;
    }
    if (!isJsonRecord(node)) return;
    for (const [key, entry] of Object.entries(node)) {
      if (aliasFold(key) === "url" && key !== "url") claudeFail("HOOK_FIELD_ALIAS");
      if (key === "url" && typeof entry === "string") {
        const folded = aliasFold(entry);
        if (/^http:\/\/127\.0\.0\.1:\d+\/hooks\/claude-code$/.test(folded)) {
          kinds.push(entry === folded ? "owned" : "alias");
        }
      }
      visit(entry);
    }
  };
  visit(value);
  return kinds;
}

function isOwnedClaudeHook(value: unknown) {
  const kinds = hookUrlKinds(value);
  if (kinds.includes("alias")) claudeFail("HOOK_URL_ALIAS");
  return kinds.includes("owned");
}

function parseClaudeDocument(current: string) {
  if (current === "") return {} as Record<string, unknown>;
  try {
    const parsed = JSON.parse(current) as unknown;
    if (!isJsonRecord(parsed)) claudeFail("INVALID_ROOT");
    return parsed;
  } catch (error) {
    if (error instanceof ClaudeConfigError) throw error;
    claudeFail("MALFORMED_JSON");
  }
}

function reconcileClaudeDocument(
  currentSource: string,
  generated: { env: Record<string, string>; hooks?: Record<string, unknown[]> },
) {
  const current = parseClaudeDocument(currentSource);
  if (!isJsonRecord(generated) || !isJsonRecord(generated.env)) claudeFail("INVALID_GENERATED_ENV");
  if (generated.hooks !== undefined && !isJsonRecord(generated.hooks)) {
    claudeFail("INVALID_GENERATED_HOOKS");
  }
  if (!Object.values(generated.env).every((value) => typeof value === "string")) {
    claudeFail("INVALID_GENERATED_ENV");
  }
  if (current.env !== undefined && !isJsonRecord(current.env)) claudeFail("INVALID_ENV");
  if (current.hooks !== undefined && !isJsonRecord(current.hooks)) claudeFail("INVALID_HOOKS");

  const generatedEnvKeys = Object.keys(generated.env);
  const currentEnv = (current.env ?? {}) as Record<string, unknown>;
  rejectManagedAliases(Object.keys(currentEnv), generatedEnvKeys, "ENV_KEY_ALIAS");
  const env = { ...currentEnv };
  const changes: string[] = [];
  for (const [key, value] of Object.entries(generated.env)) {
    if (!Object.hasOwn(env, key) || env[key] !== value) {
      env[key] = value;
      changes.push(`claude.env.${key}.set`);
    }
  }

  const generatedHooks = generated.hooks ?? {};
  const generatedEvents = Object.keys(generatedHooks);
  const currentHooks = (current.hooks ?? {}) as Record<string, unknown>;
  rejectManagedAliases(Object.keys(currentHooks), generatedEvents, "HOOK_EVENT_ALIAS");
  const hooks = { ...currentHooks };
  for (const [event, expectedEntries] of Object.entries(generatedHooks)) {
    if (!Array.isArray(expectedEntries) || expectedEntries.length === 0) {
      claudeFail("INVALID_GENERATED_HOOKS");
    }
    if (!expectedEntries.every((entry) => isOwnedClaudeHook(entry))) {
      claudeFail("INVALID_GENERATED_HOOKS");
    }
    const existingValue = currentHooks[event];
    if (existingValue !== undefined && !Array.isArray(existingValue)) claudeFail("INVALID_HOOK_EVENT");
    const existing = (existingValue ?? []) as unknown[];
    const foreign: unknown[] = [];
    const owned: unknown[] = [];
    for (const entry of existing) {
      (isOwnedClaudeHook(entry) ? owned : foreign).push(entry);
    }
    const alreadyExact = owned.length === expectedEntries.length &&
      owned.every((entry, index) => isDeepStrictEqual(entry, expectedEntries[index]));
    if (!alreadyExact) {
      hooks[event] = [...foreign, ...expectedEntries];
      changes.push(`claude.hooks.${event}.reconcile`);
    }
  }

  const next = { ...current, env, hooks };
  const reparsed = JSON.parse(`${JSON.stringify(next, null, 2)}\n`) as unknown;
  if (!isJsonRecord(reparsed) || !isDeepStrictEqual(reparsed, next)) {
    claudeFail("INVALID_PLAN");
  }
  return { next: `${JSON.stringify(next, null, 2)}\n`, changes };
}

/** Merge generated env + hooks into Claude Code settings.json. */
export function applyClaudeSettings(
  file: string,
  generated: { env: Record<string, string>; hooks?: Record<string, unknown[]> },
  options: ClaudeApplyOptions = {},
): ApplyResult {
  try {
    const { snapshot, current } = readClaudePreimage(file);
    const plan = reconcileClaudeDocument(current, generated);
    if (plan.changes.length === 0) return { path: file, changed: false, changes: [] };
    if (options.dryRun) return { path: file, changed: true, changes: plan.changes };
    const backupPath = writeClaudePlan(snapshot, current, plan.next, options.transactionHooks);
    return { path: file, changed: true, changes: plan.changes, backupPath };
  } catch (error) {
    if (error instanceof ClaudeConfigError) throw error;
    claudeFail("IO_FAILURE");
  }
}

type TomlRecord = Record<string, unknown>;

type TomlHeader = {
  index: number;
  kind: "table" | "array";
  path: string[];
};

type TomlAssignment = {
  index: number;
  tableKind: "table" | "array" | "root";
  tablePath: string[];
  keyPath: string[];
  valueStart: number;
  valueEnd: number;
  valueRaw: string;
};

type TomlScan = {
  headers: TomlHeader[];
  assignments: TomlAssignment[];
};

type ManagedTable = {
  path: string[];
  keys: string[];
};

type FileIdentity = {
  device: number;
  inode: number;
  mode: number;
};

type PathIdentity = FileIdentity & {
  path: string;
};

type CodexPathSnapshot = {
  absolutePath: string;
  ancestors: PathIdentity[];
  exists: boolean;
  leaf?: FileIdentity;
};

export type CodexApplyOptions = {
  dryRun?: boolean;
  /** Deterministic synthetic-proof seam; production callers must leave this unset. */
  transactionHooks?: {
    afterBackup?: () => void;
    beforeCommit?: () => void;
    afterCommit?: () => void;
  };
};

const MANAGED_TABLES: ManagedTable[] = [
  { path: ["otel"], keys: ["environment", "log_user_prompt"] },
  { path: ["otel", "exporter", "otlp-http"], keys: ["endpoint", "protocol", "headers"] },
  { path: ["otel", "trace_exporter", "otlp-http"], keys: ["endpoint", "protocol", "headers"] },
  { path: ["otel", "metrics_exporter", "otlp-http"], keys: ["endpoint", "protocol", "headers"] },
  { path: ["features"], keys: ["hooks"] },
];

const HOOK_EVENTS = ["UserPromptSubmit", "PostToolUse", "Stop"] as const;
const PLIMSOLL_HEADER = "x-plimsoll-source";
const LEGACY_PLIMSOLL_HEADER = "x-cfo-one-source";

function fileIdentity(stat: fs.Stats): FileIdentity {
  return { device: stat.dev, inode: stat.ino, mode: stat.mode };
}

function sameIdentity(left: FileIdentity, right: FileIdentity) {
  return left.device === right.device && left.inode === right.inode;
}

function unsafePath(file: string, detail: string): never {
  throw new Error(`${file}: ${detail}; refusing to read through a link or create a backup/write.`);
}

function lstat(file: string): fs.Stats | null {
  try {
    return fs.lstatSync(file);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

/**
 * Capture every writable path component. macOS root compatibility aliases
 * such as /var -> /private/var are root-owned and accepted; any symlink below
 * the filesystem root is an unsafe operator-controlled traversal.
 */
function inspectCodexPath(file: string): CodexPathSnapshot {
  const absolutePath = path.resolve(file);
  const parsed = path.parse(absolutePath);
  const segments = absolutePath.slice(parsed.root.length).split(path.sep).filter(Boolean);
  const ancestors: PathIdentity[] = [];
  let cursor = parsed.root;
  let missingAncestor = false;
  for (const segment of segments.slice(0, -1)) {
    cursor = path.join(cursor, segment);
    if (missingAncestor) continue;
    const stat = lstat(cursor);
    if (!stat) {
      missingAncestor = true;
      continue;
    }
    if (stat.isSymbolicLink()) {
      if (path.dirname(cursor) === parsed.root && stat.uid === 0) continue;
      unsafePath(file, `ancestor ${JSON.stringify(cursor)} is a symbolic link`);
    }
    if (!stat.isDirectory()) unsafePath(file, `ancestor ${JSON.stringify(cursor)} is not a directory`);
    ancestors.push({ path: cursor, ...fileIdentity(stat) });
  }

  const leafStat = lstat(absolutePath);
  if (!leafStat) return { absolutePath, ancestors, exists: false };
  if (missingAncestor) unsafePath(file, "an ancestor appeared while inspecting config.toml");
  if (leafStat.isSymbolicLink()) unsafePath(file, "config.toml is a symbolic link");
  if (!leafStat.isFile()) unsafePath(file, "config.toml is not a regular file");
  return { absolutePath, ancestors, exists: true, leaf: fileIdentity(leafStat) };
}

function sameAncestors(left: PathIdentity[], right: PathIdentity[]) {
  return left.length === right.length && left.every((entry, index) =>
    entry.path === right[index]?.path && sameIdentity(entry, right[index]!)
  );
}

function assertStableCodexPath(file: string, expected: CodexPathSnapshot) {
  const current = inspectCodexPath(file);
  if (current.absolutePath !== expected.absolutePath || current.exists !== expected.exists) {
    unsafePath(file, "config.toml path identity changed after planning");
  }
  if (!sameAncestors(current.ancestors, expected.ancestors)) {
    unsafePath(file, "a config.toml ancestor changed after planning");
  }
  if (expected.exists && (!current.leaf || !expected.leaf || !sameIdentity(current.leaf, expected.leaf))) {
    unsafePath(file, "config.toml was replaced after planning");
  }
}

function openNoFollow(file: string, flags: number) {
  try {
    return fs.openSync(file, flags | fs.constants.O_NOFOLLOW);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ELOOP" || code === "EMLINK") unsafePath(file, "config.toml became a symbolic link");
    throw error;
  }
}

function readCodexPreimage(file: string) {
  const snapshot = inspectCodexPath(file);
  if (!snapshot.exists) return { snapshot, current: "" };
  const descriptor = openNoFollow(snapshot.absolutePath, fs.constants.O_RDONLY);
  try {
    const opened = fileIdentity(fs.fstatSync(descriptor));
    if (!snapshot.leaf || !sameIdentity(opened, snapshot.leaf)) {
      unsafePath(file, "config.toml was replaced while opening it");
    }
    return { snapshot, current: fs.readFileSync(descriptor, "utf8") };
  } finally {
    fs.closeSync(descriptor);
  }
}

function readDescriptor(descriptor: number) {
  const chunks: Buffer[] = [];
  let position = 0;
  while (true) {
    const chunk = Buffer.allocUnsafe(64 * 1024);
    const bytesRead = fs.readSync(descriptor, chunk, 0, chunk.length, position);
    if (bytesRead === 0) break;
    chunks.push(chunk.subarray(0, bytesRead));
    position += bytesRead;
  }
  return Buffer.concat(chunks).toString("utf8");
}

function writeDescriptor(descriptor: number, content: string) {
  const bytes = Buffer.from(content);
  let offset = 0;
  while (offset < bytes.length) {
    offset += fs.writeSync(descriptor, bytes, offset, bytes.length - offset, offset);
  }
  fs.fsyncSync(descriptor);
}

function unlinkIfIdentity(file: string, identity: FileIdentity | undefined) {
  const leaf = lstat(file);
  if (identity && leaf && !leaf.isSymbolicLink() && sameIdentity(identity, fileIdentity(leaf))) {
    fs.unlinkSync(file);
  }
}

function assertBoundPreimage(
  file: string,
  descriptor: number,
  expectedIdentity: FileIdentity,
  expectedContent: string,
) {
  const beforeRead = fileIdentity(fs.fstatSync(descriptor));
  if (!sameIdentity(beforeRead, expectedIdentity)) {
    unsafePath(file, "bound config.toml identity changed before commit");
  }
  const content = readDescriptor(descriptor);
  const afterRead = fileIdentity(fs.fstatSync(descriptor));
  if (!sameIdentity(afterRead, expectedIdentity) || content !== expectedContent) {
    unsafePath(file, "bound config.toml content changed before commit");
  }
}

function assertVisiblePreimage(
  file: string,
  snapshot: CodexPathSnapshot,
  expectedContent: string,
) {
  assertStableCodexPath(file, snapshot);
  if (!snapshot.leaf) unsafePath(file, "config.toml preimage identity is missing");
  const descriptor = openNoFollow(snapshot.absolutePath, fs.constants.O_RDONLY);
  try {
    assertBoundPreimage(file, descriptor, snapshot.leaf, expectedContent);
  } finally {
    fs.closeSync(descriptor);
  }
  assertStableCodexPath(file, snapshot);
}

function assertVisibleCommit(
  file: string,
  snapshot: CodexPathSnapshot,
  committedIdentity: FileIdentity,
  expectedContent: string,
) {
  const visible = inspectCodexPath(file);
  if (
    !visible.exists ||
    !visible.leaf ||
    !sameAncestors(visible.ancestors, snapshot.ancestors) ||
    !sameIdentity(visible.leaf, committedIdentity)
  ) {
    unsafePath(file, "visible config.toml identity does not match the committed plan");
  }
  const descriptor = openNoFollow(visible.absolutePath, fs.constants.O_RDONLY);
  try {
    assertBoundPreimage(file, descriptor, committedIdentity, expectedContent);
  } finally {
    fs.closeSync(descriptor);
  }
  const afterRead = inspectCodexPath(file);
  if (
    !afterRead.exists ||
    !afterRead.leaf ||
    !sameAncestors(afterRead.ancestors, snapshot.ancestors) ||
    !sameIdentity(afterRead.leaf, committedIdentity)
  ) {
    unsafePath(file, "visible config.toml changed while verifying the committed plan");
  }
}

function fsyncParentDirectory(file: string) {
  const descriptor = fs.openSync(path.dirname(path.resolve(file)), fs.constants.O_RDONLY);
  try {
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
}

function prepareCodexTemp(file: string, snapshot: CodexPathSnapshot, next: string) {
  const directory = path.dirname(snapshot.absolutePath);
  const tempPath = path.join(
    directory,
    `.${path.basename(snapshot.absolutePath)}.plimsoll-tmp-${randomUUID()}`,
  );
  const descriptor = openNoFollow(
    tempPath,
    fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL,
  );
  const identity = fileIdentity(fs.fstatSync(descriptor));
  try {
    fs.fchmodSync(descriptor, (snapshot.leaf?.mode ?? 0o600) & 0o777);
    writeDescriptor(descriptor, next);
    return { tempPath, identity };
  } catch (error) {
    unlinkIfIdentity(tempPath, identity);
    throw error;
  } finally {
    fs.closeSync(descriptor);
  }
}

function backupCodexPreimage(
  file: string,
  snapshot: CodexPathSnapshot,
  current: string,
  boundDescriptor: number,
) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const backupPath = `${file}.plimsoll-backup-${stamp}`;
  const absoluteBackupPath = path.resolve(backupPath);
  const descriptor = openNoFollow(
    absoluteBackupPath,
    fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL,
  );
  const created = fileIdentity(fs.fstatSync(descriptor));
  try {
    if (!snapshot.leaf) unsafePath(file, "config.toml preimage identity is missing before backup");
    assertBoundPreimage(file, boundDescriptor, snapshot.leaf, current);
    assertVisiblePreimage(file, snapshot, current);
    fs.fchmodSync(descriptor, (snapshot.leaf?.mode ?? 0o600) & 0o777);
    writeDescriptor(descriptor, current);
    assertBoundPreimage(file, boundDescriptor, snapshot.leaf, current);
    assertVisiblePreimage(file, snapshot, current);
  } catch (error) {
    unlinkIfIdentity(absoluteBackupPath, created);
    throw error;
  } finally {
    fs.closeSync(descriptor);
  }
  return backupPath;
}

function writeCodexPlan(
  file: string,
  snapshot: CodexPathSnapshot,
  current: string,
  next: string,
  hooks: NonNullable<CodexApplyOptions["transactionHooks"]> = {},
) {
  assertStableCodexPath(file, snapshot);
  const boundDescriptor = snapshot.exists
    ? openNoFollow(snapshot.absolutePath, fs.constants.O_RDONLY)
    : undefined;
  let temp: ReturnType<typeof prepareCodexTemp> | undefined;
  try {
    if (snapshot.exists) {
      if (boundDescriptor === undefined || !snapshot.leaf) {
        unsafePath(file, "config.toml preimage could not be bound before commit");
      }
      assertBoundPreimage(file, boundDescriptor, snapshot.leaf, current);
      assertVisiblePreimage(file, snapshot, current);
    }

    temp = prepareCodexTemp(file, snapshot, next);
    assertStableCodexPath(file, snapshot);

    const backupPath = snapshot.exists
      ? backupCodexPreimage(file, snapshot, current, boundDescriptor!)
      : undefined;
    if (backupPath) fsyncParentDirectory(file);
    hooks.afterBackup?.();

    if (snapshot.exists) {
      assertBoundPreimage(file, boundDescriptor!, snapshot.leaf!, current);
      assertVisiblePreimage(file, snapshot, current);
    } else {
      assertStableCodexPath(file, snapshot);
    }

    hooks.beforeCommit?.();
    if (snapshot.exists) {
      assertBoundPreimage(file, boundDescriptor!, snapshot.leaf!, current);
      assertVisiblePreimage(file, snapshot, current);
      fs.renameSync(temp.tempPath, snapshot.absolutePath);
    } else {
      assertStableCodexPath(file, snapshot);
      fs.linkSync(temp.tempPath, snapshot.absolutePath);
      fs.unlinkSync(temp.tempPath);
    }

    hooks.afterCommit?.();
    assertVisibleCommit(file, snapshot, temp.identity, next);
    fsyncParentDirectory(file);
    assertVisibleCommit(file, snapshot, temp.identity, next);
    return backupPath;
  } finally {
    if (boundDescriptor !== undefined) fs.closeSync(boundDescriptor);
    if (temp) unlinkIfIdentity(temp.tempPath, temp.identity);
  }
}

function isRecord(value: unknown): value is TomlRecord {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function samePath(left: string[], right: string[]) {
  return left.length === right.length && left.every((part, index) => part === right[index]);
}

function displayPath(parts: string[]) {
  return parts.map((part) => (/^[A-Za-z0-9_-]+$/.test(part) ? part : JSON.stringify(part))).join(".");
}

function getPath(root: unknown, parts: string[]): unknown {
  let current = root;
  for (const part of parts) {
    if (!isRecord(current) || !Object.hasOwn(current, part)) return undefined;
    current = current[part];
  }
  return current;
}

function containsExpected(actual: unknown, expected: unknown): boolean {
  if (Array.isArray(expected)) {
    return Array.isArray(actual) && expected.every((expectedEntry) =>
      actual.some((actualEntry) => containsExpected(actualEntry, expectedEntry))
    );
  }
  if (isRecord(expected)) {
    return isRecord(actual) && Object.entries(expected).every(([key, value]) =>
      Object.hasOwn(actual, key) && containsExpected(actual[key], value)
    );
  }
  return isDeepStrictEqual(actual, expected);
}

function parseDocument(file: string, source: string, generated = false): TomlRecord {
  try {
    const parsed = parseToml(source);
    if (!isRecord(parsed)) throw new Error("root is not a table");
    return parsed;
  } catch {
    const subject = generated ? "generated Codex TOML" : "existing Codex config.toml";
    throw new Error(`${file}: ${subject} is invalid; refusing to write or create a backup.`);
  }
}

function commentStart(line: string) {
  let quote: "single" | "double" | null = null;
  let escaped = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (quote === "double") {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') quote = null;
      continue;
    }
    if (quote === "single") {
      if (char === "'") quote = null;
      continue;
    }
    if (char === '"') quote = "double";
    else if (char === "'") quote = "single";
    else if (char === "#") return index;
  }
  return line.length;
}

function topLevelEquals(value: string) {
  let quote: "single" | "double" | null = null;
  let escaped = false;
  let braces = 0;
  let brackets = 0;
  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];
    if (quote === "double") {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') quote = null;
      continue;
    }
    if (quote === "single") {
      if (char === "'") quote = null;
      continue;
    }
    if (char === '"') quote = "double";
    else if (char === "'") quote = "single";
    else if (char === "{") braces += 1;
    else if (char === "}") braces -= 1;
    else if (char === "[") brackets += 1;
    else if (char === "]") brackets -= 1;
    else if (char === "=" && braces === 0 && brackets === 0) return index;
  }
  return -1;
}

function parseDottedKey(value: string): string[] | null {
  const parts: string[] = [];
  let index = 0;
  const whitespace = () => {
    while (index < value.length && /\s/.test(value[index]!)) index += 1;
  };
  whitespace();
  while (index < value.length) {
    let part = "";
    if (value[index] === '"') {
      const start = index;
      index += 1;
      let escaped = false;
      while (index < value.length) {
        const char = value[index++]!;
        if (escaped) escaped = false;
        else if (char === "\\") escaped = true;
        else if (char === '"') break;
      }
      if (value[index - 1] !== '"') return null;
      try {
        part = JSON.parse(value.slice(start, index)) as string;
      } catch {
        return null;
      }
    } else if (value[index] === "'") {
      index += 1;
      const end = value.indexOf("'", index);
      if (end === -1) return null;
      part = value.slice(index, end);
      index = end + 1;
    } else {
      const match = value.slice(index).match(/^[A-Za-z0-9_-]+/);
      if (!match) return null;
      part = match[0];
      index += part.length;
    }
    parts.push(part);
    whitespace();
    if (index === value.length) return parts;
    if (value[index] !== ".") return null;
    index += 1;
    whitespace();
  }
  return parts.length > 0 ? parts : null;
}

function scanToml(lines: string[]): TomlScan {
  const headers: TomlHeader[] = [];
  const assignments: TomlAssignment[] = [];
  let tableKind: "table" | "array" | "root" = "root";
  let tablePath: string[] = [];

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index]!;
    const end = commentStart(line);
    const content = line.slice(0, end).trim();
    if (!content) continue;
    const arrayHeader = content.startsWith("[[") && content.endsWith("]]" );
    const tableHeader = !arrayHeader && content.startsWith("[") && content.endsWith("]");
    if (arrayHeader || tableHeader) {
      const inner = content.slice(arrayHeader ? 2 : 1, arrayHeader ? -2 : -1);
      const parsed = parseDottedKey(inner);
      if (parsed) {
        tableKind = arrayHeader ? "array" : "table";
        tablePath = parsed;
        headers.push({ index, kind: tableKind, path: parsed });
      }
      continue;
    }
    const equals = topLevelEquals(line.slice(0, end));
    if (equals === -1) continue;
    const keyPath = parseDottedKey(line.slice(0, equals).trim());
    if (!keyPath) continue;
    let valueStart = equals + 1;
    while (valueStart < end && /\s/.test(line[valueStart]!)) valueStart += 1;
    let valueEnd = end;
    while (valueEnd > valueStart && /\s/.test(line[valueEnd - 1]!)) valueEnd -= 1;
    assignments.push({
      index,
      tableKind,
      tablePath: [...tablePath],
      keyPath,
      valueStart,
      valueEnd,
      valueRaw: line.slice(valueStart, valueEnd),
    });
  }
  return { headers, assignments };
}

function detectLineEnding(file: string, source: string) {
  const withoutCrLf = source.replace(/\r\n/g, "");
  if (withoutCrLf.includes("\r") || (source.includes("\r\n") && withoutCrLf.includes("\n"))) {
    throw new Error(`${file}: existing Codex config.toml has mixed or unsupported line endings; refusing to write or create a backup.`);
  }
  return source.includes("\r\n") ? "\r\n" : "\n";
}

function appendBlock(lines: string[], block: string[]) {
  if (lines.length === 1 && lines[0] === "") {
    lines.splice(0, 1, ...block, "");
    return;
  }
  if (lines.at(-1) !== "") lines.push("");
  if (lines.length > 1 && lines.at(-2) !== "") lines.push("");
  lines.push(...block, "");
}

function generatedAssignment(
  file: string,
  generatedLines: string[],
  tablePath: string[],
  key: string,
) {
  const matches = scanToml(generatedLines).assignments.filter((entry) =>
    entry.tableKind === "table" &&
    samePath(entry.tablePath, tablePath) &&
    entry.keyPath.length === 1 &&
    entry.keyPath[0] === key
  );
  if (matches.length !== 1) {
    throw new Error(`${file}: generated Codex TOML has an unsupported ${displayPath([...tablePath, key])} layout.`);
  }
  return matches[0]!;
}

function splitInlineTable(value: string): string[] | null {
  const trimmed = value.trim();
  if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) return null;
  const inner = trimmed.slice(1, -1);
  const entries: string[] = [];
  let start = 0;
  let quote: "single" | "double" | null = null;
  let escaped = false;
  let braces = 0;
  let brackets = 0;
  for (let index = 0; index < inner.length; index += 1) {
    const char = inner[index];
    if (quote === "double") {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') quote = null;
      continue;
    }
    if (quote === "single") {
      if (char === "'") quote = null;
      continue;
    }
    if (char === '"') quote = "double";
    else if (char === "'") quote = "single";
    else if (char === "{") braces += 1;
    else if (char === "}") braces -= 1;
    else if (char === "[") brackets += 1;
    else if (char === "]") brackets -= 1;
    else if (char === "," && braces === 0 && brackets === 0) {
      entries.push(inner.slice(start, index).trim());
      start = index + 1;
    }
  }
  const tail = inner.slice(start).trim();
  if (tail) entries.push(tail);
  if (entries.some((entry) => !entry || topLevelEquals(entry) === -1)) return null;
  return entries;
}

function reconcileHeaders(file: string, raw: string) {
  const entries = splitInlineTable(raw);
  if (!entries) {
    throw new Error(`${file}: managed exporter headers use an unsupported layout; refusing to write or create a backup.`);
  }
  const kept: string[] = [];
  let foundPlimsoll = false;
  let removedLegacy = false;
  for (const entry of entries) {
    const equals = topLevelEquals(entry);
    const key = equals === -1 ? null : parseDottedKey(entry.slice(0, equals).trim());
    if (!key || key.length !== 1) {
      kept.push(entry);
      continue;
    }
    const header = key[0]!;
    if (!/^[\x00-\x7f]+$/.test(header)) {
      throw new Error(
        `${file}: managed exporter headers contain non-ASCII header name ${JSON.stringify(header)}; ` +
        "refusing to write or create a backup.",
      );
    }
    const folded = header.toLowerCase();
    if (folded === LEGACY_PLIMSOLL_HEADER) {
      removedLegacy = true;
      continue;
    }
    if (folded === PLIMSOLL_HEADER) {
      foundPlimsoll = true;
      continue;
    }
    kept.push(entry);
  }
  kept.push(`"${PLIMSOLL_HEADER}" = "codex"`);
  return {
    value: `{ ${kept.join(", ")} }`,
    action: removedLegacy
      ? `replace legacy x-cfo-one-source with ${PLIMSOLL_HEADER}`
      : foundPlimsoll
        ? `update ${PLIMSOLL_HEADER}`
        : `add ${PLIMSOLL_HEADER}`,
  };
}

function headersNeedReconciliation(headers: TomlRecord) {
  let canonicalCount = 0;
  for (const [header, value] of Object.entries(headers)) {
    if (!/^[\x00-\x7f]+$/.test(header)) return true;
    const folded = header.toLowerCase();
    if (folded === LEGACY_PLIMSOLL_HEADER) return true;
    if (folded !== PLIMSOLL_HEADER) continue;
    canonicalCount += 1;
    if (header !== PLIMSOLL_HEADER || value !== "codex") return true;
  }
  return canonicalCount !== 1;
}

function hasOwnedHeaderDrift(document: TomlRecord) {
  return MANAGED_TABLES.some((table) => {
    if (!table.keys.includes("headers")) return false;
    const headers = getPath(document, [...table.path, "headers"]);
    return isRecord(headers) && headersNeedReconciliation(headers);
  });
}

function hookCommands(value: unknown): string[] {
  if (Array.isArray(value)) return value.flatMap((entry) => hookCommands(entry));
  if (!isRecord(value)) return [];
  return Object.entries(value).flatMap(([key, entry]) =>
    key === "command" && typeof entry === "string" ? [entry] : hookCommands(entry)
  );
}

function isPlimsollHookPath(command: string) {
  return command.normalize("NFKC").toLowerCase().includes("/hooks/codex");
}

function hookOwnershipDrift(currentEntries: unknown, expectedEntries: unknown) {
  if (!Array.isArray(expectedEntries) || expectedEntries.length !== 1) return true;
  const expectedOwnedCommands = hookCommands(expectedEntries).filter(isPlimsollHookPath);
  const currentOwnedCommands = hookCommands(currentEntries).filter(isPlimsollHookPath);
  return expectedOwnedCommands.length !== 1 ||
    currentOwnedCommands.length !== 1 ||
    currentOwnedCommands[0] !== expectedOwnedCommands[0] ||
    !containsExpected(currentEntries, expectedEntries);
}

function hasOwnedHookDrift(document: TomlRecord, expected: TomlRecord) {
  return HOOK_EVENTS.some((event) =>
    hookOwnershipDrift(
      getPath(document, ["hooks", event]),
      getPath(expected, ["hooks", event]),
    )
  );
}

function generatedHookBlock(file: string, generatedLines: string[], event: string) {
  const headers = scanToml(generatedLines).headers;
  const startHeader = headers.find((entry) =>
    entry.kind === "array" && samePath(entry.path, ["hooks", event])
  );
  if (!startHeader) throw new Error(`${file}: generated Codex TOML is missing hooks.${event}.`);
  const nextEvent = headers.find((entry) =>
    entry.index > startHeader.index &&
    entry.kind === "array" &&
    entry.path.length === 2 &&
    entry.path[0] === "hooks"
  );
  let end = nextEvent?.index ?? generatedLines.length;
  while (end > startHeader.index && generatedLines[end - 1]?.trim() === "") end -= 1;
  return generatedLines.slice(startHeader.index, end);
}

function reconcileCodexToml(file: string, current: string, generatedToml: string) {
  const document = parseDocument(file, current);
  const expected = parseDocument(file, generatedToml, true);
  if (
    containsExpected(document, expected) &&
    !hasOwnedHeaderDrift(document) &&
    !hasOwnedHookDrift(document, expected)
  ) {
    return { next: current, changes: [] as string[] };
  }

  const expectedEndpointPaths = MANAGED_TABLES
    .filter((table) => table.keys.includes("endpoint"))
    .map((table) => [...table.path, "endpoint"]);
  const hasExpectedEndpoint = expectedEndpointPaths.some((parts) =>
    isDeepStrictEqual(getPath(document, parts), getPath(expected, parts))
  );
  if (getPath(document, ["otel"]) !== undefined && !hasExpectedEndpoint) {
    return {
      next: current,
      changes: [] as string[],
      conflict:
        "config.toml already has an [otel] configuration without this Plimsoll collector endpoint — not touching it. " +
        "Remove or repoint it manually, then re-run.",
    };
  }

  const lineEnding = detectLineEnding(file, current);
  const lines = current.split(/\r?\n/);
  const generatedLines = generatedToml.split("\n");
  const changes: string[] = [];

  for (const table of MANAGED_TABLES) {
    const scan = scanToml(lines);
    const headers = scan.headers.filter((entry) => entry.kind === "table" && samePath(entry.path, table.path));
    if (headers.length > 1) {
      throw new Error(`${file}: ${displayPath(table.path)} is declared more than once; refusing to write or create a backup.`);
    }
    const desired = table.keys.map((key) => ({
      key,
      value: getPath(expected, [...table.path, key]),
      generated: generatedAssignment(file, generatedLines, table.path, key),
    }));

    if (headers.length === 0) {
      const represented = desired.filter(({ key }) => getPath(document, [...table.path, key]) !== undefined);
      if (represented.length > 0) {
        throw new Error(
          `${file}: ${displayPath(table.path)} uses dotted, inline, or implicit managed keys without a writable table; ` +
          "refusing to write or create a backup.",
        );
      }
      appendBlock(lines, [
        `[${table.path.map((part) => (/^[A-Za-z0-9_-]+$/.test(part) ? part : JSON.stringify(part))).join(".")}]`,
        ...desired.map(({ key, generated }) => `${key} = ${generated.valueRaw}`),
      ]);
      for (const { key } of desired) changes.push(`${displayPath([...table.path, key])} + generated Plimsoll value`);
      continue;
    }

    const header = headers[0]!;
    const nextHeader = scan.headers.find((entry) => entry.index > header.index);
    const sectionEnd = nextHeader?.index ?? lines.length;
    const assignments = scan.assignments.filter((entry) =>
      entry.index > header.index &&
      entry.index < sectionEnd &&
      entry.tableKind === "table" &&
      samePath(entry.tablePath, table.path) &&
      entry.keyPath.length === 1
    );
    const additions: string[] = [];
    for (const { key, value, generated } of desired) {
      const currentValue = getPath(document, [...table.path, key]);
      const assignment = assignments.find((entry) => entry.keyPath[0] === key);
      const ownedHeaderDrift = key === "headers" && isRecord(currentValue) &&
        headersNeedReconciliation(currentValue);
      if (isDeepStrictEqual(currentValue, value) && !ownedHeaderDrift) continue;
      if (currentValue !== undefined && !assignment) {
        throw new Error(
          `${file}: ${displayPath([...table.path, key])} uses an unsupported dotted or inline layout; ` +
          "refusing to write or create a backup.",
        );
      }
      if (!assignment) {
        additions.push(`${key} = ${generated.valueRaw}`);
        changes.push(`${displayPath([...table.path, key])} + generated Plimsoll value`);
        continue;
      }

      let nextValue = generated.valueRaw;
      let action = "replace with generated Plimsoll value";
      if (key === "headers" && isRecord(currentValue)) {
        const reconciled = reconcileHeaders(file, assignment.valueRaw);
        nextValue = reconciled.value;
        action = reconciled.action;
      }
      const line = lines[assignment.index]!;
      lines[assignment.index] = `${line.slice(0, assignment.valueStart)}${nextValue}${line.slice(assignment.valueEnd)}`;
      changes.push(`${displayPath([...table.path, key])} ${action}`);
    }
    if (additions.length > 0) {
      const insertion = assignments.length > 0
        ? Math.max(...assignments.map((entry) => entry.index)) + 1
        : header.index + 1;
      lines.splice(insertion, 0, ...additions);
    }
  }

  const hookBlocks: string[] = [];
  for (const event of HOOK_EVENTS) {
    const expectedEntries = getPath(expected, ["hooks", event]);
    const currentEntries = getPath(document, ["hooks", event]);
    if (!Array.isArray(expectedEntries) || expectedEntries.length !== 1) {
      throw new Error(`${file}: generated Codex TOML has an unsupported hooks.${event} layout.`);
    }
    const expectedOwnedCommands = hookCommands(expectedEntries).filter(isPlimsollHookPath);
    if (expectedOwnedCommands.length !== 1) {
      throw new Error(`${file}: generated Codex TOML has an unsupported hooks.${event} command layout.`);
    }
    const currentOwnedCommands = hookCommands(currentEntries).filter(isPlimsollHookPath);
    const containsCanonicalEntry = containsExpected(currentEntries, expectedEntries);
    if (currentOwnedCommands.length > 0 && hookOwnershipDrift(currentEntries, expectedEntries)) {
      throw new Error(
        `${file}: hooks.${event} already contains a different Plimsoll Codex hook or a non-canonical owned alias; ` +
        "refusing to write or create a backup.",
      );
    }
    if (containsCanonicalEntry) continue;
    hookBlocks.push(...generatedHookBlock(file, generatedLines, event), "");
    changes.push(`hooks.${event} + generated Plimsoll command hook`);
  }
  if (hookBlocks.length > 0) {
    const hasHooksTable = scanToml(lines).headers.some((entry) =>
      entry.kind === "table" && samePath(entry.path, ["hooks"])
    );
    appendBlock(lines, [...(hasHooksTable ? [] : ["[hooks]", ""]), ...hookBlocks].slice(0, -1));
  }

  const next = lines.join(lineEnding);
  let reconciled: TomlRecord;
  try {
    reconciled = parseToml(next) as TomlRecord;
  } catch {
    throw new Error(
      `${file}: managed Codex fields use an ambiguous layout that cannot be reconciled without duplicate TOML keys or tables; ` +
      "refusing to write or create a backup.",
    );
  }
  if (
    !containsExpected(reconciled, expected) ||
    hasOwnedHeaderDrift(reconciled) ||
    hasOwnedHookDrift(reconciled, expected)
  ) {
    throw new Error(`${file}: Codex reconciliation did not produce the complete generated subset; refusing to write or create a backup.`);
  }
  return { next, changes };
}

/** Reconcile Plimsoll's generated subset into an existing Codex config.toml. */
export function applyCodexConfig(
  file: string,
  generatedToml: string,
  options: CodexApplyOptions = {},
): ApplyResult {
  const { snapshot, current } = readCodexPreimage(file);
  const plan = reconcileCodexToml(file, current, generatedToml);
  if (plan.conflict) {
    return { path: file, changed: false, changes: [], conflict: plan.conflict };
  }
  const changes = plan.changes;
  if (changes.length === 0) return { path: file, changed: false, changes: [] };
  if (options.dryRun) {
    return { path: file, changed: true, changes };
  }
  const backupPath = writeCodexPlan(file, snapshot, current, plan.next, options.transactionHooks);
  return { path: file, changed: true, changes, backupPath };
}
