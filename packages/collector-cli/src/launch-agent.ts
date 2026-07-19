import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";

import { collectorHome } from "./config";

export const LAUNCH_AGENT_LABEL = "com.plimsoll.collector";
export const LAUNCH_AGENT_SYSTEM_PATHS = [
  "/opt/homebrew/bin",
  "/usr/local/bin",
  "/usr/bin",
  "/bin",
  "/usr/sbin",
  "/sbin",
] as const;

const MANIFEST_MODE = 0o600;
const DIRECTORY_MODE = 0o700;
const PERMISSION_MODE_MASK = 0o7777;
const SPECIAL_PERMISSION_MASK = 0o7000;
const MAX_MANIFEST_BYTES = 128 * 1024;
const XML_PREAMBLE = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">`;

export type LaunchAgentTransactionHooks = {
  /** Deterministic hostile-proof seams. Production callers leave these unset. */
  afterParentCreate?: () => void;
  afterPrepare?: () => void;
  afterRollback?: () => void;
  beforeCommit?: () => void;
  afterCommit?: () => void;
};

export type LaunchAgentOptions = {
  homeDir?: string;
  repoRoot: string;
  pnpmPath?: string;
  label?: string;
  /** Packaged installs exec the bundled CLI directly. */
  programArguments?: string[];
  restartThrottleSeconds?: number;
  workingDirectory?: string;
  dryRun?: boolean;
  transactionHooks?: LaunchAgentTransactionHooks;
};

export type LaunchAgentInstallReceipt = {
  schema: "plimsoll.launch-agent-install.v1";
  operation: "install";
  status: "preview" | "installed" | "unchanged";
  target: "user_launch_agent";
  label: typeof LAUNCH_AGENT_LABEL;
  changed: boolean;
  wouldChange: boolean;
  manifestDigest: string;
  manifestMode: "0600";
  privacyMode: "metadata_only";
  environmentKeys: readonly ["PATH", "PLIMSOLL_COLLECTOR_DATA_MODE"];
  rollback: null | {
    available: true;
    preimageDigest: string;
    preimageMode: string;
    preimagePathHash: string;
    receiptPathHash: string;
  };
};

export type LaunchAgentInstallResult = {
  plistPath: string;
  receipt: LaunchAgentInstallReceipt;
  rollbackFiles?: {
    preimagePath: string;
    receiptPath: string;
  };
};

export type LaunchAgentUninstallReceipt = {
  schema: "plimsoll.launch-agent-uninstall.v1";
  operation: "uninstall";
  status: "preview" | "removed" | "absent";
  target: "user_launch_agent";
  label: typeof LAUNCH_AGENT_LABEL;
  changed: boolean;
  wouldChange: boolean;
  removedManifestDigest: string | null;
};

export class LaunchAgentTransactionError extends Error {
  constructor(readonly code: string) {
    super(`LAUNCH_AGENT_${code}`);
    this.name = "LaunchAgentTransactionError";
  }
}

type FileIdentity = {
  device: number;
  inode: number;
  mode: number;
  links: number;
  uid: number;
  gid: number;
  size: number;
  modifiedMs: number;
  changedMs: number;
};

type PathIdentity = FileIdentity & { path: string };
type PathSnapshot = {
  absolutePath: string;
  ancestors: PathIdentity[];
  exists: boolean;
  leaf?: FileIdentity;
};

type PlistValue = string | number | boolean | PlistValue[] | { [key: string]: PlistValue };

function fail(code: string): never {
  throw new LaunchAgentTransactionError(code);
}

function runHook(hook: (() => void) | undefined) {
  if (!hook) return;
  try {
    hook();
  } catch {
    fail("TRANSACTION_ABORTED");
  }
}

function digest(value: string | Buffer) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function pathDigest(value: string) {
  return digest(path.resolve(value));
}

function identityDigest(value: FileIdentity) {
  return digest(JSON.stringify([
    value.device,
    value.inode,
    value.mode,
    value.links,
    value.uid,
    value.gid,
    value.size,
    value.modifiedMs,
    value.changedMs,
  ]));
}

function currentUid() {
  return typeof process.getuid === "function" ? process.getuid() : undefined;
}

function lstat(file: string): fs.Stats | null {
  try {
    return fs.lstatSync(file);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

function identity(stat: fs.Stats): FileIdentity {
  return {
    device: stat.dev,
    inode: stat.ino,
    mode: stat.mode,
    links: stat.nlink,
    uid: stat.uid,
    gid: stat.gid,
    size: stat.size,
    modifiedMs: stat.mtimeMs,
    changedMs: stat.ctimeMs,
  };
}

function sameIdentity(left: FileIdentity, right: FileIdentity) {
  return left.device === right.device &&
    left.inode === right.inode &&
    left.mode === right.mode &&
    left.links === right.links &&
    left.uid === right.uid &&
    left.gid === right.gid &&
    left.size === right.size &&
    left.modifiedMs === right.modifiedMs &&
    left.changedMs === right.changedMs;
}

function sameObject(left: FileIdentity, right: FileIdentity) {
  return left.device === right.device && left.inode === right.inode;
}

function permissionMode(mode: number) {
  return mode & PERMISSION_MODE_MASK;
}

function formatPermissionMode(mode: number) {
  return permissionMode(mode).toString(8).padStart(4, "0");
}

function assertExactPrivateMode(file: FileIdentity, code: string) {
  if (permissionMode(file.mode) !== MANIFEST_MODE) fail(code);
}

function assertSafeAncestor(stat: fs.Stats, finalParent = false) {
  if (stat.isSymbolicLink()) fail("UNSAFE_ANCESTOR_SYMLINK");
  if (!stat.isDirectory()) fail("UNSAFE_ANCESTOR_TYPE");
  const uid = currentUid();
  if (uid !== undefined && stat.uid !== uid && stat.uid !== 0) fail("UNSAFE_ANCESTOR_OWNER");
  if (finalParent && uid !== undefined && stat.uid !== uid) fail("UNSAFE_PARENT_OWNER");
  const permissions = stat.mode & 0o7777;
  const trustedStickyRoot = stat.uid === 0 && (permissions & 0o1000) !== 0;
  if ((permissions & 0o022) !== 0 && !trustedStickyRoot) fail("UNSAFE_ANCESTOR_MODE");
}

function assertSafeLeaf(stat: fs.Stats) {
  if (stat.isSymbolicLink()) fail("UNSAFE_LEAF_SYMLINK");
  if (!stat.isFile()) fail("UNSAFE_LEAF_TYPE");
  if (stat.nlink !== 1) fail("UNSAFE_LEAF_LINK_COUNT");
  if (
    (stat.mode & SPECIAL_PERMISSION_MASK) !== 0 ||
    (stat.mode & 0o111) !== 0 ||
    (stat.mode & 0o022) !== 0
  ) fail("UNSAFE_LEAF_MODE");
  const uid = currentUid();
  if (uid !== undefined && stat.uid !== uid) fail("UNSAFE_LEAF_OWNER");
}

function inspectPath(file: string): PathSnapshot {
  const absolutePath = path.resolve(file);
  const parsed = path.parse(absolutePath);
  const segments = absolutePath.slice(parsed.root.length).split(path.sep).filter(Boolean);
  const ancestors: PathIdentity[] = [];
  let cursor = parsed.root;
  let missing = false;
  for (const segment of segments.slice(0, -1)) {
    cursor = path.join(cursor, segment);
    if (missing) continue;
    const stat = lstat(cursor);
    if (!stat) {
      missing = true;
      continue;
    }
    if (stat.isSymbolicLink()) {
      // macOS exposes root-owned top-level compatibility aliases such as /var.
      if (path.dirname(cursor) === parsed.root && stat.uid === 0) continue;
      fail("UNSAFE_ANCESTOR_SYMLINK");
    }
    assertSafeAncestor(stat, cursor === path.dirname(absolutePath));
    ancestors.push({ path: cursor, ...identity(stat) });
  }
  const leaf = lstat(absolutePath);
  if (!leaf) return { absolutePath, ancestors, exists: false };
  if (missing) fail("PATH_CHANGED");
  assertSafeLeaf(leaf);
  return { absolutePath, ancestors, exists: true, leaf: identity(leaf) };
}

function sameAncestors(left: PathIdentity[], right: PathIdentity[]) {
  return left.length === right.length && left.every((entry, index) => {
    const other = right[index];
    return Boolean(
      other &&
      entry.path === other.path &&
      entry.device === other.device &&
      entry.inode === other.inode &&
      entry.mode === other.mode &&
      entry.uid === other.uid &&
      entry.gid === other.gid,
    );
  });
}

function assertStablePath(snapshot: PathSnapshot) {
  const current = inspectPath(snapshot.absolutePath);
  if (current.exists !== snapshot.exists || !sameAncestors(current.ancestors, snapshot.ancestors)) {
    fail("ANCESTOR_CHANGED");
  }
  if (snapshot.exists && (!snapshot.leaf || !current.leaf || !sameIdentity(snapshot.leaf, current.leaf))) {
    fail("LEAF_CHANGED");
  }
}

function openNoFollow(file: string, flags: number, mode?: number) {
  try {
    return fs.openSync(file, flags | fs.constants.O_NOFOLLOW, mode);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ELOOP" || code === "EMLINK") fail("UNSAFE_SYMLINK");
    throw error;
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
  return Buffer.concat(chunks);
}

function writeDescriptor(descriptor: number, content: Buffer) {
  let offset = 0;
  while (offset < content.length) {
    offset += fs.writeSync(descriptor, content, offset, content.length - offset, offset);
  }
  fs.fsyncSync(descriptor);
}

function fsyncDirectory(directory: string) {
  const descriptor = openNoFollow(directory, fs.constants.O_RDONLY);
  try {
    const stat = fs.fstatSync(descriptor);
    assertSafeAncestor(stat, true);
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
}

function ensureHomeRoot(homeDir: string) {
  const resolved = path.resolve(homeDir);
  const stat = lstat(resolved);
  if (!stat || stat.isSymbolicLink() || !stat.isDirectory()) fail("UNSAFE_HOME");
  const uid = currentUid();
  if (uid !== undefined && stat.uid !== uid) fail("UNSAFE_HOME_OWNER");
  if ((stat.mode & 0o022) !== 0) fail("UNSAFE_HOME_MODE");
  return resolved;
}

function ensureParent(initial: PathSnapshot, homeDir: string, hook: (() => void) | undefined) {
  const home = ensureHomeRoot(homeDir);
  const directory = path.dirname(initial.absolutePath);
  if (directory !== path.join(home, "Library", "LaunchAgents")) fail("UNSAFE_TARGET_PATH");
  const relative = path.relative(home, directory).split(path.sep).filter(Boolean);
  let cursor = home;
  for (const segment of relative) {
    const parent = cursor;
    cursor = path.join(cursor, segment);
    const before = lstat(cursor);
    if (!before) {
      try {
        fs.mkdirSync(cursor, { mode: DIRECTORY_MODE });
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      }
      const created = lstat(cursor);
      if (!created || created.isSymbolicLink() || !created.isDirectory()) fail("PARENT_CREATE_RACE");
      const uid = currentUid();
      if (permissionMode(created.mode) !== DIRECTORY_MODE || (uid !== undefined && created.uid !== uid)) {
        fail("UNSAFE_PARENT_MODE");
      }
      fsyncDirectory(parent);
    } else {
      assertSafeAncestor(before, cursor === directory);
    }
  }
  const ready = inspectPath(initial.absolutePath);
  if (ready.exists !== initial.exists) fail("PATH_CHANGED");
  for (const ancestor of initial.ancestors) {
    const current = ready.ancestors.find((entry) => entry.path === ancestor.path);
    if (!current || !sameObject(current, ancestor) || current.mode !== ancestor.mode || current.uid !== ancestor.uid) {
      fail("ANCESTOR_CHANGED");
    }
  }
  runHook(hook);
  const after = inspectPath(initial.absolutePath);
  if (
    after.exists !== ready.exists ||
    !sameAncestors(after.ancestors, ready.ancestors) ||
    (ready.exists && (!ready.leaf || !after.leaf || !sameIdentity(ready.leaf, after.leaf)))
  ) {
    fail("ANCESTOR_CHANGED");
  }
  return after;
}

function unlinkIfIdentity(file: string, expected: FileIdentity | undefined) {
  const current = lstat(file);
  if (current && expected && !current.isSymbolicLink() && current.isFile() && sameObject(identity(current), expected)) {
    fs.unlinkSync(file);
  }
}

function readBound(descriptor: number, expected: FileIdentity) {
  const before = identity(fs.fstatSync(descriptor));
  if (!sameIdentity(before, expected)) fail("BOUND_IDENTITY_CHANGED");
  const content = readDescriptor(descriptor);
  const after = identity(fs.fstatSync(descriptor));
  if (!sameIdentity(after, expected)) fail("BOUND_CONTENT_CHANGED");
  return content;
}

function assertVisibleContent(snapshot: PathSnapshot, expected: FileIdentity, expectedContent: Buffer) {
  const visible = inspectPath(snapshot.absolutePath);
  if (
    !visible.exists ||
    !visible.leaf ||
    !sameAncestors(visible.ancestors, snapshot.ancestors) ||
    !sameIdentity(visible.leaf, expected)
  ) {
    fail("VISIBLE_IDENTITY_MISMATCH");
  }
  const descriptor = openNoFollow(visible.absolutePath, fs.constants.O_RDONLY);
  try {
    const content = readBound(descriptor, expected);
    if (!content.equals(expectedContent)) fail("BOUND_CONTENT_CHANGED");
  } finally {
    fs.closeSync(descriptor);
  }
  const after = inspectPath(snapshot.absolutePath);
  if (!after.leaf || !sameAncestors(after.ancestors, snapshot.ancestors) || !sameIdentity(after.leaf, expected)) {
    fail("VISIBLE_POSTCONDITION_CHANGED");
  }
}

function readPreimage(file: string) {
  const snapshot = inspectPath(file);
  if (!snapshot.exists) return { snapshot, content: null as Buffer | null, descriptor: undefined as number | undefined };
  const descriptor = openNoFollow(snapshot.absolutePath, fs.constants.O_RDONLY);
  try {
    const opened = identity(fs.fstatSync(descriptor));
    if (!snapshot.leaf || !sameIdentity(opened, snapshot.leaf)) fail("LEAF_CHANGED");
    if (opened.size > MAX_MANIFEST_BYTES) fail("PLIST_TOO_LARGE");
    const content = readBound(descriptor, snapshot.leaf);
    assertStablePath(snapshot);
    return { snapshot, content, descriptor };
  } catch (error) {
    fs.closeSync(descriptor);
    throw error;
  }
}

function decodeXml(value: string) {
  if (/&(?!(?:amp|lt|gt|quot|apos);)/.test(value)) fail("PLIST_ALIAS_OR_ENTITY");
  return value.replace(/&(amp|lt|gt|quot|apos);/g, (_match, name: string) => ({
    amp: "&",
    lt: "<",
    gt: ">",
    quot: '"',
    apos: "'",
  })[name]!);
}

function parsePlist(source: string): Record<string, PlistValue> {
  if (source.includes("\u0000") || source.includes("<!ENTITY") || source.includes("<![")) {
    fail("PLIST_ALIAS_OR_ENTITY");
  }
  const prefix = new RegExp(
    `^${XML_PREAMBLE.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/\\n/g, "\\s*")}\\s*<plist version="1\\.0">\\s*`,
  );
  const prefixMatch = source.match(prefix);
  if (!prefixMatch) fail("PLIST_PREAMBLE_INVALID");
  const tail = source.slice(prefixMatch[0].length);
  const suffixMatch = tail.match(/\s*<\/plist>\s*$/);
  if (!suffixMatch || suffixMatch.index === undefined) fail("PLIST_SUFFIX_INVALID");
  const body = tail.slice(0, suffixMatch.index);
  const tokenPattern = /\s*(?:(<dict>)|(<\/dict>)|(<array>)|(<\/array>)|<key>([\s\S]*?)<\/key>|<string>([\s\S]*?)<\/string>|<integer>(-?\d+)<\/integer>|(<true\s*\/>)|(<false\s*\/>))/gy;
  const tokens: Array<{ type: string; value?: string }> = [];
  let position = 0;
  while (position < body.length) {
    tokenPattern.lastIndex = position;
    const match = tokenPattern.exec(body);
    if (!match || match.index !== position) fail("PLIST_TOKEN_INVALID");
    position = tokenPattern.lastIndex;
    if (match[1]) tokens.push({ type: "dict-open" });
    else if (match[2]) tokens.push({ type: "dict-close" });
    else if (match[3]) tokens.push({ type: "array-open" });
    else if (match[4]) tokens.push({ type: "array-close" });
    else if (match[5] !== undefined) tokens.push({ type: "key", value: decodeXml(match[5]) });
    else if (match[6] !== undefined) tokens.push({ type: "string", value: decodeXml(match[6]) });
    else if (match[7] !== undefined) tokens.push({ type: "integer", value: match[7] });
    else if (match[8]) tokens.push({ type: "boolean", value: "true" });
    else if (match[9]) tokens.push({ type: "boolean", value: "false" });
  }
  let cursor = 0;
  const parseValue = (): PlistValue => {
    const token = tokens[cursor++];
    if (!token) fail("PLIST_VALUE_MISSING");
    if (token.type === "string") return token.value ?? "";
    if (token.type === "integer") {
      const parsed = Number(token.value);
      if (!Number.isSafeInteger(parsed)) fail("PLIST_INTEGER_INVALID");
      return parsed;
    }
    if (token.type === "boolean") return token.value === "true";
    if (token.type === "array-open") {
      const values: PlistValue[] = [];
      while (tokens[cursor]?.type !== "array-close") values.push(parseValue());
      if (tokens[cursor++]?.type !== "array-close") fail("PLIST_ARRAY_UNCLOSED");
      return values;
    }
    if (token.type === "dict-open") {
      const value: Record<string, PlistValue> = {};
      while (tokens[cursor]?.type !== "dict-close") {
        const key = tokens[cursor++];
        if (!key || key.type !== "key" || key.value === undefined) fail("PLIST_KEY_INVALID");
        if (Object.hasOwn(value, key.value)) fail("PLIST_DUPLICATE_KEY");
        value[key.value] = parseValue();
      }
      if (tokens[cursor++]?.type !== "dict-close") fail("PLIST_DICT_UNCLOSED");
      return value;
    }
    fail("PLIST_VALUE_INVALID");
  };
  const parsed = parseValue();
  if (cursor !== tokens.length || !parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    fail("PLIST_ROOT_INVALID");
  }
  return parsed as Record<string, PlistValue>;
}

function exactKeys(value: Record<string, PlistValue>, expected: string[], code: string) {
  const actual = Object.keys(value).sort();
  if (!isDeepStrictEqual(actual, [...expected].sort())) fail(code);
}

function record(value: PlistValue | undefined, code: string): Record<string, PlistValue> {
  if (!value || typeof value !== "object" || Array.isArray(value)) fail(code);
  return value;
}

function strings(value: PlistValue | undefined, code: string): string[] {
  if (!Array.isArray(value) || !value.every((entry) => typeof entry === "string")) fail(code);
  return value as string[];
}

function assertSafeString(value: string, code: string) {
  if (!value || /[\u0000-\u001f\u007f-\u009f]/.test(value)) fail(code);
}

function validateOwnedManifest(source: string) {
  if (Buffer.byteLength(source) > MAX_MANIFEST_BYTES) fail("PLIST_TOO_LARGE");
  const plist = parsePlist(source);
  exactKeys(plist, [
    "Label",
    "ProgramArguments",
    "WorkingDirectory",
    "RunAtLoad",
    "KeepAlive",
    "ThrottleInterval",
    "StandardOutPath",
    "StandardErrorPath",
    "EnvironmentVariables",
  ], "PLIST_KEYS_UNEXPECTED");
  if (plist.Label !== LAUNCH_AGENT_LABEL) fail("PLIST_LABEL_UNOWNED");
  if (plist.RunAtLoad !== true || plist.ThrottleInterval !== 30) fail("PLIST_LIFECYCLE_UNEXPECTED");
  const keepAlive = record(plist.KeepAlive, "PLIST_KEEPALIVE_INVALID");
  exactKeys(keepAlive, ["SuccessfulExit"], "PLIST_KEEPALIVE_KEYS_UNEXPECTED");
  if (keepAlive.SuccessfulExit !== false) fail("PLIST_KEEPALIVE_INVALID");
  const environment = record(plist.EnvironmentVariables, "PLIST_ENVIRONMENT_INVALID");
  exactKeys(environment, ["PATH", "PLIMSOLL_COLLECTOR_DATA_MODE"], "PLIST_ENVIRONMENT_KEYS_UNEXPECTED");
  if (environment.PLIMSOLL_COLLECTOR_DATA_MODE !== "metadata") fail("PLIST_PRIVACY_MODE_INVALID");
  if (typeof environment.PATH !== "string") fail("PLIST_PATH_INVALID");
  const pathEntries = environment.PATH.split(path.delimiter);
  if (pathEntries.length === 0 || pathEntries.some((entry) => !path.isAbsolute(entry))) fail("PLIST_PATH_INVALID");
  if (pathEntries.some((entry) => /[\u0000-\u001f\u007f-\u009f]/.test(entry))) fail("PLIST_PATH_INVALID");
  const normalized = pathEntries.map((entry) => path.resolve(entry));
  if (new Set(normalized).size !== normalized.length) fail("PLIST_PATH_DUPLICATE");
  if (LAUNCH_AGENT_SYSTEM_PATHS.some((entry) => !normalized.includes(path.resolve(entry)))) {
    fail("PLIST_PATH_INCOMPLETE");
  }
  if (typeof plist.WorkingDirectory !== "string" || !path.isAbsolute(plist.WorkingDirectory)) {
    fail("PLIST_WORKING_DIRECTORY_INVALID");
  }
  assertSafeString(plist.WorkingDirectory, "PLIST_WORKING_DIRECTORY_INVALID");
  const programArguments = strings(plist.ProgramArguments, "PLIST_ARGUMENTS_INVALID");
  programArguments.forEach((entry) => assertSafeString(entry, "PLIST_ARGUMENTS_INVALID"));
  const development =
    programArguments.length === 5 &&
    path.isAbsolute(programArguments[0] ?? "") &&
    path.basename(programArguments[0] ?? "") === "pnpm" &&
    programArguments[1] === "--dir" &&
    path.isAbsolute(programArguments[2] ?? "") &&
    programArguments[2] === plist.WorkingDirectory &&
    programArguments[3] === "collector" &&
    programArguments[4] === "start";
  const packaged =
    programArguments.length === 3 &&
    path.isAbsolute(programArguments[0] ?? "") &&
    path.isAbsolute(programArguments[1] ?? "") &&
    /\.(?:mjs|cjs|js)$/.test(programArguments[1] ?? "") &&
    programArguments[2] === "start" &&
    path.dirname(programArguments[1] ?? "") === plist.WorkingDirectory;
  if (!development && !packaged) fail("PLIST_ARGUMENTS_UNOWNED");
  // Ownership follows the runtime recorded in the manifest. The installer may
  // itself be running under a newer Node path while replacing an older owned
  // manifest; requiring the installer's path here would make safe upgrades
  // impossible. The newly rendered object independently includes both paths.
  const requiredRuntimePaths = [path.dirname(programArguments[0]!)];
  if (requiredRuntimePaths.some((entry) => !normalized.includes(path.resolve(entry)))) {
    fail("PLIST_PATH_RUNTIME_MISSING");
  }
  for (const key of ["StandardOutPath", "StandardErrorPath"] as const) {
    const value = plist[key];
    if (typeof value !== "string" || !path.isAbsolute(value)) fail("PLIST_LOG_PATH_INVALID");
    assertSafeString(value, "PLIST_LOG_PATH_INVALID");
  }
  if (
    path.dirname(plist.StandardOutPath as string) !== path.dirname(plist.StandardErrorPath as string) ||
    path.basename(plist.StandardOutPath as string) !== "collector.out.log" ||
    path.basename(plist.StandardErrorPath as string) !== "collector.err.log"
  ) {
    fail("PLIST_LOG_PATH_INVALID");
  }
  return plist;
}

function escapeXml(value: string) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function stringElement(value: string) {
  return `    <string>${escapeXml(value)}</string>`;
}

function assertAllowedOptions(options: LaunchAgentOptions) {
  if ((options.label ?? LAUNCH_AGENT_LABEL) !== LAUNCH_AGENT_LABEL) fail("LABEL_NOT_ALLOWLISTED");
  if ((options.restartThrottleSeconds ?? 30) !== 30) fail("THROTTLE_NOT_ALLOWLISTED");
  if (!path.isAbsolute(options.repoRoot)) fail("REPO_ROOT_NOT_ABSOLUTE");
  if (options.workingDirectory && !path.isAbsolute(options.workingDirectory)) fail("WORKING_DIRECTORY_NOT_ABSOLUTE");
  if (options.programArguments?.some((entry) => typeof entry !== "string")) fail("ARGUMENTS_INVALID");
}

function resolvePnpmPath(value: string) {
  if (path.isAbsolute(value)) return path.resolve(value);
  if (value !== "pnpm") fail("PNPM_PATH_NOT_ABSOLUTE");
  for (const directory of (process.env.PATH ?? "").split(path.delimiter)) {
    if (!directory || !path.isAbsolute(directory)) continue;
    const candidate = path.join(directory, value);
    try {
      fs.accessSync(candidate, fs.constants.X_OK);
      return path.resolve(candidate);
    } catch {
      // Continue through the bounded PATH list.
    }
  }
  fail("PNPM_NOT_FOUND");
}

function normalizedOptions(options: LaunchAgentOptions): LaunchAgentOptions {
  assertAllowedOptions(options);
  if (options.programArguments) return { ...options, label: LAUNCH_AGENT_LABEL, restartThrottleSeconds: 30 };
  return {
    ...options,
    label: LAUNCH_AGENT_LABEL,
    restartThrottleSeconds: 30,
    pnpmPath: resolvePnpmPath(options.pnpmPath ?? "pnpm"),
  };
}

export function launchAgentsDir(homeDir = os.homedir()) {
  return path.join(homeDir, "Library", "LaunchAgents");
}

export function launchAgentPlistPath(homeDir = os.homedir(), label = LAUNCH_AGENT_LABEL) {
  if (label !== LAUNCH_AGENT_LABEL) fail("LABEL_NOT_ALLOWLISTED");
  return path.join(launchAgentsDir(homeDir), `${label}.plist`);
}

export function renderLaunchAgentPlist(options: LaunchAgentOptions) {
  assertAllowedOptions(options);
  const homeDir = options.homeDir ?? os.homedir();
  const label = LAUNCH_AGENT_LABEL;
  const pnpmPath = options.pnpmPath ?? "pnpm";
  const logDirectory = collectorHome(homeDir);
  const programArguments = options.programArguments ?? [pnpmPath, "--dir", options.repoRoot, "collector", "start"];
  const workingDirectory = options.workingDirectory ?? options.repoRoot;
  const inheritedPathEntries = (process.env.PATH ?? "").split(path.delimiter);
  const pathCandidates = [
    path.dirname(process.execPath),
    path.dirname(pnpmPath),
    path.dirname(programArguments[0] ?? ""),
    ...inheritedPathEntries,
    ...LAUNCH_AGENT_SYSTEM_PATHS,
  ];
  const pathEntries: string[] = [];
  const normalizedEntries = new Set<string>();
  for (const candidate of pathCandidates) {
    if (!candidate || !path.isAbsolute(candidate) || /[\u0000-\u001f\u007f-\u009f]/.test(candidate)) continue;
    const normalized = path.resolve(candidate);
    if (normalizedEntries.has(normalized)) continue;
    normalizedEntries.add(normalized);
    pathEntries.push(normalized);
  }
  const launchAgentPath = pathEntries.join(path.delimiter);

  return `${XML_PREAMBLE}
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${escapeXml(label)}</string>
  <key>ProgramArguments</key>
  <array>
${programArguments.map(stringElement).join("\n")}
  </array>
  <key>WorkingDirectory</key>
  <string>${escapeXml(workingDirectory)}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>30</integer>
  <key>StandardOutPath</key>
  <string>${escapeXml(path.join(logDirectory, "collector.out.log"))}</string>
  <key>StandardErrorPath</key>
  <string>${escapeXml(path.join(logDirectory, "collector.err.log"))}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PLIMSOLL_COLLECTOR_DATA_MODE</key>
    <string>metadata</string>
    <key>PATH</key>
    <string>${escapeXml(launchAgentPath)}</string>
  </dict>
</dict>
</plist>
`;
}

function prepareFile(snapshot: PathSnapshot, content: Buffer) {
  const temporary = path.join(
    path.dirname(snapshot.absolutePath),
    `.${path.basename(snapshot.absolutePath)}.plimsoll-prepared-${randomUUID()}`,
  );
  const descriptor = openNoFollow(
    temporary,
    fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_EXCL,
    MANIFEST_MODE,
  );
  let preparedIdentity = identity(fs.fstatSync(descriptor));
  try {
    if (preparedIdentity.links !== 1) fail("PREPARED_LINK_COUNT");
    fs.fchmodSync(descriptor, MANIFEST_MODE);
    preparedIdentity = identity(fs.fstatSync(descriptor));
    assertExactPrivateMode(preparedIdentity, "PREPARED_MODE_INVALID");
    writeDescriptor(descriptor, content);
    preparedIdentity = identity(fs.fstatSync(descriptor));
    assertExactPrivateMode(preparedIdentity, "PREPARED_MODE_INVALID");
    const reread = readBound(descriptor, preparedIdentity);
    if (!reread.equals(content)) fail("PREPARED_CONTENT_MISMATCH");
    validateOwnedManifest(reread.toString("utf8"));
  } catch (error) {
    fs.closeSync(descriptor);
    unlinkIfIdentity(temporary, preparedIdentity);
    throw error;
  }
  fs.closeSync(descriptor);
  assertVisibleContent(
    { ...snapshot, absolutePath: temporary, exists: true, leaf: preparedIdentity },
    preparedIdentity,
    content,
  );
  return { path: temporary, identity: preparedIdentity };
}

function writePrivateFile(file: string, content: Buffer) {
  const descriptor = openNoFollow(
    file,
    fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_EXCL,
    MANIFEST_MODE,
  );
  let created = identity(fs.fstatSync(descriptor));
  try {
    if (created.links !== 1) fail("ROLLBACK_LINK_COUNT");
    fs.fchmodSync(descriptor, MANIFEST_MODE);
    created = identity(fs.fstatSync(descriptor));
    assertExactPrivateMode(created, "ROLLBACK_MODE_INVALID");
    writeDescriptor(descriptor, content);
    created = identity(fs.fstatSync(descriptor));
    assertExactPrivateMode(created, "ROLLBACK_MODE_INVALID");
    if (!readBound(descriptor, created).equals(content)) fail("ROLLBACK_CONTENT_MISMATCH");
    return created;
  } catch (error) {
    unlinkIfIdentity(file, created);
    throw error;
  } finally {
    fs.closeSync(descriptor);
  }
}

function createRollback(snapshot: PathSnapshot, content: Buffer, boundDescriptor: number) {
  if (!snapshot.leaf) fail("PREIMAGE_IDENTITY_MISSING");
  const nonce = randomUUID();
  const base = `.${path.basename(snapshot.absolutePath)}.plimsoll-rollback-${nonce}`;
  const preimagePath = path.join(path.dirname(snapshot.absolutePath), `${base}.preimage`);
  const receiptPath = path.join(path.dirname(snapshot.absolutePath), `${base}.receipt.json`);
  const preimageIdentity = writePrivateFile(preimagePath, content);
  try {
    if (!readBound(boundDescriptor, snapshot.leaf).equals(content)) fail("BOUND_CONTENT_CHANGED");
    assertVisibleContent(snapshot, snapshot.leaf, content);
    const receipt = Buffer.from(`${JSON.stringify({
      schema: "plimsoll.launch-agent-rollback.v1",
      status: "ready",
      target: "user_launch_agent",
      label: LAUNCH_AGENT_LABEL,
      preimageFile: path.basename(preimagePath),
      preimageDigest: digest(content),
      preimageBytes: content.length,
      preimageMode: formatPermissionMode(snapshot.leaf.mode),
      preimageUid: snapshot.leaf.uid,
      preimageGid: snapshot.leaf.gid,
    }, null, 2)}\n`);
    const receiptIdentity = writePrivateFile(receiptPath, receipt);
    fsyncDirectory(path.dirname(snapshot.absolutePath));
    return { preimagePath, receiptPath, preimageIdentity, receiptIdentity, receiptContent: receipt };
  } catch (error) {
    unlinkIfIdentity(preimagePath, preimageIdentity);
    throw error;
  }
}

function assertRollback(snapshot: PathSnapshot, rollback: ReturnType<typeof createRollback>, content: Buffer) {
  assertVisibleContent(
    { ...snapshot, absolutePath: rollback.preimagePath, exists: true, leaf: rollback.preimageIdentity },
    rollback.preimageIdentity,
    content,
  );
  assertVisibleContent(
    { ...snapshot, absolutePath: rollback.receiptPath, exists: true, leaf: rollback.receiptIdentity },
    rollback.receiptIdentity,
    rollback.receiptContent,
  );
}

function restoreUnexpectedClaim(claimPath: string, destination: string, claimIdentity: FileIdentity) {
  if (lstat(destination)) return;
  const claim = lstat(claimPath);
  if (!claim || claim.isSymbolicLink() || !claim.isFile() || !sameObject(identity(claim), claimIdentity)) return;
  try {
    fs.linkSync(claimPath, destination);
    fs.unlinkSync(claimPath);
  } catch {
    return;
  }
  const restored = lstat(destination);
  if (!restored || !sameObject(identity(restored), claimIdentity)) fail("COMMIT_CLAIM_RESTORE_FAILED");
}

function installReceipt(
  status: LaunchAgentInstallReceipt["status"],
  manifestDigest: string,
  rollback: ReturnType<typeof createRollback> | undefined,
  preimage: { snapshot: PathSnapshot; content: Buffer | null },
  wouldChange = status === "installed",
): LaunchAgentInstallReceipt {
  return {
    schema: "plimsoll.launch-agent-install.v1",
    operation: "install",
    status,
    target: "user_launch_agent",
    label: LAUNCH_AGENT_LABEL,
    changed: status === "installed",
    wouldChange,
    manifestDigest,
    manifestMode: "0600",
    privacyMode: "metadata_only",
    environmentKeys: ["PATH", "PLIMSOLL_COLLECTOR_DATA_MODE"],
    rollback: rollback && preimage.content && preimage.snapshot.leaf ? {
      available: true,
      preimageDigest: digest(preimage.content),
      preimageMode: formatPermissionMode(preimage.snapshot.leaf.mode),
      preimagePathHash: pathDigest(rollback.preimagePath),
      receiptPathHash: pathDigest(rollback.receiptPath),
    } : null,
  };
}

export function installLaunchAgent(options: LaunchAgentOptions): LaunchAgentInstallResult {
  const normalized = normalizedOptions(options);
  const homeDir = ensureHomeRoot(normalized.homeDir ?? os.homedir());
  const plistPath = launchAgentPlistPath(homeDir);
  const desired = Buffer.from(renderLaunchAgentPlist({ ...normalized, homeDir }));
  validateOwnedManifest(desired.toString("utf8"));
  const initial = readPreimage(plistPath);
  try {
    if (initial.content) validateOwnedManifest(initial.content.toString("utf8"));
    const exactNoop = Boolean(
      initial.content &&
      initial.snapshot.leaf &&
      initial.content.equals(desired) &&
      permissionMode(initial.snapshot.leaf.mode) === MANIFEST_MODE,
    );
    if (options.dryRun) {
      return {
        plistPath,
        receipt: installReceipt("preview", digest(desired), undefined, initial, !exactNoop),
      };
    }
    if (exactNoop) {
      assertStablePath(initial.snapshot);
      return {
        plistPath,
        receipt: installReceipt("unchanged", digest(desired), undefined, initial),
      };
    }

    const snapshot = ensureParent(initial.snapshot, homeDir, options.transactionHooks?.afterParentCreate);
    if (snapshot.exists !== initial.snapshot.exists) fail("PATH_CHANGED");
    const boundDescriptor = initial.descriptor;
    let prepared: ReturnType<typeof prepareFile> | undefined;
    let committed: ReturnType<typeof prepareFile> | undefined;
    let published: { path: string; identity: FileIdentity } | undefined;
    let claim: { path: string; identity: FileIdentity } | undefined;
    let rollback: ReturnType<typeof createRollback> | undefined;
    try {
      if (snapshot.exists) {
        if (!initial.content || boundDescriptor === undefined || !snapshot.leaf) fail("PREIMAGE_IDENTITY_MISSING");
        if (!readBound(boundDescriptor, snapshot.leaf).equals(initial.content)) fail("BOUND_CONTENT_CHANGED");
        assertVisibleContent(snapshot, snapshot.leaf, initial.content);
      }
      prepared = prepareFile(snapshot, desired);
      runHook(options.transactionHooks?.afterPrepare);
      assertStablePath(snapshot);
      assertVisibleContent(
        { ...snapshot, absolutePath: prepared.path, exists: true, leaf: prepared.identity },
        prepared.identity,
        desired,
      );
      if (snapshot.exists) {
        rollback = createRollback(snapshot, initial.content!, boundDescriptor!);
        runHook(options.transactionHooks?.afterRollback);
        assertRollback(snapshot, rollback, initial.content!);
      }
      if (snapshot.exists) assertVisibleContent(snapshot, snapshot.leaf!, initial.content!);
      else assertStablePath(snapshot);
      runHook(options.transactionHooks?.beforeCommit);
      if (snapshot.exists) {
        assertVisibleContent(snapshot, snapshot.leaf!, initial.content!);
        if (rollback) assertRollback(snapshot, rollback, initial.content!);
        const claimPath = path.join(
          path.dirname(snapshot.absolutePath),
          `.${path.basename(snapshot.absolutePath)}.plimsoll-claim-${randomUUID()}`,
        );
        if (lstat(claimPath)) fail("COMMIT_CLAIM_COLLISION");
        fs.renameSync(snapshot.absolutePath, claimPath);
        const claimed = lstat(claimPath);
        if (!claimed) fail("COMMIT_CLAIM_MISSING");
        claim = { path: claimPath, identity: identity(claimed) };
        if (!sameObject(claim.identity, snapshot.leaf!)) {
          restoreUnexpectedClaim(claimPath, snapshot.absolutePath, claim.identity);
          claim = undefined;
          fail("COMMIT_CLAIM_MISMATCH");
        }
        try {
          assertVisibleContent(
            { ...snapshot, absolutePath: claimPath, exists: true, leaf: claim.identity },
            claim.identity,
            initial.content!,
          );
        } catch {
          restoreUnexpectedClaim(claimPath, snapshot.absolutePath, claim.identity);
          claim = undefined;
          fail("COMMIT_CLAIM_MISMATCH");
        }
      } else {
        assertStablePath(snapshot);
      }
      // Atomically move the validated object to a commit name, then publish
      // with a no-clobber hard link. Node/macOS offers no compare-and-swap
      // rename; this preserves both atomic visibility and the operator's file.
      const committedPath = path.join(
        path.dirname(snapshot.absolutePath),
        `.${path.basename(snapshot.absolutePath)}.plimsoll-commit-${randomUUID()}`,
      );
      fs.renameSync(prepared.path, committedPath);
      const committedStat = lstat(committedPath);
      if (!committedStat) fail("COMMIT_OBJECT_MISSING");
      const committedIdentity = identity(committedStat);
      if (!sameObject(committedIdentity, prepared.identity)) fail("COMMIT_OBJECT_CHANGED");
      committed = { path: committedPath, identity: committedIdentity };
      prepared = undefined;
      assertVisibleContent(
        { ...snapshot, absolutePath: committed.path, exists: true, leaf: committed.identity },
        committed.identity,
        desired,
      );
      fs.linkSync(committed.path, snapshot.absolutePath);
      fs.unlinkSync(committed.path);
      const publishedIdentity = identity(fs.lstatSync(snapshot.absolutePath));
      if (!sameObject(publishedIdentity, committed.identity) || publishedIdentity.links !== 1) {
        fail("PUBLISHED_OBJECT_CHANGED");
      }
      assertExactPrivateMode(publishedIdentity, "VISIBLE_MODE_INVALID");
      published = { path: snapshot.absolutePath, identity: publishedIdentity };
      committed = undefined;
      runHook(options.transactionHooks?.afterCommit);
      assertVisibleContent(snapshot, publishedIdentity, desired);
      const visible = inspectPath(snapshot.absolutePath);
      if (!visible.leaf || permissionMode(visible.leaf.mode) !== MANIFEST_MODE) fail("VISIBLE_MODE_INVALID");
      validateOwnedManifest(desired.toString("utf8"));
      if (rollback) assertRollback(snapshot, rollback, initial.content!);
      fsyncDirectory(path.dirname(snapshot.absolutePath));
      assertVisibleContent(snapshot, visible.leaf, desired);
      if (claim) {
        unlinkIfIdentity(claim.path, claim.identity);
        claim = undefined;
        fsyncDirectory(path.dirname(snapshot.absolutePath));
        assertVisibleContent(snapshot, visible.leaf, desired);
      }
      published = undefined;
      return {
        plistPath,
        receipt: installReceipt("installed", digest(desired), rollback, initial),
        ...(rollback ? { rollbackFiles: { preimagePath: rollback.preimagePath, receiptPath: rollback.receiptPath } } : {}),
      };
    } finally {
      if (prepared) unlinkIfIdentity(prepared.path, prepared.identity);
      if (committed) unlinkIfIdentity(committed.path, committed.identity);
      if (published) unlinkIfIdentity(published.path, published.identity);
      if (claim) restoreUnexpectedClaim(claim.path, snapshot.absolutePath, claim.identity);
    }
  } finally {
    if (initial.descriptor !== undefined) fs.closeSync(initial.descriptor);
  }
}

export function inspectLaunchAgentManifest(options: { homeDir?: string } = {}) {
  const homeDir = ensureHomeRoot(options.homeDir ?? os.homedir());
  const plistPath = launchAgentPlistPath(homeDir);
  const preimage = readPreimage(plistPath);
  try {
    if (!preimage.content) {
      return { ok: false, status: "missing" as const, plistPath, manifestDigest: null };
    }
    validateOwnedManifest(preimage.content.toString("utf8"));
    assertStablePath(preimage.snapshot);
    return {
      ok: true,
      status: "valid" as const,
      plistPath,
      manifestDigest: digest(preimage.content),
      manifestIdentityDigest: identityDigest(preimage.snapshot.leaf!),
      mode: formatPermissionMode(preimage.snapshot.leaf!.mode),
    };
  } finally {
    if (preimage.descriptor !== undefined) fs.closeSync(preimage.descriptor);
  }
}

export function uninstallLaunchAgent(options: {
  homeDir?: string;
  label?: string;
  dryRun?: boolean;
  transactionHooks?: Pick<LaunchAgentTransactionHooks, "beforeCommit" | "afterCommit">;
}): { plistPath: string; receipt: LaunchAgentUninstallReceipt } {
  if ((options.label ?? LAUNCH_AGENT_LABEL) !== LAUNCH_AGENT_LABEL) fail("LABEL_NOT_ALLOWLISTED");
  const homeDir = ensureHomeRoot(options.homeDir ?? os.homedir());
  const plistPath = launchAgentPlistPath(homeDir);
  const preimage = readPreimage(plistPath);
  try {
    if (!preimage.content) {
      return {
        plistPath,
        receipt: {
          schema: "plimsoll.launch-agent-uninstall.v1",
          operation: "uninstall",
          status: options.dryRun ? "preview" : "absent",
          target: "user_launch_agent",
          label: LAUNCH_AGENT_LABEL,
          changed: false,
          wouldChange: false,
          removedManifestDigest: null,
        },
      };
    }
    validateOwnedManifest(preimage.content.toString("utf8"));
    if (options.dryRun) {
      return {
        plistPath,
        receipt: {
          schema: "plimsoll.launch-agent-uninstall.v1",
          operation: "uninstall",
          status: "preview",
          target: "user_launch_agent",
          label: LAUNCH_AGENT_LABEL,
          changed: false,
          wouldChange: true,
          removedManifestDigest: digest(preimage.content),
        },
      };
    }
    assertStablePath(preimage.snapshot);
    runHook(options.transactionHooks?.beforeCommit);
    assertVisibleContent(preimage.snapshot, preimage.snapshot.leaf!, preimage.content);
    const claimPath = path.join(
      path.dirname(preimage.snapshot.absolutePath),
      `.${path.basename(preimage.snapshot.absolutePath)}.plimsoll-remove-${randomUUID()}`,
    );
    fs.renameSync(preimage.snapshot.absolutePath, claimPath);
    const claimed = lstat(claimPath);
    if (!claimed) fail("REMOVE_CLAIM_MISSING");
    const claimIdentity = identity(claimed);
    if (!sameObject(claimIdentity, preimage.snapshot.leaf!)) {
      restoreUnexpectedClaim(claimPath, preimage.snapshot.absolutePath, claimIdentity);
      fail("REMOVE_CLAIM_MISMATCH");
    }
    try {
      assertVisibleContent(
        { ...preimage.snapshot, absolutePath: claimPath, exists: true, leaf: claimIdentity },
        claimIdentity,
        preimage.content,
      );
    } catch {
      restoreUnexpectedClaim(claimPath, preimage.snapshot.absolutePath, claimIdentity);
      fail("REMOVE_CLAIM_MISMATCH");
    }
    runHook(options.transactionHooks?.afterCommit);
    if (lstat(preimage.snapshot.absolutePath)) {
      restoreUnexpectedClaim(claimPath, preimage.snapshot.absolutePath, claimIdentity);
      fail("REMOVE_DESTINATION_REAPPEARED");
    }
    unlinkIfIdentity(claimPath, claimIdentity);
    fsyncDirectory(path.dirname(preimage.snapshot.absolutePath));
    if (lstat(preimage.snapshot.absolutePath)) fail("REMOVE_POSTCONDITION_CHANGED");
    return {
      plistPath,
      receipt: {
        schema: "plimsoll.launch-agent-uninstall.v1",
        operation: "uninstall",
        status: "removed",
        target: "user_launch_agent",
        label: LAUNCH_AGENT_LABEL,
        changed: true,
        wouldChange: true,
        removedManifestDigest: digest(preimage.content),
      },
    };
  } finally {
    if (preimage.descriptor !== undefined) fs.closeSync(preimage.descriptor);
  }
}

export function launchctlBootstrapCommand(plistPath: string, uid = process.getuid?.()) {
  return ["launchctl", "bootstrap", `gui/${uid ?? ""}`, plistPath].filter(Boolean);
}

export function launchctlBootoutCommand(label = LAUNCH_AGENT_LABEL, uid = process.getuid?.()) {
  if (label !== LAUNCH_AGENT_LABEL) fail("LABEL_NOT_ALLOWLISTED");
  return ["launchctl", "bootout", `gui/${uid ?? ""}/${label}`].filter(Boolean);
}

export function launchctlPrintCommand(label = LAUNCH_AGENT_LABEL, uid = process.getuid?.()) {
  if (label !== LAUNCH_AGENT_LABEL) fail("LABEL_NOT_ALLOWLISTED");
  return ["launchctl", "print", `gui/${uid ?? ""}/${label}`].filter(Boolean);
}
