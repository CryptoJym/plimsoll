import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import {
  isSafeSuppressionSourceKey,
  isSensitiveMetadataSemanticKey,
  protectedMetadataFieldNames,
} from "../../shared/src/index";
import { resolveCollectorHome } from "./collector-home";

/**
 * Bead eco-6hoxj.61: hook events the local collector cannot accept right now
 * are not lost.
 *
 * A Claude Code / Codex / Grok hook is a short-lived process that forwards one
 * JSON body over loopback and exits. When the collector answers 503
 * (`storage_busy_retry`: the ledger stayed contended past the 750 ms retry
 * budget) or is not listening at all (a managed update window restarts it for
 * 1-34 s), the client used to throw `hook_forward_http_rejected:503` and the
 * event was gone — 701 rejection summaries on Studio0 on 2026-09-12 stood for
 * roughly 1,200 posts that never reached `buffered_events`.
 *
 * The spool is deliberately the dumbest durable thing that closes that path:
 * one file per event under the SAME resolved Plimsoll home the collector binds
 * for its ledger, drained by the collector through the exact `/hooks/<source>`
 * code path once the ledger is free. It is bounded (files and bytes), private
 * (0700/0600), never carries a producer token, and has a kill switch. When a
 * bound is hit the client falls back to throwing, so the loss stays visible
 * rather than becoming a silently growing directory.
 *
 * What is on disk is NOT the original body. `docs/privacy-spec.md` holds raw
 * content and private/path metadata out of every local write, and the spool is
 * a local write that happens BEFORE the collector's suppression can run. So the
 * hook process blanks it first, with the collector's own DROP rule: every key
 * `sanitizeRoutineMetadata` would remove outright — the forbidden raw-content
 * names plus the private-concept and path rules, plus a name too unsafe to put
 * in a receipt — keeps its name and loses its value (`""`). See
 * `blankForbiddenRawContent`.
 *
 * Stated exactly, a spool file holds:
 *   - the hook body's keys, all of them, with their names intact;
 *   - the values of keys the collector's sanitizer does NOT drop. Most of
 *     those are dropped a step later anyway, by `admittedHookMetadata`, as
 *     unknown metadata — the spool does not model that second step, so it can
 *     hold a value the ledger ends up discarding as unrecognised;
 *   - the values of the declared derivation inputs
 *     (`SPOOL_DERIVATION_INPUT_DISCLOSURE`): `SPOOL_DERIVATION_INPUT_KEYS`, the
 *     keys the collector reads BEFORE suppression to derive something it
 *     persists — a blanked `cwd` would silently cost the event its repository
 *     linkage — and `SPOOL_PROTECTED_IDENTITY_KEYS`, the protected identity
 *     names whose raw value the ledger hashes rather than drops, where blanking
 *     would make the ledger persist the hash of `""` instead of the hash of the
 *     real value. Those values are what the spool holds that the ledger's own
 *     bytes do not, and every one of them is in the privacy spec's exemption
 *     table with its reason.
 * It never holds raw prompt/output/tool content, credential-like values, or
 * file/transcript paths.
 */

export const HOOK_SPOOL_DIRECTORY = "hook-spool";
export const HOOK_SPOOL_REJECTED_DIRECTORY = "rejected";
export const HOOK_SPOOL_COUNTERS_FILE = ".counters.json";
export const HOOK_SPOOL_ENV = "PLIMSOLL_HOOK_SPOOL";

/** The closed set of hook sources a spooled file may claim. */
export const HOOK_SPOOL_SOURCES = ["claude_code", "codex", "grok"] as const;
export type HookSpoolSource = (typeof HOOK_SPOOL_SOURCES)[number];

export const HOOK_SPOOL_LIMITS = Object.freeze({
  /** Directory ceilings checked before every write (readdir + stat). */
  maxFiles: 5_000,
  maxBytes: 64 * 1024 * 1024,
  /** Drain cadence and per-tick work ceiling. */
  drainIntervalMs: 5_000,
  maxFilesPerTick: 200,
  /** Rejected-file retention: bounded by count and by age. */
  maxRejectedFiles: 500,
  rejectedMaxAgeMs: 7 * 24 * 60 * 60 * 1000,
  /**
   * Retention has to run on a cadence of its own, not only after a tick that
   * rejected something: a host that had a burst of rejections and then went
   * quiet kept them past the age bound forever (review r1, F3). The drain
   * prunes at start-up and then every 60th tick, i.e. about every 5 minutes.
   */
  rejectedPruneEveryTicks: 60,
  /**
   * A `*.json.tmp` older than this was left by a process that died between
   * `writeFileSync` and `renameSync` (review r1, F4). Younger than this it may
   * be another hook process mid-write, and is never touched.
   */
  temporaryOrphanMs: 60_000,
  /** Doctor says so plainly once pending files are this old. */
  stalePendingSeconds: 600,
});

export type HookSpoolBounds = { maxFiles: number; maxBytes: number };

/**
 * A spooled file name is `<utcMillis>-<pid>-<random6>.json`. The pattern is
 * strict on purpose: it is the only thing that makes a pending file, and it
 * keeps `.counters.json`, a half-written `.tmp`, and the `rejected/`
 * subdirectory out of every listing by construction.
 */
const SPOOL_FILE_PATTERN = /^(\d{13,})-(\d+)-([0-9a-f]{6})\.json$/;
/** A rejected file keeps its identity and gains `.<reason>` before `.json`. */
const REJECTED_FILE_PATTERN = /^(\d{13,})-(\d+)-([0-9a-f]{6})\.([a-z0-9_]+)\.json$/;
/**
 * Every temporary this module writes (a spool file and the counters file both
 * land through `<name>.tmp`). They are invisible to `SPOOL_FILE_PATTERN` by
 * design, which is exactly why an orphan needed its own reaper and its own
 * place in the byte bound.
 */
const TEMPORARY_FILE_PATTERN = /\.json\.tmp$/;

export type HookSpoolRejectionReason = "spool_untrusted" | string;

/** Kill switch: `PLIMSOLL_HOOK_SPOOL=off` restores the pre-0.7.22 behaviour. */
export function hookSpoolEnabled(env: NodeJS.ProcessEnv = process.env) {
  return env[HOOK_SPOOL_ENV] !== "off";
}

/**
 * The spool lives beside the ledger in the one canonical collector home
 * (issue #135). Never `os.homedir()`: a hook process that spooled somewhere
 * else than the daemon drains would be a second loss path.
 */
export function resolveHookSpoolHome(env: NodeJS.ProcessEnv = process.env) {
  return resolveCollectorHome({ env }).home;
}

export function hookSpoolDirectory(home: string) {
  return path.join(home, HOOK_SPOOL_DIRECTORY);
}

export function hookSpoolRejectedDirectory(home: string) {
  return path.join(hookSpoolDirectory(home), HOOK_SPOOL_REJECTED_DIRECTORY);
}

export function hookSpoolCountersPath(home: string) {
  return path.join(hookSpoolDirectory(home), HOOK_SPOOL_COUNTERS_FILE);
}

export function isHookSpoolSource(value: unknown): value is HookSpoolSource {
  return typeof value === "string" && (HOOK_SPOOL_SOURCES as readonly string[]).includes(value);
}

export type HookSpoolEnvelope = {
  v: 1;
  source: HookSpoolSource;
  /**
   * When the HOOK process decided to spool, stamped by that process before the
   * file is written. The drain hands this to the route as the event's
   * `observedAt` default (review r2, F1), so a recovered event carries the time
   * the hook fired rather than the time the drain got to it.
   */
  receivedAt: string;
  /** How many values `blankForbiddenRawContent` emptied before the write. */
  blanked: number;
  /**
   * The hook body with every suppressed-before-write value blanked, serialized.
   * Never a producer token, and never raw prompt/output/tool content.
   */
  body: string;
};

/**
 * The keys whose VALUES survive the blanking, because the collector reads them
 * from the raw body BEFORE its own suppression runs and derives something it
 * persists from them. Blanking one of these would not make the spool safer —
 * it would make the recovered event worse than the live one, silently.
 *
 * Every entry is exact-match on purpose: the readers below match exact key
 * names too, so a case or separator variant is not a derivation input and is
 * blanked like any other sensitive key.
 *
 * Nothing goes on this list without a named derivation; the per-fixture parity
 * proof (`scripts/hook-spool-proof.ts`, case `q`) is the guard in the other
 * direction — a key whose blanking moves a persisted value belongs here.
 */
export const SPOOL_DERIVATION_INPUT_KEYS = [
  // The four keys `extractRepoContextCwd` (`repo-context.ts:22`, `CWD_KEYS`)
  // reads from the raw payload. `appendForwardedHook` (`forwarder.ts:42`) uses
  // the result to attach the repo-context sidecar, which the ledger turns into
  // the event's repository linkage rows (repo/branch/head). Blanked, the event
  // loses its repository attribution for good.
  "cwd",
  "current_working_directory",
  "workdir",
  "working_directory",
  // The one `eventType` authority alias that is itself sensitive — as a
  // camelCase variant of the approved `hook_event_name`, `isSensitiveMetadata
  // SemanticKey` strips it. `normalizeHookPayload` selects the event's type
  // from this value in the RAW body (`hook-authority.ts:81`,
  // `normalizer.ts:364`), so blanking it would move `event_type`. The other
  // value-bearing authority aliases (`id`/`eventId`/`event_id`,
  // `eventType`/`event_type`/`type`, `actionClass`/`action_class`,
  // `observedAt`/`observed_at`/`timestamp`/`time`) are not sensitive, so they
  // are never blanked and need no exemption; the authority aliases that ARE
  // sensitive but only ever produce a receipt from the key's presence
  // (`transportPath`, `repo_hash`, `branch_hash`, `head_sha`, `tenant.id`, …)
  // are blanked, and their receipts are unchanged because a receipt is built
  // from the key path, never the value.
  "hookEventName",
] as const;

/**
 * The collector's own pre-write rule on its own, with no exemption applied:
 * the two ways `sanitizeRoutineMetadata` (`packages/shared/src/policy.ts`)
 * drops a key OUTRIGHT. `isSensitiveMetadataSemanticKey` is the forbidden
 * raw-content names, the private-concept rule and the raw/path word rule;
 * `!isSafeSuppressionSourceKey` is a name it cannot even put in a receipt.
 */
function collectorStripsKeyOutright(key: string) {
  return !isSafeSuppressionSourceKey(key) || isSensitiveMetadataSemanticKey(key);
}

/**
 * The SECOND group of derivation inputs (review r3, N3): the protected identity
 * names.
 *
 * `sanitizeRoutineMetadata` has a third branch the two above do not cover —
 * `isProtectedMetadataFieldName` keeps the key and replaces its value with
 * `hashProtectedValue` (the privacy spec's *Collected hashed* bucket). The
 * spool's rule mirrors the two DROP branches, so it never blanked these: their
 * values were already resting in the spool file undeclared, which is the whole
 * of the defect. Blanking them instead would be worse, and measurably: the
 * ledger would then persist the hash of `""` (`sha256:e3b0c44298fc1c14`) in
 * place of the hash of the real value, so the recovered row would carry a
 * different identity from the live one. So the value stays and is DECLARED
 * here — that is what this constant is for.
 *
 * Derived from the shared list, never typed by hand: every protected name that
 * the drop branches do not already strip. Typing the nine names a reviewer
 * happens to have measured would have missed two (`account_uuid`, `actor_id`)
 * that behave identically.
 */
export const SPOOL_PROTECTED_IDENTITY_KEYS: readonly string[] = protectedMetadataFieldNames.filter(
  (name) =>
    !collectorStripsKeyOutright(name) &&
    !(SPOOL_DERIVATION_INPUT_KEYS as readonly string[]).includes(name),
);

export type SpoolDerivationInputDisclosure = {
  /** The exact key name whose value survives the blanking. */
  key: string;
  /** What the collector derives from that value, in one clause. */
  reason: string;
};

/**
 * The full disclosure, both groups with their reason, exported so
 * `scripts/privacy-spec.ts` renders the exemption table from the code that
 * enforces it and the page cannot drift from the rule.
 */
export const SPOOL_DERIVATION_INPUT_DISCLOSURE: readonly SpoolDerivationInputDisclosure[] = [
  ...SPOOL_DERIVATION_INPUT_KEYS.map((key) => ({
    key: key as string,
    reason:
      key === "hookEventName"
        ? "the normalizer selects the event's type from this value in the raw body"
        : "`extractRepoContextCwd` reads this value from the raw body and the ledger turns it into the event's repository linkage",
  })),
  ...SPOOL_PROTECTED_IDENTITY_KEYS.map((key) => ({
    key,
    reason: "the ledger stores the protected hash of this value",
  })),
];

const derivationInputKeys = new Set<string>([
  ...SPOOL_DERIVATION_INPUT_KEYS,
  ...SPOOL_PROTECTED_IDENTITY_KEYS,
]);

/**
 * True when the collector would strip this key's value before the local
 * database write, so the spool must not hold it either.
 *
 * The exemption is a declaration, not a loophole: the first group names keys
 * the rule below WOULD strip and whose values a persisted derivation needs, and
 * the second names keys the rule never stripped in the first place (their
 * values are what the ledger hashes), so listing them changes nothing this
 * function returns. Both are disclosed in `SPOOL_DERIVATION_INPUT_DISCLOSURE`.
 */
function spoolSuppressedKey(key: string) {
  if (derivationInputKeys.has(key)) return false;
  return collectorStripsKeyOutright(key);
}

/**
 * Blank everything the ledger would not keep, before it can reach the disk.
 *
 * The rule is the collector's own pre-write rule, not a narrower one and not a
 * copied list: `spoolSuppressedKey` above. The traversal mirrors
 * `sanitizeRoutineMetadata` (`packages/shared/src/policy.ts`) including its
 * OTLP `{key, value}` attribute branch, so the set of keys this empties is the
 * set the collector strips — minus `SPOOL_DERIVATION_INPUT_KEYS`, whose values
 * it reads before stripping them.
 *
 * The KEY survives and only its value becomes `""`. That is what keeps a
 * recovered row identical to a live one: the collector's suppression still sees
 * the key, still removes it, and still emits the same receipt — receipts are
 * built from key paths, never from values.
 *
 * A body that is not JSON cannot be blanked, so it is not spooled at all
 * (`null`): it is a body the collector would refuse anyway, and putting
 * unexaminable bytes on disk is the thing this function exists to prevent.
 */
export function blankForbiddenRawContent(
  body: string,
): { text: string; blanked: number } | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return null;
  }
  let blanked = 0;
  const blank = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map((item) => blank(item));
    if (!value || typeof value !== "object") return value;
    const record = value as Record<string, unknown>;
    // OTLP-shaped attribute: the name is in `key` and the content is in
    // `value`. `sanitizeRoutineMetadata` judges the attribute by that name and
    // stops descending; so do we.
    const semanticKey = typeof record.key === "string" ? record.key : undefined;
    if (semanticKey && "value" in record && spoolSuppressedKey(semanticKey)) {
      blanked += 1;
      return { ...record, value: "" };
    }
    const next: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(record)) {
      if (spoolSuppressedKey(key)) {
        blanked += 1;
        next[key] = "";
        continue;
      }
      next[key] = blank(nested);
    }
    return next;
  };
  let text: string | undefined;
  try {
    text = JSON.stringify(blank(parsed));
  } catch {
    return null;
  }
  return text === undefined ? null : { text, blanked };
}

/**
 * Private-path rule, identical to the one the collector home itself is held
 * to: owned by this uid, a real (non-symlink) entry of the expected kind, and
 * no group/other permission bits or special bits. A spool file this process
 * did not write with mode 0600 is not replayed into the ledger — it is
 * quarantined under `rejected/`.
 */
export function hookSpoolEntryTrusted(
  target: string,
  kind: "directory" | "file",
  uid = typeof process.getuid === "function" ? process.getuid() : undefined,
) {
  let stat: fs.Stats;
  try {
    stat = fs.lstatSync(target);
  } catch {
    return false;
  }
  if (stat.isSymbolicLink()) return false;
  if (kind === "directory" ? !stat.isDirectory() : !stat.isFile()) return false;
  if (uid !== undefined && stat.uid !== uid) return false;
  return (stat.mode & 0o7077) === 0;
}

function ensureSpoolDirectory(directory: string) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  // mkdir's mode is masked by umask, and an existing directory keeps whatever
  // mode it already had. The spool holds captured event bodies; it is 0700.
  fs.chmodSync(directory, 0o700);
}

export type HookSpoolFile = {
  name: string;
  path: string;
  /** Spool time in epoch milliseconds, from the file name. */
  spooledAtMs: number;
  bytes: number;
};

/** Pending files, oldest first. Nothing else in the directory is listed. */
export function listHookSpoolFiles(home: string, limit = Number.POSITIVE_INFINITY) {
  const directory = hookSpoolDirectory(home);
  let names: string[];
  try {
    names = fs.readdirSync(directory);
  } catch {
    return [] as HookSpoolFile[];
  }
  const files: HookSpoolFile[] = [];
  for (const name of names) {
    const match = SPOOL_FILE_PATTERN.exec(name);
    if (!match) continue;
    const file = path.join(directory, name);
    let stat: fs.Stats;
    try {
      stat = fs.statSync(file);
    } catch {
      continue;
    }
    if (!stat.isFile()) continue;
    files.push({ name, path: file, spooledAtMs: Number(match[1]), bytes: stat.size });
  }
  files.sort((left, right) =>
    left.spooledAtMs - right.spooledAtMs || left.name.localeCompare(right.name),
  );
  return Number.isFinite(limit) ? files.slice(0, limit) : files;
}

/**
 * Reap orphan temporaries and report what the live ones still cost.
 *
 * A `*.json.tmp` is invisible to every listing (review r1, F4): it was not
 * counted toward a bound, not drained, and nothing deleted it, so repeated
 * crashes between `writeFileSync` and `renameSync` could accumulate bytes the
 * 64 MiB ceiling could not see. Anything older than `temporaryOrphanMs` is
 * unlinked; anything younger may be another hook process mid-write and is left
 * alone, but its bytes are returned so the caller can charge them to `maxBytes`.
 */
export function reapHookSpoolTemporaries(home: string, nowMs = Date.now()) {
  const directory = hookSpoolDirectory(home);
  let names: string[];
  try {
    names = fs.readdirSync(directory);
  } catch {
    return { deleted: 0, remainingFiles: 0, remainingBytes: 0 };
  }
  let deleted = 0;
  let remainingFiles = 0;
  let remainingBytes = 0;
  for (const name of names) {
    if (!TEMPORARY_FILE_PATTERN.test(name)) continue;
    const file = path.join(directory, name);
    let stat: fs.Stats;
    try {
      stat = fs.lstatSync(file);
    } catch {
      continue;
    }
    if (!stat.isFile()) continue;
    if (nowMs - stat.mtimeMs > HOOK_SPOOL_LIMITS.temporaryOrphanMs) {
      try {
        fs.unlinkSync(file);
        deleted += 1;
        continue;
      } catch {
        /* fall through and charge its bytes until the next pass */
      }
    }
    remainingFiles += 1;
    remainingBytes += stat.size;
  }
  return { deleted, remainingFiles, remainingBytes };
}

export type HookSpoolPending = {
  pendingFiles: number;
  pendingBytes: number;
  oldestPendingAgeSeconds: number | null;
};

export function hookSpoolPending(home: string, nowMs = Date.now()): HookSpoolPending {
  const files = listHookSpoolFiles(home);
  return {
    pendingFiles: files.length,
    pendingBytes: files.reduce((total, file) => total + file.bytes, 0),
    oldestPendingAgeSeconds: files.length === 0
      ? null
      : Math.max(0, Math.floor((nowMs - files[0]!.spooledAtMs) / 1000)),
  };
}

export type HookSpoolCounters = {
  recovered: number;
  rejected: number;
  deferred: number;
  lastDrainAt: string | null;
};

export const EMPTY_HOOK_SPOOL_COUNTERS: HookSpoolCounters = Object.freeze({
  recovered: 0,
  rejected: 0,
  deferred: 0,
  lastDrainAt: null,
});

function counterValue(value: unknown) {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : 0;
}

/**
 * Counters live in the spool directory, not the ledger: this bead adds no
 * schema change, and the numbers must survive exactly the situation that
 * produced them — a ledger nobody can write.
 */
export function readHookSpoolCounters(home: string): HookSpoolCounters {
  try {
    const parsed = JSON.parse(fs.readFileSync(hookSpoolCountersPath(home), "utf8")) as
      Record<string, unknown>;
    return {
      recovered: counterValue(parsed.recovered),
      rejected: counterValue(parsed.rejected),
      deferred: counterValue(parsed.deferred),
      lastDrainAt: typeof parsed.lastDrainAt === "string" ? parsed.lastDrainAt : null,
    };
  } catch {
    return { ...EMPTY_HOOK_SPOOL_COUNTERS };
  }
}

export function writeHookSpoolCounters(home: string, counters: HookSpoolCounters) {
  const directory = hookSpoolDirectory(home);
  ensureSpoolDirectory(directory);
  const target = hookSpoolCountersPath(home);
  const temporary = `${target}.tmp`;
  fs.writeFileSync(temporary, `${JSON.stringify(counters)}\n`, { mode: 0o600 });
  fs.chmodSync(temporary, 0o600);
  fs.renameSync(temporary, target);
}

/**
 * Write one event to the spool, or answer null when a directory bound is hit
 * or the write itself fails. Null means the caller keeps today's behaviour and
 * surfaces the rejection, so a full or broken spool is a visible loss rather
 * than a silent one.
 */
export function writeHookSpoolFile(options: {
  home: string;
  source: HookSpoolSource;
  body: string;
  /** Forbidden raw-content values the caller emptied before handing it over. */
  blanked?: number;
  nowMs?: number;
  limits?: Partial<HookSpoolBounds>;
}): { path: string } | null {
  const maxFiles = options.limits?.maxFiles ?? HOOK_SPOOL_LIMITS.maxFiles;
  const maxBytes = options.limits?.maxBytes ?? HOOK_SPOOL_LIMITS.maxBytes;
  const nowMs = options.nowMs ?? Date.now();
  const envelope: HookSpoolEnvelope = {
    v: 1,
    source: options.source,
    receivedAt: new Date(nowMs).toISOString(),
    blanked: options.blanked ?? 0,
    body: options.body,
  };
  const content = JSON.stringify(envelope);
  const contentBytes = Buffer.byteLength(content);
  try {
    const directory = hookSpoolDirectory(options.home);
    ensureSpoolDirectory(directory);
    // The bound check is the hook process's one pass over the directory, so it
    // is also where an orphan temporary gets reaped and where the temporaries
    // still in flight are charged to the byte ceiling (review r1, F4).
    const temporaries = reapHookSpoolTemporaries(options.home, nowMs);
    const existing = listHookSpoolFiles(options.home);
    if (existing.length + 1 > maxFiles) return null;
    const usedBytes =
      existing.reduce((total, file) => total + file.bytes, 0) + temporaries.remainingBytes;
    if (usedBytes + contentBytes > maxBytes) return null;
    const name = `${nowMs}-${process.pid}-${crypto.randomBytes(3).toString("hex")}.json`;
    const target = path.join(directory, name);
    const temporary = `${target}.tmp`;
    fs.writeFileSync(temporary, content, { mode: 0o600 });
    fs.chmodSync(temporary, 0o600);
    fs.renameSync(temporary, target);
    return { path: target };
  } catch {
    return null;
  }
}

export type HookSpoolReadResult =
  | { ok: true; envelope: HookSpoolEnvelope }
  | { ok: false; reason: "spool_untrusted" };

/**
 * Read one spooled file under the trust boundary. Envelope shape failures are
 * untrusted (this file was not written by the client), not contract failures:
 * only the `body` string is ever handed to the hook route.
 */
export function readHookSpoolFile(file: string): HookSpoolReadResult {
  let text: string;
  try {
    text = fs.readFileSync(file, "utf8");
  } catch {
    return { ok: false, reason: "spool_untrusted" };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { ok: false, reason: "spool_untrusted" };
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { ok: false, reason: "spool_untrusted" };
  }
  const record = parsed as Record<string, unknown>;
  if (record.v !== 1) return { ok: false, reason: "spool_untrusted" };
  if (!isHookSpoolSource(record.source)) return { ok: false, reason: "spool_untrusted" };
  if (typeof record.body !== "string") return { ok: false, reason: "spool_untrusted" };
  if (typeof record.receivedAt !== "string") return { ok: false, reason: "spool_untrusted" };
  return {
    ok: true,
    envelope: {
      v: 1,
      source: record.source,
      receivedAt: record.receivedAt,
      // A receipt of how much the hook process emptied, not a trust input: the
      // drain hands the route `body` and nothing else either way.
      blanked: counterValue(record.blanked),
      body: record.body,
    },
  };
}

/** Quarantine one spooled file under `rejected/`, naming why in the file name. */
export function rejectHookSpoolFile(
  home: string,
  file: HookSpoolFile,
  reason: HookSpoolRejectionReason,
) {
  const directory = hookSpoolRejectedDirectory(home);
  ensureSpoolDirectory(directory);
  const safeReason = /^[a-z0-9_]+$/.test(reason) ? reason : "spool_untrusted";
  const base = file.name.slice(0, -".json".length);
  const target = path.join(directory, `${base}.${safeReason}.json`);
  try {
    fs.renameSync(file.path, target);
  } catch {
    // A file that cannot be moved must not be replayed forever: drop it rather
    // than let one unmovable entry stall every later event behind it.
    try {
      fs.unlinkSync(file.path);
    } catch {
      /* the next tick re-observes whatever is actually there */
    }
    return null;
  }
  return target;
}

/** Bound the quarantine by age and count; oldest goes first. */
export function pruneHookSpoolRejected(home: string, nowMs = Date.now()) {
  const directory = hookSpoolRejectedDirectory(home);
  let names: string[];
  try {
    names = fs.readdirSync(directory);
  } catch {
    return { deleted: 0, retained: 0 };
  }
  const entries = names
    .map((name) => ({ name, match: REJECTED_FILE_PATTERN.exec(name) }))
    .filter((entry): entry is { name: string; match: RegExpExecArray } => entry.match !== null)
    .map((entry) => ({ name: entry.name, spooledAtMs: Number(entry.match[1]) }))
    .sort((left, right) =>
      left.spooledAtMs - right.spooledAtMs || left.name.localeCompare(right.name),
    );
  const expired = new Set(
    entries
      .filter((entry) => nowMs - entry.spooledAtMs > HOOK_SPOOL_LIMITS.rejectedMaxAgeMs)
      .map((entry) => entry.name),
  );
  const surviving = entries.filter((entry) => !expired.has(entry.name));
  const overflow = new Set(
    surviving
      .slice(0, Math.max(0, surviving.length - HOOK_SPOOL_LIMITS.maxRejectedFiles))
      .map((entry) => entry.name),
  );
  let deleted = 0;
  for (const entry of entries) {
    if (!expired.has(entry.name) && !overflow.has(entry.name)) continue;
    try {
      fs.unlinkSync(path.join(directory, entry.name));
      deleted += 1;
    } catch {
      /* leave it for the next tick */
    }
  }
  return { deleted, retained: entries.length - deleted };
}

export type HookSpoolStatus = HookSpoolCounters & HookSpoolPending & { enabled: boolean };

/**
 * Where an operator surface got its `enabled` reading, since it is not local.
 *
 *   collector             the daemon answered and named its kill-switch state;
 *   collector_too_old     the daemon answered, and healthily, but its /status
 *                         carries no `hookSpool` section — it predates 0.7.22
 *                         (review r2, F3). The spool is filling and will drain
 *                         as soon as that daemon is updated;
 *   collector_unreachable nobody answered, or the answer was not a healthy
 *                         collector's.
 */
export type HookSpoolEnabledSource = "collector" | "collector_too_old" | "collector_unreachable";

/**
 * One daemon reading. `enabled` is a boolean only when `source` is
 * `"collector"`; the other two sources mean the daemon did not say, and the
 * surfaces print `null` rather than guess.
 */
export type HookSpoolDaemonReading = {
  enabled: boolean | null;
  source: HookSpoolEnabledSource;
};

export const HOOK_SPOOL_COLLECTOR_UNREACHABLE: HookSpoolDaemonReading = Object.freeze({
  enabled: null,
  source: "collector_unreachable",
});

export const HOOK_SPOOL_COLLECTOR_TOO_OLD: HookSpoolDaemonReading = Object.freeze({
  enabled: null,
  source: "collector_too_old",
});

export function hookSpoolDaemonEnabled(enabled: boolean): HookSpoolDaemonReading {
  return { enabled, source: "collector" };
}

export type HookSpoolOperatorStatus = HookSpoolCounters &
  HookSpoolPending & { enabled: boolean | null; enabledSource: HookSpoolEnabledSource };

/**
 * The `hookSpool` shape `plimsoll status` and `plimsoll doctor` print.
 *
 * `enabled` is the DAEMON's kill switch, never the invoking shell's (review r1,
 * F5): the real deployment puts `PLIMSOLL_HOOK_SPOOL` in the LaunchAgent, so a
 * shell that reads its own environment reports a state nobody is in. The caller
 * passes what the daemon's own /status said — the value its drain captured at
 * start — or a reading that says why it could not be asked.
 */
export function hookSpoolOperatorStatus(
  home: string,
  daemon: HookSpoolDaemonReading,
  options: { nowMs?: number } = {},
): HookSpoolOperatorStatus {
  const nowMs = options.nowMs ?? Date.now();
  return {
    enabled: daemon.source === "collector" ? daemon.enabled : null,
    enabledSource: daemon.source,
    ...readHookSpoolCounters(home),
    ...hookSpoolPending(home, nowMs),
  };
}

/**
 * Doctor section. Pending files that are still pending ten minutes later mean
 * the drain is not running or cannot write the ledger, which is exactly the
 * state that used to be invisible; doctor says so in words.
 */
export function hookSpoolDoctorSection(
  home: string,
  daemon: HookSpoolDaemonReading,
  options: { nowMs?: number } = {},
) {
  const status = hookSpoolOperatorStatus(home, daemon, options);
  const stalled =
    status.pendingFiles > 0 &&
    status.oldestPendingAgeSeconds !== null &&
    status.oldestPendingAgeSeconds >= HOOK_SPOOL_LIMITS.stalePendingSeconds;
  // `draining` is a claim about the collector, so it needs the collector to be
  // draining (review r2, F5): with the kill switch off, or with a daemon that
  // cannot be asked, nothing is draining this spool whatever the pending age
  // says. The age diagnostic below is unchanged and still fires on its own.
  const draining = status.enabled === true && !stalled;
  // One `note`, whatever combination fired: the stall sentence first because it
  // is the count-and-age fact, then the mixed-version sentence when the daemon
  // that answered predates the drain (review r2, F3).
  const notes = [
    ...(stalled
      ? [
          `${status.pendingFiles} spooled hook event(s) have been waiting ` +
          `${status.oldestPendingAgeSeconds}s (over ${HOOK_SPOOL_LIMITS.stalePendingSeconds}s) ` +
          "— the collector is not draining the hook spool; these events are captured but not yet in the ledger.",
        ]
      : []),
    ...(status.enabledSource === "collector_too_old"
      ? [
          "the collector answered /status without a hookSpool section, so it predates 0.7.22 and " +
          "cannot drain the spool; spooled hook events are held and drain once the collector is updated.",
        ]
      : []),
  ];
  return {
    ...status,
    stalePendingSeconds: HOOK_SPOOL_LIMITS.stalePendingSeconds,
    draining,
    ...(stalled ? { diagnostic: "hook_spool_pending_not_draining" } : {}),
    ...(notes.length > 0 ? { note: notes.join(" ") } : {}),
  };
}
