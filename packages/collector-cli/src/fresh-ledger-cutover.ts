import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

import Database from "better-sqlite3";
import { z } from "zod";

import { LocalEventBuffer } from "./buffer";
import { carriedCaptureCursorMatches, recordReplacementUnseenFileFences,
  type CaptureBaselineFileObservation } from "./capture-baseline";
import { captureRootBaselineFiles, captureRootBaselineObservations,
  rootCursorKey } from "./capture-root-inventory";
import { readAccountAssertionAdapterState, ACCOUNT_ASSERTION_STATE_KEY } from "./account-assertion";
import { readLiveProducerBindings, LIVE_BINDINGS_FILE } from "./codex-live-usage-auth";
import type { CollectorConfig } from "./config";
import { loadJsonlScanCursorByKey, ensureJsonlScanState,
  jsonlScanStateKey } from "./jsonl-byte-tailer";
import { otherProcessesWithFilesOpen } from "./lifecycle-adapters";
import { LifecycleMutationAuthority } from "./lifecycle-authority";
import { validateRolloutParserState } from "./rollout-tailer";
import { validateTranscriptParserState } from "./transcript-tailer";
import { utcWeekStart } from "./weekly-tool-stats";

const MIN_REPLACEMENT_VERSION = "0.7.46";
/** Includes only copied SQLite value payloads, not the 88 GB archive. A
 * 948,890-byte / 20,000-row fixture took 4.6-7.0 s to plan and switch;
 * scaling to 8 MiB stays below the 90-second window at those rates. */
const MAX_CARRIED_VALUE_BYTES = 8 * 1024 * 1024;
const CURSOR_KEY = /^[a-f0-9]{64}$/;
const CURSOR_IDENTITY = /^\d+:\d+:\d+$/;
const LIVE_TABLES = [
  "codex_live_producers",
  "codex_live_bindings",
  "codex_live_pins",
  "codex_live_attachments",
  "codex_live_packet_keys",
  "codex_live_receipts",
  "codex_live_diagnostics",
] as const;
const CARRIED_TABLES = [...LIVE_TABLES, "session_usage_authority"] as const;
const CARRIED_COLUMNS: Record<(typeof CARRIED_TABLES)[number], readonly string[]> = {
  codex_live_producers: ["producer_id", "context_digest", "context_json", "credential_id", "enabled"],
  codex_live_bindings: ["producer_id", "credential_id", "scope_digest", "context_digest",
    "token_sha256", "enrolled_at", "revoked"],
  codex_live_pins: ["source", "session_id", "producer_id", "context_digest", "context_json", "claimed_at"],
  codex_live_attachments: ["scope_digest", "attachment_id", "thread_id", "checkpoint_json", "held_reason"],
  codex_live_packet_keys: ["scope_digest", "kind", "attachment_id", "packet_key", "packet_digest"],
  codex_live_receipts: ["scope_digest", "kind", "attachment_id", "packet_key", "packet_digest", "receipt_json"],
  codex_live_diagnostics: ["disposition", "count", "last_at"],
  session_usage_authority: ["source", "session_id", "authority", "claimed_at"],
};
const STAGE_SUFFIX = ".replacement-stage";

export type FreshLedgerCutoverPlan = {
  status: "ready" | "refused";
  reason: string | null;
  readOnly: true;
  rootCount: number;
  installationEpochId: string | null;
  archiveIdentity: string | null;
  archiveLatestRecordedAt: string | null;
  cursorRows: number;
  carriedRows: Record<string, number>;
  carriedBytes: Record<string, number>;
  totalCarriedBytes: number;
  carryBudgetBytes: number;
  untrackedFileFences: number;
  dueWeeksWithoutAcknowledgement: string[];
  recoveryStagePresent: boolean;
  sidecarsMayAppear: boolean;
  operatorAction: string | null;
  unacknowledgedWeeklyReports: number;
  currentUtcWeekToolAttempts: number;
  priorUtcWeekToolAttempts: number;
  priorWeekReportAcknowledged: boolean;
  nextSafeWindowAt: string | null;
  requiresReportAcknowledgement: boolean;
};

export type ReplacementLedgerMarker = {
  archiveIdentity: string;
  archivePath: string;
  minCollectorVersion: typeof MIN_REPLACEMENT_VERSION;
  switchedAt: string;
  cursorRows: number;
};

type CutoverInput = {
  ledgerPath: string;
  archivePath: string;
  config: CollectorConfig;
  now?: () => Date;
  /** Same authority root as lifecycle mutations for the collector home. */
  authorityRoot?: string;
  /** Fault-injection seam; never used by the CLI. */
  onStep?: (step: "old_locked" | "stage_bound" | "archive_linked" | "switched") => void;
  /** Fault-injection seam for a process kill during a carried-table copy. */
  onCopyRow?: (table: string, copied: number) => void;
};

function valueBytes(row: Record<string, unknown>): number {
  return Object.values(row).reduce<number>((total, value) => total +
    (typeof value === "string" ? Buffer.byteLength(value) : Buffer.isBuffer(value)
      ? value.byteLength : typeof value === "number" || typeof value === "bigint" ? 8 : 0), 0);
}

function hasTable(db: Database.Database, table: string): boolean {
  return Boolean(db.prepare("select 1 from sqlite_master where type='table' and name=?").get(table));
}

function columns(db: Database.Database, table: string): string[] {
  return (db.pragma(`table_info(${table})`) as Array<{ name: string }>).map(row => row.name);
}

function nextWeekAt(now: Date): string {
  const next = new Date(`${utcWeekStart(now)}T00:00:00.000Z`);
  next.setUTCDate(next.getUTCDate() + 7);
  return next.toISOString();
}

function latestRecordedTime(db: Database.Database, cursorLatest: string | null): string | null {
  const times: string[] = cursorLatest ? [cursorLatest] : [];
  const select = (table: string, column: string) => {
    if (!hasTable(db, table) || !columns(db, table).includes(column)) return;
    const row = db.prepare(`select max(${column}) as at from ${table}`).get() as { at: unknown };
    if (row.at !== null) {
      if (typeof row.at !== "string" || !Number.isFinite(Date.parse(row.at))) {
        throw new Error("archive_time_invalid");
      }
      times.push(row.at);
    }
  };
  // The large event/fact queries use existing time indexes. Cursor rows are
  // already streamed for validity below, so their scan time is accumulated.
  select("buffered_events", "observed_at");
  select("buffered_events", "created_at");
  select("metric_samples", "created_at");
  select("tool_attempt_facts", "started_at");
  select("collector_workspace_binding", "changed_at");
  select("collector_workspace_binding", "current_installation_epoch_started_at");
  select("maintenance_state", "updated_at");
  select("codex_live_bindings", "enrolled_at");
  return times.length ? times.reduce((latest, at) =>
    Date.parse(at) > Date.parse(latest) ? at : latest) : null;
}

function inspectCursors(db: Database.Database, staleFileKeys: ReadonlySet<string>) {
  if (!hasTable(db, "rollout_scan_state")) return { rows: 0, bytes: 0, latest: null as string | null };
  const required = [
    "file", "size", "scanned_at", "committed_offset", "deferred_bytes",
    "file_identity", "head_hash", "head_bytes", "continuity_hash", "continuity_bytes",
    "mtime_ms", "ctime_ms", "work_remaining", "unresolved_kind", "unresolved_offset",
    "unresolved_observed_bytes", "unresolved_available_bytes", "unresolved_byte_budget",
    "parser_kind", "checkpoint_version", "parser_state_json",
  ];
  if (required.some(name => !columns(db, "rollout_scan_state").includes(name))) {
    throw new Error("archive_cursor_schema_unreadable");
  }
  let rows = 0;
  let bytes = 0;
  let latest: string | null = null;
  for (const row of db.prepare("select * from rollout_scan_state").iterate() as Iterable<Record<string, unknown>>) {
    const scannedAt = row.scanned_at, parserKind = row.parser_kind,
      checkpointVersion = row.checkpoint_version;
    if (typeof row.file !== "string" || !CURSOR_KEY.test(row.file) ||
        typeof scannedAt !== "string" || !Number.isFinite(Date.parse(scannedAt))) {
      throw new Error("archive_cursor_state_inconsistent");
    }
    const cursor = parserKind === "codex-rollout-v2" && checkpointVersion === 2
      ? loadJsonlScanCursorByKey(db, row.file, "codex-rollout-v2", 2, validateRolloutParserState)
      : parserKind === "claude-transcript-v3" && checkpointVersion === 3
        ? loadJsonlScanCursorByKey(db, row.file, "claude-transcript-v3", 3, validateTranscriptParserState)
        : undefined;
    if (!cursor || cursor.checkpointStatus !== "valid" || !cursor.fileIdentity ||
        !CURSOR_IDENTITY.test(cursor.fileIdentity) || cursor.committedOffset === null) {
      throw new Error("archive_cursor_state_inconsistent");
    }
    if (!latest || Date.parse(scannedAt) > Date.parse(latest)) latest = scannedAt;
    if (!staleFileKeys.has(row.file)) {
      rows += 1;
      bytes += valueBytes(row);
    }
  }
  return { rows, bytes, latest };
}

function inspectLiveState(db: Database.Database, home: string) {
  let registry: ReturnType<typeof readLiveProducerBindings> | { bindings: [] };
  try {
    registry = fs.existsSync(path.join(home, LIVE_BINDINGS_FILE))
      ? readLiveProducerBindings(home) : { bindings: [] };
    readAccountAssertionAdapterState(db);
  } catch {
    throw new Error("archive_live_authorization_unreadable");
  }
  const active = registry.bindings.filter(binding => binding.enabled);
  if (active.length && (!hasTable(db, "codex_live_producers") ||
      !hasTable(db, "codex_live_bindings"))) throw new Error("archive_live_binding_unreadable");
  for (const binding of active) {
    const row = db.prepare(`select p.enabled,p.credential_id as credentialId,b.revoked,
      b.token_sha256 as tokenSha256,b.enrolled_at as enrolledAt
      from codex_live_producers p join codex_live_bindings b using(producer_id)
      where b.producer_id=? and b.credential_id=?`).get(binding.producerId, binding.credentialId) as
      { enabled: number; credentialId: string; revoked: number; tokenSha256: string; enrolledAt: string } | undefined;
    if (!row || !row.enabled || row.revoked || row.credentialId !== binding.credentialId ||
        row.tokenSha256 !== binding.tokenSha256 || row.enrolledAt !== binding.enrolledAt) {
      throw new Error("archive_live_binding_inconsistent");
    }
  }
  const carriedRows: Record<string, number> = {};
  const carriedBytes: Record<string, number> = {};
  try {
    for (const table of CARRIED_TABLES) {
      if (!hasTable(db, table)) { carriedRows[table] = 0; carriedBytes[table] = 0; continue; }
      const available = columns(db, table);
      if (CARRIED_COLUMNS[table].some(column => !available.includes(column))) {
        throw new Error("archive_live_authorization_unreadable");
      }
      let rows = 0;
      let bytes = 0;
      // Read every carried row now, not just the B-tree count. A damaged
      // authorization row must refuse the read-only plan, not fail at switch.
      for (const row of db.prepare(`select * from ${table}`).iterate() as Iterable<Record<string, unknown>>) {
        rows += 1;
        bytes += valueBytes(row);
      }
      carriedRows[table] = rows;
      carriedBytes[table] = bytes;
    }
  } catch {
    throw new Error("archive_live_authorization_unreadable");
  }
  const assertion = hasTable(db, "maintenance_state")
    ? db.prepare("select * from maintenance_state where key=?").get(ACCOUNT_ASSERTION_STATE_KEY) as
      Record<string, unknown> | undefined : undefined;
  carriedRows[ACCOUNT_ASSERTION_STATE_KEY] = Number(Boolean(assertion));
  carriedBytes[ACCOUNT_ASSERTION_STATE_KEY] = assertion ? valueBytes(assertion) : 0;
  return { carriedRows, carriedBytes };
}

function inspectArchive(db: Database.Database, input: CutoverInput, oldStat: fs.Stats,
  staleFileKeys: ReadonlySet<string>) {
  const binding = hasTable(db, "collector_workspace_binding")
    ? db.prepare(`select current_workspace_id as workspaceId,current_device_id as deviceId,
        current_installation_epoch_id as epochId
        from collector_workspace_binding where singleton=1`).get() as
      { workspaceId: string; deviceId: string | null; epochId: string | null } | undefined
    : undefined;
  if (!binding || binding.workspaceId !== input.config.tenantId ||
      binding.deviceId !== input.config.deviceId) throw new Error("archive_identity_mismatch");
  if (binding.epochId !== rootEpoch(input)) throw new Error("archive_epoch_mismatch");
  if (hasTable(db, "collector_replacement_ledger")) throw new Error("replacement_ledger_already_active");
  const cursors = inspectCursors(db, staleFileKeys);
  const live = inspectLiveState(db, path.dirname(input.ledgerPath));
  const carriedRows = { rollout_scan_state: cursors.rows, ...live.carriedRows };
  const carriedBytes = { rollout_scan_state: cursors.bytes, ...live.carriedBytes };
  const totalCarriedBytes = Object.values(carriedBytes).reduce((sum, bytes) => sum + bytes, 0);
  const latest = latestRecordedTime(db, cursors.latest);
  const now = (input.now ?? (() => new Date()))();
  if (!Number.isFinite(now.getTime())) throw new Error("cutover_clock_invalid");
  if (latest && now.getTime() < Date.parse(latest)) throw new Error("archive_clock_regressed");
  const week = utcWeekStart(now);
  const next = nextWeekAt(now);
  const prior = new Date(`${week}T00:00:00.000Z`);
  prior.setUTCDate(prior.getUTCDate() - 7);
  const priorWeek = prior.toISOString().slice(0, 10);
  const pending = hasTable(db, "weekly_tool_stats_uploads")
    ? (db.prepare("select count(*) as n from weekly_tool_stats_uploads where delivered=0").get() as { n: number }).n : 0;
  const attempts = hasTable(db, "tool_attempt_facts")
    ? (db.prepare(`select count(*) as n from tool_attempt_facts
        where started_at>=? and started_at<?`).get(`${week}T00:00:00.000Z`, next) as { n: number }).n : 0;
  const priorAttempts = hasTable(db, "tool_attempt_facts")
    ? (db.prepare(`select count(*) as n from tool_attempt_facts
        where started_at>=? and started_at<?`).get(`${priorWeek}T00:00:00.000Z`,
          `${week}T00:00:00.000Z`) as { n: number }).n : 0;
  const priorAcknowledged = priorAttempts === 0 || (hasTable(db, "weekly_tool_stats_uploads") &&
    Boolean(db.prepare(`select 1 from weekly_tool_stats_uploads
      where week_start=? and delivered=1 limit 1`).get(priorWeek)));
  const dueWeeksWithoutAcknowledgement: string[] = [];
  const first = hasTable(db, "weekly_tool_stats_control")
    ? db.prepare(`select first_week as firstWeek from weekly_tool_stats_control
        where workspace_id=? and device_id=?`).get(input.config.tenantId, input.config.deviceId) as
        { firstWeek: string } | undefined : undefined;
  if (first) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(first.firstWeek) ||
        utcWeekStart(new Date(`${first.firstWeek}T00:00:00.000Z`)) !== first.firstWeek) {
      throw new Error("weekly_first_week_invalid");
    }
    const weekCursor = new Date(`${first.firstWeek}T00:00:00.000Z`);
    const count = db.prepare(`select count(*) as n from tool_attempt_facts
      where started_at>=? and started_at<?`);
    const acknowledged = hasTable(db, "weekly_tool_stats_uploads")
      ? db.prepare(`select 1 from weekly_tool_stats_uploads where week_start=? and delivered=1 limit 1`)
      : null;
    while (weekCursor.toISOString().slice(0, 10) < week) {
      const day = weekCursor.toISOString().slice(0, 10);
      weekCursor.setUTCDate(weekCursor.getUTCDate() + 7);
      if ((count.get(`${day}T00:00:00.000Z`, weekCursor.toISOString()) as { n: number }).n > 0 &&
          !acknowledged?.get(day)) dueWeeksWithoutAcknowledgement.push(day);
    }
  }
  const archiveIdentity = fileIdentityDigest(input.ledgerPath, oldStat,
    input.config.tenantId, input.config.deviceId, path.resolve(input.archivePath));
  return { archiveIdentity, latest, cursorRows: cursors.rows, carriedRows, carriedBytes,
    totalCarriedBytes, dueWeeksWithoutAcknowledgement,
    pending, attempts, priorAttempts, priorAcknowledged,
    nextSafeWindowAt: attempts ? next : pending || !priorAcknowledged || dueWeeksWithoutAcknowledgement.length
      ? now.toISOString() : null };
}

function fileIdentityDigest(file: string, stat: fs.Stats, workspace: string,
  device: string, archivePath: string): string {
  // Do not open a second descriptor on this inode while SQLite holds its
  // POSIX lock: closing that descriptor would release the process-wide lock.
  // The separate archive manifest supplies the full-file checksum.
  void file;
  return crypto.createHash("sha256").update(JSON.stringify([
    stat.dev, stat.ino, stat.size, stat.mtimeMs, workspace, device, archivePath,
  ])).digest("hex");
}

function rootEpoch(input: CutoverInput): string | null {
  const epochs = new Set((input.config.captureRoots ?? []).map(root => root.installationEpochId));
  if (epochs.size !== 1) throw new Error(epochs.size ? "capture_root_epochs_conflict" : "capture_roots_required");
  const epoch = [...epochs][0]!;
  if (!z.string().uuid().safeParse(epoch).success) {
    throw new Error("installation_epoch_id_invalid");
  }
  return epoch;
}

function assertPrivateDirectory(directory: string, reason: string): fs.Stats {
  const absolute = path.resolve(directory);
  let current = path.parse(absolute).root;
  for (const part of absolute.slice(current.length).split(path.sep).filter(Boolean)) {
    current = path.join(current, part);
    const stat = fs.lstatSync(current);
    if (!stat.isDirectory() || stat.isSymbolicLink() ||
        (stat.uid !== process.getuid?.() && stat.uid !== 0) ||
        (stat.mode & 0o022) !== 0) throw new Error(`${reason}:unsafe_path_component:${current}`);
  }
  const leaf = fs.lstatSync(absolute);
  if (leaf.uid !== process.getuid?.() || (leaf.mode & 0o777) !== 0o700) {
    throw new Error(`${reason}:directory_must_be_owned_0700:${absolute}`);
  }
  return leaf;
}

function stageArtifactPresent(stage: string): boolean {
  const artifacts = [stage, `${stage}-wal`, `${stage}-shm`].filter(file => {
    try { fs.lstatSync(file); return true; }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
      throw error;
    }
  });
  for (const artifact of artifacts) {
    const stat = fs.lstatSync(artifact);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.uid !== process.getuid?.()) {
      throw new Error("staging_artifact_unsafe");
    }
  }
  return artifacts.length > 0;
}

function assertPaths(input: CutoverInput) {
  const ledger = path.resolve(input.ledgerPath), archive = path.resolve(input.archivePath);
  if (!path.isAbsolute(input.ledgerPath) || !path.isAbsolute(input.archivePath) ||
      ledger === archive || path.dirname(ledger) === path.dirname(archive)) {
    throw new Error("archive_path_invalid");
  }
  const old = fs.lstatSync(ledger);
  if (!old.isFile() || old.isSymbolicLink()) throw new Error("ledger_not_regular");
  assertPrivateDirectory(path.dirname(ledger), "ledger_directory_unsafe");
  const directory = assertPrivateDirectory(path.dirname(archive), "archive_directory_unsafe");
  if (directory.dev !== old.dev ||
      fs.realpathSync(path.dirname(archive)) !== path.dirname(archive)) {
    throw new Error("archive_directory_unsafe");
  }
  const prelinked = fs.existsSync(archive);
  if (prelinked) {
    const linked = fs.lstatSync(archive);
    if (!linked.isFile() || linked.isSymbolicLink() || linked.dev !== old.dev ||
        linked.ino !== old.ino) throw new Error("archive_path_exists");
  } else if (fs.existsSync(`${archive}-wal`) || fs.existsSync(`${archive}-shm`)) {
    throw new Error("archive_path_exists");
  }
  // A checkpointed WAL may retain its 32-byte header, and the shared-memory
  // index normally retains 32 KiB. Only WAL frames can contain unmerged data.
  if (prelinked && fs.existsSync(`${archive}-wal`) &&
      fs.statSync(`${archive}-wal`).size > 32) {
    throw new Error("archive_sidecar_nonempty");
  }
  // A killed copy leaves a partial stage. Plan observes it without changing
  // any file; switch removes only these owned artifacts under the lease and
  // SQLite exclusive lock, after proving the active inode is still the old one.
  stageArtifactPresent(`${ledger}${STAGE_SUFFIX}`);
  return old;
}

function untrackedRootFiles(db: Database.Database, input: CutoverInput): Array<{
  source: "codex" | "claude_code"; fileKey: string;
  observation: CaptureBaselineFileObservation;
}> {
  const result: Array<{ source: "codex" | "claude_code";
    fileKey: string; observation: CaptureBaselineFileObservation }> = [];
  const cursor = hasTable(db, "rollout_scan_state")
    ? db.prepare("select file_identity as fileIdentity from rollout_scan_state where file=?") : null;
  const roots = input.config.captureRoots ?? [];
  for (const root of roots) {
    const files = captureRootBaselineFiles(root.source, root.directory);
    if (files.errors) throw new Error("capture_root_file_inventory_unreadable");
    const observed = captureRootBaselineObservations(files.files);
    if (observed.errors) throw new Error("capture_root_file_stat_unreadable");
    for (const observation of observed.observations) {
      const key = jsonlScanStateKey(rootCursorKey(roots, observation.path));
      const row = cursor?.get(key) as { fileIdentity: string } | undefined;
      const identity = `${observation.device}:${observation.inode}:${observation.birthtimeNs}`;
      if (row?.fileIdentity !== identity) result.push({ source: root.source,
        fileKey: key, observation });
    }
  }
  return result;
}

function openReadOnlyPlanDatabase(ledgerPath: string): {
  db: Database.Database; sidecarsMayAppear: boolean;
} {
  const files = [ledgerPath, `${ledgerPath}-wal`, `${ledgerPath}-shm`].filter(fs.existsSync);
  const check = spawnSync("/usr/sbin/lsof", ["-S", "2", "-t", "-w", "--", ...files], {
    encoding: "utf8", timeout: 60_000,
    env: { PATH: "/usr/bin:/bin:/usr/sbin" }, stdio: ["ignore", "pipe", "pipe"],
  });
  const provenClosed = !check.error && check.status === 1 &&
    !(check.stdout ?? "").trim() && !(check.stderr ?? "").trim();
  if (provenClosed) {
    if (fs.existsSync(`${ledgerPath}-wal`) && fs.statSync(`${ledgerPath}-wal`).size > 32) {
      throw new Error("closed_ledger_wal_requires_checkpoint_before_immutable_plan");
    }
    // better-sqlite3 does not enable SQLITE_OPEN_URI. Node's built-in SQLite
    // does, allowing an immutable read of a closed, checkpointed WAL ledger
    // without creating WAL/SHM sidecars.
    const SQLite = createRequire(import.meta.url)("node:sqlite") as {
      DatabaseSync: new (file: string, options: { readOnly: boolean }) => {
        prepare(sql: string): { all(...params: unknown[]): unknown[];
          get(...params: unknown[]): unknown; iterate(...params: unknown[]): Iterable<unknown> };
        close(): void;
      };
    };
    const native = new SQLite.DatabaseSync(`${pathToFileURL(ledgerPath).href}?immutable=1`,
      { readOnly: true });
    const db = {
      prepare: (sql: string) => native.prepare(sql),
      pragma: (sql: string) => native.prepare(`pragma ${sql}`).all(),
      close: () => native.close(),
    } as unknown as Database.Database;
    return { db, sidecarsMayAppear: false };
  }
  return { db: new Database(ledgerPath, { readonly: true, fileMustExist: true, timeout: 0 }),
    sidecarsMayAppear: true };
}

/** Read-only preflight. The switch repeats every check while holding the
 * original inode exclusively; a prior plan is never an authorization token. */
export function planFreshLedgerCutover(input: CutoverInput): FreshLedgerCutoverPlan {
  const roots = input.config.captureRoots ?? [];
  const base = {
    readOnly: true as const, rootCount: roots.length, installationEpochId: null as string | null,
    archiveIdentity: null as string | null, archiveLatestRecordedAt: null as string | null,
    cursorRows: 0, carriedRows: {} as Record<string, number>,
    carriedBytes: {} as Record<string, number>, totalCarriedBytes: 0,
    carryBudgetBytes: MAX_CARRIED_VALUE_BYTES, untrackedFileFences: 0,
    dueWeeksWithoutAcknowledgement: [] as string[], recoveryStagePresent: false,
    sidecarsMayAppear: false, operatorAction: null as string | null,
    unacknowledgedWeeklyReports: 0, currentUtcWeekToolAttempts: 0,
    priorUtcWeekToolAttempts: 0, priorWeekReportAcknowledged: true,
    nextSafeWindowAt: null as string | null, requiresReportAcknowledgement: false,
  };
  try {
    const epoch = rootEpoch(input);
    const old = assertPaths(input);
    const { db, sidecarsMayAppear } = openReadOnlyPlanDatabase(input.ledgerPath);
    try {
      if (sidecarsMayAppear) db.pragma("query_only = ON");
      const untracked = untrackedRootFiles(db, input);
      const inspection = inspectArchive(db, input, old,
        new Set(untracked.map(row => row.fileKey)));
      const fields = { ...base, installationEpochId: epoch,
        archiveIdentity: inspection.archiveIdentity, archiveLatestRecordedAt: inspection.latest,
        cursorRows: inspection.cursorRows, carriedRows: inspection.carriedRows,
        carriedBytes: inspection.carriedBytes, totalCarriedBytes: inspection.totalCarriedBytes,
        untrackedFileFences: untracked.length,
        dueWeeksWithoutAcknowledgement: inspection.dueWeeksWithoutAcknowledgement,
        recoveryStagePresent: stageArtifactPresent(`${input.ledgerPath}${STAGE_SUFFIX}`),
        sidecarsMayAppear,
        unacknowledgedWeeklyReports: inspection.pending,
        currentUtcWeekToolAttempts: inspection.attempts,
        priorUtcWeekToolAttempts: inspection.priorAttempts,
        priorWeekReportAcknowledged: inspection.priorAcknowledged,
        nextSafeWindowAt: inspection.nextSafeWindowAt,
        requiresReportAcknowledgement: inspection.pending > 0 || !inspection.priorAcknowledged ||
          inspection.dueWeeksWithoutAcknowledgement.length > 0 };
      if (inspection.totalCarriedBytes > MAX_CARRIED_VALUE_BYTES) return {
        ...fields, status: "refused", reason: "carried_state_exceeds_8_mib_budget",
        operatorAction: "Reduce carried authorization/cursor state before scheduling the 90-second window.",
      };
      if (inspection.pending) return { ...fields, status: "refused", reason: "weekly_report_unacknowledged" };
      if (inspection.attempts) return { ...fields, status: "refused", reason: "current_utc_week_tool_attempts" };
      if (!inspection.priorAcknowledged) return { ...fields, status: "refused",
        reason: "prior_week_report_not_acknowledged",
        operatorAction: `Run the weekly tool-stats upload first for ${inspection.dueWeeksWithoutAcknowledgement.join(", ") || "the prior UTC week"}.` };
      if (inspection.dueWeeksWithoutAcknowledgement.length) return {
        ...fields, status: "refused", reason: "due_week_reports_unacknowledged",
        operatorAction: `Run the weekly tool-stats upload first for ${inspection.dueWeeksWithoutAcknowledgement.join(", ")}; cut over early in a later UTC week after acknowledgement.`,
      };
      return { ...fields, status: "ready", reason: null };
    } finally { db.close(); }
  } catch (error) {
    return { ...base, status: "refused", reason: error instanceof Error ? error.message : "archive_unreadable" };
  }
}

function copyTable(source: Database.Database, target: Database.Database, table: string,
  heartbeat: () => void, onCopyRow?: CutoverInput["onCopyRow"]): number {
  if (!hasTable(source, table)) return 0;
  const targetColumns = columns(target, table);
  if (targetColumns.some(column => !columns(source, table).includes(column))) {
    throw new Error(`archive_${table}_schema_unreadable`);
  }
  const names = targetColumns.map(name => `"${name.replaceAll('"', '""')}"`).join(",");
  const insert = target.prepare(`insert into ${table} (${names}) values (${targetColumns.map(() => "?").join(",")})`);
  let copied = 0;
  for (const row of source.prepare(`select ${names} from ${table}`).iterate() as Iterable<Record<string, unknown>>) {
    insert.run(...targetColumns.map(column => row[column]));
    copied += 1;
    onCopyRow?.(table, copied);
    if (copied % 128 === 0) heartbeat();
  }
  heartbeat();
  return copied;
}

function fsyncDirectory(directory: string) {
  const fd = fs.openSync(directory, fs.constants.O_RDONLY);
  try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
}

function removeOwnedStage(stage: string) {
  for (const file of [stage, `${stage}-wal`, `${stage}-shm`]) {
    if (fs.existsSync(file)) fs.unlinkSync(file);
  }
}

/** Prepare a fully bound replacement beside the old ledger, then atomically
 * replace the active pathname. A hard link preserves the quiesced old inode
 * at the archive path without copying the 88 GB history. */
export function switchFreshLedger(input: CutoverInput): FreshLedgerCutoverPlan {
  const first = planFreshLedgerCutover(input);
  if (first.status !== "ready") throw new Error(first.reason ?? "cutover_refused");
  const authority = new LifecycleMutationAuthority(input.authorityRoot ??
    path.join(path.dirname(input.ledgerPath), "lifecycle-authority"));
  const acquired = authority.acquire();
  if (acquired.kind !== "acquired") throw new Error("cutover_lifecycle_authority_unavailable");
  const lease = acquired.lease;
  let old: Database.Database | null = null;
  let staged: LocalEventBuffer | null = null;
  let replacementLock: Database.Database | null = null;
  const stage = `${input.ledgerPath}${STAGE_SUFFIX}`;
  let switched = false;
  const heartbeat = () => {
    if (!lease.renew().ok) throw new Error("cutover_lifecycle_authority_lost");
  };
  try {
    old = new Database(input.ledgerPath, { fileMustExist: true, timeout: 0 });
    old.pragma("locking_mode = EXCLUSIVE");
    old.exec("BEGIN EXCLUSIVE; COMMIT");
    const others = otherProcessesWithFilesOpen([input.ledgerPath,
      `${input.ledgerPath}-wal`, `${input.ledgerPath}-shm`, input.archivePath,
      `${input.archivePath}-wal`, `${input.archivePath}-shm`]);
    if (others === null || others.length) throw new Error("ledger_quiescence_unproven");
    if (old.pragma("journal_mode", { simple: true }) === "wal") {
      old.pragma("checkpoint_fullfsync = ON");
      const [checkpoint] = old.pragma("wal_checkpoint(TRUNCATE)") as Array<{ busy: number; log: number }>;
      if (!checkpoint || checkpoint.busy || checkpoint.log) throw new Error("archive_wal_not_checkpointed");
    }
    // Keep the old inode locked in its existing journal mode throughout the
    // swap. Changing journal mode here would briefly release SQLite's lock.
    old.exec("BEGIN EXCLUSIVE");
    input.onStep?.("old_locked");
    // A SIGKILL during the previous carry may have left only our incomplete
    // stage. The active old inode and lifecycle lease are now proved stable.
    if (stageArtifactPresent(stage)) removeOwnedStage(stage);
    const epoch = rootEpoch(input)!;
    const stat = assertPaths(input);
    // One checked time is both the admission fence and the marker time.
    // A clock adjustment between inspection and first open cannot move the
    // replacement's cutoff behind the archive.
    const switchNow = (input.now ?? (() => new Date()))();
    const untracked = untrackedRootFiles(old, input);
    const staleFileKeys = new Set(untracked.map(row => row.fileKey));
    const inspection = inspectArchive(old, { ...input, now: () => switchNow }, stat,
      staleFileKeys);
    if (inspection.totalCarriedBytes > MAX_CARRIED_VALUE_BYTES) {
      throw new Error("carried_state_exceeds_8_mib_budget");
    }
    if (inspection.pending) throw new Error("weekly_report_unacknowledged");
    if (inspection.attempts) throw new Error("current_utc_week_tool_attempts");
    if (!inspection.priorAcknowledged) throw new Error("prior_week_report_not_acknowledged");
    if (inspection.dueWeeksWithoutAcknowledgement.length) {
      throw new Error(`due_week_reports_unacknowledged:${inspection.dueWeeksWithoutAcknowledgement.join(",")}`);
    }
    heartbeat();
    staged = new LocalEventBuffer(stage, { workspaceId: input.config.tenantId,
      deviceId: input.config.deviceId, freshCaptureRootEpoch: epoch, enrollmentNow: () => switchNow });
    const target = staged.database;
    ensureJsonlScanState(target);
    target.exec(`create table replacement_capture_cursors (
      file_key text primary key, source text not null check(source in ('codex','claude_code')),
      file_identity text not null, committed_offset integer not null check(committed_offset>=0)
    ) without rowid;
    create table collector_replacement_ledger (
      singleton integer primary key check(singleton=1), archive_identity text not null,
      archive_path text not null, min_version text not null, switched_at text not null,
      cursor_rows integer not null
    );`);
    target.transaction(() => {
      if (hasTable(old!, "rollout_scan_state")) {
        const cursorColumns = columns(target, "rollout_scan_state");
        const names = cursorColumns.map(name => `"${name.replaceAll('"', '""')}"`).join(",");
        const insert = target.prepare(`insert into rollout_scan_state (${names})
          values (${cursorColumns.map(() => "?").join(",")})`);
        const mark = target.prepare(`insert into replacement_capture_cursors
          (file_key,source,file_identity,committed_offset) values (?,?,?,?)`);
        let copied = 0;
        for (const row of old!.prepare(`select ${names} from rollout_scan_state`).iterate() as
          Iterable<Record<string, unknown>>) {
          // A path can now point at a different inode. Its archive cursor is
          // still validated above, but carrying it would prevent the new
          // generation's fenced growth from starting at the switch size.
          if (staleFileKeys.has(row.file as string)) continue;
          insert.run(...cursorColumns.map(column => row[column]));
          mark.run(row.file, row.parser_kind === "codex-rollout-v2" ? "codex" : "claude_code",
            row.file_identity, row.committed_offset);
          copied += 1;
          input.onCopyRow?.("rollout_scan_state", copied);
          if (copied % 128 === 0) heartbeat();
        }
        heartbeat();
      }
      for (const table of CARRIED_TABLES) copyTable(old!, target, table, heartbeat, input.onCopyRow);
      for (const source of ["codex", "claude_code"] as const) {
        recordReplacementUnseenFileFences(target, source,
          untracked.filter(row => row.source === source).map(row => row.observation));
        heartbeat();
      }
      if (hasTable(old!, "maintenance_state")) {
        const row = old!.prepare("select key,value,updated_at from maintenance_state where key=?")
          .get(ACCOUNT_ASSERTION_STATE_KEY) as { key: string; value: string; updated_at: string } | undefined;
        if (row) target.prepare("insert into maintenance_state(key,value,updated_at) values(?,?,?)")
          .run(row.key, row.value, row.updated_at);
      }
      target.prepare(`insert into collector_replacement_ledger
        (singleton,archive_identity,archive_path,min_version,switched_at,cursor_rows)
        values(1,?,?,?,?,?)`).run(inspection.archiveIdentity, path.resolve(input.archivePath),
          MIN_REPLACEMENT_VERSION, switchNow.toISOString(), inspection.cursorRows);
    }).immediate();
    target.pragma("wal_checkpoint(TRUNCATE)");
    target.pragma("journal_mode = DELETE");
    staged.close(); staged = null;
    fs.chmodSync(stage, 0o600);
    for (const sidecar of [`${stage}-wal`, `${stage}-shm`]) {
      if (fs.existsSync(sidecar)) fs.unlinkSync(sidecar);
    }
    replacementLock = new Database(stage, { fileMustExist: true, timeout: 0 });
    replacementLock.pragma("locking_mode = EXCLUSIVE");
    replacementLock.exec("BEGIN EXCLUSIVE");
    input.onStep?.("stage_bound");
    heartbeat();
    // The old WAL is checkpointed; any empty sidecars follow its inode to
    // the archive. The new inode remains locked until the old one closes.
    // chmod is on the old inode: a retained hard link can never expose the
    // pre-cutover 0644 SQLite mode, including across a crash after link.
    fs.chmodSync(input.ledgerPath, 0o600);
    if (!fs.existsSync(input.archivePath)) fs.linkSync(input.ledgerPath, input.archivePath);
    for (const suffix of ["-wal", "-shm"] as const) {
      const source = `${input.ledgerPath}${suffix}`;
      if (fs.existsSync(source)) {
        fs.chmodSync(source, 0o600);
        fs.renameSync(source, `${input.archivePath}${suffix}`);
      }
    }
    fsyncDirectory(path.dirname(input.archivePath));
    input.onStep?.("archive_linked");
    heartbeat();
    fs.renameSync(stage, input.ledgerPath);
    fsyncDirectory(path.dirname(input.ledgerPath));
    switched = true;
    input.onStep?.("switched");
    return { ...first, archiveIdentity: inspection.archiveIdentity,
      archiveLatestRecordedAt: inspection.latest, untrackedFileFences: untracked.length };
  } finally {
    staged?.close();
    if (old?.inTransaction) old.exec("COMMIT");
    old?.close();
    if (replacementLock?.inTransaction) replacementLock.exec("COMMIT");
    replacementLock?.close();
    if (!switched) removeOwnedStage(stage);
    lease.release();
  }
}

export function readReplacementLedgerMarker(ledgerPath: string): ReplacementLedgerMarker | null {
  if (!fs.existsSync(ledgerPath)) return null;
  const db = new Database(ledgerPath, { readonly: true, fileMustExist: true, timeout: 0 });
  try {
    if (!hasTable(db, "collector_replacement_ledger")) return null;
    const row = db.prepare(`select archive_identity as archiveIdentity,archive_path as archivePath,
      min_version as minCollectorVersion,switched_at as switchedAt,cursor_rows as cursorRows
      from collector_replacement_ledger where singleton=1`).get() as ReplacementLedgerMarker | undefined;
    if (!row || !CURSOR_KEY.test(row.archiveIdentity) || !path.isAbsolute(row.archivePath) ||
        row.minCollectorVersion !== MIN_REPLACEMENT_VERSION ||
        !Number.isFinite(Date.parse(row.switchedAt)) ||
        !Number.isSafeInteger(row.cursorRows) || row.cursorRows < 0) {
      throw new Error("replacement_ledger_marker_invalid");
    }
    return row;
  } finally { db.close(); }
}

function foldReplacementWeeklyFacts(replacement: Database.Database, restored: Database.Database,
  switchedAt: string, heartbeat: () => void): number {
  const firstWeek = `${utcWeekStart(new Date(switchedAt))}T00:00:00.000Z`;
  const pending = replacement.prepare(`select week_start as weekStart
    from weekly_tool_stats_uploads where week_start>=? limit 1`).get(firstWeek.slice(0, 10)) as
    { weekStart: string } | undefined;
  // A frozen upload may already have reached the cloud. Refuse before the
  // pathname swap rather than publish a partial week after restore.
  if (pending) throw new Error(`restore_weekly_report_reconcile_required:${pending.weekStart}`);
  const copy = (table: string, where: string, params: unknown[]) => {
    const names = columns(restored, table).map(name => `"${name.replaceAll('"', '""')}"`).join(",");
    const targetColumns = columns(restored, table);
    if (targetColumns.some(name => !columns(replacement, table).includes(name))) {
      throw new Error(`restore_${table}_schema_mismatch`);
    }
    const insert = restored.prepare(`insert or ignore into ${table} (${names})
      values (${targetColumns.map(() => "?").join(",")})`);
    const existing = restored.prepare(`select ${names} from ${table} where ${table === "buffered_events"
      ? "id=?" : "operation_id=?"}`);
    let count = 0;
    for (const row of replacement.prepare(`select ${names} from ${table} where ${where}`)
      .iterate(...params) as Iterable<Record<string, unknown>>) {
      const values = targetColumns.map(name => row[name]);
      const inserted = insert.run(...values);
      if (!inserted.changes) {
        const prior = existing.get(row[table === "buffered_events" ? "id" : "operation_id"]) as
          Record<string, unknown> | undefined;
        if (!prior || JSON.stringify(prior) !== JSON.stringify(row)) {
          throw new Error(`restore_${table}_identity_conflict`);
        }
      }
      count += 1;
      if (count % 128 === 0) heartbeat();
    }
    heartbeat();
    return count;
  };
  const events = copy("buffered_events", `observed_at>=? or id in (
    select d.event_id from tool_stat_attempt_dimensions d
    join tool_attempt_facts a on a.operation_id=d.operation_id where a.started_at>=?)`,
    [firstWeek, firstWeek]);
  copy("tool_attempt_facts", "started_at>=?", [firstWeek]);
  copy("tool_stat_attempt_dimensions", `operation_id in (
    select operation_id from tool_attempt_facts where started_at>=?)`, [firstWeek]);
  return events;
}

/** Restore the archived image by an APFS clone while retaining both originals.
 * The active pathname is replaced atomically, so even a mistakenly started old
 * runtime can only see a complete, original ledger after this command returns. */
export function restoreArchivedLedger(input: {
  ledgerPath: string; archivePath: string; freshAttemptPath: string; authorityRoot?: string;
}): { archiveIdentity: string; freshAttemptPath: string; archivePreserved: true } {
  if (![input.ledgerPath, input.archivePath, input.freshAttemptPath].every(path.isAbsolute) ||
      new Set([input.ledgerPath, input.archivePath, input.freshAttemptPath]
        .map(file => path.resolve(file))).size !== 3) {
    throw new Error("restore_path_invalid");
  }
  const marker = readReplacementLedgerMarker(input.ledgerPath);
  if (!marker || marker.archivePath !== path.resolve(input.archivePath)) {
    throw new Error("replacement_archive_marker_mismatch");
  }
  const archiveStat = fs.lstatSync(input.archivePath);
  const currentStat = fs.lstatSync(input.ledgerPath);
  const attemptDirectory = fs.lstatSync(path.dirname(input.freshAttemptPath));
  assertPrivateDirectory(path.dirname(input.archivePath), "archive_directory_unsafe");
  assertPrivateDirectory(path.dirname(input.freshAttemptPath), "fresh_attempt_directory_unsafe");
  if (!archiveStat.isFile() || archiveStat.isSymbolicLink() ||
      !currentStat.isFile() || currentStat.isSymbolicLink() ||
      !attemptDirectory.isDirectory() || attemptDirectory.isSymbolicLink() ||
      attemptDirectory.dev !== currentStat.dev || archiveStat.dev !== currentStat.dev ||
      fs.existsSync(input.freshAttemptPath) ||
      fs.existsSync(`${input.freshAttemptPath}-wal`) ||
      fs.existsSync(`${input.freshAttemptPath}-shm`)) {
    throw new Error("restore_paths_unsafe");
  }
  const archived = new Database(input.archivePath, { readonly: true, fileMustExist: true, timeout: 0 });
  try {
    const binding = archived.prepare(`select current_workspace_id as workspaceId,
      current_device_id as deviceId from collector_workspace_binding where singleton=1`).get() as
      { workspaceId: string; deviceId: string | null } | undefined;
    if (!binding?.deviceId || fileIdentityDigest(input.archivePath, archiveStat,
      binding.workspaceId, binding.deviceId, path.resolve(input.archivePath)) !== marker.archiveIdentity) {
      throw new Error("replacement_archive_identity_changed");
    }
  } finally { archived.close(); }
  const stage = `${input.ledgerPath}.restore-stage`;
  if ([stage, `${stage}-wal`, `${stage}-shm`].some(fs.existsSync)) {
    throw new Error("restore_stage_exists");
  }
  const authority = new LifecycleMutationAuthority(input.authorityRoot ??
    path.join(path.dirname(input.ledgerPath), "lifecycle-authority"));
  const acquired = authority.acquire();
  if (acquired.kind !== "acquired") throw new Error("restore_lifecycle_authority_unavailable");
  const lease = acquired.lease;
  let replacement: Database.Database | null = null;
  let restoredLock: Database.Database | null = null;
  let restored = false;
  try {
    replacement = new Database(input.ledgerPath, { fileMustExist: true, timeout: 0 });
    replacement.pragma("locking_mode = EXCLUSIVE");
    replacement.exec("BEGIN EXCLUSIVE; COMMIT");
    const others = otherProcessesWithFilesOpen([input.ledgerPath,
      `${input.ledgerPath}-wal`, `${input.ledgerPath}-shm`]);
    if (others === null || others.length) throw new Error("ledger_quiescence_unproven");
    const currentMarker = replacement.prepare(
      "select archive_identity as archiveIdentity from collector_replacement_ledger where singleton=1")
      .get() as { archiveIdentity: string } | undefined;
    if (currentMarker?.archiveIdentity !== marker.archiveIdentity) {
      throw new Error("replacement_archive_marker_changed");
    }
    if (replacement.pragma("journal_mode", { simple: true }) === "wal") {
      replacement.pragma("checkpoint_fullfsync = ON");
      const [checkpoint] = replacement.pragma("wal_checkpoint(TRUNCATE)") as
        Array<{ busy: number; log: number }>;
      if (!checkpoint || checkpoint.busy || checkpoint.log) throw new Error("replacement_wal_not_checkpointed");
    }
    replacement.exec("BEGIN EXCLUSIVE");
    // A clone is copy-on-write: the preserved archive inode is never handed
    // to the old runtime as a hard link it could modify.
    const clone = spawnSync("/bin/cp", ["-c", input.archivePath, stage], {
      stdio: "ignore", timeout: 300_000,
    });
    if (clone.error || clone.status !== 0) throw new Error("archive_clone_unavailable");
    restoredLock = new Database(stage, { fileMustExist: true, timeout: 0 });
    restoredLock.pragma("locking_mode = EXCLUSIVE");
    restoredLock.exec("BEGIN EXCLUSIVE");
    const heartbeat = () => {
      if (!lease.renew().ok) throw new Error("restore_lifecycle_authority_lost");
    };
    foldReplacementWeeklyFacts(replacement, restoredLock, marker.switchedAt, heartbeat);
    // The restored image must contain its committed weekly fold before it
    // becomes active. No uploader can see the clone until the atomic rename.
    restoredLock.exec("COMMIT");
    if (restoredLock.pragma("journal_mode", { simple: true }) === "wal") {
      const [checkpoint] = restoredLock.pragma("wal_checkpoint(TRUNCATE)") as
        Array<{ busy: number; log: number }>;
      if (!checkpoint || checkpoint.busy || checkpoint.log) throw new Error("restore_fold_not_checkpointed");
    }
    restoredLock.pragma("journal_mode = DELETE");
    restoredLock.close(); restoredLock = null;
    for (const sidecar of [`${stage}-wal`, `${stage}-shm`]) {
      if (fs.existsSync(sidecar)) fs.unlinkSync(sidecar);
    }
    fs.chmodSync(stage, 0o600);
    restoredLock = new Database(stage, { fileMustExist: true, timeout: 0 });
    restoredLock.pragma("locking_mode = EXCLUSIVE");
    restoredLock.exec("BEGIN EXCLUSIVE");
    heartbeat();
    fs.chmodSync(input.ledgerPath, 0o600);
    fs.linkSync(input.ledgerPath, input.freshAttemptPath);
    for (const suffix of ["-wal", "-shm"] as const) {
      const source = `${input.ledgerPath}${suffix}`;
      if (fs.existsSync(source)) {
        fs.chmodSync(source, 0o600);
        fs.renameSync(source, `${input.freshAttemptPath}${suffix}`);
      }
    }
    fsyncDirectory(path.dirname(input.freshAttemptPath));
    fs.renameSync(stage, input.ledgerPath);
    fsyncDirectory(path.dirname(input.ledgerPath));
    restored = true;
    return { archiveIdentity: marker.archiveIdentity, freshAttemptPath: input.freshAttemptPath,
      archivePreserved: true };
  } finally {
    if (replacement?.inTransaction) replacement.exec("COMMIT");
    replacement?.close();
    if (restoredLock?.inTransaction) restoredLock.exec("COMMIT");
    restoredLock?.close();
    if (!restored) removeOwnedStage(stage);
    lease.release();
  }
}

/** Called after artifact resolution and before any lifecycle mutation. */
export function assertReplacementRuntimeCompatible(ledgerPath: string, version: string): void {
  const marker = readReplacementLedgerMarker(ledgerPath);
  if (!marker) return;
  const parsed = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+([0-9A-Za-z.-]+))?$/.exec(version);
  if (!parsed || (Number(parsed[1]) === 0 && (Number(parsed[2]) < 7 ||
      (Number(parsed[2]) === 7 && (Number(parsed[3]) < 46 ||
        (Number(parsed[3]) === 46 && parsed[4] !== undefined)))))) {
    throw new Error("replacement_ledger_requires_archive_restore_before_downgrade");
  }
}
