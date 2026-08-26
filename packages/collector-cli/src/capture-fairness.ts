import type Database from "better-sqlite3";

/**
 * Fair capture rotation across concurrent generation files (#141).
 *
 * Automatic cadences serve one fixed slice quantum per generation per
 * rotation turn, then resume after whichever generation was served last
 * cadence, so an incomplete generation can never monopolize consecutive
 * cadences while another eligible generation remains unserviced. Explicit
 * scans never rotate: they drain each candidate fully in one pass.
 *
 * The rotation state is one durable key/value row per source inside the
 * ledger's existing maintenance_state table — no schema migration, no new
 * background work, and a missing/corrupt row degrades to plain ordered
 * service instead of failing the scan.
 */

export const CAPTURE_ROTATION_SCHEMA_VERSION = 1 as const;

export type CaptureRotationSource = "codex" | "claude_code";

export type CaptureRotation = {
  schemaVersion: typeof CAPTURE_ROTATION_SCHEMA_VERSION;
  source: CaptureRotationSource;
  /** maintenanceCandidateHash of the candidate served last cadence. */
  lastServedRotationKey: string;
};

const ROTATION_KEY_PREFIX = "capture_rotation:";
const MAX_ROTATION_KEY_BYTES = 256;
const SOURCE_PATTERN = /^[a-z0-9_]{1,64}$/;

function rotationStateKey(source: CaptureRotationSource) {
  if (!SOURCE_PATTERN.test(source)) throw new Error("capture rotation source must be a bounded identifier");
  return `${ROTATION_KEY_PREFIX}${source}`;
}

function ensureRotationTable(db: Database.Database) {
  db.exec(`
    create table if not exists maintenance_state (
      key text primary key,
      value text not null,
      updated_at text not null
    );
  `);
}

/** Reads the durable rotation marker; any anomaly degrades to null. */
export function loadCaptureRotation(
  db: Database.Database,
  source: CaptureRotationSource,
): CaptureRotation | null {
  const key = rotationStateKey(source);
  let raw: string | undefined;
  try {
    ensureRotationTable(db);
    const row = db.prepare("select value from maintenance_state where key = ?").get(key) as
      | { value?: unknown }
      | undefined;
    if (!row || typeof row.value !== "string") return null;
    if (Buffer.byteLength(row.value, "utf8") > MAX_ROTATION_KEY_BYTES) return null;
    raw = row.value;
  } catch {
    return null;
  }
  try {
    const parsed = JSON.parse(raw!) as {
      schemaVersion?: unknown;
      source?: unknown;
      lastServedRotationKey?: unknown;
    };
    if (
      parsed?.schemaVersion !== CAPTURE_ROTATION_SCHEMA_VERSION ||
      parsed.source !== source ||
      typeof parsed.lastServedRotationKey !== "string" ||
      parsed.lastServedRotationKey.length === 0 ||
      Buffer.byteLength(parsed.lastServedRotationKey, "utf8") > MAX_ROTATION_KEY_BYTES
    ) {
      return null;
    }
    return {
      schemaVersion: CAPTURE_ROTATION_SCHEMA_VERSION,
      source,
      lastServedRotationKey: parsed.lastServedRotationKey,
    };
  } catch {
    return null;
  }
}

/** Persists the rotation marker for this cadence; null clears it. */
export function rememberCaptureRotation(
  db: Database.Database,
  source: CaptureRotationSource,
  lastServedRotationKey: string | null,
): void {
  const key = rotationStateKey(source);
  ensureRotationTable(db);
  if (lastServedRotationKey === null) {
    db.prepare("delete from maintenance_state where key = ?").run(key);
    return;
  }
  if (typeof lastServedRotationKey !== "string" || lastServedRotationKey.length === 0 ||
      Buffer.byteLength(lastServedRotationKey, "utf8") > MAX_ROTATION_KEY_BYTES) {
    throw new Error("capture rotation key must be a bounded non-empty string");
  }
  const record: CaptureRotation = {
    schemaVersion: CAPTURE_ROTATION_SCHEMA_VERSION,
    source,
    lastServedRotationKey,
  };
  db.prepare(
    `insert into maintenance_state (key, value, updated_at) values (?, ?, ?)
     on conflict(key) do update set value = excluded.value, updated_at = excluded.updated_at`,
  ).run(key, JSON.stringify(record), new Date().toISOString());
}

/**
 * Reorders candidates so service resumes AFTER the generation served last
 * cadence. Relative order is preserved, an absent/unknown marker changes
 * nothing, and the last-served candidate itself keeps eligibility by moving
 * to the tail (it already consumed its share last cadence).
 */
export function rotateAfterServed<T>(
  candidates: readonly T[],
  rotationKeyOf: (candidate: T) => string,
  rotation: CaptureRotation | null,
): T[] {
  if (!rotation || candidates.length <= 1) return [...candidates];
  const index = candidates.findIndex((candidate) => rotationKeyOf(candidate) === rotation.lastServedRotationKey);
  if (index < 0) return [...candidates];
  return [...candidates.slice(index + 1), ...candidates.slice(0, index + 1)];
}
