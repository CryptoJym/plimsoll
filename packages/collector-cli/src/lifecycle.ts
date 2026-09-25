import path from "node:path";

import { PLIMSOLL_VERSION } from "./version";

export const LIFECYCLE_SCHEMA_VERSION = 1 as const;
export const SUPPORTED_NODE_RANGE = { minimum: 20, maximumExclusive: 25 } as const;
export const PURGE_CONFIRMATION = "PURGE PLIMSOLL LOCAL DATA" as const;

export type SupportedArchitecture = "arm64" | "x64";
export type LifecycleOperationKind = "update" | "rollback";
export type LifecyclePhase =
  | "prepared"
  | "snapshotted"
  | "staged"
  | "switched"
  | "verified"
  | "rollback_required"
  | "rollback_complete";

export type LifecyclePurgeOnlyTarget =
  | "collector_config"
  | "workspace_credentials"
  | "ledger"
  | "history"
  | "status_summary"
  | "lifecycle_snapshots";

export type LifecycleRetainedTarget = LifecyclePurgeOnlyTarget | "workspace_membership";

export const LIFECYCLE_PURGE_ONLY_TARGETS = [
  "collector_config",
  "workspace_credentials",
  "ledger",
  "history",
  "status_summary",
  "lifecycle_snapshots",
] as const satisfies readonly LifecyclePurgeOnlyTarget[];

export const LIFECYCLE_UNINSTALL_RETAINED_TARGETS = [
  ...LIFECYCLE_PURGE_ONLY_TARGETS,
  "workspace_membership",
] as const satisfies readonly LifecycleRetainedTarget[];

export type RuntimeArtifactFile = {
  /** Bounded POSIX subpath under the immutable runtime directory. */
  relativePath: string;
  sha256: `sha256:${string}`;
  sourcePath: string;
};

export type RuntimeArtifact = {
  version: string;
  platform: "darwin";
  architecture: SupportedArchitecture;
  nodeMajor: number;
  sha256: `sha256:${string}`;
  sourcePath: string;
  /**
   * Additional files staged beside the executable so the packaged runtime is
   * self-contained (vendored native dependencies). Optional for backward
   * compatibility with single-file artifacts.
   */
  files?: readonly RuntimeArtifactFile[];
};

export type LifecycleJournal = {
  schemaVersion: typeof LIFECYCLE_SCHEMA_VERSION;
  operationId: string;
  kind: LifecycleOperationKind;
  fromVersion: string | null;
  toVersion: string;
  phase: LifecyclePhase;
  snapshotId: string;
  /** Set with rollback_complete: how the ledger was restored, for a receipt written on retry. */
  restore?: LifecycleRestoreRecord;
};

export type LifecycleReadiness = {
  ready: boolean;
  runtimeVersion: string | null;
  serviceReady: boolean;
  configCompatible: boolean;
  databaseCompatible: boolean;
  reason: "ready" | "runtime_mismatch" | "service_unready" | "config_incompatible" | "database_incompatible";
};

/**
 * Completed update/rollback snapshots kept after every healthy completion.
 * The newest one restores the version that ran before the current one; the
 * second is a restore point for a problem found only after a further update,
 * or after a same-version re-pin whose snapshot restores the current version
 * itself. Each copy can cost a full ledger, so the count stays small.
 */
export const LIFECYCLE_RETAINED_SNAPSHOTS = 2;
export const LIFECYCLE_MAX_RETAINED_SNAPSHOTS = 64;

/** Free space a full ledger copy must leave behind: max(2 GiB, 5% of the ledger). */
export function snapshotHeadroomBytes(ledgerBytes: number) {
  return Math.max(2 * 1024 ** 3, Math.ceil(ledgerBytes * 0.05));
}

export type LifecycleSnapshotMethod = "clone" | "online_backup";

/** Why a snapshot was not an APFS clone of the quiesced ledger. */
export type LifecycleCloneFallback =
  | "ledger_in_use"
  | "ledger_not_wal"
  | "wal_not_empty"
  | "quiescence_unproven"
  | "clone_unsupported";

export type LifecycleSnapshotRecord = {
  /** Null when no ledger existed or the database adapter did not report it. */
  method: LifecycleSnapshotMethod | null;
  /** An exclusive SQLite lock proved no other connection and an empty WAL. */
  quiesced: boolean;
  cloneFallback: LifecycleCloneFallback | null;
  databaseBytes: number;
};

/**
 * How a rollback put the snapshot's ledger back: an APFS clone of it, or a
 * byte copy that first had to fit next to the live ledger. "none": the
 * snapshot held no ledger, so any live ledger was removed.
 */
export type LifecycleRestoreRecord = {
  method: "clone" | "copy" | "none";
  cloneFallback: "clone_unsupported" | null;
  databaseBytes: number;
  /**
   * "preexisting_damage": the restored copy fails PRAGMA integrity_check, but
   * only with complaints the live ledger it replaced already had.
   */
  integrity?: "ok" | "preexisting_damage";
};

/** Why a rollback left the live ledger untouched instead of restoring it. */
export type LifecycleRestoreRefusalRecord = {
  reason: "insufficient_free_space" | "integrity_check_failed" | "ledger_in_use" | "quiescence_unproven";
  requiredFreeBytes: number | null;
  freeBytes: number | null;
};

/**
 * Why an update or rollback changed nothing. A ledger that another process
 * has open (or that cannot be locked exclusively) is refused outright: a
 * rollback would have to replace a database that process could keep using.
 */
export type LifecycleRefusal = {
  reason: "insufficient_free_space" | "ledger_in_use" | "quiescence_unproven";
  /** The full copy that would not fit; null when the ledger was not quiesced. */
  method: "online_backup" | null;
  cloneFallback: LifecycleCloneFallback | null;
  ledgerBytes: number;
  headroomBytes: number;
  requiredFreeBytes: number;
  freeBytes: number;
};

/** Update --preflight: the snapshot the next update will take once the collector is stopped. */
export type LifecycleSnapshotPlan = {
  method: LifecycleSnapshotMethod | "none";
  cloneCapable: boolean;
  ledgerBytes: number;
  headroomBytes: number;
  requiredFreeBytes: number;
  /**
   * What a full copy (snapshot plus a rollback's byte copy) needs if cloning
   * fails at update time. A ledger still in use then is refused, not copied.
   */
  requiredFreeBytesIfCloneFails: number;
  freeBytes: number;
  ok: boolean;
  reason: "insufficient_free_space" | null;
};

export type LifecycleRemovedItem = {
  kind: "snapshot" | "runtime_version";
  name: string;
  bytes: number;
};

export type LifecycleRetentionRecord = {
  keepSnapshots: number;
  status: "applied" | "preview" | "skipped";
  skippedReason:
    | "lifecycle_state_unreadable"
    | "journal_unreadable"
    | "completion_order_unproven"
    | "removal_record_unreadable"
    | "retention_failed"
    | "skipped_by_operator"
    | null;
  removed: LifecycleRemovedItem[];
  removedBytes: number;
  /** Removals an interrupted earlier retention left in the trash, finished now. */
  recovered: LifecycleRemovedItem[];
  /** Recorded trash entries restored to their original locations to preserve a usable way back. */
  restored?: LifecycleRemovedItem[];
  keptSnapshots: string[];
  keptVersions: string[];
  /**
   * Only with `skipped_by_operator` (`--retention keep-all`): what retention
   * would have removed at that moment, of which nothing was. Absent when that
   * read-only preview was blocked or could not be computed.
   */
  wouldRemove?: LifecycleRemovedItem[];
};

/**
 * What `snapshots reconcile` found and did: names, counts and bytes only.
 * "rebuilt": the order record was restored from the receipts' own
 * sequences; "sealed": the operator named the snapshots to keep; "none":
 * nothing needed repair; "needs_keep": only the operator's keep-set can
 * decide (the order cannot be proved, or a snapshot's receipt cannot be read
 * or ordered). `neededRepair` is what reconcile would do without a keep-set.
 */
export type LifecycleReconcileRecord = {
  status: "preview" | "applied";
  blockedBefore: "lifecycle_state_unreadable" | "journal_unreadable" | "completion_order_unproven" | "removal_record_unreadable" | null;
  findings: {
    orderRecord: "valid" | "absent" | "invalid";
    duplicateSequences: string[];
    sequencesBeyondRecord: string[];
    unsequencedAfterSequencing: string[];
    unreadableReceipts: string[];
    /** Complete receipts without a completion sequence: written by a command that could not read the order record. */
    receiptsWithoutSequence: string[];
    snapshotsWithoutReceipt: string[];
    /** Snapshots retention keeps only because it cannot order them or read their receipt. */
    undecidedSnapshots: string[];
    unreadableRemovalRecords: string[];
    staleTemporaries: number;
  };
  repair: "none" | "rebuilt" | "sealed" | "needs_keep";
  neededRepair: "none" | "rebuilt" | "needs_keep";
  /** A seal applied with --force although nothing needed one or it releases the newest way back. */
  forced: boolean;
  keep: string[];
  released: string[];
  /** The newest snapshot (by its recorded time) that can restore a version other than the installed one. */
  newestWayBack: string | null;
  newestWayBackReleased: boolean;
  /** Snapshots that would restore an earlier version but cannot, and why: they never count as a way back. */
  unusableWaysBack: Array<{ id: string; restoresVersion: string; reason: string }>;
  /** Unreadable removal records moved to lifecycle/removals-unreadable/, kept byte for byte. */
  quarantined: Array<{ name: string; bytes: number; sha256: string }>;
  lastSequence: number | null;
};

export type LifecycleSnapshotState = "completed" | "rolled_back" | "in_progress" | "rollback_required" | "unknown";
export type LifecycleSnapshotRetentionReason =
  | "newest_completed"
  | "restores_previous_version"
  | "unfinished_operation"
  | "operation_unknown"
  | "receipt_without_sequence"
  | "completion_order_unproven"
  | "kept_by_reconcile"
  | "released_by_reconcile"
  | "older_completed"
  | "rolled_back_operation"
  | "incomplete_snapshot";
export type LifecycleVersionRetentionReason =
  | "current"
  | "service_manifest"
  | "unfinished_operation"
  | "restore_target"
  | "restore_target_unknown"
  | "unreferenced";

export type LifecycleRetentionSnapshot = {
  id: string;
  bytes: number;
  metadataValid: boolean;
  /** Version the snapshot restores (the one installed before its operation). */
  restoresVersion: string | null;
  method: LifecycleSnapshotMethod | null;
  /**
   * False when the snapshot could not actually be restored: its own files or
   * the runtime it restores are gone, or that runtime no longer matches the
   * digest the snapshot recorded. Absent means not checked (treated as usable).
   */
  restorable?: boolean;
};

/** A completed update/rollback with its durable completion order. */
export type LifecycleOrderedOperation = LifecycleCompletedOperation & {
  /** A pre-sequencing receipt recorded as existing when sequencing began: older than every sequenced one. */
  predatesSequence: boolean;
};

export type LifecycleRetentionInput = {
  installedVersion: string | null;
  pinnedVersions: readonly { version: string; reason: "current" | "service_manifest" }[];
  journal: Pick<LifecycleJournal, "operationId" | "snapshotId" | "fromVersion" | "toVersion" | "phase"> | null;
  /**
   * Every update/rollback whose completion receipt is valid, including those
   * whose snapshot is already gone. A snapshot with no entry here belongs to
   * an unknown operation.
   */
  operations: readonly LifecycleOrderedOperation[];
  /**
   * Snapshot IDs whose receipt is complete except for its completion
   * sequence: a lifecycle command that could not read the order record wrote
   * it (a pre-0.7.41 command on a sealed host, or with the record lost). Kept as unknown
   * until `snapshots reconcile` decides them.
   */
  receiptsWithoutSequence?: readonly string[];
  order: {
    /** False when the durable order itself is missing, duplicated or contradictory: nothing is pruned. */
    proven: boolean;
    /**
     * Pre-sequencing receipts may be ordered by their version chain only when
     * every completed operation is visible: no unreadable receipt, no snapshot
     * without one, and none recorded at sequencing that has since vanished.
     */
    legacyChain: boolean;
    /**
     * Written by `snapshots reconcile --keep-snapshots`: every snapshot that
     * existed then (`covered`) other than the operator's keep-set is released;
     * the keep-set counts as older than every completion sequenced after
     * `sequence`, and the pre-seal receipts are no longer ordered.
     */
    seal?: LifecycleOrderSeal | null;
  };
  snapshots: readonly LifecycleRetentionSnapshot[];
  versions: readonly { version: string; bytes: number }[];
};

export type LifecycleOrderSeal = {
  sequence: number;
  keep: readonly string[];
  covered: readonly string[];
};

export type LifecycleRetentionPlan = {
  snapshots: Array<{ id: string; keep: boolean; reason: LifecycleSnapshotRetentionReason; state: LifecycleSnapshotState }>;
  versions: Array<{ version: string; keep: boolean; reason: LifecycleVersionRetentionReason }>;
};

export type LifecycleSnapshotInventory = {
  keepSnapshots: number;
  installedVersion: string | null;
  /** Set when retention would refuse to remove anything. */
  blockedReason:
    | "lifecycle_state_unreadable"
    | "journal_unreadable"
    | "completion_order_unproven"
    | "removal_record_unreadable"
    | null;
  snapshots: Array<{
    id: string;
    createdAt: string | null;
    bytes: number;
    method: LifecycleSnapshotMethod | "unrecorded";
    restoresVersion: string | null;
    operationState: LifecycleSnapshotState;
    retention: "keep" | "prune";
    reason: LifecycleSnapshotRetentionReason;
  }>;
  versions: Array<{
    version: string;
    bytes: number;
    retention: "keep" | "prune";
    reason: LifecycleVersionRetentionReason;
  }>;
  pendingRemoval: LifecycleRemovedItem[];
  bytes: { snapshots: number; versions: number; prunable: number; pendingRemoval: number };
};

export type LifecycleReceipt = {
  schemaVersion: typeof LIFECYCLE_SCHEMA_VERSION;
  toolVersion: string;
  operationId: string;
  operation: LifecycleOperationKind | "uninstall" | "purge" | "support_bundle" | "snapshots_prune" | "snapshots_reconcile";
  status: "completed" | "rolled_back" | "rollback_required" | "preview" | "purged" | "generated" | "refused";
  fromVersion: string | null;
  toVersion: string | null;
  restoredVersion: string | null;
  health: LifecycleReadiness | null;
  ownedTargets: readonly string[];
  retainedTargets: readonly LifecycleRetainedTarget[];
  purgeOnlyTargets: readonly LifecyclePurgeOnlyTarget[];
  preserved: readonly ("ledger" | "history" | "credentials" | "workspace_membership")[];
  /** Update/rollback: how the operation's rollback snapshot was taken. */
  snapshot?: LifecycleSnapshotRecord;
  /** Completed update/rollback and snapshots_prune: what retention removed (names and bytes only). */
  retention?: LifecycleRetentionRecord;
  /** Refused update/rollback: why nothing was changed. */
  refusal?: LifecycleRefusal;
  /**
   * Completed or rolled-back update/rollback: its place in the durable,
   * monotonic completion order (assigned under the mutation lease). Retention
   * orders snapshots by it, never by file times.
   */
  completionSequence?: number;
  /** Rolled-back update/rollback: how the ledger was restored. */
  restore?: LifecycleRestoreRecord;
  /** Rollback still required: why the restore left the live ledger untouched. */
  restoreRefusal?: LifecycleRestoreRefusalRecord;
  /** snapshots_reconcile: what was found and repaired. */
  reconcile?: LifecycleReconcileRecord;
};

export type LifecycleSupportSnapshot = {
  installedVersion: string | null;
  runtimeVersion: string | null;
  platform: string;
  architecture: string;
  nodeMajor: number;
  readiness: LifecycleReadiness;
  counters: {
    activeDelivery: number;
    deadDelivery: number;
    tokenAttributedEvents: number;
    maintenancePending: number;
  };
  boundedLogs: readonly {
    source: "collector_stdout" | "collector_stderr" | "lifecycle";
    severity: "info" | "warn" | "error";
    code: string;
    count: number;
  }[];
};

export type LifecycleAdapter = {
  acquireLock(operationId: string): Promise<boolean>;
  releaseLock(operationId: string): Promise<void>;
  /**
   * Issue #158: optional fencing hook. Adapters backed by the shared
   * lifecycle mutation authority revalidate their lease here; managers call
   * it immediately before every mutating step so a stale authority can never
   * act on a successor's state. Adapters without an authority leave it unset.
   * Fence loss throws LifecycleInterruption: the journal stays resumable and
   * no automatic rollback runs on behalf of a superseded owner.
   */
  assertFence?(operationId: string): Promise<void>;
  readJournal(): Promise<LifecycleJournal | null>;
  writeJournal(journal: LifecycleJournal): Promise<void>;
  clearJournal(operationId: string): Promise<void>;
  operationIdExists(operationId: string): Promise<boolean>;
  installedVersion(): Promise<string | null>;
  snapshot(operationId: string): Promise<string>;
  stage(artifact: RuntimeArtifact): Promise<void>;
  switchTo(artifact: RuntimeArtifact): Promise<void>;
  readiness(expectedVersion: string, input: { signal: AbortSignal; deadlineMs: number }): Promise<LifecycleReadiness>;
  /** Adapters that know how the ledger was restored return it for the receipt. */
  restore(snapshotId: string, operationId?: string): Promise<void | LifecycleRestoreRecord>;
  persistReceipt(receipt: LifecycleReceipt): Promise<void>;
  uninstallOwned(input: { apply: boolean }): Promise<readonly string[]>;
  purgeOwnedData(input: { apply: boolean; confirmation: string | null }): Promise<readonly string[]>;
  supportSnapshot(): Promise<LifecycleSupportSnapshot>;
  /** Durable record of how a snapshot was taken, read back from its metadata. */
  snapshotRecord?(snapshotId: string): Promise<LifecycleSnapshotRecord | null>;
  /**
   * Durably reserves the next completion sequence for a completing update or
   * rollback, under its mutation lease. Null when the durable order record is
   * unusable; the receipt then carries no sequence and retention keeps
   * everything until the order is repaired.
   */
  assignCompletionSequence?(operationId: string): Promise<number | null>;
  /** Read-only: the snapshot the next update will take and the free space it needs. */
  planSnapshot?(): Promise<LifecycleSnapshotPlan>;
  /** Read-only: every snapshot and runtime version with its retention decision. */
  inspectSnapshots?(input: { keep: number }): Promise<LifecycleSnapshotInventory>;
  /**
   * Previews (apply false, read-only) or applies retention. Anything the
   * journal, an unknown operation, the installed state, the current pointer or
   * the service manifest references is never removed. Apply revalidates the
   * operation's fence before each removal.
   */
  retainSnapshots?(input: { operationId: string; keep: number; apply: boolean }): Promise<LifecycleRetentionRecord>;
  /**
   * After the receipt of an applied retention is persisted: drops the durable
   * removal records that receipt accounts for, once it names every item.
   */
  commitRetention?(operationId: string): Promise<void>;
  /**
   * Diagnoses (apply false, read-only) or repairs what blocks retention: a
   * lost or contradictory completion order, unreadable removal records and
   * stale temporaries. The order is rebuilt only from provable facts;
   * otherwise `keep` (the operator's keep-set) seals it. Deletes nothing.
   */
  reconcileRetention?(input: { operationId: string; keep: readonly string[] | null; apply: boolean; force?: boolean }): Promise<LifecycleReconcileRecord>;
};

export class LifecycleInterruption extends Error {
  readonly code = "LIFECYCLE_INTERRUPTED";
}

function snapshotRefusalMessage(refusal: LifecycleRefusal) {
  if (refusal.reason === "ledger_in_use") {
    return "lifecycle update refused before any change: another process has the ledger open (the collector " +
      "service or another plimsoll command); stop it so the update can prove the ledger is quiesced, then retry " +
      "the same operation ID";
  }
  if (refusal.reason === "quiescence_unproven") {
    return "lifecycle update refused before any change: the ledger could not be locked exclusively (it may be " +
      "unreadable or not a SQLite database), so a rollback could not safely replace it; repair it, then retry " +
      "the same operation ID";
  }
  return `lifecycle update refused before any change: the ledger could not be cloned ` +
    `(${refusal.cloneFallback ?? "no clone"}) and a full copy needs ${refusal.requiredFreeBytes} bytes free ` +
    `(the ledger ${refusal.ledgerBytes} twice, for the snapshot and for a rollback's copy, plus headroom ` +
    `${refusal.headroomBytes}) but ${refusal.freeBytes} are free; ` +
    "free space (plimsoll lifecycle snapshots prune --apply) or stop the collector so the ledger can be cloned, " +
    "then retry the same operation ID";
}

/**
 * Thrown before any change when the ledger is in use, cannot be locked, or a
 * needed full copy would not fit; nothing was changed.
 */
export class LifecycleSnapshotRefusal extends Error {
  readonly code = "LIFECYCLE_SNAPSHOT_REFUSED";

  constructor(readonly refusal: LifecycleRefusal) {
    super(snapshotRefusalMessage(refusal));
  }
}

const RESTORE_REFUSAL_MESSAGES: Record<Exclude<LifecycleRestoreRefusalRecord["reason"], "insufficient_free_space">, string> = {
  integrity_check_failed: "the restored copy failed PRAGMA integrity_check with damage the live ledger does not " +
    "already have",
  ledger_in_use: "another process has the live ledger open and could keep writing to the replaced file " +
    "(ledger_in_use); stop the collector service and every plimsoll command",
  quiescence_unproven: "the live ledger could not be locked exclusively (quiescence_unproven)",
};

/** Thrown by a ledger restore that changed nothing live; the rollback stays resumable. */
export class LifecycleRestoreRefusal extends Error {
  readonly code = "LIFECYCLE_RESTORE_REFUSED";

  constructor(readonly refusal: LifecycleRestoreRefusalRecord) {
    super(`ledger restore refused before any change: ${refusal.reason === "insufficient_free_space"
      ? `the snapshot could not be cloned and a byte copy needs ${refusal.requiredFreeBytes} bytes free beside ` +
        `the live ledger but ${refusal.freeBytes} are free`
      : RESTORE_REFUSAL_MESSAGES[refusal.reason]}`);
  }
}

function assertRetainedSnapshotCount(keep: number) {
  if (!Number.isSafeInteger(keep) || keep < 1 || keep > LIFECYCLE_MAX_RETAINED_SNAPSHOTS) {
    throw new Error(`--keep must be an integer from 1 to ${LIFECYCLE_MAX_RETAINED_SNAPSHOTS}`);
  }
}

/**
 * Newest-first completion order that clocks cannot affect. Sequenced
 * receipts are ordered by their durable sequence. Receipts written before
 * sequencing are all older than every sequenced one; among themselves they
 * are ordered only by their version chain: the latest completed operation
 * installed the version the next one started from (the installed version,
 * or the unfinished operation's starting version, when none is sequenced),
 * and each earlier step is the one operation whose toVersion is the
 * fromVersion of the step after it. The chain stops at the first step that
 * is missing or ambiguous (re-pins, rollbacks to a repeated version, a fresh
 * install). Every completed operation off the list is provably older than
 * every operation on it.
 */
function provenCompletionOrder(input: LifecycleRetentionInput) {
  const seal = input.order.seal ?? null;
  const completed = input.operations.filter((operation) => operation.status === "completed");
  // After a reconcile seal only completions sequenced after it are ordered;
  // the operator's keep-set stands for everything before it.
  const sequenced = completed.filter((operation) => operation.sequence !== null && (!seal || operation.sequence > seal.sequence))
    .sort((left, right) => right.sequence! - left.sequence!);
  const ordered: LifecycleOrderedOperation[] = [...sequenced];
  if (input.order.legacyChain && !seal) {
    const legacy = completed.filter((operation) => operation.sequence === null);
    let anchor = sequenced.length > 0
      ? sequenced[sequenced.length - 1]!.fromVersion
      : input.journal ? input.journal.fromVersion : input.installedVersion;
    while (anchor !== null) {
      const step = legacy.filter((operation) => operation.toVersion === anchor && !ordered.includes(operation));
      if (step.length !== 1) break;
      ordered.push(step[0]!);
      anchor = step[0]!.fromVersion;
    }
  }
  return ordered;
}

/**
 * Pure retention policy. Keeps the `keep` newest completed update/rollback
 * snapshots plus the newest one that restores a version other than the
 * installed one, and everything an unfinished or unknown operation owns. A
 * completed snapshot is removed only when `keep` kept snapshots are provably
 * newer; when the durable completion order cannot be proved, nothing is.
 * Snapshots of updates whose automatic rollback already restored them are
 * removable. Runtime versions are kept when installed, pinned by the current
 * pointer or service manifest, owned by the journal, or restored by a kept
 * snapshot.
 */
export function planLifecycleRetention(input: LifecycleRetentionInput, keep: number): LifecycleRetentionPlan {
  assertRetainedSnapshotCount(keep);
  type Decision = { keep: boolean; reason: LifecycleSnapshotRetentionReason; state: LifecycleSnapshotState };
  const decisions = new Map<string, Decision>();
  const journal = input.journal;
  const operations = new Map(input.operations.map((operation) => [operation.id, operation]));
  const seal = input.order.seal ?? null;
  const covered = new Set(seal?.covered ?? []);
  const sealedKeep = new Set(seal?.keep ?? []);
  const completed: LifecycleRetentionSnapshot[] = [];
  const keptBySeal: LifecycleRetentionSnapshot[] = [];
  for (const snapshot of input.snapshots) {
    if (journal && (snapshot.id === journal.snapshotId || snapshot.id === journal.operationId)) {
      decisions.set(snapshot.id, {
        keep: true,
        reason: "unfinished_operation",
        state: journal.phase === "rollback_required" || journal.phase === "rollback_complete"
          ? "rollback_required"
          : "in_progress",
      });
      continue;
    }
    const operation = operations.get(snapshot.id);
    // A later operation that reuses the ID of a covered snapshot without a
    // receipt completes after the seal: the seal does not decide it.
    if (seal && covered.has(snapshot.id) && (operation?.sequence ?? 0) <= seal.sequence && input.order.proven) {
      // The operator named what to keep when the order could not be proved.
      if (sealedKeep.has(snapshot.id)) keptBySeal.push(snapshot);
      else decisions.set(snapshot.id, { keep: false, reason: "released_by_reconcile", state: operation?.status ?? "unknown" });
      continue;
    }
    if (!operation) {
      decisions.set(snapshot.id, {
        keep: true,
        reason: input.receiptsWithoutSequence?.includes(snapshot.id) ? "receipt_without_sequence" : "operation_unknown",
        state: "unknown",
      });
    } else if (!input.order.proven) {
      decisions.set(snapshot.id, { keep: true, reason: "completion_order_unproven", state: operation.status });
    } else if (operation.status === "rolled_back") {
      decisions.set(snapshot.id, { keep: false, reason: "rolled_back_operation", state: "rolled_back" });
    } else {
      completed.push(snapshot);
    }
  }
  if (completed.length > 0 || keptBySeal.length > 0) {
    const byId = new Map(completed.map((snapshot) => [snapshot.id, snapshot]));
    const ordered = provenCompletionOrder(input).flatMap((operation) => byId.get(operation.id) ?? []);
    ordered.forEach((snapshot, index) => decisions.set(snapshot.id, {
      keep: index < keep,
      reason: index < keep ? "newest_completed" : snapshot.metadataValid ? "older_completed" : "incomplete_snapshot",
      state: "completed",
    }));
    // Everything off the proven order is older than all of it, so it may go
    // only once `keep` snapshots on the order are kept.
    const unordered = completed.filter((snapshot) => !decisions.has(snapshot.id));
    const olderThanKept = ordered.length >= keep;
    for (const snapshot of unordered) {
      const operation = operations.get(snapshot.id)!;
      const provablyOlder = olderThanKept && (operation.sequence === null &&
        (operation.predatesSequence || !input.operations.some((candidate) => candidate.sequence !== null)));
      decisions.set(snapshot.id, provablyOlder
        ? { keep: false, reason: snapshot.metadataValid ? "older_completed" : "incomplete_snapshot", state: "completed" }
        : { keep: true, reason: "completion_order_unproven", state: "completed" });
    }
    // The operator's keep-set is older than every completion sequenced after
    // the seal: it goes once `keep` of those are kept.
    for (const snapshot of keptBySeal) {
      const state = operations.get(snapshot.id)?.status ?? "unknown";
      decisions.set(snapshot.id, olderThanKept
        ? { keep: false, reason: snapshot.metadataValid ? "older_completed" : "incomplete_snapshot", state }
        : { keep: true, reason: "kept_by_reconcile", state });
    }
    const restoresOther = (snapshot: LifecycleRetentionSnapshot) =>
      snapshot.metadataValid && snapshot.restoresVersion !== input.installedVersion;
    const keepAsWayBack = (snapshot: LifecycleRetentionSnapshot) => {
      if (!decisions.get(snapshot.id)!.keep) {
        decisions.set(snapshot.id, {
          keep: true,
          reason: "restores_previous_version",
          state: operations.get(snapshot.id)?.status ?? "unknown",
        });
      }
    };
    const rollbackPoint = ordered.find(restoresOther);
    if (rollbackPoint) keepAsWayBack(rollbackPoint);
    // The newest way back is somewhere among the unordered: keep them all.
    else for (const snapshot of [...unordered, ...keptBySeal].filter(restoresOther)) keepAsWayBack(snapshot);
    // A way back that cannot actually restore does not count. When none that
    // can is kept, also keep the newest one that can (every such one when the
    // order cannot tell which is newest).
    const usable = (snapshot: LifecycleRetentionSnapshot) => restoresOther(snapshot) && snapshot.restorable !== false;
    if (!input.snapshots.some((snapshot) => usable(snapshot) && decisions.get(snapshot.id)?.keep)) {
      const newestUsable = ordered.find(usable);
      if (newestUsable) keepAsWayBack(newestUsable);
      else for (const snapshot of [...unordered, ...keptBySeal].filter(usable)) keepAsWayBack(snapshot);
    }
  }

  const snapshots = input.snapshots.map((snapshot) => ({ id: snapshot.id, ...decisions.get(snapshot.id)! }));
  const kept = input.snapshots.filter((snapshot) => decisions.get(snapshot.id)!.keep);
  const versionReasons = new Map<string, LifecycleVersionRetentionReason>();
  const protect = (version: string | null | undefined, reason: LifecycleVersionRetentionReason) => {
    if (version && !versionReasons.has(version)) versionReasons.set(version, reason);
  };
  protect(input.installedVersion, "current");
  for (const pinned of input.pinnedVersions) protect(pinned.version, pinned.reason);
  protect(journal?.toVersion, "unfinished_operation");
  protect(journal?.fromVersion, "unfinished_operation");
  for (const snapshot of kept) protect(snapshot.restoresVersion, "restore_target");
  // A kept snapshot whose metadata cannot name its runtime could need any of
  // them. The journal already names both runtimes of an unfinished operation.
  const unknownTarget = kept.some((snapshot) =>
    !snapshot.metadataValid && decisions.get(snapshot.id)!.reason !== "unfinished_operation");
  const versions = input.versions.map(({ version }) => {
    const reason = versionReasons.get(version) ?? (unknownTarget ? "restore_target_unknown" : "unreferenced");
    return { version, keep: reason !== "unreferenced", reason };
  });
  return { snapshots, versions };
}

function assertBoundedIdentifier(value: string, label: string) {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/.test(value) || value.includes("..")) {
    throw new Error(`${label} must be a bounded identifier`);
  }
}

export function validateRuntimeArtifact(artifact: RuntimeArtifact) {
  assertBoundedIdentifier(artifact.version, "version");
  if (artifact.platform !== "darwin") throw new Error("only darwin artifacts are supported");
  if (artifact.architecture !== "arm64" && artifact.architecture !== "x64") {
    throw new Error("unsupported architecture");
  }
  if (!Number.isInteger(artifact.nodeMajor) ||
      artifact.nodeMajor < SUPPORTED_NODE_RANGE.minimum ||
      artifact.nodeMajor >= SUPPORTED_NODE_RANGE.maximumExclusive) {
    throw new Error(`unsupported Node major ${artifact.nodeMajor}`);
  }
  if (!path.isAbsolute(artifact.sourcePath)) throw new Error("artifact source must be absolute");
  if (!/^sha256:[a-f0-9]{64}$/.test(artifact.sha256)) throw new Error("artifact digest must be sha256");
  const seen = new Set<string>();
  for (const [index, file] of (artifact.files ?? []).entries()) {
    const label = `artifact file ${index}`;
    if (!path.isAbsolute(file.sourcePath)) throw new Error(`${label} source must be absolute`);
    if (!/^sha256:[a-f0-9]{64}$/.test(file.sha256)) throw new Error(`${label} digest must be sha256`);
    if (!/^[A-Za-z0-9._/-]+$/.test(file.relativePath) || file.relativePath.includes("..") ||
        file.relativePath.startsWith("/") || file.relativePath.endsWith("/")) {
      throw new Error(`${label} relative path is not bounded`);
    }
    const normalized = path.posix.normalize(file.relativePath);
    if (normalized === "" || normalized === "." || normalized !== file.relativePath) {
      throw new Error(`${label} relative path is not normalized`);
    }
    if (normalized === path.posix.join("bin", "plimsoll.mjs")) {
      throw new Error(`${label} collides with the runtime executable`);
    }
    if (seen.has(normalized)) throw new Error(`${label} duplicates a previous relative path`);
    seen.add(normalized);
  }
}

export function immutableRuntimeRelativePath(artifact: RuntimeArtifact) {
  validateRuntimeArtifact(artifact);
  return path.join("versions", artifact.version, `${artifact.platform}-${artifact.architecture}`, "bin", "plimsoll.mjs");
}

function phaseAtLeast(phase: LifecyclePhase, expected: LifecyclePhase) {
  return ["prepared", "snapshotted", "staged", "switched", "verified"].indexOf(phase) >=
    ["prepared", "snapshotted", "staged", "switched", "verified"].indexOf(expected);
}

const READINESS_REASONS: LifecycleReadiness["reason"][] = [
  "ready", "runtime_mismatch", "service_unready", "config_incompatible", "database_incompatible",
];

function ownPlainRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  try {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) return null;
    return value as Record<string, unknown>;
  } catch {
    return null;
  }
}

function ownDataValue(record: Record<string, unknown> | null, key: string): unknown {
  if (!record) return undefined;
  try {
    const descriptor = Object.getOwnPropertyDescriptor(record, key);
    return descriptor && "value" in descriptor ? descriptor.value : undefined;
  } catch {
    return undefined;
  }
}

function safeVersion(value: unknown) {
  return typeof value === "string" && !value.includes("..") && /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/.test(value)
    ? value
    : null;
}

export function sanitizeLifecycleReadiness(readiness: unknown): LifecycleReadiness {
  const record = ownPlainRecord(readiness);
  const reason = ownDataValue(record, "reason");
  return {
    ready: ownDataValue(record, "ready") === true,
    runtimeVersion: safeVersion(ownDataValue(record, "runtimeVersion")),
    serviceReady: ownDataValue(record, "serviceReady") === true,
    configCompatible: ownDataValue(record, "configCompatible") === true,
    databaseCompatible: ownDataValue(record, "databaseCompatible") === true,
    reason: READINESS_REASONS.includes(reason as LifecycleReadiness["reason"])
      ? reason as LifecycleReadiness["reason"]
      : "service_unready",
  };
}

/** A completed or rolled-back update/rollback, as its durable completion receipt proves it. */
export type LifecycleCompletedOperation = {
  id: string;
  kind: LifecycleOperationKind;
  status: "completed" | "rolled_back";
  fromVersion: string | null;
  toVersion: string;
  /** Durable completion sequence; null for a receipt written before sequencing (0.7.37 and earlier). */
  sequence: number | null;
};

const PRESERVED = ["ledger", "history", "credentials", "workspace_membership"] as const;
/**
 * The retained and purge-only target lists a receipt can name, as one pair:
 * this release's, or the pair every release from 0.7.0 to 0.7.39 wrote before
 * the status summary became a purge-only target.
 */
const RECEIPT_TARGET_LISTS: ReadonlyArray<{ retained: readonly string[]; purgeOnly: readonly string[] }> = [
  { retained: LIFECYCLE_UNINSTALL_RETAINED_TARGETS, purgeOnly: LIFECYCLE_PURGE_ONLY_TARGETS },
  {
    retained: ["collector_config", "workspace_credentials", "ledger", "history", "lifecycle_snapshots", "workspace_membership"],
    purgeOnly: ["collector_config", "workspace_credentials", "ledger", "history", "lifecycle_snapshots"],
  },
];
const COMPLETED_OWNED_TARGETS = ["runtime", "service_manifest"] as const;
const ROLLED_BACK_OWNED_TARGETS = ["runtime", "config", "database", "service_manifest"] as const;
const RECEIPT_KEYS = [
  "schemaVersion", "toolVersion", "operationId", "operation", "status", "fromVersion", "toVersion",
  "restoredVersion", "health", "ownedTargets", "retainedTargets", "purgeOnlyTargets", "preserved",
] as const;
const CLONE_FALLBACK_VALUES: readonly (LifecycleCloneFallback | null)[] = [
  null, "ledger_in_use", "ledger_not_wal", "wal_not_empty", "quiescence_unproven", "clone_unsupported",
];
const RETENTION_SKIPPED_REASONS = [
  "lifecycle_state_unreadable", "journal_unreadable", "completion_order_unproven", "removal_record_unreadable",
  "retention_failed", "skipped_by_operator",
];
const MAX_RECEIPT_LIST = 100_000;

function exactKeys(record: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []) {
  const keys = Reflect.ownKeys(record);
  if (keys.some((key) => typeof key !== "string" || !(required.includes(key) || optional.includes(key)))) return false;
  if (required.some((key) => !keys.includes(key))) return false;
  return keys.every((key) => {
    const descriptor = Object.getOwnPropertyDescriptor(record, key);
    return descriptor !== undefined && "value" in descriptor;
  });
}

function sameList(value: unknown, expected: readonly string[]) {
  return Array.isArray(value) && value.length === expected.length && value.every((item, index) => item === expected[index]);
}

function nonnegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function boundedList<T>(value: unknown, item: (entry: unknown) => entry is T): value is T[] {
  return Array.isArray(value) && Object.getPrototypeOf(value) === Array.prototype &&
    value.length <= MAX_RECEIPT_LIST && value.every((entry) => item(entry));
}

function isRemovedItem(value: unknown): value is LifecycleRemovedItem {
  const record = ownPlainRecord(value);
  return record !== null && exactKeys(record, ["kind", "name", "bytes"]) &&
    (record.kind === "snapshot" || record.kind === "runtime_version") &&
    safeVersion(record.name) !== null && nonnegativeInteger(record.bytes);
}

function isIdentifier(value: unknown): value is string {
  return safeVersion(value) !== null;
}

function validSnapshotRecord(value: unknown) {
  const record = ownPlainRecord(value);
  if (!record || !exactKeys(record, ["method", "quiesced", "cloneFallback", "databaseBytes"])) return false;
  if (record.method !== "clone" && record.method !== "online_backup" && record.method !== null) return false;
  if (typeof record.quiesced !== "boolean" || !nonnegativeInteger(record.databaseBytes)) return false;
  if (!CLONE_FALLBACK_VALUES.includes(record.cloneFallback as LifecycleCloneFallback | null)) return false;
  return record.method !== "clone" || (record.quiesced && record.cloneFallback === null);
}

export function validRestoreRecord(value: unknown) {
  const record = ownPlainRecord(value);
  if (!record || !exactKeys(record, ["method", "cloneFallback", "databaseBytes"], ["integrity"])) return false;
  if (!nonnegativeInteger(record.databaseBytes)) return false;
  if ("integrity" in record && record.integrity !== "ok" && record.integrity !== "preexisting_damage") return false;
  if (record.method === "clone" || record.method === "none") return record.cloneFallback === null;
  return record.method === "copy" && (record.cloneFallback === "clone_unsupported" || record.cloneFallback === null);
}

function validRetentionRecord(value: unknown) {
  const record = ownPlainRecord(value);
  if (!record || !exactKeys(record, [
    "keepSnapshots", "status", "skippedReason", "removed", "removedBytes", "recovered", "keptSnapshots", "keptVersions",
  ], ["wouldRemove", "restored"])) return false;
  if (!nonnegativeInteger(record.keepSnapshots) || record.keepSnapshots < 1 ||
      record.keepSnapshots > LIFECYCLE_MAX_RETAINED_SNAPSHOTS) return false;
  if (!boundedList(record.removed, isRemovedItem) || !boundedList(record.recovered, isRemovedItem)) return false;
  if ("restored" in record && (!boundedList(record.restored, isRemovedItem) || record.status !== "applied")) return false;
  if (!boundedList(record.keptSnapshots, isIdentifier) || !boundedList(record.keptVersions, isIdentifier)) return false;
  if (record.removedBytes !== record.removed.reduce((total, item) => total + item.bytes, 0)) return false;
  // An operator keep-all removed and recovered nothing; only it may carry the preview.
  if (record.skippedReason === "skipped_by_operator") {
    if (record.recovered.length > 0 || ("restored" in record && Array.isArray(record.restored) && record.restored.length > 0)) return false;
    if ("wouldRemove" in record && !boundedList(record.wouldRemove, isRemovedItem)) return false;
  } else if ("wouldRemove" in record) {
    return false;
  }
  if (record.status === "applied") return record.skippedReason === null;
  return record.status === "skipped" && RETENTION_SKIPPED_REASONS.includes(record.skippedReason as string) &&
    record.removed.length === 0;
}

function validCompletedHealth(value: unknown, toVersion: string) {
  const record = ownPlainRecord(value);
  return record !== null &&
    exactKeys(record, ["ready", "runtimeVersion", "serviceReady", "configCompatible", "databaseCompatible", "reason"]) &&
    record.ready === true && record.runtimeVersion === toVersion && record.serviceReady === true &&
    record.configCompatible === true && record.databaseCompatible === true && record.reason === "ready";
}

/**
 * Reads a completion marker as the full immutable receipt its operation
 * wrote. Only a receipt that is complete, has no unknown field, is internally
 * consistent and names `operationId` (the marker's file name and snapshot ID)
 * proves a completed or rolled-back update/rollback; anything else returns
 * null, which retention treats as an unknown operation it never prunes.
 */
export function parseCompletionReceipt(
  value: unknown,
  operationId: string,
  options: { sequenceOptional?: boolean } = {},
): LifecycleCompletedOperation | null {
  const record = ownPlainRecord(value);
  if (!record || !exactKeys(record, RECEIPT_KEYS, ["snapshot", "retention", "restore", "completionSequence"])) return null;
  const { operation, status, fromVersion, toVersion } = record;
  if (record.schemaVersion !== LIFECYCLE_SCHEMA_VERSION || record.operationId !== operationId) return null;
  if (typeof record.toolVersion !== "string" || !/^[0-9A-Za-z][0-9A-Za-z.+-]{0,63}$/.test(record.toolVersion)) return null;
  if ((operation !== "update" && operation !== "rollback") || (status !== "completed" && status !== "rolled_back")) return null;
  if (!(fromVersion === null || isIdentifier(fromVersion)) || !isIdentifier(toVersion)) return null;
  if (!RECEIPT_TARGET_LISTS.some((lists) =>
        sameList(record.retainedTargets, lists.retained) && sameList(record.purgeOnlyTargets, lists.purgeOnly)) ||
      !sameList(record.preserved, PRESERVED)) return null;
  if ("snapshot" in record && !validSnapshotRecord(record.snapshot)) return null;
  // Receipts that record a snapshot, retention or restore were written after
  // sequencing began, so each must carry its durable completion sequence.
  const sequence = record.completionSequence;
  if (sequence !== undefined && !(nonnegativeInteger(sequence) && sequence > 0)) return null;
  if (sequence === undefined && !options.sequenceOptional && ("snapshot" in record || "retention" in record || "restore" in record)) return null;
  if (status === "completed") {
    if (record.restoredVersion !== null || !validCompletedHealth(record.health, toVersion)) return null;
    if (!sameList(record.ownedTargets, COMPLETED_OWNED_TARGETS) || "restore" in record) return null;
    if ("retention" in record && !validRetentionRecord(record.retention)) return null;
  } else {
    if (record.restoredVersion !== fromVersion || record.health !== null) return null;
    if (!sameList(record.ownedTargets, ROLLED_BACK_OWNED_TARGETS) || "retention" in record) return null;
    if ("restore" in record && !validRestoreRecord(record.restore)) return null;
  }
  return { id: operationId, kind: operation, status, fromVersion, toVersion, sequence: sequence ?? null };
}

function assertReadiness(readiness: LifecycleReadiness, version: string) {
  if (!readiness.ready || readiness.runtimeVersion !== version || !readiness.serviceReady ||
      !readiness.configCompatible || !readiness.databaseCompatible || readiness.reason !== "ready") {
    throw new Error(`readiness failed: ${readiness.reason}`);
  }
}

/**
 * Deterministic lifecycle transaction coordinator. It has no filesystem,
 * process, service-manager, network, or credential access of its own; callers
 * must inject those boundaries through LifecycleAdapter.
 */
export class LifecycleManager {
  private readonly readinessTimeoutMs: number;

  constructor(
    private readonly adapter: LifecycleAdapter,
    options: { readinessTimeoutMs?: number } = {},
  ) {
    const requested = options.readinessTimeoutMs ?? 10_000;
    if (!Number.isSafeInteger(requested) || requested < 10 || requested > 60_000) {
      throw new Error("readiness timeout must be between 10 and 60000 milliseconds");
    }
    this.readinessTimeoutMs = requested;
  }

  private async assertFreshOperation(operationId: string) {
    if (await this.adapter.operationIdExists(operationId)) {
      throw new Error("operationId was already completed; use a fresh operationId");
    }
  }

  private fence(operationId: string) {
    return this.adapter.assertFence?.(operationId);
  }

  private async boundedReadiness(expectedVersion: string) {
    const controller = new AbortController();
    let timer: NodeJS.Timeout | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new Error("readiness deadline exceeded"));
      }, this.readinessTimeoutMs);
    });
    try {
      const result = await Promise.race([
        this.adapter.readiness(expectedVersion, {
          signal: controller.signal,
          deadlineMs: this.readinessTimeoutMs,
        }),
        deadline,
      ]);
      return sanitizeLifecycleReadiness(result);
    } finally {
      if (timer) clearTimeout(timer);
      controller.abort();
    }
  }

  private async snapshotRecord(snapshotId: string) {
    const record = await this.adapter.snapshotRecord?.(snapshotId);
    return record ? { snapshot: record } : {};
  }

  /** The completing operation's place in the durable completion order. */
  private async completionSequence(operationId: string) {
    if (!this.adapter.assignCompletionSequence) return {};
    await this.fence(operationId);
    const completionSequence = await this.adapter.assignCompletionSequence(operationId);
    return completionSequence === null ? {} : { completionSequence };
  }

  private async rollbackReceipt(
    journal: LifecycleJournal,
    status: "rolled_back" | "rollback_required",
    outcome: { restore?: LifecycleRestoreRecord; restoreRefusal?: LifecycleRestoreRefusalRecord } = {},
  ): Promise<LifecycleReceipt> {
    return {
      schemaVersion: LIFECYCLE_SCHEMA_VERSION,
      toolVersion: PLIMSOLL_VERSION,
      operationId: journal.operationId,
      operation: journal.kind,
      status,
      fromVersion: journal.fromVersion,
      toVersion: journal.toVersion,
      restoredVersion: status === "rolled_back" ? journal.fromVersion : null,
      health: null,
      ownedTargets: ["runtime", "config", "database", "service_manifest"],
      retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
      purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
      preserved: ["ledger", "history", "credentials", "workspace_membership"],
      ...await this.snapshotRecord(journal.snapshotId),
      ...status === "rolled_back" ? await this.completionSequence(journal.operationId) : {},
      ...outcome,
    };
  }

  private async finishRequiredRollback(journal: LifecycleJournal, operationId: string) {
    if (journal.phase === "rollback_required") {
      await this.fence(operationId);
      let restored: LifecycleRestoreRecord | undefined;
      try {
        restored = await this.adapter.restore(journal.snapshotId, operationId) ?? undefined;
      } catch (error) {
        // A lost fence means a successor owns the lifecycle root: record nothing.
        if (error instanceof LifecycleInterruption) throw error;
        const refusal = error instanceof LifecycleRestoreRefusal ? error.refusal : undefined;
        const blocked = await this.rollbackReceipt(journal, "rollback_required", refusal ? { restoreRefusal: refusal } : {});
        await this.adapter.persistReceipt(blocked);
        throw new Error(refusal
          ? `rollback required: ${(error as Error).message}; the live ledger was left untouched; retry the same operationId`
          : "rollback required: restore failed; retry the same operationId");
      }
      journal.phase = "rollback_complete";
      if (restored) journal.restore = restored;
      await this.adapter.writeJournal(journal);
    }
    if (journal.phase !== "rollback_complete") {
      throw new Error("rollback recovery state is invalid");
    }
    const receipt = await this.rollbackReceipt(journal, "rolled_back", journal.restore ? { restore: journal.restore } : {});
    await this.adapter.persistReceipt(receipt);
    await this.adapter.clearJournal(journal.operationId);
    return receipt;
  }

  /**
   * A refused snapshot happens before anything else: the journal is still
   * "prepared", nothing was staged or switched and the service was untouched.
   * Clearing that journal returns to the pre-operation state; the receipt
   * records why and the operation ID stays usable for a retry.
   */
  private async refuseBeforeChange(journal: LifecycleJournal, refusal: LifecycleRefusal) {
    await this.fence(journal.operationId);
    await this.adapter.persistReceipt({
      schemaVersion: LIFECYCLE_SCHEMA_VERSION,
      toolVersion: PLIMSOLL_VERSION,
      operationId: journal.operationId,
      operation: journal.kind,
      status: "refused",
      fromVersion: journal.fromVersion,
      toVersion: journal.toVersion,
      restoredVersion: null,
      health: null,
      ownedTargets: [],
      retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
      purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
      preserved: ["ledger", "history", "credentials", "workspace_membership"],
      refusal,
    });
    await this.adapter.clearJournal(journal.operationId);
  }

  /**
   * Runs after the update's receipt is durable and its journal cleared, so a
   * retention problem can never undo or fail a completed update. A lost fence
   * means a successor owns the lifecycle root: nothing more is touched. Other
   * failures are recorded; the next completion or `snapshots prune` retries.
   */
  private async retainAfterCompletion(receipt: LifecycleReceipt): Promise<LifecycleReceipt> {
    if (!this.adapter.retainSnapshots) return receipt;
    let retention: LifecycleRetentionRecord;
    try {
      await this.fence(receipt.operationId);
      retention = await this.adapter.retainSnapshots({
        operationId: receipt.operationId,
        keep: LIFECYCLE_RETAINED_SNAPSHOTS,
        apply: true,
      });
    } catch (error) {
      if (error instanceof LifecycleInterruption) return receipt;
      retention = {
        keepSnapshots: LIFECYCLE_RETAINED_SNAPSHOTS,
        status: "skipped",
        skippedReason: "retention_failed",
        removed: [],
        removedBytes: 0,
        recovered: [],
        keptSnapshots: [],
        keptVersions: [],
      };
    }
    const retained = { ...receipt, retention };
    try {
      await this.fence(receipt.operationId);
      await this.adapter.persistReceipt(retained);
      if (retention.status === "applied") await this.adapter.commitRetention?.(receipt.operationId);
    } catch {
      // The committed receipt without the retention addendum stays durable,
      // and so do the removal records: the next apply reports them recovered.
    }
    return retained;
  }

  async update(input: {
    operationId: string;
    artifact: RuntimeArtifact;
    kind?: LifecycleOperationKind;
  }): Promise<LifecycleReceipt> {
    const { operationId, artifact } = input;
    const kind = input.kind ?? "update";
    assertBoundedIdentifier(operationId, "operationId");
    validateRuntimeArtifact(artifact);

    if (!(await this.adapter.acquireLock(operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }

    let journal: LifecycleJournal | null = null;
    try {
      const existing = await this.adapter.readJournal();
      const fromVersion = await this.adapter.installedVersion();
      if (existing) {
        if (existing.operationId !== operationId || existing.kind !== kind || existing.toVersion !== artifact.version) {
          throw new Error("a different interrupted lifecycle operation requires recovery");
        }
        journal = existing;
        if (journal.phase === "rollback_required" || journal.phase === "rollback_complete") {
          return await this.finishRequiredRollback(journal, operationId);
        }
      } else {
        await this.assertFreshOperation(operationId);
        journal = {
          schemaVersion: LIFECYCLE_SCHEMA_VERSION,
          operationId,
          kind,
          fromVersion,
          toVersion: artifact.version,
          phase: "prepared",
          snapshotId: operationId,
        };
        await this.adapter.writeJournal(journal);
      }

      if (!phaseAtLeast(journal.phase, "snapshotted")) {
        await this.fence(operationId);
        journal.snapshotId = await this.adapter.snapshot(operationId);
        journal.phase = "snapshotted";
        await this.adapter.writeJournal(journal);
      }
      if (!phaseAtLeast(journal.phase, "staged")) {
        await this.fence(operationId);
        await this.adapter.stage(artifact);
        journal.phase = "staged";
        await this.adapter.writeJournal(journal);
      }
      if (!phaseAtLeast(journal.phase, "switched")) {
        await this.fence(operationId);
        await this.adapter.switchTo(artifact);
        journal.phase = "switched";
        await this.adapter.writeJournal(journal);
      }
      const health = await this.boundedReadiness(artifact.version);
      assertReadiness(health, artifact.version);
      journal.phase = "verified";
      await this.adapter.writeJournal(journal);

      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId,
        operation: kind,
        status: "completed",
        fromVersion: journal.fromVersion,
        toVersion: artifact.version,
        restoredVersion: null,
        health,
        ownedTargets: ["runtime", "service_manifest"],
        retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
        purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
        preserved: ["ledger", "history", "credentials", "workspace_membership"],
        ...await this.snapshotRecord(journal.snapshotId),
        ...await this.completionSequence(operationId),
      };
      await this.adapter.persistReceipt(receipt);
      await this.adapter.clearJournal(operationId);
      return await this.retainAfterCompletion(receipt);
    } catch (error) {
      if (error instanceof LifecycleInterruption) throw error;
      if (error instanceof LifecycleSnapshotRefusal && journal?.phase === "prepared") {
        await this.refuseBeforeChange(journal, error.refusal);
        throw error;
      }
      if (journal?.phase === "rollback_required" || journal?.phase === "rollback_complete" || journal?.phase === "verified") {
        throw error;
      }
      if (journal && phaseAtLeast(journal.phase, "snapshotted")) {
        journal.phase = "rollback_required";
        await this.adapter.writeJournal(journal);
        await this.finishRequiredRollback(journal, operationId);
      }
      throw error;
    } finally {
      await this.adapter.releaseLock(operationId);
    }
  }

  rollback(input: { operationId: string; artifact: RuntimeArtifact }) {
    return this.update({ ...input, kind: "rollback" });
  }

  /** Read-only (creates nothing); takes no lock. Run before stopping the service for an update. */
  async preflightUpdate(): Promise<LifecycleSnapshotPlan> {
    if (!this.adapter.planSnapshot) throw new Error("this lifecycle adapter cannot plan snapshots");
    return this.adapter.planSnapshot();
  }

  /** Read-only; takes no lock and writes nothing. */
  async listSnapshots(input: { keep?: number } = {}): Promise<LifecycleSnapshotInventory> {
    const keep = input.keep ?? LIFECYCLE_RETAINED_SNAPSHOTS;
    assertRetainedSnapshotCount(keep);
    if (!this.adapter.inspectSnapshots) throw new Error("this lifecycle adapter cannot list snapshots");
    return this.adapter.inspectSnapshots({ keep });
  }

  /**
   * Dry run by default: no lock, no receipt, no change. Apply holds the
   * mutation lease and removes only what retention would, with the same
   * protections. A rollback-required journal may prune to free restore space;
   * a completed rollback still blocks until its receipt is durable.
   */
  async pruneSnapshots(input: { operationId: string; keep?: number; apply?: boolean }): Promise<{
    receipt: LifecycleReceipt | null;
    retention: LifecycleRetentionRecord;
  }> {
    assertBoundedIdentifier(input.operationId, "operationId");
    const keep = input.keep ?? LIFECYCLE_RETAINED_SNAPSHOTS;
    assertRetainedSnapshotCount(keep);
    const retainSnapshots = this.adapter.retainSnapshots?.bind(this.adapter);
    if (!retainSnapshots) throw new Error("this lifecycle adapter cannot prune snapshots");
    if (input.apply !== true) {
      return { receipt: null, retention: await retainSnapshots({ operationId: input.operationId, keep, apply: false }) };
    }
    if (!(await this.adapter.acquireLock(input.operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }
    try {
      // A rollback-required journal may need prune to free the space needed by
      // its restore. Once the restore is complete, its receipt must be durable
      // before another prune mutates retention.
      const journal = await this.adapter.readJournal();
      if (journal?.phase === "rollback_complete") {
        throw new Error("lifecycle recovery is required before prune");
      }
      await this.assertFreshOperation(input.operationId);
      await this.fence(input.operationId);
      const retention = await retainSnapshots({ operationId: input.operationId, keep, apply: true });
      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId: input.operationId,
        operation: "snapshots_prune",
        status: retention.status === "applied" ? "completed" : "refused",
        fromVersion: retention.skippedReason === "lifecycle_state_unreadable" ? null : await this.adapter.installedVersion(),
        toVersion: null,
        restoredVersion: null,
        health: null,
        ownedTargets: ["lifecycle_snapshots", "runtime_versions"],
        retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
        purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
        preserved: ["ledger", "history", "credentials", "workspace_membership"],
        retention,
      };
      await this.fence(input.operationId);
      await this.adapter.persistReceipt(receipt);
      if (retention.status === "applied") await this.adapter.commitRetention?.(input.operationId);
      return { receipt, retention };
    } finally {
      await this.adapter.releaseLock(input.operationId);
    }
  }

  /**
   * Dry run by default (read-only, no receipt). Apply holds the mutation lease,
   * refuses while an interrupted operation awaits recovery, repairs what it
   * can prove (or seals the order with the operator's keep-set) and records
   * everything in a snapshots_reconcile receipt. It deletes no snapshot: the
   * next prune or completed update does, with the usual removal records.
   */
  async reconcileSnapshots(input: { operationId: string; keep?: readonly string[]; apply?: boolean; force?: boolean }): Promise<{
    receipt: LifecycleReceipt | null;
    reconcile: LifecycleReconcileRecord;
  }> {
    assertBoundedIdentifier(input.operationId, "operationId");
    const keep = input.keep ?? null;
    if (keep) {
      if (keep.length === 0 || keep.length > LIFECYCLE_MAX_RETAINED_SNAPSHOTS || new Set(keep).size !== keep.length) {
        throw new Error(`--keep-snapshots must name 1 to ${LIFECYCLE_MAX_RETAINED_SNAPSHOTS} distinct snapshot IDs`);
      }
      for (const id of keep) assertBoundedIdentifier(id, "snapshot ID");
    }
    const reconcile = this.adapter.reconcileRetention?.bind(this.adapter);
    if (!reconcile) throw new Error("this lifecycle adapter cannot reconcile snapshots");
    if (input.apply !== true) {
      return { receipt: null, reconcile: await reconcile({ operationId: input.operationId, keep, apply: false, force: input.force === true }) };
    }
    if (!(await this.adapter.acquireLock(input.operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }
    try {
      if (await this.adapter.readJournal()) throw new Error("lifecycle recovery is required before reconcile");
      await this.assertFreshOperation(input.operationId);
      await this.fence(input.operationId);
      const record = await reconcile({ operationId: input.operationId, keep, apply: true, force: input.force === true });
      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId: input.operationId,
        operation: "snapshots_reconcile",
        status: "completed",
        fromVersion: await this.adapter.installedVersion().catch(() => null),
        toVersion: null,
        restoredVersion: null,
        health: null,
        ownedTargets: ["lifecycle_snapshots"],
        retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
        purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
        preserved: ["ledger", "history", "credentials", "workspace_membership"],
        reconcile: record,
      };
      await this.fence(input.operationId);
      await this.adapter.persistReceipt(receipt);
      return { receipt, reconcile: record };
    } finally {
      await this.adapter.releaseLock(input.operationId);
    }
  }

  async uninstall(input: { operationId: string; apply?: boolean }): Promise<LifecycleReceipt> {
    assertBoundedIdentifier(input.operationId, "operationId");
    const apply = input.apply === true;
    if (!(await this.adapter.acquireLock(input.operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }
    try {
      if (await this.adapter.readJournal()) throw new Error("lifecycle recovery is required before uninstall");
      await this.assertFreshOperation(input.operationId);
      const fromVersion = await this.adapter.installedVersion();
      if (apply) await this.fence(input.operationId);
      const ownedTargets = await this.adapter.uninstallOwned({ apply });
      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId: input.operationId,
        operation: "uninstall",
        status: apply ? "completed" : "preview",
        fromVersion,
        toVersion: null,
        restoredVersion: null,
        health: null,
        ownedTargets,
        retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
        purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
        preserved: ["ledger", "history", "credentials", "workspace_membership"],
      };
      await this.adapter.persistReceipt(receipt);
      return receipt;
    } finally {
      await this.adapter.releaseLock(input.operationId);
    }
  }

  async purge(input: { operationId: string; apply?: boolean; confirmation?: string }): Promise<LifecycleReceipt> {
    assertBoundedIdentifier(input.operationId, "operationId");
    const apply = input.apply === true;
    if (apply && input.confirmation !== PURGE_CONFIRMATION) {
      throw new Error(`purge requires exact confirmation: ${PURGE_CONFIRMATION}`);
    }
    if (!(await this.adapter.acquireLock(input.operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }
    try {
      if (await this.adapter.readJournal()) throw new Error("lifecycle recovery is required before purge");
      await this.assertFreshOperation(input.operationId);
      if (apply) await this.fence(input.operationId);
      const targets = await this.adapter.purgeOwnedData({
        apply,
        confirmation: apply ? input.confirmation ?? null : null,
      });
      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId: input.operationId,
        operation: "purge",
        status: apply ? "purged" : "preview",
        fromVersion: await this.adapter.installedVersion(),
        toVersion: null,
        restoredVersion: null,
        health: null,
        ownedTargets: targets,
        retainedTargets: apply
          ? ["workspace_membership"]
          : LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
        purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
        preserved: apply
          ? ["workspace_membership"]
          : ["ledger", "history", "credentials", "workspace_membership"],
      };
      await this.adapter.persistReceipt(receipt);
      return receipt;
    } finally {
      await this.adapter.releaseLock(input.operationId);
    }
  }

  async supportBundle(operationId: string): Promise<{ receipt: LifecycleReceipt; bundle: LifecycleSupportSnapshot }> {
    assertBoundedIdentifier(operationId, "operationId");
    if (!(await this.adapter.acquireLock(operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }
    try {
      if (await this.adapter.readJournal()) throw new Error("lifecycle recovery is required before support bundle");
      await this.assertFreshOperation(operationId);
      const bundle = sanitizeSupportSnapshot(await this.adapter.supportSnapshot());
      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId,
        operation: "support_bundle",
        status: "generated",
        fromVersion: bundle.installedVersion,
        toVersion: null,
        restoredVersion: null,
        health: bundle.readiness,
        ownedTargets: ["support_bundle"],
        retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
        purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
        preserved: ["ledger", "history", "credentials", "workspace_membership"],
      };
      await this.adapter.persistReceipt(receipt);
      return { receipt, bundle };
    } finally {
      await this.adapter.releaseLock(operationId);
    }
  }
}

const SAFE_LOG_CODE = /^[a-z0-9][a-z0-9._-]{0,63}$/;

export function sanitizeSupportSnapshot(snapshot: unknown): LifecycleSupportSnapshot {
  const record = ownPlainRecord(snapshot);
  const nonnegative = (value: number) => Number.isSafeInteger(value) && value >= 0 ? value : 0;
  const logsCandidate = ownDataValue(record, "boundedLogs");
  const boundedLogs: Array<LifecycleSupportSnapshot["boundedLogs"][number]> = [];
  let logsArray: unknown[] | null = null;
  if (Array.isArray(logsCandidate)) {
    try {
      logsArray = Object.getPrototypeOf(logsCandidate) === Array.prototype ? logsCandidate : null;
    } catch {
      logsArray = null;
    }
  }
  if (logsArray) {
    const lengthDescriptor = Object.getOwnPropertyDescriptor(logsArray, "length");
    const rawLength = lengthDescriptor && "value" in lengthDescriptor ? lengthDescriptor.value : 0;
    const length = Number.isSafeInteger(rawLength) ? Math.min(Math.max(rawLength as number, 0), 32) : 0;
    for (let index = 0; index < length; index += 1) {
      const itemDescriptor = Object.getOwnPropertyDescriptor(logsArray, String(index));
      if (!itemDescriptor || !("value" in itemDescriptor)) continue;
      const row = ownPlainRecord(itemDescriptor.value);
      const source = ownDataValue(row, "source");
      const severity = ownDataValue(row, "severity");
      const code = ownDataValue(row, "code");
      const count = ownDataValue(row, "count");
      if (typeof code !== "string" || !SAFE_LOG_CODE.test(code)) continue;
      if (source !== "collector_stdout" && source !== "collector_stderr" && source !== "lifecycle") continue;
      if (severity !== "info" && severity !== "warn" && severity !== "error") continue;
      // Rebuild from approved scalar fields. Unknown own, prototype,
      // accessor, nested, case-alias, and Unicode-alias keys have no output.
      boundedLogs.push({
        source,
        severity,
        code,
        count: Math.min(nonnegative(typeof count === "number" ? count : 0), 1_000_000),
      });
    }
  }
  const counters = ownPlainRecord(ownDataValue(record, "counters"));
  const safeReadiness = sanitizeLifecycleReadiness(ownDataValue(record, "readiness"));
  const architecture = ownDataValue(record, "architecture");
  const nodeMajor = ownDataValue(record, "nodeMajor");
  return {
    installedVersion: safeVersion(ownDataValue(record, "installedVersion")),
    runtimeVersion: safeVersion(ownDataValue(record, "runtimeVersion")),
    platform: ownDataValue(record, "platform") === "darwin" ? "darwin" : "unsupported",
    architecture: architecture === "arm64" || architecture === "x64" ? architecture : "unsupported",
    nodeMajor: typeof nodeMajor === "number" && Number.isInteger(nodeMajor) ? nodeMajor : 0,
    readiness: safeReadiness,
    counters: {
      activeDelivery: nonnegative(typeof ownDataValue(counters, "activeDelivery") === "number" ? ownDataValue(counters, "activeDelivery") as number : 0),
      deadDelivery: nonnegative(typeof ownDataValue(counters, "deadDelivery") === "number" ? ownDataValue(counters, "deadDelivery") as number : 0),
      tokenAttributedEvents: nonnegative(typeof ownDataValue(counters, "tokenAttributedEvents") === "number" ? ownDataValue(counters, "tokenAttributedEvents") as number : 0),
      maintenancePending: nonnegative(typeof ownDataValue(counters, "maintenancePending") === "number" ? ownDataValue(counters, "maintenancePending") as number : 0),
    },
    boundedLogs,
  };
}

export function lifecycleBoundaryStatement() {
  return {
    leave: "distinct_operation_not_performed",
    revoke: "hosted_owner_operation_not_performed",
    credentialsMoved: false,
    liveServiceTouched: false,
  } as const;
}
