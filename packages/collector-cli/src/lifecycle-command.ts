import { randomBytes } from "node:crypto";

import {
  LIFECYCLE_RETAINED_SNAPSHOTS,
  PURGE_CONFIRMATION,
  LifecycleManager,
  lifecycleBoundaryStatement,
  type LifecycleAdapter,
  type LifecycleSnapshotInventory,
  type RuntimeArtifact,
} from "./lifecycle";

export type LifecycleArtifactResolver = (reference: string) => Promise<RuntimeArtifact>;

function option(argv: readonly string[], name: string) {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : undefined;
}

function keepOption(argv: readonly string[]) {
  const value = option(argv, "--keep");
  if (value === undefined) return undefined;
  if (!/^\d{1,3}$/.test(value)) throw new Error("--keep must be a whole number");
  return Number(value);
}

/** The only options `lifecycle update|rollback` take; each is followed by its value. */
const UPDATE_VALUE_OPTIONS = ["--operation-id", "--artifact", "--artifact-version", "--readiness-timeout-ms", "--retention"];

/**
 * `--retention keep-all` (lifecycle update and rollback only): the operation
 * removes no snapshot, runtime, trash entry or receipt, and its receipt
 * records what retention would have removed. Update and rollback, which
 * prune by default, accept no other option than theirs, so a misspelled flag
 * (`--keep-all`, `--retension keep-all`) fails before any change instead of
 * falling back to pruning; so do a missing or other value, a repeated or
 * `=`-joined flag, and the flag on any other action.
 */
export function lifecycleRetentionKeepAll(argv: readonly string[]) {
  if (argv.some((arg) => arg.startsWith("--retention") && arg !== "--retention")) {
    throw new Error("--retention takes its value as the next argument: --retention keep-all");
  }
  const [action] = argv;
  if (action === "update" || action === "rollback") {
    for (let position = 1; position < argv.length; position += 1) {
      const arg = argv[position]!;
      if (UPDATE_VALUE_OPTIONS.includes(arg)) {
        position += 1;
      } else if (!(action === "update" && arg === "--preflight")) {
        throw new Error(`lifecycle ${action} does not take ${JSON.stringify(arg)}; its options are ` +
          `${UPDATE_VALUE_OPTIONS.join(", ")}${action === "update" ? " and --preflight" : ""}`);
      }
    }
  }
  const index = argv.indexOf("--retention");
  if (index < 0) return false;
  if (argv.indexOf("--retention", index + 1) >= 0) throw new Error("--retention may be given only once");
  if (argv[0] !== "update" && argv[0] !== "rollback") {
    throw new Error("--retention applies only to lifecycle update and rollback");
  }
  if (argv[index + 1] !== "keep-all") throw new Error("--retention accepts only keep-all");
  return true;
}

/** Injectable command boundary used by the packaged installer. */
export async function runLifecycleCommand(input: {
  argv: readonly string[];
  adapter: LifecycleAdapter;
  resolveArtifact: LifecycleArtifactResolver;
  readinessTimeoutMs?: number;
}) {
  const [action] = input.argv;
  const operationId = option(input.argv, "--operation-id") ?? "";
  const manager = new LifecycleManager(input.adapter, {
    ...(input.readinessTimeoutMs !== undefined ? { readinessTimeoutMs: input.readinessTimeoutMs } : {}),
  });
  if (action === "update" || action === "rollback") {
    const reference = option(input.argv, "--artifact");
    if (!reference) throw new Error(`${action} requires --artifact`);
    const artifact = await input.resolveArtifact(reference);
    const receipt = action === "update"
      ? await manager.update({ operationId, artifact })
      : await manager.rollback({ operationId, artifact });
    return { receipt, boundary: lifecycleBoundaryStatement() };
  }
  if (action === "uninstall") {
    const receipt = await manager.uninstall({ operationId, apply: input.argv.includes("--apply") });
    return { receipt, boundary: lifecycleBoundaryStatement() };
  }
  if (action === "purge") {
    const apply = input.argv.includes("--apply");
    const confirmation = option(input.argv, "--confirm-exact") ?? "";
    if (apply && confirmation !== PURGE_CONFIRMATION) throw new Error("purge exact confirmation missing");
    const receipt = await manager.purge({ operationId, apply, confirmation });
    return { receipt, boundary: lifecycleBoundaryStatement() };
  }
  if (action === "support-bundle") {
    const result = await manager.supportBundle(operationId);
    return { ...result, boundary: lifecycleBoundaryStatement() };
  }
  throw new Error("Expected lifecycle update|rollback|uninstall|purge|support-bundle");
}

/**
 * Snapshot and disk commands: `update --preflight` (read-only, run before the
 * collector is stopped), `snapshots list` (read-only), `snapshots prune` and
 * `snapshots reconcile` (both dry runs unless --apply).
 */
export async function runLifecycleSnapshotCommand(input: {
  argv: readonly string[];
  adapter: LifecycleAdapter;
}) {
  const manager = new LifecycleManager(input.adapter);
  const boundary = lifecycleBoundaryStatement();
  if (input.argv[0] === "update" && input.argv.includes("--preflight")) {
    return { kind: "preflight" as const, preflight: await manager.preflightUpdate(), boundary };
  }
  if (input.argv[0] !== "snapshots") {
    throw new Error("Expected lifecycle update --preflight or lifecycle snapshots list|prune|reconcile");
  }
  const keep = keepOption(input.argv);
  if (input.argv[1] === "list") {
    return { kind: "list" as const, snapshots: await manager.listSnapshots({ ...(keep !== undefined ? { keep } : {}) }), boundary };
  }
  if (input.argv[1] === "prune") {
    const result = await manager.pruneSnapshots({
      operationId: option(input.argv, "--operation-id") ??
        `snapshots-prune-${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`,
      ...(keep !== undefined ? { keep } : {}),
      apply: input.argv.includes("--apply"),
    });
    return { kind: "prune" as const, ...result, boundary };
  }
  if (input.argv[1] === "reconcile") {
    const keepSnapshots = option(input.argv, "--keep-snapshots");
    const result = await manager.reconcileSnapshots({
      operationId: option(input.argv, "--operation-id") ??
        `snapshots-reconcile-${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`,
      ...(keepSnapshots !== undefined ? { keep: keepSnapshots.split(",").map((id) => id.trim()).filter(Boolean) } : {}),
      apply: input.argv.includes("--apply"),
      force: input.argv.includes("--force"),
    });
    return { kind: "reconcile" as const, ...result, boundary };
  }
  throw new Error("Expected lifecycle snapshots list|prune|reconcile");
}

function formatBytes(bytes: number) {
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GiB`;
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MiB`;
  return `${(bytes / 1024).toFixed(1)} KiB`;
}

function table(rows: readonly (readonly string[])[]) {
  const widths = rows[0]!.map((_cell, column) => Math.max(...rows.map((row) => row[column]!.length)));
  return rows.map((row) => row.map((cell, column) => cell.padEnd(widths[column]!)).join("  ").trimEnd()).join("\n");
}

/** Operator view of `lifecycle snapshots list`: names, sizes and decisions only. */
export function formatSnapshotInventory(inventory: LifecycleSnapshotInventory) {
  const lines = [
    `Lifecycle snapshots: ${inventory.snapshots.length} (${formatBytes(inventory.bytes.snapshots)}); ` +
      `installed runtime ${inventory.installedVersion ?? "none"}; retention keeps the ${inventory.keepSnapshots} newest completed.`,
  ];
  if (inventory.snapshots.length > 0) {
    lines.push("", table([
      ["ID", "CREATED (UTC)", "SIZE", "METHOD", "OPERATION", "RESTORES", "RETENTION"],
      ...inventory.snapshots.map((row) => [
        row.id,
        row.createdAt?.slice(0, 16).replace("T", " ") ?? "-",
        formatBytes(row.bytes),
        row.method,
        row.operationState,
        row.restoresVersion ?? "-",
        `${row.retention} (${row.reason})`,
      ]),
    ]));
  }
  if (inventory.versions.length > 0) {
    lines.push("", table([
      ["RUNTIME", "SIZE", "RETENTION"],
      ...inventory.versions.map((row) => [row.version, formatBytes(row.bytes), `${row.retention} (${row.reason})`]),
    ]));
  }
  if (inventory.pendingRemoval.length > 0) {
    lines.push("", `Interrupted removal pending: ${inventory.pendingRemoval.length} item(s), ${formatBytes(inventory.bytes.pendingRemoval)}; ` +
      "the next prune --apply or completed update finishes it.");
  }
  lines.push("");
  const unknown = inventory.snapshots.filter((row) => row.reason === "operation_unknown" || row.reason === "receipt_without_sequence");
  if (unknown.length > 0) {
    const unsequenced = unknown.filter((row) => row.reason === "receipt_without_sequence").length;
    lines.push(`${unknown.length} snapshot(s) are kept because their operation cannot be read or ordered` +
      (unsequenced > 0
        ? ` (${unsequenced} recorded without a completion sequence by a lifecycle command older than 0.7.40 that could not read this host's order record)`
        : "") +
      ". `plimsoll lifecycle snapshots reconcile` shows them and how to decide them with --keep-snapshots.");
  }
  if (inventory.blockedReason === "completion_order_unproven" || inventory.blockedReason === "removal_record_unreadable") {
    lines.push(`Retention is blocked (${inventory.blockedReason}): ` +
      (inventory.blockedReason === "completion_order_unproven"
        ? "the order in which these operations completed cannot be proved"
        : "a removal record cannot be read") +
      ", so nothing will be removed. `plimsoll lifecycle snapshots reconcile` shows why and how to repair it.");
  } else if (inventory.blockedReason) {
    lines.push(`Retention is blocked (${inventory.blockedReason}); nothing will be removed until lifecycle recovery.`);
  } else {
    const prunable = [...inventory.snapshots, ...inventory.versions].filter((row) => row.retention === "prune").length;
    const keep = inventory.keepSnapshots === LIFECYCLE_RETAINED_SNAPSHOTS ? "" : ` --keep ${inventory.keepSnapshots}`;
    lines.push(prunable === 0
      ? "Nothing to prune."
      : `Prunable: ${prunable} item(s), ${formatBytes(inventory.bytes.prunable)}. ` +
        `Preview with \`plimsoll lifecycle snapshots prune${keep}\`; remove with \`plimsoll lifecycle snapshots prune${keep} --apply\`.`);
  }
  return lines.join("\n");
}
