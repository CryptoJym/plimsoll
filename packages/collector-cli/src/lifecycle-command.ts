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
 * collector is stopped), `snapshots list` (read-only) and `snapshots prune`
 * (dry run unless --apply).
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
  if (input.argv[0] !== "snapshots") throw new Error("Expected lifecycle update --preflight or lifecycle snapshots list|prune");
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
  throw new Error("Expected lifecycle snapshots list|prune");
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
  if (inventory.blockedReason === "completion_order_unproven") {
    lines.push("Retention is blocked (completion_order_unproven): the order in which these operations completed " +
      "cannot be proved, so nothing will be removed.");
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
