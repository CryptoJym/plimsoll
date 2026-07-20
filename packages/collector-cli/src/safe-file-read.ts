import fs from "node:fs";

const READ_CHUNK_BYTES = 16 * 1024;
const WORLD_WRITE_BIT = 0o002n;

export type BoundedRegularFileRead =
  | { kind: "ok"; value: string }
  | { kind: "missing" }
  | { kind: "unsafe" };

/** @internal Test-only race hooks. Production callers must omit these. */
export type BoundedRegularFileReadHooks = {
  afterPreflight?: () => void;
  afterOpen?: (descriptor: number) => void;
  afterFirstChunk?: (descriptor: number) => void;
};

function missing(error: unknown) {
  const code = (error as NodeJS.ErrnoException | undefined)?.code;
  return code === "ENOENT" || code === "ENOTDIR";
}

function trustedRegularFile(stat: fs.BigIntStats) {
  // Group-shared and read-only foreign-owned repositories are legitimate Git
  // layouts. Ownership, mode and link count are still identity-bound below;
  // only a leaf writable by every local account is rejected outright.
  return stat.isFile() && (stat.mode & WORLD_WRITE_BIT) === 0n;
}

function sameStableIdentity(left: fs.BigIntStats, right: fs.BigIntStats) {
  return (
    left.dev === right.dev &&
    left.ino === right.ino &&
    left.mode === right.mode &&
    left.nlink === right.nlink &&
    left.uid === right.uid &&
    left.gid === right.gid &&
    left.size === right.size &&
    left.mtimeNs === right.mtimeNs &&
    left.ctimeNs === right.ctimeNs &&
    left.birthtimeNs === right.birthtimeNs
  );
}

/**
 * Read an untrusted metadata leaf without following a final symlink or ever
 * waiting for a FIFO/device writer. The result intentionally carries no path
 * or content on failure so callers cannot leak either into health receipts.
 */
export function readBoundedRegularFile(
  filePath: string,
  limitBytes: number,
  hooks: BoundedRegularFileReadHooks = {},
): BoundedRegularFileRead {
  if (!Number.isSafeInteger(limitBytes) || limitBytes < 0) return { kind: "unsafe" };
  if (
    typeof fs.constants.O_NOFOLLOW !== "number" ||
    typeof fs.constants.O_NONBLOCK !== "number"
  ) {
    return { kind: "unsafe" };
  }

  let beforePath: fs.BigIntStats;
  try {
    beforePath = fs.lstatSync(filePath, { bigint: true });
  } catch (error) {
    return missing(error) ? { kind: "missing" } : { kind: "unsafe" };
  }
  if (!trustedRegularFile(beforePath) || beforePath.size > BigInt(limitBytes)) {
    return { kind: "unsafe" };
  }

  let descriptor: number | undefined;
  try {
    hooks.afterPreflight?.();
    descriptor = fs.openSync(
      filePath,
      fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK,
    );
    const beforeDescriptor = fs.fstatSync(descriptor, { bigint: true });
    if (
      !trustedRegularFile(beforeDescriptor) ||
      beforeDescriptor.size > BigInt(limitBytes) ||
      !sameStableIdentity(beforePath, beforeDescriptor)
    ) {
      return { kind: "unsafe" };
    }

    hooks.afterOpen?.(descriptor);
    const chunks: Buffer[] = [];
    let total = 0;
    let firstChunk = true;
    while (total <= limitBytes) {
      const remaining = limitBytes + 1 - total;
      const chunk = Buffer.allocUnsafe(Math.min(READ_CHUNK_BYTES, remaining));
      const bytesRead = fs.readSync(descriptor, chunk, 0, chunk.length, total);
      if (bytesRead === 0) break;
      chunks.push(chunk.subarray(0, bytesRead));
      total += bytesRead;
      if (firstChunk) {
        firstChunk = false;
        hooks.afterFirstChunk?.(descriptor);
      }
    }
    if (total > limitBytes) return { kind: "unsafe" };

    const afterDescriptor = fs.fstatSync(descriptor, { bigint: true });
    let afterPath: fs.BigIntStats;
    try {
      afterPath = fs.lstatSync(filePath, { bigint: true });
    } catch {
      return { kind: "unsafe" };
    }
    if (
      !trustedRegularFile(afterDescriptor) ||
      !trustedRegularFile(afterPath) ||
      !sameStableIdentity(beforeDescriptor, afterDescriptor) ||
      !sameStableIdentity(afterDescriptor, afterPath)
    ) {
      return { kind: "unsafe" };
    }

    return { kind: "ok", value: Buffer.concat(chunks, total).toString("utf8") };
  } catch {
    return { kind: "unsafe" };
  } finally {
    if (descriptor !== undefined) {
      try {
        fs.closeSync(descriptor);
      } catch {
        // The read already fails closed; closing cannot upgrade its result.
      }
    }
  }
}
