import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";

/** Bounds apply before allocating or parsing a collector profile. */
export const MAX_COLLECTOR_PROFILE_BYTES = 32 * 1024 * 1024;
export const STATE_READ_DEADLINE_MS = 2_000;

export function assertPrivateStateDirectory(directory: string) {
  const stat = fs.lstatSync(directory);
  if (!path.isAbsolute(directory) || fs.realpathSync(directory) !== directory ||
      !stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o7077) !== 0 ||
      (process.getuid && stat.uid !== process.getuid())) {
    throw new Error("collector_state_directory_unsafe");
  }
  return stat;
}

export function assertPrivateStateFile(stat: fs.Stats, maxBytes: number) {
  if (!stat.isFile() || stat.nlink !== 1 || (stat.mode & 0o7077) !== 0 ||
      (process.getuid && stat.uid !== process.getuid())) throw new Error("collector_state_file_unsafe");
  if (!Number.isSafeInteger(stat.size) || stat.size < 0 || stat.size > maxBytes)
    throw new Error("collector_state_byte_bound_exceeded");
}

/** Pin a no-follow descriptor and refuse replacement, growth, and late reads. */
export function readPrivateStateFile(file: string, maxBytes = MAX_COLLECTOR_PROFILE_BYTES) {
  const started = performance.now();
  assertPrivateStateDirectory(path.dirname(file));
  const descriptor = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const before = fs.fstatSync(descriptor);
    assertPrivateStateFile(before, maxBytes);
    const bytes = Buffer.alloc(before.size);
    let offset = 0;
    while (offset < bytes.length) {
      if (performance.now() - started > STATE_READ_DEADLINE_MS)
        throw new Error("collector_state_read_deadline_exceeded");
      const read = fs.readSync(descriptor, bytes, offset, Math.min(64 * 1024, bytes.length - offset), offset);
      if (!read) throw new Error("collector_state_read_changed");
      offset += read;
    }
    const after = fs.fstatSync(descriptor), named = fs.lstatSync(file);
    assertPrivateStateFile(after, maxBytes);
    if (named.isSymbolicLink() || before.dev !== named.dev || before.ino !== named.ino ||
        before.size !== after.size || before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs)
      throw new Error("collector_state_read_changed");
    if (performance.now() - started > STATE_READ_DEADLINE_MS)
      throw new Error("collector_state_read_deadline_exceeded");
    return bytes;
  } finally { fs.closeSync(descriptor); }
}

export function fsyncStateDirectory(directory: string) {
  assertPrivateStateDirectory(directory);
  const descriptor = fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
}
