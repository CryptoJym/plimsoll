"use strict";
/**
 * Proof-only preload for scripts/producer-token-rotation-proof.ts (bead
 * eco-6hoxj.152, review r3 G3). The proof loads it with `--require` into the
 * rotation CLI child it spawns; production code never loads it and nothing in
 * the CLI reads its variables.
 *
 * It watches one absolute path (PLIMSOLL_PROOF_WATCH) and appends one line per
 * event to PLIMSOLL_PROOF_READ_LOG:
 *   `open`  a descriptor was opened on the path;
 *   `read`  bytes were read from the path or through such a descriptor;
 *   `swap`  the path was replaced as requested below.
 *
 * PLIMSOLL_PROOF_SWAP replaces the path once, at a deterministic point chosen
 * by PLIMSOLL_PROOF_SWAP_AT, so a race is driven without sleeps:
 *   `lstat` right after the first lstat of the path returns;
 *   `close` right after the first descriptor opened on the path is closed.
 * Replacements: `symlink:<target>`, `fifo`, or `file:<source>` (a copy of
 * <source> renamed over the path, so it is a different regular file).
 */
const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { syncBuiltinESMExports } = require("node:module");

const watched = process.env.PLIMSOLL_PROOF_WATCH;
const log = process.env.PLIMSOLL_PROOF_READ_LOG;

if (watched && log) {
  const original = {
    lstatSync: fs.lstatSync,
    openSync: fs.openSync,
    readSync: fs.readSync,
    readFileSync: fs.readFileSync,
    closeSync: fs.closeSync,
  };
  const descriptors = new Set();
  const swapAt = process.env.PLIMSOLL_PROOF_SWAP_AT ?? "lstat";
  const replacement = process.env.PLIMSOLL_PROOF_SWAP;
  let swapped = false;
  const note = (event) => fs.appendFileSync(log, `${event}\n`);
  const isWatched = (file) => typeof file !== "number" && String(file) === watched;
  const swap = (point) => {
    if (swapped || !replacement || point !== swapAt) return;
    swapped = true;
    if (replacement.startsWith("file:")) {
      const staged = path.join(path.dirname(watched), `.proof-swap-${process.pid}`);
      fs.copyFileSync(replacement.slice("file:".length), staged);
      fs.chmodSync(staged, 0o600);
      fs.renameSync(staged, watched);
    } else {
      fs.unlinkSync(watched);
      if (replacement === "fifo") {
        if (spawnSync("mkfifo", [watched]).status !== 0) throw new Error("proof observer: mkfifo failed");
      } else if (replacement.startsWith("symlink:")) {
        fs.symlinkSync(replacement.slice("symlink:".length), watched);
      } else {
        throw new Error("proof observer: unknown PLIMSOLL_PROOF_SWAP");
      }
    }
    note("swap");
  };

  fs.lstatSync = function lstatSync(file, ...rest) {
    const result = original.lstatSync.call(this, file, ...rest);
    if (isWatched(file)) swap("lstat");
    return result;
  };
  fs.openSync = function openSync(file, ...rest) {
    const descriptor = original.openSync.call(this, file, ...rest);
    if (isWatched(file)) {
      descriptors.add(descriptor);
      note("open");
    }
    return descriptor;
  };
  fs.readSync = function readSync(descriptor, ...rest) {
    if (descriptors.has(descriptor)) note("read");
    return original.readSync.call(this, descriptor, ...rest);
  };
  fs.readFileSync = function readFileSync(file, ...rest) {
    if (isWatched(file) || descriptors.has(file)) note("read");
    return original.readFileSync.call(this, file, ...rest);
  };
  fs.closeSync = function closeSync(descriptor, ...rest) {
    const result = original.closeSync.call(this, descriptor, ...rest);
    if (descriptors.delete(descriptor)) swap("close");
    return result;
  };
  syncBuiltinESMExports();
}
