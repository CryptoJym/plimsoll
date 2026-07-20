#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

const waitState = new Int32Array(new SharedArrayBuffer(4));

class FixtureError extends Error {
  constructor(code) {
    super(code);
    this.name = code;
  }
}

function fail(code) {
  throw new FixtureError(code);
}

function within(parent, candidate) {
  const relative = path.relative(path.resolve(parent), path.resolve(candidate));
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function requireOwnedPath(root, candidate) {
  if (!path.isAbsolute(candidate) || !within(root, candidate)) {
    fail("FixtureProtocolInvalid");
  }
}

function waitForRegularFile(candidate) {
  const deadline = Date.now() + 10_000;
  for (;;) {
    try {
      const stat = fs.lstatSync(candidate);
      if (!stat.isFile() || stat.isSymbolicLink()) fail("FixtureBarrierInvalid");
      return;
    } catch (error) {
      if (error instanceof FixtureError) throw error;
      if (error?.code !== "ENOENT") fail("FixtureBarrierUnavailable");
    }
    if (Date.now() >= deadline) fail("FixtureBarrierTimeout");
    Atomics.wait(waitState, 0, 0, 10);
  }
}

function main() {
  const input = fs.readFileSync(0, "utf8");
  if (Buffer.byteLength(input) > 64 * 1024) fail("FixtureProtocolInvalid");
  let request;
  try {
    request = JSON.parse(input);
  } catch {
    fail("FixtureProtocolInvalid");
  }
  if (
    !request ||
    typeof request !== "object" ||
    typeof request.fixtureRoot !== "string" ||
    typeof request.parent !== "string" ||
    typeof request.displacedParent !== "string" ||
    typeof request.barrierDirectory !== "string" ||
    typeof request.finalName !== "string" ||
    typeof request.replacementReceipt !== "string" ||
    !request.finalName ||
    path.basename(request.finalName) !== request.finalName
  ) {
    fail("FixtureProtocolInvalid");
  }

  const fixtureRoot = fs.realpathSync.native(request.fixtureRoot);
  for (const candidate of [
    request.parent,
    request.displacedParent,
    request.barrierDirectory,
  ]) {
    requireOwnedPath(fixtureRoot, candidate);
  }
  if (
    path.dirname(path.resolve(request.parent)) !==
      path.dirname(path.resolve(request.displacedParent)) ||
    path.resolve(request.parent) === path.resolve(request.displacedParent)
  ) {
    fail("FixtureProtocolInvalid");
  }

  const readyPath = path.join(request.barrierDirectory, "writer-ready");
  const displacedPath = path.join(request.barrierDirectory, "parent-displaced");
  const writerDonePath = path.join(request.barrierDirectory, "writer-done");
  fs.writeFileSync(path.join(request.barrierDirectory, "racer-ready"), "ready\n", {
    flag: "wx",
    mode: 0o600,
  });
  waitForRegularFile(readyPath);
  fs.renameSync(request.parent, request.displacedParent);
  fs.writeFileSync(displacedPath, "displaced\n", { flag: "wx", mode: 0o600 });
  waitForRegularFile(writerDonePath);

  const parentAbsentBeforeReplacement = !fs.existsSync(request.parent);
  if (!parentAbsentBeforeReplacement) fail("FixtureParentRecreated");
  fs.mkdirSync(request.parent, { mode: 0o700 });
  fs.writeFileSync(
    path.join(request.parent, request.finalName),
    request.replacementReceipt,
    { flag: "wx", mode: 0o600 },
  );
  process.stdout.write(
    `${JSON.stringify({
      passed: true,
      parentAbsentBeforeReplacement,
      replacementInstalled: true,
    })}\n`,
  );
}

try {
  main();
} catch (error) {
  const symbolicError =
    error instanceof FixtureError ? error.name : "FixtureOperationFailed";
  process.stdout.write(
    `${JSON.stringify({ passed: false, error: symbolicError })}\n`,
  );
  process.exitCode = 1;
}
