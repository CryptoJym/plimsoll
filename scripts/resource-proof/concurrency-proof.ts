#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";

import {
  createResourceSandbox,
  removeResourceSandbox,
  runDirectoryObserverConcurrencyContract,
  type ResourceSandbox,
} from "./scenarios";

async function withSandboxes<T>(
  count: number,
  run: (sandboxes: ResourceSandbox[]) => Promise<T>,
) {
  const sandboxes: ResourceSandbox[] = [];
  try {
    for (let index = 0; index < count; index += 1) {
      sandboxes.push(await createResourceSandbox());
    }
    return await run(sandboxes);
  } finally {
    await Promise.all(sandboxes.map((sandbox) => removeResourceSandbox(sandbox)));
  }
}

async function main() {
  const originalReaddirSync = fs.readdirSync;
  const originalOpendirSync = fs.opendirSync;
  const result = await withSandboxes(2, async ([first, second]) => {
    assert.ok(first && second);
    return runDirectoryObserverConcurrencyContract([first.root, second.root]);
  });

  assert.equal(result.exercises.length, 2);
  assert.deepEqual(result.isolationAssertions, [true, true]);
  assert.equal(result.isolationProved, true);
  assert.equal(result.crossCountedEntries, false);
  assert.equal(result.injectedFailure, "fail");
  assert.equal(result.injectedFailureEntriesScanned, 0);
  assert.equal(result.injectedFailureObserverRestored, true);
  assert.equal(result.exactGlobalIdentityRestored, true);
  assert.equal(fs.readdirSync, originalReaddirSync);
  assert.equal(fs.opendirSync, originalOpendirSync);

  process.stdout.write(
    `${JSON.stringify(
      {
        status: "pass",
        proof: "resource-proof-directory-observer-concurrency",
        concurrentSuccesses: result.exercises.length,
        eachFilesystemEntriesScanned: result.exercises.map((exercise) => exercise.entries),
        eachFilesystemEnumerationCalls: result.exercises.map((exercise) => exercise.calls),
        eachFailedDirectoryEnumerationCalls: result.exercises.map(
          (exercise) => exercise.failedCalls,
        ),
        eachDirectoryReadFailures: result.exercises.map((exercise) => exercise.readFailures),
        eachDeduplicatedEntries: result.exercises.map(
          (exercise) => exercise.deduplicatedEntries,
        ),
        eachCounterProvenance: result.isolationAssertions,
        eachObserverRestored: result.exercises.map((exercise) => exercise.restored),
        crossCountedEntries: result.crossCountedEntries,
        successAfterInjectedFailure: "pass",
        injectedFailure: result.injectedFailure,
        injectedFailureEntriesScanned: result.injectedFailureEntriesScanned,
        exactGlobalIdentityRestored: result.exactGlobalIdentityRestored,
      },
      null,
      2,
    )}\n`,
  );
}

main().catch((error) => {
  process.stderr.write(
    `${JSON.stringify({
      status: "fail",
      proof: "resource-proof-directory-observer-concurrency",
      error: error instanceof Error ? error.stack ?? error.message : String(error),
    })}\n`,
  );
  process.exitCode = 1;
});
