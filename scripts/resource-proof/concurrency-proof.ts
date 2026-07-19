#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";

import {
  createResourceSandbox,
  removeResourceSandbox,
  runNoChangeConstantWorkContract,
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

  const concurrentSuccess = await withSandboxes(2, async ([first, second]) => {
    assert.ok(first && second);
    return Promise.all([
      runNoChangeConstantWorkContract(first),
      runNoChangeConstantWorkContract(second),
    ]);
  });
  const expectedEntries = concurrentSuccess[0]!.counters.filesystemEntriesScanned;
  for (const receipt of concurrentSuccess) {
    assert.equal(receipt.status, "pass");
    assert.equal(receipt.counters.filesystemEntriesScanned, expectedEntries);
    assert.ok(receipt.counters.filesystemEntriesScanned > 2_000);
    assert.equal(receipt.counters.maintenanceRuns, 6);
    assert.equal(receipt.measurements?.counterProvenanceProved, true);
    assert.equal(receipt.measurements?.filesystemObserverRestored, true);
  }
  assert.deepEqual(
    concurrentSuccess.map((receipt) => receipt.counters.filesystemEntriesScanned),
    [expectedEntries, expectedEntries],
    "separate sandbox observers must not count each other's directory entries",
  );
  assert.equal(
    fs.readdirSync,
    originalReaddirSync,
    "concurrent success must restore the exact original fs.readdirSync identity",
  );

  const [successAfterFailure, injectedFailure] = await withSandboxes(
    2,
    async ([first, second]) => {
      assert.ok(first && second);
      return Promise.all([
        runNoChangeConstantWorkContract(first),
        runNoChangeConstantWorkContract(second, {
          injectFailureAfterObserverRegistration: true,
        }),
      ]);
    },
  );
  assert.equal(successAfterFailure.status, "pass");
  assert.equal(successAfterFailure.counters.filesystemEntriesScanned, expectedEntries);
  assert.equal(successAfterFailure.measurements?.counterProvenanceProved, true);
  assert.equal(injectedFailure.status, "fail");
  assert.equal(
    injectedFailure.counters.filesystemEntriesScanned,
    0,
    "the injected failure must not inherit the concurrent sandbox's observations",
  );
  assert.equal(injectedFailure.measurements?.filesystemObserverRestored, true);
  assert.equal(injectedFailure.measurements?.counterProvenanceProved, true);
  assert.equal(
    fs.readdirSync,
    originalReaddirSync,
    "injected failure must restore the exact original fs.readdirSync identity",
  );

  process.stdout.write(
    `${JSON.stringify(
      {
        status: "pass",
        proof: "resource-proof-directory-observer-concurrency",
        concurrentSuccesses: concurrentSuccess.length,
        eachFilesystemEntriesScanned: concurrentSuccess.map(
          (receipt) => receipt.counters.filesystemEntriesScanned,
        ),
        eachCounterProvenance: concurrentSuccess.map(
          (receipt) => receipt.measurements?.counterProvenanceProved === true,
        ),
        eachObserverRestored: concurrentSuccess.map(
          (receipt) => receipt.measurements?.filesystemObserverRestored === true,
        ),
        crossCountedEntries: false,
        successAfterInjectedFailure: successAfterFailure.status,
        injectedFailure: injectedFailure.status,
        injectedFailureEntriesScanned:
          injectedFailure.counters.filesystemEntriesScanned,
        exactGlobalIdentityRestored: fs.readdirSync === originalReaddirSync,
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
      error: error instanceof Error ? error.name : "UnknownError",
    })}\n`,
  );
  process.exitCode = 1;
});
