import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  DEFAULT_STARTUP_WAL_CHECKPOINT_BYTES,
  runStartupWalSelfHeal,
} from "../packages/collector-cli/src/startup-wal-self-heal";

async function main() {
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-wal-self-heal-"));
try {
  const ledgerPath = path.join(tempDir, "work-ledger.sqlite");
  const walPath = `${ledgerPath}-wal`;
  fs.writeFileSync(ledgerPath, "fixture");

  let calls = 0;
  fs.writeFileSync(walPath, Buffer.alloc(65));
  const above = await runStartupWalSelfHeal({
    ledgerPath,
    thresholdBytes: 64,
    timeoutMs: 50,
    runCheckpoint: async () => {
      calls += 1;
      fs.truncateSync(walPath, 0);
      return { busy: 0, log: 16, checkpointed: 16 };
    },
  });
  assert.equal(calls, 1);
  assert.deepEqual(above, {
    status: "startup_wal_checkpoint",
    attempted: true,
    outcome: "completed",
    thresholdBytes: 64,
    bytesBefore: 65,
    bytesAfter: 0,
    busy: 0,
    log: 16,
    checkpointed: 16,
    timeoutMs: 50,
  });

  fs.writeFileSync(walPath, Buffer.alloc(64));
  const below = await runStartupWalSelfHeal({
    ledgerPath,
    thresholdBytes: 64,
    runCheckpoint: async () => {
      calls += 1;
      return { busy: 0, log: 0, checkpointed: 0 };
    },
  });
  assert.equal(calls, 1, "the threshold boundary must skip without checkpointing");
  assert.equal(below.attempted, false);
  assert.equal(below.outcome, "skipped_below_threshold");
  assert.equal(DEFAULT_STARTUP_WAL_CHECKPOINT_BYTES, 1024 * 1024 * 1024);

  fs.writeFileSync(walPath, Buffer.alloc(65));
  const timedOut = await runStartupWalSelfHeal({
    ledgerPath,
    thresholdBytes: 64,
    timeoutMs: 5,
    runCheckpoint: () => new Promise(() => undefined),
  });
  assert.equal(timedOut.outcome, "timed_out");

  console.log(JSON.stringify({
    proof: "startup_wal_self_heal",
    checks: 3,
    passed: 3,
    calls,
    above,
    below,
    timedOut,
  }));
} finally {
  fs.rmSync(tempDir, { recursive: true, force: true });
}
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
