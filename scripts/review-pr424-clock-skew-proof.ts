/** The tagged client retry must hold attestation across wall-clock jumps. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr424-clock-skew-")));
try {
  for (const offsetMs of [-86_400_000, 86_400_000]) {
    const home = path.join(root, String(offsetMs));
    fs.mkdirSync(home, { mode: 0o700 });
    markMaintenanceRebuildPause(home);
    finishMaintenanceRebuildPause(home);
    const written = writeHookSpoolEnvelope({ home, source: "claude_code", body: "{}",
      nowMs: Date.now() + offsetMs, cause: "maintenance_rebuild" });
    assert.equal(written.ok, true);
    const pending = captureSpoolState(home);
    console.log(JSON.stringify({ check: "tagged_retry_clock_skew", offsetMs,
      pending: pending.maintenanceRebuildPending, count: pending.pendingFiles }));
    assert.equal(pending.maintenanceRebuildPending, true);
    assert.equal(pending.pendingFiles, 1);
  }
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
