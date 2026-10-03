import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { listHookSpoolFiles, writeHookSpoolFile } from "../packages/collector-cli/src/hook-spool";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { forwardHookOverLoopback } from "../packages/collector-cli/src/local-hook-client";
import { maintenanceRebuildPauseSeen, readMaintenanceRebuildPause } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";
import { createHookSpoolDrain } from "../packages/collector-cli/src/server";
import { releaseStopWindowListener, runStopWindowListener } from "../packages/collector-cli/src/stop-window-listener";

async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port === 48271 ? freePort() : port;
}

async function main() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "b13-r4-spool-")));
  const home = path.join(root, "home");
  fs.mkdirSync(home, { mode: 0o700 });
  const auth = loadOrCreateLocalIngestAuth(home);
  const config = collectorConfigSchema.parse({ port: await freePort() });
  const listener = runStopWindowListener(config, home, { mode: "maintenance_rebuild" });
  let released = false;
  let endedAtMs = 0;
  try {
    let ready = false;
    for (let attempt = 0; attempt < 100; attempt += 1) {
      try { ready = (await fetch(`http://127.0.0.1:${config.port}/healthz`)).status === 200; }
      catch { /* binding */ }
      if (ready) break;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(ready, true);
    const body = JSON.stringify({ hook_event_name: "UserPromptSubmit", session_id: "late-client",
      timestamp: "2026-09-27T00:00:00.000Z", prompt: "private body" });
    const fetchImpl: typeof fetch = async (input, init) => {
      const response = await fetch(input, init);
      assert.equal(response.status, 503, "the final hook request was refused by maintenance");
      await response.text();
      await releaseStopWindowListener(config.port, home);
      await listener;
      released = true;
      endedAtMs = Date.parse(readMaintenanceRebuildPause(home)!.endedAt!);
      assert.equal(captureSpoolState(home).pendingFiles, 0);
      assert.equal(captureSpoolState(home).maintenanceRebuildPending, true,
        "the authenticated 503 holds attestation before the client writes its retry file");
      assert.equal(maintenanceRebuildPauseSeen(home), true,
        "the marker stays until the durable refusal is resolved");
      await new Promise((resolve) => setTimeout(resolve, 5));
      return response;
    };
    const forwarded = await forwardHookOverLoopback(body, { source: "claude_code", port: config.port,
      auth, fetchImpl, env: { ...process.env, PLIMSOLL_HOME: home } });
    assert.ok("spooled" in forwarded && forwarded.spooled);
    const lateFile = listHookSpoolFiles(home)[0]!;
    assert.match(lateFile.name, /^\d{13,}-\d+-[0-9a-f]{6}\.json$/,
      "the client retry remains readable by the 0.7.44 spool reader");
    const writtenAtMs = fs.statSync(lateFile.path).mtimeMs;
    assert.ok(writtenAtMs > endedAtMs, "the client wrote its spool after endedAt");
    const pending = captureSpoolState(home);
    assert.equal(pending.pendingFiles, 1);
    assert.equal(pending.maintenanceRebuildPending, true,
      "a late maintenance-rejected spool still withholds an attested capture claim");
    const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
      workspaceId: config.tenantId, delivery: { enabled: true },
    });
    try {
      buffer.delivery.migrateLegacy({ now: new Date() });
      const claim = buffer.delivery.captureClaim([], pending);
      assert.equal(claim?.unattested, "maintenance_rebuild");
      assert.equal(claim?.through, null);
      const drained = await createHookSpoolDrain(config, buffer, { home }).tick();
      assert.equal(drained.recovered, 1);
      assert.equal(captureSpoolState(home).maintenanceRebuildPending, false);
      const unrelated = writeHookSpoolFile({ home, source: "claude_code", body });
      assert.ok(unrelated);
      assert.equal(captureSpoolState(home).maintenanceRebuildPending, false,
        "later ordinary backlog is not labelled as rebuild pending");
      fs.unlinkSync(unrelated.path);
      console.log(JSON.stringify({ check: "r4_delayed_client_spool_after_pause_end",
        endedAtMs, requestAtMs: lateFile.spooledAtMs, writtenAtMs, lateFile: lateFile.name,
        pending: pending.maintenanceRebuildPending, claim: claim?.unattested, drained: drained.recovered }));
    } finally { buffer.close(); }
  } finally {
    if (!released) await releaseStopWindowListener(config.port, home).catch(() => false);
    await listener;
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
