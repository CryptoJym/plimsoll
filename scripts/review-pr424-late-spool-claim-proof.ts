/** Regression: an acknowledged 503 cannot leave an attested claim before its retry is spooled. */
import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { advanceCaptureFrontier, CAPTURE_WRITE_LAG_MS } from "../packages/collector-cli/src/capture-frontier";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { listHookSpoolFiles } from "../packages/collector-cli/src/hook-spool";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { forwardHookOverLoopback } from "../packages/collector-cli/src/local-hook-client";
import { releaseStopWindowListener, runStopWindowListener } from "../packages/collector-cli/src/stop-window-listener";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr424-claim-gap-")));
const home = path.join(root, "home");
fs.mkdirSync(home, { mode: 0o700 });
const workspaceId = "33333333-3333-7333-8333-333333333333";
const deviceId = "44444444-4444-7444-8444-444444444444";

async function freePort() {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
}

async function main() {
  const auth = loadOrCreateLocalIngestAuth(home);
  const config = collectorConfigSchema.parse({ tenantId: workspaceId, deviceId, port: await freePort() });
  const listener = runStopWindowListener(config, home, { mode: "maintenance_rebuild" });
  const holder: { buffer: LocalEventBuffer | null } = { buffer: null };
  let before: { unattested?: string; through?: string | null } | null | undefined;
  // The pending response is held in fetchImpl while the resumed daemon publishes a claim.
  try {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      try { if ((await fetch(`http://127.0.0.1:${config.port}/healthz`)).status === 200) break; }
      catch { /* listener is binding */ }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    const body = JSON.stringify({ hook_event_name: "UserPromptSubmit", session_id: "delayed-retry",
      timestamp: new Date().toISOString(), prompt: "private fixture body" });
    const fetchImpl: typeof fetch = async (input, init) => {
      const response = await fetch(input, init);
      assert.equal(response.status, 503);
      assert.equal(response.headers.get("x-plimsoll-maintenance-rebuild"), "paused");
      await response.text();
      await releaseStopWindowListener(config.port, home);
      await listener;
      holder.buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
        workspaceId, deviceId, delivery: { enabled: true },
      });
      holder.buffer.delivery.migrateLegacy({ now: new Date() });
      const observed = new Date(Date.now() + CAPTURE_WRITE_LAG_MS).toISOString();
      for (const source of ["codex", "claude_code", "grok"] as const) {
        advanceCaptureFrontier(holder.buffer.database, source, { complete: true, files: [] }, observed);
      }
      before = holder.buffer.delivery.captureClaim([], captureSpoolState(home));
      return response;
    };
    const forwarded = await forwardHookOverLoopback(body, { source: "claude_code", port: config.port,
      auth, fetchImpl, env: { ...process.env, PLIMSOLL_HOME: home } });
    assert.ok("spooled" in forwarded && forwarded.spooled);
    assert.ok(holder.buffer);
    const after = holder.buffer.delivery.captureClaim([], captureSpoolState(home));
    const pending = listHookSpoolFiles(home).length;
    console.log(JSON.stringify({ check: "claim_before_delayed_client_spool", beforeUnattested: before?.unattested,
      beforeThrough: before?.through, afterUnattested: after?.unattested, pending }));
    assert.equal(pending, 1);
    assert.equal(after?.unattested, "maintenance_rebuild");
    assert.equal(before?.unattested, "maintenance_rebuild",
      "the first resumed claim must remain held before a paused client writes its retry file");
  } finally {
    if (holder.buffer) holder.buffer.close();
    await releaseStopWindowListener(config.port, home).catch(() => false);
    await listener;
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
