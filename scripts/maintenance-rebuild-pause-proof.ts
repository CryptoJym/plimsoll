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
import { preflightMaintenanceRebuild, readMaintenanceRebuildHeadroomStatus } from
  "../packages/collector-cli/src/maintenance-rebuild";
import { OtlpIntakeSpool } from "../packages/collector-cli/src/otlp-spool";
import { createCollectorServer, createHookSpoolDrain } from "../packages/collector-cli/src/server";
import { releaseStopWindowListener, runStopWindowListener } from "../packages/collector-cli/src/stop-window-listener";

async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port === 48271 ? freePort() : port;
}

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-rebuild-pause-"));
  const home = path.join(root, "home");
  fs.mkdirSync(home, { mode: 0o700 });
  const auth = loadOrCreateLocalIngestAuth(home);
  const port = await freePort();
  const config = collectorConfigSchema.parse({ port });
  const listener = runStopWindowListener(config, home, { mode: "maintenance_rebuild" });
  try {
    let ready = false;
    for (let attempt = 0; attempt < 100; attempt += 1) {
      try {
        const response = await fetch(`http://127.0.0.1:${port}/healthz`);
        const body = await response.json() as { mode?: string };
        if (body.mode === "maintenance_rebuild") { ready = true; break; }
      } catch { /* listener is still binding */ }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.ok(ready, "the copy listener reaches its pause state");
    const pausedStatus = await fetch(`http://127.0.0.1:${port}/status`, {
      headers: { "x-plimsoll-token": auth.managementRead },
    });
    const pausedBody = await pausedStatus.json() as {
      maintenance?: { rebuild?: string }; captureClaim?: { through?: unknown };
    };
    assert.equal(pausedStatus.status, 200);
    assert.equal(pausedBody.maintenance?.rebuild, "paused");
    assert.equal(pausedBody.captureClaim?.through, null);
    const hook = await fetch(`http://127.0.0.1:${port}/hooks/claude-code`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-plimsoll-token": auth.claudeCodeProducer },
      body: JSON.stringify({ hook_event_name: "UserPromptSubmit", session_id: "fixture-session",
        timestamp: "2026-09-27T00:00:00.000Z", prompt: "private body" }),
    });
    assert.equal(hook.status, 503);
    assert.equal(hook.headers.get("retry-after"), "1");
    assert.equal(listHookSpoolFiles(home).length, 1);
    const otlp = await fetch(`http://127.0.0.1:${port}/v1/traces`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-plimsoll-source": "codex",
        "x-plimsoll-token": auth.codexProducer },
      body: JSON.stringify({ resourceSpans: [{ scopeSpans: [{ spans: [{
        name: "handle_responses", traceId: "1".padStart(32, "0"), spanId: "1".padStart(16, "0"),
        startTimeUnixNano: String(BigInt(Date.now()) * 1_000_000n),
        attributes: [{ key: "gen_ai.usage.input_tokens", value: { intValue: "5" } }],
      }] }] }] }),
    });
    assert.equal(otlp.status, 503);
    assert.equal(otlp.headers.get("retry-after"), "1");
    const spool = new OtlpIntakeSpool({ home });
    assert.equal(spool.status().pendingFiles, 1);
    const pending = captureSpoolState(home);
    assert.equal(pending.maintenanceRebuildPending, true);
    await releaseStopWindowListener(port, home);
    await listener;
    const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
      workspaceId: config.tenantId, delivery: { enabled: true },
    });
    try {
      buffer.delivery.migrateLegacy({ now: new Date() });
      const held = buffer.delivery.captureClaim([], pending);
      assert.equal(held?.unattested, "maintenance_rebuild");
      assert.equal(held?.through, null);
      const hookDrain = createHookSpoolDrain(config, buffer, { home });
      const hookResult = await hookDrain.tick();
      const otlpResult = await spool.drain(buffer);
      assert.equal(hookResult.recovered, 1);
      assert.equal(otlpResult.replayed, 1);
      const drained = captureSpoolState(home);
      assert.equal(drained.pendingFiles, 0);
      assert.equal(drained.maintenanceRebuildPending, false);
      const coveredMs = Date.now();
      for (const source of ["codex", "claude_code", "grok"] as const) {
        advanceCaptureFrontier(buffer.database, source, { complete: true, files: [] },
          new Date(coveredMs + CAPTURE_WRITE_LAG_MS).toISOString());
      }
      const recovered = buffer.delivery.captureClaim([], drained);
      assert.notEqual(recovered?.through, null);
      assert.equal(recovered?.unattested, undefined);
      const ledgerPath = path.join(root, "ledger.sqlite");
      assert.throws(() => preflightMaintenanceRebuild({ ledgerPath, stage: "S10",
        walHighWaterBytes: 0, copyDrill: true, freeBytes: 1 }), /insufficient_headroom/);
      const headroom = readMaintenanceRebuildHeadroomStatus(ledgerPath);
      assert.ok(headroom && headroom.shortfallBytes > 0);
      const statusServer = createCollectorServer(config, buffer, {
        localAuth: auth, localAuthHome: home,
        maintenanceStatus: () => ({ rebuild: headroom }),
      });
      try {
        await new Promise<void>((resolve) => statusServer.listen(0, "127.0.0.1", resolve));
        const statusPort = (statusServer.address() as { port: number }).port;
        const response = await fetch(`http://127.0.0.1:${statusPort}/status`, {
          headers: { "x-plimsoll-token": auth.managementRead },
        });
        const body = await response.json() as { maintenance?: { rebuild?: { shortfallBytes?: number } } };
        assert.equal(response.status, 200);
        assert.equal(body.maintenance?.rebuild?.shortfallBytes, headroom.shortfallBytes);
      } finally {
        await new Promise<void>((resolve) => statusServer.close(() => resolve()));
      }
    } finally { buffer.close(); }
    console.log(JSON.stringify({ check: "maintenance_pause_spools_503",
      hookStatus: hook.status, otlpStatus: otlp.status, hookFiles: 1, otlpFiles: 1,
      pausedStatus: pausedStatus.status, pausedClaimThrough: null,
      drained: true, claimRecovered: true, headroomStatus: true }));
  } finally {
    await releaseStopWindowListener(port, home);
    await listener;
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
