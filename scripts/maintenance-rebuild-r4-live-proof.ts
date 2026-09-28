import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { provisionLiveProducer } from "../packages/collector-cli/src/codex-live-usage-auth";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { listHookSpoolFiles } from "../packages/collector-cli/src/hook-spool";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { releaseStopWindowListener, runStopWindowListener } from "../packages/collector-cli/src/stop-window-listener";

async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port === 48271 ? freePort() : port;
}

async function main() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "b13-r4-live-")));
  const home = path.join(root, "home");
  fs.mkdirSync(home, { mode: 0o700 });
  const ordinary = loadOrCreateLocalIngestAuth(home);
  const workspaceId = "33333333-3333-7333-8333-333333333333";
  const deviceId = "44444444-4444-7444-8444-444444444444";
  const enrolledAt = "2026-09-08T08:00:00.000Z";
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), { workspaceId, deviceId,
    enrollmentNow: () => new Date(enrolledAt), delivery: { enabled: true } });
  const captureRoot = { rootId: "synthetic-root", profileId: "synthetic-profile", source: "codex" as const,
    installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId!,
    directory: path.join(root, "empty-source"), dispatch: [] };
  fs.mkdirSync(captureRoot.directory, { mode: 0o700 });
  const config = collectorConfigSchema.parse({ tenantId: workspaceId, deviceId,
    captureRoots: [captureRoot], port: await freePort() });
  const producerId = "synthetic-producer";
  const enrolled = provisionLiveProducer({ home, buffer, config, producerId,
    credentialId: "synthetic-credential", captureRootId: captureRoot.rootId, enrolledAt });
  const token = fs.readFileSync(enrolled.credentialFile, "utf8");
  buffer.close();

  const listener = runStopWindowListener(config, home, { mode: "maintenance_rebuild" });
  try {
    let ready = false;
    for (let attempt = 0; attempt < 100; attempt += 1) {
      try { ready = (await fetch(`http://127.0.0.1:${config.port}/healthz`)).status === 200; }
      catch { /* binding */ }
      if (ready) break;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(ready, true);
    const post = (credential: string) => fetch(`http://127.0.0.1:${config.port}/hooks/codex`, {
      method: "POST", headers: { "content-type": "application/json", "x-plimsoll-producer-id": producerId,
        "x-plimsoll-token": credential }, body: "{}",
    });
    const paused = await post(token);
    assert.equal(paused.status, 503, "enrolled live usage receives a retryable pause, not generic hook auth");
    assert.equal(paused.headers.get("retry-after"), "1");
    assert.equal((await paused.json() as { status?: string }).status, "maintenance_rebuild_paused");
    const invalid = await post(ordinary.codexProducer);
    assert.equal(invalid.status, 401, "the ordinary hook credential cannot impersonate a live producer");
    assert.equal(listHookSpoolFiles(home).length, 0, "503 creates no server hook spool");
    console.log(JSON.stringify({ check: "r4_live_usage_authenticated_before_hook_gate",
      enrolledStatus: paused.status, invalidStatus: invalid.status, retryAfter: paused.headers.get("retry-after") }));
  } finally {
    await releaseStopWindowListener(config.port, home).catch(() => false);
    await listener;
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
