/** Operator-only pause drill. The argument must be a fresh cp -cR studio5 clone. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { performance } from "node:perf_hooks";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { LearningMaterializationStateStore } from "../packages/collector-cli/src/learning-materializer";
import { OutcomeTimelineStore } from "../packages/collector-cli/src/outcome-timeline-store";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { listHookSpoolFiles } from "../packages/collector-cli/src/hook-spool";
import { forwardHookOverLoopback } from "../packages/collector-cli/src/local-hook-client";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { connectionOwnershipClosed, observeRebuildConnectionOwnership,
  rebuildLedger } from "../packages/collector-cli/src/maintenance-rebuild";
import { exerciseRebuildWriterRoutes } from "./maintenance-rebuild-writer-routes";
import { createHookSpoolDrain } from "../packages/collector-cli/src/server";
import { releaseStopWindowListener, runStopWindowListener } from "../packages/collector-cli/src/stop-window-listener";

const cloneRoot = fs.realpathSync(process.argv[2] ?? "");
assert.ok(cloneRoot.includes("/eco-6hoxj.164.7/copy-"), "drill_requires_lane_clone");
const ledger = path.join(cloneRoot, "work-ledger.sqlite");
const hash = spawnSync("shasum", ["-a", "256", ledger], { encoding: "utf8" });
assert.equal(hash.status, 0, hash.stderr);
assert.equal(hash.stdout.split(" ")[0], "4a99680dc7af70d5458f29c4ff6a63a55177588a498eb5c33a8b4bfd69c7b1c4");

async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port === 48271 ? freePort() : port;
}
async function main() {
  const home = path.join(cloneRoot, "pause-home");
  fs.mkdirSync(home, { mode: 0o700 });
  const auth = loadOrCreateLocalIngestAuth(home);
  const port = await freePort();
  const config = collectorConfigSchema.parse({ port });
  const started = performance.now();
  const listener = runStopWindowListener(config, home, { mode: "maintenance_rebuild" });
  try {
    let ready = false;
    for (let n = 0; n < 100; n += 1) {
      try { ready = (await fetch(`http://127.0.0.1:${port}/healthz`)).status === 200; }
      catch { /* binding */ }
      if (ready) break;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(ready, true);
    const client = await forwardHookOverLoopback(JSON.stringify({ hook_event_name: "UserPromptSubmit",
      session_id: "studio5-copy-pause", timestamp: new Date().toISOString(), prompt: "fixture" }), {
      source: "claude_code", port, auth, env: { ...process.env, PLIMSOLL_HOME: home },
    });
    assert.ok("spooled" in client && client.spooled);
    assert.equal(listHookSpoolFiles(home).length, 1);
    const pending = captureSpoolState(home);
    assert.equal(pending.maintenanceRebuildPending, true);
    const bindingDb = new Database(ledger, { readonly: true, fileMustExist: true });
    const binding = bindingDb.prepare(`select current_workspace_id as workspaceId,
      current_device_id as deviceId, current_installation_epoch_id as epochId
      from collector_workspace_binding where singleton=1`).get() as
      { workspaceId: string; deviceId: string | null; epochId: string | null };
    bindingDb.close();
    const writer = new LocalEventBuffer(ledger, { workspaceId: binding.workspaceId,
      deviceId: binding.deviceId ?? undefined });
    let routeProbes: Awaited<ReturnType<typeof exerciseRebuildWriterRoutes>>;
    try {
      writer.useWorkspace(binding.workspaceId, binding.deviceId, binding.epochId ?? undefined);
      routeProbes = await exerciseRebuildWriterRoutes(writer, ledger, home);
      const held = observeRebuildConnectionOwnership(ledger);
      assert.equal(held.openTokens.length, 1);
      assert.ok(held.writerLeases.some((lease) => lease.owner === "local_event_buffer"));
      await assert.rejects(rebuildLedger({ ledgerPath: ledger, stage: "S10", walHighWaterBytes: 0,
        copyDrill: true,
        quiesce: async () => ({ before: held, after: held, connectionsClosed: connectionOwnershipClosed(held) }),
        resume: async () => undefined,
      }), /writer_not_quiesced/);
      console.log(JSON.stringify({ check: "studio5_copy_real_writer_entries_held",
        routes: routeProbes, observedOwner: held }));
    } finally { writer.close(); }
    assert.equal(connectionOwnershipClosed(observeRebuildConnectionOwnership(ledger)), true);
    const lock = `${ledger}.maintenance-rebuild.lock`;
    fs.writeFileSync(lock, `${process.pid}\n`);
    try {
      const ownership = observeRebuildConnectionOwnership(ledger);
      assert.equal(connectionOwnershipClosed(ownership), true);
      assert.throws(() => new LocalEventBuffer(ledger), /maintenance_rebuild_paused/);
      assert.throws(() => new OutcomeTimelineStore(ledger), /maintenance_rebuild_paused/);
      assert.throws(() => new LearningMaterializationStateStore(ledger), /maintenance_rebuild_paused/);
      const cli = path.resolve("packages/collector-cli/src/cli.ts");
      for (const args of [
        ["maintenance", "--disable-account-assertion", "codex", "--yes"],
        ["lifecycle", "pairing-indexes", "--apply"],
      ]) {
        const refused = spawnSync(process.execPath, ["--import", "tsx", cli, ...args], {
          cwd: process.cwd(), env: { ...process.env, PLIMSOLL_HOME: cloneRoot },
          encoding: "utf8", timeout: 30_000,
        });
        assert.notEqual(refused.status, 0);
        assert.match(refused.stderr, /maintenance_rebuild_paused/);
      }
      console.log(JSON.stringify({ check: "studio5_copy_writer_routes_during_pause",
        realEntriesExercised: routeProbes.length, independentStoresRefused: 2, ownership,
        independentCliOpenersRefused: 2 }));
    } finally { fs.unlinkSync(lock); }
    await releaseStopWindowListener(port, home);
    await listener;
    const pauseMs = performance.now() - started;
    const buffer = new LocalEventBuffer(ledger, { workspaceId: binding.workspaceId,
      deviceId: binding.deviceId ?? undefined });
    try {
      buffer.useWorkspace(binding.workspaceId, binding.deviceId, binding.epochId ?? undefined);
      assert.equal(buffer.eventAdmissionReason(new Date().toISOString()), null);
      const count = () => (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
      const before = count();
      const drain = createHookSpoolDrain(config, buffer, { home });
      const first = await drain.tick();
      const after = count();
      const second = await drain.tick();
      assert.equal(first.recovered, 1);
      assert.equal(second.recovered, 0);
      assert.equal(after, before + 1);
      const drained = captureSpoolState(home);
      assert.equal(drained.pendingFiles, 0);
      assert.equal(drained.maintenanceRebuildPending, false);
      console.log(JSON.stringify({ check: "studio5_copy_pause_client_retry", pauseMs,
        before, after, first, second, markerCleared: !fs.existsSync(path.join(home, "maintenance-rebuild-pause.json")) }));
    } finally { buffer.close(); }
  } finally {
    await releaseStopWindowListener(port, home).catch(() => false);
    await listener;
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
