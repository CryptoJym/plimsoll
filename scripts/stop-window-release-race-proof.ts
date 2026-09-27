import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { listHookSpoolFiles } from "../packages/collector-cli/src/hook-spool";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { runStopWindowListener, STOP_WINDOW_RELEASE_PATH } from "../packages/collector-cli/src/stop-window-listener";

async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port === 48271 ? freePort() : port;
}
async function status(port: number) {
  try { return (await fetch(`http://127.0.0.1:${port}/healthz`)).status; }
  catch { return 0; }
}
async function main() {
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "plimsoll-release-race-"));
  try {
    for (const mode of ["stop_window", "maintenance_rebuild"] as const) {
      const home = path.join(root, mode);
      fs.mkdirSync(home, { mode: 0o700 });
      const auth = loadOrCreateLocalIngestAuth(home);
      const port = await freePort();
      const listener = runStopWindowListener(collectorConfigSchema.parse({ port }), home, { mode });
      try {
        for (let n = 0; n < 100 && (await status(port)) !== 200; n += 1) {
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        assert.equal(await status(port), 200);
        const body = JSON.stringify({ hook_event_name: "UserPromptSubmit", session_id: "release-race",
          timestamp: "2026-09-27T00:00:00.000Z", prompt: "private" });
        const prefix = body.slice(0, 12);
        let responseStatus = 0;
        const push = new Promise<void>((resolve, reject) => {
          const request = http.request({ hostname: "127.0.0.1", port, path: "/hooks/claude-code", method: "POST",
            headers: { "content-type": "application/json", "content-length": Buffer.byteLength(body),
              "x-plimsoll-token": auth.claudeCodeProducer } }, (response) => {
            responseStatus = response.statusCode ?? 0;
            response.resume();
            response.on("end", resolve);
          });
          request.on("error", reject);
          request.write(prefix);
          // Release while this accepted push is still awaiting its body.
          void (async () => {
            await new Promise((done) => setTimeout(done, 25));
            const release = await fetch(`http://127.0.0.1:${port}${STOP_WINDOW_RELEASE_PATH}`, {
              method: "POST", headers: { "x-plimsoll-token": auth.managementRead },
            });
            assert.equal(release.status, 200);
            request.end(body.slice(prefix.length));
          })().catch(reject);
        });
        await push;
        await listener;
        const files = listHookSpoolFiles(home);
        if (mode === "stop_window") {
          assert.equal(responseStatus, 202);
          assert.equal(files.length, 1, "202 has a durable server spool file");
          assert.ok(fs.statSync(files[0]!.path).size > 0);
        } else {
          assert.equal(responseStatus, 503);
          assert.equal(files.length, 0, "503 changes no server spool state");
        }
        console.log(JSON.stringify({ check: `concurrent_release_push_${mode}`, responseStatus,
          durableSpoolFiles: files.length }));
      } finally {
        if ((await status(port)) === 200) {
          await fetch(`http://127.0.0.1:${port}${STOP_WINDOW_RELEASE_PATH}`, {
            method: "POST", headers: { "x-plimsoll-token": auth.managementRead },
          }).catch(() => undefined);
        }
        await listener;
      }
    }
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
