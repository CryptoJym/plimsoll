/** A local peer that accepts a request but never responds must have a deadline. */
import { EventEmitter } from "node:events";
import http from "node:http";
import type { AddressInfo } from "node:net";

import { fetchCollectorUrl } from "../packages/collector-cli/src/http-transport";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("direct-loopback-timeout", 2);
const realSetTimeout = globalThis.setTimeout;
const scheduled = { connect: 0, headers: 0 };
const errorCode = (error: unknown) => (error as { code?: string })?.code ?? String(error);

async function main() {
  const stalled = http.createServer(() => { /* Intentionally never send headers. */ });
  await new Promise<void>((resolve) => stalled.listen(0, "127.0.0.1", resolve));
  const port = (stalled.address() as AddressInfo).port;
  let requests = 0;
  stalled.on("request", () => { requests += 1; });
  // Shorten only the two production deadlines for this proof. The fallback
  // guard stays on the real clock so the original missing deadlines fail.
  globalThis.setTimeout = ((callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
    if (delay === 10_000) { scheduled.connect += 1; delay = 120; }
    if (delay === 300_000) { scheduled.headers += 1; delay = 240; }
    return (realSetTimeout as (...values: unknown[]) => ReturnType<typeof setTimeout>)(callback, delay, ...args);
  }) as typeof setTimeout;
  try {
    const signal = AbortSignal.timeout(1_500);
    let headerFailure = "none";
    try {
      await fetchCollectorUrl(`http://127.0.0.1:${port}/hooks/codex`, { signal });
    } catch (error) { headerFailure = errorCode(error); }
    completion.check("stalled_local_server_hits_header_deadline",
      requests === 1 && headerFailure === "UND_ERR_HEADERS_TIMEOUT" && scheduled.headers > 0);

    // A synthetic socket that never connects checks the independent connect
    // deadline without depending on OS backlog behavior.
    const originalRequest = http.request;
    const pending = new EventEmitter() as EventEmitter & {
      end: () => void; destroy: (error: Error) => void;
    };
    pending.end = () => { queueMicrotask(() => pending.emit("socket",
      Object.assign(new EventEmitter(), { connecting: true }))); };
    pending.destroy = (error) => { queueMicrotask(() => { pending.emit("error", error); pending.emit("close"); }); };
    (http as { request: typeof http.request }).request = (() => pending) as unknown as typeof http.request;
    let connectFailure = "none";
    try {
      await Promise.race([
        fetchCollectorUrl(`http://127.0.0.1:${port}/connect-stall`),
        new Promise<never>((_resolve, reject) =>
          realSetTimeout(() => reject(new Error("guard_timeout")), 700)),
      ]);
    } catch (error) { connectFailure = errorCode(error); }
    finally { (http as { request: typeof http.request }).request = originalRequest; }
    completion.check("stalled_connect_hits_connect_deadline",
      connectFailure === "UND_ERR_CONNECT_TIMEOUT" && scheduled.connect > 0);
    console.log(JSON.stringify({ schema: "plimsoll.direct-loopback-timeout-proof/v1",
      headerFailure, connectFailure, requests, scheduled }));
    completion.complete();
  } finally {
    globalThis.setTimeout = realSetTimeout;
    stalled.closeAllConnections();
    await new Promise<void>((resolve) => stalled.close(() => resolve()));
  }
}

void main().catch((error) => { console.error(error); process.exitCode = 1; });
