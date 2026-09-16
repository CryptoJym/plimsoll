import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("status-http-rollout", 6);
/**
 * eco-6hoxj.154: /status stays credential-gated; GET /healthz stays the only
 * unauthenticated liveness surface; the migrated fleet reader never opens the
 * management route.
 *
 * Isolated temporary home + loopback listener only. No live collector, no
 * installed tool config, no credential printing.
 */
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";

type Result = { status: number; body: Record<string, unknown> };

const checks: Array<{ name: string; passed: boolean; detail: unknown }> = [];
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const readerPath = path.join(repoRoot, "scripts", "native-status-read.py");

function check(name: string, passed: boolean, detail: unknown) {
  checks.push({ name, passed, detail });
  completion.check(name, passed);
}

function runPython(args: string[], timeoutMs = 8_000) {
  return new Promise<{ status: number | null; stdout: string; stderr: string }>((resolve, reject) => {
    const child = spawn("python3", args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error("python_reader_timeout"));
    }, timeoutMs);
    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", (status) => {
      clearTimeout(timer);
      resolve({ status, stdout, stderr });
    });
  });
}

function request(
  port: number,
  route: string,
  method: "GET" | "POST" = "GET",
  headers: Record<string, string> = {},
): Promise<Result> {
  return new Promise((resolve, reject) => {
    const client = http.request(
      {
        host: "127.0.0.1",
        port,
        path: route,
        method,
        headers: { connection: "close", ...headers },
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        response.on("end", () => {
          let parsed: Record<string, unknown> = {};
          try {
            parsed = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
          } catch {
            parsed = {};
          }
          resolve({ status: response.statusCode ?? 0, body: parsed });
        });
      },
    );
    client.setTimeout(5_000, () => client.destroy(new Error("proof_request_timeout")));
    client.on("error", reject);
    client.end();
  });
}

async function main() {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-status-http-rollout-"));
  fs.chmodSync(home, 0o700);
  const auth = loadOrCreateLocalIngestAuth(home);
  const buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
  const server = createCollectorServer(
    collectorConfigSchema.parse({}),
    buffer,
    { localAuth: auth, perSourceRequestLimit: 10_000 },
  );

  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;

    const health = await request(port, "/healthz");
    const healthKeys = Object.keys(health.body).sort();
    check(
      "healthz_remains_ok_true_only",
      health.status === 200 && healthKeys.length === 1 && healthKeys[0] === "ok" && health.body.ok === true,
      { status: health.status, keys: healthKeys },
    );

    const closed = await request(port, "/status");
    check(
      "unauthenticated_status_stays_management_credential_required",
      closed.status === 401 &&
        closed.body.reason === "management_credential_required" &&
        closed.body.runtimeIdentity === undefined &&
        closed.body.stats === undefined &&
        closed.body.captureHealth === undefined,
      { status: closed.status, reason: closed.body.reason, keys: Object.keys(closed.body).sort() },
    );

    const opened = await request(port, "/status", "GET", { "x-plimsoll-token": auth.managementRead });
    check(
      "management_credential_still_opens_status",
      opened.status === 200 && opened.body.ok === true,
      { status: opened.status, ok: opened.body.ok },
    );

    const readerSource = fs.readFileSync(readerPath, "utf8");
    check(
      "fleet_reader_source_never_opens_management_route",
      readerSource.includes(HEALTHZ_MARK) &&
        !readerSource.includes('"/status"') &&
        !readerSource.includes("'/status'") &&
        !readerSource.includes("`/status`") &&
        !/\bx-plimsoll-token\b/i.test(readerSource),
      { bytes: Buffer.byteLength(readerSource) },
    );

    const selfTest = await runPython([readerPath, "--self-test"]);
    let selfTestBody: { status?: string; checks?: string[] } | null = null;
    try {
      selfTestBody = JSON.parse(selfTest.stdout.trim()) as { status?: string; checks?: string[] };
    } catch {
      selfTestBody = null;
    }
    check(
      "sweep_tick_boundary_is_transient_unless_it_persists_10s",
      selfTest.status === 0 &&
        selfTestBody?.status === "pass" &&
        Array.isArray(selfTestBody.checks) &&
        selfTestBody.checks.includes("persisted_lie_is_check") &&
        selfTestBody.checks.includes("single_lie_is_transient") &&
        selfTestBody.checks.includes("boundary_then_progress_is_transient"),
      {
        exit: selfTest.status,
        stderr: (selfTest.stderr ?? "").trim().slice(0, 240),
        stdout: (selfTest.stdout ?? "").trim().slice(0, 240),
      },
    );

    const python = await runPython([readerPath, "--liveness-only", "--port", String(port)]);
    let liveness: Record<string, unknown> | null = null;
    try {
      liveness = JSON.parse(python.stdout.trim()) as Record<string, unknown>;
    } catch {
      liveness = null;
    }
    const stdout = python.stdout ?? "";
    check(
      "migrated_fleet_reader_uses_healthz_only",
      python.status === 0 &&
        liveness?.ok === true &&
        liveness.port === port &&
        Object.keys(liveness).sort().join(",") === "ok,port" &&
        !stdout.includes(auth.managementRead) &&
        !stdout.includes(auth.claudeCodeProducer) &&
        !stdout.includes(auth.codexProducer),
      {
        exit: python.status,
        stderr: (python.stderr ?? "").trim().slice(0, 240),
        stdout: (python.stdout ?? "").trim().slice(0, 240),
        keys: liveness ? Object.keys(liveness) : [],
      },
    );

  } finally {
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
    fs.rmSync(home, { recursive: true, force: true });
  }

  for (const result of checks) {
    console.log(`${result.passed ? "PASS" : "FAIL"} ${result.name} ${JSON.stringify(result.detail)}`);
  }
  const failed = checks.filter((result) => !result.passed);
  console.log(JSON.stringify({ checks: checks.length, passed: checks.length - failed.length, failed: failed.length }));
  if (failed.length > 0) process.exitCode = 1;
  completion.complete();
}

const HEALTHZ_MARK = "/healthz";

main().catch((error) => {
  console.error(JSON.stringify({
    error: "status_http_rollout_proof_failed",
    reason: error instanceof Error ? error.message : "unknown",
  }));
  process.exitCode = 1;
});
