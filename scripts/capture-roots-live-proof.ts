/**
 * An unregistered native home can already report through the live producer.
 * Fixture homes and the collector config stay under the isolated proof HOME.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline } from "../packages/collector-cli/src/capture-baseline";
import { deriveCaptureRootIdentity } from "../packages/collector-cli/src/capture-root-inventory";
import { runCodexReconciliationMaintenance } from "../packages/collector-cli/src/codex-reconciliation";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { useFixtureRoot } from "./lib/fixture-root";

const repo = path.resolve(import.meta.dirname, "..");
const cli = path.join(repo, "packages/collector-cli/src/cli.ts");
const tsx = path.join(repo, "node_modules/tsx/dist/loader.mjs");
const machine = "fixture-machine";
const workspace = "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10";
const device = "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d";
const checks: string[] = [];

function check(name: string, condition: unknown, detail: unknown) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push(name);
}

async function freePort() {
  const server = net.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return address.port;
}

async function main() {
  const sandbox = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-live-roots-")));
  const fixture = useFixtureRoot(sandbox, { home: path.join(sandbox, "home") });
  const home = fixture.home;
  const data = path.join(home, ".plimsoll");
  const port = await freePort();
  try {
    const registered = path.join(home, ".codex/sessions");
    const codexLive = path.join(home, ".codex-profiles/live/sessions");
    const claudeHooks = path.join(home, ".claude-seats/hooks/projects");
    const claudeOtel = path.join(home, ".claude-seats/otel/projects");
    const gap = path.join(home, ".claude-seats/gap/projects");
    for (const directory of [registered, codexLive, claudeHooks, claudeOtel, gap, data]) {
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    }
    fs.writeFileSync(path.join(home, ".codex-profiles/live/config.toml"),
      `[otel.trace_exporter."otlp-http"]\nendpoint = "http://127.0.0.1:${port}/v1/traces"\nprotocol = "json"\nheaders = { "x-plimsoll-source" = "codex", "x-plimsoll-token" = "never-print-this-token" }\n`,
      { mode: 0o600 });
    fs.writeFileSync(path.join(home, ".claude-seats/hooks/settings.json"), JSON.stringify({
      hooks: { Stop: [{ hooks: [{ type: "http", url: `http://127.0.0.1:${port}/hooks/claude-code`,
        headers: { "x-plimsoll-source": "claude_code", "x-plimsoll-token": "never-print-this-token" } }] }] },
    }), { mode: 0o600 });
    fs.writeFileSync(path.join(home, ".claude-seats/otel/settings.json"), JSON.stringify({
      env: { CLAUDE_CODE_ENABLE_TELEMETRY: "1", OTEL_LOGS_EXPORTER: "otlp",
        OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: `http://127.0.0.1:${port}/v1/logs`,
        OTEL_EXPORTER_OTLP_HEADERS: "x-plimsoll-source=claude_code,x-plimsoll-token=never-print-this-token" },
    }), { mode: 0o600 });
    fs.writeFileSync(path.join(home, ".claude-seats/gap/settings.json"), JSON.stringify({
      env: { OTEL_LOGS_EXPORTER: "otlp", OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: "http://127.0.0.1:1/v1/logs" },
    }), { mode: 0o600 });

    const ledger = path.join(data, "work-ledger.sqlite");
    const buffer = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device });
    let epoch: string;
    try {
      epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
      for (const source of ["codex", "claude_code"] as const) {
        const begun = beginAutomaticCaptureBaseline(buffer.database, source,
          { startedAt: "2026-01-01T00:00:00.000Z", filesDiscovered: 0 });
        completeAutomaticCaptureBaseline(buffer.database, source,
          { runId: begun.latestRun!.runId, completedAt: "2026-01-01T00:00:00.000Z" });
      }
    } finally { buffer.close(); }
    const config = collectorConfigSchema.parse({
      port, tenantId: workspace, deviceId: device, installKey: "fixture-install-key", managed: true,
      captureRoots: [{ ...deriveCaptureRootIdentity(machine, "codex", registered),
        installationEpochId: epoch, source: "codex", directory: registered }],
    });
    fs.writeFileSync(path.join(data, "collector.config.json"), `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
    const env = { ...process.env, ...fixture.env, PATH: process.env.PATH! };
    const run = (args: string[]) => {
      const result = spawnSync(process.execPath, ["--import", tsx, cli, ...args], {
        cwd: sandbox, env, encoding: "utf8", timeout: 30_000,
      });
      const payload = JSON.parse(result.stdout || "{}") as Record<string, any>;
      return { code: result.status, payload, stderr: result.stderr };
    };
    const discovered = run(["capture-roots", "discover", "--json"]);
    check("discover_separates_three_live_paths_from_one_gap",
      discovered.code === 0 && discovered.payload.counts?.candidate === 1 &&
      discovered.payload.counts?.liveCovered === 3 &&
      discovered.payload.liveCovered?.length === 3 &&
      discovered.payload.roots?.filter((entry: any) => entry.state === "candidate").length === 1 &&
      discovered.payload.roots?.some((entry: any) => entry.directory === ".claude-seats/gap/projects"),
      discovered.payload);
    const live = discovered.payload.liveCovered as Array<{ directory: string; evidence: string[] }>;
    check("discover_names_live_path_keys_without_values",
      live.some((entry) => entry.directory === ".codex-profiles/live/sessions" &&
        entry.evidence.includes("otel.trace_exporter.otlp-http.headers.x-plimsoll-source")) &&
      live.some((entry) => entry.directory === ".claude-seats/hooks/projects" &&
        entry.evidence.includes("hooks.Stop.hooks.url")) &&
      live.some((entry) => entry.directory === ".claude-seats/otel/projects" &&
        entry.evidence.includes("env.OTEL_EXPORTER_OTLP_HEADERS")) &&
      !JSON.stringify(live).includes("never-print-this-token") &&
      !JSON.stringify(live).includes(`127.0.0.1:${port}`), live);

    const addArgs = ["capture-roots", "add", "--source", "codex", "--directory", codexLive,
      "--machine", machine, "--json"];
    const planned = run([...addArgs, "--dry-run"]);
    check("dry_run_warns_that_live_reporting_and_file_capture_will_coexist",
      planned.code === 0 && planned.payload.status === "capture_roots_add_plan" &&
      planned.payload.warnings?.some((warning: string) => /already reports live/i.test(warning) &&
        /file path/i.test(warning)), planned.payload);
    const applied = run(addArgs);
    check("add_enrolls_live_home_and_keeps_plain_warning",
      applied.code === 0 && applied.payload.status === "capture_roots_added" &&
      applied.payload.warnings?.some((warning: string) => /already reports live/i.test(warning) &&
        /file path/i.test(warning)), applied.payload);
    const after = run(["capture-roots", "discover", "--json"]);
    check("enrolled_live_home_moves_to_registered_without_new_gap",
      after.code === 0 && after.payload.counts?.registered === 2 &&
      after.payload.counts?.liveCovered === 2 && after.payload.counts?.candidate === 1,
      after.payload.counts);
    // Use the root written by add, the existing ledger, and the 0.7.42
    // rollout tailer. The unchanged pairing proofs below cover the two OTLP
    // shapes; this fixture checks the newly enabled file path beside them.
    const enrolledConfig = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(
      path.join(data, "collector.config.json"), "utf8")));
    const enrolledRoot = enrolledConfig.captureRoots?.find((root) => root.directory === codexLive);
    check("added_root_is_the_file_path_in_the_config", Boolean(enrolledRoot),
      enrolledConfig.captureRoots?.map((root) => root.directory));
    const session = "019e9100-0000-7000-8000-000000000091";
    const now = new Date();
    const timestamp = now.toISOString();
    const day = path.join(codexLive, timestamp.slice(0, 4), timestamp.slice(5, 7), timestamp.slice(8, 10));
    fs.mkdirSync(day, { recursive: true });
    const line = (type: string, payload: Record<string, unknown>, at = timestamp) =>
      JSON.stringify({ timestamp: at, type, payload });
    fs.writeFileSync(path.join(day, `rollout-${timestamp.replace(/[:.]/g, "-")}-${session}.jsonl`), [
      line("session_meta", { id: session, cwd: sandbox }),
      line("turn_context", { model: "gpt-5.5", cwd: sandbox }),
      line("event_msg", { type: "token_count", info: { total_token_usage: {
        input_tokens: 0, cached_input_tokens: 0, output_tokens: 0,
        reasoning_output_tokens: 0, total_tokens: 0 } } }),
      line("event_msg", { type: "token_count", info: { total_token_usage: {
        input_tokens: 2400, cached_input_tokens: 0, output_tokens: 510,
        reasoning_output_tokens: 0, total_tokens: 2910 } } }),
    ].join("\n") + "\n", { mode: 0o600 });
    const enrolledBuffer = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device });
    try {
      const liveEvent = aiInteractionEventSchema.parse({
        id: "fixture-live-response-91", tenantId: workspace, source: "codex",
        dataMode: "metadata", eventType: "assistant_response", observedAt: timestamp,
        sessionId: session, inputTokens: 2400, outputTokens: 510,
        metadata: { otelEventName: "codex.sse_event", serviceName: "codex-app-server" },
      });
      const liveAppended = enrolledBuffer.append(liveEvent);
      const scan = await new RolloutTailer(enrolledBuffer, codexLive, () => [], undefined,
        [enrolledRoot!]).scan({ scope: "full" });
      runCodexReconciliationMaintenance(enrolledBuffer.database);
      const rows = enrolledBuffer.database.prepare(`select event_type as eventType,
          input_tokens as inputTokens, output_tokens as outputTokens
        from buffered_events where source = 'codex' and session_id = ?
          and input_tokens is not null`).all(session) as Array<{
            eventType: string; inputTokens: number; outputTokens: number }>;
      check("enrolled_live_response_and_file_path_reconcile_to_one_row",
        liveAppended && scan.sessionsSkippedOtlpCovered === 1 &&
        rows.length === 1 && rows[0]?.eventType === "assistant_response" &&
        rows[0]?.inputTokens === 2400 && rows[0]?.outputTokens === 510,
        { liveAppended, skipped: scan.sessionsSkippedOtlpCovered, rows });
    } finally { enrolledBuffer.close(); }
    // These are the 0.7.42 response-pair and reconciliation fixtures, run
    // unchanged after enrollment. They assert one countable row per response.
    for (const script of ["codex-reconciliation-proof.ts", "codex-usage-pairing-proof.ts"]) {
      const result = spawnSync(process.execPath, ["--import", tsx, path.join(repo, "scripts", script)],
        { cwd: repo, env, encoding: "utf8", timeout: 180_000 });
      if (result.stdout) process.stdout.write(result.stdout);
      if (result.stderr) process.stderr.write(result.stderr);
      check(`${script}_keeps_one_row_per_response_after_enrollment`, result.status === 0,
        { code: result.status, stderr: result.stderr?.slice(-500) });
    }
    console.log(JSON.stringify({ status: "pass", proof: "capture-roots-live", checks }));
  } finally {
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
