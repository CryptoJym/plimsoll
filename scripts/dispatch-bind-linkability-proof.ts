import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";

import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { rootEventMetadata } from "../packages/collector-cli/src/capture-root-inventory";
import { sealOutboundEvent } from "../packages/collector-cli/src/outbound-envelope";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("dispatch-bind-linkability");
const home = process.env.PLIMSOLL_HOME!;
const directory = path.join(process.env.CODEX_HOME!, "sessions");
const configPath = path.join(home, "collector.config.json");
const projectKey = `sha256:${"a".repeat(64)}`;
const workItemId = "beads:eco-6hoxj.163.104";
const attemptId = "12345678-1234-4234-8234-123456789abc";

function cli(args: string[]) {
  const child = spawnSync(process.execPath, ["--import", path.resolve("node_modules/tsx/dist/loader.mjs"),
    path.resolve("packages/collector-cli/src/cli.ts"), ...args], {
    cwd: path.resolve("."), env: process.env, encoding: "utf8", timeout: 30_000,
  });
  if (child.error) throw child.error;
  return { code: child.status, stdout: child.stdout, stderr: child.stderr };
}

async function unusedPort(): Promise<number> {
  const listener = net.createServer();
  await new Promise<void>((resolve) => listener.listen(0, "127.0.0.1", resolve));
  const address = listener.address();
  assert.ok(address && typeof address !== "string");
  const port = address.port;
  await new Promise<void>((resolve) => listener.close(() => resolve()));
  assert.notEqual(port, 48271);
  return port;
}

async function main() {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const root = { rootId: "codex-root", profileId: "codex-profile",
    installationEpochId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    source: "codex" as const, directory };
  const config = collectorConfigSchema.parse({ port: await unusedPort(), captureRoots: [root] });
  fs.writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
  const validFrom = new Date(Date.now() - 60_000).toISOString();
  const bindArgs = (sessionId: string, work: string, attempt: string) => ["dispatch", "bind",
    "--session-id", sessionId, "--work-item-id", work, "--project-key", projectKey,
    "--attempt-id", attempt, "--valid-from", validFrom];

  const canonical = cli(bindArgs("canonical-session", workItemId, attemptId));
  assert.equal(canonical.code, 0, canonical.stderr);
  assert.equal(canonical.stderr.trim(), "");
  assert.equal(JSON.parse(canonical.stdout).linkage.state, "linkable");
  const metadata = rootEventMetadata(root, "canonical-event", new Date().toISOString(), "canonical-session");
  const event = aiInteractionEventSchema.parse({ id: "11111111-1111-4111-8111-111111111111",
    source: "codex", eventType: "assistant_response", dataMode: "metadata",
    observedAt: new Date().toISOString(), sessionId: "canonical-session",
    inputTokens: 1, outputTokens: 1, metadata });
  const sealed = sealOutboundEvent(event);
  assert.equal(sealed.ok, true);
  if (!sealed.ok) throw new Error("canonical event was not sealed");
  assert.deepEqual(sealed.event.metadata.work_ref, {
    schema: "work-ref/v1", work_id: "eco-6hoxj.163.104", run_id: attemptId,
  });
  completion.check("canonical_bind_links_on_work_ref_v1");

  const noncanonical = cli(bindArgs("unlinkable-session", "beads:invalid-work", "lane-1"));
  assert.equal(noncanonical.code, 0, noncanonical.stderr);
  assert.equal(JSON.parse(noncanonical.stdout).linkage.state, "unlinkable");
  assert.match(noncanonical.stderr, /--work-item-id.*canonical Beads ID/);
  assert.match(noncanonical.stderr, /--attempt-id.*canonical UUIDv4/);
  const stored = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
  assert.equal(stored.captureRoots?.[0].dispatch?.length, 2);
  const status = cli(["status", "--json"]);
  assert.equal(status.code, 0, status.stderr);
  assert.equal(JSON.parse(status.stdout).unlinkableBindCount, 1);
  assert.equal(status.stdout.includes("unlinkable-session"), false);
  assert.equal(status.stdout.includes("beads:invalid-work"), false);
  completion.check("legacy_bind_warns_and_is_counted_without_ids_in_status");

  const beforeStrict = fs.readFileSync(configPath);
  const strict = cli([...bindArgs("strict-session", "beads:invalid-work", "lane-2"), "--strict"]);
  assert.notEqual(strict.code, 0);
  assert.match(strict.stderr, /--work-item-id.*canonical Beads ID/);
  assert.match(strict.stderr, /--attempt-id.*canonical UUIDv4/);
  assert.deepEqual(fs.readFileSync(configPath), beforeStrict);
  completion.check("strict_rejects_unlinkable_bind_without_mutating_config");
  completion.complete();
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
