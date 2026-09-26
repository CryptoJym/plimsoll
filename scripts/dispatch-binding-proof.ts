import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { dispatchBindingMetadata, dispatchBindingSchema, namespacedWorkItemIdSchema, rootEventMetadata } from "../packages/collector-cli/src/capture-root-inventory";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { remoteLinkageHash } from "../packages/shared/src/linkage";
import { validatedMetadataAttribute } from "../packages/shared/src/analytical-metadata";
import { createProofCompletion } from "./lib/proof-completion";

const proof = createProofCompletion("dispatch-binding");
async function main() {
const root = process.env.PLIMSOLL_PROOF_ROOT!;
const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
const codexDirectory = path.join(home, ".codex", "sessions");
const claudeDirectory = path.join(home, ".claude", "projects");
for (const directory of [plimsoll, codexDirectory, claudeDirectory]) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
}
const baseConfig = collectorConfigSchema.parse({ deviceId: "dev_dispatch-device-1", uploadUrl: "http://127.0.0.1:1/unused" });
const ledgerPath = path.join(plimsoll, "work-ledger.sqlite");
const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: baseConfig.tenantId,
  deviceId: baseConfig.deviceId, enrollmentNow: () => new Date("2026-09-01T00:00:00.000Z"),
  delivery: { enabled: true } });
const installationEpochId = buffer.workspaceBinding()?.currentInstallationEpochId;
assert.ok(installationEpochId);
const roots = ([
  { rootId: "codex-root", profileId: "codex-profile", installationEpochId, source: "codex", directory: codexDirectory },
  { rootId: "claude-root", profileId: "claude-profile", installationEpochId, source: "claude_code", directory: claudeDirectory },
] as const).map((entry) => ({ ...entry }));
const configPath = path.join(plimsoll, "collector.config.json");
fs.writeFileSync(configPath, `${JSON.stringify(collectorConfigSchema.parse({ ...baseConfig, captureRoots: roots }), null, 2)}\n`, { mode: 0o600 });

function cli(args: string[]) {
  const result = spawnSync(process.execPath, ["--import", path.resolve("node_modules/tsx/dist/loader.mjs"),
    path.resolve("packages/collector-cli/src/cli.ts"), ...args], {
    cwd: path.resolve("."), encoding: "utf8", timeout: 30_000,
    env: { ...process.env, HOME: home, USERPROFILE: home, PLIMSOLL_HOME: plimsoll,
      CODEX_HOME: path.join(home, ".codex"), CLAUDE_CONFIG_DIR: path.join(home, ".claude") },
  });
  return { code: result.status, stdout: result.stdout, stderr: result.stderr };
}

const key = `sha256:${"a".repeat(64)}`;
for (const valid of ["beads:eco-6hoxj.165.3", "github:CryptoJym/plimsoll/pull/42", "github:123456/pull/42",
  `github:sha256:${"a".repeat(64)}/pull/42`, "jira:PLIM-42"])
  assert.equal(namespacedWorkItemIdSchema.parse(valid), valid);
for (const invalid of ["eco-6hoxj.165.3", "github:CryptoJym/plimsoll/pull/0", "jira:"])
  assert.equal(namespacedWorkItemIdSchema.safeParse(invalid).success, false);
proof.check("work_item_namespace_contract");
const githubRepoHash = remoteLinkageHash("https://github.com/CryptoJym/plimsoll.git");
assert.ok(githubRepoHash);
for (const [index, workItemId, outboundWorkItemId] of [
  [0, "beads:eco-6hoxj.165.3", "beads:eco-6hoxj.165.3"],
  [1, "github:CryptoJym/plimsoll/pull/42", `github:${githubRepoHash}/pull/42`],
  [2, "github:123456/pull/42", "github:123456/pull/42"],
  [3, "jira:PLIM-42", "jira:PLIM-42"],
] as const) {
  const binding = dispatchBindingSchema.parse({ sessionId: `delivery-${index}`, workItemId,
    projectKey: key, companyRef: null, attemptId: `delivery-attempt-${index}`,
    parentAttemptId: null, acceptedOutcomeId: null,
    validFrom: "2026-09-25T00:00:00.000Z", validUntil: null, evidenceRef: `delivery-evidence-${index}` });
  const id = `delivery-event-${index}`;
  assert.equal(buffer.append({ id, source: "codex", eventType: "assistant_response", dataMode: "metadata",
    observedAt: "2026-09-25T00:00:01.000Z", sessionId: binding.sessionId,
    actionClass: "other", intent: "unknown", inputTokens: 1, outputTokens: 1,
    metadata: dispatchBindingMetadata(binding) }, []), true);
  const raw = buffer.database.prepare("select privacy_disposition as disposition from buffered_events where id=?")
    .get(id) as { disposition: string | null };
  assert.equal(raw.disposition, null, workItemId);
  const queued = buffer.database.prepare("select base_envelope_json as envelope from upload_outbox where raw_id=?")
    .get(id) as { envelope: string } | undefined;
  assert.ok(queued, workItemId);
  assert.equal(JSON.parse(queued.envelope).event.metadata.workItemId, outboundWorkItemId);
  assert.equal(queued.envelope.includes("CryptoJym/plimsoll"), false);
}
proof.check("all_supported_work_item_namespaces_queue_private_outbound_usage");
for (const malformed of ["github:CryptoJym/plimsoll/issues/42", "github:CryptoJym/plimsoll/pull/0",
  "github:CryptoJym/plimsoll/pull/42/extra", "github:../plimsoll/pull/42", "github:plimsoll"]) {
  assert.equal(namespacedWorkItemIdSchema.safeParse(malformed).success, false);
  assert.equal(validatedMetadataAttribute("workItemId", malformed).accepted, false);
}
proof.check("malformed_github_work_items_remain_refused");
const bound = cli(["dispatch", "bind", "--session-id", "codex-session", "--work-item-id", "beads:eco-6hoxj.165.3",
  "--project-key", key, "--attempt-id", "lane-1", "--parent-attempt-id", "lead-session",
  "--role", "author", "--work-class", "implementation", "--complexity-band", "medium",
  "--technique-id", "tech-1", "--technique-version", "1", "--assignment-id", "assign-1",
  "--arm", "control", "--launched-by", "fleet-delegate", "--valid-from", "2026-09-25T00:00:00.000Z"]);
assert.equal(bound.code, 0, bound.stderr || bound.stdout);
assert.equal(JSON.parse(bound.stdout).status, "dispatch_bound");
const configured = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
assert.equal(configured.captureRoots?.length, 2);
for (const captureRoot of configured.captureRoots ?? []) {
  assert.equal(captureRoot.dispatch?.length, 1);
  assert.equal(captureRoot.dispatch?.[0].workItemId, "beads:eco-6hoxj.165.3");
}
proof.check("bind_upserts_all_registered_roots");
assert.equal(cli(["dispatch", "bind", "--session-id", "codex-session", "--work-item-id", "beads:eco-6hoxj.165.3",
  "--project-key", key, "--attempt-id", "lane-1", "--parent-attempt-id", "lead-session",
  "--role", "author", "--work-class", "implementation", "--complexity-band", "medium",
  "--technique-id", "tech-1", "--technique-version", "1", "--assignment-id", "assign-1",
  "--arm", "control", "--launched-by", "fleet-delegate", "--valid-from", "2026-09-25T00:00:00.000Z"]).code, 0);
assert.equal(collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8"))).captureRoots?.[0].dispatch?.length, 1);
proof.check("repeat_bind_is_idempotent");

const rootMetadata = rootEventMetadata(roots[0], "first-codex-usage", "2026-09-25T00:00:01.000Z", "codex-session");
assert.equal(rootMetadata.workItemId, "beads:eco-6hoxj.165.3");
assert.equal(rootMetadata.parentAttemptId, "lead-session");
assert.equal(rootMetadata.role, "author");
assert.equal(rootMetadata.techniqueId, "tech-1");
proof.check("running_capture_reads_new_binding_without_restart");

const attr = (key: string, value: string) => ({ key, value: { stringValue: value } });
const otlp = explodeOtlpPayload({ resourceLogs: [{ resource: { attributes: [attr("service.name", "codex_exec")] },
  scopeLogs: [{ logRecords: [{ observedTimeUnixNano: String(Date.parse("2026-09-25T00:00:02.000Z") * 1_000_000),
    attributes: [attr("event.name", "codex.sse_event"), attr("conversation.id", "codex-session"),
      attr("input_token_count", "12"), attr("output_token_count", "4")],
  }] }] }] }, { source: "codex" });
assert.equal(otlp.events.length, 1);
const log = otlp.events[0].event;
for (const [field, expected] of Object.entries({ workItemId: "beads:eco-6hoxj.165.3",
  dispatchProjectKey: key, attemptId: "lane-1", role: "author", parentAttemptId: "lead-session",
  techniqueId: "tech-1", techniqueVersion: "1", assignmentId: "assign-1", arm: "control" })) {
  assert.equal(log.metadata[field], expected, field);
}
proof.check("codex_otlp_log_carries_dispatch_and_technique_fields");

const firstTool = {
  id: "dispatch-tool-first", source: "codex" as const, eventType: "tool_use" as const,
  dataMode: "metadata" as const, observedAt: "2026-09-25T00:00:03.000Z",
  sessionId: "codex-session", actionClass: "shell" as const, intent: "unknown" as const,
  metadata: { call_id: "dispatch-op-first" },
};
assert.equal(buffer.append(firstTool, []), true);
assert.equal(buffer.learningFacts.episodes().length, 1);
assert.equal(buffer.learningFacts.episodes()[0].workClass, "implementation");
assert.equal(buffer.learningFacts.episodes()[0].complexityBand, "medium");
assert.equal(buffer.learningFacts.exposures().length, 1);
assert.equal(buffer.learningFacts.exposures()[0].exposedAt, firstTool.observedAt);
assert.equal(buffer.learningFacts.exposures()[0].mode, "control");
assert.equal(buffer.learningFacts.exposures()[0].assignmentId, "assign-1");
assert.equal(buffer.append({ ...firstTool, id: "dispatch-tool-second", observedAt: "2026-09-25T00:00:04.000Z",
  metadata: { call_id: "dispatch-op-second" } }, []), true);
assert.equal(buffer.learningFacts.exposures().length, 1);
proof.check("technique_exposed_once_at_first_tool_episode_with_bound_dimensions");

const claudeSession = "44445555-6666-4777-8888-99990000aaaa";
const claudeBind = cli(["dispatch", "bind", "--session-id", claudeSession,
  "--work-item-id", "beads:claude-work", "--project-key", key, "--attempt-id", "claude-lane",
  "--parent-attempt-id", "lead-session", "--role", "reviewer",
  "--valid-from", "2026-09-25T00:00:00.000Z"]);
assert.equal(claudeBind.code, 0, claudeBind.stderr);
const claudeProject = path.join(claudeDirectory, "-proof-project");
fs.mkdirSync(claudeProject, { recursive: true });
fs.writeFileSync(path.join(claudeProject, `${claudeSession}.jsonl`), [1, 2].map((n) => JSON.stringify({
  type: "assistant", sessionId: claudeSession, cwd: home,
  timestamp: `2026-09-25T00:01:0${n}.000Z`,
  message: { id: `claude-msg-${n}`, model: "claude-sonnet-4-20250514", content: [],
    usage: { input_tokens: 10 * n, output_tokens: n, cache_read_input_tokens: 0,
      cache_creation_input_tokens: 0 } },
})).join("\n") + "\n");
const transcript = new TranscriptTailer(buffer, claudeDirectory, undefined, [roots[1]]);
try { await transcript.scan({ scope: "full" }); } finally { transcript.close(); }
const claudeRows = buffer.database.prepare(`select payload_json from buffered_events
  where session_id=? and event_type='usage_transcript'`).all(claudeSession) as Array<{payload_json:string}>;
assert.equal(claudeRows.length, 2);
for (const row of claudeRows) {
  const metadata = JSON.parse(row.payload_json).metadata;
  assert.deepEqual([metadata.workItemId, metadata.dispatchProjectKey, metadata.attemptId,
    metadata.role, metadata.parentAttemptId], ["beads:claude-work", key, "claude-lane", "reviewer", "lead-session"]);
}
proof.check("all_tagged_claude_transcript_usage_rows_carry_dispatch_fields");

const rolloutSession = "019e1111-2222-7333-8444-555555555555";
const rolloutBind = cli(["dispatch", "bind", "--session-id", rolloutSession,
  "--work-item-id", "beads:rollout-work", "--project-key", key, "--attempt-id", "rollout-lane",
  "--role", "author", "--valid-from", "2026-09-25T00:00:00.000Z"]);
assert.equal(rolloutBind.code, 0, rolloutBind.stderr);
const rolloutDay = path.join(codexDirectory, "2026", "09", "25");
fs.mkdirSync(rolloutDay, { recursive: true });
const rolloutLine = (timestamp: string, type: string, payload: Record<string, unknown>) => JSON.stringify({ timestamp, type, payload });
const tokenLine = (timestamp: string, input: number, output: number) => rolloutLine(timestamp, "event_msg", {
  type: "token_count", info: { total_token_usage: { input_tokens: input,
    cached_input_tokens: 0, output_tokens: output, reasoning_output_tokens: 0, total_tokens: input + output } },
});
fs.writeFileSync(path.join(rolloutDay, `rollout-2026-09-25T00-02-00-${rolloutSession}.jsonl`), [
  rolloutLine("2026-09-25T00:02:00.000Z", "session_meta", { id: rolloutSession, cwd: home, originator: "codex_exec" }),
  rolloutLine("2026-09-25T00:02:01.000Z", "turn_context", { model: "gpt-6-sol", cwd: home }),
  tokenLine("2026-09-25T00:02:02.000Z", 10, 2),tokenLine("2026-09-25T00:02:03.000Z", 18, 4),
].join("\n") + "\n");
const rollout = new RolloutTailer(buffer, codexDirectory, () => [], undefined, [roots[0]]);
try { await rollout.scan({ scope: "full" }); } finally { rollout.close(); }
const rolloutRows = buffer.database.prepare(`select payload_json from buffered_events
  where session_id=? and event_type='usage_rollout'`).all(rolloutSession) as Array<{payload_json:string}>;
assert.equal(rolloutRows.length, 2);
for (const row of rolloutRows) {
  const metadata = JSON.parse(row.payload_json).metadata;
  assert.deepEqual([metadata.workItemId, metadata.dispatchProjectKey, metadata.attemptId, metadata.role],
    ["beads:rollout-work", key, "rollout-lane", "author"]);
}
proof.check("all_tagged_codex_rollout_usage_rows_carry_dispatch_fields");
assert.equal(buffer.append(log, []), true);
const otlpStored = buffer.database.prepare("select payload_json from buffered_events where id=?")
  .get(log.id) as { payload_json:string };
assert.equal(JSON.parse(otlpStored.payload_json).metadata.workItemId, "beads:eco-6hoxj.165.3");
const otlpOutbox = buffer.database.prepare("select base_envelope_json from upload_outbox where raw_id=?")
  .get(log.id) as { base_envelope_json:string };
const otlpOutbound = JSON.parse(otlpOutbox.base_envelope_json).event.metadata;
assert.equal(otlpOutbound.role, "author");
assert.equal(otlpOutbound.techniqueId, "tech-1");
assert.equal(otlpOutbound.assignmentId, "assign-1");
assert.equal(otlpOutbound.arm, "control");
proof.check("codex_otlp_log_usage_is_persisted_with_dispatch_fields");

const lateEvent = { id: "dispatch-late-usage", source: "codex" as const, eventType: "assistant_response" as const,
  dataMode: "metadata" as const, observedAt: "2026-09-25T00:10:00.000Z", sessionId: "late-session",
  actionClass: "other" as const, intent: "unknown" as const, inputTokens: 9, outputTokens: 3, metadata: {} };
assert.equal(buffer.append(lateEvent, []), true);
const attemptedEvent = { ...lateEvent, id: "dispatch-late-attempted", observedAt: "2026-09-25T00:10:01.000Z" };
assert.equal(buffer.append(attemptedEvent, []), true);
buffer.database.prepare("update upload_outbox set attempt_count=1 where raw_id=?").run(attemptedEvent.id);
const rawMetadata = () => JSON.parse((buffer.database.prepare("select payload_json from buffered_events where id=?")
  .get(lateEvent.id) as { payload_json: string }).payload_json).metadata as Record<string, unknown>;
assert.equal(rawMetadata().workItemId, undefined);
const lateBound = cli(["dispatch", "bind", "--session-id", "late-session", "--work-item-id", "beads:late-work",
  "--project-key", key, "--attempt-id", "late-lane", "--parent-attempt-id", "lead-session",
  "--role", "reviewer", "--valid-from", "2026-09-25T00:09:00.000Z"]);
assert.equal(lateBound.code, 0, lateBound.stderr);
assert.equal(rawMetadata().workItemId, undefined, "capture-time attribution stays red after a late bind");
proof.check("late_binding_does_not_rewrite_captured_usage_implicitly");
buffer.close();
const restamped = cli(["dispatch", "restamp", "--attempt-id", "late-lane"]);
assert.equal(restamped.code, 0, restamped.stderr || restamped.stdout);
assert.equal(JSON.parse(restamped.stdout).restamped, 1);
assert.equal(JSON.parse(restamped.stdout).skipped, 0);
const db = new Database(ledgerPath, { readonly: true });
try {
  const updated = JSON.parse((db.prepare("select payload_json from buffered_events where id=?")
    .get(lateEvent.id) as { payload_json: string }).payload_json);
  assert.equal(updated.metadata.workItemId, "beads:late-work");
  assert.equal(updated.metadata.role, "reviewer");
  assert.equal(updated.metadata.parentAttemptId, "lead-session");
  const outbox = db.prepare("select base_envelope_json from upload_outbox where raw_id=?")
    .get(lateEvent.id) as { base_envelope_json: string };
  assert.equal(JSON.parse(outbox.base_envelope_json).event.metadata.workItemId, "beads:late-work");
  const attempted = JSON.parse((db.prepare("select payload_json from buffered_events where id=?")
    .get(attemptedEvent.id) as {payload_json:string}).payload_json);
  assert.equal(attempted.metadata.workItemId, undefined);
} finally { db.close(); }
proof.check("explicit_restamp_updates_unsealed_rows_and_preserves_attempted_rows");

const validBindArgs = ["dispatch", "bind", "--session-id", "invalid-session", "--work-item-id", "beads:valid-work",
  "--project-key", key, "--attempt-id", "invalid-lane", "--valid-from", "2026-09-25T00:00:00.000Z"];
const beforeInvalid = fs.readFileSync(configPath);
for (const args of [
  validBindArgs.map(value => value === "beads:valid-work" ? "valid-work" : value),
  validBindArgs.map(value => value === key ? "project-key" : value),
  [...validBindArgs, "--role", "manager"],
  [...validBindArgs, "--technique-id", "partial"],
  [...validBindArgs, "--unknown", "value"],
  [...validBindArgs, "--valid-until", "2026-09-24T00:00:00.000Z"],
  validBindArgs.map(value => value === "invalid-session" ? "codex-session" : value),
]) assert.notEqual(cli(args).code, 0, JSON.stringify(args));
assert.deepEqual(fs.readFileSync(configPath), beforeInvalid);
proof.check("invalid_keys_flags_windows_and_overlap_leave_inventory_unchanged");

const forPrune = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
for (const captureRoot of forPrune.captureRoots ?? []) captureRoot.dispatch?.push({
  sessionId: "old-session",workItemId: "beads:old-work",projectKey: key,attemptId: "old-lane",
  parentAttemptId: null,companyRef: null,acceptedOutcomeId: null,evidenceRef: "old-evidence",
  validFrom: "2026-09-01T00:00:00.000Z",validUntil: "2026-09-02T00:00:00.000Z",role: "author",
});
fs.writeFileSync(configPath, `${JSON.stringify(forPrune, null, 2)}\n`);
const closed = cli(["dispatch", "close", "--attempt-id", "lane-1"]);
assert.equal(closed.code, 0, closed.stderr);
assert.equal(JSON.parse(closed.stdout).closed, 2);
assert.equal(JSON.parse(closed.stdout).pruned, 2);
const afterClose = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
assert.ok(afterClose.captureRoots?.every(captureRoot => captureRoot.dispatch?.every(binding => binding.attemptId !== "old-lane")));
assert.ok(afterClose.captureRoots?.every(captureRoot => captureRoot.dispatch?.find(binding => binding.attemptId === "lane-1")?.validUntil));
assert.equal(rootEventMetadata(roots[0], "after-close", "2026-10-01T00:00:00.000Z", "codex-session").workItemId, undefined);
proof.check("close_ends_attempt_and_prunes_closed_bindings_older_than_seven_days");

const scheduled = cli(["dispatch", "bind", "--session-id", "scheduled-session", "--work-item-id", "beads:scheduled",
  "--project-key", key, "--attempt-id", "scheduled-lane", "--valid-from", "2026-09-25T00:00:00.000Z",
  "--valid-until", "2026-10-01T00:00:00.000Z"]);
assert.equal(scheduled.code, 0, scheduled.stderr);
const scheduledClose = cli(["dispatch", "close", "--attempt-id", "scheduled-lane"]);
assert.equal(scheduledClose.code, 0, scheduledClose.stderr);
const end = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8"))).captureRoots![0].dispatch!
  .find(binding => binding.attemptId === "scheduled-lane")!.validUntil!;
assert.ok(Date.parse(end) < Date.parse("2026-10-01T00:00:00.000Z"), end);
proof.check("close_shortens_a_scheduled_validity_window");

const atCap = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
const rootAtCap = atCap.captureRoots![0];
const fillerCount = 1000 - (rootAtCap.dispatch?.length ?? 0);
for (let index = 0; index < fillerCount; index++) rootAtCap.dispatch!.push({
  sessionId: `cap-session-${index}`,workItemId: `beads:cap-${index}`,projectKey: key,
  attemptId: `cap-attempt-${index}`,parentAttemptId: null,companyRef: null,acceptedOutcomeId: null,
  evidenceRef: `cap-evidence-${index}`,validFrom: new Date(Date.parse("2026-09-20T00:00:00.000Z") + index * 1000).toISOString(),
  validUntil: null,role: "author",
});
fs.writeFileSync(configPath, `${JSON.stringify(atCap, null, 2)}\n`);
const capBound = cli(["dispatch", "bind", "--session-id", "cap-new-session", "--work-item-id", "beads:cap-new",
  "--project-key", key, "--attempt-id", "cap-new-lane", "--valid-from", "2026-09-25T23:00:00.000Z"]);
assert.equal(capBound.code, 0, capBound.stderr);
const kept = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8"))).captureRoots![0].dispatch!;
assert.equal(kept.length, 1000);
assert.ok(kept.some(binding => binding.attemptId === "cap-new-lane"));
assert.ok(!kept.some(binding => binding.attemptId === "cap-attempt-0"));
proof.check("capacity_prune_keeps_newest_bindings");
proof.complete();
}
main().catch(error => { console.error(error); process.exitCode = 1; });
