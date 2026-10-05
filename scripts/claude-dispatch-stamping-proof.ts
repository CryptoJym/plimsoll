/** Bound Claude capture through the production hook, OTLP and transcript paths. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { appendRootObservation, currentDispatchCaptureRoots, dispatchBindingMetadata,
  rootEventMetadata, type DispatchBinding } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema, rollbackCollectorDispatchHistory } from "../packages/collector-cli/src/config";
import { bindDispatch, restampDispatch } from "../packages/collector-cli/src/dispatch-command";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { createProofCompletion } from "./lib/proof-completion";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const proof = createProofCompletion("claude-dispatch-stamping");
const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
const projectKey = `sha256:${"a".repeat(64)}`;
const workItemId = "beads:eco-6hoxj.165.97";
const attemptId = "11111111-1111-4111-8111-111111111111";
const sessionId = "22222222-2222-4222-8222-222222222222";
const atMs = Date.now() - 300_000;
const at = (offset: number) => new Date(atMs + offset * 1000).toISOString();
const nano = (offset: number) => String(BigInt(atMs + offset * 1000) * 1_000_000n);
const attr = (key: string, value: string | number) => ({
  key, value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value },
});

function raw(buffer: LocalEventBuffer, id: string) {
  const row = buffer.database.prepare(`select payload_json as payload, privacy_disposition as privacy
    from buffered_events where id=?`).get(id) as { payload: string; privacy: string | null } | undefined;
  assert.ok(row, `missing raw ${id}`);
  return { event: JSON.parse(row.payload) as { metadata: Record<string, unknown> }, privacy: row.privacy };
}
function envelope(buffer: LocalEventBuffer, id: string) {
  const row = buffer.database.prepare(`select base_envelope_json as payload from upload_outbox where raw_id=?`)
    .get(id) as { payload: string } | undefined;
  assert.ok(row, `missing outbox ${id}`);
  return JSON.parse(row.payload) as { event: { metadata: Record<string, unknown> } };
}
function stamped(buffer: LocalEventBuffer, id: string, binding: DispatchBinding) {
  const stored = raw(buffer, id);
  assert.equal(stored.privacy, null);
  for (const [key, value] of Object.entries(dispatchBindingMetadata(binding))) {
    assert.deepEqual(stored.event.metadata[key], value, `${id} local ${key}`);
    assert.deepEqual(envelope(buffer, id).event.metadata[key], value, `${id} wire ${key}`);
  }
  assert.deepEqual(envelope(buffer, id).event.metadata.work_ref, {
    schema: "work-ref/v1", work_id: "eco-6hoxj.165.97", run_id: binding.attemptId,
  });
}
function unstamped(buffer: LocalEventBuffer, id: string) {
  for (const field of ["workItemId", "attemptId", "workEvidenceRef", "dispatchProjectKey", "work_ref"]) {
    assert.equal(raw(buffer, id).event.metadata[field], undefined, `${id} local ${field}`);
    assert.equal(envelope(buffer, id).event.metadata[field], undefined, `${id} wire ${field}`);
  }
}
function ids(buffer: LocalEventBuffer, source: string, session: string, type?: string) {
  const rows = buffer.database.prepare(`select id from buffered_events where source=? and session_id=?
    and (? is null or event_type=?) order by rowid`).all(source, session, type ?? null, type ?? null) as Array<{ id: string }>;
  return rows.map(row => row.id);
}
function logPayload(session: string, offset: number, withUsage = true, service = "claude-code") {
  return { resourceLogs: [{ resource: { attributes: [attr("service.name", service)] },
    scopeLogs: [{ logRecords: [{ timeUnixNano: nano(offset), attributes: [
      attr("event.name", "claude_code.api_request"), attr("session.id", session),
      ...(withUsage ? [attr("input_token_count", 12), attr("output_token_count", 4)] : []),
      attr("prompt", "SENSITIVE_CLAUDE_FIXTURE_PAYLOAD"),
    ] }] }] }] };
}
function spanPayload(session: string, offset: number, service = "claude-code") {
  return { resourceSpans: [{ resource: { attributes: [attr("service.name", service)] },
    scopeSpans: [{ spans: [{ name: "claude_code.api_request", startTimeUnixNano: nano(offset),
      endTimeUnixNano: nano(offset + 1), attributes: [attr("session.id", session),
        attr("gen_ai.usage.input_tokens", 7), attr("gen_ai.usage.output_tokens", 2),
        attr("prompt", "SENSITIVE_CLAUDE_FIXTURE_PAYLOAD")],
    }] }] }] };
}
function transcriptLine(session: string, offset: number, index: number) {
  return JSON.stringify({ type: "assistant", sessionId: session, cwd: home, timestamp: at(offset),
    message: { id: `fixture-message-${session}-${index}`, model: "claude-sonnet-4-20250514", content: [],
      usage: { input_tokens: 10 * index, output_tokens: index,
        cache_read_input_tokens: 0, cache_creation_input_tokens: 0 } } });
}

async function main() {
  const claudeA = path.join(home, ".claude", "projects");
  const claudeB = path.join(home, ".claude-seats", "seat-1", "projects");
  const claudeC = path.join(home, ".claude-seats", "seat-2", "projects");
  const codex = path.join(home, ".codex", "sessions");
  for (const directory of [claudeA, claudeB, claudeC, codex, plimsoll]) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const base = collectorConfigSchema.parse({ deviceId: "dev_claude-dispatch-fixture",
    port: 49873, uploadUrl: "http://127.0.0.1:1/unused" });
  const buffer = new LocalEventBuffer(path.join(plimsoll, "work-ledger.sqlite"), {
    workspaceId: base.tenantId, deviceId: base.deviceId,
    enrollmentNow: () => new Date(atMs - 3_600_000), delivery: { enabled: true },
  });
  const installationEpochId = buffer.workspaceBinding()?.currentInstallationEpochId;
  assert.ok(installationEpochId);
  const roots = [
    { rootId: "claude-a", profileId: "claude-a", installationEpochId, source: "claude_code" as const, directory: claudeA },
    { rootId: "claude-b", profileId: "claude-b", installationEpochId, source: "claude_code" as const, directory: claudeB },
    { rootId: "claude-c", profileId: "claude-c", installationEpochId, source: "claude_code" as const, directory: claudeC },
    { rootId: "codex", profileId: "codex", installationEpochId, source: "codex" as const, directory: codex },
  ];
  const configPath = path.join(plimsoll, "collector.config.json");
  fs.writeFileSync(configPath, JSON.stringify(collectorConfigSchema.parse({ ...base, captureRoots: roots })) + "\n", { mode: 0o600 });
  const bind = (session: string, attempt: string, work = workItemId) => {
    const result = bindDispatch([
    "--session-id", session, "--work-item-id", work, "--project-key", projectKey,
    "--attempt-id", attempt, "--valid-from", at(-10), "--valid-until", at(10),
    ]);
    // The later fanout/conflict cases intentionally edit legacy hot arrays.
    // Materialize through the production lossless downgrade helper first;
    // never delete or rewrite immutable historical membership to seed them.
    rollbackCollectorDispatchHistory();
    return result;
  };
  assert.equal(bind(sessionId, attemptId).roots, 4);
  const rooted = (session: string, eventType: "session_start" | "tool_use", rootIndex = 0) => {
    const id = crypto.randomUUID();
    const observedAt=at(0);
    const event=aiInteractionEventSchema.parse({ id,sessionId:session,source:"claude_code",
      dataMode:"metadata",eventType,observedAt,
      metadata:rootEventMetadata(roots[rootIndex],id,observedAt,session) });
    assert.equal(appendRootObservation(buffer,event,roots[rootIndex]),true);
    return id;
  };
  const binding = currentDispatchCaptureRoots()[0].dispatch?.find(row => row.sessionId === sessionId);
  assert.ok(binding);
  proof.check("bind_records_the_synthetic_session_in_every_capture_root");

  // The transcript tailer and live OTLP writer are mutually exclusive usage
  // authorities for one session, so prove both with separately bound sessions.
  const transcriptSession = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  bind(transcriptSession, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");
  const transcriptBinding = currentDispatchCaptureRoots()[0].dispatch?.find(row => row.sessionId === transcriptSession);
  assert.ok(transcriptBinding);
  const project = path.join(claudeA, "-synthetic-project");
  fs.mkdirSync(project, { recursive: true });
  fs.writeFileSync(path.join(project, `${transcriptSession}.jsonl`), [-15, 0, 15]
    .map((offset, index) => transcriptLine(transcriptSession, offset, index + 1)).join("\n") + "\n");
  const tailer = new TranscriptTailer(buffer, claudeA, undefined, [roots[0]]);
  try { await tailer.scan({ scope: "full" }); } finally { tailer.close(); }
  const transcriptIds = ids(buffer, "claude_code", transcriptSession, "usage_transcript");
  assert.equal(transcriptIds.length, 3);
  unstamped(buffer, transcriptIds[0]);
  stamped(buffer, transcriptIds[1], transcriptBinding);
  unstamped(buffer, transcriptIds[2]);
  proof.check("transcript_usage_obeys_the_binding_window_and_queues_work_ref");
  // Hook/OTLP have no root credential. A captured, known-root event makes A
  // the sole observed Claude root for this session before their intake.
  rooted(sessionId,"session_start");

  const auth = loadOrCreateLocalIngestAuth(plimsoll);
  const server = createCollectorServer(base, buffer, { localAuth: auth });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(49873, "127.0.0.1", resolve);
    });
    const post = async (route: string, payload: unknown, source = "claude_code") => {
      const response = await fetch(`http://127.0.0.1:49873${route}`, { method: "POST",
        headers: { "content-type": "application/json", "x-plimsoll-source": source,
          "x-plimsoll-token": source === "codex" ? auth.codexProducer : auth.claudeCodeProducer },
        body: JSON.stringify(payload) });
      const body = await response.text();
      assert.equal(response.status, 202, `${route}: ${response.status} ${body}`);
    };
    const hook = async (session: string, kind: string, offset: number) => {
      const id = crypto.randomUUID();
      await post("/hooks/claude-code", { id, hook_event_name: kind, session_id: session,
        timestamp: at(offset), prompt: "SENSITIVE_CLAUDE_FIXTURE_PAYLOAD" });
      return id;
    };
    // Identical copies in three Claude roots describe one fanout binding.
    const kinds = ["UserPromptSubmit", "PreToolUse", "PostToolUse", "AssistantResponse",
      "Stop", "SessionStart", "Notification"];
    const fanoutHookIds = [];
    for (const kind of kinds) fanoutHookIds.push(await hook(sessionId, kind, 0));
    for (const id of fanoutHookIds) stamped(buffer, id, binding);
    const priorFanoutOtlp = ids(buffer, "claude_code", sessionId).length;
    await post("/v1/logs", logPayload(sessionId, 0));
    await post("/v1/traces", spanPayload(sessionId, 0));
    const fanoutOtlpIds = ids(buffer, "claude_code", sessionId).slice(priorFanoutOtlp);
    assert.equal(fanoutOtlpIds.length, 2);
    for (const id of fanoutOtlpIds) stamped(buffer, id, binding);
    proof.check("identical_three_root_fanout_stamps_all_hook_kinds_and_otlp");
    // Model a later root B that was not present when A's dispatch was bound.
    // Only A retains this session's binding for the ordinary hook/OTLP cases.
    const uniquelyBound = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
    for (const root of uniquelyBound.captureRoots!.filter(root => root.source === "claude_code" && root.rootId !== "claude-a"))
      root.dispatch = root.dispatch?.filter(row => row.sessionId !== sessionId);
    fs.writeFileSync(configPath, JSON.stringify(uniquelyBound) + "\n");
    const hookIds = [];
    for (const kind of kinds) hookIds.push(await hook(sessionId, kind, 0));
    for (const id of hookIds) stamped(buffer, id, binding);
    proof.check("all_claude_hook_kinds_queue_bound_work_refs");
    const unboundId = await hook("unbound-claude-session", "UserPromptSubmit", 0);
    const beforeId = await hook(sessionId, "UserPromptSubmit", -15);
    const afterId = await hook(sessionId, "UserPromptSubmit", 15);
    for (const id of [unboundId, beforeId, afterId]) unstamped(buffer, id);
    proof.check("unbound_and_outside_window_hooks_remain_unstamped");
    const boundKeys = Object.keys(raw(buffer, hookIds[0]).event.metadata).sort();
    const unboundKeys = Object.keys(raw(buffer, unboundId).event.metadata).sort();
    assert.deepEqual(boundKeys.filter(key => !Object.keys(dispatchBindingMetadata(binding)).includes(key)), unboundKeys);
    for (const id of [hookIds[0], unboundId]) {
      assert.equal(JSON.stringify(raw(buffer, id)).includes("SENSITIVE_CLAUDE_FIXTURE_PAYLOAD"), false);
      assert.equal(JSON.stringify(envelope(buffer, id)).includes("SENSITIVE_CLAUDE_FIXTURE_PAYLOAD"), false);
    }
    proof.check("hook_privacy_and_metadata_allowlist_are_unchanged");

    const beforeLog = ids(buffer, "claude_code", sessionId).length;
    await post("/v1/logs", logPayload(sessionId, 1));
    await post("/v1/traces", spanPayload(sessionId, 1));
    const newRows = ids(buffer, "claude_code", sessionId).slice(beforeLog);
    assert.equal(newRows.length, 2);
    for (const id of newRows) stamped(buffer, id, binding);
    assert.ok(newRows.some(id => (raw(buffer, id).event as { inputTokens?: number }).inputTokens === 12));
    proof.check("claude_otlp_logs_spans_and_log_usage_queue_bound_work_refs");
    const unboundOtlpIds: string[] = [];
    for (const [session, offset] of [["unbound-claude-session", 0], [sessionId, -15], [sessionId, 15]] as const) {
      const previous = ids(buffer, "claude_code", session).length;
      await post("/v1/logs", logPayload(session, offset));
      await post("/v1/traces", spanPayload(session, offset));
      const added = ids(buffer, "claude_code", session).slice(previous);
      assert.equal(added.length, 2);
      for (const id of added) unstamped(buffer, id);
      if (session === "unbound-claude-session") unboundOtlpIds.push(...added);
    }
    proof.check("unbound_and_outside_window_otlp_remain_unstamped");
    for (let index = 0; index < 2; index += 1) {
      const metadataKeys: string[] = Object.keys(raw(buffer, newRows[index]).event.metadata)
        .filter(key => !Object.keys(dispatchBindingMetadata(binding)).includes(key)).sort();
      assert.deepEqual(metadataKeys, Object.keys(raw(buffer, unboundOtlpIds[index]).event.metadata).sort());
    }
    for (const id of [...newRows, ...unboundOtlpIds]) {
      assert.equal(JSON.stringify(raw(buffer, id)).includes("SENSITIVE_CLAUDE_FIXTURE_PAYLOAD"), false);
      assert.equal(JSON.stringify(envelope(buffer, id)).includes("SENSITIVE_CLAUDE_FIXTURE_PAYLOAD"), false);
    }
    proof.check("otlp_privacy_and_metadata_allowlist_are_unchanged");

    // B has no copy of this binding and independently records the same session.
    // Anonymous transports must now refuse A's work and report the veto.
    rooted(sessionId, "session_start", 1);
    const otherRootHook = await hook(sessionId, "AssistantResponse", 0);
    unstamped(buffer, otherRootHook);
    const priorOtherRootOtlp = ids(buffer, "claude_code", sessionId).length;
    await post("/v1/logs", logPayload(sessionId, 2));
    await post("/v1/traces", spanPayload(sessionId, 2));
    const otherRootOtlpIds = ids(buffer, "claude_code", sessionId).slice(priorOtherRootOtlp);
    assert.equal(otherRootOtlpIds.length, 2);
    for (const id of otherRootOtlpIds) unstamped(buffer, id);
    const statusResponse = await fetch("http://127.0.0.1:49873/status", {
      headers: { "x-plimsoll-token": auth.managementRead },
    });
    assert.equal(statusResponse.status, 200);
    const status = await statusResponse.json() as { claudeDispatchSkips?: { otherRootSeen?: number } };
    assert.ok((status.claudeDispatchSkips?.otherRootSeen ?? 0) >= 3);
    const statusCli = await new Promise<string>((resolve, reject) => {
      const child = spawn(process.execPath, ["--import", path.join(process.cwd(),
        "node_modules/tsx/dist/loader.mjs"), "packages/collector-cli/src/cli.ts", "status"], {
        cwd: process.cwd(), env: process.env,
      });
      let stdout = "", stderr = "";
      child.stdout.setEncoding("utf8").on("data", data => { stdout += data; });
      child.stderr.setEncoding("utf8").on("data", data => { stderr += data; });
      child.once("error", reject);
      child.once("close", code => code === 0 ? resolve(stdout) : reject(new Error(
        `synthetic plimsoll status exited ${code}: ${stderr}`)));
    });
    const cliStatus = JSON.parse(statusCli) as { claudeDispatchSkips?: { otherRootSeen?: number } };
    assert.ok((cliStatus.claudeDispatchSkips?.otherRootSeen ?? 0) >= 3);
    proof.check("other_root_sighting_vetoes_anonymous_intake_and_status_counts_it");

    const codexSession = "33333333-3333-4333-8333-333333333333";
    bind(codexSession, "44444444-4444-4444-8444-444444444444");
    const codexOtlp = explodeOtlpPayload({ ...logPayload(codexSession, 0, true, "codex_exec"),
      ...spanPayload(codexSession, 0, "codex_exec") }, { source: "codex" });
    assert.equal(codexOtlp.events.length, 2);
    assert.ok(codexOtlp.events.every(row => row.event.source === "codex"));
    assert.equal(codexOtlp.events[0].event.metadata.workItemId, workItemId);
    assert.equal(codexOtlp.events[1].event.metadata.workItemId, undefined);
    proof.check("codex_log_stamping_and_span_behavior_are_unchanged");

    const conflictSession = "55555555-5555-4555-8555-555555555555";
    const conflictTranscriptSession = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
    bind(conflictSession, "66666666-6666-4666-8666-666666666666");
    bind(conflictTranscriptSession, "dddddddd-dddd-4ddd-8ddd-dddddddddddd");
    const conflicting = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
    for (const session of [conflictSession, conflictTranscriptSession]) {
      const other = conflicting.captureRoots?.[1].dispatch?.find(row => row.sessionId === session);
      assert.ok(other);
      other.workItemId = "beads:eco-6hoxj.165.98";
      other.attemptId = "77777777-7777-4777-8777-777777777777";
    }
    fs.writeFileSync(configPath, JSON.stringify(conflicting) + "\n");
    const conflictHook = await hook(conflictSession, "UserPromptSubmit", 0);
    unstamped(buffer, conflictHook);
    const priorConflict = ids(buffer, "claude_code", conflictSession).length;
    await post("/v1/logs", logPayload(conflictSession, 0));
    await post("/v1/traces", spanPayload(conflictSession, 0));
    for (const id of ids(buffer, "claude_code", conflictSession).slice(priorConflict)) unstamped(buffer, id);
    const conflictProject = path.join(claudeA, "-conflict-project");
    fs.mkdirSync(conflictProject, { recursive: true });
    fs.writeFileSync(path.join(conflictProject, `${conflictTranscriptSession}.jsonl`),
      transcriptLine(conflictTranscriptSession, 0, 1) + "\n");
    const conflictTailer = new TranscriptTailer(buffer, claudeA, undefined, [roots[0]]);
    try { await conflictTailer.scan({ scope: "full" }); } finally { conflictTailer.close(); }
    const conflictUsage = ids(buffer, "claude_code", conflictTranscriptSession, "usage_transcript");
    assert.equal(conflictUsage.length, 1);
    unstamped(buffer, conflictUsage[0]);
    const conflictStatusResponse = await fetch("http://127.0.0.1:49873/status", {
      headers: { "x-plimsoll-token": auth.managementRead },
    });
    assert.equal(conflictStatusResponse.status, 200);
    const conflictStatus = await conflictStatusResponse.json() as {
      claudeDispatchSkips?: { conflictingBindings?: number }
    };
    assert.ok((conflictStatus.claudeDispatchSkips?.conflictingBindings ?? 0) >= 3);
    proof.check("cross_root_conflict_stamps_no_claude_path");
    const conflictedRestamp = restampDispatch(["--attempt-id", "66666666-6666-4666-8666-666666666666"],
      buffer, currentDispatchCaptureRoots());
    assert.equal(conflictedRestamp.restamped, 0);
    for (const id of [conflictHook, ...ids(buffer, "claude_code", conflictSession).slice(priorConflict)])
      unstamped(buffer, id);
    proof.check("restamp_does_not_override_a_cross_root_conflict");

    const lateSession = "88888888-8888-4888-8888-888888888888";
    const lateHook = await hook(lateSession, "AssistantResponse", 0);
    const priorLate = ids(buffer, "claude_code", lateSession).length;
    await post("/v1/logs", logPayload(lateSession, 0));
    const lateUsage = ids(buffer, "claude_code", lateSession).slice(priorLate);
    assert.equal(lateUsage.length, 1);
    unstamped(buffer, lateHook);
    unstamped(buffer, lateUsage[0]);
    const rootedLate=[rooted(lateSession,"session_start"),rooted(lateSession,"tool_use")];
    for(const id of rootedLate) unstamped(buffer,id);
    const lateAttempt = "99999999-9999-4999-8999-999999999999";
    bind(lateSession, lateAttempt);
    const result = restampDispatch(["--attempt-id", lateAttempt], buffer, currentDispatchCaptureRoots());
    assert.equal(result.restamped, 2);
    const lateBinding = currentDispatchCaptureRoots()[0].dispatch?.find(row => row.sessionId === lateSession);
    assert.ok(lateBinding);
    for(const id of rootedLate) stamped(buffer,id,lateBinding);
    unstamped(buffer,lateHook);
    unstamped(buffer,lateUsage[0]);
    proof.check("restamp_corrects_only_known_root_claude_rows_and_outbox");
  } finally {
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    buffer.close();
  }
  proof.complete();
}
main().catch(error => { console.error(error); process.exitCode = 1; });
