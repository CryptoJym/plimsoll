import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { LOCAL_TENANT_ID, type AiInteractionEvent } from "../packages/shared/src/index";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { appendRootObservation, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline, captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { performJoin, resumePendingJoin } from "../packages/collector-cli/src/join";
import { collectorBufferPath } from "../packages/collector-cli/src/config";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { createProfileCapture } from "../packages/collector-cli/src/profile-capture";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";

const root = fs.mkdtempSync(path.join(fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir()), "future-admission-proof-"));
const A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const E1 = "10000000-0000-4000-8000-000000000001", E2 = "10000000-0000-4000-8000-000000000002";
const T0 = "2030-04-02T10:00:00.000Z", T1 = "2030-04-02T10:01:00.000Z", T2 = "2030-04-02T10:02:00.000Z";
const OLD = "2020-01-01T00:00:00.000Z";
const checks: Array<{ name: string; passed: boolean; error?: string }> = [];
async function check(name: string, run: () => unknown) {
  try { await run(); checks.push({ name, passed: true }); }
  catch (error) { checks.push({ name, passed: false, error: error instanceof Error ? error.message : String(error) }); }
}
function event(id: string, observedAt: string, source: "codex" | "claude_code" = "codex"): AiInteractionEvent {
  return { id, tenantId: "local", source, eventType: "usage_rollout", dataMode: "metadata", observedAt, inputTokens: 3,
    intent: "unknown", actionClass: "other", metadata: {} };
}
const rows = (b: LocalEventBuffer) => b.database.prepare("select id,workspace_id,installation_epoch_id,payload_json from buffered_events order by id").all();

async function bufferProof() {
  let clock = T0;
  const filename = path.join(root, "ledger.sqlite");
  let b = new LocalEventBuffer(filename, { workspaceId: LOCAL_TENANT_ID, enrollmentNow: () => new Date(clock) } as any);
  b.append(event("private", OLD));
  const privateBefore = rows(b);
  for (const source of ["codex", "claude_code"] as const) {
    const baseline = beginAutomaticCaptureBaseline(b.database, source, { startedAt: OLD, filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(b.database, source, { runId: baseline.latestRun!.runId, completedAt: OLD });
  }
  (b.transitionWorkspace as Function)(LOCAL_TENANT_ID, A, undefined, E1);
  await check("first_enrollment_persists_actual_epoch_and_cutoff", () => {
    assert.equal(b.workspaceBinding()!.currentInstallationEpochId, E1);
    assert.equal(b.workspaceBinding()!.currentInstallationEpochStartedAt, T0);
  });
  await check("new_enrollment_reopens_file_baseline_at_cutoff", () => {
    for (const status of captureBaselineStatus(b.database).sources) {
      assert.equal(status.status, "in_progress"); assert.equal(status.latestRun!.startedAt, T0);
    }
  });
  await check("explicitly_disabled_providers_reestablish_empty_baseline", async () => {
    const capture = createProfileCapture(b, { captureRoots: [] });
    try {
      // The metadata fence requires two stable discovery sweeps, even empty.
      for (let sweep = 0; sweep < 3; sweep++) {
        for (const tailer of [capture.rollout, capture.transcript])
          await tailer.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
      }
      assert.equal(captureBaselineStatus(b.database).status, "complete");
    } finally { capture.close(); }
  });
  for (const source of ["codex", "claude_code"] as const) {
    await check(`${source}_backdated_direct_append_rejected`, () => assert.equal(b.append(event(source + "-old", OLD, source)), false));
    await check(`${source}_boundary_and_future_append_unknown_account_allowed`, () => {
      assert.equal(b.append(event(source + "-boundary", T0, source)), true);
      assert.equal(b.append(event(source + "-future", T1, source)), true);
    });
  }
  await check("missing_invalid_and_timezone_free_timestamps_rejected", () => {
    for (const [index, at] of [undefined, "nonsense", "2030-02-30T12:00:00Z", "2030-04-02T10:00:00"].entries())
      assert.equal(b.append(event("invalid-" + index, at as string)), false);
  });
  await check("stale_profile_epoch_is_rejected_without_conflict_receipt", () => {
    const r: CaptureRoot = { rootId: "fixture", profileId: "fixture", directory: "/fixture/sessions", source: "codex", installationEpochId: E2 };
    assert.equal(appendRootObservation(b, { ...event("wrong-epoch", T1), metadata: { installationEpochId: E2 } }, r), false);
  });
  const firstBinding = b.workspaceBinding();
  b.close(); clock = T2;
  b = new LocalEventBuffer(filename, { workspaceId: A, enrollmentNow: () => new Date(clock) } as any);
  await check("restart_same_epoch_keeps_original_cutoff", () => assert.deepEqual(b.workspaceBinding(), firstBinding));
  (b.transitionWorkspace as Function)(A, A, undefined, E2);
  await check("same_workspace_rejoin_rotates_epoch_and_cutoff", () => {
    assert.equal(b.workspaceBinding()!.currentInstallationEpochId, E2);
    assert.equal(b.workspaceBinding()!.currentInstallationEpochStartedAt, T2);
    assert.equal(b.append(event("late-first-epoch", T1)), false);
    assert.equal(b.append(event("second-epoch-future", T2)), true);
  });
  await check("join_resume_with_same_activation_id_preserves_cutoff", () => {
    clock = "2030-04-02T10:05:00.000Z";
    (b.transitionWorkspace as Function)(A, A, undefined, E2);
    assert.equal(b.workspaceBinding()!.currentInstallationEpochStartedAt, T2);
  });
  b.transitionWorkspace(A, B);
  await check("cross_workspace_rejoin_rejects_prior_epoch_time", () => assert.equal(b.append(event("late-second-epoch", T2)), false));
  await check("regressed_enrollment_clock_cannot_backdate_authority", () => {
    clock = OLD;
    assert.throws(() => b.transitionWorkspace(B, A), /clock_regressed/);
    assert.equal(b.workspaceBinding()!.currentWorkspaceId, B);
  });
  await check("private_and_previous_epoch_rows_never_relabel", () => {
    assert.deepEqual(rows(b).filter((r: any) => r.id === "private"), privateBefore);
    assert.equal((rows(b).find((r: any) => r.id === "codex-future") as any).workspace_id, A);
  });
  b.close();
}

async function providerProof(source: "codex" | "claude_code") {
  const base = path.join(root, source); fs.mkdirSync(base);
  const filename = path.join(root, source + ".sqlite");
  const b = new LocalEventBuffer(filename, { workspaceId: A, enrollmentNow: () => new Date(T0) } as any);
  const capture: CaptureRoot = { rootId: source, profileId: source, source, directory: base,
    installationEpochId: b.workspaceBinding()!.currentInstallationEpochId! };
  const makeTailer = () => source === "codex" ? new RolloutTailer(b, undefined, () => [], undefined, [capture])
    : new TranscriptTailer(b, undefined, undefined, [capture]);
  let tailer = makeTailer();
  function write(session: string, records: Array<{ at?: string; tokens: number }>) {
    const file = source === "codex" ? path.join(base, "2030", "04", "02", `rollout-${session}.jsonl`)
      : path.join(base, "fixture-project", `${session}.jsonl`);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const lines = source === "codex" ? [
      { type: "session_meta", timestamp: OLD, payload: { id: session } },
      { type: "turn_context", payload: { model: "gpt-5.5" } },
      ...[{ at: OLD, tokens: 0 }, ...records].map(({at, tokens}) => ({ type: "event_msg", timestamp: at,
        payload: { type: "token_count", info: { total_token_usage: { input_tokens: tokens, cached_input_tokens: 0, output_tokens: 0, reasoning_output_tokens: 0 } } } })),
    ] : records.map(({at, tokens}) => ({ type: "assistant", timestamp: at,
      message: { id: "msg-" + session, model: "claude-opus-5", usage: { input_tokens: tokens, output_tokens: 0 } } }));
    fs.writeFileSync(file, lines.map(r => JSON.stringify(r)).join("\n") + "\n");
  }
  try {
    write("00000000-0000-4000-8000-000000000001", [{ at: OLD, tokens: 100 }, { at: T1, tokens: 107 }]);
    write("00000000-0000-4000-8000-000000000002", [{ tokens: 100 }, { at: T1, tokens: 109 }]);
    write("00000000-0000-4000-8000-000000000003", [{ at: "invalid", tokens: 100 }, { at: T1, tokens: 113 }]);
    write("00000000-0000-4000-8000-000000000004", [{ at: OLD, tokens: 80 }]);
    const result = await tailer.scan({ scope: "full" });
    await check(`${source}_restored_old_undated_invalid_events_excluded`, () => {
      assert.equal((b.database.prepare("select count(*) n from buffered_events where observed_at <> ?").get(T1) as {n:number}).n, 0);
      assert.equal(result.parseErrors, 0);
    });
    await check(`${source}_future_marginals_exclude_prior_counters`, () => {
      const total = b.database.prepare("select count(*) n, sum(input_tokens) tokens from buffered_events").get() as any;
      assert.equal(total.n, 3); assert.equal(total.tokens, 29);
    });
    tailer.close(); tailer = makeTailer();
    await tailer.scan({ scope: "full" });
    await check(`${source}_restart_does_not_duplicate_or_relabel`, () => assert.equal((b.database.prepare("select count(*) n from buffered_events").get() as any).n, 3));
  } finally { tailer.close(); b.close(); }
}

async function joinProof() {
  const home = path.join(root, "join-home"); fs.mkdirSync(home);
  const priorHome = process.env.PLIMSOLL_HOME;
  process.env.PLIMSOLL_HOME = path.join(home, "plimsoll");
  fs.mkdirSync(process.env.PLIMSOLL_HOME, { mode: 0o700 });
  try {
  const privateBuffer = new LocalEventBuffer(collectorBufferPath(home), { workspaceId: LOCAL_TENANT_ID });
  privateBuffer.append(event("native-private", OLD)); const before = rows(privateBuffer); privateBuffer.close();
  const acceptedFixtureIds = new Set<string>();
  const fakeFetch = (async (input: any, init: RequestInit) => {
    let body: Record<string, unknown>;
    if (String(input).endsWith("/join")) body = { ok: true, tenantId: A, installKey: "fixture-install", uploadUrl: "https://fixture.invalid/api/work-intelligence/ingest" };
    else {
      const expected = deliveryExpectation(String(init.body), "fixture-install");
      for (const id of expected.itemIds) acceptedFixtureIds.add(id);
      body = { ok: true, accepted: expected.itemIds.length,
        ack: deliveryAcknowledgement(expected, expected.itemIds.filter(id => acceptedFixtureIds.has(id))) };
    }
    return new Response(JSON.stringify(body), { status: String(input).endsWith("/join") ? 201 : 200, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
  const args = { target: "pljt_" + "a".repeat(40), baseUrl: "https://fixture.invalid", homeDir: home, appVersion: "0.7.0", fetchImpl: fakeFetch, temporaryRoot: path.join(root, "join-temp") };
  function snapshot() { const b = new LocalEventBuffer(collectorBufferPath(home)); try { return { binding: b.workspaceBinding(), rows: rows(b) }; } finally { b.close(); } }
  const one = await performJoin(args); const first = snapshot();
  await check("native_join_rotates_private_epoch_without_relabel", () => {
    assert.equal(one.joined, true); assert.deepEqual(first.rows, before);
    if (one.joined) {
      assert.equal(one.enrollment.installationEpochId, first.binding!.currentInstallationEpochId);
      assert.equal(one.enrollment.admissionCutoffAt, first.binding!.currentInstallationEpochStartedAt);
    }
  });
  let threw = false;
  try { await performJoin({ ...args, afterConfigActivation() { throw new Error("fixture-after-activation"); } }); } catch { threw = true; }
  const second = snapshot();
  await check("native_same_workspace_new_join_creates_new_epoch", () => { assert.equal(threw, true); assert.notEqual(first.binding!.currentInstallationEpochId, second.binding!.currentInstallationEpochId); });
  await check("native_pending_resume_keeps_same_epoch_cutoff_and_private_rows", async () => {
    await resumePendingJoin({ homeDir: home, fetchImpl: fakeFetch, temporaryRoot: path.join(root, "join-temp") });
    assert.deepEqual(snapshot(), second);
  });
  } finally {
    if (priorHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = priorHome;
  }
}

async function connectionAndMetricProof() {
  const filename = path.join(root, "connections.sqlite");
  const old = new LocalEventBuffer(filename, { workspaceId: A, deviceId: "fixture-device",
    enrollmentNow: () => new Date(T0), delivery: { enabled: true } } as any);
  const current = new LocalEventBuffer(filename, { workspaceId: A, deviceId: "fixture-device",
    enrollmentNow: () => new Date(T2), delivery: { enabled: true } } as any);
  const metric = (id: string, at: string) => ({ id, source: "codex" as const,
    metricName: "codex.token.usage", observedAt: at, sampleType: "input", value: 5, attrs: {}, suppressedFields: [] });
  try {
    current.transitionWorkspace(A, B);
    await check("stale_connection_cannot_split_raw_and_outbox_audiences", () => {
      assert.equal(old.append(event("stale-audience", T2)), false);
      assert.equal((current.database.prepare("select count(*) n from buffered_events where id='stale-audience'").get() as any).n, 0);
      assert.equal((current.database.prepare("select count(*) n from upload_outbox where raw_id='stale-audience'").get() as any).n, 0);
    });
    await check("stale_connection_metric_append_is_rejected", () => {
      old.appendMany([], [metric("stale-metric", T2)]);
      assert.equal((current.database.prepare("select count(*) n from metric_samples where id='stale-metric'").get() as any).n, 0);
    });
    await check("managed_backdated_metrics_are_rejected", () => {
      current.appendMany([], [metric("old-metric", OLD)]);
      assert.equal((current.database.prepare("select count(*) n from metric_samples where id='old-metric'").get() as any).n, 0);
    });
    await check("managed_undated_metrics_rejected_and_future_metric_stays_local", () => {
      current.appendMany([], [metric("undated-metric", undefined as any), metric("future-metric", T2)]);
      assert.deepEqual(current.database.prepare("select id from metric_samples where id in ('old-metric','undated-metric','future-metric') order by id").all(), [{id: "future-metric"}]);
      assert.equal((current.database.prepare("select count(*) n from upload_outbox").get() as any).n, 0);
    });
    old.useWorkspace(B, "fixture-device");
    await check("explicit_refresh_recovers_consistent_raw_and_outbox_audience", () => {
      assert.equal(old.append(event("fresh-audience", T2)), true);
      const row = current.database.prepare(`select e.workspace_id as raw, o.workspace_id as outbox,
        e.device_id as rawDevice, o.device_id as outboxDevice from buffered_events e
        join upload_outbox o on o.raw_id=e.id where e.id='fresh-audience'`).get() as any;
      assert.deepEqual(row, {raw: B, outbox: B, rawDevice: "fixture-device", outboxDevice: "fixture-device"});
    });
    (current.useWorkspace as Function)(B, "fixture-device", E2);
    await check("same_workspace_epoch_rotation_invalidates_old_connection", () => assert.equal(old.append(event("stale-same-workspace", T2)), false));
    current.database.prepare("update collector_workspace_binding set current_device_id='rotated-fixture-device' where singleton=1").run();
    await check("device_rotation_invalidates_old_connection", () => assert.equal(current.append(event("stale-device", T2)), false));
  } finally { old.close(); current.close(); }
}

async function main() {
  try {
    await bufferProof();
    for (const source of ["codex", "claude_code"] as const) await check(`${source}_provider_fixture_completes`, () => providerProof(source));
    await check("native_join_fixture_completes", joinProof);
    await check("connection_and_metric_fixture_completes", connectionAndMetricProof);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
  const receipt = { schema: "plimsoll.future-admission-proof.v1", fixtureOnly: true, checks,
    passed: checks.filter(c => c.passed).length, failed: checks.filter(c => !c.passed).length };
  console.log(JSON.stringify(receipt, null, 2));
  process.exitCode = receipt.failed ? 1 : 0;
}
main().catch(error => { console.error(error); process.exitCode = 1; });
