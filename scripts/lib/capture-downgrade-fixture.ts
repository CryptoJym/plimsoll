import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { DEFAULT_JSONL_TAILER_IO } from "../../packages/collector-cli/src/jsonl-byte-tailer";
import { remoteLinkageHash } from "../../packages/shared/src/index";
import { REPO_CONTEXT_CAPTURE_POLICY_GENERATION } from "../../packages/collector-cli/src/repo-context";
import { CODEX_MODEL_WAIT_MS } from "../../packages/collector-cli/src/codex-model-capture";
import { homeCodexCaptureBinding } from "./home-codex-capture-fixture";

const releases = [
  { version: "0.7.51", commit: "71d6ff27f0d39aa31d188c9bcc31d37bf188c384", reupgrade: true },
  { version: "0.7.50", commit: "121b55437555c3a3c34bafe5889f4d6d8870509f", reupgrade: false },
];
const hash = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
type Reader = {
  buffer: typeof import("../../packages/collector-cli/src/buffer");
  codex: typeof import("../../packages/collector-cli/src/rollout-tailer");
  claude: typeof import("../../packages/collector-cli/src/transcript-tailer");
  context: typeof import("../../packages/collector-cli/src/repo-context");
};
type UsageRow = { id: string; input: number; output: number; repo: string | null; payload: string };
type Cursor = { offset: number; state: string };

async function reader(directory: string): Promise<Reader> {
  const load = (file: string) => import(pathToFileURL(path.join(directory, "packages/collector-cli/src", file + ".ts")).href);
  return { buffer: await load("buffer"), codex: await load("rollout-tailer"),
    claude: await load("transcript-tailer"), context: await load("repo-context") };
}

/** The real released capture modules read the same ledger and growing files.
 * No mock validator, restarted counters, copied ledger or network is used. */
export async function runCaptureDowngradeFixtures() {
  const currentRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "capture-downgrade-"));
  const originalHome = os.homedir;
  const originalFetch = globalThis.fetch;
  let transportCalls = 0;
  globalThis.fetch = async () => { transportCalls++; throw new Error("downgrade fixture must not send a request"); };
  const trees: string[] = [];
  const observations: Array<Record<string, unknown>> = [];
  try {
    const current = await reader(currentRoot);
    for (const release of releases) {
      const tree = path.join(root, release.version);
      execFileSync("git", ["worktree", "add", "--quiet", "--detach", tree, release.commit]);
      trees.push(tree);
      fs.symlinkSync(path.join(currentRoot, "node_modules"), path.join(tree, "node_modules"), "dir");
      assert.equal(JSON.parse(fs.readFileSync(path.join(tree, "packages/collector-cli/package.json"), "utf8")).version, release.version);
      const previous = await reader(tree);
      for (const kind of ["claude", "codex"] as const) {
        const directory = path.join(root, `${release.version}-${kind}`);
        const home = path.join(directory, "home");
        const chat = path.join(home, "Documents/chat");
        const write = (file: string, content: string) => {
          fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
          fs.writeFileSync(file, content, { mode: 0o600 });
        };
        fs.mkdirSync(chat, { recursive: true, mode: 0o700 });
        const remote = "https://github.com/CryptoJym/plimsoll.git";
        write(path.join(home, ".git/HEAD"), "ref: refs/heads/main\n");
        write(path.join(home, ".git/refs/heads/main"), "a".repeat(40) + "\n");
        write(path.join(home, ".git/config"), `[remote "origin"]\n url = ${remote}\n`);
        os.homedir = () => home;
        const session = "019f8000-0000-7000-8000-000000000473";
        const turnId = "019f8000-0000-7000-8000-000000000474";
        let producerTurnId = turnId;
        const sessions = path.join(directory, "sessions");
        const file = kind === "claude" ? path.join(sessions, "project", session + ".jsonl")
          : path.join(sessions, "2026/10/05", `rollout-proof-${session}.jsonl`);
        const baseMs = Date.parse("2026-10-05T10:00:00.000Z");
        let now = baseMs;
        // buffer.ts:815,1092,2732,2750 supplies the real capture boundary.
        const options = { ...(kind === "codex" ? homeCodexCaptureBinding : {}),
          enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => new Date(now) } };
        const ledger = path.join(directory, "ledger.sqlite");
        const stages = ["released", "head", "downgraded", "downgraded", "downgraded",
          ...(release.reupgrade ? ["reupgraded", "reupgraded"] : [])];
        const frozenRows = new Map<string, string>();
        const frozenSeals = new Map<string, string>();
        const bareReleasedSeals = new Set<string>();
        const retiredRawIds = new Set<string>();
        let totalRetired = 0;
        const cadences: Array<Record<string, unknown>> = [];
        let priorOffset = 0;
        for (const [index, stage] of stages.entries()) {
          now = baseMs + index * (121_001 + (kind === "codex" ? CODEX_MODEL_WAIT_MS + 1 : 0));
          // Every previous 120-second lease, including a head's model hold, has expired.
          if (kind === "codex" && (stage === "head" || stage === "reupgraded")) {
            producerTurnId = `019f8000-0000-7000-8000-${String(474 + index).padStart(12, "0")}`;
          }
          const count = index + 1;
          const line = kind === "claude" ? { type: "assistant", sessionId: session,
            timestamp: new Date(now).toISOString(), ...(index === 0 ? { cwd: chat } : {}),
            message: { id: "growing-message", model: "claude-sonnet-4-20250514", usage: {
              input_tokens: count * 19, output_tokens: count * 2, cache_read_input_tokens: 0, cache_creation_input_tokens: 0,
            } } } : { type: "event_msg", timestamp: new Date(now).toISOString(), payload: {
              type: "token_count", turn_id: producerTurnId, info: { total_token_usage: { input_tokens: count * 19, output_tokens: count * 2,
                cached_input_tokens: 0, reasoning_output_tokens: 0, total_tokens: count * 21 } },
            } };
          const initial = kind === "codex" ? [
            { type: "session_meta", timestamp: new Date(now).toISOString(), payload: { id: session, cwd: chat } },
            { type: "turn_context", timestamp: new Date(now).toISOString(), payload: { model: "gpt-6.1-sol", cwd: chat, turn_id: turnId } },
            // Observe zero before paid usage, preserving the released
            // counter-lineage rule instead of assuming a zero baseline.
            { type: "event_msg", timestamp: new Date(now).toISOString(), payload: { type: "token_count", turn_id: turnId,
              info: { total_token_usage: { input_tokens: 0, output_tokens: 0,
                cached_input_tokens: 0, reasoning_output_tokens: 0, total_tokens: 0 } } } },
          ] : [];
          if (index === 0) write(file, [...initial, line].map(value => JSON.stringify(value)).join("\n") + "\n");
          else fs.appendFileSync(file, (kind === "codex" && (stage === "head" || stage === "reupgraded")
            // rollout-tailer.ts:1944,1949,2081 reads the producer's native turn.
            ? JSON.stringify({ type: "turn_context", timestamp: new Date(now).toISOString(),
              payload: { model: "gpt-6.1-sol", turn_id: producerTurnId } }) + "\n" : "") + JSON.stringify(line) + "\n");
          const usingHead = stage === "head" || stage === "reupgraded";
          const modules = usingHead ? current : previous;
          const buffer = new modules.buffer.LocalEventBuffer(ledger, options);
          const io = { ...DEFAULT_JSONL_TAILER_IO, now: () => now };
          const tailer = kind === "claude" ? new modules.claude.TranscriptTailer(buffer, sessions, io)
            : new modules.codex.RolloutTailer(buffer, sessions, () => [], io);
          try {
            const scan = await tailer.scan({ scope: "full", now: new Date(now) });
            const requests = buffer.beginRepoContextResolution(buffer.takeRepoContextBatch());
            if (requests.length) buffer.applyRepoContextResults(modules.context.resolveRepoContextRequests(requests));
            const cursor = buffer.database.prepare("select committed_offset as offset, parser_state_json as state from rollout_scan_state").get() as Cursor;
            const rows = buffer.database.prepare(`select id, input_tokens as input, output_tokens as output,
              repo_hash as repo, payload_json as payload from buffered_events order by observed_at`).all() as UsageRow[];
            const duplicates = (buffer.database.prepare("select coalesce(sum(conflict_count),0) as n from event_collision_quarantine")
              .get() as { n: number }).n;
            const state = JSON.parse(cursor.state);
            const releasedValidatorAccepts = Boolean(kind === "claude" ? previous.claude.validateTranscriptParserState(state)
              : previous.codex.validateRolloutParserState(state));
            const observation = { stage, releasedValidatorAccepts, version: usingHead ? "head" : release.version,
              eventsAppended: scan.eventsAppended, parseErrors: scan.parseErrors,
              checkpointRebuilds: scan.checkpointRebuilds, offset: cursor.offset,
              fileSize: fs.statSync(file).size, usageRows: rows.length, duplicateCollisionReceipts: duplicates,
              summedInput: rows.reduce((sum, row) => sum + row.input, 0),
              summedOutput: rows.reduce((sum, row) => sum + row.output, 0) };
            cadences.push(observation);
            console.log(JSON.stringify({ captureDowngrade: `${release.version}-${kind}`, ...observation }));
            assert.equal(scan.eventsAppended, 1, `${stage} captures fresh ${kind} usage`);
            assert.equal(scan.parseErrors, 0, `${stage} has no parse errors`);
            assert.equal(scan.checkpointRebuilds, 0, `${stage} resumes the checkpoint`);
            assert.ok(cursor.offset > priorOffset);
            assert.equal(cursor.offset, fs.statSync(file).size);
            priorOffset = cursor.offset;
            if (!usingHead) assert.ok(releasedValidatorAccepts);
            assert.equal(rows.length, count);
            assert.equal(new Set(rows.map(row => row.id)).size, count);
            assert.equal(observation.summedInput, count * 19);
            assert.equal(observation.summedOutput, count * 2);
            assert.equal(duplicates, 0, "valid checkpoints need no replay or duplicate receipts");
            for (const row of rows) {
              assert.equal(row.input, 19); assert.equal(row.output, 2);
              const capturedRow = JSON.stringify(buffer.database.prepare("select * from buffered_events where id=?").get(row.id));
              if (frozenRows.has(row.id)) assert.equal(capturedRow, frozenRows.get(row.id));
              else frozenRows.set(row.id, capturedRow);
            }
            const latest = rows.at(-1)!;
            if (index === 0) assert.equal(latest.repo, remoteLinkageHash(remote));
            if (usingHead) {
              assert.equal(latest.repo, null);
              assert.equal(JSON.parse(latest.payload).metadata.repoContextPolicyGeneration, REPO_CONTEXT_CAPTURE_POLICY_GENERATION);
            }
            const beforeLeaseRaw = buffer.database.prepare("select * from buffered_events order by rowid").all();
            // outbox.ts:1578: expire the actual native-capture hold with the injected clock.
            if (usingHead && kind === "codex") now += CODEX_MODEL_WAIT_MS + 1;
            const lease = buffer.delivery.lease({ now: new Date(now) });
            if (usingHead && kind === "codex") {
              // .51/.50 freeze a bare model, discarding native turn provenance.
              // The head must retire those deliveries without rewriting raw
              // diagnostics or making that model billable again.
              const expectedRetired = index === 1 ? 1 : index === 5 ? 3 : 0;
              assert.equal(bareReleasedSeals.size, expectedRetired);
              assert.equal(lease.locallyDead, expectedRetired);
              assert.equal(lease.items.length, count - expectedRetired, "fresh native rows qualify after the model-capture hold");
              assert.equal(JSON.stringify(buffer.database.prepare("select * from buffered_events order by rowid").all()),
                JSON.stringify(beforeLeaseRaw), "all raw rows remain byte-identical across retirement");
              const capturedRaw = buffer.database.prepare("select payload_json, uploaded_at from buffered_events").all() as
                Array<{ payload_json: string; uploaded_at: string | null }>;
              assert.ok(capturedRaw.every(row => row.uploaded_at === null));
              const pending = buffer.database.prepare("select delivery_id, raw_id, base_envelope_json, sealed_envelope_json from upload_outbox where state='pending'")
                .all() as Array<{ delivery_id: string; raw_id: string; base_envelope_json: string; sealed_envelope_json: string | null }>;
              const gaps = pending.filter(row => bareReleasedSeals.has(row.raw_id) &&
                JSON.parse(row.base_envelope_json).event.metadata.usageSource === "capture_gap");
              assert.equal(gaps.length, expectedRetired);
              assert.deepEqual(new Set(gaps.map(row => row.raw_id)), bareReleasedSeals, "one distinct pending gap per retired delivery");
              for (const gap of gaps) {
                retiredRawIds.add(gap.raw_id);
                assert.notEqual(gap.delivery_id, gap.raw_id);
                assert.equal(gap.sealed_envelope_json, null);
                const event = JSON.parse(gap.base_envelope_json).event;
                for (const key of ["model", "inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens", "costUsd"]) {
                  assert.ok(!Object.hasOwn(event, key), `retirement gap has no ${key}`);
                }
                assert.deepEqual(buffer.database.prepare("select terminal_state, reason from upload_receipts where delivery_id=?")
                  .get(gap.raw_id), { terminal_state: "dead", reason: "local_model_capture_gap" });
              }
              for (const item of lease.items) {
                if (retiredRawIds.has(item.rawId!)) {
                  assert.equal(item.envelope.event.metadata.usageSource, "capture_gap");
                  for (const key of ["model", "inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens", "costUsd"]) {
                    assert.ok(!Object.hasOwn(item.envelope.event, key), `head never bills the retired row's ${key}`);
                  }
                } else {
                  assert.equal(item.envelope.event.model, "gpt-6.1-sol");
                  assert.equal(item.envelope.event.inputTokens, 19);
                  assert.equal(item.envelope.event.outputTokens, 2);
                  const raw = JSON.parse((buffer.database.prepare("select payload_json from buffered_events where id=?")
                    .get(item.rawId!) as { payload_json: string }).payload_json);
                  assert.ok(raw.metadata.codexTurnId, "a named head delivery has its own native turn");
                }
              }
              assert.equal(transportCalls, 0);
              totalRetired += expectedRetired;
              bareReleasedSeals.clear();
              console.log(JSON.stringify({ proof: "home-capture-legacy-retirement", releasedVersion: release.version,
                stage, retired: expectedRetired, rawRowsUnchanged: true, uploadedRawRows: 0,
                pendingReplacementGaps: gaps.length, headLeaseItems: lease.items.length, transportCalls,
                retiredRowsBilledInputTokens: 0, retiredRowsBilledOutputTokens: 0, accumulatedRetirements: totalRetired }));
            } else {
              assert.equal(lease.locallyDead, 0);
              assert.equal(lease.items.length, count);
            }
            for (const item of lease.items) {
              if (frozenSeals.has(item.deliveryId)) assert.equal(item.envelopeJson, frozenSeals.get(item.deliveryId));
              else frozenSeals.set(item.deliveryId, item.envelopeJson);
              if (!usingHead && kind === "codex" && item.envelope.event.model === "gpt-6.1-sol") {
                const raw = JSON.parse((buffer.database.prepare("select payload_json from buffered_events where id=?")
                  .get(item.rawId!) as { payload_json: string }).payload_json);
                if (!raw.metadata.codexTurnId) bareReleasedSeals.add(item.deliveryId);
              }
            }
            if (usingHead) {
              const envelope = lease.items.find(item => item.deliveryId === latest.id)!.envelope;
              assert.equal(envelope.event.projectKey, undefined);
              assert.equal(envelope.event.metadata.projectBasis, "unallocated");
              assert.equal(envelope.event.metadata.git, undefined);
              assert.equal(envelope.event.metadata.branchHash, undefined);
              assert.equal(envelope.event.metadata.headSha, undefined);
              assert.ok(!JSON.stringify(envelope).includes("repoContextPolicyGeneration"));
              assert.equal(envelope.event.inputTokens, 19); assert.equal(envelope.event.outputTokens, 2);
            }
          } finally { tailer.close(); buffer.close(); }
        }
        assert.ok(cadences.every(cadence => cadence.releasedValidatorAccepts), "every head checkpoint keeps the released shape");
        // The released reader also delivers everything the branch sealed.
        now += 121_001;
        const final = new previous.buffer.LocalEventBuffer(ledger, options);
        try {
          const lease = final.delivery.lease({ now: new Date(now) });
          assert.equal(lease.locallyDead, 0); assert.equal(lease.items.length, stages.length);
          for (const item of lease.items) assert.equal(item.envelopeJson, frozenSeals.get(item.deliveryId));
          assert.equal(final.delivery.acknowledge(lease.leaseId!, lease.items.map(item => item.deliveryId), new Date(now)).acknowledged, stages.length);
          assert.deepEqual(final.database.prepare(`select
            (select count(*) from upload_outbox) as pending,
            (select count(*) from upload_receipts where terminal_state='dead') as dead,
            (select count(*) from buffered_events where uploaded_at is not null) as uploaded`).get(),
          { pending: 0, dead: totalRetired, uploaded: stages.length });
          assert.equal(totalRetired, kind === "claude" ? 0 : release.reupgrade ? 4 : 1);
        } finally { final.close(); }
        observations.push({ kind, releasedVersion: release.version, releasedCommit: release.commit,
          releasedTailerSha256: hash(fs.readFileSync(path.join(tree, "packages/collector-cli/src", kind === "claude" ? "transcript-tailer.ts" : "rollout-tailer.ts"))),
          sequence: stages, cadences, rawPayloadsAndFrozenSealsUnchanged: true,
          acknowledged: stages.length, retiredUnsafeSeals: totalRetired,
          dropped: 0, doubled: 0, duplicateCollisionReceipts: 0 });
      }
    }
    return observations;
  } finally {
    os.homedir = originalHome;
    globalThis.fetch = originalFetch;
    for (const tree of trees.reverse()) execFileSync("git", ["worktree", "remove", "--force", tree], { stdio: "ignore" });
    fs.rmSync(root, { recursive: true, force: true });
  }
}

/** A released reader's turn-less named rows are retained as local diagnostics.
 * The head may retire an unsafe sealed delivery, but cannot rewrite the raw
 * rows, send guessed usage, or replace it with another token-bearing event. */
export async function runMissingNativeTurnFixture() {
  const currentRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "missing-native-turn-"));
  const tree = path.join(root, "released");
  const release = releases[0];
  const originalFetch = globalThis.fetch;
  let transportCalls = 0;
  globalThis.fetch = async () => {
    transportCalls++;
    throw new Error("missing-native-turn fixture must never send a request");
  };
  let treeAdded = false;
  try {
    execFileSync("git", ["worktree", "add", "--quiet", "--detach", tree, release.commit]);
    treeAdded = true;
    fs.symlinkSync(path.join(currentRoot, "node_modules"), path.join(tree, "node_modules"), "dir");
    const previous = await reader(tree);
    const current = await reader(currentRoot);
    const sessions = path.join(root, "sessions");
    fs.mkdirSync(sessions, { recursive: true, mode: 0o700 });
    const session = "019f8000-0000-7000-8000-000000000475";
    const file = path.join(sessions, "2026/10/05", `rollout-missing-turn-${session}.jsonl`);
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
    const baseMs = Date.parse("2026-10-05T10:00:00.000Z");
    let now = baseMs;
    const counter = (count: number) => ({ type: "event_msg", timestamp: new Date(now).toISOString(),
      payload: { type: "token_count", info: { total_token_usage: { input_tokens: count * 19,
        output_tokens: count * 2, cached_input_tokens: 0, reasoning_output_tokens: 0, total_tokens: count * 21 } } } });
    fs.writeFileSync(file, [
      { type: "session_meta", timestamp: new Date(now).toISOString(), payload: { id: session } },
      { type: "turn_context", timestamp: new Date(now).toISOString(), payload: { model: "gpt-6.1-sol" } },
      counter(0), counter(1),
    ].map(value => JSON.stringify(value)).join("\n") + "\n", { mode: 0o600 });
    const ledger = path.join(root, "ledger.sqlite");
    const options = { enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => new Date(now) } };
    const releasedBuffer = new previous.buffer.LocalEventBuffer(ledger, options);
    const releasedTailer = new previous.codex.RolloutTailer(releasedBuffer, sessions, () => [],
      { ...DEFAULT_JSONL_TAILER_IO, now: () => now });
    let sealedDeliveryId: string;
    try {
      assert.equal((await releasedTailer.scan({ scope: "full", now: new Date(now) })).eventsAppended, 1);
      const lease = releasedBuffer.delivery.lease({ now: new Date(now) });
      assert.equal(lease.items.length, 1, "the actual released reader freezes the turn-less named row");
      sealedDeliveryId = lease.items[0].deliveryId;
      assert.equal(lease.items[0].envelope.event.model, "gpt-6.1-sol");
      assert.equal(lease.items[0].envelope.event.inputTokens, 19);
      assert.equal(lease.items[0].envelope.event.outputTokens, 2);
    } finally { releasedTailer.close(); releasedBuffer.close(); }
    now += 121_001;
    fs.appendFileSync(file, JSON.stringify(counter(2)) + "\n");
    const buffer = new current.buffer.LocalEventBuffer(ledger, options);
    const tailer = new current.codex.RolloutTailer(buffer, sessions, () => [],
      { ...DEFAULT_JSONL_TAILER_IO, now: () => now });
    try {
      assert.equal((await tailer.scan({ scope: "full", now: new Date(now) })).eventsAppended, 1);
      const rawRows = () => buffer.database.prepare("select * from buffered_events order by rowid").all() as
        Array<{ id: string; payload_json: string; input_tokens: number; output_tokens: number; uploaded_at: string | null }>;
      const before = rawRows();
      assert.equal(before.length, 2);
      assert.deepEqual(before.map(row => [row.input_tokens, row.output_tokens]), [[19, 2], [19, 2]]);
      assert.ok(before.every(row => !JSON.parse(row.payload_json).metadata.codexTurnId));
      const rawBytes = before.map(row => JSON.stringify(row));
      const lease = buffer.delivery.lease({ now: new Date(now) });
      assert.equal(lease.locallyDead, 1);
      assert.equal(lease.items.length, 0, "no guessed model is made billable by the head lease");
      assert.deepEqual(rawRows().map(row => JSON.stringify(row)), rawBytes, "both complete raw rows remain byte-identical");
      assert.ok(rawRows().every(row => row.uploaded_at === null), "neither raw row is marked uploaded");
      const pending = buffer.database.prepare("select delivery_id, base_envelope_json, sealed_envelope_json from upload_outbox where state='pending'")
        .all() as Array<{ delivery_id: string; base_envelope_json: string; sealed_envelope_json: string | null }>;
      const gaps = pending.filter(row => JSON.parse(row.base_envelope_json).event.metadata.usageSource === "capture_gap");
      assert.equal(gaps.length, 1, "exactly one pending model-unknown gap replaces the unsafe seal");
      const gap = JSON.parse(gaps[0].base_envelope_json).event;
      assert.notEqual(gaps[0].delivery_id, sealedDeliveryId!);
      assert.equal(gaps[0].sealed_envelope_json, null);
      for (const key of ["model", "inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens", "costUsd"]) {
        assert.ok(!Object.hasOwn(gap, key), `gap has no billable ${key}`);
      }
      assert.ok(pending.every(row => row.sealed_envelope_json === null), "no pending named usage was sealed for transport");
      assert.deepEqual(buffer.database.prepare("select terminal_state, reason from upload_receipts where delivery_id=?")
        .get(sealedDeliveryId!), { terminal_state: "dead", reason: "local_model_capture_gap" });
      assert.equal(transportCalls, 0, "no request was sent");
      const observation = { proof: "home-capture-missing-native-turn", status: "PASS", releasedVersion: release.version,
        releasedCommit: release.commit, rawRows: 2, rawRowsByteIdentical: true, uploadedRawRows: 0,
        diagnosticInputTokens: 38, diagnosticOutputTokens: 4, pendingTokenlessModelUnknownGaps: gaps.length,
        retiredUnsafeSeals: lease.locallyDead, headLeaseItems: lease.items.length, transportCalls, billedInputTokens: 0,
        billedOutputTokens: 0 };
      console.log(JSON.stringify(observation));
      return observation;
    } finally { tailer.close(); buffer.close(); }
  } finally {
    globalThis.fetch = originalFetch;
    if (treeAdded) execFileSync("git", ["worktree", "remove", "--force", tree], { stdio: "ignore" });
    fs.rmSync(root, { recursive: true, force: true });
  }
}

/** The rollout safeguard runs on .51, before the head sees an unsafe retry.
 * Only the released reader's ACK empties the conservative raw drain gate. */
export async function runPreupgradeDrainFixture() {
  const currentRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
  const gateSql = fs.readFileSync(path.join(currentRoot, "scripts/fixtures/codex-upgrade-drain.sql"), "utf8");
  const namedSql = fs.readFileSync(path.join(currentRoot, "scripts/fixtures/codex-named-financial-exposure.sql"), "utf8");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "preupgrade-drain-"));
  const tree = path.join(root, "released");
  const release = releases[0];
  let treeAdded = false;
  try {
    execFileSync("git", ["worktree", "add", "--quiet", "--detach", tree, release.commit]);
    treeAdded = true;
    fs.symlinkSync(path.join(currentRoot, "node_modules"), path.join(tree, "node_modules"), "dir");
    const previous = await reader(tree);
    const current = await reader(currentRoot);
    const sessions = path.join(root, "sessions");
    const session = "019f8000-0000-7000-8000-000000000476";
    const file = path.join(sessions, "2026/10/05", `rollout-drain-${session}.jsonl`);
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
    const baseMs = Date.parse("2026-10-05T10:00:00.000Z");
    const counter = (count: number) => ({ type: "event_msg", timestamp: new Date(baseMs).toISOString(),
      payload: { type: "token_count", info: { total_token_usage: { input_tokens: count * 19,
        output_tokens: count * 2, cached_input_tokens: 0, reasoning_output_tokens: 0, total_tokens: count * 21 } } } });
    fs.writeFileSync(file, [
      { type: "session_meta", timestamp: new Date(baseMs).toISOString(), payload: { id: session } },
      { type: "turn_context", timestamp: new Date(baseMs).toISOString(),
        payload: { model: "gpt-6.1-sol", turn_id: "019f8000-0000-7000-8000-000000000477" } },
      counter(0), counter(1),
    ].map(value => JSON.stringify(value)).join("\n") + "\n", { mode: 0o600 });
    const ledger = path.join(root, "ledger.sqlite");
    const options = { enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => new Date(baseMs) } };
    const gate = (buffer: InstanceType<Reader["buffer"]["LocalEventBuffer"]>) =>
      (buffer.database.prepare(gateSql).get() as { codex_raw_unacknowledged: number }).codex_raw_unacknowledged;
    const named = (buffer: InstanceType<Reader["buffer"]["LocalEventBuffer"]>) =>
      (buffer.database.prepare(namedSql).get() as { codex_named_financial_exposure: number }).codex_named_financial_exposure;
    const releasedBuffer = new previous.buffer.LocalEventBuffer(ledger, options);
    const releasedTailer = new previous.codex.RolloutTailer(releasedBuffer, sessions, () => [],
      { ...DEFAULT_JSONL_TAILER_IO, now: () => baseMs });
    let capturedPayload: string;
    try {
      assert.equal((await releasedTailer.scan({ scope: "full", now: new Date(baseMs) })).eventsAppended, 1);
      assert.equal(gate(releasedBuffer), 1, "unacknowledged .51 raw row blocks the upgrade");
      assert.equal(named(releasedBuffer), 1);
      const lease = releasedBuffer.delivery.lease({ now: new Date(baseMs) });
      assert.equal(lease.locallyDead, 0);
      assert.equal(lease.items.length, 1);
      capturedPayload = (releasedBuffer.database.prepare("select payload_json from buffered_events").get() as { payload_json: string }).payload_json;
      assert.equal(releasedBuffer.delivery.acknowledge(lease.leaseId, lease.items.map(item => item.deliveryId), new Date(baseMs)).acknowledged, 1);
      assert.equal(gate(releasedBuffer), 0, "only the released-reader ACK empties the raw upgrade gate");
      assert.equal(named(releasedBuffer), 0);
    } finally { releasedTailer.close(); releasedBuffer.close(); }
    const buffer = new current.buffer.LocalEventBuffer(ledger, options);
    const tailer = new current.codex.RolloutTailer(buffer, sessions, () => [],
      { ...DEFAULT_JSONL_TAILER_IO, now: () => baseMs });
    try {
      assert.equal((await tailer.scan({ scope: "full", now: new Date(baseMs) })).eventsAppended, 0);
      const lease = buffer.delivery.lease({ now: new Date(baseMs) });
      assert.equal(lease.locallyDead, 0, "a drained upgrade retires no named delivery");
      assert.equal(lease.items.length, 0);
      assert.equal(gate(buffer), 0);
      assert.equal(named(buffer), 0);
      const raw = buffer.database.prepare("select payload_json, uploaded_at from buffered_events").get() as
        { payload_json: string; uploaded_at: string | null };
      assert.equal(raw.payload_json, capturedPayload!);
      assert.ok(raw.uploaded_at);
      assert.equal(JSON.parse(raw.payload_json).model, "gpt-6.1-sol");
      assert.equal((buffer.database.prepare("select count(*) as n from upload_receipts where terminal_state='dead'").get() as { n: number }).n, 0);
      const decisionsTable = buffer.database.prepare("select name from sqlite_master where type='table' and name='codex_capture_decisions'").get();
      if (decisionsTable) {
        assert.equal((buffer.database.prepare("select count(*) as n from codex_capture_decisions where decision='gap'").get() as { n: number }).n, 0);
      }
      assert.equal((buffer.database.prepare("select count(*) as n from upload_outbox").get() as { n: number }).n, 0);
      assert.equal(JSON.parse(raw.payload_json).metadata.usageSource, "rollout");
      const observation = { proof: "home-capture-preupgrade-ack-drain", status: "PASS", releasedVersion: release.version,
        releasedCommit: release.commit, gateSqlSha256: hash(gateSql), diagnosticsSqlSha256: hash(namedSql),
        gateBeforeAck: 1, gateAfterReleasedAck: 0, gateAfterUpgrade: 0, headRetired: 0, modelUnknownGaps: 0,
        acknowledgement: "actual .51 reader ACK invoked locally; no cloud transport exercised" };
      console.log(JSON.stringify(observation));
      return observation;
    } finally { tailer.close(); buffer.close(); }
  } finally {
    if (treeAdded) execFileSync("git", ["worktree", "remove", "--force", tree], { stdio: "ignore" });
    fs.rmSync(root, { recursive: true, force: true });
  }
}
