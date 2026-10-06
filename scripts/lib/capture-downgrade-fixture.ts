import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { DEFAULT_JSONL_TAILER_IO } from "../../packages/collector-cli/src/jsonl-byte-tailer";
import { remoteLinkageHash } from "../../packages/shared/src/index";

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
        const sessions = path.join(directory, "sessions");
        const file = kind === "claude" ? path.join(sessions, "project", session + ".jsonl")
          : path.join(sessions, "2026/10/05", `rollout-proof-${session}.jsonl`);
        const baseMs = Date.parse("2026-10-05T10:00:00.000Z");
        let now = baseMs;
        const options = { enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => new Date(now) } };
        const ledger = path.join(directory, "ledger.sqlite");
        const stages = ["released", "head", "downgraded", "downgraded", "downgraded",
          ...(release.reupgrade ? ["reupgraded", "reupgraded"] : [])];
        const frozenRows = new Map<string, string>();
        const frozenSeals = new Map<string, string>();
        const cadences: Array<Record<string, unknown>> = [];
        let priorOffset = 0;
        for (const [index, stage] of stages.entries()) {
          now = baseMs + index * 121_001; // Every previous 120-second lease has expired.
          const count = index + 1;
          const line = kind === "claude" ? { type: "assistant", sessionId: session,
            timestamp: new Date(now).toISOString(), ...(index === 0 ? { cwd: chat } : {}),
            message: { id: "growing-message", model: "claude-sonnet-4-20250514", usage: {
              input_tokens: count * 19, output_tokens: count * 2, cache_read_input_tokens: 0, cache_creation_input_tokens: 0,
            } } } : { type: "event_msg", timestamp: new Date(now).toISOString(), payload: {
              type: "token_count", info: { total_token_usage: { input_tokens: count * 19, output_tokens: count * 2,
                cached_input_tokens: 0, reasoning_output_tokens: 0, total_tokens: count * 21 } },
            } };
          const initial = kind === "codex" ? [
            { type: "session_meta", timestamp: new Date(now).toISOString(), payload: { id: session, cwd: chat } },
            { type: "turn_context", timestamp: new Date(now).toISOString(), payload: { model: "gpt-6.1-sol", cwd: chat } },
            // Observe zero before paid usage, preserving the released
            // counter-lineage rule instead of assuming a zero baseline.
            { type: "event_msg", timestamp: new Date(now).toISOString(), payload: { type: "token_count",
              info: { total_token_usage: { input_tokens: 0, output_tokens: 0,
                cached_input_tokens: 0, reasoning_output_tokens: 0, total_tokens: 0 } } } },
          ] : [];
          if (index === 0) write(file, [...initial, line].map(value => JSON.stringify(value)).join("\n") + "\n");
          else fs.appendFileSync(file, JSON.stringify(line) + "\n");
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
              assert.equal(JSON.parse(latest.payload).metadata.repoContextPolicyGeneration, 2);
            }
            const lease = buffer.delivery.lease({ now: new Date(now) });
            assert.equal(lease.locallyDead, 0);
            assert.equal(lease.items.length, count);
            for (const item of lease.items) {
              if (frozenSeals.has(item.deliveryId)) assert.equal(item.envelopeJson, frozenSeals.get(item.deliveryId));
              else frozenSeals.set(item.deliveryId, item.envelopeJson);
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
          { pending: 0, dead: 0, uploaded: stages.length });
        } finally { final.close(); }
        observations.push({ kind, releasedVersion: release.version, releasedCommit: release.commit,
          releasedTailerSha256: hash(fs.readFileSync(path.join(tree, "packages/collector-cli/src", kind === "claude" ? "transcript-tailer.ts" : "rollout-tailer.ts"))),
          sequence: stages, cadences, rawPayloadsAndFrozenSealsUnchanged: true,
          acknowledged: stages.length, dropped: 0, doubled: 0, duplicateCollisionReceipts: 0 });
      }
    }
    return observations;
  } finally {
    os.homedir = originalHome;
    for (const tree of trees.reverse()) execFileSync("git", ["worktree", "remove", "--force", tree], { stdio: "ignore" });
    fs.rmSync(root, { recursive: true, force: true });
  }
}
