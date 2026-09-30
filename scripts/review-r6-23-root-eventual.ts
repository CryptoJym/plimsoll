import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

async function main() {
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr426-23-roots-")));
  const epoch = "10000000-0000-4000-8000-000000000001";
  const codex = path.join(fixture, "codex");
  fs.mkdirSync(codex);
  const roots = Array.from({ length: 23 }, (_, i) => {
    const directory = path.join(fixture, `claude-${i}`);
    fs.mkdirSync(directory);
    return { source: "claude_code" as const, directory, installationEpochId: epoch,
      rootId: `50000000-0000-4000-8000-${String(i).padStart(12, "0")}`,
      profileId: `60000000-0000-4000-8000-${String(i).padStart(12, "0")}` };
  });
  const buffer = new LocalEventBuffer(path.join(fixture, "replacement.sqlite"), {
    workspaceId: "30000000-0000-4000-8000-000000000003", freshCaptureRootEpoch: epoch,
  });
  const rollout = new RolloutTailer(buffer, undefined, () => [], undefined, [{ source: "codex" as const,
    rootId: "codex-root", profileId: "codex-profile", installationEpochId: epoch, directory: codex }]);
  const tailer = new TranscriptTailer(buffer, undefined, undefined, roots);
  try {
    for (let pass = 0; pass < 30 && captureBaselineStatus(buffer.database).status !== "complete"; pass += 1) {
      for (const source of [rollout, tailer]) {
        await source.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
      }
    }
    assert.equal(captureBaselineStatus(buffer.database).status, "complete");
    const at = new Date().toISOString();
    for (let i = 0; i < roots.length; i += 1) {
      const session = `70000000-0000-4000-8000-${String(i).padStart(12, "0")}`;
      const file = path.join(roots[i]!.directory, "project", `${session}.jsonl`);
      fs.mkdirSync(path.dirname(file));
      fs.writeFileSync(file, JSON.stringify({ type: "assistant", timestamp: at,
        message: { id: `message-${i}`, model: "claude-opus-5",
          usage: { input_tokens: i + 1, output_tokens: 0 } } }) + "\n");
    }
    for (let pass = 0; pass < 80; pass += 1) {
      if (((buffer.database.prepare("select count(distinct session_id) as n from buffered_events").get() as {n:number}).n) === 23) break;
      await tailer.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
    }
    const row = buffer.database.prepare("select count(distinct session_id) as sessions from buffered_events")
      .get() as { sessions: number };
    console.log(JSON.stringify({ eventualSessions: row.sessions }));
    assert.equal(row.sessions, 23);
    console.log(JSON.stringify({ status: "PASS", captureRoots: 23, sessionsCaptured: row.sessions,
      epoch: buffer.workspaceBinding()!.currentInstallationEpochId, baselineStatus: "complete" }));
  } finally {
    tailer.close(); rollout.close(); buffer.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
