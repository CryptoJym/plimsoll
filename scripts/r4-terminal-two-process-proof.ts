/** A two-process torn-tail repair must retain both fsynced terminal outcomes. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  recordMaintenanceRebuildRefusal, resolveMaintenanceRebuildRefusal } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const script = path.resolve(process.argv[1]!);
const homeArg = process.argv[3];
if (process.argv[2] === "--worker-a" && homeArg) {
  const original = fs.ftruncateSync;
  try {
    (fs as typeof fs & { ftruncateSync: typeof fs.ftruncateSync }).ftruncateSync = ((fd: number, size: number) => {
      fs.writeFileSync(path.join(homeArg, "a-at-truncate"), "ready\n");
      const deadline = Date.now() + 10_000;
      while (!fs.existsSync(path.join(homeArg, "a-release"))) {
        if (Date.now() > deadline) throw new Error("A's truncate seam was not released");
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5);
      }
      return original(fd, size);
    }) as typeof fs.ftruncateSync;
    resolveMaintenanceRebuildRefusal(homeArg, "hook", "claude_code",
      fs.readFileSync(path.join(homeArg, "a-body.json"), "utf8"), { outcome: "terminal" });
  } finally {
    (fs as typeof fs & { ftruncateSync: typeof fs.ftruncateSync }).ftruncateSync = original;
  }
} else if (process.argv[2] === "--worker-b" && homeArg) {
  resolveMaintenanceRebuildRefusal(homeArg, "hook", "claude_code",
    fs.readFileSync(path.join(homeArg, "b-body.json"), "utf8"), { outcome: "terminal" });
} else {
  void (async () => {
  async function waitForFile(file: string) {
    const deadline = Date.now() + 12_000;
    while (!fs.existsSync(file)) {
      if (Date.now() > deadline) throw new Error(`missing worker barrier ${path.basename(file)}`);
      await delay(10);
    }
  }
  function waitForExit(child: ChildProcess) {
    return new Promise<number>((resolve, reject) => {
      child.on("error", reject);
      child.on("exit", (code) => resolve(code ?? -1));
    });
  }
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r4-terminal-two-")));
  const children: ChildProcess[] = [];
  try {
    const a = JSON.stringify({ id: randomUUID(), hook_event_name: "UserPromptSubmit", prompt: "A" });
    const b = JSON.stringify({ id: randomUUID(), hook_event_name: "UserPromptSubmit", prompt: "B" });
    fs.writeFileSync(path.join(home, "a-body.json"), a);
    fs.writeFileSync(path.join(home, "b-body.json"), b);
    markMaintenanceRebuildPause(home);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", a);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", b);
    finishMaintenanceRebuildPause(home);
    const directory = path.join(home, "maintenance-rebuild-refusals");
    assert.equal(fs.readdirSync(directory).length, 2);
    const journal = path.join(home, "maintenance-rebuild-terminal.jsonl");
    fs.writeFileSync(journal, '{"version":1,"receipt":"torn', { mode: 0o600 });
    const loader = path.resolve(path.dirname(script), "../node_modules/tsx/dist/loader.mjs");
    const aChild = spawn(process.execPath, ["--import", loader, script, "--worker-a", home],
      { stdio: "inherit", env: process.env });
    children.push(aChild);
    const aExit = waitForExit(aChild);
    await waitForFile(path.join(home, "a-at-truncate"));
    const bChild = spawn(process.execPath, ["--import", loader, script, "--worker-b", home],
      { stdio: "inherit", env: process.env });
    children.push(bChild);
    const bExit = waitForExit(bChild);
    // On the unfenced base B can finish during A's stale tail inspection;
    // with the process-shared lock B must wait until A releases it. In both
    // cases release A, then inspect the durable journal rather than demanding
    // an impossible B-before-A order from the repaired implementation.
    const bFinishedBeforeRelease = await Promise.race([
      bExit.then(() => true), delay(2_500).then(() => false),
    ]);
    fs.writeFileSync(path.join(home, "a-release"), "go\n");
    assert.equal(await bExit, 0);
    assert.equal(await aExit, 0);
    const lines = fs.readFileSync(journal, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line));
    const terminalIds = lines.filter((line) => line.outcome === "terminal").map((line) => line.eventId);
    console.log(JSON.stringify({ check: "two_process_terminal_torn_tail", bFinishedBeforeRelease, terminalIds,
      receiptFilesRemaining: fs.readdirSync(directory).filter((name) => name.endsWith(".receipt")).length }));
    assert.equal(terminalIds.length, 2, "both terminal journal rows must survive");
  } finally {
    for (const child of children) if (child.exitCode === null && child.pid) child.kill("SIGTERM");
    fs.rmSync(home, { recursive: true, force: true });
  }
  })().catch((error) => { console.error(error); process.exitCode = 1; });
}
