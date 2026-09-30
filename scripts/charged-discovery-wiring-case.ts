// A charged-wall regression case through the real maintenance and tailer path.
// It keeps the independent review's 20-root hot-snapshot fixture and spends
// 201 ms at a discovery progress step in every fairness cadence. Removing the
// automatic tailers' minimumSteps request must starve the new sentinel here.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { rootCursorKey, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { installVirtualClock, restoreRealClock, spend } from "./lib/virtual-clock";

async function prove(source: CaptureRoot["source"]) {
  const base = fs.mkdtempSync(path.join(fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir()), "revisit-focus-"));
  const directories: Array<{ source: CaptureRoot["source"]; directory: string }> = [];
  const dateParts = new Date().toISOString().slice(0, 10).split("-");
  for (const [provider, count, files] of [["codex", 13, 173], ["claude_code", 7, 75]] as const) {
    for (let i = 0; i < count; i++) {
      const directory = path.join(base, provider, String(i));
      directories.push({ source: provider, directory });
      const leaf = provider === "codex" ? path.join(directory, ...dateParts) : path.join(directory, "project");
      fs.mkdirSync(leaf, { recursive: true });
      for (let j = i; j < files; j += count) fs.writeFileSync(path.join(leaf, `rollout-legacy-${j}.jsonl`), "PRIVATE_SYNTHETIC_HISTORY_MUST_NOT_BE_READ\n");
    }
  }
  await new Promise(resolve => setTimeout(resolve, 5));
  const bufferOptions = { workspaceId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", delivery: { enabled: true } };
  let buffer = new LocalEventBuffer(path.join(base, "ledger.sqlite"), bufferOptions);
  const roots = directories.map((r, i): CaptureRoot => ({ ...r, rootId: `root-${i}`, profileId: `profile-${i}`,
    installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! }));
  const providerRoots = roots.filter(r => r.source === source);
  const fileAt = (root: CaptureRoot, name: string) => path.join(root.directory,
    ...(source === "codex" ? dateParts : ["project"]), `rollout-${name}.jsonl`);
  const at = new Date().toISOString();
  const usage = (tokens: number, id: string, padding = 0) => source === "codex"
    ? { type: "event_msg", timestamp: at, payload: { type: "token_count", info: {
      total_token_usage: { input_tokens: tokens, cached_input_tokens: 0, output_tokens: 0, reasoning_output_tokens: 0 },
    } }, padding: "x".repeat(padding) }
    : { type: "assistant", sessionId: id, timestamp: at, message: { id: `${id}-${tokens}`, model: "claude-opus-5",
      usage: { input_tokens: 1, output_tokens: 0 } }, padding: "x".repeat(padding) };
  const prefix = (id: string) => source === "codex" ? [
    { type: "session_meta", timestamp: at, payload: { id } },
    { type: "turn_context", timestamp: at, payload: { model: "gpt-5.5" } }, usage(0, id),
  ] : [];
  const encode = (lines: unknown[]) => lines.map(line => JSON.stringify(line)).join("\n") + "\n";
  let maintenance: CollectorMaintenance;
  const make = () => new CollectorMaintenance(buffer,
    new RolloutTailer(buffer, undefined, () => [], undefined, roots.filter(r => r.source === "codex")),
    new TranscriptTailer(buffer, undefined, undefined, roots.filter(r => r.source === "claude_code")));
  maintenance = make();
  const restart = () => { maintenance.close(); buffer.close();
    buffer = new LocalEventBuffer(path.join(base, "ledger.sqlite"), bufferOptions); maintenance = make(); };
  const cursor = (file: string) => buffer.database.prepare("select committed_offset, deferred_bytes from rollout_scan_state where file = ?")
    .get(jsonlScanStateKey(rootCursorKey(roots, file))) as { committed_offset: number; deferred_bytes: number } | undefined;
  let chargeWork = false;
  const run = async () => {
    let frames = 0, lastKey = "", preempted = false;
    const result = await maintenance.runRecent({
      onDurableCommit: () => frames < 120 ? (++frames, true) : false,
      onProgress: p => {
        if (chargeWork && !preempted && ["discovery_directory", "discovery_read", "candidate_metadata"].includes(p.stage)) { spend(201); preempted = true; }
        const key = `${p.source}:${p.stage}:${p.candidateHash ?? "none"}`;
        if (p.stage !== "git_context" && key === lastKey) return true;
        if (p.stage === "jsonl_open" && frames >= 118) return false;
        if (frames >= ((p.stage === "source_scan" || p.stage === "jsonl_validation") ? 120 : 112)) return false;
        frames++; lastKey = key; return true;
      },
    });
    return result;
  };
  try {
    installVirtualClock();
    let baselineCadences = 0;
    while (baselineCadences < 64 && captureBaselineStatus(buffer.database).status !== "complete") {
      await run(); baselineCadences++;
    }
    if (captureBaselineStatus(buffer.database).status !== "complete") throw new Error("baseline incomplete");
    const hotId = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
    const hot = fileAt(providerRoots[0]!, hotId);
    fs.writeFileSync(hot, encode([...prefix(hotId), ...Array.from({ length: 400 }, (_, i) => usage(i + 1, hotId, 8192))]));
    restart(); chargeWork = true;
    let sentinel = "", sentinelCadences = 0, hotServices = 0, hotRetiredAfter = 0;
    let consumedWhileHot = false, sentinelConsumed = false, hotDeferredAtEnd = 0;
    let wallExhausted = 0, fairnessCadences = 0;
    for (let turn = 0; turn < 80; turn++) {
      const hotBefore = cursor(hot)?.committed_offset ?? 0;
      await run(); fairnessCadences++;
      if (maintenance.status().budget?.exhaustedBy === "wall") wallExhausted++;
      if ((cursor(hot)?.committed_offset ?? 0) > hotBefore) hotServices++;
      const tailer = source === "codex" ? (maintenance as any).rolloutTailer : (maintenance as any).transcriptTailer;
      const hotPending = (tailer?.captureAttempt?.pendingFiles ?? []).some((p: any) => p.file === hot);
      if (!hotRetiredAfter && hotServices > 0 && !hotPending && (cursor(hot)?.deferred_bytes ?? 0) > 0)
        hotRetiredAfter = hotServices;
      if ((cursor(hot)?.committed_offset ?? 0) > 0 && !sentinel) {
        const id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
        sentinel = fileAt(providerRoots.at(-1)!, id);
        fs.writeFileSync(sentinel, encode([...prefix(id), usage(1, id)]));
      }
      if (sentinel) {
        sentinelCadences++;
        sentinelConsumed = cursor(sentinel)?.deferred_bytes === 0;
        consumedWhileHot ||= sentinelConsumed && (cursor(hot)?.deferred_bytes ?? 0) > 0;
      }
      // The original assertion is irrecoverably false after this point.
      if (sentinelCadences > 32 && !sentinelConsumed) break;
      if (sentinelConsumed && hotRetiredAfter > 0) break;
    }
    hotDeferredAtEnd = cursor(hot)?.deferred_bytes ?? 0;
    const passed = Boolean(sentinel) && consumedWhileHot && hotRetiredAfter > 0 &&
      sentinelCadences <= 32 && wallExhausted > 0;
    return { source, passed, baselineCadences, fairnessCadences, sentinelCadences, hotServices,
      hotRetiredAfter, hotDeferredAtEnd, sentinelConsumed, consumedWhileHot, wallExhausted };
  } finally {
    restoreRealClock();
    maintenance.close(); buffer.close(); fs.rmSync(base, { recursive: true, force: true });
  }
}

export async function proveChargedDiscoveryWiring() {
  const proofs = [];
  for (const source of ["codex", "claude_code"] as const) proofs.push(await prove(source));
  const passed = proofs.every(p => p.passed);
  return { passed, proofs };
}
