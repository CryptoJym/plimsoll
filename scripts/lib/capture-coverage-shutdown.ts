import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import type { CaptureRoot } from "../../packages/collector-cli/src/capture-root-inventory";
import { DEFAULT_JSONL_TAILER_IO } from "../../packages/collector-cli/src/jsonl-byte-tailer";
import { GrokUsageTailer } from "../../packages/collector-cli/src/grok-usage-tailer";
import { CollectorMaintenance } from "../../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../../packages/collector-cli/src/transcript-tailer";

type Check = (name: string, passed: boolean, detail: Record<string, unknown>) => void;

/** A suspended real-tailer cursor must be closed by maintenance shutdown. */
export function shutdownCoverageCheck(check: Check) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "coverage-shutdown-"));
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
    workspaceId: "00000000-0000-4000-8000-0000000000c1",
    delivery: { enabled: true, limits: { maxOldestAgeDays: 3650 } },
  });
  const epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
  const codex = path.join(root, "codex");
  const claude = path.join(root, "claude");
  const grokHome = path.join(root, "grok");
  const day = path.join(codex, "2026", "09", "25");
  const project = path.join(claude, "project");
  fs.mkdirSync(day, { recursive: true });
  fs.mkdirSync(project, { recursive: true });
  fs.mkdirSync(path.join(grokHome, "sessions"), { recursive: true });
  for (let i = 0; i < 200; i += 1) {
    fs.writeFileSync(path.join(day, `rollout-2026-09-25T00-00-00-${String(i).padStart(12, "0")}.jsonl`), "");
    fs.writeFileSync(path.join(project, `${String(i).padStart(8, "0")}.jsonl`), "");
  }
  const roots: CaptureRoot[] = [
    { source: "codex", rootId: "codex-0", profileId: "codex-0", directory: codex, installationEpochId: epoch },
    { source: "claude_code", rootId: "claude-0", profileId: "claude-0", directory: claude, installationEpochId: epoch },
  ];
  const rollout = new RolloutTailer(buffer, undefined, () => [], DEFAULT_JSONL_TAILER_IO, roots.slice(0, 1));
  const transcript = new TranscriptTailer(buffer, undefined, DEFAULT_JSONL_TAILER_IO, roots.slice(1));
  const grok = new GrokUsageTailer(buffer, grokHome);
  const maintenance = new CollectorMaintenance(buffer, rollout, transcript, undefined, grok,
    { captureCoverageTurnMs: 90, captureCoverageIntervalMs: 0 });
  const originalOpen = fs.opendirSync;
  const live = new Set<fs.Dir>();
  fs.opendirSync = ((directory: fs.PathLike, options?: fs.OpenDirOptions) => {
    const handle = originalOpen(directory, options);
    if (String(directory).startsWith(root + path.sep)) {
      live.add(handle);
      const read = handle.readSync.bind(handle);
      const close = handle.closeSync.bind(handle);
      handle.readSync = () => {
        if (String(directory) === day || String(directory) === project) {
          const until = performance.now() + 1;
          while (performance.now() < until) { /* suspend the cursor mid-directory */ }
        }
        return read();
      };
      handle.closeSync = () => { live.delete(handle); return close(); };
    }
    return handle;
  }) as typeof fs.opendirSync;
  try {
    (maintenance as unknown as { checkCaptureCoverage(): void }).checkCaptureCoverage();
    const suspended = live.size;
    maintenance.close();
    check("maintenance_shutdown_closes_real_tailer_directory_handles",
      suspended > 0 && live.size === 0, { suspended, afterClose: live.size });
  } finally {
    for (const handle of live) handle.closeSync();
    fs.opendirSync = originalOpen;
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}
