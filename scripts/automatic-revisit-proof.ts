// Adapted from the accepted eco-6hoxj.3 0.7.2 pipeline-proof.ts: same real
// filesystem baseline, 20 roots, 248 excluded files, SQLite and maintenance.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { AUTOMATIC_CAPTURE_LIMITS } from "../packages/collector-cli/src/capture-work-budget";
import { DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey, readJsonlTail } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { rootCursorKey, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";

type Visit = { cadence: number; offset: number; deferred: number };
async function prove(source: CaptureRoot["source"]) {
  const base = fs.mkdtempSync(path.join(fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir()), "revisit-"));
  const directories: Array<{ source: CaptureRoot["source"]; directory: string }> = [];
  const privateFiles = new Set<string>();
  const dateParts = new Date().toISOString().slice(0, 10).split("-");
  for (const [provider, count, files] of [["codex", 13, 173], ["claude_code", 7, 75]] as const) {
    for (let i = 0; i < count; i++) {
      const directory = path.join(base, provider, String(i));
      directories.push({ source: provider, directory });
      const leaf = provider === "codex" ? path.join(directory, ...dateParts) : path.join(directory, "project");
      fs.mkdirSync(leaf, { recursive: true });
      for (let j = i; j < files; j += count) {
        const file = path.join(leaf, `rollout-legacy-${j}.jsonl`);
        fs.writeFileSync(file, "PRIVATE_SYNTHETIC_HISTORY_MUST_NOT_BE_READ\n");
        privateFiles.add(file);
      }
    }
  }
  await new Promise(resolve => setTimeout(resolve, 5));
  const bufferOptions = { workspaceId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", delivery: { enabled: true } };
  let buffer = new LocalEventBuffer(path.join(base, "ledger.sqlite"), bufferOptions);
  const roots = directories.map((r, i): CaptureRoot => ({ ...r, rootId: `root-${i}`, profileId: `profile-${i}`,
    installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! }));
  let bodyReads = 0, privateReadAttempts = 0, maxSliceRecords = 0;
  const readAttempts = new Map<string, number>();
  const io = { ...DEFAULT_JSONL_TAILER_IO, readTail: (...args: Parameters<typeof readJsonlTail>) => {
    readAttempts.set(args[0], (readAttempts.get(args[0]) ?? 0) + 1);
    if (privateFiles.has(args[0])) privateReadAttempts++;
    assert(!privateFiles.has(args[0]), "baseline history body read"); bodyReads++;
    maxSliceRecords = Math.max(maxSliceRecords, args[3]?.maxRecords ?? 0);
    assert((args[3]?.maxRecords ?? 0) <= 64, "record quantum stays bounded");
    return readJsonlTail(...args);
  } };
  let tailers: [RolloutTailer, TranscriptTailer];
  const make = () => {
    tailers = [new RolloutTailer(buffer, undefined, () => [], io, roots.filter(r => r.source === "codex")),
      new TranscriptTailer(buffer, undefined, io, roots.filter(r => r.source === "claude_code"))];
    return new CollectorMaintenance(buffer, ...tailers);
  };
  let maintenance = make();
  const restart = () => { maintenance.close(); buffer.close();
    buffer = new LocalEventBuffer(path.join(base, "ledger.sqlite"), bufferOptions); maintenance = make(); };
  const cadences: any[] = [];
  const run = async (phase: string) => {
    let frames = 0, lastKey = "";
    let sourceAdmission: { frames: number; remainingMs: number } | null = null;
    const result = await maintenance.runRecent({
      onDurableCommit: () => frames < 120 ? (++frames, true) : false,
      onProgress: p => {
        if (p.stage === "source_scan" && p.source === source) {
          const budget = maintenance.status().budget!;
          sourceAdmission = { frames, remainingMs: budget.maxWallMs - budget.elapsedWallMs };
        }
        const key = `${p.source}:${p.stage}:${p.candidateHash ?? "none"}`;
        if (p.stage !== "git_context" && key === lastKey) return true;
        if (p.stage === "jsonl_open" && frames >= 118) return false;
        if (frames >= ((p.stage === "source_scan" || p.stage === "jsonl_validation") ? 120 : 112)) return false;
        frames++; lastKey = key; return true;
      },
    });
    const summary = (r: typeof result.rollout | typeof result.transcript) => ({ bytesRead: r.bytesRead, recordsParsed: r.recordsParsed,
      eventsAppended: r.eventsAppended, readErrors: r.readErrors, unresolvedRecords: r.unresolvedRecords });
    // Read-only inspection of actual in-memory inventory, not an alternate scheduler.
    const pending = tailers!.map(t => ((t as any).captureAttempt?.pendingFiles ?? []).map((f: any) => ({
      file: path.relative(base, f.file), servicedCadences: f.servicedCadences ?? 0 })));
    const budget = maintenance.status().budget!;
    assert(pending.every(p => p.length <= 64));
    assert(budget.bytesRead <= 524288 && budget.recordsParsed <= 512 && budget.eventsAppended <= 512);
    assert(budget.maxWallMs === 200);
    cadences.push({ phase, frames, sourceAdmission, pending, codex: summary(result.rollout),
      claude: summary(result.transcript), budget });
    return source === "codex" ? result.rollout : result.transcript;
  };
  const checks: Array<{ name: string; passed: boolean }> = [];
  const check = (name: string, pass: boolean) => { checks.push({ name, passed: pass }); };
  const cursor = (file: string) => buffer.database.prepare(`select committed_offset, deferred_bytes, unresolved_kind,
    unresolved_byte_budget, file_identity from rollout_scan_state where file = ?`)
    .get(jsonlScanStateKey(rootCursorKey(roots, file))) as any;
  try {
    for (let turn = 0; turn < 64 && captureBaselineStatus(buffer.database).status !== "complete"; turn++) await run("baseline");
    check("both real filesystem baselines complete", captureBaselineStatus(buffer.database).status === "complete");
    check("248 historical generations excluded without body reads", captureBaselineStatus(buffer.database).sources.reduce((n, s) => n + s.excludedGenerations, 0) === 248 && bodyReads === 0);
    const baselineCadences = cadences.length;
    const at = new Date().toISOString();
    const session = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const providerRoots = roots.filter(r => r.source === source);
    const fileAt = (root: CaptureRoot, name: string) => path.join(root.directory,
      ...(source === "codex" ? dateParts : ["project"]), `rollout-${name}.jsonl`);
    const target = fileAt(providerRoots.at(-1)!, session);
    const usage = (tokens: number, id = session, padding = 0) => source === "codex"
      ? { type: "event_msg", timestamp: at, payload: { type: "token_count", info: {
        total_token_usage: { input_tokens: tokens, cached_input_tokens: 0, output_tokens: 0, reasoning_output_tokens: 0 },
      } }, padding: "x".repeat(padding) }
      : { type: "assistant", sessionId: id, timestamp: at, message: { id: `${id}-${tokens}`, model: "claude-opus-5",
        usage: { input_tokens: 1, output_tokens: 0 } }, padding: "x".repeat(padding) };
    const prefix = (id: string) => source === "codex" ? [
      { type: "session_meta", timestamp: at, payload: { id } },
      { type: "turn_context", timestamp: at, payload: { model: "gpt-5.5" } }, usage(0, id),
    ] : [];
    const large = (bytes: number) => ({ type: "event_msg", timestamp: at, payload: {
      type: "user_message", message: "SYNTHETIC_BODY_" + "x".repeat(bytes) } });
    const encode = (lines: unknown[]) => lines.map(line => JSON.stringify(line)).join("\n") + "\n";
    fs.writeFileSync(target, encode([...prefix(session), large(350 * 1024), ...Array.from({ length: 300 }, (_, i) => usage(i + 1))]));
    let sawUnresolved = false, sawLargerRetry = false, prior = 0, restartDone = false;
    const visits: Visit[] = [];
    let partialAppends = 0, pendingAppend: { offset: number; attempts: number } | null = null;
    const appendRevisits: Array<{ before: number; after: number; errors: number }> = [];
    let sealedBefore: Array<{ delivery_id: string; sealed_envelope_json: string }> = [];
    const sealed = () => buffer.database.prepare("select delivery_id, sealed_envelope_json from upload_outbox where attempt_count > 0 order by delivery_id").all() as typeof sealedBefore;
    for (let turn = 0; turn < 80 && (cursor(target)?.deferred_bytes !== 0 || !cursor(target)); turn++) {
      const result = await run("capture");
      const c = cursor(target);
      if (pendingAppend && (readAttempts.get(target) ?? 0) > pendingAppend.attempts) {
        appendRevisits.push({ before: pendingAppend.offset, after: c?.committed_offset ?? 0, errors: result.readErrors });
        pendingAppend = null;
      }
      if (result.filesRead > 0) visits.push({ cadence: turn, offset: c?.committed_offset ?? 0, deferred: c?.deferred_bytes ?? 0 });
      sawUnresolved ||= c?.unresolved_kind === "record_exceeds_byte_budget";
      sawLargerRetry ||= result.bytesRead > 65536;
      if (c) { assert(c.committed_offset >= prior); prior = c.committed_offset; }
      if (sawUnresolved && !restartDone) { restart(); restartDone = true; }
      if (result.eventsAppended && sealedBefore.length === 0) {
        buffer.delivery.lease({ leaseId: "revisit-immutable" }); sealedBefore = sealed();
      }
      const attempt = (tailers![source === "codex" ? 0 : 1] as any).captureAttempt;
      if (partialAppends < 3 && !pendingAppend && c?.committed_offset > 1000 && c.deferred_bytes > 0 &&
          attempt?.discoveryDone && attempt.pendingFiles.some((p: any) => p.file === target && p.servicedCadences > 0)) {
        fs.appendFileSync(target, encode([{ type: "fixture_append", timestamp: at }]));
        partialAppends++;
        pendingAppend = { offset: c.committed_offset, attempts: readAttempts.get(target) ?? 0 };
      }
    }
    const advancing = visits.filter(v => v.offset > 1000);
    check("eligible partial snapshot gets useful next-cadence progress", advancing.some((v, i) => i > 0 &&
      advancing[i - 1]!.deferred > 0 && v.cadence === advancing[i - 1]!.cadence + 1 && v.offset > advancing[i - 1]!.offset));
    check("between-cadence appends to retained partial snapshots progress on first retry", partialAppends === 3 && appendRevisits.length === 3 &&
      appendRevisits.every(r => r.after > r.before && r.errors === 0));
    const stat = fs.lstatSync(target, { bigint: true });
    check("real discovery records oversized line", sawUnresolved);
    check("persisted retry clears after cold ledger restart", restartDone && sawLargerRetry && cursor(target)?.unresolved_kind === null);
    check("finite future snapshot fully consumed", cursor(target)?.committed_offset === Number(stat.size) && cursor(target)?.deferred_bytes === 0);
    check("nanosecond physical identity preserved", cursor(target)?.file_identity === `${stat.dev}:${stat.ino}:${stat.birthtimeNs}`);
    // Finite growth is rediscovered through the real directory walk, then sliced normally.
    fs.appendFileSync(target, encode(Array.from({ length: 100 }, (_, i) => usage(301 + i))));
    const growthVisits: Visit[] = [];
    for (let turn = 0; turn < 48 && cursor(target)?.committed_offset !== fs.statSync(target).size; turn++) {
      const before = cursor(target)?.committed_offset;
      await run("growth");
      const c = cursor(target);
      if (c?.committed_offset > before) growthVisits.push({ cadence: turn, offset: c.committed_offset, deferred: c.deferred_bytes });
    }
    check("finite appended work fully consumed", cursor(target)?.committed_offset === fs.statSync(target).size);
    restart();
    for (let turn = 0; turn < 12; turn++) await run("dedupe");
    const total = buffer.database.prepare("select count(*) as n, coalesce(sum(input_tokens),0) as tokens from buffered_events").get() as any;
    check("400 future tokens conserved without duplicate events after restart", total.n === 400 && total.tokens === 400);
    check("attempted outbox bytes remain immutable", sealedBefore.length > 0 && JSON.stringify(sealed()) === JSON.stringify(sealedBefore));
    check("synthetic private source content absent from ledger", !JSON.stringify(buffer.database.prepare("select payload_json from buffered_events").all()).includes("SYNTHETIC_BODY_"));

    // A hot first-root snapshot exceeds five cadence byte allowances. Later-root
    // discovery must advance while that file still has work, including after growth.
    const hot = fileAt(providerRoots[0]!, "cccccccc-cccc-4ccc-8ccc-cccccccccccc");
    const hotId = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
    fs.writeFileSync(hot, encode([...prefix(hotId), ...Array.from({ length: 400 }, (_, i) => usage(i + 1, hotId, 8192))]));
    restart(); // Start a fresh real sweep at the first root, retaining durable truth.
    let discoverySentinel = "";
    let hotServices = 0, hotRetiredAfter = 0;
    let sentinelCadences = 0;
    for (let turn = 0; turn < 80; turn++) {
      const hotBefore = cursor(hot)?.committed_offset ?? 0;
      await run("fairness");
      if ((cursor(hot)?.committed_offset ?? 0) > hotBefore) hotServices++;
      const hotPending = cadences.at(-1).pending.flat().some((p: any) => p.file === path.relative(base, hot));
      if (!hotRetiredAfter && hotServices > 0 && !hotPending && cursor(hot)?.deferred_bytes > 0) hotRetiredAfter = hotServices;
      if (cursor(hot)?.committed_offset > 0 && !discoverySentinel) {
        discoverySentinel = fileAt(providerRoots.at(-1)!, "dddddddd-dddd-4ddd-8ddd-dddddddddddd");
        fs.writeFileSync(discoverySentinel, encode([...prefix("dddddddd-dddd-4ddd-8ddd-dddddddddddd"), usage(1, "dddddddd-dddd-4ddd-8ddd-dddddddddddd")]));
      }
      if (discoverySentinel) sentinelCadences++;
      if (cursor(discoverySentinel)?.deferred_bytes === 0 && hotRetiredAfter > 0) break;
    }
    check("new-file discovery progresses despite a hot partial snapshot", Boolean(discoverySentinel) &&
      cursor(discoverySentinel)?.deferred_bytes === 0 && cursor(hot)?.deferred_bytes > 0 && sentinelCadences <= 32);

    check("hot snapshot releases its slot within five serviced cadences", hotRetiredAfter > 0 && hotRetiredAfter <= 5);
    fs.appendFileSync(hot, encode([usage(401, hotId, 8192)]));
    const giant = fileAt(providerRoots[0]!, "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee");
    fs.writeFileSync(giant, encode([large(600 * 1024)]));
    restart();
    let giantSentinel = "", giantCadences = 0;
    for (let turn = 0; turn < 80; turn++) {
      await run("giant");
      if (cursor(giant)?.unresolved_byte_budget === 524288 && !giantSentinel) {
        giantSentinel = fileAt(providerRoots.at(-1)!, "ffffffff-ffff-4fff-8fff-ffffffffffff");
        fs.writeFileSync(giantSentinel, encode([...prefix("ffffffff-ffff-4fff-8fff-ffffffffffff"), usage(1, "ffffffff-ffff-4fff-8fff-ffffffffffff")]));
      }
      if (giantSentinel) giantCadences++;
      if (cursor(giantSentinel)?.deferred_bytes === 0) break;
    }
    check("above-512KiB record remains truthfully unresolved", cursor(giant)?.committed_offset === 0 &&
      cursor(giant)?.unresolved_kind === "record_exceeds_byte_budget" && cursor(giant)?.unresolved_byte_budget === 524288);
    check("above-cap record releases discovery queue", Boolean(giantSentinel) && cursor(giantSentinel)?.deferred_bytes === 0 && giantCadences <= 32);
    check("pending metadata and all cooperative limits remain bounded", cadences.every(c => c.pending.every((p: any[]) => p.length <= 64)) &&
      JSON.stringify(AUTOMATIC_CAPTURE_LIMITS) === JSON.stringify({ maxBytes: 524288, maxRecords: 512, maxEvents: 512, maxWallMs: 200, sliceBytes: 65536, sliceRecords: 64 }));
    check("no excluded historical body reads attempted throughout capture", privateReadAttempts === 0);
    check("all actual reader slices retain the 64-record ceiling", maxSliceRecords === 64);
    const race = path.join(base, "race.jsonl"); fs.writeFileSync(race, '{}\n');
    const stale = fs.lstatSync(race); fs.appendFileSync(race, '{}\n');
    let refused = false; try { readJsonlTail(race, stale, undefined)?.close(); } catch { refused = true; }
    check("stale discovery size still refuses commit", refused);
    const fresh = readJsonlTail(race, fs.lstatSync(race), undefined)!;
    fresh.assertStableForCommit(); fresh.close();
    check("fresh precise metadata retries same physical generation", Boolean(fresh));
    return { source, roots: 20, historicalFiles: 248, baselineCadences, checks, visits, growthVisits, partialAppends, appendRevisits,
      sentinelCadences, giantCadences, hotRetiredAfter, privateReadAttempts, maxSliceRecords, finalCursor: cursor(target), cadences, passed: checks.every(c => c.passed) };
  } finally { maintenance.close(); buffer.close(); fs.rmSync(base, { recursive: true, force: true }); }
}
async function main() {
  const proofs = [];
  for (const source of ["codex", "claude_code"] as const) proofs.push(await prove(source));
  const passed = proofs.every(p => p.passed);
  console.log(JSON.stringify({ schema: "plimsoll.automatic-revisit-proof.v1", syntheticOnly: true, proofs, passed }, null, 2));
  if (!passed) process.exitCode = 1;
}
main().catch(error => { console.error(error); process.exitCode = 1; });
