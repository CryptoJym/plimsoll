/**
 * Bead eco-6hoxj.46 — source-scoped validation rejections and replayable dead
 * letters.
 *
 * On 2026-09-12 a hosted cloud whose AiToolSource enum had no GROK rejected
 * every Grok envelope per item (400/422). studio4 bisected a cycle down to 16
 * Grok singletons with no acceptance, inferred a broken contract, and opened
 * the whole-host `contract_blocked` circuit for an hour, holding 33 deliverable
 * claude_code events. The 102 dead letters it had already written were
 * terminal: nothing could re-queue them once the cloud was fixed.
 *
 * These checks fix both halves against a fixture cloud that behaves exactly
 * like the hosted one: a batch containing any item of the rejected source
 * fails 422 as a whole, so the uploader bisects to singletons.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  DeliveryUploadError,
  uploadBufferedEvents as uploadWithProtocol,
} from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acknowledgingFetch } from "./fixtures/delivery-ack-fixture";
import { useFixtureRoot } from "./lib/fixture-root";

const uploadBufferedEvents: typeof uploadWithProtocol = (config, buffer, options = {}) =>
  uploadWithProtocol(config, buffer, {
    ...options,
    ...(options.fetchImpl ? { fetchImpl: acknowledgingFetch(options.fetchImpl) } : {}),
  });

type Check = { name: string; passed: boolean; detail: Record<string, unknown> };
const checks: Check[] = [];
const record = (name: string, passed: boolean, detail: Record<string, unknown> = {}) => {
  checks.push({ name, passed, detail });
  if (!passed) throw new Error(`${name} failed: ${JSON.stringify(detail)}`);
};

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-upload-replay-proof-"));
let ledgerIndex = 0;
const ledger = () => path.join(root, `ledger-${++ledgerIndex}.sqlite`);
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const proofBaseMs = Date.now() + 60 * 60 * 1_000;
const instant = (seconds = 0) => new Date(proofBaseMs + seconds * 1_000);

const config = (delivery: Record<string, number> = {}) =>
  collectorConfigSchema.parse({
    uploadUrl: "http://127.0.0.1:1/ingest",
    tenantId: "00000000-0000-4000-8000-000000000001",
    installKey: "proof-install",
    delivery: {
      maxOldestAgeDays: 3650,
      maxBackoffSeconds: 30,
      requestTimeoutSeconds: 1,
      ...delivery,
    },
  });

function event(n: number, input: Record<string, unknown> = {}) {
  return aiInteractionEventSchema.parse({
    id: uuid(n),
    sessionId: uuid(100_000 + n),
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: instant(n).toISOString(),
    actionClass: "other",
    inputTokens: n + 1,
    outputTokens: 1,
    metadata: { proof: true },
    ...input,
  });
}

function enabledBuffer(file = ledger(), overrides: Record<string, number> = {}) {
  const cfg = config(overrides);
  return {
    cfg,
    file,
    buffer: new LocalEventBuffer(file, {
      workspaceId: cfg.tenantId,
      delivery: { enabled: true, limits: cfg.delivery },
    }),
  };
}

function response(status: number, body: Record<string, unknown> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function requestIds(init?: RequestInit) {
  const parsed = JSON.parse(String(init?.body ?? "{}")) as {
    events?: Array<{ event?: { id?: string } }>;
  };
  return (parsed.events ?? []).map((entry) => entry.event?.id ?? "");
}

/** The hosted cloud's exact shape: any item of a rejected source fails the
 * whole request with a per-item 422, so the uploader must bisect to singletons
 * to find which item is poison. */
function sourceRejectingCloud(rejectedIds: Set<string>, seen?: string[][]) {
  return async (_input: RequestInfo | URL, init?: RequestInit) => {
    const ids = requestIds(init);
    seen?.push(ids);
    const offending = ids.filter((id) => rejectedIds.has(id));
    return offending.length > 0
      ? response(422, {
          error: "invalid_source",
          items: offending.map((id) => ({ id, status: 422 })),
        })
      : response(200, { accepted: ids.length });
  };
}

function acceptingCloud(seen?: string[][]) {
  return async (_input: RequestInfo | URL, init?: RequestInit) => {
    const ids = requestIds(init);
    seen?.push(ids);
    return response(200, { accepted: ids.length });
  };
}

async function expectDeliveryError(run: () => Promise<unknown>, expected: string) {
  try {
    await run();
    return false;
  } catch (error) {
    return error instanceof DeliveryUploadError && error.failureClass === expected;
  }
}

type UploadOutcome = Awaited<ReturnType<typeof uploadWithProtocol>>;
const circuitOf = (result: UploadOutcome) =>
  "circuit" in result.delivery ? result.delivery.circuit : null;

type ReceiptRow = { id: string; state: string; reason: string };
const receipts = (buffer: LocalEventBuffer) =>
  buffer.database
    .prepare(
      `select delivery_id as id, terminal_state as state, reason
       from upload_receipts order by delivery_id`,
    )
    .all() as ReceiptRow[];

const controlCounters = (buffer: LocalEventBuffer) =>
  buffer.database
    .prepare(
      `select receipt_acknowledged as acknowledged, receipt_dead as dead
       from upload_control where singleton = 1`,
    )
    .get() as { acknowledged: number; dead: number };

/** Establish the durable validation witness: one acknowledged sanitized
 * envelope under this contract hash. Every witness-gated case below depends on
 * it existing, exactly as a real host's does after its first acknowledgement. */
async function seedWitness(
  cfg: ReturnType<typeof config>,
  buffer: LocalEventBuffer,
  n: number,
  at: number,
) {
  buffer.append(event(n));
  const seeded = await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: acceptingCloud(),
    now: () => instant(at),
  });
  const witnessRows = (buffer.database
    .prepare(`select count(*) as n from upload_validation_witness`)
    .get() as { n: number }).n;
  return { uploaded: seeded.uploadedEvents, witnessRows };
}

// 1. Mixed-source batch: the rejected source dead-letters per delivery, the
//    accepted source keeps being acknowledged, and the host circuit stays shut.
async function mixedSourceProof() {
  const { buffer, cfg } = enabledBuffer();
  const seed = await seedWitness(cfg, buffer, 1, 10);
  const grokIds = new Set([uuid(11), uuid(12), uuid(13)]);
  for (const n of [11, 12, 13]) buffer.append(event(n, { source: "grok" }));
  for (const n of [14, 15, 16]) buffer.append(event(n));

  const requests: string[][] = [];
  const cycle = await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: sourceRejectingCloud(grokIds, requests),
    now: () => instant(20),
  });
  const afterCycle = buffer.delivery.status(instant(20));
  const cycleReceipts = receipts(buffer);
  const grokDead = cycleReceipts.filter(
    (row) => grokIds.has(row.id) && row.state === "dead" && row.reason === "remote_validation_rejected",
  );
  const codexAcked = cycleReceipts.filter(
    (row) => !grokIds.has(row.id) && row.state === "acknowledged",
  );

  // A following cycle keeps delivering the accepted source: nothing is held.
  const follower = await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: sourceRejectingCloud(grokIds),
    now: () => instant(40),
  });
  const finalStatus = buffer.delivery.status(instant(40));
  const finalAcked = receipts(buffer).filter(
    (row) => !grokIds.has(row.id) && row.state === "acknowledged",
  ).length;

  record(
    "mixed_source_rejection_dead_letters_only_that_source_and_keeps_circuit_none",
    seed.uploaded === 1 &&
      seed.witnessRows === 1 &&
      grokDead.length === 3 &&
      afterCycle.circuit.kind === "none" &&
      finalStatus.circuit.kind === "none" &&
      // every accepted-source delivery reaches acknowledged across the two cycles
      finalAcked === 4 &&
      codexAcked.length >= 1 &&
      circuitOf(cycle) === "none" &&
      circuitOf(follower) === "none" &&
      finalStatus.remainingDelivery === 0 &&
      requests.length <= cfg.delivery.maxProbesPerCycle,
    {
      seed,
      probes: requests.length,
      maxProbes: cfg.delivery.maxProbesPerCycle,
      cycle: { uploaded: cycle.uploadedEvents, dead: cycle.delivery.deadLetters, circuit: circuitOf(cycle) },
      follower: { uploaded: follower.uploadedEvents, circuit: circuitOf(follower) },
      grokDead: grokDead.length,
      acknowledgedAcceptedSource: finalAcked,
      remaining: finalStatus.remainingDelivery,
    },
  );
  buffer.close();
}

// 2a. Single-source cycle, every item rejected, witness available and accepted:
//     the witness proves the contract inside the cycle, so the singletons are
//     proven candidates and the circuit never opens.
async function singleSourceWitnessProvenProof() {
  const { buffer, cfg } = enabledBuffer();
  const seed = await seedWitness(cfg, buffer, 20, 100);
  const grokIds = new Set([uuid(21), uuid(22), uuid(23), uuid(24)]);
  for (const n of [21, 22, 23, 24]) buffer.append(event(n, { source: "grok" }));

  const requests: string[][] = [];
  const cycle = await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: sourceRejectingCloud(grokIds, requests),
    now: () => instant(110),
  });
  const status = buffer.delivery.status(instant(110));
  const dead = receipts(buffer).filter(
    (row) => grokIds.has(row.id) && row.state === "dead" && row.reason === "remote_validation_rejected",
  );
  record(
    "single_source_all_rejected_witness_proven_dead_letters_without_circuit",
    seed.witnessRows === 1 &&
      dead.length === 4 &&
      status.circuit.kind === "none" &&
      cycle.uploadedEvents === 0 &&
      cycle.delivery.deadLetters === 4 &&
      status.remainingDelivery === 0 &&
      requests.length <= cfg.delivery.maxProbesPerCycle &&
      cycle.delivery.attempts <= cfg.delivery.maxProbesPerCycle,
    {
      dead: dead.length,
      circuit: status.circuit.kind,
      probes: requests.length,
      reportedAttempts: cycle.delivery.attempts,
      maxProbes: cfg.delivery.maxProbesPerCycle,
      remaining: status.remainingDelivery,
    },
  );
  buffer.close();
}

// 2b. The studio4 shape: a full bisection of the rejected source spends the
//     entire probe budget, so the witness cannot be probed in this cycle. The
//     inference is deferred (remote_validation) instead of opening the
//     whole-host circuit, and the next cycle's witness reprobe settles it.
//     Events of every other source stay deliverable throughout.
async function probeBudgetDeferralProof() {
  const { buffer, cfg } = enabledBuffer(ledger(), { maxProbesPerCycle: 7 });
  const seed = await seedWitness(cfg, buffer, 30, 200);
  const grokIds = new Set([uuid(31), uuid(32), uuid(33), uuid(34)]);
  for (const n of [31, 32, 33, 34]) buffer.append(event(n, { source: "grok" }));

  const requests: string[][] = [];
  // 4 poison rows bisect as 4 + 2 + 1 = 7 probes: the budget is exactly spent
  // when the queue empties, which is the point at which studio4 inferred a
  // broken contract and opened contract_blocked for an hour.
  const deferred = await expectDeliveryError(
    () => uploadBufferedEvents(cfg, buffer, {
      fetchImpl: sourceRejectingCloud(grokIds, requests),
      now: () => instant(210),
    }),
    "remote_validation",
  );
  const afterDefer = buffer.delivery.status(instant(210));

  // The events the circuit would have held: enqueued while the rejected source
  // is still failing.
  for (const n of [35, 36]) buffer.append(event(n));

  const settle = await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: sourceRejectingCloud(grokIds),
    now: () => instant(260),
  });
  const afterSettle = buffer.delivery.status(instant(260));
  const deliverAccepted = await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: sourceRejectingCloud(grokIds),
    now: () => instant(320),
  });
  const finalStatus = buffer.delivery.status(instant(320));
  const finalReceipts = receipts(buffer);
  const grokDead = finalReceipts.filter(
    (row) => grokIds.has(row.id) && row.state === "dead" && row.reason === "remote_validation_rejected",
  );
  const acceptedAcked = finalReceipts.filter(
    (row) => (row.id === uuid(35) || row.id === uuid(36)) && row.state === "acknowledged",
  );

  record(
    "exhausted_probe_budget_defers_instead_of_opening_whole_host_circuit",
    seed.witnessRows === 1 &&
      deferred &&
      requests.length === 7 &&
      requests.length <= cfg.delivery.maxProbesPerCycle &&
      afterDefer.circuit.kind === "none" &&
      afterDefer.receipts.dead === 0 &&
      settle.delivery.deadLetters === 4 &&
      afterSettle.circuit.kind === "none" &&
      grokDead.length === 4 &&
      acceptedAcked.length === 2 &&
      deliverAccepted.uploadedEvents === 2 &&
      finalStatus.circuit.kind === "none" &&
      finalStatus.remainingDelivery === 0,
    {
      deferred,
      probes: requests.length,
      maxProbes: cfg.delivery.maxProbesPerCycle,
      afterDefer: { circuit: afterDefer.circuit.kind, dead: afterDefer.receipts.dead },
      settleDead: settle.delivery.deadLetters,
      afterSettleCircuit: afterSettle.circuit.kind,
      grokDead: grokDead.length,
      acceptedAcknowledged: acceptedAcked.length,
      finalRemaining: finalStatus.remainingDelivery,
    },
  );
  buffer.close();
}

// 3. The conservative half of the rule is unchanged: a rejected witness, and a
//    fresh host that has never had an acknowledgement under this contract hash,
//    both still open contract_blocked with zero dead letters.
async function contractBlockedStillOpensProof() {
  {
    const { buffer, cfg } = enabledBuffer();
    const seed = await seedWitness(cfg, buffer, 40, 300);
    for (const n of [41, 42, 43]) buffer.append(event(n, { source: "grok" }));
    let probes = 0;
    const failed = await expectDeliveryError(
      () => uploadBufferedEvents(cfg, buffer, {
        fetchImpl: async () => {
          probes += 1;
          return response(422, { error: "contract_broken" });
        },
        now: () => instant(310),
      }),
      "remote_contract",
    );
    const status = buffer.delivery.status(instant(310));
    record(
      "rejected_witness_still_opens_contract_blocked_with_zero_dead_letters",
      seed.witnessRows === 1 &&
        failed &&
        status.circuit.kind === "contract_blocked" &&
        status.receipts.dead === 0 &&
        status.remainingDelivery === 3 &&
        probes <= cfg.delivery.maxProbesPerCycle,
      { failed, circuit: status.circuit.kind, dead: status.receipts.dead, probes },
    );
    buffer.close();
  }
  {
    // No witness at all: nothing proves the contract, so the conservative
    // inference stands exactly as it does on main today.
    const { buffer, cfg } = enabledBuffer();
    for (const n of [51, 52, 53]) buffer.append(event(n, { source: "grok" }));
    let probes = 0;
    const failed = await expectDeliveryError(
      () => uploadBufferedEvents(cfg, buffer, {
        fetchImpl: async () => {
          probes += 1;
          return response(422, { error: "global" });
        },
        now: () => instant(410),
      }),
      "remote_contract",
    );
    const status = buffer.delivery.status(instant(410));
    const witnessRows = (buffer.database
      .prepare(`select count(*) as n from upload_validation_witness`)
      .get() as { n: number }).n;
    record(
      "no_witness_keeps_conservative_contract_blocked",
      failed &&
        witnessRows === 0 &&
        status.circuit.kind === "contract_blocked" &&
        status.receipts.dead === 0 &&
        status.remainingDelivery === 3 &&
        probes <= cfg.delivery.maxProbesPerCycle,
      { failed, witnessRows, circuit: status.circuit.kind, dead: status.receipts.dead, probes },
    );
    buffer.close();
  }
}

// 4. Replay after the contract is fixed.
async function replayProof() {
  const { buffer, cfg, file } = enabledBuffer();
  await seedWitness(cfg, buffer, 60, 500);
  const grokIds = [uuid(61), uuid(62), uuid(63)];
  for (const n of [61, 62, 63]) buffer.append(event(n, { source: "grok" }));
  await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: sourceRejectingCloud(new Set(grokIds)),
    now: () => instant(510),
  });
  const beforeReplay = buffer.delivery.status(instant(510));
  const deadBefore = receipts(buffer).filter(
    (row) => row.state === "dead" && row.reason === "remote_validation_rejected",
  ).length;

  // --dry-run must leave the ledger byte-identical.
  const ledgerFiles = [file, `${file}-wal`, `${file}-shm`];
  const snapshot = () =>
    ledgerFiles.map((candidate) => {
      if (!fs.existsSync(candidate)) return `${path.basename(candidate)}:absent`;
      const stat = fs.statSync(candidate);
      return `${path.basename(candidate)}:${stat.size}:${stat.mtimeMs}`;
    }).join("|");
  const beforeDryRun = snapshot();
  const dry = buffer.delivery.replayDeadLetters({
    reason: "remote_validation_rejected",
    dryRun: true,
    now: instant(520),
  });
  const afterDryRun = snapshot();

  const replayed = buffer.delivery.replayDeadLetters({
    reason: "remote_validation_rejected",
    now: instant(530),
  });
  const afterRequeue = buffer.delivery.status(instant(530));

  // The contract is fixed: the fixture now accepts every source.
  const delivered = await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: acceptingCloud(),
    now: () => instant(540),
  });
  const finalReceipts = receipts(buffer);
  const replayedRows = finalReceipts.filter((row) => grokIds.includes(row.id));
  const counters = controlCounters(buffer);
  const receiptTotals = {
    acknowledged: finalReceipts.filter((row) => row.state === "acknowledged").length,
    dead: finalReceipts.filter((row) => row.state === "dead").length,
  };
  const second = buffer.delivery.replayDeadLetters({
    reason: "remote_validation_rejected",
    now: instant(550),
  });
  const replayLedger = buffer.database
    .prepare(`select delivery_id as id, replay_count as count from upload_replays order by delivery_id`)
    .all() as Array<{ id: string; count: number }>;

  record(
    "replay_requeues_dead_letters_exactly_once_and_is_idempotent",
    deadBefore === 3 &&
      beforeReplay.circuit.kind === "none" &&
      dry.selected === 3 && dry.requeued === 3 && dry.dryRun === true &&
      beforeDryRun === afterDryRun &&
      replayed.selected === 3 && replayed.requeued === 3 &&
      replayed.skipped.alreadyActive === 0 &&
      replayed.skipped.alreadyAcknowledged === 0 &&
      replayed.skipped.missingRaw === 0 &&
      replayed.skipped.privacyDisposed === 0 &&
      afterRequeue.receipts.dead === 0 &&
      afterRequeue.remainingDelivery === 3 &&
      delivered.uploadedEvents === 3 &&
      replayedRows.length === 3 &&
      replayedRows.every((row) => row.state === "acknowledged" && row.reason === "remote_acknowledged") &&
      finalReceipts.filter((row) => row.state === "dead").length === 0 &&
      counters.acknowledged === receiptTotals.acknowledged &&
      counters.dead === receiptTotals.dead &&
      second.selected === 3 &&
      second.requeued === 0 &&
      second.skipped.alreadyAcknowledged === 3 &&
      replayLedger.length === 3 &&
      replayLedger.every((row) => row.count === 1),
    {
      deadBefore,
      dry,
      dryRunLedgerUnchanged: beforeDryRun === afterDryRun,
      beforeDryRun,
      afterDryRun,
      replayed,
      afterRequeue: { dead: afterRequeue.receipts.dead, remaining: afterRequeue.remainingDelivery },
      delivered: delivered.uploadedEvents,
      replayedRows,
      counters,
      receiptTotals,
      second,
      replayLedger,
    },
  );
  buffer.close();
}

// 5. Refusals and bounds.
async function replayBoundsProof() {
  const { buffer, cfg } = enabledBuffer();
  await seedWitness(cfg, buffer, 70, 600);
  const grokIds = [uuid(71), uuid(72), uuid(73), uuid(74)];
  for (const n of [71, 72, 73, 74]) buffer.append(event(n, { source: "grok" }));
  await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: sourceRejectingCloud(new Set(grokIds)),
    now: () => instant(610),
  });

  const localReasons = [
    "local_privacy_violation",
    "local_evidence_quarantined",
    "local_item_oversize",
    "local_schema_invalid",
    "local_payload_unparseable",
    "remote_acknowledged",
  ];
  const refusals = localReasons.map((reason) => {
    try {
      buffer.delivery.replayDeadLetters({ reason, now: instant(620) });
      return { reason, refused: false, message: "" };
    } catch (error) {
      return { reason, refused: true, message: error instanceof Error ? error.message : String(error) };
    }
  });

  // --limit bounds the selection; --since filters on when the delivery died.
  const limited = buffer.delivery.replayDeadLetters({
    reason: "remote_validation_rejected",
    limit: 1,
    dryRun: true,
    now: instant(621),
  });
  const deadAt = buffer.database
    .prepare(
      `select terminal_at as at from upload_receipts
       where terminal_state = 'dead' order by terminal_at limit 1`,
    )
    .get() as { at: string };
  const sinceFuture = buffer.delivery.replayDeadLetters({
    reason: "remote_validation_rejected",
    since: new Date(Date.parse(deadAt.at) + 60_000).toISOString(),
    dryRun: true,
    now: instant(622),
  });
  const sincePast = buffer.delivery.replayDeadLetters({
    reason: "remote_validation_rejected",
    since: new Date(Date.parse(deadAt.at) - 60_000).toISOString(),
    dryRun: true,
    now: instant(623),
  });
  let sinceRejected = false;
  try {
    buffer.delivery.replayDeadLetters({ reason: "remote_validation_rejected", since: "not-a-date" });
  } catch {
    sinceRejected = true;
  }

  // A privacy-disposed raw row and a raw row that no longer exists are both
  // skipped instead of being re-queued.
  buffer.database
    .prepare(`update buffered_events set privacy_disposition = 'local_privacy_violation' where id = ?`)
    .run(grokIds[0]);
  buffer.database.prepare(`delete from buffered_events where id = ?`).run(grokIds[1]);
  const skipping = buffer.delivery.replayDeadLetters({
    reason: "remote_validation_rejected",
    now: instant(630),
  });
  const stillDead = receipts(buffer).filter(
    (row) => (row.id === grokIds[0] || row.id === grokIds[1]) && row.state === "dead",
  ).length;

  record(
    "replay_refuses_local_reasons_and_honours_since_limit_and_skips",
    refusals.every((entry) => entry.refused) &&
      refusals.every((entry) => /only remote terminal reasons are replayable/.test(entry.message)) &&
      limited.selected === 1 &&
      sinceFuture.selected === 0 &&
      sincePast.selected === 4 &&
      sinceRejected &&
      skipping.selected === 4 &&
      skipping.requeued === 2 &&
      skipping.skipped.privacyDisposed === 1 &&
      skipping.skipped.missingRaw === 1 &&
      stillDead === 2,
    {
      refusals: refusals.map((entry) => ({ reason: entry.reason, refused: entry.refused })),
      limited,
      sinceFuture: sinceFuture.selected,
      sincePast: sincePast.selected,
      sinceRejected,
      skipping,
      stillDead,
    },
  );
  buffer.close();
}

// The command is reachable from the CLI and its refusal is a clear error.
function cliSurfaceProof() {
  const sandbox = fs.mkdtempSync(path.join(root, "cli-"));
  const fixture = useFixtureRoot(sandbox, { home: path.join(sandbox, "home") });
  try {
    const cli = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
    const tsx = path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs");
    const run = (args: string[]) =>
      spawnSync(process.execPath, [tsx, cli, ...args], {
        encoding: "utf8",
        env: { ...process.env, ...fixture.env },
        timeout: 120_000,
      });
    const help = run(["--help"]);
    const refused = run(["upload-replay", "--reason", "local_privacy_violation"]);
    const missingReason = run(["upload-replay"]);
    record(
      "cli_registers_upload_replay_and_refuses_local_reasons",
      /upload-replay/.test(help.stdout) &&
        /--reason <receipt reason>/.test(help.stdout) &&
        refused.status === 1 &&
        /only remote terminal reasons are replayable/.test(refused.stderr) &&
        missingReason.status === 1 &&
        /upload-replay requires --reason/.test(missingReason.stderr),
      {
        helpListsCommand: /upload-replay/.test(help.stdout),
        refusedStatus: refused.status,
        refusedStderr: refused.stderr.slice(0, 400),
        missingReasonStatus: missingReason.status,
        missingReasonStderr: missingReason.stderr.slice(0, 400),
      },
    );
  } finally {
    fixture.restore();
  }
}

// 7. Review r1, finding 2: the row limit is a budget for work. A host with a
//    lifetime of already-replayed deliveries must still re-queue the dead
//    letters written today — the replay ledger sorts by the FIRST death, so
//    before the fix those inert rows filled `--limit` and the recovery tool
//    reported a full `selected` while re-queueing nothing.
async function replayLimitCountsOnlyActionableProof() {
  const { buffer, cfg } = enabledBuffer();
  await seedWitness(cfg, buffer, 90, 700);

  // Three deliveries that died early, were replayed, and were then delivered:
  // their `upload_replays` rows keep that early `original_terminal_at` forever.
  const oldIds = [uuid(91), uuid(92), uuid(93)];
  for (const n of [91, 92, 93]) buffer.append(event(n, { source: "grok" }));
  await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: sourceRejectingCloud(new Set(oldIds)),
    now: () => instant(710),
  });
  const firstReplay = buffer.delivery.replayDeadLetters({
    reason: "remote_validation_rejected",
    now: instant(720),
  });
  const settled = await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: acceptingCloud(),
    now: () => instant(730),
  });

  // Later, two fresh dead letters under the same reason.
  const freshIds = [uuid(94), uuid(95)];
  for (const n of [94, 95]) buffer.append(event(n, { source: "grok" }));
  await uploadBufferedEvents(cfg, buffer, {
    fetchImpl: sourceRejectingCloud(new Set(freshIds)),
    now: () => instant(760),
  });
  const deadBefore = receipts(buffer)
    .filter((row) => row.state === "dead" && row.reason === "remote_validation_rejected")
    .map((row) => row.id);

  // The reviewer's reproduction: 3 old replayed-and-acknowledged deliveries,
  // 2 fresh dead letters, `--limit 3`.
  const limited = buffer.delivery.replayDeadLetters({
    reason: "remote_validation_rejected",
    limit: 3,
    now: instant(770),
  });
  const freshLive = (buffer.database
    .prepare(`select count(*) as n from upload_outbox where delivery_id in (?, ?)`)
    .get(freshIds[0], freshIds[1]) as { n: number }).n;
  const freshStillDead = receipts(buffer).filter(
    (row) => freshIds.includes(row.id) && row.state === "dead",
  ).length;

  // Nothing actionable is left, so a full `--limit` of inert rows re-queues
  // nothing: the JSON must point the operator at `--since` rather than being
  // a silent no-op.
  const stalled = buffer.delivery.replayDeadLetters({
    reason: "remote_validation_rejected",
    limit: 2,
    dryRun: true,
    now: instant(780),
  });

  record(
    "replay_limit_counts_only_actionable_rows_and_hints_when_stalled",
    firstReplay.requeued === 3 &&
      settled.uploadedEvents === 3 &&
      deadBefore.length === 2 &&
      freshIds.every((id) => deadBefore.includes(id)) &&
      // 2 actionable + 3 inert: the skips are reported, never charged.
      limited.selected === 5 &&
      limited.requeued === 2 &&
      limited.skipped.alreadyAcknowledged === 3 &&
      limited.skipped.alreadyActive === 0 &&
      limited.skipped.missingRaw === 0 &&
      limited.skipped.privacyDisposed === 0 &&
      limited.hint === undefined &&
      freshLive === 2 &&
      freshStillDead === 0 &&
      stalled.selected === 2 &&
      stalled.requeued === 0 &&
      typeof stalled.hint === "string" &&
      /--since/.test(stalled.hint ?? ""),
    {
      firstReplay,
      settledUploads: settled.uploadedEvents,
      deadBefore,
      limited,
      freshLive,
      freshStillDead,
      stalled,
    },
  );
  buffer.close();
}

// 8. Review r1, finding 3: a regression guard for the POST-LOOP witness gate.
//    Check 2a reaches the witness through the pre-existing in-loop probe at
//    upload.ts:575-610, which needs `queue.length === 0` at the singleton.
//    Here the last queue entry is still pending when the poison singleton is
//    rejected, and that entry then revalidates to zero items (a privacy sweep
//    disposed its raw rows mid-cycle), so the loop exits with probe budget
//    left having never taken the in-loop branch. Only the post-loop gate can
//    prove the contract, and the singleton must be dead-lettered with the
//    circuit at `none`.
async function postLoopWitnessGateProof() {
  const { buffer, cfg } = enabledBuffer(ledger(), { maxProbesPerCycle: 12 });
  const seed = await seedWitness(cfg, buffer, 80, 800);
  const poisonIds = [uuid(81), uuid(82), uuid(83)];
  for (const n of [81, 82, 83]) buffer.append(event(n, { source: "grok" }));

  const requests: string[][] = [];
  const rejecting = sourceRejectingCloud(new Set(poisonIds), requests);
  let disposed: string[] = [];
  const fetchImpl = async (input: RequestInfo | URL, init?: RequestInit) => {
    const ids = requestIds(init);
    const result = await rejecting(input, init);
    // On the singleton probe, dispose the raw rows of the group still queued
    // behind it. The loop shifts that group next, revalidates it to zero
    // deliverable items and exits — with the in-loop witness branch never
    // reachable, because `queue.length` was 1 at the singleton.
    if (ids.length === 1 && poisonIds.includes(ids[0]) && disposed.length === 0) {
      disposed = poisonIds.filter((id) => id !== ids[0]);
      for (const id of disposed) {
        buffer.database
          .prepare(
            `update buffered_events set privacy_disposition = 'local_privacy_violation' where id = ?`,
          )
          .run(id);
      }
    }
    return result;
  };

  // Without the post-loop gate this shape infers a broken contract and throws
  // remote_contract, so the throw is caught and reported as a failed check
  // rather than an opaque stack trace.
  let threw: string | null = null;
  let outcome: UploadOutcome | null = null;
  try {
    outcome = await uploadBufferedEvents(cfg, buffer, {
      fetchImpl,
      now: () => instant(810),
    });
  } catch (error) {
    threw = error instanceof DeliveryUploadError ? error.failureClass : String(error);
  }
  const cycle = outcome;
  const status = buffer.delivery.status(instant(810));
  const rows = receipts(buffer);
  const singletonId = requests[1]?.[0] ?? "";
  const singleton = rows.find((row) => row.id === singletonId);
  const disposedRows = rows.filter(
    (row) => disposed.includes(row.id) && row.state === "dead" && row.reason === "local_privacy_violation",
  );

  record(
    "post_loop_witness_gate_proves_contract_and_dead_letters_without_circuit",
    seed.witnessRows === 1 &&
      // probe sizes 3 (whole lease), 1 (poison singleton), 1 (the witness)
      requests.length === 3 &&
      requests[0].length === 3 &&
      requests[1].length === 1 &&
      requests[2].length === 1 &&
      requests[2][0] === uuid(80) &&
      disposed.length === 2 &&
      singleton?.state === "dead" &&
      singleton?.reason === "remote_validation_rejected" &&
      disposedRows.length === 2 &&
      status.circuit.kind === "none" &&
      threw === null &&
      cycle !== null &&
      cycle.uploadedEvents === 0 &&
      circuitOf(cycle) === "none" &&
      cycle.delivery.attempts === 3 &&
      cycle.delivery.attempts <= cfg.delivery.maxProbesPerCycle &&
      requests.length <= cfg.delivery.maxProbesPerCycle &&
      status.remainingDelivery === 0,
    {
      probes: requests.map((ids) => ids.length),
      witnessProbeIsWitness: requests[2]?.[0] === uuid(80),
      disposedMidCycle: disposed,
      singleton: singleton ?? null,
      disposedDeadLetters: disposedRows.length,
      circuit: status.circuit.kind,
      threw,
      reportedAttempts: cycle?.delivery.attempts ?? null,
      maxProbes: cfg.delivery.maxProbesPerCycle,
      deadLetters: cycle?.delivery.deadLetters ?? null,
      remaining: status.remainingDelivery,
    },
  );
  buffer.close();
}

async function main() {
  try {
    await mixedSourceProof();
    await singleSourceWitnessProvenProof();
    await probeBudgetDeferralProof();
    await contractBlockedStillOpensProof();
    await replayProof();
    await replayBoundsProof();
    await replayLimitCountsOnlyActionableProof();
    cliSurfaceProof();
    // Last: the post-loop witness gate is the only check that discriminates
    // the `validationWitnessProven` assignment at upload.ts:678, so a negative
    // control that deletes it must fail here with every other check green.
    await postLoopWitnessGateProof();
    const failed = checks.filter((check) => !check.passed);
    console.log(
      JSON.stringify(
        {
          schema: "plimsoll.upload-replay-proof.v1",
          status: failed.length === 0 ? "pass" : "fail",
          checks: checks.length,
          failed: failed.length,
          names: checks.map((check) => check.name),
          liveStateTouched: false,
          providerNetworkCalled: false,
        },
        null,
        2,
      ),
    );
    if (failed.length > 0) process.exitCode = 1;
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

void main().catch((error) => {
  console.error(error instanceof Error ? `${error.message}\n${error.stack}` : String(error));
  process.exitCode = 1;
});
