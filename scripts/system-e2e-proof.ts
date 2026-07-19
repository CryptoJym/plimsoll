#!/usr/bin/env node

/**
 * Source-only integrated release proof for issue #105.
 *
 * This is intentionally a controller plus one shared synthetic flow. The
 * shared event/session/pull identifiers cross the real ledger, outbox,
 * allocation, immutable outcome, attempt/exposure, and learning-packet code.
 * Existing adversarial proofs support the install, join, privacy, lifecycle,
 * and idle/dashboard resource boundaries. No live service or provider is
 * reachable from this proof.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  LEARNING_ANALYSIS_VERSION,
  LEARNING_EVIDENCE_SCHEMA_VERSION,
  aiInteractionEventSchema,
  compileLearningEvidencePacket,
  computeLearningPairDigest,
  derivePullOutcomeTimeline,
  type AiInteractionEvent,
  type LearningEvidenceManifest,
  type LearningObservation,
  type PullTimelineFact,
} from "../packages/shared/src/index";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  adaptToolInteractionEvent,
  buildTechniqueExposureFact,
  buildWorkEpisodeFact,
} from "../packages/collector-cli/src/learning-facts";
import { OutcomeTimelineStore } from "../packages/collector-cli/src/outcome-timeline-store";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import {
  allocateEvents,
  collectAllocationEvents,
  type PullCandidate,
} from "./event-allocation";
import {
  SYSTEM_E2E_SCHEMA,
  SYSTEM_E2E_BUDGETS,
  digest,
  loadSupportContract,
  loadRootGuardContract,
  parseSupportingArtifact,
  supportContractPath,
  rootGuardContractPath,
  type SupportingKind,
} from "./system-e2e/contract";

type PhaseReceipt = {
  schema: "plimsoll.system-e2e-phase-receipt.v1";
  name: string;
  status: "pass";
  expectedFlowFingerprint: string;
  sourceHeadCommit: string;
  testedTreeCommit: string;
  artifact: unknown;
  artifactDigest: string;
  semanticDigest: string;
  outputDigest: string;
  measurements: {
    wallMs: number;
    cpuMs: number;
    maxRssBytes: number;
    blockInputOperations: number;
    blockOutputOperations: number;
    capturedOutputBytes: number;
  };
};

const SCHEMA = SYSTEM_E2E_SCHEMA;
const repoRoot = path.resolve(import.meta.dirname, "..");
const tsx = path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs");
const proofRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-system-e2e-"));
const evidenceRoot = path.join(proofRoot, "evidence");
const machineAHome = path.join(proofRoot, "machine-a", "home");
const machineBHome = path.join(proofRoot, "machine-b", "home");
const machineATmp = path.join(proofRoot, "machine-a", "tmp");
const machineBTmp = path.join(proofRoot, "machine-b", "tmp");

for (const directory of [evidenceRoot, machineAHome, machineBHome, machineATmp, machineBTmp]) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
}

type RootGuard = {
  label: string;
  root: string;
  beforeDigest: string;
  beforeEntries: number;
  mode: number;
};
let activeRootGuards: RootGuard[] = [];

function guardedTree(root: string) {
  const rows: Array<{ relative: string; kind: "file" | "directory"; mode: number; contentDigest?: string }> = [];
  const visit = (directory: string) => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const absolute = path.join(directory, entry.name);
      const relative = path.relative(root, absolute);
      const stat = fs.lstatSync(absolute);
      assert.ok(!stat.isSymbolicLink(), `guarded root ${root} contains a symlink`);
      if (entry.isDirectory()) {
        rows.push({ relative, kind: "directory", mode: stat.mode & 0o777 });
        visit(absolute);
      } else {
        assert.ok(entry.isFile(), `guarded root ${root} contains an unsupported entry`);
        rows.push({
          relative,
          kind: "file",
          mode: stat.mode & 0o777,
          contentDigest: digest(fs.readFileSync(absolute).toString("base64")),
        });
      }
    }
  };
  visit(root);
  return { digest: digest(rows), entries: rows.length };
}

function prepareRootGuards(): RootGuard[] {
  const definitions = [
    ["machine_a_codex_skills", path.join(machineAHome, ".codex", "skills")],
    ["machine_a_codex_memories", path.join(machineAHome, ".codex", "memories")],
    ["machine_a_claude_skills", path.join(machineAHome, ".claude", "skills")],
    ["machine_b_codex_skills", path.join(machineBHome, ".codex", "skills")],
    ["machine_b_codex_memories", path.join(machineBHome, ".codex", "memories")],
    ["machine_b_claude_skills", path.join(machineBHome, ".claude", "skills")],
    ["operator_live_shadow", path.join(proofRoot, "forbidden", "operator-live-shadow")],
  ] as const;
  activeRootGuards = definitions.map(([label, root]) => {
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
    const sentinel = path.join(root, "write-deny.guard");
    fs.writeFileSync(sentinel, `${label}:source-only\n`, { mode: 0o400 });
    fs.chmodSync(sentinel, 0o400);
    fs.chmodSync(root, 0o500);
    const before = guardedTree(root);
    assert.ok(before.entries > 0, `${label} guard is empty`);
    const committedGuard = rootGuardContract.guards.find((guard) => guard.label === label);
    assert.ok(committedGuard, `${label} has no committed sentinel-tree contract`);
    assert.equal(
      before.digest,
      committedGuard.expectedSentinelTreeDigest,
      `${label} sentinel-tree digest does not match committed contract; actual=${before.digest}`,
    );
    return { label, root, beforeDigest: before.digest, beforeEntries: before.entries, mode: 0o500 };
  });
  return activeRootGuards;
}

function finalizeRootGuards(guards: RootGuard[]) {
  return guards.map((guard) => {
    const after = guardedTree(guard.root);
    const mode = fs.lstatSync(guard.root).mode & 0o777;
    assert.equal(mode, guard.mode, `${guard.label} write-deny mode changed`);
    assert.equal(after.entries, guard.beforeEntries, `${guard.label} entry count changed`);
    assert.equal(after.digest, guard.beforeDigest, `${guard.label} content changed`);
    return {
      label: guard.label,
      beforeDigest: guard.beforeDigest,
      afterDigest: after.digest,
      entriesBefore: guard.beforeEntries,
      entriesAfter: after.entries,
      writeDenyMode: mode,
      unchanged: true,
    };
  });
}

function releaseRootGuards() {
  for (const guard of activeRootGuards) {
    if (fs.existsSync(guard.root)) fs.chmodSync(guard.root, 0o700);
  }
  activeRootGuards = [];
}

const BUDGETS = SYSTEM_E2E_BUDGETS;
const supportContract = loadSupportContract(supportContractPath(repoRoot));
const rootGuardContract = loadRootGuardContract(rootGuardContractPath(repoRoot));

const WORKSPACE_A = "10000000-0000-4000-8000-000000000001";
const WORKSPACE_B = "20000000-0000-4000-8000-000000000002";
const MACHINE_A = "machine-a-opaque";
const MACHINE_B = "machine-b-opaque";
const SESSION_A = "system-e2e-session-a";
const SESSION_B = "system-e2e-session-b";
const REPO_A = `sha256:${"a".repeat(64)}`;
const REPO_B = `sha256:${"b".repeat(64)}`;
const BRANCH_A = `sha256:${"c".repeat(64)}`;
const BRANCH_B = `sha256:${"d".repeat(64)}`;
const SHA_A1 = "1".repeat(40);
const SHA_A2 = "2".repeat(40);
const SHA_B1 = "3".repeat(40);
const MERGE_SHA = "4".repeat(40);
const REVERT_SHA = "5".repeat(40);
const EVENT_IDS = [
  "00000000-0000-4000-8000-000000000101",
  "00000000-0000-4000-8000-000000000102",
  "00000000-0000-4000-8000-000000000103",
  "00000000-0000-4000-8000-000000000104",
] as const;
const CONTROL_EVENT_ID = "00000000-0000-4000-8000-000000000201";
const FLOW_TIME = {
  start: "2026-07-01T10:00:00.000Z",
  exposure: "2026-07-01T10:00:30.000Z",
  attempt1: "2026-07-01T10:01:00.000Z",
  attempt1Result: "2026-07-01T10:02:00.000Z",
  attempt2: "2026-07-01T10:03:00.000Z",
  attempt2Result: "2026-07-01T10:04:00.000Z",
  end: "2026-07-01T10:30:00.000Z",
} as const;

const flowFingerprint = digest({
  schema: SCHEMA,
  machines: [MACHINE_A, MACHINE_B],
  workspaces: [WORKSPACE_A, WORKSPACE_B],
  sessions: [SESSION_A, SESSION_B],
  eventIds: [...EVENT_IDS, CONTROL_EVENT_ID],
  pulls: [`${REPO_A}#101`, `${REPO_B}#102`],
});

function parseTimeMetric(stderr: string, label: string) {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const valueFirst = stderr.match(new RegExp(`^\\s*([0-9.]+)\\s+${escaped}\\s*$`, "m"));
  if (valueFirst) return Number(valueFirst[1]);
  const labelFirst = stderr.match(new RegExp(`^\\s*${escaped}\\s+([0-9.]+)\\s*$`, "m"));
  return labelFirst ? Number(labelFirst[1]) : 0;
}

function isolatedEnvironment(home: string, temp: string) {
  return {
    HOME: home,
    PLIMSOLL_HOME: path.join(home, ".plimsoll"),
    TMPDIR: temp,
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    SHELL: "/bin/zsh",
    LANG: "C.UTF-8",
    LC_ALL: "C.UTF-8",
    USER: "plimsoll-e2e",
    LOGNAME: "plimsoll-e2e",
    TERM: "dumb",
    CI: "1",
    NO_COLOR: "1",
    PLIMSOLL_SYSTEM_E2E_FINGERPRINT: flowFingerprint,
  } satisfies NodeJS.ProcessEnv;
}

function runSupportingProof(options: {
  name: string;
  kind: SupportingKind;
  script: string;
  args?: string[];
  home: string;
  temp: string;
  requiredAssertions: string[];
  receipt?: string;
  sourceHeadCommit: string;
  testedTreeCommit: string;
}): PhaseReceipt {
  const result = spawnSync(
    "/usr/bin/time",
    ["-lp", process.execPath, tsx, path.join(repoRoot, options.script), ...(options.args ?? [])],
    {
      cwd: repoRoot,
      env: isolatedEnvironment(options.home, options.temp),
      encoding: "utf8",
      maxBuffer: 12 * 1024 * 1024,
      timeout: 90_000,
    },
  );
  assert.equal(result.error, undefined, `${options.name} could not start: ${String(result.error)}`);
  assert.equal(result.signal, null, `${options.name} terminated by ${String(result.signal)}`);
  assert.equal(result.status, 0, `${options.name} failed: ${result.stderr.slice(-2_000)}`);
  for (const assertionName of options.requiredAssertions) {
    assert.ok(
      result.stdout.includes(assertionName),
      `${options.name} did not prove required assertion ${assertionName}`,
    );
  }

  const wallMs = Math.round(parseTimeMetric(result.stderr, "real") * 1_000);
  const cpuMs = Math.round(
    (parseTimeMetric(result.stderr, "user") + parseTimeMetric(result.stderr, "sys")) * 1_000,
  );
  const maxRssBytes = parseTimeMetric(result.stderr, "maximum resident set size");
  const blockInputOperations = parseTimeMetric(result.stderr, "block input operations");
  const blockOutputOperations = parseTimeMetric(result.stderr, "block output operations");
  const capturedOutputBytes = Buffer.byteLength(result.stdout) + Buffer.byteLength(result.stderr);
  assert.ok(wallMs <= 90_000, `${options.name} exceeded its 90s wall budget`);
  assert.ok(maxRssBytes <= BUDGETS.maxRssBytes, `${options.name} exceeded RSS budget`);
  assert.ok(capturedOutputBytes <= 12 * 1024 * 1024, `${options.name} exceeded output budget`);
  if (options.receipt) {
    assert.ok(fs.existsSync(options.receipt), `${options.name} did not write its selected receipt`);
  }

  const artifact = parseSupportingArtifact(
    options.kind,
    result.stdout,
    {
      baseDirectory: repoRoot,
      roots: [
        { label: "repository", absolutePath: repoRoot },
        { label: "proof", absolutePath: proofRoot },
        { label: "machine-home", absolutePath: options.home },
        { label: "machine-temp", absolutePath: options.temp },
        { label: "node-runtime", absolutePath: path.dirname(process.execPath) },
      ],
    },
    options.receipt,
  );
  const artifactDigest = digest(artifact);
  const phaseContract = supportContract.phases.find((phase) => phase.name === options.name);
  assert.ok(phaseContract, `${options.name} has no committed support contract`);
  assert.equal(phaseContract.kind, options.kind, `${options.name} support kind drifted`);
  assert.equal(
    artifactDigest,
    phaseContract.expectedArtifactDigest,
    `${options.name} actual artifact digest drifted; actual=${artifactDigest}`,
  );
  const semanticDigest = digest({
    name: options.name,
    status: "pass",
    expectedFlowFingerprint: flowFingerprint,
    sourceHeadCommit: options.sourceHeadCommit,
    testedTreeCommit: options.testedTreeCommit,
    artifactDigest,
    artifact,
  });
  const deterministicPhase = {
    schema: "plimsoll.system-e2e-phase-receipt.v1" as const,
    name: options.name,
    status: "pass" as const,
    expectedFlowFingerprint: flowFingerprint,
    sourceHeadCommit: options.sourceHeadCommit,
    testedTreeCommit: options.testedTreeCommit,
    artifact,
    artifactDigest,
    semanticDigest,
  };
  const phaseReceipt: PhaseReceipt = {
    ...deterministicPhase,
    outputDigest: digest(deterministicPhase),
    measurements: {
      wallMs,
      cpuMs,
      maxRssBytes,
      blockInputOperations,
      blockOutputOperations,
      capturedOutputBytes,
    },
  };
  const phasePath = path.join(evidenceRoot, "phases", `${options.name}.json`);
  fs.mkdirSync(path.dirname(phasePath), { recursive: true, mode: 0o700 });
  fs.writeFileSync(phasePath, `${JSON.stringify(phaseReceipt, null, 2)}\n`, { mode: 0o600 });
  assert.deepEqual(
    JSON.parse(fs.readFileSync(phasePath, "utf8")),
    phaseReceipt,
    `${options.name} structured phase receipt did not round trip`,
  );
  return phaseReceipt;
}

function gitHead() {
  const result = spawnSync("/usr/bin/git", ["rev-parse", "HEAD"], {
    cwd: repoRoot,
    encoding: "utf8",
    env: isolatedEnvironment(machineAHome, machineATmp),
  });
  assert.equal(result.status, 0, "could not read source commit");
  return result.stdout.trim();
}

function commitIsAncestor(ancestor: string, descendant: string) {
  const result = spawnSync("/usr/bin/git", ["merge-base", "--is-ancestor", ancestor, descendant], {
    cwd: repoRoot,
    encoding: "utf8",
    env: isolatedEnvironment(machineAHome, machineATmp),
  });
  return result.status === 0;
}

function optionValue(name: string) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

function event(input: {
  id: string;
  eventType: "tool_use" | "tool_result";
  observedAt: string;
  actionClass: "shell" | "test";
  inputTokens: number;
  outputTokens: number;
  costUsd?: number;
  projectKey?: string;
  git?: { remoteUrlHash: string; branchHash: string; headSha: string };
}) {
  return aiInteractionEventSchema.parse({
    id: input.id,
    sessionId: SESSION_A,
    actorId: MACHINE_A,
    source: "codex",
    dataMode: "metadata",
    eventType: input.eventType,
    observedAt: input.observedAt,
    actionClass: input.actionClass,
    inputTokens: input.inputTokens,
    outputTokens: input.outputTokens,
    ...(input.costUsd === undefined ? {} : { costUsd: input.costUsd }),
    ...(input.projectKey ? { projectKey: input.projectKey } : {}),
    metadata: input.git ? { git: input.git } : {},
  });
}

function requestIds(init?: RequestInit) {
  const parsed = JSON.parse(String(init?.body ?? "{}")) as {
    events?: Array<{ event?: { id?: string } }>;
  };
  return (parsed.events ?? []).flatMap((row) => (row.event?.id ? [row.event.id] : []));
}

async function runSharedFlow() {
  const machineALedger = path.join(machineAHome, ".plimsoll", "work-ledger.sqlite");
  const machineBLedger = path.join(machineBHome, ".plimsoll", "work-ledger.sqlite");
  const timelineLedger = path.join(machineAHome, ".plimsoll", "outcome-timeline.sqlite");
  fs.mkdirSync(path.dirname(machineALedger), { recursive: true, mode: 0o700 });
  fs.mkdirSync(path.dirname(machineBLedger), { recursive: true, mode: 0o700 });

  const config = collectorConfigSchema.parse({
    managed: true,
    uploadUrl: "http://127.0.0.1:1/api/work-intelligence/ingest",
    tenantId: WORKSPACE_A,
    installKey: "source-e2e-install-a",
    delivery: {
      maxOldestAgeDays: 3650,
      maxBackoffSeconds: 30,
      requestTimeoutSeconds: 1,
      maxProbesPerCycle: 31,
    },
  });
  const bufferA = new LocalEventBuffer(machineALedger, {
    delivery: { enabled: true, limits: config.delivery },
    workspaceId: WORKSPACE_A,
  });
  const bufferB = new LocalEventBuffer(machineBLedger, { workspaceId: WORKSPACE_B });
  const timelineStore = new OutcomeTimelineStore(timelineLedger);
  const sqliteChanges = () => {
    const count = (database: { prepare(sql: string): { get(): unknown } }) =>
      (database.prepare(`select total_changes() as n`).get() as { n: number }).n;
    return count(bufferA.database) + count(bufferB.database) + count(timelineStore.database);
  };
  const sqliteChangesBefore = sqliteChanges();

  try {
    const events = [
      event({
        id: EVENT_IDS[0],
        eventType: "tool_use",
        observedAt: FLOW_TIME.attempt1,
        actionClass: "shell",
        inputTokens: 35,
        outputTokens: 5,
        costUsd: 0.04,
        projectKey: REPO_A,
        git: { remoteUrlHash: REPO_A, branchHash: BRANCH_A, headSha: SHA_A1 },
      }),
      event({
        id: EVENT_IDS[1],
        eventType: "tool_result",
        observedAt: FLOW_TIME.attempt1Result,
        actionClass: "shell",
        inputTokens: 8,
        outputTokens: 2,
        costUsd: 0.01,
      }),
      event({
        id: EVENT_IDS[2],
        eventType: "tool_use",
        observedAt: FLOW_TIME.attempt2,
        actionClass: "test",
        inputTokens: 25,
        outputTokens: 5,
        costUsd: 0.03,
        projectKey: REPO_B,
        git: { remoteUrlHash: REPO_B, branchHash: BRANCH_B, headSha: SHA_B1 },
      }),
      event({
        id: EVENT_IDS[3],
        eventType: "tool_result",
        observedAt: FLOW_TIME.attempt2Result,
        actionClass: "test",
        inputTokens: 15,
        outputTokens: 5,
        projectKey: REPO_B,
        git: { remoteUrlHash: REPO_B, branchHash: BRANCH_B, headSha: SHA_B1 },
      }),
    ];
    for (const captured of events) bufferA.append(captured);
    bufferB.append(aiInteractionEventSchema.parse({
      id: CONTROL_EVENT_ID,
      sessionId: SESSION_B,
      actorId: MACHINE_B,
      source: "codex",
      dataMode: "metadata",
      eventType: "assistant_response",
      observedAt: FLOW_TIME.attempt2Result,
      actionClass: "other",
      inputTokens: 0,
      outputTokens: 0,
      projectKey: REPO_A,
      metadata: { machineFixture: MACHINE_B },
    }));
    const workspaceARow = bufferA.database
      .prepare(`select workspace_id as workspaceId from buffered_events where id = ?`)
      .get(EVENT_IDS[0]) as { workspaceId: string };
    const workspaceBRow = bufferB.database
      .prepare(`select workspace_id as workspaceId from buffered_events where id = ?`)
      .get(CONTROL_EVENT_ID) as { workspaceId: string };
    assert.equal(workspaceARow.workspaceId, WORKSPACE_A);
    assert.equal(workspaceBRow.workspaceId, WORKSPACE_B);
    assert.notEqual(workspaceARow.workspaceId, workspaceBRow.workspaceId);
    assert.notEqual(path.dirname(machineALedger), path.dirname(machineBLedger));

    const firstRequests: string[][] = [];
    let offlineFailure = false;
    try {
      await uploadBufferedEvents(config, bufferA, {
        now: () => new Date("2026-08-01T11:00:00.000Z"),
        fetchImpl: async (_input, init) => {
          firstRequests.push(requestIds(init));
          throw new Error("injected offline transport");
        },
      });
    } catch {
      offlineFailure = true;
    }
    assert.equal(offlineFailure, true, "offline upload must remain retryable");
    assert.equal(firstRequests.length, 1, "offline cycle must issue one bounded request");

    const accepted: string[] = [];
    const reconnectRequests: string[][] = [];
    const poisonId = EVENT_IDS[1];
    const reconnect = await uploadBufferedEvents(config, bufferA, {
      now: () => new Date("2026-08-01T11:10:00.000Z"),
      fetchImpl: async (_input, init) => {
        const ids = requestIds(init);
        reconnectRequests.push(ids);
        if (ids.includes(poisonId)) {
          return new Response(JSON.stringify({ code: "fixture_validation" }), {
            status: 422,
            headers: { "content-type": "application/json" },
          });
        }
        accepted.push(...ids);
        return new Response(JSON.stringify({ accepted: ids.length }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      },
    });
    const deliveryStatus = bufferA.delivery.status(new Date("2026-08-01T11:10:01.000Z"));
    assert.equal(reconnect.uploadedEvents, 3, "three valid rows must reconnect exactly once");
    assert.equal(deliveryStatus.receipts.dead, 1, "poison row must have one terminal receipt");
    assert.equal(deliveryStatus.remainingDelivery, 0, "poison must not starve valid rows");
    assert.equal(new Set(accepted).size, 3, "valid rows must be accepted exactly once");
    assert.equal(accepted.length, 3, "no valid row may be replayed during reconnect");
    assert.ok(!accepted.includes(poisonId), "poison row must never be accepted");
    const expectedAcceptedIds = [EVENT_IDS[0], EVENT_IDS[2], EVENT_IDS[3]].sort();
    assert.deepEqual([...accepted].sort(), expectedAcceptedIds, "accepted IDs must be exactly the three valid machine-A IDs");
    assert.ok(!accepted.includes(CONTROL_EVENT_ID), "no machine-B event may cross the machine-A outbox");

    const candidates: PullCandidate[] = [
      {
        pull: 101,
        repoHash: REPO_A,
        branchHash: BRANCH_A,
        headSha: SHA_A1,
        commitShas: [SHA_A1, SHA_A2],
        createdAt: "2026-07-01T09:00:00.000Z",
        updatedAt: "2026-07-02T12:00:00.000Z",
        mergedAt: "2026-07-01T10:12:00.000Z",
      },
      {
        pull: 102,
        repoHash: REPO_B,
        branchHash: BRANCH_B,
        headSha: SHA_B1,
        commitShas: [SHA_B1],
        createdAt: "2026-07-01T09:00:00.000Z",
        updatedAt: "2026-07-02T12:00:00.000Z",
        mergedAt: "2026-07-01T10:15:00.000Z",
      },
    ];
    const allocationEvents = collectAllocationEvents(bufferA.database, FLOW_TIME.start);
    const allocation = allocateEvents(allocationEvents, candidates);
    const capturedPrimary =
      allocation.coverage.captured.inputTokens + allocation.coverage.captured.outputTokens;
    const allocatedPrimary = allocation.pullRows.reduce(
      (sum, row) => sum + row.inputTokens + row.outputTokens,
      0,
    );
    assert.equal(capturedPrimary, 100, "shared fixture must capture exactly 100 primary tokens");
    assert.equal(allocatedPrimary, 90, "unallocated effort must not be copied into PRs");
    assert.ok(allocatedPrimary <= capturedPrimary, "allocated effort cannot exceed captured effort");
    assert.equal(allocation.coverage.unallocated.inputTokens + allocation.coverage.unallocated.outputTokens, 10);
    assert.equal(allocation.coverage.captured.unpricedEvents, 1, "unpriced effort stays explicit");
    assert.equal(allocation.coverage.captured.costStatus, "partial", "unknown cost cannot become zero");
    assert.equal(allocation.coverage.reconciliation.exact, true);
    assert.equal(new Set(allocation.pullRows.map((row) => row.repoHash)).size, 2);
    assert.equal(allocation.pullRows.length, 2);
    assert.deepEqual(
      allocation.receipts.map((row) => row.eventId),
      [...EVENT_IDS],
      "allocation must preserve every capture id in canonical order",
    );

    const base = {
      schemaVersion: 1 as const,
      repositoryExternalId: REPO_A,
      pullExternalId: "pull-101",
      pullNumber: 101,
    };
    const facts: PullTimelineFact[] = [
      { ...base, kind: "pull", externalId: "pull-101-open", createdAt: "2026-07-01T09:00:00.000Z" },
      { ...base, kind: "pull_revision", externalId: "revision-a1", sha: SHA_A1, committedAt: "2026-07-01T09:30:00.000Z" },
      { ...base, kind: "check_attempt", externalId: "check-a1", checkRunExternalId: "check-run-a1", sha: SHA_A1, name: "ci", conclusion: "failure", startedAt: "2026-07-01T10:01:00.000Z", completedAt: "2026-07-01T10:02:00.000Z" },
      { ...base, kind: "review_outcome", externalId: "review-request", reviewExternalId: "review-request", sha: SHA_A1, outcome: "changes_requested", submittedAt: "2026-07-01T10:02:30.000Z" },
      { ...base, kind: "pull_revision", externalId: "revision-a2", sha: SHA_A2, committedAt: "2026-07-01T10:03:00.000Z" },
      { ...base, kind: "check_attempt", externalId: "check-a2", checkRunExternalId: "check-run-a2", sha: SHA_A2, name: "ci", conclusion: "success", startedAt: "2026-07-01T10:03:00.000Z", completedAt: "2026-07-01T10:04:00.000Z" },
      { ...base, kind: "review_outcome", externalId: "review-approval", reviewExternalId: "review-approval", sha: SHA_A2, outcome: "approved", submittedAt: "2026-07-01T10:05:00.000Z" },
      { ...base, kind: "merge", externalId: "merge-101", mergeSha: MERGE_SHA, mergedAt: "2026-07-01T10:12:00.000Z" },
      { ...base, kind: "revert", externalId: "revert-101", revertSha: REVERT_SHA, revertedSha: MERGE_SHA, revertedAt: "2026-07-02T10:12:00.000Z", evidence: { source: "commit_message_full_sha", matchedFullSha: MERGE_SHA } },
    ];
    const coverage = [{ runId: "system-e2e", repositoryExternalId: REPO_A, pullExternalId: "pull-101", dimension: "checks" as const, status: "complete" as const, reason: "complete" as const }];
    timelineStore.appendFacts(facts, "2026-07-03T12:00:00.000Z");
    timelineStore.recordCoverage(coverage, "2026-07-03T12:00:00.000Z");
    const persistedTimelineFacts = timelineStore.facts(REPO_A);
    const persistedTimelineCoverage = timelineStore.coverage("system-e2e");
    assert.equal(persistedTimelineFacts.length, facts.length);
    assert.equal(persistedTimelineCoverage.length, coverage.length);
    const outcome = derivePullOutcomeTimeline({
      facts: persistedTimelineFacts,
      coverage: persistedTimelineCoverage,
      requiredChecks: { names: ["ci"] },
      reworkWindowDays: 14,
    })[0];
    assert.equal(outcome.pullNumber, allocation.pullRows.find((row) => row.pull === 101)?.pull);
    assert.equal(outcome.coverage, "complete");
    assert.equal(outcome.firstPassSuccess, false, "failed first revision stays visible");
    assert.equal(outcome.correctionLoops?.length, 1, "failed commit to green commit is a correction");
    assert.equal(outcome.reviewCorrections.length, 1, "review correction lineage is immutable");
    assert.equal(outcome.rework.filter((row) => row.inWindow).length, 1, "revert remains visible as rework");

    const episodeA = buildWorkEpisodeFact({
      source: "codex",
      sessionId: SESSION_A,
      sourceEpisodeKey: "shared-flow-treatment",
      workClass: "implementation",
      complexityBand: "medium",
      startedAt: FLOW_TIME.start,
      endedAt: FLOW_TIME.end,
    });
    const episodeB = buildWorkEpisodeFact({
      source: "codex",
      sessionId: SESSION_B,
      sourceEpisodeKey: "shared-flow-control",
      workClass: "implementation",
      complexityBand: "medium",
      startedAt: FLOW_TIME.start,
      endedAt: FLOW_TIME.end,
    });
    bufferA.learningFacts.recordWorkEpisode(episodeA);
    bufferB.learningFacts.recordWorkEpisode(episodeB);
    const exposureA = buildTechniqueExposureFact({
      episodeId: episodeA.episodeId,
      techniqueId: "bounded-retry-strategy",
      techniqueVersion: "1.0.0",
      contentDigest: `sha256:${"6".repeat(64)}`,
      assignmentId: "system-e2e-pair",
      workClass: "implementation",
      complexityBand: "medium",
      exposedAt: FLOW_TIME.exposure,
      mode: "treatment",
    });
    const exposureB = buildTechniqueExposureFact({
      episodeId: episodeB.episodeId,
      techniqueId: "bounded-retry-strategy",
      techniqueVersion: "1.0.0",
      contentDigest: `sha256:${"6".repeat(64)}`,
      assignmentId: "system-e2e-pair",
      workClass: "implementation",
      complexityBand: "medium",
      exposedAt: FLOW_TIME.exposure,
      mode: "control",
    });
    bufferA.learningFacts.recordTechniqueExposure(exposureA, { outcomeObservedAt: "2026-07-02T12:00:00.000Z" });
    bufferB.learningFacts.recordTechniqueExposure(exposureB, { outcomeObservedAt: "2026-07-02T12:00:00.000Z" });

    const startOne = adaptToolInteractionEvent({ event: events[0], sourceOperationKey: EVENT_IDS[0], episodeId: episodeA.episodeId });
    const resultOne = adaptToolInteractionEvent({ event: events[1], sourceOperationKey: EVENT_IDS[0], episodeId: episodeA.episodeId, resultStatus: "failure", errorCategory: "validation" });
    const startTwo = adaptToolInteractionEvent({ event: events[2], sourceOperationKey: EVENT_IDS[2], retryOfSourceOperationKey: EVENT_IDS[0], episodeId: episodeA.episodeId });
    const resultTwo = adaptToolInteractionEvent({ event: events[3], sourceOperationKey: EVENT_IDS[2], episodeId: episodeA.episodeId, resultStatus: "success" });
    for (const signal of [startOne, resultOne, startTwo, resultTwo]) {
      bufferA.learningFacts.recordToolSignal(signal);
    }
    const attempts = bufferA.learningFacts.attempts();
    assert.equal(attempts.length, 2);
    assert.equal(attempts[0]?.resultStatus, "failure");
    assert.equal(attempts[1]?.retryOf, attempts[0]?.operationId);
    assert.equal(attempts[1]?.resultStatus, "success");
    assert.equal(bufferA.learningFacts.exposures()[0]?.episodeId, episodeA.episodeId);

    const persistedEpisodes = [
      ...bufferA.learningFacts.episodes(),
      ...bufferB.learningFacts.episodes(),
    ];
    const persistedExposures = [
      ...bufferA.learningFacts.exposures(),
      ...bufferB.learningFacts.exposures(),
    ];
    const persistedEpisodeA = persistedEpisodes.find((row) => row.episodeId === episodeA.episodeId);
    const persistedEpisodeB = persistedEpisodes.find((row) => row.episodeId === episodeB.episodeId);
    assert.ok(persistedEpisodeA && persistedEpisodeB, "persisted episode rows are incomplete");
    const persistedExposureA = persistedExposures.find((row) => row.episodeId === episodeA.episodeId);
    const persistedExposureB = persistedExposures.find((row) => row.episodeId === episodeB.episodeId);
    assert.ok(persistedExposureA && persistedExposureB);
    const cohort = {
      projectId: REPO_A,
      workType: "implementation" as const,
      complexityBand: "medium" as const,
      modelId: "model-source-e2e",
      toolVersion: "0.6.0",
      actorClusterId: "actor-cluster-source-e2e",
      repoClusterId: REPO_A,
      epochId: "epoch-2026-07",
    };
    const observation = (
      id: string,
      exposure: typeof persistedExposureA,
      value: number,
    ): LearningObservation => ({
      observationId: id,
      workStartedAt: "2026-07-01T10:01:00.000Z",
      outcomeObservedAt: "2026-07-02T12:00:00.000Z",
      cohort: structuredClone(cohort),
      exposure,
      outcome: {
        metricId: "first-pass-success",
        metricVersion: "1.0.0",
        unit: "boolean-point",
        direction: "higher_is_better",
        value,
      },
      attribution: { method: "direct", projectAllocation: "exact", coverage: 1 },
    });
    const pairs = [{
      pairId: "system-e2e-pair",
      exposed: observation("shared-treatment", persistedExposureA, outcome.firstPassSuccess ? 1 : 0),
      control: observation("shared-control", persistedExposureB, 1),
    }];
    const manifest: LearningEvidenceManifest = {
      schemaVersion: LEARNING_EVIDENCE_SCHEMA_VERSION,
      analysisVersion: LEARNING_ANALYSIS_VERSION,
      analysisId: "system-e2e-shared-lineage",
      source: {
        snapshotId: flowFingerprint,
        queryHash: flowFingerprint.replace("sha256:", ""),
        rowDigest: computeLearningPairDigest(pairs),
        declaredPairCount: pairs.length,
        sourceKind: "local_owned_aggregate",
      },
      metricVersions: {
        outcomeMetric: "1.0.0",
        techniqueExposure: "1.0.0",
        projectAllocation: "1.0.0",
      },
      outcomeContract: {
        metricId: "first-pass-success",
        metricVersion: "1.0.0",
        unit: "boolean-point",
        direction: "higher_is_better",
      },
      techniqueContract: {
        techniqueId: persistedExposureA.techniqueId,
        techniqueVersion: persistedExposureA.techniqueVersion ?? null,
        contentDigest: persistedExposureA.contentDigest ?? null,
      },
      window: { startInclusive: "2026-07-01T00:00:00.000Z", endExclusive: "2026-07-03T00:00:00.000Z" },
      asOf: "2026-07-03T12:00:00.000Z",
      hypothesisFamily: {
        familyId: "system-e2e-family",
        hypothesisIndex: 1,
        hypothesesTested: 1,
        selectionPolicy: "pre_registered",
        correction: "none",
        familyWiseAlpha: 0.05,
        registeredAt: "2026-06-30T00:00:00.000Z",
      },
      gates: {
        statisticalMinCompletePairs: 2,
        statisticalMinActorClusters: 3,
        statisticalMinRepoClusters: 3,
        privacyMinCompletePairs: 1,
        minimumAttributionCoverage: 0.8,
        maxAbsoluteOutcome: 1,
        maxPairs: BUDGETS.learningPairs,
        maxCounterexamples: 10,
        maxRuntimeMs: 2_000,
      },
      declaredConfounders: ["nonrandom_assignment"],
      pairs,
    };
    const evidence = compileLearningEvidencePacket(manifest);
    assert.equal(evidence.status, "computed");
    assert.equal(evidence.packet.skillCandidateReview.publicationAuthorized, false);
    assert.equal(evidence.packet.skillCandidateReview.installationAuthorized, false);
    assert.equal(evidence.packet.skillCandidateReview.containsExecutableInstructions, false);
    assert.equal(evidence.packet.prescriptiveClaim, false);
    assert.equal(evidence.packet.causalClaim, false);
    assert.equal(evidence.packet.attribution.unallocatedCount, 0);
    const unchanged = compileLearningEvidencePacket(manifest, {
      previousSourceFingerprint: evidence.sourceFingerprint,
    });
    assert.deepEqual(unchanged, {
      status: "unchanged",
      sourceFingerprint: evidence.sourceFingerprint,
      analysisWorkUnits: 0,
      packet: null,
    });

    const captureRows = [
      ...(bufferA.database.prepare(
        `select id as eventId, session_id as sessionId, account_hash as machineId,
           workspace_id as workspaceId, input_tokens as inputTokens,
           output_tokens as outputTokens, cost_usd as costUsd,
           repo_hash as repoHash, branch_hash as branchHash, head_sha as headSha
         from buffered_events order by id`,
      ).all() as Array<Record<string, unknown>>),
      ...(bufferB.database.prepare(
        `select id as eventId, session_id as sessionId, account_hash as machineId,
           workspace_id as workspaceId, input_tokens as inputTokens,
           output_tokens as outputTokens, cost_usd as costUsd,
           repo_hash as repoHash, branch_hash as branchHash, head_sha as headSha
         from buffered_events order by id`,
      ).all() as Array<Record<string, unknown>>),
    ].sort((left, right) => String(left.eventId).localeCompare(String(right.eventId)));
    const captureMaterial = { rows: captureRows };
    const capture = { ...captureMaterial, digest: digest(captureMaterial) };
    const deliveryMaterial = {
      offlineRequestEventIds: [...firstRequests[0]!].sort(),
      acceptedEventIds: [...accepted].sort(),
      poisonEventIds: [poisonId],
      reconnectRequestEventIds: reconnectRequests.map((ids) => [...ids].sort()),
      acknowledgedReceipts: deliveryStatus.receipts.acknowledged,
      deadReceipts: deliveryStatus.receipts.dead,
    };
    const delivery = { ...deliveryMaterial, digest: digest(deliveryMaterial) };
    const allocationMaterial = {
      receipts: allocation.receipts,
      pullRows: allocation.pullRows,
      coverage: allocation.coverage,
      capturedPrimaryTokens: capturedPrimary,
      allocatedPrimaryTokens: allocatedPrimary,
      unallocatedPrimaryTokens:
        allocation.coverage.unallocated.inputTokens + allocation.coverage.unallocated.outputTokens,
    };
    const allocationLineage = { ...allocationMaterial, digest: digest(allocationMaterial) };
    const outcomeMaterial = {
      facts: persistedTimelineFacts,
      coverage: persistedTimelineCoverage,
      requiredChecks: ["ci"],
      reworkWindowDays: 14,
      derived: outcome,
    };
    const outcomeLineage = { ...outcomeMaterial, digest: digest(outcomeMaterial) };
    const learningFactMaterial = {
      episodeBindings: [
        { sourceEpisodeKey: "shared-flow-treatment", fact: persistedEpisodeA },
        { sourceEpisodeKey: "shared-flow-control", fact: persistedEpisodeB },
      ],
      attemptEventBindings: [
        { eventId: EVENT_IDS[0], sourceOperationKey: EVENT_IDS[0], signal: "start", operationId: attempts[0]!.operationId },
        { eventId: EVENT_IDS[1], sourceOperationKey: EVENT_IDS[0], signal: "result", operationId: attempts[0]!.operationId },
        { eventId: EVENT_IDS[2], sourceOperationKey: EVENT_IDS[2], signal: "start", operationId: attempts[1]!.operationId },
        { eventId: EVENT_IDS[3], sourceOperationKey: EVENT_IDS[2], signal: "result", operationId: attempts[1]!.operationId },
      ],
      attempts,
      exposures: persistedExposures,
    };
    const learningFacts = { ...learningFactMaterial, digest: digest(learningFactMaterial) };
    const evidenceMaterial = {
      manifest,
      sourceFingerprint: evidence.sourceFingerprint,
      packetFingerprint: evidence.packet.packetFingerprint,
      claimClass: evidence.packet.claimClass,
      causalClaim: evidence.packet.causalClaim,
      prescriptiveClaim: evidence.packet.prescriptiveClaim,
      skillPublicationAuthorized: evidence.packet.skillCandidateReview.publicationAuthorized,
      skillInstallationAuthorized: evidence.packet.skillCandidateReview.installationAuthorized,
      analysisWorkUnits: evidence.analysisWorkUnits,
      unchangedAnalysisWorkUnits: unchanged.analysisWorkUnits,
    };
    const evidenceLineage = { ...evidenceMaterial, digest: digest(evidenceMaterial) };
    const sqliteWriteChanges = sqliteChanges() - sqliteChangesBefore;
    const rowWork = {
      captureRowsRead: captureRows.length,
      allocationRowsRead: allocationEvents.length,
      timelineFactRowsRead: persistedTimelineFacts.length,
      timelineCoverageRowsRead: persistedTimelineCoverage.length,
      attemptRowsRead: attempts.length,
      episodeRowsRead: persistedEpisodes.length,
      exposureRowsRead: persistedExposures.length,
      learningPairRowsAnalyzed: evidence.analysisWorkUnits,
      sqliteWriteChanges,
    };
    const directRowOperations = Object.values(rowWork).reduce((sum, value) => sum + value, 0);
    assert.ok(sqliteWriteChanges > 0, "actual SQLite write work must be nonzero");
    assert.ok(
      directRowOperations > 0 && directRowOperations <= BUDGETS.directRows,
      `direct row operations ${directRowOperations} exceeded budget ${BUDGETS.directRows}`,
    );
    const identity = {
      machines: [MACHINE_A, MACHINE_B],
      workspaces: [WORKSPACE_A, WORKSPACE_B],
      sessions: [SESSION_A, SESSION_B],
      eventIds: [...EVENT_IDS, CONTROL_EVENT_ID],
      pulls: [`${REPO_A}#101`, `${REPO_B}#102`],
    };
    const lineage = {
      identity,
      capture,
      delivery,
      allocation: allocationLineage,
      outcome: outcomeLineage,
      learningFacts,
      evidence: evidenceLineage,
      rowWork,
    };
    return {
      status: "pass" as const,
      inputFingerprint: flowFingerprint,
      lineage,
      outputDigest: digest(lineage),
      measurements: {
        machineRoots: 2,
        workspaceIdentities: 2,
        memberRegistryCoverage: "not_run_requires_hosted_authorization" as const,
        capturedRows: captureRows.length,
        learningFactRows: attempts.length + persistedEpisodes.length + persistedExposures.length,
        directRowOperations,
        capturedPrimaryTokens: capturedPrimary,
        allocatedPrimaryTokens: allocatedPrimary,
        unallocatedPrimaryTokens: 10,
        projectRepoCount: new Set(allocation.pullRows.map((row) => row.repoHash)).size,
        pullCount: allocation.pullRows.length,
        unpricedEvents: allocation.coverage.captured.unpricedEvents,
        costStatus: allocation.coverage.captured.costStatus,
        offlineRequests: firstRequests.length,
        reconnectRequests: reconnectRequests.length,
        acceptedExactlyOnce: accepted.length,
        poisonReceipts: deliveryStatus.receipts.dead,
        outcomeFailedFirstPass: outcome.firstPassSuccess === false,
        correctionLoops: outcome.correctionLoops?.length ?? 0,
        reviewCorrections: outcome.reviewCorrections.length,
        inWindowRework: outcome.rework.filter((row) => row.inWindow).length,
        attempts: attempts.length,
        retryLinks: attempts.filter((row) => row.retryOf).length,
        episodes: persistedEpisodes.length,
        exposures: persistedExposures.length,
        analysisWorkUnits: evidence.analysisWorkUnits,
        unchangedAnalysisWorkUnits: unchanged.analysisWorkUnits,
        autoSkillWrites: 0,
      },
    };
  } finally {
    timelineStore.close();
    bufferA.close();
    bufferB.close();
  }
}

async function main() {
  assert.equal(process.versions.node.split(".")[0], "22", "system E2E requires exact Node 22");
  const startedAt = Date.now();
  const usageBefore = process.resourceUsage();
  const testedTreeCommit = gitHead();
  const sourceHeadCommit = optionValue("--expected-source-commit") ?? testedTreeCommit;
  assert.match(sourceHeadCommit, /^[a-f0-9]{40}$/, "expected source head commit must be a full SHA");
  assert.ok(
    commitIsAncestor(sourceHeadCommit, testedTreeCommit),
    "expected source head commit is not an ancestor of the tested tree commit",
  );
  const rootGuards = prepareRootGuards();
  const sharedFlow = await runSharedFlow();

  const install = runSupportingProof({
    name: "install_doctor",
    kind: "json_result",
    script: "scripts/install-doctor-proof.ts",
    home: machineAHome,
    temp: machineATmp,
    requiredAssertions: [
      "source_installer_dry_run_succeeds",
      "source_installer_dry_run_creates_nothing",
      "blank_doctor_reports_not_installed",
      "packaged_doctor_reports_signal_verified_read_only",
      "proof_never_invokes_launchctl",
    ],
    sourceHeadCommit,
    testedTreeCommit,
  });
  const join = runSupportingProof({
    name: "transactional_join",
    kind: "json_result",
    script: "scripts/join-isolation-proof.ts",
    home: machineBHome,
    temp: machineBTmp,
    requiredAssertions: [
      "workspace_backlog_bound_and_post_activation_upload_isolated",
      "concurrent_join_loser_cannot_redeem_before_winner_releases_lock",
      "join_dry_run_rejected_before_token_network_or_mutation",
      "cli_stdin_keeps_secret_out_of_argv_and_output",
    ],
    sourceHeadCommit,
    testedTreeCommit,
  });
  const privacyReceipt = path.join(evidenceRoot, "privacy.json");
  const privacy = runSupportingProof({
    name: "metadata_only_privacy",
    kind: "line_summary_with_receipt",
    script: "scripts/privacy-mode-proof.ts",
    args: ["--receipt", privacyReceipt],
    receipt: privacyReceipt,
    home: machineAHome,
    temp: machineATmp,
    requiredAssertions: [
      "legacy_evidence_rows_are_quarantined_and_never_uploaded",
      "terminal_privacy_disposition_is_zero_across_every_export_and_dashboard_lane",
      "lease_export_ack_races_and_reopen_remain_terminal_and_local",
      "proof_receipt_contains_no_private_sentinel",
    ],
    sourceHeadCommit,
    testedTreeCommit,
  });
  const lifecycle = runSupportingProof({
    name: "canonical_lifecycle",
    kind: "line_summary",
    script: "scripts/lifecycle-proof.ts",
    home: machineBHome,
    temp: machineBTmp,
    requiredAssertions: [
      "update_completes_with_one_version_receipt",
      "health_failure_restores_runtime_config_database_service",
      "reopen_resumes_idempotently_to_verified",
      "explicit_rollback_uses_same_transaction_and_returns_to_v1",
      "uninstall_command_preview_discloses_retained_and_purge_only_snapshots",
      "purge_is_separate_exact_and_deletes_live_plus_snapshot_secret_copies",
    ],
    sourceHeadCommit,
    testedTreeCommit,
  });
  const resourceReceiptPath = path.join(evidenceRoot, "resource.json");
  const resource = runSupportingProof({
    name: "idle_dashboard_resources",
    kind: "json_receipt",
    script: "scripts/resource-proof/index.ts",
    args: ["--require-integrated", "--receipt", resourceReceiptPath],
    receipt: resourceReceiptPath,
    home: machineAHome,
    temp: machineATmp,
    requiredAssertions: [
      '"id": "no_change_constant_work"',
      '"id": "dashboard_projection_budget"',
      '"overall": "pass"',
    ],
    sourceHeadCommit,
    testedTreeCommit,
  });

  const resourceReceipt = JSON.parse(fs.readFileSync(resourceReceiptPath, "utf8")) as {
    overall: string;
    environment: { liveStateTouched: boolean; providerNetwork: string };
    scenarios: Array<{
      id: string;
      status: string;
      counters: Record<string, number>;
      measurements: Record<string, unknown>;
    }>;
  };
  assert.equal(resourceReceipt.overall, "pass");
  assert.equal(resourceReceipt.environment.liveStateTouched, false);
  assert.equal(resourceReceipt.environment.providerNetwork, "not-configured");
  const idle = resourceReceipt.scenarios.find((row) => row.id === "no_change_constant_work");
  const dashboard = resourceReceipt.scenarios.find((row) => row.id === "dashboard_projection_budget");
  assert.equal(idle?.status, "pass");
  // Only the two post-baseline generations are inserted. Replaying their
  // byte cursors is idempotent and must not inflate this durable write count.
  assert.equal(idle?.counters.rawEventWrites, 2);
  assert.equal(idle?.counters.rawEventRewrites, 0);
  assert.equal(idle?.counters.fullHistoryFileReads, 2_012);
  assert.equal(idle?.counters.filesOpened, 2_016);
  assert.ok((idle?.counters.fileBytesRead ?? 0) > 0);
  assert.equal(idle?.counters.overlappingJobs, 0);
  assert.equal(idle?.measurements.firstBootRecentOnly, true);
  assert.equal(idle?.measurements.oldContentReadsAtBoot, 0);
  assert.equal(idle?.measurements.restartZeroWork, true);
  assert.equal(idle?.measurements.appendedExactlyOnce, true);
  assert.equal(idle?.measurements.replayRolloutFilesRead, 1);
  assert.equal(idle?.measurements.replayTranscriptFilesRead, 1);
  assert.equal(idle?.measurements.replayEventsAppended, 0);
  assert.equal(idle?.measurements.replayRawEventWrites, 0);
  assert.equal(idle?.measurements.replayEventMutationsInserted, 0);
  assert.equal(idle?.measurements.inaccessibleItemsBlockPromotion, true);
  assert.equal(idle?.measurements.failedAttemptDisclosedAfterComplete, true);
  assert.equal(idle?.measurements.unchangedParseFailuresRetained, true);
  assert.equal(idle?.measurements.parseFailuresPersistAcrossRestart, true);
  assert.equal(idle?.measurements.repairedParseFailuresPromote, true);
  assert.equal(idle?.measurements.coveragePersistsAcrossRestart, true);
  assert.equal(dashboard?.status, "pass");
  assert.equal(dashboard?.counters.rawRowsScanned, 0);
  assert.equal(dashboard?.counters.filesOpened, 0);
  assert.equal(dashboard?.counters.fileBytesRead, 0);
  assert.equal(dashboard?.counters.filesystemEntriesScanned, 0);

  const phases = [install, join, privacy, lifecycle, resource];
  assert.equal(phases.length, supportContract.phases.length, "support phase count drifted");
  const rootGuardReceipts = finalizeRootGuards(rootGuards);
  assert.ok(rootGuardReceipts.length > 0 && rootGuardReceipts.every((guard) => guard.unchanged));
  const wallMs = Date.now() - startedAt;
  const usageAfter = process.resourceUsage();
  const phaseCpuMs = phases.reduce((sum, phase) => sum + phase.measurements.cpuMs, 0);
  const controllerCpuMs =
    (usageAfter.userCPUTime - usageBefore.userCPUTime +
      usageAfter.systemCPUTime - usageBefore.systemCPUTime) /
    1_000;
  const totalCpuMs = phaseCpuMs + controllerCpuMs;
  const phaseOutputBytes = phases.reduce(
    (sum, phase) => sum + phase.measurements.capturedOutputBytes,
    0,
  );
  const childBlockInputOperations = phases.reduce(
    (sum, phase) => sum + phase.measurements.blockInputOperations,
    0,
  );
  const childBlockOutputOperations = phases.reduce(
    (sum, phase) => sum + phase.measurements.blockOutputOperations,
    0,
  );
  const controllerBlockInputOperations = usageAfter.fsRead - usageBefore.fsRead;
  const controllerBlockOutputOperations = usageAfter.fsWrite - usageBefore.fsWrite;
  const blockOperations =
    childBlockInputOperations + childBlockOutputOperations +
    controllerBlockInputOperations + controllerBlockOutputOperations;
  const resourceRowCounterNames = [
    "eventsObserved",
    "eventsAdmitted",
    "eventsDropped",
    "rawEventWrites",
    "rawEventRewrites",
    "rawRowsScanned",
    "projectionRowsVisited",
    "projectionRowsWritten",
    "outboxRowsEnqueued",
    "outboxAttempts",
    "deadLettersWritten",
    "repriceRowsVisited",
    "reconciliationRowsVisited",
    "enrichmentRowsVisited",
    "learningFactRowsWritten",
  ] as const;
  const supportRowOperations = resourceReceipt.scenarios.reduce(
    (total, scenario) =>
      total + resourceRowCounterNames.reduce((sum, name) => sum + (scenario.counters[name] ?? 0), 0),
    0,
  );
  const directRowOperations = sharedFlow.measurements.directRowOperations;
  const totalRowOperations = directRowOperations + supportRowOperations;
  const controllerMaxRssBytes = usageAfter.maxRSS * 1_024;
  const maxRssBytes = Math.max(
    controllerMaxRssBytes,
    ...phases.map((phase) => phase.measurements.maxRssBytes),
  );
  assert.ok(totalCpuMs > 0, "observed parent plus child CPU must be nonzero");
  assert.ok(totalRowOperations > 0, "observed row work must be nonzero");
  assert.ok(wallMs <= BUDGETS.wallMs, "system E2E exceeded wall budget");
  assert.ok(totalCpuMs <= BUDGETS.cpuMs, "system E2E exceeded total parent plus child CPU budget");
  assert.ok(maxRssBytes <= BUDGETS.maxRssBytes, "system E2E exceeded RSS budget");
  assert.ok(blockOperations <= BUDGETS.blockOperations, "system E2E exceeded block-I/O budget");
  assert.ok(totalRowOperations <= BUDGETS.totalRowOperations, "system E2E exceeded row-work budget");
  assert.ok(phaseOutputBytes <= BUDGETS.capturedOutputBytes, "system E2E exceeded output budget");

  const deterministicPhases = phases.map(({ measurements: _volatile, ...phase }) => phase);
  const phaseChainDigest = digest(deterministicPhases);
  const measurements = {
    phases: phases.map((phase) => ({ name: phase.name, ...phase.measurements })),
    wallMs,
    cpu: {
      childMs: phaseCpuMs,
      controllerMs: controllerCpuMs,
      totalMs: totalCpuMs,
    },
    rss: { controllerMaxBytes: controllerMaxRssBytes, maxBytes: maxRssBytes },
    blockIo: {
      childInputOperations: childBlockInputOperations,
      childOutputOperations: childBlockOutputOperations,
      controllerInputOperations: controllerBlockInputOperations,
      controllerOutputOperations: controllerBlockOutputOperations,
      totalOperations: blockOperations,
    },
    rowWork: {
      directOperations: directRowOperations,
      supportOperations: supportRowOperations,
      totalOperations: totalRowOperations,
    },
    capturedOutputBytes: phaseOutputBytes,
    budgetMargins: {
      wallMs: BUDGETS.wallMs - wallMs,
      cpuMs: BUDGETS.cpuMs - totalCpuMs,
      rssBytes: BUDGETS.maxRssBytes - maxRssBytes,
      blockOperations: BUDGETS.blockOperations - blockOperations,
      rowOperations: BUDGETS.totalRowOperations - totalRowOperations,
      capturedOutputBytes: BUDGETS.capturedOutputBytes - phaseOutputBytes,
    },
    idle: {
      rawEventWrites: idle?.counters.rawEventWrites,
      rawEventRewrites: idle?.counters.rawEventRewrites,
      filesOpened: idle?.counters.filesOpened,
      fileBytesRead: idle?.counters.fileBytesRead,
      fullHistoryFileReads: idle?.counters.fullHistoryFileReads,
      overlappingJobs: idle?.counters.overlappingJobs,
    },
    dashboard: {
      rawRowsScanned: dashboard?.counters.rawRowsScanned,
      filesOpened: dashboard?.counters.filesOpened,
      fileBytesRead: dashboard?.counters.fileBytesRead,
      filesystemEntriesScanned: dashboard?.counters.filesystemEntriesScanned,
    },
  };
  const deterministicMaterial = {
    schema: SCHEMA,
    status: "pass",
    sourceHeadCommit,
    testedTreeCommit,
    nodeMajor: 22,
    isolation: {
      temporaryMachineRoots: 2,
      distinctMachineIdentities: 2,
      distinctWorkspaceIdentities: 2,
      credentialCopyOperations: 0,
      realLaunchAgentsTouched: 0,
      realProviderCalls: 0,
      liveLedgersOrConfigsTouched: 0,
      packagePublishes: 0,
      skillOrMemoryWrites: 0,
      rootGuards: rootGuardReceipts,
    },
    flow: {
      fingerprint: flowFingerprint,
      sharedFlow,
      phaseChain: deterministicPhases,
      phaseChainDigest,
    },
    budgets: BUDGETS,
    externalGates: [
      { gate: "hosted_two_member_registry", status: "not_run_requires_hosted_authorization" },
      { gate: "hosted_device_revocation", status: "not_run_requires_hosted_authorization" },
      { gate: "hosted_device_credential_rotation", status: "not_run_requires_hosted_authorization" },
      { gate: "real_mac_package_install_and_checksum", status: "not_run_requires_owner_authorization" },
      { gate: "first_real_token_sync_revoke_uninstall", status: "not_run_requires_owner_authorization" },
      { gate: "package_publish", status: "not_run_requires_owner_authorization" },
    ],
    contentPolicy: {
      rawContentIncluded: false,
      childOutputIncluded: false,
      executableSkillIncluded: false,
      privateSentinelIncluded: false,
    },
    liveStateTouched: false,
  };
  const receipt = {
    ...deterministicMaterial,
    measurements,
    volatileFieldsExcludedFromDeterministicDigest: [
      "measurements",
    ],
    deterministicDigest: digest(deterministicMaterial),
  };

  const serialized = `${JSON.stringify(receipt, null, 2)}\n`;
  assert.ok(!/(?:credential-sentinel|api[_-]?key["'=:\s]|bearer\s+[a-z0-9])/i.test(serialized));
  const compareArg = process.argv.indexOf("--compare-deterministic-receipt");
  if (compareArg >= 0) {
    const previousPath = path.resolve(process.argv[compareArg + 1] ?? "");
    assert.ok(fs.existsSync(previousPath), "comparison receipt is missing");
    const previous = JSON.parse(fs.readFileSync(previousPath, "utf8")) as {
      schema?: unknown;
      deterministicDigest?: unknown;
    };
    assert.equal(previous.schema, SCHEMA, "comparison receipt schema drifted");
    assert.equal(
      previous.deterministicDigest,
      receipt.deterministicDigest,
      "deterministic digest changed across isolated runs",
    );
  }
  const receiptArg = process.argv.indexOf("--receipt");
  const receiptPath = path.resolve(
    receiptArg >= 0 && process.argv[receiptArg + 1]
      ? process.argv[receiptArg + 1]!
      : path.join(repoRoot, "evidence", "system-e2e-proof.json"),
  );
  const liveHome = path.resolve(os.homedir(), ".plimsoll");
  assert.ok(!receiptPath.startsWith(`${liveHome}${path.sep}`), "receipt cannot target live Plimsoll home");
  fs.mkdirSync(path.dirname(receiptPath), { recursive: true });
  fs.writeFileSync(receiptPath, serialized, { mode: 0o600 });
  console.log(JSON.stringify({
    schema: receipt.schema,
    status: receipt.status,
    sourceHeadCommit: receipt.sourceHeadCommit,
    testedTreeCommit: receipt.testedTreeCommit,
    flowFingerprint,
    sharedPrimaryTokens: sharedFlow.measurements.capturedPrimaryTokens,
    phaseCount: phases.length,
    wallMs,
    externalGates: receipt.externalGates,
    receipt: path.relative(repoRoot, receiptPath),
    liveStateTouched: false,
  }, null, 2));
}

main()
  .catch((error) => {
    console.error(error instanceof Error ? error.stack ?? error.message : String(error));
    process.exitCode = 1;
  })
  .finally(() => {
    releaseRootGuards();
    fs.rmSync(proofRoot, { recursive: true, force: true });
  });
