import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";

import {
  compileLearningEvidencePacket,
  derivePullOutcomeTimeline,
  toolAttemptFactSchema,
  validateTechniqueExposureFactIdentity,
  workEpisodeFactSchema,
  type LearningEvidenceManifest,
  type PullTimelineFact,
} from "../../packages/shared/src/index";
import {
  buildWorkEpisodeFact,
  deterministicToolOperationId,
} from "../../packages/collector-cli/src/learning-facts";
import {
  SYSTEM_E2E_BUDGETS,
  SYSTEM_E2E_SCHEMA,
  digest,
  exactKeys,
  loadSupportContract,
  loadRootGuardContract,
  rootGuardContractPath,
  supportContractPath,
} from "./contract";

const EXPECTED = {
  machines: ["machine-a-opaque", "machine-b-opaque"],
  workspaces: [
    "10000000-0000-4000-8000-000000000001",
    "20000000-0000-4000-8000-000000000002",
  ],
  sessions: ["system-e2e-session-a", "system-e2e-session-b"],
  eventIds: [
    "00000000-0000-4000-8000-000000000101",
    "00000000-0000-4000-8000-000000000102",
    "00000000-0000-4000-8000-000000000103",
    "00000000-0000-4000-8000-000000000104",
    "00000000-0000-4000-8000-000000000201",
  ],
  pulls: [
    `sha256:${"a".repeat(64)}#101`,
    `sha256:${"b".repeat(64)}#102`,
  ],
} as const;

const RESOURCE_ROW_COUNTER_NAMES = [
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

function object(value: unknown, label: string): Record<string, unknown> {
  assert.ok(value && typeof value === "object" && !Array.isArray(value), `${label} must be an object`);
  return value as Record<string, unknown>;
}

function array(value: unknown, label: string): unknown[] {
  assert.ok(Array.isArray(value), `${label} must be an array`);
  return value;
}

function number(value: unknown, label: string): number {
  assert.ok(typeof value === "number" && Number.isFinite(value), `${label} must be finite`);
  return value;
}

function nonnegative(value: unknown, label: string): number {
  const parsed = number(value, label);
  assert.ok(parsed >= 0, `${label} must be nonnegative`);
  return parsed;
}

function integer(value: unknown, label: string): number {
  const parsed = nonnegative(value, label);
  assert.ok(Number.isSafeInteger(parsed), `${label} must be a safe integer`);
  return parsed;
}

function exactStage(
  value: unknown,
  materialKeys: readonly string[],
  label: string,
): Record<string, unknown> {
  exactKeys(value, [...materialKeys, "digest"], label);
  const material = Object.fromEntries(materialKeys.map((key) => [key, value[key]]));
  assert.equal(value.digest, digest(material), `${label} digest mismatch`);
  return value;
}

function sortedStrings(value: unknown, label: string) {
  const parsed = array(value, label).map((entry) => {
    assert.equal(typeof entry, "string", `${label} must contain strings`);
    return entry as string;
  });
  return [...parsed].sort();
}

function verifyFixedBudgets(receipt: Record<string, unknown>) {
  assert.deepEqual(receipt.budgets, SYSTEM_E2E_BUDGETS, "fixed system budgets changed");
}

function verifyMeasurements(
  receipt: Record<string, unknown>,
  sharedFlow: Record<string, unknown>,
  phases: Record<string, unknown>[],
) {
  const measurements = object(receipt.measurements, "measurements");
  exactKeys(
    measurements,
    ["phases", "wallMs", "cpu", "rss", "blockIo", "rowWork", "capturedOutputBytes", "budgetMargins", "idle", "dashboard"],
    "measurements",
  );
  const wallMs = nonnegative(measurements.wallMs, "wall measurement");
  assert.ok(wallMs <= SYSTEM_E2E_BUDGETS.wallMs, "wall budget exceeded");

  const cpu = object(measurements.cpu, "cpu measurements");
  exactKeys(cpu, ["childMs", "controllerMs", "totalMs"], "cpu measurements");
  const childCpu = nonnegative(cpu.childMs, "child CPU");
  const controllerCpu = nonnegative(cpu.controllerMs, "controller CPU");
  const totalCpu = nonnegative(cpu.totalMs, "total CPU");
  assert.ok(totalCpu > 0, "total CPU must be nonzero");
  assert.ok(totalCpu <= SYSTEM_E2E_BUDGETS.cpuMs, "CPU budget exceeded");
  assert.equal(totalCpu, childCpu + controllerCpu, "total CPU does not include child plus controller CPU");

  const rss = object(measurements.rss, "RSS measurements");
  exactKeys(rss, ["controllerMaxBytes", "maxBytes"], "RSS measurements");
  const controllerRss = nonnegative(rss.controllerMaxBytes, "controller RSS");
  const maxRss = nonnegative(rss.maxBytes, "maximum RSS");
  assert.ok(maxRss <= SYSTEM_E2E_BUDGETS.maxRssBytes, "RSS budget exceeded");

  const phaseMeasurements = array(measurements.phases, "phase measurements").map((entry, index) => {
    exactKeys(
      entry,
      ["name", "wallMs", "cpuMs", "maxRssBytes", "blockInputOperations", "blockOutputOperations", "capturedOutputBytes"],
      `phase measurement ${index}`,
    );
    return entry;
  });
  assert.equal(phaseMeasurements.length, phases.length, "phase measurement count mismatch");
  assert.deepEqual(
    phaseMeasurements.map((phase) => phase.name),
    phases.map((phase) => phase.name),
    "phase measurement order mismatch",
  );
  for (const [index, phase] of phaseMeasurements.entries()) {
    nonnegative(phase.wallMs, `phase ${index} wall`);
    nonnegative(phase.cpuMs, `phase ${index} CPU`);
    nonnegative(phase.maxRssBytes, `phase ${index} RSS`);
    integer(phase.blockInputOperations, `phase ${index} block input`);
    integer(phase.blockOutputOperations, `phase ${index} block output`);
    integer(phase.capturedOutputBytes, `phase ${index} output bytes`);
  }
  assert.equal(
    childCpu,
    phaseMeasurements.reduce((sum, phase) => sum + number(phase.cpuMs, "phase CPU"), 0),
    "child CPU total mismatch",
  );
  assert.equal(
    maxRss,
    Math.max(controllerRss, ...phaseMeasurements.map((phase) => number(phase.maxRssBytes, "phase RSS"))),
    "maximum RSS total mismatch",
  );

  const blockIo = object(measurements.blockIo, "block I/O measurements");
  exactKeys(
    blockIo,
    ["childInputOperations", "childOutputOperations", "controllerInputOperations", "controllerOutputOperations", "totalOperations"],
    "block I/O measurements",
  );
  const blockParts = [
    integer(blockIo.childInputOperations, "child block input"),
    integer(blockIo.childOutputOperations, "child block output"),
    integer(blockIo.controllerInputOperations, "controller block input"),
    integer(blockIo.controllerOutputOperations, "controller block output"),
  ];
  const totalBlock = integer(blockIo.totalOperations, "total block I/O");
  assert.ok(totalBlock <= SYSTEM_E2E_BUDGETS.blockOperations, "block I/O budget exceeded");
  assert.equal(totalBlock, blockParts.reduce((sum, value) => sum + value, 0), "block I/O total mismatch");
  assert.equal(blockParts[0], phaseMeasurements.reduce((sum, phase) => sum + number(phase.blockInputOperations, "phase block input"), 0), "child block input total mismatch");
  assert.equal(blockParts[1], phaseMeasurements.reduce((sum, phase) => sum + number(phase.blockOutputOperations, "phase block output"), 0), "child block output total mismatch");

  const sharedMeasurements = object(sharedFlow.measurements, "shared-flow measurements");
  exactKeys(
    sharedMeasurements,
    [
      "machineRoots", "workspaceIdentities", "memberRegistryCoverage", "capturedRows", "learningFactRows",
      "directRowOperations", "capturedPrimaryTokens", "allocatedPrimaryTokens", "unallocatedPrimaryTokens",
      "projectRepoCount", "pullCount", "unpricedEvents", "costStatus", "offlineRequests", "reconnectRequests",
      "acceptedExactlyOnce", "poisonReceipts", "outcomeFailedFirstPass", "correctionLoops", "reviewCorrections",
      "inWindowRework", "attempts", "retryLinks", "episodes", "exposures", "analysisWorkUnits",
      "unchangedAnalysisWorkUnits", "autoSkillWrites",
    ],
    "shared-flow measurements",
  );
  const directRows = integer(sharedMeasurements.directRowOperations, "direct row operations");
  assert.ok(directRows > 0, "direct row operations must be nonzero");
  assert.ok(directRows <= SYSTEM_E2E_BUDGETS.directRows, "direct row budget exceeded");

  const resourcePhase = phases.find((phase) => phase.name === "idle_dashboard_resources");
  assert.ok(resourcePhase, "resource phase missing");
  const resourceArtifact = object(resourcePhase.artifact, "resource artifact");
  const resourceScenarios = array(resourceArtifact.scenarios, "resource scenarios");
  const supportRows = resourceScenarios.reduce<number>((total, entry, scenarioIndex) => {
    const scenario = object(entry, `resource scenario ${scenarioIndex}`);
    const counters = object(scenario.counters, `resource scenario ${scenarioIndex} counters`);
    return total + RESOURCE_ROW_COUNTER_NAMES.reduce(
      (sum, name) => sum + integer(counters[name] ?? 0, `resource counter ${name}`),
      0,
    );
  }, 0);
  const rowWork = object(measurements.rowWork, "row-work measurements");
  exactKeys(rowWork, ["directOperations", "supportOperations", "totalOperations"], "row-work measurements");
  assert.equal(rowWork.directOperations, directRows, "direct row-work measurement mismatch");
  assert.equal(rowWork.supportOperations, supportRows, "support row-work measurement mismatch");
  const totalRows = integer(rowWork.totalOperations, "total row operations");
  assert.equal(totalRows, directRows + supportRows, "total row-work mismatch");
  assert.ok(totalRows > 0, "total row operations must be nonzero");
  assert.ok(totalRows <= SYSTEM_E2E_BUDGETS.totalRowOperations, "total row budget exceeded");

  const outputBytes = integer(measurements.capturedOutputBytes, "captured output bytes");
  assert.ok(outputBytes <= SYSTEM_E2E_BUDGETS.capturedOutputBytes, "captured output budget exceeded");
  assert.equal(outputBytes, phaseMeasurements.reduce((sum, phase) => sum + number(phase.capturedOutputBytes, "phase output bytes"), 0), "captured output byte total mismatch");

  const margins = object(measurements.budgetMargins, "budget margins");
  exactKeys(margins, ["wallMs", "cpuMs", "rssBytes", "blockOperations", "rowOperations", "capturedOutputBytes"], "budget margins");
  assert.equal(margins.wallMs, SYSTEM_E2E_BUDGETS.wallMs - wallMs, "wall budget margin mismatch");
  assert.equal(margins.cpuMs, SYSTEM_E2E_BUDGETS.cpuMs - totalCpu, "CPU budget margin mismatch");
  assert.equal(margins.rssBytes, SYSTEM_E2E_BUDGETS.maxRssBytes - maxRss, "RSS budget margin mismatch");
  assert.equal(margins.blockOperations, SYSTEM_E2E_BUDGETS.blockOperations - totalBlock, "block I/O budget margin mismatch");
  assert.equal(margins.rowOperations, SYSTEM_E2E_BUDGETS.totalRowOperations - totalRows, "row budget margin mismatch");
  assert.equal(margins.capturedOutputBytes, SYSTEM_E2E_BUDGETS.capturedOutputBytes - outputBytes, "output budget margin mismatch");

  const idle = object(measurements.idle, "idle measurements");
  exactKeys(idle, ["rawEventWrites", "rawEventRewrites", "filesOpened", "fileBytesRead", "fullHistoryFileReads", "overlappingJobs"], "idle measurements");
  assert.ok(Object.values(idle).every((value) => value === 0), "idle work must remain exactly zero");
  const dashboard = object(measurements.dashboard, "dashboard measurements");
  exactKeys(dashboard, ["rawRowsScanned", "filesOpened", "fileBytesRead", "filesystemEntriesScanned"], "dashboard measurements");
  assert.ok(Object.values(dashboard).every((value) => value === 0), "dashboard scan work must remain exactly zero");
}

function verifyIsolation(receipt: Record<string, unknown>, repoRoot: string) {
  const isolation = object(receipt.isolation, "isolation");
  exactKeys(
    isolation,
    ["temporaryMachineRoots", "distinctMachineIdentities", "distinctWorkspaceIdentities", "credentialCopyOperations", "realLaunchAgentsTouched", "realProviderCalls", "liveLedgersOrConfigsTouched", "packagePublishes", "skillOrMemoryWrites", "rootGuards"],
    "isolation",
  );
  assert.deepEqual(
    {
      temporaryMachineRoots: isolation.temporaryMachineRoots,
      distinctMachineIdentities: isolation.distinctMachineIdentities,
      distinctWorkspaceIdentities: isolation.distinctWorkspaceIdentities,
      credentialCopyOperations: isolation.credentialCopyOperations,
      realLaunchAgentsTouched: isolation.realLaunchAgentsTouched,
      realProviderCalls: isolation.realProviderCalls,
      liveLedgersOrConfigsTouched: isolation.liveLedgersOrConfigsTouched,
      packagePublishes: isolation.packagePublishes,
      skillOrMemoryWrites: isolation.skillOrMemoryWrites,
    },
    {
      temporaryMachineRoots: 2,
      distinctMachineIdentities: 2,
      distinctWorkspaceIdentities: 2,
      credentialCopyOperations: 0,
      realLaunchAgentsTouched: 0,
      realProviderCalls: 0,
      liveLedgersOrConfigsTouched: 0,
      packagePublishes: 0,
      skillOrMemoryWrites: 0,
    },
    "isolation invariants changed",
  );
  const guards = array(isolation.rootGuards, "root guards");
  const guardContract = loadRootGuardContract(rootGuardContractPath(repoRoot));
  assert.equal(guards.length, 7, "all seven forbidden roots must be observed");
  const expectedLabels = [
    "machine_a_codex_skills", "machine_a_codex_memories", "machine_a_claude_skills",
    "machine_b_codex_skills", "machine_b_codex_memories", "machine_b_claude_skills",
    "operator_live_shadow",
  ];
  assert.deepEqual(
    guardContract.guards.map((guard) => guard.label),
    expectedLabels,
    "committed root guard contract labels or order changed",
  );
  assert.deepEqual(guards.map((entry) => object(entry, "root guard").label), expectedLabels, "root guard coverage changed");
  for (const [index, entry] of guards.entries()) {
    exactKeys(entry, ["label", "beforeDigest", "afterDigest", "entriesBefore", "entriesAfter", "writeDenyMode", "unchanged"], `root guard ${index}`);
    assert.equal(entry.beforeDigest, entry.afterDigest, `root guard ${index} changed`);
    assert.equal(
      entry.beforeDigest,
      guardContract.guards[index]?.expectedSentinelTreeDigest,
      `root guard ${index} expected sentinel-tree digest mismatch`,
    );
    assert.equal(entry.entriesBefore, entry.entriesAfter, `root guard ${index} entry count changed`);
    assert.ok(integer(entry.entriesBefore, `root guard ${index} entries`) > 0, `root guard ${index} is empty`);
    assert.equal(entry.writeDenyMode, 0o500, `root guard ${index} write-deny mode changed`);
    assert.equal(entry.unchanged, true, `root guard ${index} is not unchanged`);
  }
}

function verifyPhases(
  root: string,
  flow: Record<string, unknown>,
  sourceHeadCommit: string,
  testedTreeCommit: string,
) {
  const phaseChain = array(flow.phaseChain, "phase chain");
  const supportContract = loadSupportContract(supportContractPath(root));
  assert.equal(phaseChain.length, supportContract.phases.length, "phase chain count mismatch");
  const phases = phaseChain.map((entry, index) => {
    exactKeys(
      entry,
      ["schema", "name", "status", "expectedFlowFingerprint", "sourceHeadCommit", "testedTreeCommit", "artifact", "artifactDigest", "semanticDigest", "outputDigest"],
      `phase ${index}`,
    );
    const contract = supportContract.phases[index]!;
    assert.equal(entry.schema, "plimsoll.system-e2e-phase-receipt.v1", `phase ${index} schema mismatch`);
    assert.equal(entry.name, contract.name, `phase ${index} name mismatch`);
    assert.equal(entry.status, "pass", `phase ${index} status mismatch`);
    assert.equal(entry.expectedFlowFingerprint, flow.fingerprint, `phase ${index} flow fingerprint mismatch`);
    assert.equal(entry.sourceHeadCommit, sourceHeadCommit, `phase ${index} source head commit mismatch`);
    assert.equal(entry.testedTreeCommit, testedTreeCommit, `phase ${index} tested tree commit mismatch`);
    assert.equal(entry.artifactDigest, digest(entry.artifact), `phase ${index} artifact digest mismatch`);
    assert.equal(entry.artifactDigest, contract.expectedArtifactDigest, `phase ${index} committed artifact contract mismatch`);
    const semanticMaterial = {
      name: entry.name,
      status: entry.status,
      expectedFlowFingerprint: entry.expectedFlowFingerprint,
      sourceHeadCommit: entry.sourceHeadCommit,
      testedTreeCommit: entry.testedTreeCommit,
      artifactDigest: entry.artifactDigest,
      artifact: entry.artifact,
    };
    assert.equal(entry.semanticDigest, digest(semanticMaterial), `phase ${index} semantic digest mismatch`);
    const outputMaterial = Object.fromEntries(
      ["schema", "name", "status", "expectedFlowFingerprint", "sourceHeadCommit", "testedTreeCommit", "artifact", "artifactDigest", "semanticDigest"]
        .map((key) => [key, entry[key]]),
    );
    assert.equal(entry.outputDigest, digest(outputMaterial), `phase ${index} output digest mismatch`);
    return entry;
  });
  assert.equal(flow.phaseChainDigest, digest(phases), "phase-chain digest mismatch");
  return phases;
}

function verifySharedFlow(flow: Record<string, unknown>) {
  const shared = object(flow.sharedFlow, "shared flow");
  exactKeys(shared, ["status", "inputFingerprint", "lineage", "outputDigest", "measurements"], "shared flow");
  assert.equal(shared.status, "pass", "shared flow did not pass");
  assert.equal(shared.inputFingerprint, flow.fingerprint, "shared flow fingerprint mismatch");
  const lineage = object(shared.lineage, "shared lineage");
  exactKeys(lineage, ["identity", "capture", "delivery", "allocation", "outcome", "learningFacts", "evidence", "rowWork"], "shared lineage");

  const identity = object(lineage.identity, "flow identity");
  exactKeys(identity, ["machines", "workspaces", "sessions", "eventIds", "pulls"], "flow identity");
  assert.deepEqual(identity, EXPECTED, "flow identity fixture changed");
  const expectedFingerprint = digest({ schema: SYSTEM_E2E_SCHEMA, ...identity });
  assert.equal(flow.fingerprint, expectedFingerprint, "flow fingerprint mismatch");

  const capture = exactStage(lineage.capture, ["rows"], "capture lineage");
  const captureRows = array(capture.rows, "capture rows").map((entry, index) => {
    exactKeys(entry, ["eventId", "sessionId", "machineId", "workspaceId", "inputTokens", "outputTokens", "costUsd", "repoHash", "branchHash", "headSha"], `capture row ${index}`);
    return entry;
  });
  assert.equal(captureRows.length, EXPECTED.eventIds.length, "capture row count mismatch");
  assert.deepEqual(captureRows.map((row) => row.eventId), EXPECTED.eventIds, "capture event IDs mismatch");
  for (const row of captureRows.slice(0, 4)) {
    assert.equal(row.sessionId, EXPECTED.sessions[0], "machine-A capture session mismatch");
    assert.equal(row.machineId, EXPECTED.machines[0], "machine-A capture machine mismatch");
    assert.equal(row.workspaceId, EXPECTED.workspaces[0], "machine-A capture workspace mismatch");
  }
  assert.deepEqual(
    { sessionId: captureRows[4]!.sessionId, machineId: captureRows[4]!.machineId, workspaceId: captureRows[4]!.workspaceId },
    { sessionId: EXPECTED.sessions[1], machineId: EXPECTED.machines[1], workspaceId: EXPECTED.workspaces[1] },
    "machine-B capture identity mismatch",
  );

  const delivery = exactStage(
    lineage.delivery,
    ["offlineRequestEventIds", "acceptedEventIds", "poisonEventIds", "reconnectRequestEventIds", "acknowledgedReceipts", "deadReceipts"],
    "delivery lineage",
  );
  const machineAIds = [...EXPECTED.eventIds.slice(0, 4)].sort();
  const acceptedIds = [...EXPECTED.eventIds.slice(0, 1), ...EXPECTED.eventIds.slice(2, 4)].sort();
  assert.deepEqual(sortedStrings(delivery.offlineRequestEventIds, "offline request IDs"), machineAIds, "offline request IDs mismatch");
  assert.deepEqual(sortedStrings(delivery.acceptedEventIds, "accepted event IDs"), acceptedIds, "accepted event IDs must be exactly three valid machine-A IDs");
  assert.deepEqual(sortedStrings(delivery.poisonEventIds, "poison event IDs"), [EXPECTED.eventIds[1]], "poison event ID mismatch");
  assert.ok(!acceptedIds.includes(EXPECTED.eventIds[4]), "accepted set contains machine-B event");
  assert.equal(delivery.acknowledgedReceipts, 3, "acknowledged receipt count mismatch");
  assert.equal(delivery.deadReceipts, 1, "dead receipt count mismatch");

  const allocation = exactStage(
    lineage.allocation,
    ["receipts", "pullRows", "coverage", "capturedPrimaryTokens", "allocatedPrimaryTokens", "unallocatedPrimaryTokens"],
    "allocation lineage",
  );
  const allocationReceipts = array(allocation.receipts, "allocation receipts").map((entry, index) => object(entry, `allocation receipt ${index}`));
  assert.deepEqual(allocationReceipts.map((row) => row.eventId), EXPECTED.eventIds.slice(0, 4), "allocation receipt event IDs mismatch");
  assert.ok(allocationReceipts.every((row) => row.sessionId === EXPECTED.sessions[0]), "allocation receipt session mismatch");
  const capturedPrimary = allocationReceipts.reduce(
    (sum, row) => {
      const amounts = object(row.amounts, "allocation amounts");
      return sum + integer(amounts.inputTokens, "allocation input tokens") + integer(amounts.outputTokens, "allocation output tokens");
    },
    0,
  );
  assert.equal(capturedPrimary, 100, "captured allocation total mismatch");
  assert.equal(allocation.capturedPrimaryTokens, capturedPrimary, "declared captured allocation total mismatch");
  const pullRows = array(allocation.pullRows, "allocation pull rows").map((entry, index) => object(entry, `pull row ${index}`));
  const allocatedPrimary = pullRows.reduce(
    (sum, row) => sum + integer(row.inputTokens, "pull input tokens") + integer(row.outputTokens, "pull output tokens"),
    0,
  );
  assert.equal(allocation.allocatedPrimaryTokens, allocatedPrimary, "declared allocated total mismatch");
  assert.equal(allocation.unallocatedPrimaryTokens, capturedPrimary - allocatedPrimary, "allocation remainder mismatch");
  assert.deepEqual(pullRows.map((row) => row.pull), [101, 102], "allocated pull IDs mismatch");
  const coverage = object(allocation.coverage, "allocation coverage");
  const reconciliation = object(coverage.reconciliation, "allocation reconciliation");
  assert.ok(Object.values(reconciliation).every((value) => value === true), "allocation reconciliation is not exact");

  const outcome = exactStage(
    lineage.outcome,
    ["facts", "coverage", "requiredChecks", "reworkWindowDays", "derived"],
    "outcome lineage",
  );
  const outcomeFacts = array(outcome.facts, "outcome facts") as PullTimelineFact[];
  assert.deepEqual(
    outcomeFacts.map((fact) => fact.externalId),
    ["check-a1", "check-a2", "merge-101", "pull-101-open", "revert-101", "review-approval", "review-request", "revision-a1", "revision-a2"],
    "outcome external IDs mismatch",
  );
  const derived = derivePullOutcomeTimeline({
    facts: outcomeFacts,
    coverage: array(outcome.coverage, "outcome coverage") as Parameters<typeof derivePullOutcomeTimeline>[0]["coverage"],
    requiredChecks: { names: array(outcome.requiredChecks, "required checks") as string[] },
    reworkWindowDays: integer(outcome.reworkWindowDays, "rework window"),
  });
  assert.equal(derived.length, 1, "outcome derivation count mismatch");
  assert.deepEqual(outcome.derived, derived[0], "outcome derivation mismatch");
  const outcomeDerived = object(outcome.derived, "derived outcome");
  assert.equal(outcomeDerived.pullNumber, pullRows[0]!.pull, "outcome pull is not allocation-bound");
  assert.equal(outcomeDerived.firstPassSuccess, false, "failed first pass was lost");
  assert.equal(array(outcomeDerived.correctionLoops, "correction loops").length, 1, "correction-loop count mismatch");
  assert.equal(array(outcomeDerived.reviewCorrections, "review corrections").length, 1, "review-correction count mismatch");
  assert.equal(array(outcomeDerived.rework, "rework").length, 1, "rework count mismatch");

  const learning = exactStage(
    lineage.learningFacts,
    ["episodeBindings", "attemptEventBindings", "attempts", "exposures"],
    "learning-fact lineage",
  );
  const episodeBindings = array(learning.episodeBindings, "episode bindings").map((entry, index) => {
    exactKeys(entry, ["sourceEpisodeKey", "fact"], `episode binding ${index}`);
    const fact = workEpisodeFactSchema.parse(entry.fact);
    assert.deepEqual(
      fact,
      buildWorkEpisodeFact({
        source: fact.source,
        sessionId: fact.sessionId,
        sourceEpisodeKey: String(entry.sourceEpisodeKey),
        workClass: fact.workClass,
        complexityBand: fact.complexityBand,
        startedAt: fact.startedAt,
        endedAt: fact.endedAt,
      }),
      `episode ${index} deterministic identity mismatch`,
    );
    return { sourceEpisodeKey: entry.sourceEpisodeKey, fact };
  });
  assert.deepEqual(episodeBindings.map((entry) => entry.sourceEpisodeKey), ["shared-flow-treatment", "shared-flow-control"], "episode binding keys mismatch");
  assert.deepEqual(episodeBindings.map((entry) => entry.fact.sessionId), EXPECTED.sessions, "episode sessions mismatch");

  const bindings = array(learning.attemptEventBindings, "attempt event bindings").map((entry, index) => {
    exactKeys(entry, ["eventId", "sourceOperationKey", "signal", "operationId"], `attempt event binding ${index}`);
    const captureRow = captureRows.find((row) => row.eventId === entry.eventId);
    assert.ok(captureRow, `attempt binding ${index} does not reference a captured event`);
    const operationId = deterministicToolOperationId({
      source: "codex",
      sessionId: String(captureRow.sessionId),
      sourceOperationKey: String(entry.sourceOperationKey),
    });
    assert.equal(entry.operationId, operationId, `attempt binding ${index} operation ID mismatch`);
    return entry;
  });
  assert.deepEqual(bindings.map((entry) => entry.eventId), EXPECTED.eventIds.slice(0, 4), "attempt binding event IDs mismatch");
  assert.deepEqual(bindings.map((entry) => entry.signal), ["start", "result", "start", "result"], "attempt signal sequence mismatch");
  const attempts = array(learning.attempts, "attempts").map((entry) => toolAttemptFactSchema.parse(entry));
  assert.equal(attempts.length, 2, "attempt count mismatch");
  assert.deepEqual(attempts.map((entry) => entry.operationId), [bindings[0]!.operationId, bindings[2]!.operationId], "attempt operation IDs mismatch");
  assert.equal(attempts[0]!.episodeId, episodeBindings[0]!.fact.episodeId, "attempt episode mismatch");
  assert.equal(attempts[1]!.retryOf, attempts[0]!.operationId, "retry lineage mismatch");
  assert.deepEqual(attempts.map((entry) => entry.resultStatus), ["failure", "success"], "attempt results mismatch");
  const exposures = array(learning.exposures, "exposures").map((entry) => validateTechniqueExposureFactIdentity(entry));
  assert.equal(exposures.length, 2, "exposure count mismatch");
  assert.deepEqual(exposures.map((entry) => entry.episodeId), episodeBindings.map((entry) => entry.fact.episodeId), "exposure episode bindings mismatch");
  assert.deepEqual(exposures.map((entry) => entry.mode), ["treatment", "control"], "exposure modes mismatch");

  const evidence = exactStage(
    lineage.evidence,
    ["manifest", "sourceFingerprint", "packetFingerprint", "claimClass", "causalClaim", "prescriptiveClaim", "skillPublicationAuthorized", "skillInstallationAuthorized", "analysisWorkUnits", "unchangedAnalysisWorkUnits"],
    "evidence lineage",
  );
  const manifest = evidence.manifest as LearningEvidenceManifest;
  assert.equal(manifest.source.snapshotId, flow.fingerprint, "evidence snapshot is not flow-bound");
  assert.equal(manifest.source.queryHash, String(flow.fingerprint).replace("sha256:", ""), "evidence query hash is not flow-bound");
  assert.deepEqual(manifest.pairs[0]?.exposed.exposure, exposures[0], "treatment evidence exposure is not persisted-row-bound");
  assert.deepEqual(manifest.pairs[0]?.control.exposure, exposures[1], "control evidence exposure is not persisted-row-bound");
  assert.equal(manifest.pairs[0]?.exposed.outcome.value, outcomeDerived.firstPassSuccess ? 1 : 0, "evidence outcome is not persisted-outcome-bound");
  const compiled = compileLearningEvidencePacket(manifest);
  assert.equal(compiled.status, "computed", "evidence did not compute");
  assert.equal(evidence.sourceFingerprint, compiled.sourceFingerprint, "evidence source fingerprint mismatch");
  assert.equal(evidence.packetFingerprint, compiled.packet.packetFingerprint, "evidence packet fingerprint mismatch");
  assert.equal(evidence.claimClass, compiled.packet.claimClass, "evidence claim class mismatch");
  assert.equal(evidence.causalClaim, false, "causal claim must remain false");
  assert.equal(evidence.prescriptiveClaim, false, "prescriptive claim must remain false");
  assert.equal(evidence.skillPublicationAuthorized, false, "skill publication must remain unauthorized");
  assert.equal(evidence.skillInstallationAuthorized, false, "skill installation must remain unauthorized");
  assert.equal(evidence.analysisWorkUnits, compiled.analysisWorkUnits, "analysis work-unit mismatch");
  assert.equal(evidence.unchangedAnalysisWorkUnits, 0, "unchanged analysis must remain zero-work");

  const rowWork = object(lineage.rowWork, "lineage row work");
  exactKeys(rowWork, ["captureRowsRead", "allocationRowsRead", "timelineFactRowsRead", "timelineCoverageRowsRead", "attemptRowsRead", "episodeRowsRead", "exposureRowsRead", "learningPairRowsAnalyzed", "sqliteWriteChanges"], "lineage row work");
  const expectedCounts = {
    captureRowsRead: captureRows.length,
    allocationRowsRead: allocationReceipts.length,
    timelineFactRowsRead: array(outcome.facts, "outcome facts").length,
    timelineCoverageRowsRead: array(outcome.coverage, "outcome coverage").length,
    attemptRowsRead: attempts.length,
    episodeRowsRead: episodeBindings.length,
    exposureRowsRead: exposures.length,
    learningPairRowsAnalyzed: compiled.analysisWorkUnits,
  };
  for (const [key, expected] of Object.entries(expectedCounts)) {
    assert.equal(rowWork[key], expected, `lineage row counter ${key} mismatch`);
  }
  assert.ok(integer(rowWork.sqliteWriteChanges, "SQLite write changes") > 0, "SQLite write work must be nonzero");
  const directRows = Object.values(rowWork).reduce<number>((sum, value) => sum + integer(value, "lineage row counter"), 0);
  const sharedMeasurements = object(shared.measurements, "shared-flow measurements");
  assert.equal(sharedMeasurements.directRowOperations, directRows, "direct row operations do not equal persisted row work");

  assert.equal(shared.outputDigest, digest(lineage), "shared-flow output digest mismatch");
  return shared;
}

function gitHead(repoRoot: string) {
  const result = spawnSync("/usr/bin/git", ["rev-parse", "HEAD"], { cwd: repoRoot, encoding: "utf8" });
  assert.equal(result.status, 0, "verifier could not read tested tree commit");
  return result.stdout.trim();
}

function commitIsAncestor(repoRoot: string, ancestor: string, descendant: string) {
  return spawnSync("/usr/bin/git", ["merge-base", "--is-ancestor", ancestor, descendant], {
    cwd: repoRoot,
    encoding: "utf8",
  }).status === 0;
}

export function verifySystemE2EReceipt(
  receiptValue: unknown,
  repoRoot: string,
  expectedSourceHeadCommit: string,
) {
  assert.match(expectedSourceHeadCommit, /^[a-f0-9]{40}$/, "expected source head commit must be a full SHA");
  exactKeys(
    receiptValue,
    ["schema", "status", "sourceHeadCommit", "testedTreeCommit", "nodeMajor", "isolation", "flow", "budgets", "externalGates", "contentPolicy", "liveStateTouched", "measurements", "volatileFieldsExcludedFromDeterministicDigest", "deterministicDigest"],
    "system E2E receipt",
  );
  const receipt = receiptValue;
  assert.equal(receipt.schema, SYSTEM_E2E_SCHEMA, "system E2E schema mismatch");
  assert.equal(receipt.status, "pass", "system E2E status mismatch");
  assert.match(String(receipt.sourceHeadCommit), /^[a-f0-9]{40}$/, "source head commit is not a full SHA");
  assert.match(String(receipt.testedTreeCommit), /^[a-f0-9]{40}$/, "tested tree commit is not a full SHA");
  assert.equal(receipt.sourceHeadCommit, expectedSourceHeadCommit, "source head commit does not match out-of-band expected commit");
  const actualTestedTreeCommit = gitHead(repoRoot);
  assert.equal(receipt.testedTreeCommit, actualTestedTreeCommit, "tested tree commit does not match verifier checkout");
  assert.ok(
    commitIsAncestor(repoRoot, expectedSourceHeadCommit, actualTestedTreeCommit),
    "expected source head commit is not an ancestor of verifier tested tree commit",
  );
  assert.equal(receipt.nodeMajor, 22, "system E2E Node major mismatch");
  assert.equal(receipt.liveStateTouched, false, "system E2E touched live state");
  verifyFixedBudgets(receipt);
  verifyIsolation(receipt, repoRoot);

  const flow = object(receipt.flow, "flow");
  exactKeys(flow, ["fingerprint", "sharedFlow", "phaseChain", "phaseChainDigest"], "flow");
  const phases = verifyPhases(
    repoRoot,
    flow,
    String(receipt.sourceHeadCommit),
    String(receipt.testedTreeCommit),
  );
  const sharedFlow = verifySharedFlow(flow);
  verifyMeasurements(receipt, sharedFlow, phases);

  const contentPolicy = object(receipt.contentPolicy, "content policy");
  exactKeys(contentPolicy, ["rawContentIncluded", "childOutputIncluded", "executableSkillIncluded", "privateSentinelIncluded"], "content policy");
  assert.ok(Object.values(contentPolicy).every((value) => value === false), "content policy included prohibited material");
  const externalGates = array(receipt.externalGates, "external gates");
  assert.equal(externalGates.length, 6, "external gate inventory mismatch");
  for (const [index, gate] of externalGates.entries()) exactKeys(gate, ["gate", "status"], `external gate ${index}`);
  assert.ok(externalGates.every((gate) => String(object(gate, "external gate").status).startsWith("not_run_requires_")), "external gates were overstated");

  assert.deepEqual(receipt.volatileFieldsExcludedFromDeterministicDigest, ["measurements"], "volatile field exclusion changed");
  const deterministicMaterial = Object.fromEntries(
    Object.entries(receipt).filter(([key]) => !["measurements", "volatileFieldsExcludedFromDeterministicDigest", "deterministicDigest"].includes(key)),
  );
  assert.equal(receipt.deterministicDigest, digest(deterministicMaterial), "deterministic digest mismatch");
  return {
    schema: receipt.schema,
    sourceHeadCommit: receipt.sourceHeadCommit,
    testedTreeCommit: receipt.testedTreeCommit,
    flowFingerprint: flow.fingerprint,
    deterministicDigest: receipt.deterministicDigest,
    phaseCount: phases.length,
    status: "verified" as const,
  };
}
