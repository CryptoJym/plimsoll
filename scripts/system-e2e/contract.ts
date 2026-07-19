import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export const SYSTEM_E2E_SCHEMA = "plimsoll.system-e2e-proof.v2" as const;
export const SUPPORT_NORMALIZATION_VERSION = 5 as const;
/** Fixed release thresholds. These are never derived from an observed run. */
export const SYSTEM_E2E_BUDGETS = {
  directRows: 500,
  totalRowOperations: 10_000,
  learningPairs: 100,
  wallMs: 180_000,
  cpuMs: 180_000,
  maxRssBytes: 1_500_000_000,
  blockOperations: 500_000,
  capturedOutputBytes: 24 * 1024 * 1024,
} as const;

export type SupportingKind =
  | "json_result"
  | "line_summary"
  | "line_summary_with_receipt"
  | "json_receipt";

export type SupportingNormalizationContext = {
  baseDirectory: string;
  roots: Array<{ label: string; absolutePath: string }>;
};

export type SupportContract = {
  schema: "plimsoll.system-e2e-support-contract.v1";
  normalizationVersion: typeof SUPPORT_NORMALIZATION_VERSION;
  phases: Array<{
    name: string;
    kind: SupportingKind;
    expectedArtifactDigest: string;
  }>;
};

export type TamperMutation = {
  pointer: string;
  replacement?: unknown;
  remove?: true;
};

export type TamperCase = {
  id: string;
  mutations: TamperMutation[];
  resign: boolean;
  expectedError: string;
};

export type TamperContract = {
  schema: "plimsoll.system-e2e-tamper-cases.v1";
  cases: TamperCase[];
};

export type RootGuardContract = {
  schema: "plimsoll.system-e2e-root-guards.v1";
  guards: Array<{ label: string; expectedSentinelTreeDigest: string }>;
};

export function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, child]) => [key, canonical(child)]),
    );
  }
  return value;
}

export function canonicalBytes(value: unknown) {
  return JSON.stringify(canonical(value));
}

export function digest(value: unknown) {
  const bytes = typeof value === "string" ? value : canonicalBytes(value);
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

export function exactKeys(
  value: unknown,
  expected: readonly string[],
  label: string,
): asserts value is Record<string, unknown> {
  assert.ok(value && typeof value === "object" && !Array.isArray(value), `${label} must be an object`);
  const actual = Object.keys(value as object).sort();
  assert.deepEqual(actual, [...expected].sort(), `${label} has missing, extra, or unknown fields`);
}

const DECLARED_PATH_KEYS = new Set([
  "blankHome",
  "blankPlimsoll",
  "bufferPath",
  "claudeCredential",
  "claudeSettings",
  "codexConfig",
  "codexCredential",
  "commandLog",
  "configPath",
  "dryHome",
  "dryPlimsoll",
  "dryTarget",
  "execPath",
  "home",
  "ledger",
  "missingRequiredEntries",
  "packagedCli",
  "path",
  "pidPath",
  "plistPath",
  "receipt",
  "root",
  "target",
  "unsupportedCommandLog",
  "unsupportedHome",
  "unsupportedPlimsoll",
  "unsupportedTarget",
  "workflow",
]);
const STABLE_SYSTEM_PATHS = new Set([
  "/neutral/bin",
  "/opt/homebrew/bin",
  "/usr/local/bin",
  "/usr/bin",
  "/bin",
  "/usr/sbin",
  "/sbin",
]);
const DIAGNOSTIC_TEXT_KEYS = new Set(["detail", "stdout", "stderr"]);
const DECLARED_LOOPBACK_URL_KEYS = new Set(["statusUrl", "url"]);

function isPathLike(value: string) {
  return path.isAbsolute(value) || value.startsWith(".") || value.includes("/");
}

function within(root: string, candidate: string) {
  const relative = path.relative(root, candidate);
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

/**
 * Resolve only schema-declared path fields. The semantic artifact records the
 * owned root role, never a temp basename or machine-specific absolute path.
 */
export function canonicalizeDeclaredPath(
  value: string,
  key: string,
  context: SupportingNormalizationContext,
) {
  assert.ok(DECLARED_PATH_KEYS.has(key), `${key} is not a declared path field`);
  assert.ok(isPathLike(value), `${key} is not a path-like declared path value`);
  if (key === "missingRequiredEntries" && STABLE_SYSTEM_PATHS.has(value)) {
    return `<required-system-path:${value}>`;
  }
  const candidate = path.resolve(context.baseDirectory, value);
  const roots = context.roots
    .map((root) => ({ label: root.label, absolutePath: path.resolve(root.absolutePath) }))
    .sort((left, right) => right.absolutePath.length - left.absolutePath.length);
  const owner = roots.find((root) => within(root.absolutePath, candidate));
  assert.ok(owner, `DeclaredPathOutsideAllowedRoots:${key}`);
  return `<path-root:${owner.label}>`;
}

function normalizeString(
  value: string,
  key: string,
  context: SupportingNormalizationContext,
): string {
  if (DECLARED_PATH_KEYS.has(key) && isPathLike(value)) {
    return canonicalizeDeclaredPath(value, key, context);
  }
  if (DECLARED_LOOPBACK_URL_KEYS.has(key)) {
    const parsed = new URL(value);
    assert.equal(parsed.protocol, "http:", `${key} must use the fixture HTTP protocol`);
    assert.ok(
      ["127.0.0.1", "localhost", "[::1]"].includes(parsed.hostname),
      `${key} must remain on the fixture loopback interface`,
    );
    assert.match(parsed.port, /^\d+$/, `${key} must include the fixture port`);
    return `<loopback-http-url>${parsed.pathname}`;
  }
  if (/^(?:v)?22\.\d+\.\d+$/.test(value) && /^(?:node|version)$/i.test(key)) {
    return "<node-major-22>";
  }
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value) && /(?:At|Time)$/i.test(key)) {
    return "<volatile-iso-time>";
  }
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value) &&
      /(?:instance|nonce|runId|selfTestEventId)/i.test(key)) {
    return "<volatile-uuid>";
  }
  if (/^(?:sha256:)?[0-9a-f]{64}$/i.test(value) &&
      /(?:before|after|fingerprint|digest)/i.test(key)) {
    return "<volatile-digest>";
  }
  if (DIAGNOSTIC_TEXT_KEYS.has(key)) return value.length > 0 ? `<nonempty-${key}>` : "";
  return value;
}

const VOLATILE_NUMBER_KEYS = /^(?:pid|port|durationMs|elapsedMs|warmP95Ms|tempBytes|maxRssBytes|serializedBytes|receiptBytes|parentCredentialLikeNameCount)$/i;
const RESOURCE_VOLATILE_NUMBER_PATH =
  /^(?:root\.scenarios\[\d+\]\{id=bounded_generation_capture\}\.(?:counters\.(?:fileBytesRead|filesOpened|maintenanceRuns)|measurements\.(?:rssGrowthBytes|statusProbes|warmStatusP95Ms))|root\.scenarios\[\d+\]\{id=dashboard_projection_budget\}\.measurements\.generation|root\.scenarios\[\d+\]\{id=no_change_constant_work\}\.(?:counters\.maintenanceRuns|measurements\.(?:baselineCadences|maxCodexPendingMetadata|maxClaudePendingMetadata|maxAggregatePendingMetadata)))$/;

/**
 * Preserve the complete parsed result shape while replacing only explicitly
 * volatile values. Unknown/missing fields therefore change the committed
 * artifact digest instead of being silently ignored.
 */
export function normalizeSupportingArtifact(
  value: unknown,
  context: SupportingNormalizationContext,
  key = "root",
  fieldPath = "root",
): unknown {
  if (Array.isArray(value)) {
    return value.map((child, index) =>
      normalizeSupportingArtifact(child, context, key, `${fieldPath}[${index}]`)
    );
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const objectPath = typeof record.id === "string"
      ? `${fieldPath}{id=${record.id}}`
      : fieldPath;
    return Object.fromEntries(
      Object.entries(record)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([childKey, child]) => [
          childKey,
          normalizeSupportingArtifact(child, context, childKey, `${objectPath}.${childKey}`),
        ]),
    );
  }
  if (typeof value === "string") return normalizeString(value, key, context);
  if (
    typeof value === "number" &&
    (VOLATILE_NUMBER_KEYS.test(key) || RESOURCE_VOLATILE_NUMBER_PATH.test(fieldPath))
  ) {
    assert.ok(Number.isFinite(value) && value >= 0, `${key} must be a nonnegative finite measurement`);
    return "<volatile-number>";
  }
  return value;
}

function omitNonSemanticReceiptLocations(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(omitNonSemanticReceiptLocations);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .filter(([key, child]) => !(key === "receipt" && typeof child === "string" && isPathLike(child)))
        .map(([key, child]) => [key, omitNonSemanticReceiptLocations(child)]),
    );
  }
  return value;
}

function parseJsonExactly(text: string, label: string) {
  const trimmed = text.trim();
  assert.ok(trimmed.startsWith("{") && trimmed.endsWith("}"), `${label} is not one canonical JSON object`);
  const parsed = JSON.parse(trimmed) as unknown;
  assert.equal(JSON.stringify(parsed), trimmed, `${label} is not canonical compact JSON`);
  return parsed;
}

function parsePrettyJsonExactly(text: string, label: string) {
  const trimmed = text.trim();
  assert.ok(trimmed.startsWith("{") && trimmed.endsWith("}"), `${label} is not one JSON object`);
  return JSON.parse(trimmed) as unknown;
}

function parsePassLinesWithSummary(text: string, label: string) {
  const lines = text.trim().split("\n");
  assert.ok(lines.length >= 2, `${label} is missing PASS lines or summary`);
  const summaryLine = lines.pop()!;
  const passNames = lines.map((line) => {
    assert.match(line, /^PASS [a-z0-9_]+$/, `${label} emitted noncanonical or unknown output`);
    return line.slice(5);
  });
  assert.equal(new Set(passNames).size, passNames.length, `${label} emitted duplicate PASS names`);
  return { passNames, summary: parseJsonExactly(summaryLine, `${label} summary`) };
}

function assertInstallResult(value: unknown) {
  exactKeys(value, ["issue", "ok", "node", "checks"], "install result");
  assert.equal(value.issue, 107);
  assert.equal(value.ok, true);
  exactKeys(value.node, ["execPath", "version"], "install node");
  assert.ok(Array.isArray(value.checks) && value.checks.length > 0, "install checks are missing");
  for (const [index, check] of value.checks.entries()) {
    exactKeys(check, ["name", "passed", "detail"], `install check ${index}`);
    assert.match(String(check.name), /^[a-z0-9_]+$/);
    assert.equal(check.passed, true, `install check ${String(check.name)} did not pass`);
  }
}

function assertJoinResult(value: unknown) {
  exactKeys(value, ["ok", "proof", "appVersion", "node", "checks"], "join result");
  assert.equal(value.ok, true);
  assert.equal(value.proof, "join-isolation");
  assert.ok(Array.isArray(value.checks) && value.checks.length > 0, "join checks are missing");
  for (const [index, check] of value.checks.entries()) {
    exactKeys(check, ["name", "detail"], `join check ${index}`);
    assert.match(String(check.name), /^[a-z0-9_]+$/);
  }
}

function assertLifecycleResult(value: ReturnType<typeof parsePassLinesWithSummary>) {
  exactKeys(value.summary, ["proof", "checks", "passed", "failed", "liveStateTouched"], "lifecycle summary");
  assert.equal(value.summary.proof, "lifecycle");
  assert.equal(value.summary.passed, value.passNames.length);
  assert.equal(value.summary.checks, value.passNames.length);
  assert.deepEqual(value.summary.failed, []);
  assert.equal(value.summary.liveStateTouched, false);
}

function assertPrivacyResult(
  value: ReturnType<typeof parsePassLinesWithSummary>,
  receipt: unknown,
) {
  exactKeys(value.summary, ["passed", "checks", "failures", "receipt"], "privacy summary");
  assert.equal(value.summary.passed, true);
  assert.deepEqual(value.summary.failures, []);
  assert.equal(value.summary.checks, value.passNames.length);
  exactKeys(
    receipt,
    ["schemaVersion", "issue", "mode", "evidenceVault", "legacyEvidenceDisposition", "node", "passed", "checks", "measurements"],
    "privacy receipt",
  );
  assert.equal(receipt.passed, true);
  assert.ok(Array.isArray(receipt.checks));
  assert.equal(receipt.checks.length, value.passNames.length);
  for (const [index, check] of receipt.checks.entries()) {
    exactKeys(check, ["name", "passed", "detail"], `privacy receipt check ${index}`);
    assert.equal(check.passed, true);
    assert.equal(check.name, value.passNames[index]);
  }
  exactKeys(receipt.measurements, ["durationMs", "tempBytes", "tempHomes"], "privacy measurements");
}

function assertResourceReceipt(receipt: unknown) {
  exactKeys(
    receipt,
    ["schema", "generatedAt", "overall", "gateReady", "requireIntegrated", "environment", "summary", "scenarios"],
    "resource receipt",
  );
  assert.equal(receipt.schema, "plimsoll.resource-proof.v1");
  assert.equal(receipt.overall, "pass");
  assert.equal(receipt.gateReady, true);
  assert.equal(receipt.requireIntegrated, true);
  exactKeys(receipt.summary, ["passed", "failed", "notWired", "skipped", "requiredIncomplete"], "resource summary");
  assert.equal(receipt.summary.failed, 0);
  assert.equal(receipt.summary.notWired, 0);
  assert.equal(receipt.summary.requiredIncomplete, 0);
  const resourceScenarios = receipt.scenarios;
  assert.ok(Array.isArray(resourceScenarios) && resourceScenarios.length > 0);
  for (const [index, scenario] of resourceScenarios.entries()) {
    if (scenario.status === "pass") {
      exactKeys(scenario, ["id", "required", "status", "detail", "durationMs", "counters", "measurements"], `resource scenario ${index}`);
    } else {
      exactKeys(scenario, ["id", "required", "status", "detail", "durationMs", "counters"], `resource scenario ${index}`);
      assert.equal(scenario.status, "skipped");
      assert.equal(scenario.required, false);
    }
  }

  const scenario = (id: string) => {
    const value = resourceScenarios.find((entry) =>
      entry && typeof entry === "object" && (entry as Record<string, unknown>).id === id
    );
    assert.ok(value && typeof value === "object" && !Array.isArray(value), `resource scenario ${id} is missing`);
    return value as Record<string, unknown>;
  };
  const integer = (value: unknown, label: string) => {
    assert.ok(Number.isSafeInteger(value) && Number(value) >= 0, `${label} must be a nonnegative safe integer`);
    return Number(value);
  };
  const finite = (value: unknown, label: string) => {
    assert.ok(typeof value === "number" && Number.isFinite(value) && value >= 0, `${label} must be nonnegative and finite`);
    return value;
  };
  const childRecord = (value: unknown, label: string) => {
    assert.ok(value && typeof value === "object" && !Array.isArray(value), `${label} must be an object`);
    return value as Record<string, unknown>;
  };

  // The 200ms automatic wall cap intentionally makes the exact number of
  // 64KiB slices scheduling-dependent. Validate the raw receipt against the
  // fixture and per-cadence bounds before normalizing only those runtime
  // counts. Loss/duplication outcomes remain exact and digest-bound.
  const bounded = scenario("bounded_generation_capture");
  const boundedCounters = childRecord(bounded.counters, "bounded capture counters");
  const boundedMeasurements = childRecord(bounded.measurements, "bounded capture measurements");
  const statusProbes = integer(boundedMeasurements.statusProbes, "bounded capture status probes");
  const maintenanceRuns = integer(boundedCounters.maintenanceRuns, "bounded capture maintenance runs");
  const filesOpened = integer(boundedCounters.filesOpened, "bounded capture files opened");
  const fileBytesRead = integer(boundedCounters.fileBytesRead, "bounded capture file bytes read");
  const denseBytes = integer(boundedMeasurements.denseBytes, "bounded capture dense bytes");
  assert.ok(statusProbes >= 1 && statusProbes <= 180, "bounded capture cadence count exceeded fixture cap");
  assert.equal(maintenanceRuns, statusProbes + 4, "bounded capture maintenance count lost its fixed adversarial runs");
  assert.ok(denseBytes >= 13 * 1024 * 1024, "bounded capture dense fixture shrank below 13 MiB");
  assert.ok(fileBytesRead >= denseBytes, "bounded capture did not read the complete dense fixture");
  assert.ok(fileBytesRead <= filesOpened * 128 * 1024, "bounded capture exceeded the per-open byte ceiling");
  // Dense capture has at most eight 64KiB slices per cadence. The fixture has
  // eight fixed adversarial/rotation cadences (also eight slices each) plus
  // at most eight 128KiB explicit-history opens.
  assert.ok(filesOpened >= statusProbes, "bounded capture opened fewer files than active cadences");
  assert.ok(filesOpened <= statusProbes * 8 + 72, "bounded capture exceeded fixture-derived open work");
  assert.equal(integer(boundedCounters.rawEventWrites, "bounded capture raw writes"), 102);
  assert.equal(integer(boundedCounters.fullHistoryFileReads, "bounded capture full-history reads"), 0);
  assert.equal(integer(boundedMeasurements.automaticPreinstallBodyReads, "bounded capture automatic preinstall reads"), 0);
  assert.equal(integer(boundedMeasurements.explicitFullPreinstallBodyReads, "bounded capture explicit preinstall reads"), 1);
  assert.equal(integer(boundedMeasurements.claudePreinstallBodyReads, "bounded capture Claude preinstall reads"), 0);
  assert.equal(integer(boundedMeasurements.ignoredAliasEntriesVisited, "bounded capture ignored alias entries"), 1);
  assert.equal(boundedMeasurements.irrelevantSymlinkBaselineSafe, true);
  assert.equal(boundedMeasurements.irrelevantExternalDirectorySymlinkIgnored, true);
  assert.equal(boundedMeasurements.matchingSymlinkFailsClosed, true);
  assert.equal(boundedMeasurements.nonmatchingNonregularIgnored, true);
  assert.equal(boundedMeasurements.matchingNonregularFailsClosed, true);
  assert.equal(boundedMeasurements.realDirectoriesStillRecurse, true);
  assert.equal(boundedMeasurements.denseExact, true);
  assert.equal(boundedMeasurements.rotationExactlyOnce, true);
  assert.equal(boundedMeasurements.truncationBlockedWithoutRead, true);
  assert.equal(boundedMeasurements.replacementRecoveredExactlyOnce, true);
  assert.equal(integer(boundedMeasurements.recoveryBodyReads, "replacement recovery reads"), 1);
  assert.ok(finite(boundedMeasurements.warmStatusP95Ms, "bounded capture warm status p95") <= 500);
  assert.ok(finite(boundedMeasurements.rssGrowthBytes, "bounded capture RSS growth") < 768 * 1024 * 1024);

  const noChangeMeasurements = childRecord(
    scenario("no_change_constant_work").measurements,
    "no-change measurements",
  );
  assert.equal(integer(noChangeMeasurements.replayRolloutFilesRead, "replay rollout reads"), 1);
  assert.equal(integer(noChangeMeasurements.replayTranscriptFilesRead, "replay transcript reads"), 1);
  assert.equal(integer(noChangeMeasurements.replayEventsAppended, "replay appended events"), 0);
  assert.equal(integer(noChangeMeasurements.replayRawEventWrites, "replay raw writes"), 0);
  assert.equal(integer(noChangeMeasurements.replayEventMutationsInserted, "replay inserted mutations"), 0);
  assert.ok(integer(noChangeMeasurements.baselineCodexGenerations, "baseline Codex generations") >= 200);
  assert.ok(integer(noChangeMeasurements.baselineClaudeGenerations, "baseline Claude generations") >= 1_200);
  assert.ok(integer(noChangeMeasurements.nestedNoncandidateEntries, "nested noncandidate entries") >= 300);
  assert.ok(
    integer(noChangeMeasurements.baselineCadences, "baseline cadences") <=
      integer(noChangeMeasurements.baselineCadenceLimit, "baseline cadence limit"),
  );
  assert.equal(integer(noChangeMeasurements.startupReadinessUpperBoundSeconds, "startup readiness bound"), 290);
  assert.equal(finite(noChangeMeasurements.maximumStartupDutyCycle, "startup duty cycle"), 0.04);
  assert.ok(integer(noChangeMeasurements.maxCodexPendingMetadata, "Codex pending metadata") <= 64);
  assert.ok(integer(noChangeMeasurements.maxClaudePendingMetadata, "Claude pending metadata") <= 64);
  assert.ok(integer(noChangeMeasurements.maxAggregatePendingMetadata, "aggregate pending metadata") <= 128);
  assert.equal(integer(noChangeMeasurements.pendingMetadataPerSourceCap, "pending metadata source cap"), 64);
  assert.equal(integer(noChangeMeasurements.pendingMetadataAggregateCap, "pending metadata aggregate cap"), 128);
  assert.equal(noChangeMeasurements.pendingMetadataWithinCap, true);
  assert.equal(noChangeMeasurements.codexValidatedBeforeComplete, true);
  assert.equal(noChangeMeasurements.claudeValidatedBeforeComplete, true);
  assert.equal(noChangeMeasurements.baselineProgressFair, true);

  const maintenanceMeasurements = childRecord(
    scenario("maintenance_regression_proof").measurements,
    "maintenance regression measurements",
  );
  assert.equal(integer(maintenanceMeasurements.exitCode, "maintenance proof exit code"), 0);
  assert.ok(integer(maintenanceMeasurements.checks, "maintenance proof checks") >= 20);
  assert.equal(maintenanceMeasurements.exactPendingIdentityProved, true);
  assert.equal(maintenanceMeasurements.stalledCadenceBackoffProved, true);

  const ownershipMeasurements = childRecord(
    scenario("duplicate_start_single_owner").measurements,
    "duplicate-start ownership measurements",
  );
  assert.equal(ownershipMeasurements.stopCommandExitedCleanly, true);
  assert.equal(ownershipMeasurements.stopReceiptReportedStopped, true);
  assert.equal(ownershipMeasurements.stopReceiptReason, "none");
  assert.equal(ownershipMeasurements.ownerExitedCleanly, true);
  assert.equal(integer(ownershipMeasurements.ownerExitCode, "owner exit code"), 0);
  assert.equal(ownershipMeasurements.ownerExitSignal, "none");
  assert.equal(ownershipMeasurements.stoppedThroughCli, true);
  assert.equal(ownershipMeasurements.pidRecordRemoved, true);

  const dashboardMeasurements = childRecord(
    scenario("dashboard_projection_budget").measurements,
    "dashboard measurements",
  );
  assert.ok(integer(dashboardMeasurements.generation, "dashboard generation") > 0);
  assert.equal(dashboardMeasurements.coherent, true);
  assert.equal(integer(dashboardMeasurements.warmRequests, "dashboard warm requests"), 20);
  assert.ok(finite(dashboardMeasurements.warmP95Ms, "dashboard warm p95") <= 500);
  assert.equal(integer(dashboardMeasurements.snapshotBuildsDuringRefresh, "dashboard snapshot builds"), 0);
  assert.equal(integer(dashboardMeasurements.snapshotCacheHits, "dashboard snapshot cache hits"), 25);
}

export function parseSupportingArtifact(
  kind: SupportingKind,
  stdout: string,
  context: SupportingNormalizationContext,
  receiptPath?: string,
): unknown {
  if (kind === "json_result") {
    const parsed = parsePrettyJsonExactly(stdout, "supporting JSON result");
    if ((parsed as Record<string, unknown>).issue === 107) assertInstallResult(parsed);
    else assertJoinResult(parsed);
    return normalizeSupportingArtifact(parsed, context);
  }
  if (kind === "line_summary") {
    const parsed = parsePassLinesWithSummary(stdout, "supporting line result");
    assertLifecycleResult(parsed);
    return normalizeSupportingArtifact(parsed, context);
  }
  assert.ok(receiptPath && fs.existsSync(receiptPath), "supporting receipt is missing");
  const receiptBytes = fs.readFileSync(receiptPath, "utf8");
  const receipt = parsePrettyJsonExactly(receiptBytes, "supporting receipt");
  if (kind === "line_summary_with_receipt") {
    const parsed = parsePassLinesWithSummary(stdout, "supporting line result");
    assertPrivacyResult(parsed, receipt);
    return normalizeSupportingArtifact(
      omitNonSemanticReceiptLocations({ ...parsed, receipt }),
      context,
    );
  }
  const stdoutReceipt = parsePrettyJsonExactly(stdout, "resource stdout receipt");
  assert.deepEqual(stdoutReceipt, receipt, "resource stdout and selected receipt diverged");
  assertResourceReceipt(receipt);
  return normalizeSupportingArtifact(receipt, context);
}

export function loadSupportContract(file: string): SupportContract {
  const value = JSON.parse(fs.readFileSync(file, "utf8")) as unknown;
  exactKeys(value, ["schema", "normalizationVersion", "phases"], "support contract");
  assert.equal(value.schema, "plimsoll.system-e2e-support-contract.v1");
  assert.equal(value.normalizationVersion, SUPPORT_NORMALIZATION_VERSION);
  assert.ok(Array.isArray(value.phases) && value.phases.length > 0);
  for (const [index, phase] of value.phases.entries()) {
    exactKeys(phase, ["name", "kind", "expectedArtifactDigest"], `support contract phase ${index}`);
    assert.match(String(phase.expectedArtifactDigest), /^sha256:[a-f0-9]{64}$/);
  }
  return value as SupportContract;
}

export function loadTamperContract(file: string): TamperContract {
  const value = JSON.parse(fs.readFileSync(file, "utf8")) as unknown;
  exactKeys(value, ["schema", "cases"], "tamper contract");
  assert.equal(value.schema, "plimsoll.system-e2e-tamper-cases.v1");
  assert.ok(Array.isArray(value.cases) && value.cases.length > 0);
  for (const [index, entry] of value.cases.entries()) {
    exactKeys(entry, ["id", "mutations", "resign", "expectedError"], `tamper case ${index}`);
    assert.match(String(entry.id), /^[a-z0-9_]+$/);
    assert.ok(Array.isArray(entry.mutations) && entry.mutations.length > 0);
    for (const [mutationIndex, mutation] of entry.mutations.entries()) {
      assert.ok(mutation && typeof mutation === "object" && !Array.isArray(mutation));
      const mutationRecord = mutation as Record<string, unknown>;
      const remove = mutationRecord.remove === true;
      exactKeys(
        mutation,
        remove ? ["pointer", "remove"] : ["pointer", "replacement"],
        `tamper case ${index} mutation ${mutationIndex}`,
      );
      assert.ok(String(mutationRecord.pointer).startsWith("/"));
    }
    assert.equal(typeof entry.resign, "boolean");
    assert.ok(String(entry.expectedError).length > 0);
  }
  return value as TamperContract;
}

export function loadRootGuardContract(file: string): RootGuardContract {
  const value = JSON.parse(fs.readFileSync(file, "utf8")) as unknown;
  exactKeys(value, ["schema", "guards"], "root guard contract");
  assert.equal(value.schema, "plimsoll.system-e2e-root-guards.v1");
  assert.ok(Array.isArray(value.guards) && value.guards.length > 0);
  for (const [index, guard] of value.guards.entries()) {
    exactKeys(guard, ["label", "expectedSentinelTreeDigest"], `root guard contract ${index}`);
    assert.match(String(guard.label), /^[a-z0-9_]+$/);
    assert.match(String(guard.expectedSentinelTreeDigest), /^sha256:[a-f0-9]{64}$/);
  }
  return value as RootGuardContract;
}

export function supportContractPath(root: string) {
  return path.join(root, "scripts", "system-e2e", "fixtures", "support-contract.json");
}

export function tamperContractPath(root: string) {
  return path.join(root, "scripts", "system-e2e", "fixtures", "tamper-cases.json");
}

export function rootGuardContractPath(root: string) {
  return path.join(root, "scripts", "system-e2e", "fixtures", "root-guards.json");
}
