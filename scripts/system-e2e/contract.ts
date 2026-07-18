import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export const SYSTEM_E2E_SCHEMA = "plimsoll.system-e2e-proof.v2" as const;
export const SUPPORT_NORMALIZATION_VERSION = 3 as const;
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

/**
 * Preserve the complete parsed result shape while replacing only explicitly
 * volatile values. Unknown/missing fields therefore change the committed
 * artifact digest instead of being silently ignored.
 */
export function normalizeSupportingArtifact(
  value: unknown,
  context: SupportingNormalizationContext,
  key = "root",
): unknown {
  if (Array.isArray(value)) {
    return value.map((child) => normalizeSupportingArtifact(child, context, key));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([childKey, child]) => [childKey, normalizeSupportingArtifact(child, context, childKey)]),
    );
  }
  if (typeof value === "string") return normalizeString(value, key, context);
  if (typeof value === "number" && VOLATILE_NUMBER_KEYS.test(key)) {
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
  assert.ok(Array.isArray(receipt.scenarios) && receipt.scenarios.length > 0);
  for (const [index, scenario] of receipt.scenarios.entries()) {
    if (scenario.status === "pass") {
      exactKeys(scenario, ["id", "required", "status", "detail", "durationMs", "counters", "measurements"], `resource scenario ${index}`);
    } else {
      exactKeys(scenario, ["id", "required", "status", "detail", "durationMs", "counters"], `resource scenario ${index}`);
      assert.equal(scenario.status, "skipped");
      assert.equal(scenario.required, false);
    }
  }
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
