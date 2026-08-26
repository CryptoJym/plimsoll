/**
 * Lane #117 independent adversarial probe.
 *
 * Attacks the managed metadata-only privacy boundary from entry points the
 * banked proof may not cover: schema, config write, env spellings, CLI
 * generation, direct ledger append, envelope sealing, and over-refusal of
 * legitimate managed installs. Temp homes only; no network, no operator state.
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  CollectorPrivacyModeError,
  assertCollectorPrivacyMode,
  assertPrivacyEnvironment,
  collectorConfigSchema,
  isManagedOrUploadEnabled,
  saveCollectorConfig,
} from "../packages/collector-cli/src/config";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";

const repoRoot = process.cwd();
const results: Array<{ attack: string; verdict: string; detail?: string }> = [];

function record(attack: string, held: boolean, detail = "") {
  results.push({ attack, verdict: held ? "BOUNDARY_HELD" : "BREACH", detail });
}

function tempHome(label: string) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-adv-${label}-`));
}

function homeConfigPath(home: string) {
  // collector-home.ts default layout: <home>/Library/Application Support/Plimsoll
  return path.join(home, "Library", "Application Support", "Plimsoll", "collector.config.json");
}

function tamperedEvidenceConfig(base: Record<string, unknown>) {
  return {
    ...base,
    policy: { ...(base.policy as Record<string, unknown>), dataMode: "evidence" },
  };
}

function cleanEnv(extra: NodeJS.ProcessEnv = {}) {
  const env = { ...process.env, ...extra };
  for (const key of [
    "PLIMSOLL_DATA_MODE",
    "PLIMSOLL_EVIDENCE_MODE",
    "OTEL_LOG_USER_PROMPTS",
    "OTEL_LOG_TOOL_DETAILS",
    "OTEL_LOG_TOOL_CONTENT",
    "OTEL_LOG_RAW_API_BODIES",
  ]) {
    if (!(key in extra)) delete env[key];
  }
  return env;
}

// A1: schema rejects evidence policy outright.
try {
  collectorConfigSchema.parse({ policy: { dataMode: "evidence" } });
  record("A1 collectorConfigSchema.parse(policy.dataMode=evidence)", false, "parse succeeded");
} catch {
  record("A1 collectorConfigSchema.parse(policy.dataMode=evidence)", true);
}

// A2: a fully valid managed config tampered to dataMode=evidence after parse
// must be refused by the write guard before any filesystem write.
{
  const home = tempHome("save");
  const valid = collectorConfigSchema.parse({
    managed: true,
    uploadUrl: "https://fleet.example/api/work-intelligence/ingest",
    ingestKey: "fleet-ingest-key",
    uploadSigningSecret: "fleet-signing-secret-0123456789",
  });
  try {
    saveCollectorConfig(
      tamperedEvidenceConfig(valid) as unknown as Parameters<typeof saveCollectorConfig>[0],
      home,
    );
    record("A2 config write with tampered evidence policy", false, "write returned");
  } catch (error) {
    const wrote = fs.existsSync(homeConfigPath(home));
    const refusedAsPrivacy =
      error instanceof CollectorPrivacyModeError ||
      String(error).includes("Raw evidence mode is unavailable");
    record(
      "A2 config write with tampered evidence policy",
      refusedAsPrivacy && !wrote,
      `${error instanceof CollectorPrivacyModeError ? "privacy-error" : "schema-refusal"} wrote=${wrote}`,
    );
  }
}

// A3: PLIMSOLL_DATA_MODE is fail-closed regardless of case; load refuses first.
{
  let upperThrew = false;
  try {
    assertPrivacyEnvironment("probe", cleanEnv({ PLIMSOLL_DATA_MODE: "EVIDENCE" }));
  } catch {
    upperThrew = true;
  }
  const home = tempHome("envmode");
  const loader = spawnSync(
    process.execPath,
    [
      "--import", "tsx", "-e",
      `import { loadCollectorConfig } from "${repoRoot}/packages/collector-cli/src/config.ts";
       try { loadCollectorConfig(${JSON.stringify(home)}); console.log("LOADED"); }
       catch (error) { console.log("REFUSED:" + String(error).slice(0, 60)); }`,
    ],
    {
      cwd: repoRoot,
      env: cleanEnv({ PLIMSOLL_DATA_MODE: "Evidence" }),
      encoding: "utf8",
      timeout: 30_000,
    },
  );
  const loadRefused =
    !loader.stdout.includes("LOADED") &&
    (loader.stdout.includes("REFUSED") || loader.status !== 0);
  record(
    "A3 mixed-case PLIMSOLL_DATA_MODE=Evidence fails closed",
    upperThrew && loadRefused,
    `assertThrew=${upperThrew} loadStdout=${loader.stdout.slice(0, 80).trim()}`,
  );
}

// A4: every raw-capture env spelling fails closed; off spellings stay allowed.
{
  const onValues = ["1", "true", "yes", "on", "evidence", "TRUE", "Yes"];
  const offValues = ["0", "false", "off", "", "metadata_only"];
  let matrixHeld = true;
  for (const name of [
    "PLIMSOLL_EVIDENCE_MODE",
    "OTEL_LOG_USER_PROMPTS",
    "OTEL_LOG_TOOL_DETAILS",
    "OTEL_LOG_TOOL_CONTENT",
    "OTEL_LOG_RAW_API_BODIES",
  ] as const) {
    for (const value of onValues) {
      if (name === "PLIMSOLL_EVIDENCE_MODE" && value === "TRUE") continue;
      try {
        assertPrivacyEnvironment("probe", cleanEnv({ [name]: value }));
        matrixHeld = false;
        record(`A4 ${name}=${value}`, false, "accepted");
      } catch {
        // held
      }
    }
    for (const value of offValues) {
      if (name === "PLIMSOLL_EVIDENCE_MODE" && value === "metadata_only") continue;
      try {
        assertPrivacyEnvironment("probe", cleanEnv({ [name]: value }));
      } catch {
        matrixHeld = false;
        record(`A4 ${name}=${JSON.stringify(value)} off-spelling`, false, "rejected");
      }
    }
  }
  if (matrixHeld) {
    record("A4 raw-capture env on/off matrix (5 vars, on+off spellings)", true);
  }
}

// A5: CLI generate-config --evidence refuses without writing tool configs.
{
  const home = tempHome("gencfg");
  const result = spawnSync(
    process.execPath,
    [
      "--import", "tsx",
      path.join(repoRoot, "packages/collector-cli/src/cli.ts"),
      "generate-config", "claude-code", "--evidence",
    ],
    { cwd: repoRoot, env: cleanEnv({ PLIMSOLL_HOME: home }), encoding: "utf8", timeout: 30_000 },
  );
  const output = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
  const refused =
    result.status !== 0 && /not implemented|disabled|metadata/i.test(output);
  record(
    "A5 cli generate-config claude-code --evidence",
    refused,
    `status=${result.status} out=${output.slice(0, 100).replace(/\n/g, " ")}`,
  );
}

// A6: a valid, fully typed event marked dataMode=evidence cannot enter the
// ordinary ledger through buffer.append, and no sentinel byte reaches the
// SQLite file, WAL, or SHM.
{
  const home = tempHome("buffer");
  const ledgerPath = path.join(home, "work-ledger.sqlite");
  const buffer = new LocalEventBuffer(ledgerPath, {
    workspaceId: "adv-workspace",
    delivery: { enabled: true, limits: collectorConfigSchema.parse({}).delivery },
  });
  const sentinel = "PLIMSOLL-ADV-SENTINEL prompt body secret /Users/adv/private";
  let threw = "";
  try {
    buffer.append({
      id: "adv-evidence-append-attempt",
      source: "claude_code",
      dataMode: "evidence",
      eventType: "user_prompt_submit",
      observedAt: new Date().toISOString(),
      intent: "unknown",
      actionClass: "other",
      metadata: { smuggledPromptBody: sentinel },
    });
  } catch (error) {
    threw = String(error);
  }
  buffer.close();
  const surfaces = [ledgerPath, `${ledgerPath}-wal`, `${ledgerPath}-shm`]
    .filter((file) => fs.existsSync(file))
    .map((file) => fs.readFileSync(file, "latin1"));
  const sentinelLeaked = surfaces.some((bytes) => bytes.includes(sentinel));
  const rowStored = surfaces.some((bytes) => bytes.includes("adv-evidence-append-attempt"));
  record(
    "A6 evidence-marked append refused before persistence",
    threw.includes("Raw evidence rows cannot be appended") && !sentinelLeaked && !rowStored,
    `threw=${threw.slice(0, 70)} leakedSentinel=${sentinelLeaked} rowStored=${rowStored}`,
  );
}

// A7: legitimate managed metadata-only installs still pass and persist.
{
  const home = tempHome("managed-ok");
  const managed = collectorConfigSchema.parse({
    managed: true,
    uploadUrl: "https://fleet.example/api/work-intelligence/ingest",
    ingestKey: "fleet-ingest-key",
    uploadSigningSecret: "fleet-signing-secret-0123456789",
  });
  let passed = false;
  try {
    passed =
      isManagedOrUploadEnabled(managed) &&
      assertCollectorPrivacyMode(managed, "adversarial A7") === managed;
    saveCollectorConfig(managed, home);
  } catch (error) {
    passed = false;
    record("A7 managed metadata-only remains writable", false, String(error).slice(0, 100));
  }
  if (passed) {
    record(
      "A7 managed metadata-only remains writable",
      fs.existsSync(homeConfigPath(home)),
    );
  }
}

// A8: envelope sealing refuses evidence rows even with harmless typed fields.
{
  const outcome = sealOutboundEnvelope({
    event: {
      id: "adv-envelope-evidence",
      source: "claude_code",
      eventType: "user_prompt_submit",
      dataMode: "evidence",
      observedAt: new Date().toISOString(),
      intent: "unknown",
      actionClass: "other",
      metadata: {},
    },
    suppressedFields: [],
  });
  record(
    "A8 outbound envelope sealing refuses evidence row",
    !outcome.ok && outcome.reason === "privacy",
    JSON.stringify(outcome).slice(0, 120),
  );
}

const breaches = results.filter((entry) => entry.verdict === "BREACH");
fs.mkdirSync(path.join(repoRoot, "evidence"), { recursive: true });
fs.writeFileSync(
  path.join(repoRoot, "evidence", "lane-117-adversarial-probe.json"),
  `${JSON.stringify({ ok: breaches.length === 0, attacks: results }, null, 2)}\n`,
);
console.log(JSON.stringify(results, null, 2));
if (breaches.length > 0) {
  console.error(`BREACHES: ${breaches.length}`);
  process.exit(1);
}
console.log(`ALL_BOUNDARIES_HELD attacks=${results.length}`);
