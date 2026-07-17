/**
 * Issue #117 privacy-mode proof.
 *
 * Every fixture runs under a fresh temp home. The proof never opens the
 * operator ledger, changes LaunchAgents, or contacts a non-loopback service.
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  collectorConfigPath,
  collectorConfigSchema,
  saveCollectorConfig,
  type CollectorConfig,
} from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { performJoin } from "../packages/collector-cli/src/join";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { runWorkspaceHistoryUpload } from "../packages/collector-cli/src/upload-history";
import {
  aiInteractionEventSchema,
  policyConfigSchema,
} from "../packages/shared/src/index";

type Check = { name: string; passed: boolean; detail: Record<string, unknown> };
type SentinelFixture = {
  schemaVersion: number;
  prefixLength: number;
  sentinels: Record<string, string>;
};

const startedAt = Date.now();
const repoRoot = process.cwd();
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-privacy-mode-proof-"));
const checks: Check[] = [];
const cliSource = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const fixture = JSON.parse(
  fs.readFileSync(
    path.join(repoRoot, "scripts", "resource-proof", "fixtures", "metadata-privacy-sentinels.json"),
    "utf8",
  ),
) as SentinelFixture;
const sentinelValues = Object.values(fixture.sentinels);
const sentinelPrefixes = sentinelValues.map((value) => value.slice(0, fixture.prefixLength));
const privateTerms = [...sentinelValues, ...sentinelPrefixes];

function record(name: string, passed: boolean, detail: Record<string, unknown> = {}) {
  checks.push({ name, passed, detail });
  console.log(`${passed ? "PASS" : "FAIL"} ${name}`);
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
  env.PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS = "10";
  return env;
}

function runCli(home: string, args: string[], extraEnv: NodeJS.ProcessEnv = {}) {
  const result = spawnSync(process.execPath, ["--import", "tsx", cliSource, ...args], {
    cwd: repoRoot,
    env: cleanEnv({ PLIMSOLL_HOME: home, ...extraEnv }),
    encoding: "utf8",
    timeout: 15_000,
  });
  return {
    status: result.status,
    signal: result.signal,
    output: `${result.stdout ?? ""}\n${result.stderr ?? ""}`,
  };
}

function hasPrivateTerm(value: string | Buffer) {
  if (typeof value === "string") return privateTerms.some((term) => value.includes(term));
  return privateTerms.some((term) => value.includes(Buffer.from(term)));
}

function fileSurfaces(file: string) {
  return [file, `${file}-wal`, `${file}-shm`]
    .filter((candidate) => fs.existsSync(candidate))
    .map((candidate) => fs.readFileSync(candidate));
}

function directoryBytes(directory: string): number {
  let total = 0;
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const target = path.join(directory, entry.name);
    total += entry.isDirectory() ? directoryBytes(target) : fs.statSync(target).size;
  }
  return total;
}

async function listen(server: http.Server) {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  return (server.address() as AddressInfo).port;
}

async function close(server: http.Server) {
  await new Promise<void>((resolve) => server.close(() => resolve()));
}

async function main() {
  if (Number(process.versions.node.split(".")[0]) !== 22) {
    throw new Error(`Privacy proof requires Node 22; received ${process.version}`);
  }
  if (fixture.schemaVersion !== 1 || fixture.prefixLength < 8) {
    throw new Error("Privacy sentinel fixture is invalid.");
  }

  const evidencePolicy = policyConfigSchema.parse(
    JSON.parse(
      fs.readFileSync(
        path.join(repoRoot, "packages", "shared", "fixtures", "policies", "evidence-policy.json"),
        "utf8",
      ),
    ),
  );
  const metadataManagedConfig = collectorConfigSchema.parse({
    managed: true,
    uploadUrl: "http://127.0.0.1:1/ingest",
    installKey: "privacy-proof-managed-install",
  });
  const evidenceConfig = {
    ...metadataManagedConfig,
    policy: evidencePolicy,
  } as CollectorConfig;
  const schemaRejected = !collectorConfigSchema.safeParse(evidenceConfig).success;

  const saveHome = path.join(root, "config-write-home");
  let saveRejected = false;
  try {
    saveCollectorConfig(evidenceConfig, saveHome);
  } catch (error) {
    saveRejected = /vault is not implemented|evidence/i.test(String(error));
  }
  record(
    "managed_config_write_rejects_evidence_before_filesystem_write",
    schemaRejected && saveRejected && !fs.existsSync(collectorConfigPath(saveHome)),
    { schemaRejected, configWritten: fs.existsSync(collectorConfigPath(saveHome)) },
  );

  const envHome = path.join(root, "env-home");
  const envMode = runCli(envHome, ["status"], { PLIMSOLL_DATA_MODE: "evidence" });
  const envRaw = runCli(envHome, ["status"], { OTEL_LOG_USER_PROMPTS: "1" });
  record(
    "environment_enable_attempts_fail_before_config_or_ledger_write",
    envMode.status !== 0 &&
      envRaw.status !== 0 &&
      /vault is not implemented/i.test(envMode.output) &&
      /vault is not implemented/i.test(envRaw.output) &&
      !fs.existsSync(path.join(envHome, "collector.config.json")) &&
      !fs.existsSync(path.join(envHome, "work-ledger.sqlite")),
    { modeExit: envMode.status, rawFlagExit: envRaw.status },
  );

  const cliEvidenceHome = path.join(root, "cli-evidence-home");
  const cliEvidence = runCli(cliEvidenceHome, [
    "generate-config",
    "all",
    "--evidence",
    "--confirm-evidence",
  ]);
  record(
    "cli_evidence_enable_attempt_has_no_silent_downgrade",
    cliEvidence.status !== 0 &&
      /vault is not implemented/i.test(cliEvidence.output) &&
      !cliEvidence.output.includes("OTEL_LOG_USER_PROMPTS\": \"1"),
    { exit: cliEvidence.status },
  );

  const blockedHome = path.join(root, "blocked-home");
  fs.mkdirSync(blockedHome, { recursive: true, mode: 0o700 });
  const blockedConfigPath = path.join(blockedHome, "collector.config.json");
  const blockedBytes = `${JSON.stringify(evidenceConfig, null, 2)}\n`;
  fs.writeFileSync(blockedConfigPath, blockedBytes, { mode: 0o600 });
  const setupClaude = path.join(root, "blocked-settings.json");
  const setupCodex = path.join(root, "blocked-config.toml");
  const blockedStart = runCli(blockedHome, ["start"]);
  const blockedRestart = runCli(blockedHome, ["start"]);
  const blockedSetup = runCli(blockedHome, [
    "setup",
    "--yes",
    "--claude-settings",
    setupClaude,
    "--codex-config",
    setupCodex,
  ]);
  const blockedJoin = runCli(blockedHome, [
    "join",
    "pljt_privacy_proof",
    "--url",
    "http://127.0.0.1:1",
  ]);
  record(
    "start_restart_setup_and_join_reject_evidence_before_side_effects",
    [blockedStart, blockedRestart, blockedSetup, blockedJoin].every(
      (result) => result.status !== 0 && /vault is not implemented/i.test(result.output),
    ) &&
      fs.readFileSync(blockedConfigPath, "utf8") === blockedBytes &&
      !fs.existsSync(path.join(blockedHome, "work-ledger.sqlite")) &&
      !fs.existsSync(path.join(blockedHome, "collector.pid")) &&
      !fs.existsSync(setupClaude) &&
      !fs.existsSync(setupCodex),
    {
      startExit: blockedStart.status,
      restartExit: blockedRestart.status,
      setupExit: blockedSetup.status,
      joinExit: blockedJoin.status,
    },
  );

  const joinHome = path.join(root, "direct-join-home");
  const joinConfigPath = collectorConfigPath(joinHome);
  fs.mkdirSync(path.dirname(joinConfigPath), { recursive: true, mode: 0o700 });
  fs.writeFileSync(joinConfigPath, blockedBytes, { mode: 0o600 });
  let joinFetches = 0;
  let directJoinRejected = false;
  try {
    await performJoin({
      target: "pljt_privacy_proof",
      baseUrl: "http://127.0.0.1:1",
      homeDir: joinHome,
      fetchImpl: async () => {
        joinFetches += 1;
        return new Response("{}", { status: 500 });
      },
    });
  } catch (error) {
    directJoinRejected = /vault is not implemented|evidence/i.test(String(error));
  }
  record(
    "direct_join_rejects_before_token_redemption_or_config_write",
    directJoinRejected && joinFetches === 0 && fs.readFileSync(joinConfigPath, "utf8") === blockedBytes,
    { fetches: joinFetches },
  );

  const malformedHome = path.join(root, "malformed-home");
  fs.mkdirSync(malformedHome, { recursive: true, mode: 0o700 });
  const malformedPath = path.join(malformedHome, "collector.config.json");
  const malformedBytes = '{"policy":{"dataMode":"evidence"';
  fs.writeFileSync(malformedPath, malformedBytes, { mode: 0o600 });
  const malformed = runCli(malformedHome, ["start"]);
  record(
    "malformed_config_fails_closed_without_rewrite_or_ledger_open",
    malformed.status !== 0 &&
      fs.readFileSync(malformedPath, "utf8") === malformedBytes &&
      !fs.existsSync(path.join(malformedHome, "work-ledger.sqlite")),
    { exit: malformed.status },
  );

  const metadataHome = path.join(root, "metadata-home");
  const metadataClaude = path.join(root, "metadata-settings.json");
  const metadataCodex = path.join(root, "metadata-config.toml");
  const setup = runCli(metadataHome, [
    "setup",
    "--yes",
    "--claude-settings",
    metadataClaude,
    "--codex-config",
    metadataCodex,
  ]);
  const status = runCli(metadataHome, ["status"]);
  const doctor = runCli(metadataHome, ["doctor"]);
  const generatedSettings = JSON.parse(fs.readFileSync(metadataClaude, "utf8")) as {
    env?: Record<string, string>;
  };
  const generatedCodex = fs.readFileSync(metadataCodex, "utf8");
  record(
    "metadata_only_setup_status_and_doctor_remain_operational_and_literal",
    setup.status === 0 &&
      status.status === 0 &&
      doctor.status === 0 &&
      generatedSettings.env?.OTEL_LOG_USER_PROMPTS === "0" &&
      generatedSettings.env?.OTEL_LOG_TOOL_DETAILS === "0" &&
      generatedSettings.env?.OTEL_LOG_TOOL_CONTENT === "0" &&
      generatedSettings.env?.OTEL_LOG_RAW_API_BODIES === "0" &&
      generatedCodex.includes("log_user_prompt = false") &&
      status.output.includes('"privacyMode": "metadata_only"') &&
      doctor.output.includes('"privacyMode": "metadata_only"') &&
      doctor.output.includes('"evidenceVault": "not_implemented"') &&
      doctor.output.includes('"legacyEvidenceDisposition": "local_quarantine_migration_required"'),
    { setupExit: setup.status, statusExit: status.status, doctorExit: doctor.status },
  );

  const captureHome = path.join(root, "capture-home");
  fs.mkdirSync(captureHome, { recursive: true, mode: 0o700 });
  const captureLedger = path.join(captureHome, "work-ledger.sqlite");
  const metadataConfig = collectorConfigSchema.parse({
    managed: true,
    uploadUrl: "http://127.0.0.1/fake-ingest",
    installKey: "privacy-proof-metadata-install",
  });
  const captureBuffer = new LocalEventBuffer(captureLedger, {
    delivery: { enabled: true, limits: metadataConfig.delivery },
  });
  const hookPayload = {
    id: "11711711-1111-4111-8111-111111111117",
    event_type: "PostToolUse",
    tool_name: "Bash",
    prompt: fixture.sentinels.prompt,
    command: fixture.sentinels.toolArguments,
    args: [fixture.sentinels.toolArguments],
    stack: fixture.sentinels.response,
    path: fixture.sentinels.absolutePath,
    content: fixture.sentinels.multibyte,
    provider_body: fixture.sentinels.response,
    secret: fixture.sentinels.credential,
    email: fixture.sentinels.email,
    cwd: fixture.sentinels.absolutePath,
  };
  const captureReceipt = appendForwardedHook(hookPayload, {
    config: metadataConfig,
    buffer: captureBuffer,
    source: "codex",
  });
  const dashboardServer = createCollectorServer(metadataConfig, captureBuffer);
  const dashboardPort = await listen(dashboardServer);
  const dashboardStatus = await (await fetch(`http://127.0.0.1:${dashboardPort}/status`)).text();
  const dashboardHtml = await (await fetch(`http://127.0.0.1:${dashboardPort}/`)).text();
  const uploadBodies: string[] = [];
  const upload = await uploadBufferedEvents(metadataConfig, captureBuffer, {
    fetchImpl: async (_input, init) => {
      uploadBodies.push(String(init?.body ?? ""));
      return new Response(JSON.stringify({ accepted: 1 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });
  const openLedgerFiles = fileSurfaces(captureLedger);
  await close(dashboardServer);
  captureBuffer.close();
  const closedLedgerFiles = fileSurfaces(captureLedger);
  const captureSurfaces: Array<string | Buffer> = [
    JSON.stringify(captureReceipt),
    dashboardStatus,
    dashboardHtml,
    ...uploadBodies,
    ...openLedgerFiles,
    ...closedLedgerFiles,
  ];
  record(
    "raw_prompt_command_args_stack_path_content_provider_secret_and_pii_never_reach_surfaces",
    upload.uploadedEvents === 1 &&
      uploadBodies.length === 1 &&
      openLedgerFiles.length >= 2 &&
      captureSurfaces.every((surface) => !hasPrivateTerm(surface)),
    {
      uploadCalls: uploadBodies.length,
      openLedgerArtifacts: openLedgerFiles.length,
      surfacesScanned: captureSurfaces.length,
    },
  );

  const legacyHome = path.join(root, "legacy-home");
  fs.mkdirSync(legacyHome, { recursive: true, mode: 0o700 });
  const legacyLedger = path.join(legacyHome, "work-ledger.sqlite");
  let legacyBuffer = new LocalEventBuffer(legacyLedger);
  const evidenceEvent = {
    id: "11711711-1111-4111-8111-111111111118",
    source: "codex",
    dataMode: "evidence",
    eventType: "user_prompt_submit",
    observedAt: "2026-07-17T12:00:00.000Z",
    actionClass: "other",
    metadata: { prompt: fixture.sentinels.prompt },
  };
  const evidenceInsert = legacyBuffer.database
    .prepare(
      `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at)
       values (?,?,?,?,?,?,?,?)`,
    )
    .run(
      evidenceEvent.id,
      evidenceEvent.source,
      evidenceEvent.eventType,
      evidenceEvent.dataMode,
      evidenceEvent.observedAt,
      JSON.stringify(evidenceEvent),
      "[]",
      evidenceEvent.observedAt,
    );
  const evidenceRawRowid = Number(evidenceInsert.lastInsertRowid);
  const safeEvent = aiInteractionEventSchema.parse({
    id: "11711711-1111-4111-8111-111111111119",
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: "2026-07-17T12:00:01.000Z",
    actionClass: "other",
    inputTokens: 1,
    outputTokens: 1,
    metadata: { admission: "privacy-proof" },
  });
  legacyBuffer.append(safeEvent);
  legacyBuffer.close();
  legacyBuffer = new LocalEventBuffer(legacyLedger, {
    delivery: { enabled: true, limits: metadataConfig.delivery },
  });
  const outboxInsert = legacyBuffer.database.prepare(
    `insert into upload_outbox
     (delivery_id,raw_rowid,base_envelope_json,base_bytes,sealed_envelope_json,sealed_bytes,
      state,attempt_count,next_attempt_at,last_failure_class,created_at,updated_at)
     values (@id,@rawRowid,@envelope,@bytes,@envelope,@bytes,
      'pending',0,@at,'none',@at,@at)`,
  );
  const canonicalMetadataEnvelope = (id: string, observedAt: string) => {
    const sealed = sealOutboundEnvelope({
      event: { ...safeEvent, id, observedAt, metadata: {} },
      suppressedFields: [],
    });
    if (!sealed.ok) throw new Error(`Could not construct canonical metadata envelope: ${sealed.reason}`);
    return JSON.stringify(sealed.envelope);
  };
  const staleEnvelopeIds = [
    "11711711-1111-4111-8111-111111111120",
    "11711711-1111-4111-8111-111111111121",
  ];
  for (const [index, id] of staleEnvelopeIds.entries()) {
    const envelope = canonicalMetadataEnvelope(id, `2026-07-17T12:00:0${index + 2}.000Z`);
    outboxInsert.run({
      id,
      rawRowid: evidenceRawRowid,
      envelope,
      bytes: Buffer.byteLength(envelope),
      at: `2026-07-17T12:00:00.${index + 5}00Z`,
    });
  }

  const nullRawId = "11711711-1111-4111-8111-111111111122";
  const nullRawEnvelope = canonicalMetadataEnvelope(nullRawId, "2026-07-17T12:00:04.000Z");
  outboxInsert.run({
    id: nullRawId,
    rawRowid: null,
    envelope: nullRawEnvelope,
    bytes: Buffer.byteLength(nullRawEnvelope),
    at: "2026-07-17T12:00:04.000Z",
  });

  const deletedRawId = "11711711-1111-4111-8111-111111111123";
  const deletedRaw = legacyBuffer.database
    .prepare(
      `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at)
       values (@id,'codex','assistant_response','metadata',@at,@payload,'[]',@at)`,
    )
    .run({
      id: deletedRawId,
      at: "2026-07-17T12:00:05.000Z",
      payload: canonicalMetadataEnvelope(deletedRawId, "2026-07-17T12:00:05.000Z"),
    });
  const deletedRawEnvelope = canonicalMetadataEnvelope(deletedRawId, "2026-07-17T12:00:05.000Z");
  outboxInsert.run({
    id: deletedRawId,
    rawRowid: Number(deletedRaw.lastInsertRowid),
    envelope: deletedRawEnvelope,
    bytes: Buffer.byteLength(deletedRawEnvelope),
    at: "2026-07-17T12:00:05.000Z",
  });
  legacyBuffer.database
    .prepare(`delete from buffered_events where rowid = ?`)
    .run(Number(deletedRaw.lastInsertRowid));

  const terminalRawId = "11711711-1111-4111-8111-111111111124";
  const terminalRaw = legacyBuffer.database
    .prepare(
      `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at)
       values (@id,'codex','assistant_response','metadata',@at,@payload,'[]',@at)`,
    )
    .run({
      id: terminalRawId,
      at: "2026-07-17T12:00:06.000Z",
      payload: JSON.stringify({ ...safeEvent, id: terminalRawId, observedAt: "2026-07-17T12:00:06.000Z" }),
    });
  const terminalEnvelope = canonicalMetadataEnvelope(terminalRawId, "2026-07-17T12:00:06.000Z");
  outboxInsert.run({
    id: terminalRawId,
    rawRowid: Number(terminalRaw.lastInsertRowid),
    envelope: terminalEnvelope,
    bytes: Buffer.byteLength(terminalEnvelope),
    at: "2026-07-17T12:00:06.000Z",
  });
  legacyBuffer.database
    .prepare(
      `insert into upload_receipts
       (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
       values (?,'dead','local_evidence_quarantined','local_validation',0,?,?)`,
    )
    .run(terminalRawId, "2026-07-17T12:00:06.000Z", "2026-07-17T12:00:06.500Z");
  const migration = legacyBuffer.delivery.migrateLegacy();
  const legacyUploadBodies: string[] = [];
  const legacyUpload = await uploadBufferedEvents(metadataConfig, legacyBuffer, {
    fetchImpl: async (_input, init) => {
      legacyUploadBodies.push(String(init?.body ?? ""));
      return new Response(JSON.stringify({ accepted: 1 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });
  const evidenceRow = legacyBuffer.database
    .prepare(`select uploaded_at as uploadedAt from buffered_events where id = ?`)
    .get(evidenceEvent.id) as { uploadedAt: string | null };
  const quarantineReceipt = legacyBuffer.database
    .prepare(`select reason from upload_receipts where delivery_id = ?`)
    .get(evidenceEvent.id) as { reason: string } | undefined;
  const staleReceipts = staleEnvelopeIds.map(
    (id) => legacyBuffer.database
      .prepare(`select reason from upload_receipts where delivery_id = ?`)
      .get(id) as { reason: string } | undefined,
  );
  const lineageReceipts = [nullRawId, deletedRawId, terminalRawId].map(
    (id) => legacyBuffer.database
      .prepare(`select reason from upload_receipts where delivery_id = ?`)
      .get(id) as { reason: string } | undefined,
  );
  const activeInvalidRows = (
    legacyBuffer.database.prepare(
      `select count(*) as n from upload_outbox where delivery_id in (${[
        ...staleEnvelopeIds,
        nullRawId,
        deletedRawId,
        terminalRawId,
      ].map(() => "?").join(",")})`,
    ).get(...staleEnvelopeIds, nullRawId, deletedRawId, terminalRawId) as { n: number }
  ).n;
  const statelessBodies: string[] = [];
  const statelessUpload = await uploadBufferedEvents(metadataConfig, legacyBuffer, {
    markUploaded: false,
    fetchImpl: async (_input, init) => {
      statelessBodies.push(String(init?.body ?? ""));
      return new Response(JSON.stringify({ accepted: 1 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });
  const deliveryStatus = legacyBuffer.delivery.status();
  legacyBuffer.close();
  legacyBuffer = new LocalEventBuffer(legacyLedger, {
    delivery: { enabled: true, limits: metadataConfig.delivery },
  });
  const reopenUploadBodies: string[] = [];
  const reopenUpload = await uploadBufferedEvents(metadataConfig, legacyBuffer, {
    fetchImpl: async (_input, init) => {
      reopenUploadBodies.push(String(init?.body ?? ""));
      return new Response(JSON.stringify({ accepted: 1 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });
  const reopenedInvalidRows = (
    legacyBuffer.database.prepare(
      `select count(*) as n from upload_outbox where delivery_id in (${[
        ...staleEnvelopeIds,
        nullRawId,
        deletedRawId,
        terminalRawId,
      ].map(() => "?").join(",")})`,
    ).get(...staleEnvelopeIds, nullRawId, deletedRawId, terminalRawId) as { n: number }
  ).n;
  legacyBuffer.close();

  const historyBodies: string[] = [];
  const historyLogs: string[] = [];
  const history = await runWorkspaceHistoryUpload(metadataConfig, {
    full: true,
    ledgerPath: legacyLedger,
    statePath: path.join(legacyHome, "backfill-state.json"),
    delayMs: 0,
    sleep: async () => undefined,
    log: (line) => historyLogs.push(line),
    fetchImpl: async (_input, init) => {
      historyBodies.push(String(init?.body ?? ""));
      const body = JSON.parse(String(init?.body ?? "{}")) as { events?: unknown[] };
      return new Response(JSON.stringify({ accepted: body.events?.length ?? 0 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });
  const legacyOutboundSurfaces = [...legacyUploadBodies, ...historyBodies, ...historyLogs];
  record(
    "canonical_presealed_metadata_envelope_cannot_override_linked_evidence_row",
    staleReceipts.length === 2 &&
      staleReceipts.every((receipt) => receipt?.reason === "local_evidence_quarantined") &&
      evidenceRow.uploadedAt === null &&
      legacyUploadBodies.length === 1 &&
      legacyUploadBodies.every((body) => !hasPrivateTerm(body)),
    {
      duplicateDeliveries: staleReceipts.length,
      receipts: staleReceipts.map((receipt) => receipt?.reason ?? null),
      evidenceMarkedUploaded: evidenceRow.uploadedAt !== null,
      wireCalls: legacyUploadBodies.length,
    },
  );
  record(
    "legacy_evidence_rows_are_quarantined_and_never_uploaded",
    migration.quarantinedEvidence === 1 &&
      legacyUpload.uploadedEvents === 1 &&
      evidenceRow.uploadedAt === null &&
      quarantineReceipt?.reason === "local_evidence_quarantined" &&
      staleReceipts.every((receipt) => receipt?.reason === "local_evidence_quarantined") &&
      lineageReceipts[0]?.reason === "local_privacy_violation" &&
      lineageReceipts[1]?.reason === "local_privacy_violation" &&
      lineageReceipts[2]?.reason === "local_evidence_quarantined" &&
      activeInvalidRows === 0 &&
      statelessUpload.uploadedEvents === 0 &&
      statelessBodies.length === 0 &&
      reopenUpload.uploadedEvents === 0 &&
      reopenUploadBodies.length === 0 &&
      reopenedInvalidRows === 0 &&
      history.audit.skipped.local_evidence_quarantine_migration_required === 2 &&
      history.sentEvents === 1 &&
      legacyOutboundSurfaces.every((surface) => !hasPrivateTerm(surface)) &&
      deliveryStatus.privacy.legacyEvidenceDisposition ===
        "local_quarantine_migration_required" &&
      deliveryStatus.work.rawRowsScanned === 0,
    {
      quarantined: migration.quarantinedEvidence,
      staleEnvelopeReceipts: staleReceipts.map((receipt) => receipt?.reason ?? null),
      lineageReceipts: lineageReceipts.map((receipt) => receipt?.reason ?? null),
      activeInvalidRows,
      statelessUploadCalls: statelessBodies.length,
      reopenedInvalidRows,
      reopenUploadCalls: reopenUploadBodies.length,
      uploadedMetadataRows: legacyUpload.uploadedEvents,
      historySkipped: history.audit.skipped.local_evidence_quarantine_migration_required ?? 0,
      readinessRawScans: deliveryStatus.work.rawRowsScanned,
    },
  );

  const raceHome = path.join(root, "race-home");
  fs.mkdirSync(raceHome, { recursive: true, mode: 0o700 });
  const raceLedger = path.join(raceHome, "work-ledger.sqlite");
  let raceBuffer = new LocalEventBuffer(raceLedger, {
    delivery: { enabled: true, limits: metadataConfig.delivery },
  });
  const beforeRemoteRace = aiInteractionEventSchema.parse({
    ...safeEvent,
    id: "11711711-1111-4111-8111-111111111125",
    observedAt: "2026-07-17T12:00:07.000Z",
    metadata: { admission: "before-remote-race" },
  });
  raceBuffer.append(beforeRemoteRace);
  let beforeRemoteMutated = false;
  const beforeRemoteBodies: string[] = [];
  const beforeRemoteUpload = await uploadBufferedEvents(metadataConfig, raceBuffer, {
    beforeRemote: () => {
      if (beforeRemoteMutated) return;
      beforeRemoteMutated = true;
      raceBuffer.database
        .prepare(`update buffered_events set data_mode = 'evidence', payload_json = ? where id = ?`)
        .run(
          JSON.stringify({
            ...beforeRemoteRace,
            dataMode: "evidence",
            metadata: { prompt: fixture.sentinels.prompt },
          }),
          beforeRemoteRace.id,
        );
    },
    fetchImpl: async (_input, init) => {
      beforeRemoteBodies.push(String(init?.body ?? ""));
      return new Response(JSON.stringify({ accepted: 1 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });

  const afterRemoteRace = aiInteractionEventSchema.parse({
    ...safeEvent,
    id: "11711711-1111-4111-8111-111111111126",
    observedAt: "2026-07-17T12:00:08.000Z",
    metadata: { admission: "after-remote-race" },
  });
  raceBuffer.append(afterRemoteRace);
  let afterRemoteMutated = false;
  const afterRemoteBodies: string[] = [];
  const afterRemoteUpload = await uploadBufferedEvents(metadataConfig, raceBuffer, {
    afterRemote: () => {
      if (afterRemoteMutated) return;
      afterRemoteMutated = true;
      raceBuffer.database
        .prepare(`update buffered_events set data_mode = 'evidence', payload_json = ? where id = ?`)
        .run(
          JSON.stringify({
            ...afterRemoteRace,
            dataMode: "evidence",
            metadata: { prompt: fixture.sentinels.response },
          }),
          afterRemoteRace.id,
        );
    },
    fetchImpl: async (_input, init) => {
      afterRemoteBodies.push(String(init?.body ?? ""));
      return new Response(JSON.stringify({ accepted: 1 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });
  const raceRows = raceBuffer.database
    .prepare(`select id, uploaded_at as uploadedAt from buffered_events order by id`)
    .all() as Array<{ id: string; uploadedAt: string | null }>;
  const raceReceipts = raceBuffer.database
    .prepare(`select delivery_id as deliveryId, reason from upload_receipts order by delivery_id`)
    .all() as Array<{ deliveryId: string; reason: string }>;
  raceBuffer.close();
  raceBuffer = new LocalEventBuffer(raceLedger, {
    delivery: { enabled: true, limits: metadataConfig.delivery },
  });
  const raceReopenBodies: string[] = [];
  const raceReopenUpload = await uploadBufferedEvents(metadataConfig, raceBuffer, {
    fetchImpl: async (_input, init) => {
      raceReopenBodies.push(String(init?.body ?? ""));
      return new Response(JSON.stringify({ accepted: 1 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });
  const raceActive = (
    raceBuffer.database.prepare(`select count(*) as n from upload_outbox`).get() as { n: number }
  ).n;
  raceBuffer.close();
  record(
    "lease_export_ack_races_and_reopen_remain_terminal_and_local",
    beforeRemoteMutated &&
      beforeRemoteBodies.length === 0 &&
      beforeRemoteUpload.uploadedEvents === 0 &&
      afterRemoteMutated &&
      afterRemoteBodies.length === 1 &&
      afterRemoteBodies.every((body) => !hasPrivateTerm(body)) &&
      afterRemoteUpload.uploadedEvents === 0 &&
      afterRemoteUpload.markedUploaded === 0 &&
      raceRows.every((row) => row.uploadedAt === null) &&
      raceReceipts.length === 2 &&
      raceReceipts.every((receipt) => receipt.reason === "local_evidence_quarantined") &&
      !hasPrivateTerm(JSON.stringify(raceReceipts)) &&
      raceReopenUpload.uploadedEvents === 0 &&
      raceReopenBodies.length === 0 &&
      raceActive === 0,
    {
      beforeRemoteCalls: beforeRemoteBodies.length,
      afterRemoteCalls: afterRemoteBodies.length,
      reopenedCalls: raceReopenBodies.length,
      uploadedMarkers: raceRows.filter((row) => row.uploadedAt !== null).length,
      terminalReceipts: raceReceipts.length,
      activeAfterReopen: raceActive,
    },
  );

  const durationMs = Date.now() - startedAt;
  const tempBytes = directoryBytes(root);
  record(
    "isolated_temp_home_resource_budget",
    durationMs < 60_000 && tempBytes < 64 * 1024 * 1024,
    { durationMs, tempBytes, durationBudgetMs: 60_000, byteBudget: 64 * 1024 * 1024 },
  );

  const receiptArg = process.argv.indexOf("--receipt");
  const receiptPath = path.resolve(
    receiptArg >= 0 && process.argv[receiptArg + 1]
      ? process.argv[receiptArg + 1]
      : path.join(repoRoot, "evidence", "privacy-mode-proof.json"),
  );
  const receipt = {
    schemaVersion: 1,
    issue: 117,
    mode: "metadata_only",
    evidenceVault: "not_implemented",
    legacyEvidenceDisposition: "local_quarantine_migration_required",
    node: process.version,
    passed: checks.every((check) => check.passed),
    checks,
    measurements: { durationMs, tempBytes, tempHomes: 9 },
  };
  fs.mkdirSync(path.dirname(receiptPath), { recursive: true });
  fs.writeFileSync(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`);
  record(
    "proof_receipt_contains_no_private_sentinel",
    !hasPrivateTerm(fs.readFileSync(receiptPath)),
    { receipt: path.relative(repoRoot, receiptPath) },
  );

  // Refresh the receipt with its own final check included.
  receipt.passed = checks.every((check) => check.passed);
  receipt.checks = checks;
  fs.writeFileSync(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`);
  console.log(
    JSON.stringify({
      passed: receipt.passed,
      checks: checks.length,
      failures: checks.filter((check) => !check.passed).map((check) => check.name),
      receipt: path.relative(repoRoot, receiptPath),
    }),
  );
  if (!receipt.passed) process.exitCode = 1;
}

main()
  .catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  })
  .finally(() => {
    fs.rmSync(root, { recursive: true, force: true });
  });
