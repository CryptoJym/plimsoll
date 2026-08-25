#!/usr/bin/env node
/**
 * Issue 0089 — Enrollment privacy: future-only join must quarantine prior
 * history.
 *
 * Adversarial contract: every check here FAILS against a join path that
 * relabels or uploads pre-enrollment rows, and PASSES only when enrollment is
 * strictly future-only. Fixtures live entirely in a temp directory; no real
 * collector home, sqlite file, launchd job, or live process is touched.
 */

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  collectorBufferPath,
  collectorConfigPath,
  collectorConfigSchema,
  type CollectorConfig,
} from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import {
  COLLECTOR_APP_VERSION,
  JOIN_HANDSHAKE_DIRECTORY_PREFIX,
  performJoin,
} from "../packages/collector-cli/src/join";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { LOCAL_TENANT_ID } from "../packages/shared/src/index";
import { installProofCompletionGuard } from "./lib/completion-guard";

type Check = { name: string; adversarial: boolean; detail: Record<string, unknown> };
type RequestRecord = { init?: RequestInit; url: string; body: Record<string, unknown> };

const checks: Check[] = [];

// Issue #210: refuse silent partial runs (see scripts/lib/completion-guard.ts).
const EXPECTED_CHECKS = 21;
const completion = installProofCompletionGuard({ proof: "enrollment-privacy-proof", expectedChecks: EXPECTED_CHECKS });
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-enrollment-privacy-proof-"));
const originalPlimsollHome = process.env.PLIMSOLL_HOME;
delete process.env.PLIMSOLL_HOME;

const TENANT_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const TENANT_C = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const INSTALL_B = "pli_workspace_b_install";
const INSTALL_C = "pli_workspace_c_install";
const TOKEN = "pljt_enrollment_privacy_single_use_token";
const LEGACY_EVENT_IDS = [
  "legacy_pre_enrollment_event_one",
  "legacy_pre_enrollment_event_two",
];
const POST_JOIN_EVENT_ID = "post_join_workspace_b_event";
const REJOIN_POST_EVENT_ID = "post_rejoin_workspace_b_event";

const defaultConfig = (): CollectorConfig => collectorConfigSchema.parse({});

function check(
  name: string,
  adversarial: boolean,
  condition: unknown,
  detail: Record<string, unknown>,
) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  completion.check(name);
  checks.push({ name, adversarial, detail });
}

function home(name: string) {
  return path.join(root, name);
}

function responseJson(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function requestUrl(input: Parameters<typeof fetch>[0]) {
  if (typeof input === "string") return new URL(input);
  if (input instanceof URL) return input;
  return new URL(input.url);
}

function requestBody(init?: RequestInit) {
  return JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
}

async function expectRejected(action: () => Promise<unknown> | unknown, pattern: RegExp) {
  let message = "";
  try {
    await action();
  } catch (error) {
    message = error instanceof Error ? error.message : String(error);
  }
  assert.match(message, pattern);
  return message;
}

/** Grant B (or C) on /join; explicit accept on ingest. Records every request. */
function grantFetch(options: {
  requests: RequestRecord[];
  tenantId?: string;
  installKey?: string;
  uploadOrigin?: string;
}) {
  const tenantId = options.tenantId ?? TENANT_B;
  const installKey = options.installKey ?? INSTALL_B;
  const origin = options.uploadOrigin ?? "https://workspace-b.example";
  return (async (input, init) => {
    const url = requestUrl(input);
    options.requests.push({ url: url.href, init, body: requestBody(init) });
    if (url.pathname.endsWith("/join")) {
      return responseJson({
        ok: true,
        tenantId,
        installKey,
        uploadUrl: `${origin}/api/work-intelligence/ingest`,
      }, 201);
    }
    return responseJson({ ok: true, accepted: 1 }, 200);
  }) as typeof fetch;
}

function uploadedEventIds(body: Record<string, unknown>): string[] {
  const events = (body.events ?? []) as Array<{ event?: { id?: string } }>;
  return events.map((entry) => entry.event?.id ?? "");
}

interface QuarantineRow {
  eventId: string | null;
  table: "buffered_events" | "upload_outbox";
}

function quarantineRows(ledgerPath: string): QuarantineRow[] {
  const buffer = new LocalEventBuffer(ledgerPath);
  try {
    const events = buffer.database
      .prepare(`select id as eventId from buffered_events where workspace_id is null`)
      .all() as Array<{ eventId: string }>;
    const outbox = buffer.database
      .prepare(`select raw_id as eventId from upload_outbox where workspace_id is null`)
      .all() as Array<{ eventId: string | null }>;
    const binding = buffer.workspaceBinding();
    return [
      ...events.map((row) => ({ table: "buffered_events" as const, eventId: row.eventId })),
      ...outbox.map((row) => ({ table: "upload_outbox" as const, eventId: row.eventId })),
    ];
  } finally {
    buffer.close();
  }
}

function bindingOf(ledgerPath: string) {
  const buffer = new LocalEventBuffer(ledgerPath);
  try {
    return buffer.workspaceBinding();
  } finally {
    buffer.close();
  }
}

function eventWorkspace(ledgerPath: string, id: string): string | null {
  const buffer = new LocalEventBuffer(ledgerPath);
  try {
    const row = buffer.database
      .prepare(`select workspace_id as workspaceId from buffered_events where id = ?`)
      .get(id) as { workspaceId: string | null } | undefined;
    return row?.workspaceId ?? null;
  } finally {
    buffer.close();
  }
}

function enrollmentCounts(ledgerPath: string) {
  const buffer = new LocalEventBuffer(ledgerPath);
  try {
    return buffer.enrollmentStatus();
  } finally {
    buffer.close();
  }
}

/** Seed pre-enrollment history: unbound events AND unbound outbox rows. */
function seedUnmanagedHistory(homeDir: string, ids: string[]) {
  const ledgerPath = collectorBufferPath(homeDir);
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true, mode: 0o700 });
  const seed = new LocalEventBuffer(ledgerPath, { delivery: { enabled: true } });
  const storedIds: string[] = [];
  try {
    for (const id of ids) {
      const appended = appendForwardedHook(
        { id, source: "claude_code", event_type: "UserPromptSubmit" },
        { config: defaultConfig(), buffer: seed, source: "claude_code" },
      );
      storedIds.push(appended.event.id);
    }
  } finally {
    seed.close();
  }
  // Prove the fixture itself is quarantined before any enrollment.
  const seeded = quarantineRows(ledgerPath);
  assert.deepEqual(
    seeded.filter((row) => row.table === "buffered_events").map((row) => row.eventId).sort(),
    [...storedIds].sort(),
    `fixture events must be unbound: ${JSON.stringify(seeded)}`,
  );
  assert.ok(
    seeded.filter((row) => row.table === "upload_outbox").length >= storedIds.length,
    `fixture must include unbound outbox rows: ${JSON.stringify(seeded)}`,
  );
  return { ledgerPath, storedIds };
}

/**
 * Seed pre-enrollment history EXACTLY THE PRODUCTION WAY (#163 rework).
 *
 * Production never writes workspace-unassigned rows: `cli.ts openBuffer`
 * always passes `workspaceId: config.tenantId`, and a pre-enrollment config
 * carries the unmanaged default LOCAL tenant. So real pre-join history is
 * LOCAL-BOUND, not NULL, and a proof that only ever seeds NULL rows cannot
 * see a leak of the rows production actually holds.
 */
function seedProductionHistory(homeDir: string, ids: string[]) {
  const ledgerPath = collectorBufferPath(homeDir);
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true, mode: 0o700 });
  const seed = new LocalEventBuffer(ledgerPath, {
    workspaceId: LOCAL_TENANT_ID,
    delivery: { enabled: true },
  });
  const storedIds: string[] = [];
  try {
    for (const id of ids) {
      const appended = appendForwardedHook(
        { id, source: "claude_code", event_type: "UserPromptSubmit" },
        { config: defaultConfig(), buffer: seed, source: "claude_code" },
      );
      storedIds.push(appended.event.id);
    }
  } finally {
    seed.close();
  }
  // Prove the fixture itself carries the production shape: a LOCAL ledger
  // binding, LOCAL-bound events, LOCAL-bound outbox rows, zero NULL rows.
  assert.equal(
    bindingOf(ledgerPath)?.currentWorkspaceId,
    LOCAL_TENANT_ID,
    "production-shape fixture must bind the ledger to the LOCAL tenant",
  );
  const census = censusByWorkspace(ledgerPath);
  assert.deepEqual(
    Object.keys(census).sort(),
    [`buffered_events:${LOCAL_TENANT_ID}`, `upload_outbox:${LOCAL_TENANT_ID}`],
    `production-shape fixture must hold only LOCAL-bound rows: ${JSON.stringify(census)}`,
  );
  assert.equal(
    census[`buffered_events:${LOCAL_TENANT_ID}`],
    storedIds.length,
    `production-shape fixture must store every seeded event: ${JSON.stringify(census)}`,
  );
  assert.ok(
    census[`upload_outbox:${LOCAL_TENANT_ID}`] >= storedIds.length,
    `production-shape fixture must include LOCAL-bound outbox rows: ${JSON.stringify(census)}`,
  );
  assert.equal(
    quarantineRows(ledgerPath).length,
    0,
    "production-shape fixture holds no unassigned rows at all — that is the point",
  );
  return { ledgerPath, storedIds };
}

/**
 * The two shapes a pre-enrollment ledger can hold. `production` is what
 * `cli.ts openBuffer` writes on every capture today; `legacy_unbound` is the
 * pre-workspace-column shape kept as an additional case (#163 rework).
 */
type SeedShape = {
  label: string;
  /** The workspace every pre-enrollment row carries in this shape. */
  preEnrollmentWorkspaceId: string | null;
  seed: (homeDir: string, ids: string[]) => { ledgerPath: string; storedIds: string[] };
};

const PRODUCTION_SHAPE: SeedShape = {
  label: "production_local_bound",
  preEnrollmentWorkspaceId: LOCAL_TENANT_ID,
  seed: seedProductionHistory,
};
const LEGACY_UNBOUND_SHAPE: SeedShape = {
  label: "legacy_unbound",
  preEnrollmentWorkspaceId: null,
  seed: seedUnmanagedHistory,
};
const SEED_SHAPES = [PRODUCTION_SHAPE, LEGACY_UNBOUND_SHAPE];

/**
 * Rows WITHHELD FROM THE CURRENT WORKSPACE: bound to a different workspace
 * (the production pre-join shape) or unassigned (the legacy shape). Mirrors
 * `LocalEventBuffer.enrollmentStatus` / doctor's readonly count, so a receipt
 * comparison is a real comparison. Payload-free: ids and labels only.
 */
function withheldRows(ledgerPath: string): QuarantineRow[] {
  const buffer = new LocalEventBuffer(ledgerPath);
  try {
    const current = buffer.workspaceBinding()?.currentWorkspaceId ?? null;
    const predicate = current === null
      ? `workspace_id is null`
      : `(workspace_id is null or workspace_id <> ?)`;
    const params = current === null ? [] : [current];
    const events = buffer.database
      .prepare(`select id as eventId from buffered_events where ${predicate}`)
      .all(...params) as Array<{ eventId: string }>;
    const outbox = buffer.database
      .prepare(`select raw_id as eventId from upload_outbox where ${predicate}`)
      .all(...params) as Array<{ eventId: string | null }>;
    return [
      ...events.map((row) => ({ table: "buffered_events" as const, eventId: row.eventId })),
      ...outbox.map((row) => ({ table: "upload_outbox" as const, eventId: row.eventId })),
    ];
  } finally {
    buffer.close();
  }
}

/**
 * Payload-free per-workspace row census across both durable tables
 * (unassigned rows under a sentinel key). Comparing it before vs after an
 * operation trips on ANY relabel or release of an existing row — the check
 * a NULL-count comparison cannot make.
 */
function censusByWorkspace(ledgerPath: string): Record<string, number> {
  const buffer = new LocalEventBuffer(ledgerPath);
  try {
    const census: Record<string, number> = {};
    for (const table of ["buffered_events", "upload_outbox"] as const) {
      const rows = buffer.database
        .prepare(
          `select coalesce(workspace_id, '∅unbound') as ws, count(*) as n
           from ${table} group by ws order by ws`,
        )
        .all() as Array<{ ws: string; n: number }>;
      for (const row of rows) census[`${table}:${row.ws}`] = row.n;
    }
    return census;
  } finally {
    buffer.close();
  }
}

/** Rows carrying the pre-enrollment label of `shape`, per durable table. */
function preEnrollmentBucket(census: Record<string, number>, shape: SeedShape) {
  const key = shape.preEnrollmentWorkspaceId ?? "∅unbound";
  return {
    events: census[`buffered_events:${key}`] ?? 0,
    outbox: census[`upload_outbox:${key}`] ?? 0,
  };
}

/**
 * In-process library calls receive an explicit homeDir and (with
 * PLIMSOLL_HOME unset) resolve state under
 * `<home>/Library/Application Support/Plimsoll`. Spawned CLI processes honor
 * PLIMSOLL_HOME verbatim, so hand them the same resolved directory.
 */
function stateDirectory(homeDir: string) {
  return path.join(homeDir, "Library", "Application Support", "Plimsoll");
}

async function runCli(args: string[], homeDir: string) {
  const child = spawn(
    process.execPath,
    ["node_modules/tsx/dist/cli.mjs", "packages/collector-cli/src/cli.ts", ...args],
    {
      cwd: process.cwd(),
      env: { ...process.env, PLIMSOLL_HOME: stateDirectory(homeDir), TSX_DISABLE_CACHE: "1" },
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  child.stdin.end("");
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => (stdout += chunk));
  child.stderr.on("data", (chunk: string) => (stderr += chunk));
  const exit = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
    (resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => resolve({ code, signal }));
    },
  );
  return { exit, stdout, stderr };
}

/**
 * Scenario 1 + 2, run once per pre-enrollment ledger shape (#163 rework).
 * `PRODUCTION_SHAPE` is what production actually holds (LOCAL-bound rows);
 * `LEGACY_UNBOUND_SHAPE` keeps the original unassigned-row fixture as an
 * additional case. Every assertion below must hold in BOTH shapes.
 */
async function firstJoinAndRejoinScenario(shape: SeedShape) {
  //
  // Scenario 1 — first managed enrollment is future-only.
  //
  {
    const firstHome = home(`first-join-${shape.label}`);
    const seeded = shape.seed(firstHome, LEGACY_EVENT_IDS);
    const ledgerPath = seeded.ledgerPath;
    const storedLegacyIds = seeded.storedIds;
    const beforeRows = withheldRows(ledgerPath);
    const censusBefore = censusByWorkspace(ledgerPath);
    const requests: RequestRecord[] = [];
    const temporaryRoot = path.join(root, `first-join-temp-${shape.label}`);
    fs.mkdirSync(temporaryRoot);

    const joined = await performJoin({
      target: TOKEN,
      baseUrl: "https://workspace-b.example",
      homeDir: firstHome,
      temporaryRoot,
      fetchImpl: grantFetch({ requests }),
    });

    const afterRows = withheldRows(ledgerPath);
    const censusAfter = censusByWorkspace(ledgerPath);
    const bucketBefore = preEnrollmentBucket(censusBefore, shape);
    const bucketAfter = preEnrollmentBucket(censusAfter, shape);
    const handshakeRequest = requests[1];
    const handshakeIds = handshakeRequest ? uploadedEventIds(handshakeRequest.body) : [];
    const activatedRaw = JSON.parse(fs.readFileSync(collectorConfigPath(firstHome), "utf8"));
    const activatedConfig = collectorConfigSchema.parse(activatedRaw);
    // Defensive: a pre-0089 result has no `enrollment` receipt at all.
    const joinedAny = joined as { enrollment?: { mode?: string; quarantinedHistoryRows?: number } };
    const receiptQuarantined = typeof joinedAny.enrollment?.quarantinedHistoryRows === "number"
      ? joinedAny.enrollment.quarantinedHistoryRows
      : -1;
    check(
      `first_join_sends_exactly_one_probe_and_zero_history__${shape.label}`,
      true,
      joined.joined &&
        requests.length === 2 &&
        requests[0]?.body.token === TOKEN &&
        requests[0]?.body.appVersion === COLLECTOR_APP_VERSION &&
        handshakeIds.length === 1 &&
        handshakeIds[0] === (joined.joined ? joined.handshake.selfTestEventId : "") &&
        handshakeIds.every((id) => !LEGACY_EVENT_IDS.includes(id)) &&
        (joined.joined ? joined.handshake.uploadedEvents : -1) === 1,
      {
        requests: requests.length,
        handshakeIds,
        legacyEventIds: LEGACY_EVENT_IDS,
        joined: joined.joined,
      },
    );
    check(
      `first_join_leaves_every_pre_enrollment_row_withheld__${shape.label}`,
      true,
      joined.joined &&
        // In the legacy shape nothing changes label, so the withheld count is
        // literally unchanged. In the production shape the SAME rows go from
        // "the current audience" (LOCAL) to "withheld from the current
        // audience" (B) without a single row being touched — the bucket
        // equality below is what proves no row moved.
        (shape.preEnrollmentWorkspaceId === null
          ? afterRows.length === beforeRows.length
          : beforeRows.length === 0 &&
            afterRows.length === bucketAfter.events + bucketAfter.outbox) &&
        JSON.stringify(
          afterRows.filter((row) => row.table === "buffered_events").map((r) => r.eventId).sort(),
        ) === JSON.stringify([...storedLegacyIds].sort()) &&
        bindingOf(ledgerPath)?.currentWorkspaceId === TENANT_B &&
        // #163 rework: the pre-enrollment bucket is byte-identical before and
        // after, every seeded row still carries its pre-enrollment label, and
        // the joined workspace owns NO pre-existing row of either table.
        JSON.stringify(bucketBefore) === JSON.stringify(bucketAfter) &&
        storedLegacyIds.every(
          (id) => eventWorkspace(ledgerPath, id) === shape.preEnrollmentWorkspaceId,
        ) &&
        (censusAfter[`buffered_events:${TENANT_B}`] ?? 0) === 0 &&
        (censusAfter[`upload_outbox:${TENANT_B}`] ?? 0) === 0,
      {
        adversarialNote:
          "pre-fix code relabeled null-workspace rows to the prior/local tenant during transitionWorkspace/useWorkspace; " +
          "#163 rework: production pre-enrollment rows are LOCAL-bound (openBuffer passes config.tenantId), so a relabel " +
          "of prior LOCAL rows into the joined workspace is the leak this must see",
        seedShape: shape.label,
        preEnrollmentWorkspaceId: shape.preEnrollmentWorkspaceId,
        storedLegacyIds,
        beforeCount: beforeRows.length,
        afterCount: afterRows.length,
        afterEvents: afterRows.filter((row) => row.table === "buffered_events"),
        legacyRowWorkspaces: storedLegacyIds.map((id) => eventWorkspace(ledgerPath, id)),
        censusBefore,
        censusAfter,
        binding: bindingOf(ledgerPath),
      },
    );
    check(
      `join_receipt_reports_future_only_and_quarantined_counts__${shape.label}`,
      true,
      joined.joined &&
        joinedAny.enrollment?.mode === "future_only" &&
        joinedAny.enrollment?.quarantinedHistoryRows === afterRows.length &&
        (joined.workspaceBoundary as { boundLegacyRows?: number }).boundLegacyRows === 0 &&
        receiptQuarantined === afterRows.length &&
        // #163 rework gap 3: in the production shape the withheld rows are
        // LOCAL-bound, so a receipt that still counts only NULL rows reports 0
        // here and fails.
        (shape.preEnrollmentWorkspaceId === null || receiptQuarantined > 0),
      {
        seedShape: shape.label,
        enrollment: joinedAny.enrollment ?? null,
        actualQuarantinedRows: afterRows.length,
      },
    );

    // Post-activation capture binds to B and may enter its normal outbox.
    const postBuffer = new LocalEventBuffer(ledgerPath, {
      workspaceId: TENANT_B,
      delivery: { enabled: true, limits: activatedConfig.delivery },
    });
    const ordinaryBodies: Array<Record<string, unknown>> = [];
    const postJoinAppended = appendForwardedHook(
      { id: POST_JOIN_EVENT_ID, source: "claude_code", event_type: "UserPromptSubmit" },
      { config: activatedConfig, buffer: postBuffer, source: "claude_code" },
    );
    const postJoinStoredId = postJoinAppended.event.id;
    const ordinaryUpload = await uploadBufferedEvents(activatedConfig, postBuffer, {
      fetchImpl: (async (_input, init) => {
        ordinaryBodies.push(requestBody(init));
        return responseJson({ ok: true, accepted: 1 }, 200);
      }) as typeof fetch,
    });
    const ordinaryIds = ordinaryBodies.flatMap((body) => uploadedEventIds(body));
    const ordinaryBodyText = JSON.stringify(ordinaryBodies);
    const stillQuarantined = withheldRows(ledgerPath)
      .filter((row) => row.table === "buffered_events")
      .map((row) => row.eventId)
      .sort();
    check(
      `post_activation_events_bound_to_new_workspace_and_uploaded_without_history__${shape.label}`,
      true,
      ordinaryUpload.uploadedEvents === 1 &&
        ordinaryBodies.length === 1 &&
        ordinaryIds.length === 1 &&
        ordinaryIds[0] === postJoinStoredId &&
        eventWorkspace(ledgerPath, postJoinStoredId) === TENANT_B &&
        JSON.stringify(stillQuarantined) === JSON.stringify([...storedLegacyIds].sort()) &&
        // #163 rework: over the wire, not by label — no pre-enrollment id may
        // appear anywhere in an ordinary post-join upload body.
        storedLegacyIds.every((id) => !ordinaryBodyText.includes(id)) &&
        LEGACY_EVENT_IDS.every((id) => !ordinaryBodyText.includes(id)),
      {
        seedShape: shape.label,
        uploadedEvents: ordinaryUpload.uploadedEvents,
        ordinaryIds,
        stillQuarantined,
        legacyIdsInUploadBodies: storedLegacyIds.filter((id) => ordinaryBodyText.includes(id)),
        postJoinWorkspace: eventWorkspace(ledgerPath, postJoinStoredId),
      },
    );
    postBuffer.close();

    //
    // Scenario 2 — same-workspace rejoin is idempotent and never releases
    // quarantined history.
    //
    const quarantineBeforeRejoin = withheldRows(ledgerPath).length;
    const censusBeforeRejoin = censusByWorkspace(ledgerPath);
    const rejoinRequests: RequestRecord[] = [];
    const rejoined = await performJoin({
      target: `${TOKEN}-rejoin`,
      baseUrl: "https://workspace-b.example",
      homeDir: firstHome,
      temporaryRoot,
      fetchImpl: grantFetch({ requests: rejoinRequests }),
    });
    const rejoinedProbeIds = rejoinRequests[1]
      ? uploadedEventIds(rejoinRequests[1].body)
      : [];
    // Idempotency contract: an already-activated same-workspace rejoin must
    // not replay any handshake upload at all.
    const expectedRejoinRequests = rejoined.joined && rejoined.handshake.response != null &&
      (rejoined.handshake.response as { status?: string }).status === "already_activated"
      ? 1
      : 2;
    const effectiveRejoinProbeIds =
      expectedRejoinRequests === 1 ? [] : rejoinedProbeIds;
    const rejoinPostBuffer = new LocalEventBuffer(ledgerPath, {
      workspaceId: TENANT_B,
      delivery: { enabled: true, limits: activatedConfig.delivery },
    });
    const rejoinBodies: Array<Record<string, unknown>> = [];
    const rejoinAppended = appendForwardedHook(
      { id: REJOIN_POST_EVENT_ID, source: "claude_code", event_type: "UserPromptSubmit" },
      { config: activatedConfig, buffer: rejoinPostBuffer, source: "claude_code" },
    );
    const rejoinStoredId = rejoinAppended.event.id;
    const rejoinUpload = await uploadBufferedEvents(activatedConfig, rejoinPostBuffer, {
      fetchImpl: (async (_input, init) => {
        rejoinBodies.push(requestBody(init));
        return responseJson({ ok: true, accepted: 1 }, 200);
      }) as typeof fetch,
    });
    const rejoinIds = rejoinBodies.flatMap((body) => uploadedEventIds(body));
    const rejoinBodyText = JSON.stringify(rejoinBodies);
    const quarantineAfterRejoin = withheldRows(ledgerPath);
    const censusAfterRejoin = censusByWorkspace(ledgerPath);
    check(
      `same_workspace_rejoin_is_idempotent_and_does_not_release_quarantine__${shape.label}`,
      true,
      rejoined.joined &&
        rejoinRequests.length === expectedRejoinRequests &&
        effectiveRejoinProbeIds.every((id) => !storedLegacyIds.includes(id)) &&
        quarantineAfterRejoin.length === quarantineBeforeRejoin &&
        JSON.stringify(
          quarantineAfterRejoin.filter((r) => r.table === "buffered_events").map((r) => r.eventId).sort(),
        ) === JSON.stringify([...storedLegacyIds].sort()) &&
        rejoinUpload.uploadedEvents === 1 &&
        rejoinIds.length === 1 &&
        rejoinIds[0] === rejoinStoredId &&
        // #163 rework: the pre-enrollment bucket survives the rejoin intact
        // and no pre-enrollment id crosses the wire on the rejoin upload.
        JSON.stringify(preEnrollmentBucket(censusBeforeRejoin, shape)) ===
          JSON.stringify(preEnrollmentBucket(censusAfterRejoin, shape)) &&
        storedLegacyIds.every((id) => !rejoinBodyText.includes(id)),
      {
        seedShape: shape.label,
        legacyIdsInRejoinBodies: storedLegacyIds.filter((id) => rejoinBodyText.includes(id)),
        preEnrollmentBucketBeforeRejoin: preEnrollmentBucket(censusBeforeRejoin, shape),
        preEnrollmentBucketAfterRejoin: preEnrollmentBucket(censusAfterRejoin, shape),
        rejoinRequests: rejoinRequests.length,
        expectedRejoinRequests,
        rejoinProbeIds: effectiveRejoinProbeIds,
        quarantineBeforeRejoin,
        quarantineAfterRejoin: quarantineAfterRejoin.length,
        quarantinedEventIdsAfterRejoin:
          quarantineAfterRejoin.filter((r) => r.table === "buffered_events").map((r) => r.eventId),
        rejoinUploaded: rejoinUpload.uploadedEvents,
        rejoinIds,
      },
    );
    rejoinPostBuffer.close();

    //
    // Scenario 5a — status receipt reports future-only enrollment without
    // reading or printing payloads.
    //
    const status = await runCli(["status"], firstHome);
    if (process.env.PLIMSOLL_PROOF_DEBUG) {
      console.error("DEBUG ledgerPath:", ledgerPath, "exists:", fs.existsSync(ledgerPath));
      console.error("DEBUG preCount:", quarantineBeforeRejoin);
      console.error("DEBUG statusStderr:", status.stderr.slice(0, 400));
      console.error("DEBUG statusStdoutHead:", status.stdout.slice(0, 300));
    }
    const statusReceipt = JSON.parse(status.stdout) as Record<string, any>;
    const statusOutput = `${status.stdout}\n${status.stderr}`;
    check(
      `status_receipt_reports_future_only_quarantine_state_without_payloads__${shape.label}`,
      true,
      status.exit.code === 0 &&
        statusReceipt.enrollment?.futureOnlyEnrollment === true &&
        statusReceipt.enrollment?.currentWorkspaceId === TENANT_B &&
        statusReceipt.enrollment?.quarantinedHistoryRows === quarantineBeforeRejoin &&
        LEGACY_EVENT_IDS.every((id) => !statusOutput.includes(id)) &&
        // #163 rework gap 3: in the production shape a NULL-only receipt would
        // report 0 against a ledger that really is withholding rows.
        (shape.preEnrollmentWorkspaceId === null ||
          statusReceipt.enrollment?.quarantinedHistoryRows > 0),
      {
        seedShape: shape.label,
        exit: status.exit,
        enrollment: statusReceipt.enrollment ?? null,
        payloadLeak: LEGACY_EVENT_IDS.filter((id) => statusOutput.includes(id)),
      },
    );
    const doctor = await runCli(["doctor"], firstHome);
    const doctorReceipt = JSON.parse(doctor.stdout) as Record<string, any>;
    const doctorOutput = `${doctor.stdout}\n${doctor.stderr}`;
    check(
      `doctor_receipt_reports_future_only_quarantine_state_readonly__${shape.label}`,
      true,
      typeof doctorReceipt.ok === "boolean" &&
        doctorReceipt.enrollment?.futureOnlyEnrollment === true &&
        doctorReceipt.enrollment?.quarantinedHistoryRows === quarantineBeforeRejoin &&
        LEGACY_EVENT_IDS.every((id) => !doctorOutput.includes(id)) &&
        (shape.preEnrollmentWorkspaceId === null ||
          doctorReceipt.enrollment?.quarantinedHistoryRows > 0),
      {
        seedShape: shape.label,
        exit: doctor.exit,
        ok: doctorReceipt.ok,
        enrollment: doctorReceipt.enrollment ?? null,
        payloadLeak: LEGACY_EVENT_IDS.filter((id) => doctorOutput.includes(id)),
      },
    );
  }
}

/**
 * Scenario 3, run once per pre-enrollment ledger shape (#163 rework): a failed
 * handshake rolls back cleanly — no activation, no binding change, no relabel
 * or release of seeded history.
 */
async function failedHandshakeScenario(shape: SeedShape) {
  {
    const failedHome = home(`failed-handshake-${shape.label}`);
    const seeded = shape.seed(failedHome, [LEGACY_EVENT_IDS[0]]);
    const ledgerPath = seeded.ledgerPath;
    const storedLegacyId = seeded.storedIds[0];
    const beforeRows = withheldRows(ledgerPath);
    const censusBefore = censusByWorkspace(ledgerPath);
    const bindingBefore = bindingOf(ledgerPath);
    const failedTemp = path.join(root, `failed-handshake-temp-${shape.label}`);
    fs.mkdirSync(failedTemp);
    let calls = 0;
    const failureMessage = await expectRejected(
      () =>
        performJoin({
          target: TOKEN,
          baseUrl: "https://workspace-b.example",
          homeDir: failedHome,
          temporaryRoot: failedTemp,
          fetchImpl: (async (input, init) => {
            calls += 1;
            requestUrl(input);
            if (requestUrl(input).pathname.endsWith("/join")) {
              return responseJson({
                ok: true,
                tenantId: TENANT_B,
                installKey: INSTALL_B,
                uploadUrl: "https://workspace-b.example/api/work-intelligence/ingest",
              }, 201);
            }
            return responseJson({ ok: false }, 503);
          }) as typeof fetch,
        }),
      /not activated.*handshake failed/i,
    );
    const afterBinding = bindingOf(ledgerPath);
    const afterRows = withheldRows(ledgerPath);
    const censusAfter = censusByWorkspace(ledgerPath);
    check(
      `failed_handshake_rollback_keeps_history_withheld_and_unactivated__${shape.label}`,
      false,
      calls === 2 &&
        !fs.existsSync(collectorConfigPath(failedHome)) &&
        fs.existsSync(path.join(path.dirname(collectorConfigPath(failedHome)), "join.pending.json")) &&
        // The legacy shape has no binding to keep; the production shape must
        // still be bound to LOCAL, exactly as it was before the failed join.
        (shape.preEnrollmentWorkspaceId === null
          ? afterBinding === null
          : afterBinding?.currentWorkspaceId === shape.preEnrollmentWorkspaceId &&
            bindingBefore?.currentWorkspaceId === shape.preEnrollmentWorkspaceId) &&
        afterRows.length === beforeRows.length &&
        JSON.stringify(afterRows.map((r) => r.eventId).sort()) ===
          JSON.stringify(beforeRows.map((r) => r.eventId).sort()) &&
        fs.readdirSync(failedTemp).filter((entry) =>
          entry.startsWith(JOIN_HANDSHAKE_DIRECTORY_PREFIX),
        ).length === 0 &&
        // #163 rework: not one row of either table changed workspace, and the
        // seeded row still carries its pre-enrollment label.
        JSON.stringify(censusBefore) === JSON.stringify(censusAfter) &&
        eventWorkspace(ledgerPath, storedLegacyId) === shape.preEnrollmentWorkspaceId &&
        (censusAfter[`buffered_events:${TENANT_B}`] ?? 0) === 0 &&
        (censusAfter[`upload_outbox:${TENANT_B}`] ?? 0) === 0,
      {
        seedShape: shape.label,
        failureMessage,
        calls,
        configExists: fs.existsSync(collectorConfigPath(failedHome)),
        binding: afterBinding,
        bindingBefore,
        storedLegacyId,
        storedLegacyWorkspace: eventWorkspace(ledgerPath, storedLegacyId),
        censusBefore,
        censusAfter,
        beforeRows: beforeRows.length,
        afterRows: afterRows.length,
      },
    );
  }
}

/**
 * RED TEAM 1 (#163 rework) — the audit's Plant C shape.
 *
 * Defect reproduced: first join from LOCAL relabels the prior LOCAL rows into
 * the joined workspace (an `update ... set workspace_id = @to where
 * workspace_id = @from` on the transition path). This fixture MUST go red for
 * that plant: either the in-join census guard refuses the activation (join
 * throws) or the rows land in the joined workspace — both fail here.
 */
async function redTeamFirstJoinFromLocalRelabel() {
  const relabelHome = home("red-team-relabel");
  const seeded = seedProductionHistory(relabelHome, LEGACY_EVENT_IDS);
  const ledgerPath = seeded.ledgerPath;
  const storedIds = seeded.storedIds;
  const censusBefore = censusByWorkspace(ledgerPath);
  const temporaryRoot = path.join(root, "red-team-relabel-temp");
  fs.mkdirSync(temporaryRoot);
  const requests: RequestRecord[] = [];

  let joinError: string | null = null;
  let joinedOk = false;
  try {
    const joined = await performJoin({
      target: TOKEN,
      baseUrl: "https://workspace-b.example",
      homeDir: relabelHome,
      temporaryRoot,
      fetchImpl: grantFetch({ requests }),
    });
    joinedOk = joined.joined;
  } catch (error) {
    joinError = error instanceof Error ? error.message : String(error);
  }

  const censusAfter = censusByWorkspace(ledgerPath);
  const rowWorkspaces = storedIds.map((id) => eventWorkspace(ledgerPath, id));
  const relabelledIntoJoined = rowWorkspaces.filter((ws) => ws === TENANT_B).length;
  const withheld = withheldRows(ledgerPath);
  check(
    "red_team_first_join_from_local_never_relabels_prior_local_rows",
    true,
    // A planted relabel breaks this three ways: the census guard rejects the
    // activation (joinError), the per-row labels move to B, or the census
    // buckets shift. All three are asserted, so removing the guard does not
    // buy the plant a green run.
    joinError === null &&
      joinedOk &&
      relabelledIntoJoined === 0 &&
      rowWorkspaces.every((ws) => ws === LOCAL_TENANT_ID) &&
      (censusAfter[`buffered_events:${TENANT_B}`] ?? 0) === 0 &&
      (censusAfter[`upload_outbox:${TENANT_B}`] ?? 0) === 0 &&
      JSON.stringify(censusBefore) === JSON.stringify(censusAfter) &&
      censusAfter[`buffered_events:${LOCAL_TENANT_ID}`] === storedIds.length &&
      withheld.length === storedIds.length + (censusAfter[`upload_outbox:${LOCAL_TENANT_ID}`] ?? 0),
    {
      adversarialNote:
        "AUDIT PLANT C: a relabel of prior LOCAL rows into the joined workspace on the transition path. " +
        "The pre-rework proof could not see it because its fixture only ever seeded NULL rows.",
      joinError,
      joinedOk,
      rowWorkspaces,
      relabelledIntoJoined,
      censusBefore,
      censusAfter,
      withheldRows: withheld.length,
    },
  );
}

/**
 * RED TEAM 2 (#163 rework) — the audit's outbox-release plant.
 *
 * Defect reproduced: after joining B, the outbox releases pre-enrollment rows
 * (LOCAL-bound) to the new workspace and UPLOADS them. Labels alone cannot see
 * this — a widened claim predicate leases and sends the rows without changing
 * one `workspace_id`. So this fixture asserts on the UPLOAD BODIES and the
 * MARKED counts: what crossed the wire, and what the ledger recorded as sent.
 */
async function redTeamOutboxReleaseAfterJoin() {
  const releaseHome = home("red-team-outbox-release");
  const seeded = seedProductionHistory(releaseHome, LEGACY_EVENT_IDS);
  const ledgerPath = seeded.ledgerPath;
  const storedIds = seeded.storedIds;
  const temporaryRoot = path.join(root, "red-team-outbox-release-temp");
  fs.mkdirSync(temporaryRoot);
  const requests: RequestRecord[] = [];
  const joined = await performJoin({
    target: TOKEN,
    baseUrl: "https://workspace-b.example",
    homeDir: releaseHome,
    temporaryRoot,
    fetchImpl: grantFetch({ requests }),
  });
  const activatedConfig = collectorConfigSchema.parse(
    JSON.parse(fs.readFileSync(collectorConfigPath(releaseHome), "utf8")),
  );

  // Drain as the joined workspace with NOTHING of its own to send. Every byte
  // that leaves here is pre-enrollment history.
  const bodies: Array<Record<string, unknown>> = [];
  const buffer = new LocalEventBuffer(ledgerPath, {
    workspaceId: TENANT_B,
    delivery: { enabled: true, limits: activatedConfig.delivery },
  });
  let durableUpload: Awaited<ReturnType<typeof uploadBufferedEvents>>;
  let statelessUpload: Awaited<ReturnType<typeof uploadBufferedEvents>>;
  try {
    durableUpload = await uploadBufferedEvents(activatedConfig, buffer, {
      fetchImpl: (async (_input, init) => {
        bodies.push(requestBody(init));
        return responseJson({ ok: true, accepted: 1 }, 200);
      }) as typeof fetch,
    });
    // The second release surface: the no-mark path reads listUnuploaded
    // instead of the outbox lease.
    statelessUpload = await uploadBufferedEvents(activatedConfig, buffer, {
      markUploaded: false,
      fetchImpl: (async (_input, init) => {
        bodies.push(requestBody(init));
        return responseJson({ ok: true, accepted: 1 }, 200);
      }) as typeof fetch,
    });
  } finally {
    buffer.close();
  }

  const bodyText = JSON.stringify(bodies);
  const sentIds = bodies.flatMap((body) => uploadedEventIds(body));
  const markedUploadedEvents = (() => {
    const reader = new LocalEventBuffer(ledgerPath);
    try {
      return (
        reader.database
          .prepare(`select count(*) as n from buffered_events where uploaded_at is not null`)
          .get() as { n: number }
      ).n;
    } finally {
      reader.close();
    }
  })();
  const outboxCensus = censusByWorkspace(ledgerPath);
  check(
    "red_team_outbox_never_releases_pre_enrollment_rows_to_joined_workspace",
    true,
    joined.joined &&
      // Nothing crossed the wire at all: no request, no ids, no id anywhere in
      // a body.
      bodies.length === 0 &&
      sentIds.length === 0 &&
      storedIds.every((id) => !bodyText.includes(id)) &&
      LEGACY_EVENT_IDS.every((id) => !bodyText.includes(id)) &&
      // Nothing was recorded as delivered, by either upload mode.
      durableUpload.uploadedEvents === 0 &&
      durableUpload.markedUploaded === 0 &&
      statelessUpload.uploadedEvents === 0 &&
      markedUploadedEvents === 0 &&
      // The rows are still the LOCAL workspace's, still withheld from B.
      (outboxCensus[`upload_outbox:${TENANT_B}`] ?? 0) === 0 &&
      (outboxCensus[`upload_outbox:${LOCAL_TENANT_ID}`] ?? 0) >= storedIds.length,
    {
      adversarialNote:
        "AUDIT PLANT: an outbox release of pre-enrollment rows to the joined workspace. " +
        "Asserted on upload bodies and marked counts, not labels — a widened claim predicate " +
        "sends the rows without relabeling a single one.",
      uploadRequests: bodies.length,
      sentIds,
      legacyIdsInBodies: storedIds.filter((id) => bodyText.includes(id)),
      durableUploadedEvents: durableUpload.uploadedEvents,
      durableMarkedUploaded: durableUpload.markedUploaded,
      statelessUploadedEvents: statelessUpload.uploadedEvents,
      markedUploadedEvents,
      census: outboxCensus,
    },
  );
}

async function main() {
try {
  for (const shape of SEED_SHAPES) {
    await firstJoinAndRejoinScenario(shape);
    await failedHandshakeScenario(shape);
  }
  await redTeamFirstJoinFromLocalRelabel();
  await redTeamOutboxReleaseAfterJoin();

  //
  // Scenario 4 — cross-workspace reassignment is fail-closed and cannot
  // relabel prior rows (A-bound stays A, unbound stays unbound).
  //
  {
    const reassignHome = home("cross-workspace-reassign");
    const configA: CollectorConfig = collectorConfigSchema.parse({
      port: 49131,
      tenantId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      installKey: "pli_workspace_a_install",
      uploadUrl: "https://workspace-a.example/api/work-intelligence/ingest",
    });
    fs.mkdirSync(path.dirname(collectorConfigPath(reassignHome)), { recursive: true, mode: 0o700 });
    fs.writeFileSync(
      collectorConfigPath(reassignHome),
      JSON.stringify(configA, null, 2),
      { mode: 0o600 },
    );
    const ledgerPath = collectorBufferPath(reassignHome);
    const boundSeed = new LocalEventBuffer(ledgerPath, {
      workspaceId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      delivery: { enabled: true },
    });
    let boundStoredId = "";
    try {
      const appended = appendForwardedHook(
        { id: "legacy_bound_to_a_event", source: "claude_code", event_type: "UserPromptSubmit" },
        { config: configA, buffer: boundSeed, source: "claude_code" },
      );
      boundStoredId = appended.event.id;
    } finally {
      boundSeed.close();
    }
    const unboundSeed = new LocalEventBuffer(ledgerPath, { delivery: { enabled: true } });
    let unboundStoredId = "";
    try {
      const appended = appendForwardedHook(
        { id: LEGACY_EVENT_IDS[1], source: "claude_code", event_type: "UserPromptSubmit" },
        { config: defaultConfig(), buffer: unboundSeed, source: "claude_code" },
      );
      unboundStoredId = appended.event.id;
    } finally {
      unboundSeed.close();
    }
    const beforeRows = quarantineRows(ledgerPath);
    assert.equal(eventWorkspace(ledgerPath, boundStoredId), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");

    const requests: RequestRecord[] = [];
    const temporaryRoot = path.join(root, "reassign-temp");
    fs.mkdirSync(temporaryRoot);
    const reassigned = await performJoin({
      target: TOKEN,
      baseUrl: "https://workspace-c.example",
      homeDir: reassignHome,
      temporaryRoot,
      fetchImpl: grantFetch({
        requests,
        tenantId: TENANT_C,
        installKey: INSTALL_C,
        uploadOrigin: "https://workspace-c.example",
      }),
    });

    // Fail-closed direct mismatch: an initialized C ledger refuses audience A.
    const mismatchMessage = await expectRejected(
      () => {
        const buffer = new LocalEventBuffer(ledgerPath, {
          workspaceId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        });
        buffer.close();
      },
      /binding mismatch|refusing to relabel/i,
    );

    const afterRows = quarantineRows(ledgerPath);
    const unboundStillUnbound = afterRows.some(
      (row) =>
        row.table === "buffered_events" &&
        row.eventId === unboundStoredId,
    );
    const asC = new LocalEventBuffer(ledgerPath, {
      workspaceId: TENANT_C,
      delivery: { enabled: true },
    });
    const reassignBodies: Array<Record<string, unknown>> = [];
    let reassignUploadCount = -1;
    try {
      const upload = await uploadBufferedEvents(
        collectorConfigSchema.parse(JSON.parse(fs.readFileSync(collectorConfigPath(reassignHome), "utf8"))),
        asC,
        {
          fetchImpl: (async (_input, init) => {
            reassignBodies.push(requestBody(init));
            return responseJson({ ok: true, accepted: 1 }, 200);
          }) as typeof fetch,
        },
      );
      reassignUploadCount = upload.uploadedEvents;
    } finally {
      asC.close();
    }
    const reassignIds = reassignBodies.flatMap((body) => uploadedEventIds(body));
    check(
      "cross_workspace_reassignment_fail_closed_cannot_relabel_prior_rows",
      true,
      reassigned.joined &&
        bindingOf(ledgerPath)?.currentWorkspaceId === TENANT_C &&
        eventWorkspace(ledgerPath, boundStoredId) ===
          "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" &&
        unboundStillUnbound &&
        afterRows.length === beforeRows.length &&
        mismatchMessage.length > 0 &&
        reassignUploadCount === 0 &&
        reassignIds.length === 0,
      {
        adversarialNote:
          "pre-fix code relabeled unassigned rows to the FROM tenant and could migrate them on next useWorkspace selection",
        fromWorkspaceId: reassigned.joined ? reassigned.workspaceBoundary.fromWorkspaceId : null,
        toWorkspaceId: reassigned.joined ? reassigned.workspaceBoundary.toWorkspaceId : null,
        boundARowWorkspace: eventWorkspace(ledgerPath, boundStoredId),
        unboundStillUnbound,
        mismatchMessage,
        reassignUploadCount,
        reassignIds,
        afterRows: afterRows.length,
        beforeRows: beforeRows.length,
      },
    );
  }

  //
  // Unit-level adversarial: useWorkspace must NEVER bind unassigned history,
  // directly or across selections.
  //
  {
    const unitLedger = path.join(root, "unit-ledger.sqlite");
    const seed = new LocalEventBuffer(unitLedger, { delivery: { enabled: true } });
    const appended = appendForwardedHook(
      { id: LEGACY_EVENT_IDS[0], source: "claude_code", event_type: "UserPromptSubmit" },
      { config: defaultConfig(), buffer: seed, source: "claude_code" },
    );
    seed.close();
    const storedId = appended.event.id;

    const buffer = new LocalEventBuffer(unitLedger);
    const firstSelection = (() => {
      buffer.useWorkspace(TENANT_B);
      return quarantineRows(unitLedger);
    })();
    buffer.useWorkspace(TENANT_B); // idempotent same-selection
    const mismatchMessage = await expectRejected(
      () => buffer.useWorkspace(TENANT_C),
      /binding mismatch/i,
    );
    const transition = await expectRejected(() => {
      // Wrong-direction transition must fail closed, not relabel.
      buffer.transitionWorkspace(TENANT_C, "dddddddd-dddd-4ddd-8ddd-dddddddddddd");
    }, /Cannot transition ledger/);
    buffer.close();
    const finalRows = quarantineRows(unitLedger);
    const quarantinedEventIds = finalRows
      .filter((row) => row.table === "buffered_events")
      .map((row) => row.eventId);
    check(
      "useWorkspace_never_binds_unassigned_history_fail_closed_on_mismatch",
      true,
      JSON.stringify(quarantinedEventIds) === JSON.stringify([storedId]) &&
        mismatchMessage.length > 0 &&
        transition.length > 0,
      {
        adversarialNote:
          "pre-fix code migrated unassigned rows to the requested workspace on first and same-workspace selections",
        storedId,
        quarantinedEventIdsAfterSelection: quarantinedEventIds,
        mismatchMessage,
        transitionMessage: transition,
      },
    );
  }

  //
  // Explicit history upload remains a distinct confirmed operation that join
  // does not invoke.
  //
  {
    const cliSource = fs.readFileSync("packages/collector-cli/src/cli.ts", "utf8");
    const joinSource = fs.readFileSync("packages/collector-cli/src/join.ts", "utf8");
    check(
      "explicit_history_upload_remains_distinct_and_not_invoked_by_join",
      false,
      joinSource.includes("runWorkspaceHistoryUpload") === false &&
        joinSource.toLowerCase().includes("upload-history") === false &&
        cliSource.includes("upload-history") &&
        cliSource.includes("runWorkspaceHistoryUpload"),
      {
        joinReferencesHistoryUpload: joinSource.toLowerCase().includes("upload-history"),
        cliKeepsExplicitCommand:
          cliSource.includes("upload-history") && cliSource.includes("runWorkspaceHistoryUpload"),
      },
    );
  }

  completion.complete();
  const adversarialChecks = checks.filter((entry) => entry.adversarial);
  console.log(
    JSON.stringify(
      {
        ok: true,
        proof: "enrollment-privacy",
        appVersion: COLLECTOR_APP_VERSION,
        node: process.version,
        totalChecks: checks.length,
        adversarialChecks: adversarialChecks.length,
        checks,
      },
      null,
      2,
    ),
  );
} finally {
  if (originalPlimsollHome === undefined) delete process.env.PLIMSOLL_HOME;
  else process.env.PLIMSOLL_HOME = originalPlimsollHome;
  fs.rmSync(root, { recursive: true, force: true });
}
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
