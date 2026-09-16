/**
 * `rotate-producer-token` for every producer source (bead eco-6hoxj.152).
 *
 * The command accepted only `--source codex`, although the credential store is
 * generic per source, so a Gemini token exposed in a transcript on 2026-09-16
 * could not be revoked. Under one fixture HOME that `setup --yes` managed —
 * Claude user settings plus two Claude seats, one of them written unmanaged
 * after setup and one malformed; a Codex home plus one seat profile; Gemini
 * settings; the Grok header file and a hook fragment rewritten into the legacy
 * inline-token shape — and against a collector that is RUNNING throughout and
 * loaded its authority before any rotation, this proof pins for each of
 * claude_code, codex, gemini_cli and grok:
 *
 *   a) `--dry-run` mints nothing and writes nothing;
 *   b) the rotation rewrites every managed surface of that source with a
 *      backup beside each rewritten file, leaves unmanaged/malformed seats
 *      byte-identical, prints the receipt shape Codex prints and no token, and
 *      does not touch another source's credential or surfaces;
 *   c) the running collector admits the token read back from the rewritten
 *      surface immediately, still admits the superseded token inside the grace
 *      window, refuses a foreign (other-source) token, and — with its clock
 *      injected past the deadline — refuses the superseded token while the
 *      rewritten surface keeps working;
 *   d) doctor reports `producerTokenRotation.<source>` active, value-blind;
 *   e) a managed file the host does not have is reported `absent` and never
 *      created, and a Grok hook whose header file is absent is not rewritten.
 *
 * Review r1 (BLOCKED) added, each named after the finding it pins:
 *   F1  Gemini rotation changes only the token query value: non-default
 *       telemetry, other query parameters, the fragment and foreign keys keep
 *       their bytes; unmanaged and malformed endpoints refuse without minting.
 *   F2  a discovered Claude seat or Codex profile that refuses or fails makes
 *       the rotation `rotation_incomplete`, exit 1, with the window recorded.
 *   F3  the injected collector clock is load-bearing in each server hunk and in
 *       `assertProducerToken`, pinned by auth-file read counts and a cached
 *       authority with no reload home.
 *   F4  another managed Grok hooks/*.json carrying the token is rewritten;
 *       unrelated hooks keep their bytes; an unclassifiable one is `skipped`.
 *   F5  an absent discovered Claude seat is listed `absent`.
 *   F6  the guard resolves a nonexistent path through its nearest existing
 *       ancestor, so a symlink to the real home cannot hide behind a new child.
 *   F8  `--grace-seconds` above 86400 is rejected without minting.
 *
 * Review r2 (BLOCKED) added:
 *   G1  a managed Grok hook copy in the header-file form (no token in the JSON)
 *       is discovered: one reading the managed header is reported `unchanged`,
 *       one reading its own header file gets that file rotated with a backup,
 *       one whose header file is absent is `skipped`, and a header file the
 *       rewrite refuses makes the rotation `rotation_incomplete`.
 *   G2  a hardlinked Codex config.toml is refused like a hardlinked Claude
 *       file: a discovered profile makes the rotation incomplete, and the owned
 *       ~/.codex/config.toml refuses the rotation before anything is minted.
 *
 * Review r3 (BLOCKED) added:
 *   G3  a header file a Grok hook copy names is classified only from bytes read
 *       through the guarded descriptor its rewrite uses. With a proof-only
 *       preload that logs every open and read of the path and replaces it at a
 *       deterministic point: a header behind a symlinked ancestor, a leaf that
 *       becomes a symlink, a FIFO or a different file between its lstat and
 *       its open, is refused with the guard's reason and never read, and the
 *       FIFO run finishes with a receipt; a header replaced after it was
 *       classified is refused rather than rewritten.
 *
 * FIXTURE HOME ONLY. Every path is under a per-run temporary directory, the
 * child CLI runs get a clean environment pointing only there, and the proof
 * refuses to start when the fixture home, or any tool home it exports,
 * resolves to or inside the operator's real home.
 */
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import type http from "node:http";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";
import { parse as parseToml } from "smol-toml";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import type { LocalProducerSource } from "../packages/collector-cli/src/http-boundary";
import {
  assertProducerToken,
  loadOrCreateLocalIngestAuth,
  readLocalIngestAuth,
  rotateLocalProducerToken,
} from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { useFixtureRoot } from "./lib/fixture-root";

type Check = { name: string; passed: boolean; detail: Record<string, unknown> };

const repoRoot = path.resolve(import.meta.dirname, "..");
const cli = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const loader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
const observerPreload = path.join(repoRoot, "scripts", "lib", "rotation-read-observer.cjs");
const checks: Check[] = [];
const SOURCES = ["claude_code", "codex", "gemini_cli", "grok"] as const satisfies readonly LocalProducerSource[];
const AUTH_FIELDS = {
  claude_code: "claudeCodeProducer",
  codex: "codexProducer",
  gemini_cli: "geminiCliProducer",
  grok: "grokProducer",
} as const;
const RECEIPT_KEYS = ["status", "source", "rotated", "graceSeconds", "previousTokenExpiresAt", "targets", "nextSteps"];
const GRACE_SECONDS = 600;
/** Set by this proof to the fixture HOME it created; the guard requires it. */
const FIXTURE_HOME_ENV = "PLIMSOLL_ROTATION_PROOF_FIXTURE_HOME";
const TOOL_HOME_VARIABLES = ["HOME", "USERPROFILE", "PLIMSOLL_HOME", "CODEX_HOME", "GROK_HOME", "CLAUDE_CONFIG_DIR"];

/** Record a named result; a failure is collected, not thrown, so every red check is named. */
function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  checks.push({ name, passed: Boolean(condition), detail });
}

function sha256(value: string | Buffer) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function digestOf(file: string) {
  return fs.existsSync(file) ? sha256(fs.readFileSync(file)) : "absent";
}

/**
 * Resolve through symlinks as far as the path exists, then append the missing
 * suffix — the production guard's rule (collector-config fixture-root.ts). A
 * plain `path.resolve` fallback let `<fixture>/link-to-real-home/new-child`
 * pass as inside the fixture (review r1 F6).
 */
function realish(value: string) {
  const pending: string[] = [];
  let current = path.resolve(value);
  for (;;) {
    try {
      return path.join(fs.realpathSync(current), ...pending.reverse());
    } catch {
      const parent = path.dirname(current);
      if (parent === current) return path.resolve(value);
      pending.push(path.basename(current));
      current = parent;
    }
  }
}

function insideOrEqual(child: string, parent: string) {
  const relative = path.relative(realish(parent), realish(child));
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

/**
 * Refuse to run against the real home: the env var must name the fixture home
 * this run created, every tool-home variable must sit inside the sandbox, and
 * neither the sandbox nor any of those homes may be (or be inside) the
 * operator's home from the password database. Returns the refusal, or null.
 */
function fixtureHomeRefusal(sandbox: string, env: NodeJS.ProcessEnv, realHome = os.userInfo().homedir) {
  const declared = env[FIXTURE_HOME_ENV];
  if (typeof declared !== "string" || !path.isAbsolute(declared)) return `${FIXTURE_HOME_ENV}_unset`;
  if (env.HOME !== declared) return "home_is_not_the_declared_fixture_home";
  if (insideOrEqual(sandbox, realHome) || insideOrEqual(realHome, sandbox)) return "sandbox_overlaps_real_home";
  for (const variable of TOOL_HOME_VARIABLES) {
    const value = env[variable];
    if (typeof value !== "string" || !insideOrEqual(value, sandbox)) return `${variable}_outside_sandbox`;
    if (insideOrEqual(value, realHome)) return `${variable}_inside_real_home`;
  }
  return null;
}

/**
 * `observer` preloads scripts/lib/rotation-read-observer.cjs into the child
 * (review r3 G3 only) and `timeoutMs` bounds a run that must not block.
 */
function runCli(
  args: string[],
  env: Record<string, string>,
  options: { observer?: Record<string, string>; timeoutMs?: number } = {},
) {
  const preload = options.observer ? ["--require", observerPreload] : [];
  const started = Date.now();
  const result = spawnSync(process.execPath, [...preload, "--import", loader, cli, ...args], {
    cwd: repoRoot,
    env: {
      PATH: process.env.PATH,
      LANG: "en_US.UTF-8",
      TZ: "UTC",
      PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "300",
      ...env,
      ...options.observer,
    },
    encoding: "utf8",
    timeout: options.timeoutMs ?? 180_000,
  });
  return {
    code: result.status,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
    timedOut: (result.error as NodeJS.ErrnoException | undefined)?.code === "ETIMEDOUT",
    elapsedMs: Date.now() - started,
  };
}

/** The last complete JSON document a command printed (plan lines may precede it). */
function lastJson(stdout: string): Record<string, any> {
  try {
    const start = stdout.lastIndexOf("\n{");
    return JSON.parse(start === -1 ? stdout.slice(stdout.indexOf("{")) : stdout.slice(start + 1));
  } catch {
    return {};
  }
}

function backups(directory: string) {
  return fs.existsSync(directory)
    ? fs.readdirSync(directory).filter((name) => name.includes(".plimsoll-backup-")).sort()
    : [];
}

function readJson(file: string): Record<string, any> {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

/** Every producer token a Claude settings file carries: OTLP env header and hook headers. */
function claudeTokens(file: string) {
  const settings = readJson(file);
  const otlp = String(settings.env?.OTEL_EXPORTER_OTLP_HEADERS ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .find((entry) => entry.startsWith("x-plimsoll-token="))
    ?.slice("x-plimsoll-token=".length);
  const hooks = Object.values(settings.hooks ?? {}).flatMap((groups: any) =>
    (groups as any[]).flatMap((group) => (group.hooks ?? []).map((handler: any) => handler.headers?.["x-plimsoll-token"]))
  ).filter((token): token is string => typeof token === "string");
  return { otlp, otlpHeaders: String(settings.env?.OTEL_EXPORTER_OTLP_HEADERS ?? ""), hooks };
}

function codexExporterTokens(file: string) {
  const document = parseToml(fs.readFileSync(file, "utf8")) as Record<string, any>;
  return ["exporter", "trace_exporter", "metrics_exporter"]
    .map((exporter) => document.otel?.[exporter]?.["otlp-http"]?.headers?.["x-plimsoll-token"]);
}

function headerFileToken(file: string) {
  return fs.readFileSync(file, "utf8").match(/^x-plimsoll-token: ([A-Za-z0-9_-]{43})\n$/)?.[1];
}

function geminiEndpoint(file: string) {
  return new URL(String(readJson(file).telemetry?.otlpEndpoint));
}

async function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-rotation-proof-"));
  const home = path.join(sandbox, "home");
  const fixture = useFixtureRoot(sandbox, { home, plimsollHome: path.join(home, ".plimsoll") });
  process.env[FIXTURE_HOME_ENV] = home;
  const env: Record<string, string> = { ...fixture.env, [FIXTURE_HOME_ENV]: home };
  const refusal = fixtureHomeRefusal(sandbox, process.env);
  if (refusal !== null) {
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
    throw new Error(`refusing to run outside a fixture home: ${refusal}`);
  }
  const realHome = os.userInfo().homedir;
  check("fixture_guard_refuses_the_real_home", [
    fixtureHomeRefusal(sandbox, { ...process.env, HOME: realHome, [FIXTURE_HOME_ENV]: realHome }),
    fixtureHomeRefusal(sandbox, { ...process.env, [FIXTURE_HOME_ENV]: undefined }),
    fixtureHomeRefusal(sandbox, { ...process.env, CODEX_HOME: path.join(realHome, ".codex") }),
    fixtureHomeRefusal(realHome, { ...process.env }),
  ].every((reason) => reason !== null) && refusal === null, { fixtureHome: "sandbox/home" });
  // A link inside the sandbox pointing at the real home, plus a child that does
  // not exist. Only the link is created; nothing below the real home is.
  const realHomeLink = path.join(sandbox, "link-to-real-home");
  fs.symlinkSync(realHome, realHomeLink);
  const throughLink = fixtureHomeRefusal(sandbox, {
    ...process.env,
    CODEX_HOME: path.join(realHomeLink, "rotation-proof-nonexistent", ".codex"),
  });
  fs.unlinkSync(realHomeLink);
  check("fixture_guard_refuses_a_symlink_to_the_real_home_with_a_nonexistent_child",
    throughLink !== null, { refusal: throughLink });

  const plimsollHome = env.PLIMSOLL_HOME!;
  const claudeFile = path.join(home, ".claude", "settings.json");
  const seatsRoot = path.join(home, ".claude-seats");
  const managedSeatFile = path.join(seatsRoot, "rot-managed-seat", "settings.json");
  const unmanagedSeatFile = path.join(seatsRoot, "rot-unmanaged-seat", "settings.json");
  const malformedSeatFile = path.join(seatsRoot, "rot-malformed-seat", "settings.json");
  const absentSeatFile = path.join(seatsRoot, "rot-absent-seat", "settings.json");
  const codexHome = path.join(home, ".codex");
  const codexConfig = path.join(codexHome, "config.toml");
  const codexHeader = path.join(codexHome, "plimsoll.headers");
  const codexProfileConfig = path.join(home, ".codex-profiles", "rot-profile", "config.toml");
  const geminiFile = path.join(home, ".gemini", "settings.json");
  const grokHooks = path.join(home, ".grok", "hooks", "plimsoll.json");
  const grokHeader = path.join(home, ".grok", "hooks", "plimsoll.headers");

  let server: http.Server | undefined;
  let ledger: LocalEventBuffer | undefined;
  try {
    // ---- Fixture: setup manages one seat and one profile ------------------
    fs.mkdirSync(plimsollHome, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(plimsollHome, "collector.config.json"), `${JSON.stringify({ port: 48271 }, null, 2)}\n`, { mode: 0o600 });
    fs.mkdirSync(path.dirname(managedSeatFile), { recursive: true, mode: 0o700 });
    fs.writeFileSync(managedSeatFile, `${JSON.stringify({ model: "synthetic-seat-model" }, null, 2)}\n`, { mode: 0o600 });
    fs.mkdirSync(path.dirname(absentSeatFile), { recursive: true, mode: 0o700 });
    fs.mkdirSync(path.dirname(codexProfileConfig), { recursive: true, mode: 0o700 });
    fs.writeFileSync(codexProfileConfig, 'model = "synthetic-profile-model"\n', { mode: 0o600 });
    const setup = runCli(["setup", "--yes"], env);
    // Written after setup: never given the managed block, and unparseable.
    fs.mkdirSync(path.dirname(unmanagedSeatFile), { recursive: true, mode: 0o700 });
    fs.writeFileSync(unmanagedSeatFile, `${JSON.stringify({ env: { SYNTHETIC: "1" } }, null, 2)}\n`, { mode: 0o600 });
    fs.mkdirSync(path.dirname(malformedSeatFile), { recursive: true, mode: 0o700 });
    fs.writeFileSync(malformedSeatFile, "{ synthetic-malformed-seat", { mode: 0o600 });
    const provisioned = readLocalIngestAuth(plimsollHome);
    // A legacy Grok fragment that still embeds the token inline in each command.
    if (provisioned && fs.existsSync(grokHooks)) {
      const fragment = readJson(grokHooks);
      const legacy = `if [ -n "\${GROK_HOOK_EVENT:-}" ]; then curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-source: grok' -H 'x-plimsoll-token: ${provisioned.grokProducer}' --data-binary @- http://127.0.0.1:48271/hooks/grok || true; fi`;
      for (const groups of Object.values(fragment.hooks) as any[]) {
        for (const group of groups) for (const handler of group.hooks) handler.command = legacy;
      }
      fs.writeFileSync(grokHooks, `${JSON.stringify(fragment, null, 2)}\n`, { mode: 0o600 });
    }
    for (const directory of new Set([claudeFile, managedSeatFile, codexConfig, codexProfileConfig, geminiFile, grokHooks].map(path.dirname))) {
      for (const name of backups(directory)) fs.rmSync(path.join(directory, name));
    }
    check("fixture_setup_manages_every_source_surface", setup.code === 0 && provisioned !== null &&
      claudeTokens(claudeFile).otlp === provisioned.claudeCodeProducer &&
      claudeTokens(managedSeatFile).otlp === provisioned.claudeCodeProducer &&
      codexExporterTokens(codexConfig).every((token) => token === provisioned.codexProducer) &&
      codexExporterTokens(codexProfileConfig).every((token) => token === provisioned.codexProducer) &&
      geminiEndpoint(geminiFile).searchParams.get("x-plimsoll-token") === provisioned.geminiCliProducer &&
      headerFileToken(grokHeader) === provisioned.grokProducer &&
      fs.readFileSync(grokHooks, "utf8").includes(provisioned.grokProducer!) &&
      !fs.existsSync(absentSeatFile), { setupExit: setup.code, stderr: setup.stderr.slice(0, 400) });
    if (!provisioned) throw new Error("fixture setup did not provision credentials");

    // ---- The running collector, loaded before any rotation ---------------
    let clockOffsetMs = 0;
    ledger = new LocalEventBuffer(path.join(sandbox, "ledger.sqlite"));
    server = createCollectorServer(collectorConfigSchema.parse({ port: 48271 }), ledger, {
      localAuth: provisioned,
      localAuthHome: plimsollHome,
      producerAuthNowMs: () => Date.now() + clockOffsetMs,
    });
    await new Promise<void>((resolve, reject) => {
      server!.once("error", reject);
      server!.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;
    let requestSerial = 0;
    const post = async (route: string, headers: Record<string, string>, body: unknown) => {
      const response = await fetch(`http://127.0.0.1:${port}${route}`, {
        method: "POST",
        headers: { "content-type": "application/json", ...headers },
        body: JSON.stringify(body),
      });
      const text = await response.text();
      let reason: unknown;
      try {
        reason = (JSON.parse(text) as Record<string, unknown>).reason;
      } catch {
        reason = undefined;
      }
      return { status: response.status, reason };
    };
    const emptyLogs = { resourceLogs: [] };
    const hookBody = (event: string) => ({
      id: crypto.randomUUID(),
      session_id: `rotation-proof-${(requestSerial += 1)}`,
      hook_event_name: event,
      timestamp: "2026-09-16T05:00:00.000Z",
    });
    /** One request exactly as the source's producer sends it with `token`. */
    const producerRequest: Record<(typeof SOURCES)[number], (token: string) => ReturnType<typeof post>> = {
      claude_code: (token) => post("/v1/logs", { "x-plimsoll-source": "claude_code", "x-plimsoll-token": token }, emptyLogs),
      codex: (token) => post("/v1/logs", { "x-plimsoll-source": "codex", "x-plimsoll-token": token }, emptyLogs),
      gemini_cli: (token) => post(`/gemini/v1/logs?x-plimsoll-token=${encodeURIComponent(token)}`, {}, emptyLogs),
      grok: (token) => post("/hooks/grok", { "x-plimsoll-token": token }, hookBody("Stop")),
    };
    /** The request the rewritten surface itself makes: its token read back from disk. */
    const surfaceRequest: Record<(typeof SOURCES)[number], () => ReturnType<typeof post>> = {
      claude_code: () => {
        const seat = claudeTokens(managedSeatFile);
        const headers = Object.fromEntries(seat.otlpHeaders.split(",").map((entry) => entry.split("=") as [string, string]));
        return post("/v1/logs", headers, emptyLogs);
      },
      codex: () => {
        const document = parseToml(fs.readFileSync(codexConfig, "utf8")) as Record<string, any>;
        return post("/v1/logs", document.otel.exporter["otlp-http"].headers, emptyLogs);
      },
      gemini_cli: () => {
        const endpoint = geminiEndpoint(geminiFile);
        return post(`${endpoint.pathname}/v1/logs${endpoint.search}`, {}, emptyLogs);
      },
      grok: () => post("/hooks/grok", { "x-plimsoll-token": headerFileToken(grokHeader) ?? "" }, hookBody("Stop")),
    };
    const baseline = await Promise.all(SOURCES.map((source) => producerRequest[source](provisioned[AUTH_FIELDS[source]]!)));
    check("running_collector_admits_every_provisioned_token_before_rotation",
      baseline.every((response) => response.status === 202), { statuses: baseline.map((response) => response.status) });

    /** Every managed surface per source, for isolation and dry-run digests. */
    const surfaces: Record<(typeof SOURCES)[number], string[]> = {
      claude_code: [claudeFile, managedSeatFile],
      codex: [codexHeader, codexConfig, codexProfileConfig],
      gemini_cli: [geminiFile],
      grok: [grokHeader, grokHooks],
    };
    const untouchedSeats = [unmanagedSeatFile, malformedSeatFile];

    for (const source of SOURCES) {
      clockOffsetMs = 0;
      const field = AUTH_FIELDS[source];
      const before = readLocalIngestAuth(plimsollHome)!;
      const superseded = before[field]!;
      const allFiles = [...Object.values(surfaces).flat(), ...untouchedSeats];
      const digestsBefore = Object.fromEntries(allFiles.map((file) => [file, digestOf(file)]));
      const directories = [...new Set(allFiles.map(path.dirname))];
      const backupsBefore = Object.fromEntries(directories.map((directory) => [directory, backups(directory).length]));

      // a) dry run
      const dry = runCli(["rotate-producer-token", "--source", source, "--dry-run"], env);
      const dryReceipt = lastJson(dry.stdout);
      check(`${source}_dry_run_mints_nothing_and_writes_nothing`,
        dry.code === 0 && dryReceipt.status === "rotation_dry_run" && dryReceipt.source === source &&
          readLocalIngestAuth(plimsollHome)![field] === superseded &&
          allFiles.every((file) => digestOf(file) === digestsBefore[file]) &&
          directories.every((directory) => backups(directory).length === backupsBefore[directory]) &&
          (dryReceipt.targets ?? []).every((target: any) => target.status === "would_rotate" ||
            (target.path === absentSeatFile && target.status === "absent")),
        { exit: dry.code, status: dryReceipt.status, targets: dryReceipt.targets?.length });

      // b) rotation
      const rotate = runCli(["rotate-producer-token", "--source", source, "--grace-seconds", String(GRACE_SECONDS)], env);
      const receipt = lastJson(rotate.stdout);
      const after = readLocalIngestAuth(plimsollHome)!;
      const replacement = after[field]!;
      const receiptTargets = (receipt.targets ?? []) as Array<{ path: string; status: string; backup: string | null }>;
      const rotatedTargets = receiptTargets.filter((target) => target.status === "rotated");
      const everyToken = [superseded, replacement, after.managementRead,
        ...SOURCES.map((other) => after[AUTH_FIELDS[other]]!)];
      check(`${source}_receipt_has_the_codex_shape_and_prints_no_token`,
        rotate.code === 0 && receipt.status === "rotation_applied" && receipt.source === source &&
          receipt.rotated === true && receipt.graceSeconds === GRACE_SECONDS &&
          typeof receipt.previousTokenExpiresAt === "string" &&
          Math.abs(Date.parse(receipt.previousTokenExpiresAt) - after.rotations?.[source]?.expiresAt!) < 1 &&
          RECEIPT_KEYS.every((key) => Object.hasOwn(receipt, key)) &&
          receiptTargets.every((target) => typeof target.path === "string" && typeof target.status === "string") &&
          (receipt.nextSteps ?? []).some((step: string) => step.includes(`producerTokenRotation.${source}`)) &&
          everyToken.every((token) => !rotate.stdout.includes(token) && !rotate.stderr.includes(token)),
        { exit: rotate.code, status: receipt.status, keys: Object.keys(receipt), stderr: rotate.stderr.slice(0, 400) });

      const surfaceState = {
        claude_code: () => [claudeFile, managedSeatFile].every((file) => {
          const tokens = claudeTokens(file);
          return tokens.otlp === replacement && tokens.hooks.length === 3 && tokens.hooks.every((token) => token === replacement);
        }),
        codex: () => headerFileToken(codexHeader) === replacement &&
          [codexConfig, codexProfileConfig].every((file) => codexExporterTokens(file).every((token) => token === replacement)),
        gemini_cli: () => geminiEndpoint(geminiFile).searchParams.get("x-plimsoll-token") === replacement,
        grok: () => headerFileToken(grokHeader) === replacement &&
          !fs.readFileSync(grokHooks, "utf8").includes("x-plimsoll-token:") &&
          fs.readFileSync(grokHooks, "utf8").includes(`@${grokHeader}`),
      }[source];
      const ownDirectories = [...new Set(surfaces[source].map(path.dirname))];
      check(`${source}_rotation_rewrites_every_managed_surface_with_a_backup_beside_it`,
        replacement !== superseded && after.rotations?.[source]?.token === superseded &&
          surfaceState() &&
          surfaces[source].every((file) => !fs.readFileSync(file, "utf8").includes(superseded)) &&
          surfaces[source].every((file) => rotatedTargets.some((target) => target.path === file)) &&
          rotatedTargets.every((target) => target.backup !== null && fs.existsSync(target.backup) &&
            path.dirname(target.backup) === path.dirname(target.path)) &&
          ownDirectories.every((directory) => backups(directory).length ===
            backupsBefore[directory]! + rotatedTargets.filter((target) => path.dirname(target.path) === directory).length),
        {
          rotatedTargets: rotatedTargets.length,
          surfaces: surfaces[source].length,
          statuses: receiptTargets.map((target) => target.status),
        });

      check(`${source}_rotation_leaves_other_sources_and_unmanaged_seats_untouched`,
        SOURCES.filter((other) => other !== source).every((other) => after[AUTH_FIELDS[other]] === before[AUTH_FIELDS[other]]) &&
          after.managementRead === before.managementRead &&
          allFiles.filter((file) => !surfaces[source].includes(file)).every((file) => digestOf(file) === digestsBefore[file]) &&
          directories.filter((directory) => !ownDirectories.includes(directory))
            .every((directory) => backups(directory).length === backupsBefore[directory]) &&
          !fs.existsSync(absentSeatFile) &&
          (source !== "claude_code" || (
            JSON.stringify((receipt.seatsSkipped ?? []).map((entry: any) => [entry.slug, entry.reason])) ===
              JSON.stringify([["rot-malformed-seat", "claude_seat_settings_unreadable"], ["rot-unmanaged-seat", "claude_seat_settings_unmanaged"]]) &&
            !rotate.stdout.includes("synthetic-malformed-seat"))),
        { seatsSkipped: receipt.seatsSkipped ?? null });
      if (source === "claude_code") {
        check("claude_code_absent_discovered_seat_is_reported_absent_and_never_created",
          receiptTargets.filter((target) => target.path === absentSeatFile).length === 1 &&
            JSON.stringify(receiptTargets.find((target) => target.path === absentSeatFile)) ===
              JSON.stringify({ path: absentSeatFile, status: "absent", backup: null }) &&
            (dryReceipt.targets ?? []).some((target: any) => target.path === absentSeatFile && target.status === "absent") &&
            !fs.existsSync(absentSeatFile) && fs.existsSync(path.dirname(absentSeatFile)),
          { target: receiptTargets.find((target) => target.path === absentSeatFile) ?? null });
      }

      // c) the running collector
      const fromSurface = await surfaceRequest[source]();
      const oldInGrace = await producerRequest[source](superseded);
      const foreignSource = source === "codex" ? "claude_code" : "codex";
      const foreign = await producerRequest[source](after[AUTH_FIELDS[foreignSource]]!);
      check(`${source}_running_collector_admits_the_rewritten_surface_token_immediately`,
        fromSurface.status === 202, { status: fromSurface.status, reason: fromSurface.reason });
      check(`${source}_running_collector_admits_the_superseded_token_inside_the_grace_window`,
        oldInGrace.status === 202, { status: oldInGrace.status, reason: oldInGrace.reason });
      check(`${source}_running_collector_refuses_a_foreign_token`,
        foreign.status === 401 && foreign.reason === "producer_token_invalid", { status: foreign.status, reason: foreign.reason });
      clockOffsetMs = after.rotations![source]!.expiresAt - Date.now() + 1_000;
      const oldAfterDeadline = await producerRequest[source](superseded);
      const surfaceAfterDeadline = await surfaceRequest[source]();
      clockOffsetMs = 0;
      check(`${source}_running_collector_refuses_the_superseded_token_after_the_deadline`,
        oldAfterDeadline.status === 401 && oldAfterDeadline.reason === "producer_token_invalid",
        { status: oldAfterDeadline.status, reason: oldAfterDeadline.reason });
      check(`${source}_rewritten_surface_keeps_working_after_the_deadline`,
        surfaceAfterDeadline.status === 202, { status: surfaceAfterDeadline.status, reason: surfaceAfterDeadline.reason });

      // d) doctor
      const doctor = runCli(["doctor", "--read-only", "--json"], env);
      const doctorReceipt = lastJson(doctor.stdout);
      const rotationState = doctorReceipt.producerTokenRotation?.[source];
      check(`${source}_doctor_reports_the_rotation_window_value_blind`,
        rotationState?.state === "active" && rotationState.expiresAt === receipt.previousTokenExpiresAt &&
          rotationState.secondsRemaining > 0 && rotationState.secondsRemaining <= GRACE_SECONDS &&
          everyToken.every((token) => !doctor.stdout.includes(token)),
        { rotation: rotationState ?? null });
    }

    const authFile = path.join(plimsollHome, "local-ingest-auth.json");

    // ---- F1: Gemini rotation changes only the token query value ----------
    {
      const geminiBaseline = fs.readFileSync(geminiFile, "utf8");
      const settings = readJson(geminiFile);
      const managedEndpoint = String(settings.telemetry.otlpEndpoint);
      const nonDefault = {
        theme: "synthetic-foreign-theme",
        telemetry: {
          ...settings.telemetry,
          enabled: false,
          traces: true,
          otlpEndpoint: `${managedEndpoint}&synthetic-keep=a%20b+c#synthetic-fragment`,
          syntheticForeignTelemetry: { nested: [1, "two"] },
        },
        syntheticForeignTopLevel: { keep: true },
      };
      // Four-space indent: a re-serialized document could not keep these bytes.
      const nonDefaultBytes = `${JSON.stringify(nonDefault, null, 4)}\n`;
      fs.writeFileSync(geminiFile, nonDefaultBytes, { mode: 0o600 });
      const previous = readLocalIngestAuth(plimsollHome)!.geminiCliProducer!;
      const rotated = runCli(["rotate-producer-token", "--source", "gemini_cli", "--grace-seconds", "60"], env);
      const rotatedReceipt = lastJson(rotated.stdout);
      const next = readLocalIngestAuth(plimsollHome)!.geminiCliProducer!;
      const expectedBytes = nonDefaultBytes.replace(`x-plimsoll-token=${previous}`, `x-plimsoll-token=${next}`);
      const afterBytes = fs.readFileSync(geminiFile, "utf8");
      const afterEndpoint = geminiEndpoint(geminiFile);
      const geminiTarget = (rotatedReceipt.targets ?? [])[0];
      check("gemini_cli_rotation_changes_only_the_token_query_value",
        rotated.code === 0 && rotatedReceipt.status === "rotation_applied" && next !== previous &&
          afterBytes === expectedBytes && afterBytes !== nonDefaultBytes &&
          afterEndpoint.searchParams.get("x-plimsoll-token") === next &&
          afterEndpoint.searchParams.get("synthetic-keep") === "a b c" &&
          afterEndpoint.hash === "#synthetic-fragment" &&
          readJson(geminiFile).telemetry.enabled === false && readJson(geminiFile).telemetry.traces === true &&
          geminiTarget?.status === "rotated" && typeof geminiTarget.backup === "string" &&
          fs.readFileSync(geminiTarget.backup, "utf8") === nonDefaultBytes &&
          ![previous, next].some((token) => rotated.stdout.includes(token) || rotated.stderr.includes(token)),
        { exit: rotated.code, status: rotatedReceipt.status, bytesMatch: afterBytes === expectedBytes, target: geminiTarget?.status ?? null });

      const refusal = (label: string, endpoint: string, reason: string, encode = (text: string) => text) => {
        const bytes = encode(`${JSON.stringify({ ...nonDefault, telemetry: { ...nonDefault.telemetry, otlpEndpoint: endpoint } }, null, 2)}\n`);
        fs.writeFileSync(geminiFile, bytes, { mode: 0o600 });
        const credentialBefore = digestOf(authFile);
        const backupsBeforeRefusal = backups(path.dirname(geminiFile)).length;
        const refused = runCli(["rotate-producer-token", "--source", "gemini_cli"], env);
        const refusedReceipt = lastJson(refused.stdout);
        check(label,
          refused.code === 1 && refusedReceipt.status === "rotation_refused" && refusedReceipt.rotated === false &&
            JSON.stringify(refusedReceipt.targets) === JSON.stringify([{ path: geminiFile, status: "refused", reason }]) &&
            (refusedReceipt.nextSteps ?? []).some((step: string) => step.startsWith("plimsoll setup --yes")) &&
            digestOf(authFile) === credentialBefore && fs.readFileSync(geminiFile, "utf8") === bytes &&
            backups(path.dirname(geminiFile)).length === backupsBeforeRefusal &&
            !refused.stdout.includes(next) && !refused.stderr.includes(next),
          { exit: refused.code, status: refusedReceipt.status, targets: refusedReceipt.targets ?? null });
      };
      const endpointWithoutToken = new URL(managedEndpoint);
      endpointWithoutToken.searchParams.delete("x-plimsoll-token");
      refusal("gemini_cli_unmanaged_settings_refuse_rotation_without_minting",
        endpointWithoutToken.toString(), "gemini_settings_unmanaged");
      refusal("gemini_cli_malformed_endpoint_refuses_rotation_without_minting",
        `http://[synthetic-malformed/v1?x-plimsoll-token=${next}`, "gemini_settings_malformed");
      // A percent-encoded second token parameter after the real one: the raw
      // query has one, the exporter's URL parser sees two. Refused by the
      // preflight, not after minting.
      refusal("gemini_cli_ambiguous_token_parameter_refuses_rotation_without_minting",
        `${managedEndpoint.split("?")[0]}?x-plimsoll-token=${next}&x-plimsoll%2Dtoken=synthetic`, "gemini_settings_malformed");
      // Valid JSON whose parameter is escaped in the file: it cannot be replaced
      // in place, and re-serializing could alter other values, so it is refused.
      refusal("gemini_cli_json_escaped_token_parameter_refuses_rotation_without_minting",
        managedEndpoint.replace(/x-plimsoll-token=[A-Za-z0-9_-]{43}/, `x-plimsoll-token=${next}`), "gemini_settings_malformed",
        (text) => text.replace("x-plimsoll-token=", "x-plimsoll-token\\u003d"));
      fs.writeFileSync(geminiFile, geminiBaseline.replace(/x-plimsoll-token=[A-Za-z0-9_-]{43}/, `x-plimsoll-token=${next}`), { mode: 0o600 });
    }

    // ---- F2: a discovered target that is not rewritten is not success ----
    {
      const incomplete = (
        label: string,
        source: "claude_code" | "codex",
        target: string,
        expected: { status: string; reason: (reason: string) => boolean; dryRunRefused: boolean },
        arrange: () => () => void,
      ) => {
        const beforeAuth = readLocalIngestAuth(plimsollHome)!;
        const restore = arrange();
        try {
        const targetBytes = fs.readFileSync(target);
        const dry = lastJson(runCli(["rotate-producer-token", "--source", source, "--dry-run"], env).stdout);
        const dryEntry = (dry.targets ?? []).find((candidate: any) => candidate.path === target);
        const dryMatches = expected.dryRunRefused
          ? dryEntry?.status === "would_refuse" && expected.reason(String(dryEntry.reason ?? ""))
          : dryEntry?.status === "would_rotate";
        const dryMintedNothing = readLocalIngestAuth(plimsollHome)![AUTH_FIELDS[source]] === beforeAuth[AUTH_FIELDS[source]];
        const run = runCli(["rotate-producer-token", "--source", source, "--grace-seconds", "120"], env);
        const receipt = lastJson(run.stdout);
        const afterAuth = readLocalIngestAuth(plimsollHome)!;
        const entry = (receipt.targets ?? []).find((candidate: any) => candidate.path === target);
        const others = (receipt.targets ?? []).filter((candidate: any) => candidate.path !== target && candidate.status !== "absent");
        check(label,
          dryMatches && dryMintedNothing &&
            run.code === 1 && receipt.status === "rotation_incomplete" && receipt.rotated === true &&
            entry?.status === expected.status && expected.reason(String(entry?.reason ?? "")) &&
            fs.readFileSync(target).equals(targetBytes) &&
            others.length > 0 && others.every((candidate: any) => candidate.status === "rotated") &&
            afterAuth[AUTH_FIELDS[source]] !== beforeAuth[AUTH_FIELDS[source]] &&
            afterAuth.rotations?.[source]?.token === beforeAuth[AUTH_FIELDS[source]] &&
            afterAuth.rotations?.[source]?.expiresAt! > Date.now() &&
            receipt.previousTokenExpiresAt === new Date(afterAuth.rotations![source]!.expiresAt).toISOString() &&
            (receipt.nextSteps ?? []).some((step: string) => step.includes("plimsoll setup --yes") && step.includes("do not run rotate-producer-token again")) &&
            ![afterAuth[AUTH_FIELDS[source]]!, beforeAuth[AUTH_FIELDS[source]]!].some((token) => run.stdout.includes(token)),
          { exit: run.code, status: receipt.status, dryRun: dryEntry?.status ?? null, target: entry ? { status: entry.status, reason: entry.reason } : null, others: others.map((candidate: any) => candidate.status) });
        } finally {
          restore();
        }
      };
      const hardlink = path.join(sandbox, "managed-seat-hardlink");
      incomplete("claude_code_hardlinked_discovered_seat_makes_rotation_incomplete", "claude_code", managedSeatFile,
        { status: "refused", reason: (reason) => reason === "CLAUDE_CONFIG_UNSAFE_LEAF_LINK_COUNT", dryRunRefused: true },
        () => {
          fs.linkSync(managedSeatFile, hardlink);
          return () => fs.rmSync(hardlink, { force: true });
        });
      incomplete("claude_code_unwritable_discovered_seat_makes_rotation_incomplete", "claude_code", managedSeatFile,
        { status: "failed", reason: (reason) => reason === "CLAUDE_CONFIG_IO_FAILURE", dryRunRefused: false },
        () => {
          fs.chmodSync(path.dirname(managedSeatFile), 0o500);
          return () => fs.chmodSync(path.dirname(managedSeatFile), 0o700);
        });
      incomplete("codex_unwritable_discovered_profile_makes_rotation_incomplete", "codex", codexProfileConfig,
        { status: "failed", reason: (reason) => reason.length > 0, dryRunRefused: false },
        () => {
          fs.chmodSync(path.dirname(codexProfileConfig), 0o500);
          return () => fs.chmodSync(path.dirname(codexProfileConfig), 0o700);
        });
      incomplete("codex_refused_discovered_profile_makes_rotation_incomplete", "codex", codexProfileConfig,
        { status: "refused", reason: (reason) => reason.includes("refusing to write"), dryRunRefused: true },
        () => {
          // Still carries the managed token (so it is selected), but its metrics
          // exporter is an inline table the reconciler refuses to rewrite.
          const original = fs.readFileSync(codexProfileConfig, "utf8");
          const section = original.match(/\[otel\.metrics_exporter\.(?:"otlp-http"|otlp-http)\]\n[\s\S]*?(?=\n\[)/)?.[0] ?? "";
          const token = readLocalIngestAuth(plimsollHome)!.codexProducer!;
          fs.writeFileSync(codexProfileConfig, original.replace(section,
            `[otel.metrics_exporter]\n"otlp-http" = { endpoint = "http://127.0.0.1:48271/v1/metrics", headers = { "x-plimsoll-token" = "${token}" } }\n`));
          return () => fs.writeFileSync(codexProfileConfig, original);
        });
      // G2: the Codex writer publishes by atomic rename, which would leave a
      // second hard link on the superseded token while reporting success.
      const profileHardlink = path.join(sandbox, "managed-profile-hardlink");
      incomplete("codex_hardlinked_discovered_profile_makes_rotation_incomplete", "codex", codexProfileConfig,
        { status: "refused", reason: (reason) => reason.includes("CODEX_CONFIG_UNSAFE_LEAF_LINK_COUNT"), dryRunRefused: true },
        () => {
          fs.linkSync(codexProfileConfig, profileHardlink);
          return () => fs.rmSync(profileHardlink, { force: true });
        });
      {
        const ownedHardlink = path.join(sandbox, "owned-codex-config-hardlink");
        fs.linkSync(codexConfig, ownedHardlink);
        const configBytes = fs.readFileSync(codexConfig);
        const credentialBefore = digestOf(authFile);
        const backupsBefore = backups(path.dirname(codexConfig)).length;
        const run = runCli(["rotate-producer-token", "--source", "codex"], env);
        const receipt = lastJson(run.stdout);
        const entry = (receipt.targets ?? []).find((candidate: any) => candidate.path === codexConfig);
        check("codex_hardlinked_owned_config_refuses_rotation_without_minting",
          run.code === 1 && receipt.status === "rotation_refused" && receipt.rotated === false &&
            entry?.status === "refused" && String(entry?.reason ?? "").includes("CODEX_CONFIG_UNSAFE_LEAF_LINK_COUNT") &&
            digestOf(authFile) === credentialBefore &&
            fs.readFileSync(codexConfig).equals(configBytes) && fs.readFileSync(ownedHardlink).equals(configBytes) &&
            backups(path.dirname(codexConfig)).length === backupsBefore &&
            (receipt.nextSteps ?? []).some((step: string) => step.startsWith("plimsoll setup --yes")),
          { exit: run.code, status: receipt.status, target: entry ? { status: entry.status, reason: entry.reason } : null });
        fs.rmSync(ownedHardlink);
      }
      // Put the refused profile back so later sections start from a managed one.
      const repaired = runCli(["setup", "--yes"], env);
      check("setup_re_applies_the_current_token_after_an_incomplete_rotation",
        repaired.code === 0 &&
          codexExporterTokens(codexProfileConfig).every((token) => token === readLocalIngestAuth(plimsollHome)!.codexProducer) &&
          claudeTokens(managedSeatFile).otlp === readLocalIngestAuth(plimsollHome)!.claudeCodeProducer,
        { exit: repaired.code });
    }

    // ---- F4: other managed Grok hook JSON carrying the token -------------
    {
      const hooksDirectory = path.dirname(grokHooks);
      const current = readLocalIngestAuth(plimsollHome)!.grokProducer!;
      const legacyCommand = `if [ -n "\${GROK_HOOK_EVENT:-}" ]; then curl -s --max-time 2 -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-source: grok' -H 'x-plimsoll-token: ${current}' --data-binary @- http://127.0.0.1:48271/hooks/grok || true; fi`;
      const legacyCopy = readJson(grokHooks);
      for (const groups of Object.values(legacyCopy.hooks) as any[]) {
        for (const group of groups) for (const handler of group.hooks) handler.command = legacyCommand;
      }
      const copyFile = path.join(hooksDirectory, "synthetic-legacy-copy.json");
      const unrelatedFile = path.join(hooksDirectory, "synthetic-unrelated.json");
      const foreignTokenFile = path.join(hooksDirectory, "synthetic-foreign-token.json");
      fs.writeFileSync(copyFile, `${JSON.stringify(legacyCopy, null, 2)}\n`, { mode: 0o600 });
      const unrelatedBytes = `${JSON.stringify({ hooks: { Stop: [{ hooks: [{ type: "command", command: "true", timeout: 5 }] }] } })}\n`;
      fs.writeFileSync(unrelatedFile, unrelatedBytes, { mode: 0o600 });
      const foreignTokenBytes = `${JSON.stringify({ hooks: { Stop: [{ hooks: [{ type: "command", command: `synthetic-forwarder --token ${current}`, timeout: 9 }] }] } })}\n`;
      fs.writeFileSync(foreignTokenFile, foreignTokenBytes, { mode: 0o600 });
      // Reached through a symlink (the no-follow rewrite refuses it), plus a
      // directory and a FIFO named *.json that must be ignored, not read.
      const linkTarget = path.join(sandbox, "synthetic-linked-legacy-hook.json");
      const linkBytes = `${JSON.stringify(legacyCopy, null, 2)}\n`;
      fs.writeFileSync(linkTarget, linkBytes, { mode: 0o600 });
      const linkFile = path.join(hooksDirectory, "synthetic-link.json");
      fs.symlinkSync(linkTarget, linkFile);
      const directoryFile = path.join(hooksDirectory, "synthetic-directory.json");
      fs.mkdirSync(directoryFile);
      const fifoFile = path.join(hooksDirectory, "synthetic-fifo.json");
      const fifo = spawnSync("mkfifo", [fifoFile]);
      const copyBackupsBefore = backups(hooksDirectory).length;
      const grokRun = runCli(["rotate-producer-token", "--source", "grok", "--grace-seconds", "60"], env);
      const grokReceipt = lastJson(grokRun.stdout);
      const next = readLocalIngestAuth(plimsollHome)!.grokProducer!;
      const byPath = (file: string) => (grokReceipt.targets ?? []).find((target: any) => target.path === file);
      const copyText = fs.readFileSync(copyFile, "utf8");
      check("grok_other_managed_hook_json_is_rewritten_and_unrelated_hooks_untouched",
        grokRun.code === 0 && grokReceipt.status === "rotation_applied" && next !== current &&
          byPath(copyFile)?.status === "rotated" && typeof byPath(copyFile)?.backup === "string" &&
          path.dirname(byPath(copyFile).backup) === hooksDirectory &&
          !copyText.includes(current) && !copyText.includes("x-plimsoll-token:") && copyText.includes(`@${grokHeader}`) &&
          headerFileToken(grokHeader) === next &&
          fs.readFileSync(unrelatedFile, "utf8") === unrelatedBytes && byPath(unrelatedFile) === undefined &&
          backups(hooksDirectory).length === copyBackupsBefore + (grokReceipt.targets ?? []).filter((target: any) => target.status === "rotated").length &&
          ![current, next].some((token) => grokRun.stdout.includes(token) || grokRun.stderr.includes(token)),
        { exit: grokRun.code, status: grokReceipt.status, copy: byPath(copyFile)?.status ?? null, unrelatedReported: byPath(unrelatedFile) !== undefined });
      check("grok_unclassifiable_token_bearing_hook_json_is_reported_skipped",
        JSON.stringify(byPath(foreignTokenFile)) ===
          JSON.stringify({ path: foreignTokenFile, status: "skipped", backup: null, reason: "grok_hook_unmanaged_token_bearing" }) &&
          fs.readFileSync(foreignTokenFile, "utf8") === foreignTokenBytes &&
          JSON.stringify(byPath(linkFile)) ===
            JSON.stringify({ path: linkFile, status: "skipped", backup: null, reason: "grok_hook_symlink" }) &&
          fs.readFileSync(linkTarget, "utf8") === linkBytes &&
          fifo.status === 0 && byPath(fifoFile) === undefined && byPath(directoryFile) === undefined &&
          (grokReceipt.nextSteps ?? []).some((step: string) => step.includes("Grok hook file beside plimsoll.json")),
        { foreign: byPath(foreignTokenFile) ?? null, link: byPath(linkFile) ?? null, fifoCreated: fifo.status === 0 });
      for (const file of [unrelatedFile, foreignTokenFile, linkFile, fifoFile, linkTarget]) fs.rmSync(file, { force: true });
      fs.rmdirSync(directoryFile);

      // A managed copy the rewrite would refuse (hardlinked) cannot be repaired
      // by setup, so it blocks the rotation before anything is minted.
      const legacyAgain = readLocalIngestAuth(plimsollHome)!.grokProducer!;
      fs.writeFileSync(copyFile, `${JSON.stringify(legacyCopy, null, 2)}\n`.split(current).join(legacyAgain), { mode: 0o600 });
      const copyHardlink = path.join(sandbox, "synthetic-legacy-copy-hardlink.json");
      fs.linkSync(copyFile, copyHardlink);
      const copyBytes = fs.readFileSync(copyFile, "utf8");
      const credentialBeforeBlocked = digestOf(authFile);
      const blocked = runCli(["rotate-producer-token", "--source", "grok"], env);
      const blockedReceipt = lastJson(blocked.stdout);
      check("grok_refusing_hook_json_copy_blocks_rotation_before_minting",
        blocked.code === 1 && blockedReceipt.status === "rotation_refused" &&
          (blockedReceipt.targets ?? []).length === 1 && blockedReceipt.targets[0].path === copyFile &&
          blockedReceipt.targets[0].status === "refused" &&
          digestOf(authFile) === credentialBeforeBlocked && fs.readFileSync(copyFile, "utf8") === copyBytes &&
          (blockedReceipt.nextSteps ?? []).some((step: string) => step.includes("by hand")) &&
          !(blockedReceipt.nextSteps ?? []).some((step: string) => step.startsWith("plimsoll setup")) &&
          !blocked.stdout.includes(legacyAgain),
        { exit: blocked.code, status: blockedReceipt.status, targets: (blockedReceipt.targets ?? []).map((target: any) => target.status) });
      fs.rmSync(copyHardlink);
      fs.rmSync(copyFile);
    }

    // ---- G1: managed Grok hook copies in the header-file form -------------
    {
      const hooksDirectory = path.dirname(grokHooks);
      const current = readLocalIngestAuth(plimsollHome)!.grokProducer!;
      const installed = fs.readFileSync(grokHooks, "utf8");
      const headerBytes = fs.readFileSync(grokHeader, "utf8");
      const sharedCopy = path.join(hooksDirectory, "synthetic-shared-header-copy.json");
      const privateCopy = path.join(hooksDirectory, "synthetic-private-header-copy.json");
      const privateHeader = path.join(hooksDirectory, "synthetic-private-copy.headers");
      const absentCopy = path.join(hooksDirectory, "synthetic-absent-header-copy.json");
      const absentHeader = path.join(hooksDirectory, "synthetic-absent-copy.headers");
      const unrelatedFile = path.join(hooksDirectory, "synthetic-unrelated.json");
      const unrelatedBytes = `${JSON.stringify({ hooks: { Stop: [{ hooks: [{ type: "command", command: "true", timeout: 5 }] }] } })}\n`;
      const privateCopyBytes = installed.split(grokHeader).join(privateHeader);
      const absentCopyBytes = installed.split(grokHeader).join(absentHeader);
      fs.writeFileSync(sharedCopy, installed, { mode: 0o600 });
      fs.writeFileSync(privateHeader, headerBytes, { mode: 0o600 });
      fs.writeFileSync(privateCopy, privateCopyBytes, { mode: 0o600 });
      fs.writeFileSync(absentCopy, absentCopyBytes, { mode: 0o600 });
      fs.writeFileSync(unrelatedFile, unrelatedBytes, { mode: 0o600 });
      const run = runCli(["rotate-producer-token", "--source", "grok", "--grace-seconds", "60"], env);
      const receipt = lastJson(run.stdout);
      const after = readLocalIngestAuth(plimsollHome)!;
      const next = after.grokProducer!;
      const byPath = (file: string) => (receipt.targets ?? []).find((target: any) => target.path === file);
      const noToken = ![current, next].some((token) => run.stdout.includes(token) || run.stderr.includes(token));
      check("grok_shared_header_hook_copy_is_reported_unchanged",
        run.code === 0 && receipt.status === "rotation_applied" && next !== current &&
          JSON.stringify(byPath(sharedCopy)) === JSON.stringify({
            path: sharedCopy, status: "unchanged", backup: null,
            reason: "grok_hook_reads_the_managed_header_file_rotated_above",
          }) &&
          fs.readFileSync(sharedCopy, "utf8") === installed &&
          byPath(grokHeader)?.status === "rotated" && headerFileToken(grokHeader) === next &&
          fs.readFileSync(unrelatedFile, "utf8") === unrelatedBytes && byPath(unrelatedFile) === undefined && noToken,
        { exit: run.code, status: receipt.status, shared: byPath(sharedCopy) ?? null });
      const admits = (token: string | undefined, now: number) => {
        try {
          assertProducerToken(
            { headers: { "x-plimsoll-token": token } } as unknown as http.IncomingMessage,
            after,
            "grok",
            new URL("http://127.0.0.1/hooks/grok"),
            now,
          );
          return true;
        } catch {
          return false;
        }
      };
      const deadline = after.rotations!.grok!.expiresAt;
      const privateEntry = byPath(privateHeader);
      const privateBackup = typeof privateEntry?.backup === "string" ? privateEntry.backup : "";
      const managedBackup = typeof byPath(grokHeader)?.backup === "string" ? byPath(grokHeader).backup : "";
      const superseded = privateBackup && fs.existsSync(privateBackup) ? headerFileToken(privateBackup) : undefined;
      check("grok_private_header_hook_copy_rotates_its_header_file",
        run.code === 0 &&
          JSON.stringify(byPath(privateCopy)) === JSON.stringify({
            path: privateCopy, status: "unchanged", backup: null,
            reason: "grok_hook_reads_its_own_header_file_rotated_as_its_own_target",
          }) &&
          fs.readFileSync(privateCopy, "utf8") === privateCopyBytes &&
          privateEntry?.status === "rotated" && path.dirname(privateBackup) === hooksDirectory &&
          fs.readFileSync(privateBackup, "utf8") === headerBytes &&
          managedBackup !== "" && fs.readFileSync(managedBackup, "utf8") === headerBytes &&
          headerFileToken(privateHeader) === next && (fs.statSync(privateHeader).mode & 0o777) === 0o600 &&
          superseded === current &&
          admits(superseded, deadline - 1) && !admits(superseded, deadline) &&
          admits(headerFileToken(privateHeader), deadline) && noToken,
        {
          copy: byPath(privateCopy) ?? null,
          header: privateEntry ? { status: privateEntry.status, backup: typeof privateEntry.backup } : null,
          supersededBeforeDeadline: admits(superseded, deadline - 1),
          supersededAtDeadline: admits(superseded, deadline),
          headerAtDeadline: admits(headerFileToken(privateHeader), deadline),
        });
      check("grok_private_header_absent_is_reported_skipped",
        JSON.stringify(byPath(absentCopy)) === JSON.stringify({
          path: absentCopy, status: "skipped", backup: null, reason: "grok_hook_header_file_absent",
        }) &&
          byPath(absentHeader) === undefined && !fs.existsSync(absentHeader) &&
          fs.readFileSync(absentCopy, "utf8") === absentCopyBytes &&
          (receipt.nextSteps ?? []).some((step: string) => step.includes("by hand")),
        { absent: byPath(absentCopy) ?? null });

      // The private header file is not managed by setup, so a refusal of its
      // rewrite does not block the rotation, but it is never silent.
      fs.rmSync(absentCopy);
      const headerHardlink = path.join(sandbox, "synthetic-private-copy-hardlink.headers");
      fs.linkSync(privateHeader, headerHardlink);
      const refusedHeaderBytes = fs.readFileSync(privateHeader, "utf8");
      const refusedRun = runCli(["rotate-producer-token", "--source", "grok", "--grace-seconds", "60"], env);
      const refusedReceipt = lastJson(refusedRun.stdout);
      const refusedEntry = (refusedReceipt.targets ?? []).find((target: any) => target.path === privateHeader);
      check("grok_refusing_private_header_file_makes_rotation_incomplete",
        refusedRun.code === 1 && refusedReceipt.status === "rotation_incomplete" &&
          refusedEntry?.status === "refused" && String(refusedEntry?.reason ?? "") === "GROK_CONFIG_UNSAFE_LEAF_LINK_COUNT" &&
          fs.readFileSync(privateHeader, "utf8") === refusedHeaderBytes &&
          readLocalIngestAuth(plimsollHome)!.grokProducer !== next &&
          (refusedReceipt.nextSteps ?? []).some((step: string) => step.includes("by hand")),
        { exit: refusedRun.code, status: refusedReceipt.status, header: refusedEntry ?? null });
      for (const file of [headerHardlink, sharedCopy, privateCopy, privateHeader, unrelatedFile]) fs.rmSync(file, { force: true });
    }

    // ---- G3: header files are classified only through a guarded descriptor --
    {
      const hooksDirectory = path.dirname(grokHooks);
      const installed = fs.readFileSync(grokHooks, "utf8");
      const copy = path.join(hooksDirectory, "synthetic-g3-copy.json");
      const readLog = path.join(sandbox, "g3-reads.log");
      // A run finishes in well under a second; a blocked open is killed here.
      const G3_RUN_BUDGET_MS = 30_000;
      const events = () => fs.existsSync(readLog) ? fs.readFileSync(readLog, "utf8").split("\n").filter(Boolean) : [];
      /**
       * One rotation whose only extra hook is a managed copy reading `reference`,
       * a 0600 header file carrying the current token unless `arrange` places it.
       * `swap` is handed to the observer, which watches `reference`.
       */
      const g3Run = (reference: string, swap: Record<string, string> = {}) => {
        fs.writeFileSync(copy, installed.split(grokHeader).join(reference), { mode: 0o600 });
        fs.rmSync(readLog, { force: true });
        const before = readLocalIngestAuth(plimsollHome)!.grokProducer!;
        const run = runCli(["rotate-producer-token", "--source", "grok", "--grace-seconds", "60"], env, {
          observer: { PLIMSOLL_PROOF_WATCH: reference, PLIMSOLL_PROOF_READ_LOG: readLog, ...swap },
          timeoutMs: G3_RUN_BUDGET_MS,
        });
        const receipt = lastJson(run.stdout);
        const after = readLocalIngestAuth(plimsollHome)!.grokProducer!;
        const entry = (receipt.targets ?? []).find((target: any) => target.path === reference);
        const refusedWith = (reason: string) =>
          !run.timedOut && run.code === 1 && receipt.status === "rotation_incomplete" &&
          entry?.status === "refused" && entry?.reason === reason && entry?.backup === null &&
          (receipt.nextSteps ?? []).some((step: string) => step.includes("by hand")) &&
          ![before, after].some((token) => run.stdout.includes(token) || run.stderr.includes(token));
        return {
          run, receipt, entry, refusedWith, log: events(),
          detail: { exit: run.code, timedOut: run.timedOut, elapsedMs: run.elapsedMs, status: receipt.status ?? null, entry: entry ?? null, events: events() },
        };
      };
      const privateHeaderCopy = (file: string) => {
        fs.copyFileSync(grokHeader, file);
        fs.chmodSync(file, 0o600);
        return fs.readFileSync(file, "utf8");
      };

      // A regular private header reached through a symlinked parent directory.
      const realDirectory = path.join(sandbox, "g3-real-headers");
      const aliasDirectory = path.join(sandbox, "g3-alias-headers");
      fs.mkdirSync(realDirectory, { mode: 0o700 });
      fs.symlinkSync(realDirectory, aliasDirectory);
      const realHeader = path.join(realDirectory, "private.headers");
      const realHeaderBytes = privateHeaderCopy(realHeader);
      const ancestor = g3Run(path.join(aliasDirectory, "private.headers"));
      check("grok_private_header_behind_symlinked_ancestor_is_never_read",
        ancestor.refusedWith("GROK_CONFIG_UNSAFE_ANCESTOR_SYMLINK") &&
          !ancestor.log.includes("open") && !ancestor.log.includes("read") &&
          fs.readFileSync(realHeader, "utf8") === realHeaderBytes,
        ancestor.detail);
      fs.rmSync(aliasDirectory);
      fs.rmSync(realDirectory, { recursive: true });

      const reference = path.join(hooksDirectory, "synthetic-g3.headers");
      const elsewhere = path.join(sandbox, "g3-elsewhere.headers");
      // A regular leaf that becomes a symlink right after its lstat returns.
      privateHeaderCopy(reference);
      const elsewhereBytes = privateHeaderCopy(elsewhere);
      const leafLink = g3Run(reference, { PLIMSOLL_PROOF_SWAP: `symlink:${elsewhere}`, PLIMSOLL_PROOF_SWAP_AT: "lstat" });
      check("grok_private_header_leaf_symlink_is_never_read",
        leafLink.refusedWith("GROK_CONFIG_UNSAFE_LEAF_SYMLINK") &&
          leafLink.log.includes("swap") && !leafLink.log.includes("open") && !leafLink.log.includes("read") &&
          fs.lstatSync(reference).isSymbolicLink() && fs.readFileSync(elsewhere, "utf8") === elsewhereBytes,
        leafLink.detail);
      fs.rmSync(reference);

      // A regular leaf that becomes a FIFO right after its lstat returns: the
      // open must not block, and the run must finish with a receipt.
      privateHeaderCopy(reference);
      const fifo = g3Run(reference, { PLIMSOLL_PROOF_SWAP: "fifo", PLIMSOLL_PROOF_SWAP_AT: "lstat" });
      check("grok_private_header_leaf_fifo_does_not_block",
        fifo.refusedWith("GROK_CONFIG_UNSAFE_LEAF_TYPE") && fifo.run.elapsedMs < G3_RUN_BUDGET_MS &&
          fifo.log.includes("swap") && !fifo.log.includes("read") && fs.lstatSync(reference).isFIFO(),
        fifo.detail);
      fs.rmSync(reference);

      // A regular leaf replaced by a different regular file (same token, same
      // mode) between its lstat and its open: the fstat identity differs.
      privateHeaderCopy(reference);
      const replacementBytes = privateHeaderCopy(elsewhere);
      const replaced = g3Run(reference, { PLIMSOLL_PROOF_SWAP: `file:${elsewhere}`, PLIMSOLL_PROOF_SWAP_AT: "lstat" });
      check("grok_private_header_identity_change_between_stat_and_open_is_refused",
        replaced.refusedWith("GROK_CONFIG_LEAF_CHANGED") &&
          replaced.log.includes("swap") && replaced.log.includes("open") && !replaced.log.includes("read") &&
          fs.readFileSync(reference, "utf8") === replacementBytes,
        replaced.detail);
      fs.rmSync(reference);

      // A header replaced after its bytes were classified: the rewrite is bound
      // to the classified identity, so the replacement is not rewritten.
      privateHeaderCopy(reference);
      const lateBytes = privateHeaderCopy(elsewhere);
      const late = g3Run(reference, { PLIMSOLL_PROOF_SWAP: `file:${elsewhere}`, PLIMSOLL_PROOF_SWAP_AT: "close" });
      check("grok_private_header_replaced_after_classification_is_not_rewritten",
        late.refusedWith("GROK_CONFIG_LEAF_CHANGED") && late.log.includes("swap") &&
          fs.readFileSync(reference, "utf8") === lateBytes &&
          !backups(hooksDirectory).some((name) => name.startsWith("synthetic-g3.headers")),
        late.detail);
      for (const file of [reference, elsewhere, copy, readLog]) fs.rmSync(file, { force: true });
    }

    // ---- F8: grace above the maximum is rejected, not clamped ------------
    {
      const credentialBefore = digestOf(authFile);
      const runs = SOURCES.map((source) => runCli(["rotate-producer-token", "--source", source, "--grace-seconds", "86401"], env));
      check("grace_above_maximum_is_rejected_without_minting",
        runs.every((run) => run.code === 1 && run.stdout === "" && run.stderr.includes("between 1 and 86400")) &&
          digestOf(authFile) === credentialBefore,
        { exits: runs.map((run) => run.code) });
    }

    // ---- e) absent managed files are reported, never created -------------
    fs.rmSync(geminiFile);
    const geminiAbsent = runCli(["rotate-producer-token", "--source", "gemini_cli", "--grace-seconds", "60"], env);
    const geminiAbsentReceipt = lastJson(geminiAbsent.stdout);
    check("gemini_cli_absent_settings_are_reported_absent_and_never_created",
      geminiAbsent.code === 0 && geminiAbsentReceipt.status === "rotation_applied" &&
        JSON.stringify(geminiAbsentReceipt.targets) === JSON.stringify([{ path: geminiFile, status: "absent", backup: null }]) &&
        !fs.existsSync(geminiFile),
      { targets: geminiAbsentReceipt.targets ?? null });
    const grokHooksDigest = digestOf(grokHooks);
    fs.rmSync(grokHeader);
    const grokDry = runCli(["rotate-producer-token", "--source", "grok", "--dry-run"], env);
    const grokAbsent = runCli(["rotate-producer-token", "--source", "grok", "--grace-seconds", "60"], env);
    const grokAbsentReceipt = lastJson(grokAbsent.stdout);
    check("grok_absent_header_file_is_never_created_and_its_hook_is_not_rewritten",
      grokDry.code === 0 && lastJson(grokDry.stdout).targets?.map((target: any) => target.status).join(",") === "absent,skipped" &&
        grokAbsent.code === 0 && grokAbsentReceipt.status === "rotation_applied" &&
        JSON.stringify(grokAbsentReceipt.targets) === JSON.stringify([
          { path: grokHeader, status: "absent", backup: null },
          { path: grokHooks, status: "skipped", backup: null, reason: "grok_header_file_absent" },
        ]) &&
        !fs.existsSync(grokHeader) && digestOf(grokHooks) === grokHooksDigest,
      { targets: grokAbsentReceipt.targets ?? null });
    const credentialDigest = digestOf(path.join(plimsollHome, "local-ingest-auth.json"));
    const unknown = runCli(["rotate-producer-token", "--source", "management"], env);
    check("unknown_source_is_a_usage_error_that_writes_nothing",
      unknown.code === 1 && unknown.stderr.includes("--source <claude_code|codex|gemini_cli|grok>") &&
        unknown.stdout === "" && digestOf(path.join(plimsollHome, "local-ingest-auth.json")) === credentialDigest,
      { exit: unknown.code });

    // ---- F3: the injected clock is load-bearing in every server hunk -----
    // HTTP status alone cannot tell the hunks apart (expiry pruning and token
    // validation cover for each other), so this section counts reads of the
    // credential file and adds an authority with no reload home.
    const clockHome = path.join(sandbox, "clock-auth");
    const clockAuthFile = path.join(clockHome, "local-ingest-auth.json");
    const originalReadFileSync = fs.readFileSync;
    let authReads = 0;
    fs.readFileSync = ((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      if (String(file) === clockAuthFile) authReads += 1;
      return (originalReadFileSync as (...args: unknown[]) => unknown)(file, ...rest);
    }) as typeof fs.readFileSync;
    const clockServers: http.Server[] = [];
    try {
      const initial = loadOrCreateLocalIngestAuth(clockHome);
      let clockNow = Date.now();
      const listen = async (options: Parameters<typeof createCollectorServer>[2]) => {
        const clockServer = createCollectorServer(collectorConfigSchema.parse({}), ledger!, options);
        clockServers.push(clockServer);
        await new Promise<void>((resolve, reject) => {
          clockServer.once("error", reject);
          clockServer.listen(0, "127.0.0.1", resolve);
        });
        return (clockServer.address() as AddressInfo).port;
      };
      const postTo = async (clockPort: number, token: string) => {
        const response = await fetch(`http://127.0.0.1:${clockPort}/v1/logs`, {
          method: "POST",
          headers: { "content-type": "application/json", "x-plimsoll-source": "claude_code", "x-plimsoll-token": token },
          body: JSON.stringify(emptyLogs),
        });
        await response.text();
        return response.status;
      };
      const reloadingPort = await listen({ localAuth: initial, localAuthHome: clockHome, producerAuthNowMs: () => clockNow });
      const rotation = rotateLocalProducerToken(clockHome, "claude_code", { graceMs: GRACE_SECONDS * 1000, now: clockNow });
      const newStatus = await postTo(reloadingPort, rotation.auth.claudeCodeProducer);
      clockNow = rotation.expiresAt;
      let readsBefore = authReads;
      const oldAtDeadline = await postTo(reloadingPort, initial.claudeCodeProducer);
      check("deadline_triggers_reload",
        newStatus === 202 && oldAtDeadline === 401 && authReads === readsBefore + 1,
        { newStatus, oldAtDeadline, reads: authReads - readsBefore });
      readsBefore = authReads;
      const newAfterDeadline = await postTo(reloadingPort, rotation.auth.claudeCodeProducer);
      const newAgain = await postTo(reloadingPort, rotation.auth.claudeCodeProducer);
      check("expired_row_evicted_no_repeat_read",
        newAfterDeadline === 202 && newAgain === 202 && authReads === readsBefore,
        { newAfterDeadline, newAgain, reads: authReads - readsBefore });
      // The rotated authority itself, with its grace row, and no home to reload.
      const cachedPort = await listen({ localAuth: rotation.auth, producerAuthNowMs: () => rotation.expiresAt });
      const cachedOld = await postTo(cachedPort, initial.claudeCodeProducer);
      const cachedNew = await postTo(cachedPort, rotation.auth.claudeCodeProducer);
      check("server_cached_authority_uses_injected_clock",
        cachedOld === 401 && cachedNew === 202, { cachedOld, cachedNew });
      const assertAt = (now: number) => {
        try {
          assertProducerToken(
            { headers: { "x-plimsoll-token": initial.claudeCodeProducer } } as unknown as http.IncomingMessage,
            rotation.auth,
            "claude_code",
            new URL("http://127.0.0.1/v1/logs"),
            now,
          );
          return "admitted";
        } catch {
          return "refused";
        }
      };
      check("assert_uses_injected_clock",
        assertAt(rotation.expiresAt) === "refused" && assertAt(rotation.expiresAt - 1) === "admitted",
        { atDeadline: assertAt(rotation.expiresAt), beforeDeadline: assertAt(rotation.expiresAt - 1) });
    } finally {
      fs.readFileSync = originalReadFileSync;
      for (const clockServer of clockServers) await new Promise<void>((resolve) => clockServer.close(() => resolve()));
    }
  } finally {
    if (server) await new Promise<void>((resolve) => server!.close(() => resolve()));
    ledger?.close();
    fixture.restore();
    delete process.env[FIXTURE_HOME_ENV];
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
  const failed = checks.filter((entry) => !entry.passed).map((entry) => entry.name);
  console.log(JSON.stringify({ proof: "producer-token-rotation", passed: checks.length - failed.length, failed, checks }, null, 2));
  if (failed.length > 0) process.exitCode = 1;
}

void main().catch((error) => {
  const failed = checks.filter((entry) => !entry.passed).map((entry) => entry.name);
  console.log(JSON.stringify({ proof: "producer-token-rotation", passed: checks.length - failed.length, failed, checks }, null, 2));
  console.error(error);
  process.exitCode = 1;
});
