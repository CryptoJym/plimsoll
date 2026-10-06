import { spawn, execFileSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import readline from "node:readline/promises";
import { createRequire } from "node:module";
import { z } from "zod";
import { readCollectorConfig } from "./config";
import {
  ProjectIntentProducer, type IntentChoice, type IntentLaunch, type IntentSession,
} from "./project-intent-producer";
import { canonicalSourceRoot, intentDigest, IntentError, type IntentSource } from "./project-intent-identity";

export const INTENT_COMMAND_HELP = `
  intent choices [--offline]
  intent declare --source codex|claude_code|grok|gemini_cli --source-root DIR
      (--project KEY | --needs-project) [--basis routed_launch|hand_start|trusted_folder_default|repo_observation]
      [--launch-id UUID] [--observed-repo-key HASH] [--principal OPAQUE_ID]
      [--work-authority NAME --work-namespace ID --work-item ID]
      [--native-session ID --continuation] [--offline] [--queue-only]
  intent bind --launch-id UUID --native-session ID [--continuation]
      [--source-root DIR] [--principal OPAQUE_ID] [--queue-only]
  intent hook --source claude_code|codex [--launch-id UUID]   (bounded native JSON on stdin)
  intent status [--launch-id UUID]
  intent sync
  intent folder-default (--project KEY | --clear) [--offline]
  launch claude|codex [--project KEY | --needs-project | --use-folder-default] [--offline] [-- PROVIDER_ARGS...]
      Persists intent before spawning, preserving cwd. Codex binds via a reviewed hook or intent bind.
      Exit codes: 0 delivered/saved; 2 bad input/refused; 3 pending native/root; 4 queued for replay/review.
      launch preserves the provider's exit code after the child starts.
`;
const sourceSchema = z.enum(["codex", "claude_code", "grok", "gemini_cli"]);
const basisSchema = z.enum(["routed_launch", "hand_start", "trusted_folder_default", "repo_observation"]);
const booleanFlags = new Set(["--offline", "--needs-project", "--queue-only", "--continuation", "--clear", "--use-folder-default"]);
const valueFlags = new Set(["--source", "--source-root", "--project", "--basis", "--launch-id", "--observed-repo-key", "--principal",
  "--work-authority", "--work-namespace", "--work-item", "--native-session"]);
function argsObject(args: string[], allowed: readonly string[]) {
  const parsed: Record<string, string | true> = {};
  for (let i = 0; i < args.length; i++) {
    const name = args[i];
    if (!allowed.includes(name) || name in parsed) throw new IntentError("invalid_intent_arguments");
    if (booleanFlags.has(name)) parsed[name] = true;
    else if (valueFlags.has(name) && args[i + 1] && !args[i + 1].startsWith("--")) parsed[name] = args[++i];
    else throw new IntentError("invalid_intent_arguments");
  }
  return parsed;
}
function value(args: Record<string, string | true>, flag: string): string | undefined {
  return typeof args[flag] === "string" ? args[flag] : undefined;
}
function required(args: Record<string, string | true>, flag: string): string {
  const result = value(args, flag);
  if (!result) throw new IntentError("missing_intent_argument");
  return result;
}
function producer() {
  const config = readCollectorConfig();
  if (config.status !== "valid") throw new IntentError("intent_install_not_registered");
  return new ProjectIntentProducer(config.config);
}
export function sourceStateRoot(source: IntentSource, env: NodeJS.ProcessEnv = process.env): string | undefined {
  if (source === "codex") return env.CODEX_HOME ?? path.join(os.homedir(), ".codex");
  if (source === "claude_code") return env.CLAUDE_CONFIG_DIR ?? path.join(os.homedir(), ".claude");
  // Other routers must supply their actual root. No guessed provider directory.
  return undefined;
}
function output(value: unknown) { process.stdout.write(`${JSON.stringify(value)}\n`); }
function launchView(launch: IntentLaunch) {
  return { launchId: launch.launchId, localState: launch.localState,
    projectKey: launch.receiptDraft.projectKey, basis: launch.receiptDraft.basis,
    projectState: launch.receiptDraft.projectKey ? launch.receiptDraft.basis === "trusted_folder_default" ? "Default (unconfirmed)" : "Chosen project" : "Needs a project",
    active: launch.active, sessions: launch.bindings.map(binding => binding.sessionId) };
}
async function delivery(p: ProjectIntentProducer, sessionId: string, queueOnly: boolean) {
  if (queueOnly) { output({ localState: "queued", sessionId }); return 4; }
  const result = await p.sendSession(sessionId);
  output(result);
  return result.queued || result.reason ? 4 : 0;
}

/** Native hook envelopes are consumed once, whitelisted locally, and never retained/uploaded. */
async function hookInput() {
  let size = 0; const chunks: Buffer[] = [];
  const timer = setTimeout(() => process.stdin.destroy(new IntentError("intent_hook_input_timeout")), 2000);
  try {
    for await (const chunk of process.stdin) {
      const bytes = Buffer.from(chunk); size += bytes.length;
      if (size > 64 * 1024) throw new IntentError("intent_hook_input_too_large");
      chunks.push(bytes);
    }
    const input = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
    if (input.hook_event_name !== "SessionStart" || typeof input.session_id !== "string" ||
      !["startup", "resume", "clear", "compact"].includes(String(input.source))) throw new IntentError("invalid_intent_hook");
    return { nativeId: input.session_id, continuation: input.source === "resume" || input.source === "compact" };
  } finally { clearTimeout(timer); }
}
function hookOwnerFingerprint(pid: number): string {
  try { return execFileSync("ps", ["-o", "lstart=", "-p", String(pid)], { encoding: "utf8", timeout: 500, env: { ...process.env, TZ: "UTC" } }).trim(); }
  catch { return ""; }
}
type HookOwner = { pid: number; fingerprint: string };
function ownedHook(p: ProjectIntentProducer, launchId: string): boolean {
  const owner = p.store.read<HookOwner>("defaults", `owner-${launchId}`);
  if (!owner || !owner.fingerprint || owner.fingerprint !== hookOwnerFingerprint(owner.pid)) return false;
  let pid = process.ppid;
  for (let depth = 0; depth < 12 && pid > 1; depth++) {
    if (pid === owner.pid) return true;
    try { pid = Number(execFileSync("ps", ["-o", "ppid=", "-p", String(pid)], { encoding: "utf8", timeout: 500 }).trim()); }
    catch { return false; }
  }
  return false;
}
function folderKey() {
  const folder = canonicalSourceRoot(process.cwd());
  if (!folder) throw new IntentError("intent_folder_unavailable");
  return intentDigest("folder-default", [folder]).slice(7);
}
type FolderDefault = { scope: ProjectIntentProducer["scope"]; project: IntentChoice | null };
async function selectedChoice(p: ProjectIntentProducer, args: Record<string, string | true>, interactive: boolean) {
  if ([args["--project"], args["--needs-project"], args["--use-folder-default"]].filter(Boolean).length > 1)
    throw new IntentError("conflicting_project_choices");
  if (args["--needs-project"]) return { project: null, basis: "hand_start" as const, cached: false };
  if (!args["--project"] && !args["--use-folder-default"] && (!interactive || !process.stdin.isTTY))
    return { project: null, basis: "hand_start" as const, cached: false };
  const registry = await p.choices(Boolean(args["--offline"]));
  const projectKey = value(args, "--project");
  if (projectKey) {
    const project = registry.projects.find(choice => choice.projectKey === projectKey);
    if (!project) throw new IntentError("project_not_registered");
    return { project, basis: "hand_start" as const, cached: registry.cached };
  }
  const saved = p.store.read<FolderDefault>("defaults", folderKey());
  const folderDefault = saved && JSON.stringify(saved.scope) === JSON.stringify(p.scope)
    ? registry.projects.find(choice => choice.projectKey === saved.project?.projectKey &&
      choice.projectRegistryRevision === saved.project.projectRegistryRevision) ?? null : null;
  if (args["--use-folder-default"]) {
    if (!folderDefault) throw new IntentError("trusted_folder_default_stale_or_missing");
    return { project: folderDefault, basis: "trusted_folder_default" as const, cached: registry.cached };
  }
  if (!interactive || !process.stdin.isTTY) return { project: null, basis: "hand_start" as const, cached: registry.cached };
  const prompt = readline.createInterface({ input: process.stdin, output: process.stderr });
  try {
    registry.projects.forEach((choice, index) => process.stderr.write(`${index + 1}. ${JSON.stringify(choice.projectLabel)}\n`));
    process.stderr.write(`0. Needs a project\n${folderDefault ? `Default (unconfirmed): ${JSON.stringify(folderDefault.projectLabel)}\n` : ""}`);
    const answer = (await prompt.question("Project number (Enter keeps the shown default or Needs a project): ")).trim();
    if (!answer && folderDefault) return { project: folderDefault, basis: "trusted_folder_default" as const, cached: registry.cached };
    if (!answer || answer === "0") return { project: null, basis: "hand_start" as const, cached: registry.cached };
    const project = /^\d+$/.test(answer) ? registry.projects[Number(answer) - 1] : null;
    if (!project) throw new IntentError("invalid_project_choice");
    return { project, basis: "hand_start" as const, cached: registry.cached };
  } finally { prompt.close(); }
}

function shellQuote(argument: string): string { return `'${argument.replace(/'/g, "'\\''")}'`; }
export function ownIntentHookCommand(launchId: string, source: IntentSource) {
  const execArgs = [...process.execArgv];
  for (let i = 0; i < execArgs.length; i++) if (execArgs[i] === "--import" && execArgs[i + 1] === "tsx")
    execArgs[++i] = createRequire(import.meta.url).resolve("tsx");
  return [process.execPath, ...execArgs, path.resolve(process.argv[1]), "intent", "hook", "--source", source, "--launch-id", launchId]
    .map(shellQuote).join(" ");
}
export function launchEnvironment(env: NodeJS.ProcessEnv, launchId: string): NodeJS.ProcessEnv {
  const clean = { ...env };
  for (const name of Object.keys(clean)) if (/^PLIMSOLL_(?:PROJECT|FOLDER_PROJECT|INTENT)/.test(name)) delete clean[name];
  clean.PLIMSOLL_INTENT_LAUNCH_ID = launchId;
  return clean;
}

async function launchProvider(args: string[]): Promise<number> {
  if (process.platform === "win32") throw new IntentError("intent_launch_platform_unsupported");
  const name = args[0];
  if (!["claude", "codex"].includes(name)) throw new IntentError("invalid_launch_provider");
  const separator = args.indexOf("--");
  const parsed = argsObject(args.slice(1, separator === -1 ? undefined : separator),
    ["--project", "--needs-project", "--use-folder-default", "--offline"]);
  const nativeArgs = separator === -1 ? [] : args.slice(separator + 1);
  const p = producer();
  const selection = await selectedChoice(p, parsed, true);
  const source = name === "claude" ? "claude_code" : "codex";
  const launch = p.declare({ source, sourceRoot: sourceStateRoot(source), project: selection.project, basis: selection.basis });
  const owner = { pid: process.pid, fingerprint: hookOwnerFingerprint(process.pid) };
  p.store.mutate<HookOwner, void>("defaults", `owner-${launch.launchId}`, () => ({ state: owner, result: undefined }));
  // Claude merges this additional per-invocation settings object. No files/settings are installed.
  const providerArgs = name === "claude" ? ["--settings", JSON.stringify({ hooks: { SessionStart: [
    { matcher: "startup|resume|clear|compact", hooks: [{ type: "command", command: ownIntentHookCommand(launch.launchId, source), timeout: 10 }] },
  ] } }), ...nativeArgs] : nativeArgs;
  process.stderr.write(`${launchView(launch).projectState}${selection.project ? `: ${JSON.stringify(selection.project.projectLabel)}` : ""}${selection.cached ? " (offline registry copy)" : ""}\n`);
  process.stderr.write(`Plimsoll launch ${launch.launchId}${name === "codex" ? ": native binding needs a reviewed SessionStart hook or intent bind" : ""}\n`);
  let child: ReturnType<typeof spawn> | undefined;
  const relay = (signal: NodeJS.Signals) => { child?.kill(signal); };
  const interrupt = () => relay("SIGINT"), terminate = () => relay("SIGTERM");
  process.on("SIGINT", interrupt); process.on("SIGTERM", terminate);
  try {
    return await new Promise<number>(resolve => {
      child = spawn(name, providerArgs, { cwd: process.cwd(), env: launchEnvironment(process.env, launch.launchId), stdio: "inherit" });
      child.once("error", () => resolve(2));
      child.once("exit", (code, signal) => resolve(code ?? (signal === "SIGINT" ? 130 : 143)));
    });
  } finally {
    process.off("SIGINT", interrupt); process.off("SIGTERM", terminate);
    p.close(launch.launchId);
    // The uploader drains receipts. A launcher/hook never waits on the network.
  }
}

export async function projectIntentCommand(args: string[]): Promise<number> {
  try {
    if (args[0] === "launch") return await launchProvider(args.slice(1));
    const action = args[1];
    if (action === "hook") {
      const parsed = argsObject(args.slice(2), ["--source", "--launch-id"]);
      const source = sourceSchema.parse(required(parsed, "--source"));
      const native = await hookInput();
      const launchId = value(parsed, "--launch-id") ?? process.env.PLIMSOLL_INTENT_LAUNCH_ID;
      if (!launchId || !z.string().uuid().safeParse(launchId).success) { output({ systemMessage: "Needs a project" }); return 0; }
      const p = producer();
      const launch = p.store.read<IntentLaunch>("launches", launchId.toLowerCase());
      if (!launch || !launch.active || launch.receiptDraft.source !== source || !ownedHook(p, launchId)) {
        output({ systemMessage: "Needs a project: launch binding unavailable" }); return 0;
      }
      const receipt = p.bind(launchId, native.nativeId, { continuation: native.continuation, hook: true,
        sourceRoot: sourceStateRoot(source) });
      if (!receipt) output({ systemMessage: "Needs a project: provider state or collector binding unavailable" });
      return 0;
    }
    const p = producer();
    if (action === "choices") {
      const parsed = argsObject(args.slice(2), ["--offline"]);
      output(await p.choices(Boolean(parsed["--offline"]))); return 0;
    }
    if (action === "declare") {
      const parsed = argsObject(args.slice(2), [...valueFlags, "--needs-project", "--offline", "--queue-only", "--continuation"]);
      if (!parsed["--project"] && !parsed["--needs-project"]) throw new IntentError("project_choice_required");
      const source = sourceSchema.parse(required(parsed, "--source"));
      const selection = await selectedChoice(p, parsed, false);
      const workParts = ["--work-authority", "--work-namespace", "--work-item"].filter(flag => parsed[flag]);
      if (workParts.length > 0 && workParts.length !== 3) throw new IntentError("incomplete_work_identity");
      const launch = p.declare({ source, sourceRoot: required(parsed, "--source-root"), project: selection.project,
        basis: basisSchema.parse(value(parsed, "--basis") ?? "hand_start"), observedRepoKey: value(parsed, "--observed-repo-key"),
        principal: value(parsed, "--principal"), launchId: value(parsed, "--launch-id"),
        work: workParts.length ? { authority: required(parsed, "--work-authority"), namespace: required(parsed, "--work-namespace"), id: required(parsed, "--work-item") } : undefined,
      });
      output(launchView(launch));
      const native = value(parsed, "--native-session");
      if (!native) return 3;
      const receipt = p.bind(launch.launchId, native, { continuation: Boolean(parsed["--continuation"]) });
      return receipt ? await delivery(p, receipt.sessionId, Boolean(parsed["--queue-only"])) : 3;
    }
    if (action === "bind") {
      const parsed = argsObject(args.slice(2), ["--launch-id", "--native-session", "--continuation", "--source-root", "--principal", "--queue-only"]);
      const receipt = p.bind(required(parsed, "--launch-id"), required(parsed, "--native-session"), {
        continuation: Boolean(parsed["--continuation"]), sourceRoot: value(parsed, "--source-root"), principal: value(parsed, "--principal"),
      });
      if (!receipt) { output({ localState: "awaiting_native_binding", reason: "native_binding_unavailable" }); return 3; }
      return delivery(p, receipt.sessionId, Boolean(parsed["--queue-only"]));
    }
    if (action === "sync") {
      argsObject(args.slice(2), []);
      const result = await p.replay(); output(result); return result.queued || result.reasons.length || result.queueCoverage === "partial" ? 4 : 0;
    }
    if (action === "status") {
      const parsed = argsObject(args.slice(2), ["--launch-id"]);
      const launchIds = value(parsed, "--launch-id") ? [z.string().uuid().parse(parsed["--launch-id"]).toLowerCase()] : p.store.ids("launches");
      const sessionIds = p.store.ids("sessions");
      const ids = launchIds.slice(-128);
      output({ launches: ids.map(id => p.store.read<IntentLaunch>("launches", id)).filter((launch): launch is IntentLaunch => launch !== null).map(launchView),
        sessions: sessionIds.slice(-128).map(id => {
          const session = p.store.read<IntentSession>("sessions", id)!;
          return { sessionId: id, observedRevision: session.observedRevision,
            queued: session.receipts.filter(row => !row.delivered).length,
            review: session.receipts.filter(row => row.review).map(row => ({ receiptId: row.receipt.receiptId, reason: row.review })) };
        }), localOnly: true, limit: 128,
        inventoryCoverage: launchIds.length > 128 || sessionIds.length > 128 ? "partial" : "complete",
        unexaminedLaunches: Math.max(0, launchIds.length - 128), unexaminedSessions: Math.max(0, sessionIds.length - 128),
      }); return 0;
    }
    if (action === "folder-default") {
      const parsed = argsObject(args.slice(2), ["--project", "--clear", "--offline"]);
      if (Boolean(parsed["--project"]) === Boolean(parsed["--clear"])) throw new IntentError("project_choice_required");
      const selection = parsed["--clear"] ? { project: null } : await selectedChoice(p, parsed, false);
      p.store.mutate<FolderDefault, void>("defaults", folderKey(), () => ({ state: { scope: p.scope, project: selection.project }, result: undefined }));
      output({ projectState: selection.project ? "Default (unconfirmed)" : "Needs a project", projectKey: selection.project?.projectKey ?? null }); return 0;
    }
    throw new IntentError("invalid_intent_command");
  } catch (error) {
    // Neither native envelopes, argv values, credential values nor parser messages are diagnostics.
    const code = error instanceof IntentError ? error.code : "intent_command_failed";
    output({ error: code }); return 2;
  }
}
