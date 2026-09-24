import fs from "node:fs";
import path from "node:path";

import { parse } from "yaml";

import {
  INERT_EXPRESSION,
  INERT_EXPRESSIONS,
  RUN_PROOF_WRAPPER,
  isRepoFile,
  mentionedWords,
  normalizeFile,
  packageScriptForm,
  runnerInvocation,
  workflowLineForm,
  type Problem,
} from "./canonical-commands";
import { modelWorkflow, type WorkflowStep } from "./ci-workflow-model";
import { PROOF_SUITES, readProofSuites } from "./proof-suites";

/**
 * Which proofs CI actually runs (eco-6hoxj.163.23).
 *
 * A proof that CI never runs rots silently: proof:enrollment-privacy failed
 * from 2026-09-07 until eco-6hoxj.163.22 while the published privacy spec
 * cited it. The unit of coverage is a proof entry file, found three ways:
 * the file each `proof`/`proof:*` package script runs, every proof-named file
 * any package script names, and every proof-named file under scripts/. Every
 * proof script must be canonical (scripts/lib/canonical-commands.ts): exactly
 * one runner invocation of one file, or a pure alias of another script. A
 * unit is covered only when a workflow step that provably runs on every
 * successful push/PR to main executes a canonical line that reaches it (by
 * entry file, or through canonical package scripts), or when
 * scripts/proof-suites.json declares it as a sub-proof of a suite CI runs.
 * Every line of a step that names a proof must be canonical, and no step up
 * to the job's last proof step may interpolate `${{ }}` other than the head
 * SHA. A proof job may not set an execution-changing environment variable
 * (env maps, assignments, exports, $GITHUB_ENV or $GITHUB_PATH), run an
 * unknown action before its proofs or run in a container, and the repository
 * may not configure how pnpm runs scripts (script-shell, node-options, a
 * pnpmfile). A counted proof may not receive a variable it reads from a
 * workflow assignment or workflow/job/step `env:`; the explicit allow-list is
 * `PLIMSOLL_PROOF_HOME` and `REJECTION_PROOF_SCALE`. Runtime pnpm/npm config
 * writes are refused through the last proof. Every other unit needs a reviewed entry in
 * scripts/proof-local-only.json.
 */

export const WORKFLOW_DIRECTORY = ".github/workflows";
export const PROOF_EXCEPTIONS = "scripts/proof-local-only.json";
export const GATE_ENTRY = "scripts/ci-coverage-proof.ts";
const PROOF_SCRIPT = /^proof(?::|$)/;

/**
 * Environment variables that change how bash, Node or pnpm run a proof
 * rather than what it reads (review 2): BASH_ENV, NODE_OPTIONS or
 * npm_config_script_shell each turned a proof into a successful no-op.
 */
const EXECUTION_ENV = ["BASH_ENV", "ENV", "SHELLOPTS", "BASHOPTS", "PATH", "LD_PRELOAD", "DYLD_INSERT_LIBRARIES"];
const EXECUTION_WORD = new RegExp(`(?<![A-Za-z0-9_])(?:${EXECUTION_ENV.join("|")}|BASH_FUNC_\\w*)(?![A-Za-z0-9_])`);
const CONFIG_WORD = /(?<![A-Za-z0-9_])p?npm_config_\w*/i;
const BENIGN_NODE_OPTIONS = /^--max-old-space-size=[1-9][0-9]*$/;
/** Proof-specific settings that are intentionally permitted on a counted line. */
export const PROOF_ENV_ALLOWLIST = new Set(["PLIMSOLL_PROOF_HOME", "REJECTION_PROOF_SCALE"]);
const WORKFLOW_ENV_ALLOWLIST = new Set([
  ...PROOF_ENV_ALLOWLIST, "HOME", "USERPROFILE", "PLIMSOLL_HOME", "CODEX_HOME", "CLAUDE_CONFIG_DIR",
  "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME", "TMPDIR",
]);
const allowedWorkflowEnvironment = (name: string, value?: unknown) =>
  WORKFLOW_ENV_ALLOWLIST.has(name) || (name === "NODE_OPTIONS" && typeof value === "string" && BENIGN_NODE_OPTIONS.test(value.trim()));
export const changesExecution = (name: string, value?: unknown) =>
  !(name === "NODE_OPTIONS" && typeof value === "string" && BENIGN_NODE_OPTIONS.test(value.trim())) &&
  (name === "NODE_OPTIONS" || EXECUTION_WORD.test(name) || CONFIG_WORD.test(name));

function readsProcessEnvironment(source: string, name: string) {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp("process\\.env(?:\\." + escaped + "(?![A-Za-z0-9_])|\\[\\s*[\"'`]" + escaped + "[\"'`]\\s*\\])").test(source);
}

function proofOwnedEnvironmentProblems(source: string, names: string[], where: string, values?: Record<string, unknown> | null) {
  return names
    .filter((name) => !PROOF_ENV_ALLOWLIST.has(name) &&
      !(name === "NODE_OPTIONS" && !changesExecution(name, values?.[name])) &&
      readsProcessEnvironment(source, name))
    .map((name) => `${where} sets ${name}, which is read by the counted proof; proof-owned CI settings are not allowed (allow-list: ${[...PROOF_ENV_ALLOWLIST].join(", ")})`);
}

/**
 * A quarantine (a known-red proof) lasts at most this many days from today,
 * then the gate fails until the proof is repaired or the entry is renewed.
 */
export const MAX_QUARANTINE_DAYS = 30;
const DAY_MS = 86_400_000;
/** Milliseconds of a real calendar date written YYYY-MM-DD, or null. */
function calendarDay(text: string): number | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  const time = Date.parse(`${text}T00:00:00Z`);
  return Number.isNaN(time) || new Date(time).toISOString().slice(0, 10) !== text ? null : time;
}
/**
 * Variables CI or the proof harness always sets, so a proof that needs only
 * these can run in CI and is not local-only.
 */
const PROVIDED_IN_CI =
  /^(?:CI|HOME|USERPROFILE|PATH|TMPDIR|TEMP|TMP|USER|SHELL|LANG|TZ|PWD|GITHUB_\w+|RUNNER_\w+|ACTIONS_\w+|XDG_\w+|PLIMSOLL_PROOF_\w+|PLIMSOLL_FIXTURE_ROOT|PLIMSOLL_HOME|CODEX_HOME|CLAUDE_CONFIG_DIR|GROK_HOME)$/;

/** Actions a proof job may run before its last proof: checkout, pnpm and Node setup, evidence upload. */
const KNOWN_ACTIONS = ["actions/checkout", "actions/setup-node", "pnpm/action-setup", "actions/upload-artifact", "actions/cache"];
const actionName = (uses: string) => uses.split("@")[0]!.split("/").slice(0, 2).join("/");

/**
 * pnpm settings that change how `pnpm <script>` runs. Verified with pnpm
 * 10.25.0: script-shell and node-options (in .npmrc or pnpm-workspace.yaml)
 * and a .pnpmfile.cjs that exits each made every script a successful no-op.
 */
const PNPM_EXECUTION_SETTINGS = ["script-shell", "node-options", "shell-emulator", "pnpmfile", "global-pnpmfile"];
const settingKey = (key: string) => key.toLowerCase().replace(/[^a-z0-9]/g, "");
/** pnpm's own commands: `pnpm <name>` runs these, not a package script of the same name. */
const PNPM_BUILTINS = new Set([
  "add", "approve-builds", "audit", "bin", "c", "cat-file", "cat-index", "config", "create", "dedupe", "deploy",
  "dlx", "doctor", "env", "exec", "fetch", "find-hash", "help", "i", "ignored-builds", "import", "init", "install",
  "install-test", "it", "licenses", "link", "list", "ln", "ls", "m", "multi", "outdated", "pack", "patch",
  "patch-commit", "patch-remove", "prune", "publish", "rb", "rebuild", "recursive", "remove", "restart", "rm", "root",
  "run", "self-update", "server", "setup", "store", "un", "uni", "uninstall", "unlink", "up", "update", "upgrade", "why",
]);

export type CoverageInput = {
  scripts: Record<string, string>;
  workflows: Array<{ path: string; text: string }>;
  exceptions: unknown;
  /** scripts/proof-suites.json: suite file -> the sub-proof files it runs. */
  suites: unknown;
  /** Proof entry files present under scripts/ (repo-relative). */
  proofFiles: string[];
  /** Repository file text, or null when absent. */
  readFile: (file: string) => string | null;
  /** UTC date (YYYY-MM-DD) that expires quarantines. */
  today: string;
};

export type Invocation = {
  workflow: string;
  job: string;
  step: string;
  stepIndex: number;
  /** Position of the command among its step's lines. */
  position: number;
  line: number;
  command: string;
  via: string[];
};

export type UnitStatus = "ci" | "runs-inside" | "local-only" | "quarantined" | "uncovered";

export type UnitCoverage = {
  unit: string;
  scripts: string[];
  sources: string[];
  covered: Invocation[];
  ignored: Array<Invocation & { reason: string }>;
  status: UnitStatus;
  exception: Record<string, unknown> | null;
};

export type CoverageReport = {
  errors: string[];
  workflows: string[];
  units: UnitCoverage[];
  uncovered: string[];
};

/** Same rule the managed-config fixture guard uses for proof entries (fixture-root.ts). */
export function isProofEntry(file: string) {
  const base = path.posix.basename(file);
  if (/(?:-proof|\.test)\.[cm]?[jt]s$/.test(base)) return true;
  return /^index\.[cm]?[jt]s$/.test(base) && path.posix.basename(path.posix.dirname(file)).endsWith("-proof");
}

type Resolved = { file: string; via: string[]; args: string[] } | Problem;

/**
 * Follow a package script through pure aliases to the one file it runs, or
 * say why it is not canonical: a non-canonical body, an alias cycle, or a
 * pre/post script pnpm would also run.
 */
function scriptResolver(scripts: Record<string, string>) {
  const cache = new Map<string, Resolved>();
  const resolve = (name: string, chain: string[] = []): Resolved => {
    if (chain.includes(name)) return { problem: `alias cycle ${[...chain, name].join(" → ")}` };
    const cached = cache.get(name);
    if (cached) return cached;
    const hooks = [`pre${name}`, `post${name}`].filter((hook) => Object.hasOwn(scripts, hook));
    let result: Resolved;
    if (!Object.hasOwn(scripts, name)) result = { problem: `package.json has no script \`${name}\`` };
    else if (hooks.length > 0) result = { problem: `pnpm also runs ${hooks.join(" and ")} around \`${name}\`` };
    else {
      const form = packageScriptForm(scripts[name]!);
      if ("problem" in form) result = { problem: `\`${name}\`: ${form.problem}` };
      else if ("alias" in form) {
        const inner = resolve(form.alias, [...chain, name]);
        result = "problem" in inner ? inner : { file: inner.file, via: [name, ...inner.via], args: inner.args };
      } else result = { file: form.file, via: [name], args: form.args };
    }
    cache.set(name, result);
    return result;
  };
  return resolve;
}

/** True when a script names a proof file, directly or through scripts it names (over-approximate). */
function mentionsProof(scripts: Record<string, string>, units: Map<string, unknown>, name: string, seen = new Set<string>()): boolean {
  if (seen.has(name) || !Object.hasOwn(scripts, name)) return false;
  seen.add(name);
  return mentionedWords(scripts[name]!).some(
    (word) => units.has(normalizeFile(word)) || PROOF_SCRIPT.test(word) || mentionsProof(scripts, units, word, seen),
  );
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

/** Repository pnpm configuration that could change how every proof script runs. */
function packageManagerProblems(input: CoverageInput): string[] {
  const problems: string[] = [];
  const denied = new Set(PNPM_EXECUTION_SETTINGS.map(settingKey));
  for (const line of (input.readFile(".npmrc") ?? "").split("\n")) {
    const key = /^\s*([^#;=\s][^=]*?)\s*=/.exec(line)?.[1];
    if (key && denied.has(settingKey(key))) problems.push(`.npmrc sets ${key}, which changes how pnpm runs every proof script`);
  }
  const workspace = input.readFile("pnpm-workspace.yaml");
  if (workspace !== null) {
    try {
      const settings = parse(workspace, { merge: true }) as unknown;
      for (const key of isRecord(settings) ? Object.keys(settings) : []) {
        if (denied.has(settingKey(key))) problems.push(`pnpm-workspace.yaml sets ${key}, which changes how pnpm runs every proof script`);
      }
    } catch (error) {
      problems.push(`pnpm-workspace.yaml: ${error instanceof Error ? error.message.split("\n")[0] : String(error)}`);
    }
  }
  const manifest = JSON.parse(input.readFile("package.json") ?? "{}") as { pnpm?: unknown };
  for (const key of isRecord(manifest.pnpm) ? Object.keys(manifest.pnpm) : []) {
    if (denied.has(settingKey(key))) problems.push(`package.json pnpm.${key} changes how pnpm runs every proof script`);
  }
  for (const file of [".pnpmfile.cjs", ".pnpmfile.mjs"]) {
    if (input.readFile(file) !== null) problems.push(`${file} exists: pnpm loads it for every \`pnpm run\`, so it can end every proof script before it starts`);
  }
  return problems;
}

/** Runtime package-manager configuration writes can turn a later proof into a no-op. */
function runtimePackageManagerWrite(line: string): string | null {
  const text = line.trim();
  if (text === "" || text.startsWith("#")) return null;
  // The command can carry flags before or between `config`/`c` and `set`.
  // Refuse any package-manager `set` rather than trying to model its flags.
  const commands = text.split(/[;&|]/);
  if (commands.some((command) => /\b(?:pnpm|npm|yarn)\b[^;&|]*\bset\b/i.test(command))) {
    return "runs a package-manager config/set command before the last proof";
  }
  const rcFile = String.raw`(?:\.(?:npm|pnpm|yarn)rc(?:\.yml)?|[/\\](?:pnpm|yarn)[/\\]rc)(?![A-Za-z0-9_])`;
  if (new RegExp(String.raw`(?:>>?|\b(?:cp|mv|tee|install)\b)[^;&|\n]*${rcFile}`, "i").test(text)) {
    return "writes a package-manager rc file before the last proof";
  }
  return null;
}

/** Only literal, reviewed assignments may cross GitHub's step environment boundary. */
function githubEnvironmentWriteProblem(runText: string): string | null {
  if (!runText.includes("GITHUB_ENV")) return null;
  const group = /\{\s*\n([\s\S]*?)\}\s*>>\s*"?\$GITHUB_ENV"?/g;
  let remaining = runText.replace(group, (_whole, body: string) => {
    const assignments = body.split("\n").map((line) => line.trim()).filter(Boolean);
    if (assignments.some((line) => !/^echo\s+["']?[A-Z][A-Z0-9_]*=/.test(line))) return "INVALID_GITHUB_ENV_WRITE";
    return assignments.join("\n");
  });
  if (remaining.includes("INVALID_GITHUB_ENV_WRITE")) return "writes a nonliteral value to $GITHUB_ENV";
  for (const line of remaining.split("\n").filter((candidate) => candidate.includes("GITHUB_ENV"))) {
    if (!/^\s*echo\s+["']?([A-Z][A-Z0-9_]*)=/.test(line) || !/>>\s*"?\$GITHUB_ENV"?\s*$/.test(line)) {
      return "writes a nonliteral value to $GITHUB_ENV";
    }
  }
  const names = [...remaining.matchAll(/\becho\s+["']?([A-Z][A-Z0-9_]*)=([^"'\n]*)/g)];
  const denied = names.find((match) => !allowedWorkflowEnvironment(match[1]!, match[2]));
  return denied ? `writes ${denied[1]} to $GITHUB_ENV outside the environment allow-list` : null;
}

export function proofCiCoverage(input: CoverageInput): CoverageReport {
  const errors: string[] = [];
  const resolve = scriptResolver(input.scripts);

  // 1. Inventory.
  const units = new Map<string, UnitCoverage>();
  const addUnit = (unit: string, source: string, script?: string) => {
    const entry = units.get(unit) ?? { unit, scripts: [], sources: [], covered: [], ignored: [], status: "uncovered" as UnitStatus, exception: null };
    if (!entry.sources.includes(source)) entry.sources.push(source);
    if (script && !entry.scripts.includes(script)) entry.scripts.push(script);
    units.set(unit, entry);
  };
  for (const name of Object.keys(input.scripts).sort()) {
    if (PROOF_SCRIPT.test(name)) {
      const resolved = resolve(name);
      if ("problem" in resolved) errors.push(`package.json "${name}" is not a canonical proof command: ${resolved.problem}`);
      else {
        if (resolved.file === GATE_ENTRY && (resolved.args.length > 0 || resolved.via.some((script) => input.scripts[script]?.includes("--direct-node")))) {
          errors.push(`package.json "${name}" passes arguments to ${GATE_ENTRY}; the CI gate script must use its exact invocation so audit dates and roots cannot be pinned`);
        }
        addUnit(resolved.file, "proof script leaf", name);
      }
    }
    for (const word of mentionedWords(input.scripts[name]!)) {
      const file = normalizeFile(word);
      if (isRepoFile(file) && isProofEntry(file) && file !== RUN_PROOF_WRAPPER) addUnit(file, "proof file named by a package script", name);
    }
  }
  // The run-proof wrapper matches the proof file-name rule but is not a proof.
  for (const file of input.proofFiles) if (file !== RUN_PROOF_WRAPPER) addUnit(file, "proof file on disk");

  // 2. What CI runs.
  if (input.workflows.length === 0) errors.push(`no workflow files under ${WORKFLOW_DIRECTORY}: nothing runs any proof`);
  const models = input.workflows.map((workflow) => modelWorkflow(workflow.path, workflow.text));
  for (const model of models) errors.push(...model.errors);
  errors.push(...packageManagerProblems(input));
  const namesProof = (text: string) =>
    text
      .split("\n")
      .filter((line) => !line.trim().startsWith("#"))
      .some((line) =>
        mentionedWords(line).some(
          (word) => units.has(normalizeFile(word)) || PROOF_SCRIPT.test(word) || mentionsProof(input.scripts, units, word),
        ),
      );
  for (const model of models) {
    const jobs = new Map<string, WorkflowStep[]>();
    for (const step of model.steps) jobs.set(step.job, [...(jobs.get(step.job) ?? []), step]);
    for (const [job, steps] of jobs) {
      const proofSteps = steps.filter((step) => step.run !== null && namesProof(step.run));
      if (proofSteps.length === 0) continue;
      // GitHub pastes `${{ }}` into a run script before bash reads it, so a
      // value can add `|| true`, `exit 0` or a new line. From the first such
      // step, no proof line later in the job can be trusted.
      const lastProof = proofSteps.at(-1)!.stepIndex;
      const inertExpressions = [INERT_EXPRESSION, ...Object.keys(INERT_EXPRESSIONS).filter((expression) => expression !== INERT_EXPRESSION)];
      const withoutInertExpressions = (text: string) => inertExpressions.reduce((value, expression) => value.split(expression).join(""), text);
      const tainted = steps.find(
        (step) => step.stepIndex <= lastProof && step.run !== null && withoutInertExpressions(step.run).includes("${{"),
      );
      if (tainted) {
        errors.push(
          `${model.path} job "${job}" step "${tainted.name}" pastes a GitHub expression into its script (only the head-SHA expression is allowed), so no proof at or after it counts`,
        );
      }
      // Nothing a proof sees may change how bash, Node or pnpm run it.
      const checkEnv = (names: string[] | null, values: Record<string, unknown> | null, where: string) => {
        if (names === null) errors.push(`${where} env: is not a literal mapping, so it could set anything`);
        for (const name of names ?? []) {
          if (!allowedWorkflowEnvironment(name, values?.[name])) errors.push(`${where} env: sets ${name} outside the environment allow-list`);
          else if (changesExecution(name, values?.[name])) errors.push(`${where} env: sets ${name}, which changes how the proofs run`);
        }
      };
      checkEnv(model.env, model.envValues, model.path);
      checkEnv(model.jobs[job]?.env ?? null, model.jobs[job]?.envValues ?? null, `${model.path} job "${job}"`);
      if (model.jobs[job]?.container) errors.push(`${model.path} job "${job}" runs in a container, which the gate does not model`);
      for (const step of steps.filter((candidate) => candidate.stepIndex <= lastProof)) {
        const where = `${model.path} step "${step.name}"`;
        const runText = (step.run ?? "").replace(/\\\r?\n\s*/g, " ");
        const activeRun = runText.split("\n").filter((line) => !line.trim().startsWith("#")).join("\n");
        checkEnv(step.env, step.envValues, where);
        if (step.uses !== null && !KNOWN_ACTIONS.includes(actionName(step.uses))) {
          errors.push(`${where} runs action ${step.uses} before the job's last proof; only ${KNOWN_ACTIONS.join(", ")} may`);
        }
        if (step.uses !== null && actionName(step.uses) === "actions/cache") {
          const cachePath = step.withValues?.path;
          const unsafe = typeof cachePath !== "string" || cachePath.split(/\s+/).some((entry) =>
            entry.startsWith("~") || entry.includes("$") ||
            /(?:^|[/\\])(?:node_modules|\.(?:npm|pnpm|yarn)rc(?:\.yml)?|\.pnpmfile(?:\.[cm]?js)?)(?:[/\\]|$)/i.test(entry));
          if (unsafe) errors.push(`${where} caches a proof-controlling path (HOME dotfile, rc file, node_modules or pnpmfile)`);
        }
        const word = activeRun.match(EXECUTION_WORD)?.[0] ?? activeRun.match(CONFIG_WORD)?.[0];
        if (word) errors.push(`${where} names ${word} in its script; setting it (as a prefix, with export or through $GITHUB_ENV) changes how the proofs run`);
        if (activeRun.includes("GITHUB_PATH")) errors.push(`${where} writes $GITHUB_PATH, which changes which programs the proofs run`);
        const githubEnvironmentProblem = githubEnvironmentWriteProblem(activeRun);
        if (githubEnvironmentProblem) errors.push(`${where} ${githubEnvironmentProblem}`);
        for (const match of activeRun.matchAll(/\b(?:export\s+)?([A-Z][A-Z0-9_]*)=([^\s"']*)/g)) {
          if (!allowedWorkflowEnvironment(match[1]!, match[2])) errors.push(`${where} sets ${match[1]} outside the environment allow-list`);
        }
        for (const line of activeRun.split("\n")) {
          const packageManagerProblem = runtimePackageManagerWrite(line);
          if (packageManagerProblem) errors.push(`${where} ${packageManagerProblem}: ${line.trim()}`);
        }
      }
      for (const step of proofSteps) {
        const where = `${model.path} step "${step.name}"`;
        const benignLine = (text: string) =>
          text === "set -euo pipefail" ||
          /^echo \"::(?:group::[A-Za-z0-9_.:/ -]+|endgroup::)\"$/.test(text);
        const lines = step
          .run!.split("\n")
          .map((text, index) => ({ number: index + 1, text: text.trim() }))
          .filter((line) => line.text !== "" && !line.text.startsWith("#"))
          .map((line) => ({ ...line, form: benignLine(line.text) ? ({ kind: "benign" } as const) : workflowLineForm(line.text) }));
        const bad = lines.find((line) => "problem" in line.form);
        if (bad) {
          errors.push(`${where} line ${bad.number} is not canonical (${(bad.form as Problem).problem}): ${bad.text}`);
          continue;
        }
        const reason =
          step.notCovering ?? (tainted && tainted.stepIndex <= step.stepIndex ? `step "${tainted.name}" pastes a GitHub expression into its script` : null);
        for (const [position, line] of lines.entries()) {
          if (!("kind" in line.form) || line.form.kind !== "command") continue;
          const words = line.form.words;
          let reached: { file: string; via: string[]; args: string[] } | null = null;
          let problem = "it names a proof but runs it in a form the gate does not count";
          let workflowExtraArgs: string[] = [];
          if (words[0] === "pnpm") {
            const runForm = words[1] === "run";
            const scriptIndex = runForm ? 2 : 1;
            const script = words[scriptIndex] ?? "";
            workflowExtraArgs = words.slice(scriptIndex + 1);
            if (!script.startsWith("-") && !PNPM_BUILTINS.has(script) && Object.hasOwn(input.scripts, script)) {
              const resolved = resolve(script);
              if ("problem" in resolved) problem = `\`pnpm ${script}\` is not a canonical command: ${resolved.problem}`;
              else reached = resolved;
            } else if (PROOF_SCRIPT.test(script)) {
              problem = `\`pnpm ${script}\`: package.json does not define it`;
            }
          } else {
            const invocation = runnerInvocation(words);
            if ("problem" in invocation) problem = invocation.problem;
            else reached = { file: invocation.file, via: [], args: invocation.args };
          }
          if (!reached) {
            // A non-canonical proof script is reported once, in the inventory.
            const reportedScript = words[1] === "run" ? words[2] ?? "" : words[1] ?? "";
            const reported = words[0] === "pnpm" && PROOF_SCRIPT.test(reportedScript) && Object.hasOwn(input.scripts, reportedScript);
            if (namesProof(line.text) && !reported) errors.push(`${where} line ${line.number}: ${problem}: ${line.text}`);
            continue;
          }
          if (reached.file === GATE_ENTRY && (reached.args.length > 0 || workflowExtraArgs.length > 0 || line.text.includes("--direct-node") || reached.via.some((script) => input.scripts[script]?.includes("--direct-node")))) {
            errors.push(`${where} line ${line.number} passes arguments to ${GATE_ENTRY}; the CI gate line must use its exact invocation so audit dates and roots cannot be pinned`);
          }
          const source = input.readFile(reached.file) ?? "";
          errors.push(...proofOwnedEnvironmentProblems(source, line.form.assignments, `${where} line ${line.number}`));
          errors.push(...proofOwnedEnvironmentProblems(source, model.env ?? [], `${model.path} env`, model.envValues));
          errors.push(...proofOwnedEnvironmentProblems(source, model.jobs[job]?.env ?? [], `${model.path} job "${job}" env`, model.jobs[job]?.envValues));
          errors.push(...proofOwnedEnvironmentProblems(source, step.env ?? [], `${where} env`, step.envValues));
          const unit = units.get(reached.file);
          if (!unit) continue;
          const invocation = { workflow: model.path, job, step: step.name, stepIndex: step.stepIndex, position, line: step.line, command: line.text, via: reached.via };
          if (reason) unit.ignored.push({ ...invocation, reason });
          else unit.covered.push(invocation);
        }
      }
    }
  }

  for (const unit of units.values()) {
    if (unit.covered.length > 0) unit.status = "ci";
  }

  // 3. Suites: a sub-proof runs in CI only as a declared entry of a suite CI runs.
  const suites = isRecord(input.suites) ? input.suites : {};
  if (!isRecord(input.suites)) errors.push(`${PROOF_SUITES}: not a mapping of suite files to sub-proof files`);
  const suiteOf = new Map<string, string>();
  for (const [suite, declared] of Object.entries(suites)) {
    const where = `${PROOF_SUITES} \`${suite}\``;
    if (!units.has(suite)) {
      errors.push(`${where}: not a proof file in the inventory`);
      continue;
    }
    if (!Array.isArray(declared) || declared.length === 0 || !declared.every((file) => typeof file === "string")) {
      errors.push(`${where}: must list at least one sub-proof file`);
      continue;
    }
    for (const child of declared as string[]) {
      if (!units.has(child)) errors.push(`${where}: sub-proof ${child} is not a proof file on disk`);
      else if (Object.hasOwn(suites, child)) errors.push(`${where}: sub-proof ${child} is itself a suite`);
      else if (suiteOf.has(child)) errors.push(`${where}: sub-proof ${child} is also declared by ${suiteOf.get(child)}`);
      else suiteOf.set(child, suite);
    }
    if (units.get(suite)!.status !== "ci") errors.push(`${where}: the suite is not run by CI, so none of its sub-proofs are`);
  }

  // 4. Reviewed exceptions.
  const exceptions = isRecord(input.exceptions) ? input.exceptions : {};
  const sections = ["localOnly", "quarantined"] as const;
  for (const key of Object.keys(exceptions)) {
    if (!(sections as readonly string[]).includes(key)) {
      const hint = key === "runsInside" ? ` (sub-proofs are declared in ${PROOF_SUITES})` : "";
      errors.push(`${PROOF_EXCEPTIONS}: unknown section \`${key}\`${hint}`);
    }
  }
  const claimed = new Map<string, string>();
  for (const section of sections) {
    const entries = exceptions[section] ?? {};
    if (!isRecord(entries)) {
      errors.push(`${PROOF_EXCEPTIONS}: \`${section}\` is not a mapping`);
      continue;
    }
    for (const [unitId, raw] of Object.entries(entries)) {
      const where = `${PROOF_EXCEPTIONS} ${section} \`${unitId}\``;
      const unit = units.get(unitId);
      if (!unit) {
        errors.push(`${where}: no such proof in the inventory (stale entry)`);
        continue;
      }
      if (claimed.has(unitId)) {
        errors.push(`${where}: also listed under ${claimed.get(unitId)}`);
        continue;
      }
      claimed.set(unitId, section);
      if (unit.status === "ci") {
        errors.push(`${where}: CI already runs it (remove the entry)`);
        continue;
      }
      if (suiteOf.has(unitId)) {
        errors.push(`${where}: ${PROOF_SUITES} declares it as a sub-proof of ${suiteOf.get(unitId)} (remove one)`);
        continue;
      }
      const fields = section === "localOnly" ? ["owner", "needs", "reviewedOn", "expires", "reason"] : ["owner", "expires", "reason"];
      if (!isRecord(raw)) {
        errors.push(`${where}: entry must be a mapping of ${fields.join(", ")}`);
        continue;
      }
      const entry = raw;
      const unknownFields = Object.keys(entry).filter((field) => !fields.includes(field));
      const missing = fields.filter((field) =>
        field === "needs" ? !Array.isArray(entry.needs) || entry.needs.length === 0 : typeof entry[field] !== "string" || !entry[field].trim(),
      );
      if (unknownFields.length > 0 || missing.length > 0) {
        const parts = [missing.length > 0 ? `missing ${missing.join(", ")}` : "", unknownFields.length > 0 ? `unknown ${unknownFields.join(", ")}` : ""];
        const hint = section === "localOnly" && missing.includes("needs")
          ? " (local-only is for a proof that needs an input CI lacks; a proof that simply fails belongs under quarantined, with an expiry)"
          : "";
        errors.push(`${where}: ${parts.filter(Boolean).join("; ")}${hint}`);
        continue;
      }
      if (section === "quarantined") {
        const expires = calendarDay(entry.expires as string);
        const today = calendarDay(input.today);
        if (expires === null || today === null) {
          errors.push(`${where}: expires (${String(entry.expires)}) and today (${input.today}) must be real calendar dates, YYYY-MM-DD`);
          continue;
        }
        if (today > expires) {
          errors.push(`${where}: quarantine expired on ${entry.expires} (owner ${entry.owner}); repair and wire it, or renew the entry in review`);
          continue;
        }
        if (expires - today > MAX_QUARANTINE_DAYS * DAY_MS) {
          const latest = new Date(today + MAX_QUARANTINE_DAYS * DAY_MS).toISOString().slice(0, 10);
          errors.push(`${where}: expires ${entry.expires}, more than ${MAX_QUARANTINE_DAYS} days away (latest allowed today: ${latest}); renew it in review instead`);
          continue;
        }
      } else {
        const reviewedOn = calendarDay(entry.reviewedOn as string);
        const expires = calendarDay(entry.expires as string);
        const today = calendarDay(input.today);
        if (reviewedOn === null || expires === null || today === null) {
          errors.push(`${where}: reviewedOn (${String(entry.reviewedOn)}), expires (${String(entry.expires)}) and today (${input.today}) must be real calendar dates, YYYY-MM-DD`);
          continue;
        }
        if (reviewedOn > today) {
          errors.push(`${where}: reviewedOn ${entry.reviewedOn} is in the future (review the local-only declaration before using it)`);
          continue;
        }
        if (expires < reviewedOn) {
          errors.push(`${where}: expires ${entry.expires} is before reviewedOn ${entry.reviewedOn}`);
          continue;
        }
        if (today > expires) {
          errors.push(`${where}: local-only review expired on ${entry.expires} (owner ${entry.owner}); renew the reviewed declaration or wire the proof into CI`);
          continue;
        }
        if (expires - today > MAX_QUARANTINE_DAYS * DAY_MS) {
          const latest = new Date(today + MAX_QUARANTINE_DAYS * DAY_MS).toISOString().slice(0, 10);
          errors.push(`${where}: expires ${entry.expires}, more than ${MAX_QUARANTINE_DAYS} days away (latest allowed today: ${latest}); renew it in review instead`);
          continue;
        }
        // A local-only proof needs an input CI does not have. The gate checks
        // that the proof reads each named variable and that neither CI nor
        // any workflow provides it; a proof that simply fails cannot say that.
        const source = input.readFile(unitId) ?? "";
        const needProblems = (entry.needs as unknown[]).flatMap((need): string[] => {
          if (typeof need !== "string" || !/^[A-Z][A-Z0-9_]*$/.test(need)) return [`need ${JSON.stringify(need)} is not an environment variable name`];
          if (PROVIDED_IN_CI.test(need) || changesExecution(need)) return [`CI provides ${need}`];
          const word = new RegExp(`(?<![A-Za-z0-9_])${need}(?![A-Za-z0-9_])`);
          const workflow = input.workflows.find((candidate) => word.test(candidate.text));
          if (workflow) return [`${workflow.path} mentions ${need}, so CI may provide it`];
          const read = new RegExp(`process\\.env(?:\\.${need}(?![A-Za-z0-9_])|\\[\\s*["'\`]${need}["'\`]\\s*\\])`);
          return read.test(source) ? [] : [`${unitId} does not read process.env.${need}`];
        });
        if (needProblems.length > 0) {
          errors.push(`${where}: ${needProblems.join("; ")} (local-only needs an input CI lacks; a red proof belongs under quarantined)`);
          continue;
        }
      }
      unit.exception = { section, ...entry };
      unit.status = section === "localOnly" ? "local-only" : "quarantined";
    }
  }
  for (const [child, suite] of suiteOf) {
    const unit = units.get(child)!;
    if (unit.status !== "uncovered" || units.get(suite)!.status !== "ci") continue;
    unit.exception = { section: "suite", suite };
    unit.status = "runs-inside";
  }
  const sorted = [...units.values()].sort((a, b) => a.unit.localeCompare(b.unit));
  return {
    errors,
    workflows: models.map((model) => model.path),
    units: sorted,
    uncovered: sorted.filter((unit) => unit.status === "uncovered").map((unit) => unit.unit),
  };
}

function listProofFiles(repoRoot: string) {
  const found: string[] = [];
  const walk = (relative: string) => {
    for (const entry of fs.readdirSync(path.join(repoRoot, relative), { withFileTypes: true })) {
      const child = path.posix.join(relative, entry.name);
      if (entry.isDirectory()) {
        if (entry.name !== "node_modules" && entry.name !== "fixtures" && !entry.name.startsWith(".")) walk(child);
      } else if (entry.isFile() && isProofEntry(child)) {
        found.push(child);
      }
    }
  };
  walk("scripts");
  return found.sort();
}

/** Read package.json, every workflow, the exceptions file and the on-disk proof files of a checkout. */
export function readCoverageInput(repoRoot: string, today = new Date().toISOString().slice(0, 10)): CoverageInput {
  const pkg = JSON.parse(fs.readFileSync(path.join(repoRoot, "package.json"), "utf8")) as { scripts?: Record<string, string> };
  const workflowDirectory = path.join(repoRoot, WORKFLOW_DIRECTORY);
  const workflows = fs.existsSync(workflowDirectory)
    ? fs.readdirSync(workflowDirectory)
        .filter((name) => /\.ya?ml$/.test(name))
        .sort()
        .map((name) => ({
          path: path.posix.join(WORKFLOW_DIRECTORY, name),
          text: fs.readFileSync(path.join(workflowDirectory, name), "utf8"),
        }))
    : [];
  const exceptionsPath = path.join(repoRoot, PROOF_EXCEPTIONS);
  return {
    scripts: pkg.scripts ?? {},
    workflows,
    exceptions: fs.existsSync(exceptionsPath) ? JSON.parse(fs.readFileSync(exceptionsPath, "utf8")) : {},
    suites: readProofSuites(repoRoot),
    proofFiles: listProofFiles(repoRoot),
    readFile: (file) => {
      const full = path.join(repoRoot, file);
      return fs.existsSync(full) ? fs.readFileSync(full, "utf8") : null;
    },
    today,
  };
}

/** Null when the gate runs before every other proof in its job (fail fast); otherwise why not. */
export function gateFirstProblem(report: CoverageReport): string | null {
  const gate = report.units.find((unit) => unit.unit === GATE_ENTRY)?.covered[0];
  if (!gate) return `${GATE_ENTRY} is not run by CI`;
  for (const unit of report.units) {
    if (unit.unit === GATE_ENTRY) continue;
    for (const invocation of unit.covered) {
      const sameJob = invocation.workflow === gate.workflow && invocation.job === gate.job;
      const earlier = invocation.stepIndex < gate.stepIndex || (invocation.stepIndex === gate.stepIndex && invocation.position < gate.position);
      if (sameJob && earlier) return `${unit.unit} runs before the gate (step "${invocation.step}")`;
    }
  }
  return null;
}

/** Proof entry files every successful push/PR run executes, directly or inside a suite CI runs. */
export function proofFilesRunInCi(report: CoverageReport) {
  return new Set(report.units.filter((unit) => unit.status === "ci" || unit.status === "runs-inside").map((unit) => unit.unit));
}
