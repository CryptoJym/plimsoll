import fs from "node:fs";
import path from "node:path";

import { modelWorkflow } from "./ci-workflow-model";
import { PROOF_SUITES, readProofSuites } from "./proof-suites";
import { analyzeErrexitScript, analyzePackageScript } from "./shell-commands";

/**
 * Which proofs CI actually runs (eco-6hoxj.163.23).
 *
 * A proof that CI never runs rots silently: proof:enrollment-privacy failed
 * from 2026-09-07 until eco-6hoxj.163.22 while the published privacy spec
 * cited it. The unit of coverage is a proof entry file, found three ways:
 * every leaf file a `proof`/`proof:*` package script runs, every proof-named
 * file any package script runs, and every proof-named file under scripts/.
 * A unit is covered only when a workflow step that provably runs on every
 * successful push/PR to main executes a command that reaches it (directly, by
 * entry file, or through package-script aliases whose failure propagates), or
 * when scripts/proof-suites.json declares it as a sub-proof of a suite CI runs.
 * Every other unit needs a reviewed entry in scripts/proof-local-only.json.
 */

export const WORKFLOW_DIRECTORY = ".github/workflows";
export const PROOF_EXCEPTIONS = "scripts/proof-local-only.json";
export const GATE_ENTRY = "scripts/ci-coverage-proof.ts";
const PROOF_SCRIPT = /^proof(?::|$)/;
const RUN_PROOF_WRAPPER = "scripts/run-proof.ts";
const PNPM_BUILTINS = new Set([
  "add", "audit", "bin", "config", "create", "deploy", "dlx", "env", "fetch", "i", "import", "init",
  "install", "licenses", "link", "list", "ls", "outdated", "pack", "patch", "prune", "publish",
  "rebuild", "remove", "rm", "root", "server", "setup", "store", "unlink", "up", "update", "why",
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
  /** Position of the command within its step's script. */
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
  exception: Record<string, string> | null;
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

const normalizeFile = (word: string) => word.replace(/^\.\//, "");
const isRepoFile = (file: string) => /^(?:scripts|packages)\/\S+\.[cm]?[jt]s$/.test(file);

/** Files and package scripts one simple command runs; empty when it is not a recognised runner. */
export function commandTargets(words: Array<string | null>): { files: string[]; scripts: string[] } {
  const none = { files: [], scripts: [] };
  const [head, ...rest] = words;
  if (head === null || head === undefined) return none;
  if (head === "pnpm") {
    const [sub, ...args] = rest;
    if (sub === "run") return typeof args[0] === "string" ? { files: [], scripts: [args[0]] } : none;
    if (sub === "exec") return commandTargets(args);
    if (typeof sub !== "string" || sub.startsWith("-") || PNPM_BUILTINS.has(sub)) return none;
    return { files: [], scripts: [sub] };
  }
  if (head === "npm") return rest[0] === "run" && typeof rest[1] === "string" ? { files: [], scripts: [rest[1]] } : none;
  if (head === "npx") {
    const index = rest.findIndex((word) => word === null || !word.startsWith("-"));
    return index === -1 ? none : commandTargets(rest.slice(index));
  }
  const fileAfterFlags = (args: Array<string | null>, valueFlags: string[]) => {
    for (let index = 0; index < args.length; index += 1) {
      const word = args[index];
      if (word === null) return { file: null, rest: [] as Array<string | null> };
      if (word.startsWith("-")) {
        if (["-e", "--eval", "-p", "--print"].includes(word)) return { file: null, rest: [] };
        if (valueFlags.includes(word)) index += 1;
        continue;
      }
      return { file: normalizeFile(word), rest: args.slice(index + 1) };
    }
    return { file: null, rest: [] };
  };
  const leaf = (file: string | null, args: Array<string | null>): { files: string[]; scripts: string[] } => {
    if (!file || !isRepoFile(file)) return none;
    if (file === RUN_PROOF_WRAPPER) {
      const entryArgs = args[0] === "--direct-node" ? args.slice(1) : args;
      const entry = typeof entryArgs[0] === "string" ? normalizeFile(entryArgs[0]) : null;
      return entry && isRepoFile(entry) ? { files: [entry], scripts: [] } : none;
    }
    return { files: [file], scripts: [] };
  };
  if (head === "tsx" || head.endsWith("/tsx")) {
    const { file, rest: args } = fileAfterFlags(rest, ["--tsconfig", "--import", "--require", "-r"]);
    return leaf(file, args);
  }
  if (head === "node") {
    const { file, rest: args } = fileAfterFlags(rest, ["--import", "-r", "--require", "--loader", "--experimental-loader"]);
    if (file && /(?:^|\/)node_modules\/(?:tsx\/dist\/cli\.[cm]?js|\.bin\/tsx)$/.test(file)) {
      const inner = fileAfterFlags(args, ["--tsconfig", "--import", "--require", "-r"]);
      return leaf(inner.file, inner.rest);
    }
    return leaf(file, args);
  }
  return none;
}

type Resolution = { all: Set<string>; propagating: Set<string>; aliases: Set<string> };

function packageResolver(scripts: Record<string, string>) {
  const cache = new Map<string, Resolution>();
  const resolve = (name: string, stack: string[] = []): Resolution => {
    const cached = cache.get(name);
    if (cached) return cached;
    const empty: Resolution = { all: new Set(), propagating: new Set(), aliases: new Set() };
    if (!Object.hasOwn(scripts, name) || stack.includes(name)) return empty;
    const analysis = analyzePackageScript(scripts[name]!);
    const result: Resolution = { all: new Set(), propagating: new Set([`package.json#${name}`]), aliases: new Set() };
    for (const command of analysis.seen) {
      const targets = commandTargets(command.words);
      for (const file of targets.files) result.all.add(file);
      for (const ref of targets.scripts) {
        result.aliases.add(ref);
        for (const file of resolve(ref, [...stack, name]).all) result.all.add(file);
      }
    }
    for (const command of analysis.executed) {
      const targets = commandTargets(command.words);
      for (const file of targets.files) result.propagating.add(file);
      for (const ref of targets.scripts) for (const target of resolve(ref, [...stack, name]).propagating) result.propagating.add(target);
    }
    cache.set(name, result);
    return result;
  };
  return resolve;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

export function proofCiCoverage(input: CoverageInput): CoverageReport {
  const errors: string[] = [];
  const resolve = packageResolver(input.scripts);

  // 1. Inventory.
  const units = new Map<string, UnitCoverage>();
  const addUnit = (unit: string, source: string, script?: string) => {
    const entry = units.get(unit) ?? { unit, scripts: [], sources: [], covered: [], ignored: [], status: "uncovered" as UnitStatus, exception: null };
    if (!entry.sources.includes(source)) entry.sources.push(source);
    if (script && !entry.scripts.includes(script)) entry.scripts.push(script);
    units.set(unit, entry);
  };
  for (const name of Object.keys(input.scripts).sort()) {
    const { all } = resolve(name);
    if (PROOF_SCRIPT.test(name)) {
      if (all.size === 0) addUnit(`package.json#${name}`, "proof script without an entry file", name);
      for (const file of all) addUnit(file, "proof script leaf", name);
    } else {
      for (const file of all) if (isProofEntry(file)) addUnit(file, "proof file run by a package script", name);
    }
  }
  // The run-proof wrapper matches the proof file-name rule but is not a proof.
  for (const file of input.proofFiles) if (file !== RUN_PROOF_WRAPPER) addUnit(file, "proof file on disk");

  // 2. What CI runs.
  if (input.workflows.length === 0) errors.push(`no workflow files under ${WORKFLOW_DIRECTORY}: nothing runs any proof`);
  const models = input.workflows.map((workflow) => modelWorkflow(workflow.path, workflow.text));
  for (const model of models) errors.push(...model.errors);
  for (const model of models) {
    for (const step of model.steps) {
      if (step.run === null) continue;
      const analysis = analyzeErrexitScript(step.run);
      const executed = new Set(analysis.executed);
      for (const [position, command] of analysis.seen.entries()) {
        const targets = commandTargets(command.words);
        const reached = new Map<string, string[]>();
        for (const file of targets.files) reached.set(file, []);
        for (const ref of targets.scripts) {
          if (!Object.hasOwn(input.scripts, ref)) {
            if (PROOF_SCRIPT.test(ref)) errors.push(`${model.path} step "${step.name}" runs \`pnpm ${ref}\`, which package.json does not define`);
            continue;
          }
          const { propagating } = resolve(ref);
          for (const target of propagating) if (!reached.has(target)) reached.set(target, [ref]);
        }
        const invocation = { workflow: model.path, job: step.job, step: step.name, stepIndex: step.stepIndex, position, line: step.line, command: command.text };
        const reason = step.notCovering ?? (executed.has(command) ? null : analysis.stoppedAt?.reason ?? "not a standalone reachable command");
        for (const [target, via] of reached) {
          const unit = units.get(target);
          if (!unit) continue;
          if (reason) unit.ignored.push({ ...invocation, via, reason });
          else unit.covered.push({ ...invocation, via });
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
      if (!isRecord(raw) || Object.values(raw).some((value) => typeof value !== "string")) {
        errors.push(`${where}: entry must be a mapping of strings`);
        continue;
      }
      const entry = raw as Record<string, string>;
      const required = section === "localOnly" ? ["reason", "owner"] : ["reason", "owner", "expires"];
      const missing = required.filter((field) => !entry[field]?.trim());
      if (missing.length > 0) {
        errors.push(`${where}: missing ${missing.join(", ")}`);
        continue;
      }
      if (section === "quarantined") {
        if (!/^\d{4}-\d{2}-\d{2}$/.test(entry.expires!)) {
          errors.push(`${where}: expires must be YYYY-MM-DD`);
          continue;
        }
        if (input.today > entry.expires!) {
          errors.push(`${where}: quarantine expired on ${entry.expires} (owner ${entry.owner}); repair and wire it, or renew the entry in review`);
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
