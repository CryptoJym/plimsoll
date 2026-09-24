import { LineCounter, parseDocument, type Document } from "yaml";

/**
 * Which GitHub Actions `run:` steps provably execute in every successful run
 * for a push to main and a pull request into main (eco-6hoxj.163.23).
 *
 * The model is conservative by construction: a step counts only when its
 * workflow triggers on both events without path filters, its job and the step
 * itself run under every event and matrix combination (`if:` evaluated
 * three-valued: anything not provably true is not true), neither sets
 * `continue-on-error`, the step runs bash in the workspace root, and nothing
 * it depends on can be skipped. Everything else is reported with a reason.
 * A YAML merge key (`<<`) or a workflow, job or step key GitHub does not
 * define is an error: the gate does not guess what an unknown key does.
 */

export const COVERAGE_EVENTS = ["push", "pull_request"] as const;
export type CoverageEvent = (typeof COVERAGE_EVENTS)[number];

export type WorkflowStep = {
  workflow: string;
  job: string;
  stepIndex: number;
  name: string;
  line: number;
  run: string | null;
  uses: string | null;
  /** Names the step's `env:` sets; null when it is not a literal mapping. */
  env: string[] | null;
  /** Null when every successful push/PR run executes this step's shell script with errexit. */
  notCovering: string | null;
};

export type WorkflowModel = {
  path: string;
  errors: string[];
  triggerProblems: string[];
  steps: WorkflowStep[];
  /** Names the workflow-level `env:` sets; null when it is not a literal mapping. */
  env: string[] | null;
  /** Per job: the names its `env:` sets (null when not a literal mapping), and whether it runs in a container. */
  jobs: Record<string, { env: string[] | null; container: boolean }>;
};

// ---------------------------------------------------------------------------
// Expressions: three-valued evaluation of GitHub Actions `${{ }}` syntax.
// ---------------------------------------------------------------------------

const UNKNOWN = Symbol("unknown");
type Value = unknown; // a JSON value or UNKNOWN
type Truth = "true" | "false" | "unknown";

/** Context object whose missing keys are null (complete) or unknown (partial). */
class Ctx {
  constructor(readonly entries: Record<string, Value>, readonly complete: boolean) {}
  get(key: string): Value {
    const match = Object.keys(this.entries).find((name) => name.toLowerCase() === key.toLowerCase());
    if (match !== undefined) return this.entries[match];
    return this.complete ? null : UNKNOWN;
  }
}

type Token = { kind: "str" | "num" | "id" | "op"; value: string };

function tokenizeExpression(text: string): Token[] | null {
  const tokens: Token[] = [];
  let i = 0;
  while (i < text.length) {
    const c = text[i]!;
    if (/\s/.test(c)) {
      i += 1;
      continue;
    }
    if (c === "'") {
      let value = "";
      i += 1;
      for (;;) {
        if (i >= text.length) return null;
        if (text[i] === "'" && text[i + 1] === "'") {
          value += "'";
          i += 2;
          continue;
        }
        if (text[i] === "'") break;
        value += text[i];
        i += 1;
      }
      i += 1;
      tokens.push({ kind: "str", value });
      continue;
    }
    const two = text.slice(i, i + 2);
    if (["==", "!=", "<=", ">=", "&&", "||"].includes(two)) {
      tokens.push({ kind: "op", value: two });
      i += 2;
      continue;
    }
    if ("!<>()[].,*".includes(c)) {
      tokens.push({ kind: "op", value: c });
      i += 1;
      continue;
    }
    const number = /^(?:0x[0-9a-f]+|-?\d+(?:\.\d+)?(?:e[+-]?\d+)?)/i.exec(text.slice(i));
    if (number && !/[A-Za-z_]/.test(text[i + number[0].length] ?? "")) {
      tokens.push({ kind: "num", value: number[0] });
      i += number[0].length;
      continue;
    }
    const id = /^[A-Za-z_][A-Za-z0-9_-]*/.exec(text.slice(i));
    if (id) {
      tokens.push({ kind: "id", value: id[0] });
      i += id[0].length;
      continue;
    }
    return null;
  }
  return tokens;
}

type Node =
  | { type: "lit"; value: Value }
  | { type: "ctx"; name: string }
  | { type: "prop"; object: Node; key: Node | string }
  | { type: "call"; name: string; args: Node[] }
  | { type: "not"; operand: Node }
  | { type: "bin"; op: string; left: Node; right: Node };

function parseExpression(text: string): Node | null {
  const tokens = tokenizeExpression(text);
  if (!tokens) return null;
  let position = 0;
  const peek = () => tokens[position];
  const accept = (value: string) => {
    if (peek()?.kind === "op" && peek()!.value === value) {
      position += 1;
      return true;
    }
    return false;
  };
  const binary = (next: () => Node | null, ops: string[]) => (): Node | null => {
    let left = next();
    while (left && peek()?.kind === "op" && ops.includes(peek()!.value)) {
      const op = tokens[position++]!.value;
      const right = next();
      if (!right) return null;
      left = { type: "bin", op, left, right };
    }
    return left;
  };
  const primary = (): Node | null => {
    const token = tokens[position++];
    if (!token) return null;
    if (token.kind === "str") return { type: "lit", value: token.value };
    if (token.kind === "num") return { type: "lit", value: Number(token.value) };
    if (token.kind === "op" && token.value === "(") {
      const inner = or();
      return inner && accept(")") ? inner : null;
    }
    if (token.kind === "op" && token.value === "!") {
      const operand = unary();
      return operand ? { type: "not", operand } : null;
    }
    if (token.kind !== "id") return null;
    if (token.value === "true") return { type: "lit", value: true };
    if (token.value === "false") return { type: "lit", value: false };
    if (token.value === "null") return { type: "lit", value: null };
    if (accept("(")) {
      const args: Node[] = [];
      if (!accept(")")) {
        for (;;) {
          const arg = or();
          if (!arg) return null;
          args.push(arg);
          if (accept(")")) break;
          if (!accept(",")) return null;
        }
      }
      return { type: "call", name: token.value.toLowerCase(), args };
    }
    return { type: "ctx", name: token.value };
  };
  const postfix = (): Node | null => {
    let node = primary();
    while (node) {
      if (accept(".")) {
        const key = tokens[position++];
        if (!key || (key.kind !== "id" && !(key.kind === "op" && key.value === "*"))) return null;
        node = { type: "prop", object: node, key: key.value };
      } else if (accept("[")) {
        const key = or();
        if (!key || !accept("]")) return null;
        node = { type: "prop", object: node, key };
      } else {
        break;
      }
    }
    return node;
  };
  const unary = (): Node | null => (accept("!") ? ((operand) => (operand ? { type: "not", operand } as Node : null))(unary()) : postfix());
  const comparison = binary(unary, ["<", "<=", ">", ">="]);
  const equality = binary(comparison, ["==", "!="]);
  const and = binary(equality, ["&&"]);
  const or = binary(and, ["||"]);
  const node = or();
  return node && position === tokens.length ? node : null;
}

function truthOf(value: Value): Truth {
  if (value === UNKNOWN) return "unknown";
  if (value === false || value === null || value === "" || value === 0 || Number.isNaN(value)) return "false";
  return "true";
}

function toNumber(value: Value): number {
  if (value === null) return 0;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "number") return value;
  if (typeof value === "string") return value.trim() === "" ? 0 : Number(value);
  return Number.NaN;
}

function looseEqual(left: Value, right: Value): Value {
  if (left === UNKNOWN || right === UNKNOWN) return UNKNOWN;
  if (typeof left === "string" && typeof right === "string") return left.toLowerCase() === right.toLowerCase();
  if (typeof left === typeof right && (typeof left !== "object" || left === null)) return left === right;
  if (typeof left === "object" && left !== null) return left === right;
  if (typeof right === "object" && right !== null) return false;
  const a = toNumber(left);
  const b = toNumber(right);
  return !Number.isNaN(a) && a === b;
}

function evaluate(node: Node, contexts: Record<string, Ctx>): Value {
  switch (node.type) {
    case "lit":
      return node.value;
    case "ctx":
      return contexts[node.name.toLowerCase()] ?? UNKNOWN;
    case "prop": {
      const object = evaluate(node.object, contexts);
      const key = typeof node.key === "string" ? node.key : evaluate(node.key, contexts);
      if (object === UNKNOWN || key === UNKNOWN || key === "*") return UNKNOWN;
      if (object instanceof Ctx) return object.get(String(key));
      if (object !== null && typeof object === "object" && !Array.isArray(object)) {
        const record = object as Record<string, Value>;
        const match = Object.keys(record).find((name) => name.toLowerCase() === String(key).toLowerCase());
        return match === undefined ? null : record[match];
      }
      return null;
    }
    case "not": {
      const truth = truthOf(asPlain(evaluate(node.operand, contexts)));
      return truth === "unknown" ? UNKNOWN : truth === "false";
    }
    case "call": {
      const args = node.args.map((arg) => asPlain(evaluate(arg, contexts)));
      switch (node.name) {
        case "success":
        case "always":
          return args.length === 0 ? true : UNKNOWN;
        case "failure":
        case "cancelled":
          return args.length === 0 ? false : UNKNOWN;
        case "contains":
        case "startswith":
        case "endswith": {
          const [haystack, needle] = args;
          if (args.length !== 2 || haystack === UNKNOWN || needle === UNKNOWN) return UNKNOWN;
          if (node.name === "contains" && Array.isArray(haystack)) {
            if (haystack.some((item) => item === UNKNOWN)) return UNKNOWN;
            return haystack.some((item) => looseEqual(item, needle) === true);
          }
          if (typeof haystack !== "string" || typeof needle !== "string") return UNKNOWN;
          const h = haystack.toLowerCase();
          const n = needle.toLowerCase();
          return node.name === "contains" ? h.includes(n) : node.name === "startswith" ? h.startsWith(n) : h.endsWith(n);
        }
        default:
          return UNKNOWN;
      }
    }
    case "bin": {
      if (node.op === "&&" || node.op === "||") {
        const left = asPlain(evaluate(node.left, contexts));
        const leftTruth = truthOf(left);
        if (node.op === "&&" && leftTruth === "false") return left;
        if (node.op === "||" && leftTruth === "true") return left;
        const right = asPlain(evaluate(node.right, contexts));
        if (leftTruth === "unknown") {
          // Result is left or right: only the truth value may still be decided.
          const rightTruth = truthOf(right);
          if (node.op === "&&" && rightTruth === "false") return false;
          if (node.op === "||" && rightTruth === "true") return true;
          return UNKNOWN;
        }
        return right;
      }
      const left = asPlain(evaluate(node.left, contexts));
      const right = asPlain(evaluate(node.right, contexts));
      if (left === UNKNOWN || right === UNKNOWN) return UNKNOWN;
      if (node.op === "==") return looseEqual(left, right);
      if (node.op === "!=") {
        const equal = looseEqual(left, right);
        return equal === UNKNOWN ? UNKNOWN : !equal;
      }
      if (typeof left === "string" && typeof right === "string") {
        const a = left.toLowerCase();
        const b = right.toLowerCase();
        return node.op === "<" ? a < b : node.op === "<=" ? a <= b : node.op === ">" ? a > b : a >= b;
      }
      const a = toNumber(left);
      const b = toNumber(right);
      if (Number.isNaN(a) || Number.isNaN(b)) return false;
      return node.op === "<" ? a < b : node.op === "<=" ? a <= b : node.op === ">" ? a > b : a >= b;
    }
  }
}

/** Contexts are objects too: a whole-context value is opaque to operators. */
function asPlain(value: Value): Value {
  return value instanceof Ctx ? UNKNOWN : value;
}

/** Evaluate an `if:` (or similar) value; anything not provably truthy is not "true". */
export function evaluateCondition(raw: unknown, contexts: Record<string, Ctx>): Truth {
  if (raw === undefined) return "true"; // implicit success()
  if (typeof raw === "boolean" || typeof raw === "number" || raw === null) return truthOf(raw);
  if (typeof raw !== "string") return "unknown";
  let text = raw.trim();
  const wrapped = /^\$\{\{([\s\S]*)\}\}$/.exec(text);
  if (wrapped) {
    if (wrapped[1]!.includes("}}") || wrapped[1]!.includes("${{")) return "unknown";
    text = wrapped[1]!;
  } else if (text.includes("${{")) {
    return "unknown"; // mixed text and interpolation
  }
  const node = parseExpression(text);
  if (!node) return "unknown";
  return truthOf(asPlain(evaluate(node, contexts)));
}

// ---------------------------------------------------------------------------
// Workflow structure.
// ---------------------------------------------------------------------------

type Json = Record<string, unknown>;
const isRecord = (value: unknown): value is Json => value !== null && typeof value === "object" && !Array.isArray(value);

function globMatchesMain(pattern: unknown) {
  // GitHub filter globs; only patterns that certainly match "main" count.
  return pattern === "main" || pattern === "*" || pattern === "**";
}

/** Reasons the workflow might not run for this event (empty = it always runs). */
function triggerProblems(on: unknown, event: CoverageEvent): string[] {
  let config: unknown;
  if (typeof on === "string") config = on === event ? {} : undefined;
  else if (Array.isArray(on)) config = on.includes(event) ? {} : undefined;
  else if (isRecord(on)) config = Object.hasOwn(on, event) ? on[event] ?? {} : undefined;
  if (config === undefined) return [`does not trigger on ${event}`];
  if (!isRecord(config)) return [`${event} trigger is not a mapping`];
  const problems: string[] = [];
  for (const key of Object.keys(config)) {
    if (!["branches", "branches-ignore", "types", "tags", "tags-ignore"].includes(key)) {
      problems.push(`${event} trigger filter \`${key}\` can skip runs`);
    }
  }
  if (Object.hasOwn(config, "branches")) {
    const branches = config.branches;
    if (!Array.isArray(branches) || !branches.some(globMatchesMain) || branches.some((b) => typeof b === "string" && b.startsWith("!"))) {
      problems.push(`${event} branches filter does not provably include main`);
    }
  }
  if (Object.hasOwn(config, "branches-ignore")) problems.push(`${event} branches-ignore can skip main`);
  if (event === "push" && (Object.hasOwn(config, "tags") || Object.hasOwn(config, "tags-ignore")) && !Object.hasOwn(config, "branches")) {
    problems.push("push trigger lists only tags, so branch pushes do not run it");
  }
  if (event === "pull_request" && Object.hasOwn(config, "types")) {
    const types = config.types;
    const required = ["opened", "synchronize", "reopened"];
    if (!Array.isArray(types) || !required.every((type) => types.includes(type))) {
      problems.push("pull_request types do not include opened, synchronize and reopened");
    }
  }
  return problems;
}

function githubContext(event: CoverageEvent): Ctx {
  return event === "push"
    ? new Ctx({ event_name: "push", ref: "refs/heads/main", ref_name: "main", ref_type: "branch", base_ref: "", head_ref: "" }, false)
    : new Ctx({ event_name: "pull_request", base_ref: "main" }, false);
}

/** Static matrix combinations, or a reason the matrix cannot be known. */
function matrixCombinations(strategy: unknown): { combos: Json[] } | { problem: string } {
  if (strategy === undefined) return { combos: [{}] };
  if (!isRecord(strategy)) return { problem: "strategy is not a mapping" };
  const matrix = strategy.matrix;
  if (matrix === undefined) return { combos: [{}] };
  if (!isRecord(matrix)) return { problem: "matrix is computed at run time" };
  const include = matrix.include ?? [];
  const exclude = matrix.exclude ?? [];
  if (!Array.isArray(include) || !Array.isArray(exclude) || ![...include, ...exclude].every(isRecord)) {
    return { problem: "matrix include/exclude is not a static list of mappings" };
  }
  const axes = Object.entries(matrix).filter(([key]) => key !== "include" && key !== "exclude");
  if (axes.some(([, values]) => !Array.isArray(values))) return { problem: "matrix axis is computed at run time" };
  const hasExpression = (value: unknown): boolean =>
    typeof value === "string" ? value.includes("${{") : Array.isArray(value) ? value.some(hasExpression) : isRecord(value) ? Object.values(value).some(hasExpression) : false;
  if (hasExpression(matrix)) return { problem: "matrix contains an expression" };
  let combos: Json[] = [{}];
  for (const [key, values] of axes) {
    combos = combos.flatMap((combo) => (values as unknown[]).map((value) => ({ ...combo, [key]: value })));
  }
  if (axes.length === 0) combos = [];
  combos = combos.filter((combo) => !exclude.some((entry) => Object.entries(entry as Json).every(([k, v]) => looseEqual(combo[k], v) === true)));
  const axisKeys = new Set(axes.map(([key]) => key));
  for (const entry of include as Json[]) {
    let merged = false;
    for (const combo of combos) {
      const conflicts = Object.entries(entry).some(([k, v]) => axisKeys.has(k) && looseEqual(combo[k], v) !== true);
      if (!conflicts) {
        Object.assign(combo, entry);
        merged = true;
      }
    }
    if (!merged) combos.push({ ...entry });
  }
  if (combos.length === 0) return { problem: "matrix has no combinations" };
  return { combos };
}

const NON_WINDOWS_RUNNER = /^(?:macos|ubuntu)-[A-Za-z0-9.-]+$/;

const WORKFLOW_KEYS = ["name", "run-name", "on", "permissions", "env", "defaults", "concurrency", "jobs"];
const JOB_KEYS = [
  "name", "permissions", "needs", "if", "runs-on", "snapshot", "environment", "concurrency", "outputs", "env",
  "defaults", "steps", "timeout-minutes", "strategy", "continue-on-error", "container", "services", "uses", "with", "secrets",
];
const STEP_KEYS = ["id", "if", "name", "uses", "run", "shell", "with", "env", "continue-on-error", "timeout-minutes", "working-directory"];

/** Where a YAML merge key appears (yaml parses `<<` as a plain key under YAML 1.2). */
function mergeKeys(value: unknown, at: string): string[] {
  if (Array.isArray(value)) return value.flatMap((item, index) => mergeKeys(item, `${at}[${index}]`));
  if (!isRecord(value)) return [];
  return Object.entries(value).flatMap(([key, child]) => [...(key === "<<" ? [at || "(top level)"] : []), ...mergeKeys(child, at ? `${at}.${key}` : key)]);
}

/** Keys of a workflow, its jobs and their steps that GitHub does not define. */
function unknownKeys(path: string, workflow: Json): string[] {
  const unknown = (record: Json, known: string[], where: string) =>
    Object.keys(record)
      .filter((key) => key !== "<<" && !known.includes(key))
      .map((key) => `${where}: unknown key \`${key}\``);
  const problems = unknown(workflow, WORKFLOW_KEYS, path);
  for (const [id, job] of Object.entries(isRecord(workflow.jobs) ? workflow.jobs : {})) {
    if (!isRecord(job)) continue;
    problems.push(...unknown(job, JOB_KEYS, `${path} job "${id}"`));
    (Array.isArray(job.steps) ? job.steps : []).forEach((step: unknown, index: number) => {
      if (isRecord(step)) problems.push(...unknown(step, STEP_KEYS, `${path} job "${id}" step ${index + 1}`));
    });
  }
  return problems;
}

const envNames = (env: unknown): string[] | null => (env === undefined ? [] : isRecord(env) ? Object.keys(env) : null);

function lineOf(document: Document, lineCounter: LineCounter, path: Array<string | number>) {
  const node = document.getIn(path, true) as { range?: [number, number, number] } | undefined;
  return node?.range ? lineCounter.linePos(node.range[0]).line : 0;
}

export function modelWorkflow(path: string, text: string): WorkflowModel {
  const lineCounter = new LineCounter();
  const document = parseDocument(text, { lineCounter, prettyErrors: true, uniqueKeys: true });
  const errors = [...document.errors, ...document.warnings].map((problem) => `${path}: ${problem.message.split("\n")[0]}`);
  if (errors.length > 0) return { path, errors, triggerProblems: [], steps: [], env: [], jobs: {} };
  let workflow: unknown;
  try {
    workflow = document.toJS({ maxAliasCount: 100 });
  } catch (error) {
    return { path, errors: [`${path}: ${error instanceof Error ? error.message : String(error)}`], triggerProblems: [], steps: [], env: [], jobs: {} };
  }
  if (!isRecord(workflow) || !isRecord(workflow.jobs)) {
    return { path, errors: [`${path}: not a workflow with a jobs mapping`], triggerProblems: [], steps: [], env: [], jobs: {} };
  }
  const structure = [
    ...mergeKeys(workflow, "").map((at) => `${path}: YAML merge key \`<<\` at ${at}; GitHub does not merge keys and the gate does not guess`),
    ...unknownKeys(path, workflow),
  ];
  const triggers = COVERAGE_EVENTS.flatMap((event) => triggerProblems(workflow.on, event));
  const jobs = workflow.jobs as Json;
  const workflowDefaults = isRecord(workflow.defaults) && isRecord(workflow.defaults.run) ? workflow.defaults.run : {};

  const jobCovering = new Map<string, string | null>();
  const jobReason = (id: string, seen: Set<string>): string | null => {
    if (jobCovering.has(id)) return jobCovering.get(id)!;
    if (seen.has(id)) return "job dependency cycle";
    seen.add(id);
    const job = jobs[id];
    let reason: string | null = null;
    if (!isRecord(job)) reason = "job is not a mapping";
    else if (job.uses !== undefined) reason = "reusable workflow call is not modelled";
    else if (job["continue-on-error"] !== undefined && job["continue-on-error"] !== false) reason = "job sets continue-on-error";
    else {
      const needs = job.needs === undefined ? [] : Array.isArray(job.needs) ? job.needs : [job.needs];
      for (const need of needs) {
        const needReason = typeof need === "string" && Object.hasOwn(jobs, need) ? jobReason(need, seen) : "needs an unknown job";
        if (needReason) {
          reason = `needs \`${String(need)}\`, which ${needReason}`;
          break;
        }
      }
      if (!reason) {
        for (const event of COVERAGE_EVENTS) {
          const needsContext = new Ctx(
            Object.fromEntries(needs.map((need) => [String(need), new Ctx({ result: "success" }, false)])),
            false,
          );
          const truth = evaluateCondition(job.if, { github: githubContext(event), needs: needsContext });
          if (truth !== "true") {
            reason = `job if: is not provably true on ${event} (${truth})`;
            break;
          }
        }
      }
    }
    jobCovering.set(id, reason);
    return reason;
  };

  const steps: WorkflowStep[] = [];
  for (const [jobId, job] of Object.entries(jobs)) {
    const jobProblem = jobReason(jobId, new Set());
    const jobDefaults = isRecord(job) && isRecord(job.defaults) && isRecord(job.defaults.run) ? job.defaults.run : {};
    const knownBashRunner = isRecord(job) && typeof job["runs-on"] === "string" && NON_WINDOWS_RUNNER.test(job["runs-on"]);
    const jobSteps = isRecord(job) && Array.isArray(job.steps) ? job.steps : [];
    const matrix = isRecord(job) ? matrixCombinations(job.strategy) : { problem: "job is not a mapping" };
    // Step ids that ran and succeeded in every successful run, per event and matrix combination.
    const succeeded = new Map<string, Set<string>>();
    jobSteps.forEach((step: unknown, index: number) => {
      const name = isRecord(step) ? String(step.name ?? step.uses ?? step.run ?? `step ${index + 1}`).split("\n")[0]! : `step ${index + 1}`;
      const line = lineOf(document, lineCounter, ["jobs", jobId, "steps", index]);
      const run = isRecord(step) && typeof step.run === "string" ? step.run : null;
      // 1. Does this step run, and succeed, in every successful push/PR run?
      let runs: string | null = triggers.length > 0 ? `workflow ${triggers.join("; ")}` : jobProblem;
      if (!runs && "problem" in matrix) runs = matrix.problem;
      if (!runs && !isRecord(step)) runs = "step is not a mapping";
      const record = (isRecord(step) ? step : {}) as Json;
      if (!runs && record["continue-on-error"] !== undefined && record["continue-on-error"] !== false) {
        runs = "step sets continue-on-error";
      }
      if (!runs && "combos" in matrix) {
        outer: for (const event of COVERAGE_EVENTS) {
          for (const combo of matrix.combos) {
            const done = succeeded.get(`${event}:${JSON.stringify(combo)}`) ?? new Set<string>();
            const stepsContext = new Ctx(
              Object.fromEntries([...done].map((id) => [id, new Ctx({ outcome: "success", conclusion: "success" }, false)])),
              false,
            );
            const truth = evaluateCondition(record.if, {
              github: githubContext(event),
              matrix: new Ctx(combo, true),
              steps: stepsContext,
            });
            if (truth !== "true") {
              runs = `step if: is not provably true on ${event}${Object.keys(combo).length ? ` for matrix ${JSON.stringify(combo)}` : ""} (${truth})`;
              break outer;
            }
          }
        }
      }
      if (!runs && typeof record.id === "string" && "combos" in matrix) {
        for (const event of COVERAGE_EVENTS) {
          for (const combo of matrix.combos) {
            const key = `${event}:${JSON.stringify(combo)}`;
            succeeded.set(key, (succeeded.get(key) ?? new Set()).add(record.id));
          }
        }
      }
      // 2. Can its script count: bash with errexit, in the workspace root?
      let notCovering = runs;
      if (!notCovering && run !== null) {
        const shell = record.shell ?? jobDefaults.shell ?? workflowDefaults.shell;
        if (shell === undefined && !knownBashRunner) notCovering = "default shell is unknown (runs-on is not a known macOS/Ubuntu runner)";
        else if (shell !== undefined && shell !== "bash") notCovering = `shell \`${String(shell)}\` is not modelled`;
        const workingDirectory = record["working-directory"] ?? jobDefaults["working-directory"] ?? workflowDefaults["working-directory"];
        if (!notCovering && workingDirectory !== undefined && !["", ".", "./", "${{ github.workspace }}"].includes(String(workingDirectory))) {
          notCovering = `runs in working-directory \`${String(workingDirectory)}\``;
        }
      }
      const uses = typeof record.uses === "string" ? record.uses : null;
      steps.push({ workflow: path, job: jobId, stepIndex: index, name, line, run, uses, env: envNames(record.env), notCovering });
    });
  }
  const jobInfo = Object.fromEntries(
    Object.entries(jobs).map(([id, job]) => [id, { env: isRecord(job) ? envNames(job.env) : null, container: isRecord(job) && job.container !== undefined }]),
  );
  return { path, errors: structure, triggerProblems: triggers, steps, env: envNames(workflow.env), jobs: jobInfo };
}
