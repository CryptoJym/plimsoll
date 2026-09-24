/**
 * The only command forms proof:ci-coverage counts as running a proof
 * (eco-6hoxj.163.23).
 *
 * This is an allow-list, not a shell parser. Two independent reviews found
 * that each shell feature the gate modelled by hand left another way to make
 * a proof look run when it was not. So a command either matches one of the
 * exact forms below, or it is not canonical and the gate fails.
 *
 * A package script (pnpm runs it with `sh -c`) is canonical when it is
 * exactly one of:
 *   tsx <file> [args]
 *   node [--expose-gc] --import tsx <file> [args]
 *   node ./node_modules/tsx/dist/cli.mjs <file> [args]
 *   pnpm <script>                                     (a pure alias)
 * where <file> is a repository .ts/.js file under scripts/ or packages/ and
 * `tsx scripts/run-proof.ts [--direct-node] <file> [args]` runs <file>. Every
 * word must be unquoted and made only of the characters [A-Za-z0-9_./:@%+,=-],
 * so a second statement, `;`, `&&`, `||`, a pipe, a redirection, quoting, an
 * expansion, an environment prefix, `exit`, `trap` or any other flag (such as
 * `node --check`) makes the script non-canonical.
 */

export const LITERAL_WORD = /^[A-Za-z0-9_./:@%+,=-]+$/;
export const RUN_PROOF_WRAPPER = "scripts/run-proof.ts";
const TSX_CLI = "node_modules/tsx/dist/cli.mjs";
const REPO_FILE = /^(?:scripts|packages)\/(?:[A-Za-z0-9_.-]+\/)*[A-Za-z0-9_.-]+\.[cm]?[jt]s$/;
const ASSIGNMENT = /^[A-Za-z_][A-Za-z0-9_]*=/;

export type Problem = { problem: string };
export type RunnerInvocation = { file: string; args: string[] };
export type ScriptForm = RunnerInvocation | { alias: string };

export const normalizeFile = (word: string) => word.replace(/^\.\//, "");

/** A repository script path (scripts/ or packages/, no `.` or `..` segments). */
export function isRepoFile(word: string) {
  const file = normalizeFile(word);
  return REPO_FILE.test(file) && !file.split("/").some((segment) => segment === "." || segment === "..");
}

/** The file one runner command executes, or why its words are not a canonical runner invocation. */
export function runnerInvocation(words: string[]): RunnerInvocation | Problem {
  const [head, ...rest] = words;
  let fileAt = 0;
  if (head === "node") {
    if (rest[0] !== undefined && normalizeFile(rest[0]) === TSX_CLI) {
      fileAt = 1;
    } else {
      let loadsTsx = false;
      while (rest[fileAt]?.startsWith("-")) {
        if (rest[fileAt] === "--import" && rest[fileAt + 1] === "tsx" && !loadsTsx) {
          loadsTsx = true;
          fileAt += 2;
        } else if (rest[fileAt] === "--expose-gc") {
          fileAt += 1;
        } else {
          return { problem: `node flag \`${rest[fileAt]}\` is not allowed (only --import tsx and --expose-gc)` };
        }
      }
      if (!loadsTsx) return { problem: "node must load tsx (`--import tsx`) or run ./node_modules/tsx/dist/cli.mjs" };
    }
  } else if (head !== "tsx") {
    return { problem: `\`${head ?? ""}\` is not an allowed runner (tsx, node or a pure \`pnpm <script>\` alias)` };
  }
  const file = rest[fileAt];
  if (file === undefined) return { problem: `${head} runs no file` };
  if (file.startsWith("-")) return { problem: `${head} flag \`${file}\` is not allowed` };
  if (!isRepoFile(file)) return { problem: `\`${file}\` is not a script file under scripts/ or packages/` };
  const args = rest.slice(fileAt + 1);
  if (normalizeFile(file) !== RUN_PROOF_WRAPPER) return { file: normalizeFile(file), args };
  const entryArgs = args[0] === "--direct-node" ? args.slice(1) : args;
  const entry = entryArgs[0];
  if (entry === undefined || !isRepoFile(entry) || normalizeFile(entry) === RUN_PROOF_WRAPPER) {
    return { problem: `${RUN_PROOF_WRAPPER} must be given one script file (optionally after --direct-node)` };
  }
  return { file: normalizeFile(entry), args: entryArgs.slice(1) };
}

/** The canonical form of a package script, or why it is not canonical. */
export function packageScriptForm(text: string): ScriptForm | Problem {
  const words = text.trim().split(/[ \t]+/);
  if (text.trim() === "") return { problem: "the script is empty" };
  const plain = words.find((word) => !LITERAL_WORD.test(word));
  if (plain !== undefined) {
    return { problem: `\`${plain}\` is not a plain word (no operators, redirections, quoting, expansions or newlines)` };
  }
  if (ASSIGNMENT.test(words[0]!)) return { problem: `\`${words[0]}\` sets the environment before the command` };
  if (words[0] === "pnpm") {
    return words.length === 2 && !words[1]!.startsWith("-")
      ? { alias: words[1]! }
      : { problem: "pnpm is allowed only as a pure alias, `pnpm <script>`" };
  }
  return runnerInvocation(words);
}

/**
 * Every word-like token of arbitrary shell text. Over-approximate on
 * purpose: it decides what must be canonical and what the inventory holds,
 * and never counts anything as run.
 */
export function mentionedWords(text: string): string[] {
  return text.split(/[\s;&|()<>'"`$={}\\]+/).filter(Boolean);
}
