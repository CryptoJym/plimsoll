#!/usr/bin/env node
/**
 * REVIEW-68-r2 N3 — comment stripping that a regex literal cannot fool.
 *
 * This replaces the hand-rolled character state machine the eco-6hoxj.68 lane
 * shipped as `BUILDER-EVIDENCE/strip-comments.mjs`. That machine tracked string
 * and comment state by scanning characters and had no regex-literal state, so a
 * `/` that opens a REGEX whose body contains `//` or `/*` — `/https:\/\/x/`,
 * `/\/\*.*\*\//` — put it into comment state and it swallowed real code to the
 * end of the line (or to the next `*​/`). Any "this change is comment-only"
 * digest it produced was therefore unreliable in exactly the files most likely
 * to hold a URL or path regex. `strip-comments-naive.mjs` beside this file is a
 * faithful reconstruction of that machine (the original is not in this tree),
 * kept only so the defect can be reproduced; `fixtures/regex-literal.ts` is the
 * counterexample, and `comment-strip-digests.txt` records the divergence.
 *
 * The replacement does not scan characters at all. It hands the file to the
 * TypeScript compiler's own parser, which is the only thing in the room that
 * knows whether a `/` opens a division or a regular expression, and takes the
 * answer from the resulting tree two independent ways:
 *
 *   code   — `ts.createPrinter({ removeComments: true }).printFile(ast)`.
 *            Re-emitted from the AST, so comments are gone and formatting is
 *            normalized: a pure re-indent reads as "no code change" too.
 *   spans  — the original text with every comment RANGE cut out, the ranges
 *            taken from `ts.getLeadingCommentRanges` / `getTrailingCommentRanges`
 *            at positions the parser identified as trivia. Keeps the original
 *            formatting, so it is the stricter of the two.
 *
 * Both are reported. They answer the same question from different directions;
 * a file whose two verdicts disagree is a file to look at by hand.
 *
 * Usage:
 *   node BUILDER-EVIDENCE/strip-comments.mjs <file.ts> [...]        # digests
 *   node BUILDER-EVIDENCE/strip-comments.mjs --emit=code <file.ts>  # stripped text
 *   node BUILDER-EVIDENCE/strip-comments.mjs --emit=spans <file.ts>
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const ts = require("typescript");

const scriptKindFor = (file) => {
  const ext = path.extname(file).toLowerCase();
  if (ext === ".tsx") return ts.ScriptKind.TSX;
  if (ext === ".jsx") return ts.ScriptKind.JSX;
  if (ext === ".mjs" || ext === ".cjs" || ext === ".js") return ts.ScriptKind.JS;
  return ts.ScriptKind.TS;
};

const parse = (file, text) =>
  ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, scriptKindFor(file));

/** Comments gone because the code was re-emitted from the parsed tree. */
export function strippedByPrinter(file, text) {
  const printer = ts.createPrinter({ removeComments: true, newLine: ts.NewLineKind.LineFeed });
  return printer.printFile(parse(file, text));
}

/** Comments gone because their exact source ranges were cut out. */
export function strippedBySpans(file, text) {
  const source = parse(file, text);
  const ranges = [];
  const collect = (pos) => {
    for (const range of ts.getLeadingCommentRanges(text, pos) ?? []) ranges.push(range);
    for (const range of ts.getTrailingCommentRanges(text, pos) ?? []) ranges.push(range);
  };
  const walk = (node) => {
    if (node.getFullStart() !== node.getStart(source, true) || node.getFullStart() === 0) {
      collect(node.getFullStart());
    }
    collect(node.getEnd());
    node.forEachChild(walk);
  };
  walk(source);
  // The trailing trivia after the last token, and the file-leading trivia, are
  // reached by the two collects above; de-duplicate and cut back to front.
  const unique = [...new Map(ranges.map((range) => [`${range.pos}:${range.end}`, range])).values()]
    .sort((a, b) => b.pos - a.pos);
  let out = text;
  for (const range of unique) out = `${out.slice(0, range.pos)}${out.slice(range.end)}`;
  return out;
}

const sha = (value) => crypto.createHash("sha256").update(value).digest("hex");
/** Whitespace is not code: normalize it so a re-wrap is not a code change. */
const normalized = (value) => value.replace(/\s+/g, " ").trim();

export function digestsFor(file) {
  const text = fs.readFileSync(file, "utf8");
  const code = strippedByPrinter(file, text);
  const spans = strippedBySpans(file, text);
  return {
    file,
    bytes: text.length,
    rawSha256: sha(text),
    codeSha256: sha(normalized(code)),
    spansSha256: sha(normalized(spans)),
  };
}

const args = process.argv.slice(2);
const emit = args.find((arg) => arg.startsWith("--emit="))?.slice("--emit=".length);
const files = args.filter((arg) => !arg.startsWith("--"));
if (files.length === 0) {
  console.error("usage: strip-comments.mjs [--emit=code|spans] <file> [...]");
  process.exit(2);
}
if (emit) {
  for (const file of files) {
    const text = fs.readFileSync(file, "utf8");
    process.stdout.write(emit === "spans" ? strippedBySpans(file, text) : strippedByPrinter(file, text));
  }
} else {
  for (const file of files) {
    const digest = digestsFor(file);
    console.log(
      `${digest.file}\n  raw    ${digest.rawSha256}\n  code   ${digest.codeSha256}  (printer, removeComments)\n  spans  ${digest.spansSha256}  (comment ranges cut)`,
    );
  }
}
