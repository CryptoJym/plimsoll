#!/usr/bin/env node
/**
 * REVIEW-68-r2 N3 — a faithful reconstruction of the character state machine
 * the eco-6hoxj.68 lane used to produce its comment-only digests. The original
 * file is not in this tree (it lived in that lane's directory, never committed),
 * so this is rebuilt from its description: track single-quote, double-quote and
 * template-literal state, track line and block comment state, and decide on a
 * `/` by looking at the next character. There is NO regex-literal state, which
 * is the defect.
 *
 * It exists only so `fixtures/regex-literal.ts` can demonstrate the divergence
 * against `strip-comments.mjs`. Do not use it for a digest.
 */
import fs from "node:fs";
import crypto from "node:crypto";

export function stripCommentsNaively(text) {
  let out = "";
  let inLine = false;
  let inBlock = false;
  let quote = null;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    const next = text[i + 1];
    if (inLine) {
      if (ch === "\n") {
        inLine = false;
        out += ch;
      }
      continue;
    }
    if (inBlock) {
      if (ch === "*" && next === "/") {
        inBlock = false;
        i += 1;
      }
      continue;
    }
    if (quote) {
      out += ch;
      if (ch === "\\") {
        out += next ?? "";
        i += 1;
        continue;
      }
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === "`") {
      quote = ch;
      out += ch;
      continue;
    }
    if (ch === "/" && next === "/") {
      inLine = true;
      i += 1;
      continue;
    }
    if (ch === "/" && next === "*") {
      inBlock = true;
      i += 1;
      continue;
    }
    out += ch;
  }
  return out;
}

const files = process.argv.slice(2).filter((arg) => !arg.startsWith("--"));
const emit = process.argv.includes("--emit");
for (const file of files) {
  const stripped = stripCommentsNaively(fs.readFileSync(file, "utf8"));
  if (emit) process.stdout.write(stripped);
  else
    console.log(
      `${file}\n  naive  ${crypto.createHash("sha256").update(stripped.replace(/\s+/g, " ").trim()).digest("hex")}`,
    );
}
