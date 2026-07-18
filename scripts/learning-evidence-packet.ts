#!/usr/bin/env node
/** Offline CLI for one bounded learning evidence review artifact. */
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

import {
  assertLearningReviewOutputPath,
  compileLearningEvidencePacket,
  type LearningEvidenceManifest,
} from "../packages/shared/src/index";

type Args = {
  input: string;
  output: string;
  previous: string | null;
};

function usage(): string {
  return [
    "Usage: pnpm learning:evidence -- --input <manifest.json> --out <evidence/packet.json> [--previous <packet.json>]",
    "",
    "The command is offline and writes one review-only JSON artifact inside the current workspace.",
    "It never writes skills, memory, installed devices, or remote services.",
  ].join("\n");
}

function parseArgs(argv: readonly string[]): Args {
  let input: string | null = null;
  let output: string | null = null;
  let previous: string | null = null;
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--help" || argument === "-h") {
      console.log(usage());
      process.exit(0);
    }
    const value = argv[index + 1];
    if (argument === "--input" || argument === "--out" || argument === "--previous") {
      if (!value || value.startsWith("--")) throw new Error(`${argument} requires a path`);
      if (argument === "--input") input = value;
      if (argument === "--out") output = value;
      if (argument === "--previous") previous = value;
      index += 1;
      continue;
    }
    throw new Error(`unsupported argument: ${argument}`);
  }
  if (!input || !output) throw new Error("--input and --out are required");
  return { input, output, previous };
}

function parseJson(path: string): unknown {
  return JSON.parse(readFileSync(path, "utf8")) as unknown;
}

function previousFingerprint(path: string | null): string | null {
  if (!path || !existsSync(path)) return null;
  const value = parseJson(path);
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`previous packet ${path} must be a JSON object`);
  }
  const fingerprint = (value as Record<string, unknown>).sourceFingerprint;
  if (typeof fingerprint !== "string") throw new Error(`previous packet ${path} has no sourceFingerprint`);
  return fingerprint;
}

function writeAtomic(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  const temporary = `${path}.tmp-${process.pid}`;
  try {
    writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
    renameSync(temporary, path);
  } finally {
    rmSync(temporary, { force: true });
  }
}

function main(): void {
  const args = parseArgs(process.argv.slice(2));
  const output = assertLearningReviewOutputPath(args.output);
  const existing = args.previous ?? (existsSync(output) ? output : null);
  const manifest = parseJson(args.input) as LearningEvidenceManifest;
  const run = compileLearningEvidencePacket(manifest, {
    previousSourceFingerprint: previousFingerprint(existing),
  });
  if (run.status === "computed") writeAtomic(output, run.packet);
  console.log(JSON.stringify({
    status: run.status,
    sourceFingerprint: run.sourceFingerprint,
    analysisWorkUnits: run.analysisWorkUnits,
    output,
    outputWritten: run.status === "computed",
  }));
}

main();
