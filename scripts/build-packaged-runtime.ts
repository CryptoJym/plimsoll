/**
 * Issue #155: build ONE self-contained collector entry artifact from the
 * approved source and record its full provenance, or fail closed with
 * nothing written. This is the single canonical builder for the packaged
 * runtime; `packages/collector-cli` `pnpm build` delegates here so exactly
 * one implementation owns the esbuild options.
 *
 * Modes:
 *   tsx scripts/build-packaged-runtime.ts            # build + write manifest
 *   tsx scripts/build-packaged-runtime.ts --verify   # recompute digests vs manifest
 *
 * Fail-closed contract: when the bundle fails (unresolvable import, scan
 * violation) or verification fails (digest mismatch, incomplete manifest),
 * the process exits nonzero and NO runtime manifest is emitted or trusted.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build, version as esbuildVersion } from "esbuild";

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const DEFAULT_REPO_ROOT = path.resolve(path.dirname(SCRIPT_PATH), "..");
const ARTIFACT_NAME = "cli.mjs";
const MANIFEST_SCHEMA = "plimsoll.packaged-runtime.v1";
const BUILD_TARGET = "node20";
const EXTERNALS = ["better-sqlite3"];

type Args = {
  repoRoot: string;
  entry?: string;
  dist?: string;
  manifest?: string;
  packageManifest?: string;
  sourceCommit?: string;
  verify: boolean;
};

function parseArgs(argv: readonly string[]): Args {
  const args: Args = { repoRoot: DEFAULT_REPO_ROOT, verify: false };
  for (let index = 2; index < argv.length; index++) {
    const value = argv[index];
    const next = () => {
      index += 1;
      const operand = argv[index];
      if (!operand) fail("missing_value", `flag ${value} requires an operand`);
      return operand;
    };
    if (value === "--verify") args.verify = true;
    else if (value === "--repo-root") args.repoRoot = path.resolve(next());
    else if (value === "--entry") args.entry = path.resolve(next());
    else if (value === "--dist") args.dist = path.resolve(next());
    else if (value === "--manifest") args.manifest = path.resolve(next());
    else if (value === "--package-manifest") args.packageManifest = path.resolve(next());
    else if (value === "--source-commit") args.sourceCommit = next();
    else fail("unknown_flag", `unknown flag ${value}`);
  }
  return args;
}

function fail(code: string, detail: string): never {
  console.error(JSON.stringify({ ok: false, code, detail }));
  process.exit(1);
}

function sha256File(file: string): { digest: string; bytes: number } {
  const content = fs.readFileSync(file);
  return {
    digest: createHash("sha256").update(content).digest("hex"),
    bytes: content.length,
  };
}

/**
 * Self-containment classification for a bundled specifier. The packaged
 * entry may only depend on node builtins, relative files it ships with,
 * absolute paths, or the declared native external.
 */
export function classifySpecifier(
  specifier: string,
  externals: readonly string[],
): "ok" | "violation" {
  if (
    specifier.startsWith("node:") ||
    specifier.startsWith(".") ||
    specifier.startsWith("/") ||
    externals.includes(specifier)
  ) {
    return "ok";
  }
  return "violation";
}

export function scanBundleSpecifiers(
  bundleText: string,
  externals: readonly string[],
): Array<{ specifier: string; kind: string }> {
  const violations: Array<{ specifier: string; kind: string }> = [];
  const patterns: Array<[RegExp, string]> = [
    [/(?:^|[\s;}])((?:import|export)\s)[^;'"]*?from\s*["']([^"']+)["']/g, "static"],
    [/(?:^|[\s;}])import\s*["']([^"']+)["']/g, "side-effect"],
    [/import\(\s*["']([^"']+)["']\s*\)/g, "dynamic"],
  ];
  for (const [pattern, kind] of patterns) {
    pattern.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(bundleText)) !== null) {
      const specifier = match[2] ?? match[1]!;
      if (classifySpecifier(specifier, externals) === "violation") {
        violations.push({ specifier, kind });
      }
    }
  }
  return violations;
}

function gitSourceCommit(repoRoot: string): { commit: string; dirty: boolean } {
  const head = spawnSync("git", ["rev-parse", "HEAD"], { cwd: repoRoot, encoding: "utf8" });
  if (head.status !== 0 || !/^[0-9a-f]{40}$/.test((head.stdout ?? "").trim())) {
    fail(
      "git_head_unavailable",
      `git rev-parse HEAD failed: ${(head.stderr ?? head.stdout ?? "").trim()}`,
    );
  }
  const status = spawnSync("git", ["status", "--porcelain"], { cwd: repoRoot, encoding: "utf8" });
  if (status.status !== 0) {
    fail("git_status_unavailable", `git status --porcelain failed: ${(status.stderr ?? "").trim()}`);
  }
  return { commit: (head.stdout ?? "").trim(), dirty: (status.stdout ?? "").trim().length > 0 };
}

function readPackageManifest(file: string): {
  name: string;
  version: string;
  enginesNode: string | null;
} {
  let parsed: unknown;
  try {
    parsed = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (error) {
    fail("package_manifest_unreadable", `${file}: ${(error as Error).message}`);
  }
  const record = parsed as { name?: unknown; version?: unknown; engines?: { node?: unknown } };
  if (typeof record.name !== "string" || typeof record.version !== "string") {
    fail("package_manifest_invalid", `${file} must carry string name and version`);
  }
  return {
    name: record.name,
    version: record.version,
    enginesNode:
      record.engines && typeof record.engines.node === "string" ? record.engines.node : null,
  };
}

export type RuntimeManifest = Record<string, any>;

export async function buildRuntime(options: {
  repoRoot: string;
  entry: string;
  dist: string;
  packageManifestPath: string;
  sourceCommitOverride?: string;
}): Promise<RuntimeManifest> {
  const { repoRoot, entry, dist, packageManifestPath } = options;
  if (!fs.existsSync(entry)) fail("entry_missing", entry);
  fs.mkdirSync(dist, { recursive: true });
  const outfile = path.join(dist, ARTIFACT_NAME);
  const manifestPath = path.join(dist, "runtime-manifest.json");
  // A stale manifest must never survive alongside a new/failed build.
  fs.rmSync(manifestPath, { force: true });

  const result = await build({
    bundle: true,
    entryPoints: [entry],
    external: [...EXTERNALS],
    format: "esm",
    outfile,
    platform: "node",
    target: BUILD_TARGET,
    logLevel: "silent",
  }).catch((error: unknown) => {
    fail("esbuild_failed", error instanceof Error ? error.message : String(error));
  });
  if (!result || result.errors.length > 0) {
    const formatted = (result?.errors ?? []).map(String).join("\n");
    fs.rmSync(outfile, { force: true });
    fail("bundle_failed", formatted);
  }

  const bundleText = fs.readFileSync(outfile, "utf8");
  const violations = scanBundleSpecifiers(bundleText, EXTERNALS);
  if (violations.length > 0) {
    fs.rmSync(outfile, { force: true });
    fail("self_containment_violation", JSON.stringify(violations.slice(0, 16)));
  }
  fs.chmodSync(outfile, 0o755);

  // Companion static assets the daemon reads next to the bundle.
  const companions: Array<Record<string, unknown>> = [];
  const dashboardSource = path.join(path.dirname(entry), "dashboard.html");
  if (fs.existsSync(dashboardSource)) {
    const destination = path.join(dist, "dashboard.html");
    fs.copyFileSync(dashboardSource, destination);
    const identity = sha256File(destination);
    companions.push({ name: path.basename(destination), sha256: identity.digest, bytes: identity.bytes });
  }

  const artifact = sha256File(outfile);
  const pkg = readPackageManifest(packageManifestPath);
  const source = options.sourceCommitOverride
    ? { commit: options.sourceCommitOverride, dirty: false }
    : gitSourceCommit(repoRoot);

  const manifest: RuntimeManifest = {
    schema: MANIFEST_SCHEMA,
    status: "built",
    artifact: { name: ARTIFACT_NAME, bytes: artifact.bytes, sha256: artifact.digest },
    companions,
    package: { name: pkg.name, version: pkg.version },
    source: { commit: source.commit, dirty: source.dirty, entrypoint: entry },
    nodeCompatibility: {
      engines: pkg.enginesNode,
      buildTarget: BUILD_TARGET,
      runtime: { node: process.versions.node },
    },
    provenance: {
      builder: path.relative(repoRoot, SCRIPT_PATH) || SCRIPT_PATH,
      bundler: { name: "esbuild", version: esbuildVersion },
      bundle: true,
      format: "esm",
      platform: `${process.platform}-${process.arch}`,
      external: [...EXTERNALS],
      builtAt: new Date().toISOString(),
    },
  };
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o644 });
  console.log(JSON.stringify({ ok: true, manifest: manifestPath, sha256: artifact.digest }));
  return manifest;
}

const REQUIRED_SCALAR_FIELDS: Array<{ read: (manifest: any) => unknown; label: string }> = [
  { read: (m) => m.schema, label: "schema" },
  { read: (m) => m.artifact?.name, label: "artifact.name" },
  { read: (m) => m.artifact?.sha256, label: "artifact.sha256" },
  { read: (m) => m.artifact?.bytes, label: "artifact.bytes" },
  { read: (m) => m.package?.name, label: "package.name" },
  { read: (m) => m.package?.version, label: "package.version" },
  { read: (m) => m.source?.commit, label: "source.commit" },
  { read: (m) => m.source?.entrypoint, label: "source.entrypoint" },
  { read: (m) => m.nodeCompatibility?.engines, label: "nodeCompatibility.engines" },
  { read: (m) => m.nodeCompatibility?.buildTarget, label: "nodeCompatibility.buildTarget" },
  { read: (m) => m.provenance?.builder, label: "provenance.builder" },
  { read: (m) => m.provenance?.bundler?.name, label: "provenance.bundler.name" },
  { read: (m) => m.provenance?.bundler?.version, label: "provenance.bundler.version" },
  { read: (m) => m.provenance?.builtAt, label: "provenance.builtAt" },
];

function verifyFailure(code: string, detail: string) {
  console.error(JSON.stringify({ ok: false, code, detail }));
  return { ok: false as const, code, detail };
}

export function verifyRuntime(manifestPath: string):
  | { ok: true; sha256: string }
  | { ok: false; code: string; detail: string } {
  let parsed: any;
  try {
    parsed = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  } catch (error) {
    return verifyFailure("manifest_unreadable", `${manifestPath}: ${(error as Error).message}`);
  }
  if (parsed?.schema !== MANIFEST_SCHEMA) {
    return verifyFailure("manifest_invalid", `schema must be ${MANIFEST_SCHEMA}`);
  }
  const missing = REQUIRED_SCALAR_FIELDS.filter(({ read }) => {
    const value = read(parsed);
    return value === undefined || value === null || value === "";
  }).map(({ label }) => label);
  if (missing.length > 0) {
    return verifyFailure("manifest_invalid", `missing fields: ${missing.join(", ")}`);
  }
  if (!/^[0-9a-f]{64}$/.test(parsed.artifact.sha256)) {
    return verifyFailure("manifest_invalid", "artifact.sha256 is not a lowercase sha-256 hex digest");
  }
  const artifactPath = path.join(path.dirname(manifestPath), parsed.artifact.name);
  if (!fs.existsSync(artifactPath)) {
    return verifyFailure("artifact_missing", artifactPath);
  }
  const observed = sha256File(artifactPath);
  if (observed.digest !== parsed.artifact.sha256) {
    return verifyFailure(
      "digest_mismatch",
      `artifact recorded ${parsed.artifact.sha256} but disk has ${observed.digest}`,
    );
  }
  for (const companion of parsed.companions ?? []) {
    const companionPath = path.join(path.dirname(manifestPath), companion.name);
    if (!fs.existsSync(companionPath)) {
      return verifyFailure("artifact_missing", companionPath);
    }
    const companionObserved = sha256File(companionPath);
    if (companionObserved.digest !== companion.sha256) {
      return verifyFailure(
        "digest_mismatch",
        `companion ${companion.name} recorded ${companion.sha256} but disk has ${companionObserved.digest}`,
      );
    }
  }
  console.log(JSON.stringify({ ok: true, manifest: manifestPath, sha256: parsed.artifact.sha256 }));
  return { ok: true, sha256: parsed.artifact.sha256 };
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.verify) {
    const manifest = args.manifest ??
      path.join(args.repoRoot, "packages", "collector-cli", "dist", "runtime-manifest.json");
    const result = verifyRuntime(manifest);
    process.exit(result.ok ? 0 : result.code === "digest_mismatch" ? 2 : 1);
  }
  await buildRuntime({
    repoRoot: args.repoRoot,
    entry: args.entry ?? path.join(args.repoRoot, "packages", "collector-cli", "src", "cli.ts"),
    dist: args.dist ?? path.join(args.repoRoot, "packages", "collector-cli", "dist"),
    packageManifestPath: args.packageManifest ??
      path.join(args.repoRoot, "packages", "collector-cli", "package.json"),
    ...(args.sourceCommit ? { sourceCommitOverride: args.sourceCommit } : {}),
  });
}

if (process.argv[1] && path.resolve(process.argv[1]) === SCRIPT_PATH) {
  void main();
}
