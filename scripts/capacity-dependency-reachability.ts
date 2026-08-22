/**
 * Reachability-based capacity doctrine scanner (issue #195).
 *
 * The previous gate matched import specifiers literally ending in
 * "capacity", so `import { buildCapacityPlan } from "./index"` through the
 * shared barrel left the proof green while making the bypass the DEFAULT
 * import style. This scanner resolves REACHABILITY instead of specifier
 * spelling:
 *
 * 1. It parses every relative import / re-export in each TypeScript source
 *    file and resolves specifiers to concrete files, so barrel and index
 *    re-export chains are followed.
 * 2. It computes which files can transitively reach a capacity-defining
 *    module.
 * 3. It detects decision surfaces by CONTENT (exported declarations with
 *    routing/coaching/ranking/compensation/discipline/intervention/verdict/
 *    scoring verbs, or imports of known decision-adjacent modules), not just
 *    by file path — a decision surface with an innocuous filename cannot
 *    escape by renaming.
 * 4. A file offends when it reaches capacity AND actually consumes a
 *    capacity-defined symbol (named import or identifier usage), or imports
 *    a capacity module directly by specifier.
 */

import fs from "node:fs";
import path from "node:path";

export type CapacityReachabilityOffense = {
  file: string;
  reasons: string[];
};

export type CapacityReachabilityReport = {
  scannedFiles: string[];
  capacityModules: string[];
  filesReachingCapacity: string[];
  offendingImporters: CapacityReachabilityOffense[];
};

/** Decision-surface signal in a FILE NAME (kept from the original gate). */
const DECISION_SURFACE_NAME_PATTERN =
  /(?:rout|coach|rank|ranking|compensat|disciplin|intervent|verdict|score|scoring|performance[-_.]?layer|metric[-_.]?registry)/i;

/**
 * Decision-surface signal in file CONTENT: an exported declaration whose
 * name carries a forbidden verb. Anchored to declarations so prose comments
 * cannot trigger it.
 */
const DECISION_SURFACE_DECLARATION_PATTERN =
  /\bexport\s+(?:async\s+)?(?:function|class|type|interface|const)\s+[A-Za-z0-9_]*(?:Route|route|Coach|coach|Rank|rank|Compensat|compensat|Disciplin|disciplin|Intervent|intervent|Verdict|verdict|Score|score|Scoring|scoring)[A-Za-z0-9_]*/;

/** Imports of known decision-adjacent modules are themselves a signal. */
const DECISION_MODULE_IMPORT_PATTERN =
  /from\s+["'][^"']*(?:performance-layer|metric-registry|learning-evidence)(?:\.js)?["']/;

function collectTypeScriptFiles(rootDir: string): string[] {
  const files: string[] = [];
  const walk = (dir: string) => {
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (entry.name === "node_modules" || entry.name.startsWith(".")) continue;
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.name.endsWith(".ts")) files.push(full);
    }
  };
  for (const dir of ["packages", "scripts"]) walk(path.join(rootDir, dir));
  return files.sort();
}

function extractRelativeSpecifiers(text: string): string[] {
  const specs = new Set<string>();
  for (const match of text.matchAll(/from\s+["']([^"']+)["']/g)) specs.add(match[1]!);
  for (const match of text.matchAll(/import\s*\(\s*["']([^"']+)["']\s*\)/g)) specs.add(match[1]!);
  for (const match of text.matchAll(/(?:^|\n)\s*import\s+["']([^"']+)["']/g)) specs.add(match[1]!);
  return [...specs].filter((spec) => spec.startsWith("."));
}

function resolveSpecifier(fromFile: string, specifier: string): string | null {
  const base = path.resolve(path.dirname(fromFile), specifier);
  for (const candidate of [base, `${base}.ts`, path.join(base, "index.ts")]) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) return candidate;
  }
  return null;
}

function extractNamedImports(text: string): string[] {
  const names: string[] = [];
  // import { a, b as c } from "..."
  for (const statement of text.matchAll(/import\s*\{([^}]*)\}\s*from/g)) {
    for (const piece of statement[1]!.split(",")) {
      const name = piece.split(/\s+as\s+/)[0]!.trim();
      if (name.length > 0) names.push(name);
    }
  }
  // export { a, b } from "..." re-exports count as consumption too
  for (const statement of text.matchAll(/export\s*\{([^}]*)\}\s*from/g)) {
    for (const piece of statement[1]!.split(",")) {
      const name = piece.split(/\s+as\s+/)[0]!.trim();
      if (name.length > 0) names.push(name);
    }
  }
  return names;
}

export function scanCapacityDoctrine(rootDir: string): CapacityReachabilityReport {
  const files = collectTypeScriptFiles(rootDir);
  const fileSet = new Set(files);

  const capacityModules = new Set(
    files.filter((file) => /capacity/i.test(path.basename(file))),
  );

  // Exported symbol names defined by capacity modules (content-derived).
  const capacitySymbols = new Set<string>();
  for (const file of capacityModules) {
    const text = fs.readFileSync(file, "utf8");
    for (const match of text.matchAll(
      /\bexport\s+(?:async\s+)?(?:function|class|type|interface|const|enum)\s+([A-Za-z0-9_]+)/g,
    )) {
      capacitySymbols.add(match[1]!);
    }
  }

  // Resolve every relative edge; keep only edges that land inside the tree.
  const deps = new Map<string, string[]>();
  const textByFile = new Map<string, string>();
  for (const file of files) {
    if (capacityModules.has(file)) continue; // capacity internals are out of scope
    const text = fs.readFileSync(file, "utf8");
    textByFile.set(file, text);
    const resolved: string[] = [];
    for (const spec of extractRelativeSpecifiers(text)) {
      const target = resolveSpecifier(file, spec);
      if (target !== null && fileSet.has(target) && target !== file) resolved.push(target);
    }
    deps.set(file, resolved);
  }

  // Transitive reachability of any capacity module from each file.
  const reachesCapacity = new Map<string, boolean>();
  const visiting = new Set<string>();
  const computeReachable = (file: string, seen: Set<string>): boolean => {
    if (reachesCapacity.has(file)) return reachesCapacity.get(file)!;
    if (visiting.has(file)) return false; // cycle guard
    visiting.add(file);
    seen.add(file);
    let reachable = false;
    for (const dep of deps.get(file) ?? []) {
      if (capacityModules.has(dep) || computeReachable(dep, seen)) {
        reachable = true;
        break;
      }
    }
    visiting.delete(file);
    reachesCapacity.set(file, reachable);
    return reachable;
  };
  for (const file of deps.keys()) computeReachable(file, new Set());

  const filesReachingCapacity = [...deps.keys()].filter(
    (file) => reachesCapacity.get(file) === true,
  );

  // Tier 0: files that CONSUME capacity symbols — a named import (through
  // any specifier spelling, barrels included), any identifier reference to a
  // capacity-defined symbol, or a true import form of a capacity module.
  // A pure re-export (`export * from "./capacity"`) is the sanctioned
  // distribution surface, not consumption.
  const consumesCapacitySymbols = new Set<string>();
  for (const [file, text] of textByFile) {
    const importsCapacityBySpecifier =
      /import\s+[^;'"]*from\s*["'][^"']*capacity["']/.test(text) ||
      /import\s*\(\s*["'][^"']*capacity/.test(text) ||
      /(?:^|\n)\s*import\s+["'][^"']*capacity["']/.test(text);
    const namedImports = extractNamedImports(text);
    const importsCapacitySymbol = namedImports.some((name) => capacitySymbols.has(name));
    const usesCapacityIdentifier = [...capacitySymbols].some((symbol) =>
      new RegExp(`\\b${symbol}\\b`).test(text),
    );
    if (importsCapacityBySpecifier || importsCapacitySymbol || usesCapacityIdentifier) {
      consumesCapacitySymbols.add(file);
    }
  }

  // Tier 1: consumption propagates through real module edges. If file A
  // imports file B and B consumes capacity, then capacity values can flow
  // into A even when A never spells a capacity symbol. Edges through PURE
  // RE-EXPORT BARRELS do not propagate: the barrel is the sanctioned export
  // surface, and importing an unrelated helper from it must not condemn the
  // importer (that would outlaw the barrel entirely instead of the bypass).
  const isPureReExportBarrel = (file: string): boolean => {
    if (!textByFile.has(file)) return false;
    const lines = textByFile.get(file)!.split("\n");
    let inBlockComment = false;
    for (const line of lines) {
      const current = line.trim();
      if (inBlockComment) {
        if (current.includes("*/")) inBlockComment = false;
        continue;
      }
      if (current.startsWith("/*")) {
        if (!current.includes("*/")) inBlockComment = true;
        continue;
      }
      if (current.length === 0 || current.startsWith("//")) continue;
      if (!/^export\s+(?:\*|\{)/.test(current)) return false;
    }
    return true;
  };
  const transitiveConsumers = new Set<string>(consumesCapacitySymbols);
  let changed = true;
  while (changed) {
    changed = false;
    for (const [file, fileDeps] of deps) {
      if (transitiveConsumers.has(file)) continue;
      const infected = fileDeps.some(
        (dep) => transitiveConsumers.has(dep) && !isPureReExportBarrel(dep),
      );
      if (infected) {
        transitiveConsumers.add(file);
        changed = true;
      }
    }
  }

  const offendingImporters: CapacityReachabilityOffense[] = [];
  for (const [file, text] of textByFile) {
    const directConsumption = consumesCapacitySymbols.has(file);
    const transitivelyConsumes = transitiveConsumers.has(file);
    if (!directConsumption && !transitivelyConsumes) continue;

    const isDecisionSurface =
      DECISION_SURFACE_NAME_PATTERN.test(path.basename(file)) ||
      DECISION_SURFACE_DECLARATION_PATTERN.test(text) ||
      DECISION_MODULE_IMPORT_PATTERN.test(text);

    const reasons: string[] = [];
    if (directConsumption && isDecisionSurface) {
      reasons.push("decision_surface_consumes_capacity_symbols");
    } else if (directConsumption) {
      reasons.push("capacity_symbol_consumption_outside_capacity_modules");
    } else if (transitivelyConsumes && isDecisionSurface) {
      reasons.push("decision_surface_transitively_consumes_capacity");
    }

    if (reasons.length > 0) {
      offendingImporters.push({ file, reasons });
    }
  }

  offendingImporters.sort((left, right) => left.file.localeCompare(right.file));

  return {
    scannedFiles: files,
    capacityModules: [...capacityModules].sort(),
    filesReachingCapacity: filesReachingCapacity.sort(),
    offendingImporters,
  };
}
