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
 *
 * Issue #199 hardening:
 *
 * 5. (P2) Namespace imports bind the whole module surface, so computed
 *    member access like `(deep as any)["build" + "Capacity" + "Plan"]`
 *    dodges every identifier scan. Bracket/computed access on a namespace
 *    import bound to a capacity-reaching module is treated as consumption,
 *    and a decision surface taking ANY namespace import of a capacity-
 *    reaching module is flagged conservatively (this may flag benign
 *    namespace use — accepted deliberately, see NOTES.md).
 * 6. (P3) Capacity-named files are no longer exempt from scanning. The two
 *    roles a `/capacity/i` basename used to conflate are separated:
 *      - PROVIDER: the file contributes its exported declarations to the
 *        capacity symbol set.
 *      - CONSUMER: the file is scanned like every other file and offends on
 *        the same terms as every other file.
 *    A capacity-named file is BOTH. It does not offend for defining or using
 *    its OWN exports (defining a symbol is not consuming it), but it offends
 *    exactly like an ordinary file when it consumes capacity from outside
 *    itself — including when it is not a decision surface. Naming a module
 *    after capacity buys no exemption of any kind.
 * 7. (#199 tooling rule) The ONE exemption is the gate's own enforcement
 *    tooling, keyed to exact repo-relative PATHS (see ENFORCEMENT_TOOLING),
 *    never to a name pattern: the scanner does not police itself, and its
 *    proof legitimately imports and exercises capacity APIs because that is
 *    what enforcing the doctrine requires. The allowlist is a closed set of
 *    exact paths (issue #174 adds the matched-outcome capacity research
 *    guardrail proof), so no decision surface can join it by renaming itself,
 *    and a file with the same basename in any other directory is still policed.
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

/**
 * Issue #199 tooling rule — the gate's own enforcement code.
 *
 * These two files ARE the capacity doctrine gate: the scanner and the proof
 * that exercises it. They name, import and execute capacity symbols because
 * enforcing the doctrine requires handling it — the scanner does not police
 * itself. They are still walked, still parsed, and still contribute edges to
 * the graph; they are only excused from being reported as offenders.
 *
 * The key is an exact repo-relative PATH, never a name pattern. That is the
 * whole point: a `/capacity/i` basename exemption is the P3 bypass this issue
 * exists to close, so the allowlist is a closed set of exact paths that
 * nothing can join by renaming itself, and `packages/shared/src/capacity-proof.ts`
 * (same basename, different directory) is policed like any other file.
 */
const ENFORCEMENT_TOOLING: ReadonlySet<string> = new Set([
  "scripts/capacity-dependency-reachability.ts",
  "scripts/capacity-proof.ts",
  // Issue #174: the matched-outcome capacity research guardrail proof. It
  // exercises the capacity research protocol APIs for the same reason the
  // #173 proof exercises the planning APIs — enforcement requires handling.
  "scripts/capacity-research-proof.ts",
]);

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

/** Exported declaration names defined by a file (issue #199 role separation). */
function extractExportedNames(text: string): Set<string> {
  const names = new Set<string>();
  for (const match of text.matchAll(
    /\bexport\s+(?:async\s+)?(?:function|class|type|interface|const|enum)\s+([A-Za-z0-9_]+)/g,
  )) {
    names.add(match[1]!);
  }
  return names;
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

  /** Exact repo-relative path match against the enforcement-tooling allowlist. */
  const isEnforcementTooling = (file: string): boolean =>
    ENFORCEMENT_TOOLING.has(path.relative(rootDir, file).split(path.sep).join("/"));

  // PROVIDER role: capacity-named files supply the doctrine symbol set. The
  // gate's own tooling is excluded — `scanCapacityDoctrine` and the report
  // types are enforcement machinery, not capacity doctrine facts, and letting
  // them seed the symbol set makes every reader of the scanner look like a
  // capacity consumer.
  const capacityModules = new Set(
    files.filter(
      (file) => /capacity/i.test(path.basename(file)) && !isEnforcementTooling(file),
    ),
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
  // Issue #199 (P3): capacity-named files are parsed like any other file so
  // they are scannable consumers; only their OFFENSE semantics differ below.
  const deps = new Map<string, string[]>();
  const textByFile = new Map<string, string>();
  const exportedNamesByFile = new Map<string, Set<string>>();
  for (const file of files) {
    const text = fs.readFileSync(file, "utf8");
    textByFile.set(file, text);
    exportedNamesByFile.set(file, extractExportedNames(text));
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
  //
  // Issue #199 (P2): namespace imports are handled separately below — they
  // bind the whole module surface, so identifier scans cannot see computed
  // member access on them.
  const consumesCapacitySymbols = new Set<string>();
  // Issue #199 (P2): files performing bracket/computed access on a namespace
  // import bound to a capacity-reaching module.
  const obfuscatedNamespaceAccess = new Set<string>();
  // Issue #199 (P2): files USING any namespace binding into a
  // capacity-reaching module (superset of the bracket-access shape).
  const namespaceConsumers = new Set<string>();
  // Issue #199 (P2): decision surfaces taking any namespace import of a
  // capacity-reaching module (conservative flag, may hit benign use).
  const namespaceImportingDecisionSurfaces = new Set<string>();
  for (const [file, text] of textByFile) {
    // Issue #199 (P3) role separation: a capacity-named file only offends
    // when it consumes capacity from OUTSIDE itself. The declarations it
    // exports are its own sanctioned surface, not consumption.
    const ownExports =
      capacityModules.has(file) ? exportedNamesByFile.get(file)! : new Set<string>();
    const external = (symbol: string): boolean => !ownExports.has(symbol);

    const importsCapacityBySpecifier = capacityModules.has(file)
      ? (deps.get(file) ?? []).some((dep) => capacityModules.has(dep))
      : /import\s+[^;'"]*from\s*["'][^"']*capacity["']/.test(text) ||
        /import\s*\(\s*["'][^"']*capacity/.test(text) ||
        /(?:^|\n)\s*import\s+["'][^"']*capacity["']/.test(text);
    const namedImports = extractNamedImports(text).filter(external);
    const importsCapacitySymbol = namedImports.some((name) => capacitySymbols.has(name));
    const usesCapacityIdentifier = [...capacitySymbols]
      .filter(external)
      .some((symbol) => new RegExp(`\\b${symbol}\\b`).test(text));
    if (importsCapacityBySpecifier || importsCapacitySymbol || usesCapacityIdentifier) {
      consumesCapacitySymbols.add(file);
    }

    // Issue #199 (P2): namespace-import handling. `import * as ns from X`
    // where X is a capacity module or transitively reaches one.
    for (const match of text.matchAll(
      /import\s*\*\s*as\s+([A-Za-z0-9_]+)\s+from\s+["']([^"']+)["']/g,
    )) {
      const binding = match[1]!;
      const target = resolveSpecifier(file, match[2]!);
      if (target === null || !fileSet.has(target)) continue;
      const targetReachesCapacity =
        capacityModules.has(target) ||
        computeReachable(target, new Set()) === true;
      if (!targetReachesCapacity) continue;
      // CONSERVATIVE (issue #199 acceptance a/b): the namespace binding
      // exposes every capacity export, and computed access can be hidden
      // behind arbitrary casts (`ns as any`, `ns as unknown as ...`). Any
      // USE of the binding outside its own import statement therefore
      // counts as consumption; this may flag benign namespace use.
      const body = text.replace(
        new RegExp(
          `import\\s*\\*\\s*as\\s+${binding}\\s*from\\s*["'][^"']+["'];?`,
          "g",
        ),
        "",
      );
      if (new RegExp(`\\b${binding}\\b`).test(body)) {
        namespaceConsumers.add(file);
        // Bracket/computed member access is the known exploit shape.
        if (new RegExp(`\\b${binding}\\s*(?:\\]|\\[)`).test(body)) {
          obfuscatedNamespaceAccess.add(file);
        }
      }
      // Conservative rule: a decision surface must not take a namespace
      // import of a capacity-reaching module at all, used or not.
      const fileIsDecisionSurface =
        DECISION_SURFACE_NAME_PATTERN.test(path.basename(file)) ||
        DECISION_SURFACE_DECLARATION_PATTERN.test(text) ||
        DECISION_MODULE_IMPORT_PATTERN.test(text);
      if (fileIsDecisionSurface) namespaceImportingDecisionSurfaces.add(file);
    }
  }

  // Issue #199 (P2): namespace consumers join the direct-consumption set.
  for (const file of namespaceConsumers) consumesCapacitySymbols.add(file);

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
    const directConsumption =
      consumesCapacitySymbols.has(file) || namespaceImportingDecisionSurfaces.has(file);
    const transitivelyConsumes = transitiveConsumers.has(file);
    if (!directConsumption && !transitivelyConsumes) continue;

    const isDecisionSurface =
      DECISION_SURFACE_NAME_PATTERN.test(path.basename(file)) ||
      DECISION_SURFACE_DECLARATION_PATTERN.test(text) ||
      DECISION_MODULE_IMPORT_PATTERN.test(text);

    // Issue #199 tooling rule: the scanner does not police itself. This is
    // the ONLY offense exemption, and it is keyed to an exact repo-relative
    // path — NOT to the `/capacity/i` basename, which would re-open the very
    // P3 bypass this issue closes. A capacity-named file that is not this
    // gate's own tooling offends on exactly the same terms as any other file,
    // decision surface or not.
    if (isEnforcementTooling(file)) continue;

    const reasons: string[] = [];
    if (isDecisionSurface) {
      if (namespaceImportingDecisionSurfaces.has(file)) {
        reasons.push("decision_surface_namespace_imports_capacity_reachable_module");
      } else if (consumesCapacitySymbols.has(file)) {
        reasons.push("decision_surface_consumes_capacity_symbols");
      } else if (transitivelyConsumes) {
        reasons.push("decision_surface_transitively_consumes_capacity");
      }
    } else if (consumesCapacitySymbols.has(file)) {
      reasons.push(
        obfuscatedNamespaceAccess.has(file)
          ? "obfuscated_namespace_access_reaches_capacity_symbols"
          : namespaceConsumers.has(file)
            ? "namespace_import_of_capacity_module_consumed"
            : "capacity_symbol_consumption_outside_capacity_modules",
      );
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
