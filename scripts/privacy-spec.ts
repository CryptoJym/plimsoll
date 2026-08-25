/**
 * Issue 0013 — generates docs/privacy-spec.md ("what leaves the machine")
 * from the actual source lists so the page cannot drift from the code.
 *
 * Derived inputs (imported, not copied):
 *   - forbiddenRawContentFieldNames   packages/shared/src/schemas.ts
 *   - rawContentCategorySchema        packages/shared/src/schemas.ts
 *   - aiWorkIngestBatchSchema         packages/shared/src/schemas.ts
 *   - protectedMetadataFieldNames     packages/shared/src/policy.ts
 *   - hashProtectedValue, DEFAULT_POLICY packages/shared/src/policy.ts
 *
 * Every behavioral claim in the rendered page points at a named sentinel
 * check; this script verifies each referenced check name still exists in
 * scripts/**\/*.ts and refuses to render otherwise.
 *
 * Usage:
 *   pnpm docs:privacy             regenerate docs/privacy-spec.md
 *   pnpm docs:privacy --check     exit 1 if the committed page is stale
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  aiWorkIngestBatchSchema,
  forbiddenRawContentFieldNames,
  rawContentCategorySchema,
} from "../packages/shared/src/schemas";
import {
  DEFAULT_POLICY,
  hashProtectedValue,
  isProtectedMetadataFieldName,
  protectedMetadataFieldNames,
} from "../packages/shared/src/policy";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const docPath = path.join(repoRoot, "docs", "privacy-spec.md");
const scriptsDir = path.join(repoRoot, "scripts");
const SOURCE_SCHEMAS = "packages/shared/src/schemas.ts";
const SOURCE_POLICY = "packages/shared/src/policy.ts";

type FieldNote = {
  name: string;
  type: string;
  optional: boolean;
  arrayBounds?: { min?: number; max?: number };
};

export type PrivacySpecModel = {
  policyVersion: string;
  policyDataMode: string;
  suppressedBeforeLocalWrite: string[];
  admissibleEvidenceCategories: string[];
  neverCollectibleAnyMode: string[];
  hashedKeys: string[];
  hashedExample: { input: string; output: string };
  batchEnvelopeFields: FieldNote[];
  eventFields: FieldNote[];
  suppressedReceiptFieldName: string;
};

type ProofCheckRef = {
  checks: string[];
};

/**
 * Named sentinel checks that enforce each section. Names are verified
 * against the proof sources at render time (verifyProofChecks) and again by
 * scripts/privacy-spec-proof.ts, so a renamed or deleted check breaks the
 * build instead of leaving a dangling promise on the public trust page.
 */
export const PROOF_CHECKS: Record<string, ProofCheckRef> = {
  suppression: {
    checks: [
      "raw_command_and_path_suppressed",
      "rollout_content_never_persisted",
      "transcript_rescan_idempotent_and_content_free",
      "history_upload_bodies_stay_metadata_only",
      "raw_prompt_command_args_stack_path_content_provider_secret_and_pii_never_reach_surfaces",
      "terminal_privacy_disposition_is_zero_across_every_export_and_dashboard_lane",
      "metadata_privacy_sentinels_absent_from_raw_delivery_request_receipt_status",
    ],
  },
  never_collected: {
    checks: [
      "managed_config_write_rejects_evidence_before_filesystem_write",
      "environment_enable_attempts_fail_before_config_or_ledger_write",
      "cli_evidence_enable_attempt_has_no_silent_downgrade",
      "start_restart_setup_and_join_reject_evidence_before_side_effects",
      "legacy_evidence_rows_are_quarantined_and_never_uploaded",
      "explicit_history_upload_remains_distinct_and_not_invoked_by_join",
    ],
  },
  hashed: {
    checks: [
      "account_email_never_uploaded",
      "machine_and_account_label_never_uploaded",
      "repo_label_never_uploaded",
      "repo_labels_disclose_slugs_never_urls",
      "repo_label_shared_schema_requires_canonical_linkage",
      "session_sync_shared_boundary_blocks_raw_ids_values_and_linkage",
      "outcomes_linkage_boundary_omits_invalid_and_normalizes_exact_hashes",
    ],
  },
  plain_envelope: {
    checks: [
      "history_batches_obey_ingest_contract",
      "history_shared_sealer_blocks_value_bypass_and_omits_unknowns",
      "no_mark_uses_same_sealed_envelope_privacy_boundary",
      "allowed_metadata_and_top_level_string_value_matrix_fails_closed",
      "upload_watermark_drains",
    ],
  },
  receipts: {
    checks: [
      "suppression_receipt_private_values_absent_from_closed_ledger_artifacts",
      "suppression_receipts_survive_capture_reopen_seal_and_upload_with_exact_parity",
      "proof_receipt_contains_no_private_sentinel",
    ],
  },
};

function normalizeFieldName(field: string) {
  return field.replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
}

function zodArrayBounds(schema: unknown): { min?: number; max?: number } | undefined {
  const checks = (schema as { _def?: { checks?: Array<{ _zod?: { def?: Record<string, unknown> } }> } })
    ._def?.checks;
  if (!checks) return undefined;
  const bounds: { min?: number; max?: number } = {};
  for (const check of checks) {
    const def = check._zod?.def;
    if (!def) continue;
    if (def.check === "min_length" && typeof def.minimum === "number") bounds.min = def.minimum;
    if (def.check === "max_length" && typeof def.maximum === "number") bounds.max = def.maximum;
  }
  return Object.keys(bounds).length > 0 ? bounds : undefined;
}

function describeZod(schema: any): { type: string; optional: boolean; arrayBounds?: { min?: number; max?: number } } {
  let optional = false;
  let current = schema;
  while (current?._def?.type === "optional" || current?._def?.type === "default") {
    // Both wrappers mean the field may be omitted on input ("default"
    // additionally supplies a value when omitted).
    optional = true;
    current = current._def.innerType;
  }
  const type = current?._def?.type as string | undefined;
  switch (type) {
    case "enum": {
      const options: readonly string[] = current.options ?? current._def.values ?? [];
      return { type: `enum(${options.join(" | ")})`, optional };
    }
    case "record": {
      const keyType = describeZod(current._def.keyType).type;
      const valueType = current._def.valueType ? describeZod(current._def.valueType).type : "unknown";
      return { type: `record(${keyType}, ${valueType})`, optional };
    }
    case "array": {
      const element = describeZod(current._def.element).type;
      return { type: `array<${element}>`, optional, arrayBounds: zodArrayBounds(current) };
    }
    case "object":
      return { type: "object", optional };
    default:
      return { type: typeof type === "string" ? type : "unknown", optional };
  }
}

function fieldNotesFromShape(shape: Record<string, any>): FieldNote[] {
  return Object.entries(shape).map(([name, schema]) => {
    const described = describeZod(schema);
    return {
      name,
      type: described.type,
      optional: described.optional,
      arrayBounds: described.arrayBounds,
    };
  });
}

export function collectPrivacySpecModel(): PrivacySpecModel {
  const admissibleEvidenceCategories = [...(rawContentCategorySchema as any).options as string[]];
  const admissibleNormalized = new Set(admissibleEvidenceCategories.map(normalizeFieldName));
  const neverCollectibleAnyMode = forbiddenRawContentFieldNames.filter(
    (field) => !admissibleNormalized.has(normalizeFieldName(field)),
  );
  const eventWrapper = ((aiWorkIngestBatchSchema as any).shape.events as any).element;
  const batchEnvelopeFields = fieldNotesFromShape((aiWorkIngestBatchSchema as any).shape);

  return {
    policyVersion: DEFAULT_POLICY.version,
    policyDataMode: DEFAULT_POLICY.dataMode,
    suppressedBeforeLocalWrite: [...forbiddenRawContentFieldNames],
    admissibleEvidenceCategories,
    neverCollectibleAnyMode,
    hashedKeys: [...protectedMetadataFieldNames],
    hashedExample: {
      input: "sentinel-actor@example.test",
      output: hashProtectedValue("sentinel-actor@example.test"),
    },
    batchEnvelopeFields,
    eventFields: fieldNotesFromShape(eventWrapper.shape.event.shape),
    suppressedReceiptFieldName: Object.keys(eventWrapper.shape).find(
      (key) => key !== "event",
    ) as string,
  };
}

function listTable(items: readonly string[], sourceLabel: string): string[] {
  return [
    `| # | Field name |`,
    `|---|---|`,
    ...items.map((item, index) => `| ${index + 1} | \`${item}\` |`),
    ``,
    `Count: **${items.length}** — source: \`${sourceLabel}\`.`,
  ];
}

function formatFieldNote(note: FieldNote): string {
  const bounds = note.arrayBounds
    ? ` (length ${note.arrayBounds.min !== undefined ? `≥ ${note.arrayBounds.min}` : ""}${
        note.arrayBounds.min !== undefined && note.arrayBounds.max !== undefined ? ", " : ""
      }${note.arrayBounds.max !== undefined ? `≤ ${note.arrayBounds.max}` : ""})`
    : "";
  return `\`${note.name}\` — ${note.type}${bounds}${note.optional ? ", optional" : ", required"}`;
}

function findCheckDefinitionFiles(): Map<string, string[]> {
  const index = new Map<string, string[]>();
  const walk = (dir: string) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
        continue;
      }
      // Only actual proof scripts count as definitions; this generator and
      // its own proof reference the same literals without enforcing them.
      if (!entry.name.endsWith("-proof.ts")) continue;
      const contents = fs.readFileSync(full, "utf8");
      for (const ref of Object.values(PROOF_CHECKS)) {
        for (const name of ref.checks) {
          // A check name counts as defined where a proof registers it via
          // its check()/record()/prove() helper — not on any incidental
          // string mention.
          const registration = new RegExp(
            `\\b(?:check|record|prove)\\(\\s*\\n?\\s*"${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"`,
          );
          if (!registration.test(contents)) continue;
          const rel = path.relative(repoRoot, full);
          const existing = index.get(name) ?? [];
          if (!existing.includes(rel)) existing.push(rel);
          index.set(name, existing);
        }
      }
    }
  };
  walk(scriptsDir);
  return index;
}

export function verifyProofChecks(): Map<string, string[]> {
  const found = findCheckDefinitionFiles();
  const missing: string[] = [];
  for (const ref of Object.values(PROOF_CHECKS)) {
    for (const name of ref.checks) {
      if (!found.has(name)) missing.push(name);
    }
  }
  if (missing.length > 0) {
    throw new Error(
      `privacy-spec: referenced proof check name(s) not found in scripts/: ${missing.join(", ")}`,
    );
  }
  return found;
}

export function renderGuaranteesSection(sectionKeys: Array<keyof typeof PROOF_CHECKS>, found: Map<string, string[]>): string[] {
  const lines = [`Named sentinel checks enforcing this section:`, ``];
  for (const key of sectionKeys) {
    for (const name of PROOF_CHECKS[key].checks) {
      const files = found.get(name) ?? [];
      lines.push(`- \`${name}\` — ${files.map((file) => `\`${file}\``).join(", ")}`);
    }
  }
  lines.push(``);
  return lines;
}

export function renderPrivacySpec(model: PrivacySpecModel): string {
  const found = verifyProofChecks();
  const lines: string[] = [];

  lines.push(`<!-- AUTO-GENERATED by scripts/privacy-spec.ts — do not edit by hand.`);
  lines.push(`     Regenerate with \`pnpm docs:privacy\`; CI diff-checks freshness with \`pnpm docs:privacy --check\`.`);
  lines.push(`     Every list below is imported at generation time from the named source export.`);
  lines.push(`     Every behavioral claim links to a named sentinel proof check whose presence is`);
  lines.push(`     verified at render time. -->`);
  lines.push(``);
  lines.push(`# Privacy spec — what leaves the machine`);
  lines.push(``);
  lines.push(`This page is generated from the code that enforces it, not written beside it.`);
  lines.push(`The shipped collector runs data mode \`${model.policyDataMode}\``);
  lines.push(`(\`DEFAULT_POLICY.version = ${model.policyVersion}\`, \`${SOURCE_POLICY}\`).`);
  lines.push(`Managed or upload-enabled installs are locked to that mode; see`);
  lines.push(`[ADR-0004](architecture/0004-managed-metadata-only-privacy.md).`);
  lines.push(``);
  lines.push(`## The four buckets`);
  lines.push(``);
  lines.push(`| Bucket | Rule (derived, mechanical) | Source of truth |`);
  lines.push(`|---|---|---|`);
  lines.push(`| Suppressed before local write | Any captured payload key matching \`forbiddenRawContentFieldNames\` is removed before the local database write in every supported mode; only its field name survives as a receipt. | \`forbiddenRawContentFieldNames\` in \`${SOURCE_SCHEMAS}\` |`);
  lines.push(`| Never collected — no mode | Forbidden field names outside \`rawContentCategorySchema\`: even a hypothetical evidence-mode policy admits only the enum's categories via \`evidence.allowedCategories\`, so nothing outside them can ever be admitted. | computed: \`forbiddenRawContentFieldNames\\rawContentCategorySchema\` |`);
  lines.push(`| Collected hashed | Captured payload keys matching \`protectedMetadataFieldNames\` have their value replaced by a truncated SHA-256 (\`sha256:\` + 16 hex chars) before persistence. | \`protectedMetadataFieldNames\`, \`hashProtectedValue\` in \`${SOURCE_POLICY}\` |`);
  lines.push(`| Collected plain (typed envelope) | The typed fields of the upload payload schema cross the boundary as-is, validated by the outbound sealer. | \`aiWorkIngestBatchSchema\`, \`aiInteractionEventSchema\` in \`${SOURCE_SCHEMAS}\` |`);
  lines.push(``);
  lines.push(`## Suppressed before anything is stored (${model.policyDataMode} mode)`);
  lines.push(``);
  lines.push(`These raw-content field names are removed at capture time — before the local`);
  lines.push(`database write — by \`sanitizeForPolicy\` / \`evaluatePolicyInput\``);
  lines.push(`(\`${SOURCE_POLICY}\`). Removals emit suppression receipts carrying field`);
  lines.push(`names only, never values.`);
  lines.push(``);
  lines.push(...listTable(model.suppressedBeforeLocalWrite, `${SOURCE_SCHEMAS} :: forbiddenRawContentFieldNames`));
  lines.push(...renderGuaranteesSection(["suppression", "receipts"], found));
  lines.push(`## Never collected — not admittable in any mode`);
  lines.push(``);
  lines.push(`Computed as the forbidden names that are not among the`);
  lines.push(`\`rawContentCategorySchema\` categories (${model.admissibleEvidenceCategories.map((c) => `\`${c}\``).join(", ")})`);
  lines.push(`— the only values \`policyConfigSchema.evidence.allowedCategories\` accepts.`);
  lines.push(`No implemented or planned policy input can admit these keys, and evidence`);
  lines.push(`mode itself is rejected before config write on managed installs with no`);
  lines.push(`evidence vault implemented (ADR-0004).`);
  lines.push(``);
  lines.push(...listTable(model.neverCollectibleAnyMode, `computed from forbiddenRawContentFieldNames minus rawContentCategorySchema`));
  lines.push(...renderGuaranteesSection(["never_collected"], found));
  lines.push(`## Collected hashed`);
  lines.push(``);
  lines.push(`Any captured payload key matching one of these names has its value replaced`);
  lines.push(`by \`hashProtectedValue\`: \`sha256:\` + the first 16 hex characters of the`);
  lines.push(`SHA-256 digest. Example (deterministic):`);
  lines.push(``);
  lines.push(`    hashProtectedValue("${model.hashedExample.input}")`);
  lines.push(`    → "${model.hashedExample.output}"`);
  lines.push(``);
  lines.push(...listTable(model.hashedKeys, `${SOURCE_POLICY} :: protectedMetadataFieldNames`));
  lines.push(...renderGuaranteesSection(["hashed"], found));
  lines.push(`## Collected plain — upload envelope`);
  lines.push(``);
  lines.push(`The upload payload shape is \`aiWorkIngestBatchSchema\``);
  lines.push(`(\`${SOURCE_SCHEMAS}\`). Envelope fields:`);
  lines.push(``);
  model.batchEnvelopeFields.forEach((note, index) => {
    lines.push(`${index + 1}. ${formatFieldNote(note)}`);
  });
  lines.push(``);
  lines.push(`Each \`events[i]\` wraps an \`aiInteractionEventSchema\` object plus a`);
  lines.push(`\`${model.suppressedReceiptFieldName}\` receipt list (bounded ASCII field`);
  lines.push(`names only). Typed event fields:`);
  lines.push(``);
  model.eventFields.forEach((note, index) => {
    const collision = isProtectedMetadataFieldName(note.name)
      ? ` — normalized name also appears in \`protectedMetadataFieldNames\`; inside free-form metadata such keys are value-hashed (see *Collected hashed*), and as a typed field it crosses under the outbound identifier contract`
      : ``;
    lines.push(`${index + 1}. ${formatFieldNote(note)}${collision}.`);
  });
  lines.push(``);
  lines.push(`Free-form \`metadata\` crosses only when the outbound sealer admits each key;`);
  lines.push(`unknown keys default to local-only.`);
  lines.push(``);
  lines.push(...renderGuaranteesSection(["plain_envelope"], found));
  lines.push(`## Regeneration`);
  lines.push(``);
  lines.push(`    pnpm docs:privacy          # regenerate this page`);
  lines.push(`    pnpm docs:privacy --check  # exit 1 if stale (CI gate)`);
  lines.push(`    pnpm proof:privacy-spec    # derivation + tamper proof`);
  lines.push(``);

  return `${lines.join("\n")}`;
}

export function docIsStale(rendered: string, committed: string): { stale: boolean; firstDiffLine?: string } {
  if (rendered === committed) return { stale: false };
  const renderedLines = rendered.split("\n");
  const committedLines = committed.split("\n");
  for (let i = 0; i < Math.max(renderedLines.length, committedLines.length); i += 1) {
    if (renderedLines[i] !== committedLines[i]) {
      return {
        stale: true,
        firstDiffLine: `line ${i + 1}: expected ${JSON.stringify(renderedLines[i] ?? "<eof>")}, found ${JSON.stringify(committedLines[i] ?? "<eof>")}`,
      };
    }
  }
  return { stale: true };
}

function main() {
  const checkMode = process.argv.includes("--check");
  const rendered = renderPrivacySpec(collectPrivacySpecModel());
  if (!checkMode) {
    fs.writeFileSync(docPath, rendered);
    console.log(`wrote ${path.relative(repoRoot, docPath)} (${rendered.length} bytes)`);
    return;
  }
  const committed = fs.readFileSync(docPath, "utf8");
  const verdict = docIsStale(rendered, committed);
  if (verdict.stale) {
    console.error(`docs/privacy-spec.md is stale (${verdict.firstDiffLine}).`);
    console.error(`Run \`pnpm docs:privacy\` and commit the regenerated page.`);
    process.exit(1);
  }
  console.log(`docs/privacy-spec.md is fresh.`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main();
}
