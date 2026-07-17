import { z } from "zod";

export const LOCAL_TENANT_ID = "00000000-0000-4000-8000-000000000001";

export const dataModeSchema = z.enum(["metadata", "event_detail", "evidence"]);
export type DataMode = z.infer<typeof dataModeSchema>;

export const toolSourceSchema = z.enum([
  "anthropic_admin",
  "anthropic_usage",
  "claude_code",
  "codex",
  "github",
  "openai_usage",
  "manual",
  "unknown",
]);
export type ToolSource = z.infer<typeof toolSourceSchema>;

export const workIntentSchema = z.enum([
  "customer",
  "project",
  "workflow",
  "internal",
  "overhead",
  "unknown",
]);
export type WorkIntent = z.infer<typeof workIntentSchema>;

export const actionClassSchema = z.enum([
  "continue",
  "validate",
  "test",
  "edit",
  "read",
  "write",
  "shell",
  "mcp",
  "browser",
  "review",
  "other",
]);
export type ActionClass = z.infer<typeof actionClassSchema>;

export const rawContentCategorySchema = z.enum([
  "prompt",
  "output",
  "tool_input",
  "tool_output",
  "command_body",
  "file_body",
  "api_body",
]);
export type RawContentCategory = z.infer<typeof rawContentCategorySchema>;

const timestampSchema = z
  .string()
  .trim()
  .min(1)
  .refine(
    (value) =>
      !Number.isNaN(Date.parse(value)) && /(?:Z|[+-]\d{2}:\d{2})$/.test(value),
    "Expected an ISO timestamp with a timezone offset.",
  );
const idSchema = z.string().trim().min(1);
const keySchema = z.string().trim().min(1);
const metadataSchema = z.record(z.string(), z.unknown()).default({});

/** Canonical privacy-preserving linkage used on every outbound boundary.
 * Uppercase hexadecimal input is accepted for legacy compatibility and
 * normalized to the exact lowercase wire representation. */
export const canonicalLinkageSchema = z
  .string()
  .trim()
  .regex(/^sha256:[a-f0-9]{64}$/i, "Expected sha256: followed by exactly 64 hexadecimal characters.")
  .transform((value) => value.toLowerCase());

export const forbiddenRawContentFieldNames = [
  "api_request_body",
  "api_response_body",
  "arguments",
  "args",
  "assistant_message",
  "assistant_response",
  "browser_history",
  "clipboard",
  "clipboard_body",
  "cmd",
  "command",
  "command_body",
  "diff",
  "file_body",
  "full_command",
  "output",
  "patch",
  "prompt",
  "prompt_body",
  "raw",
  "raw_api_body",
  "raw_body",
  "raw_output",
  "raw_prompt",
  "response",
  "screenshot",
  "stderr",
  "stdin",
  "stdout",
  "tool_arguments",
  "tool_input",
  "tool_output",
  "tool_response",
  "user_prompt",
] as const;

const forbiddenRawContentKeys = new Set<string>(
  forbiddenRawContentFieldNames.map((field) => normalizeFieldName(field)),
);

function normalizeFieldName(field: string) {
  return field.replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
}

export function isForbiddenRawContentFieldName(field: string) {
  return forbiddenRawContentKeys.has(normalizeFieldName(field));
}

export function findForbiddenRawContentFields(
  value: unknown,
  prefix = "",
): string[] {
  if (!value || typeof value !== "object") {
    return [];
  }

  if (Array.isArray(value)) {
    return value.flatMap((item, index) =>
      findForbiddenRawContentFields(item, `${prefix}[${index}]`),
    );
  }

  const hits: string[] = [];
  const record = value as Record<string, unknown>;
  const semanticKey = typeof record.key === "string" ? record.key : undefined;
  if (semanticKey && isForbiddenRawContentFieldName(semanticKey)) {
    hits.push(prefix ? `${prefix}.${semanticKey}` : semanticKey);
    return hits;
  }

  for (const [key, nestedValue] of Object.entries(record)) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (isForbiddenRawContentFieldName(key)) {
      hits.push(path);
      continue;
    }

    hits.push(...findForbiddenRawContentFields(nestedValue, path));
  }

  return hits;
}

export const tenantSchema = z.object({
  id: idSchema,
  name: z.string().trim().min(1),
  plan: z.enum(["free_local", "team_private", "enterprise"]).default("free_local"),
  benchmarkContribution: z
    .enum(["disabled", "explicit_opt_in", "enabled_for_free"])
    .default("disabled"),
  createdAt: timestampSchema,
});
export type Tenant = z.infer<typeof tenantSchema>;

export const actorSchema = z.object({
  id: idSchema,
  tenantId: idSchema,
  emailHash: z.string().trim().min(1),
  displayName: z.string().trim().optional(),
  teamId: idSchema.optional(),
  role: z.enum(["employee", "manager", "finance", "admin"]).default("employee"),
});
export type Actor = z.infer<typeof actorSchema>;

export const deviceInstallSchema = z.object({
  id: idSchema,
  tenantId: idSchema.optional(),
  actorId: idSchema.optional(),
  installKey: keySchema,
  platform: z.enum(["macos", "linux", "windows"]).default("macos"),
  appVersion: z.string().trim().min(1),
  dataMode: dataModeSchema.default("metadata"),
  createdAt: timestampSchema,
});
export type DeviceInstall = z.infer<typeof deviceInstallSchema>;

export const toolIntegrationSchema = z.object({
  id: idSchema,
  tenantId: idSchema,
  source: toolSourceSchema,
  mode: z.enum(["api", "hook", "otel", "manual_export"]),
  status: z.enum(["active", "paused", "error", "needs_setup"]),
  lastSeenAt: timestampSchema.optional(),
});
export type ToolIntegration = z.infer<typeof toolIntegrationSchema>;

export const repositorySchema = z.object({
  id: idSchema,
  tenantId: idSchema,
  provider: z.enum(["github", "gitlab", "local_git", "unknown"]).default("github"),
  owner: z.string().trim().optional(),
  name: z.string().trim().min(1),
  remoteUrlHash: z.string().trim().optional(),
});
export type RepositoryRecord = z.infer<typeof repositorySchema>;

export const projectMapSchema = z.object({
  id: idSchema,
  tenantId: idSchema,
  repositoryId: idSchema.optional(),
  projectKey: keySchema,
  projectName: z.string().trim().min(1),
  customerKey: keySchema.optional(),
  workflowKey: keySchema.optional(),
});
export type ProjectMap = z.infer<typeof projectMapSchema>;

export const customerMapSchema = z.object({
  id: idSchema,
  tenantId: idSchema,
  customerKey: keySchema,
  customerName: z.string().trim().min(1),
  serviceLine: z.string().trim().optional(),
});
export type CustomerMap = z.infer<typeof customerMapSchema>;

export const aiWorkSessionSchema = z
  .object({
    id: idSchema,
    tenantId: idSchema.optional(),
    actorId: idSchema.optional(),
    deviceInstallId: idSchema.optional(),
    source: toolSourceSchema,
    dataMode: dataModeSchema.default("metadata"),
    startedAt: timestampSchema,
    endedAt: timestampSchema.optional(),
    repositoryId: idSchema.optional(),
    repoPathHash: z.string().trim().optional(),
    branch: z.string().trim().optional(),
    projectKey: keySchema.optional(),
    customerKey: keySchema.optional(),
    workflowKey: keySchema.optional(),
    intent: workIntentSchema.default("unknown"),
    metadata: metadataSchema,
  })
  .strict();
export type AiWorkSession = z.infer<typeof aiWorkSessionSchema>;

export const aiInteractionEventSchema = z
  .object({
    id: idSchema,
    sessionId: idSchema.optional(),
    tenantId: idSchema.optional(),
    actorId: idSchema.optional(),
    source: toolSourceSchema,
    dataMode: dataModeSchema.default("metadata"),
    eventType: z.enum([
      "session_start",
      "session_stop",
      "user_prompt_submit",
      "assistant_response",
      "tool_use",
      "tool_result",
      "otel_span",
      "usage_rollout",
      "usage_transcript",
      "unknown",
    ]),
    observedAt: timestampSchema,
    model: z.string().trim().optional(),
    projectKey: keySchema.optional(),
    customerKey: keySchema.optional(),
    workflowKey: keySchema.optional(),
    intent: workIntentSchema.default("unknown"),
    actionClass: actionClassSchema.default("other"),
    inputTokens: z.number().int().nonnegative().optional(),
    outputTokens: z.number().int().nonnegative().optional(),
    cacheReadTokens: z.number().int().nonnegative().optional(),
    cacheCreationTokens: z.number().int().nonnegative().optional(),
    costUsd: z.number().nonnegative().optional(),
    metadata: metadataSchema,
  })
  .strict();
export type AiInteractionEvent = z.infer<typeof aiInteractionEventSchema>;

export const aiWorkIngestEventSchema = z
  .object({
    event: aiInteractionEventSchema,
    suppressedFields: z.array(z.string().trim().min(1)).default([]),
  })
  .strict();
export type AiWorkIngestEvent = z.infer<typeof aiWorkIngestEventSchema>;

export const aiWorkIngestBatchSchema = z
  .object({
    tenantId: idSchema.default(LOCAL_TENANT_ID),
    installKey: keySchema,
    appVersion: z.string().trim().min(1).default("0.1.0"),
    events: z.array(aiWorkIngestEventSchema).min(1).max(500),
  })
  .strict();
export type AiWorkIngestBatch = z.infer<typeof aiWorkIngestBatchSchema>;
/**
 * Attribution repair (issue 0036): the bulk ingest lane is first-writer-wins
 * (createMany skipDuplicates — cloud PR #19), so events uploaded before the
 * collector learned to send projectKey can never be back-filled by re-sending
 * them. This batch shape carries ONLY {id, projectKey} pairs; the cloud
 * applies one set-based, tenant-scoped, FILL-ONLY update per batch (a row
 * with a differing non-null projectKey is left alone — first-writer-wins
 * extended to attribution).
 *
 * `kind` discriminates it from a normal event batch on the shared ingest
 * route. projectKey is the repo linkage hash (sha256:…) — the privacy-
 * preserving key DESIGNED to cross boundaries; raw URLs/branches never do.
 */
const uuidShapedIdSchema = z
  .string()
  .trim()
  .regex(
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    "Expected a UUID-shaped event id.",
  );

export const aiWorkAttributionRepairRowSchema = z
  .object({
    id: uuidShapedIdSchema,
    projectKey: keySchema,
  })
  .strict();
export type AiWorkAttributionRepairRow = z.infer<typeof aiWorkAttributionRepairRowSchema>;

export const aiWorkAttributionRepairBatchSchema = z
  .object({
    kind: z.literal("attribution_repair"),
    tenantId: idSchema.default(LOCAL_TENANT_ID),
    installKey: keySchema,
    appVersion: z.string().trim().min(1).default("0.1.0"),
    rows: z.array(aiWorkAttributionRepairRowSchema).min(1).max(500),
  })
  .strict();
export type AiWorkAttributionRepairBatch = z.infer<typeof aiWorkAttributionRepairBatchSchema>;

/**
 * Session sync (issue 0037 / cloud Phase D1): the local ledger stitches
 * sessions from events (session_id on every row); hosted AiWorkSession held
 * 0 rows. This lane carries full SNAPSHOTS of one session's aggregate state,
 * recomputed from the ledger on every send — sessions GROW over time (endedAt
 * advances, totals climb), so unlike immutable events the cloud applies a
 * grow-only last-writer-wins upsert.
 *
 * Id rule (the join contract): a ledger session id that Postgres' uuid column
 * accepts passes through VERBATIM (lowercased) — that exact value is what the
 * event lane already stored on event rows, in the session_id uuid column
 * (claude v4 ids and codex v7 ids) — so session rows JOIN to their events.
 * Non-uuid ledger ids derive the same UUID on every run
 * (collector-cli/session-sync.ts); the raw local identifier never crosses the
 * session-sync outbound boundary.
 *
 * `kind` discriminates it from event batches on the shared ingest route (the
 * attribution_repair pattern). Totals are typed — not loose metadata — so the
 * cloud and proofs can reconcile them against event rows; costUsd sums PRICED
 * events only and pricedEvents says how many (honest-numbers doctrine: an
 * unpriced session is never a fabricated $0.00).
 */
export const aiWorkSessionTotalsSchema = z
  .object({
    events: z.number().int().positive(),
    inputTokens: z.number().int().nonnegative().default(0),
    outputTokens: z.number().int().nonnegative().default(0),
    cacheReadTokens: z.number().int().nonnegative().default(0),
    cacheCreationTokens: z.number().int().nonnegative().default(0),
    pricedEvents: z.number().int().nonnegative().default(0),
    costUsd: z.number().nonnegative().default(0),
  })
  .strict();
export type AiWorkSessionTotals = z.infer<typeof aiWorkSessionTotalsSchema>;

export const aiWorkSessionSyncRowSchema = z
  .object({
    session: aiWorkSessionSchema.extend({
      id: uuidShapedIdSchema,
      endedAt: timestampSchema,
    }),
    totals: aiWorkSessionTotalsSchema,
  })
  .strict()
  .refine(
    (row) => Date.parse(row.session.endedAt) >= Date.parse(row.session.startedAt),
    "Session endedAt must not precede startedAt.",
  );
export type AiWorkSessionSyncRow = z.infer<typeof aiWorkSessionSyncRowSchema>;

export const aiWorkSessionSyncBatchSchema = z
  .object({
    kind: z.literal("session_sync"),
    tenantId: idSchema.default(LOCAL_TENANT_ID),
    installKey: keySchema,
    appVersion: z.string().trim().min(1).default("0.1.0"),
    sessions: z.array(aiWorkSessionSyncRowSchema).min(1).max(500),
  })
  .strict();
export type AiWorkSessionSyncBatch = z.infer<typeof aiWorkSessionSyncBatchSchema>;

/**
 * Repo label disclosure (issue 0036): repo display names are deliberate,
 * owner-run disclosures (push-repo-labels previews the exact payload first).
 * Only bounded printable ASCII slugs cross the wire. URL/path/email,
 * multibyte, auth, credential, secret-prefix, JWT, and private-key shapes are
 * refused so deliberate display-name disclosure cannot become a value bypass.
 */
const REPO_DISPLAY_SLUG = /^[a-zA-Z0-9._-]{1,200}$/;
const REPO_SECRET_PREFIX = /(?:^|[._-])(?:sk_live|sk_test|sk-|ghp[a-z0-9_-]*|github_pat[a-z0-9_-]*|xox[a-z0-9_-]*)/i;
const REPO_AUTH_SCHEME = /^(?:bearer|basic)(?:[._-]|$)/i;
const REPO_JWT = /^eyj[a-z0-9_-]*\.[a-z0-9_-]+\.[a-z0-9_-]+$/i;
const REPO_CREDENTIAL_WORD = /^(?:auth|credential|credentials|password|secret|secrets|token|tokens)$/i;
const REPO_CREDENTIAL_COMPOUND = /(?:access|api|client|private|signing|ssh)(?:key|secret|token)/i;

function isSafeRepoDisplaySlug(value: string) {
  if (
    !/^[\x20-\x7e]+$/.test(value) ||
    !REPO_DISPLAY_SLUG.test(value) ||
    value === "." ||
    value === ".." ||
    REPO_SECRET_PREFIX.test(value) ||
    REPO_AUTH_SCHEME.test(value) ||
    REPO_JWT.test(value)
  ) {
    return false;
  }
  const words = value.split(/[._-]+/).filter(Boolean);
  const collapsed = words.join("");
  return !words.some((word) => REPO_CREDENTIAL_WORD.test(word)) &&
    !REPO_CREDENTIAL_COMPOUND.test(collapsed);
}

const repoDisplaySlugSchema = z
  .string()
  .trim()
  .min(1)
  .max(200)
  .refine(
    isSafeRepoDisplaySlug,
    "Expected a bounded printable ASCII repo slug without URL, path, email, credential, or secret shapes.",
  );

export const workRepoLabelSchema = z
  .object({
    remoteUrlHash: canonicalLinkageSchema,
    name: repoDisplaySlugSchema,
    owner: repoDisplaySlugSchema.optional(),
    provider: z.enum(["github", "gitlab", "local_git", "unknown"]).default("github"),
  })
  .strict();
export type WorkRepoLabel = z.infer<typeof workRepoLabelSchema>;

export const workRepoLabelsBatchSchema = z
  .object({
    tenantId: idSchema.default(LOCAL_TENANT_ID),
    installKey: keySchema,
    appVersion: z.string().trim().min(1).default("0.1.0"),
    repositories: z.array(workRepoLabelSchema).min(1).max(200),
  })
  .strict();
export type WorkRepoLabelsBatch = z.infer<typeof workRepoLabelsBatchSchema>;


export const toolActionEventSchema = z
  .object({
    id: idSchema,
    sessionId: idSchema.optional(),
    interactionEventId: idSchema.optional(),
    source: toolSourceSchema,
    dataMode: dataModeSchema.default("metadata"),
    observedAt: timestampSchema,
    toolName: z.string().trim().min(1),
    actionClass: actionClassSchema,
    allowed: z.boolean().default(true),
    durationMs: z.number().int().nonnegative().optional(),
    resultStatus: z.enum(["success", "failure", "blocked", "unknown"]).default("unknown"),
    metadata: metadataSchema,
  })
  .strict();
export type ToolActionEvent = z.infer<typeof toolActionEventSchema>;

/**
 * Learning facts are deliberately low-cardinality and metadata-only. Tool
 * names are canonical categories rather than provider function names, and
 * error categories never carry messages, stacks, commands, arguments, or
 * paths. The collector hashes source operation keys before these schemas see
 * them.
 */
const learningFactIdSchema = z
  .string()
  .trim()
  .regex(
    /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    "Expected a deterministic UUIDv5-shaped fact id.",
  )
  .transform((value) => value.toLowerCase());

const learningDimensionIdSchema = z
  .string()
  .trim()
  .min(1)
  .max(96)
  .regex(
    /^[a-zA-Z0-9][a-zA-Z0-9._:-]*$/,
    "Expected a bounded metadata identifier without whitespace, path, email, or content characters.",
  )
  .refine(
    (value) =>
      !/^(?:sk_(?:live|test)|sk-|ghp|github_pat|xox)[a-z0-9._:-]*/i.test(value) &&
      !/^(?:bearer|basic)[._:-]/i.test(value),
    "Secret-shaped identifiers are not allowed in learning facts.",
  );

/**
 * Exposure identity fields are metric dimensions, so aliases are rejected at
 * the boundary instead of being silently trimmed, Unicode-normalized, or
 * case-folded. This keeps one accepted spelling for every hashed identity.
 */
const canonicalExposureDimensionIdSchema = z
  .string()
  .min(1)
  .max(96)
  .refine((value) => value === value.trim().normalize("NFKC"), {
    message: "Exposure identity must already be trimmed and NFKC-canonical.",
  })
  .regex(
    /^[a-zA-Z0-9][a-zA-Z0-9._:-]*$/,
    "Expected a bounded canonical metadata identifier.",
  )
  .refine(
    (value) =>
      !/^(?:sk_(?:live|test)|sk-|ghp|github_pat|xox)[a-z0-9._:-]*/i.test(value) &&
      !/^(?:bearer|basic)[._:-]/i.test(value),
    "Secret-shaped identifiers are not allowed in learning facts.",
  );

const canonicalExposureVersionSchema = z
  .string()
  .min(1)
  .max(64)
  .refine((value) => value === value.trim().normalize("NFKC"), {
    message: "Exposure version must already be trimmed and NFKC-canonical.",
  })
  .regex(
    /^[a-zA-Z0-9][a-zA-Z0-9._+-]*$/,
    "Expected a bounded canonical version identifier.",
  )
  .refine(
    (value) => !/^(?:sk_(?:live|test)|sk-|ghp|github_pat|xox)/i.test(value),
    "Secret-shaped versions are not allowed in learning facts.",
  );

const canonicalExposureFactIdSchema = z.string().regex(
  /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
  "Expected an already-canonical lowercase deterministic fact id.",
);

const canonicalExposureDigestSchema = z.string().regex(
  /^sha256:[a-f0-9]{64}$/,
  "Expected an already-canonical lowercase sha256 digest.",
);

export const toolFactClassSchema = z.enum([
  "compute",
  "local_io",
  "network",
  "coordination",
  "other",
]);
export type ToolFactClass = z.infer<typeof toolFactClassSchema>;

export const toolFactNameSchema = z.enum([
  "continue",
  "validate",
  "test",
  "edit",
  "read",
  "write",
  "shell",
  "mcp",
  "browser",
  "review",
  "other",
]);
export type ToolFactName = z.infer<typeof toolFactNameSchema>;

export const toolAttemptResultStatusSchema = z.enum(["success", "failure", "unknown"]);
export type ToolAttemptResultStatus = z.infer<typeof toolAttemptResultStatusSchema>;

export const toolAttemptErrorCategorySchema = z.enum([
  "none",
  "auth",
  "rate_limit",
  "timeout",
  "network",
  "validation",
  "not_found",
  "conflict",
  "provider",
  "tool",
  "unknown",
]);
export type ToolAttemptErrorCategory = z.infer<typeof toolAttemptErrorCategorySchema>;

const toolAttemptIdentitySchema = z.object({
  operationId: learningFactIdSchema,
  source: toolSourceSchema,
  sessionId: learningDimensionIdSchema,
  episodeId: learningFactIdSchema.optional(),
  toolClass: toolFactClassSchema,
  toolName: toolFactNameSchema,
});

export const toolAttemptStartSignalSchema = toolAttemptIdentitySchema
  .extend({
    kind: z.literal("attempt"),
    startedAt: timestampSchema,
    retryOf: learningFactIdSchema.optional(),
  })
  .strict()
  .refine((signal) => signal.retryOf !== signal.operationId, {
    message: "An attempt cannot retry itself.",
    path: ["retryOf"],
  });
export type ToolAttemptStartSignal = z.infer<typeof toolAttemptStartSignalSchema>;

export const toolAttemptResultSignalSchema = z
  .object({
    kind: z.literal("result"),
    operationId: learningFactIdSchema,
    source: toolSourceSchema,
    sessionId: learningDimensionIdSchema,
    endedAt: timestampSchema,
    resultStatus: toolAttemptResultStatusSchema.default("unknown"),
    errorCategory: toolAttemptErrorCategorySchema.optional(),
  })
  .strict()
  .superRefine((signal, context) => {
    if (
      signal.resultStatus === "success" &&
      signal.errorCategory &&
      signal.errorCategory !== "none"
    ) {
      context.addIssue({
        code: "custom",
        message: "Successful attempts cannot carry an error category.",
        path: ["errorCategory"],
      });
    }
    if (signal.resultStatus === "failure" && signal.errorCategory === "none") {
      context.addIssue({
        code: "custom",
        message: "Failed attempts cannot use the none error category.",
        path: ["errorCategory"],
      });
    }
    if (
      signal.resultStatus === "unknown" &&
      signal.errorCategory &&
      signal.errorCategory !== "unknown"
    ) {
      context.addIssue({
        code: "custom",
        message: "Unknown results cannot assert a specific error category.",
        path: ["errorCategory"],
      });
    }
  });
export type ToolAttemptResultSignal = z.infer<typeof toolAttemptResultSignalSchema>;

export const toolAttemptSignalSchema = z.union([
  toolAttemptStartSignalSchema,
  toolAttemptResultSignalSchema,
]);
export type ToolAttemptSignal = z.infer<typeof toolAttemptSignalSchema>;

export const toolAttemptFactSchema = toolAttemptIdentitySchema
  .extend({
    startedAt: timestampSchema,
    endedAt: timestampSchema.optional(),
    durationMs: z
      .number()
      .int()
      .nonnegative()
      .max(7 * 24 * 60 * 60 * 1_000)
      .optional(),
    resultStatus: toolAttemptResultStatusSchema,
    errorCategory: toolAttemptErrorCategorySchema,
    retryOf: learningFactIdSchema.optional(),
  })
  .strict()
  .superRefine((fact, context) => {
    const startedMs = Date.parse(fact.startedAt);
    const endedMs = fact.endedAt ? Date.parse(fact.endedAt) : undefined;
    if (endedMs !== undefined && endedMs < startedMs) {
      context.addIssue({
        code: "custom",
        message: "Attempt endedAt must not precede startedAt.",
        path: ["endedAt"],
      });
    }
    if ((fact.endedAt === undefined) !== (fact.durationMs === undefined)) {
      context.addIssue({
        code: "custom",
        message: "Attempt endedAt and durationMs must be present together.",
        path: ["durationMs"],
      });
    }
    if (
      endedMs !== undefined &&
      fact.durationMs !== undefined &&
      fact.durationMs !== endedMs - startedMs
    ) {
      context.addIssue({
        code: "custom",
        message: "Attempt durationMs must be derived exactly from its timestamps.",
        path: ["durationMs"],
      });
    }
    if (fact.resultStatus !== "unknown" && fact.endedAt === undefined) {
      context.addIssue({
        code: "custom",
        message: "Known results require an end timestamp.",
        path: ["endedAt"],
      });
    }
    if (fact.retryOf === fact.operationId) {
      context.addIssue({
        code: "custom",
        message: "An attempt cannot retry itself.",
        path: ["retryOf"],
      });
    }
  });
export type ToolAttemptFact = z.infer<typeof toolAttemptFactSchema>;

export const workClassSchema = z.enum([
  "implementation",
  "debugging",
  "review",
  "research",
  "operations",
  "other",
]);
export type WorkClass = z.infer<typeof workClassSchema>;

export const workComplexityBandSchema = z.enum(["low", "medium", "high", "unknown"]);
export type WorkComplexityBand = z.infer<typeof workComplexityBandSchema>;

export const workEpisodeFactSchema = z
  .object({
    episodeId: learningFactIdSchema,
    source: toolSourceSchema,
    sessionId: learningDimensionIdSchema,
    workClass: workClassSchema,
    complexityBand: workComplexityBandSchema,
    startedAt: timestampSchema,
    endedAt: timestampSchema.optional(),
    durationMs: z
      .number()
      .int()
      .nonnegative()
      .max(30 * 24 * 60 * 60 * 1_000)
      .optional(),
  })
  .strict()
  .superRefine((fact, context) => {
    const startedMs = Date.parse(fact.startedAt);
    const endedMs = fact.endedAt ? Date.parse(fact.endedAt) : undefined;
    if (endedMs !== undefined && endedMs < startedMs) {
      context.addIssue({
        code: "custom",
        message: "Episode endedAt must not precede startedAt.",
        path: ["endedAt"],
      });
    }
    if ((fact.endedAt === undefined) !== (fact.durationMs === undefined)) {
      context.addIssue({
        code: "custom",
        message: "Episode endedAt and durationMs must be present together.",
        path: ["durationMs"],
      });
    }
    if (
      endedMs !== undefined &&
      fact.durationMs !== undefined &&
      fact.durationMs !== endedMs - startedMs
    ) {
      context.addIssue({
        code: "custom",
        message: "Episode durationMs must be derived exactly from its timestamps.",
        path: ["durationMs"],
      });
    }
  });
export type WorkEpisodeFact = z.infer<typeof workEpisodeFactSchema>;

export const techniqueExposureInputSchema = z
  .object({
    episodeId: canonicalExposureFactIdSchema,
    techniqueId: canonicalExposureDimensionIdSchema,
    techniqueVersion: canonicalExposureVersionSchema.optional(),
    contentDigest: canonicalExposureDigestSchema.optional(),
    assignmentId: canonicalExposureDimensionIdSchema,
    workClass: workClassSchema,
    complexityBand: workComplexityBandSchema,
    exposedAt: timestampSchema,
    mode: z.enum(["control", "treatment"]),
  })
  .strict()
  .refine((fact) => Boolean(fact.techniqueVersion || fact.contentDigest), {
    message: "Technique exposure requires a version or content digest.",
    path: ["techniqueVersion"],
  });
export type TechniqueExposureInput = z.infer<typeof techniqueExposureInputSchema>;

export const techniqueExposureFactSchema = techniqueExposureInputSchema
  .safeExtend({
    exposureId: canonicalExposureFactIdSchema,
    assertion: z.literal("exposure_only"),
  })
  .strict();
export type TechniqueExposureFact = z.infer<typeof techniqueExposureFactSchema>;

export const workArtifactSchema = z.object({
  id: idSchema,
  tenantId: idSchema.optional(),
  actorId: idSchema.optional(),
  repositoryId: idSchema.optional(),
  sessionId: idSchema.optional(),
  artifactType: z.enum([
    "commit",
    "pull_request",
    "review",
    "check_run",
    "deployment",
    "revert",
    "document",
    "test_report",
  ]),
  externalId: z.string().trim().min(1),
  observedAt: timestampSchema,
  status: z.enum(["created", "passed", "failed", "merged", "reverted", "reopened", "unknown"]),
  metadata: metadataSchema,
});
export type WorkArtifact = z.infer<typeof workArtifactSchema>;

export const reviewOutcomeSchema = z.object({
  id: idSchema,
  workArtifactId: idSchema,
  reviewerActorId: idSchema.optional(),
  outcome: z.enum([
    "approved",
    "changes_requested",
    "commented",
    "failed_check",
    "merged",
    "neutral_check",
    "passed_check",
    "reopened",
    "reverted",
    "unknown_check",
  ]),
  observedAt: timestampSchema,
  metadata: metadataSchema,
});
export type ReviewOutcome = z.infer<typeof reviewOutcomeSchema>;

export const githubOutcomeIngestBatchSchema = z
  .object({
    tenantId: idSchema.default(LOCAL_TENANT_ID),
    repository: repositorySchema.pick({
      owner: true,
      name: true,
      provider: true,
      remoteUrlHash: true,
    }),
    artifacts: z.array(workArtifactSchema).min(1).max(500),
    outcomes: z.array(reviewOutcomeSchema).max(1000).default([]),
  })
  .strict();
export type GitHubOutcomeIngestBatch = z.infer<typeof githubOutcomeIngestBatchSchema>;

export const interventionSchema = z.object({
  id: idSchema,
  tenantId: idSchema,
  targetActorId: idSchema.optional(),
  targetTeamId: idSchema.optional(),
  createdByActorId: idSchema,
  interventionType: z.enum(["feedback", "skill", "playbook", "policy_change"]),
  status: z.enum(["draft", "assigned", "accepted", "completed", "dismissed"]).default("draft"),
  createdAt: timestampSchema,
  metadata: metadataSchema,
});
export type Intervention = z.infer<typeof interventionSchema>;

export const skillRecommendationSchema = z.object({
  id: idSchema,
  tenantId: idSchema,
  interventionId: idSchema.optional(),
  title: z.string().trim().min(1),
  rationale: z.string().trim().min(1),
  status: z.enum(["draft", "approved", "rejected", "published"]).default("draft"),
  createdAt: timestampSchema,
});
export type SkillRecommendation = z.infer<typeof skillRecommendationSchema>;

export const behaviorDeltaSchema = z.object({
  id: idSchema,
  interventionId: idSchema,
  metricName: z.string().trim().min(1),
  beforeValue: z.number().optional(),
  afterValue: z.number().optional(),
  confidence: z.enum(["low", "medium", "high"]).default("low"),
  measuredAt: timestampSchema,
});
export type BehaviorDelta = z.infer<typeof behaviorDeltaSchema>;

export const costUsageRecordSchema = z.object({
  id: idSchema,
  tenantId: idSchema,
  source: toolSourceSchema,
  periodStart: timestampSchema,
  periodEnd: timestampSchema,
  sourceRecordKey: keySchema,
  model: z.string().trim().optional(),
  apiKeyHash: z.string().trim().optional(),
  workspaceKey: z.string().trim().optional(),
  projectKey: z.string().trim().optional(),
  inputTokens: z.number().int().nonnegative().optional(),
  outputTokens: z.number().int().nonnegative().optional(),
  actualCostUsd: z.number().nonnegative().optional(),
  estimatedCostUsd: z.number().nonnegative().optional(),
  metadata: metadataSchema,
});
export type CostUsageRecord = z.infer<typeof costUsageRecordSchema>;

export const costAllocationSchema = z.object({
  id: idSchema,
  tenantId: idSchema,
  sourceCostRecordId: idSchema.optional(),
  allocationType: z.enum(["direct", "seat_allocated", "usage_allocated", "overhead", "unmapped"]),
  amountUsd: z.number().nonnegative(),
  actorId: idSchema.optional(),
  teamId: idSchema.optional(),
  projectKey: keySchema.optional(),
  customerKey: keySchema.optional(),
  workflowKey: keySchema.optional(),
  confidence: z.enum(["low", "medium", "high"]).default("low"),
  basis: z.string().trim().min(1),
  metadata: metadataSchema,
});
export type CostAllocation = z.infer<typeof costAllocationSchema>;

export const costUsageIngestBatchSchema = z
  .object({
    tenantId: idSchema.default(LOCAL_TENANT_ID),
    records: z.array(costUsageRecordSchema).min(1).max(500),
    allocations: z.array(costAllocationSchema).max(1000).default([]),
  })
  .strict();
export type CostUsageIngestBatch = z.infer<typeof costUsageIngestBatchSchema>;

export const policyConfigSchema = z
  .object({
    id: idSchema,
    tenantId: idSchema,
    dataMode: dataModeSchema.default("metadata"),
    version: z.string().trim().min(1),
    benchmarkContribution: z.enum(["disabled", "explicit_opt_in", "enabled_for_free"]).default("disabled"),
    employeeSelfViewEnabled: z.boolean().default(true),
    managerDrilldownEnabled: z.boolean().default(false),
    minimumCohortSize: z.number().int().min(5).default(5),
    evidence: z
      .object({
        enabled: z.boolean().default(false),
        noticeVersion: z.string().trim().optional(),
        noticeText: z.string().trim().optional(),
        retentionDays: z.number().int().min(1).max(365).optional(),
        allowedCategories: z.array(rawContentCategorySchema).default([]),
        rbacScopes: z.array(z.string().trim().min(1)).default([]),
      })
      .default({ enabled: false, allowedCategories: [], rbacScopes: [] }),
    updatedAt: timestampSchema,
  })
  .superRefine((policy, context) => {
    if (policy.dataMode !== "evidence") {
      return;
    }

    if (!policy.evidence.enabled) {
      context.addIssue({
        code: "custom",
        message: "Evidence mode requires evidence.enabled=true.",
        path: ["evidence", "enabled"],
      });
    }

    if (!policy.evidence.noticeVersion) {
      context.addIssue({
        code: "custom",
        message: "Evidence mode requires an employee notice version.",
        path: ["evidence", "noticeVersion"],
      });
    }

    if (!policy.evidence.noticeText) {
      context.addIssue({
        code: "custom",
        message: "Evidence mode requires employee notice text.",
        path: ["evidence", "noticeText"],
      });
    }

    if (!policy.evidence.retentionDays) {
      context.addIssue({
        code: "custom",
        message: "Evidence mode requires retentionDays.",
        path: ["evidence", "retentionDays"],
      });
    }

    if (policy.evidence.rbacScopes.length === 0) {
      context.addIssue({
        code: "custom",
        message: "Evidence mode requires at least one RBAC scope.",
        path: ["evidence", "rbacScopes"],
      });
    }

    if (policy.evidence.allowedCategories.length === 0) {
      context.addIssue({
        code: "custom",
        message: "Evidence mode requires at least one raw-content category opt-in.",
        path: ["evidence", "allowedCategories"],
      });
    }
  });
export type PolicyConfig = z.infer<typeof policyConfigSchema>;

export const evidenceVaultRecordSchema = z.object({
  id: idSchema,
  tenantId: idSchema,
  sessionId: idSchema.optional(),
  interactionEventId: idSchema.optional(),
  category: rawContentCategorySchema,
  policyVersion: z.string().trim().min(1),
  noticeVersion: z.string().trim().min(1),
  expiresAt: timestampSchema,
  encryptedPayloadRef: z.string().trim().min(1),
  createdAt: timestampSchema,
});
export type EvidenceVaultRecord = z.infer<typeof evidenceVaultRecordSchema>;

export const evidenceVaultIngestSchema = z
  .object({
    tenantId: idSchema.default(LOCAL_TENANT_ID),
    sessionId: idSchema.optional(),
    interactionEventId: idSchema.optional(),
    category: rawContentCategorySchema,
    noticeVersion: z.string().trim().min(1),
    encryptedPayloadRef: z.string().trim().min(1),
    metadata: metadataSchema,
  })
  .strict();
export type EvidenceVaultIngest = z.infer<typeof evidenceVaultIngestSchema>;

export const benchmarkContributionSchema = z.object({
  id: idSchema,
  cohortKey: keySchema,
  metricName: z.string().trim().min(1),
  metricValue: z.number(),
  aggregationWindowStart: timestampSchema,
  aggregationWindowEnd: timestampSchema,
  cohortSize: z.number().int().min(5),
  minimumThresholdMet: z.boolean(),
  excludedFields: z.array(z.string().trim().min(1)),
  contributionTermsVersion: z.string().trim().min(1),
  createdAt: timestampSchema,
});
export type BenchmarkContribution = z.infer<typeof benchmarkContributionSchema>;

export const benchmarkContributionExportRowSchema = z
  .object({
    cohortKey: keySchema,
    metricName: z.string().trim().min(1),
    metricValue: z.number().finite(),
    aggregationWindowStart: timestampSchema,
    aggregationWindowEnd: timestampSchema,
    cohortSize: z.number().int().positive(),
    contributionTermsVersion: z.string().trim().min(1),
    metadata: metadataSchema,
  })
  .strict();
export type BenchmarkContributionExportRow = z.infer<
  typeof benchmarkContributionExportRowSchema
>;

export const benchmarkContributionExportBatchSchema = z
  .object({
    tenantId: idSchema.default(LOCAL_TENANT_ID),
    rows: z.array(benchmarkContributionExportRowSchema).min(1).max(1000),
  })
  .strict();
export type BenchmarkContributionExportBatch = z.infer<
  typeof benchmarkContributionExportBatchSchema
>;
