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
