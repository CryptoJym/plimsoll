import { z } from "zod";

export const PROJECT_INTENT_SCHEMA = "plimsoll-project-intent/v1" as const;
export const PROJECT_INTENT_PATH = "/api/work-intelligence/project-intents";
export const PROJECT_INTENT_MAX_RECEIPTS = 128;
const key = z.string().regex(/^sha256:[0-9a-f]{64}$/);
const uuid = z.string().uuid().transform(value => value.toLowerCase());
const instant = z.string().datetime().transform(value => new Date(value).toISOString());

/** An outbound receipt, never a raw hook/OTLP envelope. All identity evidence
 * that could carry a path, account name, native label or work title is hashed
 * locally using the existing linkage convention before reaching this schema.
 * Shape proves neither registration nor authority: the server checks both. */
export const projectIntentReceiptSchema = z.object({
  schema: z.literal(PROJECT_INTENT_SCHEMA),
  receiptId: uuid,
  installId: uuid,
  source: z.enum(["codex", "claude_code", "gemini_cli", "grok"]),
  sourceRootKey: key,
  accountKey: key.nullable(),
  sessionId: uuid,
  nativeSessionKey: key,
  sessionEpochKey: key,
  rootAttemptId: uuid,
  attemptId: uuid,
  parentAttemptId: uuid.nullable(),
  workItemKey: key.nullable(),
  projectKey: key.nullable(),
  projectRegistryRevision: z.number().int().positive().nullable(),
  observedRepoKey: key.nullable(),
  effectiveFrom: instant,
  effectiveUntil: instant.nullable(),
  basis: z.enum(["routed_launch", "hand_start", "trusted_folder_default", "repo_observation"]),
  evidenceRef: key,
  adapterId: z.string().regex(/^[a-z][a-z0-9-]{0,47}$/),
  adapterVersion: z.string().max(32).regex(/^\d+\.\d+\.\d+$/),
}).strict().superRefine((row, ctx) => {
  if (row.effectiveUntil && Date.parse(row.effectiveUntil) <= Date.parse(row.effectiveFrom))
    ctx.addIssue({ code: "custom", message: "intent_interval_invalid" });
  if ((row.projectKey === null) !== (row.projectRegistryRevision === null))
    ctx.addIssue({ code: "custom", message: "project_revision_required" });
  if (row.basis === "repo_observation" && row.projectKey !== null)
    ctx.addIssue({ code: "custom", message: "observation_cannot_declare_project" });
});
export type ProjectIntentReceipt = z.infer<typeof projectIntentReceiptSchema>;
export const PROJECT_INTENT_OUTBOUND_FIELDS = Object.keys(projectIntentReceiptSchema.shape);
export const PROJECT_INTENT_LOCAL_ONLY_FIELDS = [
  "prompts", "replies", "code", "commands", "fileNames", "cwd", "sourceRootPath",
  "rawNativeSessionId", "rawAccountId", "accountEmail", "rawWorkItemId", "environmentValues", "credentials",
] as const;
export const projectIntentRequestSchema = z.object({
  tenantId: uuid,
  expectedRevision: z.number().int().nonnegative(),
  receipt: projectIntentReceiptSchema,
}).strict();
export type ProjectIntentRequest = z.infer<typeof projectIntentRequestSchema>;

export class ProjectIntentContractError extends Error {
  constructor(readonly code: string) { super(code); }
}

export function assertSupportedProjectIntentAdapter(row: ProjectIntentReceipt): void {
  const expected = row.basis === "routed_launch" ? "routed-launch"
    : row.basis === "repo_observation" ? "repo-observation" : "hand-start";
  if (row.adapterId !== expected || row.adapterVersion !== "1.0.0")
    throw new ProjectIntentContractError("unsupported_adapter");
}

/** Immutable receipt history on the existing session. A reused native ID
 * needs another session incarnation and ledger UUID. Account/root rotation
 * needs a new attempt referencing its predecessor; a resume keeps its epoch.
 * Open declarations are cut over at read time by the next declaration.
 * Observations do not cut over or grant an explicit project choice. */
export function appendProjectIntentReceipt(previous: readonly ProjectIntentReceipt[], row: ProjectIntentReceipt):
  { receipts: ProjectIntentReceipt[]; replayed: boolean } {
  assertSupportedProjectIntentAdapter(row);
  const same = previous.find(receipt => receipt.receiptId === row.receiptId);
  if (same) {
    if (JSON.stringify(same) !== JSON.stringify(row)) throw new ProjectIntentContractError("receipt_replay_conflict");
    return { receipts: [...previous], replayed: true };
  }
  if (previous.length >= PROJECT_INTENT_MAX_RECEIPTS) throw new ProjectIntentContractError("intent_history_full");
  const first = previous[0], last = previous.at(-1);
  if (!first) {
    if (row.rootAttemptId !== row.attemptId || row.parentAttemptId !== null)
      throw new ProjectIntentContractError("attempt_lineage_invalid");
  } else {
    for (const field of ["installId", "source", "sessionId", "nativeSessionKey", "sessionEpochKey", "rootAttemptId"] as const)
      if (first[field] !== row[field]) throw new ProjectIntentContractError("session_identity_conflict");
    const attempt = previous.find(receipt => receipt.attemptId === row.attemptId);
    if (attempt) {
      if (attempt.accountKey !== row.accountKey || attempt.sourceRootKey !== row.sourceRootKey ||
        attempt.parentAttemptId !== row.parentAttemptId)
        throw new ProjectIntentContractError("attempt_identity_conflict");
    } else if (!row.parentAttemptId || !previous.some(receipt => receipt.attemptId === row.parentAttemptId)) {
      throw new ProjectIntentContractError("attempt_lineage_invalid");
    }
    if (last && Date.parse(row.effectiveFrom) < Date.parse(last.effectiveFrom))
      throw new ProjectIntentContractError("intent_time_regression");
    if (row.basis !== "repo_observation") {
      const prior = previous.filter(receipt => receipt.basis !== "repo_observation").at(-1);
      if (prior?.effectiveUntil && Date.parse(prior.effectiveUntil) > Date.parse(row.effectiveFrom))
        throw new ProjectIntentContractError("intent_interval_overlap");
      if (prior && prior.effectiveFrom === row.effectiveFrom && prior.projectKey !== row.projectKey)
        throw new ProjectIntentContractError("intent_conflict");
    }
  }
  return { receipts: [...previous, row], replayed: false };
}

/** A folder default is a suggestion. It cannot replace an explicit choice.
 * A closed/superseded declaration never revives after a later one expires. */
export function selectedProjectIntent(receipts: readonly ProjectIntentReceipt[], at: string): ProjectIntentReceipt | null {
  const time = Date.parse(at);
  if (!Number.isFinite(time)) throw new ProjectIntentContractError("intent_time_invalid");
  const declarations = receipts.filter(row => row.basis !== "repo_observation" && Date.parse(row.effectiveFrom) <= time);
  const explicit = declarations.filter(row => row.basis !== "trusted_folder_default");
  const latest = (explicit.length ? explicit : declarations).at(-1);
  return latest && (!latest.effectiveUntil || time < Date.parse(latest.effectiveUntil)) ? latest : null;
}
