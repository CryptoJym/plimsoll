import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { z } from "zod";
import {
  appendProjectIntentReceipt, PROJECT_INTENT_PATH, PROJECT_INTENT_SCHEMA,
  projectIntentReceiptSchema, projectIntentRequestSchema, type ProjectIntentReceipt,
} from "../../shared/src/project-intent";
import { collectorHome, assertCollectorPrivacyMode, type CollectorConfig } from "./config";
import { authenticatedJsonGet, authenticatedJsonPost, TransportError, validatedTransportUrl } from "./http-transport";
import { ensureUuidSessionId } from "./session-sync";
import { openLedgerDatabase } from "./ledger-connection";
import {
  canonicalIdentity, canonicalSourceRoot, intentAccountKey, intentDigest, IntentError,
  intentWorkItemKey, sealIntentReceipt, type IntentSource,
} from "./project-intent-identity";
import { INTENT_STATE_DIRECTORY, INTENT_STATE_LIMITS, IntentStore } from "./project-intent-store";

const key = z.string().regex(/^sha256:[0-9a-f]{64}$/);
const revision = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
const uuid = z.string().uuid();
const dispatchSchema = z.object({ attempts: revision.nullable(), pending: z.boolean(), possibleAdmission: z.boolean() }).strict();
const newDispatch = (): z.infer<typeof dispatchSchema> => ({ attempts: 0, pending: false, possibleAdmission: false });
export const intentChoiceSchema = z.object({ projectKey: key, projectLabel: z.string().max(200),
  projectRegistryRevision: revision.refine(value => value > 0) }).strict();
const choicesSchema = z.object({ schema: z.literal("plimsoll-project-intent-projects/v1"),
  projects: z.array(intentChoiceSchema).max(200), nextCursor: key.nullable() }).strict();
export type IntentChoice = z.infer<typeof intentChoiceSchema>;
const ackSchema = z.object({ schema: z.literal("plimsoll-project-intent-ack/v1"),
  acknowledged: z.literal(true), receiptId: uuid, sessionId: uuid, replayed: z.boolean(),
  revision, receiptRevision: revision.refine(value => value > 0),
  project: z.object({ state: z.enum(["known", "unknown"]), projectKey: key.nullable(),
    company: z.string().max(200).nullable(), registryRevision: revision.nullable(),
    reason: z.enum(["needs_project", "project_not_registered"]).nullable(),
    companyReason: z.enum(["needs_project", "project_not_registered", "company_mapping_missing", "company_mapping_invalid"]).nullable(),
  }).strict(),
}).strict().refine(row => row.revision >= row.receiptRevision);
const staleSchema = z.object({ error: z.literal("intent_revision_stale"), receiptId: uuid, sessionId: uuid, revision }).strict();
const REFUSALS = new Set(["device_revoked", "install_tenant_mismatch", "project_registry_stale", "unsupported_adapter",
  "unsupported_source", "invalid_project_intent", "receipt_replay_conflict", "intent_history_full", "session_identity_conflict",
  "attempt_identity_conflict", "attempt_lineage_invalid", "intent_time_regression", "intent_interval_overlap", "intent_conflict",
  "session_not_found", "unauthorized", "forbidden", "service_unavailable"]);
function refusal(body: unknown): string {
  const value = body && typeof body === "object" ? (body as { error?: unknown }).error : null;
  return typeof value === "string" && REFUSALS.has(value) ? value : "intent_remote_refused";
}
function definitiveRefusal(status: number, body: unknown): boolean {
  return status >= 400 && status < 500 && body !== null && typeof body === "object" &&
    Object.keys(body).length === 1 && refusal(body) !== "intent_remote_refused" && refusal(body) !== "service_unavailable";
}

type Scope = { installId: string; tenantId: string; audience: string; ledgerEpoch: string | null };
type Registry = { scope: Scope; fetchedAt: string; projects: IntentChoice[]; refused: string | null };
export type IntentDraft = {
  source: IntentSource; sourceRootPath: string | null; accountKey: string | null; workItemKey: string | null;
  projectKey: string | null; projectRegistryRevision: number | null; observedRepoKey: string | null;
  effectiveFrom: string; effectiveUntil: string | null; basis: ProjectIntentReceipt["basis"];
  sessionId: null; nativeSessionKey: null; sessionEpochKey: null;
};
export type IntentLaunch = {
  schema: "plimsoll-project-intent-launch/v1"; launchId: string; scope: Scope;
  localState: "awaiting_native_binding" | "bound"; receiptDraft: IntentDraft;
  active: boolean; closedAt: string | null; bindings: Array<{ sessionId: string; receiptId: string }>;
};
type QueuedReceipt = { receipt: ProjectIntentReceipt; expectedRevision: number; delivered: boolean;
  launchId: string;
  staleRecoveries: number; supersededBy: string | null;
  dispatch?: z.infer<typeof dispatchSchema>; supersession?: { refusedReceiptId: string };
  ack: z.infer<typeof ackSchema> | null; review: string | null; evidence: { sourceRootPath: string; incarnationId: string } };
export type IntentSession = {
  scope: Scope; source: IntentSource; sessionId: string; nativeSessionKey: string; incarnationId: string;
  observedRevision: number; receipts: QueuedReceipt[]; lease: { id: string; until: number } | null;
};
const sameScope = (a: Scope, b: Scope) => JSON.stringify(a) === JSON.stringify(b);

function provedUnadmitted(row: QueuedReceipt): boolean {
  const parsed = dispatchSchema.safeParse(row.dispatch);
  return !row.delivered && row.ack === null && parsed.success && parsed.data.attempts !== null &&
    !parsed.data.pending && !parsed.data.possibleAdmission;
}
/** A refused queue head also blocks every later proposal. Retire the branch only with durable non-admission proof. */
function renewalBranch(rows: QueuedReceipt[]): QueuedReceipt[] {
  const start = rows.findIndex(row => !row.delivered && !row.supersededBy && row.review === "project_registry_stale");
  if (start === -1) return [];
  const branch = rows.slice(start).filter(row => !row.supersededBy);
  if (branch.some(row => !provedUnadmitted(row))) throw new IntentError("intent_renewal_admission_unproved");
  return branch;
}
function settleDispatch(row: QueuedReceipt, unadmitted: boolean): void {
  const parsed = dispatchSchema.safeParse(row.dispatch);
  if (!parsed.success) throw new IntentError("intent_dispatch_evidence_invalid");
  row.dispatch = { ...parsed.data, pending: false, possibleAdmission: parsed.data.possibleAdmission || !unadmitted };
}
/** Superseded proposals remain immutable evidence outside the admissible P02 history. */
function admissionHistory(rows: QueuedReceipt[], renewing: QueuedReceipt[] = []): ProjectIntentReceipt[] {
  let history: ProjectIntentReceipt[] = [];
  for (const row of rows) {
    if (row.supersededBy || renewing.includes(row)) {
      // Keep compatibility with already-superseded R2 refusals; new branch links require explicit proof.
      const legacyRefusal = row.supersededBy && !row.supersession && row.review === "project_registry_stale";
      const refusal = rows.find(candidate => candidate.receipt.receiptId === row.supersession?.refusedReceiptId);
      const branchProof = refusal?.review === "project_registry_stale" && provedUnadmitted(refusal) && provedUnadmitted(row);
      if (row.delivered || row.ack !== null || !(legacyRefusal || branchProof || renewing.includes(row) && provedUnadmitted(row)))
        throw new IntentError("intent_supersession_unproved");
      continue;
    }
    try { history = appendProjectIntentReceipt(history, row.receipt).receipts; }
    catch { throw new IntentError("intent_lineage_unproved"); }
  }
  return history;
}

/** Every remote call uses the joined upload audience and install signature. No credential arguments. */
export class ProjectIntentProducer {
  readonly store: IntentStore;
  readonly scope: Scope;
  private readonly endpoint: string;
  private readonly ledgerPath: string;
  constructor(readonly config: CollectorConfig, readonly options: {
    directory?: string; fetchImpl?: typeof fetch; now?: () => Date;
  } = {}) {
    assertCollectorPrivacyMode(config, "project intent");
    if (!config.uploadUrl || !config.uploadSigningSecret || config.installKey === "local-dev" ||
      !uuid.safeParse(config.cloudDeviceId).success || !uuid.safeParse(config.tenantId).success)
      throw new IntentError("intent_install_not_registered");
    const audience = validatedTransportUrl(config.uploadUrl, "Configured upload URL").origin;
    this.ledgerPath = path.join(options.directory ?? collectorHome(), "work-ledger.sqlite");
    let ledgerEpoch: string | null = null;
    if (fs.existsSync(this.ledgerPath)) {
      const ledger = openLedgerDatabase(this.ledgerPath, { readonly: true, fileMustExist: true });
      try {
        const binding = ledger.prepare("select current_workspace_id as tenantId, current_installation_epoch_id as epoch from collector_workspace_binding where singleton=1").get() as { tenantId: string; epoch: string } | undefined;
        if (!binding || binding.tenantId !== config.tenantId || !uuid.safeParse(binding.epoch).success)
          throw new IntentError("intent_ledger_binding_unavailable");
        ledgerEpoch = binding.epoch.toLowerCase();
      } finally { ledger.close(); }
    }
    this.scope = { installId: config.cloudDeviceId!.toLowerCase(), tenantId: config.tenantId.toLowerCase(), audience, ledgerEpoch };
    this.endpoint = new URL(PROJECT_INTENT_PATH, audience).href;
    this.store = new IntentStore(options.directory ?? collectorHome());
  }
  private now() { return (this.options.now ?? (() => new Date()))(); }
  private requestOptions() {
    return { installKey: this.config.installKey, signingSecret: this.config.uploadSigningSecret!,
      fetchImpl: this.options.fetchImpl, now: () => this.now(), timeoutMs: Math.min(30_000, this.config.delivery.requestTimeoutSeconds * 1000) };
  }
  private assertScope(scope: Scope) {
    if (!sameScope(scope, this.scope)) throw new IntentError("intent_install_binding_changed");
  }
  private assertNotRevoked() {
    const cached = this.store.read<Registry>("registry", "current");
    if (cached && sameScope(cached.scope, this.scope) && cached.refused)
      throw new IntentError(cached.refused);
  }
  async choices(allowCached = false): Promise<{ projects: IntentChoice[]; cached: boolean }> {
    const projects: IntentChoice[] = [];
    let after: string | null = null;
    const cursors = new Set<string>();
    const started = performance.now();
    try {
      for (let page = 0; page < 32; page++) {
        const remainingMs = 30_000 - (performance.now() - started);
        if (remainingMs <= 0) throw new IntentError("intent_choices_deadline");
        const url = new URL(this.endpoint); url.searchParams.set("tenantId", this.scope.tenantId);
        if (after) url.searchParams.set("after", after);
        const response = await authenticatedJsonGet({ ...this.requestOptions(), timeoutMs: Math.min(this.requestOptions().timeoutMs, remainingMs), url: url.href });
        if (response.status !== 200) {
          const code = refusal(response.body);
          if (response.status === 401 || response.status === 403)
            this.store.mutate<Registry, void>("registry", "current", () => ({ state: {
              scope: this.scope, fetchedAt: this.now().toISOString(), projects: [], refused: code,
            }, result: undefined }));
          throw new IntentError(code);
        }
        const parsed = choicesSchema.safeParse(response.body);
        if (!parsed.success) throw new IntentError("invalid_intent_choices");
        for (const choice of parsed.data.projects) {
          if (projects.some(previous => previous.projectKey === choice.projectKey)) throw new IntentError("invalid_intent_choices");
          projects.push(choice);
        }
        after = parsed.data.nextCursor;
        if (!after) {
          this.store.mutate<Registry, void>("registry", "current", () => ({ state: {
            scope: this.scope, fetchedAt: this.now().toISOString(), projects, refused: null,
          }, result: undefined }));
          return { projects, cached: false };
        }
        if (cursors.has(after)) throw new IntentError("invalid_intent_choices");
        cursors.add(after);
      }
      throw new IntentError("intent_choices_page_bound");
    } catch (error) {
      // Only an ambiguous transport failure may use previously authenticated choices.
      if (!allowCached || !(error instanceof TransportError) || !["network_error", "deadline_exceeded"].includes(error.code)) throw error;
      const cached = this.store.read<Registry>("registry", "current");
      if (!cached || !sameScope(cached.scope, this.scope) || cached.refused ||
        this.now().getTime() - Date.parse(cached.fetchedAt) > 24 * 60 * 60 * 1000 ||
        this.now().getTime() < Date.parse(cached.fetchedAt)) throw new IntentError("intent_choices_unavailable");
      return { projects: cached.projects.map(project => intentChoiceSchema.parse(project)), cached: true };
    }
  }
  /** Authorization comes only from the signed registry (or its bounded, scoped offline copy). */
  declare(input: {
    source: IntentSource; sourceRoot?: string; project?: IntentChoice | null;
    basis?: ProjectIntentReceipt["basis"]; observedRepoKey?: string | null; principal?: string;
    work?: { authority: string; namespace: string; id: string }; launchId?: string;
  }): IntentLaunch {
    this.assertNotRevoked();
    const project = input.project ? intentChoiceSchema.parse(input.project) : null;
    if (project) {
      const registry = this.store.read<Registry>("registry", "current");
      if (!registry || !sameScope(registry.scope, this.scope) || !registry.projects.some(choice =>
        choice.projectKey === project.projectKey && choice.projectRegistryRevision === project.projectRegistryRevision))
        throw new IntentError("project_not_registered");
      const age = this.now().getTime() - Date.parse(registry.fetchedAt);
      if (age < 0 || age > 24 * 60 * 60 * 1000) throw new IntentError("intent_choices_unavailable");
    }
    const basis = input.basis ?? "hand_start";
    if (basis === "repo_observation" && project) throw new IntentError("observation_cannot_declare_project");
    const launchId = uuid.parse(input.launchId ?? crypto.randomUUID()).toLowerCase();
    // Validate the locator now, but retain it locally even if state has not appeared yet.
    // NFC is a wire identity rule; using the normalized path to reopen a directory is unsafe.
    canonicalSourceRoot(input.sourceRoot);
    const draft: IntentDraft = {
      source: input.source, sourceRootPath: input.sourceRoot ?? null,
      accountKey: intentAccountKey(input.source, input.principal), workItemKey: intentWorkItemKey(input.work),
      projectKey: project?.projectKey ?? null, projectRegistryRevision: project?.projectRegistryRevision ?? null,
      observedRepoKey: input.observedRepoKey ? key.parse(input.observedRepoKey) : null,
      effectiveFrom: this.now().toISOString(), effectiveUntil: null, basis,
      sessionId: null, nativeSessionKey: null, sessionEpochKey: null,
    };
    return this.store.mutate<IntentLaunch, IntentLaunch>("launches", launchId, previous => {
      if (previous) throw new IntentError("intent_launch_already_exists");
      const state: IntentLaunch = { schema: "plimsoll-project-intent-launch/v1", launchId, scope: this.scope,
        localState: "awaiting_native_binding", receiptDraft: draft, active: true, closedAt: null, bindings: [] };
      return { state, result: state };
    });
  }
  bind(launchId: string, nativeId: string, input: {
    continuation?: boolean; hook?: boolean; sourceRoot?: string; principal?: string;
  } = {}): ProjectIntentReceipt | null {
    this.assertNotRevoked();
    const native = canonicalIdentity(nativeId);
    return this.store.mutate<IntentLaunch, ProjectIntentReceipt | null>("launches", uuid.parse(launchId).toLowerCase(), launch => {
      if (!launch) throw new IntentError("intent_launch_missing");
      if (!this.scope.ledgerEpoch) return { state: launch, result: null };
      // A draft made before ledger initialization can acquire that first binding exactly once.
      if (launch.scope.ledgerEpoch === null && launch.bindings.length === 0) launch.scope.ledgerEpoch = this.scope.ledgerEpoch;
      this.assertScope(launch.scope);
      if (input.hook && !launch.active) throw new IntentError("intent_launch_closed");
      const draft = launch.receiptDraft;
      const root = canonicalSourceRoot(input.sourceRoot ?? draft.sourceRootPath ?? undefined);
      if (!root) return { state: launch, result: null };
      const sourceRootKey = intentDigest("source-root", [this.scope.installId, draft.source, root]);
      const nativeSessionKey = intentDigest("native-session", [this.scope.installId, draft.source, native]);
      // This is the existing event/session-sync join. A separate generated P02 session UUID would not join usage.
      const sessionId = ensureUuidSessionId(nativeId).id;
      const priorLaunchBinding = launch.bindings.find(binding => binding.sessionId === sessionId);
      const row = this.store.mutate<IntentSession, ProjectIntentReceipt>("sessions", sessionId, previous => {
        if (!previous && priorLaunchBinding) throw new IntentError("intent_continuity_unproved");
        if (previous) {
          this.assertScope(previous.scope);
          if (previous.source !== draft.source || previous.nativeSessionKey !== nativeSessionKey)
            throw new IntentError("intent_session_binding_conflict");
          if (!priorLaunchBinding && !previous.receipts.some(item => item.launchId === launchId) && !input.continuation)
            throw new IntentError("native_id_reused_requires_fresh_ledger_binding");
        } else if (input.continuation) {
          throw new IntentError("intent_continuity_unproved");
        } else {
          const ledger = openLedgerDatabase(this.ledgerPath, { readonly: true, fileMustExist: true });
          try {
            const older = ledger.prepare("select 1 from buffered_events where source=? and session_id=? and observed_at < ? limit 1")
              .get(draft.source, nativeId, draft.effectiveFrom);
            if (older) throw new IntentError("intent_continuity_unproved");
          } finally { ledger.close(); }
        }
        const session: IntentSession = previous ?? { scope: this.scope, source: draft.source, sessionId, nativeSessionKey,
          incarnationId: crypto.randomUUID(), observedRevision: 0, receipts: [], lease: null };
        const accountKey = input.principal === undefined ? draft.accountKey : intentAccountKey(draft.source, input.principal);
        // Recover publication across a crash without reviving a superseded declaration.
        const existing = session.receipts.filter(item => item.launchId === launchId).at(-1);
        if (existing && existing.receipt.accountKey === accountKey && existing.receipt.sourceRootKey === sourceRootKey)
          return { state: session, result: existing.receipt };
        if (existing?.supersededBy) throw new IntentError("intent_declaration_superseded");
        const renewing = renewalBranch(session.receipts);
        if (renewing.some(item => item.launchId === launchId))
          throw new IntentError("intent_registry_renewal_requires_new_declaration");
        if (renewing.length && session.lease && session.lease.until > Date.now()) throw new IntentError("intent_delivery_busy");
        // Include accepted and ambiguous facts. Only a new declaration can retire the proved unadmitted suffix.
        const history = admissionHistory(session.receipts, renewing);
        const last = history.at(-1);
        const sameAttempt = last && last.accountKey === accountKey && last.sourceRootKey === sourceRootKey;
        const attemptId = sameAttempt ? last.attemptId : crypto.randomUUID();
        const effectiveFrom = priorLaunchBinding || launch.bindings.length > 0 ? this.now().toISOString() : draft.effectiveFrom;
        const receipt = sealIntentReceipt({
          schema: PROJECT_INTENT_SCHEMA, receiptId: crypto.randomUUID(), installId: this.scope.installId,
          source: draft.source, sourceRootKey, accountKey, sessionId, nativeSessionKey,
          sessionEpochKey: intentDigest("session-epoch", [this.scope.installId, draft.source, nativeSessionKey, session.incarnationId]),
          rootAttemptId: last?.rootAttemptId ?? attemptId, attemptId,
          parentAttemptId: sameAttempt ? last.parentAttemptId : last?.attemptId ?? null,
          workItemKey: draft.workItemKey, projectKey: draft.projectKey, projectRegistryRevision: draft.projectRegistryRevision,
          observedRepoKey: draft.observedRepoKey, effectiveFrom, effectiveUntil: draft.effectiveUntil, basis: draft.basis,
          adapterId: draft.basis === "routed_launch" ? "routed-launch" : draft.basis === "repo_observation" ? "repo-observation" : "hand-start",
          adapterVersion: "1.0.0",
        });
        appendProjectIntentReceipt(history, receipt);
        for (const refused of renewing) {
          refused.supersededBy = receipt.receiptId;
          refused.supersession = { refusedReceiptId: renewing[0].receipt.receiptId };
        }
        session.receipts.push({ receipt, launchId, staleRecoveries: 0, supersededBy: null, expectedRevision: session.observedRevision, delivered: false, ack: null,
          dispatch: newDispatch(), review: null, evidence: { sourceRootPath: root, incarnationId: session.incarnationId } });
        return { state: session, result: receipt };
      });
      launch.localState = "bound";
      launch.receiptDraft.sourceRootPath = input.sourceRoot ?? draft.sourceRootPath;
      launch.receiptDraft.accountKey = row.accountKey;
      const existing = launch.bindings.find(binding => binding.sessionId === row.sessionId);
      if (existing) existing.receiptId = row.receiptId;
      else launch.bindings.push({ sessionId: row.sessionId, receiptId: row.receiptId });
      return { state: launch, result: row };
    });
  }
  close(launchId: string): void {
    this.store.mutate<IntentLaunch, void>("launches", launchId, launch => {
      if (!launch) throw new IntentError("intent_launch_missing");
      this.assertScope(launch.scope);
      if (!launch.active) return { state: launch, result: undefined };
      const until = this.now().toISOString();
      if (launch.receiptDraft.basis === "trusted_folder_default") {
        // Close a suggestion by appending a null cutover, never rewriting an emitted receipt.
        for (const binding of launch.bindings) this.store.mutate<IntentSession, void>("sessions", binding.sessionId, session => {
          if (!session) throw new IntentError("intent_continuity_unproved");
          if (session.receipts.some(item => item.launchId === `${launchId}-close`)) return { state: session, result: undefined };
          const last = session.receipts.at(-1)!.receipt;
          if (last.receiptId !== binding.receiptId || last.basis !== "trusted_folder_default") return { state: session, result: undefined };
          const from = new Date(Math.max(Date.parse(until), Date.parse(last.effectiveFrom) + 1)).toISOString();
          const receipt = sealIntentReceipt({ ...last, receiptId: crypto.randomUUID(), projectKey: null,
            projectRegistryRevision: null, effectiveFrom: from, effectiveUntil: null, basis: "hand_start", adapterId: "hand-start" });
          appendProjectIntentReceipt(admissionHistory(session.receipts), receipt);
          session.receipts.push({ receipt, launchId: `${launchId}-close`, staleRecoveries: 0, supersededBy: null, expectedRevision: session.observedRevision, delivered: false, ack: null,
            dispatch: newDispatch(), review: null, evidence: session.receipts.at(-1)!.evidence });
          return { state: session, result: undefined };
        });
        launch.receiptDraft.effectiveUntil = until;
      }
      launch.active = false; launch.closedAt = until;
      return { state: launch, result: undefined };
    });
  }
  private review(sessionId: string, receiptId: string, code: string, leaseId: string, unadmitted = false) {
    this.store.mutate<IntentSession, void>("sessions", sessionId, state => {
      if (!state || state.lease?.id !== leaseId) throw new IntentError("intent_delivery_lease_lost");
      const item = state.receipts.find(row => row.receipt.receiptId === receiptId)!;
      settleDispatch(item, unadmitted);
      item.review = code;
      return { state, result: undefined };
    });
  }
  async sendSession(sessionId: string, maxRequests = 3): Promise<{
    delivered: number; queued: number | null; retainedRefusals: number | null; reason: string | null;
  }> {
    this.assertNotRevoked();
    const leaseId = crypto.randomUUID();
    const acquired = this.store.mutate<IntentSession, boolean>("sessions", sessionId, state => {
      if (!state) throw new IntentError("intent_session_missing");
      this.assertScope(state.scope);
      if (state.lease && state.lease.until > Date.now()) return { state, result: false };
      state.lease = { id: leaseId, until: Date.now() + 120_000 };
      return { state, result: true };
    });
    if (!acquired) return { delivered: 0, queued: null, retainedRefusals: null, reason: "intent_delivery_busy" };
    let delivered = 0, reason: string | null = null;
    try {
      for (let send = 0; send < Math.min(3, maxRequests); send++) {
        const session = this.store.read<IntentSession>("sessions", sessionId)!;
        const pending = session.receipts.find(item => !item.delivered && !item.supersededBy);
        if (!pending) break;
        if (pending.review && !["intent_transport_deferred", "intent_revision_stale", "invalid_intent_ack", "session_not_found", "service_unavailable"].includes(pending.review)) {
          reason = pending.review; break;
        }
        if (session.lease?.id !== leaseId) throw new IntentError("intent_delivery_lease_lost");
        const parsed = projectIntentReceiptSchema.parse(pending.receipt);
        if (JSON.stringify(parsed) !== JSON.stringify(pending.receipt) || sealIntentReceipt(parsed).evidenceRef !== parsed.evidenceRef)
          throw new IntentError("intent_receipt_changed");
        const body = JSON.stringify(projectIntentRequestSchema.parse({ tenantId: this.scope.tenantId,
          expectedRevision: Math.max(session.observedRevision, pending.expectedRevision), receipt: parsed }));
        // Save before the network call: an interrupted dispatch or any prior ambiguity cannot be renewed away.
        this.store.mutate<IntentSession, void>("sessions", sessionId, state => {
          if (!state || state.lease?.id !== leaseId || state.lease.until <= Date.now())
            throw new IntentError("intent_delivery_lease_lost");
          const item = state.receipts.find(row => row.receipt.receiptId === parsed.receiptId)!;
          if (item.delivered || item.supersededBy) throw new IntentError("intent_delivery_lease_lost");
          const recorded = dispatchSchema.safeParse(item.dispatch);
          if (item.dispatch !== undefined && !recorded.success) throw new IntentError("intent_dispatch_evidence_invalid");
          const prior = recorded.success ? recorded.data : { attempts: null, pending: false, possibleAdmission: true };
          if (prior.attempts === Number.MAX_SAFE_INTEGER) throw new IntentError("intent_dispatch_limit");
          item.dispatch = { attempts: prior.attempts === null ? null : prior.attempts + 1, pending: true,
            possibleAdmission: prior.possibleAdmission || prior.pending };
          return { state, result: undefined };
        });
        let response;
        try { response = await authenticatedJsonPost({ ...this.requestOptions(), url: this.endpoint, body }); }
        catch { reason = "intent_transport_deferred"; this.review(sessionId, parsed.receiptId, reason, leaseId); break; }
        const ack = response.status === 202 ? ackSchema.safeParse(response.body) : null;
        if (ack?.success && ack.data.receiptId === parsed.receiptId && ack.data.sessionId === sessionId) {
          this.store.mutate<IntentSession, void>("sessions", sessionId, state => {
            if (!state || state.lease?.id !== leaseId) throw new IntentError("intent_delivery_lease_lost");
            const item = state.receipts.find(row => row.receipt.receiptId === parsed.receiptId)!;
            settleDispatch(item, false);
            item.ack = ack.data; item.delivered = true;
            item.review = ack.data.project.reason ?? ack.data.project.companyReason;
            state.observedRevision = Math.max(state.observedRevision, ack.data.revision);
            return { state, result: undefined };
          });
          delivered++; continue;
        }
        const stale = response.status === 409 ? staleSchema.safeParse(response.body) : null;
        if (stale?.success && stale.data.receiptId === parsed.receiptId && stale.data.sessionId === sessionId) {
          this.store.mutate<IntentSession, void>("sessions", sessionId, state => {
            if (!state || state.lease?.id !== leaseId) throw new IntentError("intent_delivery_lease_lost");
            state.observedRevision = Math.max(state.observedRevision, stale.data.revision);
            const item = state.receipts.find(row => row.receipt.receiptId === parsed.receiptId)!;
            settleDispatch(item, true);
            item.expectedRevision = state.observedRevision;
            item.staleRecoveries++;
            item.review = item.staleRecoveries >= 3 ? "intent_revision_retry_exhausted" : "intent_revision_stale";
            return { state, result: undefined };
          });
          reason = this.store.read<IntentSession>("sessions", sessionId)!.receipts.find(item => item.receipt.receiptId === parsed.receiptId)!.review;
          continue;
        }
        reason = response.status === 202 ? "invalid_intent_ack" : refusal(response.body);
        this.review(sessionId, parsed.receiptId, reason, leaseId, definitiveRefusal(response.status, response.body));
        if (response.status === 401 || response.status === 403)
          this.store.mutate<Registry, void>("registry", "current", () => ({ state: {
            scope: this.scope, fetchedAt: this.now().toISOString(), projects: [], refused: reason,
          }, result: undefined }));
        if (reason === "project_registry_stale") {
          // Refresh available choices, retain the refused facts. Only an explicit new declaration may renew authority.
          try { await this.choices(); } catch { /* The immutable refused receipt stays queued for review. */ }
        }
        break;
      }
    } finally {
      this.store.mutate<IntentSession, void>("sessions", sessionId, state => {
        if (!state) throw new IntentError("intent_session_missing");
        if (state.lease?.id === leaseId) state.lease = null;
        return { state, result: undefined };
      });
    }
    const receipts = this.store.read<IntentSession>("sessions", sessionId)!.receipts;
    const queued = receipts.filter(item => !item.delivered && !item.supersededBy).length;
    const retainedRefusals = receipts.filter(item => !item.delivered && item.supersededBy !== null).length;
    return { delivered, queued, retainedRefusals, reason: queued ? reason : null };
  }
  async replay(maxSessions: number = INTENT_STATE_LIMITS.replaySessions, maxRequests = 3) {
    let delivered = 0, queued = 0, retainedRefusals = 0;
    const reasons: string[] = [];
    const ids = this.store.ids("sessions");
    const cursor = this.store.read<{ after: string | null }>("registry", "replay")?.after;
    const candidates = [...ids.filter(id => !cursor || id > cursor), ...ids.filter(id => cursor && id <= cursor)]
      .slice(0, Math.min(INTENT_STATE_LIMITS.replaySessions, maxSessions));
    const examined: string[] = [];
    for (const sessionId of candidates) {
      examined.push(sessionId);
      if (!this.store.read<IntentSession>("sessions", sessionId)?.receipts.some(row => !row.delivered)) continue;
      const result = await this.sendSession(sessionId, maxRequests);
      delivered += result.delivered; queued += result.queued ?? 0;
      retainedRefusals += result.retainedRefusals ?? 0;
      if (result.reason) reasons.push(result.reason);
      if (result.reason === "device_revoked" || result.reason === "unauthorized" || result.reason === "install_tenant_mismatch") break;
    }
    this.store.mutate<{ after: string | null }, void>("registry", "replay", () => ({
      state: { after: examined.at(-1) ?? null }, result: undefined,
    }));
    return { delivered, queued, retainedRefusals, queueCoverage: ids.length > examined.length || reasons.includes("intent_delivery_busy") ? "partial" : "complete",
      unexaminedSessions: ids.length - examined.length, reasons };
  }
}

/** Additive upload cycle: never creates intent state for installations that have not used the producer. */
export async function replayProjectIntentsIfPresent(config: CollectorConfig) {
  if (!fs.existsSync(path.join(collectorHome(), INTENT_STATE_DIRECTORY))) return null;
  try { return await new ProjectIntentProducer(config).replay(1, 1); }
  catch (error) { return { delivered: 0, queued: null, retainedRefusals: null, reasons: [error instanceof IntentError ? error.code : "intent_replay_deferred"] }; }
}
