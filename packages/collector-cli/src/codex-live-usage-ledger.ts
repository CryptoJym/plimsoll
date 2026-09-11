import type Database from "better-sqlite3";
import type { LocalEventBuffer } from "./buffer";
import type { LiveAuthenticatedBinding, LiveSourceContext } from "./codex-live-usage-auth";
import { isAuthenticatedLiveBinding } from "./codex-live-usage-auth";
import type { CaptureRoot } from "./capture-root-inventory";
import type { MetricSample } from "./otlp";
import { HttpBoundaryRejection } from "./http-boundary";
import { readLiveUsageObservation, type AiInteractionEvent } from "../../shared/src/index";
import { canonicalJson, hasLiveUsageClaim, LIVE_COUNTERS, LIVE_SCHEMA, liveEventId, liveReceipt,
  liveSha256, validLiveTotals, type LiveDisposition, type LivePacket, type LiveReceipt, type LiveTotals,
  type LiveUsagePacket } from "./codex-live-usage-protocol";

type DB = Database.Database;
export function ensureCodexLiveUsageSchema(db: DB) {
  db.exec(`
    create table if not exists codex_live_producers (
      producer_id text primary key, context_digest text not null, context_json text not null,
      credential_id text not null, enabled integer not null check(enabled in (0,1)),
      unique(producer_id,context_digest)
    );
    create table if not exists codex_live_bindings (
      producer_id text not null, credential_id text not null, scope_digest text not null unique,
      context_digest text not null, token_sha256 text not null, enrolled_at text not null,
      revoked integer not null check(revoked in (0,1)), primary key(producer_id,credential_id),
      foreign key(producer_id,context_digest) references codex_live_producers(producer_id,context_digest)
    );
    create table if not exists codex_live_pins (
      source text not null check(source='codex'), session_id text not null,
      producer_id text not null, context_digest text not null, context_json text not null,
      claimed_at text not null, primary key(source,session_id),
      foreign key(producer_id,context_digest) references codex_live_producers(producer_id,context_digest)
    );
    create table if not exists codex_live_attachments (
      scope_digest text not null, attachment_id text not null, thread_id text not null,
      checkpoint_json text check(checkpoint_json is null or length(cast(checkpoint_json as blob)) <= 8192),
      held_reason text, primary key(scope_digest,attachment_id),
      foreign key(scope_digest) references codex_live_bindings(scope_digest)
    );
    create table if not exists codex_live_packet_keys (
      scope_digest text not null, kind text not null check(kind in ('usage','gap')),
      attachment_id text not null, packet_key text not null, packet_digest text not null,
      primary key(scope_digest,kind,attachment_id,packet_key),
      foreign key(scope_digest) references codex_live_bindings(scope_digest)
    );
    create table if not exists codex_live_receipts (
      scope_digest text not null, kind text not null, attachment_id text not null,
      packet_key text not null, packet_digest text not null,
      receipt_json text not null check(length(cast(receipt_json as blob)) <= 2048),
      primary key(scope_digest,kind,attachment_id,packet_key,packet_digest),
      foreign key(scope_digest,kind,attachment_id,packet_key)
        references codex_live_packet_keys(scope_digest,kind,attachment_id,packet_key)
    );
    create table if not exists codex_live_diagnostics (
      disposition text primary key check(disposition in ('stored','baseline_only','gap','counter_reset','authority_conflict','collision')),
      count integer not null, last_at text not null
    );
    create trigger if not exists codex_live_producer_context_immutable before update on codex_live_producers
      when new.producer_id<>old.producer_id or new.context_digest<>old.context_digest or new.context_json<>old.context_json
      begin select raise(abort,'live_context_immutable'); end;
    create trigger if not exists codex_live_binding_immutable before update on codex_live_bindings
      when new.producer_id<>old.producer_id or new.credential_id<>old.credential_id or new.scope_digest<>old.scope_digest
        or new.context_digest<>old.context_digest or new.token_sha256<>old.token_sha256 or new.enrolled_at<>old.enrolled_at
        or new.revoked<old.revoked
      begin select raise(abort,'live_binding_immutable'); end;
  `);
  for (const table of ["codex_live_producers", "codex_live_bindings", "codex_live_pins", "codex_live_packet_keys", "codex_live_receipts"]) {
    db.exec(`create trigger if not exists ${table}_no_delete before delete on ${table}
      begin select raise(abort,'live_history_immutable'); end;`);
    if (!["codex_live_producers", "codex_live_bindings"].includes(table)) db.exec(`
      create trigger if not exists ${table}_no_update before update on ${table}
      begin select raise(abort,'live_history_immutable'); end;`);
  }
}

/** Provisioning changes credential activity, never a logical source context or session pin. */
export function registerLiveCredential(db: DB, auth: LiveAuthenticatedBinding) {
  const work = () => {
    const existing = db.prepare(`select context_digest from codex_live_producers where producer_id=?`)
      .get(auth.context.producerId) as { context_digest: string } | undefined;
    if (existing && existing.context_digest !== auth.contextDigest) throw new Error("live_producer_rebinding_forbidden");
    if (db.prepare(`select 1 from codex_live_bindings where producer_id=? and credential_id=?`)
      .get(auth.context.producerId, auth.binding.credentialId)) throw new Error("live_credential_id_reused");
    db.prepare(`insert into codex_live_producers(producer_id,context_digest,context_json,credential_id,enabled)
      values(?,?,?,?,1) on conflict(producer_id) do update set credential_id=excluded.credential_id,enabled=1`)
      .run(auth.context.producerId, auth.contextDigest, canonicalJson(auth.context), auth.binding.credentialId);
    db.prepare(`update codex_live_bindings set revoked=1 where producer_id=?`).run(auth.context.producerId);
    db.prepare(`insert into codex_live_bindings values(?,?,?,?,?,?,0)`)
      .run(auth.context.producerId, auth.binding.credentialId, auth.scopeDigest, auth.contextDigest,
        auth.binding.tokenSha256, auth.binding.enrolledAt);
  };
  if (db.inTransaction) return work();
  db.transaction(work).immediate();
}
export function revokeLiveProducer(db: DB, producerId: string) {
  db.transaction(() => {
    db.prepare(`update codex_live_producers set enabled=0 where producer_id=?`).run(producerId);
    db.prepare(`update codex_live_bindings set revoked=1 where producer_id=?`).run(producerId);
  }).immediate();
}

type Pin = { producer_id: string; context_digest: string; context_json: string };
function readPin(db: DB, sessionId: string) {
  return db.prepare(`select producer_id,context_digest,context_json from codex_live_pins where source='codex' and session_id=?`)
    .get(sessionId) as Pin | undefined;
}
function pinMatches(pin: Pin | undefined, auth: LiveAuthenticatedBinding) {
  // The logical producer digest deliberately excludes installation epoch.
  // A required failover epoch therefore starts a new packet/dedupe scope but
  // retains the same session authority instead of conflicting with its own
  // immutable pre-failover pin (whose context_json records the old epoch).
  return Boolean(pin && pin.producer_id === auth.context.producerId && pin.context_digest === auth.contextDigest);
}
const liveEventCapabilities = new WeakMap<AiInteractionEvent, { database: DB; auth: LiveAuthenticatedBinding }>();
export function liveUsageMetricAllowed(db: DB, sample: MetricSample) {
  return !hasLiveUsageClaim(sample.attrs) && !(sample.source === "codex" && sample.sessionId &&
    /token/i.test(sample.metricName) && readPin(db, sample.sessionId));
}
/** All append paths share this gate. JSON/client metadata cannot create a WeakMap capability. */
export function liveUsageAppendAllowed(db: DB, event: AiInteractionEvent) {
  const capability = liveEventCapabilities.get(event);
  const observer = event.eventType === "usage_live" || hasLiveUsageClaim(event.metadata);
  const tokens = [event.inputTokens, event.outputTokens, event.cacheReadTokens, event.cacheCreationTokens].some(v => v !== undefined);
  const pin = event.source === "codex" && event.sessionId ? readPin(db, event.sessionId) : undefined;
  if (!capability) return !observer && !(pin && tokens);
  if (capability.database !== db || event.source !== "codex" || !event.sessionId || !pinMatches(pin, capability.auth) ||
      !readLiveUsageObservation(event.metadata ?? {}, event.observedAt)) return false;
  const current = db.prepare(`select p.enabled,b.revoked,p.credential_id,b.context_digest
    from codex_live_producers p join codex_live_bindings b using(producer_id)
    where p.producer_id=? and b.credential_id=?`).get(capability.auth.context.producerId, capability.auth.binding.credentialId) as
    { enabled: number; revoked: number; credential_id: string; context_digest: string } | undefined;
  return Boolean(current?.enabled && !current.revoked && current.credential_id === capability.auth.binding.credentialId &&
    current.context_digest === capability.auth.contextDigest);
}

type Account = NonNullable<CaptureRoot["account"]>;
type Work = NonNullable<CaptureRoot["dispatch"]>[number];
type Snapshot = { at: string; contextDigest: string; account: Account | null; work: Work | null;
  accountDigest: string | null; workDigest: string | null };
type Checkpoint = { seq: number; packetDigest: string; capturedAt: string; total: LiveTotals; snapshot: Snapshot };
type Attachment = { thread_id: string; checkpoint_json: string | null; held_reason: string | null };
function contains(at: string, window: { validFrom: string; validUntil: string | null }) {
  return Date.parse(window.validFrom) <= Date.parse(at) && (!window.validUntil || Date.parse(at) < Date.parse(window.validUntil));
}
function snapshot(auth: LiveAuthenticatedBinding, p: LiveUsagePacket): Snapshot {
  const accountCandidates = [...new Map(
    [ ...(auth.root.accountAssertions ?? []), ...(auth.root.account ? [auth.root.account] : []) ]
      .map(account => [`${account.validFrom}\u0000${account.evidenceRef}`, account] as const),
  ).values()];
  const matchingAccounts = accountCandidates.filter(candidate => contains(p.capturedAt, candidate));
  const account = matchingAccounts.length === 1 ? { ...matchingAccounts[0] } : null;
  const matches = (auth.root.dispatch ?? []).filter(w => w.sessionId === p.threadId && contains(p.capturedAt, w));
  const work = matches.length === 1 ? { ...matches[0] } : null;
  return { at: p.capturedAt, contextDigest: auth.contextDigest, account, work,
    accountDigest: account ? liveSha256(canonicalJson(account)) : null,
    workDigest: work ? liveSha256(canonicalJson(work)) : null };
}
function wholeWindow<T extends { validFrom: string; validUntil: string | null }>(oldValue: T | null,
  newValue: T | null, previous: string, current: string): T | null {
  if (!oldValue || !newValue || previous >= current || canonicalJson(oldValue) !== canonicalJson(newValue) ||
      !contains(previous, newValue) || !contains(current, newValue)) return null;
  return newValue;
}
function intervalEvent(auth: LiveAuthenticatedBinding, p: LiveUsagePacket, digest: string,
  prior: Checkpoint, current: Snapshot, delta: LiveTotals): AiInteractionEvent {
  let account = wholeWindow(prior.snapshot.account, current.account, prior.capturedAt, p.capturedAt);
  let work = wholeWindow(prior.snapshot.work, current.work, prior.capturedAt, p.capturedAt);
  if (prior.snapshot.contextDigest !== current.contextDigest) { account = null; work = null; }
  // Also reject a binding that overlaps any part of the interval even if the
  // chosen binding happens to be unique at both endpoint instants.
  if (work && (auth.root.dispatch ?? []).filter(w => w.sessionId === p.threadId &&
      Date.parse(w.validFrom) <= Date.parse(p.capturedAt) &&
      (!w.validUntil || Date.parse(w.validUntil) > Date.parse(prior.capturedAt))).length !== 1) work = null;
  const id = liveEventId(p);
  const metadata: Record<string, string | number> = {
    sourceVersion: LIVE_SCHEMA, sourceEventId: id, logicalSourceEventId: id, sourcePayloadDigest: digest,
    sourceIdentityEvidenceRef: "native_runtime_observed_interval_v1",
    captureRootId: auth.context.captureRootId, captureProfileId: auth.context.profileId,
    installationEpochId: auth.context.installationEpochId, liveObservationKind: "observed_interval",
    liveIntervalStart: prior.capturedAt, liveIntervalEnd: p.capturedAt,
    liveAttributionState: account && work ? "qualified" : "unresolved", liveFinanceEligibility: "unqualified_observer",
    liveTotalTokens: delta.totalTokens, liveReasoningOutputTokens: delta.reasoningOutputTokens,
    ...(account ? { captureAccountHash: account.actorHash, accountEvidenceRef: account.evidenceRef } : {}),
    ...(work ? { workItemId: work.workItemId, dispatchProjectKey: work.projectKey, workEvidenceRef: work.evidenceRef,
      attemptId: work.attemptId, ...(work.parentAttemptId ? { parentAttemptId: work.parentAttemptId } : {}),
      ...(work.companyRef ? { companyRef: work.companyRef } : {}),
      ...(work.acceptedOutcomeId ? { acceptedOutcomeId: work.acceptedOutcomeId } : {}) } : {}),
  };
  if (!readLiveUsageObservation(metadata, p.capturedAt)) throw new Error("live_interval_invalid");
  return { id, source: "codex", dataMode: "metadata", eventType: "usage_live", sessionId: p.threadId,
    tenantId: auth.context.workspaceId, observedAt: p.capturedAt, intent: "unknown", actionClass: "other",
    inputTokens: delta.inputTokens, outputTokens: delta.outputTokens, cacheReadTokens: delta.cachedInputTokens,
    cacheCreationTokens: delta.cacheWriteInputTokens, metadata };
}
class LiveEventCollision extends Error {}

/** Caller authenticates before parsing and provides the same fresh check again at transaction entry. */
export function ingestLiveUsage(buffer: LocalEventBuffer, packet: LivePacket, digest: string,
  recheck: () => LiveAuthenticatedBinding): LiveReceipt {
  const db = buffer.database;
  try {
    return db.transaction(() => buffer.transactionWithRepoContextHandoffs(() => {
      const auth = recheck();
      if (!isAuthenticatedLiveBinding(auth)) throw new HttpBoundaryRejection("producer_token_invalid", 403);
      // Echo checks always precede any scoped lookup, including a known retry.
      if (packet.producerId !== auth.binding.producerId || packet.credentialId !== auth.binding.credentialId ||
          packet.capturedAt < auth.binding.enrolledAt || buffer.eventAdmissionReason(packet.capturedAt, auth.context.installationEpochId))
        return liveReceipt(packet, digest, "enrollment_rejected", false, null);
      const key = packet.kind === "usage" ? String(packet.observationSeq) : packet.controlId;
      const identity = [auth.scopeDigest, packet.kind, packet.attachmentId, key];
      const oldReceipt = db.prepare(`select receipt_json from codex_live_receipts where scope_digest=? and kind=? and attachment_id=? and packet_key=? and packet_digest=?`)
        .get(...identity, digest) as { receipt_json: string } | undefined;
      if (oldReceipt) {
        const receipt = JSON.parse(oldReceipt.receipt_json) as LiveReceipt;
        if (receipt.replayed !== false || receipt.committed !== true || canonicalJson(liveReceipt(packet, digest,
          receipt.disposition, true, receipt.committedObservationSeq)) !== oldReceipt.receipt_json) throw new Error("live_receipt_invalid");
        return { ...receipt, replayed: true };
      }
      let attachment = db.prepare(`select thread_id,checkpoint_json,held_reason from codex_live_attachments where scope_digest=? and attachment_id=?`)
        .get(auth.scopeDigest, packet.attachmentId) as Attachment | undefined;
      const prior = attachment?.checkpoint_json ? JSON.parse(attachment.checkpoint_json) as Checkpoint : null;
      const record = (disposition: LiveDisposition, accepted: Checkpoint | null = prior, hold = false) => {
        if (hold) {
          db.prepare(`insert into codex_live_attachments(scope_digest,attachment_id,thread_id,checkpoint_json,held_reason)
            values(?,?,?,null,?) on conflict(scope_digest,attachment_id) do update set held_reason=coalesce(held_reason,excluded.held_reason)`)
            .run(auth.scopeDigest, packet.attachmentId, packet.threadId, disposition);
        }
        db.prepare(`insert into codex_live_packet_keys values(?,?,?,?,?) on conflict do nothing`).run(...identity, digest);
        const receipt = liveReceipt(packet, digest, disposition, true, accepted?.seq ?? null);
        db.prepare(`insert into codex_live_receipts values(?,?,?,?,?,?)`).run(...identity, digest, canonicalJson(receipt));
        db.prepare(`insert into codex_live_diagnostics values(?,1,?) on conflict(disposition)
          do update set count=min(2147483647,count+1),last_at=excluded.last_at`).run(disposition, packet.capturedAt);
        return receipt;
      };
      const known = db.prepare(`select packet_digest from codex_live_packet_keys where scope_digest=? and kind=? and attachment_id=? and packet_key=?`)
        .get(...identity) as { packet_digest: string } | undefined;
      if (known || (attachment && attachment.thread_id !== packet.threadId)) return record("collision", prior, true);
      const pin = readPin(db, packet.threadId);
      const authority = buffer.sessionUsageAuthority("codex", packet.threadId);
      const legacyTokens = !pin && (db.prepare(`select 1 from buffered_events where source='codex' and session_id=? and
        (input_tokens is not null or output_tokens is not null
         or cache_read_tokens is not null or cache_creation_tokens is not null) limit 1`).get(packet.threadId) ||
        db.prepare(`select 1 from metric_samples where source='codex' and session_id=? and metric_name like '%token%' limit 1`).get(packet.threadId));
      if ((pin && (!pinMatches(pin, auth) || authority !== "live")) || (!pin && authority))
        return record("authority_conflict", prior, true);
      if (legacyTokens) return record("authority_conflict", prior, true);
      if (packet.kind === "gap" || attachment?.held_reason) return record("gap", prior, true);
      if ((!prior && (packet.observationSeq !== 1 || packet.previousDigest !== null)) ||
          (prior && (packet.observationSeq !== prior.seq + 1 || packet.previousDigest !== prior.packetDigest || packet.capturedAt < prior.capturedAt)))
        return record("gap", prior, true);
      if (!validLiveTotals(packet.total)) return record("counter_reset", prior, true);
      const delta = Object.fromEntries(LIVE_COUNTERS.map(k => [k, packet.total[k] - (prior?.total[k] ?? packet.total[k])])) as LiveTotals;
      if (LIVE_COUNTERS.some(k => delta[k] < 0) || !validLiveTotals(delta)) return record("counter_reset", prior, true);
      if (!pin) {
        // IMMEDIATE transaction plus read-after-insert preserves the actual winner.
        db.prepare(`insert into session_usage_authority(source,session_id,authority,claimed_at) values('codex',?,'live',?) on conflict do nothing`)
          .run(packet.threadId, packet.capturedAt);
        db.prepare(`insert into codex_live_pins values('codex',?,?,?,?,?) on conflict do nothing`)
          .run(packet.threadId, auth.context.producerId, auth.contextDigest, canonicalJson(auth.context), packet.capturedAt);
        if (!pinMatches(readPin(db, packet.threadId), auth) || buffer.sessionUsageAuthority("codex", packet.threadId) !== "live")
          throw new Error("live_authority_race");
      }
      const current = snapshot(auth, packet);
      const next: Checkpoint = { seq: packet.observationSeq, packetDigest: digest, capturedAt: packet.capturedAt,
        total: packet.total, snapshot: current };
      try {
        return db.transaction(() => {
          if (prior && LIVE_COUNTERS.some(k => delta[k] > 0)) {
            const event = intervalEvent(auth, packet, digest, prior, current, delta);
            liveEventCapabilities.set(event, { database: db, auth });
            try {
              const result = buffer.append(event, [], { integrityReceipt: true });
              if (result.collisionQuarantined) throw new LiveEventCollision();
              if (!result.appended && !result.deduplicated) throw new Error("live_event_not_appended");
            } finally { liveEventCapabilities.delete(event); }
          }
          db.prepare(`insert into codex_live_attachments(scope_digest,attachment_id,thread_id,checkpoint_json,held_reason)
            values(?,?,?,?,null) on conflict(scope_digest,attachment_id) do update set checkpoint_json=excluded.checkpoint_json`)
            .run(auth.scopeDigest, packet.attachmentId, packet.threadId, canonicalJson(next));
          return record(prior ? "stored" : "baseline_only", next);
        })();
      } catch (error) {
        if (error instanceof LiveEventCollision) return record("collision", prior, true);
        throw error;
      }
    })).immediate();
  } catch (error) {
    if (error instanceof HttpBoundaryRejection) throw error;
    return liveReceipt(packet, digest, "retryable", false, null);
  }
}

export function liveUsageDiagnostics(db: DB) {
  return db.prepare(`select disposition,count,last_at as lastAt from codex_live_diagnostics order by disposition`).all();
}
