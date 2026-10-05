import type Database from "better-sqlite3";
import type { AiInteractionEvent } from "../../shared/src/index";

// This is an accounting witness, not model evidence for another response.
// Only the sealing paths write it, after native capture and privacy validation.
// It retains the result across ACK/terminal replay and is bound to the complete
// raw incarnation. Neither event metadata nor a reused ID can attest a capture.
const namedCaptureSchema = new WeakMap<Database.Database, {
  version: number; probe: Database.Statement;
}>();
export function ensureCodexNamedCaptures(db: Database.Database) {
  // Native batch sealing calls this for every observation. Schema shape is
  // stable until SQLite's schema cookie changes; accounting rows and frozen
  // bytes are always read/written below, never cached here. A released writer
  // or migration changing this connection's schema invalidates this check.
  const schemaVersion = db.pragma("schema_version", { simple: true }) as number;
  const cached = namedCaptureSchema.get(db);
  if (cached?.version === schemaVersion) {
    try {
      // A DDL rollback can reuse a schema-cookie number. SQLite reparses this
      // empty query against its current schema, so a missing table/column can
      // never be hidden by the numeric cache hit. It reads no accounting row.
      cached.probe.get();
      return;
    } catch (error) {
      if (!(error instanceof Error) || !/no such (table|column)/.test(error.message)) throw error;
      namedCaptureSchema.delete(db);
    }
  }
  db.exec(`create table if not exists codex_named_capture_origin (
      singleton integer primary key check(singleton=1),legacy_native_ack_eligible integer not null
    );
    insert or ignore into codex_named_capture_origin select 1,not exists(
      select 1 from sqlite_master where type='table' and name in
        ('codex_named_captures','codex_capture_decisions','codex_model_capture_gaps'));
    create table if not exists codex_named_captures (
    delivery_id text primary key,
    raw_rowid integer not null,raw_id text not null,raw_created_at text not null,raw_generation text not null,
    envelope_json text not null,captured_event_json text not null,attempt_count integer not null default 0,
    unique(raw_rowid,raw_id,raw_created_at,raw_generation)
  );`);
  if (!(db.pragma("table_info(codex_named_captures)") as Array<{name:string}>).some(c=>c.name==="attempt_count"))
    db.exec("alter table codex_named_captures add column attempt_count integer not null default 0");
  namedCaptureSchema.set(db, {
    version: db.pragma("schema_version", { simple: true }) as number,
    probe: db.prepare(`select w.attempt_count,o.singleton,o.legacy_native_ack_eligible
      from codex_named_captures w,codex_named_capture_origin o where 0`),
  });
}

export function legacyNativeAcknowledgementsEligible(db: Database.Database) {
  if (!db.prepare("select 1 from sqlite_master where name='codex_named_capture_origin' and type='table'").get()) return false;
  return (db.prepare("select legacy_native_ack_eligible as eligible from codex_named_capture_origin where singleton=1")
    .get() as {eligible:number}|undefined)?.eligible === 1;
}

/** Released readers ignore the new witness table. Additive, ledger-local
 * triggers keep their public replay path from rebuilding a captured request
 * from diagnostics. No trigger changes a frozen request's bytes. */
export function installCodexFrozenCompatibility(db: Database.Database) {
  ensureCodexNamedCaptures(db);
  db.exec(`
    create view if not exists codex_frozen_delivery_compat as
      select delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,envelope_json,attempt_count
        from codex_named_captures
      union all select delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,
        frozen_envelope_json,coalesce(frozen_attempt_count,0) from upload_replays
        where frozen_envelope_json is not null and json_valid(frozen_envelope_json)
          and json_extract(frozen_envelope_json,'$.event.metadata.usageSource')='capture_gap';
    create trigger if not exists trg_codex_frozen_replay_insert after insert on upload_outbox
    when exists(select 1 from codex_frozen_delivery_compat f where f.raw_rowid is new.raw_rowid
      and f.raw_id is new.raw_id and f.raw_created_at is new.raw_created_at and f.raw_generation is new.raw_generation
      and f.delivery_id=new.delivery_id)
    begin
      update upload_outbox set
        base_envelope_json=(select envelope_json from codex_frozen_delivery_compat where delivery_id=new.delivery_id limit 1),
        sealed_envelope_json=(select envelope_json from codex_frozen_delivery_compat where delivery_id=new.delivery_id limit 1),
        base_bytes=(select length(cast(envelope_json as blob)) from codex_frozen_delivery_compat where delivery_id=new.delivery_id limit 1),
        sealed_bytes=(select length(cast(envelope_json as blob)) from codex_frozen_delivery_compat where delivery_id=new.delivery_id limit 1),
        attempt_count=max(attempt_count,(select attempt_count from codex_frozen_delivery_compat where delivery_id=new.delivery_id limit 1))
        where delivery_id=new.delivery_id;
    end;
    create trigger if not exists trg_codex_named_attempt after update of attempt_count on upload_outbox
    begin
      update codex_named_captures set attempt_count=max(attempt_count,new.attempt_count)
        where delivery_id=new.delivery_id and raw_rowid is new.raw_rowid and raw_id is new.raw_id
          and raw_created_at is new.raw_created_at and raw_generation is new.raw_generation;
    end;
    -- Released writers always choose the SSE as owner. If a named span was
    -- already captured, retain its financial row and classify the new exact
    -- log twin as diagnostic instead. All original payload facts remain.
    create trigger if not exists trg_codex_named_span_pair after update of usage_duplicate_reason on buffered_events
    when new.usage_duplicate_reason='codex_sse_event_span' and exists(
      select 1 from codex_named_captures w join buffered_events l on l.id=new.usage_paired_event_id
      where w.raw_rowid=new.rowid and w.raw_id=new.id and w.raw_created_at=new.created_at
        and w.raw_generation is new.privacy_generation and l.workspace_id is new.workspace_id
        and l.device_id is new.device_id and l.installation_epoch_id is new.installation_epoch_id
        and l.input_tokens=old.input_tokens and l.output_tokens=old.output_tokens
        and not exists(select 1 from codex_named_captures other where other.raw_id=l.id)
        and not exists(select 1 from upload_outbox q where q.raw_id=l.id and q.sealed_envelope_json is not null))
    begin
      update buffered_events set usage_duplicate_reason=old.usage_duplicate_reason,event_type=old.event_type,
        input_tokens=old.input_tokens,output_tokens=old.output_tokens,cache_read_tokens=old.cache_read_tokens,
        cache_creation_tokens=old.cache_creation_tokens,cost_usd=old.cost_usd where rowid=new.rowid;
      update buffered_events set usage_duplicate_reason='codex_sse_event_span',event_type='otel_span',
        input_tokens=null,output_tokens=null,cache_read_tokens=null,cache_creation_tokens=null,cost_usd=null
        where id=new.usage_paired_event_id;
    end;
    create trigger if not exists trg_codex_named_span_pair_keep before delete on upload_outbox
    when not exists(select 1 from upload_receipts where delivery_id=old.delivery_id) and exists(
      select 1 from codex_named_captures w join buffered_events s on s.rowid=w.raw_rowid and s.id=w.raw_id
        and s.created_at=w.raw_created_at and s.privacy_generation is w.raw_generation
        join buffered_events l on l.id=s.usage_paired_event_id and l.usage_duplicate_reason='codex_sse_event_span'
      where w.delivery_id=old.delivery_id and w.raw_rowid is old.raw_rowid and w.raw_id is old.raw_id
        and w.raw_created_at is old.raw_created_at and w.raw_generation is old.raw_generation
        and s.privacy_disposition is null)
    begin select raise(ignore); end;
    create trigger if not exists trg_codex_named_span_pair_skip after insert on upload_outbox
    when new.sealed_envelope_json is null and exists(
      select 1 from buffered_events l join codex_named_captures w on w.raw_id=l.usage_paired_event_id
        join buffered_events s on s.rowid=w.raw_rowid and s.id=w.raw_id and s.created_at=w.raw_created_at
          and s.privacy_generation is w.raw_generation
      where l.rowid=new.raw_rowid and l.id=new.raw_id and l.created_at=new.raw_created_at
        and l.privacy_generation is new.raw_generation and l.usage_duplicate_reason='codex_sse_event_span')
    begin delete from upload_outbox where delivery_id=new.delivery_id; end;
    create trigger if not exists trg_codex_terminal_frozen after insert on upload_receipts
    when new.terminal_state='dead' and new.reason like 'remote_%'
    begin
      insert into upload_replays (delivery_id,reason,original_terminal_at,replayed_at,replay_count,
        raw_rowid,raw_id,raw_created_at,raw_generation,frozen_envelope_json,frozen_bytes,frozen_attempt_count)
        select o.delivery_id,new.reason,new.terminal_at,new.terminal_at,0,
          o.raw_rowid,o.raw_id,o.raw_created_at,o.raw_generation,o.sealed_envelope_json,o.sealed_bytes,o.attempt_count
        from upload_outbox o where o.delivery_id=new.delivery_id and o.sealed_envelope_json is not null
          and (exists(select 1 from codex_named_captures w where w.delivery_id=o.delivery_id
            and w.raw_rowid is o.raw_rowid and w.raw_id is o.raw_id
            and w.raw_created_at is o.raw_created_at and w.raw_generation is o.raw_generation)
            or (json_valid(o.sealed_envelope_json) and
              json_extract(o.sealed_envelope_json,'$.event.source')='codex' and
              json_extract(o.sealed_envelope_json,'$.event.metadata.usageSource')='capture_gap'))
        on conflict(delivery_id) do update set
          raw_rowid=coalesce(upload_replays.raw_rowid,excluded.raw_rowid),
          raw_id=coalesce(upload_replays.raw_id,excluded.raw_id),
          raw_created_at=coalesce(upload_replays.raw_created_at,excluded.raw_created_at),
          raw_generation=coalesce(upload_replays.raw_generation,excluded.raw_generation),
          frozen_envelope_json=coalesce(upload_replays.frozen_envelope_json,excluded.frozen_envelope_json),
          frozen_bytes=coalesce(upload_replays.frozen_bytes,excluded.frozen_bytes),
          frozen_attempt_count=coalesce(upload_replays.frozen_attempt_count,excluded.frozen_attempt_count);
    end;
  `);
}

export function frozenCodexCapture(db: Database.Database, rawId: string) {
  if (!db.prepare("select 1 from sqlite_master where name='codex_named_captures' and type='table'").get())
    return undefined;
  const witness = db.prepare(`select w.delivery_id as deliveryId,w.envelope_json as envelopeJson,
      w.captured_event_json as capturedEventJson
    from codex_named_captures w join buffered_events e on e.rowid=w.raw_rowid and e.id=w.raw_id
      and e.created_at=w.raw_created_at and e.privacy_generation=w.raw_generation
    where e.id=?`).get(rawId) as
      { deliveryId: string; envelopeJson: string; capturedEventJson: string } | undefined;
  if (!witness) return undefined;
  return { ...witness, event: JSON.parse(witness.capturedEventJson) as AiInteractionEvent };
}

export function frozenCodexDelivery(db: Database.Database, lineage: {
  deliveryId: string; rawRowid: number | null; rawId: string | null;
  rawCreatedAt: string | null; rawGeneration: string | null;
}) {
  if (!db.prepare("select 1 from sqlite_master where name='codex_named_captures' and type='table'").get())
    return undefined;
  // Lease privacy/retention validation owns raw eligibility. This lookup can
  // still preserve a frozen request after ordinary raw retention removed it.
  const witness = db.prepare(`select envelope_json as envelopeJson from codex_named_captures
    where delivery_id=? and raw_rowid is ? and raw_id is ? and raw_created_at is ? and raw_generation is ?`)
    .get(lineage.deliveryId,lineage.rawRowid,lineage.rawId,lineage.rawCreatedAt,lineage.rawGeneration) as
      { envelopeJson: string } | undefined;
  return witness;
}

export function rememberFrozenCodexCapture(
  db: Database.Database, rawId: string, deliveryId: string, envelopeJson: string, captured: AiInteractionEvent,
) {
  if (db.readonly || captured.source !== "codex" || !captured.model ||
      !captured.metadata.modelCaptureSource || captured.metadata.usageSource === "capture_gap" ||
      captured.metadata.captureGap === true || captured.eventType === "usage_live" ||
      ![captured.inputTokens,captured.outputTokens,captured.cacheReadTokens,captured.cacheCreationTokens,captured.costUsd]
        .some(value => value !== undefined)) return;
  ensureCodexNamedCaptures(db);
  db.prepare(`insert or ignore into codex_named_captures
    (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,envelope_json,captured_event_json)
    select ?,rowid,id,created_at,privacy_generation,?,? from buffered_events
      where id=? and privacy_generation is not null`).run(deliveryId,envelopeJson,JSON.stringify(captured),rawId);
  // Stateless/history upload can freeze a request before the outbox leases it.
  // Freeze the matching queue copy too, including for an older reader. Use the
  // stored first witness, never a later caller's restamped candidate bytes.
  if (!db.prepare("select 1 from sqlite_master where type='table' and name='upload_outbox'").get()) return;
  db.prepare(`update upload_outbox as o set sealed_envelope_json=(select w.envelope_json
      from codex_named_captures w where w.delivery_id=o.delivery_id),
    sealed_bytes=(select length(cast(w.envelope_json as blob)) from codex_named_captures w where w.delivery_id=o.delivery_id)
    where o.delivery_id=? and o.sealed_envelope_json is null and exists(select 1 from codex_named_captures w
      where w.delivery_id=o.delivery_id and w.raw_rowid is o.raw_rowid and w.raw_id is o.raw_id
        and w.raw_created_at is o.raw_created_at and w.raw_generation is o.raw_generation)`).run(deliveryId);
}
