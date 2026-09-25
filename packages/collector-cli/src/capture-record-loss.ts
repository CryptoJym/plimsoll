import crypto from "node:crypto";
import type Database from "better-sqlite3";

export type CaptureSkippedRecord = {
  offset: number;
  bytes: number;
  reason: string;
  kind: "codex_token_count" | "codex_non_usage" | "claude_assistant" | "claude_non_usage" | "unknown";
  usagePossible: boolean;
};

/** Only a short prefix is inspected. Unknown spelling is always a possible
 * usage loss. No source bytes, paths, or prefix strings are stored. */
export function classifySkippedRecord(provider: "codex" | "claude", prefix: Buffer):
  Pick<CaptureSkippedRecord, "kind" | "usagePossible"> {
  const text = prefix.subarray(0, 2048).toString("utf8");
  const types = [...text.matchAll(/"type"\s*:\s*"([a-z_]+)"/g)].map((match) => match[1]);
  const payloadType = text.match(/"payload"\s*:\s*\{\s*"type"\s*:\s*"([a-z_]+)"/)?.[1];
  if (provider === "codex") {
    if (types[0] === "event_msg" && payloadType === "token_count") return { kind: "codex_token_count", usagePossible: true };
    if (types[0] === "event_msg" && payloadType && payloadType !== "token_count" ||
        types[0] === "session_meta" || types[0] === "turn_context" || types[0] === "response_item") {
      return { kind: "codex_non_usage", usagePossible: false };
    }
  } else {
    if (types[0] === "assistant") return { kind: "claude_assistant", usagePossible: true };
    if (types[0] === "user") return { kind: "claude_non_usage", usagePossible: false };
  }
  return { kind: "unknown", usagePossible: true };
}

export function ensureCaptureRecordLosses(database: Database.Database) {
  database.exec(`create table if not exists capture_record_losses (
    installation_epoch_id text not null,
    source text not null check(source in ('codex','claude_code')),
    identity_key text not null check(length(identity_key)=64),
    kind text not null,
    reason text not null,
    skipped_bytes integer not null check(skipped_bytes>=0),
    usage_possible integer not null check(usage_possible in (0,1)),
    detected_at text not null,
    primary key(installation_epoch_id,source,identity_key)
  ) without rowid`);
}

export function recordCaptureRecordLoss(database: Database.Database, input: {
  source: "codex" | "claude_code";
  fileKey: string;
  record: CaptureSkippedRecord;
  detectedAt?: string;
}) {
  ensureCaptureRecordLosses(database);
  const epoch = database.prepare(`select current_installation_epoch_id as id
    from collector_workspace_binding where singleton=1`).get() as { id: string | null } | undefined;
  // A collector can scan before workspace enrollment. Keep its local loss
  // receipt without assigning it to a later cloud installation epoch.
  const epochId = epoch?.id ?? "unbound";
  const identity = crypto.createHash("sha256").update(JSON.stringify([
    input.fileKey, input.record.offset, input.record.reason, input.record.kind, input.record.bytes,
  ])).digest("hex");
  database.prepare(`insert or ignore into capture_record_losses
    (installation_epoch_id,source,identity_key,kind,reason,skipped_bytes,usage_possible,detected_at)
    values (?,?,?,?,?,?,?,?)`).run(epochId, input.source, identity, input.record.kind, input.record.reason,
      input.record.bytes, input.record.usagePossible ? 1 : 0,
      input.detectedAt ?? new Date().toISOString());
}

/** Permanent gaps survive EOF. Without source content, a lost event's time is
 * unknown, so the claim conservatively covers this epoch through detection. */
export function captureRecordLossGaps(database: Database.Database, epochStartMs: number, epochId: string) {
  if (!database.prepare("select 1 from sqlite_master where type='table' and name='capture_record_losses'").get()) return [];
  const row = database.prepare(`select max(detected_at) as detectedAt
    from capture_record_losses where installation_epoch_id=? and usage_possible=1`).get(epochId) as {detectedAt:string|null};
  return row.detectedAt ? [{fromMs:epochStartMs,toMs:Math.max(epochStartMs,Date.parse(row.detectedAt))}] : [];
}
