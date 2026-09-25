import crypto from "node:crypto";
import type Database from "better-sqlite3";

export type CaptureSkippedRecord = {
  offset: number;
  bytes: number;
  reason: string;
  /** SHA-256 of at most the first 2048 source bytes; never source content. */
  fingerprint: string;
  kind: "codex_token_count" | "codex_non_usage" | "claude_assistant" | "claude_non_usage" | "unknown";
  usagePossible: boolean;
};

/** Read only JSON object keys at their actual nesting level. The prefix may
 * end in the middle of a value; missing or malformed discriminators remain a
 * possible usage loss. Nothing from the prefix is retained. */
function recordTypes(prefix: Buffer): {type?: string; payloadType?: string; complete: boolean} {
  const text = prefix.subarray(0, 2048).toString("utf8");
  let at = 0;
  const found: {type?: string; payloadType?: string} = {};
  const space = () => { while (at < text.length && /\s/.test(text[at]!)) at++; };
  const string = (): string | undefined => {
    const start = at++;
    for (let escaped = false; at < text.length; at++) {
      const char = text[at]!;
      if (escaped) { escaped = false; continue; }
      if (char === "\\") { escaped = true; continue; }
      if (char === '"') return JSON.parse(text.slice(start, ++at));
    }
    return undefined;
  };
  // A truncated token returns false. Structural errors throw and are unknown.
  const value = (path: "root" | "payload" | null, key: string | null, depth: number): boolean => {
    if (depth > 64) throw new Error("prefix_depth");
    space();
    if (at >= text.length) return false;
    const char = text[at]!;
    if (char === '"') {
      const parsed = string();
      if (parsed === undefined) return false;
      if (path === "root" && key === "type") found.type = parsed;
      if (path === "payload" && key === "type") found.payloadType = parsed;
      return true;
    }
    if (char === "{" || char === "[") {
      const object = char === "{";
      const childPath = depth === 0 ? "root" : path === "root" && key === "payload" ? "payload" : null;
      at++;
      space();
      if (at >= text.length) return false;
      if (text[at] === (object ? "}" : "]")) { at++; return true; }
      while (at < text.length) {
        let childKey: string | null = null;
        if (object) {
          if (text[at] !== '"') throw new Error("prefix_key");
          childKey = string() ?? null;
          if (childKey === null) return false;
          space();
          if (at >= text.length) return false;
          if (text[at++] !== ":") throw new Error("prefix_colon");
        }
        if (!value(object ? childPath : null, childKey, depth + 1)) return false;
        space();
        if (at >= text.length) return false;
        if (text[at] === (object ? "}" : "]")) { at++; return true; }
        if (text[at++] !== ",") throw new Error("prefix_separator");
        space();
      }
      return false;
    }
    if (!/[\-0-9tfn]/.test(char)) throw new Error("prefix_value");
    while (at < text.length && !/[\s,}\]]/.test(text[at]!)) at++;
    return at < text.length;
  };
  try {
    const complete = value("root", null, 0);
    space();
    return {...found,complete:complete && at===text.length};
  } catch { return {complete:false}; }
}

/** Only a short prefix is inspected. Unknown spelling or structure is always
 * a possible usage loss. No source bytes, paths, or prefix strings are stored. */
export type SkippedDiscriminatorProbe = {
  scanned: number;
  typeCount: number;
  payloadCount: number;
  typeMatch: number;
  payloadMatch: number;
  escaped: boolean;
};

const TYPE_KEY = Buffer.from('"type"');
const PAYLOAD_KEY = Buffer.from('"payload"');

export function newSkippedDiscriminatorProbe(): SkippedDiscriminatorProbe {
  return {scanned:0,typeCount:0,payloadCount:0,typeMatch:0,payloadMatch:0,escaped:false};
}

/** Count candidate key spellings over the whole record, including across read
 * slices. A backslash may spell a key with Unicode escapes. These bounded
 * counters retain no source bytes and deliberately overcount string values. */
export function observeSkippedDiscriminators(probe: SkippedDiscriminatorProbe, bytes: Buffer) {
  const count = (key: Buffer, match: "typeMatch" | "payloadMatch", hits: "typeCount" | "payloadCount", cap: number) => {
    // Only a match ending in the first key.length-1 bytes can have begun in
    // the prior slice. The native search handles every match inside this one.
    let state = probe[match];
    for (let at=0;at<Math.min(bytes.length,key.length-1);at++) {
      state = bytes[at] === key[state] ? state+1 : bytes[at] === key[0] ? 1 : 0;
      if (state === key.length) {probe[hits]=Math.min(cap,probe[hits]+1);state=1;}
    }
    for (let at=bytes.indexOf(key);at>=0 && probe[hits]<cap;at=bytes.indexOf(key,at+1))
      probe[hits]++;
    if (bytes.length >= key.length-1) {
      state=0;
      for (let at=bytes.length-key.length+1;at<bytes.length;at++)
        state=bytes[at]===key[state] ? state+1 : bytes[at]===key[0] ? 1 : 0;
    }
    probe[match]=state;
  };
  if (!probe.escaped && probe.typeCount<3 && probe.payloadCount<2) {
    probe.escaped=bytes.includes(0x5c);
    if (!probe.escaped) {count(TYPE_KEY,"typeMatch","typeCount",3);count(PAYLOAD_KEY,"payloadMatch","payloadCount",2);}
  }
  probe.scanned += bytes.length;
}

export type SkippedPrefixClassification = Pick<CaptureSkippedRecord, "kind" | "usagePossible"> &
  {nonUsageProof?: "top_type" | "payload_type"};

function classifyTypes(provider: "codex" | "claude", type?: string, payloadType?: string): SkippedPrefixClassification {
  if (provider === "codex") {
    if (type === "event_msg" && payloadType === "token_count") return { kind: "codex_token_count", usagePossible: true };
    if (type === "event_msg" && payloadType && payloadType !== "token_count")
      return {kind:"codex_non_usage",usagePossible:false,nonUsageProof:"payload_type"};
    if (type === "session_meta" || type === "turn_context" || type === "response_item")
      return {kind:"codex_non_usage",usagePossible:false,nonUsageProof:"top_type"};
  } else {
    if (type === "assistant") return { kind: "claude_assistant", usagePossible: true };
    if (type === "user") return {kind:"claude_non_usage",usagePossible:false,nonUsageProof:"top_type"};
  }
  return { kind: "unknown", usagePossible: true };
}

/** Tentative kind for a bounded prefix. A non-usage result needs the probe's
 * whole-record proof before it may be committed. */
export function classifySkippedRecordWithProof(provider: "codex" | "claude", prefix: Buffer): SkippedPrefixClassification {
  const {type,payloadType}=recordTypes(prefix);
  return classifyTypes(provider,type,payloadType);
}

export function classifySkippedRecord(provider: "codex" | "claude", prefix: Buffer):
  Pick<CaptureSkippedRecord, "kind" | "usagePossible"> {
  const {type,payloadType,complete}=recordTypes(prefix);
  const {nonUsageProof: _proof, ...classification} = classifyTypes(provider,type,payloadType);
  if (!complete && !classification.usagePossible) return {kind:"unknown",usagePossible:true};
  return classification;
}

/** A prefix classification is final only if the complete record contains no
 * extra spelling of the relevant keys. Any escape or extra candidate is a
 * possible usage loss, even when the extra bytes were inside a string. */
export function proveSkippedNonUsage(proof: SkippedPrefixClassification["nonUsageProof"],
  probe: SkippedDiscriminatorProbe | undefined): boolean {
  if (!proof || !probe || probe.escaped) return false;
  return proof === "top_type" ? probe.typeCount === 1 :
    probe.typeCount === 2 && probe.payloadCount === 1;
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
    input.record.fingerprint,
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
