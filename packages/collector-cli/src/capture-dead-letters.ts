import type Database from "better-sqlite3";
import { aiInteractionEventSchema } from "../../shared/src/index";
import type { CaptureGap } from "./capture-frontier";

export type CaptureDeadLetter = {
  eventType: string;
  reason: string;
  count: number;
  tokens: number | null;
  costUsd: number | null;
};

export type CaptureDeadLetterInterval = CaptureGap & { deadLetters: CaptureDeadLetter[] };

const receiptReasons = [
  "local_evidence_quarantined", "local_payload_unparseable", "local_schema_invalid",
  "local_privacy_violation", "local_item_oversize", "local_usage_duplicate",
  "remote_rejected_exhausted", "remote_validation_rejected",
];
const sqlNames = (values: readonly string[]) => values.map(value => `'${value}'`).join(",");
const tokenKeys = ["inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens"];
const zeroOrAbsent = (key: string) => `(json_type(payload, '$.${key}') is null or
  (json_type(payload, '$.${key}') in ('integer','real') and json_extract(payload, '$.${key}') = 0))`;
const planHasNoUsage = `eventType = 'plan_limit_observation' and
  json_extract(payload, '$.eventType') = eventType and
  ${[...tokenKeys, "costUsd"].map(zeroOrAbsent).join(" and ")}`;

/** Exact receipt counts for one closed interval. Read only admitted fields,
 * never copy a payload into the census. A missing amount poisons its sum. */
export function captureDeadLetterCensus(
  database: Database.Database, epochStartedAt: string, interval: CaptureGap,
): CaptureDeadLetter[] | undefined {
  const rows = database.prepare(`
    with receipts as (
      select case when b.event_type in (${sqlNames(aiInteractionEventSchema.shape.eventType.options)})
          then b.event_type else 'unknown' end as eventType,
        case when r.reason in (${sqlNames(receiptReasons)}) then r.reason else 'unknown_receipt_reason' end as reason,
        case when json_valid(b.payload_json) then b.payload_json else '{}' end as payload,
        coalesce(unixepoch(b.observed_at, 'subsec'), @epochStartSeconds) as f,
        coalesce(unixepoch(b.observed_at, 'subsec'), unixepoch(r.created_at, 'subsec'), @epochStartSeconds) as t
      from upload_receipts r left join buffered_events b on b.id = r.delivery_id
      where r.terminal_state = 'dead' and r.created_at >= @epochStartedAt
    ), amounts as (
      select eventType, reason,
        case when ${planHasNoUsage} then 0
          when json_extract(payload, '$.eventType') = eventType and
            json_type(payload, '$.inputTokens') = 'integer' and json_extract(payload, '$.inputTokens') >= 0 and
            json_type(payload, '$.outputTokens') = 'integer' and json_extract(payload, '$.outputTokens') >= 0
          then json_extract(payload, '$.inputTokens') + json_extract(payload, '$.outputTokens') end as tokens,
        case when ${planHasNoUsage} then 0
          when json_extract(payload, '$.eventType') = eventType and
            json_type(payload, '$.costUsd') in ('integer','real') and json_extract(payload, '$.costUsd') >= 0
          then json_extract(payload, '$.costUsd') end as costUsd
      from receipts where t >= @fromSeconds and f <= @toSeconds
    )
    select eventType, reason, count(*) as count,
      case when count(tokens) = count(*) then total(tokens) end as tokens,
      case when count(costUsd) = count(*) then sum(costUsd) end as costUsd
    from amounts group by eventType, reason order by eventType, reason
  `).all({ epochStartedAt, epochStartSeconds: Date.parse(epochStartedAt) / 1000,
    fromSeconds: interval.fromMs / 1000, toSeconds: interval.toMs / 1000 }) as CaptureDeadLetter[];
  // Never truncate a census: an omitted summary keeps the whole interval a gap.
  if (!rows.length || rows.length > 16 || rows.some(row => row.count > 2_147_483_647)) return undefined;
  return rows.map(row => ({ ...row,
    tokens: row.tokens !== null && Number.isSafeInteger(row.tokens) ? row.tokens : null,
    costUsd: row.costUsd !== null && Number.isFinite(row.costUsd) ? row.costUsd : null,
  }));
}
