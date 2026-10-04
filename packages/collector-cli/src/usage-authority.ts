import type Database from "better-sqlite3";
import type { AiInteractionEvent } from "../../shared/src/index";
import { captureCodexModel, codexHasUsage, codexMisfiledUnderClaude, isCaptureGap } from "./codex-model-capture";
import { CODEX_SESSION_AUTHORITY_SQL, isCodexResponseSpan } from "./codex-span-rollout-pairing";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";

// Raw counters are diagnostics until the capture contract admits usage. The
// same predicate governs ingest, the native tailer and projection suppression.
export function rowHasAdmittedUsage(db: Database.Database, rawId: string, sessionAuthority = false): boolean {
  const row = db.prepare(`select payload_json as payload,usage_duplicate_reason as duplicate,workspace_id as workspace,
    device_id as device,installation_epoch_id as epoch
    from buffered_events where id=?`).get(rawId) as { payload: string; duplicate: string | null; workspace: string | null; device: string | null; epoch: string | null } | undefined;
  if (!row || row.duplicate) return false;
  let event: AiInteractionEvent;
  try { event = JSON.parse(row.payload); } catch { return false; }
  if (event.source !== "codex" && !codexMisfiledUnderClaude(event)) return true;
  if (sessionAuthority) {
    const binding = db.prepare(`select current_workspace_id as workspace,current_device_id as device,
      current_installation_epoch_id as epoch from collector_workspace_binding where singleton=1`).get() as
      {workspace:string;device:string|null;epoch:string|null} | undefined;
    if (!binding?.epoch || row.workspace !== binding.workspace || row.device !== binding.device || row.epoch !== binding.epoch)
      return false;
  }
  if (event.eventType === "usage_live") return !sessionAuthority;
  if (!codexHasUsage(event) || (sessionAuthority && isCodexResponseSpan(event))) return false;
  const captured = captureCodexModel(db, event, rawId, false, false);
  return !isCaptureGap(captured) &&
    [captured.inputTokens, captured.outputTokens, captured.cacheReadTokens,
      captured.cacheCreationTokens, captured.costUsd].some(value => value !== undefined);
}

export function rowCanOwnSessionUsage(db: Database.Database, rawId: string) {
  return rowHasAdmittedUsage(db, rawId, true);
}

export function hasSessionUsageAuthority(
  db: Database.Database, source: string, sessionId: string, kind: "live" | "tailer",
  excludeRowids: number[] = [], probeRows?: number, commitCoverage?: (rawId: string) => boolean,
): boolean | "undecided" {
  // Codex witnesses cover responses, never an entire conversation. Keep the
  // old API conservative for sibling/older local readers.
  if (source === "codex") return false;
  const candidates = probeRows === undefined ? "buffered_events" : `(select rowid,* from buffered_events
    where source=@source and session_id=@session order by observed_at desc limit @probe)`;
  const eligible = source === "codex" ? terminalPrivacyEligibilitySql(db, "e") : "1";
  const query = db.prepare(`select e.id,e.observed_at as at from ${candidates} e
    where e.source=@source and e.session_id=@session and ${CODEX_SESSION_AUTHORITY_SQL}
      and ${eligible} ${source === "codex" ? "and e.usage_duplicate_reason is null" : ""}
      and e.event_type ${kind === "live" ? "not" : ""} in ('usage_rollout','usage_transcript')
      and (e.input_tokens is not null or e.output_tokens is not null
        or e.cache_read_tokens is not null or e.cache_creation_tokens is not null or e.cost_usd is not null)
      and e.rowid not in (select value from json_each(@excluded))
      and (@cursorAt is null or e.observed_at<@cursorAt or (e.observed_at=@cursorAt and e.id<@cursorId))
    order by e.observed_at desc,e.id desc limit 128`);
  if (hasAdmittedWitness(db,query,source,{source,session:sessionId,excluded:JSON.stringify(excludeRowids),
    ...(probeRows === undefined ? {} : {probe:probeRows})},commitCoverage)) return true;
  if (probeRows !== undefined) {
    const { n } = db.prepare(`select count(*) as n from (select 1 from buffered_events
      where source=? and session_id=? order by observed_at desc limit ?)`)
      .get(source, sessionId, probeRows + 1) as { n: number };
    if (n > probeRows) return "undecided";
  }
  return false;
}

function hasAdmittedWitness(db: Database.Database,query: Database.Statement,source: string,parameters: Record<string,unknown>,
  commitCoverage?: (rawId: string) => boolean) {
  let cursorAt: string | null = null, cursorId: string | null = null;
  // Finish each bounded SQL page before capture reads schema/lineage. An
  // active SQLite iterator would prevent capture's schema-cookie pragma.
  while (true) {
    const rows = query.all({...parameters,cursorAt,cursorId}) as Array<{id:string;at:string}>;
    for (const row of rows) if ((source !== "codex" || rowCanOwnSessionUsage(db,row.id)) &&
      (!commitCoverage || commitCoverage(row.id))) return true;
    if (rows.length<128) break;
    cursorAt = rows[rows.length-1]!.at; cursorId = rows[rows.length-1]!.id;
  }
  return false;
}

export function hasUnkeyedLiveUsageOverlap(db: Database.Database,source: string,from: string,to: string) {
  const eligible = terminalPrivacyEligibilitySql(db,"e");
  const query = db.prepare(`select e.id,e.observed_at as at from buffered_events e
    where e.source=@source and e.session_id is null and e.observed_at between @from and @to
      and ${CODEX_SESSION_AUTHORITY_SQL} and ${eligible}
      and e.event_type not in ('usage_rollout','usage_transcript')
      and (e.input_tokens is not null or e.output_tokens is not null or e.cache_read_tokens is not null
        or e.cache_creation_tokens is not null or e.cost_usd is not null)
      and (@cursorAt is null or e.observed_at<@cursorAt or (e.observed_at=@cursorAt and e.id<@cursorId))
    order by e.observed_at desc,e.id desc limit 128`);
  return hasAdmittedWitness(db,query,source,{source,from,to});
}
