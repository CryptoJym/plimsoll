import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import type Database from "better-sqlite3";

/**
 * Capture health: does the ledger actually contain what ran on this machine?
 *
 * v1 died silently for 2.5 weeks; on 2026-06-10 ten of eleven codex sessions
 * captured zero usage while the dashboard lamp read "collecting" (issue 0021).
 * The baseline for "should we be receiving telemetry" is LOCAL TOOL ACTIVITY —
 * Claude Code transcript files and codex rollout files — not wall-clock
 * freshness, so a sleeping laptop stays green and a broken pipe goes red the
 * moment the tools are demonstrably in use without the ledger hearing it.
 */

export type SourceHealthStatus = "green" | "amber" | "red";

export type SourceHealth = {
  source: "claude_code" | "codex";
  lastEventAt: string | null;
  lastTokenEventAt: string | null;
  localLastActivityAt: string | null;
  /** Local artifact files touched today (UTC): transcripts / rollouts. */
  localSessionsToday: number;
  ledgerSessionsToday: number;
  tokenSessionsToday: number;
  status: SourceHealthStatus;
  reason: string;
};

export type CaptureHealth = {
  generatedAt: string;
  overall: SourceHealthStatus;
  sources: SourceHealth[];
};

/** Local activity must appear in the ledger within this lag. */
export const CAPTURE_LAG_LIMIT_MS = 10 * 60 * 1000;
/** Only activity this recent demands fresh capture (older = session over). */
export const ACTIVITY_LOOKBACK_MS = 60 * 60 * 1000;

export type CaptureHealthOptions = {
  claudeProjectsDir?: string;
  codexSessionsDir?: string;
  now?: Date;
};

type LocalActivity = { lastMtime: Date | null; filesToday: number };

/** Recursive .jsonl scan, bounded; missing dir is normal (tool not installed). */
function scanActivity(root: string, today: string, limit = 50_000): LocalActivity {
  let lastMtime: Date | null = null;
  let filesToday = 0;
  let seen = 0;
  const stack = [root];
  while (stack.length > 0) {
    const dir = stack.pop()!;
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      if (seen++ > limit) return { lastMtime, filesToday };
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        stack.push(full);
        continue;
      }
      if (!entry.name.endsWith(".jsonl")) continue;
      let stat: fs.Stats;
      try {
        stat = fs.statSync(full);
      } catch {
        continue;
      }
      if (!lastMtime || stat.mtime > lastMtime) lastMtime = stat.mtime;
      if (stat.mtime.toISOString().slice(0, 10) === today) filesToday += 1;
    }
  }
  return { lastMtime, filesToday };
}

function ledgerSide(db: Database.Database, source: string, today: string) {
  const row = db
    .prepare(
      `select max(observed_at) as lastEventAt,
        max(case when input_tokens is not null then observed_at end) as lastTokenEventAt,
        count(distinct case when substr(observed_at, 1, 10) = ? then session_id end) as ledgerSessionsToday,
        count(distinct case when substr(observed_at, 1, 10) = ? and input_tokens is not null then session_id end) as tokenSessionsToday
      from buffered_events where source = ?`,
    )
    .get(today, today, source) as {
    lastEventAt: string | null;
    lastTokenEventAt: string | null;
    ledgerSessionsToday: number;
    tokenSessionsToday: number;
  };
  return row;
}

function minutes(ms: number) {
  return `${Math.round(ms / 60_000)}m`;
}

function judge(
  source: "claude_code" | "codex",
  ledger: ReturnType<typeof ledgerSide>,
  local: LocalActivity,
  now: Date,
): { status: SourceHealthStatus; reason: string } {
  if (!local.lastMtime) {
    return { status: "green", reason: "no local activity observed — quiet is expected" };
  }
  const activityAge = now.getTime() - local.lastMtime.getTime();
  const lastEventMs = ledger.lastEventAt ? Date.parse(ledger.lastEventAt) : null;
  const lag = lastEventMs === null ? Number.POSITIVE_INFINITY : local.lastMtime.getTime() - lastEventMs;

  if (activityAge <= ACTIVITY_LOOKBACK_MS && lag > CAPTURE_LAG_LIMIT_MS) {
    return {
      status: "red",
      reason: `local activity ${minutes(activityAge)} ago but last captured event ${
        ledger.lastEventAt ? `${minutes(now.getTime() - lastEventMs!)} ago` : "never"
      } — telemetry is not reaching the collector`,
    };
  }
  if (local.filesToday > 0 && ledger.tokenSessionsToday === 0) {
    return {
      status: "red",
      reason: `${local.filesToday} local session file(s) touched today, 0 sessions captured with tokens`,
    };
  }
  if (source === "codex" && local.filesToday > 0 && ledger.tokenSessionsToday * 2 < local.filesToday) {
    return {
      status: "amber",
      reason: `${local.filesToday} rollout(s) today, only ${ledger.tokenSessionsToday} with token capture — see issue 0022`,
    };
  }
  return {
    status: "green",
    reason: `capture current — ${ledger.tokenSessionsToday} session(s) with tokens today`,
  };
}

const WORST: Record<SourceHealthStatus, number> = { green: 0, amber: 1, red: 2 };

export function computeCaptureHealth(
  db: Database.Database,
  options: CaptureHealthOptions = {},
): CaptureHealth {
  const now = options.now ?? new Date();
  const today = now.toISOString().slice(0, 10);
  const probes: Array<{ source: "claude_code" | "codex"; dir: string }> = [
    {
      source: "claude_code",
      dir: options.claudeProjectsDir ?? path.join(os.homedir(), ".claude", "projects"),
    },
    {
      source: "codex",
      dir: options.codexSessionsDir ?? path.join(os.homedir(), ".codex", "sessions"),
    },
  ];

  const sources = probes.map(({ source, dir }) => {
    const local = scanActivity(dir, today);
    const ledger = ledgerSide(db, source, today);
    const verdict = judge(source, ledger, local, now);
    return {
      source,
      lastEventAt: ledger.lastEventAt,
      lastTokenEventAt: ledger.lastTokenEventAt,
      localLastActivityAt: local.lastMtime ? local.lastMtime.toISOString() : null,
      localSessionsToday: local.filesToday,
      ledgerSessionsToday: ledger.ledgerSessionsToday,
      tokenSessionsToday: ledger.tokenSessionsToday,
      status: verdict.status,
      reason: verdict.reason,
    } satisfies SourceHealth;
  });

  const overall = sources.reduce<SourceHealthStatus>(
    (worst, s) => (WORST[s.status] > WORST[worst] ? s.status : worst),
    "green",
  );

  return { generatedAt: now.toISOString(), overall, sources };
}
