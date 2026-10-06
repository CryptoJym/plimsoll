import type { LocalEventBuffer } from "./buffer";
import { deterministicEventId } from "./normalizer";
import {
  aiInteractionEventSchema,
  isPlanLimitWindowWithinBound,
  MAX_PLAN_LIMIT_WINDOW_MINUTES,
  type AiInteractionEvent,
} from "../../shared/src/index";

export type PlanLimitWindow = {
  window: string;
  minutes?: number;
  usedPercent: number;
  resetsAt: string;
};

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function resetInstant(value: unknown): string | undefined {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    const time = new Date(value * 1000);
    return Number.isFinite(time.getTime()) ? time.toISOString() : undefined;
  }
  if (typeof value === "string" && Number.isFinite(Date.parse(value))) return new Date(value).toISOString();
  return undefined;
}

function percentage(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

export function codexPlanLimitWindows(rateLimits: unknown): PlanLimitWindow[] {
  const limits = record(rateLimits);
  const windows: PlanLimitWindow[] = [];
  for (const slot of ["primary", "secondary"]) {
    const limit = record(limits[slot]);
    const minutes = limit.window_minutes;
    const resetsAt = resetInstant(limit.resets_at);
    if (!percentage(limit.used_percent) || !Number.isSafeInteger(minutes) ||
      (minutes as number) <= 0 || (minutes as number) > MAX_PLAN_LIMIT_WINDOW_MINUTES || !resetsAt) continue;
    windows.push({
      window: minutes === 300 ? "five_hour" : minutes === 10080 ? "weekly" : `window_${minutes}m`,
      minutes: minutes as number,
      usedPercent: limit.used_percent,
      resetsAt,
    });
  }
  return windows;
}

export function claudePlanLimitWindows(statusLineInput: unknown): PlanLimitWindow[] {
  const limits = record(record(statusLineInput).rate_limits);
  const windows: PlanLimitWindow[] = [];
  for (const [label, raw] of Object.entries(limits)) {
    const window = label === "five_hour" ? "five_hour" : label === "seven_day" ? "weekly" :
      /^seven_day_[a-z0-9_]+$/.test(label) ? `weekly_${label.slice("seven_day_".length)}` : undefined;
    if (!window) continue;
    const value = record(raw);
    const resetsAt = resetInstant(value.resets_at);
    if (!percentage(value.used_percentage) || !resetsAt) continue;
    windows.push({ window, minutes: window === "five_hour" ? 300 : 10080,
      usedPercent: value.used_percentage, resetsAt });
  }
  return windows;
}

type Observation = {
  source: "codex" | "claude_code";
  accountKey?: string;
  observedAt: string;
  window: PlanLimitWindow;
  planLimitSource: "codex_rollout" | "claude_status_line";
  planType?: string;
  planLimitId?: string;
  sessionId?: string;
  metadata?: Record<string, unknown>;
};

type LastEmission = { usedPercent: number; resetsAt: string; observedAt: string };

/** Persists only hashed account keys and provider-reported limits. */
export class PlanLimitEmitter {
  private readonly last = new Map<string, LastEmission | null>();
  private schemaReady = false;

  constructor(private readonly buffer: LocalEventBuffer) {}

  private ensureSchema() {
    if (this.schemaReady) return;
    this.buffer.database.exec(`create table if not exists plan_limit_emission_state (
      source text not null, account_key text not null, window text not null,
      used_percent real not null, resets_at text not null, observed_at text not null,
      primary key (source, account_key, window)
    )`);
    this.schemaReady = true;
  }

  observe(input: Observation): boolean {
    const { source, accountKey, window, observedAt } = input;
    if (!isPlanLimitWindowWithinBound({
      planLimitWindow: window.window,
      ...(window.minutes === undefined ? {} : { planLimitWindowMinutes: window.minutes }),
    }) || !accountKey || !/^sha256:[a-f0-9]{16}$/.test(accountKey) ||
      !Number.isFinite(Date.parse(observedAt))) return false;
    this.ensureSchema();
    const stateKey = accountKey;
    const cacheKey = JSON.stringify([source, stateKey, window.window]);
    if (!this.last.has(cacheKey)) {
      const row = this.buffer.database.prepare(`select used_percent as usedPercent, resets_at as resetsAt,
        observed_at as observedAt from plan_limit_emission_state where source=? and account_key=? and window=?`)
        .get(source, stateKey, window.window) as LastEmission | undefined;
      this.last.set(cacheKey, row ?? null);
    }
    const previous = this.last.get(cacheKey);
    if (previous && (Date.parse(observedAt) < Date.parse(previous.observedAt) ||
      (Math.abs(window.usedPercent - previous.usedPercent) < 1 &&
        window.resetsAt === previous.resetsAt &&
        Date.parse(observedAt) - Date.parse(previous.observedAt) < 15 * 60_000))) return false;

    const accountField = source === "codex" ? "user.account_id" : "user.account_uuid";
    const metadata: Record<string, unknown> = {
      ...input.metadata,
      ...(accountKey ? { [accountField]: accountKey } : {}),
      planLimitSource: input.planLimitSource,
      planLimitWindow: window.window,
      ...(window.minutes !== undefined ? { planLimitWindowMinutes: window.minutes } : {}),
      planLimitUsedPercent: window.usedPercent,
      planLimitResetsAt: window.resetsAt,
      ...(input.planType ? { planType: input.planType } : {}),
      ...(input.planLimitId ? { planLimitId: input.planLimitId } : {}),
    };
    const event: AiInteractionEvent = aiInteractionEventSchema.parse({
      id: deterministicEventId(["plan-limit", source, stateKey, window.window, window.resetsAt,
        String(Math.floor(window.usedPercent)), String(Math.floor(Date.parse(observedAt) / (15 * 60_000)))]),
      source,
      dataMode: "metadata",
      eventType: "plan_limit_observation",
      observedAt,
      ...(input.sessionId ? { sessionId: input.sessionId } : {}),
      metadata,
    });
    const appended = this.buffer.append(event);
    if (!appended) return false;
    const latest = { usedPercent: window.usedPercent, resetsAt: window.resetsAt, observedAt };
    this.buffer.database.prepare(`insert into plan_limit_emission_state
      (source,account_key,window,used_percent,resets_at,observed_at) values (?,?,?,?,?,?)
      on conflict(source,account_key,window) do update set
      used_percent=excluded.used_percent,resets_at=excluded.resets_at,observed_at=excluded.observed_at`)
      .run(source, stateKey, window.window, latest.usedPercent, latest.resetsAt, latest.observedAt);
    this.last.set(cacheKey, latest);
    return true;
  }
}
