import assert from "node:assert/strict";
import Database from "better-sqlite3";

import { budgetHostClass, ensureBudgetSchema } from "../packages/collector-cli/src/budget-sampler";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  BUDGET_RETENTION_FLOOR_DAYS,
  DEFAULT_RETENTION_DAYS,
  RAW_BYTES_PER_ROW_ESTIMATE,
  deriveRetentionPolicy,
  resolveRetentionPolicy,
} from "../packages/collector-cli/src/retention-policy";

/**
 * 0.7.52: the effective raw retention. A configured value other than the
 * schema default, or a pinned one, always wins. A `studio0_scale` host with
 * seven complete measured days takes the budget-derived default (floored at
 * 7, capped at 90). Every other host keeps the default.
 */

const DAY_MS = 86_400_000;
const now = new Date("2026-11-01T18:00:00.000Z");
const day = (offset: number) => new Date(now.getTime() + offset * DAY_MS).toISOString().slice(0, 10);

function ledger(options: { startedDaysAgo: number; days: number; attemptedRowsPerDay: number }) {
  const db = new Database(":memory:");
  ensureBudgetSchema(db);
  db.prepare("update budget_control set started_day = ? where singleton = 1").run(day(-options.startedDaysAgo));
  const insert = db.prepare("insert into budget_daily (day, attempted_rows) values (?, ?)");
  for (let index = 1; index <= options.days; index += 1) insert.run(day(-index), options.attemptedRowsPerDay);
  return db;
}

const checks: string[] = [];
function check(name: string, run: () => void) {
  run();
  checks.push(name);
}

check("a ledger without budget tables keeps the schema default", () => {
  const policy = resolveRetentionPolicy(collectorConfigSchema.parse({}), new Database(":memory:"), now);
  assert.deepEqual(
    { days: policy.days, source: policy.source, hostClass: policy.basis.hostClass, pinned: policy.pinned },
    { days: DEFAULT_RETENTION_DAYS, source: "default", hostClass: null, pinned: false },
  );
});

check("the config schema materializes retentionDaysPinned", () => {
  assert.equal(collectorConfigSchema.parse({}).retentionDaysPinned, false);
  assert.equal(collectorConfigSchema.parse({ retentionDaysPinned: true }).retentionDaysPinned, true);
  assert.equal(collectorConfigSchema.parse({}).retentionDays, DEFAULT_RETENTION_DAYS);
});

const mature = ledger({ startedDaysAgo: 10, days: 7, attemptedRowsPerDay: 760_000 });
check("a studio0_scale host with seven measured days derives its default from the budget", () => {
  const host = budgetHostClass(mature, now.getTime());
  assert.deepEqual({ hostClass: host.hostClass, provisional: host.provisional, observedDays: host.observedDays },
    { hostClass: "studio0_scale", provisional: false, observedDays: 7 });
  assert.equal(host.targets?.ledgerBytes, 16e9);
  const policy = resolveRetentionPolicy(collectorConfigSchema.parse({}), mature, now);
  const projected = Math.floor(16e9 / (760_000 * RAW_BYTES_PER_ROW_ESTIMATE));
  assert.equal(projected, 8);
  assert.deepEqual(
    { days: policy.days, source: policy.source, projected: policy.basis.projectedDays, target: policy.basis.ledgerBytesTarget },
    { days: 8, source: "budget", projected: 8, target: 16e9 },
  );
  assert.match(policy.reason, /studio0_scale/);
  assert.equal(policy.resolvedAt, now.toISOString());
});

check("an explicit retentionDays wins on the same host", () => {
  const policy = resolveRetentionPolicy(collectorConfigSchema.parse({ retentionDays: 14 }), mature, now);
  assert.deepEqual({ days: policy.days, source: policy.source, configured: policy.configuredDays, pinned: policy.pinned },
    { days: 14, source: "config", configured: 14, pinned: false });
  assert.equal(policy.basis.hostClass, "studio0_scale");
});

check("a pinned default wins on the same host", () => {
  const policy = resolveRetentionPolicy(collectorConfigSchema.parse({ retentionDays: 90, retentionDaysPinned: true }), mature, now);
  assert.deepEqual({ days: policy.days, source: policy.source, pinned: policy.pinned }, { days: 90, source: "config", pinned: true });
  assert.match(policy.reason, /pinned/);
});

check("a provisional studio0_scale rate keeps the default until seven days are measured", () => {
  const young = ledger({ startedDaysAgo: 3, days: 3, attemptedRowsPerDay: 760_000 });
  assert.equal(budgetHostClass(young, now.getTime()).provisional, true);
  const policy = resolveRetentionPolicy(collectorConfigSchema.parse({}), young, now);
  assert.deepEqual({ days: policy.days, source: policy.source }, { days: DEFAULT_RETENTION_DAYS, source: "default" });
  assert.match(policy.reason, /provisional/);
});

check("the budget never derives fewer than seven days", () => {
  const heavy = ledger({ startedDaysAgo: 10, days: 7, attemptedRowsPerDay: 2_000_000 });
  const policy = resolveRetentionPolicy(collectorConfigSchema.parse({}), heavy, now);
  assert.equal(policy.basis.projectedDays, 3);
  assert.deepEqual({ days: policy.days, source: policy.source }, { days: BUDGET_RETENTION_FLOOR_DAYS, source: "budget" });
});

check("the budget never derives more than the default", () => {
  const policy = deriveRetentionPolicy({
    configuredDays: DEFAULT_RETENTION_DAYS, hostClass: "studio0_scale", attemptedRowsPerDay: 10_000,
    observedDays: 7, provisional: false, ledgerBytesTarget: 16e9, now,
  });
  assert.equal(policy.basis.projectedDays, 666);
  assert.deepEqual({ days: policy.days, source: policy.source }, { days: DEFAULT_RETENTION_DAYS, source: "budget" });
});

check("a busy host keeps the default", () => {
  const busy = ledger({ startedDaysAgo: 10, days: 7, attemptedRowsPerDay: 100_000 });
  assert.equal(budgetHostClass(busy, now.getTime()).hostClass, "busy");
  const policy = resolveRetentionPolicy(collectorConfigSchema.parse({}), busy, now);
  assert.deepEqual({ days: policy.days, source: policy.source }, { days: DEFAULT_RETENTION_DAYS, source: "default" });
  assert.match(policy.reason, /busy/);
});

check("a missing target or rate keeps the default", () => {
  const noTarget = deriveRetentionPolicy({ configuredDays: 90, hostClass: "studio0_scale", attemptedRowsPerDay: 760_000,
    provisional: false, ledgerBytesTarget: null, now });
  const noRate = deriveRetentionPolicy({ configuredDays: 90, hostClass: "studio0_scale", attemptedRowsPerDay: null,
    provisional: false, ledgerBytesTarget: 16e9, now });
  assert.deepEqual([noTarget.source, noTarget.days, noRate.source, noRate.days], ["default", 90, "default", 90]);
  assert.equal(noTarget.basis.projectedDays, null);
});

console.log(JSON.stringify({ proof: "retention_policy", checks: checks.length, passed: checks.length, names: checks }));
