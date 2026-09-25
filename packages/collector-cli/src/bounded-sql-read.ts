import Database from "better-sqlite3";

/** A non-deterministic predicate lets SQLite stop a synchronous scan mid-step. */
export const BOUNDED_SQL_READ_PREDICATE = "plimsoll_bounded_sync_read()";

export class BoundedSqlReadError extends Error {
  constructor(readonly reason: "deadline" | "row_limit") {
    super(`bounded_sql_read_${reason}`);
  }
}

const budgets = new WeakMap<Database.Database, { deadline: number; expired: boolean }>();

/** Run a small compatibility read on the main thread, or fail to an off-thread full walk. */
export function boundedSqlRows<T>(
  db: Database.Database,
  sql: string,
  params: Record<string, unknown>,
  maxRows: number,
): T[] {
  if (!sql.includes(BOUNDED_SQL_READ_PREDICATE)) throw new Error("bounded_sql_read_predicate_missing");
  let budget = budgets.get(db);
  if (!budget) {
    budget = { deadline: Number.POSITIVE_INFINITY, expired: false };
    const registered = budget;
    db.function("plimsoll_bounded_sync_read", () => {
      if (performance.now() >= registered.deadline) {
        registered.expired = true;
        throw new BoundedSqlReadError("deadline");
      }
      return 1;
    });
    budgets.set(db, budget);
  }
  budget.deadline = performance.now() + 250;
  budget.expired = false;
  try {
    const rows = db.prepare(sql).all(params) as T[];
    if (budget.expired || performance.now() >= budget.deadline) throw new BoundedSqlReadError("deadline");
    if (rows.length > maxRows) throw new BoundedSqlReadError("row_limit");
    return rows;
  } catch (error) {
    if (budget.expired) throw new BoundedSqlReadError("deadline");
    throw error;
  } finally {
    budget.deadline = Number.POSITIVE_INFINITY;
  }
}
