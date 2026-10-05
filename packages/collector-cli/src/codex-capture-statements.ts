import type Database from "better-sqlite3";

// Compile capture/witness SQL once per connection, never cache its result.
// Every execution still reads current identities, native contradictions and
// frozen bytes. The database's statement wrappers and write guards remain.
// SQLite invalidates prepared programs when its schema changes.
const byConnection = new WeakMap<Database.Database, Map<string, Database.Statement>>();
export function codexCaptureStatement(db: Database.Database, sql: string): Database.Statement {
  let statements = byConnection.get(db);
  if (!statements) { statements = new Map(); byConnection.set(db, statements); }
  let statement = statements.get(sql);
  if (!statement) { statement = db.prepare(sql); statements.set(sql, statement); }
  return statement;
}
