import assert from "node:assert/strict";
import { openRebuildFencedDatabase } from "../packages/collector-cli/src/rebuild-open-gate";

const ledger = process.argv[2];
if (!ledger) throw new Error("ledger_required");
process.argv[2] = "start";
assert.throws(() => openRebuildFencedDatabase(ledger), /maintenance_rebuild_paused/);
console.log("second_start_refused");
