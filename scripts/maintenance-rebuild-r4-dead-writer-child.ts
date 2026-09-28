import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const ledger = process.argv[2];
if (!ledger) throw new Error("ledger_required");
const buffer = new LocalEventBuffer(ledger);
process.send?.({ ready: true, pid: process.pid });
// The parent SIGKILLs this process; close() deliberately never runs.
setInterval(() => void buffer, 1000);
