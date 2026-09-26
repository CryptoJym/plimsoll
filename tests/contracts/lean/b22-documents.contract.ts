/**
 * B22 document guard (NOT pending; green today): docs/lean/BEADS.md B22, PROOF.md §8 item 1 and ARCHITECTURE.md §3.6 agree on
 * the open-ended interval for unresolved files (CONTRACTS.md C5). The document half of fixtures/b22_false_complete.py, ported
 * with repository-relative paths; the behavioural half is capture-gaps.contract.ts.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const docs = (name: string) => readFileSync(new URL(`../../../docs/lean/${name}`, import.meta.url), "utf8");
const prescribed = (text: string) => (/epoch[_-]to[_-]last[_-]write/.test(text) ? "epoch_to_last_write" : text.includes("epoch_open") ? "epoch_open" : "unspecified");

test("B22 guard: the bead, the acceptance drill and the architecture prescribe epoch_open with a null end", () => {
  const b22 = docs("BEADS.md").split("\n").find((l) => l.startsWith("| **B22**")) ?? "";
  const sec8 = docs("PROOF.md").split("\n## 8.")[1]?.split("\n## ")[0] ?? "";
  const item1 = sec8.split("\n").find((l) => l.trim().startsWith("1.")) ?? "";
  assert.equal(prescribed(b22), "epoch_open");
  assert.equal(prescribed(item1), "epoch_open");
  assert.ok(item1.includes("null"));
  const arch = docs("ARCHITECTURE.md").replace(/[`*]/g, "");
  assert.ok(arch.includes("epoch_open") && arch.includes("ended_at_ms = null"));
  assert.ok(sec8.includes("b22_false_complete.py") && b22.includes("b22_false_complete.py"), "both name the fixture at the B22 gate");
});
