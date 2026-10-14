import assert from "node:assert/strict";
import Database from "better-sqlite3";
import { importCursorSubscriptionExport, latestSubscriptionExportThreadLinks } from "../packages/collector-cli/src/subscription-exports";

const db=new Database(":memory:");
const header="Date,Cloud Agent ID,Kind,Model,Input (w/ Cache Write),Input (w/o Cache Write),Cache Read,Output Tokens,Total Tokens,Cost\n";
const options={accountRef:`native:${"a".repeat(32)}`,periodStart:"2026-10-01T00:00:00Z",periodEnd:"2026-10-15T00:00:00Z"};
try {
  importCursorSubscriptionExport(db,header+"2026-10-10T12:00:00Z,old-chat,Included,model,0,80,0,20,100,Included\n",{...options,now:"2026-10-11T12:00:00Z"});
  const newer=header+"2026-10-10T12:00:00Z,new-chat,Included,model,0,20,0,5,25,Included\n"+
    "2026-10-11T12:00:00Z,,Included,model,0,30,0,5,35,Included\n"+
    "2026-10-11T12:00:00Z,new-chat,Included,model,,,,,,Included\n";
  importCursorSubscriptionExport(db,newer,{...options,now:"2026-10-12T12:00:00Z"});
  let rows=latestSubscriptionExportThreadLinks(db);
  assert.equal(rows.length,1);assert.equal(rows[0].threadId,"new-chat");assert.equal(rows[0].observedRows,2);
  assert.equal(rows[0].observedTotalTokens,25);assert.equal(rows[0].rowsWithTotalTokens,1);
  console.log("PASS only latest account snapshot contributes; missing IDs/counters retain their boundaries");
  importCursorSubscriptionExport(db,newer,{...options,now:"2026-10-13T12:00:00Z"});
  assert.deepEqual(latestSubscriptionExportThreadLinks(db),rows);
  console.log("PASS replaying an export cannot add chat tokens or refresh its import clock");
  importCursorSubscriptionExport(db,newer,{...options,accountRef:`native:${"b".repeat(32)}`,now:"2026-10-12T12:00:00Z"});
  rows=latestSubscriptionExportThreadLinks(db);assert.equal(rows.length,2);assert.equal(new Set(rows.map(row=>row.accountRef)).size,2);
  console.log("PASS equal provider chat IDs on different accounts remain separately scoped");
  console.log(JSON.stringify({schema:"subscription-export-links-proof/v1",checks:3,passed:3,data:"SYNTHETIC_ONLY"}));
} finally {db.close();}
