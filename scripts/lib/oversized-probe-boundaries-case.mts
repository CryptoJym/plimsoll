import assert from "node:assert/strict";
import {newSkippedDiscriminatorProbe,observeSkippedDiscriminators,proveSkippedNonUsage} from
  "../../packages/collector-cli/src/capture-record-loss.ts";

const records = [
  {name:"duplicate_type",raw:'{"type":"user","padding":"abc","type":"assistant"}',proof:"top_type",known:false},
  {name:"duplicate_payload",raw:'{"type":"event_msg","payload":{"type":"user_message"},"payload":{"type":"token_count"}}',proof:"payload_type",known:false},
  {name:"codex_plain",raw:'{"type":"event_msg","payload":{"type":"exec_command_end","output":"abc"}}',proof:"payload_type",known:true},
  {name:"claude_plain",raw:'{"type":"user","message":{"content":"abc"}}',proof:"top_type",known:true},
  {name:"unicode_key",raw:'{"type":"user","\\u0074ype":"assistant"}',proof:"top_type",known:false},
  {name:"unicode_payload",raw:'{"type":"event_msg","payload":{"type":"user_message"},"\\u0070ayload":{"\\u0074ype":"token_count"}}',proof:"payload_type",known:false},
  {name:"escaped_json_string",raw:'{"type":"user","content":"{\\"type\\":\\"assistant\\"}"}',proof:"top_type",known:false},
  {name:"payload_cap_then_type",raw:'{"type":"user","payload":{},"payload":{},"padding":"abc","type":"assistant"}',proof:"top_type",known:false,typeCount:2},
] as const;
for (const row of records) {
  const bytes=Buffer.from(row.raw);
  const verify=(chunks:Buffer[],label:string) => {
    const probe=newSkippedDiscriminatorProbe();
    for (const chunk of chunks) observeSkippedDiscriminators(probe,chunk);
    assert.equal(probe.scanned,bytes.length,`${row.name} scan ${label}`);
    if ("typeCount" in row) assert.equal(probe.typeCount,row.typeCount,`${row.name} count ${label}`);
    assert.equal(proveSkippedNonUsage(row.proof,probe,bytes.length),row.known,`${row.name} proof ${label}`);
  };
  for (let at=0;at<=bytes.length;at++) verify([bytes.subarray(0,at),bytes.subarray(at)],String(at));
  verify([...bytes].map((_,at)=>bytes.subarray(at,at+1)),"one_byte");
}
console.log(JSON.stringify({schema:"plimsoll.oversized-probe-boundaries/v1",records:records.length,passed:true}));
