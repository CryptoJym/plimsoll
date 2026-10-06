import assert from "node:assert/strict";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";

/** Only this proof process changes its OS-query scheduling allowance. Product
 * calls/refusals are untouched: no fake identity, cached PID, or refusal retry.
 * Actual ps output is returned, and every non-timeout failure still fails closed.
 */
export function proofProcessIdentity() {
  const original=childProcess.execFileSync;
  let queries=0,timeouts=0;
  const execute: typeof original = ((file: string,args: string[],options: any) => {
    if(file!=="/bin/ps"||args?.length!==4||args[0]!=="-p"||args[2]!=="-o"||args[3]!=="lstart="||options?.timeout!==2000)
      return original(file,args,options);
    queries++;
    for(let attempt=0;;attempt++)try {
      return original(file,args,{...options,timeout:15000});
    } catch(error) {
      if((error as NodeJS.ErrnoException).code!=="ETIMEDOUT"||attempt>=2)throw error;
      timeouts++;
    }
  }) as typeof original;
  childProcess.execFileSync=execute;syncBuiltinESMExports();
  return {stats:()=>({queries,timeouts,timeoutMs:15000,maxAttempts:3}),restore:()=>{
    assert.equal(childProcess.execFileSync,execute);childProcess.execFileSync=original;syncBuiltinESMExports();
  }};
}
