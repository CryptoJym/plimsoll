import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { refreshAutomaticCaptureFile, advanceAutomaticCaptureFiles } from '../packages/collector-cli/src/automatic-capture-retry';
const base=fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir(),'metadata-'));
const checks:Array<{name:string;passed:boolean}>=[];
const check=(name:string,body:()=>void)=>{try{body();checks.push({name,passed:true});}catch{checks.push({name,passed:false});}};
try {
  const file=path.join(base,'candidate.jsonl');
  const reset=()=>{try{fs.unlinkSync(file);}catch{}fs.writeFileSync(file,'{}\n{}\n');return fs.lstatSync(file,{bigint:true});};
  let prior=reset();fs.appendFileSync(file,'{}\n');
  check('ordinary append refresh keeps precise generation',()=>{
    const next=refreshAutomaticCaptureFile(file,prior,fs.lstatSync(file));assert.equal(next.precise.ino,prior.ino);assert.equal(next.precise.birthtimeNs,prior.birthtimeNs);assert.equal(next.stat.size,9);
  });
  prior=reset();fs.truncateSync(file,1);
  check('truncation refuses retained snapshot refresh',()=>assert.throws(()=>refreshAutomaticCaptureFile(file,prior,fs.lstatSync(file))));
  prior=reset();const other=path.join(base,'replacement.jsonl');fs.writeFileSync(other,'{}\n{}\n');fs.renameSync(other,file);
  check('same-sized replacement refuses retained generation',()=>assert.throws(()=>refreshAutomaticCaptureFile(file,prior,fs.lstatSync(file))));
  prior=reset();fs.unlinkSync(file);fs.writeFileSync(other,'{}\n{}\n');fs.symlinkSync(other,file);
  check('symlink replacement refuses retained generation',()=>assert.throws(()=>refreshAutomaticCaptureFile(file,prior,fs.lstatSync(file))));
  prior=reset();const stale=fs.lstatSync(file);fs.appendFileSync(file,'{}\n');
  check('normal/precise stat race refuses refresh',()=>assert.throws(()=>refreshAutomaticCaptureFile(file,prior,stale)));
  check('growth does not renew the five-service retry allowance',()=>{
    let pending=[{file,stat:fs.lstatSync(file),precise:fs.lstatSync(file,{bigint:true})}];
    for(let service=1;service<=5;service++) {
      fs.appendFileSync(file,'{}\n');
      Object.assign(pending[0]!,refreshAutomaticCaptureFile(file,pending[0]!.precise,fs.lstatSync(file)));
      pending=advanceAutomaticCaptureFiles(pending,new Set(),new Set([file]));
      assert.equal(pending.length,service<5?1:0);
      if(pending.length)assert.equal((pending[0] as any).servicedCadences,service);
    }
  });
  console.log(JSON.stringify({checks,passed:checks.every(c=>c.passed)},null,2));if(!checks.every(c=>c.passed))process.exitCode=1;
} finally {fs.rmSync(base,{recursive:true,force:true});}
