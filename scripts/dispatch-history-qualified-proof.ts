import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { withDispatchHistoryAdoption } from '../packages/collector-cli/src/dispatch-history-adoption';
import { dispatchHistoryAdoptionFixture, installDispatchHistoryWriterTimingFixture } from './lib/dispatch-history-adoption-fixture';
import { createProofCompletion, requireIsolatedProofEnvironment } from './lib/proof-completion';
requireIsolatedProofEnvironment();
installDispatchHistoryWriterTimingFixture();
const original=path.resolve('scripts/dispatch-history-proof.ts');
const hash=()=>crypto.createHash('sha256').update(fs.readFileSync(original)).digest('hex');
const before=hash(),worker=process.argv[2]==='worker';
// The original suite's actual subprocess fixtures receive the same explicitly
// requested qualification. Production CLI and ordinary run-proof retain no grant.
process.env.NODE_OPTIONS=`--import ${path.resolve('node_modules/tsx/dist/loader.mjs')} --import ${fileURLToPath(import.meta.url)}`;
process.on('exit',()=>{
  if(hash()!==before)throw new Error('original history proof changed');
  if(!worker&&process.exitCode!==1){
    const receipt=JSON.parse(fs.readFileSync(process.env.PLIMSOLL_PROOF_RECEIPT!,'utf8'));
    if(receipt.counts?.total!==95||receipt.counts?.passed!==95||receipt.completed!==true)
      throw new Error('all unchanged 95 history checks must complete');
    // The declared suite must name its actual sub-proof at the parent boundary.
    // Retain the full 95-check receipt as well; never substitute one assertion
    // for executing and verifying the unchanged history suite.
    const completion=createProofCompletion('dispatch-history-qualified',1);
    completion.check('scripts/dispatch-history-proof.ts');completion.complete();
    const parent=JSON.parse(fs.readFileSync(process.env.PLIMSOLL_PROOF_RECEIPT!,'utf8'));
    fs.writeFileSync(process.env.PLIMSOLL_PROOF_RECEIPT!,JSON.stringify({...parent,
      subProofs:[{entry:'scripts/dispatch-history-proof.ts',entrySha256:before,receipt}]},null,2)+'\n',{mode:0o600});
  }
});
withDispatchHistoryAdoption(dispatchHistoryAdoptionFixture,()=>import('./dispatch-history-proof')).catch(error=>{
  console.error(error);process.exitCode=1;
});
