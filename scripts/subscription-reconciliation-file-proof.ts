import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { readSubscriptionReconciliation } from "../packages/collector-cli/src/subscription-reconciliation-file";

const home=fs.mkdtempSync(path.join(os.tmpdir(),"subscription-file-proof-"));
const file=path.join(home,"subscription-reconciliation-input.json");
const now="2026-10-14T19:00:00.000Z", evidence=`sha256:${"a".repeat(64)}`;
const source=(sourceRef:string,type:string)=>({sourceRef,type,readAt:now,lastSyncedAt:now,providerObservedAt:now,
  coverage:{state:"PARTIAL",periodStart:"2026-10-01",periodEnd:"2026-10-15"},evidenceRefs:[evidence]});
const input={schema:"subscription-reconciliation/v1",observedAt:now,
  sources:[source("receipts","INVOICES"),source("bank","BANK")],
  invoices:[{sourceRef:"receipts",invoiceRef:"invoice-synthetic",revision:1,sourceObservedAt:now,provider:"anthropic",
    amountCents:21490,currency:"USD",status:"PAID",paidAt:"2026-10-10",invoiceDate:"2026-10-10",kind:"MONTHLY_SUBSCRIPTION",
    paymentLast4:"4242",evidenceRefs:[evidence]}],
  bankTransactions:[{sourceRef:"bank",transactionRef:"bank-synthetic",revision:1,sourceObservedAt:now,provider:"anthropic",
    amountCents:21490,currency:"USD",state:"POSTED",transactedAt:"2026-10-10",postedAt:"2026-10-11",cardLast4:"4242",evidenceRefs:[evidence]}]};
let checks=0;
function check(name:string,fn:()=>void){fn();checks++;console.log(`PASS ${name}`);}
function write(){fs.writeFileSync(file,JSON.stringify(input),{mode:0o600});fs.chmodSync(file,0o600);}
try {
  check("missing private input is UNKNOWN, without an invented zero",()=>{
    const result=readSubscriptionReconciliation(home,now);assert.equal(result.state,"UNKNOWN");assert.equal(result.currentCertifiedSettledTotals,null);
  });
  check("pending and removed bank observations cannot increase observed posted debits",()=>{
    const original=input.bankTransactions.slice();input.bankTransactions.push(
      {...original[0],transactionRef:"pending-synthetic",state:"PENDING"},
      {...original[0],transactionRef:"removed-synthetic",state:"REMOVED"});write();
    const result=readSubscriptionReconciliation(home,now);assert.equal(result.observedPostedBankDebits[0].amountCents,21490);
    assert.equal(result.observedPostedBankDebits[0].rows,1);input.bankTransactions=original;write();
  });
  write();
  check("fresh private records expose a verified subtotal while completeness and cash allocation stay UNKNOWN",()=>{
    const result=readSubscriptionReconciliation(home,now);assert.equal(result.state,"partial");assert.equal(result.currentCertifiedSettledTotals?.[0].amountCents,21490);
    assert.equal(result.completeness,"UNKNOWN");assert.equal(result.cashAllocation,"UNKNOWN");assert.equal(result.result?.candidates[0].accountIdentityState,"UNKNOWN");
  });
  check("reprocessing stale records retains historical proof but masks current totals",()=>{
    const result=readSubscriptionReconciliation(home,"2026-10-16T19:00:00.000Z");assert.equal(result.state,"UNKNOWN");
    assert.equal(result.currentCertifiedSettledTotals,null);assert.equal(result.result?.sources[1].lastSyncedAt,now);
    assert.equal(result.result?.certifiedSettledTotals[0].amountCents,21490);
  });
  check("fresh sources with no verified payment expose UNKNOWN rather than a zero total",()=>{
    const saved=input.bankTransactions[0].cardLast4;input.bankTransactions[0].cardLast4=null as unknown as string;write();
    const result=readSubscriptionReconciliation(home,now);assert.equal(result.state,"UNKNOWN");assert.equal(result.currentCertifiedSettledTotals,null);
    assert.equal(result.observedPostedBankDebits[0].amountCents,21490);assert.equal(result.observedPostedBankDebits[0].accountJoin,"UNKNOWN");
    assert.equal(result.result?.candidates[0].state,"UNKNOWN");input.bankTransactions[0].cardLast4=saved;write();
  });
  check("fresh file read cannot refresh missing provider sync evidence",()=>{
    const saved=input.sources[1].lastSyncedAt;input.sources[1].lastSyncedAt=null as unknown as string;write();
    assert.equal(readSubscriptionReconciliation(home,now).state,"UNKNOWN");input.sources[1].lastSyncedAt=saved;write();
  });
  check("world-readable input and symlink input are rejected",()=>{
    fs.chmodSync(file,0o644);assert.equal(readSubscriptionReconciliation(home,now).result,null);
    fs.chmodSync(file,0o600);const target=path.join(home,"target.json");fs.renameSync(file,target);fs.symlinkSync(target,file);
    assert.equal(readSubscriptionReconciliation(home,now).result,null);fs.unlinkSync(file);fs.renameSync(target,file);
  });
  check("malformed evidence fails closed without echoing its content",()=>{
    fs.writeFileSync(file,'{"private":"do-not-echo"}',{mode:0o600});const result=readSubscriptionReconciliation(home,now);
    assert.equal(result.result,null);assert.ok(!JSON.stringify(result).includes("do-not-echo"));
  });
  console.log(JSON.stringify({schema:"subscription-file-proof/v1",checks,passed:checks,data:"SYNTHETIC_ONLY"}));
} finally {fs.rmSync(home,{recursive:true,force:true});}
