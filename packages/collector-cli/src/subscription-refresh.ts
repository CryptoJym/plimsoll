import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import Database from "better-sqlite3";
import { defaultNativeSubscriptionPaths, type NativeSubscriptionPaths } from "./native-subscription";
import { collectNativeTelemetry, enrichNativeProjectMetadata, nativeTelemetryReport } from "./native-telemetry-store";
import { importCursorSubscriptionExport, latestSubscriptionExportThreadLinks } from "./subscription-exports";
import { readSubscriptionReconciliation } from "./subscription-reconciliation-file";

function atomicWrite(file: string, contents: string) {
  const temp = `${file}.${process.pid}.tmp`;
  try { fs.writeFileSync(temp,contents,{mode:0o600}); fs.renameSync(temp,file); }
  finally { if (fs.existsSync(temp)) fs.unlinkSync(temp); }
}

function escape(value: unknown): string {
  return String(value ?? "UNKNOWN").replace(/[&<>"']/g,(c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]!);
}

function refreshStoredBankSource(home: string, now: string) {
  if (path.resolve(home)!==path.resolve(path.dirname(defaultNativeSubscriptionPaths().nativeSubscriptionDb))) return {state:"UNKNOWN",attempted:false,reason:"BANK_REFRESH_NON_NATIVE_HOME",intervalSeconds:3600};
  const script=path.join(home,"subscription-bank-source-refresh.py"), receiptFile=path.join(home,"subscription-bank-source-refresh-receipt.json");
  const stateFile=path.join(home,"subscription-bank-refresh-state.json");
  const readPrivate=(file:string,maximum:number)=>{
    const stat=fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size>maximum || (stat.mode&0o077)!==0 ||
        (process.getuid && stat.uid!==process.getuid())) throw new Error("BANK_REFRESH_HELPER_UNKNOWN");
    const fd=fs.openSync(file,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);
    try {return fs.readFileSync(fd,"utf8");} finally {fs.closeSync(fd);}
  };
  try {
    const code=readPrivate(script,128*1024), receipt=JSON.parse(readPrivate(receiptFile,16*1024)) as {sha256?:string};
    if (receipt.sha256!==`sha256:${crypto.createHash("sha256").update(code).digest("hex")}`) throw new Error("BANK_REFRESH_HELPER_UNKNOWN");
    const source=readSubscriptionReconciliation(home,now).result?.sources.find(source=>source.type === "BANK");
    const readAge=source ? Date.parse(now)-Date.parse(source.readAt) : NaN;
    if (Number.isFinite(readAge) && readAge>=-300_000 && readAge<3_600_000) return {state:"configured",attempted:false,reason:"NOT_DUE",intervalSeconds:3600};
    try {
      const previous=JSON.parse(readPrivate(stateFile,16*1024)) as {lastAttemptAt?:string};
      const age=previous.lastAttemptAt ? Date.parse(now)-Date.parse(previous.lastAttemptAt) : NaN;
      if (Number.isFinite(age) && age>=-300_000 && age<3_600_000) return {state:"UNKNOWN",attempted:false,reason:"RETRY_NOT_DUE",intervalSeconds:3600};
    } catch { /* No prior attempt. */ }
    atomicWrite(stateFile,JSON.stringify({lastAttemptAt:now,state:"UNKNOWN"})+"\n");
    const run=spawnSync("/usr/bin/python3",[script],{encoding:"utf8",timeout:25_000,maxBuffer:64*1024,stdio:["ignore","pipe","pipe"]});
    let succeeded=false;
    if (run.status===0) {
      try { succeeded=(JSON.parse(run.stdout) as {status?:string}).status === "UPDATED"; }
      catch { /* Never echo child output or credentials on malformed output. */ }
    }
    const outcome={state:succeeded ? "observed" : "UNKNOWN",attempted:true,reason:succeeded ? "STORED_BANK_READ" : "BANK_REFRESH_FAILED",intervalSeconds:3600};
    atomicWrite(stateFile,JSON.stringify({lastAttemptAt:now,...outcome})+"\n");
    return outcome;
  } catch { return {state:"UNKNOWN",attempted:false,reason:"BANK_REFRESH_HELPER_UNKNOWN",intervalSeconds:3600}; }
}

function localAccountLabels(file: string): Map<string, string> {
  const labels = new Map<string,string>();
  try {
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 64*1024*1024) return labels;
    const fd = fs.openSync(file,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);
    let payload: {accounts?:Array<{accountKey?:unknown;identity?:{email?:unknown;authenticated?:unknown;matchesExpected?:unknown;proof?:unknown}}>};
    try { payload=JSON.parse(fs.readFileSync(fd,"utf8")); } finally { fs.closeSync(fd); }
    for (const account of payload.accounts ?? []) {
      const identity=account.identity;
      if (typeof account.accountKey !== "string" || identity?.authenticated !== true || identity.matchesExpected !== true ||
          typeof identity.proof !== "string" || identity.proof === "configured_only" ||
          typeof identity.email !== "string" || identity.email.length>254 || !/^[^\s<>@]+@[^\s<>@]+$/.test(identity.email)) continue;
      labels.set(`native:${crypto.createHash("sha256").update(account.accountKey).digest("hex").slice(0,32)}`,identity.email);
    }
  } catch { /* Account references stay readable even without a proved label. */ }
  return labels;
}

function registeredClaudeProjectRoots(paths: NativeSubscriptionPaths): string[] {
  const roots=[path.join(path.dirname(paths.claudeConfigFile),".claude/projects")];
  try {
    const stat=fs.lstatSync(paths.nativeUsageFile);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size>64*1024*1024 || !paths.claudeSeatsRoot) return roots;
    const fd=fs.openSync(paths.nativeUsageFile,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);
    let payload:{accounts?:Array<{provider?:unknown;source?:{profile?:unknown}}>};
    try {payload=JSON.parse(fs.readFileSync(fd,"utf8"));} finally {fs.closeSync(fd);}
    for (const account of (payload.accounts ?? []).slice(0,128)) {
      const profile=account.source?.profile;
      if (!["claude","anthropic"].includes(String(account.provider)) || typeof profile!=="string" ||
          !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$/.test(profile)) continue;
      const seat=path.join(paths.claudeSeatsRoot,profile), projectRoot=path.join(seat,"projects");
      const safe=[paths.claudeSeatsRoot,seat,projectRoot].every(directory=>{
        try {
          const directoryStat=fs.lstatSync(directory);return directoryStat.isDirectory() && !directoryStat.isSymbolicLink() &&
            (directoryStat.mode&0o022)===0 && (!process.getuid || directoryStat.uid===process.getuid());
        } catch {return false;}
      });
      if (safe) roots.push(projectRoot);
    }
  } catch { /* Unavailable registered metadata remains UNKNOWN. */ }
  return [...new Set(roots)];
}

function renderDashboard(status: Record<string, unknown>, report: ReturnType<typeof nativeTelemetryReport>, labels: Map<string,string>): string {
  const accountName=(ref: unknown) => typeof ref === "string" ? labels.get(ref) ?? ref : "UNKNOWN";
  const providers = status.providers as Array<{provider:string;accountCount:number;planTypes:string[];subscription:{state:string};quota:{state:string};threadTelemetry:{tokenState:string}}>;
  const providerRows = providers.map((provider) => `<tr><td>${escape(provider.provider)}</td><td>${provider.accountCount}</td><td>${escape(provider.planTypes.join(", "))}</td><td>${escape(provider.subscription.state.toUpperCase())}</td><td>${escape(provider.quota.state.toUpperCase())}</td><td>${escape(provider.threadTelemetry.tokenState.toUpperCase())}</td></tr>`).join("");
  const threads = report.threads.slice(0,100).map((thread) => `<tr><td>${escape(thread.source)}</td><td><code>${escape(thread.threadId)}</code></td><td>${escape(thread.accountRefs.map(accountName).join(", ") || "UNKNOWN")}<br><small>${escape(thread.accountLinkCoverage.linkedEvents)}/${escape(thread.accountLinkCoverage.totalEvents)} linked events; ${escape(thread.subscriptionAccountJoin)}</small></td><td>${thread.projectUrl ? `<a href="${escape(thread.projectUrl)}">${escape(thread.projectLabel ?? thread.projectKey)}</a>` : escape(thread.projectLabel ?? thread.projectKey)}<br><small>${escape(thread.projectState)}${thread.projectConflict ? "; conflicting candidates" : ""}</small></td><td>${escape(thread.observedInputTokens)}</td><td>${escape(thread.observedOutputTokens)}</td><td>${escape(thread.observedCacheReadTokens)}</td></tr>`).join("");
  const exports = report.dashboardExports.map((item) => `<li>${escape(item.provider)}: ${escape(item.records)} export rows; ${escape(item.observedTotalTokens)} observed tokens; ${escape(item.reportedOnDemandUsageUsd)} reported on-demand USD. Imported ${escape(item.importedAt)}; thread IDs present on ${escape(item.rowsWithThreadId)} rows. Completeness: UNKNOWN. New provider data requires a new dashboard export.</li>`).join("");
  const windows = (status.quotaWindows as Array<Record<string,unknown>>).map((window) => `<tr><td>${escape(window.provider)}</td><td>${escape(accountName(window.accountRef))}</td><td>${escape(window.planType)}</td><td>${escape(window.windowLabel ?? window.windowSlot)}</td><td>${escape(window.currentUsedPercent)}</td><td>${escape(window.currentRemainingPercent)}</td><td>${escape(window.currentResetAt ?? window.resetState)}</td><td>${escape(window.currentState)}</td></tr>`).join("");
  const accountRows=report.accountActivity.map(activity => `<tr><td>${escape(activity.source)}</td><td>${escape(accountName(activity.accountRef))}</td><td>${escape(activity.observedEvents)}</td><td>${escape(activity.observedInputTokens)}</td><td>${escape(activity.observedOutputTokens)}</td><td>${escape(activity.subscriptionAccountJoin)}${activity.unknownReason ? `: ${escape(activity.unknownReason)}` : ""}</td></tr>`).join("");
  const exportThreads=status.dashboardThreadLinks as Array<ReturnType<typeof latestSubscriptionExportThreadLinks>[number]&{accountLinkState:string}>;
  const exportThreadRows=exportThreads.map(thread=>`<tr><td>${escape(thread.provider)}</td><td><code>${escape(thread.threadId)}</code></td><td>${escape(accountName(thread.accountRef))}</td><td>${escape(thread.accountLinkState)}</td><td>${escape(thread.observedRows)}</td><td>${escape(thread.observedTotalTokens)}</td><td>UNKNOWN</td></tr>`).join("");
  const exportThreadSection=`<h3>Chats identified in provider exports</h3><p>These IDs come directly from the latest export. Matching them to local Cursor conversations and projects remains UNKNOWN. They are separate control observations and are not added to the native event totals.</p><div class="scroll"><table><thead><tr><th>Provider</th><th>Export chat ID</th><th>Account</th><th>Account proof</th><th>Rows</th><th>Observed tokens</th><th>Project</th></tr></thead><tbody>${exportThreadRows || '<tr><td colspan="7">UNKNOWN: no identified export chats</td></tr>'}</tbody></table></div>`;
  const money=status.subscriptionReconciliation as ReturnType<typeof readSubscriptionReconciliation>;
  const result=money.result;
  const verified=result?.candidates.filter(candidate=>candidate.state === "VERIFIED") ?? [];
  const matches=verified.map(candidate=>{
    const invoice=result!.invoices[candidate.invoiceIndex].record, bank=result!.bankTransactions[candidate.bankIndex].record;
    return `<tr><td>${escape(invoice.provider)}</td><td>${escape(accountName(candidate.accountRef))}</td><td>${escape(invoice.kind)}</td><td>${escape(invoice.currency)} ${(invoice.amountCents/100).toFixed(2)}</td><td>${escape(invoice.paidAt)} → ${escape(bank.postedAt)}</td><td>${escape(candidate.basis)}; ${escape(candidate.accountIdentityState)} account</td></tr>`;
  }).join("");
  const bankStates=result?.bankTransactions.reduce((counts,row)=>{if(row.disposition === "ACTIVE") counts[row.record.state]=(counts[row.record.state] ?? 0)+1;return counts;},{} as Record<string,number>) ?? {};
  const debitRows=money.observedPostedBankDebits.map(group=>`<tr><td>${escape(group.provider)}</td><td>${escape(group.kind)}</td><td>${escape(group.currency)} ${group.amountCents === null ? "UNKNOWN" : (group.amountCents/100).toFixed(2)}</td><td>${escape(group.rows)}</td><td>${escape(group.asOf)} (${escape(group.freshness)})</td><td>UNKNOWN</td></tr>`).join("");
  const bankDebits=`<h3>Observed posted bank debits</h3><p>Positive posted debits in each provided bank source and posting window. Pending/removed rows are excluded. These are bank observations across charge kinds; subscription-account assignment, invoice matching and complete bank coverage remain UNKNOWN. Sources are not summed together.</p><div class="scroll"><table><thead><tr><th>Provider</th><th>Kind</th><th>Observed amount</th><th>Rows</th><th>Source sync time</th><th>Subscription account</th></tr></thead><tbody>${debitRows || '<tr><td colspan="6">UNKNOWN: no eligible posted debit observations</td></tr>'}</tbody></table></div>`;
  const invoiceRows=result?.invoices.filter(row=>row.disposition === "ACTIVE").map(row=>`<tr><td>${escape(row.record.provider)}</td><td>${escape(accountName(row.record.accountIdentityState === "VERIFIED" ? row.record.accountRef : null))}</td><td>${escape(row.record.kind)}</td><td>${escape(row.record.currency)} ${(row.record.amountCents/100).toFixed(2)}</td><td>${escape(row.record.paidAt)}</td><td>${escape(row.state)}${row.state === "UNKNOWN" ? `<br><small>${escape(row.reasons.join(", "))}</small>` : ""}</td></tr>`).join("") ?? "";
  const financial=`<h2>Subscription payments and bank reconciliation</h2><p>State: ${escape(money.state)}. ${escape(verified.length)} invoice-to-bank matches in the provided records. Source coverage: UNKNOWN; assigning cash to chats or projects: UNKNOWN. ${escape(result?.invoices.length)} invoice records; ${escape(bankStates.POSTED ?? "UNKNOWN")} active posted bank rows, ${escape(bankStates.PENDING ?? "UNKNOWN")} pending, ${escape(bankStates.REMOVED ?? "UNKNOWN")} removed. ${escape(money.supplemental.hostedBankRows)} additional hosted bank observations and ${escape(money.supplemental.unknownReceiptCount)} unavailable receipt remain UNKNOWN and are excluded from totals.</p><p><small>${money.freshness.map(source=>`${escape(source.sourceRef)}: ${escape(source.state)} as of ${escape(source.asOf)}`).join("; ")}. Reprocessing retains source dates.</small></p>${bankDebits}<h3>Verified matches as of their sources</h3><div class="scroll"><table><thead><tr><th>Provider</th><th>Subscription account</th><th>Charge</th><th>Amount</th><th>Paid day → posted day</th><th>Proof</th></tr></thead><tbody>${matches || '<tr><td colspan="6">UNKNOWN: no verified matches</td></tr>'}</tbody></table></div><details><summary>Invoice adjudication</summary><div class="scroll"><table><thead><tr><th>Provider</th><th>Account</th><th>Kind</th><th>Paid amount</th><th>Paid day</th><th>Bank match</th></tr></thead><tbody>${invoiceRows}</tbody></table></div></details>`;
  const comparisons = (status.exportControlComparisons as Array<{note:string}>).map((comparison) => `<p>${escape(comparison.note)}</p>`).join("");
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="60"><title>Subscription telemetry</title><style>body{font:15px system-ui;max-width:1180px;margin:36px auto;padding:0 20px;color:#163127;background:#f7faf8}h1{font-size:28px}h2{font-size:20px;margin-top:30px}table{border-collapse:collapse;width:100%;background:white}td,th{padding:10px;text-align:left;border-bottom:1px solid #dbe5df}code{font-size:12px}p{line-height:1.5}small{color:#52655a}.scroll{overflow:auto}</style></head><body><h1>Subscription telemetry</h1><p>Updated ${escape(status.observedAt)}. Native allowances, locally observed tokens, and actual money paid are separate measures.</p><div class="scroll"><table><thead><tr><th>Provider</th><th>Account records</th><th>Observed plans</th><th>Plan evidence</th><th>Current quota</th><th>Local token capture</th></tr></thead><tbody>${providerRows}</tbody></table></div><h2>Current account allowances</h2><p>UNKNOWN windows retain their last known readings in the JSON evidence, without displaying those readings as current. Account references are private aliases.</p><div class="scroll"><table><thead><tr><th>Provider</th><th>Account</th><th>Plan</th><th>Window</th><th>Used %</th><th>Remaining %</th><th>Reset</th><th>State</th></tr></thead><tbody>${windows}</tbody></table></div><h2>Captured usage by subscription account</h2><p>${escape(report.accountLinkCoverage.linkedEvents)} of ${escape(report.accountLinkCoverage.totalEvents)} captured events linked; ${escape(report.accountLinkCoverage.unknownEvents)} remain UNKNOWN. Fleet and history completeness: UNKNOWN.</p><div class="scroll"><table><thead><tr><th>Source</th><th>Account</th><th>Events</th><th>Input</th><th>Output</th><th>Identity proof</th></tr></thead><tbody>${accountRows}</tbody></table></div>${financial}<h2>Provider exports</h2><ul>${exports || "<li>No verified token export imported. UNKNOWN.</li>"}</ul>${comparisons}${exportThreadSection}<h2>Recent chats and projects</h2><p>Projects labeled inferred come from native repository metadata; conflicts remain UNKNOWN. Account links cover only events with captured identity proof. Token history, fleet completeness and cash allocation to chats remain UNKNOWN.</p><div class="scroll"><table><thead><tr><th>Source</th><th>Chat</th><th>Subscription account</th><th>Project</th><th>Input</th><th>Output</th><th>Cache read</th></tr></thead><tbody>${threads || '<tr><td colspan="7">UNKNOWN: no captured thread token events</td></tr>'}</tbody></table></div><p><small>${escape(report.note)}</small></p></body></html>`;
}

// studio0's disk serves a constant stream of other reads, so between two
// 1-minute runs much of the store leaves the page cache; the report then
// pays tens of thousands of random 4 KiB reads behind a saturated queue
// (sampled in unixRead under Statement::JS_all, 11-02). One sequential pass
// over the file brings it back in a few large reads. Bounded to 1 GiB.
function warmStoreFile(file: string): void {
  let fd: number | undefined;
  try {
    const size = fs.statSync(file).size;
    if (size > 1024 * 1024 * 1024) return;
    fd = fs.openSync(file, "r");
    const chunk = Buffer.allocUnsafe(8 * 1024 * 1024);
    for (let offset = 0; offset < size; offset += chunk.length) {
      if (fs.readSync(fd, chunk, 0, chunk.length, offset) <= 0) break;
    }
  } catch {
    // A failed warm read changes nothing; the queries read the pages themselves.
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
  }
}

export function refreshSubscriptionTelemetry(options: { paths?: Partial<NativeSubscriptionPaths>; now?: string } = {}) {
  const paths = { ...defaultNativeSubscriptionPaths(), ...options.paths };
  const home = path.dirname(paths.nativeSubscriptionDb);
  fs.mkdirSync(home,{recursive:true,mode:0o700});
  const bankSourceRefresh=refreshStoredBankSource(home,options.now ?? new Date().toISOString());
  const db = new Database(paths.nativeSubscriptionDb,{timeout:1_000});
  fs.chmodSync(paths.nativeSubscriptionDb,0o600);
  // Write-ahead log: one fsync per checkpoint instead of two per autocommit
  // statement, and readers (the native snapshot, Lean) no longer wait on the
  // writer. Under a saturated studio0 disk the rollback journal made a run of
  // a few hundred small writes take minutes (sampled in unixWrite, 11-02).
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  // The report groups and sorts every stored event; with SQLite's default 2 MiB
  // page cache those temporary B-trees spilled a 50 MiB etilqs file to disk on
  // every run (sampled in unixWrite under Statement::JS_all, studio0 11-02).
  // Keep them in memory: a bounded 256 MiB cache for a process that lives
  // seconds, once a minute.
  db.pragma("temp_store = MEMORY");
  db.pragma("cache_size = -262144");
  warmStoreFile(paths.nativeSubscriptionDb);
  try {
    const {inspection,receipt} = collectNativeTelemetry(db,options);
    const projectMetadataRefresh=enrichNativeProjectMetadata(db,{now:inspection.observedAt,
      claudeProjectsRoots:registeredClaudeProjectRoots(paths)});
    const exportReceipts: unknown[] = [], errors: string[] = [], exportControlComparisons: Array<{state:string;note:string}> = [];
    const cursorInbox = path.join(home,"subscription-exports/cursor");
    if (fs.existsSync(cursorInbox)) {
      for (const name of fs.readdirSync(cursorInbox).filter((name) => name.endsWith(".csv")).sort().reverse().slice(0,32)) {
        try {
          const file = path.join(cursorInbox,name), metadataFile = file.replace(/\.csv$/,".metadata.json");
          const stat = fs.lstatSync(file), metadataStat = fs.lstatSync(metadataFile);
          if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 32*1024*1024 || !metadataStat.isFile() || metadataStat.isSymbolicLink() || metadataStat.size>16_384) throw new Error("invalid_export");
          const metadata = JSON.parse(fs.readFileSync(metadataFile,"utf8")) as {accountRef:string;scope:string;sourceDigest:string;periodStart:string;periodEnd:string;comparisonNote?:string};
          if (metadata.scope !== "individual_subscription" || !inspection.accounts.some((account) => account.provider === "cursor" && account.accountRef === metadata.accountRef)) throw new Error("unproved_export_account");
          const csv = fs.readFileSync(file,"utf8");
          if (`sha256:${crypto.createHash("sha256").update(csv).digest("hex")}` !== metadata.sourceDigest) throw new Error("export_digest_mismatch");
          exportReceipts.push(importCursorSubscriptionExport(db,csv,{accountRef:metadata.accountRef,periodStart:metadata.periodStart,periodEnd:metadata.periodEnd,now:options.now}));
          if (typeof metadata.comparisonNote === "string" && metadata.comparisonNote.length < 1000) exportControlComparisons.push({state:"UNKNOWN",note:metadata.comparisonNote});
        } catch { errors.push("CURSOR_EXPORT_UNKNOWN"); }
      }
    }
    const report = nativeTelemetryReport(db,200);
    const validatedExportIds=new Set(exportReceipts.map(receipt=>(receipt as {exportId:string}).exportId));
    const dashboardThreadLinks=latestSubscriptionExportThreadLinks(db).map(thread=>({...thread,
      accountLinkState:validatedExportIds.has(thread.exportId) && inspection.providers.some(provider=>
        provider.provider === thread.provider && provider.accountCount === 1 && provider.dashboardExport.state === "verified") ? "observed" : "UNKNOWN",
      source:"provider_dashboard_export",localConversationJoin:"UNKNOWN",projectState:"UNKNOWN",tokenCompleteness:"UNKNOWN",cashAllocation:"UNKNOWN"}));
    const {ledgerReading,threadObservations,quotaSnapshots,...surfaceStatus} = inspection;
    const status = {...surfaceStatus,quotaWindows:quotaSnapshots.map((snapshot) => ({...snapshot,
      currentUsedPercent:snapshot.evidenceState === "verified" ? snapshot.usedPercent : null,
      currentRemainingPercent:snapshot.evidenceState === "verified" ? snapshot.remainingPercent : null,
      currentResetAt:snapshot.evidenceState === "verified" ? snapshot.resetAt : null,
      currentState:snapshot.evidenceState === "verified" ? "VERIFIED" : "UNKNOWN",
      resetState:snapshot.evidenceState !== "verified" ? "UNKNOWN" : snapshot.resetAt ? "REPORTED" : snapshot.provider === "anthropic" && snapshot.windowSlot.endsWith(":five_hour") && snapshot.usedPercent === 0 ? "NOT_STARTED" : "UNKNOWN",
    })),errors,report,receipt,exportReceipts,exportControlComparisons,dashboardThreadLinks,bankSourceRefresh,projectMetadataRefresh,
      subscriptionReconciliation:readSubscriptionReconciliation(home,inspection.observedAt)};
    atomicWrite(path.join(home,"subscription-telemetry-status.json"),JSON.stringify(status,null,2)+"\n");
    atomicWrite(path.join(home,"subscription-telemetry.html"),renderDashboard(status,report,localAccountLabels(paths.nativeUsageFile)));
    return {status:"subscription_telemetry_refreshed",observedAt:inspection.observedAt,...receipt,
      exportFiles:exportReceipts.length,exportErrors:errors.length,observedTokenEvents:report.observedTokenEvents};
  } finally { db.close(); }
}
