import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { defaultNativeSubscriptionPaths, type NativeSubscriptionPaths } from "./native-subscription";
import { collectNativeTelemetry, nativeTelemetryReport } from "./native-telemetry-store";
import { importCursorSubscriptionExport } from "./subscription-exports";

function atomicWrite(file: string, contents: string) {
  const temp = `${file}.${process.pid}.tmp`;
  try { fs.writeFileSync(temp,contents,{mode:0o600}); fs.renameSync(temp,file); }
  finally { if (fs.existsSync(temp)) fs.unlinkSync(temp); }
}

function escape(value: unknown): string {
  return String(value ?? "UNKNOWN").replace(/[&<>"']/g,(c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]!);
}

function renderDashboard(status: Record<string, unknown>, report: ReturnType<typeof nativeTelemetryReport>): string {
  const providers = status.providers as Array<{provider:string;accountCount:number;planTypes:string[];subscription:{state:string};quota:{state:string};threadTelemetry:{tokenState:string}}>;
  const providerRows = providers.map((provider) => `<tr><td>${escape(provider.provider)}</td><td>${provider.accountCount}</td><td>${escape(provider.planTypes.join(", "))}</td><td>${escape(provider.subscription.state.toUpperCase())}</td><td>${escape(provider.quota.state.toUpperCase())}</td><td>${escape(provider.threadTelemetry.tokenState.toUpperCase())}</td></tr>`).join("");
  const threads = report.threads.slice(0,50).map((thread) => `<tr><td>${escape(thread.source)}</td><td><code>${escape(String(thread.threadId).slice(0,24))}</code></td><td>${escape(thread.projectLabel ?? thread.projectKey)}</td><td>${escape(thread.projectState)}</td><td>${escape(thread.observedInputTokens)}</td><td>${escape(thread.observedOutputTokens)}</td><td>${escape(thread.observedCacheReadTokens)}</td></tr>`).join("");
  const exports = report.dashboardExports.map((item) => `<li>${escape(item.provider)}: ${escape(item.records)} export rows; ${escape(item.observedTotalTokens)} observed tokens; ${escape(item.reportedOnDemandUsageUsd)} reported on-demand USD. Imported ${escape(item.importedAt)}; thread IDs present on ${escape(item.rowsWithThreadId)} rows. Completeness: UNKNOWN. New provider data requires a new dashboard export.</li>`).join("");
  const windows = (status.quotaWindows as Array<Record<string,unknown>>).map((window) => `<tr><td>${escape(window.provider)}</td><td><code>${escape(String(window.accountRef ?? "UNKNOWN").slice(0,19))}</code></td><td>${escape(window.planType)}</td><td>${escape(window.windowLabel ?? window.windowSlot)}</td><td>${escape(window.currentUsedPercent)}</td><td>${escape(window.currentRemainingPercent)}</td><td>${escape(window.currentResetAt ?? window.resetState)}</td><td>${escape(window.currentState)}</td></tr>`).join("");
  const comparisons = (status.exportControlComparisons as Array<{note:string}>).map((comparison) => `<p>${escape(comparison.note)}</p>`).join("");
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="60"><title>Subscription telemetry</title><style>body{font:15px system-ui;max-width:1180px;margin:36px auto;padding:0 20px;color:#163127;background:#f7faf8}h1{font-size:28px}h2{font-size:20px;margin-top:30px}table{border-collapse:collapse;width:100%;background:white}td,th{padding:10px;text-align:left;border-bottom:1px solid #dbe5df}code{font-size:12px}p{line-height:1.5}small{color:#52655a}.scroll{overflow:auto}</style></head><body><h1>Subscription telemetry</h1><p>Updated ${escape(status.observedAt)}. Native allowances, locally observed tokens, and actual money paid are separate measures.</p><div class="scroll"><table><thead><tr><th>Provider</th><th>Account records</th><th>Observed plans</th><th>Plan evidence</th><th>Current quota</th><th>Local token capture</th></tr></thead><tbody>${providerRows}</tbody></table></div><h2>Current account allowances</h2><p>UNKNOWN windows retain their last known readings in the JSON evidence, without displaying those readings as current. Account references are private aliases.</p><div class="scroll"><table><thead><tr><th>Provider</th><th>Account</th><th>Plan</th><th>Window</th><th>Used %</th><th>Remaining %</th><th>Reset</th><th>State</th></tr></thead><tbody>${windows}</tbody></table></div><h2>Provider exports</h2><ul>${exports || "<li>No verified token export imported. UNKNOWN.</li>"}</ul>${comparisons}<h2>Recent chats and projects</h2><p>Projects labeled inferred come from native repository metadata. Token figures cover captured events; complete history and account-to-subscription allocation remain UNKNOWN. Settled bank charges remain UNKNOWN.</p><div class="scroll"><table><thead><tr><th>Source</th><th>Chat</th><th>Project</th><th>Link</th><th>Input</th><th>Output</th><th>Cache read</th></tr></thead><tbody>${threads || '<tr><td colspan="7">UNKNOWN: no captured thread token events</td></tr>'}</tbody></table></div><p><small>${escape(report.note)}</small></p></body></html>`;
}

export function refreshSubscriptionTelemetry(options: { paths?: Partial<NativeSubscriptionPaths>; now?: string } = {}) {
  const paths = { ...defaultNativeSubscriptionPaths(), ...options.paths };
  const home = path.dirname(paths.nativeSubscriptionDb);
  fs.mkdirSync(home,{recursive:true,mode:0o700});
  const db = new Database(paths.nativeSubscriptionDb,{timeout:1_000});
  fs.chmodSync(paths.nativeSubscriptionDb,0o600);
  try {
    const {inspection,receipt} = collectNativeTelemetry(db,options);
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
    const {ledgerReading,threadObservations,quotaSnapshots,...surfaceStatus} = inspection;
    const status = {...surfaceStatus,quotaWindows:quotaSnapshots.map((snapshot) => ({...snapshot,
      currentUsedPercent:snapshot.evidenceState === "verified" ? snapshot.usedPercent : null,
      currentRemainingPercent:snapshot.evidenceState === "verified" ? snapshot.remainingPercent : null,
      currentResetAt:snapshot.evidenceState === "verified" ? snapshot.resetAt : null,
      currentState:snapshot.evidenceState === "verified" ? "VERIFIED" : "UNKNOWN",
      resetState:snapshot.evidenceState !== "verified" ? "UNKNOWN" : snapshot.resetAt ? "REPORTED" : snapshot.provider === "anthropic" && snapshot.windowSlot.endsWith(":five_hour") && snapshot.usedPercent === 0 ? "NOT_STARTED" : "UNKNOWN",
    })),errors,report,receipt,exportReceipts,exportControlComparisons};
    atomicWrite(path.join(home,"subscription-telemetry-status.json"),JSON.stringify(status,null,2)+"\n");
    atomicWrite(path.join(home,"subscription-telemetry.html"),renderDashboard(status,report));
    return {status:"subscription_telemetry_refreshed",observedAt:inspection.observedAt,...receipt,
      exportFiles:exportReceipts.length,exportErrors:errors.length,observedTokenEvents:report.observedTokenEvents};
  } finally { db.close(); }
}
