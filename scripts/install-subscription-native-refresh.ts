import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {createRequire} from "node:module";
import {spawnSync} from "node:child_process";
import {buildSync} from "esbuild";
import {defaultNativeSubscriptionPaths} from "../packages/collector-cli/src/native-subscription";

const label="com.utlyze.plimsoll-subscription-telemetry";
const paths=defaultNativeSubscriptionPaths();
const home=path.dirname(paths.nativeSubscriptionDb);
const repo=path.resolve(process.cwd());
if (!fs.existsSync(path.join(repo,"scripts/subscription-native-refresh.ts"))) throw new Error("subscription_refresh_run_from_repository_root");
const result=buildSync({entryPoints:[path.join(repo,"scripts/subscription-native-refresh.ts")],
  bundle:true,platform:"node",format:"esm",target:"node22",write:false,outfile:"refresh.mjs",
  external:["better-sqlite3"],logLevel:"silent"});
const contents=result.outputFiles[0].contents;
const digest=crypto.createHash("sha256").update(contents).digest("hex");
const release=path.join(home,"subscription-native-runtime",digest.slice(0,16));
const entry=path.join(release,"refresh.mjs");
const plist=path.join(os.homedir(),"Library/LaunchAgents",`${label}.plist`);
const xml=(value:string)=>value.replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&apos;"})[c]!);
const body=`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>${label}</string>
<key>ProgramArguments</key><array><string>${xml(process.execPath)}</string><string>${xml(entry)}</string></array>
<key>RunAtLoad</key><true/><key>StartInterval</key><integer>60</integer>
<key>ProcessType</key><string>Standard</string>
<key>StandardOutPath</key><string>${xml(path.join(home,"subscription-native-refresh.log"))}</string>
<key>StandardErrorPath</key><string>${xml(path.join(home,"subscription-native-refresh-errors.log"))}</string>
</dict></plist>\n`;
if (!process.argv.includes("--install")) {
  console.log(JSON.stringify({status:"prepared",label,intervalSeconds:60,bundleSha256:digest,scope:"local subscription feed, bounded ledger reads, verified CSV inbox; no provider model calls"},null,2));
} else {
  if (fs.existsSync(plist) && fs.readFileSync(plist,"utf8")!==body) throw new Error("subscription_refresh_existing_service_requires_inspection");
  const bankHelperSource=path.join(repo,"scripts/subscription-bank-source-refresh.py");
  const bankHelper=fs.readFileSync(bankHelperSource);
  const bankHelperSha256=`sha256:${crypto.createHash("sha256").update(bankHelper).digest("hex")}`;
  fs.mkdirSync(home,{recursive:true,mode:0o700});
  fs.writeFileSync(path.join(home,"subscription-bank-source-refresh.py"),bankHelper,{mode:0o600});
  fs.chmodSync(path.join(home,"subscription-bank-source-refresh.py"),0o600);
  fs.writeFileSync(path.join(home,"subscription-bank-source-refresh-receipt.json"),JSON.stringify({sha256:bankHelperSha256,installedAt:new Date().toISOString(),scope:"read-only stored company tracker; no Plaid calls"},null,2)+"\n",{mode:0o600});
  fs.mkdirSync(path.join(release,"node_modules"),{recursive:true,mode:0o700});
  fs.writeFileSync(entry,contents,{mode:0o600});
  const require=createRequire(path.join(repo,"package.json"));
  const driver=fs.realpathSync(path.dirname(require.resolve("better-sqlite3/package.json")));
  const driverLink=path.join(release,"node_modules/better-sqlite3");
  if (!fs.existsSync(driverLink)) fs.symlinkSync(driver,driverLink,"dir");
  const smoke=spawnSync(process.execPath,[entry],{encoding:"utf8",timeout:45_000,maxBuffer:64*1024});
  if (smoke.status!==0 || !smoke.stdout.includes('"status":"subscription_telemetry_refreshed"')) throw new Error("subscription_refresh_runtime_check_failed");
  fs.mkdirSync(path.dirname(plist),{recursive:true,mode:0o700});
  const alreadyConfigured=fs.existsSync(plist);
  fs.writeFileSync(plist,body,{mode:0o600});
  fs.writeFileSync(path.join(release,"receipt.json"),JSON.stringify({label,intervalSeconds:60,bundleSha256:digest,bankHelperSha256,bankRefreshIntervalSeconds:3600,createdAt:new Date().toISOString(),node:process.version},null,2)+"\n",{mode:0o600});
  if (!alreadyConfigured) {
    const bootstrap=spawnSync("/bin/launchctl",["bootstrap",`gui/${process.getuid!()}`,plist],{encoding:"utf8",timeout:10_000,maxBuffer:16*1024});
    if (bootstrap.status!==0) throw new Error("subscription_refresh_launchd_bootstrap_failed");
  }
  console.log(JSON.stringify({status:alreadyConfigured?"configured":"installed",label,intervalSeconds:60,bundleSha256:digest,runtimeCheck:"passed"},null,2));
}
