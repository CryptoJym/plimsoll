/** Two bounded test workers, each with its own actual SQLite connection. */
import { parentPort, workerData } from "node:worker_threads";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { authenticateLiveProducer } from "../packages/collector-cli/src/codex-live-usage-auth";
import { ingestLiveUsage } from "../packages/collector-cli/src/codex-live-usage-ledger";
import { canonicalJson, liveSha256 } from "../packages/collector-cli/src/codex-live-usage-protocol";
const w=workerData;
const buffer=new LocalEventBuffer(w.file,{workspaceId:w.config.tenantId,deviceId:w.config.deviceId,databaseBusyTimeoutMs:100,
  enrollmentNow:()=>new Date(w.epochStart),delivery:{enabled:true}});
parentPort!.postMessage({ready:true});
const barrier=new Int32Array(w.barrier);
if(Atomics.wait(barrier,0,0,5000)==="timed-out") throw new Error("race_barrier_timeout");
try {
  const result=w.event ? buffer.append(w.event,[],{integrityReceipt:true}) : ingestLiveUsage(buffer,w.packet,
    liveSha256(canonicalJson(w.packet)),()=>authenticateLiveProducer(w.home,buffer,w.config,w.packet.producerId,w.token));
  parentPort!.postMessage({result});
} catch (error) {
  if ((error as {code?:string}).code === "SQLITE_BUSY") parentPort!.postMessage({result:{busy:true}});
  else throw error;
} finally {buffer.close();}
