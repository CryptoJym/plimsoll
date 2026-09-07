import assert from "node:assert/strict";
import fs from "node:fs";
import {
  CdpClient,
  NeverResolvingSocket,
  ProofTimeoutError,
  runNeverResolvingCdpScenario,
} from "./dashboard-security-proof";

async function main() {
  const socket = new NeverResolvingSocket();
  const client = CdpClient.fromSocketForProof(socket);
  const privateExpression = "CDP_PRIVATE_EXPRESSION_SENTINEL";
  let timeout: unknown;
  try {
    await client.send("Runtime.evaluate", { expression: privateExpression }, { timeoutMs: 25 });
  } catch (error) {
    timeout = error;
  } finally {
    client.close();
  }
  assert(timeout instanceof ProofTimeoutError);
  assert.equal(timeout.message, "proof_timeout:cdp_command:Runtime.evaluate");
  assert.equal(typeof timeout.elapsedMs, "number");
  assert(!JSON.stringify(timeout).includes(privateExpression));
  assert.equal(client.pendingCount, 0);
  assert.equal(socket.listenerCount, 0);
  assert(socket.closed);
  console.log(JSON.stringify({ check: "command_timeout_identifies_method_without_parameters", passed: true }));

  const scenario = await runNeverResolvingCdpScenario();
  assert(scenario.timeoutError instanceof ProofTimeoutError);
  assert.equal(scenario.timeoutError.message, "proof_timeout:browser_proof_overall");
  assert.equal(scenario.socket.sends, 2, "the watchdog must interrupt an actual CDP command before Browser.close");
  assert.deepEqual(scenario.operationSettlement, { status: "settled", value: "rejected" });
  assert.equal(scenario.shutdownReceipt?.escalatedTo, "SIGKILL");
  assert(scenario.child.exitCode !== null || scenario.child.signalCode !== null);
  assert(!fs.existsSync(scenario.profile));
  assert.equal(scenario.cdp.pendingCount, 0);
  assert.equal(scenario.socket.listenerCount, 0);
  assert(scenario.socket.closed);
  console.log(JSON.stringify({ check: "slow_fixture_startup_precedes_cdp_watchdog_and_cleanup", passed: true }));
  console.log(JSON.stringify({ proof: "dashboard-security-cdp", checks: 2, passed: 2, failed: [] }));
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
