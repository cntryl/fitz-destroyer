import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { runLeaseContentionScenario } from "./lease-contention.js";
import { runRpcStreamHose } from "./rpc-stream-hose.js";

export async function runLiveChurnScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const phases: Array<{ phase: string; elapsedMs: number }> = [];

  let phaseStarted = performance.now();
  await stack.runConnectionStorm(shape);
  phases.push({ phase: "notice-subscription-and-rpc-registration-churn", elapsedMs: elapsed(phaseStarted) });

  phaseStarted = performance.now();
  await runLeaseContentionScenario(stack, config, shape, artifacts);
  phases.push({ phase: "lease-expiry-renewal-loss-and-waiters", elapsedMs: elapsed(phaseStarted) });

  phaseStarted = performance.now();
  await runRpcStreamHose(stack, config, shape, artifacts);
  phases.push({ phase: "rpc-worker-replacement-during-active-calls", elapsedMs: elapsed(phaseStarted) });

  const ledger = { phases, elapsedMs: elapsed(startedAt) };
  await artifacts.writeJson("live-churn-ledger.json", ledger);
  await artifacts.event("live_churn_complete", ledger);
}

function elapsed(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}
