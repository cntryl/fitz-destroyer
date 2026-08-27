import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { assertLeaseWaiterRaceEvidence } from "../workloads/lease-waiter-disconnect-races.js";
import { requiredEvent } from "./workload-log.js";

export async function runLeaseWaiterDisconnectRacesScenario(stack: ComposeStack, _config: RunConfig, shape: WorkloadShape, artifacts: Artifacts): Promise<void> {
  const startedAt = performance.now();
  const role = await stack.startRoleContainers("lease-waiter-disconnect-races", 1, shape);
  const logs = await stack.finishRoleContainers(role, "lease-waiter-disconnect-races");
  const log = logs.get("0");
  if (log === undefined) throw new Error("lease waiter race worker log was missing");
  const evidence = requiredEvent(log, "lease_waiter_disconnect_races_worker_complete");
  assertLeaseWaiterRaceEvidence(evidence);
  await artifacts.writeJson("lease-waiter-disconnect-races-evidence.json", evidence);
  await artifacts.event("lease_waiter_disconnect_races_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt) });
}
