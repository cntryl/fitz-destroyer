import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { assertSessionBoundaryEvidence } from "./half-open-session.js";

export async function runBrokerPauseScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const holder = await stack.startRoleContainers("session-boundaries", 1, shape, {
    DESTROYER_SEED: String(shape.seed),
  });
  await stack.waitForRoleEvent(holder, "session_boundaries_armed");
  const pausedAt = performance.now();
  try {
    await stack.pauseFitz();
    await stack.waitForRoleEvent(holder, "session_boundaries_disconnect_observed");
  } finally {
    await stack.unpauseFitz();
  }
  const pausedMs = Math.round(performance.now() - pausedAt);
  await stack.ensureReady();
  const logs = await stack.finishRoleContainers(holder, "broker-pause");
  const log = logs.get("0");
  if (log === undefined) throw new Error("Broker pause worker log was missing");
  assertSessionBoundaryEvidence(log);
  await artifacts.event("broker_pause_complete", {
    pausedMs,
    staleRejections: 4,
    queueRedelivered: 1,
    leaseReacquired: 1,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}
