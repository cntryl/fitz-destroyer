import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { assertSessionBoundaryEvidence } from "./half-open-session.js";

export async function runOutboundBlackholeScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const holder = await stack.startRoleContainers("session-boundaries", 1, shape, {
    FITZ_URL: "ws://client-proxy:4090/ws",
    DESTROYER_SEED: String(shape.seed),
  });
  await stack.waitForRoleEvent(holder, "session_boundaries_armed");
  try {
    await stack.setFaultProxy("client-proxy", { mode: "downstream-drop" });
    await stack.waitForRoleEvent(holder, "session_boundaries_disconnect_observed");
  } finally {
    await stack.setFaultProxy("client-proxy", { mode: "healthy" }).catch(() => undefined);
  }
  const logs = await stack.finishRoleContainers(holder, "outbound-blackhole");
  const log = logs.get("0");
  if (log === undefined) throw new Error("Outbound blackhole worker log was missing");
  assertSessionBoundaryEvidence(log);
  await artifacts.event("outbound_blackhole_complete", {
    staleRejections: 4,
    queueRedelivered: 1,
    leaseReacquired: 1,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}
