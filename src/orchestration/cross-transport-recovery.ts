import type { RunConfig } from "../config.js";
import { totalDurableEntries, type WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";

export async function runCrossTransportRecoveryScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const reverse = { ...shape, namespace: `${shape.namespace}-tcp-source` };

  await stack.runRecoveryJob("load", shape, "ws");
  await stack.discardFitzCacheAndRestart();
  await stack.runRecoveryJob("verify", shape, "tcp");

  await stack.runRecoveryJob("load", reverse, "tcp");
  await stack.gracefulRestartFitz();
  await stack.runRecoveryJob("verify", reverse, "ws");

  await artifacts.event("cross_transport_recovery_complete", {
    websocketToTcpVerified: totalDurableEntries(shape),
    tcpToWebsocketVerified: totalDurableEntries(reverse),
    transports: ["ws", "tcp"],
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}
