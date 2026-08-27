import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";

export async function runColdBootProviderOutageScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  await stack.runRecoveryJob("load", shape);
  await stack.stopFitz();
  await stack.setFaultProxy("storage-proxy", { mode: "partition" });
  let observations: readonly number[] = [];
  try {
    await stack.startFitzUnchecked();
    observations = await observeUnready(
      `http://127.0.0.1:${config.port}/readyz`,
      Math.max(1_000, Math.min(config.phaseMs, 5_000)),
    );
    assertColdBootObservation(observations);
  } finally {
    await stack.setFaultProxy("storage-proxy", { mode: "healthy" });
  }
  const recoveryStartedAt = performance.now();
  await stack.restartFitz();
  await stack.runRecoveryJob("verify", shape);
  const recoveryMs = Math.round(performance.now() - recoveryStartedAt);
  await artifacts.event("cold_boot_provider_outage_complete", {
    readinessChecks: observations.length,
    readyResponsesDuringOutage: observations.filter((status) => status === 200).length,
    recoveryMs,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function assertColdBootObservation(observations: readonly number[]): void {
  if (observations.length === 0) throw new Error("Cold-boot outage produced no readiness evidence");
  const ready = observations.filter((status) => status === 200).length;
  if (ready > 0) throw new Error(`Fitz reported ready ${ready} times while its provider was unavailable`);
}

async function observeUnready(url: string, durationMs: number): Promise<number[]> {
  const deadline = Date.now() + durationMs;
  const statuses: number[] = [];
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(500) });
      statuses.push(response.status);
      await response.body?.cancel();
    } catch {
      statuses.push(0);
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  return statuses;
}
