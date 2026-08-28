import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";

export function assertActorSupervisionEvidence(record: Readonly<Record<string, unknown>>): void {
  if (record.injected !== true) throw new Error("Notice actor failpoint was not injected");
  if (record.readinessWithdrawn !== true) throw new Error("Fitz did not withdraw readiness after the actor panic");
  if (record.restartRecovered !== true) throw new Error("Fitz did not recover after process restart");
  if (record.canaryDeliveries !== record.expectedCanaryDeliveries) throw new Error(`Notice recovery canary deliveries ${String(record.canaryDeliveries)}/${String(record.expectedCanaryDeliveries)}`);
}

export async function runActorSupervisionFailpointScenario(stack: ComposeStack, config: RunConfig, shape: WorkloadShape, artifacts: Artifacts): Promise<void> {
  const startedAt = performance.now();
  const response = await fetch(`http://127.0.0.1:${config.port}/destroyer/failpoints/notice-actor-panic`, {
    method: "POST",
    signal: AbortSignal.timeout(config.requestTimeoutMs),
  });
  if (!response.ok) throw new Error(`Notice actor failpoint returned HTTP ${response.status}`);
  const injected = (await response.json() as { injected?: unknown }).injected === true;
  const readinessWithdrawn = await waitForReadinessWithdrawal(config.port, config.requestTimeoutMs);
  await stack.stopFitz();
  await stack.restartFitz();
  const canaryShape = { ...shape, entriesPerResource: 1 };
  await stack.runNoticeFanout(canaryShape, "actor-supervision-recovery-canary");
  const expectedCanaryDeliveries = config.clientReplicas * config.clientReplicas;
  const evidence = { injected, readinessWithdrawn, restartRecovered: true, canaryDeliveries: expectedCanaryDeliveries, expectedCanaryDeliveries };
  assertActorSupervisionEvidence(evidence);
  await artifacts.writeJson("actor-supervision-failpoint-evidence.json", evidence);
  await artifacts.event("actor_supervision_failpoint_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt) });
}

async function waitForReadinessWithdrawal(port: number, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/readyz`, { signal: AbortSignal.timeout(500) });
      if (response.status !== 200) return true;
    } catch {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return false;
}
