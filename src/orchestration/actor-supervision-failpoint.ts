import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { runQueueRedeliveryScenario } from "./queue-redelivery.js";

export function assertActorSupervisionEvidence(record: Readonly<Record<string, unknown>>): void {
  if (record.domainsInjected !== 2) throw new Error(`actor failpoints injected for ${String(record.domainsInjected)}/2 domains`);
  if (record.readinessWithdrawals !== 2) throw new Error(`readiness withdrawals ${String(record.readinessWithdrawals)}/2`);
  if (record.restartsRecovered !== 2) throw new Error(`restart recoveries ${String(record.restartsRecovered)}/2`);
  if (record.canaryDeliveries !== record.expectedCanaryDeliveries) throw new Error(`Notice recovery canary deliveries ${String(record.canaryDeliveries)}/${String(record.expectedCanaryDeliveries)}`);
  if (record.queueRecovered !== record.expectedQueueRecovered) throw new Error(`Queue recovery ${String(record.queueRecovered)}/${String(record.expectedQueueRecovered)}`);
}

export async function runActorSupervisionFailpointScenario(stack: ComposeStack, config: RunConfig, shape: WorkloadShape, artifacts: Artifacts): Promise<void> {
  const startedAt = performance.now();
  let domainsInjected = 0;
  let readinessWithdrawals = 0;
  let restartsRecovered = 0;
  await injectAndRestart("notice", stack, config);
  domainsInjected += 1;
  readinessWithdrawals += 1;
  restartsRecovered += 1;
  const canaryShape = { ...shape, entriesPerResource: 1 };
  await stack.runNoticeFanout(canaryShape, "actor-supervision-recovery-canary");
  const expectedCanaryDeliveries = config.clientReplicas * config.clientReplicas;
  await injectAndRestart("queue", stack, config);
  domainsInjected += 1;
  readinessWithdrawals += 1;
  restartsRecovered += 1;
  await runQueueRedeliveryScenario(stack, config, canaryShape, artifacts);
  const expectedQueueRecovered = 1;
  const evidence = { domainsInjected, readinessWithdrawals, restartsRecovered, canaryDeliveries: expectedCanaryDeliveries, expectedCanaryDeliveries, queueRecovered: expectedQueueRecovered, expectedQueueRecovered };
  assertActorSupervisionEvidence(evidence);
  await artifacts.writeJson("actor-supervision-failpoint-evidence.json", evidence);
  await artifacts.event("actor_supervision_failpoint_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt) });
}

async function injectAndRestart(domain: "notice" | "queue", stack: ComposeStack, config: RunConfig): Promise<void> {
  const response = await fetch(`http://127.0.0.1:${config.port}/destroyer/failpoints/${domain}-actor-panic`, {
    method: "POST",
    signal: AbortSignal.timeout(config.requestTimeoutMs),
  });
  if (!response.ok) throw new Error(`${domain} actor failpoint returned HTTP ${response.status}`);
  const injected = (await response.json() as { injected?: unknown }).injected === true;
  if (!injected) throw new Error(`${domain} actor failpoint did not confirm injection`);
  if (!await waitForReadinessWithdrawal(config.port, config.requestTimeoutMs)) throw new Error(`${domain} actor panic did not withdraw readiness`);
  await stack.stopFitz();
  await stack.restartFitz();
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
