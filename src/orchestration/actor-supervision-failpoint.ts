import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { runQueueRedeliveryScenario } from "./queue-redelivery.js";
import { requiredEvent } from "./workload-log.js";

export function assertActorSupervisionEvidence(record: Readonly<Record<string, unknown>>): void {
  if (record.domainsInjected !== 5) throw new Error(`actor failpoints injected for ${String(record.domainsInjected)}/5 domains`);
  if (record.readinessWithdrawals !== 5) throw new Error(`readiness withdrawals ${String(record.readinessWithdrawals)}/5`);
  if (record.restartsRecovered !== 5) throw new Error(`restart recoveries ${String(record.restartsRecovered)}/5`);
  if (record.canaryDeliveries !== record.expectedCanaryDeliveries) throw new Error(`Notice recovery canary deliveries ${String(record.canaryDeliveries)}/${String(record.expectedCanaryDeliveries)}`);
  if (record.queueRecovered !== record.expectedQueueRecovered) throw new Error(`Queue recovery ${String(record.queueRecovered)}/${String(record.expectedQueueRecovered)}`);
  if (record.kvRecovered !== 1) throw new Error(`KV recovery ${String(record.kvRecovered)}/1`);
  if (record.leaseRecovered !== 1) throw new Error(`Lease recovery ${String(record.leaseRecovered)}/1`);
  if (record.scheduleRecovered !== 1) throw new Error(`Schedule recovery ${String(record.scheduleRecovered)}/1`);
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
  await injectAndRestart("kv", stack, config);
  domainsInjected += 1;
  readinessWithdrawals += 1;
  restartsRecovered += 1;
  const kvCanary = await stack.startRoleContainers("canary", 1, canaryShape, {
    DESTROYER_NAMESPACE: recoveryCanaryNamespace(shape.namespace, "kv"),
  });
  const kvCanaryLogs = await stack.finishRoleContainers(kvCanary, "actor-supervision-kv-recovery-canary");
  const kvComplete = requiredEvent(onlyLog(kvCanaryLogs), "canary_complete");
  const kvRecovered = Array.isArray(kvComplete.domains) && kvComplete.domains.includes("kv") && kvComplete.operationsPerDomain === 1 ? 1 : 0;
  await injectAndRestart("lease", stack, config);
  domainsInjected += 1;
  readinessWithdrawals += 1;
  restartsRecovered += 1;
  const leaseCanary = await stack.startRoleContainers("canary", 1, canaryShape, {
    DESTROYER_NAMESPACE: recoveryCanaryNamespace(shape.namespace, "lease"),
  });
  const leaseCanaryLogs = await stack.finishRoleContainers(leaseCanary, "actor-supervision-lease-recovery-canary");
  const leaseComplete = requiredEvent(onlyLog(leaseCanaryLogs), "canary_complete");
  const leaseRecovered = Array.isArray(leaseComplete.domains) && leaseComplete.domains.includes("lease") && leaseComplete.operationsPerDomain === 1 ? 1 : 0;
  await injectAndRestart("schedule", stack, config);
  domainsInjected += 1;
  readinessWithdrawals += 1;
  restartsRecovered += 1;
  const scheduleCanary = await stack.startRoleContainers("canary", 1, canaryShape, {
    DESTROYER_NAMESPACE: recoveryCanaryNamespace(shape.namespace, "schedule"),
  });
  const scheduleCanaryLogs = await stack.finishRoleContainers(scheduleCanary, "actor-supervision-schedule-recovery-canary");
  const scheduleComplete = requiredEvent(onlyLog(scheduleCanaryLogs), "canary_complete");
  const scheduleRecovered = Array.isArray(scheduleComplete.domains) && scheduleComplete.domains.includes("schedule") && scheduleComplete.operationsPerDomain === 1 ? 1 : 0;
  const evidence = { domainsInjected, readinessWithdrawals, restartsRecovered, canaryDeliveries: expectedCanaryDeliveries, expectedCanaryDeliveries, queueRecovered: expectedQueueRecovered, expectedQueueRecovered, kvRecovered, leaseRecovered, scheduleRecovered };
  assertActorSupervisionEvidence(evidence);
  await artifacts.writeJson("actor-supervision-failpoint-evidence.json", evidence);
  await artifacts.event("actor_supervision_failpoint_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt) });
}

export function recoveryCanaryNamespace(namespace: string, domain: string): string {
  return `${namespace}-${domain}-recovery`;
}

async function injectAndRestart(domain: "notice" | "queue" | "kv" | "lease" | "schedule", stack: ComposeStack, config: RunConfig): Promise<void> {
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

function onlyLog(logs: ReadonlyMap<string, string>): string {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) throw new Error(`Expected one canary log, found ${logs.size}`);
  return log;
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
