import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, parseJsonRecords, recordField, requiredEvent } from "./workload-log.js";

export async function runHotRouteCanaryScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const hot = await stack.startRoleContainers("hot-route", config.clientReplicas, shape, {
    DESTROYER_SHARED_ROUTE: "true",
  });
  await stack.waitForRoleEvent(hot, "progress");
  const canaryLogs = await runCanary(stack, shape, "hot-route-canary");
  await stack.signalRoleContainers(hot, "SIGTERM");
  const hotLogs = await stack.finishRoleContainers(hot, "hot-route-bombarders");
  const totals = hotRouteTotals(hotLogs, config.bombardDomains);
  for (const domain of config.bombardDomains) {
    if ((totals[domain]?.success ?? 0) === 0) {
      throw new Error(`Hot ${domain} route made no successful progress`);
    }
  }
  const canary = requiredEvent(onlyLog(canaryLogs), "canary_complete");
  await artifacts.event("hot_route_canary_complete", {
    hotTotals: totals,
    canaryMaximumMs: recordField(canary, "maximumMs"),
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export async function runProtocolAbuseScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const attackers = await stack.startRoleContainers("protocol-abuse", config.clientReplicas, shape);
  const attackLogs = await stack.finishRoleContainers(attackers, "protocol-abuse");
  let attacks = 0;
  for (const log of attackLogs.values()) {
    attacks += numericField(requiredEvent(log, "protocol_abuse_complete"), "completed");
  }
  const canaryLogs = await runCanary(stack, shape, "protocol-abuse-canary");
  const canary = requiredEvent(onlyLog(canaryLogs), "canary_complete");
  await artifacts.event("protocol_abuse_scenario_complete", {
    attacks,
    canaryMaximumMs: recordField(canary, "maximumMs"),
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

function runCanary(
  stack: ComposeStack,
  shape: WorkloadShape,
  label: string,
): Promise<Map<string, string>> {
  const canaryShape = { ...shape, entriesPerResource: Math.min(shape.entriesPerResource, 20) };
  return stack.startRoleContainers("canary", 1, canaryShape).then((containers) =>
    stack.finishRoleContainers(containers, label),
  );
}

function hotRouteTotals(
  logs: ReadonlyMap<string, string>,
  domains: readonly string[],
): Record<string, { success: number; error: number }> {
  const totals: Record<string, { success: number; error: number }> = {};
  for (const log of logs.values()) {
    const stopped = requiredEvent(log, "stopped");
    const workerTotals = recordField(stopped, "totals");
    for (const domain of domains) {
      const value = workerTotals[domain];
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new Error(`Hot route log omitted ${domain} totals`);
      }
      const record = value as Readonly<Record<string, unknown>>;
      const total = totals[domain] ?? { success: 0, error: 0 };
      total.success += numericField(record, "success");
      total.error += numericField(record, "error");
      totals[domain] = total;
    }
  }
  return totals;
}

function onlyLog(logs: ReadonlyMap<string, string>): string {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) throw new Error(`Expected one log, found ${logs.size}`);
  return log;
}

export function protocolAttackCount(log: string): number {
  return parseJsonRecords(log).filter(({ event }) => event === "protocol_attack_complete").length;
}
