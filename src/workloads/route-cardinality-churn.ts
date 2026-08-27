import type { Client } from "@cntryl/fitz";
import { allCanaryDomains, runCanaryOperation } from "./canary.js";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import type { Domain } from "./model.js";

export async function runRouteCardinalityChurn(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  let completed = 0;
  for (let sequence = 0; sequence < options.operations; sequence += 1) {
    for (const domain of allCanaryDomains()) {
      await runCanaryOperation(
        client,
        { ...options, domains: allCanaryDomains() },
        domain,
        sequence,
        routeCardinalityRoute(domain, options.namespace, sequence),
      );
      completed += 1;
    }
  }
  log("route_cardinality_churn_complete", {
    routes: completed,
    domains: allCanaryDomains(),
    operationsPerDomain: options.operations,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function routeCardinalityRoute(domain: Domain, namespace: string, sequence: number): string {
  const suffix = sequence.toString().padStart(8, "0");
  if (domain === "schedule") {
    return `schedule://destroyer/${namespace}/cardinality/job-${suffix}`;
  }
  return `${domain}://destroyer/${namespace}/cardinality-${suffix}`;
}
