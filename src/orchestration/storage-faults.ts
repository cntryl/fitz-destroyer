import type { RunConfig } from "../config.js";
import { normalizeErrorClass } from "../pressure.js";
import { DURABLE_DOMAINS, type DurableDomain, type WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import {
  analyzeDurabilityLedger,
  assertDurabilityLedger,
  type DomainLedger,
} from "./durability-crash-cuts.js";
import { parseJsonRecords } from "./workload-log.js";

export const STORAGE_FAULT_IDENTITIES = [
  "bounded-latency",
  "connection-reset",
  "provider-partition",
  "provider-recovery",
  "fitz-crash-inflight",
] as const;
export type StorageFaultIdentity = (typeof STORAGE_FAULT_IDENTITIES)[number];
export type StorageFailureLayer = "none" | "admission" | "routing" | "persistence" | "recovery";

export type StorageFaultIteration = {
  iteration: number;
  sequence: number;
  seed: number;
  fault: StorageFaultIdentity;
};

export type StorageFaultLedger = {
  seed: number;
  iterations: readonly (StorageFaultIteration & {
    outcomes: Record<DurableDomain, "acknowledged" | "failed" | "ambiguous">;
    failureLayers: readonly StorageFailureLayer[];
    errorSamples: readonly string[];
  })[];
  domains: DomainLedger;
};

export async function runStorageFaultsScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const plan = storageFaultPlan(config.iterations, shape.seed);
  const environment = {
    DESTROYER_SEED: String(shape.seed),
    DESTROYER_DURABILITY_ITERATIONS: String(config.iterations),
  };
  await artifacts.event("storage_faults_started", { plan });
  await stack.setStorageProxyFault({ mode: "healthy" });

  const baseline = await stack.startRoleContainers("durability-writer", 1, shape, {
    ...environment,
    DESTROYER_DURABILITY_ACTION: "baseline",
    DESTROYER_DURABILITY_SEQUENCE: "0",
  });
  const baselineLogs = await stack.finishRoleContainers(baseline, "storage-faults-baseline");
  const cutLogs = new Map<string, string>();
  const iterationEvidence: StorageFaultLedger["iterations"][number][] = [];

  for (const iteration of plan) {
    const iterationStartedAt = performance.now();
    const logs = await runStorageFaultIteration(stack, shape, environment, iteration);
    for (const [worker, log] of logs) cutLogs.set(`${iteration.iteration}-${worker}`, log);
    const evidence = storageOperationEvidence(logs, iteration.sequence);
    iterationEvidence.push({ ...iteration, ...evidence });
    await artifacts.event("storage_fault_iteration_complete", {
      ...iteration,
      ...evidence,
      elapsedMs: Math.round(performance.now() - iterationStartedAt),
    });
  }

  await stack.setStorageProxyFault({ mode: "healthy" });
  await stack.ensureReady();
  const verifier = await stack.startRoleContainers("durability-verifier", 1, shape, {
    ...environment,
    DESTROYER_DURABILITY_SEQUENCE: "0",
  });
  let verifyLogs: ReadonlyMap<string, string>;
  try {
    verifyLogs = await stack.finishRoleContainers(verifier, "storage-faults-verify");
  } catch (error) {
    const partial = await stack.roleLogs(verifier);
    const ledger: StorageFaultLedger = {
      seed: shape.seed,
      iterations: iterationEvidence.map((item) => ({
        ...item,
        failureLayers: [...new Set([...item.failureLayers, "recovery" as const])],
      })),
      domains: emptyDomainLedger(),
    };
    await artifacts.writeJson("storage-fault-ledger.json", {
      ...ledger,
      recoveryError: errorMessage(error),
      verifierLogs: Object.fromEntries(partial),
    });
    throw error;
  }
  const domains = analyzeDurabilityLedger(baselineLogs, cutLogs, verifyLogs);
  const ledger: StorageFaultLedger = { seed: shape.seed, iterations: iterationEvidence, domains };
  await artifacts.writeJson("storage-fault-ledger.json", ledger);
  assertDurabilityLedger(domains, config.iterations);
  await artifacts.event("storage_faults_complete", {
    iterations: iterationEvidence.length,
    domains,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function storageFaultPlan(iterations: number, seed: number): StorageFaultIteration[] {
  return Array.from({ length: iterations }, (_, index) => ({
    iteration: index + 1,
    sequence: index + 1,
    seed,
    fault: STORAGE_FAULT_IDENTITIES[index % STORAGE_FAULT_IDENTITIES.length]!,
  }));
}

export function classifyStorageFailure(error: string): StorageFailureLayer {
  if (/\b(?:recover|hydrate|replay|restore)\w*\b/iu.test(error)) return "recovery";
  if (/\b(?:storage|provider|persist|durab|cloud|s3|sqrzl|object)\w*\b/iu.test(error)) {
    return "persistence";
  }
  const normalized = normalizeErrorClass(new Error(error));
  if (normalized === "connection" || normalized === "timeout" || normalized === "protocol") {
    return "routing";
  }
  return "admission";
}

async function runStorageFaultIteration(
  stack: ComposeStack,
  shape: WorkloadShape,
  environment: Readonly<Record<string, string>>,
  iteration: StorageFaultIteration,
): Promise<ReadonlyMap<string, string>> {
  await stack.setStorageProxyFault({ mode: "healthy" });
  const waitForGate = iteration.fault !== "bounded-latency" && iteration.fault !== "provider-recovery";

  if (iteration.fault === "bounded-latency") {
    await stack.setStorageProxyFault({ mode: "latency", latencyMs: 250 });
  } else if (iteration.fault === "connection-reset") {
    await stack.setStorageProxyFault({ mode: "reset" });
  } else if (iteration.fault === "provider-partition" || iteration.fault === "fitz-crash-inflight") {
    await stack.setStorageProxyFault({ mode: "partition" });
  }

  const writer = await stack.startRoleContainers("durability-writer", 1, shape, {
    ...environment,
    DESTROYER_DURABILITY_ACTION: "cut",
    DESTROYER_DURABILITY_SEQUENCE: String(iteration.sequence),
    ...(waitForGate ? { DESTROYER_WAIT_FOR_START_SIGNAL: "true" } : {}),
  });
  if (waitForGate) {
    await stack.waitForRoleEvent(writer, "live_producer_ready");
    await stack.signalRoleContainers(writer, "SIGUSR1");
    await stack.waitForRoleEvent(writer, "durability_operations_dispatched");
  }

  if (iteration.fault === "connection-reset") {
    await sleep(250);
    await stack.setStorageProxyFault({ mode: "healthy" });
  } else if (iteration.fault === "provider-partition") {
    await sleep(5_000);
    await stack.setStorageProxyFault({ mode: "healthy" });
  } else if (iteration.fault === "fitz-crash-inflight") {
    await stack.killFitz();
    await stack.setStorageProxyFault({ mode: "healthy" });
    await stack.restartFitz();
  }

  const logs = await stack.finishRoleContainers(
    writer,
    `storage-fault-${iteration.iteration.toString().padStart(3, "0")}-${iteration.fault}`,
  );
  await stack.setStorageProxyFault({ mode: "healthy" });
  await stack.ensureReady();
  return logs;
}

function storageOperationEvidence(
  logs: ReadonlyMap<string, string>,
  sequence: number,
): Pick<StorageFaultLedger["iterations"][number], "outcomes" | "failureLayers" | "errorSamples"> {
  const outcomes = Object.fromEntries(
    DURABLE_DOMAINS.map((domain) => [domain, "ambiguous"]),
  ) as Record<DurableDomain, "acknowledged" | "failed" | "ambiguous">;
  const errorSamples: string[] = [];
  for (const log of logs.values()) {
    for (const record of parseJsonRecords(log)) {
      if (record.sequence !== sequence || typeof record.domain !== "string") continue;
      const domain = DURABLE_DOMAINS.find((candidate) => candidate === record.domain);
      if (domain === undefined) continue;
      if (record.event === "durability_operation_acknowledged") outcomes[domain] = "acknowledged";
      if (record.event === "durability_operation_failed" && outcomes[domain] !== "acknowledged") {
        outcomes[domain] = "failed";
        if (typeof record.error === "string" && errorSamples.length < 20) errorSamples.push(record.error);
      }
    }
  }
  const failureLayers = [...new Set(errorSamples.map(classifyStorageFailure))];
  return { outcomes, failureLayers, errorSamples };
}

function emptyDomainLedger(): DomainLedger {
  return {
    queue: { started: [], acknowledged: [], failed: [], observed: [] },
    kv: { started: [], acknowledged: [], failed: [], observed: [] },
    stream: { started: [], acknowledged: [], failed: [], observed: [] },
    schedule: { started: [], acknowledged: [], failed: [], observed: [] },
  };
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
