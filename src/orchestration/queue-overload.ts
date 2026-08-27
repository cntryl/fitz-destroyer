import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, recordField, requiredEvent } from "./workload-log.js";

export type QueueOverloadProducerEvidence = {
  started: number;
  acknowledged: number[];
  failed: number[];
};

export async function runQueueOverloadRecoveryScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const workers = Math.max(2, config.clientReplicas);
  const operations = Math.max(64, config.liveConcurrency * 4);
  const faultShape = { ...shape, namespace: `${shape.namespace}-fault`, entriesPerResource: operations };
  const workerIds = Array.from({ length: workers }, (_, index) => String(index));
  await artifacts.event("queue_overload_recovery_started", {
    workers,
    operationsPerWorker: operations,
    fault: "storage-partition",
  });

  const producers = await stack.startRoleContainers("queue-overload-producer", workers, faultShape, {
    DESTROYER_QUEUE_OVERLOAD_ACTION: "produce",
    DESTROYER_REQUEST_TIMEOUT_MS: "1000",
    DESTROYER_WAIT_FOR_START_SIGNAL: "true",
  });
  await stack.waitForRoleEvent(producers, "live_producer_ready");
  await stack.setFaultProxy("storage-proxy", { mode: "partition" });
  await stack.signalRoleContainers(producers, "SIGUSR1");
  await stack.waitForRoleEvent(producers, "queue_overload_failure_observed");
  await stack.setFaultProxy("storage-proxy", { mode: "healthy" });

  const producerLogs = await stack.finishRoleContainers(producers, "queue-overload-fault");
  const evidence = producerEvidence(producerLogs);
  if (Object.values(evidence).reduce((total, item) => total + item.failed.length, 0) === 0) {
    throw new Error("Queue overload did not produce a bounded admission failure");
  }
  const drain = await drainQueue(stack, faultShape, workerIds, "queue-overload-fault-drain");
  assertQueueOverloadReconciled(evidence, drain);

  const probeShape = {
    ...shape,
    namespace: `${shape.namespace}-probe`,
    entriesPerResource: Math.max(8, Math.min(64, shape.entriesPerResource)),
  };
  const probe = await stack.startRoleContainers("queue-overload-producer", 1, probeShape, {
    DESTROYER_QUEUE_OVERLOAD_ACTION: "produce",
  });
  const probeLogs = await stack.finishRoleContainers(probe, "queue-overload-probe");
  const probeEvidence = producerEvidence(probeLogs);
  if (probeEvidence["0"]?.acknowledged.length !== probeShape.entriesPerResource) {
    throw new Error("Queue did not accept every post-overload probe operation");
  }
  const probeObserved = await drainQueue(stack, probeShape, ["0"], "queue-overload-probe-drain");
  assertQueueOverloadReconciled(probeEvidence, probeObserved);

  const failed = Object.values(evidence).reduce((total, item) => total + item.failed.length, 0);
  const recovered = Object.values(drain).reduce((total, values) => total + values.length, 0);
  await artifacts.writeJson("queue-overload-ledger.json", { evidence, observed: drain });
  await artifacts.event("queue_overload_recovery_complete", {
    attempted: workers * operations,
    failed,
    recovered,
    probeCompleted: probeShape.entriesPerResource,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

async function drainQueue(
  stack: ComposeStack,
  shape: WorkloadShape,
  workers: readonly string[],
  label: string,
): Promise<Record<string, number[]>> {
  const drainer = await stack.startRoleContainers("queue-overload-drainer", 1, shape, {
    DESTROYER_QUEUE_OVERLOAD_ACTION: "drain",
    DESTROYER_QUEUE_OVERLOAD_WORKERS: workers.join(","),
  });
  const logs = await stack.finishRoleContainers(drainer, label);
  const log = logs.get("0");
  if (log === undefined) throw new Error(`${label} log was missing`);
  const observed = recordField(requiredEvent(log, "queue_overload_drain_complete"), "observed");
  return Object.fromEntries(workers.map((worker) => {
    const values = observed[worker];
    if (!Array.isArray(values)) throw new Error(`${label} omitted worker ${worker}`);
    return [worker, values.map((value) => {
      if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
        throw new Error(`${label} contained an invalid Queue sequence`);
      }
      return value;
    })];
  }));
}

function producerEvidence(
  logs: ReadonlyMap<string, string>,
): Record<string, QueueOverloadProducerEvidence> {
  return Object.fromEntries([...logs.entries()].map(([worker, log]) => {
    const complete = requiredEvent(log, "queue_overload_producer_complete");
    return [worker, {
      started: numericField(complete, "started"),
      acknowledged: integerArray(complete.acknowledged, `${worker} acknowledged`),
      failed: integerArray(complete.failed, `${worker} failed`),
    }];
  }));
}

export function assertQueueOverloadReconciled(
  producers: Readonly<Record<string, QueueOverloadProducerEvidence>>,
  observed: Readonly<Record<string, readonly number[]>>,
): void {
  for (const [worker, producer] of Object.entries(producers)) {
    const values = observed[worker] ?? [];
    for (const sequence of producer.acknowledged) {
      if (!values.includes(sequence)) {
        throw new Error(`Acknowledged Queue sequence ${worker}/${sequence} disappeared`);
      }
    }
    for (const sequence of values) {
      if (sequence >= producer.started) {
        throw new Error(`Queue exposed unstarted sequence ${worker}/${sequence}`);
      }
    }
  }
}

function integerArray(value: unknown, label: string): number[] {
  if (!Array.isArray(value)) throw new Error(`${label} was not an array`);
  return value.map((item) => {
    if (typeof item !== "number" || !Number.isSafeInteger(item) || item < 0) {
      throw new Error(`${label} contained an invalid sequence`);
    }
    return item;
  });
}
