import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericValue, parseJsonRecords, requiredEvent } from "./workload-log.js";

export type QueueLifecycleLedger = {
  operations: number;
  producerCompleted: readonly number[];
  abandoned: number;
  consumers: Readonly<Record<string, readonly number[]>>;
};

export async function runQueueLifecycleScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const operations = Math.max(8, shape.resources * shape.entriesPerResource);
  const runShape = { ...shape, entriesPerResource: operations };
  const environment = { DESTROYER_SEED: String(shape.seed) };
  const producer = await stack.startRoleContainers("queue-lifecycle-producer", 1, runShape, {
    ...environment,
    DESTROYER_QUEUE_LIFECYCLE_ACTION: "produce",
  });
  const producerLogs = await stack.finishRoleContainers(producer, "queue-lifecycle-producer");

  const abandoner = await stack.startRoleContainers("queue-lifecycle-abandoner", 1, runShape, {
    ...environment,
    DESTROYER_QUEUE_LIFECYCLE_ACTION: "abandon",
  });
  const abandonerLogs = await stack.finishRoleContainers(abandoner, "queue-lifecycle-abandoner");

  const consumers = await stack.startRoleContainers(
    "queue-lifecycle-consumer",
    config.clientReplicas,
    runShape,
    {
      ...environment,
      DESTROYER_QUEUE_LIFECYCLE_ACTION: "consume",
      DESTROYER_WAIT_FOR_START_SIGNAL: "true",
    },
  );
  await stack.waitForRoleEvent(consumers, "live_producer_ready");
  await stack.signalRoleContainers(consumers, "SIGUSR1");
  const consumerLogs = await stack.finishRoleContainers(consumers, "queue-lifecycle-consumer");
  const ledger = analyzeQueueLifecycle(producerLogs, abandonerLogs, consumerLogs, operations);
  await artifacts.writeJson("queue-lifecycle-ledger.json", ledger);
  assertQueueLifecycle(ledger, config.clientReplicas);
  await stack.waitForPressureQuiescence();
  await artifacts.event("queue_lifecycle_complete", ledger);
}

export function analyzeQueueLifecycle(
  producerLogs: ReadonlyMap<string, string>,
  abandonerLogs: ReadonlyMap<string, string>,
  consumerLogs: ReadonlyMap<string, string>,
  operations: number,
): QueueLifecycleLedger {
  const producerLog = onlyLog(producerLogs, "Queue lifecycle producer");
  const abandonerLog = onlyLog(abandonerLogs, "Queue lifecycle abandoner");
  const producer = requiredEvent(producerLog, "queue_lifecycle_producer_complete");
  const abandoner = requiredEvent(abandonerLog, "queue_lifecycle_abandoned");
  const consumers: Record<string, number[]> = {};
  for (const [worker, log] of consumerLogs) {
    const complete = requiredEvent(log, "queue_lifecycle_consumer_complete");
    consumers[worker] = numericArray(complete.sequences, `${worker} sequences`);
  }
  return {
    operations,
    producerCompleted: numericArray(producer.completed, "producer completed"),
    abandoned: numericValue(abandoner.sequence, "abandoned sequence"),
    consumers,
  };
}

export function assertQueueLifecycle(ledger: QueueLifecycleLedger, replicas: number): void {
  if (Object.keys(ledger.consumers).length !== replicas) {
    throw new Error(`Queue lifecycle consumer count ${Object.keys(ledger.consumers).length}/${replicas}`);
  }
  const all = [
    ...ledger.producerCompleted,
    ...Object.values(ledger.consumers).flatMap((sequences) => sequences),
  ];
  const duplicate = findDuplicate(all);
  if (duplicate !== undefined) throw new Error(`Queue lifecycle sequence ${duplicate} completed twice`);
  const expected = Array.from({ length: ledger.operations }, (_, sequence) => sequence);
  const actual = [...all].sort((left, right) => left - right);
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`Queue lifecycle did not drain exact backlog: ${JSON.stringify(actual)}`);
  }
  if (!Object.values(ledger.consumers).some((sequences) => sequences.includes(ledger.abandoned))) {
    throw new Error(`Abandoned Queue sequence ${ledger.abandoned} was not redelivered`);
  }
  const unfair = Object.entries(ledger.consumers).filter(([, sequences]) => sequences.length === 0);
  if (unfair.length > 0) {
    throw new Error(`Queue consumer fairness failed for ${unfair.map(([worker]) => worker).join(", ")}`);
  }
}

function onlyLog(logs: ReadonlyMap<string, string>, label: string): string {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) throw new Error(`${label} log count ${logs.size}/1`);
  return log;
}

function numericArray(value: unknown, label: string): number[] {
  if (!Array.isArray(value)) throw new Error(`${label} must be an array`);
  return value.map((item) => numericValue(item, label));
}

function findDuplicate(values: readonly number[]): number | undefined {
  const seen = new Set<number>();
  for (const value of values) {
    if (seen.has(value)) return value;
    seen.add(value);
  }
  return undefined;
}
