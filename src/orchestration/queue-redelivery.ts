import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runQueueRedeliveryScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const environment = { DESTROYER_SEED: String(shape.seed) };
  const producer = await stack.startRoleContainers("queue-redelivery-producer", 1, shape, {
    ...environment,
    DESTROYER_QUEUE_REDELIVERY_ACTION: "produce",
  });
  const producerLogs = await stack.finishRoleContainers(producer, "queue-redelivery-producer");
  const produced = numericField(
    requiredEvent(onlyLog(producerLogs), "queue_redelivery_producer_complete"),
    "produced",
  );
  if (produced !== shape.entriesPerResource) {
    throw new Error(`Queue producer acknowledged ${produced}/${shape.entriesPerResource}`);
  }

  const victim = await stack.startRoleContainers("queue-redelivery-victim", 1, shape, {
    ...environment,
    DESTROYER_QUEUE_REDELIVERY_ACTION: "victim",
  });
  await stack.waitForRoleEvent(victim, "queue_victim_reserved");
  const victimLogs = await stack.killRoleContainers(victim, "queue-redelivery-victim-sigkill");
  const victimSequences = numericArray(
    requiredEvent(onlyLog(victimLogs), "queue_victim_reserved"),
    "sequences",
  );

  const drainers = await stack.startRoleContainers(
    "queue-redelivery-drainer",
    config.clientReplicas,
    shape,
    {
      ...environment,
      DESTROYER_QUEUE_REDELIVERY_ACTION: "drain",
    },
  );
  const drainerLogs = await stack.finishRoleContainers(drainers, "queue-redelivery-drainers");
  const recovered: number[] = [];
  for (const log of drainerLogs.values()) {
    recovered.push(...numericArray(requiredEvent(log, "queue_drainer_complete"), "sequences"));
  }
  assertQueueRedelivery(shape.entriesPerResource, victimSequences, recovered);
  await artifacts.writeJson("queue-redelivery-ledger.json", {
    produced,
    killedReservations: victimSequences,
    recovered,
  });
  await artifacts.event("queue_redelivery_complete", {
    produced,
    killedReservations: victimSequences.length,
    recovered: recovered.length,
    drainers: config.clientReplicas,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function assertQueueRedelivery(
  expected: number,
  victimSequences: readonly number[],
  recovered: readonly number[],
): void {
  const recoveredSet = new Set(recovered);
  if (recoveredSet.size !== recovered.length) {
    throw new Error(`Queue recovery contained ${recovered.length - recoveredSet.size} duplicates`);
  }
  if (recovered.length !== expected) {
    throw new Error(`Queue recovery completed ${recovered.length}/${expected} messages`);
  }
  for (let sequence = 0; sequence < expected; sequence += 1) {
    if (!recoveredSet.has(sequence)) throw new Error(`Queue recovery lost sequence ${sequence}`);
  }
  for (const sequence of victimSequences) {
    if (!recoveredSet.has(sequence)) {
      throw new Error(`Queue reservation ${sequence} disappeared with its killed consumer`);
    }
  }
}

function onlyLog(logs: ReadonlyMap<string, string>): string {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) throw new Error(`Expected one log, found ${logs.size}`);
  return log;
}

function numericArray(record: Readonly<Record<string, unknown>>, field: string): number[] {
  const values = record[field];
  if (!Array.isArray(values)) throw new Error(`${field} is not an array`);
  return values.map((value) => {
    if (typeof value !== "number" || !Number.isSafeInteger(value)) {
      throw new Error(`${field} contained a non-integer value`);
    }
    return value;
  });
}
