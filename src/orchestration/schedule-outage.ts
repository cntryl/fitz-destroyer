import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack, RoleContainer } from "./compose.js";
import { numericValue, parseJsonRecords, requiredEvent } from "./workload-log.js";

export type ScheduleOutageLedger = {
  missedAtMs: number;
  repeatedAtMs: number;
  missedDeliveries: number;
  repeatedSequences: readonly number[];
  raceSequences: readonly number[];
  duplicateDeliveries: number;
  cancellationAcknowledged: readonly number[];
  cancellationFailed: readonly number[];
};

export async function runScheduleOutageScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const missedAtMs = nextConsecutiveMinutePair(Date.now() + config.scheduleLeadMs);
  const repeatedAtMs = missedAtMs + 60_000;
  const baseline = await stack.liveDomainSnapshot("schedule");
  const environment = {
    DESTROYER_SEED: String(shape.seed),
    DESTROYER_SCHEDULE_OUTAGE_MISSED_AT_MS: String(missedAtMs),
    DESTROYER_SCHEDULE_OUTAGE_RACE_AT_MS: String(repeatedAtMs),
  };
  const subscribers = await stack.startRoleContainers(
    "schedule-outage-subscriber",
    config.clientReplicas,
    shape,
    {
      ...environment,
      DESTROYER_SCHEDULE_OUTAGE_ACTION: "subscribe",
      DESTROYER_HANDLER_DELAY_MS: String(Math.max(250, config.handlerDelayMs)),
    },
  );
  let subscribersFinished = false;
  try {
    await stack.waitForRoleEvent(subscribers, "schedule_outage_subscriber_ready");
    const producer = await stack.startRoleContainers("schedule-outage-producer", 1, shape, {
      ...environment,
      DESTROYER_SCHEDULE_OUTAGE_ACTION: "create",
    });
    const producerLogs = await stack.finishRoleContainers(producer, "schedule-outage-producer");
    const create = requiredEvent(onlyLog(producerLogs), "schedule_outage_create_complete");
    if (numericValue(create.created, "Schedule outage created") !== shape.entriesPerResource * 2) {
      throw new Error("Schedule outage producer did not create every definition");
    }
    const stopAtMs = missedAtMs - 10_000;
    if (stopAtMs <= Date.now()) throw new Error("Schedule outage lacks ten-second shutdown margin");
    await sleepUntil(stopAtMs);
    await stack.stopFitz();
    await sleepUntil(missedAtMs + 2_000);
    await stack.restartFitz();
    const missedLogs = await stack.roleLogs(subscribers);
    const missedDeliveries = countDeliveriesBefore(missedLogs, repeatedAtMs);
    if (missedDeliveries !== 0) {
      throw new Error(`Schedule outage replayed ${missedDeliveries} missed occurrences`);
    }

    const canceller = await stack.startRoleContainers("schedule-outage-canceller", 1, shape, {
      ...environment,
      DESTROYER_SCHEDULE_OUTAGE_ACTION: "race-cancel",
    });
    await sleepUntil(repeatedAtMs + deliveryDrainMs(config, shape));
    const cancellerLogs = await stack.finishRoleContainers(canceller, "schedule-outage-canceller");
    await stack.signalRoleContainers(subscribers, "SIGTERM");
    const subscriberLogs = await stack.finishRoleContainers(subscribers, "schedule-outage-subscriber");
    subscribersFinished = true;
    const ledger = analyzeScheduleOutage(
      subscriberLogs,
      cancellerLogs,
      missedAtMs,
      repeatedAtMs,
    );
    await artifacts.writeJson("schedule-outage-ledger.json", ledger);
    assertScheduleOutage(ledger, shape.entriesPerResource);

    const cleanup = await stack.startRoleContainers("schedule-outage-cleanup", 1, shape, {
      ...environment,
      DESTROYER_SCHEDULE_OUTAGE_ACTION: "cleanup",
    });
    await stack.finishRoleContainers(cleanup, "schedule-outage-cleanup");
    await stack.waitForLiveDomainQuiescence("schedule", baseline, "schedule-outage");
    await artifacts.event("schedule_outage_complete", {
      ...ledger,
      elapsedMs: Math.round(performance.now() - startedAt),
    });
  } catch (error) {
    if (!subscribersFinished) await stopSubscribers(stack, subscribers);
    throw error;
  }
}

export function analyzeScheduleOutage(
  subscriberLogs: ReadonlyMap<string, string>,
  cancellerLogs: ReadonlyMap<string, string>,
  missedAtMs: number,
  repeatedAtMs: number,
): ScheduleOutageLedger {
  const repeatedSequences: number[] = [];
  const raceSequences: number[] = [];
  let duplicates = 0;
  let missedDeliveries = 0;
  for (const log of subscriberLogs.values()) {
    const complete = requiredEvent(log, "schedule_outage_subscriber_complete");
    duplicates += numericValue(complete.duplicates, "Schedule outage duplicates");
    if (!Array.isArray(complete.deliveries)) throw new Error("Schedule outage deliveries must be an array");
    for (const value of complete.deliveries) {
      if (typeof value !== "object" || value === null || Array.isArray(value)) continue;
      const delivery = value as Record<string, unknown>;
      const sequence = numericValue(delivery.sequence, "Schedule outage sequence");
      const receivedAt = numericValue(delivery.receivedAtMs, "Schedule outage receivedAtMs");
      if (receivedAt < repeatedAtMs) missedDeliveries += 1;
      if (delivery.kind === "repeated") repeatedSequences.push(sequence);
      else if (delivery.kind === "race") raceSequences.push(sequence);
      else throw new Error(`Unexpected Schedule outage kind ${String(delivery.kind)}`);
    }
  }
  const cancel = requiredEvent(onlyLog(cancellerLogs), "schedule_outage_race_cancel_complete");
  return {
    missedAtMs,
    repeatedAtMs,
    missedDeliveries,
    repeatedSequences,
    raceSequences,
    duplicateDeliveries: duplicates,
    cancellationAcknowledged: numericArray(cancel.acknowledged, "Schedule cancellation acknowledged"),
    cancellationFailed: numericArray(cancel.failed, "Schedule cancellation failed"),
  };
}

export function assertScheduleOutage(ledger: ScheduleOutageLedger, operations: number): void {
  if (ledger.missedDeliveries !== 0 || ledger.duplicateDeliveries !== 0) {
    throw new Error(`Schedule outage produced missed/duplicate delivery: ${JSON.stringify(ledger)}`);
  }
  assertExactSequences(ledger.repeatedSequences, operations, "repeated Schedule occurrence");
  const raceDuplicate = findDuplicate(ledger.raceSequences);
  if (raceDuplicate !== undefined) {
    throw new Error(`Schedule cancellation race delivered sequence ${raceDuplicate} twice`);
  }
  for (const sequence of ledger.raceSequences) {
    if (sequence < 0 || sequence >= operations) throw new Error(`Schedule race sequence ${sequence} is invalid`);
  }
  const cancellationOutcomes = [...ledger.cancellationAcknowledged, ...ledger.cancellationFailed].sort(numericSort);
  assertExactSequences(cancellationOutcomes, operations, "Schedule cancellation outcome");
}

export function nextWholeMinute(timestampMs: number): number {
  return Math.ceil(timestampMs / 60_000) * 60_000;
}

export function nextConsecutiveMinutePair(timestampMs: number): number {
  let candidate = nextWholeMinute(timestampMs);
  if (new Date(candidate).getUTCMinutes() === 59) candidate += 60_000;
  return candidate;
}

function countDeliveriesBefore(logs: ReadonlyMap<string, string>, beforeMs: number): number {
  return [...logs.values()].reduce(
    (count, log) =>
      count +
      parseJsonRecords(log).filter(
        (record) =>
          record.event === "schedule_outage_delivery" &&
            typeof record.receivedAtMs === "number" &&
            record.receivedAtMs < beforeMs,
      ).length,
    0,
  );
}

function deliveryDrainMs(config: RunConfig, shape: WorkloadShape): number {
  const waves = Math.ceil(shape.entriesPerResource / (config.clientReplicas * config.liveConcurrency));
  return Math.max(config.phaseMs, waves * Math.max(250, config.handlerDelayMs) + 2_000);
}

async function stopSubscribers(stack: ComposeStack, subscribers: readonly RoleContainer[]): Promise<void> {
  await stack.signalRoleContainers(subscribers, "SIGTERM").catch(() => undefined);
  await stack.finishRoleContainers(subscribers, "schedule-outage-subscriber-failed").catch(() => undefined);
}

function onlyLog(logs: ReadonlyMap<string, string>): string {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) throw new Error(`Expected one Schedule outage log, found ${logs.size}`);
  return log;
}

function numericArray(value: unknown, label: string): number[] {
  if (!Array.isArray(value)) throw new Error(`${label} must be an array`);
  return value.map((item) => numericValue(item, label));
}

function assertExactSequences(values: readonly number[], operations: number, label: string): void {
  const actual = [...values].sort(numericSort);
  const expected = Array.from({ length: operations }, (_, sequence) => sequence);
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label} sequences ${JSON.stringify(actual)} != ${JSON.stringify(expected)}`);
  }
}

function findDuplicate(values: readonly number[]): number | undefined {
  const seen = new Set<number>();
  for (const value of values) {
    if (seen.has(value)) return value;
    seen.add(value);
  }
  return undefined;
}

function numericSort(left: number, right: number): number {
  return left - right;
}

function sleepUntil(timestampMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, timestampMs - Date.now())));
}
