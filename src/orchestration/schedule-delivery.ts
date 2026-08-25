import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack, RoleContainer } from "./compose.js";
import { numericField, numericValue, parseJsonRecords, requiredEvent } from "./workload-log.js";

const MINIMUM_RESTART_MARGIN_MS = 10_000;

export type ScheduleDeliverySummary = {
  broadcastDeliveries: number;
  singleDeliveries: number;
  cancelledDeliveries: number;
  duplicates: number;
  invalid: number;
  maxLatenessMs: number;
};

export type ScheduleDeliveryEvidence = {
  expected: {
    replicas: number;
    firesPerMode: number;
    broadcastDeliveries: number;
    singleDeliveries: number;
    cancelledDeliveries: number;
  };
  observed: ScheduleDeliverySummary;
  subscriberBroadcastDeliveries: Readonly<Record<string, number>>;
  clientEventCounts: Readonly<Record<string, number>>;
  clientHandlerSaturations: Readonly<Record<string, number>>;
  missingBroadcastCount: number;
  missingBroadcastSequences: readonly number[];
  missingSingleCount: number;
  missingSingleSequences: readonly number[];
  duplicateSingleCount: number;
  duplicateSingleSequences: readonly number[];
};

export async function runScheduleDelivery(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const runLabel = "schedule-delivery";
  const startedAt = performance.now();
  const replicas = config.clientReplicas;
  const firesPerMode = shape.entriesPerResource;
  const activeDefinitions = safeProduct(firesPerMode, 2, "active Schedule definitions");
  const expectedBroadcastDeliveries = safeProduct(
    firesPerMode,
    replicas,
    "Broadcast Schedule deliveries",
  );
  const fireAtMs = nextWholeMinute(Date.now() + config.scheduleLeadMs);
  const baseline = await stack.liveDomainSnapshot("schedule");
  await artifacts.writeJson(`${runLabel}-stats-before.json`, baseline);
  const baselineSchedules = numericValue(baseline.domain.schedules_active, "schedules_active");
  const baselineSubscriptions = numericValue(
    baseline.domain.subscriptions_active,
    "subscriptions_active",
  );
  await artifacts.event("schedule_delivery_started", {
    subscribers: replicas,
    firesPerMode,
    definitionsCreated: firesPerMode * 3,
    definitionsCancelledBeforeFire: firesPerMode,
    expectedActiveDefinitions: activeDefinitions,
    expectedBroadcastDeliveries,
    expectedSingleDeliveries: firesPerMode,
    expectedCancelledDeliveries: 0,
    fireAtMs,
    fireAt: new Date(fireAtMs).toISOString(),
    deliveryToleranceMs: 1_000,
    restartBeforeFire: true,
  });

  const environment = {
    DESTROYER_SCHEDULE_FIRE_AT_MS: String(fireAtMs),
    DESTROYER_SEED: String(shape.seed),
  };
  const subscribers = await stack.startRoleContainers(
    "schedule-subscriber",
    replicas,
    shape,
    environment,
  );
  let subscribersFinished = false;

  try {
    await stack.waitForRoleEvent(subscribers, "schedule_subscriber_ready");
    await waitForScheduleCounts(
      stack,
      config.startupTimeoutMs,
      baselineSchedules,
      baselineSubscriptions + replicas,
      "subscriber registration",
    );

    const producers = await stack.startRoleContainers("schedule-producer", 1, shape, {
      ...environment,
      DESTROYER_SCHEDULE_ACTION: "create",
    });
    const producerLogs = await stack.finishRoleContainers(
      producers,
      `${runLabel}-producer-create`,
    );
    assertProducerComplete(producerLogs, "create", firesPerMode);
    await waitForScheduleCounts(
      stack,
      config.startupTimeoutMs,
      baselineSchedules + activeDefinitions,
      baselineSubscriptions + replicas,
      "definition creation",
    );

    assertRestartMargin(fireAtMs, "before Fitz restart");
    await stack.gracefulRestartFitz();
    await waitForScheduleCounts(
      stack,
      config.startupTimeoutMs,
      baselineSchedules + activeDefinitions,
      baselineSubscriptions + replicas,
      "post-restart recovery and re-subscription",
    );
    assertRestartMargin(fireAtMs, "after Fitz restart");
    const deliveryBaseline = await stack.liveDomainSnapshot("schedule");
    await artifacts.writeJson(`${runLabel}-stats-after-restart.json`, deliveryBaseline);
    await artifacts.event("schedule_delivery_armed", {
      fireAtMs,
      remainingMs: fireAtMs - Date.now(),
      schedules: baselineSchedules + activeDefinitions,
      subscriptions: baselineSubscriptions + replicas,
    });

    await sleepUntil(fireAtMs + Math.max(2_000, config.phaseMs));
    await stack.signalRoleContainers(subscribers, "SIGTERM");
    const subscriberLogs = await stack.finishRoleContainers(
      subscribers,
      `${runLabel}-subscriber`,
    );
    subscribersFinished = true;
    const evidence = analyzeScheduleDeliveryLogs(
      subscriberLogs,
      firesPerMode,
      replicas,
    );
    await artifacts.writeJson(`${runLabel}-observed.json`, evidence);
    assertScheduleDelivery(evidence);
    const summary = evidence.observed;

    const cleanup = await stack.startRoleContainers("schedule-producer", 1, shape, {
      ...environment,
      DESTROYER_SCHEDULE_ACTION: "cancel",
    });
    const cleanupLogs = await stack.finishRoleContainers(
      cleanup,
      `${runLabel}-producer-cancel`,
    );
    assertProducerComplete(cleanupLogs, "cancel", firesPerMode);
    const quiescence = await stack.waitForLiveDomainQuiescence(
      "schedule",
      deliveryBaseline,
      runLabel,
    );

    await artifacts.event("schedule_delivery_complete", {
      ...summary,
      expectedBroadcastDeliveries,
      expectedSingleDeliveries: firesPerMode,
      expectedCancelledDeliveries: 0,
      cleanup: quiescence.cleanup,
      elapsedMs: elapsedMs(startedAt),
    });
  } catch (error) {
    if (!subscribersFinished) await stopSubscribers(stack, subscribers, runLabel);
    throw error;
  }
}

export function summarizeScheduleDeliveryLogs(
  logs: ReadonlyMap<string, string>,
  firesPerMode: number,
  expectedReplicas: number,
): ScheduleDeliverySummary {
  const evidence = analyzeScheduleDeliveryLogs(logs, firesPerMode, expectedReplicas);
  assertScheduleDelivery(evidence);
  return evidence.observed;
}

export function analyzeScheduleDeliveryLogs(
  logs: ReadonlyMap<string, string>,
  firesPerMode: number,
  expectedReplicas: number,
): ScheduleDeliveryEvidence {
  if (logs.size !== expectedReplicas) {
    throw new Error(`Schedule subscriber logs cover ${logs.size}/${expectedReplicas} replicas`);
  }
  const broadcastWorkers = new Map<number, Set<string>>();
  const singleCounts = new Map<number, number>();
  const subscriberBroadcastDeliveries: Record<string, number> = {};
  const clientEventCounts: Record<string, number> = {};
  const clientHandlerSaturations: Record<string, number> = {};
  let broadcastDeliveries = 0;
  let singleDeliveries = 0;
  let cancelledDeliveries = 0;
  let duplicates = 0;
  let invalid = 0;
  let maxLatenessMs = 0;

  for (const [workerId, log] of logs) {
    const complete = requiredEvent(log, "schedule_subscriber_complete");
    const completeBroadcast = numericField(complete, "broadcast");
    const completeSingle = numericField(complete, "single");
    const completeCancelled = numericField(complete, "cancelled");
    const completeDuplicates = numericField(complete, "duplicates");
    const completeInvalid = numericField(complete, "invalid");
    broadcastDeliveries += completeBroadcast;
    singleDeliveries += completeSingle;
    cancelledDeliveries += completeCancelled;
    duplicates += completeDuplicates;
    invalid += completeInvalid;
    maxLatenessMs = Math.max(maxLatenessMs, numericField(complete, "maxLatenessMs"));
    subscriberBroadcastDeliveries[workerId] = completeBroadcast;

    for (const record of parseJsonRecords(log)) {
      if (record.event === "fitz_client_event") {
        const clientEvent = String(record.clientEvent);
        clientEventCounts[clientEvent] = (clientEventCounts[clientEvent] ?? 0) + 1;
        if (clientEvent === "fitz.connection.handler_saturated") {
          clientHandlerSaturations[workerId] =
            (clientHandlerSaturations[workerId] ?? 0) + 1;
        }
        continue;
      }
      if (record.event !== "schedule_notification_received") continue;
      const sequence = numericValue(record.sequence, "schedule notification sequence");
      if (sequence >= firesPerMode) {
        throw new Error(`Schedule notification sequence ${sequence} exceeds ${firesPerMode - 1}`);
      }
      if (record.kind === "broadcast") {
        const workers = broadcastWorkers.get(sequence) ?? new Set<string>();
        workers.add(workerId);
        broadcastWorkers.set(sequence, workers);
      } else if (record.kind === "single") {
        singleCounts.set(sequence, (singleCounts.get(sequence) ?? 0) + 1);
      } else if (record.kind !== "cancelled") {
        throw new Error(`Schedule notification has invalid kind '${String(record.kind)}'`);
      }
    }
  }

  const missingBroadcast = sequenceViolations(
    firesPerMode,
    (sequence) => broadcastWorkers.get(sequence)?.size === expectedReplicas,
  );
  const missingSingle = sequenceViolations(
    firesPerMode,
    (sequence) => (singleCounts.get(sequence) ?? 0) > 0,
  );
  const duplicateSingle = sequenceViolations(
    firesPerMode,
    (sequence) => (singleCounts.get(sequence) ?? 0) <= 1,
  );
  const expectedBroadcast = firesPerMode * expectedReplicas;
  return {
    expected: {
      replicas: expectedReplicas,
      firesPerMode,
      broadcastDeliveries: expectedBroadcast,
      singleDeliveries: firesPerMode,
      cancelledDeliveries: 0,
    },
    observed: {
      broadcastDeliveries,
      singleDeliveries,
      cancelledDeliveries,
      duplicates,
      invalid,
      maxLatenessMs,
    },
    subscriberBroadcastDeliveries,
    clientEventCounts,
    clientHandlerSaturations,
    missingBroadcastCount: missingBroadcast.count,
    missingBroadcastSequences: missingBroadcast.sample,
    missingSingleCount: missingSingle.count,
    missingSingleSequences: missingSingle.sample,
    duplicateSingleCount: duplicateSingle.count,
    duplicateSingleSequences: duplicateSingle.sample,
  };
}

function assertScheduleDelivery(evidence: ScheduleDeliveryEvidence): void {
  const { expected, observed } = evidence;
  const subscriberDeficits = Object.entries(evidence.subscriberBroadcastDeliveries)
    .filter(([, deliveries]) => deliveries !== expected.firesPerMode)
    .map(([workerId, deliveries]) => `${workerId}:${deliveries}/${expected.firesPerMode}`);
  if (
    observed.broadcastDeliveries === expected.broadcastDeliveries &&
    observed.singleDeliveries === expected.singleDeliveries &&
    observed.cancelledDeliveries === expected.cancelledDeliveries &&
    observed.duplicates === 0 &&
    observed.invalid === 0 &&
    evidence.missingBroadcastCount === 0 &&
    evidence.missingSingleCount === 0 &&
    evidence.duplicateSingleCount === 0 &&
    Object.keys(evidence.clientHandlerSaturations).length === 0 &&
    subscriberDeficits.length === 0
  ) {
    return;
  }
  throw new Error(
    [
      "Schedule delivery failed",
      `Broadcast=${observed.broadcastDeliveries}/${expected.broadcastDeliveries}`,
      `Single=${observed.singleDeliveries}/${expected.singleDeliveries}`,
      `cancelled=${observed.cancelledDeliveries}/${expected.cancelledDeliveries}`,
      `duplicates=${observed.duplicates}`,
      `invalid=${observed.invalid}`,
      `subscriber Broadcast=${subscriberDeficits.join(",") || "none"}`,
      `client handler saturation=${formatCounts(evidence.clientHandlerSaturations)}`,
      `missing Broadcast=${formatViolationSample(evidence.missingBroadcastCount, evidence.missingBroadcastSequences)}`,
      `missing Single=${formatViolationSample(evidence.missingSingleCount, evidence.missingSingleSequences)}`,
      `duplicate Single=${formatViolationSample(evidence.duplicateSingleCount, evidence.duplicateSingleSequences)}`,
    ].join("; "),
  );
}

async function waitForScheduleCounts(
  stack: ComposeStack,
  timeoutMs: number,
  expectedSchedules: number,
  expectedSubscriptions: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let observedSchedules: unknown = "unknown";
  let observedSubscriptions: unknown = "unknown";
  while (Date.now() < deadline) {
    const snapshot = await stack.liveDomainSnapshot("schedule");
    observedSchedules = snapshot.domain.schedules_active;
    observedSubscriptions = snapshot.domain.subscriptions_active;
    if (
      observedSchedules === expectedSchedules &&
      observedSubscriptions === expectedSubscriptions
    ) {
      return;
    }
    await sleep(100);
  }
  throw new Error(
    `Timed out waiting for Schedule ${label}: schedules=${String(observedSchedules)}/${expectedSchedules}, subscriptions=${String(observedSubscriptions)}/${expectedSubscriptions}`,
  );
}

function assertProducerComplete(
  logs: ReadonlyMap<string, string>,
  action: "create" | "cancel",
  firesPerMode: number,
): void {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) {
    throw new Error(`Expected one Schedule ${action} producer log, found ${logs.size}`);
  }
  const complete = requiredEvent(log, "schedule_producer_complete");
  const created = numericField(complete, "created");
  const cancelled = numericField(complete, "cancelled");
  const listed = numericField(complete, "listed");
  const expectedCreated = action === "create" ? firesPerMode * 3 : 0;
  const expectedCancelled = action === "create" ? firesPerMode : firesPerMode * 2;
  const expectedListed = action === "create" ? firesPerMode * 2 : 0;
  if (
    complete.action !== action ||
    created !== expectedCreated ||
    cancelled !== expectedCancelled ||
    listed !== expectedListed
  ) {
    throw new Error(
      `Schedule ${action} producer counts do not match: created=${created}/${expectedCreated}, cancelled=${cancelled}/${expectedCancelled}, listed=${listed}/${expectedListed}`,
    );
  }
}

async function stopSubscribers(
  stack: ComposeStack,
  subscribers: readonly RoleContainer[],
  runLabel: string,
): Promise<void> {
  await stack.signalRoleContainers(subscribers, "SIGTERM").catch(() => undefined);
  await stack
    .finishRoleContainers(subscribers, `${runLabel}-subscriber-after-failure`)
    .catch(() => undefined);
}

function assertRestartMargin(fireAtMs: number, phase: string): void {
  const remainingMs = fireAtMs - Date.now();
  if (remainingMs < MINIMUM_RESTART_MARGIN_MS) {
    throw new Error(
      `Schedule due minute is only ${remainingMs}ms away ${phase}; increase --schedule-lead-ms`,
    );
  }
}

function nextWholeMinute(timestampMs: number): number {
  return Math.ceil(timestampMs / 60_000) * 60_000;
}

function sequenceViolations(
  count: number,
  predicate: (sequence: number) => boolean,
): { count: number; sample: number[] } {
  let violationCount = 0;
  const sample: number[] = [];
  for (let sequence = 0; sequence < count; sequence += 1) {
    if (predicate(sequence)) continue;
    violationCount += 1;
    if (sample.length < 20) sample.push(sequence);
  }
  return { count: violationCount, sample };
}

function formatViolationSample(count: number, sample: readonly number[]): string {
  if (count === 0) return "0";
  return `${count} [${sample.join(",")}${count > sample.length ? ",..." : ""}]`;
}

function formatCounts(counts: Readonly<Record<string, number>>): string {
  const entries = Object.entries(counts);
  return entries.length === 0
    ? "none"
    : `${entries.reduce((total, [, count]) => total + count, 0)} [${entries
        .map(([key, count]) => `${key}:${count}`)
        .join(",")}]`;
}

function safeProduct(left: number, right: number, label: string): number {
  const result = left * right;
  if (!Number.isSafeInteger(result)) throw new Error(`${label} exceeds safe integer range`);
  return result;
}

async function sleepUntil(timestampMs: number): Promise<void> {
  const remainingMs = timestampMs - Date.now();
  if (remainingMs > 0) await sleep(remainingMs);
}

function elapsedMs(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
