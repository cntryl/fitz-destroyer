import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { cleanupDelta, type CleanupMetrics } from "./live-observability.js";
import { numericField, numericValue, recordField, requiredEvent } from "./workload-log.js";

export async function runNoticeFanoutScenario(
  stack: ComposeStack,
  config: RunConfig,
  artifacts: Artifacts,
  shape: WorkloadShape,
  runLabel = "notice-fanout",
): Promise<void> {
  const replicas = config.clientReplicas;
  const expectedPerSubscriber = replicas * shape.entriesPerResource;
  const startedAt = performance.now();
  const baseline = await stack.liveDomainSnapshot("notice");
  await artifacts.writeJson(`${runLabel}-stats-before.json`, baseline);
  await artifacts.event("notice_fanout_started", {
    runLabel,
    publishers: replicas,
    subscribers: replicas,
    messagesPerPublisher: shape.entriesPerResource,
    expectedPublications: expectedPerSubscriber,
    expectedDeliveries: expectedPerSubscriber * replicas,
    payloadBytes: shape.payloadBytes,
    concurrency: config.liveConcurrency,
    handlerDelayMs: config.handlerDelayMs,
  });

  const subscribers = await stack.startRoleContainers("notice-subscriber", replicas, shape, {
    DESTROYER_PUBLISHER_COUNT: String(replicas),
  });
  await stack.waitForRoleEvent(subscribers, "notice_subscriber_ready");

  const publishers = await stack.startRoleContainers("notice-publisher", replicas, shape, {
    DESTROYER_WAIT_FOR_START_SIGNAL: "true",
  });
  await stack.waitForRoleEvent(publishers, "live_producer_ready");
  await stack.signalRoleContainers(publishers, "SIGUSR1");
  const publisherLogs = await stack.finishRoleContainers(
    publishers,
    `${runLabel}-notice-publisher`,
  );
  const subscriberLogs = await stack.finishRoleContainers(
    subscribers,
    `${runLabel}-notice-subscriber`,
  );

  let publications = 0;
  for (const log of publisherLogs.values()) {
    publications += numericField(requiredEvent(log, "notice_publisher_complete"), "published");
  }
  let deliveries = 0;
  let maxActiveHandlers = 0;
  for (const log of subscriberLogs.values()) {
    const complete = requiredEvent(log, "notice_subscriber_complete");
    deliveries += numericField(complete, "received");
    maxActiveHandlers = Math.max(
      maxActiveHandlers,
      numericField(complete, "maxActiveHandlers"),
    );
  }
  if (publications !== expectedPerSubscriber || deliveries !== expectedPerSubscriber * replicas) {
    throw new Error(
      `Notice fanout counts do not match: publications=${publications}/${expectedPerSubscriber}, deliveries=${deliveries}/${expectedPerSubscriber * replicas}`,
    );
  }

  const quiescence = await stack.waitForLiveDomainQuiescence("notice", baseline, runLabel);

  await artifacts.event("notice_fanout_complete", {
    runLabel,
    publications,
    deliveries,
    maxActiveHandlers,
    cleanup: quiescence.cleanup,
    elapsedMs: elapsedMs(startedAt),
  });
}

export async function runRpcPressureScenario(
  stack: ComposeStack,
  config: RunConfig,
  artifacts: Artifacts,
  shape: WorkloadShape,
  runLabel = "rpc-pressure",
): Promise<void> {
  const replicas = config.clientReplicas;
  const expectedCalls = replicas * shape.entriesPerResource;
  const startedAt = performance.now();
  const baseline = await stack.liveDomainSnapshot("rpc");
  await artifacts.writeJson(`${runLabel}-stats-before.json`, baseline);
  await artifacts.event("rpc_pressure_started", {
    runLabel,
    callers: replicas,
    workers: replicas,
    callsPerCaller: shape.entriesPerResource,
    expectedCalls,
    expectedResponseFrames: expectedCalls * 2,
    payloadBytes: shape.payloadBytes,
    callerConcurrency: config.liveConcurrency,
    workerConcurrency: Math.max(1, Math.floor(config.liveConcurrency / 4)),
    handlerDelayMs: config.handlerDelayMs,
  });

  const workers = await stack.startRoleContainers("rpc-worker", replicas, shape);
  await stack.waitForRoleEvent(workers, "rpc_worker_ready");

  let callerLogs: Map<string, string> | undefined;
  let callerFailure: unknown;
  try {
    const callers = await stack.startRoleContainers("rpc-caller", replicas, shape, {
      DESTROYER_WAIT_FOR_START_SIGNAL: "true",
    });
    await stack.waitForRoleEvent(callers, "live_producer_ready");
    await stack.signalRoleContainers(callers, "SIGUSR1");
    callerLogs = await stack.finishRoleContainers(callers, `${runLabel}-rpc-caller`);
  } catch (error) {
    callerFailure = error;
  }

  await stack.signalRoleContainers(workers, "SIGTERM");
  const workerLogs = await stack.finishRoleContainers(workers, `${runLabel}-rpc-worker`);
  if (callerFailure !== undefined) throw callerFailure;
  if (callerLogs === undefined) throw new Error("RPC caller logs were not captured");

  let calls = 0;
  let responseFrames = 0;
  const selectedWorkers = new Set<string>();
  for (const log of callerLogs.values()) {
    const complete = requiredEvent(log, "rpc_caller_complete");
    calls += numericField(complete, "completed");
    responseFrames += numericField(complete, "responseFrames");
    const workerCounts = recordField(complete, "workerCounts");
    for (const [workerId, count] of Object.entries(workerCounts)) {
      if (numericValue(count, `workerCounts.${workerId}`) > 0) selectedWorkers.add(workerId);
    }
  }

  let handled = 0;
  let maxActive = 0;
  for (const log of workerLogs.values()) {
    const complete = requiredEvent(log, "rpc_worker_complete");
    const workerHandled = numericField(complete, "handled");
    const failures = numericField(complete, "failures");
    if (workerHandled === 0 || failures !== 0) {
      throw new Error(
        `RPC worker did not complete cleanly: handled=${workerHandled}, failures=${failures}`,
      );
    }
    handled += workerHandled;
    maxActive = Math.max(maxActive, numericField(complete, "maxActive"));
  }

  if (calls !== expectedCalls || responseFrames !== expectedCalls * 2 || handled !== expectedCalls) {
    throw new Error(
      `RPC pressure counts do not match: calls=${calls}/${expectedCalls}, frames=${responseFrames}/${expectedCalls * 2}, handled=${handled}/${expectedCalls}`,
    );
  }
  if (selectedWorkers.size !== replicas) {
    throw new Error(`RPC calls reached ${selectedWorkers.size}/${replicas} registered workers`);
  }

  const quiescence = await stack.waitForLiveDomainQuiescence("rpc", baseline, runLabel);

  await artifacts.event("rpc_pressure_complete", {
    runLabel,
    calls,
    responseFrames,
    handled,
    selectedWorkers: [...selectedWorkers].sort(),
    maxActive,
    cleanup: quiescence.cleanup,
    elapsedMs: elapsedMs(startedAt),
  });
}

export async function runConnectionStormScenario(
  stack: ComposeStack,
  config: RunConfig,
  artifacts: Artifacts,
  shape: WorkloadShape,
): Promise<void> {
  const waves = Math.min(shape.resources, shape.entriesPerResource);
  const baseOperations = Math.floor(shape.entriesPerResource / waves);
  const extraOperations = shape.entriesPerResource % waves;
  const startedAt = performance.now();
  const cleanupTotals: CleanupMetrics = {
    failures: 0,
    retries: 0,
    successes: 0,
    pending: 0,
    oldestAgeMs: 0,
  };
  await artifacts.event("connection_storm_started", {
    waves,
    connectionsPerWave: config.clientReplicas * 4,
    totalConnectionLifecycles: waves * config.clientReplicas * 4,
    operationsPerRole: shape.entriesPerResource,
  });

  for (let wave = 0; wave < waves; wave += 1) {
    const waveNumber = wave + 1;
    const operations = baseOperations + (wave < extraOperations ? 1 : 0);
    const runLabel = `connection-storm-wave-${waveNumber.toString().padStart(3, "0")}`;
    const waveShape = {
      ...shape,
      namespace: `${shape.namespace}-wave-${waveNumber}`,
      entriesPerResource: operations,
    };
    await artifacts.event("connection_storm_wave_started", {
      wave: waveNumber,
      waves,
      operations,
    });
    const cleanupBefore = (await stack.liveDomainSnapshot("notice")).cleanup;

    const outcomes = await Promise.allSettled([
      runNoticeFanoutScenario(stack, config, artifacts, waveShape, runLabel),
      runRpcPressureScenario(stack, config, artifacts, waveShape, runLabel),
    ]);
    const cleanupAfter = (await stack.liveDomainSnapshot("notice")).cleanup;
    const cleanup = cleanupDelta(cleanupBefore, cleanupAfter);
    cleanupTotals.failures += cleanup.failures;
    cleanupTotals.retries += cleanup.retries;
    cleanupTotals.successes += cleanup.successes;
    cleanupTotals.pending = cleanup.pending;
    cleanupTotals.oldestAgeMs = cleanup.oldestAgeMs;
    const failures = outcomes
      .filter((outcome): outcome is PromiseRejectedResult => outcome.status === "rejected")
      .map((outcome) => errorMessage(outcome.reason));
    if (failures.length > 0) {
      throw new Error(`Connection storm wave ${waveNumber} failed: ${failures.join("; ")}`);
    }
    await artifacts.event("connection_storm_wave_complete", {
      wave: waveNumber,
      waves,
      cleanup,
    });
  }

  await artifacts.event("connection_storm_complete", {
    waves,
    cleanup: cleanupTotals,
    elapsedMs: elapsedMs(startedAt),
  });
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

function elapsedMs(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}
