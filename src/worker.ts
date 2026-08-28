import { hostname } from "node:os";
import { createClient, type Client } from "@cntryl/fitz";
import {
  ALL_DOMAINS,
  parseDomainSelection,
  type Domain,
  type WorkloadShape,
} from "./workloads/model.js";
import { loadRecoveryWorkload, verifyRecoveryWorkload } from "./workloads/recovery.js";
import {
  runNoticePublisher,
  runNoticeSubscriber,
  runRpcCaller,
  runRpcStreamCaller,
  runRpcStreamWorker,
  runRpcWorker,
  type LiveCommonOptions,
} from "./workloads/live.js";
import {
  runScheduleProducer,
  runScheduleSubscriber,
  type ScheduleProducerAction,
} from "./workloads/schedule-delivery.js";
import { runSessionBoundaries } from "./workloads/session-boundaries.js";
import {
  runDurabilityCrashCut,
  type DurabilityAction,
} from "./workloads/durability-crash-cuts.js";
import {
  runLeaseContention,
  type LeaseContentionAction,
} from "./workloads/lease-contention.js";
import {
  runQueueRedelivery,
  type QueueRedeliveryAction,
} from "./workloads/queue-redelivery.js";
import { allCanaryDomains, runCanary } from "./workloads/canary.js";
import { runProtocolAbuse } from "./workloads/protocol-abuse.js";
import {
  isAmbiguousDurableError,
  recordStageError,
  recordStageLatency,
  stageMetrics,
  type PressureStages,
} from "./pressure.js";
import {
  decodePressureQueueSequence,
  runPressureQueueReconciler,
} from "./workloads/pressure.js";
import {
  runQueueLifecycle,
  type QueueLifecycleAction,
} from "./workloads/queue-lifecycle.js";
import {
  runTransactionContention,
  type TransactionContentionAction,
} from "./workloads/transaction-contention.js";
import {
  runStreamReplay,
  type StreamReplayAction,
} from "./workloads/stream-replay.js";
import {
  runScheduleOutage,
  type ScheduleOutageAction,
} from "./workloads/schedule-outage.js";
import { runQueueOverload } from "./workloads/queue-overload.js";
import {
  runAuthorizationIsolation,
  type AuthorizationIsolationAction,
} from "./workloads/authorization-isolation.js";
import {
  runStreamGlobalRecovery,
  type StreamGlobalRecoveryAction,
} from "./workloads/stream-global-recovery.js";
import { runQueueDeadLetterFencing } from "./workloads/queue-dead-letter-fencing.js";
import {
  runHostileRpcCaller,
  runHostileRpcWorker,
  type HostileRpcBehavior,
} from "./workloads/hostile-rpc-worker.js";
import { runRouteCardinalityChurn } from "./workloads/route-cardinality-churn.js";
import { runExhaustionProbe } from "./workloads/exhaustion-probe.js";
import {
  runEphemeralReplyLoss,
} from "./workloads/ephemeral-reply-loss.js";
import {
  runSlowRecipient,
  runSlowRecipientObserver,
  runSlowRecipientPublisher,
} from "./workloads/slow-recipient-isolation.js";
import { runSameShardFamilyFairness } from "./workloads/same-shard-family-fairness.js";
import {
  runWireConformance,
  type WireConformanceCase,
} from "./workloads/wire-conformance.js";
import { runShutdownReconnectCleanupStormWorkload } from "./workloads/reliability-session-state.js";
import {
  controlLaneReliabilityAction,
  runControlLaneCleanupUnderSaturationWorkload,
} from "./workloads/control-lane-cleanup-under-saturation.js";
import {
  parseRouteFamilyIdentity,
  parseRouteFamilyIsolationAction,
  runRouteFamilyIsolationMatrix,
} from "./workloads/route-family-isolation-matrix.js";
import { runRpcResponseStateConformance } from "./workloads/rpc-response-state-conformance.js";
import { runResponseEnvelopeBoundaries } from "./workloads/response-envelope-boundaries.js";
import { runLeaseWaiterDisconnectRaces } from "./workloads/lease-waiter-disconnect-races.js";
import { runWildcardRegistrationQuotaReclamation } from "./workloads/wildcard-registration-quota-reclamation.js";
import { runStreamSelectorCursorConformance } from "./workloads/stream-selector-cursor-conformance.js";

type WorkerMode =
  | "load"
  | "verify"
  | "bombard"
  | "durability-verifier"
  | "durability-writer"
  | "lease-contender"
  | "lease-owner"
  | "lease-probe"
  | "hot-route"
  | "canary"
  | "protocol-abuse"
  | "notice-publisher"
  | "notice-subscriber"
  | "schedule-producer"
  | "schedule-subscriber"
  | "session-boundaries"
  | "queue-redelivery-producer"
  | "queue-redelivery-victim"
  | "queue-redelivery-drainer"
  | "rpc-caller"
  | "rpc-worker"
  | "rpc-stream-caller"
  | "rpc-stream-worker"
  | "pressure-reconciler"
  | "queue-lifecycle-producer"
  | "queue-lifecycle-abandoner"
  | "queue-lifecycle-consumer"
  | "transaction-contender"
  | "transaction-holder"
  | "transaction-verifier"
  | "stream-replay-worker"
  | "schedule-outage-producer"
  | "schedule-outage-canceller"
  | "schedule-outage-cleanup"
  | "schedule-outage-subscriber"
  | "queue-overload-producer"
  | "queue-overload-drainer"
  | "authorization-isolation"
  | "stream-global-recovery"
  | "queue-dead-letter-fencing"
  | "hostile-rpc-worker"
  | "hostile-rpc-caller"
  | "route-cardinality-churn"
  | "exhaustion-probe"
  | "wire-conformance"
  | "ephemeral-reply-loss-preparer"
  | "ephemeral-reply-loss-victim"
  | "ephemeral-reply-loss-verifier"
  | "slow-recipient"
  | "slow-recipient-observer"
  | "slow-recipient-publisher"
  | "shutdown-reconnect-cleanup-storm"
  | "control-lane-cleanup-under-saturation"
  | "route-family-isolation-matrix"
  | "rpc-response-state-conformance"
  | "response-envelope-boundaries"
  | "lease-waiter-disconnect-races"
  | "wildcard-registration-quota-reclamation"
  | "stream-selector-cursor-conformance"
  | "same-shard-family-fairness";
type Counters = Record<Domain, { success: number; error: number }>;

const mode = requiredMode(process.env.DESTROYER_MODE);
const requestTimeoutMs = positiveEnv("DESTROYER_REQUEST_TIMEOUT_MS", 10_000);
const shutdown = new AbortController();
const clientAsyncHandlerBacklog = { active: 0, queued: 0 };
let releaseStartGate: () => void = () => undefined;
const startGate = new Promise<void>((resolve) => {
  releaseStartGate = resolve;
});
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => shutdown.abort(new Error(`received ${signal}`)));
}
process.on("SIGUSR1", () => releaseStartGate());

await main().catch((error: unknown) => {
  log("fatal", { error: errorMessage(error) });
  process.exitCode = 1;
});

async function main(): Promise<void> {
  if (mode === "protocol-abuse") {
    const signal = AbortSignal.any([
      shutdown.signal,
      AbortSignal.timeout(positiveEnv("DESTROYER_JOB_TIMEOUT_MS", 180_000)),
    ]);
    await runProtocolAbuse(
      process.env.FITZ_URL ?? "ws://fitz:4090/ws",
      positiveEnv("DESTROYER_OPERATIONS", 100),
      positiveEnv("DESTROYER_CONCURRENCY", 8),
      signal,
      log,
    );
    return;
  }
  if (mode === "wire-conformance") {
    await runWireConformance({
      namespace: routeSegment(requiredEnv("DESTROYER_NAMESPACE")),
      requestTimeoutMs,
      operations: positiveEnv("DESTROYER_OPERATIONS", 100),
      clientReplicas: positiveEnv("DESTROYER_CLIENT_REPLICAS", 4),
      wireCase: wireConformanceCase(process.env.DESTROYER_WIRE_CASE),
      url: process.env.DESTROYER_WIRE_URL ?? "ws://fitz:4090/ws",
      log,
    });
    return;
  }
  const client = makeClient(mode !== "load" && mode !== "verify");
  try {
    await client.connectWhenReady({
      timeoutMs: mode === "bombard" ? Infinity : positiveEnv("DESTROYER_STARTUP_TIMEOUT_MS", 180_000),
      signal: shutdown.signal,
    });
    log("connected", { mode, url: process.env.FITZ_URL ?? "ws://fitz:4090/ws" });

    if (mode === "load" || mode === "verify") {
      const shape = recoveryShape();
      const startedAt = performance.now();
      const entries =
        mode === "load"
          ? await loadRecoveryWorkload(client, shape)
          : await verifyRecoveryWorkload(client, shape);
      log("job_complete", {
        mode,
        entries,
        elapsedMs: Math.round(performance.now() - startedAt),
      });
      return;
    }

    if (mode === "bombard" || mode === "hot-route") {
      await bombard(client);
      return;
    }

    await runLiveRole(client, mode);
  } finally {
    await client.close().catch(() => undefined);
  }
}

function makeClient(reconnect: boolean): Client {
  const transport = transportEnv(process.env.DESTROYER_TRANSPORT);
  const defaultUrl = transport === "tcp" ? "tcp://fitz:4091" : "ws://fitz:4090/ws";
  return createClient({
    url: process.env.FITZ_URL ?? defaultUrl,
    transport,
    ...(process.env.DESTROYER_JWT === undefined
      ? {}
      : { tokenProvider: async () => requiredEnv("DESTROYER_JWT") }),
    timeout: requestTimeoutMs,
    reconnect: {
      enabled: reconnect,
      maxAttempts: Infinity,
      backoffMs: 100,
      maxBackoffMs: 2_000,
    },
    retry: { enabled: reconnect, maxAttempts: 3, backoffMs: 50, maxBackoffMs: 500 },
    heartbeat: {
      enabled: booleanEnv("DESTROYER_HEARTBEAT_ENABLED", true),
      intervalMs: 2_000,
      timeoutMs: 6_000,
    },
    maxFrameSize: 8 * 1024 * 1024,
    maxInFlightRequests: 1_024,
    maxRequestQueueSize: 16_384,
    asyncHandlers: {
      maxConcurrency: positiveEnv("DESTROYER_ASYNC_HANDLER_CONCURRENCY", 128),
      timeoutMs: requestTimeoutMs,
    },
    observability: {
      logger: {
        log(level, clientEvent, fields): void {
          if (level === "warn" || level === "error") {
            log("fitz_client_event", { level, clientEvent, fields });
          }
        },
      },
      meter: {
        counter(): void {},
        histogram(): void {},
        gauge(name, value): void {
          if (name === "fitz.async_handlers.active") {
            clientAsyncHandlerBacklog.active = value;
          } else if (name === "fitz.async_handlers.queued") {
            clientAsyncHandlerBacklog.queued = value;
          }
        },
      },
    },
  });
}

function transportEnv(value: string | undefined): "ws" | "tcp" {
  if (value === undefined || value === "ws") return "ws";
  if (value === "tcp") return "tcp";
  throw new Error(`DESTROYER_TRANSPORT must be ws or tcp, received '${value}'`);
}

async function runLiveRole(
  client: Client,
  liveMode: Exclude<WorkerMode, "load" | "verify" | "bombard">,
): Promise<void> {
  const jobTimeoutMs = positiveEnv("DESTROYER_JOB_TIMEOUT_MS", 180_000);
  const signal =
    liveMode === "durability-writer" ||
    liveMode === "lease-owner" ||
    liveMode === "queue-redelivery-victim" ||
    liveMode === "rpc-worker" ||
    liveMode === "rpc-stream-worker" ||
    liveMode === "hostile-rpc-worker" ||
    liveMode === "schedule-subscriber" ||
    liveMode === "schedule-outage-subscriber" ||
    liveMode === "session-boundaries" ||
    liveMode === "ephemeral-reply-loss-preparer" ||
    liveMode === "ephemeral-reply-loss-victim" ||
    liveMode === "slow-recipient" ||
    liveMode === "shutdown-reconnect-cleanup-storm" ||
    liveMode === "control-lane-cleanup-under-saturation" ||
    liveMode === "route-family-isolation-matrix"
      ? shutdown.signal
      : AbortSignal.any([shutdown.signal, AbortSignal.timeout(jobTimeoutMs)]);
  const options: LiveCommonOptions = {
    namespace: routeSegment(requiredEnv("DESTROYER_NAMESPACE")),
    workerId: routeSegment(requiredEnv("DESTROYER_WORKER_ID")),
    operations: positiveEnv("DESTROYER_OPERATIONS", 100),
    payloadBytes: positiveEnv("DESTROYER_PAYLOAD_BYTES", 256),
    concurrency: positiveEnv("DESTROYER_CONCURRENCY", 8),
    handlerDelayMs: nonNegativeEnv("DESTROYER_HANDLER_DELAY_MS", 1),
    requestTimeoutMs,
    signal,
  };

  if (booleanEnv("DESTROYER_WAIT_FOR_START_SIGNAL", false)) {
    log("live_producer_ready", { mode: liveMode, workerId: options.workerId });
    await Promise.race([startGate, rejectOnAbort(signal)]);
    log("live_producer_released", { mode: liveMode, workerId: options.workerId });
  }

  if (liveMode === "canary") {
    await runCanary(
      client,
      { ...options, domains: allCanaryDomains() },
      log,
    );
  } else if (liveMode === "durability-writer" || liveMode === "durability-verifier") {
    await runDurabilityCrashCut(
      client,
      {
        ...options,
        seed: nonNegativeEnv("DESTROYER_SEED", 424_242),
        sequence: nonNegativeEnv("DESTROYER_DURABILITY_SEQUENCE", 0),
        iterations: nonNegativeEnv("DESTROYER_DURABILITY_ITERATIONS", 1),
        action:
          liveMode === "durability-verifier"
            ? "verify"
            : durabilityAction(process.env.DESTROYER_DURABILITY_ACTION),
      },
      log,
    );
  } else if (
    liveMode === "authorization-isolation"
  ) {
    await runAuthorizationIsolation(
      client,
      { ...options, action: authorizationIsolationAction(process.env.DESTROYER_AUTH_ACTION) },
      log,
    );
  } else if (liveMode === "stream-global-recovery") {
    await runStreamGlobalRecovery(
      client,
      { ...options, action: streamGlobalRecoveryAction(process.env.DESTROYER_STREAM_GLOBAL_ACTION) },
      log,
    );
  } else if (liveMode === "queue-dead-letter-fencing") {
    await runQueueDeadLetterFencing(client, options, log);
  } else if (liveMode === "hostile-rpc-worker") {
    await runHostileRpcWorker(
      client,
      { ...options, behavior: hostileRpcBehavior(process.env.DESTROYER_HOSTILE_RPC_BEHAVIOR) },
      log,
    );
  } else if (liveMode === "hostile-rpc-caller") {
    await runHostileRpcCaller(
      client,
      { ...options, behavior: hostileRpcBehavior(process.env.DESTROYER_HOSTILE_RPC_BEHAVIOR) },
      log,
    );
  } else if (liveMode === "route-cardinality-churn") {
    await runRouteCardinalityChurn(client, options, log);
  } else if (liveMode === "rpc-response-state-conformance") {
    await runRpcResponseStateConformance(
      client,
      {
        ...options,
        url: process.env.DESTROYER_RPC_STATE_URL ?? "ws://fitz:4090/ws",
      },
      log,
    );
  } else if (liveMode === "response-envelope-boundaries") {
    await runResponseEnvelopeBoundaries(client, options, log);
  } else if (liveMode === "lease-waiter-disconnect-races") {
    await runLeaseWaiterDisconnectRaces(client, { ...options, url: process.env.DESTROYER_LEASE_RACE_URL ?? "ws://fitz:4090/ws" }, log);
  } else if (liveMode === "wildcard-registration-quota-reclamation") {
    await runWildcardRegistrationQuotaReclamation(client, { ...options, url: process.env.DESTROYER_WILDCARD_QUOTA_URL ?? "ws://fitz:4090/ws" }, log);
  } else if (liveMode === "stream-selector-cursor-conformance") {
    await runStreamSelectorCursorConformance(client, { ...options, url: process.env.DESTROYER_STREAM_SELECTOR_URL ?? "ws://fitz:4090/ws" }, log);
  } else if (liveMode === "same-shard-family-fairness") {
    await runSameShardFamilyFairness(client, { ...options, url: process.env.DESTROYER_SAME_SHARD_URL ?? "ws://fitz:4090/ws" }, log);
  } else if (liveMode === "exhaustion-probe") {
    await runExhaustionProbe(client, options, log);
  } else if (
    liveMode === "ephemeral-reply-loss-preparer" ||
    liveMode === "ephemeral-reply-loss-victim" ||
    liveMode === "ephemeral-reply-loss-verifier"
  ) {
    const action = liveMode === "ephemeral-reply-loss-preparer"
      ? "prepare"
      : liveMode === "ephemeral-reply-loss-victim"
        ? "victim"
        : "verify";
    await runEphemeralReplyLoss(client, { ...options, action }, log);
  } else if (liveMode === "slow-recipient") {
    await runSlowRecipient(client, options, log);
  } else if (liveMode === "slow-recipient-observer") {
    await runSlowRecipientObserver(client, options, log);
  } else if (liveMode === "slow-recipient-publisher") {
    await runSlowRecipientPublisher(client, options, log);
  } else if (liveMode === "shutdown-reconnect-cleanup-storm") {
    await runShutdownReconnectCleanupStormWorkload(
      client,
      {
        ...options,
        reconnectTimeoutMs: positiveEnv("DESTROYER_RECONNECT_TIMEOUT_MS", jobTimeoutMs),
      },
      log,
    );
  } else if (liveMode === "control-lane-cleanup-under-saturation") {
    await runControlLaneCleanupUnderSaturationWorkload(
      client,
      {
        ...options,
        action: controlLaneReliabilityAction(process.env.DESTROYER_RELIABILITY_ACTION),
        progressIntervalMs: positiveEnv("DESTROYER_PROGRESS_INTERVAL_MS", 250),
        reconnectTimeoutMs: positiveEnv("DESTROYER_RECONNECT_TIMEOUT_MS", jobTimeoutMs),
      },
      log,
    );
  } else if (liveMode === "route-family-isolation-matrix") {
    await runRouteFamilyIsolationMatrix(
      client,
      {
        ...options,
        identity: parseRouteFamilyIdentity(process.env.DESTROYER_ROUTE_FAMILY_IDENTITY),
        action: parseRouteFamilyIsolationAction(process.env.DESTROYER_ROUTE_FAMILY_ACTION),
      },
      log,
    );
  } else if (
    liveMode === "lease-contender" ||
    liveMode === "lease-owner" ||
    liveMode === "lease-probe"
  ) {
    await runLeaseContention(
      client,
      {
        ...options,
        action: leaseContentionAction(process.env.DESTROYER_LEASE_ACTION),
        participant: routeSegment(
          process.env.DESTROYER_LEASE_PARTICIPANT ?? `${liveMode}-${options.workerId}`,
        ),
      },
      log,
    );
  } else if (
    liveMode === "queue-overload-producer" ||
    liveMode === "queue-overload-drainer"
  ) {
    await runQueueOverload(
      client,
      {
        ...options,
        action: liveMode === "queue-overload-producer" ? "produce" : "drain",
        workers: (process.env.DESTROYER_QUEUE_OVERLOAD_WORKERS ?? options.workerId).split(","),
      },
      log,
    );
  } else if (liveMode === "stream-replay-worker") {
    await runStreamReplay(
      client,
      {
        ...options,
        seed: nonNegativeEnv("DESTROYER_SEED", 424_242),
        action: streamReplayAction(process.env.DESTROYER_STREAM_REPLAY_ACTION),
        commitAtMs: positiveEnv("DESTROYER_STREAM_REPLAY_COMMIT_AT_MS", 1),
      },
      log,
    );
  } else if (
    liveMode === "schedule-outage-producer" ||
    liveMode === "schedule-outage-canceller" ||
    liveMode === "schedule-outage-cleanup" ||
    liveMode === "schedule-outage-subscriber"
  ) {
    await runScheduleOutage(
      client,
      {
        ...options,
        seed: nonNegativeEnv("DESTROYER_SEED", 424_242),
        action: scheduleOutageAction(process.env.DESTROYER_SCHEDULE_OUTAGE_ACTION),
        missedAtMs: positiveEnv("DESTROYER_SCHEDULE_OUTAGE_MISSED_AT_MS", 1),
        raceAtMs: positiveEnv("DESTROYER_SCHEDULE_OUTAGE_RACE_AT_MS", 1),
        handlerBacklog: () => ({ ...clientAsyncHandlerBacklog }),
      },
      log,
    );
  } else if (
    liveMode === "transaction-contender" ||
    liveMode === "transaction-holder" ||
    liveMode === "transaction-verifier"
  ) {
    await runTransactionContention(
      client,
      {
        ...options,
        seed: nonNegativeEnv("DESTROYER_SEED", 424_242),
        action: transactionContentionAction(process.env.DESTROYER_TRANSACTION_ACTION),
        commitAtMs: positiveEnv("DESTROYER_TRANSACTION_COMMIT_AT_MS", 1),
      },
      log,
    );
  } else if (
    liveMode === "queue-redelivery-producer" ||
    liveMode === "queue-redelivery-victim" ||
    liveMode === "queue-redelivery-drainer"
  ) {
    await runQueueRedelivery(
      client,
      {
        ...options,
        seed: nonNegativeEnv("DESTROYER_SEED", 424_242),
        action: queueRedeliveryAction(process.env.DESTROYER_QUEUE_REDELIVERY_ACTION),
      },
      log,
    );
  } else if (liveMode === "session-boundaries") {
    await runSessionBoundaries(
      client,
      {
        ...options,
        seed: nonNegativeEnv("DESTROYER_SEED", 424_242),
      },
      log,
    );
  } else if (liveMode === "notice-publisher") {
    await runNoticePublisher(client, options, log);
  } else if (liveMode === "notice-subscriber") {
    await runNoticeSubscriber(
      client,
      {
        ...options,
        publisherCount: positiveEnv("DESTROYER_PUBLISHER_COUNT", 1),
      },
      log,
    );
  } else if (liveMode === "rpc-caller") {
    await runRpcCaller(client, options, log);
  } else if (liveMode === "rpc-worker") {
    await runRpcWorker(client, options, log);
  } else if (liveMode === "rpc-stream-worker") {
    await runRpcStreamWorker(
      client,
      {
        ...options,
        maxFrames: positiveEnv("DESTROYER_RPC_STREAM_FRAMES", 100),
        maxFrameBytes: positiveEnv("DESTROYER_RPC_STREAM_FRAME_BYTES", 1_024),
        progressAfterFrames: positiveEnv("DESTROYER_RPC_STREAM_PROGRESS_AFTER_FRAMES", 10),
      },
      log,
    );
  } else if (liveMode === "rpc-stream-caller") {
    const framesPerCall = positiveEnv("DESTROYER_RPC_STREAM_FRAMES", 100);
    await runRpcStreamCaller(
      client,
      {
        ...options,
        framesPerCall,
        frameBytes: positiveEnv("DESTROYER_RPC_STREAM_FRAME_BYTES", 1_024),
        readerDelayMs: nonNegativeEnv("DESTROYER_RPC_STREAM_READER_DELAY_MS", 0),
        expectedOutcome: rpcStreamExpectedOutcome(
          process.env.DESTROYER_RPC_STREAM_EXPECTED_OUTCOME,
        ),
        cancelAfterFrames: positiveEnv(
          "DESTROYER_RPC_STREAM_CANCEL_AFTER_FRAMES",
          Math.max(1, Math.floor(framesPerCall / 2)),
        ),
      },
      log,
    );
  } else if (liveMode === "pressure-reconciler") {
    await runPressureQueueReconciler(
      client,
      {
        namespace: options.namespace,
        workers: requiredEnv("DESTROYER_PRESSURE_WORKERS").split(","),
        requestTimeoutMs,
        signal,
      },
      log,
    );
  } else if (
    liveMode === "queue-lifecycle-producer" ||
    liveMode === "queue-lifecycle-abandoner" ||
    liveMode === "queue-lifecycle-consumer"
  ) {
    await runQueueLifecycle(
      client,
      {
        ...options,
        seed: nonNegativeEnv("DESTROYER_SEED", 424_242),
        action: queueLifecycleAction(process.env.DESTROYER_QUEUE_LIFECYCLE_ACTION),
      },
      log,
    );
  } else {
    const scheduleOptions = {
      ...options,
      seed: nonNegativeEnv("DESTROYER_SEED", 424_242),
      fireAtMs: positiveEnv("DESTROYER_SCHEDULE_FIRE_AT_MS", 1),
      handlerBacklog: () => ({ ...clientAsyncHandlerBacklog }),
    };
    if (liveMode === "schedule-subscriber") {
      await runScheduleSubscriber(client, scheduleOptions, log);
    } else {
      await runScheduleProducer(
        client,
        {
          ...scheduleOptions,
          action: scheduleProducerAction(process.env.DESTROYER_SCHEDULE_ACTION),
        },
        log,
      );
    }
  }
}

async function bombard(client: Client): Promise<void> {
  const namespace = routeSegment(requiredEnv("DESTROYER_NAMESPACE"));
  const worker = booleanEnv("DESTROYER_SHARED_ROUTE", false) ? "hot" : routeSegment(hostname());
  const selectedDomains = parseDomainSelection(process.env.DESTROYER_DOMAINS);
  const counters = emptyCounters();
  let window = emptyCounters();
  const stages: PressureStages = {};
  const queueOutcome = {
    acknowledged: [] as number[],
    ambiguousEnqueues: [] as number[],
    failedEnqueues: [] as number[],
    completed: [] as number[],
    ambiguousCompletions: [] as number[],
  };
  const lastErrors: Partial<Record<Domain, string>> = {};
  const payload = (domain: Domain, counter: number): Uint8Array =>
    new TextEncoder().encode(`${namespace}:${worker}:${domain}:${counter.toString().padStart(12, "0")}`);

  const rpcRoute = `rpc://destroyer/${namespace}/${worker}`;
  const rpcWorker = selectedDomains.includes("rpc")
    ? await client.rpc.registerWorker(
        rpcRoute,
        async (request, writer) => writer.end({ body: request.body }),
        { maxConcurrency: 64 },
      )
    : undefined;

  const progressTimer = setInterval(() => {
    const interval = window;
    window = emptyCounters();
    log("progress", {
      worker,
      connected: client.isConnected(),
      totals: counters,
      window: interval,
      stages,
      lastErrors,
    });
  }, positiveEnv("DESTROYER_PROGRESS_INTERVAL_MS", 1_000));

  const operations: Record<Domain, (counter: number, signal: AbortSignal) => Promise<void>> = {
    queue: async (i, signal) => {
      const route = `queue://destroyer/${namespace}/${worker}`;
      try {
        await observeStage(stages, "queue", "enqueue", () =>
          client.queue.enqueue(route, { body: payload("queue", i), signal }), true);
        queueOutcome.acknowledged.push(i);
      } catch (error) {
        if (isAmbiguousDurableError(error)) queueOutcome.ambiguousEnqueues.push(i);
        else queueOutcome.failedEnqueues.push(i);
        throw error;
      }
      const items = await observeStage(stages, "queue", "reserve", () =>
        client.queue.reserve(route, { leaseSeconds: 2, batchSize: 1, signal }), true);
      const item = items[0];
      if (item !== undefined) {
        const sequence = decodePressureQueueSequence(namespace, worker, item.body);
        try {
          await observeStage(stages, "queue", "complete", () => item.complete({ signal }), true);
          queueOutcome.completed.push(sequence);
        } catch (error) {
          if (isAmbiguousDurableError(error)) queueOutcome.ambiguousCompletions.push(sequence);
          throw error;
        }
      }
    },
    kv: async (i, signal) => {
      const route = `kv://destroyer/${namespace}/${worker}`;
      await observeStage(stages, "kv", "transaction", async () => {
        const tx = await client.kv.begin(route, { durability: "Sync", signal });
        try {
          await tx.put({ key: payload("kv", i), value: payload("kv", i + 1), signal });
          await tx.commit({ signal });
        } catch (error) {
          await tx.rollback({ signal }).catch(() => undefined);
          throw error;
        }
      }, true);
    },
    stream: async (i, signal) => {
      const route = `stream://destroyer/${namespace}/${worker}-s-${i}`;
      await observeStage(stages, "stream", "append", async () => {
        const session = await client.stream.begin(route, { signal });
        try {
          await session.append({ expectedOffset: 0n, body: payload("stream", i), signal });
          await session.commit({ mode: "Sync", signal });
        } catch (error) {
          await session.rollback({ signal }).catch(() => undefined);
          throw error;
        }
      }, true);
    },
    schedule: async (i, signal) => {
      const route = `schedule://destroyer/${namespace}/${worker}/job-${i}`;
      await observeStage(stages, "schedule", "create", () =>
        client.schedule.create(route, {
          cron: "0 0 1 1 *",
          deliveryMode: "Single",
          payload: payload("schedule", i),
          signal,
        }), true);
    },
    notice: async (i, signal) => {
      await observeStage(stages, "notice", "publish", () =>
        client.notice.publish(`notice://destroyer/${namespace}/${worker}`, {
          body: payload("notice", i),
          signal,
        }));
    },
    lease: async (_i, signal) => {
      await observeStage(stages, "lease", "acquire-release", async () => {
        const lease = await client.lease.acquire(`lease://destroyer/${namespace}/${worker}`, {
          ttlSeconds: 5,
          waitSeconds: 0,
          signal,
        });
        await lease.release({ signal });
      });
    },
    rpc: async (i, signal) => {
      await observeStage(stages, "rpc", "call", async () => {
        let responses = 0;
        for await (const response of client.rpc.call(rpcRoute, {
          body: payload("rpc", i),
          timeoutMs: requestTimeoutMs,
          signal,
        })) {
          if (response.body.length > 0) responses += 1;
        }
        if (responses !== 1) throw new Error(`expected one RPC response, received ${responses}`);
      });
    },
  };
  const loops = selectedDomains.map((domain) =>
    domainLoop(
      domain,
      counters,
      () => window,
      (next) => (window = next),
      lastErrors,
      operations[domain],
    ),
  );

  try {
    await Promise.all(loops);
  } finally {
    clearInterval(progressTimer);
    if (rpcWorker !== undefined) await rpcWorker.unsubscribe().catch(() => undefined);
    log("stopped", { worker, totals: counters, stages, queueOutcome });
  }
}

async function observeStage<T>(
  stages: PressureStages,
  domain: Domain,
  stage: string,
  operation: () => Promise<T>,
  durableOutcomeCanBeAmbiguous = false,
): Promise<T> {
  const metrics = stageMetrics(stages, domain, stage);
  const startedAt = performance.now();
  metrics.started += 1;
  try {
    const result = await operation();
    metrics.succeeded += 1;
    return result;
  } catch (error) {
    recordStageError(
      metrics,
      error,
      durableOutcomeCanBeAmbiguous && isAmbiguousDurableError(error),
      shutdown.signal.aborted,
    );
    throw error;
  } finally {
    recordStageLatency(metrics, performance.now() - startedAt);
  }
}

async function domainLoop(
  domain: Domain,
  totals: Counters,
  getWindow: () => Counters,
  setWindow: (next: Counters) => void,
  lastErrors: Partial<Record<Domain, string>>,
  operation: (counter: number, signal: AbortSignal) => Promise<void>,
): Promise<void> {
  let counter = 0;
  while (!shutdown.signal.aborted) {
    const signal = AbortSignal.any([shutdown.signal, AbortSignal.timeout(requestTimeoutMs)]);
    try {
      await operation(counter, signal);
      totals[domain].success += 1;
      getWindow()[domain].success += 1;
      delete lastErrors[domain];
    } catch (error) {
      if (shutdown.signal.aborted) break;
      totals[domain].error += 1;
      getWindow()[domain].error += 1;
      lastErrors[domain] = errorMessage(error);
    }
    counter += 1;
    setWindow(getWindow());
    await new Promise<void>((resolve) => setTimeout(resolve, 1));
  }
}

function recoveryShape(): WorkloadShape {
  return {
    namespace: routeSegment(requiredEnv("DESTROYER_NAMESPACE")),
    seed: nonNegativeEnv("DESTROYER_SEED", 424_242),
    resources: positiveEnv("DESTROYER_RESOURCES", 2),
    entriesPerResource: positiveEnv("DESTROYER_ENTRIES", 20),
    payloadBytes: positiveEnv("DESTROYER_PAYLOAD_BYTES", 256),
  };
}

function emptyCounters(): Counters {
  return Object.fromEntries(ALL_DOMAINS.map((domain) => [domain, { success: 0, error: 0 }])) as Counters;
}

function requiredMode(value: string | undefined): WorkerMode {
  if (
    value === "load" ||
    value === "verify" ||
    value === "bombard" ||
    value === "durability-verifier" ||
    value === "durability-writer" ||
    value === "lease-contender" ||
    value === "lease-owner" ||
    value === "lease-probe" ||
    value === "hot-route" ||
    value === "canary" ||
    value === "protocol-abuse" ||
    value === "notice-publisher" ||
    value === "notice-subscriber" ||
    value === "schedule-producer" ||
    value === "schedule-subscriber" ||
    value === "session-boundaries" ||
    value === "queue-redelivery-producer" ||
    value === "queue-redelivery-victim" ||
    value === "queue-redelivery-drainer" ||
    value === "rpc-caller" ||
    value === "rpc-worker" ||
    value === "rpc-stream-caller" ||
    value === "rpc-stream-worker" ||
    value === "pressure-reconciler" ||
    value === "queue-lifecycle-producer" ||
    value === "queue-lifecycle-abandoner" ||
    value === "queue-lifecycle-consumer" ||
    value === "transaction-contender" ||
    value === "transaction-holder" ||
    value === "transaction-verifier" ||
    value === "stream-replay-worker" ||
    value === "schedule-outage-producer" ||
    value === "schedule-outage-canceller" ||
    value === "schedule-outage-cleanup" ||
    value === "schedule-outage-subscriber" ||
    value === "queue-overload-producer" ||
    value === "queue-overload-drainer" ||
    value === "authorization-isolation" ||
    value === "stream-global-recovery" ||
    value === "queue-dead-letter-fencing" ||
    value === "hostile-rpc-worker" ||
    value === "hostile-rpc-caller" ||
    value === "route-cardinality-churn" ||
    value === "exhaustion-probe" ||
    value === "wire-conformance" ||
    value === "ephemeral-reply-loss-preparer" ||
    value === "ephemeral-reply-loss-victim" ||
    value === "ephemeral-reply-loss-verifier" ||
    value === "slow-recipient" ||
    value === "slow-recipient-observer" ||
    value === "slow-recipient-publisher" ||
    value === "shutdown-reconnect-cleanup-storm" ||
    value === "control-lane-cleanup-under-saturation" ||
    value === "route-family-isolation-matrix" ||
    value === "rpc-response-state-conformance" ||
    value === "response-envelope-boundaries"
    || value === "lease-waiter-disconnect-races"
    || value === "wildcard-registration-quota-reclamation"
    || value === "stream-selector-cursor-conformance"
    || value === "same-shard-family-fairness"
  ) {
    return value;
  }
  throw new Error(`DESTROYER_MODE is invalid; received ${value ?? "unset"}`);
}

function durabilityAction(value: string | undefined): DurabilityAction {
  const action = value ?? "baseline";
  if (action === "baseline" || action === "cut") return action;
  throw new Error(`DESTROYER_DURABILITY_ACTION is invalid; received ${action}`);
}

function authorizationIsolationAction(value: string | undefined): AuthorizationIsolationAction {
  const action = value ?? "verify";
  if (action === "write" || action === "verify") return action;
  throw new Error(`DESTROYER_AUTH_ACTION is invalid; received ${action}`);
}

function wireConformanceCase(value: string | undefined): WireConformanceCase {
  if (
    value === "lease-route-aliasing" ||
    value === "tcp-preauth-framing-slowloris" ||
    value === "connect-pipeline-family-rebind"
  ) {
    return value;
  }
  throw new Error(`DESTROYER_WIRE_CASE is invalid; received ${value ?? "unset"}`);
}

function streamGlobalRecoveryAction(value: string | undefined): StreamGlobalRecoveryAction {
  const action = value ?? "verify";
  if (action === "load" || action === "verify") return action;
  throw new Error(`DESTROYER_STREAM_GLOBAL_ACTION is invalid; received ${action}`);
}

function hostileRpcBehavior(value: string | undefined): HostileRpcBehavior {
  const behavior = value ?? "return-without-terminal";
  if (behavior === "return-without-terminal" || behavior === "throw") return behavior;
  throw new Error(`DESTROYER_HOSTILE_RPC_BEHAVIOR is invalid; received ${behavior}`);
}

function leaseContentionAction(value: string | undefined): LeaseContentionAction {
  const action = value ?? "contend";
  if (action === "contend" || action === "hold" || action === "probe") return action;
  throw new Error(`DESTROYER_LEASE_ACTION is invalid; received ${action}`);
}

function queueRedeliveryAction(value: string | undefined): QueueRedeliveryAction {
  const action = value ?? "drain";
  if (action === "produce" || action === "victim" || action === "drain") return action;
  throw new Error(`DESTROYER_QUEUE_REDELIVERY_ACTION is invalid; received ${action}`);
}

function queueLifecycleAction(value: string | undefined): QueueLifecycleAction {
  const action = value ?? "consume";
  if (action === "produce" || action === "abandon" || action === "consume") return action;
  throw new Error(`DESTROYER_QUEUE_LIFECYCLE_ACTION is invalid; received ${action}`);
}

function transactionContentionAction(value: string | undefined): TransactionContentionAction {
  const action = value ?? "contend";
  if (action === "prepare" || action === "contend" || action === "hold" || action === "verify") return action;
  throw new Error(`DESTROYER_TRANSACTION_ACTION is invalid; received ${action}`);
}

function streamReplayAction(value: string | undefined): StreamReplayAction {
  const action = value ?? "verify";
  if (action === "contend" || action === "verify") return action;
  throw new Error(`DESTROYER_STREAM_REPLAY_ACTION is invalid; received ${action}`);
}

function scheduleOutageAction(value: string | undefined): ScheduleOutageAction {
  const action = value ?? "create";
  if (action === "create" || action === "race-cancel" || action === "cleanup" || action === "subscribe") {
    return action;
  }
  throw new Error(`DESTROYER_SCHEDULE_OUTAGE_ACTION is invalid; received ${action}`);
}

function scheduleProducerAction(value: string | undefined): ScheduleProducerAction {
  const action = value ?? "create";
  if (action === "create" || action === "cancel") return action;
  throw new Error(`DESTROYER_SCHEDULE_ACTION is invalid; received ${action}`);
}

function rpcStreamExpectedOutcome(
  value: string | undefined,
): "complete" | "cancel" | "failure" {
  const outcome = value ?? "complete";
  if (outcome === "complete" || outcome === "cancel" || outcome === "failure") {
    return outcome;
  }
  throw new Error(`DESTROYER_RPC_STREAM_EXPECTED_OUTCOME is invalid; received ${outcome}`);
}

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function positiveEnv(name: string, fallback: number): number {
  const value = nonNegativeEnv(name, fallback);
  if (value < 1) throw new Error(`${name} must be positive`);
  return value;
}

function nonNegativeEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${name} must be a non-negative integer`);
  return value;
}

function booleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  if (raw === "true") return true;
  if (raw === "false") return false;
  throw new Error(`${name} must be true or false`);
}

function rejectOnAbort(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    signal.addEventListener("abort", () => reject(signal.reason), { once: true });
  });
}

function routeSegment(value: string): string {
  const result = value.toLowerCase().replace(/[^a-z0-9-]/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "");
  if (!result) throw new Error(`Cannot turn '${value}' into a route segment`);
  return result;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

function log(event: string, fields: Record<string, unknown>): void {
  process.stdout.write(
    `${JSON.stringify(
      { timestamp: new Date().toISOString(), event, ...fields },
      (_key, value: unknown) => (typeof value === "bigint" ? value.toString() : value),
    )}\n`,
  );
}
