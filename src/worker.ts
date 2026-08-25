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
  | "rpc-stream-worker";
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
  return createClient({
    url: process.env.FITZ_URL ?? "ws://fitz:4090/ws",
    transport: "ws",
    timeout: requestTimeoutMs,
    reconnect: {
      enabled: reconnect,
      maxAttempts: Infinity,
      backoffMs: 100,
      maxBackoffMs: 2_000,
    },
    retry: { enabled: reconnect, maxAttempts: 3, backoffMs: 50, maxBackoffMs: 500 },
    heartbeat: { enabled: true, intervalMs: 2_000, timeoutMs: 6_000 },
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
    liveMode === "schedule-subscriber" ||
    liveMode === "session-boundaries"
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
        action:
          liveMode === "durability-verifier"
            ? "verify"
            : durabilityAction(process.env.DESTROYER_DURABILITY_ACTION),
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
      lastErrors,
    });
  }, positiveEnv("DESTROYER_PROGRESS_INTERVAL_MS", 1_000));

  const operations: Record<Domain, (counter: number, signal: AbortSignal) => Promise<void>> = {
    queue: async (i, signal) => {
      const route = `queue://destroyer/${namespace}/${worker}`;
      await client.queue.enqueue(route, { body: payload("queue", i), signal });
      const items = await client.queue.reserve(route, { leaseSeconds: 30, batchSize: 1, signal });
      if (items.length > 0) await items[0]?.complete({ signal });
    },
    kv: async (i, signal) => {
      const route = `kv://destroyer/${namespace}/${worker}`;
      const tx = await client.kv.begin(route, { durability: "Sync", signal });
      try {
        await tx.put({ key: payload("kv", i), value: payload("kv", i + 1), signal });
        await tx.commit({ signal });
      } catch (error) {
        await tx.rollback({ signal }).catch(() => undefined);
        throw error;
      }
    },
    stream: async (i, signal) => {
      const route = `stream://destroyer/${namespace}/${worker}-s-${i}`;
      const session = await client.stream.begin(route, { signal });
      try {
        await session.append({ expectedOffset: 0n, body: payload("stream", i), signal });
        await session.commit({ mode: "Sync", signal });
      } catch (error) {
        await session.rollback({ signal }).catch(() => undefined);
        throw error;
      }
    },
    schedule: async (i, signal) => {
      const route = `schedule://destroyer/${namespace}/${worker}/job-${i}`;
      await client.schedule.create(route, {
        cron: "0 0 1 1 *",
        deliveryMode: "Single",
        payload: payload("schedule", i),
        signal,
      });
    },
    notice: async (i, signal) => {
      await client.notice.publish(`notice://destroyer/${namespace}/${worker}`, {
        body: payload("notice", i),
        signal,
      });
    },
    lease: async (_i, signal) => {
      const lease = await client.lease.acquire(`lease://destroyer/${namespace}/${worker}`, {
        ttlSeconds: 5,
        waitSeconds: 0,
        signal,
      });
      await lease.release({ signal });
    },
    rpc: async (i, signal) => {
      let responses = 0;
      for await (const response of client.rpc.call(rpcRoute, {
        body: payload("rpc", i),
        timeoutMs: requestTimeoutMs,
        signal,
      })) {
        if (response.body.length > 0) responses += 1;
      }
      if (responses !== 1) throw new Error(`expected one RPC response, received ${responses}`);
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
    log("stopped", { worker, totals: counters });
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
    value === "rpc-stream-worker"
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
