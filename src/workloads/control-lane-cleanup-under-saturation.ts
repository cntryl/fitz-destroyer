import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { ALL_DOMAINS, assertBytesEqual, type Domain } from "./model.js";
import {
  armReliabilitySession,
  disposeReliabilitySession,
  verifyReliabilitySessionReleased,
  type ReliabilitySessionOptions,
} from "./reliability-session-state.js";

export type ControlLaneReliabilityAction = "saturate" | "hold" | "probe";

export type ControlLaneReliabilityOptions = ReliabilitySessionOptions & {
  action: ControlLaneReliabilityAction;
  progressIntervalMs: number;
};

type Totals = Record<Domain, { success: number; error: number }>;

export async function runControlLaneCleanupUnderSaturationWorkload(
  client: Client,
  options: ControlLaneReliabilityOptions,
  log: LiveLog,
): Promise<void> {
  if (options.action === "hold") {
    await holdCleanupTarget(client, options, log);
  } else if (options.action === "probe") {
    const startedAt = performance.now();
    const evidence = await verifyReliabilitySessionReleased(client, options);
    log("control_lane_cleanup_probe_complete", {
      workerId: options.workerId,
      ...evidence,
      elapsedMs: Math.round(performance.now() - startedAt),
    });
  } else {
    await saturateNormalLanes(client, options, log);
  }
}

export function controlLaneReliabilityAction(
  value: string | undefined,
): ControlLaneReliabilityAction {
  const action = value ?? "probe";
  if (action === "saturate" || action === "hold" || action === "probe") return action;
  throw new Error(`DESTROYER_RELIABILITY_ACTION is invalid; received ${action}`);
}

async function holdCleanupTarget(
  client: Client,
  options: ControlLaneReliabilityOptions,
  log: LiveLog,
): Promise<void> {
  const state = await armReliabilitySession(client, options);
  log("control_lane_cleanup_target_armed", {
    workerId: options.workerId,
    routes: state.routes,
  });
  try {
    await waitForAbort(options.signal);
  } finally {
    await disposeReliabilitySession(state);
  }
}

async function saturateNormalLanes(
  client: Client,
  options: ControlLaneReliabilityOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const totals = emptyTotals();
  const lanesPerDomain = Math.max(1, Math.min(options.concurrency, 16));
  const rpcRoute = saturationRoute("rpc", options, 0);
  const rpcWorker = await client.rpc.registerWorker(
    rpcRoute,
    async (request, writer) => writer.end({ body: request.body }),
    { maxConcurrency: lanesPerDomain },
  );
  const progress = setInterval(() => {
    log("control_lane_saturation_progress", {
      workerId: options.workerId,
      totals,
    });
  }, options.progressIntervalMs);
  const loops = ALL_DOMAINS.flatMap((domain) =>
    Array.from({ length: lanesPerDomain }, (_, lane) =>
      saturationLoop(client, options, domain, lane, rpcRoute, totals),
    ),
  );

  log("control_lane_saturator_ready", {
    workerId: options.workerId,
    domains: ALL_DOMAINS,
    lanesPerDomain,
  });
  try {
    await Promise.all(loops);
  } finally {
    clearInterval(progress);
    await cleanupSaturationState(client, options, lanesPerDomain);
    await rpcWorker.unsubscribe().catch(() => undefined);
  }
  log("control_lane_saturation_progress", {
    workerId: options.workerId,
    totals,
  });
  log("control_lane_saturator_complete", {
    workerId: options.workerId,
    totals,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

async function saturationLoop(
  client: Client,
  options: ControlLaneReliabilityOptions,
  domain: Domain,
  lane: number,
  rpcRoute: string,
  totals: Totals,
): Promise<void> {
  let sequence = 0;
  while (!options.signal.aborted) {
    try {
      await runSaturationOperation(client, options, domain, lane, sequence, rpcRoute);
      totals[domain].success += 1;
    } catch {
      if (options.signal.aborted) break;
      totals[domain].error += 1;
    }
    sequence += 1;
    await sleep(1);
  }
}

async function runSaturationOperation(
  client: Client,
  options: LiveCommonOptions,
  domain: Domain,
  lane: number,
  sequence: number,
  rpcRoute: string,
): Promise<void> {
  const signal = operationSignal(options);
  const route = saturationRoute(domain, options, lane);
  const body = saturationPayload(options.workerId, domain, lane, sequence, options.payloadBytes);
  if (domain === "queue") {
    await client.queue.enqueue(route, { body, signal });
    const item = (await client.queue.reserve(route, {
      leaseSeconds: 5,
      batchSize: 1,
      signal,
    }))[0];
    if (item === undefined) throw new Error(`${route} omitted its saturation message`);
    await item.complete({ signal });
  } else if (domain === "kv") {
    const transaction = await client.kv.begin(route, { durability: "Sync", signal });
    try {
      await transaction.put({ key: body.subarray(0, Math.min(body.length, 32)), value: body, signal });
      await transaction.rollback({ signal });
    } catch (error) {
      await transaction.rollback().catch(() => undefined);
      throw error;
    }
  } else if (domain === "stream") {
    const session = await client.stream.begin(route, { signal });
    try {
      await session.append({ expectedOffset: 0n, body, signal });
      await session.rollback({ signal });
    } catch (error) {
      await session.rollback().catch(() => undefined);
      throw error;
    }
  } else if (domain === "schedule") {
    await client.schedule.create(route, {
      cron: "0 0 1 1 *",
      deliveryMode: "Single",
      payload: body,
      signal,
    });
    await client.schedule.cancel(route, { signal });
  } else if (domain === "notice") {
    await client.notice.publish(route, { body, signal });
  } else if (domain === "lease") {
    const lease = await client.lease.acquire(route, {
      ttlSeconds: 5,
      waitSeconds: 0,
      signal,
    });
    await lease.release({ signal });
  } else {
    let responses = 0;
    for await (const response of client.rpc.call(rpcRoute, {
      body,
      timeoutMs: options.requestTimeoutMs,
      signal,
    })) {
      assertBytesEqual(response.body, body, `${rpcRoute} saturation response`);
      responses += 1;
    }
    if (responses !== 1) throw new Error(`${rpcRoute} returned ${responses}/1 saturation frames`);
  }
}

async function cleanupSaturationState(
  client: Client,
  options: ControlLaneReliabilityOptions,
  lanesPerDomain: number,
): Promise<void> {
  await Promise.all(
    Array.from({ length: lanesPerDomain }, (_, lane) =>
      cleanupSaturationLane(client, options, lane),
    ),
  );
}

async function cleanupSaturationLane(
  client: Client,
  options: ControlLaneReliabilityOptions,
  lane: number,
): Promise<void> {
  await client.schedule
    .cancel(saturationRoute("schedule", options, lane), {
      signal: AbortSignal.timeout(options.requestTimeoutMs),
    })
    .catch(() => undefined);
  const queueRoute = saturationRoute("queue", options, lane);
  for (let pass = 0; pass < 4; pass += 1) {
    const items = await client.queue
      .reserve(queueRoute, {
        leaseSeconds: 5,
        batchSize: 1_024,
        signal: AbortSignal.timeout(options.requestTimeoutMs),
      })
      .catch(() => []);
    if (items.length === 0) break;
    const signal = AbortSignal.timeout(options.requestTimeoutMs);
    await Promise.allSettled(items.map((item) => item.complete({ signal })));
  }
}

function saturationRoute(
  domain: Domain,
  options: Pick<LiveCommonOptions, "namespace" | "workerId">,
  lane: number,
): string {
  const resource = `saturator-${options.workerId}-${lane}`;
  if (domain === "schedule") {
    return `schedule://destroyer/${options.namespace}/${resource}/pulse`;
  }
  return `${domain}://destroyer/${options.namespace}/${resource}`;
}

function saturationPayload(
  workerId: string,
  domain: Domain,
  lane: number,
  sequence: number,
  bytes: number,
): Uint8Array {
  const prefix = new TextEncoder().encode(`${workerId}:${domain}:${lane}:${sequence}:`);
  const body = new Uint8Array(bytes);
  for (let index = 0; index < body.length; index += 1) {
    body[index] = prefix[index % prefix.length] ?? 0;
  }
  return body;
}

function emptyTotals(): Totals {
  return Object.fromEntries(
    ALL_DOMAINS.map((domain) => [domain, { success: 0, error: 0 }]),
  ) as Totals;
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([
    options.signal,
    AbortSignal.timeout(options.requestTimeoutMs),
  ]);
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    signal.addEventListener("abort", () => resolve(), { once: true });
  });
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
