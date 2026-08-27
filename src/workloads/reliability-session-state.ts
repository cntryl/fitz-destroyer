import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual } from "./model.js";

export const RELIABILITY_RPC_PROBE_CALLS = 4;

export type ReliabilityRoutes = {
  queue: string;
  kv: string;
  stream: string;
  notice: string;
  rpc: string;
  lease: string;
  schedule: string;
};

export type ReliabilitySessionOptions = LiveCommonOptions & {
  reconnectTimeoutMs: number;
};

export type ReliabilityProbeEvidence = {
  queueRedelivered: number;
  kvUncommittedValues: number;
  streamUncommittedRecords: number;
  leaseHeld: number;
  leasePendingWaiters: number;
  rpcProbeCalls: number;
  rpcProbeFailures: number;
};

type DisposableRegistration = { unsubscribe(): Promise<void> };

export type ArmedSessionState = {
  routes: ReliabilityRoutes;
  body: Uint8Array;
  key: Uint8Array;
  queueItem: Awaited<ReturnType<Client["queue"]["reserve"]>>[number];
  transaction: Awaited<ReturnType<Client["kv"]["begin"]>>;
  streamSession: Awaited<ReturnType<Client["stream"]["begin"]>>;
  lease: Awaited<ReturnType<Client["lease"]["acquire"]>>;
  registrations: DisposableRegistration[];
};

export function reliabilityRoutes(namespace: string, workerId: string): ReliabilityRoutes {
  const resource = `cleanup-${workerId}`;
  return {
    queue: `queue://destroyer/${namespace}/${resource}`,
    kv: `kv://destroyer/${namespace}/${resource}`,
    stream: `stream://destroyer/${namespace}/${resource}`,
    notice: `notice://destroyer/${namespace}/${resource}`,
    rpc: `rpc://destroyer/${namespace}/${resource}`,
    lease: `lease://destroyer/${namespace}/${resource}`,
    schedule: `schedule://destroyer/${namespace}/${resource}/fire`,
  };
}

export async function runShutdownReconnectCleanupStormWorkload(
  client: Client,
  options: ReliabilitySessionOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const state = await armReliabilitySession(client, options);
  log("shutdown_reconnect_cleanup_armed", {
    workerId: options.workerId,
    routes: state.routes,
  });

  try {
    const reconnectMs = await waitForReconnect(client, state, options);
    const staleHandleRejections = await rejectStaleHandles(state, options);
    await unsubscribeRegistrations(state.registrations);
    const probe = await verifyReliabilitySessionReleased(client, options);
    log("shutdown_reconnect_cleanup_complete", {
      workerId: options.workerId,
      reconnects: 1,
      reconnectMs,
      staleHandleRejections,
      ...probe,
      elapsedMs: Math.round(performance.now() - startedAt),
    });
  } finally {
    await disposeReliabilitySession(state);
  }
}

export async function armReliabilitySession(
  client: Client,
  options: LiveCommonOptions,
): Promise<ArmedSessionState> {
  const routes = reliabilityRoutes(options.namespace, options.workerId);
  const body = reliabilityPayload(options.namespace, options.workerId, options.payloadBytes);
  const key = new TextEncoder().encode(`uncommitted-${options.workerId}`);
  const registrations: DisposableRegistration[] = [];
  let transaction: ArmedSessionState["transaction"] | undefined;
  let streamSession: ArmedSessionState["streamSession"] | undefined;
  let lease: ArmedSessionState["lease"] | undefined;
  let queueItem: ArmedSessionState["queueItem"] | undefined;

  try {
    const signal = operationSignal(options);
    await client.queue.enqueue(routes.queue, { body, signal });
    queueItem = (await client.queue.reserve(routes.queue, {
      leaseSeconds: 30,
      batchSize: 1,
      waitSeconds: 5,
      signal,
    }))[0];
    if (queueItem === undefined) throw new Error(`${routes.queue} was not reserved`);

    transaction = await client.kv.begin(routes.kv, { durability: "Sync", signal });
    await transaction.put({ key, value: body, signal });

    streamSession = await client.stream.begin(routes.stream, { signal });
    await streamSession.append({ expectedOffset: 0n, body, signal });

    lease = await client.lease.acquire(routes.lease, {
      ttlSeconds: 30,
      waitSeconds: 5,
      signal,
    });

    registrations.push(
      await client.kv.subscribe(routes.kv, () => undefined, { signal: options.signal }),
      await client.queue.subscribe(routes.queue, () => undefined, { signal: options.signal }),
      await client.stream.subscribe(routes.stream, () => undefined, { signal: options.signal }),
      await client.notice.subscribe(routes.notice, () => undefined, { signal: options.signal }),
      await client.lease.subscribe(routes.lease, () => undefined, { signal: options.signal }),
      await client.schedule.subscribe(routes.schedule, () => undefined, { signal: options.signal }),
      await client.rpc.registerWorker(
        routes.rpc,
        async (request, writer) => writer.end({ body: request.body }),
        { maxConcurrency: Math.max(1, options.concurrency) },
      ),
    );

    return {
      routes,
      body,
      key,
      queueItem,
      transaction,
      streamSession,
      lease,
      registrations,
    };
  } catch (error) {
    await unsubscribeRegistrations(registrations);
    await lease?.release().catch(() => undefined);
    await streamSession?.rollback().catch(() => undefined);
    await transaction?.rollback().catch(() => undefined);
    await queueItem?.complete().catch(() => undefined);
    throw error;
  }
}

export async function verifyReliabilitySessionReleased(
  client: Client,
  options: LiveCommonOptions,
): Promise<ReliabilityProbeEvidence> {
  const routes = reliabilityRoutes(options.namespace, options.workerId);
  const body = reliabilityPayload(options.namespace, options.workerId, options.payloadBytes);
  const key = new TextEncoder().encode(`uncommitted-${options.workerId}`);

  const reserved = await client.queue.reserve(routes.queue, {
    leaseSeconds: 30,
    batchSize: 2,
    waitSeconds: 5,
    signal: operationSignal(options, 3),
  });
  if (reserved.length !== 1 || reserved[0] === undefined) {
    throw new Error(`${routes.queue} redelivered ${reserved.length}/1 cleanup message`);
  }
  assertBytesEqual(reserved[0].body, body, `${routes.queue} cleanup redelivery`);
  await reserved[0].complete({ signal: operationSignal(options, 3) });
  const remaining = await client.queue.reserve(routes.queue, {
    leaseSeconds: 5,
    batchSize: 1,
    signal: operationSignal(options, 3),
  });
  if (remaining.length !== 0) throw new Error(`${routes.queue} retained duplicate work`);

  const transaction = await client.kv.begin(routes.kv, {
    mode: "ReadOnly",
    durability: "Sync",
    signal: operationSignal(options, 3),
  });
  let kvUncommittedValues = 0;
  try {
    const value = await transaction.get({ key, signal: operationSignal(options, 3) });
    kvUncommittedValues = value.type === "found" ? 1 : 0;
  } finally {
    await transaction.rollback({ signal: operationSignal(options, 3) }).catch(() => undefined);
  }

  let streamUncommittedRecords = 0;
  for await (const batch of client.stream.read(routes.stream, {
    fromOffset: 0n,
    mode: "replay",
    batchSize: 8,
    signal: operationSignal(options, 3),
  })) {
    streamUncommittedRecords += batch.records.length;
  }

  const leaseState = await client.lease.query(routes.lease, {
    signal: operationSignal(options, 3),
  });
  if (leaseState.isHeld || leaseState.pendingWaiters !== 0) {
    throw new Error(
      `${routes.lease} retained owner=${String(leaseState.isHeld)} waiters=${leaseState.pendingWaiters}`,
    );
  }
  const replacementLease = await client.lease.acquire(routes.lease, {
    ttlSeconds: 5,
    waitSeconds: 5,
    signal: operationSignal(options, 3),
  });
  await replacementLease.release({ signal: operationSignal(options, 3) });

  let rpcProbeCalls = 0;
  let rpcProbeFailures = 0;
  const replacementWorker = await client.rpc.registerWorker(
    routes.rpc,
    async (request, writer) => writer.end({ body: request.body }),
    { maxConcurrency: RELIABILITY_RPC_PROBE_CALLS },
  );
  try {
    for (let sequence = 0; sequence < RELIABILITY_RPC_PROBE_CALLS; sequence += 1) {
      const request = reliabilityPayload(
        options.namespace,
        `${options.workerId}-rpc-${sequence}`,
        options.payloadBytes,
      );
      try {
        let responses = 0;
        for await (const response of client.rpc.call(routes.rpc, {
          body: request,
          timeoutMs: options.requestTimeoutMs,
          signal: operationSignal(options, 3),
        })) {
          assertBytesEqual(response.body, request, `${routes.rpc} probe ${sequence}`);
          responses += 1;
        }
        if (responses !== 1) throw new Error(`${routes.rpc} returned ${responses}/1 probe frames`);
        rpcProbeCalls += 1;
      } catch (error) {
        rpcProbeFailures += 1;
        throw error;
      }
    }
  } finally {
    await replacementWorker.unsubscribe().catch(() => undefined);
  }

  if (kvUncommittedValues !== 0 || streamUncommittedRecords !== 0) {
    throw new Error(
      `Uncommitted cleanup state survived: kv=${kvUncommittedValues}, stream=${streamUncommittedRecords}`,
    );
  }

  return {
    queueRedelivered: 1,
    kvUncommittedValues,
    streamUncommittedRecords,
    leaseHeld: Number(leaseState.isHeld),
    leasePendingWaiters: leaseState.pendingWaiters,
    rpcProbeCalls,
    rpcProbeFailures,
  };
}

async function waitForReconnect(
  client: Client,
  state: ArmedSessionState,
  options: ReliabilitySessionOptions,
): Promise<number> {
  const startedAt = performance.now();
  const disconnectDeadline = Date.now() + options.reconnectTimeoutMs;
  while (
    state.transaction.isOpen() &&
    state.streamSession.isOpen() &&
    Date.now() < disconnectDeadline
  ) {
    await sleep(10);
  }
  if (state.transaction.isOpen() || state.streamSession.isOpen()) {
    throw new Error("Fitz shutdown did not invalidate connection-bound handles");
  }

  const reconnectDeadline = Date.now() + options.reconnectTimeoutMs;
  while (!client.isConnected() && Date.now() < reconnectDeadline) await sleep(10);
  if (!client.isConnected()) throw new Error("Client did not reconnect after Fitz restarted");
  return Math.round(performance.now() - startedAt);
}

async function rejectStaleHandles(
  state: ArmedSessionState,
  options: LiveCommonOptions,
): Promise<number> {
  const attempts = [
    () => state.queueItem.complete({ signal: operationSignal(options) }),
    () => state.transaction.commit({ signal: operationSignal(options) }),
    () => state.streamSession.commit({ mode: "Sync", signal: operationSignal(options) }),
    () => state.lease.release({ signal: operationSignal(options) }),
  ];
  let rejected = 0;
  for (const attempt of attempts) {
    try {
      await attempt();
    } catch {
      rejected += 1;
    }
  }
  if (rejected !== attempts.length) {
    throw new Error(`Stale cleanup handles rejected ${rejected}/${attempts.length} operations`);
  }
  return rejected;
}

export async function disposeReliabilitySession(state: ArmedSessionState): Promise<void> {
  await unsubscribeRegistrations(state.registrations);
  await state.lease.release().catch(() => undefined);
  await state.streamSession.rollback().catch(() => undefined);
  await state.transaction.rollback().catch(() => undefined);
  await state.queueItem.complete().catch(() => undefined);
}

async function unsubscribeRegistrations(
  registrations: readonly DisposableRegistration[],
): Promise<void> {
  await Promise.allSettled(registrations.map((registration) => registration.unsubscribe()));
}

function reliabilityPayload(namespace: string, identity: string, bytes: number): Uint8Array {
  const prefix = new TextEncoder().encode(`cleanup:${namespace}:${identity}:`);
  const payload = new Uint8Array(bytes);
  for (let index = 0; index < payload.length; index += 1) {
    payload[index] = prefix[index % prefix.length] ?? 0;
  }
  return payload;
}

function operationSignal(options: LiveCommonOptions, multiplier = 1): AbortSignal {
  return AbortSignal.any([
    options.signal,
    AbortSignal.timeout(options.requestTimeoutMs * multiplier),
  ]);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
