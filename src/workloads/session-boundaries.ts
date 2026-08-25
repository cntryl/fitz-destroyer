import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual, deterministicPayload, kvKey } from "./model.js";

export type SessionBoundaryOptions = LiveCommonOptions & {
  seed: number;
};

export async function runSessionBoundaries(
  client: Client,
  options: SessionBoundaryOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const queueRoute = boundaryRoute("queue", options.namespace);
  const kvRoute = boundaryRoute("kv", options.namespace);
  const streamRoute = boundaryRoute("stream", options.namespace);
  const leaseRoute = boundaryRoute("lease", options.namespace);
  const queueBody = boundaryPayload(options, "queue", 0);
  const kvBaseline = boundaryPayload(options, "kv", 0);
  const kvUncommitted = boundaryPayload(options, "kv", 1);
  const streamBaseline = boundaryPayload(options, "stream", 0);
  const streamUncommitted = boundaryPayload(options, "stream", 1);

  await client.queue.enqueue(queueRoute, { body: queueBody, signal: operationSignal(options) });
  const reserved = await client.queue.reserve(queueRoute, {
    leaseSeconds: 300,
    batchSize: 1,
    signal: operationSignal(options),
  });
  const queueItem = reserved[0];
  if (queueItem === undefined) throw new Error("Queue boundary item was not reserved");

  const kvSeed = await client.kv.begin(kvRoute, {
    durability: "Sync",
    signal: operationSignal(options),
  });
  await kvSeed.put({ key: kvKey(0), value: kvBaseline, signal: operationSignal(options) });
  await kvSeed.commit({ signal: operationSignal(options) });
  const kvTransaction = await client.kv.begin(kvRoute, {
    durability: "Sync",
    signal: operationSignal(options),
  });
  await kvTransaction.put({
    key: kvKey(1),
    value: kvUncommitted,
    signal: operationSignal(options),
  });

  const streamSeed = await client.stream.begin(streamRoute, {
    signal: operationSignal(options),
  });
  await streamSeed.append({
    expectedOffset: 0n,
    body: streamBaseline,
    signal: operationSignal(options),
  });
  await streamSeed.commit({ mode: "Sync", signal: operationSignal(options) });
  const streamSession = await client.stream.begin(streamRoute, {
    signal: operationSignal(options),
  });
  await streamSession.append({
    expectedOffset: 1n,
    body: streamUncommitted,
    signal: operationSignal(options),
  });

  const lease = await client.lease.acquire(leaseRoute, {
    ttlSeconds: 300,
    waitSeconds: 0,
    signal: operationSignal(options),
  });
  log("session_boundaries_armed", {
    queueRoute,
    kvRoute,
    streamRoute,
    leaseRoute,
  });

  await waitForConnectionCycle(client, options.signal, options.requestTimeoutMs * 6, log);
  const staleErrors = await Promise.all([
    expectRejected("queue.complete", () => queueItem.complete(), log),
    expectRejected("kv.commit", () => kvTransaction.commit(), log),
    expectRejected("stream.commit", () => streamSession.commit({ mode: "Sync" }), log),
    expectRejected("lease.release", () => lease.release(), log),
  ]);

  const redelivered = await client.queue.reserve(queueRoute, {
    leaseSeconds: 30,
    batchSize: 1,
    waitSeconds: 10,
    signal: operationSignal(options),
  });
  const redeliveredItem = redelivered[0];
  if (redeliveredItem === undefined) throw new Error("Queue item did not redeliver after restart");
  assertBytesEqual(redeliveredItem.body, queueBody, "redelivered Queue body");
  await redeliveredItem.complete({ signal: operationSignal(options) });
  const queueEmpty = await client.queue.reserve(queueRoute, {
    leaseSeconds: 30,
    batchSize: 1,
    signal: operationSignal(options),
  });
  if (queueEmpty.length !== 0) throw new Error("Completed Queue item was delivered again");

  const kvVerify = await client.kv.begin(kvRoute, {
    mode: "ReadOnly",
    durability: "Sync",
    signal: operationSignal(options),
  });
  const baselineValue = await kvVerify.get({ key: kvKey(0), signal: operationSignal(options) });
  const uncommittedValue = await kvVerify.get({
    key: kvKey(1),
    signal: operationSignal(options),
  });
  if (baselineValue.type !== "found") throw new Error("Committed KV baseline disappeared");
  assertBytesEqual(baselineValue.value, kvBaseline, "committed KV baseline");
  if (uncommittedValue.type !== "not-found") {
    throw new Error("Uncommitted KV mutation survived restart");
  }
  await kvVerify.rollback({ signal: operationSignal(options) });

  let streamRecords = 0;
  for await (const batch of client.stream.read(streamRoute, {
    fromOffset: 0n,
    mode: "replay",
    batchSize: 10,
    signal: operationSignal(options),
  })) {
    for (const record of batch.records) {
      if (record.offset !== 0n || streamRecords !== 0) {
        throw new Error(`Unexpected Stream record at offset ${record.offset}`);
      }
      assertBytesEqual(record.body, streamBaseline, "committed Stream baseline");
      streamRecords += 1;
    }
  }
  if (streamRecords !== 1) throw new Error(`Stream recovered ${streamRecords}/1 committed records`);

  const leaseAfterRestart = await client.lease.query(leaseRoute, {
    signal: operationSignal(options),
  });
  if (leaseAfterRestart.isHeld) throw new Error("Ephemeral Lease ownership survived restart");
  const replacementLease = await client.lease.acquire(leaseRoute, {
    ttlSeconds: 30,
    waitSeconds: 0,
    signal: operationSignal(options),
  });
  await replacementLease.release({ signal: operationSignal(options) });

  log("session_boundaries_complete", {
    staleRejections: staleErrors.length,
    staleErrors,
    queueRedelivered: 1,
    queueCompleted: 1,
    kvCommittedValues: 1,
    kvUncommittedValues: 0,
    streamCommittedRecords: streamRecords,
    streamUncommittedRecords: 0,
    leaseHeldAfterRestart: false,
    leaseReacquired: 1,
    elapsedMs: elapsedMs(startedAt),
  });
}

export function boundaryRoute(
  domain: "queue" | "kv" | "stream" | "lease",
  namespace: string,
): string {
  return `${domain}://destroyer/${namespace}/session-boundary`;
}

function boundaryPayload(
  options: Pick<SessionBoundaryOptions, "seed" | "payloadBytes">,
  domain: "queue" | "kv" | "stream",
  sequence: number,
): Uint8Array {
  return deterministicPayload(options, domain, 0, sequence);
}

async function waitForConnectionCycle(
  client: Client,
  signal: AbortSignal,
  timeoutMs: number,
  log: LiveLog,
): Promise<void> {
  const disconnectDeadline = Date.now() + timeoutMs;
  while (client.isConnected() && Date.now() < disconnectDeadline) {
    signal.throwIfAborted();
    await sleep(10);
  }
  if (client.isConnected()) throw new Error("Client did not observe the Fitz disconnect");
  log("session_boundaries_disconnect_observed", {});

  const reconnectDeadline = Date.now() + timeoutMs;
  while (!client.isConnected() && Date.now() < reconnectDeadline) {
    signal.throwIfAborted();
    await sleep(10);
  }
  if (!client.isConnected()) throw new Error("Client did not reconnect after Fitz restart");
  log("session_boundaries_reconnect_observed", {});
}

async function expectRejected(
  operation: string,
  invoke: () => Promise<unknown>,
  log: LiveLog,
): Promise<string> {
  try {
    await invoke();
  } catch (error) {
    const message = errorMessage(error);
    log("stale_handle_rejected", { operation, error: message });
    return `${operation}: ${message}`;
  }
  throw new Error(`${operation} unexpectedly accepted a stale handle`);
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function elapsedMs(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
