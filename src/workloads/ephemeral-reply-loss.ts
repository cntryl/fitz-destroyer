import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual } from "./model.js";

export type EphemeralReplyLossAction = "prepare" | "victim" | "verify";

export async function runEphemeralReplyLoss(
  client: Client,
  options: LiveCommonOptions & { action: EphemeralReplyLossAction },
  log: LiveLog,
): Promise<void> {
  if (options.action === "prepare") {
    await prepare(client, options, log);
  } else if (options.action === "victim") {
    await dispatchLostReplies(client, options, log);
  } else {
    await verify(client, options, log);
  }
}

export function ephemeralReplyLossRoutes(namespace: string): Readonly<{
  queue: string;
  kv: string;
  stream: string;
  notice: string;
  schedule: string;
  leaseHeld: string;
  leaseLost: string;
  rpc: string;
}> {
  const route = (domain: string, resource: string): string =>
    `${domain}://destroyer/${namespace}/${resource}`;
  return {
    queue: route("queue", "reply-loss-queue"),
    kv: route("kv", "reply-loss-kv"),
    stream: route("stream", "reply-loss-stream"),
    notice: route("notice", "reply-loss-notice"),
    // Schedule routes are concrete quads (realm/area/resource/operation), unlike
    // the three-segment routes used by the other live domains.
    schedule: `schedule://destroyer/${namespace}/reply-loss-schedule/fire`,
    leaseHeld: route("lease", "reply-loss-held"),
    leaseLost: route("lease", "reply-loss-acquire"),
    rpc: route("rpc", "reply-loss-rpc"),
  };
}

async function prepare(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const routes = ephemeralReplyLossRoutes(options.namespace);
  await client.queue.enqueue(routes.queue, {
    body: replyLossPayload(options.payloadBytes, 0),
    signal: operationSignal(options),
  });
  const lease = await client.lease.acquire(routes.leaseHeld, {
    ttlSeconds: 300,
    waitSeconds: 0,
    signal: operationSignal(options),
  });
  log("ephemeral_reply_loss_preparer_ready", { queueRoute: routes.queue, leaseRoute: routes.leaseHeld });
  await waitForAbort(options.signal);
  await lease.release().catch(() => undefined);
}

async function dispatchLostReplies(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const routes = ephemeralReplyLossRoutes(options.namespace);
  let repliesReceived = 0;
  const track = (promise: Promise<unknown>): void => {
    void promise.then(
      () => { repliesReceived += 1; },
      () => undefined,
    );
  };
  if (options.workerId === "1") {
    track(client.lease.acquire(routes.leaseLost, {
      ttlSeconds: 300,
      waitSeconds: 0,
      signal: operationSignal(options),
    }));
    await sleep(Math.min(500, Math.max(100, Math.floor(options.requestTimeoutMs / 4))));
    log("ephemeral_reply_loss_dispatched", {
      workerId: options.workerId,
      requests: 1,
      repliesReceived,
    });
    await waitForAbort(options.signal);
    return;
  }
  track(client.queue.reserve(routes.queue, {
    leaseSeconds: 300,
    batchSize: 1,
    signal: operationSignal(options),
  }));
  track(client.queue.subscribe(routes.queue, () => undefined));
  track(client.kv.begin(routes.kv, { durability: "Sync", signal: operationSignal(options) }));
  track(client.kv.subscribe(routes.kv, () => undefined));
  track(client.stream.begin(routes.stream, { signal: operationSignal(options) }));
  track(client.stream.subscribe(routes.stream, () => undefined));
  track(client.notice.subscribe(routes.notice, () => undefined));
  track(client.schedule.subscribe(routes.schedule, () => undefined));
  track(client.lease.acquire(routes.leaseHeld, {
    ttlSeconds: 300,
    waitSeconds: 300,
    signal: operationSignal(options),
  }));
  track(client.lease.subscribe(routes.leaseHeld, () => undefined));
  track(client.rpc.registerWorker(routes.rpc, async (_request, writer) => {
    await writer.end({ body: new Uint8Array() });
  }));

  // Let every request leave the Node transport before reporting that it is safe to kill the client.
  await sleep(Math.min(500, Math.max(100, Math.floor(options.requestTimeoutMs / 4))));
  log("ephemeral_reply_loss_dispatched", {
    workerId: options.workerId,
    requests: 11,
    repliesReceived,
  });
  await waitForAbort(options.signal);
}

async function verify(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const routes = ephemeralReplyLossRoutes(options.namespace);
  const expectedQueueBody = replyLossPayload(options.payloadBytes, 0);
  const queueItems = await client.queue.reserve(routes.queue, {
    leaseSeconds: 30,
    batchSize: 1,
    waitSeconds: 5,
    signal: operationSignal(options),
  });
  const queueItem = queueItems[0];
  if (queueItem === undefined) throw new Error("Lost-reply Queue reservation was not released");
  assertBytesEqual(queueItem.body, expectedQueueBody, "reply-loss Queue redelivery");
  await queueItem.complete({ signal: operationSignal(options) });

  const queueWatch = deferred<void>();
  const queueSubscription = await client.queue.subscribe(routes.queue, () => queueWatch.resolve());
  const watchedQueueBody = replyLossPayload(options.payloadBytes, 1);
  await client.queue.enqueue(routes.queue, {
    body: watchedQueueBody,
    signal: operationSignal(options),
  });
  await Promise.race([queueWatch.promise, rejectOnAbort(operationSignal(options))]);
  await queueSubscription.unsubscribe();
  const watchedItems = await client.queue.reserve(routes.queue, {
    leaseSeconds: 30,
    batchSize: 1,
    waitSeconds: 5,
    signal: operationSignal(options),
  });
  const watchedItem = watchedItems[0];
  if (watchedItem === undefined) throw new Error("Queue watch probe item was not reservable");
  assertBytesEqual(watchedItem.body, watchedQueueBody, "reply-loss Queue watch probe");
  await watchedItem.complete({ signal: operationSignal(options) });

  const kvWatch = deferred<void>();
  const kvSubscription = await client.kv.subscribe(routes.kv, () => kvWatch.resolve());
  const kvTransaction = await client.kv.begin(routes.kv, {
    durability: "Sync",
    signal: operationSignal(options),
  });
  await kvTransaction.put({
    key: new TextEncoder().encode("reply-loss-key"),
    value: replyLossPayload(options.payloadBytes, 2),
    signal: operationSignal(options),
  });
  await kvTransaction.commit({ signal: operationSignal(options) });
  await Promise.race([kvWatch.promise, rejectOnAbort(operationSignal(options))]);
  await kvSubscription.unsubscribe();

  const streamWatch = deferred<void>();
  const streamSubscription = await client.stream.subscribe(routes.stream, () => streamWatch.resolve());
  const streamSession = await client.stream.begin(routes.stream, {
    signal: operationSignal(options),
  });
  await streamSession.append({
    expectedOffset: 0n,
    body: replyLossPayload(options.payloadBytes, 3),
    signal: operationSignal(options),
  });
  await streamSession.commit({ mode: "Sync", signal: operationSignal(options) });
  await Promise.race([streamWatch.promise, rejectOnAbort(operationSignal(options))]);
  await streamSubscription.unsubscribe();

  const noticeDelivery = deferred<void>();
  const noticeSubscription = await client.notice.subscribe(routes.notice, () => noticeDelivery.resolve());
  await client.notice.publish(routes.notice, {
    body: replyLossPayload(options.payloadBytes, 4),
    signal: operationSignal(options),
  });
  await Promise.race([noticeDelivery.promise, rejectOnAbort(operationSignal(options))]);
  await noticeSubscription.unsubscribe();

  const scheduleSubscription = await client.schedule.subscribe(routes.schedule, () => undefined);
  await scheduleSubscription.unsubscribe();

  const leaseWatch = deferred<void>();
  const leaseSubscription = await client.lease.subscribe(routes.leaseHeld, () => leaseWatch.resolve());
  for (const route of [routes.leaseHeld, routes.leaseLost]) {
    const status = await client.lease.query(route, { signal: operationSignal(options) });
    if (status.isHeld) throw new Error(`${route} retained a ghost holder after reply loss`);
    const lease = await client.lease.acquire(route, {
      ttlSeconds: 30,
      waitSeconds: 0,
      signal: operationSignal(options),
    });
    await lease.release({ signal: operationSignal(options) });
  }
  await Promise.race([leaseWatch.promise, rejectOnAbort(operationSignal(options))]);
  await leaseSubscription.unsubscribe();

  const rpcWorker = await client.rpc.registerWorker(routes.rpc, async (request, writer) => {
    await writer.end({ body: request.body });
  });
  let rpcFrames = 0;
  const rpcBody = replyLossPayload(options.payloadBytes, 5);
  for await (const response of client.rpc.call(routes.rpc, {
    body: rpcBody,
    timeoutMs: options.requestTimeoutMs,
    signal: operationSignal(options),
  })) {
    assertBytesEqual(response.body, rpcBody, "reply-loss RPC probe");
    rpcFrames += 1;
  }
  await rpcWorker.unsubscribe();
  if (rpcFrames !== 1) throw new Error(`Reply-loss RPC probe returned ${rpcFrames}/1 frames`);

  log("ephemeral_reply_loss_verifier_complete", {
    queueRedelivered: 1,
    queueWatchDeliveries: 1,
    kvTransactions: 1,
    kvWatchDeliveries: 1,
    streamSessions: 1,
    streamWatchDeliveries: 1,
    noticeDeliveries: 1,
    scheduleSubscriptions: 1,
    leaseRoutesReacquired: 2,
    leaseWatchDeliveries: 1,
    rpcCallsCompleted: 1,
  });
}

function replyLossPayload(payloadBytes: number, sequence: number): Uint8Array {
  const prefix = new TextEncoder().encode(`reply-loss:${sequence}:`);
  const body = new Uint8Array(Math.max(payloadBytes, prefix.length));
  body.set(prefix);
  for (let index = prefix.length; index < body.length; index += 1) {
    body[index] = (sequence + index) & 0xff;
  }
  return body;
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => signal.addEventListener("abort", () => resolve(), { once: true }));
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

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve: (value: T) => void = () => undefined;
  const promise = new Promise<T>((accept) => { resolve = accept; });
  return { promise, resolve };
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
