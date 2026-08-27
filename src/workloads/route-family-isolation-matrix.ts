import type { Client, Lease, NoticeSubscription, RpcSubscription } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { ALL_DOMAINS, assertBytesEqual, type Domain } from "./model.js";

export type RouteFamilyIdentity = "identity-a" | "identity-b";
export type RouteFamilyIsolationAction = "hold" | "verify-survivor" | "verify-closed";

export type RouteFamilyIsolationOptions = LiveCommonOptions & {
  identity: RouteFamilyIdentity;
  action: RouteFamilyIsolationAction;
};

type IsolationRoutes = Readonly<Record<Domain, string>>;

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const KV_KEY = encoder.encode("route-family-shared-key");
const SCHEDULE_CRON = "0 0 1 1 *";

export async function runRouteFamilyIsolationMatrix(
  client: Client,
  options: RouteFamilyIsolationOptions,
  log: LiveLog,
): Promise<void> {
  if (options.action === "hold") {
    await runHolder(client, options, log);
  } else {
    await runProbe(client, options, log);
  }
}

export function routeFamilyIsolationRoutes(namespace: string): IsolationRoutes {
  const base = `${namespace}/isolation/shared`;
  return {
    queue: `queue://${base}`,
    kv: `kv://${base}`,
    stream: `stream://${base}`,
    schedule: `schedule://${base}/job`,
    notice: `notice://${base}`,
    lease: `lease://${base}`,
    rpc: `rpc://${base}`,
  };
}

export function routeFamilyIsolationPermissions(namespace: string): string[] {
  return ALL_DOMAINS.map((domain) => `${domain}://${namespace}/**#*`);
}

export function routeFamilyPayload(
  identity: RouteFamilyIdentity,
  marker: string,
): Uint8Array {
  return encoder.encode(`route-family:${identity}:${marker}`);
}

export function decodeRouteFamilyPayload(
  payload: Uint8Array,
): { identity: RouteFamilyIdentity; marker: string } {
  const match = /^route-family:(identity-[ab]):([^:]+)$/u.exec(decoder.decode(payload));
  if (match?.[1] === undefined || match[2] === undefined) {
    throw new Error("invalid route-family payload");
  }
  return { identity: match[1] as RouteFamilyIdentity, marker: match[2] };
}

export function parseRouteFamilyIdentity(value: string | undefined): RouteFamilyIdentity {
  if (value === "identity-a" || value === "identity-b") return value;
  throw new Error(`DESTROYER_ROUTE_FAMILY_IDENTITY must be identity-a or identity-b, received '${value ?? ""}'`);
}

export function parseRouteFamilyIsolationAction(
  value: string | undefined,
): RouteFamilyIsolationAction {
  if (value === "hold" || value === "verify-survivor" || value === "verify-closed") return value;
  throw new Error(
    `DESTROYER_ROUTE_FAMILY_ACTION must be hold, verify-survivor, or verify-closed, received '${value ?? ""}'`,
  );
}

async function runHolder(
  client: Client,
  options: RouteFamilyIsolationOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const routes = routeFamilyIsolationRoutes(options.namespace);
  let ownNotices = 0;
  let foreignNotices = 0;
  let rpcRequestsHandled = 0;
  let rpcResponses = 0;
  let resolveInitialNotice: () => void = () => undefined;
  const initialNotice = new Promise<void>((resolve) => {
    resolveInitialNotice = resolve;
  });
  let notice: NoticeSubscription | undefined;
  let rpc: RpcSubscription | undefined;
  let lease: Lease | undefined;

  try {
    notice = await client.notice.subscribe(routes.notice, (message) => {
      let decoded: ReturnType<typeof decodeRouteFamilyPayload> | undefined;
      try {
        decoded = decodeRouteFamilyPayload(message.body);
      } catch {
        foreignNotices += 1;
        return;
      }
      if (decoded.identity !== options.identity) {
        foreignNotices += 1;
        log("route_family_isolation_foreign_notice", {
          identity: options.identity,
          publisherIdentity: decoded.identity,
          marker: decoded.marker,
        });
        return;
      }
      ownNotices += 1;
      if (decoded.marker === "holder") resolveInitialNotice();
    });
    rpc = await client.rpc.registerWorker(routes.rpc, async (_request, writer) => {
      rpcRequestsHandled += 1;
      await writer.end({ body: routeFamilyPayload(options.identity, "rpc-response") });
    });
    lease = await client.lease.acquire(routes.lease, {
      ttlSeconds: 600,
      waitSeconds: 0,
      signal: operationSignal(options),
    });

    await seedKv(client, routes.kv, options);
    await seedQueue(client, routes.queue, options);
    await seedStream(client, routes.stream, options);
    await seedSchedule(client, routes.schedule, options);
    await client.notice.publish(routes.notice, {
      body: routeFamilyPayload(options.identity, "holder"),
      signal: operationSignal(options),
    });
    await Promise.race([initialNotice, rejectOnAbort(operationSignal(options))]);
    await verifyLeaseState(client, routes.lease, true, options);
    rpcResponses = await verifyRpc(client, routes.rpc, options.identity, options);

    log("route_family_isolation_holder_armed", {
      identity: options.identity,
      verifiedDomains: ALL_DOMAINS.length,
      routes,
    });
    await waitForAbort(options.signal);
  } finally {
    await rpc?.unsubscribe().catch(() => undefined);
    await notice?.unsubscribe().catch(() => undefined);
    await lease?.release().catch(() => undefined);
  }

  log("route_family_isolation_holder_complete", {
    identity: options.identity,
    verifiedDomains: ALL_DOMAINS.length,
    ownNotices,
    foreignNotices,
    rpcResponses,
    rpcRequestsHandled,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
  if (foreignNotices !== 0) {
    throw new Error(
      `${options.identity} received ${foreignNotices} cross-family Notice deliveries`,
    );
  }
}

async function runProbe(
  client: Client,
  options: RouteFamilyIsolationOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const routes = routeFamilyIsolationRoutes(options.namespace);
  const expectedHeld = options.action === "verify-survivor";
  const leaseHeldBeforeProbe = expectedHeld
    ? await verifyLeaseState(client, routes.lease, true, options)
    : await waitForLeaseRelease(client, routes.lease, options);
  const rpcUnavailableBeforeProbe = options.action === "verify-closed"
    ? await verifyRpcUnavailable(client, routes.rpc, options)
    : false;

  await verifyKv(client, routes.kv, options);
  await verifyQueue(client, routes.queue, options);
  await verifyStream(client, routes.stream, options);
  await verifySchedule(client, routes.schedule, options);
  await verifyNotice(client, routes.notice, options);

  if (options.action === "verify-survivor") {
    await verifyRpc(client, routes.rpc, options.identity, options);
  } else {
    const lease = await client.lease.acquire(routes.lease, {
      ttlSeconds: 30,
      waitSeconds: 0,
      signal: operationSignal(options),
    });
    await lease.release({ signal: operationSignal(options) });
    const worker = await client.rpc.registerWorker(routes.rpc, async (_request, writer) => {
      await writer.end({ body: routeFamilyPayload(options.identity, "rpc-response") });
    });
    try {
      await verifyRpc(client, routes.rpc, options.identity, options);
    } finally {
      await worker.unsubscribe().catch(() => undefined);
    }
  }

  // Let concurrently registered subscribers finish dispatching before the
  // orchestrator snapshots their cross-family delivery evidence.
  await sleep(50);
  log("route_family_isolation_probe_complete", {
    identity: options.identity,
    action: options.action,
    verifiedDomains: ALL_DOMAINS.length,
    leaseHeldBeforeProbe,
    rpcUnavailableBeforeProbe,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

async function seedKv(client: Client, route: string, options: RouteFamilyIsolationOptions): Promise<void> {
  const tx = await client.kv.begin(route, { durability: "Sync", signal: operationSignal(options) });
  try {
    await tx.put({
      key: KV_KEY,
      value: routeFamilyPayload(options.identity, "kv"),
      signal: operationSignal(options),
    });
    await tx.commit({ signal: operationSignal(options) });
  } catch (error) {
    await tx.rollback().catch(() => undefined);
    throw error;
  }
  await verifyKv(client, route, options);
}

async function verifyKv(client: Client, route: string, options: RouteFamilyIsolationOptions): Promise<void> {
  const tx = await client.kv.begin(route, {
    mode: "ReadOnly",
    durability: "Sync",
    signal: operationSignal(options),
  });
  try {
    const result = await tx.get({ key: KV_KEY, signal: operationSignal(options) });
    if (result.type !== "found") throw new Error(`${options.identity} KV value was missing`);
    assertBytesEqual(
      result.value,
      routeFamilyPayload(options.identity, "kv"),
      `${options.identity} KV value`,
    );
  } finally {
    await tx.rollback().catch(() => undefined);
  }
}

async function seedQueue(
  client: Client,
  route: string,
  options: RouteFamilyIsolationOptions,
): Promise<void> {
  await client.queue.enqueue(route, {
    body: routeFamilyPayload(options.identity, "queue-seed"),
    signal: operationSignal(options),
  });
  const seed = await reserveOne(client, route, options);
  assertBytesEqual(
    seed.body,
    routeFamilyPayload(options.identity, "queue-seed"),
    `${options.identity} Queue seed`,
  );
  await seed.complete({ signal: operationSignal(options) });
  await client.queue.enqueue(route, {
    body: routeFamilyPayload(options.identity, "queue-survivor"),
    signal: operationSignal(options),
  });
}

async function verifyQueue(
  client: Client,
  route: string,
  options: RouteFamilyIsolationOptions,
): Promise<void> {
  const item = await reserveOne(client, route, options);
  assertBytesEqual(
    item.body,
    routeFamilyPayload(options.identity, "queue-survivor"),
    `${options.identity} Queue survivor`,
  );
  await item.complete({ signal: operationSignal(options) });
}

async function reserveOne(
  client: Client,
  route: string,
  options: RouteFamilyIsolationOptions,
) {
  const items = await client.queue.reserve(route, {
    leaseSeconds: 30,
    batchSize: 1,
    waitSeconds: 5,
    signal: operationSignal(options),
  });
  const item = items[0];
  if (item === undefined) throw new Error(`${options.identity} Queue item was missing`);
  return item;
}

async function seedStream(
  client: Client,
  route: string,
  options: RouteFamilyIsolationOptions,
): Promise<void> {
  const session = await client.stream.begin(route, { signal: operationSignal(options) });
  try {
    await session.append({
      expectedOffset: 0n,
      body: routeFamilyPayload(options.identity, "stream"),
      signal: operationSignal(options),
    });
    await session.commit({ mode: "Sync", signal: operationSignal(options) });
  } catch (error) {
    await session.rollback().catch(() => undefined);
    throw error;
  }
  await verifyStream(client, route, options);
}

async function verifyStream(
  client: Client,
  route: string,
  options: RouteFamilyIsolationOptions,
): Promise<void> {
  const record = await client.stream.peek(route, { signal: operationSignal(options) });
  if (record === null) throw new Error(`${options.identity} Stream record was missing`);
  if (record.offset !== 0n) {
    throw new Error(`${options.identity} Stream offset ${record.offset} != 0`);
  }
  assertBytesEqual(
    record.body,
    routeFamilyPayload(options.identity, "stream"),
    `${options.identity} Stream value`,
  );
}

async function seedSchedule(
  client: Client,
  route: string,
  options: RouteFamilyIsolationOptions,
): Promise<void> {
  await client.schedule.create(route, {
    cron: SCHEDULE_CRON,
    deliveryMode: "Single",
    payload: routeFamilyPayload(options.identity, "schedule"),
    signal: operationSignal(options),
  });
  await verifySchedule(client, route, options);
}

async function verifySchedule(
  client: Client,
  route: string,
  options: RouteFamilyIsolationOptions,
): Promise<void> {
  const selector = route.slice(0, route.lastIndexOf("/"));
  const entries = [];
  for await (const page of client.schedule.entries(selector, {
    pageSize: 10n,
    signal: operationSignal(options),
  })) {
    entries.push(...page);
  }
  if (entries.length !== 1 || entries[0]?.route !== route) {
    throw new Error(`${options.identity} Schedule listing returned ${entries.length}/1 entries`);
  }
  const entry = entries[0];
  if (entry === undefined || entry.cron !== SCHEDULE_CRON || entry.deliveryMode !== "Single") {
    throw new Error(`${options.identity} Schedule metadata did not match`);
  }
  assertBytesEqual(
    entry.payload,
    routeFamilyPayload(options.identity, "schedule"),
    `${options.identity} Schedule payload`,
  );
}

async function verifyNotice(
  client: Client,
  route: string,
  options: RouteFamilyIsolationOptions,
): Promise<void> {
  let resolveDelivery: (body: Uint8Array) => void = () => undefined;
  const delivered = new Promise<Uint8Array>((resolve) => {
    resolveDelivery = resolve;
  });
  const subscription = await client.notice.subscribe(route, (message) => resolveDelivery(message.body));
  try {
    await client.notice.publish(route, {
      body: routeFamilyPayload(options.identity, "probe"),
      signal: operationSignal(options),
    });
    const body = await Promise.race([delivered, rejectOnAbort(operationSignal(options))]);
    assertBytesEqual(
      body,
      routeFamilyPayload(options.identity, "probe"),
      `${options.identity} Notice payload`,
    );
  } finally {
    await subscription.unsubscribe().catch(() => undefined);
  }
}

async function verifyLeaseState(
  client: Client,
  route: string,
  expectedHeld: boolean,
  options: RouteFamilyIsolationOptions,
): Promise<boolean> {
  const state = await client.lease.query(route, { signal: operationSignal(options) });
  if (state.isHeld !== expectedHeld) {
    throw new Error(
      `${options.identity} Lease isHeld=${state.isHeld}, expected ${expectedHeld}`,
    );
  }
  return state.isHeld;
}

async function waitForLeaseRelease(
  client: Client,
  route: string,
  options: RouteFamilyIsolationOptions,
): Promise<false> {
  const deadline = Date.now() + options.requestTimeoutMs;
  let held = true;
  while (Date.now() < deadline) {
    const state = await client.lease.query(route, { signal: operationSignal(options) });
    held = state.isHeld;
    if (!held) return false;
    await sleep(50);
  }
  throw new Error(`${options.identity} Lease remained held after its session was killed`);
}

async function verifyRpc(
  client: Client,
  route: string,
  identity: RouteFamilyIdentity,
  options: RouteFamilyIsolationOptions,
): Promise<number> {
  let responses = 0;
  for await (const response of client.rpc.call(route, {
    body: routeFamilyPayload(identity, "rpc-request"),
    timeoutMs: options.requestTimeoutMs,
    signal: operationSignal(options),
  })) {
    assertBytesEqual(
      response.body,
      routeFamilyPayload(identity, "rpc-response"),
      `${identity} RPC response`,
    );
    responses += 1;
  }
  if (responses !== 1) throw new Error(`${identity} RPC returned ${responses}/1 responses`);
  return responses;
}

async function verifyRpcUnavailable(
  client: Client,
  route: string,
  options: RouteFamilyIsolationOptions,
): Promise<boolean> {
  let rejected = false;
  let responses = 0;
  try {
    for await (const _response of client.rpc.call(route, {
      body: routeFamilyPayload(options.identity, "rpc-unavailable-probe"),
      timeoutMs: Math.min(options.requestTimeoutMs, 2_000),
      signal: operationSignal(options),
    })) {
      responses += 1;
    }
  } catch {
    rejected = true;
  }
  if (!rejected || responses !== 0) {
    throw new Error(
      `${options.identity} closed family retained an RPC worker: rejected=${rejected}, responses=${responses}`,
    );
  }
  return true;
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
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

function rejectOnAbort(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    signal.addEventListener("abort", () => reject(signal.reason), { once: true });
  });
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
