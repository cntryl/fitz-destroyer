import { createClient, type Client } from "@cntryl/fitz";
import { createDestroyerToken } from "../auth-token.js";
import {
  concatBytes,
  decodeStandardResponse,
  decodeTlvs,
  encodeBytes,
  encodeString,
  encodeTlv,
  encodeU32,
  encodeU64,
  openRawTcp,
  openRawWebSocket,
  type RawProtocolConnection,
} from "./raw-protocol.js";
import type { LiveLog } from "./live.js";

export type WireConformanceCase =
  | "lease-route-aliasing"
  | "tcp-preauth-framing-slowloris"
  | "connect-pipeline-family-rebind";

export type WireConformanceOptions = {
  namespace: string;
  requestTimeoutMs: number;
  operations: number;
  clientReplicas: number;
  wireCase: WireConformanceCase;
  url: string;
  log: LiveLog;
};

const CONNECT = 1;
const QUEUE_ENQUEUE = 200;
const LEASE_ACQUIRE = 400;
const LEASE_RENEW = 401;
const LEASE_RELEASE = 402;
const LEASE_QUERY = 403;
const LEASE_SUBSCRIBE = 407;
const LEASE_UNSUBSCRIBE = 408;

export async function runWireConformance(options: WireConformanceOptions): Promise<void> {
  if (options.wireCase === "lease-route-aliasing") {
    await runLeaseRouteAliasing(options);
  } else if (options.wireCase === "tcp-preauth-framing-slowloris") {
    await runTcpPreauthFramingSlowloris(options);
  } else {
    await runConnectPipelineFamilyRebind(options);
  }
}

async function runLeaseRouteAliasing(options: WireConformanceOptions): Promise<void> {
  const connection = await openRawWebSocket(options.url, options.requestTimeoutMs);
  const startedAt = performance.now();
  let rejected = 0;
  await connection.send(encodeTlv(CONNECT, new Uint8Array()));
  await sleep(25);

  try {
    for (const operation of ["acquire", "renew", "release", "query", "subscribe", "unsubscribe"] as const) {
      const route = `lease://${options.namespace}/alias/${operation}`;
      const trailing = `${route}/trailing`;
      let token: bigint | undefined;
      let canonicalSubscription = false;
      if (operation !== "acquire") {
        const acquired = await request(connection, LEASE_ACQUIRE, leasePayload("acquire", route), options.requestTimeoutMs);
        if (!acquired.ok) throw new Error(`canonical Lease acquire failed for ${operation}: ${acquired.errorMessage ?? "unknown"}`);
        token = decodeAcquireToken(acquired.data);
      }
      if (operation === "unsubscribe") {
        const subscribed = await request(connection, LEASE_SUBSCRIBE, leasePayload("subscribe", route), options.requestTimeoutMs);
        if (!subscribed.ok) throw new Error("canonical Lease subscribe failed before alias unsubscribe");
        canonicalSubscription = true;
      }
      const malformed = await request(
        connection,
        leaseMessageType(operation),
        leasePayload(operation, trailing, token),
        options.requestTimeoutMs,
      );
      if (malformed.ok) {
        throw new Error(`Lease ${operation} accepted trailing route '${trailing}'`);
      }
      rejected += 1;
      if (operation === "acquire") {
        const canonical = await request(connection, LEASE_QUERY, leasePayload("query", route), options.requestTimeoutMs);
        if (!canonical.ok || canonical.data[0] !== 0) throw new Error(`trailing acquire affected canonical route '${route}'`);
      } else if (operation !== "subscribe" && operation !== "unsubscribe") {
        const canonical = await request(connection, LEASE_QUERY, leasePayload("query", route), options.requestTimeoutMs);
        if (!canonical.ok || canonical.data[0] !== 1) throw new Error(`trailing ${operation} changed canonical Lease '${route}'`);
      }
      if (canonicalSubscription) await request(connection, LEASE_UNSUBSCRIBE, leasePayload("unsubscribe", route), options.requestTimeoutMs);
      if (token !== undefined) await request(connection, LEASE_RELEASE, leasePayload("release", route, token), options.requestTimeoutMs);
    }
  } finally {
    await connection.close();
  }
  options.log("lease_route_aliasing_complete", {
    operations: 6,
    rejected,
    canonicalPreserved: rejected,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

async function runTcpPreauthFramingSlowloris(options: WireConformanceOptions): Promise<void> {
  const count = Math.min(128, Math.max(16, options.clientReplicas * 8));
  const timeoutMs = Math.max(options.requestTimeoutMs, 10_000) + 2_000;
  const sockets: RawProtocolConnection[] = [];
  const startedAt = performance.now();
  let closed = 0;
  try {
    for (let index = 0; index < count; index += 1) {
      const socket = await openRawTcp("fitz", 4091, options.requestTimeoutMs);
      sockets.push(socket);
      if (index % 2 === 0) {
        await socket.send(new Uint8Array());
      } else {
        await socket.sendRaw(new Uint8Array([0, 0, 0, 8, 1]));
      }
    }
    const closeResults = await Promise.all(
      sockets.map(async (socket) => {
        try {
          await socket.waitForClose(timeoutMs);
          return true;
        } catch {
          socket.destroy();
          return false;
        }
      }),
    );
    closed = closeResults.filter(Boolean).length;
    if (closed !== count) throw new Error(`pre-auth TCP sockets closed=${closed}/${count}`);
    await runTransportCanary(options, "tcp");
    await runTransportCanary(options, "ws");
    const secondWave = await Promise.all(
      Array.from({ length: Math.min(8, count) }, () => openRawTcp("fitz", 4091, options.requestTimeoutMs)),
    );
    for (const socket of secondWave) socket.destroy();
    options.log("tcp_preauth_framing_slowloris_complete", {
      socketsOpened: count,
      socketsClosed: closed,
      secondWaveAdmitted: secondWave.length,
      tcpCanary: 1,
      websocketCanary: 1,
      elapsedMs: Math.round(performance.now() - startedAt),
    });
  } finally {
    for (const socket of sockets) socket.destroy();
  }
}

async function runConnectPipelineFamilyRebind(options: WireConformanceOptions): Promise<void> {
  const startedAt = performance.now();
  const transports = ["ws", "tcp"] as const;
  let accepted = 0;
  let rejected = 0;
  const failures: string[] = [];
  for (const transport of transports) {
    const route = `queue://${options.namespace}/pipeline/${transport}`;
    const body = new TextEncoder().encode(`connect-pipeline:${transport}`);
    const token = createDestroyerToken("identity-b", [`queue://${options.namespace}/**#*`]);
    const connection = transport === "ws"
      ? await openRawWebSocket(options.url, options.requestTimeoutMs)
      : await openRawTcp("fitz", 4091, options.requestTimeoutMs);
    let outcome: "accepted" | "rejected";
    let responseObserved = false;
    let terminalError: string | undefined;
    try {
      const result = await requestCombined(
        connection,
        concatBytes(encodeTlv(CONNECT, new TextEncoder().encode(token)), encodeTlv(QUEUE_ENQUEUE, queueEnqueuePayload(route, body))),
        QUEUE_ENQUEUE,
        options.requestTimeoutMs,
      );
      responseObserved = true;
      outcome = result.ok ? "accepted" : "rejected";
    } catch (error) {
      if (!isTerminalTransportError(error)) throw error;
      outcome = "rejected";
      terminalError = error instanceof Error ? error.message : String(error);
    } finally {
      await connection.close();
    }
    const familyA = await reserveForFamily(options, "identity-a", route);
    const familyB = await reserveForFamily(options, "identity-b", route);
    const familyATotal = familyA.length;
    const familyBTotal = familyB.length;
    if (outcome === "accepted") {
      accepted += 1;
      if (familyATotal !== 0 || familyBTotal !== 1) {
        failures.push(`pipeline ${transport} accepted with family A/B queue counts ${familyATotal}/${familyBTotal}`);
      }
    } else {
      rejected += 1;
      if (familyATotal !== 0 || familyBTotal !== 0) {
        failures.push(`pipeline ${transport} rejected non-atomically with family A/B queue counts ${familyATotal}/${familyBTotal}`);
      }
    }
    if (!responseObserved) {
      failures.push(`pipeline ${transport} produced no terminal response${terminalError === undefined ? "" : `: ${terminalError}`}`);
    }
    options.log("connect_pipeline_family_rebind_case", {
      transport,
      outcome,
      familyA: familyATotal,
      familyB: familyBTotal,
      responseObserved: Number(responseObserved),
      terminalError,
    });
  }
  options.log("connect_pipeline_family_rebind_complete", {
    transports: transports.length,
    accepted,
    rejected,
    failures: failures.length,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
  if (failures.length > 0) throw new Error(failures.join("; "));
}

async function reserveForFamily(
  options: WireConformanceOptions,
  identity: "identity-a" | "identity-b",
  route: string,
): Promise<readonly { complete: () => Promise<void> }[]> {
  const client = createClient({
    url: options.url,
    transport: "ws",
    tokenProvider: async () => createDestroyerToken(identity, [`queue://${options.namespace}/**#*`]),
    timeout: options.requestTimeoutMs,
    reconnect: { enabled: false },
    retry: { enabled: false },
    heartbeat: { enabled: false },
  });
  try {
    await client.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
    const items = await client.queue.reserve(route, { leaseSeconds: 30, batchSize: 2, waitSeconds: 0 });
    const result = items.map((item) => ({ complete: () => item.complete() }));
    for (const item of result) await item.complete();
    return result;
  } finally {
    await client.close().catch(() => undefined);
  }
}

async function runTransportCanary(options: WireConformanceOptions, transport: "tcp" | "ws"): Promise<void> {
  const client = createClient({
    url: transport === "tcp" ? "tcp://fitz:4091" : options.url,
    transport,
    timeout: options.requestTimeoutMs,
    reconnect: { enabled: false },
    retry: { enabled: false },
    heartbeat: { enabled: false },
  });
  try {
    await client.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
    const subscription = await client.notice.subscribe(`notice://${options.namespace}/preauth/canary`, () => undefined);
    await subscription.unsubscribe();
  } finally {
    await client.close().catch(() => undefined);
  }
}

async function request(
  connection: RawProtocolConnection,
  type: number,
  payload: Uint8Array,
  timeoutMs: number,
): Promise<ReturnType<typeof decodeStandardResponse>> {
  await connection.send(encodeTlv(type, payload));
  return decodeResponse(await receiveRecord(connection, type, timeoutMs));
}

async function requestCombined(
  connection: RawProtocolConnection,
  frame: Uint8Array,
  type: number,
  timeoutMs: number,
): Promise<ReturnType<typeof decodeStandardResponse>> {
  await connection.send(frame);
  return decodeResponse(await receiveRecord(connection, type, timeoutMs));
}

async function receiveRecord(connection: RawProtocolConnection, type: number, timeoutMs: number): Promise<Uint8Array> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const records = decodeTlvs(await connection.receive(Math.max(1, deadline - Date.now())));
    const record = records.find((item) => item.type === type);
    if (record) return record.payload;
  }
  throw new Error(`timed out waiting for response type ${type}`);
}

function decodeResponse(payload: Uint8Array): ReturnType<typeof decodeStandardResponse> {
  return decodeStandardResponse(payload);
}

function decodeAcquireToken(data: Uint8Array): bigint {
  if (data.length < 9) throw new Error("Lease acquire response omitted fencing token");
  return new DataView(data.buffer, data.byteOffset, data.byteLength).getBigUint64(1);
}

function leaseMessageType(operation: "acquire" | "renew" | "release" | "query" | "subscribe" | "unsubscribe"): number {
  return operation === "acquire" ? LEASE_ACQUIRE
    : operation === "renew" ? LEASE_RENEW
      : operation === "release" ? LEASE_RELEASE
        : operation === "query" ? LEASE_QUERY
          : operation === "subscribe" ? LEASE_SUBSCRIBE
            : LEASE_UNSUBSCRIBE;
}

function leasePayload(
  operation: "acquire" | "renew" | "release" | "query" | "subscribe" | "unsubscribe",
  route: string,
  token?: bigint,
): Uint8Array {
  const routeBytes = encodeString(route);
  if (operation === "query" || operation === "subscribe" || operation === "unsubscribe") return routeBytes;
  const owner = encodeString("");
  if (operation === "acquire") return concatBytes(routeBytes, owner, encodeU64(30n), encodeU32(0));
  if (operation === "renew") return concatBytes(routeBytes, owner, encodeU64(token ?? 0n), encodeU64(30n));
  return concatBytes(routeBytes, owner, encodeU64(token ?? 0n));
}

function queueEnqueuePayload(route: string, body: Uint8Array): Uint8Array {
  return concatBytes(encodeString(route), encodeBytes(body), new Uint8Array([0]));
}

function isTerminalTransportError(error: unknown): boolean {
  return error instanceof Error && /closed|connection|failed|timed out/u.test(error.message);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
