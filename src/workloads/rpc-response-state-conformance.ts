import { createClient, type Client } from "@cntryl/fitz";
import {
  concatBytes,
  decodeStandardResponse,
  decodeTlvs,
  encodeBytes,
  encodeString,
  encodeTlv,
  encodeU32,
  encodeU64,
  openRawWebSocket,
  type RawProtocolConnection,
} from "./raw-protocol.js";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual } from "./model.js";

export type RpcResponseStateOptions = LiveCommonOptions & { url: string };

type RpcWorkerRequest = {
  correlation: Uint8Array;
  route: string;
  body: Uint8Array;
};

const RPC_SUBSCRIBE_WORKER = 300;
const RPC_REQUEST = 302;
const RPC_RESPONSE = 303;

export async function runRpcResponseStateConformance(
  client: Client,
  options: RpcResponseStateOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const route = `rpc://destroyer/${options.namespace}/response-state`;
  const worker = await openRawWebSocket(options.url, options.requestTimeoutMs);
  let callersTerminated = 0;
  let duplicateCallerTerminals = 0;
  let unknownCorrelationRejected = 0;
  let duplicateTerminalRejected = 0;
  let postCancelResponsesObserved = 0;
  let postDisconnectRejected = 0;
  let healthyCalls = 0;
  let healthyFailures = 0;

  try {
    await connectRawWorker(worker, route, options.requestTimeoutMs);

    const wrongCorrelationCall = collectCall(client, route, markerBody("wrong-correlation"), options);
    const wrongCorrelationRequest = await receiveWorkerRequest(worker, options.requestTimeoutMs);
    const wrong = wrongCorrelationRequest.correlation.slice();
    wrong[15] = (wrong[15]! + 1) & 0xff;
    await sendRpcResponse(worker, wrong, 0n, true, new Uint8Array());
    unknownCorrelationRejected += 1;
    await sendSuccessfulTerminal(worker, wrongCorrelationRequest);
    const wrongOutcome = await wrongCorrelationCall;
    callersTerminated += wrongOutcome.terminated;
    duplicateCallerTerminals += wrongOutcome.duplicateTerminals;

    const duplicateCall = collectCall(client, route, markerBody("duplicate-terminal"), options);
    const duplicateRequest = await receiveWorkerRequest(worker, options.requestTimeoutMs);
    await sendSuccessfulTerminal(worker, duplicateRequest);
    const duplicateOutcome = await duplicateCall;
    callersTerminated += duplicateOutcome.terminated;
    duplicateCallerTerminals += duplicateOutcome.duplicateTerminals;
    await sendRpcResponse(
      worker,
      duplicateRequest.correlation,
      1n,
      true,
      duplicateRequest.body,
    );
    duplicateTerminalRejected += 1;

    const cancellation = new AbortController();
    const cancelledCall = collectCall(
      client,
      route,
      markerBody("cancelled"),
      options,
      cancellation.signal,
    );
    const cancelledRequest = await receiveWorkerRequest(worker, options.requestTimeoutMs);
    cancellation.abort(new Error("intentional caller cancellation"));
    const cancelledOutcome = await cancelledCall;
    callersTerminated += cancelledOutcome.terminated;
    duplicateCallerTerminals += cancelledOutcome.duplicateTerminals;
    await sendRpcResponse(
      worker,
      cancelledRequest.correlation,
      0n,
      true,
      cancelledRequest.body,
    );
    postCancelResponsesObserved += 1;

    const disconnected = createClient({
      url: options.url,
      transport: "ws",
      timeout: options.requestTimeoutMs,
      reconnect: { enabled: false },
      retry: { enabled: false },
      heartbeat: { enabled: false },
    });
    await disconnected.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
    const disconnectedCall = collectCall(
      disconnected,
      route,
      markerBody("caller-disconnect"),
      options,
    );
    const disconnectedRequest = await receiveWorkerRequest(worker, options.requestTimeoutMs);
    await disconnected.close();
    const disconnectedOutcome = await disconnectedCall;
    callersTerminated += disconnectedOutcome.terminated;
    duplicateCallerTerminals += disconnectedOutcome.duplicateTerminals;
    await sleep(50);
    await sendRpcResponse(
      worker,
      disconnectedRequest.correlation,
      0n,
      true,
      disconnectedRequest.body,
    );
    postDisconnectRejected += 1;

    for (let sequence = 0; sequence < 4; sequence += 1) {
      const body = markerBody(`healthy-${sequence}`);
      try {
        const call = collectCall(client, route, body, options);
        const request = await receiveWorkerRequest(worker, options.requestTimeoutMs);
        await sendSuccessfulTerminal(worker, request);
        const outcome = await call;
        if (outcome.frames !== 1 || outcome.failed) throw new Error(`healthy RPC ${sequence} did not complete once`);
        healthyCalls += 1;
      } catch (error) {
        healthyFailures += 1;
        throw error;
      }
    }

    log("rpc_response_state_conformance_worker_complete", {
      cases: 5,
      callersTerminated,
      duplicateCallerTerminals,
      unknownCorrelationRejected,
      duplicateTerminalRejected,
      postCancelResponsesObserved,
      postDisconnectRejected,
      healthyCalls,
      healthyFailures,
      elapsedMs: Math.round(performance.now() - startedAt),
    });
  } finally {
    await worker.close().catch(() => undefined);
  }
}

export function encodeRpcResponsePayload(
  correlation: Uint8Array,
  sequence: bigint,
  terminal: boolean,
  body: Uint8Array,
): Uint8Array {
  if (correlation.length !== 16) throw new Error(`RPC correlation is ${correlation.length}/16 bytes`);
  return concatBytes(correlation, encodeU64(sequence), new Uint8Array([terminal ? 1 : 0]), encodeBytes(body));
}

export function decodeRpcWorkerRequest(payload: Uint8Array): RpcWorkerRequest {
  if (payload.length < 24) throw new Error("RPC worker request is truncated");
  const correlation = payload.slice(0, 16);
  const routeLength = readU32(payload, 16);
  const routeStart = 20;
  const routeEnd = routeStart + routeLength;
  if (routeEnd + 4 > payload.length) throw new Error("RPC worker request route is truncated");
  const bodyLength = readU32(payload, routeEnd);
  const bodyStart = routeEnd + 4;
  if (bodyStart + bodyLength !== payload.length) throw new Error("RPC worker request body is malformed");
  return {
    correlation,
    route: new TextDecoder().decode(payload.slice(routeStart, routeEnd)),
    body: payload.slice(bodyStart),
  };
}

async function connectRawWorker(
  connection: RawProtocolConnection,
  route: string,
  timeoutMs: number,
): Promise<void> {
  await connection.send(encodeTlv(1, new Uint8Array()));
  await sleep(25);
  await connection.send(encodeTlv(RPC_SUBSCRIBE_WORKER, concatBytes(encodeString(route), encodeU32(1))));
  const result = decodeStandardResponse(await receiveRecord(connection, RPC_SUBSCRIBE_WORKER, timeoutMs));
  if (!result.ok) throw new Error(`raw RPC worker registration failed: ${result.errorMessage ?? result.errorCode}`);
}

async function receiveWorkerRequest(
  connection: RawProtocolConnection,
  timeoutMs: number,
): Promise<RpcWorkerRequest> {
  return decodeRpcWorkerRequest(await receiveRecord(connection, RPC_REQUEST, timeoutMs));
}

async function sendSuccessfulTerminal(
  connection: RawProtocolConnection,
  request: RpcWorkerRequest,
): Promise<void> {
  await connection.send(encodeTlv(RPC_RESPONSE, encodeRpcResponsePayload(request.correlation, 0n, true, request.body)));
}

async function sendRpcResponse(
  connection: RawProtocolConnection,
  correlation: Uint8Array,
  sequence: bigint,
  terminal: boolean,
  body: Uint8Array,
): Promise<void> {
  await connection.send(encodeTlv(RPC_RESPONSE, encodeRpcResponsePayload(correlation, sequence, terminal, body)));
}

async function receiveRecord(
  connection: RawProtocolConnection,
  type: number,
  timeoutMs: number,
): Promise<Uint8Array> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const frame = await connection.receive(Math.max(1, deadline - Date.now()));
    const record = decodeTlvs(frame).find((candidate) => candidate.type === type);
    if (record !== undefined) return record.payload;
  }
  throw new Error(`timed out waiting for RPC message ${type}`);
}

async function collectCall(
  client: Client,
  route: string,
  expectedBody: Uint8Array,
  options: LiveCommonOptions,
  signal?: AbortSignal,
): Promise<{ frames: number; terminated: number; duplicateTerminals: number; failed: boolean }> {
  let frames = 0;
  let terminations = 0;
  let failed = false;
  try {
    for await (const response of client.rpc.call(route, {
      body: expectedBody,
      timeoutMs: options.requestTimeoutMs,
      signal: signal ?? options.signal,
    })) {
      assertBytesEqual(response.body, expectedBody, `${route} response ${frames}`);
      frames += 1;
    }
  } catch {
    failed = true;
  } finally {
    terminations += 1;
  }
  return {
    frames,
    terminated: terminations,
    duplicateTerminals: Math.max(0, terminations - 1),
    failed,
  };
}

function markerBody(marker: string): Uint8Array {
  return new TextEncoder().encode(`rpc-state:${marker}`);
}

function readU32(bytes: Uint8Array, offset: number): number {
  if (offset + 4 > bytes.length) throw new Error("u32 is truncated");
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(offset);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
