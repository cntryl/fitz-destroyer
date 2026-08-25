import type { Client } from "@cntryl/fitz";

export type LiveLog = (event: string, fields: Record<string, unknown>) => void;

export type LiveCommonOptions = {
  namespace: string;
  workerId: string;
  operations: number;
  payloadBytes: number;
  concurrency: number;
  handlerDelayMs: number;
  requestTimeoutMs: number;
  signal: AbortSignal;
};

type NoticeSubscriberOptions = LiveCommonOptions & {
  publisherCount: number;
};

export type RpcStreamWorkerOptions = LiveCommonOptions & {
  maxFrames: number;
  maxFrameBytes: number;
  progressAfterFrames: number;
};

export type RpcStreamCallerOptions = LiveCommonOptions & {
  framesPerCall: number;
  frameBytes: number;
  readerDelayMs: number;
  expectedOutcome: "complete" | "cancel" | "failure";
  cancelAfterFrames: number;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export async function runNoticePublisher(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const route = noticeRoute(options.namespace, options.workerId);
  let published = 0;
  const progress = progressLogger("notice_publisher_progress", () => ({
    workerId: options.workerId,
    published,
  }), log);

  try {
    await runConcurrent(options.operations, options.concurrency, options.signal, async (sequence) => {
      const signal = operationSignal(options);
      await client.notice.publish(route, {
        body: livePayload("n", [options.workerId, sequence], options.payloadBytes),
        signal,
      });
      published += 1;
    });
  } finally {
    clearInterval(progress);
  }

  log("notice_publisher_complete", {
    workerId: options.workerId,
    route,
    published,
    elapsedMs: elapsedMs(startedAt),
  });
}

export async function runNoticeSubscriber(
  client: Client,
  options: NoticeSubscriberOptions,
  log: LiveLog,
): Promise<void> {
  const pattern = `notice://destroyer/${options.namespace}/*`;
  const expected = options.publisherCount * options.operations;
  const seen = new Set<string>();
  let duplicates = 0;
  let invalid = 0;
  let activeHandlers = 0;
  let maxActiveHandlers = 0;
  let firstInvalid: string | undefined;
  let resolveComplete: () => void = () => undefined;
  const complete = new Promise<void>((resolve) => {
    resolveComplete = resolve;
  });
  const startedAt = performance.now();

  const subscription = await client.notice.subscribe(pattern, async (message) => {
    activeHandlers += 1;
    maxActiveHandlers = Math.max(maxActiveHandlers, activeHandlers);
    try {
      if (options.handlerDelayMs > 0) await sleep(options.handlerDelayMs);
      const identity = parseLivePayload(message.body, "n", 2);
      const publisherId = integerField(identity[0], "notice publisher");
      const sequence = integerField(identity[1], "notice sequence");
      if (publisherId < 0 || publisherId >= options.publisherCount) {
        throw new Error(`notice publisher ${publisherId} is outside the expected range`);
      }
      if (sequence < 0 || sequence >= options.operations) {
        throw new Error(`notice sequence ${sequence} is outside the expected range`);
      }
      const expectedRoute = noticeRoute(options.namespace, String(publisherId));
      if (message.route !== expectedRoute) {
        throw new Error(`notice route '${message.route}' != '${expectedRoute}'`);
      }
      assertPayload(
        message.body,
        livePayload("n", [String(publisherId), sequence], options.payloadBytes),
        `notice ${publisherId}/${sequence}`,
      );
      const key = `${publisherId}:${sequence}`;
      if (seen.has(key)) {
        duplicates += 1;
      } else {
        seen.add(key);
        if (seen.size === expected) resolveComplete();
      }
    } catch (error) {
      invalid += 1;
      firstInvalid ??= errorMessage(error);
    } finally {
      activeHandlers -= 1;
    }
  });

  log("notice_subscriber_ready", {
    workerId: options.workerId,
    pattern,
    expected,
  });

  try {
    await Promise.race([complete, rejectOnAbort(options.signal)]);
    await sleep(Math.max(20, options.handlerDelayMs * 2));
  } finally {
    await subscription.unsubscribe().catch(() => undefined);
  }

  if (seen.size !== expected || duplicates !== 0 || invalid !== 0) {
    throw new Error(
      `notice verification failed: received=${seen.size}/${expected}, duplicates=${duplicates}, invalid=${invalid}, firstInvalid=${firstInvalid ?? "none"}`,
    );
  }

  log("notice_subscriber_complete", {
    workerId: options.workerId,
    received: seen.size,
    expected,
    duplicates,
    invalid,
    maxActiveHandlers,
    elapsedMs: elapsedMs(startedAt),
  });
}

export async function runRpcWorker(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const route = rpcRoute(options.namespace);
  const maxConcurrency = Math.max(1, Math.floor(options.concurrency / 4));
  let handled = 0;
  let failures = 0;
  let active = 0;
  let maxActive = 0;
  const startedAt = performance.now();
  const worker = await client.rpc.registerWorker(
    route,
    async (request, writer) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      try {
        const identity = parseLivePayload(request.body, "q", 2);
        const callerId = integerField(identity[0], "RPC caller");
        const sequence = integerField(identity[1], "RPC sequence");
        assertPayload(
          request.body,
          livePayload("q", [String(callerId), sequence], options.payloadBytes),
          `RPC request ${callerId}/${sequence}`,
        );
        await writer.write({
          body: livePayload(
            "r",
            [options.workerId, 0, String(callerId), sequence],
            options.payloadBytes,
          ),
        });
        if (options.handlerDelayMs > 0) await sleep(options.handlerDelayMs);
        await writer.end({
          body: livePayload(
            "r",
            [options.workerId, 1, String(callerId), sequence],
            options.payloadBytes,
          ),
        });
        handled += 1;
      } catch (error) {
        failures += 1;
        throw error;
      } finally {
        active -= 1;
      }
    },
    { maxConcurrency },
  );

  log("rpc_worker_ready", {
    workerId: options.workerId,
    route,
    maxConcurrency,
  });

  await waitForAbort(options.signal);
  await worker.unsubscribe().catch(() => undefined);
  log("rpc_worker_complete", {
    workerId: options.workerId,
    handled,
    failures,
    maxActive,
    elapsedMs: elapsedMs(startedAt),
  });
}

export async function runRpcCaller(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const route = rpcRoute(options.namespace);
  const workerCounts = new Map<string, number>();
  let completed = 0;
  let responseFrames = 0;
  const startedAt = performance.now();
  const progress = progressLogger("rpc_caller_progress", () => ({
    workerId: options.workerId,
    completed,
    responseFrames,
  }), log);

  try {
    await runConcurrent(options.operations, options.concurrency, options.signal, async (sequence) => {
      const signal = operationSignal(options);
      const request = livePayload("q", [options.workerId, sequence], options.payloadBytes);
      let selectedWorker: string | undefined;
      let frames = 0;
      for await (const response of client.rpc.call(route, {
        body: request,
        timeoutMs: options.requestTimeoutMs,
        signal,
      })) {
        if (response.sequence !== BigInt(frames)) {
          throw new Error(`RPC response sequence ${response.sequence} != ${frames}`);
        }
        const identity = parseLivePayload(response.body, "r", 4);
        const responseWorker = identity[0];
        const frame = integerField(identity[1], "RPC response frame");
        const callerId = integerField(identity[2], "RPC response caller");
        const responseSequence = integerField(identity[3], "RPC response sequence");
        if (responseWorker === undefined || responseWorker.length === 0) {
          throw new Error("RPC response omitted its worker identity");
        }
        if (selectedWorker !== undefined && selectedWorker !== responseWorker) {
          throw new Error(`RPC response switched workers from ${selectedWorker} to ${responseWorker}`);
        }
        if (frame !== frames || callerId !== Number(options.workerId) || responseSequence !== sequence) {
          throw new Error(`RPC response identity mismatch for caller ${options.workerId}/${sequence}`);
        }
        assertPayload(
          response.body,
          livePayload(
            "r",
            [responseWorker, frame, String(callerId), responseSequence],
            options.payloadBytes,
          ),
          `RPC response ${options.workerId}/${sequence}/${frames}`,
        );
        selectedWorker = responseWorker;
        frames += 1;
        responseFrames += 1;
      }
      if (frames !== 2 || selectedWorker === undefined) {
        throw new Error(`RPC call ${options.workerId}/${sequence} returned ${frames} frames`);
      }
      workerCounts.set(selectedWorker, (workerCounts.get(selectedWorker) ?? 0) + 1);
      completed += 1;
    });
  } finally {
    clearInterval(progress);
  }

  log("rpc_caller_complete", {
    workerId: options.workerId,
    completed,
    responseFrames,
    workerCounts: Object.fromEntries([...workerCounts.entries()].sort()),
    elapsedMs: elapsedMs(startedAt),
  });
}

export async function runRpcStreamWorker(
  client: Client,
  options: RpcStreamWorkerOptions,
  log: LiveLog,
): Promise<void> {
  const route = rpcStreamRoute(options.namespace);
  const maxConcurrency = Math.max(1, Math.floor(options.concurrency / 4));
  let handled = 0;
  let failures = 0;
  let framesSent = 0;
  let active = 0;
  let maxActive = 0;
  const startedAt = performance.now();
  const worker = await client.rpc.registerWorker(
    route,
    async (request, writer) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      try {
        const identity = parseLivePayload(request.body, "h", 4);
        const callerId = integerField(identity[0], "RPC stream caller");
        const call = integerField(identity[1], "RPC stream call");
        const frames = integerField(identity[2], "RPC stream frame count");
        const frameBytes = integerField(identity[3], "RPC stream frame bytes");
        if (frames < 2 || frames > options.maxFrames) {
          throw new Error(`RPC stream requested ${frames} frames; maximum is ${options.maxFrames}`);
        }
        if (frameBytes < 64 || frameBytes > options.maxFrameBytes) {
          throw new Error(
            `RPC stream requested ${frameBytes} frame bytes; maximum is ${options.maxFrameBytes}`,
          );
        }
        assertPayload(
          request.body,
          rpcStreamRequestPayload(callerId, call, frames, frameBytes),
          `RPC stream request ${callerId}/${call}`,
        );

        for (let frame = 0; frame < frames; frame += 1) {
          const response = {
            body: rpcStreamFramePayload(
              options.workerId,
              callerId,
              call,
              frame,
              frameBytes,
            ),
          };
          if (frame === frames - 1) {
            await writer.end(response);
          } else {
            await writer.write(response);
          }
          framesSent += 1;
          if (frame + 1 === options.progressAfterFrames) {
            log("rpc_stream_worker_progress", {
              workerId: options.workerId,
              callerId,
              call,
              framesSentForCall: frame + 1,
            });
          }
          if (frame + 1 < frames && options.handlerDelayMs > 0) {
            await sleep(options.handlerDelayMs);
          }
        }
        handled += 1;
      } catch (error) {
        failures += 1;
        throw error;
      } finally {
        active -= 1;
      }
    },
    { maxConcurrency },
  );

  log("rpc_stream_worker_ready", {
    workerId: options.workerId,
    route,
    maxConcurrency,
    maxFrames: options.maxFrames,
    maxFrameBytes: options.maxFrameBytes,
  });

  await waitForAbort(options.signal);
  await worker.unsubscribe().catch(() => undefined);
  log("rpc_stream_worker_complete", {
    workerId: options.workerId,
    handled,
    failures,
    framesSent,
    maxActive,
    elapsedMs: elapsedMs(startedAt),
  });
}

export async function runRpcStreamCaller(
  client: Client,
  options: RpcStreamCallerOptions,
  log: LiveLog,
): Promise<void> {
  if (options.expectedOutcome !== "complete" && options.operations !== 1) {
    throw new Error("RPC stream fault callers must make exactly one call");
  }
  const route = rpcStreamRoute(options.namespace);
  let completed = 0;
  let interrupted = 0;
  let responseFrames = 0;
  let responseBytes = 0;
  const startedAt = performance.now();
  const progress = progressLogger("rpc_stream_caller_progress", () => ({
    workerId: options.workerId,
    completed,
    interrupted,
    responseFrames,
    responseBytes,
  }), log);

  try {
    await runConcurrent(options.operations, options.concurrency, options.signal, async (call) => {
      const callController = new AbortController();
      const signal = AbortSignal.any([
        options.signal,
        callController.signal,
        AbortSignal.timeout(options.requestTimeoutMs),
      ]);
      let selectedWorker: string | undefined;
      let frames = 0;
      try {
        for await (const response of client.rpc.call(route, {
          body: rpcStreamRequestPayload(
            Number(options.workerId),
            call,
            options.framesPerCall,
            options.frameBytes,
          ),
          timeoutMs: options.requestTimeoutMs,
          signal,
        })) {
          if (response.sequence !== BigInt(frames)) {
            throw new Error(`RPC stream response sequence ${response.sequence} != ${frames}`);
          }
          const identity = parseLivePayload(response.body, "s", 4);
          const responseWorker = identity[0];
          const callerId = integerField(identity[1], "RPC stream response caller");
          const responseCall = integerField(identity[2], "RPC stream response call");
          const responseFrame = integerField(identity[3], "RPC stream response frame");
          if (responseWorker === undefined || responseWorker.length === 0) {
            throw new Error("RPC stream response omitted its worker identity");
          }
          if (selectedWorker !== undefined && selectedWorker !== responseWorker) {
            throw new Error(
              `RPC stream response switched workers from ${selectedWorker} to ${responseWorker}`,
            );
          }
          if (
            callerId !== Number(options.workerId) ||
            responseCall !== call ||
            responseFrame !== frames
          ) {
            throw new Error(`RPC stream response identity mismatch for ${options.workerId}/${call}`);
          }
          assertPayload(
            response.body,
            rpcStreamFramePayload(responseWorker, callerId, call, frames, options.frameBytes),
            `RPC stream response ${options.workerId}/${call}/${frames}`,
          );
          selectedWorker = responseWorker;
          frames += 1;
          responseFrames += 1;
          responseBytes += response.body.length;

          if (
            options.expectedOutcome === "cancel" &&
            frames === options.cancelAfterFrames
          ) {
            callController.abort(new Error(`cancelled after ${frames} RPC stream frames`));
          }
          if (options.readerDelayMs > 0) await sleep(options.readerDelayMs);
        }

        if (options.expectedOutcome !== "complete") {
          throw new Error(`RPC stream unexpectedly completed during ${options.expectedOutcome} phase`);
        }
        if (frames !== options.framesPerCall || selectedWorker === undefined) {
          throw new Error(
            `RPC stream call ${options.workerId}/${call} returned ${frames}/${options.framesPerCall} frames`,
          );
        }
        completed += 1;
      } catch (error) {
        if (options.expectedOutcome === "complete") throw error;
        if (options.expectedOutcome === "cancel" && !callController.signal.aborted) throw error;
        interrupted += 1;
        log("rpc_stream_fault_observed", {
          workerId: options.workerId,
          call,
          expectedOutcome: options.expectedOutcome,
          frames,
          error: errorMessage(error),
        });
      }
    });
  } finally {
    clearInterval(progress);
  }

  if (options.expectedOutcome === "complete" && completed !== options.operations) {
    throw new Error(`RPC stream completed ${completed}/${options.operations} calls`);
  }
  if (options.expectedOutcome !== "complete" && interrupted !== options.operations) {
    throw new Error(`RPC stream observed ${interrupted}/${options.operations} interruptions`);
  }

  log("rpc_stream_caller_complete", {
    workerId: options.workerId,
    expectedOutcome: options.expectedOutcome,
    completed,
    interrupted,
    responseFrames,
    responseBytes,
    elapsedMs: elapsedMs(startedAt),
  });
}

function noticeRoute(namespace: string, publisherId: string): string {
  return `notice://destroyer/${namespace}/${publisherId}`;
}

function rpcRoute(namespace: string): string {
  return `rpc://destroyer/${namespace}/pressure`;
}

function rpcStreamRoute(namespace: string): string {
  return `rpc://destroyer/${namespace}/stream-hose`;
}

export function rpcStreamRequestPayload(
  callerId: number,
  call: number,
  frames: number,
  frameBytes: number,
): Uint8Array {
  return livePayload("h", [callerId, call, frames, frameBytes], 128);
}

export function rpcStreamFramePayload(
  workerId: string,
  callerId: number,
  call: number,
  frame: number,
  size: number,
): Uint8Array {
  const fields = [workerId, callerId, call, frame] as const;
  const result = livePayload("s", fields, size);
  const headerLength = livePayloadHeader("s", fields).length;
  for (let index = headerLength; index < result.length; index += 1) {
    result[index] = (callerId * 17 + call * 31 + frame * 43 + index) & 0xff;
  }
  return result;
}

function livePayload(tag: string, fields: readonly (string | number)[], size: number): Uint8Array {
  const header = livePayloadHeader(tag, fields);
  if (header.length > size) {
    throw new Error(`live payload header requires ${header.length} bytes but payload is ${size}`);
  }
  const result = new Uint8Array(size);
  result.fill(0x78);
  result.set(header);
  return result;
}

function livePayloadHeader(
  tag: string,
  fields: readonly (string | number)[],
): Uint8Array {
  return encoder.encode(`${tag}|${fields.join("|")}|`);
}

function parseLivePayload(body: Uint8Array, tag: string, fields: number): string[] {
  const parts = decoder.decode(body).split("|");
  if (parts[0] !== tag || parts.length < fields + 2) {
    throw new Error(`invalid ${tag} live payload header`);
  }
  return parts.slice(1, fields + 1);
}

function integerField(value: string | undefined, name: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} '${value ?? ""}' is not a non-negative integer`);
  }
  return parsed;
}

function assertPayload(actual: Uint8Array, expected: Uint8Array, context: string): void {
  if (actual.length !== expected.length) {
    throw new Error(`${context}: length ${actual.length} != ${expected.length}`);
  }
  for (let index = 0; index < actual.length; index += 1) {
    if (actual[index] !== expected[index]) {
      throw new Error(`${context}: byte mismatch at ${index}`);
    }
  }
}

async function runConcurrent(
  operations: number,
  concurrency: number,
  signal: AbortSignal,
  operation: (sequence: number) => Promise<void>,
): Promise<void> {
  let next = 0;
  const lane = async (): Promise<void> => {
    while (next < operations) {
      signal.throwIfAborted();
      const sequence = next;
      next += 1;
      await operation(sequence);
    }
  };
  await Promise.all(Array.from({ length: Math.min(operations, concurrency) }, lane));
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
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

function waitForAbort(signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    signal.addEventListener("abort", () => resolve(), { once: true });
  });
}

function progressLogger(
  event: string,
  fields: () => Record<string, unknown>,
  log: LiveLog,
): ReturnType<typeof setInterval> {
  return setInterval(() => log(event, fields()), 1_000);
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
