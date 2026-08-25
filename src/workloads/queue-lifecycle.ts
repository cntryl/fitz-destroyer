import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual, deterministicPayload } from "./model.js";

export type QueueLifecycleAction = "produce" | "abandon" | "consume";
export type QueueLifecycleOptions = LiveCommonOptions & { seed: number; action: QueueLifecycleAction };

export async function runQueueLifecycle(
  client: Client,
  options: QueueLifecycleOptions,
  log: LiveLog,
): Promise<void> {
  const route = queueLifecycleRoute(options.namespace);
  if (options.action === "produce") {
    if (options.operations < 8) throw new Error("Queue lifecycle requires at least 8 operations");
    for (let sequence = 0; sequence < 4; sequence += 1) {
      await client.queue.enqueue(route, {
        body: payload(options, sequence),
        signal: operationSignal(options),
      });
    }
    const partial = await client.queue.reserve(route, {
      leaseSeconds: 1,
      batchSize: 4,
      signal: operationSignal(options),
    });
    if (partial.length !== 4) throw new Error(`Expected partial Queue batch of 4, received ${partial.length}`);
    const initiallyReserved = partial.map((item) => identify(options, item.body)).sort(numericSort);
    for (const item of partial.slice(0, 2)) await item.complete({ signal: operationSignal(options) });
    const initiallyCompleted = partial.slice(0, 2).map((item) => identify(options, item.body)).sort(numericSort);
    await sleep(1_250);
    const redelivered = await client.queue.reserve(route, {
      leaseSeconds: 30,
      batchSize: 4,
      signal: operationSignal(options),
    });
    const redeliveredSequences = redelivered.map((item) => identify(options, item.body)).sort(numericSort);
    const expectedRedelivery = partial.slice(2).map((item) => identify(options, item.body)).sort(numericSort);
    if (JSON.stringify(redeliveredSequences) !== JSON.stringify(expectedRedelivery)) {
      throw new Error(
        `Queue lease expiry redelivery ${JSON.stringify(redeliveredSequences)} != ${JSON.stringify(expectedRedelivery)}`,
      );
    }
    for (const item of redelivered) await item.complete({ signal: operationSignal(options) });
    for (let sequence = 4; sequence < options.operations; sequence += 1) {
      await client.queue.enqueue(route, {
        body: payload(options, sequence),
        signal: operationSignal(options),
      });
    }
    log("queue_lifecycle_producer_complete", {
      operations: options.operations,
      initiallyReserved,
      initiallyCompleted,
      redelivered: redeliveredSequences,
      completed: [...initiallyCompleted, ...redeliveredSequences].sort(numericSort),
    });
    return;
  }

  if (options.action === "abandon") {
    const items = await client.queue.reserve(route, {
      leaseSeconds: 30,
      batchSize: 1,
      waitSeconds: 5,
      signal: operationSignal(options),
    });
    const item = items[0];
    if (item === undefined) throw new Error("Queue abandoner did not reserve an item");
    log("queue_lifecycle_abandoned", { sequence: identify(options, item.body) });
    return;
  }

  const sequences: number[] = [];
  let emptyPolls = 0;
  while (emptyPolls < 2) {
    const items = await client.queue.reserve(route, {
      leaseSeconds: 30,
      batchSize: 1,
      waitSeconds: 1,
      signal: operationSignal(options),
    });
    if (items.length === 0) {
      emptyPolls += 1;
      continue;
    }
    emptyPolls = 0;
    const item = items[0]!;
    const sequence = identify(options, item.body);
    sequences.push(sequence);
    await item.complete({ signal: operationSignal(options) });
    if (options.handlerDelayMs > 0) await sleep(options.handlerDelayMs);
  }
  log("queue_lifecycle_consumer_complete", {
    workerId: options.workerId,
    sequences,
  });
}

export function queueLifecycleRoute(namespace: string): string {
  return `queue://destroyer/${namespace}/lifecycle`;
}

function identify(options: QueueLifecycleOptions, actual: Uint8Array): number {
  const prefix = new TextDecoder().decode(actual.subarray(0, Math.min(actual.length, 32)));
  const match = /^queue:0000:(\d{8}):/u.exec(prefix);
  const sequence = match?.[1] === undefined ? Number.NaN : Number(match[1]);
  if (!Number.isSafeInteger(sequence) || sequence < 0 || sequence >= options.operations) {
    throw new Error("Queue lifecycle payload omitted a valid deterministic sequence");
  }
  assertBytesEqual(actual, payload(options, sequence), "Queue lifecycle payload");
  return sequence;
}

function payload(options: QueueLifecycleOptions, sequence: number): Uint8Array {
  return deterministicPayload(options, "queue", 0, sequence);
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function numericSort(left: number, right: number): number {
  return left - right;
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
