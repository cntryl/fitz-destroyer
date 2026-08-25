import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual, deterministicPayload } from "./model.js";

export type QueueRedeliveryAction = "produce" | "victim" | "drain";

export type QueueRedeliveryOptions = LiveCommonOptions & {
  action: QueueRedeliveryAction;
  seed: number;
};

export async function runQueueRedelivery(
  client: Client,
  options: QueueRedeliveryOptions,
  log: LiveLog,
): Promise<void> {
  const route = queueRedeliveryRoute(options.namespace);
  if (options.action === "produce") {
    await runConcurrent(options.operations, options.concurrency, options.signal, async (sequence) => {
      await client.queue.enqueue(route, {
        body: queuePayload(options, sequence),
        signal: operationSignal(options),
      });
    });
    log("queue_redelivery_producer_complete", { route, produced: options.operations });
    return;
  }

  if (options.action === "victim") {
    const requested = Math.min(1_024, Math.max(1, Math.ceil(options.operations / 2)));
    const items = await client.queue.reserve(route, {
      leaseSeconds: 300,
      batchSize: requested,
      waitSeconds: 10,
      signal: operationSignal(options),
    });
    const sequences = items.map((item) => verifyQueueItem(options, item.body));
    if (items.length !== requested) {
      throw new Error(`Queue victim reserved ${items.length}/${requested} messages`);
    }
    log("queue_victim_reserved", { route, sequences });
    await rejectOnAbort(options.signal);
  }

  const seen = new Set<number>();
  let emptyPolls = 0;
  while (emptyPolls < 3) {
    const items = await client.queue.reserve(route, {
      leaseSeconds: 30,
      batchSize: 1_024,
      waitSeconds: 1,
      signal: operationSignal(options),
    });
    if (items.length === 0) {
      emptyPolls += 1;
      continue;
    }
    emptyPolls = 0;
    for (const item of items) {
      const sequence = verifyQueueItem(options, item.body);
      if (seen.has(sequence)) throw new Error(`Queue drainer received duplicate sequence ${sequence}`);
      seen.add(sequence);
      await item.complete({ signal: operationSignal(options) });
    }
    log("queue_drainer_progress", {
      workerId: options.workerId,
      completed: seen.size,
    });
  }
  log("queue_drainer_complete", {
    workerId: options.workerId,
    sequences: [...seen].sort((left, right) => left - right),
  });
}

export function queueRedeliveryRoute(namespace: string): string {
  return `queue://destroyer/${namespace}/redelivery`;
}

function verifyQueueItem(options: QueueRedeliveryOptions, body: Uint8Array): number {
  const prefix = new TextDecoder().decode(body.subarray(0, 20));
  const match = /^queue:0000:(\d{8}):$/u.exec(prefix);
  if (match?.[1] === undefined) throw new Error(`Queue body has invalid identity '${prefix}'`);
  const sequence = Number(match[1]);
  if (!Number.isSafeInteger(sequence) || sequence < 0 || sequence >= options.operations) {
    throw new Error(`Queue sequence ${sequence} is outside 0..${options.operations - 1}`);
  }
  assertBytesEqual(body, queuePayload(options, sequence), `Queue redelivery ${sequence}`);
  return sequence;
}

function queuePayload(
  options: Pick<QueueRedeliveryOptions, "seed" | "payloadBytes">,
  sequence: number,
): Uint8Array {
  return deterministicPayload(options, "queue", 0, sequence);
}

async function runConcurrent(
  count: number,
  concurrency: number,
  signal: AbortSignal,
  operation: (sequence: number) => Promise<void>,
): Promise<void> {
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < count) {
      signal.throwIfAborted();
      const sequence = next;
      next += 1;
      await operation(sequence);
    }
  };
  await Promise.all(Array.from({ length: Math.min(count, concurrency) }, worker));
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
