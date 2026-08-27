import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export type QueueOverloadOptions = LiveCommonOptions & {
  action: "produce" | "drain";
  workers: readonly string[];
};

export async function runQueueOverload(
  client: Client,
  options: QueueOverloadOptions,
  log: LiveLog,
): Promise<void> {
  if (options.action === "drain") {
    await drain(client, options, log);
    return;
  }
  await produce(client, options, log);
}

async function produce(
  client: Client,
  options: QueueOverloadOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const acknowledged: number[] = [];
  const failed: number[] = [];
  let failureReported = false;
  log("queue_overload_dispatched", {
    workerId: options.workerId,
    operations: options.operations,
    concurrency: options.concurrency,
  });
  await runConcurrent(options, async (sequence) => {
    try {
      await client.queue.enqueue(route(options.namespace, options.workerId), {
        body: payload(options, sequence),
        signal: operationSignal(options),
      });
      acknowledged.push(sequence);
    } catch (error) {
      failed.push(sequence);
      if (!failureReported) {
        failureReported = true;
        log("queue_overload_failure_observed", {
          workerId: options.workerId,
          sequence,
          error: errorMessage(error),
        });
      }
    }
  });
  acknowledged.sort((left, right) => left - right);
  failed.sort((left, right) => left - right);
  log("queue_overload_producer_complete", {
    workerId: options.workerId,
    started: options.operations,
    acknowledged,
    failed,
    elapsedMs: elapsedMs(startedAt),
  });
}

async function drain(
  client: Client,
  options: QueueOverloadOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const observed: Record<string, number[]> = {};
  for (const worker of options.workers) {
    const seen = new Set<number>();
    let emptyPolls = 0;
    while (emptyPolls < 3) {
      const items = await client.queue.reserve(route(options.namespace, worker), {
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
        const sequence = sequenceFromPayload(options.namespace, worker, item.body);
        if (seen.has(sequence)) {
          throw new Error(`${worker} observed duplicate Queue sequence ${sequence}`);
        }
        seen.add(sequence);
        await item.complete({ signal: operationSignal(options) });
      }
    }
    observed[worker] = [...seen].sort((left, right) => left - right);
  }
  log("queue_overload_drain_complete", {
    observed,
    elapsedMs: elapsedMs(startedAt),
  });
}

function route(namespace: string, worker: string): string {
  return `queue://destroyer/${namespace}/${worker}`;
}

function payload(options: QueueOverloadOptions, sequence: number): Uint8Array {
  const header = `${options.namespace}:${options.workerId}:${sequence.toString().padStart(12, "0")}:`;
  return new TextEncoder().encode(header.padEnd(Math.max(header.length, options.payloadBytes), "x"));
}

function sequenceFromPayload(namespace: string, worker: string, body: Uint8Array): number {
  const value = new TextDecoder().decode(body);
  const prefix = `${namespace}:${worker}:`;
  if (!value.startsWith(prefix)) throw new Error(`${worker} Queue payload had the wrong identity`);
  const sequence = Number(value.slice(prefix.length, prefix.length + 12));
  if (!Number.isSafeInteger(sequence) || sequence < 0) {
    throw new Error(`${worker} Queue payload had an invalid sequence`);
  }
  return sequence;
}

async function runConcurrent(
  options: QueueOverloadOptions,
  operation: (sequence: number) => Promise<void>,
): Promise<void> {
  let next = 0;
  const lane = async (): Promise<void> => {
    while (next < options.operations) {
      options.signal.throwIfAborted();
      const sequence = next;
      next += 1;
      await operation(sequence);
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(options.operations, options.concurrency) }, lane),
  );
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function elapsedMs(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
