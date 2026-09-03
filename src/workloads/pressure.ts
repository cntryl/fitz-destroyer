import type { Client } from "@cntryl/fitz";
import type { LiveLog } from "./live.js";
import type { Domain } from "./model.js";

const STREAM_SESSION_ALREADY_ACTIVE = 2_002;
const QUEUE_FULL = 4_005;
const NOTICE_PRESSURE_DELAY_MS = 100;
const LIVE_PRESSURE_DELAY_MS = 250;
const DURABLE_PRESSURE_DELAY_MS = 1_000;
const RECONCILIATION_BATCH_SIZE = 32;
const QUEUE_BACKPRESSURE_INITIAL_DELAY_MS = 25;
const QUEUE_BACKPRESSURE_MAX_DELAY_MS = 250;

export function pressureValue(
  namespace: string,
  worker: string,
  domain: Domain,
  counter: number,
): Uint8Array {
  return new TextEncoder().encode(
    `${namespace}:${worker}:${domain}:${counter.toString().padStart(12, "0")}`,
  );
}

export function pressureKvWrite(
  namespace: string,
  worker: string,
  counter: number,
): { key: Uint8Array; value: Uint8Array } {
  return {
    key: pressureValue(namespace, worker, "kv", 0),
    value: pressureValue(namespace, worker, "kv", counter + 1),
  };
}

export function pressureStreamWrite(
  namespace: string,
  worker: string,
  expectedOffset: bigint,
): { route: string; expectedOffset: bigint } {
  return {
    route: `stream://destroyer/${namespace}/${worker}-stream`,
    expectedOffset,
  };
}

export function nextPressureStreamOffset(latestOffset: bigint | undefined): bigint {
  return latestOffset === undefined ? 0n : latestOffset + 1n;
}

export function isPressureStreamCleanupPending(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "domainCode" in error &&
    error.domainCode === STREAM_SESSION_ALREADY_ACTIVE
  );
}

export async function replaceAndReconcilePressureStreamClient<T extends { close(): Promise<void> }>(
  current: T,
  connect: () => Promise<T>,
  peekOffset: (client: T) => Promise<bigint | undefined>,
): Promise<{ client: T; nextOffset: bigint }> {
  await current.close().catch(() => undefined);
  const client = await connect();
  try {
    return { client, nextOffset: nextPressureStreamOffset(await peekOffset(client)) };
  } catch (error) {
    await client.close().catch(() => undefined);
    throw error;
  }
}

export function pressureScheduleRoute(namespace: string, worker: string): string {
  return `schedule://destroyer/${namespace}/${worker}/job`;
}

export function pressureLoopDelayMs(domain: Domain): number {
  if (domain === "notice") return NOTICE_PRESSURE_DELAY_MS;
  if (domain === "lease" || domain === "rpc") return LIVE_PRESSURE_DELAY_MS;
  return DURABLE_PRESSURE_DELAY_MS;
}

export function isPressureQueueBackpressure(error: unknown): boolean {
  return typeof error === "object" &&
    error !== null &&
    "domainCode" in error &&
    error.domainCode === QUEUE_FULL;
}

export async function retryPressureQueueBackpressure<T>(
  operation: () => Promise<T>,
  onRetry: (attempt: number, delayMs: number, error: unknown) => void,
  wait: (delayMs: number) => Promise<void> = pressureDelay,
): Promise<T> {
  let retries = 0;
  while (true) {
    try {
      return await operation();
    } catch (error) {
      if (!isPressureQueueBackpressure(error)) throw error;
      retries += 1;
      const delayMs = Math.min(
        QUEUE_BACKPRESSURE_INITIAL_DELAY_MS * (2 ** (retries - 1)),
        QUEUE_BACKPRESSURE_MAX_DELAY_MS,
      );
      onRetry(retries, delayMs, error);
      await wait(delayMs);
    }
  }
}

export type PressureReconcileOptions = {
  namespace: string;
  workers: readonly string[];
  requestTimeoutMs: number;
  signal: AbortSignal;
};

export async function runPressureQueueReconciler(
  client: Client,
  options: PressureReconcileOptions,
  log: LiveLog,
): Promise<void> {
  const observed: Record<string, number[]> = {};
  for (const worker of options.workers) {
    const route = `queue://destroyer/${options.namespace}/${worker}`;
    const sequences: number[] = [];
    const seen = new Set<number>();
    let emptyPolls = 0;
    while (emptyPolls < 3) {
      const items = await client.queue.reserve(route, {
        leaseSeconds: 30,
        batchSize: RECONCILIATION_BATCH_SIZE,
        waitSeconds: 1,
        signal: operationSignal(options),
      });
      if (items.length === 0) {
        emptyPolls += 1;
        continue;
      }
      emptyPolls = 0;
      for (const item of items) {
        const sequence = decodePressureQueueSequence(options.namespace, worker, item.body);
        if (seen.has(sequence)) {
          throw new Error(`${worker} queue reconciler observed duplicate sequence ${sequence}`);
        }
        seen.add(sequence);
        sequences.push(sequence);
        await item.complete({ signal: operationSignal(options) });
      }
    }
    observed[worker] = sequences.sort((left, right) => left - right);
  }
  log("pressure_queue_reconcile_complete", { observed });
}

export function decodePressureQueueSequence(
  namespace: string,
  worker: string,
  body: Uint8Array,
): number {
  const value = new TextDecoder().decode(body);
  const prefix = `${namespace}:${worker}:queue:`;
  if (!value.startsWith(prefix)) {
    throw new Error(`${worker} queue payload '${value.slice(0, 160)}' has an unexpected identity`);
  }
  const sequence = Number(value.slice(prefix.length));
  if (!Number.isSafeInteger(sequence) || sequence < 0) {
    throw new Error(`${worker} queue payload has invalid sequence '${value.slice(prefix.length)}'`);
  }
  return sequence;
}

function operationSignal(options: PressureReconcileOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function pressureDelay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
