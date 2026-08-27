import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual } from "./model.js";

export function slowRecipientSaturationShape(
  requestedOperations: number,
  _requestedPayloadBytes: number,
): { operations: number; payloadBytes: number } {
  return {
    operations: Math.max(512, requestedOperations),
    // Keep the Notice TLV beneath the protocol envelope even if a generic suite
    // payload override is much larger; pressure comes from aggregate unread bytes.
    payloadBytes: 60_000,
  };
}

export async function runSlowRecipient(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  let delivered = 0;
  const subscription = await client.notice.subscribe(
    slowRecipientRoute(options.namespace),
    () => { delivered += 1; },
  );
  log("slow_recipient_ready", { route: slowRecipientRoute(options.namespace) });
  await waitForAbort(options.signal);
  await subscription.unsubscribe().catch(() => undefined);
  log("slow_recipient_complete", { delivered });
}

export async function runSlowRecipientObserver(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const seen = new Set<number>();
  let duplicates = 0;
  let invalid = 0;
  const complete = deferred<void>();
  const route = slowRecipientRoute(options.namespace);
  const subscription = await client.notice.subscribe(route, (message) => {
    try {
      if (message.route !== route) throw new Error(`unexpected route '${message.route}'`);
      const sequence = slowRecipientSequence(message.body);
      assertBytesEqual(
        message.body,
        slowRecipientPayload(sequence, options.payloadBytes),
        `slow-recipient notice ${sequence}`,
      );
      if (sequence < 0 || sequence >= options.operations) {
        throw new Error(`sequence ${sequence} outside 0..${options.operations - 1}`);
      }
      if (seen.has(sequence)) duplicates += 1;
      else seen.add(sequence);
      if (seen.size === options.operations) complete.resolve();
    } catch {
      invalid += 1;
    }
  });
  log("slow_recipient_observer_ready", { route, expected: options.operations });
  try {
    await Promise.race([complete.promise, rejectOnAbort(options.signal)]);
  } finally {
    await subscription.unsubscribe().catch(() => undefined);
  }
  if (seen.size !== options.operations || duplicates !== 0 || invalid !== 0) {
    throw new Error(
      `Healthy slow-recipient observer failed: received=${seen.size}/${options.operations}, duplicates=${duplicates}, invalid=${invalid}`,
    );
  }
  log("slow_recipient_observer_complete", {
    received: seen.size,
    duplicates,
    invalid,
  });
}

export async function runSlowRecipientPublisher(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  let next = 0;
  let published = 0;
  const lane = async (): Promise<void> => {
    while (next < options.operations) {
      options.signal.throwIfAborted();
      const sequence = next;
      next += 1;
      await client.notice.publish(slowRecipientRoute(options.namespace), {
        body: slowRecipientPayload(sequence, options.payloadBytes),
        signal: operationSignal(options),
      });
      published += 1;
    }
  };
  await Promise.all(Array.from({ length: Math.min(options.concurrency, options.operations) }, lane));
  log("slow_recipient_publisher_complete", {
    published,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function slowRecipientRoute(namespace: string): string {
  return `notice://destroyer/${namespace}/slow-recipient`;
}

function slowRecipientPayload(sequence: number, payloadBytes: number): Uint8Array {
  const prefix = new TextEncoder().encode(`slow-recipient:${sequence.toString().padStart(12, "0")}:`);
  const payload = new Uint8Array(Math.max(payloadBytes, prefix.length));
  payload.set(prefix);
  for (let index = prefix.length; index < payload.length; index += 1) {
    payload[index] = (sequence * 31 + index) & 0xff;
  }
  return payload;
}

function slowRecipientSequence(payload: Uint8Array): number {
  const prefixLength = "slow-recipient:".length;
  const value = new TextDecoder().decode(payload.subarray(prefixLength, prefixLength + 12));
  const sequence = Number(value);
  if (!Number.isSafeInteger(sequence) || sequence < 0) {
    throw new Error(`invalid slow-recipient sequence '${value}'`);
  }
  return sequence;
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
