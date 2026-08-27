import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { ALL_DOMAINS, assertBytesEqual, type Domain } from "./model.js";

export type CanaryOptions = LiveCommonOptions & {
  domains: readonly Domain[];
};

export async function runCanary(
  client: Client,
  options: CanaryOptions,
  log: LiveLog,
): Promise<void> {
  const maximums = Object.fromEntries(options.domains.map((domain) => [domain, 0])) as Partial<
    Record<Domain, number>
  >;
  for (let sequence = 0; sequence < options.operations; sequence += 1) {
    for (const domain of options.domains) {
      const startedAt = performance.now();
      await runCanaryOperation(client, options, domain, sequence);
      const elapsedMs = Math.round(performance.now() - startedAt);
      maximums[domain] = Math.max(maximums[domain] ?? 0, elapsedMs);
      log("canary_operation_complete", { domain, sequence, elapsedMs });
    }
  }
  log("canary_complete", {
    domains: options.domains,
    operationsPerDomain: options.operations,
    maximumMs: maximums,
  });
}

export function canaryRoute(domain: Domain, namespace: string, sequence = 0): string {
  if (domain === "schedule") {
    return `schedule://destroyer/${namespace}/canary/job-${sequence}`;
  }
  const suffix = domain === "stream" ? `-${sequence}` : "";
  return `${domain}://destroyer/${namespace}/canary${suffix}`;
}

export async function runCanaryOperation(
  client: Client,
  options: CanaryOptions,
  domain: Domain,
  sequence: number,
  routeOverride?: string,
): Promise<void> {
  const signal = operationSignal(options);
  const route = routeOverride ?? canaryRoute(domain, options.namespace, sequence);
  const payload = canaryPayload(domain, sequence, options.payloadBytes);
  if (domain === "queue") {
    await client.queue.enqueue(route, { body: payload, signal });
    const items = await client.queue.reserve(route, {
      leaseSeconds: 30,
      batchSize: 1,
      waitSeconds: 5,
      signal,
    });
    const item = items[0];
    if (item === undefined) throw new Error(`Queue canary ${sequence} did not reserve its message`);
    assertBytesEqual(item.body, payload, `Queue canary ${sequence}`);
    await item.complete({ signal });
  } else if (domain === "kv") {
    const transaction = await client.kv.begin(route, { durability: "Sync", signal });
    try {
      await transaction.put({ key: payload.subarray(0, 16), value: payload, signal });
      await transaction.commit({ signal });
    } catch (error) {
      await transaction.rollback().catch(() => undefined);
      throw error;
    }
  } else if (domain === "stream") {
    const session = await client.stream.begin(route, { signal });
    try {
      await session.append({ expectedOffset: 0n, body: payload, signal });
      await session.commit({ mode: "Sync", signal });
    } catch (error) {
      await session.rollback().catch(() => undefined);
      throw error;
    }
  } else if (domain === "schedule") {
    await client.schedule.create(route, {
      cron: "0 0 1 1 *",
      deliveryMode: "Single",
      payload,
      signal,
    });
    await client.schedule.cancel(route, { signal });
  } else if (domain === "notice") {
    let resolveDelivery: (body: Uint8Array) => void = () => undefined;
    const delivered = new Promise<Uint8Array>((resolve) => {
      resolveDelivery = resolve;
    });
    const subscription = await client.notice.subscribe(
      route,
      (message) => resolveDelivery(message.body),
      { signal },
    );
    try {
      await client.notice.publish(route, { body: payload, signal });
      const body = await Promise.race([delivered, rejectOnAbort(signal)]);
      assertBytesEqual(body, payload, `Notice canary ${sequence}`);
    } finally {
      await subscription.unsubscribe().catch(() => undefined);
    }
  } else if (domain === "lease") {
    const lease = await client.lease.acquire(route, {
      ttlSeconds: 5,
      waitSeconds: 5,
      signal,
    });
    await lease.release({ signal });
  } else {
    const worker = await client.rpc.registerWorker(route, async (request, writer) => {
      await writer.end({ body: request.body });
    });
    try {
      let responses = 0;
      for await (const response of client.rpc.call(route, {
        body: payload,
        timeoutMs: options.requestTimeoutMs,
        signal,
      })) {
        assertBytesEqual(response.body, payload, `RPC canary ${sequence}`);
        responses += 1;
      }
      if (responses !== 1) throw new Error(`RPC canary received ${responses}/1 responses`);
    } finally {
      await worker.unsubscribe().catch(() => undefined);
    }
  }
}

function canaryPayload(domain: Domain, sequence: number, size: number): Uint8Array {
  const prefix = new TextEncoder().encode(`canary:${domain}:${sequence}:`);
  const payload = new Uint8Array(size);
  for (let index = 0; index < size; index += 1) {
    payload[index] = index < prefix.length ? (prefix[index] ?? 0) : (sequence + index) & 0xff;
  }
  return payload;
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

export function allCanaryDomains(): readonly Domain[] {
  return ALL_DOMAINS;
}
