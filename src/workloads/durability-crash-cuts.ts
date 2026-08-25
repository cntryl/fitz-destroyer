import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import {
  DURABLE_DOMAINS,
  assertBytesEqual,
  deterministicPayload,
  kvKey,
  type DurableDomain,
} from "./model.js";

export type DurabilityAction = "baseline" | "cut" | "verify";

export type DurabilityCrashCutOptions = LiveCommonOptions & {
  seed: number;
  action: DurabilityAction;
};

export async function runDurabilityCrashCut(
  client: Client,
  options: DurabilityCrashCutOptions,
  log: LiveLog,
): Promise<void> {
  if (options.action === "verify") {
    await verifyDurabilityState(client, options, log);
    return;
  }

  const sequence = options.action === "baseline" ? 0 : 1;
  const operations = DURABLE_DOMAINS.map(async (domain) => {
    log("durability_operation_started", { action: options.action, domain, sequence });
    try {
      await writeDurableValue(client, options, domain, sequence);
      log("durability_operation_acknowledged", { action: options.action, domain, sequence });
      return { domain, outcome: "acknowledged" } as const;
    } catch (error) {
      log("durability_operation_failed", {
        action: options.action,
        domain,
        sequence,
        error: errorMessage(error),
      });
      return { domain, outcome: "failed" } as const;
    }
  });
  log("durability_operations_dispatched", {
    action: options.action,
    sequence,
    domains: DURABLE_DOMAINS,
  });
  const outcomes = await Promise.all(operations);
  log("durability_writer_complete", { action: options.action, sequence, outcomes });
  if (options.action === "baseline" && outcomes.some(({ outcome }) => outcome !== "acknowledged")) {
    throw new Error("One or more durability baselines were not acknowledged");
  }
}

export function crashCutRoute(domain: DurableDomain, namespace: string, sequence = 0): string {
  if (domain === "schedule") {
    return `schedule://destroyer/${namespace}/crash-cut/job-${sequence.toString().padStart(8, "0")}`;
  }
  return `${domain}://destroyer/${namespace}/crash-cut`;
}

async function writeDurableValue(
  client: Client,
  options: DurabilityCrashCutOptions,
  domain: DurableDomain,
  sequence: number,
): Promise<void> {
  const payload = crashCutPayload(options, domain, sequence);
  const signal = operationSignal(options);
  if (domain === "queue") {
    await client.queue.enqueue(crashCutRoute(domain, options.namespace), {
      body: payload,
      signal,
    });
  } else if (domain === "kv") {
    const transaction = await client.kv.begin(crashCutRoute(domain, options.namespace), {
      durability: "Sync",
      signal,
    });
    try {
      await transaction.put({ key: kvKey(sequence), value: payload, signal });
      await transaction.commit({ signal });
    } catch (error) {
      await transaction.rollback().catch(() => undefined);
      throw error;
    }
  } else if (domain === "stream") {
    const session = await client.stream.begin(crashCutRoute(domain, options.namespace), { signal });
    try {
      await session.append({ expectedOffset: BigInt(sequence), body: payload, signal });
      await session.commit({ mode: "Sync", signal });
    } catch (error) {
      await session.rollback().catch(() => undefined);
      throw error;
    }
  } else {
    await client.schedule.create(crashCutRoute(domain, options.namespace, sequence), {
      cron: "0 0 1 1 *",
      deliveryMode: "Single",
      payload,
      signal,
    });
  }
}

async function verifyDurabilityState(
  client: Client,
  options: DurabilityCrashCutOptions,
  log: LiveLog,
): Promise<void> {
  const observed: Record<DurableDomain, number[]> = {
    queue: [],
    kv: [],
    stream: [],
    schedule: [],
  };

  const queueRoute = crashCutRoute("queue", options.namespace);
  while (true) {
    const items = await client.queue.reserve(queueRoute, {
      leaseSeconds: 30,
      batchSize: 10,
      signal: operationSignal(options),
    });
    if (items.length === 0) break;
    for (const item of items) {
      const sequence = identifyPayload(options, "queue", item.body);
      observed.queue.push(sequence);
      await item.complete({ signal: operationSignal(options) });
    }
  }

  const kvTransaction = await client.kv.begin(crashCutRoute("kv", options.namespace), {
    mode: "ReadOnly",
    durability: "Sync",
    signal: operationSignal(options),
  });
  try {
    for (const sequence of [0, 1]) {
      const result = await kvTransaction.get({
        key: kvKey(sequence),
        signal: operationSignal(options),
      });
      if (result.type === "found") {
        assertBytesEqual(
          result.value,
          crashCutPayload(options, "kv", sequence),
          `KV crash-cut ${sequence}`,
        );
        observed.kv.push(sequence);
      }
    }
  } finally {
    await kvTransaction.rollback().catch(() => undefined);
  }

  for await (const batch of client.stream.read(crashCutRoute("stream", options.namespace), {
    fromOffset: 0n,
    mode: "replay",
    batchSize: 10,
    signal: operationSignal(options),
  })) {
    for (const record of batch.records) {
      const sequence = Number(record.offset);
      if (sequence !== 0 && sequence !== 1) {
        throw new Error(`Unexpected Stream crash-cut offset ${record.offset}`);
      }
      assertBytesEqual(
        record.body,
        crashCutPayload(options, "stream", sequence),
        `Stream crash-cut ${sequence}`,
      );
      observed.stream.push(sequence);
    }
  }

  for await (const page of client.schedule.entries(
    `schedule://destroyer/${options.namespace}/crash-cut`,
    { pageSize: 10n, signal: options.signal },
  )) {
    for (const entry of page) {
      const match = /\/job-(\d{8})$/u.exec(entry.route);
      const sequence = match?.[1] === undefined ? Number.NaN : Number(match[1]);
      if (sequence !== 0 && sequence !== 1) {
        throw new Error(`Unexpected Schedule crash-cut route ${entry.route}`);
      }
      assertBytesEqual(
        entry.payload,
        crashCutPayload(options, "schedule", sequence),
        `Schedule crash-cut ${sequence}`,
      );
      observed.schedule.push(sequence);
    }
  }

  for (const domain of DURABLE_DOMAINS) {
    observed[domain].sort((left, right) => left - right);
    for (const sequence of observed[domain]) {
      log("durability_operation_observed", { domain, sequence });
    }
  }
  log("durability_verify_complete", { observed });
}

function identifyPayload(
  options: DurabilityCrashCutOptions,
  domain: DurableDomain,
  actual: Uint8Array,
): number {
  for (const sequence of [0, 1]) {
    try {
      assertBytesEqual(actual, crashCutPayload(options, domain, sequence), `${domain} payload`);
      return sequence;
    } catch {
      // Try the other only valid sequence before reporting corruption.
    }
  }
  throw new Error(`${domain} crash-cut payload did not match a started operation`);
}

function crashCutPayload(
  options: Pick<DurabilityCrashCutOptions, "seed" | "payloadBytes">,
  domain: DurableDomain,
  sequence: number,
): Uint8Array {
  return deterministicPayload(options, domain, 0, sequence);
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs * 6)]);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
