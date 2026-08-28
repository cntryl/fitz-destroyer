import { createClient, type Client } from "@cntryl/fitz";
import { createDestroyerToken } from "../auth-token.js";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { scheduleCronAt } from "./schedule-delivery.js";

type StormScale = "smoke" | "standard" | "large";

export type ScheduleDueStormOptions = LiveCommonOptions & {
  fireAtMs: number;
  observationMs: number;
  url: string;
};

export type ScheduleDueStormEvidence = {
  definitionsCreated: number;
  definitionsCancelled: number;
  remainingDefinitions: number;
  deliveries: number;
  duplicates: number;
  invalidDeliveries: number;
  maxLatenessMs: number;
  canariesAttempted: number;
  canariesCompleted: number;
  canaryErrors: number;
  firstCanaryError?: string;
  longestCanaryMs: number;
  readinessChecks: number;
  readinessFailures: number;
  postStormCanaries: number;
  requestTimeoutMs: number;
};

export function scheduleDueStormDefinitionCount(scale: StormScale, configured: number): number {
  const minimum = scale === "smoke" ? 512 : scale === "standard" ? 2_000 : 5_000;
  return Math.max(minimum, configured);
}

export function scheduleDueStormPermissions(namespace: string): readonly string[] {
  return [
    `schedule://destroyer/${namespace}/**#*`,
    "schedule://**#read",
  ];
}

export function assertScheduleDueStormEvidence(record: Readonly<Record<string, unknown>>): void {
  const created = numberField(record, "definitionsCreated");
  const cancelled = numberField(record, "definitionsCancelled");
  const remaining = optionalNumberField(record, "remainingDefinitions");
  const deliveries = numberField(record, "deliveries");
  const duplicates = numberField(record, "duplicates");
  const invalid = numberField(record, "invalidDeliveries");
  const attempted = numberField(record, "canariesAttempted");
  const completed = numberField(record, "canariesCompleted");
  const errors = numberField(record, "canaryErrors");
  const longest = numberField(record, "longestCanaryMs");
  const readinessChecks = numberField(record, "readinessChecks");
  const readinessFailures = numberField(record, "readinessFailures");
  const postStorm = numberField(record, "postStormCanaries");
  const timeout = numberField(record, "requestTimeoutMs");
  const maxLateness = numberField(record, "maxLatenessMs");

  if (created < 1) throw new Error("schedule due storm created no definitions");
  if (cancelled !== created) throw new Error(`definitions cancelled ${cancelled}/${created}`);
  if (remaining !== 0) throw new Error(`schedule due storm left ${remaining} definitions`);
  if (deliveries !== created) throw new Error(`deliveries ${deliveries}/${created}`);
  if (duplicates !== 0) throw new Error(`duplicate deliveries=${duplicates}`);
  if (invalid !== 0) throw new Error(`invalid deliveries=${invalid}`);
  if (attempted < 1) throw new Error("schedule due storm attempted no sibling canaries");
  if (completed !== attempted) throw new Error(`canaries completed ${completed}/${attempted}`);
  if (errors !== 0) throw new Error(`sibling canary errors=${errors}`);
  if (longest >= timeout) {
    throw new Error(`sibling canary latency ${longest}ms reached request timeout ${timeout}ms`);
  }
  if (maxLateness >= timeout) {
    throw new Error(`storm delivery lateness ${maxLateness}ms reached request timeout ${timeout}ms`);
  }
  if (readinessChecks < 1) throw new Error("schedule due storm performed no readiness checks");
  if (readinessFailures !== 0) throw new Error(`readiness failures=${readinessFailures}`);
  if (postStorm !== 1) throw new Error(`post-storm canaries=${postStorm}`);
}

export async function runScheduleDueStormIsolation(
  client: Client,
  options: ScheduleDueStormOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const permissions = scheduleDueStormPermissions(options.namespace);
  const sibling = createClient({
    url: options.url,
    transport: "ws",
    tokenProvider: async () => createDestroyerToken("identity-b", permissions),
    timeout: options.requestTimeoutMs,
    reconnect: { enabled: false },
    retry: { enabled: false },
    heartbeat: { enabled: false },
    maxInFlightRequests: 1_024,
    maxRequestQueueSize: 16_384,
  });
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const seen = new Set<number>();
  let definitionsCreated = 0;
  let definitionsCancelled = 0;
  let duplicates = 0;
  let invalidDeliveries = 0;
  let maxLatenessMs = 0;
  let resolveDeliveries: (() => void) | undefined;
  const deliveriesComplete = new Promise<void>((resolve) => {
    resolveDeliveries = resolve;
  });
  const pattern = `schedule://destroyer/${options.namespace}/storm/*`;
  const subscription = await client.schedule.subscribe(pattern, (notification) => {
    const sequence = parseStormSequence(options.namespace, notification.route);
    const expected = String(sequence);
    const latenessMs = Date.now() - options.fireAtMs;
    if (sequence < 0 || sequence >= options.operations || decoder.decode(notification.payload) !== expected || latenessMs < 0) {
      invalidDeliveries += 1;
      return;
    }
    maxLatenessMs = Math.max(maxLatenessMs, latenessMs);
    if (seen.has(sequence)) duplicates += 1;
    else seen.add(sequence);
    if (seen.size === options.operations) resolveDeliveries?.();
  });

  try {
    const cron = scheduleCronAt(options.fireAtMs);
    await runConcurrent(options.operations, options.concurrency, options.signal, async (sequence) => {
      await client.schedule.create(stormRoute(options.namespace, sequence), {
        cron,
        deliveryMode: "Broadcast",
        payload: encoder.encode(String(sequence)),
        signal: operationSignal(options),
      });
      definitionsCreated += 1;
    });
    if (definitionsCreated !== options.operations) {
      throw new Error(`definitions created ${definitionsCreated}/${options.operations}`);
    }
    if (options.fireAtMs <= Date.now()) {
      throw new Error("schedule due storm definitions were not armed before their due minute");
    }
    log("schedule_due_storm_armed", {
      definitionsCreated,
      fireAtMs: options.fireAtMs,
      fireAt: new Date(options.fireAtMs).toISOString(),
    });

    await sleepUntil(Math.max(Date.now(), options.fireAtMs - 1_000), options.signal);
    await sibling.connectWhenReady({ timeoutMs: options.requestTimeoutMs, signal: options.signal });
    await sleepUntil(Math.max(Date.now(), options.fireAtMs - 250), options.signal);
    const observationEndsAt = options.fireAtMs + options.observationMs;
    const [canaries, readiness] = await Promise.all([
      runSiblingCanaries(sibling, options, observationEndsAt),
      monitorReadiness(observationEndsAt, options.requestTimeoutMs, options.signal),
      withDeadline(deliveriesComplete, options.fireAtMs + options.requestTimeoutMs, "storm deliveries"),
    ]);
    log("schedule_due_storm_window_complete", {
      deliveries: seen.size,
      duplicates,
      invalidDeliveries,
      maxLatenessMs,
      ...canaries,
      ...readiness,
    });
    const postStormCanaries = await runPostStormCanary(sibling, options);

    definitionsCancelled = await cancelStormDefinitions(client, options);
    const remainingDefinitions = await countDefinitions(
      client,
      `schedule://destroyer/${options.namespace}/storm`,
      operationSignal(options),
    );
    const evidence: ScheduleDueStormEvidence = {
      definitionsCreated,
      definitionsCancelled,
      remainingDefinitions,
      deliveries: seen.size,
      duplicates,
      invalidDeliveries,
      maxLatenessMs,
      ...canaries,
      ...readiness,
      postStormCanaries,
      requestTimeoutMs: options.requestTimeoutMs,
    };
    assertScheduleDueStormEvidence(evidence);
    log("schedule_due_storm_worker_complete", {
      ...evidence,
      elapsedMs: Math.round(performance.now() - startedAt),
    });
  } finally {
    if (definitionsCancelled < definitionsCreated) {
      await cancelStormDefinitions(client, options).catch(() => undefined);
    }
    await sibling.schedule.cancel(canaryRoute(options.namespace), {
      signal: AbortSignal.timeout(options.requestTimeoutMs),
    }).catch(() => undefined);
    await subscription.unsubscribe().catch(() => undefined);
    await sibling.close().catch(() => undefined);
  }
}

async function runSiblingCanaries(
  client: Client,
  options: ScheduleDueStormOptions,
  endsAt: number,
): Promise<Pick<ScheduleDueStormEvidence, "canariesAttempted" | "canariesCompleted" | "canaryErrors" | "longestCanaryMs" | "firstCanaryError">> {
  let canariesAttempted = 0;
  let canariesCompleted = 0;
  let canaryErrors = 0;
  let longestCanaryMs = 0;
  let firstCanaryError: string | undefined;
  const route = canaryRoute(options.namespace);
  const cron = scheduleCronAt(options.fireAtMs + 60_000);
  while (Date.now() < endsAt) {
    const canaryStarted = performance.now();
    canariesAttempted += 1;
    try {
      await client.schedule.create(route, {
        cron,
        deliveryMode: "Single",
        payload: new Uint8Array([canariesAttempted & 0xff]),
        signal: operationSignal(options),
      });
      await client.schedule.cancel(route, { signal: operationSignal(options) });
      canariesCompleted += 1;
    } catch (error) {
      canaryErrors += 1;
      firstCanaryError ??= errorMessage(error);
    }
    longestCanaryMs = Math.max(longestCanaryMs, Math.round(performance.now() - canaryStarted));
    await sleep(25, options.signal);
  }
  return {
    canariesAttempted,
    canariesCompleted,
    canaryErrors,
    longestCanaryMs,
    ...(firstCanaryError === undefined ? {} : { firstCanaryError }),
  };
}

async function runPostStormCanary(client: Client, options: ScheduleDueStormOptions): Promise<number> {
  const route = canaryRoute(options.namespace);
  const cron = scheduleCronAt(options.fireAtMs + 60_000);
  await client.schedule.create(route, {
    cron,
    deliveryMode: "Single",
    payload: new Uint8Array([1]),
    signal: operationSignal(options),
  });
  await client.schedule.cancel(route, { signal: operationSignal(options) });
  return 1;
}

async function monitorReadiness(
  endsAt: number,
  requestTimeoutMs: number,
  signal: AbortSignal,
): Promise<Pick<ScheduleDueStormEvidence, "readinessChecks" | "readinessFailures">> {
  let readinessChecks = 0;
  let readinessFailures = 0;
  while (Date.now() < endsAt) {
    readinessChecks += 1;
    try {
      const response = await fetch("http://fitz:4090/readyz", {
        signal: AbortSignal.any([signal, AbortSignal.timeout(Math.min(1_000, requestTimeoutMs))]),
      });
      if (response.status !== 200) readinessFailures += 1;
    } catch {
      readinessFailures += 1;
    }
    await sleep(100, signal);
  }
  return { readinessChecks, readinessFailures };
}

async function cancelStormDefinitions(client: Client, options: ScheduleDueStormOptions): Promise<number> {
  let cancelled = 0;
  await runConcurrent(options.operations, options.concurrency, options.signal, async (sequence) => {
    try {
      await client.schedule.cancel(stormRoute(options.namespace, sequence), {
        signal: operationSignal(options),
      });
      cancelled += 1;
    } catch {
      // The exact count below preserves incomplete cleanup as evidence.
    }
  });
  return cancelled;
}

async function countDefinitions(client: Client, selector: string, signal: AbortSignal): Promise<number> {
  let count = 0;
  for await (const page of client.schedule.entries(selector, { pageSize: 250n, signal })) {
    count += page.length;
  }
  return count;
}

function stormRoute(namespace: string, sequence: number): string {
  return `schedule://destroyer/${namespace}/storm/job-${sequence.toString().padStart(8, "0")}`;
}

function canaryRoute(namespace: string): string {
  return `schedule://destroyer/${namespace}/canary/job`;
}

function parseStormSequence(namespace: string, route: string): number {
  const match = new RegExp(`^schedule://destroyer/${escapeRegex(namespace)}/storm/job-(\\d{8})$`, "u").exec(route);
  return match?.[1] === undefined ? -1 : Number(match[1]);
}

function operationSignal(options: Pick<ScheduleDueStormOptions, "requestTimeoutMs" | "signal">): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
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

async function withDeadline(completion: Promise<void>, deadlineMs: number, label: string): Promise<void> {
  const remainingMs = Math.max(1, deadlineMs - Date.now());
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      completion,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} did not complete within ${remainingMs}ms`)),
          remainingMs,
        );
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function sleepUntil(timestampMs: number, signal: AbortSignal): Promise<void> {
  await sleep(Math.max(0, timestampMs - Date.now()), signal);
}

function sleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function numberField(record: Readonly<Record<string, unknown>>, field: keyof ScheduleDueStormEvidence): number {
  const value = record[field];
  if (typeof value !== "number" || !Number.isFinite(value)) throw new Error(`${field} is unavailable`);
  return value;
}

function optionalNumberField(record: Readonly<Record<string, unknown>>, field: keyof ScheduleDueStormEvidence): number {
  return record[field] === undefined ? 0 : numberField(record, field);
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
