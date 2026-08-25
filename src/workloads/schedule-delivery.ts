import type { Client, ScheduleDeliveryMode, ScheduleEntry } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual, deterministicPayload } from "./model.js";

export const SCHEDULE_DELIVERY_TOLERANCE_MS = 1_000;

export type ScheduleDeliveryKind = "broadcast" | "single" | "cancelled";
export type ScheduleProducerAction = "create" | "cancel";

export type ScheduleDeliveryOptions = LiveCommonOptions & {
  seed: number;
  fireAtMs: number;
  handlerBacklog: () => { active: number; queued: number };
};

export type ScheduleProducerOptions = ScheduleDeliveryOptions & {
  action: ScheduleProducerAction;
};

const DELIVERY_KINDS = ["broadcast", "single", "cancelled"] as const;

export async function runScheduleProducer(
  client: Client,
  options: ScheduleProducerOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const cron = scheduleCronAt(options.fireAtMs);

  if (options.action === "create") {
    let created = 0;
    await runConcurrent(
      options.operations,
      options.concurrency,
      options.signal,
      async (sequence) => {
        await Promise.all(
          DELIVERY_KINDS.map(async (kind) => {
            await client.schedule.create(scheduleDeliveryRoute(options.namespace, kind, sequence), {
              cron,
              deliveryMode: deliveryMode(kind),
              payload: scheduleDeliveryPayload(options, kind, sequence),
              signal: operationSignal(options),
            });
            created += 1;
          }),
        );
      },
    );

    let cancelled = 0;
    await runConcurrent(
      options.operations,
      options.concurrency,
      options.signal,
      async (sequence) => {
        await client.schedule.cancel(
          scheduleDeliveryRoute(options.namespace, "cancelled", sequence),
          { signal: operationSignal(options) },
        );
        cancelled += 1;
      },
    );
    const listed = await verifyDefinitions(client, options, cron, false);
    log("schedule_producer_complete", {
      action: options.action,
      created,
      cancelled,
      listed,
      cron,
      fireAtMs: options.fireAtMs,
      elapsedMs: elapsedMs(startedAt),
    });
    return;
  }

  let cancelled = 0;
  await runConcurrent(
    options.operations,
    options.concurrency,
    options.signal,
    async (sequence) => {
      await Promise.all(
        (["broadcast", "single"] as const).map(async (kind) => {
          await client.schedule.cancel(scheduleDeliveryRoute(options.namespace, kind, sequence), {
            signal: operationSignal(options),
          });
          cancelled += 1;
        }),
      );
    },
  );
  const listed = await verifyDefinitions(client, options, cron, true);
  log("schedule_producer_complete", {
    action: options.action,
    created: 0,
    cancelled,
    listed,
    cron,
    fireAtMs: options.fireAtMs,
    elapsedMs: elapsedMs(startedAt),
  });
}

export async function runScheduleSubscriber(
  client: Client,
  options: ScheduleDeliveryOptions,
  log: LiveLog,
): Promise<void> {
  const pattern = `schedule://destroyer/${options.namespace}/*/*`;
  const seen = new Set<string>();
  const counts: Record<ScheduleDeliveryKind, number> = {
    broadcast: 0,
    single: 0,
    cancelled: 0,
  };
  let duplicates = 0;
  let invalid = 0;
  let activeHandlers = 0;
  let maxActiveHandlers = 0;
  let maxLatenessMs = 0;
  const startedAt = performance.now();

  const subscription = await client.schedule.subscribe(pattern, async (notification) => {
    const receivedAtMs = Date.now();
    activeHandlers += 1;
    maxActiveHandlers = Math.max(maxActiveHandlers, activeHandlers);
    try {
      const identity = parseScheduleRoute(options.namespace, notification.route);
      if (identity.sequence >= options.operations) {
        throw new Error(
          `schedule sequence ${identity.sequence} is outside 0..${options.operations - 1}`,
        );
      }
      assertBytesEqual(
        notification.payload,
        scheduleDeliveryPayload(options, identity.kind, identity.sequence),
        notification.route,
      );
      const latenessMs = receivedAtMs - options.fireAtMs;
      if (latenessMs < 0 || latenessMs > SCHEDULE_DELIVERY_TOLERANCE_MS) {
        throw new Error(
          `${notification.route} arrived ${latenessMs}ms from its scheduled time`,
        );
      }
      maxLatenessMs = Math.max(maxLatenessMs, latenessMs);
      const key = `${identity.kind}:${identity.sequence}`;
      if (seen.has(key)) {
        duplicates += 1;
      } else {
        seen.add(key);
        counts[identity.kind] += 1;
      }
      log("schedule_notification_received", {
        workerId: options.workerId,
        route: notification.route,
        kind: identity.kind,
        sequence: identity.sequence,
        fireAtMs: options.fireAtMs,
        receivedAtMs,
        latenessMs,
      });
      if (options.handlerDelayMs > 0) await sleep(options.handlerDelayMs);
    } catch (error) {
      invalid += 1;
      log("schedule_notification_invalid", {
        workerId: options.workerId,
        route: notification.route,
        receivedAtMs,
        error: errorMessage(error),
      });
    } finally {
      activeHandlers -= 1;
    }
  });

  log("schedule_subscriber_ready", {
    workerId: options.workerId,
    pattern,
    fireAtMs: options.fireAtMs,
    expectedBroadcast: options.operations,
  });

  await waitForAbort(options.signal);
  await subscription.unsubscribe().catch(() => undefined);
  await waitForHandlers(
    () => activeHandlers,
    options.handlerBacklog,
    options.requestTimeoutMs,
  );
  const handlerBacklog = options.handlerBacklog();
  log("schedule_subscriber_complete", {
    workerId: options.workerId,
    broadcast: counts.broadcast,
    single: counts.single,
    cancelled: counts.cancelled,
    duplicates,
    invalid,
    maxActiveHandlers,
    clientHandlersActive: handlerBacklog.active,
    clientHandlersQueued: handlerBacklog.queued,
    maxLatenessMs,
    elapsedMs: elapsedMs(startedAt),
  });
}

export function scheduleCronAt(fireAtMs: number): string {
  if (!Number.isSafeInteger(fireAtMs) || fireAtMs <= 0 || fireAtMs % 60_000 !== 0) {
    throw new Error("Schedule fire time must be a positive whole UTC minute");
  }
  const date = new Date(fireAtMs);
  return `${date.getUTCMinutes()} ${date.getUTCHours()} ${date.getUTCDate()} ${date.getUTCMonth() + 1} *`;
}

export function scheduleDeliveryRoute(
  namespace: string,
  kind: ScheduleDeliveryKind,
  sequence: number,
): string {
  return `schedule://destroyer/${namespace}/${kind}/job-${sequence.toString().padStart(8, "0")}`;
}

export function scheduleDeliveryPayload(
  options: Pick<ScheduleDeliveryOptions, "seed" | "payloadBytes">,
  kind: ScheduleDeliveryKind,
  sequence: number,
): Uint8Array {
  return deterministicPayload(
    options,
    "schedule",
    DELIVERY_KINDS.indexOf(kind),
    sequence,
  );
}

async function verifyDefinitions(
  client: Client,
  options: ScheduleDeliveryOptions,
  cron: string,
  expectEmpty: boolean,
): Promise<number> {
  const entries: ScheduleEntry[] = [];
  for await (const page of client.schedule.entries(
    `schedule://destroyer/${options.namespace}/*`,
    { pageSize: 250n, signal: options.signal },
  )) {
    entries.push(...page);
  }
  const expectedCount = expectEmpty ? 0 : options.operations * 2;
  if (entries.length !== expectedCount) {
    throw new Error(`schedule definition count ${entries.length} != ${expectedCount}`);
  }
  if (expectEmpty) return entries.length;

  const byRoute = new Map(entries.map((entry) => [entry.route, entry]));
  for (let sequence = 0; sequence < options.operations; sequence += 1) {
    for (const kind of ["broadcast", "single"] as const) {
      const route = scheduleDeliveryRoute(options.namespace, kind, sequence);
      const entry = byRoute.get(route);
      if (entry === undefined) throw new Error(`${route} is missing from Schedule listing`);
      if (entry.cron !== cron || entry.deliveryMode !== deliveryMode(kind)) {
        throw new Error(`${route} Schedule metadata does not match its create request`);
      }
      assertBytesEqual(
        entry.payload,
        scheduleDeliveryPayload(options, kind, sequence),
        `${route} listed payload`,
      );
    }
  }
  return entries.length;
}

function parseScheduleRoute(
  namespace: string,
  route: string,
): { kind: ScheduleDeliveryKind; sequence: number } {
  const prefix = `schedule://destroyer/${namespace}/`;
  if (!route.startsWith(prefix)) throw new Error(`unexpected Schedule route '${route}'`);
  const [kind, operation, extra] = route.slice(prefix.length).split("/");
  if (!isDeliveryKind(kind) || operation === undefined || extra !== undefined) {
    throw new Error(`unexpected Schedule route '${route}'`);
  }
  const match = /^job-(\d{8})$/u.exec(operation);
  if (match?.[1] === undefined) throw new Error(`unexpected Schedule operation '${operation}'`);
  return { kind, sequence: Number(match[1]) };
}

function deliveryMode(kind: ScheduleDeliveryKind): ScheduleDeliveryMode {
  return kind === "broadcast" ? "Broadcast" : "Single";
}

function isDeliveryKind(value: string | undefined): value is ScheduleDeliveryKind {
  return value !== undefined && (DELIVERY_KINDS as readonly string[]).includes(value);
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

function waitForAbort(signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    signal.addEventListener("abort", () => resolve(), { once: true });
  });
}

async function waitForHandlers(
  active: () => number,
  backlog: () => { active: number; queued: number },
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const pending = backlog();
    if (active() === 0 && pending.active === 0 && pending.queued === 0) return;
    await sleep(10);
  }
  const pending = backlog();
  throw new Error(
    `Schedule handlers did not drain: workload=${active()}, client active=${pending.active}, client queued=${pending.queued}`,
  );
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
