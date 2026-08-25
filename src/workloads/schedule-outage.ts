import type { Client, ScheduleEntry } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual, deterministicPayload } from "./model.js";
import { scheduleCronAt } from "./schedule-delivery.js";

export type ScheduleOutageAction = "create" | "race-cancel" | "cleanup" | "subscribe";
export type ScheduleOutageKind = "repeated" | "race";
export type ScheduleOutageOptions = LiveCommonOptions & {
  seed: number;
  action: ScheduleOutageAction;
  missedAtMs: number;
  raceAtMs: number;
  handlerBacklog: () => { active: number; queued: number };
};

export async function runScheduleOutage(
  client: Client,
  options: ScheduleOutageOptions,
  log: LiveLog,
): Promise<void> {
  if (options.action === "subscribe") {
    await subscribe(client, options, log);
    return;
  }
  if (options.action === "create") {
    let created = 0;
    for (let sequence = 0; sequence < options.operations; sequence += 1) {
      await client.schedule.create(route(options.namespace, "repeated", sequence), {
        cron: consecutiveMinuteCronAt(options.missedAtMs),
        deliveryMode: "Single",
        payload: payload(options, "repeated", sequence),
        signal: operationSignal(options),
      });
      await client.schedule.create(route(options.namespace, "race", sequence), {
        cron: scheduleCronAt(options.raceAtMs),
        deliveryMode: "Single",
        payload: payload(options, "race", sequence),
        signal: operationSignal(options),
      });
      created += 2;
    }
    log("schedule_outage_create_complete", { created });
    return;
  }
  if (options.action === "race-cancel") {
    await sleepUntil(options.raceAtMs);
    const acknowledged: number[] = [];
    const failed: number[] = [];
    for (let sequence = 0; sequence < options.operations; sequence += 1) {
      try {
        await client.schedule.cancel(route(options.namespace, "race", sequence), {
          signal: operationSignal(options),
        });
        acknowledged.push(sequence);
      } catch {
        failed.push(sequence);
      }
    }
    log("schedule_outage_race_cancel_complete", { acknowledged, failed });
    return;
  }

  const entries: ScheduleEntry[] = [];
  for await (const page of client.schedule.entries(`schedule://destroyer/${options.namespace}/*`, {
    pageSize: 250n,
    signal: operationSignal(options),
  })) {
    entries.push(...page);
  }
  let cancelled = 0;
  for (const entry of entries) {
    await client.schedule.cancel(entry.route, { signal: operationSignal(options) });
    cancelled += 1;
  }
  log("schedule_outage_cleanup_complete", { listed: entries.length, cancelled });
}

export function consecutiveMinuteCronAt(firstAtMs: number): string {
  const secondAtMs = firstAtMs + 60_000;
  const first = new Date(firstAtMs);
  const second = new Date(secondAtMs);
  if (
    !Number.isSafeInteger(firstAtMs) ||
    firstAtMs <= 0 ||
    firstAtMs % 60_000 !== 0 ||
    first.getUTCFullYear() !== second.getUTCFullYear() ||
    first.getUTCMonth() !== second.getUTCMonth() ||
    first.getUTCDate() !== second.getUTCDate() ||
    first.getUTCHours() !== second.getUTCHours()
  ) {
    throw new Error("Repeated Schedule occurrences must be consecutive minutes in one UTC hour");
  }
  return `${first.getUTCMinutes()},${second.getUTCMinutes()} ${first.getUTCHours()} ${first.getUTCDate()} ${first.getUTCMonth() + 1} *`;
}

export function scheduleOutageRoute(
  namespace: string,
  kind: ScheduleOutageKind,
  sequence: number,
): string {
  return route(namespace, kind, sequence);
}

async function subscribe(
  client: Client,
  options: ScheduleOutageOptions,
  log: LiveLog,
): Promise<void> {
  const seen = new Set<string>();
  const deliveries: Array<{
    kind: ScheduleOutageKind;
    sequence: number;
    receivedAtMs: number;
  }> = [];
  let duplicates = 0;
  let activeHandlers = 0;
  const subscription = await client.schedule.subscribe(
    `schedule://destroyer/${options.namespace}/*/*`,
    async (notification) => {
      activeHandlers += 1;
      try {
        const identity = parseRoute(options.namespace, notification.route);
        assertBytesEqual(
          notification.payload,
          payload(options, identity.kind, identity.sequence),
          notification.route,
        );
        const key = `${identity.kind}:${identity.sequence}`;
        if (seen.has(key)) duplicates += 1;
        else {
          seen.add(key);
          const receivedAtMs = Date.now();
          deliveries.push({ ...identity, receivedAtMs });
          log("schedule_outage_delivery", { ...identity, receivedAtMs });
        }
        if (options.handlerDelayMs > 0) await sleep(options.handlerDelayMs);
      } finally {
        activeHandlers -= 1;
      }
    },
  );
  log("schedule_outage_subscriber_ready", { workerId: options.workerId });
  await waitForAbort(options.signal);
  await subscription.unsubscribe().catch(() => undefined);
  const deadline = Date.now() + options.requestTimeoutMs;
  while (
    Date.now() < deadline &&
    (activeHandlers > 0 || options.handlerBacklog().active > 0 || options.handlerBacklog().queued > 0)
  ) {
    await sleep(10);
  }
  const backlog = options.handlerBacklog();
  if (activeHandlers > 0 || backlog.active > 0 || backlog.queued > 0) {
    throw new Error(`Schedule outage handlers did not drain: ${JSON.stringify(backlog)}`);
  }
  log("schedule_outage_subscriber_complete", { workerId: options.workerId, deliveries, duplicates });
}

function route(namespace: string, kind: ScheduleOutageKind, sequence: number): string {
  return `schedule://destroyer/${namespace}/${kind}/job-${sequence.toString().padStart(8, "0")}`;
}

function parseRoute(
  namespace: string,
  value: string,
): { kind: ScheduleOutageKind; sequence: number } {
  const match = new RegExp(
    `^schedule://destroyer/${escapeRegex(namespace)}/(repeated|race)/job-(\\d{8})$`,
    "u",
  ).exec(value);
  if (match?.[1] === undefined || match[2] === undefined) {
    throw new Error(`Unexpected Schedule outage route ${value}`);
  }
  return { kind: match[1] as ScheduleOutageKind, sequence: Number(match[2]) };
}

function payload(
  options: Pick<ScheduleOutageOptions, "seed" | "payloadBytes">,
  kind: ScheduleOutageKind,
  sequence: number,
): Uint8Array {
  return deterministicPayload(options, "schedule", kind === "repeated" ? 0 : 1, sequence);
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) resolve();
    else signal.addEventListener("abort", () => resolve(), { once: true });
  });
}

function sleepUntil(timestampMs: number): Promise<void> {
  return sleep(Math.max(0, timestampMs - Date.now()));
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}
