import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual } from "./model.js";

const UNDELIVERABLE_BODY_BYTES = 65_536;

export async function runQueueDeadLetterFencing(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const route = queueRoute(options.namespace);
  let oversizedRejected = 0;
  try {
    await client.queue.enqueue(route, { body: new Uint8Array(UNDELIVERABLE_BODY_BYTES) });
  } catch {
    oversizedRejected = 1;
  }
  if (oversizedRejected !== 1) throw new Error("Queue accepted a body that cannot fit a reserve response");

  const body = new TextEncoder().encode("stale-delivery-token");
  await client.queue.enqueue(route, { body });
  const first = (await client.queue.reserve(route, { leaseSeconds: 1, batchSize: 1 }))[0];
  if (first === undefined) throw new Error("Queue did not return the fencing probe");
  const second = await waitForRedelivery(client, route);
  assertBytesEqual(second.body, body, "Queue redelivery body");

  let staleCompletionRejected = 0;
  try {
    await first.complete();
  } catch {
    staleCompletionRejected = 1;
  }
  if (staleCompletionRejected !== 1) throw new Error("Queue accepted a stale delivery token");
  await second.complete();

  log("queue_dead_letter_fencing_worker_complete", {
    oversizedRejected,
    staleCompletionRejected,
    redelivered: 1,
    completed: 1,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function queueRoute(namespace: string): string {
  return `queue://${namespace}/fencing/dead-letter`;
}

async function waitForRedelivery(client: Client, route: string) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const item = (await client.queue.reserve(route, { leaseSeconds: 30, batchSize: 1 }))[0];
    if (item !== undefined) return item;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("Queue did not redeliver the expired fencing probe");
}
