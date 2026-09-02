import type { Client } from "@cntryl/fitz";
import {
  assertBytesEqual,
  deterministicPayload,
  kvKey,
  resourceRoute,
  scheduleRoute,
  scheduleSelector,
  totalDurableEntries,
  type WorkloadShape,
} from "./model.js";

const MUTATION_BATCH_SIZE = 250;
const READ_BATCH_SIZE = 250;

export async function loadRecoveryWorkload(
  client: Client,
  shape: WorkloadShape,
): Promise<number> {
  await Promise.all([
    runResourceOperationsSequentially(shape.resources, (resource) =>
      loadQueue(client, shape, resource),
    ),
    runResourceOperationsSequentially(shape.resources, (resource) =>
      loadKv(client, shape, resource),
    ),
    runResourceOperationsSequentially(shape.resources, (resource) =>
      loadStream(client, shape, resource),
    ),
    runResourceOperationsSequentially(shape.resources, (resource) =>
      loadSchedules(client, shape, resource),
    ),
  ]);
  return totalDurableEntries(shape);
}

export async function verifyRecoveryWorkload(
  client: Client,
  shape: WorkloadShape,
): Promise<number> {
  await Promise.all([
    runResourceOperationsSequentially(shape.resources, (resource) =>
      verifyQueue(client, shape, resource),
    ),
    runResourceOperationsSequentially(shape.resources, (resource) =>
      verifyKv(client, shape, resource),
    ),
    runResourceOperationsSequentially(shape.resources, (resource) =>
      verifyStream(client, shape, resource),
    ),
    verifySchedules(client, shape),
  ]);
  return totalDurableEntries(shape);
}

async function loadQueue(client: Client, shape: WorkloadShape, resource: number): Promise<void> {
  const route = resourceRoute("queue", shape, resource);
  for (let entry = 0; entry < shape.entriesPerResource; entry += 1) {
    await client.queue.enqueue(route, {
      body: deterministicPayload(shape, "queue", resource, entry),
    });
  }
}

async function loadKv(client: Client, shape: WorkloadShape, resource: number): Promise<void> {
  const route = resourceRoute("kv", shape, resource);
  for (let start = 0; start < shape.entriesPerResource; start += MUTATION_BATCH_SIZE) {
    const tx = await client.kv.begin(route, { durability: "Sync" });
    try {
      const end = Math.min(shape.entriesPerResource, start + MUTATION_BATCH_SIZE);
      for (let entry = start; entry < end; entry += 1) {
        await tx.put({
          key: kvKey(entry),
          value: deterministicPayload(shape, "kv", resource, entry),
        });
      }
      await tx.commit();
    } catch (error) {
      await tx.rollback().catch(() => undefined);
      throw error;
    }
  }
}

async function loadStream(client: Client, shape: WorkloadShape, resource: number): Promise<void> {
  const route = resourceRoute("stream", shape, resource);
  for (let start = 0; start < shape.entriesPerResource; start += MUTATION_BATCH_SIZE) {
    const session = await client.stream.begin(route);
    try {
      const end = Math.min(shape.entriesPerResource, start + MUTATION_BATCH_SIZE);
      for (let entry = start; entry < end; entry += 1) {
        await session.append({
          expectedOffset: BigInt(entry),
          body: deterministicPayload(shape, "stream", resource, entry),
        });
      }
      await session.commit({ mode: "Sync" });
    } catch (error) {
      await session.rollback().catch(() => undefined);
      throw error;
    }
  }
}

async function loadSchedules(client: Client, shape: WorkloadShape, resource: number): Promise<void> {
  for (let entry = 0; entry < shape.entriesPerResource; entry += 1) {
    await client.schedule.create(scheduleRoute(shape, resource, entry), {
      cron: "0 0 1 1 *",
      deliveryMode: "Single",
      payload: deterministicPayload(shape, "schedule", resource, entry),
    });
  }
}

async function verifyQueue(client: Client, shape: WorkloadShape, resource: number): Promise<void> {
  const route = resourceRoute("queue", shape, resource);
  let observed = 0;
  while (observed < shape.entriesPerResource) {
    const items = await client.queue.reserve(route, {
      leaseSeconds: 300,
      batchSize: Math.min(1_024, shape.entriesPerResource - observed),
    });
    if (items.length === 0) {
      throw new Error(`${route}: queue ended after ${observed} entries`);
    }
    for (const item of items) {
      assertBytesEqual(
        item.body,
        deterministicPayload(shape, "queue", resource, observed),
        `${route} entry ${observed}`,
      );
      observed += 1;
    }
  }
}

async function verifyKv(client: Client, shape: WorkloadShape, resource: number): Promise<void> {
  const route = resourceRoute("kv", shape, resource);
  const tx = await client.kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
  try {
    let observed = 0;
    while (observed < shape.entriesPerResource) {
      const page = await tx.scan({
        startKey: kvKey(observed),
        limit: Math.min(READ_BATCH_SIZE, shape.entriesPerResource - observed),
      });
      if (page.entries.length === 0) {
        throw new Error(`${route}: KV scan ended after ${observed} entries`);
      }
      for (const item of page.entries) {
        assertBytesEqual(item.key, kvKey(observed), `${route} key ${observed}`);
        assertBytesEqual(
          item.value,
          deterministicPayload(shape, "kv", resource, observed),
          `${route} value ${observed}`,
        );
        observed += 1;
      }
    }
    const extra = await tx.scan({ startKey: kvKey(shape.entriesPerResource), limit: 1 });
    if (extra.entries.length !== 0) throw new Error(`${route}: KV contains unexpected extra data`);
    await tx.rollback();
  } catch (error) {
    await tx.rollback().catch(() => undefined);
    throw error;
  }
}

async function verifyStream(client: Client, shape: WorkloadShape, resource: number): Promise<void> {
  const route = resourceRoute("stream", shape, resource);
  let observed = 0;
  for await (const batch of client.stream.read(route, {
    fromOffset: 0n,
    mode: "replay",
    batchSize: READ_BATCH_SIZE,
  })) {
    for (const record of batch.records) {
      if (record.offset !== BigInt(observed)) {
        throw new Error(`${route}: stream offset ${record.offset} != ${observed}`);
      }
      assertBytesEqual(
        record.body,
        deterministicPayload(shape, "stream", resource, observed),
        `${route} entry ${observed}`,
      );
      observed += 1;
    }
  }
  if (observed !== shape.entriesPerResource) {
    throw new Error(`${route}: stream contained ${observed} entries`);
  }
}

async function verifySchedules(client: Client, shape: WorkloadShape): Promise<void> {
  const expectedCount = shape.resources * shape.entriesPerResource;
  const observed = new Map<string, { cron: string; deliveryMode: string; payload: Uint8Array }>();
  for await (const page of client.schedule.entries(scheduleSelector(shape), { pageSize: 250n })) {
    for (const entry of page) observed.set(entry.route, entry);
  }
  if (observed.size !== expectedCount) {
    throw new Error(`schedule count ${observed.size} != ${expectedCount}`);
  }
  for (let resource = 0; resource < shape.resources; resource += 1) {
    for (let entry = 0; entry < shape.entriesPerResource; entry += 1) {
      const route = scheduleRoute(shape, resource, entry);
      const actual = observed.get(route);
      if (actual === undefined) throw new Error(`${route}: schedule missing`);
      if (actual.cron !== "0 0 1 1 *" || actual.deliveryMode !== "Single") {
        throw new Error(`${route}: schedule metadata mismatch`);
      }
      assertBytesEqual(
        actual.payload,
        deterministicPayload(shape, "schedule", resource, entry),
        `${route} payload`,
      );
    }
  }
}

export async function runResourceOperationsSequentially(
  resources: number,
  operation: (resource: number) => Promise<void>,
): Promise<void> {
  // The Fitz wire has no correlation ID for these domain operations. Its
  // contract makes concurrent requests of the same message type on one
  // connection undefined, so preserve one in-flight operation per domain
  // while the four durable domains still run concurrently above.
  for (let resource = 0; resource < resources; resource += 1) {
    await operation(resource);
  }
}
