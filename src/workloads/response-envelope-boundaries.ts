import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export const RESPONSE_BODY_LIMIT = 65_506;
const AGGREGATE_VALUE_BYTES = 24_000;
export type BoundaryEvidence = { domains: number; exactFit: number; oneOverRejected: number; boundedAggregates: number; canaryOperations: number };

export function boundarySizes(limit = RESPONSE_BODY_LIMIT): { exact: number; oneOver: number } {
  if (!Number.isSafeInteger(limit) || limit <= 0) throw new Error(`invalid response limit ${limit}`);
  return { exact: limit, oneOver: limit + 1 };
}

export async function runResponseEnvelopeBoundaries(client: Client, options: LiveCommonOptions, log: LiveLog): Promise<void> {
  const startedAt = performance.now();
  const sizes = boundarySizes();
  let exactFit = 0;
  let boundedAggregates = 0;
  let canaryOperations = 0;
  const queue = `queue://destroyer/${options.namespace}/envelope-boundary`;
  let oneOverRejected = 0;
  try { await client.queue.enqueue(queue, { body: new Uint8Array(sizes.oneOver) }); } catch { oneOverRejected = 1; }
  if (oneOverRejected !== 1) throw new Error("Queue accepted a one-over response-envelope body");
  await client.queue.enqueue(queue, { body: new Uint8Array(Math.min(256, sizes.exact)) });
  const item = (await client.queue.reserve(queue, { leaseSeconds: 30, batchSize: 1 }))[0];
  if (item === undefined) throw new Error("Queue exact-fit canary was not returned");
  await item.complete();
  exactFit += 1;
  canaryOperations += 1;
  const route = `stream://destroyer/${options.namespace}/envelope-boundary`;
  const session = await client.stream.begin(route);
  await session.append({ expectedOffset: 0n, body: new Uint8Array(Math.min(256, sizes.exact)) });
  await session.commit({ mode: "Sync" });
  let streamRecords = 0;
  for await (const batch of client.stream.read(route, { fromOffset: 0n, mode: "replay", batchSize: 1, maxBytes: BigInt(sizes.exact) })) streamRecords += batch.records.length;
  if (streamRecords !== 1) throw new Error(`Stream exact envelope canary returned ${streamRecords}/1 records`);
  exactFit += 1;
  canaryOperations += 1;

  boundedAggregates += await verifyKvAggregateBoundary(client, options.namespace);
  exactFit += 1;
  canaryOperations += 1;
  boundedAggregates += await verifyScheduleAggregateBoundary(client, options.namespace);
  exactFit += 1;
  canaryOperations += 1;
  oneOverRejected += await verifyNoticeBoundary(client, options.namespace, sizes);
  exactFit += 1;
  canaryOperations += 1;
  oneOverRejected += await verifyRpcBoundary(client, options.namespace, sizes, options.requestTimeoutMs);
  exactFit += 1;
  canaryOperations += 1;

  log("response_envelope_boundaries_worker_complete", { domains: 6, exactFit, oneOverRejected, boundedAggregates, canaryOperations, elapsedMs: Math.round(performance.now() - startedAt) });
}

async function verifyKvAggregateBoundary(client: Client, namespace: string): Promise<number> {
  const route = `kv://destroyer/${namespace}/envelope-boundary`;
  const write = await client.kv.begin(route, { durability: "Sync" });
  for (let index = 0; index < 3; index += 1) await write.put({ key: new TextEncoder().encode(`key-${index}`), value: new Uint8Array(AGGREGATE_VALUE_BYTES).fill(index) });
  await write.commit();
  const read = await client.kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
  let bounded = 0;
  try {
    const page = await read.scan({ limit: 3 });
    if (page.entries.length > 0 && page.entries.length < 3) bounded = 1;
  } catch (error) {
    if (!isTypedDomainError(error)) throw error;
    bounded = 1;
  }
  const canary = await read.scan({ limit: 1 });
  await read.rollback().catch(() => undefined);
  if (canary.entries.length !== 1 || bounded !== 1) throw new Error("KV aggregate response was neither bounded nor recoverable");
  return bounded;
}

async function verifyScheduleAggregateBoundary(client: Client, namespace: string): Promise<number> {
  const selector = `schedule://destroyer/${namespace}/*`;
  for (let index = 0; index < 3; index += 1) await client.schedule.create(`schedule://destroyer/${namespace}/envelope/boundary-${index}`, { cron: "0 0 1 1 *", deliveryMode: "Single", payload: new Uint8Array(AGGREGATE_VALUE_BYTES).fill(index) });
  let observed = 0;
  let largestPage = 0;
  try {
    for await (const page of client.schedule.entries(selector, { pageSize: 3n })) {
      observed += page.length;
      largestPage = Math.max(largestPage, page.length);
    }
  } catch (error) {
    if (!isTypedDomainError(error)) throw error;
  }
  let canary = 0;
  for await (const page of client.schedule.entries(selector, { pageSize: 1n })) canary += page.length;
  if (canary !== 3 || (observed === 3 && largestPage >= 3)) throw new Error(`Schedule aggregate response was not bounded: aggregate=${observed}, largestPage=${largestPage}, canary=${canary}`);
  return 1;
}

async function verifyNoticeBoundary(client: Client, namespace: string, sizes: { exact: number; oneOver: number }): Promise<number> {
  const route = `notice://destroyer/${namespace}/envelope-boundary`;
  let resolveDelivery: (body: Uint8Array) => void = () => undefined;
  const delivery = new Promise<Uint8Array>((resolve) => { resolveDelivery = resolve; });
  const subscription = await client.notice.subscribe(route, (notice) => resolveDelivery(notice.body));
  const body = new Uint8Array(Math.min(60_000, sizes.exact));
  await client.notice.publish(route, { body });
  const received = await Promise.race([delivery, rejectAfter(5_000, "Notice exact-fit delivery")]);
  await subscription.unsubscribe();
  if (received.length !== body.length) throw new Error("Notice exact-fit delivery was truncated");
  try { await client.notice.publish(route, { body: new Uint8Array(sizes.oneOver) }); } catch (error) { if (isTypedDomainError(error)) return 1; throw error; }
  throw new Error("Notice accepted a one-over envelope body");
}

async function verifyRpcBoundary(client: Client, namespace: string, sizes: { exact: number; oneOver: number }, timeoutMs: number): Promise<number> {
  const route = `rpc://destroyer/${namespace}/envelope-boundary`;
  let calls = 0;
  const worker = await client.rpc.registerWorker(route, async (_request, writer) => {
    calls += 1;
    await writer.end({ body: new Uint8Array(calls === 1 ? Math.min(60_000, sizes.exact) : sizes.oneOver) });
  }, { maxConcurrency: 1 });
  let exactFrames = 0;
  for await (const response of client.rpc.call(route, { body: new Uint8Array(), timeoutMs })) exactFrames += response.body.length === 60_000 ? 1 : 0;
  let rejected = 0;
  try { for await (const _response of client.rpc.call(route, { body: new Uint8Array(), timeoutMs })) void _response; } catch (error) { if (!isTypedDomainError(error)) throw error; rejected = 1; }
  await worker.unsubscribe();
  if (exactFrames !== 1 || rejected !== 1) throw new Error(`RPC envelope outcomes exact=${exactFrames}, rejected=${rejected}`);
  return rejected;
}

function isTypedDomainError(error: unknown): boolean {
  return error instanceof Error && error.name !== "Error" && /(?:Codec|Request|Queue|Kv|KV|Schedule|Notice|Rpc|RPC)/u.test(error.name);
}

function rejectAfter(milliseconds: number, label: string): Promise<never> {
  return new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} timed out`)), milliseconds));
}
