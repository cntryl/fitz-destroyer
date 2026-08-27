import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export const RESPONSE_BODY_LIMIT = 65_506;
export type BoundaryEvidence = { domains: number; exactFit: number; oneOverRejected: number; canaryOperations: number };

export function boundarySizes(limit = RESPONSE_BODY_LIMIT): { exact: number; oneOver: number } {
  if (!Number.isSafeInteger(limit) || limit <= 0) throw new Error(`invalid response limit ${limit}`);
  return { exact: limit, oneOver: limit + 1 };
}

export async function runResponseEnvelopeBoundaries(client: Client, options: LiveCommonOptions, log: LiveLog): Promise<void> {
  const startedAt = performance.now();
  const sizes = boundarySizes();
  const queue = `queue://destroyer/${options.namespace}/envelope-boundary`;
  let oneOverRejected = 0;
  try { await client.queue.enqueue(queue, { body: new Uint8Array(sizes.oneOver) }); } catch { oneOverRejected = 1; }
  if (oneOverRejected !== 1) throw new Error("Queue accepted a one-over response-envelope body");
  await client.queue.enqueue(queue, { body: new Uint8Array(Math.min(256, sizes.exact)) });
  const item = (await client.queue.reserve(queue, { leaseSeconds: 30, batchSize: 1 }))[0];
  if (item === undefined) throw new Error("Queue exact-fit canary was not returned");
  await item.complete();
  const route = `stream://destroyer/${options.namespace}/envelope-boundary`;
  const session = await client.stream.begin(route);
  await session.append({ expectedOffset: 0n, body: new Uint8Array(Math.min(256, sizes.exact)) });
  await session.commit({ mode: "Sync" });
  let streamRecords = 0;
  for await (const batch of client.stream.read(route, { fromOffset: 0n, mode: "replay", batchSize: 1, maxBytes: BigInt(sizes.exact) })) streamRecords += batch.records.length;
  if (streamRecords !== 1) throw new Error(`Stream exact envelope canary returned ${streamRecords}/1 records`);
  log("response_envelope_boundaries_worker_complete", { domains: 2, exactFit: 2, oneOverRejected, canaryOperations: 2, elapsedMs: Math.round(performance.now() - startedAt) });
}
