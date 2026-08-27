import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual } from "./model.js";

export type StreamGlobalRecoveryAction = "load" | "verify";

export async function runStreamGlobalRecovery(
  client: Client,
  options: LiveCommonOptions & { action: StreamGlobalRecoveryAction },
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const routes = globalRoutes(options.namespace);
  if (options.action === "load") {
    const offsets = new Map(routes.map((route) => [route, 0]));
    for (let sequence = 0; sequence < options.operations; sequence += 1) {
      const route = routes[sequence % routes.length];
      if (route === undefined) throw new Error("Global Stream route selection failed");
      const offset = offsets.get(route) ?? 0;
      const session = await client.stream.begin(route);
      try {
        await session.append({ expectedOffset: BigInt(offset), body: payload(sequence) });
        await session.commit({ mode: "Sync" });
        offsets.set(route, offset + 1);
      } catch (error) {
        await session.rollback().catch(() => undefined);
        throw error;
      }
    }
    log("stream_global_load_complete", {
      records: options.operations,
      resources: routes.length,
      elapsedMs: Math.round(performance.now() - startedAt),
    });
    return;
  }

  let observed = 0;
  let pages = 0;
  const routeOffsets = new Map<string, bigint>();
  for await (const batch of client.stream.read("stream://**", {
    fromOffset: 0n,
    mode: "replay",
    batchSize: 7,
  })) {
    pages += 1;
    for (const record of batch.records) {
      if (!routes.includes(record.route)) throw new Error(`Unexpected global Stream route ${record.route}`);
      if (record.globalOffset !== BigInt(observed)) {
        throw new Error(`Global Stream offset ${String(record.globalOffset)} != ${observed}`);
      }
      const expectedResourceOffset = routeOffsets.get(record.route) ?? 0n;
      if (record.offset !== expectedResourceOffset) {
        throw new Error(`${record.route} offset ${record.offset} != ${expectedResourceOffset}`);
      }
      assertBytesEqual(record.body, payload(observed), `global Stream record ${observed}`);
      routeOffsets.set(record.route, expectedResourceOffset + 1n);
      observed += 1;
    }
  }
  if (observed !== options.operations) {
    throw new Error(`Global Stream replay observed ${observed}/${options.operations} records`);
  }
  log("stream_global_verify_complete", {
    records: observed,
    resources: routeOffsets.size,
    pages,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function globalRoutes(namespace: string): readonly string[] {
  return [
    `stream://${namespace}-a/area-a/resource-a`,
    `stream://${namespace}-a/area-b/resource-b`,
    `stream://${namespace}-b/area-a/resource-c`,
    `stream://${namespace}-b/area-b/resource-d`,
  ];
}

function payload(sequence: number): Uint8Array {
  return new TextEncoder().encode(`global-record-${sequence.toString().padStart(12, "0")}`);
}
