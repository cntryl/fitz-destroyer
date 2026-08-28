import { createClient, type Client, type StreamReadItem } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";

type CursorAxis = "resource" | "area" | "realm" | "global";
type SelectorCase = { selector: string; axis: CursorAxis; expectedRecords: number };
export type StreamSelectorOptions = LiveCommonOptions & { url: string };

export function streamSelectorCases(namespace: string): readonly SelectorCase[] {
  const realmA = `${namespace}-a`;
  return [
    { selector: `stream://${realmA}/area-a/resource-a`, axis: "resource", expectedRecords: 2 },
    { selector: `stream://${realmA}/area-a/*`, axis: "area", expectedRecords: 4 },
    { selector: `stream://${realmA}/*/resource-a`, axis: "realm", expectedRecords: 4 },
    { selector: `stream://${realmA}/*/*`, axis: "realm", expectedRecords: 6 },
    { selector: `stream://${realmA}/**`, axis: "realm", expectedRecords: 6 },
    { selector: "stream://*/area-a/resource-a", axis: "global", expectedRecords: 4 },
    { selector: "stream://*/*/*", axis: "global", expectedRecords: 8 },
    { selector: "stream://**", axis: "global", expectedRecords: 8 },
  ];
}

export function assertStreamSelectorEvidence(record: Readonly<Record<string, unknown>>): void {
  const expected = { selectors: 8, recordsWritten: 8, visibleRecords: 21, filteredOffsets: 21, cursorAdvances: 42, duplicateRecords: 0, missingRecords: 0, reconnectContinuations: 1 };
  for (const [field, value] of Object.entries(expected)) {
    const actual = record[field];
    if (actual !== value) throw new Error(`${field}=${String(actual)}/${value}`);
  }
}

export async function runStreamSelectorCursorConformance(client: Client, options: StreamSelectorOptions, log: LiveLog): Promise<void> {
  const startedAt = performance.now();
  const routes = streamRoutes(options.namespace);
  for (const route of routes) {
    const session = await client.stream.begin(route);
    await session.append({ expectedOffset: 0n, body: payload(route, 0), discriminator: "keep" });
    await session.append({ expectedOffset: 1n, body: payload(route, 1), discriminator: "drop" });
    await session.commit({ mode: "Sync" });
  }

  let visibleRecords = 0;
  let filteredOffsets = 0;
  let cursorAdvances = 0;
  let duplicateRecords = 0;
  let missingRecords = 0;
  for (const selectorCase of streamSelectorCases(options.namespace)) {
    const seen = new Set<string>();
    let visible = 0;
    let filtered = 0;
    let previousOffset = -1n;
    let caughtUp = false;
    for await (const batch of client.stream.read(selectorCase.selector, {
      fromOffset: 0n,
      mode: "replay",
      batchSize: 1,
      filter: { clauses: [{ kind: "Equals", value: "keep" }] },
    })) {
      if (batch.nextOffset <= batch.fromOffset || batch.nextOffset <= previousOffset) throw new Error(`${selectorCase.selector} cursor did not advance`);
      previousOffset = batch.nextOffset;
      caughtUp = batch.caughtUp;
      for (const item of batch.items) {
        const span = itemSpan(item);
        cursorAdvances += span;
        if (item.kind === "event") {
          const identity = `${item.record.route}:${String(item.record.offset)}`;
          if (seen.has(identity)) duplicateRecords += 1;
          seen.add(identity);
          assertAxis(item.record, selectorCase.axis, selectorCase.selector);
          visible += 1;
        } else {
          filtered += span;
        }
      }
    }
    const expectedVisible = selectorCase.expectedRecords / 2;
    if (!caughtUp || visible !== expectedVisible || filtered !== expectedVisible) missingRecords += 1;
    visibleRecords += visible;
    filteredOffsets += filtered;
  }

  const reconnectContinuations = await verifyReconnectContinuation(options, routes.length * 2);
  const evidence = { selectors: 8, recordsWritten: routes.length * 2, visibleRecords, filteredOffsets, cursorAdvances, duplicateRecords, missingRecords, reconnectContinuations };
  assertStreamSelectorEvidence(evidence);
  log("stream_selector_cursor_conformance_worker_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt) });
}

function streamRoutes(namespace: string): readonly string[] {
  return [
    `stream://${namespace}-a/area-a/resource-a`,
    `stream://${namespace}-a/area-a/resource-b`,
    `stream://${namespace}-a/area-b/resource-a`,
    `stream://${namespace}-b/area-a/resource-a`,
  ];
}

async function verifyReconnectContinuation(options: StreamSelectorOptions, expected: number): Promise<number> {
  const first = makeClient(options);
  await first.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
  let nextOffset = 0n;
  let firstCount = 0;
  for await (const batch of first.stream.read("stream://**", { fromOffset: 0n, mode: "replay", batchSize: 1 })) {
    firstCount += batch.records.length;
    nextOffset = batch.nextOffset;
    break;
  }
  await first.close();
  const second = makeClient(options);
  await second.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
  let remaining = 0;
  for await (const batch of second.stream.read("stream://**", { fromOffset: nextOffset, mode: "replay", batchSize: 1 })) remaining += batch.records.length;
  await second.close();
  if (firstCount !== 1 || firstCount + remaining !== expected) throw new Error(`Stream reconnect continuation observed ${firstCount}+${remaining}/${expected}`);
  return 1;
}

function itemSpan(item: StreamReadItem): number {
  return item.kind === "filtered_range" ? Number(item.toOffset - item.fromOffset + 1n) : 1;
}

function assertAxis(record: { offset: bigint; areaOffset?: bigint; realmOffset?: bigint; globalOffset?: bigint }, axis: CursorAxis, selector: string): void {
  const value = axis === "resource" ? record.offset : axis === "area" ? record.areaOffset : axis === "realm" ? record.realmOffset : record.globalOffset;
  if (value === undefined) throw new Error(`${selector} omitted ${axis} offset`);
}

function payload(route: string, offset: number): Uint8Array {
  return new TextEncoder().encode(`${route}:${offset}`);
}

function makeClient(options: StreamSelectorOptions): Client {
  return createClient({ url: options.url, transport: "ws", timeout: options.requestTimeoutMs, reconnect: { enabled: false }, retry: { enabled: false }, heartbeat: { enabled: false } });
}
