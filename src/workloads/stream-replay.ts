import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual, deterministicPayload } from "./model.js";

export type StreamReplayAction = "contend" | "verify";
export type StreamReplayOptions = LiveCommonOptions & {
  seed: number;
  action: StreamReplayAction;
  commitAtMs: number;
};

export async function runStreamReplay(
  client: Client,
  options: StreamReplayOptions,
  log: LiveLog,
): Promise<void> {
  const route = streamReplayRoute(options.namespace);
  if (options.action === "contend") {
    const writer = Number(options.workerId) + 1;
    if (writer !== 1 && writer !== 2) throw new Error(`Invalid Stream contender ${options.workerId}`);
    let session: Awaited<ReturnType<Client["stream"]["begin"]>> | undefined;
    let stage = "begin";
    try {
      session = await client.stream.begin(route, { signal: operationSignal(options) });
      stage = "append";
      await session.append({
        expectedOffset: 0n,
        body: payload(options, writer, 0),
        signal: operationSignal(options),
      });
      log("stream_replay_contender_ready", { writer, commitAtMs: options.commitAtMs });
      await sleepUntil(options.commitAtMs);
      stage = "commit";
      await session.commit({ mode: "Sync", signal: operationSignal(options) });
      log("stream_replay_contender_complete", { writer, outcome: "committed" });
    } catch (error) {
      await session?.rollback().catch(() => undefined);
      log("stream_replay_contender_complete", {
        writer,
        outcome: "rejected",
        stage,
        error: errorMessage(error),
      });
    }
    return;
  }

  const winner = await replayWinner(client, options, route);
  await assertStaleOffsetConflict(client, options, route);

  const total = Math.max(1, options.operations);
  for (let start = 1; start < total; start += 100) {
    const session = await client.stream.begin(route, { signal: operationSignal(options) });
    try {
      const end = Math.min(total, start + 100);
      for (let sequence = start; sequence < end; sequence += 1) {
        await session.append({
          expectedOffset: BigInt(sequence),
          body: payload(options, 0, sequence),
          signal: operationSignal(options),
        });
      }
      await session.commit({ mode: "Sync", signal: operationSignal(options) });
    } catch (error) {
      await session.rollback().catch(() => undefined);
      throw error;
    }
  }

  let observed = 0;
  let pages = 0;
  for await (const batch of client.stream.read(route, {
    fromOffset: 0n,
    mode: "replay",
    batchSize: 17,
    signal: operationSignal(options),
  })) {
    pages += 1;
    for (const record of batch.records) {
      if (record.offset !== BigInt(observed)) {
        throw new Error(`Stream replay offset ${record.offset} != ${observed}`);
      }
      assertBytesEqual(
        record.body,
        payload(options, observed === 0 ? winner : 0, observed),
        `Stream replay ${observed}`,
      );
      observed += 1;
    }
    if (options.handlerDelayMs > 0) await sleep(options.handlerDelayMs);
  }
  if (observed !== total) throw new Error(`Stream replay observed ${observed}/${total} records`);

  const boundaryRoute = `${route}-boundary`;
  const boundary = boundaryPayload(options.seed, 60_000);
  const boundarySession = await client.stream.begin(boundaryRoute, {
    signal: operationSignal(options),
  });
  await boundarySession.append({
    expectedOffset: 0n,
    body: boundary,
    signal: operationSignal(options),
  });
  await boundarySession.commit({ mode: "Sync", signal: operationSignal(options) });
  let boundaryRecords = 0;
  for await (const batch of client.stream.read(boundaryRoute, {
    fromOffset: 0n,
    mode: "replay",
    batchSize: 1,
    signal: operationSignal(options),
  })) {
    for (const record of batch.records) {
      assertBytesEqual(record.body, boundary, "Stream boundary replay");
      boundaryRecords += 1;
    }
  }
  if (boundaryRecords !== 1) throw new Error(`Stream boundary replay returned ${boundaryRecords} records`);

  log("stream_replay_complete", {
    winner,
    offsetConflicts: 1,
    records: observed,
    pages,
    boundaryBytes: boundary.length,
    boundaryRecords,
  });
}

async function assertStaleOffsetConflict(
  client: Client,
  options: StreamReplayOptions,
  route: string,
): Promise<void> {
  const session = await client.stream.begin(route, { signal: operationSignal(options) });
  try {
    await session.append({
      expectedOffset: 0n,
      body: payload(options, 0, 999_999),
      signal: operationSignal(options),
    });
    throw new Error("Stale Stream expected offset was accepted");
  } catch (error) {
    if (!/conflict/iu.test(errorMessage(error))) throw error;
  } finally {
    await session.rollback().catch(() => undefined);
  }
}

async function replayWinner(
  client: Client,
  options: StreamReplayOptions,
  route: string,
): Promise<number> {
  for await (const batch of client.stream.read(route, {
    fromOffset: 0n,
    mode: "replay",
    batchSize: 1,
    signal: operationSignal(options),
  })) {
    const first = batch.records[0];
    if (first === undefined || first.offset !== 0n) continue;
    for (const writer of [1, 2]) {
      try {
        assertBytesEqual(first.body, payload(options, writer, 0), "Stream contention winner");
        return writer;
      } catch {
        // Check the other deterministic contender.
      }
    }
    throw new Error("Stream offset zero does not match either contender");
  }
  throw new Error("Stream contention produced no offset-zero record");
}

export function streamReplayRoute(namespace: string): string {
  return `stream://destroyer/${namespace}/replay`;
}

export function boundaryPayload(seed: number, bytes: number): Uint8Array {
  const payload = new Uint8Array(bytes);
  let state = seed || 0x9e37_79b9;
  for (let index = 0; index < payload.length; index += 1) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    payload[index] = state & 0xff;
  }
  return payload;
}

function payload(options: StreamReplayOptions, writer: number, sequence: number): Uint8Array {
  return deterministicPayload(options, "stream", writer, sequence);
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function sleepUntil(timestampMs: number): Promise<void> {
  return sleep(Math.max(0, timestampMs - Date.now()));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
