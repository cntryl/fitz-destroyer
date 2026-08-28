import { createClient, type Client } from "@cntryl/fitz";
import { createDestroyerToken } from "../auth-token.js";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export type SameShardFamilyFailureOptions = LiveCommonOptions & {
  url: string;
  failpointUrl: string;
};

const SHARD_COUNT = 8;
const FAILED_FAMILY = 1;
const SIBLING_FAMILY = 9;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function assertSameShardFamilyFailureEvidence(record: Readonly<Record<string, unknown>>): void {
  const shardCount = positiveInteger(record, "shardCount");
  const failedFamily = positiveInteger(record, "failedFamily");
  const siblingFamily = positiveInteger(record, "siblingFamily");
  if ((failedFamily - 1) % shardCount !== (siblingFamily - 1) % shardCount) {
    throw new Error(`families ${failedFamily} and ${siblingFamily} do not share the same shard`);
  }
  exact(record, "failedFamilyRejections", 2);
  exact(record, "siblingOperations", 2);
  exact(record, "readinessChecks", 2);
  exact(record, "crossFamilyDeliveries", 0);
}

export async function runSameShardFamilyFailureIsolation(
  failedFamilyClient: Client,
  options: SameShardFamilyFailureOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const permissions = [
    `stream://${options.namespace}/**#*`,
    `rpc://${options.namespace}/**#*`,
  ];
  const siblingClient = client(options, "identity-b", permissions);
  const failedFamilyRpc = client(options, "identity-a", permissions);
  const streamRoute = `stream://${options.namespace}/same-shard-failure/shared`;
  const rpcRoute = `rpc://${options.namespace}/same-shard-failure/shared`;
  let failedFamilyRejections = 0;
  let siblingOperations = 0;
  let readinessChecks = 0;
  let crossFamilyDeliveries = 0;

  await siblingClient.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
  await failedFamilyRpc.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
  const failedWorker = await failedFamilyRpc.rpc.registerWorker(rpcRoute, async (_request, writer) => {
    await writer.end({ body: encoder.encode("identity-a") });
  });
  const siblingWorker = await siblingClient.rpc.registerWorker(rpcRoute, async (_request, writer) => {
    await writer.end({ body: encoder.encode("identity-b") });
  });
  try {
    await append(failedFamilyClient, streamRoute, 0n, "identity-a", options);
    await append(siblingClient, streamRoute, 0n, "identity-b", options);

    await inject(options, "stream");
    await expectRejected(() => failedFamilyClient.stream.peek(streamRoute, { signal: operationSignal(options) }));
    failedFamilyRejections += 1;
    await append(siblingClient, streamRoute, 1n, "identity-b-survivor", options);
    const record = await siblingClient.stream.peek(streamRoute, { signal: operationSignal(options) });
    if (record === null || decoder.decode(record.body) !== "identity-b-survivor") {
      crossFamilyDeliveries += 1;
    } else {
      siblingOperations += 1;
    }
    await requireReady(options);
    readinessChecks += 1;

    await inject(options, "rpc");
    await expectRejected(async () => {
      for await (const _response of failedFamilyRpc.rpc.call(rpcRoute, {
        body: encoder.encode("failed-family"),
        timeoutMs: options.requestTimeoutMs,
        signal: operationSignal(options),
      })) { /* no response is valid */ }
    });
    failedFamilyRejections += 1;

    await ignoreFailure(() => siblingWorker.unsubscribe());
    await ignoreFailure(() => siblingClient.close());
    await new Promise((resolve) => setTimeout(resolve, 100));
    const siblingCanary = client(options, "identity-b", permissions);
    await siblingCanary.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
    const canaryWorker = await siblingCanary.rpc.registerWorker(rpcRoute, async (_request, writer) => {
      await writer.end({ body: encoder.encode("identity-b") });
    });
    const responses: string[] = [];
    try {
      for await (const response of siblingCanary.rpc.call(rpcRoute, {
        body: encoder.encode("sibling-family"),
        timeoutMs: options.requestTimeoutMs,
        signal: operationSignal(options),
      })) {
        responses.push(decoder.decode(response.body));
      }
    } finally {
      await ignoreFailure(() => canaryWorker.unsubscribe());
      await ignoreFailure(() => siblingCanary.close());
    }
    if (responses.length !== 1 || responses[0] !== "identity-b") {
      crossFamilyDeliveries += 1;
    } else {
      siblingOperations += 1;
    }
    await requireReady(options);
    readinessChecks += 1;
  } finally {
    await ignoreFailure(() => failedWorker.unsubscribe());
    await ignoreFailure(() => siblingWorker.unsubscribe());
    await ignoreFailure(() => failedFamilyRpc.close());
    await ignoreFailure(() => siblingClient.close());
  }

  const evidence = {
    shardCount: SHARD_COUNT,
    failedFamily: FAILED_FAMILY,
    siblingFamily: SIBLING_FAMILY,
    failedFamilyRejections,
    siblingOperations,
    readinessChecks,
    crossFamilyDeliveries,
  };
  assertSameShardFamilyFailureEvidence(evidence);
  log("same_shard_family_failure_isolation_worker_complete", {
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

function client(options: SameShardFamilyFailureOptions, identity: "identity-a" | "identity-b", permissions: string[]): Client {
  return createClient({
    url: options.url,
    transport: "ws",
    tokenProvider: async () => createDestroyerToken(identity, permissions),
    timeout: options.requestTimeoutMs,
    reconnect: { enabled: false },
    retry: { enabled: false },
    heartbeat: { enabled: false },
  });
}

async function append(client: Client, route: string, expectedOffset: bigint, body: string, options: SameShardFamilyFailureOptions): Promise<void> {
  const signal = operationSignal(options);
  const session = await client.stream.begin(route, { signal });
  try {
    await session.append({ expectedOffset, body: encoder.encode(body), signal });
    await session.commit({ mode: "Sync", signal });
  } catch (error) {
    await session.rollback().catch(() => undefined);
    throw error;
  }
}

async function inject(options: SameShardFamilyFailureOptions, domain: "stream" | "rpc"): Promise<void> {
  const response = await fetch(`${options.failpointUrl}/destroyer/failpoints/${domain}-family-${FAILED_FAMILY}-actor-panic`, {
    method: "POST",
    signal: operationSignal(options),
  });
  if (!response.ok) throw new Error(`${domain} same-shard family failpoint returned HTTP ${response.status}`);
}

async function expectRejected(operation: () => Promise<unknown>): Promise<void> {
  try {
    await operation();
  } catch {
    return;
  }
  throw new Error("failed same-shard family operation unexpectedly succeeded");
}

async function requireReady(options: SameShardFamilyFailureOptions): Promise<void> {
  const response = await fetch(`${options.failpointUrl}/readyz`, { signal: operationSignal(options) });
  if (response.status !== 200) throw new Error(`same-shard family failure withdrew readiness with HTTP ${response.status}`);
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function positiveInteger(record: Readonly<Record<string, unknown>>, field: string): number {
  const value = record[field];
  if (!Number.isSafeInteger(value) || Number(value) < 1) throw new Error(`${field} is unavailable`);
  return Number(value);
}

function exact(record: Readonly<Record<string, unknown>>, field: string, expected: number): void {
  if (record[field] !== expected) throw new Error(`${field.replaceAll(/([A-Z])/gu, " $1").toLowerCase()} ${String(record[field])}/${expected}`);
}

async function ignoreFailure(operation: () => Promise<unknown>): Promise<void> {
  try {
    await operation();
  } catch { /* expected when the targeted family closes its transport */ }
}
