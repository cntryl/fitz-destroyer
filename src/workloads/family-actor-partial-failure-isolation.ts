import { createClient, type Client } from "@cntryl/fitz";
import { createDestroyerToken } from "../auth-token.js";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export type FamilyActorPartialFailureOptions = LiveCommonOptions & {
  url: string;
  failpointUrl: string;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function assertFamilyActorPartialFailureEvidence(record: Readonly<Record<string, unknown>>): void {
  exact(record, "targetedFamilies", 2);
  exact(record, "failedFamilyRejections", 2);
  exact(record, "siblingOperations", 2);
  exact(record, "readinessChecks", 2);
  exact(record, "crossFamilyDeliveries", 0);
}

export async function runFamilyActorPartialFailureIsolation(
  familyA: Client,
  options: FamilyActorPartialFailureOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const permissions = [
    `stream://${options.namespace}/**#*`,
    `rpc://${options.namespace}/**#*`,
  ];
  const familyB = createClient({
    url: options.url,
    transport: "ws",
    tokenProvider: async () => createDestroyerToken("identity-b", permissions),
    timeout: options.requestTimeoutMs,
    reconnect: { enabled: false },
    retry: { enabled: false },
    heartbeat: { enabled: false },
  });
  const familyARpc = createClient({
    url: options.url,
    transport: "ws",
    tokenProvider: async () => createDestroyerToken("identity-a", permissions),
    timeout: options.requestTimeoutMs,
    reconnect: { enabled: false },
    retry: { enabled: false },
    heartbeat: { enabled: false },
  });
  const streamRoute = `stream://${options.namespace}/partial-failure/shared`;
  const rpcRoute = `rpc://${options.namespace}/partial-failure/shared`;
  let crossFamilyDeliveries = 0;
  let failedFamilyRejections = 0;
  let siblingOperations = 0;
  let readinessChecks = 0;

  await familyB.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
  await familyARpc.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
  const workerA = await familyARpc.rpc.registerWorker(rpcRoute, async (_request, writer) => {
    await writer.end({ body: encoder.encode("identity-a") });
  });
  const workerB = await familyB.rpc.registerWorker(rpcRoute, async (_request, writer) => {
    await writer.end({ body: encoder.encode("identity-b") });
  });
  try {
    await append(familyA, streamRoute, 0n, "identity-a", options.signal);
    await append(familyB, streamRoute, 0n, "identity-b", options.signal);

    await inject(options, "stream");
    await expectRejected(() => familyA.stream.peek(streamRoute, { signal: operationSignal(options) }));
    failedFamilyRejections += 1;
    await append(familyB, streamRoute, 1n, "identity-b-survivor", options.signal);
    const siblingRecord = await familyB.stream.peek(streamRoute, { signal: operationSignal(options) });
    if (siblingRecord === null || decoder.decode(siblingRecord.body) !== "identity-b-survivor") {
      crossFamilyDeliveries += 1;
    } else {
      siblingOperations += 1;
    }
    await requireReady(options);
    readinessChecks += 1;
    log("family_actor_partial_failure_stream_complete", { failedFamilyRejections, siblingOperations, readinessChecks });

    await inject(options, "rpc");
    await expectRejected(async () => {
      for await (const _response of familyARpc.rpc.call(rpcRoute, {
        body: encoder.encode("failed-family"),
        timeoutMs: options.requestTimeoutMs,
        signal: operationSignal(options),
      })) { /* no response is valid */ }
    });
    failedFamilyRejections += 1;
    log("family_actor_partial_failure_rpc_rejected", { failedFamilyRejections });
    await ignoreFailure(() => workerB.unsubscribe());
    await ignoreFailure(() => familyB.close());
    await new Promise((resolve) => setTimeout(resolve, 100));
    const familyBRpcCanary = createClient({
      url: options.url,
      transport: "ws",
      tokenProvider: async () => createDestroyerToken("identity-b", permissions),
      timeout: options.requestTimeoutMs,
      reconnect: { enabled: false },
      retry: { enabled: false },
      heartbeat: { enabled: false },
    });
    await familyBRpcCanary.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
    const canaryWorker = await familyBRpcCanary.rpc.registerWorker(rpcRoute, async (_request, writer) => {
      await writer.end({ body: encoder.encode("identity-b") });
    });
    const responses: string[] = [];
    try {
      for await (const response of familyBRpcCanary.rpc.call(rpcRoute, {
        body: encoder.encode("sibling-family"),
        timeoutMs: options.requestTimeoutMs,
        signal: operationSignal(options),
      })) {
        responses.push(decoder.decode(response.body));
      }
    } finally {
      await ignoreFailure(() => canaryWorker.unsubscribe());
      await ignoreFailure(() => familyBRpcCanary.close());
    }
    if (responses.length !== 1 || responses[0] !== "identity-b") {
      crossFamilyDeliveries += 1;
    } else {
      siblingOperations += 1;
    }
    log("family_actor_partial_failure_rpc_sibling_complete", { responses, siblingOperations, crossFamilyDeliveries });
    await requireReady(options);
    readinessChecks += 1;
  } finally {
    await ignoreFailure(() => workerA.unsubscribe());
    await ignoreFailure(() => workerB.unsubscribe());
    await ignoreFailure(() => familyARpc.close());
    await ignoreFailure(() => familyB.close());
  }

  const evidence = { targetedFamilies: 2, failedFamilyRejections, siblingOperations, readinessChecks, crossFamilyDeliveries };
  assertFamilyActorPartialFailureEvidence(evidence);
  log("family_actor_partial_failure_isolation_worker_complete", {
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

async function append(client: Client, route: string, expectedOffset: bigint, body: string, signal: AbortSignal): Promise<void> {
  const session = await client.stream.begin(route, { signal });
  try {
    await session.append({ expectedOffset, body: encoder.encode(body), signal });
    await session.commit({ mode: "Sync", signal });
  } catch (error) {
    await session.rollback().catch(() => undefined);
    throw error;
  }
}

async function inject(options: FamilyActorPartialFailureOptions, domain: "stream" | "rpc"): Promise<void> {
  const response = await fetch(`${options.failpointUrl}/destroyer/failpoints/${domain}-family-1-actor-panic`, {
    method: "POST",
    signal: operationSignal(options),
  });
  if (!response.ok) throw new Error(`${domain} family failpoint returned HTTP ${response.status}`);
}

async function expectRejected(operation: () => Promise<unknown>): Promise<void> {
  try {
    await operation();
  } catch {
    return;
  }
  throw new Error("failed family operation unexpectedly succeeded");
}

async function requireReady(options: FamilyActorPartialFailureOptions): Promise<void> {
  const response = await fetch(`${options.failpointUrl}/readyz`, { signal: operationSignal(options) });
  if (response.status !== 200) throw new Error(`partial family failure withdrew readiness with HTTP ${response.status}`);
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function exact(record: Readonly<Record<string, unknown>>, field: string, expected: number): void {
  if (record[field] !== expected) throw new Error(`${field.replaceAll(/([A-Z])/gu, " $1").toLowerCase()} ${String(record[field])}/${expected}`);
}

async function ignoreFailure(operation: () => Promise<unknown>): Promise<void> {
  try {
    await operation();
  } catch { /* expected when the targeted family closes its transport */ }
}
