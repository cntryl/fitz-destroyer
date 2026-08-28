import { createClient, type Client } from "@cntryl/fitz";
import { createDestroyerToken } from "../auth-token.js";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export type FamilyActorInflightFailureOptions = LiveCommonOptions & {
  url: string;
  failpointUrl: string;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export async function runFamilyActorInflightConcurrentFailure(
  familyA: Client,
  options: FamilyActorInflightFailureOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const permissions = [`stream://${options.namespace}/**#*`, `rpc://${options.namespace}/**#*`];
  const familyB = client(options, "identity-b", permissions);
  const familyC = client(options, "identity-c", permissions);
  let concurrentFailures = 0;
  let streamInflightRejections = 0;
  let rpcInflightTerminations = 0;
  let siblingOperations = 0;
  let readinessChecks = 0;
  let crossFamilyDeliveries = 0;
  await Promise.all([
    familyB.connectWhenReady({ timeoutMs: options.requestTimeoutMs }),
    familyC.connectWhenReady({ timeoutMs: options.requestTimeoutMs }),
  ]);
  try {
    const stream = await prepareInflightStreams(familyA, familyB, options);
    log("family_actor_inflight_stream_prepared", {});
    await injectConcurrently(options, "stream");
    concurrentFailures += 2;
    await new Promise((resolve) => setTimeout(resolve, 100));
    for (const session of stream) {
      await expectRejected(() => session.commit({ mode: "Sync", signal: operationSignal(options) }));
      streamInflightRejections += 1;
      await ignoreFailure(() => session.rollback());
    }
    log("family_actor_inflight_stream_rejected", { streamInflightRejections });
    await appendAndCommit(familyC, `stream://${options.namespace}/inflight/shared`, "identity-c", options);
    const record = await familyC.stream.peek(`stream://${options.namespace}/inflight/shared`, { signal: operationSignal(options) });
    if (record === null || decoder.decode(record.body) !== "identity-c") crossFamilyDeliveries += 1;
    else siblingOperations += 1;
    log("family_actor_inflight_stream_sibling", { siblingOperations, crossFamilyDeliveries });
    await requireReady(options);
    readinessChecks += 1;

    const rpcA = client(options, "identity-a", permissions);
    const rpcB = client(options, "identity-b", permissions);
    const rpcCWorker = client(options, "identity-c", permissions);
    const rpcCCaller = client(options, "identity-c", permissions);
    await Promise.all([
      rpcA.connectWhenReady({ timeoutMs: options.requestTimeoutMs }),
      rpcB.connectWhenReady({ timeoutMs: options.requestTimeoutMs }),
      rpcCWorker.connectWhenReady({ timeoutMs: options.requestTimeoutMs }),
      rpcCCaller.connectWhenReady({ timeoutMs: options.requestTimeoutMs }),
    ]);
    log("family_actor_inflight_rpc_connected", {});
    const rpc = await prepareInflightRpc(rpcA, rpcB, rpcCWorker, options);
    const inflightOutcomes = Promise.allSettled([rpc.callA, rpc.callB]);
    try {
      await withTimeout(Promise.all([rpc.startedA, rpc.startedB]), options.requestTimeoutMs);
      log("family_actor_inflight_rpc_started", {});
      await injectConcurrently(options, "rpc");
      concurrentFailures += 2;
      const siblingCall = collectCall(rpcCCaller, rpc.route, "sibling", options);
      rpc.release();
      const [outcomes, siblingResponses] = await withTimeout(
        Promise.all([inflightOutcomes, siblingCall]),
        options.requestTimeoutMs,
      );
      log("family_actor_inflight_rpc_terminated", { outcomes: outcomes.map((outcome) => outcome.status) });
      rpcInflightTerminations += outcomes.length;
      for (const outcome of outcomes) {
        if (outcome.status === "fulfilled" && outcome.value.length !== 1) {
          throw new Error(`in-flight RPC completed with ${outcome.value.length} terminal responses`);
        }
      }
      log("family_actor_inflight_rpc_sibling", { siblingResponses });
      if (siblingResponses.length !== 1 || siblingResponses[0] !== "identity-c") crossFamilyDeliveries += 1;
      else siblingOperations += 1;
      await requireReady(options);
      readinessChecks += 1;
    } finally {
      rpc.release();
      await Promise.all(rpc.workers.map((worker) => ignoreFailure(() => worker.unsubscribe())));
      await Promise.all([
        ignoreFailure(() => rpcA.close()),
        ignoreFailure(() => rpcB.close()),
        ignoreFailure(() => rpcCWorker.close()),
        ignoreFailure(() => rpcCCaller.close()),
      ]);
    }
  } finally {
    await Promise.all([ignoreFailure(() => familyB.close()), ignoreFailure(() => familyC.close())]);
  }
  log("family_actor_inflight_concurrent_failure_worker_complete", {
    concurrentFailures,
    streamInflightRejections,
    rpcInflightTerminations,
    siblingOperations,
    readinessChecks,
    crossFamilyDeliveries,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

function client(options: FamilyActorInflightFailureOptions, identity: "identity-a" | "identity-b" | "identity-c", permissions: string[]): Client {
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

async function prepareInflightStreams(familyA: Client, familyB: Client, options: FamilyActorInflightFailureOptions) {
  const route = `stream://${options.namespace}/inflight/shared`;
  const sessions = await Promise.all([
    familyA.stream.begin(route, { signal: operationSignal(options) }),
    familyB.stream.begin(route, { signal: operationSignal(options) }),
  ]);
  await Promise.all(sessions.map((session, index) => session.append({
    expectedOffset: 0n,
    body: encoder.encode(`uncommitted-${index}`),
    signal: operationSignal(options),
  })));
  return sessions;
}

async function appendAndCommit(client: Client, route: string, body: string, options: FamilyActorInflightFailureOptions): Promise<void> {
  const session = await client.stream.begin(route, { signal: operationSignal(options) });
  await session.append({ expectedOffset: 0n, body: encoder.encode(body), signal: operationSignal(options) });
  await session.commit({ mode: "Sync", signal: operationSignal(options) });
}

async function prepareInflightRpc(
  familyA: Client,
  familyB: Client,
  familyC: Client,
  options: FamilyActorInflightFailureOptions,
) {
  const route = `rpc://${options.namespace}/inflight/shared`;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let startA!: () => void;
  let startB!: () => void;
  const startedA = new Promise<void>((resolve) => { startA = resolve; });
  const startedB = new Promise<void>((resolve) => { startB = resolve; });
  const workerA = await familyA.rpc.registerWorker(route, async (_request, writer) => {
    startA();
    await gate;
    await writer.end({ body: encoder.encode("identity-a") });
  });
  const workerB = await familyB.rpc.registerWorker(route, async (_request, writer) => {
    startB();
    await gate;
    await writer.end({ body: encoder.encode("identity-b") });
  });
  const workerC = await familyC.rpc.registerWorker(route, async (_request, writer) => {
    await writer.end({ body: encoder.encode("identity-c") });
  });
  return {
    route,
    startedA,
    startedB,
    release,
    callA: collectCall(familyA, route, "inflight-a", options),
    callB: collectCall(familyB, route, "inflight-b", options),
    workers: [workerA, workerB, workerC],
  };
}

async function collectCall(client: Client, route: string, body: string, options: FamilyActorInflightFailureOptions): Promise<string[]> {
  const responses: string[] = [];
  for await (const response of client.rpc.call(route, {
    body: encoder.encode(body),
    timeoutMs: options.requestTimeoutMs,
    signal: operationSignal(options),
  })) responses.push(decoder.decode(response.body));
  return responses;
}

async function injectConcurrently(options: FamilyActorInflightFailureOptions, domain: "stream" | "rpc"): Promise<void> {
  await Promise.all([1, 2].map(async (family) => {
    const response = await fetch(`${options.failpointUrl}/destroyer/failpoints/${domain}-family-${family}-actor-panic`, {
      method: "POST",
      signal: operationSignal(options),
    });
    if (!response.ok) throw new Error(`${domain} family ${family} failpoint returned HTTP ${response.status}`);
  }));
}

async function expectRejected(operation: () => Promise<unknown>): Promise<void> {
  try {
    await operation();
  } catch {
    return;
  }
  throw new Error("in-flight Stream commit unexpectedly succeeded after family failure");
}

async function requireReady(options: FamilyActorInflightFailureOptions): Promise<void> {
  const response = await fetch(`${options.failpointUrl}/readyz`, { signal: operationSignal(options) });
  if (response.status !== 200) throw new Error(`concurrent family failures withdrew readiness with HTTP ${response.status}`);
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error("in-flight operation did not terminate")), timeoutMs)),
  ]);
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

async function ignoreFailure(operation: () => Promise<unknown>): Promise<void> {
  try {
    await operation();
  } catch { /* expected when a failed family retires its transport */ }
}
