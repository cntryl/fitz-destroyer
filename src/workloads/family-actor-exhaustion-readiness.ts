import { createClient, type Client } from "@cntryl/fitz";
import { createDestroyerToken } from "../auth-token.js";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export type FamilyActorExhaustionOptions = LiveCommonOptions & {
  url: string;
  failpointUrl: string;
  domain: "stream" | "rpc";
};

const encoder = new TextEncoder();

export async function runFamilyActorExhaustionReadiness(
  familyA: Client,
  options: FamilyActorExhaustionOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const permissions = [`${options.domain}://${options.namespace}/**#*`];
  const familyB = createClient({
    url: options.url,
    transport: "ws",
    tokenProvider: async () => createDestroyerToken("identity-b", permissions),
    timeout: options.requestTimeoutMs,
    reconnect: { enabled: false },
    retry: { enabled: false },
    heartbeat: { enabled: false },
  });
  let failedFamilyRejections = 0;
  let partialReadinessChecks = 0;
  let totalReadinessWithdrawals = 0;
  await familyB.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
  try {
    const probe = options.domain === "stream"
      ? await prepareStream(familyA, familyB, options)
      : await prepareRpc(familyA, familyB, options);
    try {
      await inject(options, 1);
      await expectRejected(probe.familyA);
      failedFamilyRejections += 1;
      await requireReady(options);
      partialReadinessChecks += 1;

      await inject(options, 2);
      await expectRejected(probe.familyB);
      failedFamilyRejections += 1;
      await requireReadinessWithdrawal(options);
      totalReadinessWithdrawals += 1;
    } finally {
      await probe.cleanup();
    }
  } finally {
    await familyB.close().catch(() => undefined);
  }
  log("family_actor_exhaustion_worker_complete", {
    domain: options.domain,
    failedFamilyRejections,
    partialReadinessChecks,
    totalReadinessWithdrawals,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

async function prepareStream(familyA: Client, familyB: Client, options: FamilyActorExhaustionOptions): Promise<Probe> {
  const route = `stream://${options.namespace}/family-exhaustion/shared`;
  await append(familyA, route, "family-a", options);
  await append(familyB, route, "family-b", options);
  return {
    familyA: () => familyA.stream.peek(route, { signal: operationSignal(options) }),
    familyB: () => familyB.stream.peek(route, { signal: operationSignal(options) }),
    cleanup: async () => undefined,
  };
}

async function prepareRpc(familyA: Client, familyB: Client, options: FamilyActorExhaustionOptions): Promise<Probe> {
  const route = `rpc://${options.namespace}/family-exhaustion/shared`;
  const workerA = await familyA.rpc.registerWorker(route, async (_request, writer) => writer.end({ body: encoder.encode("a") }));
  const workerB = await familyB.rpc.registerWorker(route, async (_request, writer) => writer.end({ body: encoder.encode("b") }));
  return {
    familyA: () => drainCall(familyA, route, options),
    familyB: () => drainCall(familyB, route, options),
    cleanup: async () => {
      await workerA.unsubscribe().catch(() => undefined);
      await workerB.unsubscribe().catch(() => undefined);
    },
  };
}

type Probe = {
  familyA: () => Promise<unknown>;
  familyB: () => Promise<unknown>;
  cleanup: () => Promise<void>;
};

async function append(client: Client, route: string, body: string, options: FamilyActorExhaustionOptions): Promise<void> {
  const signal = operationSignal(options);
  const session = await client.stream.begin(route, { signal });
  await session.append({ expectedOffset: 0n, body: encoder.encode(body), signal });
  await session.commit({ mode: "Sync", signal });
}

async function drainCall(client: Client, route: string, options: FamilyActorExhaustionOptions): Promise<void> {
  for await (const _response of client.rpc.call(route, {
    body: encoder.encode("probe"),
    timeoutMs: options.requestTimeoutMs,
    signal: operationSignal(options),
  })) { /* a failed family must not return a response */ }
}

async function inject(options: FamilyActorExhaustionOptions, family: 1 | 2): Promise<void> {
  const response = await fetch(`${options.failpointUrl}/destroyer/failpoints/${options.domain}-family-${family}-actor-panic`, {
    method: "POST",
    signal: operationSignal(options),
  });
  if (!response.ok) throw new Error(`${options.domain} family ${family} failpoint returned HTTP ${response.status}`);
}

async function expectRejected(operation: () => Promise<unknown>): Promise<void> {
  try {
    await operation();
  } catch {
    return;
  }
  throw new Error("exhausted family operation unexpectedly succeeded");
}

async function requireReady(options: FamilyActorExhaustionOptions): Promise<void> {
  const response = await fetch(`${options.failpointUrl}/readyz`, { signal: operationSignal(options) });
  if (response.status !== 200) throw new Error(`partial ${options.domain} family failure withdrew readiness`);
}

async function requireReadinessWithdrawal(options: FamilyActorExhaustionOptions): Promise<void> {
  const deadline = Date.now() + options.requestTimeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${options.failpointUrl}/readyz`, { signal: AbortSignal.timeout(500) });
      if (response.status !== 200) return;
    } catch {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`exhausted ${options.domain} family pool did not withdraw readiness`);
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}
