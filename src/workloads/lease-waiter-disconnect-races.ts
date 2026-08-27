import { createClient, type Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export type LeaseWaiterRaceOptions = LiveCommonOptions & { url: string };
export type LeaseWaiterRaceEvidence = {
  rounds: number;
  waitersQueued: number;
  waitersDisconnected: number;
  ghostAcquisitions: number;
  pendingWaiters: number;
  replacementAcquisitions: number;
  fencingRegressions: number;
};

const ROUNDS = 8;
const WAITERS_PER_ROUND = 4;

export function leaseWaiterRoute(namespace: string): string {
  return `lease://destroyer/${namespace}/waiter-race`;
}

export function assertLeaseWaiterRaceEvidence(record: Readonly<Record<string, unknown>>): void {
  const expected: LeaseWaiterRaceEvidence = {
    rounds: ROUNDS,
    waitersQueued: ROUNDS * WAITERS_PER_ROUND,
    waitersDisconnected: ROUNDS * WAITERS_PER_ROUND,
    ghostAcquisitions: 0,
    pendingWaiters: 0,
    replacementAcquisitions: ROUNDS,
    fencingRegressions: 0,
  };
  for (const [field, value] of Object.entries(expected)) {
    const actual = record[field];
    if (actual !== value) throw new Error(`${field}=${String(actual)}/${value}`);
  }
}

export async function runLeaseWaiterDisconnectRaces(
  client: Client,
  options: LeaseWaiterRaceOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const route = leaseWaiterRoute(options.namespace);
  let waitersQueued = 0;
  let waitersDisconnected = 0;
  let ghostAcquisitions = 0;
  let replacementAcquisitions = 0;
  let fencingRegressions = 0;

  for (let round = 0; round < ROUNDS; round += 1) {
    const owner = await client.lease.acquire(route, { ttlSeconds: 30, waitSeconds: 0 });

    const waiters = await Promise.all(Array.from({ length: WAITERS_PER_ROUND }, async () => {
      const waiter = makeWaiter(options);
      await waiter.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
      return waiter;
    }));
    const acquisitions = waiters.map((waiter) => waiter.lease.acquire(route, {
      ttlSeconds: 30,
      waitSeconds: 30,
      signal: AbortSignal.timeout(options.requestTimeoutMs * 3),
    }));
    for (const acquisition of acquisitions) void acquisition.catch(() => undefined);
    await waitForWaiterDepth(client, route, WAITERS_PER_ROUND, options.requestTimeoutMs);
    waitersQueued += WAITERS_PER_ROUND;

    if (round % 2 === 0) {
      await Promise.all(waiters.map((waiter) => waiter.close().catch(() => undefined)));
      await owner.release();
    } else {
      await Promise.all([owner.release(), ...waiters.map((waiter) => waiter.close().catch(() => undefined))]);
    }
    waitersDisconnected += WAITERS_PER_ROUND;

    const outcomes = await Promise.allSettled(acquisitions);
    for (const outcome of outcomes) {
      if (outcome.status === "fulfilled") {
        ghostAcquisitions += 1;
        await outcome.value.release().catch(() => undefined);
      }
    }
    await waitForWaiterDepth(client, route, 0, options.requestTimeoutMs);
    const replacement = await client.lease.acquire(route, { ttlSeconds: 30, waitSeconds: 0 });
    replacementAcquisitions += 1;
    await replacement.release();
  }

  const finalState = await client.lease.query(route);
  const evidence: LeaseWaiterRaceEvidence = {
    rounds: ROUNDS,
    waitersQueued,
    waitersDisconnected,
    ghostAcquisitions,
    pendingWaiters: finalState.pendingWaiters,
    replacementAcquisitions,
    fencingRegressions,
  };
  assertLeaseWaiterRaceEvidence(evidence);
  log("lease_waiter_disconnect_races_worker_complete", {
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

function makeWaiter(options: LeaseWaiterRaceOptions): Client {
  return createClient({
    url: options.url,
    transport: "ws",
    timeout: options.requestTimeoutMs,
    reconnect: { enabled: false },
    retry: { enabled: false },
    heartbeat: { enabled: false },
  });
}

async function waitForWaiterDepth(client: Client, route: string, expected: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const state = await client.lease.query(route);
    if (state.pendingWaiters === expected && (expected > 0 || !state.isHeld)) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  const state = await client.lease.query(route);
  throw new Error(`Lease waiter depth ${state.pendingWaiters}/${expected}, held=${String(state.isHeld)}`);
}
