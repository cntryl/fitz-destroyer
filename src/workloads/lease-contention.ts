import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export type LeaseContentionAction = "contend" | "hold" | "probe";

export type LeaseContentionOptions = LiveCommonOptions & {
  action: LeaseContentionAction;
  participant: string;
};

export async function runLeaseContention(
  client: Client,
  options: LeaseContentionOptions,
  log: LiveLog,
): Promise<void> {
  const route = leaseContentionRoute(options.namespace);
  if (options.action === "probe") {
    const state = await client.lease.query(route, { signal: operationSignal(options) });
    log("lease_probe_complete", {
      route,
      isHeld: state.isHeld,
      pendingWaiters: state.pendingWaiters,
    });
    if (state.isHeld || state.pendingWaiters !== 0) {
      throw new Error(
        `Lease did not quiesce: isHeld=${state.isHeld}, pendingWaiters=${state.pendingWaiters}`,
      );
    }
    return;
  }

  const operations = options.action === "hold" ? 1 : options.operations;
  log("lease_contender_ready", {
    action: options.action,
    participant: options.participant,
    operations,
    route,
  });

  for (let sequence = 0; sequence < operations; sequence += 1) {
    let fencingToken: bigint | undefined;
    const startedAt = performance.now();
    await client.lease.withLease(
      route,
      async (leaseSignal, authority) => {
        fencingToken = authority.fencingToken;
        log("lease_entered", {
          participant: options.participant,
          sequence,
          fencingToken,
          acquiredAtMs: Date.now(),
        });
        if (options.action === "hold") {
          await rejectOnAbort(leaseSignal);
        } else {
          await sleepWithSignal(options.handlerDelayMs, leaseSignal);
        }
      },
      {
        ttlSeconds: 5,
        waitSeconds: Math.max(1, Math.ceil(options.requestTimeoutMs / 1_000)),
        signal: operationSignal(options),
      },
    );
    log("lease_exited", {
      participant: options.participant,
      sequence,
      fencingToken,
      releasedAtMs: Date.now(),
      elapsedMs: Math.round(performance.now() - startedAt),
    });
  }

  log("lease_contender_complete", {
    action: options.action,
    participant: options.participant,
    operations,
  });
}

export function leaseContentionRoute(namespace: string): string {
  return `lease://destroyer/${namespace}/contended`;
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs * 2)]);
}

function rejectOnAbort(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    signal.addEventListener("abort", () => reject(signal.reason), { once: true });
  });
}

function sleepWithSignal(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (milliseconds === 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, milliseconds);
    const abort = (): void => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    if (signal.aborted) {
      abort();
      return;
    }
    signal.addEventListener("abort", abort, { once: true });
  });
}
