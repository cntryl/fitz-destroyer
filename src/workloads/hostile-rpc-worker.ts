import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export type HostileRpcBehavior = "return-without-terminal" | "throw";

export async function runHostileRpcWorker(
  client: Client,
  options: LiveCommonOptions & { behavior: HostileRpcBehavior },
  log: LiveLog,
): Promise<void> {
  let handled = 0;
  const worker = await client.rpc.registerWorker(
    hostileRpcRoute(options.namespace),
    async () => {
      handled += 1;
      log("hostile_rpc_request_observed", { behavior: options.behavior, handled });
      if (options.behavior === "throw") throw new Error("intentional hostile RPC worker failure");
    },
    { maxConcurrency: 1 },
  );
  log("hostile_rpc_worker_ready", { behavior: options.behavior });
  await waitForAbort(options.signal);
  await worker.unsubscribe().catch(() => undefined);
  log("hostile_rpc_worker_complete", { behavior: options.behavior, handled });
}

export async function runHostileRpcCaller(
  client: Client,
  options: LiveCommonOptions & { behavior: HostileRpcBehavior },
  log: LiveLog,
): Promise<void> {
  let failures = 0;
  let frames = 0;
  try {
    for await (const _response of client.rpc.call(hostileRpcRoute(options.namespace), {
      body: new TextEncoder().encode("hostile-worker-probe"),
      timeoutMs: options.requestTimeoutMs,
      signal: AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]),
    })) {
      frames += 1;
    }
  } catch (error) {
    failures = 1;
    log("hostile_rpc_failure_observed", { error: errorMessage(error) });
  }
  const expectedFailures = options.behavior === "return-without-terminal" ? 1 : 0;
  const expectedFrames = options.behavior === "throw" ? 1 : 0;
  if (failures !== expectedFailures || frames !== expectedFrames) {
    throw new Error(
      `Hostile RPC outcome mismatch for ${options.behavior}: failures=${failures}, frames=${frames}`,
    );
  }
  log("hostile_rpc_caller_complete", { behavior: options.behavior, failures, frames });
}

export function hostileRpcRoute(namespace: string): string {
  return `rpc://${namespace}/hostile/worker`;
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => signal.addEventListener("abort", () => resolve(), { once: true }));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
