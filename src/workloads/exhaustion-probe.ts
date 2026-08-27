import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export async function runExhaustionProbe(
  client: Client,
  options: LiveCommonOptions,
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const route = `kv://destroyer/${options.namespace}/exhaustion`;
  const transaction = await client.kv.begin(route, {
    durability: "Sync",
    signal: operationSignal(options),
  }).catch((error: unknown) => {
    log("exhaustion_probe_complete", {
      acknowledged: 0,
      rejected: 1,
      stage: "begin",
      error: errorMessage(error),
      elapsedMs: Math.round(performance.now() - startedAt),
    });
    return undefined;
  });
  if (transaction === undefined) return;

  try {
    const body = new Uint8Array(options.payloadBytes).fill(0x5a);
    await transaction.put({ key: new TextEncoder().encode("probe"), value: body });
    await transaction.commit({ signal: operationSignal(options) });
  } catch (error) {
    await transaction.rollback().catch(() => undefined);
    log("exhaustion_probe_complete", {
      acknowledged: 0,
      rejected: 1,
      stage: "mutation",
      error: errorMessage(error),
      elapsedMs: Math.round(performance.now() - startedAt),
    });
    return;
  }
  throw new Error("Exhausted storage acknowledged a synchronous KV mutation");
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
