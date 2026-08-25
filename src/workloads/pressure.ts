import type { Client } from "@cntryl/fitz";
import type { LiveLog } from "./live.js";

export type PressureReconcileOptions = {
  namespace: string;
  workers: readonly string[];
  requestTimeoutMs: number;
  signal: AbortSignal;
};

export async function runPressureQueueReconciler(
  client: Client,
  options: PressureReconcileOptions,
  log: LiveLog,
): Promise<void> {
  const observed: Record<string, number[]> = {};
  for (const worker of options.workers) {
    const route = `queue://destroyer/${options.namespace}/${worker}`;
    const sequences: number[] = [];
    const seen = new Set<number>();
    let emptyPolls = 0;
    while (emptyPolls < 3) {
      const items = await client.queue.reserve(route, {
        leaseSeconds: 30,
        batchSize: 1_024,
        waitSeconds: 1,
        signal: operationSignal(options),
      });
      if (items.length === 0) {
        emptyPolls += 1;
        continue;
      }
      emptyPolls = 0;
      for (const item of items) {
        const sequence = decodePressureQueueSequence(options.namespace, worker, item.body);
        if (seen.has(sequence)) {
          throw new Error(`${worker} queue reconciler observed duplicate sequence ${sequence}`);
        }
        seen.add(sequence);
        sequences.push(sequence);
        await item.complete({ signal: operationSignal(options) });
      }
    }
    observed[worker] = sequences.sort((left, right) => left - right);
  }
  log("pressure_queue_reconcile_complete", { observed });
}

export function decodePressureQueueSequence(
  namespace: string,
  worker: string,
  body: Uint8Array,
): number {
  const value = new TextDecoder().decode(body);
  const prefix = `${namespace}:${worker}:queue:`;
  if (!value.startsWith(prefix)) {
    throw new Error(`${worker} queue payload '${value.slice(0, 160)}' has an unexpected identity`);
  }
  const sequence = Number(value.slice(prefix.length));
  if (!Number.isSafeInteger(sequence) || sequence < 0) {
    throw new Error(`${worker} queue payload has invalid sequence '${value.slice(prefix.length)}'`);
  }
  return sequence;
}

function operationSignal(options: PressureReconcileOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}
