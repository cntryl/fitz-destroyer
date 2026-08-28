import { createClient, type Client } from "@cntryl/fitz";
import { createDestroyerToken } from "../auth-token.js";
import type { LiveCommonOptions, LiveLog } from "./live.js";

export type SameShardFamilyFairnessOptions = LiveCommonOptions & { url: string };

export function sameShardFamilyConfig(shardCount: number): {
  families: number[];
  noisyFamily: number;
  canaryFamily: number;
} {
  if (!Number.isSafeInteger(shardCount) || shardCount < 1) throw new Error("shardCount must be positive");
  return {
    families: Array.from({ length: shardCount + 1 }, (_, index) => index + 1),
    noisyFamily: 1,
    canaryFamily: shardCount + 1,
  };
}

export function assertSameShardFamilyFairnessEvidence(record: Readonly<Record<string, unknown>>): void {
  const attempted = numberField(record, "canariesAttempted");
  const completed = numberField(record, "canariesCompleted");
  const errors = numberField(record, "canaryErrors");
  const noisy = numberField(record, "noisyCompleted");
  const longest = numberField(record, "longestCanaryMs");
  const timeout = numberField(record, "requestTimeoutMs");
  if (noisy < 1) throw new Error("no same-shard noisy operations completed");
  if (completed !== attempted) throw new Error(`canaries completed ${completed}/${attempted}`);
  if (errors !== 0) throw new Error(`same-shard canary errors=${errors}`);
  if (longest >= timeout) throw new Error(`same-shard canary latency ${longest}ms reached request timeout ${timeout}ms`);
}

export async function runSameShardFamilyFairness(client: Client, options: SameShardFamilyFairnessOptions, log: LiveLog): Promise<void> {
  const startedAt = performance.now();
  const route = `notice://${options.namespace}/same-shard/fairness`;
  const permissions = [`notice://${options.namespace}/**#*`];
  const canary = createClient({
    url: options.url,
    transport: "ws",
    tokenProvider: async () => createDestroyerToken("identity-b", permissions),
    timeout: options.requestTimeoutMs,
    reconnect: { enabled: false },
    retry: { enabled: false },
    heartbeat: { enabled: false },
  });
  await canary.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
  const received = new Map<number, () => void>();
  const subscription = await canary.notice.subscribe(route, (message) => {
    const sequence = Number(new TextDecoder().decode(message.body));
    received.get(sequence)?.();
    received.delete(sequence);
  });
  let noisyCompleted = 0;
  let canariesCompleted = 0;
  let canaryErrors = 0;
  let longestCanaryMs = 0;
  const canariesAttempted = 32;
  try {
    for (let sequence = 0; sequence < canariesAttempted; sequence += 1) {
      const noise = Array.from({ length: 64 }, (_, index) =>
        client.notice.publish(route, { body: new TextEncoder().encode(`noise-${sequence}-${index}`) })
          .then(() => { noisyCompleted += 1; }),
      );
      const delivered = new Promise<void>((resolve) => received.set(sequence, resolve));
      const canaryStarted = performance.now();
      try {
        await canary.notice.publish(route, { body: new TextEncoder().encode(String(sequence)) });
        await withTimeout(delivered, options.requestTimeoutMs);
        canariesCompleted += 1;
      } catch {
        canaryErrors += 1;
      }
      longestCanaryMs = Math.max(longestCanaryMs, Math.round(performance.now() - canaryStarted));
      await Promise.all(noise);
    }
  } finally {
    await subscription.unsubscribe().catch(() => undefined);
    await canary.close().catch(() => undefined);
  }
  const evidence = { noisyCompleted, canariesAttempted, canariesCompleted, canaryErrors, longestCanaryMs, requestTimeoutMs: options.requestTimeoutMs };
  assertSameShardFamilyFairnessEvidence(evidence);
  log("same_shard_family_fairness_worker_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt) });
}

function numberField(record: Readonly<Record<string, unknown>>, field: string): number {
  const value = record[field];
  if (typeof value !== "number" || !Number.isFinite(value)) throw new Error(`${field} is unavailable`);
  return value;
}

async function withTimeout(completion: Promise<void>, timeoutMs: number): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      completion,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("canary delivery timeout")), timeoutMs);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
