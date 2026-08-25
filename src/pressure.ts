import type { Domain } from "./workloads/model.js";

export const LATENCY_BUCKET_UPPER_MS = [
  1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000,
] as const;

export type NormalizedErrorClass =
  | "timeout"
  | "connection"
  | "capacity"
  | "conflict"
  | "cancelled"
  | "protocol"
  | "unknown";

export type LatencyHistogram = {
  count: number;
  totalMs: number;
  maxMs: number;
  buckets: number[];
  overflow: number;
};

export type StageMetrics = {
  started: number;
  succeeded: number;
  failed: number;
  ambiguous: number;
  latency: LatencyHistogram;
  errorClasses: Partial<Record<NormalizedErrorClass, number>>;
  errorSamples: readonly { class: NormalizedErrorClass; error: string }[];
};

export type MutableStageMetrics = Omit<StageMetrics, "errorSamples"> & {
  errorSamples: { class: NormalizedErrorClass; error: string }[];
};

export type PressureStages = Partial<Record<Domain, Record<string, MutableStageMetrics>>>;

export type QueueClientOutcome = {
  worker: string;
  acknowledged: readonly number[];
  ambiguousEnqueues: readonly number[];
  failedEnqueues: readonly number[];
  completed: readonly number[];
  ambiguousCompletions: readonly number[];
};

export type QueueReconciliation = {
  verdict: "reconciled";
  totals: {
    acknowledged: number;
    ambiguousEnqueues: number;
    failedEnqueues: number;
    completed: number;
    ambiguousCompletions: number;
    observed: number;
  };
  clients: readonly (QueueClientOutcome & { observed: readonly number[] })[];
};

export type PressureBrokerSample = {
  timestamp: string;
  queue: Readonly<Record<string, unknown>>;
  rpc: Readonly<Record<string, unknown>>;
  metrics: Readonly<Record<string, unknown>>;
  router: {
    currentMailboxDepth: number;
    ingressBackpressureRetriesTotal: number;
    ingressBackpressureAcceptedTotal: number;
    ingressBackpressureExhaustedTotal: number;
    ingressDispatchTimeoutsTotal: number;
    routerBackpressureTotal: number;
    routerHighLaneBackpressureTotal: number;
  };
  rssBytes: number;
};

export type PressureWarning = {
  code: "latency-near-timeout" | "pending-growth" | "rss-growth";
  message: string;
  details: Readonly<Record<string, unknown>>;
};

export function createStageMetrics(): MutableStageMetrics {
  return {
    started: 0,
    succeeded: 0,
    failed: 0,
    ambiguous: 0,
    latency: {
      count: 0,
      totalMs: 0,
      maxMs: 0,
      buckets: LATENCY_BUCKET_UPPER_MS.map(() => 0),
      overflow: 0,
    },
    errorClasses: {},
    errorSamples: [],
  };
}

export function stageMetrics(
  stages: PressureStages,
  domain: Domain,
  stage: string,
): MutableStageMetrics {
  const domainStages = (stages[domain] ??= {});
  return (domainStages[stage] ??= createStageMetrics());
}

export function recordStageLatency(metrics: MutableStageMetrics, elapsedMs: number): void {
  const value = Math.max(0, elapsedMs);
  metrics.latency.count += 1;
  metrics.latency.totalMs += value;
  metrics.latency.maxMs = Math.max(metrics.latency.maxMs, value);
  const index = LATENCY_BUCKET_UPPER_MS.findIndex((upper) => value <= upper);
  if (index < 0) metrics.latency.overflow += 1;
  else metrics.latency.buckets[index] = (metrics.latency.buckets[index] ?? 0) + 1;
}

export function recordStageError(
  metrics: MutableStageMetrics,
  error: unknown,
  ambiguous: boolean,
): NormalizedErrorClass {
  const errorClass = normalizeErrorClass(error);
  if (ambiguous) metrics.ambiguous += 1;
  else metrics.failed += 1;
  metrics.errorClasses[errorClass] = (metrics.errorClasses[errorClass] ?? 0) + 1;
  if (metrics.errorSamples.length < 5) {
    metrics.errorSamples.push({ class: errorClass, error: errorMessage(error) });
  }
  return errorClass;
}

export function normalizeErrorClass(error: unknown): NormalizedErrorClass {
  const name = error instanceof Error ? error.name : "";
  const message = errorMessage(error);
  if (name === "TimeoutError" || /\b(?:timeout|timed? out|deadline)\b/iu.test(message)) {
    return "timeout";
  }
  if (/\b(?:abort|cancel)\w*\b/iu.test(`${name} ${message}`)) return "cancelled";
  if (/\b(?:queuefull|requestqueuefull|capacity|overload|backpressure|mailbox full)\b/iu.test(`${name} ${message}`)) {
    return "capacity";
  }
  if (/\b(?:conflict|expectedoffsetmismatch|already exists|invalid token)\b/iu.test(`${name} ${message}`)) {
    return "conflict";
  }
  if (/\b(?:socket|connection|connect|disconnect|network|econn|closed|websocket)\b/iu.test(`${name} ${message}`)) {
    return "connection";
  }
  if (/\b(?:protocol|frame|codec|decode|encode|tlv|buffer overflow|cannot read)\b/iu.test(`${name} ${message}`)) {
    return "protocol";
  }
  return "unknown";
}

export function isAmbiguousDurableError(error: unknown): boolean {
  const errorClass = normalizeErrorClass(error);
  return errorClass === "timeout" ||
    errorClass === "connection" ||
    errorClass === "cancelled" ||
    errorClass === "protocol";
}

export function latencySummary(histogram: LatencyHistogram): {
  count: number;
  meanMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  maxMs: number;
} {
  return {
    count: histogram.count,
    meanMs: histogram.count === 0 ? 0 : round(histogram.totalMs / histogram.count),
    p50Ms: percentile(histogram, 0.5),
    p95Ms: percentile(histogram, 0.95),
    p99Ms: percentile(histogram, 0.99),
    maxMs: round(histogram.maxMs),
  };
}

export function reconcileQueueOutcomes(
  outcomes: readonly QueueClientOutcome[],
  observedByWorker: Readonly<Record<string, readonly number[]>>,
): QueueReconciliation {
  const totals = {
    acknowledged: 0,
    ambiguousEnqueues: 0,
    failedEnqueues: 0,
    completed: 0,
    ambiguousCompletions: 0,
    observed: 0,
  };
  const clients = outcomes.map((outcome) => {
    for (const [name, values] of Object.entries(outcome)) {
      if (name !== "worker") assertUnique(values as readonly number[], `${outcome.worker} ${name}`);
    }
    const acknowledged = new Set(outcome.acknowledged);
    const ambiguous = new Set(outcome.ambiguousEnqueues);
    const failed = new Set(outcome.failedEnqueues);
    assertDisjoint(outcome.worker, acknowledged, ambiguous, failed);
    const completed = new Set(outcome.completed);
    const ambiguousCompletions = new Set(outcome.ambiguousCompletions);
    const observedValues = observedByWorker[outcome.worker] ?? [];
    assertUnique(observedValues, `${outcome.worker} observed`);
    const observed = new Set(observedValues);

    for (const sequence of [...completed, ...ambiguousCompletions, ...observed]) {
      if (!acknowledged.has(sequence) && !ambiguous.has(sequence)) {
        throw new Error(`${outcome.worker} observed unacknowledged queue sequence ${sequence}`);
      }
    }
    for (const sequence of failed) {
      if (completed.has(sequence) || ambiguousCompletions.has(sequence) || observed.has(sequence)) {
        throw new Error(`${outcome.worker} failed enqueue sequence ${sequence} became visible`);
      }
    }
    for (const sequence of acknowledged) {
      const count = Number(completed.has(sequence)) + Number(observed.has(sequence));
      const maximum = ambiguousCompletions.has(sequence) ? 1 : 1;
      const minimum = ambiguousCompletions.has(sequence) ? 0 : 1;
      if (count < minimum || count > maximum) {
        throw new Error(
          `${outcome.worker} acknowledged queue sequence ${sequence} resolved ${count} times`,
        );
      }
    }
    for (const sequence of ambiguous) {
      const count = Number(completed.has(sequence)) + Number(observed.has(sequence));
      if (count > 1) {
        throw new Error(`${outcome.worker} ambiguous queue sequence ${sequence} resolved ${count} times`);
      }
    }

    totals.acknowledged += outcome.acknowledged.length;
    totals.ambiguousEnqueues += outcome.ambiguousEnqueues.length;
    totals.failedEnqueues += outcome.failedEnqueues.length;
    totals.completed += outcome.completed.length;
    totals.ambiguousCompletions += outcome.ambiguousCompletions.length;
    totals.observed += observedValues.length;
    return { ...outcome, observed: [...observedValues].sort((left, right) => left - right) };
  });

  for (const worker of Object.keys(observedByWorker)) {
    if (!outcomes.some((outcome) => outcome.worker === worker)) {
      throw new Error(`Queue reconciler returned unknown worker ${worker}`);
    }
  }
  return { verdict: "reconciled", totals, clients };
}

export function diagnosticWarnings(
  stages: readonly { client: string; domain: Domain; stage: string; latency: LatencyHistogram }[],
  samples: readonly PressureBrokerSample[],
  requestTimeoutMs: number,
): PressureWarning[] {
  const warnings: PressureWarning[] = [];
  for (const item of stages) {
    const latency = latencySummary(item.latency);
    if (latency.p95Ms > requestTimeoutMs / 2) {
      warnings.push({
        code: "latency-near-timeout",
        message: `${item.client} ${item.domain}/${item.stage} p95 ${latency.p95Ms} ms exceeds half the request timeout`,
        details: { ...item, latency, requestTimeoutMs },
      });
    }
  }

  if (samples.length >= 3) {
    const final = samples.slice(-3).map(pendingWork);
    if ((final[0] ?? 0) < (final[1] ?? 0) && (final[1] ?? 0) < (final[2] ?? 0)) {
      warnings.push({
        code: "pending-growth",
        message: "Broker pending work grew through the final three samples",
        details: { finalPendingWork: final },
      });
    }
  }

  if (samples.length >= 2) {
    const warmupIndex = Math.min(samples.length - 2, Math.max(0, Math.floor(samples.length * 0.1)));
    const baseline = samples[warmupIndex]?.rssBytes ?? 0;
    const final = samples.at(-1)?.rssBytes ?? 0;
    const growth = final - baseline;
    if (baseline > 0 && growth >= 64 * 1_024 * 1_024 && final >= baseline * 1.25) {
      warnings.push({
        code: "rss-growth",
        message: "Post-warmup Fitz RSS grew by at least 25% and 64 MiB",
        details: { baselineRssBytes: baseline, finalRssBytes: final, growthBytes: growth },
      });
    }
  }
  return warnings;
}

function percentile(histogram: LatencyHistogram, fraction: number): number {
  if (histogram.count === 0) return 0;
  const target = Math.ceil(histogram.count * fraction);
  let cumulative = 0;
  for (const [index, count] of histogram.buckets.entries()) {
    cumulative += count;
    if (cumulative >= target) return LATENCY_BUCKET_UPPER_MS[index] ?? round(histogram.maxMs);
  }
  return round(histogram.maxMs);
}

function pendingWork(sample: PressureBrokerSample): number {
  return sumMatching(sample, /^(?:messages_ready|messages_delayed|messages_pending|inflight_active|requests_pending|waiter_depth|mailboxDepth|currentMailboxDepth|session_cleanup_queue_depth)$/u);
}

function sumMatching(value: unknown, keyPattern: RegExp): number {
  if (Array.isArray(value)) return value.reduce((sum, item) => sum + sumMatching(item, keyPattern), 0);
  if (typeof value !== "object" || value === null) return 0;
  let total = 0;
  for (const [key, item] of Object.entries(value)) {
    if (keyPattern.test(key) && typeof item === "number" && Number.isFinite(item)) total += item;
    else total += sumMatching(item, keyPattern);
  }
  return total;
}

function assertUnique(values: readonly number[], label: string): void {
  const seen = new Set<number>();
  for (const value of values) {
    if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${label} has invalid sequence ${value}`);
    if (seen.has(value)) throw new Error(`${label} contains duplicate sequence ${value}`);
    seen.add(value);
  }
}

function assertDisjoint(
  worker: string,
  acknowledged: ReadonlySet<number>,
  ambiguous: ReadonlySet<number>,
  failed: ReadonlySet<number>,
): void {
  for (const sequence of acknowledged) {
    if (ambiguous.has(sequence) || failed.has(sequence)) {
      throw new Error(`${worker} queue sequence ${sequence} has multiple enqueue outcomes`);
    }
  }
  for (const sequence of ambiguous) {
    if (failed.has(sequence)) {
      throw new Error(`${worker} queue sequence ${sequence} has multiple enqueue outcomes`);
    }
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

function round(value: number): number {
  return Math.round(value * 100) / 100;
}
