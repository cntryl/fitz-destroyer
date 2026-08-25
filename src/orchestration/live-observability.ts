export type LiveDomain = "kv" | "stream" | "notice" | "rpc" | "lease" | "schedule";

export type CleanupMetrics = {
  failures: number;
  retries: number;
  successes: number;
  pending: number;
  oldestAgeMs: number;
};

export type LiveDomainSnapshot = {
  domain: Readonly<Record<string, unknown>>;
  cleanup: CleanupMetrics;
};

export function cleanupMetrics(metrics: Readonly<Record<string, unknown>>): CleanupMetrics {
  const rawSamples = metrics.samples;
  if (!Array.isArray(rawSamples)) throw new Error("Structured metrics omitted samples");
  const values = new Map<string, number>();
  for (const rawSample of rawSamples) {
    if (typeof rawSample !== "object" || rawSample === null || Array.isArray(rawSample)) continue;
    const sample = rawSample as Readonly<Record<string, unknown>>;
    if (typeof sample.name === "string" && typeof sample.value === "number") {
      values.set(sample.name, sample.value);
    }
  }
  const metric = (name: string): number => values.get(name) ?? 0;
  return {
    failures: metric("fitz_session_cleanup_failures_total"),
    retries: metric("fitz_session_cleanup_retries_total"),
    successes: metric("fitz_session_cleanup_successes_total"),
    pending: metric("fitz_session_cleanup_pending"),
    oldestAgeMs: metric("fitz_session_cleanup_oldest_age_ms"),
  };
}

export function cleanupDelta(before: CleanupMetrics, after: CleanupMetrics): CleanupMetrics {
  return {
    failures: nonNegativeDelta(before.failures, after.failures, "session cleanup failures"),
    retries: nonNegativeDelta(before.retries, after.retries, "session cleanup retries"),
    successes: nonNegativeDelta(before.successes, after.successes, "session cleanup successes"),
    pending: after.pending,
    oldestAgeMs: after.oldestAgeMs,
  };
}

export function isLiveDomainQuiescent(
  domain: LiveDomain,
  snapshot: LiveDomainSnapshot,
): boolean {
  if (snapshot.cleanup.pending !== 0 || snapshot.cleanup.oldestAgeMs !== 0) return false;
  const fields =
    domain === "kv"
      ? ["transactions_active"]
      : domain === "stream"
        ? ["append_sessions_active"]
        : domain === "notice"
      ? ["subscriptions_active", "routes_active"]
      : domain === "rpc"
        ? ["workers_registered", "requests_pending", "pending_routes_active"]
        : domain === "lease"
          ? ["leases_active", "waiter_depth"]
          : [
            "schedules_active",
            "subscriptions_active",
            "pending_fire_claims",
            "pending_ack_retries",
          ];
  return fields.every((field) => snapshot.domain[field] === 0);
}

export function assertNoDomainFailures(
  domain: LiveDomain,
  before: Readonly<Record<string, unknown>>,
  after: Readonly<Record<string, unknown>>,
): void {
  const fields =
    domain === "kv"
      ? ["commits_failed_total", "invalid_transaction_rejects_total"]
      : domain === "stream"
        ? ["failure_total", "append_conflicts_total", "notify_drops_total"]
        : domain === "notice"
      ? ["failure_total", "delivery_drops_total", "wildcard_limit_rejects_total"]
      : domain === "rpc"
        ? [
            "failure_total",
            "request_timeouts_total",
            "backpressure_rejects_total",
            "duplicate_correlation_rejects_total",
            "wrong_worker_rejects_total",
            "responses_dropped_closed_caller_total",
            "responses_missing_pending_total",
            "invalid_sequence_responses_total",
            "invalid_sequence_errors_forwarded_total",
            "invalid_sequence_errors_dropped_total",
          ]
        : domain === "lease"
          ? ["failure_total", "acquire_timeouts_total", "invalid_token_rejects_total"]
          : [
            "notify_failures_total",
            "ack_failures_total",
            "create_persistence_failures_total",
            "upsert_persistence_failures_total",
            "cancel_persistence_failures_total",
          ];
  const increased = fields.flatMap((field) => {
    const delta = nonNegativeDelta(
      counter(before, field),
      counter(after, field),
      `${domain}.${field}`,
    );
    return delta === 0 ? [] : [`${field}=+${delta}`];
  });
  if (increased.length > 0) {
    throw new Error(`${domain} reported failed operations: ${increased.join(", ")}`);
  }
}

function counter(record: Readonly<Record<string, unknown>>, field: string): number {
  const value = record[field];
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${field} is not a non-negative integer`);
  }
  return value;
}

function nonNegativeDelta(before: number, after: number, field: string): number {
  if (after < before) throw new Error(`${field} decreased from ${before} to ${after}`);
  return after - before;
}
