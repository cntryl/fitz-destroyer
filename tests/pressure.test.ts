import assert from "node:assert/strict";
import test from "node:test";
import {
  createStageMetrics,
  diagnosticWarnings,
  isAmbiguousDurableError,
  latencySummary,
  normalizeErrorClass,
  reconcileQueueOutcomes,
  recordStageLatency,
  type PressureBrokerSample,
} from "../src/pressure.js";
import { parseDockerMemoryUsage, prometheusMetric } from "../src/orchestration/compose.js";
import {
  analyzePressureLogs,
  assertProgressWindows,
  pressureUnexpectedErrors,
} from "../src/orchestration/pressure.js";

test("should_summarize_latency_percentiles_from_bounded_histograms", () => {
  const metrics = createStageMetrics();
  for (const value of [1, 2, 3, 10, 100, 1_000, 6_000, 70_000]) {
    recordStageLatency(metrics, value);
  }

  const summary = latencySummary(metrics.latency);

  assert.deepEqual(summary, {
    count: 8,
    meanMs: 9_639.5,
    p50Ms: 10,
    p95Ms: 70_000,
    p99Ms: 70_000,
    maxMs: 70_000,
  });
});

test("should_normalize_pressure_errors_without_exposing_client_classes", () => {
  assert.equal(normalizeErrorClass(new DOMException("timed out", "TimeoutError")), "timeout");
  assert.equal(normalizeErrorClass(new Error("WebSocket connection closed")), "connection");
  assert.equal(normalizeErrorClass(new Error("queue capacity exhausted")), "capacity");
  assert.equal(normalizeErrorClass(new Error("expected offset conflict")), "conflict");
  assert.equal(normalizeErrorClass(new Error("CodecError: Buffer overflow: cannot read string")), "protocol");
});

test("should_treat_a_durable_response_decode_failure_as_an_unknown_outcome", () => {
  assert.equal(
    isAmbiguousDurableError(new Error("CodecError: Buffer overflow: cannot read string")),
    true,
  );
});

test("should_reconcile_acknowledged_and_ambiguous_queue_outcomes_exactly_once", () => {
  const reconciliation = reconcileQueueOutcomes(
    [
      {
        worker: "worker-a",
        acknowledged: [0, 1, 2],
        ambiguousEnqueues: [3, 4],
        failedEnqueues: [5],
        completed: [0, 3],
        ambiguousCompletions: [2],
      },
    ],
    { "worker-a": [1, 4] },
  );

  assert.equal(reconciliation.verdict, "reconciled");
  assert.deepEqual(reconciliation.totals, {
    acknowledged: 3,
    ambiguousEnqueues: 2,
    failedEnqueues: 1,
    completed: 2,
    ambiguousCompletions: 1,
    observed: 2,
  });
});

test("should_reject_a_queue_sequence_resolved_more_than_once", () => {
  assert.throws(
    () =>
      reconcileQueueOutcomes(
        [
          {
            worker: "worker-a",
            acknowledged: [0],
            ambiguousEnqueues: [],
            failedEnqueues: [],
            completed: [0],
            ambiguousCompletions: [],
          },
        ],
        { "worker-a": [0] },
      ),
    /resolved 2 times/,
  );
});

test("should_report_diagnostic_latency_pending_and_rss_growth_without_failing", () => {
  const metrics = createStageMetrics();
  for (let index = 0; index < 20; index += 1) recordStageLatency(metrics, 6_000);
  const samples = [
    sample(100, 100 * 1_024 * 1_024),
    sample(110, 120 * 1_024 * 1_024),
    sample(120, 180 * 1_024 * 1_024),
  ];

  const warnings = diagnosticWarnings(
    [{ client: "worker-a", domain: "queue", stage: "enqueue", latency: metrics.latency }],
    samples,
    10_000,
  );

  assert.deepEqual(
    warnings.map(({ code }) => code),
    ["latency-near-timeout", "pending-growth", "rss-growth"],
  );
});

test("should_parse_per_client_domain_and_queue_stage_evidence", () => {
  const metrics = createStageMetrics();
  metrics.started = 1;
  metrics.succeeded = 1;
  recordStageLatency(metrics, 7);
  const stopped = {
    timestamp: "2026-08-25T12:00:01.000Z",
    event: "stopped",
    worker: "worker-a",
    totals: { queue: { success: 1, error: 0 } },
    stages: { queue: { enqueue: metrics, reserve: metrics, complete: metrics } },
    queueOutcome: {
      acknowledged: [0],
      ambiguousEnqueues: [],
      failedEnqueues: [],
      completed: [0],
      ambiguousCompletions: [],
    },
  };

  const clients = analyzePressureLogs(new Map([["container-a", JSON.stringify(stopped)]]), ["queue"]);

  assert.equal(clients[0]?.worker, "worker-a");
  assert.equal(clients[0]?.domains.queue?.stages.enqueue?.latency.p95Ms, 10);
  assert.deepEqual(clients[0]?.queueOutcome?.acknowledged, [0]);
});

test("should_defer_ambiguous_queue_outcomes_to_exact_reconciliation", () => {
  const metrics = createStageMetrics();
  metrics.ambiguous = 1;
  metrics.errorClasses.timeout = 1;
  const clients = [{
    container: "container-a",
    worker: "worker-a",
    domains: {
      queue: {
        succeeded: 4,
        failed: 1,
        stages: {
          enqueue: {
            ...metrics,
            latencyHistogram: metrics.latency,
            latency: latencySummary(metrics.latency),
          },
        },
      },
    },
  }];

  assert.deepEqual(pressureUnexpectedErrors(clients, ["queue"]), []);
  clients[0]!.domains.queue.stages.enqueue.failed = 1;
  assert.deepEqual(pressureUnexpectedErrors(clients, ["queue"]), ["worker-a/queue=1"]);
});

test("should_require_progress_from_every_client_and_domain_in_each_window", () => {
  const records = [
    progress("2026-08-25T12:00:05.000Z", 1, 1),
    progress("2026-08-25T12:00:15.000Z", 1, 1),
  ].join("\n");
  const start = Date.parse("2026-08-25T12:00:00.000Z");
  const end = Date.parse("2026-08-25T12:00:20.000Z");

  assert.doesNotThrow(() =>
    assertProgressWindows(new Map([["client", records]]), ["queue", "rpc"], start, end),
  );
  assert.throws(
    () =>
      assertProgressWindows(
        new Map([["client", progress("2026-08-25T12:00:05.000Z", 1, 0)]]),
        ["queue", "rpc"],
        start,
        end,
      ),
    /Missing required pressure progress/,
  );
});

test("should_parse_binary_docker_memory_units", () => {
  assert.equal(parseDockerMemoryUsage("128MiB / 1GiB"), 128 * 1_024 * 1_024);
  assert.equal(parseDockerMemoryUsage("1.5GiB / 8GiB"), 1.5 * 1_024 ** 3);
});

test("should_sum_labeled_prometheus_series_for_broker_snapshots", () => {
  const metrics = [
    "# TYPE fitz_mailbox_depth gauge",
    'fitz_mailbox_depth{actor="queue"} 7',
    'fitz_mailbox_depth{actor="rpc"} 3',
  ].join("\n");

  assert.equal(prometheusMetric(metrics, "fitz_mailbox_depth"), 10);
  assert.equal(prometheusMetric(metrics, "missing"), 0);
});

function sample(messagesPending: number, rssBytes: number): PressureBrokerSample {
  return {
    timestamp: new Date().toISOString(),
    queue: { messages_pending: messagesPending },
    rpc: {},
    metrics: {},
    router: {
      currentMailboxDepth: 0,
      ingressBackpressureRetriesTotal: 0,
      ingressBackpressureAcceptedTotal: 0,
      ingressBackpressureExhaustedTotal: 0,
      ingressDispatchTimeoutsTotal: 0,
      routerBackpressureTotal: 0,
      routerHighLaneBackpressureTotal: 0,
    },
    rssBytes,
  };
}

function progress(timestamp: string, queue: number, rpc: number): string {
  return JSON.stringify({
    timestamp,
    event: "progress",
    window: { queue: { success: queue }, rpc: { success: rpc } },
  });
}
