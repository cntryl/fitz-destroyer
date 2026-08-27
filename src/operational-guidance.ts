import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { LATENCY_BUCKET_UPPER_MS, latencySummary, type LatencyHistogram } from "./pressure.js";
import type { ConcreteScenario } from "./scenario.js";
import { ALL_DOMAINS, type Domain } from "./workloads/model.js";
import { completionMetricsForScenario } from "./operational-guidance-completion.js";

export type GuidanceRating = "clear" | "watch" | "constrained" | "not-rated";

export type CompletionSemantics = {
  key: string;
  label: string;
};

export type CountMetric = {
  kind: "count";
  key: string;
  label: string;
  value: number;
  unit: string;
  completionSemantics: string;
};

export type RateMetric = {
  kind: "rate";
  key: string;
  label: string;
  count: number;
  durationMs: number;
  valuePerSecond: number;
  unit: string;
  completionSemantics: string;
};

export type LatencyMetric = {
  kind: "latency";
  key: string;
  label: string;
  statistic: "mean" | "p50" | "p95" | "p99" | "max" | "observed";
  valueMs: number;
};

export type BandwidthMetric = {
  kind: "bandwidth";
  key: string;
  label: string;
  bytes: number;
  durationMs: number;
  bytesPerSecond: number;
  completionSemantics: string;
};

export type RecoveryMetric = {
  kind: "recovery";
  key: string;
  label: string;
  durationMs: number;
};

export type PressureStageSummary = {
  stage: string;
  started: number;
  succeeded: number;
  errors: number;
  ambiguousOutcomes: number;
  expectedCancellations: number;
  latency: ReturnType<typeof latencySummary>;
};

export type PressureDomainSummary = {
  domain: Domain;
  completionSemantics: string;
  completedOperations: number;
  observedOperationsPerSecond: number;
  errors: number;
  ambiguousOutcomes: number;
  expectedCancellations: number;
  slowestStage: string;
  stages: readonly PressureStageSummary[];
};

export type OperationalGuidance = {
  workloadDurationMs: number | null;
  completionSemantics: readonly CompletionSemantics[];
  counts: readonly CountMetric[];
  rates: readonly RateMetric[];
  latencies: readonly LatencyMetric[];
  bandwidth: readonly BandwidthMetric[];
  recoveries: readonly RecoveryMetric[];
  rating: {
    value: GuidanceRating;
    reasons: readonly string[];
  };
  pressureDomains: readonly PressureDomainSummary[];
};

export type EventRecord = Readonly<Record<string, unknown>> & { event: string };
export type MetricCollections = Pick<
  OperationalGuidance,
  "completionSemantics" | "counts" | "rates" | "latencies" | "bandwidth" | "recoveries"
>;

const NOT_PRESSURE_REASON = "Categorical ratings apply only to domain-pressure and soak.";

export function observedRate(count: number, durationMs: number): number | null {
  if (!Number.isFinite(count) || count < 0 || !Number.isFinite(durationMs) || durationMs <= 0) {
    return null;
  }
  return round(count / (durationMs / 1_000));
}

export function completionSemanticsForDomain(domain: Domain): string {
  if (domain === "queue") return "completed Queue enqueue/reserve/completion loops";
  if (domain === "kv") return "committed KV transactions";
  if (domain === "stream") return "committed Stream append sessions";
  if (domain === "schedule") return "accepted durable Schedule definitions";
  if (domain === "notice") return "publisher acceptance, not confirmed fanout";
  if (domain === "lease") return "completed Lease acquire/release cycles";
  return "completed RPC calls with a response";
}

export async function extractOperationalGuidance(
  directory: string,
  scenario: ConcreteScenario,
  summaryWorkloadDurationMs?: number | null,
): Promise<OperationalGuidance> {
  const eventEvidence = await readEvents(directory);
  const events = eventEvidence.events;
  const workloadEvents = events.slice(Math.max(0, events.findIndex(({ event }) => event === "workload_started")));
  const workloadDurationMs = validDuration(summaryWorkloadDurationMs) ??
    eventDuration(lastEvent(events, "workload_complete")) ??
    fallbackScenarioDuration(scenario, events);

  if (scenario === "domain-pressure" || scenario === "soak") {
    try {
      const pressure = await extractPressureGuidance(directory, workloadDurationMs);
      return pressure;
    } catch (error) {
      return emptyGuidance(
        workloadDurationMs,
        `Pressure evidence was unavailable or incomplete: ${errorMessage(error)}`,
      );
    }
  }

  try {
    const metrics = extractScenarioMetrics(scenario, workloadEvents, workloadDurationMs);
    const available = metricCount(metrics) > 0;
    return {
      workloadDurationMs,
      ...metrics,
      rating: {
        value: "not-rated",
        reasons: [available
          ? NOT_PRESSURE_REASON
          : eventEvidence.error ?? "Structured completion evidence was unavailable for this scenario."],
      },
      pressureDomains: [],
    };
  } catch (error) {
    return emptyGuidance(
      workloadDurationMs,
      eventEvidence.error ??
        `Operational evidence was unavailable or incomplete: ${errorMessage(error)}`,
    );
  }
}

function extractScenarioMetrics(
  scenario: ConcreteScenario,
  events: readonly EventRecord[],
  workloadDurationMs: number | null,
): MetricCollections {
  const completionMetrics = completionMetricsForScenario(scenario, events);
  if (completionMetrics !== null) return completionMetrics;
  if (scenario === "clean-restart" || scenario === "cache-loss") {
    return recoveryScenarioMetrics(events);
  }
  if (scenario === "durability-crash-cuts") return crashCutMetrics(events);
  if (scenario === "queue-redelivery") return queueRedeliveryMetrics(events);
  if (scenario === "lease-contention") return leaseContentionMetrics(events);
  if (scenario === "hot-route-canary") return hotRouteMetrics(events, workloadDurationMs);
  if (scenario === "protocol-abuse") return protocolMetrics(events, workloadDurationMs);
  if (scenario === "notice-fanout") return noticeMetrics(events);
  if (scenario === "schedule-delivery") return scheduleDeliveryMetrics(events);
  if (scenario === "session-boundaries") return sessionBoundaryMetrics(events);
  if (scenario === "rpc-pressure") return rpcPressureMetrics(events);
  if (scenario === "rpc-stream-hose") return rpcStreamMetrics(events);
  if (scenario === "connection-storm") return connectionStormMetrics(events);
  if (scenario === "chaos") return chaosMetrics(events);
  if (scenario === "storage-faults") return storageFaultMetrics(events);
  if (scenario === "queue-lifecycle") return queueLifecycleMetrics(events);
  if (scenario === "schedule-outage") return scheduleOutageMetrics(events);
  if (scenario === "transaction-contention") return transactionContentionMetrics(events);
  if (scenario === "stream-replay") return streamReplayMetrics(events);
  if (scenario === "live-churn") return liveChurnMetrics(events);
  return emptyMetrics();
}

function recoveryScenarioMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const jobs = events.filter(({ event }) => event === "client_job_complete");
  for (const mode of ["load", "verify"] as const) {
    const selected = jobs.filter((event) => event.mode === mode);
    if (selected.length === 0) continue;
    const entriesByJob = selected.map((event) => optionalNonNegativeInteger(event.entries));
    if (entriesByJob.some((value) => value === null)) continue;
    const durations = selected.map((event) => validDuration(event.workerElapsedMs));
    const entries = sum(entriesByJob as number[]);
    const durationMs = durations.some((value) => value === null) ? null : sum(durations as number[]);
    const semantics = mode === "load"
      ? "durable operations accepted by the client"
      : "durable entries read and verified";
    addCountAndRate(metrics, `${mode}-entries`, `${capitalize(mode)} entries`, entries, "entries", semantics, durationMs);
  }
  addRestartMetrics(metrics, events);
  return metrics;
}

function crashCutMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "durability_crash_cuts_complete");
  const iterations = arrayField(complete, "iterations").length;
  const elapsedMs = eventDuration(complete);
  addCountAndRate(metrics, "fault-iterations", "Fault iterations", iterations, "iterations", "completed crash-cut iterations", elapsedMs);
  const domains = recordField(complete, "domains");
  const observed = sum(Object.values(domains).map((value) => arrayField(recordValue(value, "durability domain"), "observed").length));
  addCountAndRate(metrics, "observed-records", "Observed durable records", observed, "records", "records observed after broker recovery", elapsedMs);
  for (const event of events.filter(({ event }) => event === "durability_crash_cut_iteration_complete")) {
    const iteration = numberField(event, "iteration");
    addRecovery(metrics, `iteration-${iteration}`, `Crash-cut iteration ${iteration}`, eventDuration(event));
  }
  addRestartMetrics(metrics, events);
  return metrics;
}

function queueRedeliveryMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "queue_redelivery_complete");
  const durationMs = eventDuration(complete);
  addCount(metrics, "produced", "Acknowledged enqueues", numberField(complete, "produced"), "records", "acknowledged Queue enqueues");
  addCountAndRate(metrics, "recovered", "Recovered records", numberField(complete, "recovered"), "records", "records completed after redelivery", durationMs);
  addRecovery(metrics, "redelivery", "Reservation loss to complete drain", durationMs);
  return metrics;
}

function leaseContentionMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "lease_contention_complete");
  const durationMs = eventDuration(complete);
  addCountAndRate(metrics, "contender-admissions", "Contender admissions", numberField(complete, "contenderAdmissions"), "admissions", "completed fenced Lease critical sections", durationMs);
  addCount(metrics, "post-loss-admissions", "Post-owner-loss admissions", 1, "admissions", "waiter admission after owner loss");
  addRecovery(metrics, "owner-loss", "Contention and owner-loss recovery", durationMs);
  return metrics;
}

function hotRouteMetrics(events: readonly EventRecord[], duration: number | null): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "hot_route_canary_complete");
  const elapsedMs = eventDuration(complete) ?? duration;
  const totals = recordField(complete, "hotTotals");
  const successes = sum(Object.values(totals).map((value) => numberField(recordValue(value, "hot route total"), "success")));
  const errors = sum(Object.values(totals).map((value) => numberField(recordValue(value, "hot route total"), "error")));
  addCountAndRate(metrics, "hot-route-successes", "Hot-route successes", successes, "operations", "successful operations on shared hot routes", elapsedMs);
  addCount(metrics, "hot-route-errors", "Hot-route errors", errors, "errors", "recorded hot-route operation errors");
  addCanaryLatencies(metrics, complete);
  return metrics;
}

function protocolMetrics(events: readonly EventRecord[], duration: number | null): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "protocol_abuse_scenario_complete");
  addCountAndRate(metrics, "protocol-cases", "Protocol cases", numberField(complete, "attacks"), "cases", "completed adversarial protocol cases", eventDuration(complete) ?? duration);
  addCanaryLatencies(metrics, complete);
  return metrics;
}

function noticeMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "notice_fanout_complete");
  const started = requiredLastEvent(events, "notice_fanout_started");
  const durationMs = eventDuration(complete);
  const publications = numberField(complete, "publications");
  const deliveries = numberField(complete, "deliveries");
  const payloadBytes = numberField(started, "payloadBytes");
  addCountAndRate(metrics, "publications", "Publications", publications, "publications", "publisher acceptance, not confirmed fanout", durationMs);
  addCountAndRate(metrics, "deliveries", "Verified deliveries", deliveries, "deliveries", "verified while-connected subscriber deliveries", durationMs);
  addBandwidth(metrics, "delivery-bandwidth", "Verified payload delivery bandwidth", deliveries * payloadBytes, durationMs, "verified while-connected subscriber deliveries");
  return metrics;
}

function scheduleDeliveryMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "schedule_delivery_complete");
  const durationMs = eventDuration(complete);
  const broadcast = numberField(complete, "broadcastDeliveries");
  const single = numberField(complete, "singleDeliveries");
  addCountAndRate(metrics, "schedule-deliveries", "Schedule deliveries", broadcast + single, "deliveries", "verified live Schedule handoffs", durationMs);
  addCount(metrics, "broadcast-deliveries", "Broadcast deliveries", broadcast, "deliveries", "verified broadcast handoffs");
  addCount(metrics, "single-deliveries", "Single deliveries", single, "deliveries", "verified single handoffs");
  metrics.latencies.push(latency("maximum-lateness", "Maximum delivery lateness", "max", numberField(complete, "maxLatenessMs")));
  return metrics;
}

function sessionBoundaryMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "session_boundaries_complete");
  const durationMs = eventDuration(complete);
  for (const [key, label] of [
    ["staleRejections", "Stale-handle rejections"],
    ["queueRedelivered", "Queue redeliveries"],
    ["queueCompleted", "Queue completions"],
    ["leaseReacquired", "Lease reacquisitions"],
  ] as const) {
    addCount(metrics, key, label, numberField(complete, key), "outcomes", "verified post-restart boundary outcomes");
  }
  addRecovery(metrics, "session-boundary", "Broker restart to boundary verification", durationMs);
  addRestartMetrics(metrics, events);
  return metrics;
}

function rpcPressureMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "rpc_pressure_complete");
  const started = requiredLastEvent(events, "rpc_pressure_started");
  const durationMs = eventDuration(complete);
  const calls = numberField(complete, "calls");
  const frames = numberField(complete, "responseFrames");
  addCountAndRate(metrics, "rpc-calls", "RPC calls", calls, "calls", "completed RPC calls with ordered responses", durationMs);
  addCountAndRate(metrics, "rpc-frames", "RPC response frames", frames, "frames", "verified ordered RPC response frames", durationMs);
  addBandwidth(metrics, "rpc-response-bandwidth", "RPC response payload bandwidth", frames * numberField(started, "payloadBytes"), durationMs, "verified RPC response payloads");
  return metrics;
}

function rpcStreamMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "rpc_stream_hose_complete");
  const success = events.find((event) =>
    event.event === "rpc_stream_success_phase_complete" && event.label === "rpc-stream-full"
  );
  if (success === undefined) throw new Error("Missing rpc_stream_success_phase_complete for rpc-stream-full");
  const durationMs = numberField(success, "elapsedMs");
  const calls = numberField(success, "completed");
  const frames = numberField(success, "responseFrames");
  const bytes = numberField(success, "responseBytes");
  addCountAndRate(metrics, "streaming-calls", "Streaming RPC calls", calls, "calls", "completed streaming RPC calls", durationMs);
  addCountAndRate(metrics, "streaming-frames", "Streaming response frames", frames, "frames", "verified streaming RPC response frames", durationMs);
  addBandwidth(metrics, "streaming-bandwidth", "Verified response bandwidth", bytes, durationMs, "verified streaming RPC response bytes");
  for (const event of events.filter(({ event }) => event === "rpc_stream_fault_phase_complete")) {
    const label = stringField(event, "label");
    addRecovery(metrics, label, label.replaceAll("-", " "), eventDuration(event));
  }
  metrics.recoveries.push(recovery("scenario", "Streaming workload and destructive phases", numberField(complete, "elapsedMs")));
  return metrics;
}

function connectionStormMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "connection_storm_complete");
  const started = requiredLastEvent(events, "connection_storm_started");
  const durationMs = eventDuration(complete);
  const notices = events.filter(({ event, runLabel }) => event === "notice_fanout_complete" && typeof runLabel === "string" && runLabel.startsWith("connection-storm-wave-"));
  const rpcs = events.filter(({ event, runLabel }) => event === "rpc_pressure_complete" && typeof runLabel === "string" && runLabel.startsWith("connection-storm-wave-"));
  addCountAndRate(metrics, "connection-lifecycles", "Connection lifecycles", numberField(started, "totalConnectionLifecycles"), "connections", "completed client connection lifecycles", durationMs);
  addCountAndRate(metrics, "notice-deliveries", "Notice deliveries", sum(notices.map((event) => numberField(event, "deliveries"))), "deliveries", "verified while-connected Notice deliveries", durationMs);
  addCountAndRate(metrics, "rpc-calls", "RPC calls", sum(rpcs.map((event) => numberField(event, "calls"))), "calls", "completed RPC calls", durationMs);
  for (const event of events.filter(({ event }) => event === "connection_storm_wave_complete")) {
    const wave = numberField(event, "wave");
    addRecovery(metrics, `wave-${wave}`, `Connection wave ${wave}`, eventDuration(event));
  }
  return metrics;
}

function chaosMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "chaos_complete");
  const durationMs = eventDuration(complete);
  addCountAndRate(metrics, "fault-recoveries", "Fault recoveries", numberField(complete, "iterations"), "recoveries", "faults followed by fresh progress in every selected domain", durationMs);
  for (const event of events.filter(({ event }) => event === "chaos_fault_recovery_complete")) {
    const fault = stringField(event, "fault");
    addRecovery(metrics, fault, `${fault} to fresh all-domain progress`, eventDuration(event));
  }
  return metrics;
}

function storageFaultMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "storage_faults_complete");
  const durationMs = eventDuration(complete);
  const iterations = numberField(complete, "iterations");
  addCountAndRate(metrics, "fault-iterations", "Storage fault iterations", iterations, "iterations", "completed storage-fault iterations", durationMs);
  const domains = recordField(complete, "domains");
  const observed = sum(Object.values(domains).map((value) => arrayField(recordValue(value, "storage domain"), "observed").length));
  addCountAndRate(metrics, "observed-records", "Observed durable records", observed, "records", "durable records observed after restored-provider verification", durationMs);
  for (const event of events.filter(({ event }) => event === "storage_fault_iteration_complete")) {
    const iteration = numberField(event, "iteration");
    const fault = stringField(event, "fault");
    addRecovery(metrics, `iteration-${iteration}`, `${fault} iteration ${iteration}`, eventDuration(event));
  }
  return metrics;
}

function queueLifecycleMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "queue_lifecycle_complete");
  const durationMs = eventDuration(complete);
  addCountAndRate(metrics, "drained-records", "Completed Queue records", numberField(complete, "operations"), "records", "records completed across partial completion and redelivery", durationMs);
  addCount(metrics, "abandoned-records", "Abandoned reservations", 1, "records", "reservations deliberately abandoned before redelivery");
  addRecovery(metrics, "queue-lifecycle", "Queue lifecycle completion and recovery", durationMs);
  return metrics;
}

function scheduleOutageMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "schedule_outage_complete");
  const durationMs = eventDuration(complete);
  const repeated = arrayField(complete, "repeatedSequences").length;
  const race = arrayField(complete, "raceSequences").length;
  addCountAndRate(metrics, "repeated-deliveries", "Repeated occurrences", repeated, "deliveries", "verified live delivery of the next repeated occurrence", durationMs);
  addCount(metrics, "race-deliveries", "Cancellation-race deliveries", race, "deliveries", "live handoffs accepted during cancellation races");
  addCount(metrics, "missed-deliveries", "Missed-occurrence deliveries", numberField(complete, "missedDeliveries"), "deliveries", "unexpected deliveries from the intentionally missed occurrence");
  addRecovery(metrics, "schedule-outage", "Outage through repeated-occurrence verification", durationMs);
  addRestartMetrics(metrics, events);
  return metrics;
}

function transactionContentionMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "transaction_contention_complete");
  const durationMs = eventDuration(complete);
  addCountAndRate(metrics, "transaction-outcomes", "Contention outcomes", 1 + numberField(complete, "conflicts"), "outcomes", "one committed KV winner plus rejected conflicts", durationMs);
  addCount(metrics, "commits", "Committed winners", 1, "transactions", "verified committed KV winners");
  addCount(metrics, "conflicts", "Rejected conflicts", numberField(complete, "conflicts"), "transactions", "verified conflicting KV commits rejected");
  addRecovery(metrics, "transaction-contention", "Contention and killed-transaction cleanup", durationMs);
  return metrics;
}

function streamReplayMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "stream_replay_complete");
  const durationMs = eventDuration(complete);
  addCountAndRate(metrics, "replayed-records", "Replayed records", numberField(complete, "records"), "records", "records read and verified through paged replay", durationMs);
  addCount(metrics, "replay-pages", "Replay pages", numberField(complete, "pages"), "pages", "verified replay pages");
  addBandwidth(metrics, "boundary-bandwidth", "Boundary response bandwidth", numberField(complete, "boundaryBytes"), durationMs, "verified boundary response bytes");
  return metrics;
}

function liveChurnMetrics(events: readonly EventRecord[]): MetricCollections {
  const metrics = mutableMetrics();
  const complete = requiredLastEvent(events, "live_churn_complete");
  const phases = arrayField(complete, "phases");
  const durationMs = eventDuration(complete);
  addCountAndRate(metrics, "churn-phases", "Churn phases", phases.length, "phases", "completed live-domain churn and recovery phases", durationMs);
  for (const value of phases) {
    const phase = recordValue(value, "live churn phase");
    const name = stringField(phase, "phase");
    addRecovery(metrics, name, name.replaceAll("-", " "), validDuration(phase.elapsedMs));
  }
  return metrics;
}

async function extractPressureGuidance(
  directory: string,
  workloadDurationMs: number | null,
): Promise<OperationalGuidance> {
  const value: unknown = JSON.parse(await readFile(join(directory, "pressure-evidence.json"), "utf8"));
  const evidence = recordValue(value, "pressure evidence");
  const durationMs = positiveNumberField(evidence, "durationMs");
  const requestTimeoutMs = positiveNumberField(evidence, "requestTimeoutMs");
  const selectedDomains = domainArray(evidence.selectedDomains);
  if (selectedDomains.length === 0) throw new Error("selectedDomains was empty");
  const aggregate = recordField(evidence, "aggregate");
  const pressureDomains = selectedDomains.map((domain) => parsePressureDomain(domain, aggregate[domain], durationMs));
  const brokerSummary = recordField(evidence, "brokerSummary");
  const ingressTimeouts = numberField(brokerSummary, "ingressDispatchTimeoutsDelta");
  const routerBackpressure = numberField(brokerSummary, "routerBackpressureDelta") +
    optionalNumberField(brokerSummary, "routerHighLaneBackpressureDelta");
  const warnings = arrayField(evidence, "warnings").map((item) => recordValue(item, "pressure warning"));
  const constrained: string[] = [];
  const watch: string[] = [];

  for (const domain of pressureDomains) {
    for (const stage of domain.stages) {
      if (stage.errors > 0) {
        constrained.push(`${domain.domain}/${stage.stage} recorded ${stage.errors} definite workload errors`);
      }
      if (stage.latency.p95Ms > requestTimeoutMs / 2) {
        constrained.push(`${domain.domain}/${stage.stage} p95 ${stage.latency.p95Ms} ms exceeded 50% of the ${requestTimeoutMs} ms request timeout`);
      } else if (stage.latency.p95Ms > requestTimeoutMs / 4) {
        watch.push(`${domain.domain}/${stage.stage} p95 ${stage.latency.p95Ms} ms exceeded 25% of the ${requestTimeoutMs} ms request timeout`);
      }
    }
  }
  if (ingressTimeouts > 0) constrained.push(`Ingress dispatch timeouts increased by ${ingressTimeouts}`);
  if (routerBackpressure > 0) constrained.push(`Router backpressure increased by ${routerBackpressure}`);
  for (const warning of warnings) {
    const code = stringField(warning, "code");
    if (code === "pending-growth") watch.push("Broker pending work grew through the final samples");
    if (code === "rss-growth") watch.push("Post-warmup Fitz RSS growth crossed the diagnostic signal");
  }

  const rating = constrained.length > 0
    ? { value: "constrained" as const, reasons: [...constrained, ...watch] }
    : watch.length > 0
      ? { value: "watch" as const, reasons: watch }
      : { value: "clear" as const, reasons: ["No saturation signals at the observed rate."] };
  const metrics = mutableMetrics();
  for (const domain of pressureDomains) {
    addCountAndRate(
      metrics,
      `${domain.domain}-operations`,
      `${capitalize(domain.domain)} operations`,
      domain.completedOperations,
      "operations",
      domain.completionSemantics,
      durationMs,
    );
    addCount(metrics, `${domain.domain}-errors`, `${capitalize(domain.domain)} errors`, domain.errors, "errors", "definite workload errors excluding expected shutdown cancellations");
    for (const stage of domain.stages) {
      for (const [statistic, valueMs] of [
        ["mean", stage.latency.meanMs],
        ["p50", stage.latency.p50Ms],
        ["p95", stage.latency.p95Ms],
        ["p99", stage.latency.p99Ms],
        ["max", stage.latency.maxMs],
      ] as const) {
        metrics.latencies.push(latency(`${domain.domain}-${stage.stage}-${statistic}`, `${domain.domain}/${stage.stage} ${statistic}`, statistic, valueMs));
      }
    }
  }
  return {
    workloadDurationMs,
    ...metrics,
    rating,
    pressureDomains,
  };
}

function parsePressureDomain(domain: Domain, value: unknown, durationMs: number): PressureDomainSummary {
  const evidence = recordValue(value, `${domain} pressure evidence`);
  const completedOperations = numberField(evidence, "succeeded");
  const stagesRecord = recordField(evidence, "stages");
  const stages = Object.entries(stagesRecord).sort(([left], [right]) => left.localeCompare(right)).map(([stage, item]) => {
    const record = recordValue(item, `${domain}/${stage}`);
    const histogram = latencyHistogram(record.latencyHistogram, `${domain}/${stage} latencyHistogram`);
    const errorClasses = recordField(record, "errorClasses");
    const failed = numberField(record, "failed");
    const ambiguous = numberField(record, "ambiguous");
    const cancelled = optionalNumberField(errorClasses, "cancelled");
    const shutdownCancellations = expectedShutdownCancellations(
      record.expectedShutdownCancellations,
      failed,
      ambiguous,
      cancelled,
      record.errorSamples,
      `${domain}/${stage}`,
    );
    return {
      stage,
      started: numberField(record, "started"),
      succeeded: numberField(record, "succeeded"),
      errors: failed - shutdownCancellations.failed,
      ambiguousOutcomes: ambiguous,
      expectedCancellations: shutdownCancellations.failed + shutdownCancellations.ambiguous,
      latency: latencySummary(histogram),
    };
  });
  if (stages.length === 0) throw new Error(`${domain} omitted stage evidence`);
  const rate = observedRate(completedOperations, durationMs);
  if (rate === null) throw new Error(`${domain} observed rate could not be calculated`);
  const slowest = [...stages].sort((left, right) => right.latency.p95Ms - left.latency.p95Ms || left.stage.localeCompare(right.stage))[0]!;
  return {
    domain,
    completionSemantics: completionSemanticsForDomain(domain),
    completedOperations,
    observedOperationsPerSecond: rate,
    errors: sum(stages.map(({ errors }) => errors)),
    ambiguousOutcomes: sum(stages.map(({ ambiguousOutcomes }) => ambiguousOutcomes)),
    expectedCancellations: sum(stages.map(({ expectedCancellations }) => expectedCancellations)),
    slowestStage: slowest.stage,
    stages,
  };
}

function expectedShutdownCancellations(
  value: unknown,
  failed: number,
  ambiguous: number,
  legacyCancelled: number,
  legacyErrorSamples: unknown,
  label: string,
): { failed: number; ambiguous: number } {
  if (value === undefined) {
    const inferredTotal = Math.min(
      failed + ambiguous,
      legacyCancelled + legacySignalCancellations(legacyErrorSamples),
    );
    const inferredAmbiguous = Math.min(ambiguous, inferredTotal);
    return {
      failed: Math.min(failed, inferredTotal - inferredAmbiguous),
      ambiguous: inferredAmbiguous,
    };
  }
  const record = recordValue(value, `${label} expectedShutdownCancellations`);
  const expected = {
    failed: numberField(record, "failed"),
    ambiguous: numberField(record, "ambiguous"),
  };
  if (expected.failed > failed || expected.ambiguous > ambiguous) {
    throw new Error(`${label} expected shutdown cancellations exceeded recorded outcomes`);
  }
  return expected;
}

function legacySignalCancellations(value: unknown): number {
  if (!Array.isArray(value)) return 0;
  return value.filter((sample) => {
    if (!isRecord(sample) || sample.class === "cancelled" || typeof sample.error !== "string") {
      return false;
    }
    return /\breceived SIG(?:INT|TERM)\b/u.test(sample.error);
  }).length;
}

function addRestartMetrics(metrics: ReturnType<typeof mutableMetrics>, events: readonly EventRecord[]): void {
  for (const [index, event] of events.filter(({ event }) => event === "fitz_restart_complete").entries()) {
    const kind = stringField(event, "kind");
    metrics.recoveries.push(recovery(`restart-${index + 1}`, `${kind} restart ${index + 1}`, numberField(event, "elapsedMs")));
  }
  let restartPending = false;
  const readinessEvents: EventRecord[] = [];
  for (const event of events) {
    if (
      event.event === "fitz_graceful_stop_started" ||
      event.event === "fitz_sigkill_started" ||
      event.event === "fitz_cache_discard_started" ||
      event.event === "sqrzl_sigkill_started"
    ) {
      restartPending = true;
    } else if (event.event === "fitz_ready" && restartPending) {
      readinessEvents.push(event);
      restartPending = false;
    }
  }
  for (const [index, event] of readinessEvents.entries()) {
    metrics.latencies.push(latency(`readiness-${index + 1}`, `Restart readiness ${index + 1}`, "observed", numberField(event, "elapsedMs")));
  }
}

function addCanaryLatencies(metrics: ReturnType<typeof mutableMetrics>, event: EventRecord): void {
  const maximums = recordField(event, "canaryMaximumMs");
  for (const [domain, value] of Object.entries(maximums).sort(([left], [right]) => left.localeCompare(right))) {
    metrics.latencies.push(latency(`canary-${domain}`, `${domain} canary maximum`, "max", nonNegativeNumber(value, `${domain} canary maximum`)));
  }
}

function addCountAndRate(
  metrics: ReturnType<typeof mutableMetrics>,
  key: string,
  label: string,
  value: number,
  unit: string,
  semantics: string,
  durationMs: number | null,
): void {
  addCount(metrics, key, label, value, unit, semantics);
  if (durationMs === null) return;
  const valuePerSecond = observedRate(value, durationMs);
  if (valuePerSecond === null) return;
  metrics.rates.push({
    kind: "rate",
    key,
    label,
    count: value,
    durationMs,
    valuePerSecond,
    unit: `${unit}/s`,
    completionSemantics: semantics,
  });
}

function addCount(
  metrics: ReturnType<typeof mutableMetrics>,
  key: string,
  label: string,
  value: number,
  unit: string,
  semantics: string,
): void {
  metrics.counts.push({ kind: "count", key, label, value, unit, completionSemantics: semantics });
  if (!metrics.completionSemantics.some((item) => item.key === key)) {
    metrics.completionSemantics.push({ key, label: semantics });
  }
}

function addBandwidth(
  metrics: ReturnType<typeof mutableMetrics>,
  key: string,
  label: string,
  bytes: number,
  durationMs: number | null,
  semantics: string,
): void {
  if (durationMs === null) return;
  const rate = observedRate(bytes, durationMs);
  if (rate === null) return;
  metrics.bandwidth.push({
    kind: "bandwidth",
    key,
    label,
    bytes,
    durationMs,
    bytesPerSecond: rate,
    completionSemantics: semantics,
  });
}

function addRecovery(
  metrics: ReturnType<typeof mutableMetrics>,
  key: string,
  label: string,
  durationMs: number | null,
): void {
  if (durationMs !== null) metrics.recoveries.push(recovery(key, label, durationMs));
}

function latency(key: string, label: string, statistic: LatencyMetric["statistic"], valueMs: number): LatencyMetric {
  return { kind: "latency", key, label, statistic, valueMs };
}

function recovery(key: string, label: string, durationMs: number): RecoveryMetric {
  return { kind: "recovery", key, label, durationMs };
}

function mutableMetrics(): {
  completionSemantics: CompletionSemantics[];
  counts: CountMetric[];
  rates: RateMetric[];
  latencies: LatencyMetric[];
  bandwidth: BandwidthMetric[];
  recoveries: RecoveryMetric[];
} {
  return { completionSemantics: [], counts: [], rates: [], latencies: [], bandwidth: [], recoveries: [] };
}

function emptyMetrics(): MetricCollections {
  return mutableMetrics();
}

export function emptyGuidance(
  workloadDurationMs: number | null,
  reason: string,
): OperationalGuidance {
  return {
    workloadDurationMs,
    ...emptyMetrics(),
    rating: { value: "not-rated", reasons: [reason] },
    pressureDomains: [],
  };
}

async function readEvents(directory: string): Promise<{ events: EventRecord[]; error: string | null }> {
  let contents: string;
  try {
    contents = await readFile(join(directory, "events.ndjson"), "utf8");
  } catch (error) {
    if (isMissingFile(error)) return { events: [], error: "events.ndjson was not available." };
    return { events: [], error: `events.ndjson could not be read: ${errorMessage(error)}` };
  }
  const events: EventRecord[] = [];
  for (const [index, line] of contents.split("\n").entries()) {
    if (line.trim() === "") continue;
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      return { events: [], error: `events.ndjson line ${index + 1} was malformed.` };
    }
    if (!isRecord(value) || typeof value.event !== "string") {
      return { events: [], error: `events.ndjson line ${index + 1} was not an event record.` };
    }
    events.push(value as EventRecord);
  }
  return { events, error: null };
}

function fallbackScenarioDuration(scenario: ConcreteScenario, events: readonly EventRecord[]): number | null {
  const names: Partial<Record<ConcreteScenario, string>> = {
    "durability-crash-cuts": "durability_crash_cuts_complete",
    "queue-overload-recovery": "queue_overload_recovery_complete",
    "response-loss": "response_loss_complete",
    "active-graceful-shutdown": "active_graceful_shutdown_complete",
    "half-open-session": "half_open_session_complete",
    "authorization-isolation": "authorization_isolation_complete",
    "stream-global-recovery": "stream_global_recovery_complete",
    "queue-dead-letter-fencing": "queue_dead_letter_fencing_complete",
    "cold-boot-provider-outage": "cold_boot_provider_outage_complete",
    "hostile-rpc-worker": "hostile_rpc_worker_complete",
    "queue-redelivery": "queue_redelivery_complete",
    "lease-contention": "lease_contention_complete",
    "hot-route-canary": "hot_route_canary_complete",
    "protocol-abuse": "protocol_abuse_scenario_complete",
    "notice-fanout": "notice_fanout_complete",
    "schedule-delivery": "schedule_delivery_complete",
    "session-boundaries": "session_boundaries_complete",
    "rpc-pressure": "rpc_pressure_complete",
    "rpc-stream-hose": "rpc_stream_hose_complete",
    "connection-storm": "connection_storm_complete",
    "storage-faults": "storage_faults_complete",
    "queue-lifecycle": "queue_lifecycle_complete",
    "schedule-outage": "schedule_outage_complete",
    "transaction-contention": "transaction_contention_complete",
    "stream-replay": "stream_replay_complete",
    "live-churn": "live_churn_complete",
    chaos: "chaos_complete",
    "domain-pressure": "pressure_complete",
    soak: "pressure_complete",
  };
  const name = names[scenario];
  return name === undefined ? null : eventDuration(lastEvent(events, name));
}

function latencyHistogram(value: unknown, label: string): LatencyHistogram {
  const record = recordValue(value, label);
  const buckets = arrayField(record, "buckets").map((item) => nonNegativeInteger(item, `${label} bucket`));
  if (buckets.length !== LATENCY_BUCKET_UPPER_MS.length) {
    throw new Error(`${label} expected ${LATENCY_BUCKET_UPPER_MS.length} buckets, found ${buckets.length}`);
  }
  const count = numberField(record, "count");
  const overflow = numberField(record, "overflow");
  if (sum(buckets) + overflow !== count) {
    throw new Error(`${label} count did not match its buckets and overflow`);
  }
  return {
    count,
    totalMs: nonNegativeNumber(record.totalMs, `${label}.totalMs`),
    maxMs: nonNegativeNumber(record.maxMs, `${label}.maxMs`),
    buckets,
    overflow,
  };
}

function domainArray(value: unknown): Domain[] {
  if (!Array.isArray(value)) throw new Error("selectedDomains must be an array");
  return value.map((item) => {
    if (typeof item !== "string" || !(ALL_DOMAINS as readonly string[]).includes(item)) {
      throw new Error(`selectedDomains contained ${String(item)}`);
    }
    return item as Domain;
  });
}

function requiredLastEvent(events: readonly EventRecord[], name: string): EventRecord {
  const value = lastEvent(events, name);
  if (value === undefined) throw new Error(`Missing ${name} event`);
  return value;
}

function lastEvent(events: readonly EventRecord[], name: string): EventRecord | undefined {
  return events.findLast(({ event }) => event === name);
}

function eventDuration(event: EventRecord | undefined): number | null {
  return event === undefined ? null : validDuration(event.elapsedMs);
}

function validDuration(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

function numberField(record: Readonly<Record<string, unknown>>, field: string): number {
  const value = record[field];
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${field} was not a non-negative integer`);
  }
  return value;
}

function positiveNumberField(record: Readonly<Record<string, unknown>>, field: string): number {
  const value = numberField(record, field);
  if (value === 0) throw new Error(`${field} must be greater than zero`);
  return value;
}

function optionalNumberField(record: Readonly<Record<string, unknown>>, field: string): number {
  return record[field] === undefined ? 0 : numberField(record, field);
}

function nonNegativeNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new Error(`${label} was not a non-negative number`);
  }
  return value;
}

function nonNegativeInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${label} was not a non-negative integer`);
  }
  return value;
}

function optionalNonNegativeInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function stringField(record: Readonly<Record<string, unknown>>, field: string): string {
  const value = record[field];
  if (typeof value !== "string" || value.length === 0) throw new Error(`${field} was not a string`);
  return value;
}

function arrayField(record: Readonly<Record<string, unknown>>, field: string): unknown[] {
  const value = record[field];
  if (!Array.isArray(value)) throw new Error(`${field} was not an array`);
  return value;
}

function recordField(record: Readonly<Record<string, unknown>>, field: string): Readonly<Record<string, unknown>> {
  return recordValue(record[field], field);
}

function recordValue(value: unknown, label: string): Readonly<Record<string, unknown>> {
  if (!isRecord(value)) throw new Error(`${label} was not an object`);
  return value;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isMissingFile(error: unknown): boolean {
  return isRecord(error) && error.code === "ENOENT";
}

function metricCount(metrics: MetricCollections): number {
  return metrics.counts.length + metrics.rates.length + metrics.latencies.length + metrics.bandwidth.length + metrics.recoveries.length;
}

function sum(values: readonly number[]): number {
  return values.reduce((total, value) => total + value, 0);
}

function round(value: number): number {
  return Math.round(value * 100) / 100;
}

function capitalize(value: string): string {
  return `${value.slice(0, 1).toUpperCase()}${value.slice(1)}`;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
