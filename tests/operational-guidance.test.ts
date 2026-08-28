import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  completionSemanticsForDomain,
  extractOperationalGuidance,
  observedRate,
} from "../src/operational-guidance.js";
import {
  LATENCY_BUCKET_UPPER_MS,
  mergeLatencyHistograms,
  type LatencyHistogram,
} from "../src/pressure.js";
import type { ConcreteScenario } from "../src/scenario.js";
import { ALL_SCENARIOS } from "../src/suite.js";

test("should_calculate_observed_rates_and_reject_zero_duration", () => {
  assert.equal(observedRate(25, 2_000), 12.5);
  assert.equal(observedRate(25, 0), null);
  assert.equal(observedRate(25, -1), null);
});

test("should_aggregate_bounded_latency_histograms", () => {
  const first = histogram(250, 2, 400);
  const second = histogram(500, 3, 1_200);

  const merged = mergeLatencyHistograms([first, second]);

  assert.equal(merged.count, 5);
  assert.equal(merged.totalMs, 1_600);
  assert.equal(merged.maxMs, 500);
  assert.equal(merged.buckets.reduce((total, value) => total + value, 0), 5);
});

test("should_label_notice_completion_as_publisher_acceptance", () => {
  assert.equal(
    completionSemanticsForDomain("notice"),
    "publisher acceptance, not confirmed fanout",
  );
  assert.equal(completionSemanticsForDomain("queue"), "completed Queue enqueue/reserve/completion loops");
});

test("should_apply_exact_latency_rating_boundaries", async () => {
  const exactQuarter = await pressureGuidance({ p95Ms: 250 });
  const exactHalf = await pressureGuidance({ p95Ms: 500 });
  const aboveHalf = await pressureGuidance({ p95Ms: 1_000 });

  assert.equal(exactQuarter.rating.value, "clear");
  assert.equal(exactHalf.rating.value, "watch");
  assert.equal(aboveHalf.rating.value, "constrained");
});

test("should_rate_same_run_pressure_signals_without_changing_semantics", async () => {
  const cases = [
    [{ failed: 1, errorClasses: { unknown: 1 } }, "constrained"],
    [{ ingressDispatchTimeoutsDelta: 1 }, "constrained"],
    [{ routerBackpressureDelta: 1 }, "constrained"],
    [{ warningCode: "pending-growth" }, "watch"],
    [{ warningCode: "rss-growth" }, "watch"],
    [{ failed: 1, errorClasses: { cancelled: 1 }, shutdownCancellationFailures: 1 }, "clear"],
    [{ ambiguous: 1, errorClasses: { timeout: 1 } }, "clear"],
  ] as const;

  for (const [options, expected] of cases) {
    const guidance = await pressureGuidance(options);
    assert.equal(guidance.rating.value, expected, JSON.stringify(options));
  }
});

test("should_keep_ambiguous_outcomes_and_shutdown_cancellations_visible_without_downgrade", async () => {
  const cancellation = await pressureGuidance({
    failed: 1,
    errorClasses: { cancelled: 1 },
    shutdownCancellationFailures: 1,
  });
  const ambiguous = await pressureGuidance({ ambiguous: 1, errorClasses: { timeout: 1 } });
  const mixed = await pressureGuidance({
    failed: 1,
    ambiguous: 1,
    errorClasses: { cancelled: 1, unknown: 1 },
    shutdownCancellationAmbiguous: 1,
  });

  assert.equal(cancellation.rating.value, "clear");
  assert.equal(cancellation.pressureDomains[0]?.expectedCancellations, 1);
  assert.equal(cancellation.pressureDomains[0]?.errors, 0);
  assert.equal(ambiguous.rating.value, "clear");
  assert.equal(ambiguous.pressureDomains[0]?.ambiguousOutcomes, 1);
  assert.equal(mixed.rating.value, "constrained");
  assert.equal(mixed.pressureDomains[0]?.errors, 1);
});

test("should_treat_legacy_signal_samples_as_expected_shutdown_cancellations", async () => {
  const guidance = await pressureGuidance({
    failed: 4,
    errorClasses: { cancelled: 1, unknown: 3 },
    legacyEvidence: true,
    errorSamples: [
      { class: "cancelled", error: "AbortError: The operation was aborted" },
      { class: "unknown", error: "Error: received SIGTERM" },
      { class: "unknown", error: "Error: received SIGTERM" },
      { class: "unknown", error: "Error: received SIGTERM" },
    ],
  });

  assert.equal(guidance.rating.value, "clear");
  assert.equal(guidance.pressureDomains[0]?.errors, 0);
  assert.equal(guidance.pressureDomains[0]?.expectedCancellations, 4);
});

test("should_extract_metrics_from_every_scenario_fixture", async () => {
  for (const scenario of ALL_SCENARIOS) {
    const directory = await mkdtemp(join(tmpdir(), `fitz-destroyer-${scenario}-`));
    try {
      await writeEvents(directory, fixtureEvents(scenario));
      if (scenario === "domain-pressure" || scenario === "soak") {
        await writeFile(
          join(directory, "pressure-evidence.json"),
          JSON.stringify(pressureEvidence({})),
          "utf8",
        );
      }

      const guidance = await extractOperationalGuidance(directory, scenario, 1_000);

      const metricCount = guidance.counts.length + guidance.rates.length +
        guidance.latencies.length + guidance.bandwidth.length + guidance.recoveries.length;
      assert.ok(metricCount > 0, `${scenario} did not produce operational metrics`);
      assert.equal(
        guidance.rating.value === "not-rated",
        scenario !== "domain-pressure" && scenario !== "soak",
        scenario,
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
});

test("should_report_slow_recipient_bytes_as_bandwidth", async () => {
  // Arrange
  const directory = await mkdtemp(join(tmpdir(), "fitz-destroyer-bandwidth-"));
  await writeEvents(directory, fixtureEvents("saturated-slow-recipient-isolation"));

  // Act
  const guidance = await extractOperationalGuidance(directory, "saturated-slow-recipient-isolation", 1_000);

  // Assert
  assert.equal(guidance.bandwidth[0]?.bytes, 30_720_000);
  assert.equal(guidance.bandwidth[0]?.bytesPerSecond, 30_720_000);
  await rm(directory, { recursive: true, force: true });
});

test("should_degrade_malformed_or_incomplete_evidence_to_not_rated", async () => {
  const eventDirectory = await mkdtemp(join(tmpdir(), "fitz-destroyer-malformed-events-"));
  const pressureDirectory = await mkdtemp(join(tmpdir(), "fitz-destroyer-incomplete-pressure-"));
  try {
    await writeFile(join(eventDirectory, "events.ndjson"), "{not-json}\n", "utf8");
    await writeEvents(pressureDirectory, []);
    await writeFile(
      join(pressureDirectory, "pressure-evidence.json"),
      JSON.stringify({ durationMs: 1_000, requestTimeoutMs: 1_000 }),
      "utf8",
    );

    const eventGuidance = await extractOperationalGuidance(eventDirectory, "notice-fanout", null);
    const pressure = await extractOperationalGuidance(pressureDirectory, "domain-pressure", null);

    assert.equal(eventGuidance.rating.value, "not-rated");
    assert.match(eventGuidance.rating.reasons[0] ?? "", /malformed/u);
    assert.equal(pressure.rating.value, "not-rated");
    assert.match(pressure.rating.reasons[0] ?? "", /incomplete/u);
  } finally {
    await Promise.all([
      rm(eventDirectory, { recursive: true, force: true }),
      rm(pressureDirectory, { recursive: true, force: true }),
    ]);
  }
});

test("should_accept_version_two_events_without_workload_envelope", async () => {
  const directory = await mkdtemp(join(tmpdir(), "fitz-destroyer-version-two-"));
  const countOnlyDirectory = await mkdtemp(join(tmpdir(), "fitz-destroyer-version-two-counts-"));
  try {
    await writeEvents(directory, fixtureEvents("notice-fanout"));
    await writeEvents(countOnlyDirectory, [{ event: "queue_lifecycle_complete", operations: 20 }]);

    const guidance = await extractOperationalGuidance(directory, "notice-fanout", null);
    const countOnly = await extractOperationalGuidance(countOnlyDirectory, "queue-lifecycle", null);

    assert.equal(guidance.workloadDurationMs, 1_000);
    assert.equal(guidance.rates.find(({ key }) => key === "publications")?.valuePerSecond, 20);
    assert.equal(guidance.rating.value, "not-rated");
    assert.equal(countOnly.counts.find(({ key }) => key === "drained-records")?.value, 20);
    assert.equal(countOnly.rates.length, 0);
  } finally {
    await Promise.all([
      rm(directory, { recursive: true, force: true }),
      rm(countOnlyDirectory, { recursive: true, force: true }),
    ]);
  }
});

type PressureOptions = Partial<{
  p95Ms: number;
  failed: number;
  ambiguous: number;
  errorClasses: Readonly<Record<string, number>>;
  shutdownCancellationFailures: number;
  shutdownCancellationAmbiguous: number;
  ingressDispatchTimeoutsDelta: number;
  routerBackpressureDelta: number;
  warningCode: "pending-growth" | "rss-growth";
  legacyEvidence: boolean;
  errorSamples: readonly Readonly<Record<string, string>>[];
}>;

async function pressureGuidance(options: PressureOptions) {
  const directory = await mkdtemp(join(tmpdir(), "fitz-destroyer-pressure-guidance-"));
  try {
    await writeEvents(directory, []);
    await writeFile(
      join(directory, "pressure-evidence.json"),
      JSON.stringify(pressureEvidence(options)),
      "utf8",
    );
    return await extractOperationalGuidance(directory, "domain-pressure", 1_000);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

function pressureEvidence(options: PressureOptions): object {
  const p95Ms = options.p95Ms ?? 1;
  const failed = options.failed ?? 0;
  const ambiguous = options.ambiguous ?? 0;
  const stage = {
    started: 10 + failed + ambiguous,
    succeeded: 10,
    failed,
    ambiguous,
    ...options.legacyEvidence
      ? {}
      : {
          expectedShutdownCancellations: {
            failed: options.shutdownCancellationFailures ?? 0,
            ambiguous: options.shutdownCancellationAmbiguous ?? 0,
          },
        },
    latencyHistogram: histogram(p95Ms, 10, p95Ms * 10),
    errorClasses: options.errorClasses ?? {},
    errorSamples: options.errorSamples ?? [],
  };
  return {
    durationMs: 1_000,
    requestTimeoutMs: 1_000,
    selectedDomains: ["notice"],
    aggregate: {
      notice: {
        succeeded: 10,
        failed,
        stages: {
          publish: stage,
        },
      },
    },
    brokerSummary: {
      ingressDispatchTimeoutsDelta: options.ingressDispatchTimeoutsDelta ?? 0,
      routerBackpressureDelta: options.routerBackpressureDelta ?? 0,
      routerHighLaneBackpressureDelta: 0,
    },
    warnings: options.warningCode === undefined
      ? []
      : [{ code: options.warningCode, message: options.warningCode, details: {} }],
  };
}

function histogram(upperMs: number, count: number, totalMs: number): LatencyHistogram {
  const buckets = LATENCY_BUCKET_UPPER_MS.map(() => 0);
  const index = LATENCY_BUCKET_UPPER_MS.findIndex((upper) => upper === upperMs);
  if (index < 0) throw new Error(`Unsupported fixture bucket ${upperMs}`);
  buckets[index] = count;
  return { count, totalMs, maxMs: upperMs, buckets, overflow: 0 };
}

function fixtureEvents(scenario: ConcreteScenario): object[] {
  if (scenario === "clean-restart" || scenario === "cache-loss") {
    return [
      { event: "client_job_complete", mode: "load", entries: 80, workerElapsedMs: 500, elapsedMs: 600 },
      { event: "fitz_ready", elapsedMs: 100 },
      { event: "fitz_restart_complete", kind: "graceful", elapsedMs: 200 },
      { event: "client_job_complete", mode: "verify", entries: 80, workerElapsedMs: 400, elapsedMs: 500 },
    ];
  }
  if (scenario === "durability-crash-cuts") return [{ event: "durability_crash_cut_iteration_complete", iteration: 1, elapsedMs: 500 }, { event: "durability_crash_cuts_complete", iterations: [{}], domains: durableDomains(), elapsedMs: 1_000 }];
  if (scenario === "queue-overload-recovery") return [{ event: "queue_overload_recovery_complete", attempted: 100, failed: 10, recovered: 90, probeCompleted: 20, elapsedMs: 1_000 }];
  if (scenario === "response-loss") return [{ event: "response_loss_complete", attempted: 4, acknowledgedAfterDrop: 0, observedAfterDrop: 2, elapsedMs: 1_000 }];
  if (scenario === "active-graceful-shutdown") return [{ event: "active_graceful_shutdown_complete", durableOperationsStarted: 4, rpcCallsInterrupted: 1, probeFrames: 16, elapsedMs: 1_000 }];
  if (scenario === "half-open-session") return [{ event: "half_open_session_complete", staleRejections: 4, queueRedelivered: 1, leaseReacquired: 1, elapsedMs: 1_000 }];
  if (scenario === "authorization-isolation") return [{ event: "authorization_isolation_complete", identities: 2, ownRouteOperations: 4, deniedOperations: 4, elapsedMs: 1_000 }];
  if (scenario === "stream-global-recovery") return [{ event: "stream_global_recovery_complete", loaded: 40, recovered: 40, pages: 6, elapsedMs: 1_000 }];
  if (scenario === "queue-dead-letter-fencing") return [{ event: "queue_dead_letter_fencing_complete", oversizedRejected: 1, staleCompletionRejected: 1, redelivered: 1, completed: 1, elapsedMs: 1_000 }];
  if (scenario === "cold-boot-provider-outage") return [{ event: "cold_boot_provider_outage_complete", readinessChecks: 5, readyResponsesDuringOutage: 0, recoveryMs: 500, elapsedMs: 1_000 }];
  if (scenario === "hostile-rpc-worker") {
    return [{
      event: "hostile_rpc_worker_complete",
      returnWithoutTerminalFailures: 1,
      returnWithoutTerminalFrames: 0,
      thrownHandlerFailures: 0,
      thrownHandlerFrames: 1,
      probeFrames: 2,
      elapsedMs: 1_000,
    }];
  }
  if (scenario === "upgrade-recovery") return [{ event: "upgrade_recovery_complete", loaded: 80, verified: 80, replacementMs: 200, elapsedMs: 1_000 }];
  if (scenario === "cross-transport-recovery") return [{ event: "cross_transport_recovery_complete", websocketToTcpVerified: 80, tcpToWebsocketVerified: 80, elapsedMs: 1_000 }];
  if (scenario === "outbound-blackhole") return [{ event: "outbound_blackhole_complete", staleRejections: 4, queueRedelivered: 1, leaseReacquired: 1, elapsedMs: 1_000 }];
  if (scenario === "broker-pause") return [{ event: "broker_pause_complete", pausedMs: 6_000, staleRejections: 4, queueRedelivered: 1, leaseReacquired: 1, elapsedMs: 7_000 }];
  if (scenario === "route-cardinality-churn") return [{ event: "route_cardinality_churn_scenario_complete", routes: 140, recoveryProbeOperations: 7, elapsedMs: 1_000 }];
  if (scenario === "cache-and-disk-exhaustion") return [{ event: "cache_and_disk_exhaustion_complete", rejectedMutations: 2, verified: 80, elapsedMs: 1_000 }];
  if (scenario === "queue-redelivery") return [{ event: "queue_redelivery_complete", produced: 20, recovered: 20, elapsedMs: 1_000 }];
  if (scenario === "lease-contention") return [{ event: "lease_contention_complete", contenderAdmissions: 20, elapsedMs: 1_000 }];
  if (scenario === "hot-route-canary") return [{ event: "hot_route_canary_complete", hotTotals: { queue: { success: 10, error: 0 } }, canaryMaximumMs: { queue: 5 }, elapsedMs: 1_000 }];
  if (scenario === "protocol-abuse") return [{ event: "protocol_abuse_scenario_complete", attacks: 10, canaryMaximumMs: { queue: 5 }, elapsedMs: 1_000 }];
  if (scenario === "notice-fanout") return [{ event: "notice_fanout_started", payloadBytes: 256 }, { event: "notice_fanout_complete", publications: 20, deliveries: 80, elapsedMs: 1_000 }];
  if (scenario === "schedule-delivery") return [{ event: "schedule_delivery_complete", broadcastDeliveries: 80, singleDeliveries: 20, maxLatenessMs: 12, elapsedMs: 1_000 }];
  if (scenario === "session-boundaries") return [{ event: "session_boundaries_complete", staleRejections: 4, queueRedelivered: 1, queueCompleted: 1, leaseReacquired: 1, elapsedMs: 1_000 }];
  if (scenario === "rpc-pressure") return [{ event: "rpc_pressure_started", payloadBytes: 256 }, { event: "rpc_pressure_complete", calls: 20, responseFrames: 40, elapsedMs: 1_000 }];
  if (scenario === "rpc-stream-hose") return [{ event: "rpc_stream_success_phase_complete", label: "rpc-stream-full", completed: 2, responseFrames: 200, responseBytes: 204_800, elapsedMs: 1_000 }, { event: "rpc_stream_fault_phase_complete", label: "worker-kill", elapsedMs: 100 }, { event: "rpc_stream_hose_complete", elapsedMs: 1_500 }];
  if (scenario === "connection-storm") return [{ event: "connection_storm_started", totalConnectionLifecycles: 16 }, { event: "notice_fanout_complete", runLabel: "connection-storm-wave-001", deliveries: 20 }, { event: "rpc_pressure_complete", runLabel: "connection-storm-wave-001", calls: 20 }, { event: "connection_storm_wave_complete", wave: 1, elapsedMs: 900 }, { event: "connection_storm_complete", elapsedMs: 1_000 }];
  if (scenario === "chaos") return [{ event: "chaos_fault_recovery_complete", fault: "fitz-sigkill", elapsedMs: 100 }, { event: "chaos_complete", iterations: 1, elapsedMs: 1_000 }];
  if (scenario === "storage-faults") return [{ event: "storage_fault_iteration_complete", iteration: 1, fault: "connection-reset", elapsedMs: 500 }, { event: "storage_faults_complete", iterations: 1, domains: durableDomains(), elapsedMs: 1_000 }];
  if (scenario === "queue-lifecycle") return [{ event: "queue_lifecycle_complete", operations: 20, elapsedMs: 1_000 }];
  if (scenario === "schedule-outage") return [{ event: "schedule_outage_complete", repeatedSequences: [0, 1], raceSequences: [0], missedDeliveries: 0, elapsedMs: 1_000 }];
  if (scenario === "transaction-contention") return [{ event: "transaction_contention_complete", conflicts: 1, elapsedMs: 1_000 }];
  if (scenario === "stream-replay") return [{ event: "stream_replay_complete", records: 20, pages: 2, boundaryBytes: 60_000, elapsedMs: 1_000 }];
  if (scenario === "live-churn") return [{ event: "live_churn_complete", phases: [{ phase: "notice-rpc", elapsedMs: 400 }], elapsedMs: 1_000 }];
  if (scenario === "lease-route-aliasing") return [{ event: "lease_route_aliasing_complete", operations: 6, rejected: 6, canonicalPreserved: 6, elapsedMs: 1_000 }];
  if (scenario === "tcp-preauth-framing-slowloris") return [{ event: "tcp_preauth_framing_slowloris_complete", socketsOpened: 16, socketsClosed: 16, secondWaveAdmitted: 8, tcpCanary: 1, websocketCanary: 1, elapsedMs: 1_000 }];
  if (scenario === "connect-pipeline-family-rebind") return [{ event: "connect_pipeline_family_rebind_complete", transports: 2, accepted: 1, rejected: 1, elapsedMs: 1_000 }];
  if (scenario === "ephemeral-reply-loss-cleanup") return [{ event: "ephemeral_reply_loss_cleanup_complete", lostReplies: 12, queueRedelivered: 1, kvTransactions: 1, streamSessions: 1, noticeDeliveries: 1, scheduleSubscriptions: 1, leaseRoutesReacquired: 2, rpcCallsCompleted: 1, elapsedMs: 1_000 }];
  if (scenario === "saturated-slow-recipient-isolation") return [{ event: "saturated_slow_recipient_isolation_complete", published: 512, received: 512, bytesPublished: 30_720_000, siblingCanaryDomains: 7, elapsedMs: 1_000 }];
  if (scenario === "shutdown-reconnect-cleanup-storm") return [{ event: "shutdown_reconnect_cleanup_storm_complete", cycles: 2, reconnects: 8, staleHandleRejections: 32, elapsedMs: 1_000 }];
  if (scenario === "control-lane-cleanup-under-saturation") return [{ event: "control_lane_cleanup_under_saturation_complete", targets: 4, canaryOperationsPerDomain: 1, elapsedMs: 1_000 }];
  if (scenario === "route-family-isolation-matrix") return [{ event: "route_family_isolation_matrix_complete", identities: 2, domains: 7, holderDomainChecks: 14, probeDomainChecks: 14, crossFamilyDeliveries: 0, elapsedMs: 1_000 }];
  if (scenario === "rpc-response-state-conformance") return [{ event: "rpc_response_state_conformance_complete", cases: 5, callersTerminated: 4, duplicateCallerTerminals: 0, unknownCorrelationRejected: 1, duplicateTerminalRejected: 1, postCancelResponsesObserved: 1, postDisconnectRejected: 1, healthyCalls: 4, healthyFailures: 0, elapsedMs: 1_000 }];
  if (scenario === "response-envelope-boundaries") return [{ event: "response_envelope_boundaries_complete", domains: 7, exactFit: 7, oneOverRejected: 3, boundedAggregates: 2, canaryOperations: 7, elapsedMs: 1_000 }];
  if (scenario === "lease-waiter-disconnect-races") return [{ event: "lease_waiter_disconnect_races_complete", rounds: 8, waitersQueued: 32, waitersDisconnected: 32, ghostAcquisitions: 0, pendingWaiters: 0, replacementAcquisitions: 8, fencingRegressions: 0, elapsedMs: 1_000 }];
  if (scenario === "wildcard-registration-quota-reclamation") return [{ event: "wildcard_registration_quota_reclamation_complete", domains: 6, registrations: 1536, limitRejections: 12, unsubscribeReclaims: 6, disconnectReclaims: 6, canaryFailures: 0, elapsedMs: 1_000 }];
  if (scenario === "stream-selector-cursor-conformance") return [{ event: "stream_selector_cursor_conformance_complete", selectors: 8, recordsWritten: 8, visibleRecords: 21, filteredOffsets: 21, cursorAdvances: 42, duplicateRecords: 0, missingRecords: 0, reconnectContinuations: 1, elapsedMs: 1_000 }];
  if (scenario === "same-shard-family-fairness") return [{ event: "same_shard_family_fairness_complete", noisyCompleted: 2_048, canariesAttempted: 32, canariesCompleted: 32, canaryErrors: 0, longestCanaryMs: 10, requestTimeoutMs: 1_000, elapsedMs: 1_000 }];
  if (scenario === "actor-supervision-failpoint") return [{ event: "actor_supervision_failpoint_complete", domainsInjected: 7, correlatedDomainsInjected: 7, activeFaultClients: 4, activeFaultErrors: 4, readinessWithdrawals: 8, restartsRecovered: 8, canaryDeliveries: 4, queueRecovered: 1, kvRecovered: 1, leaseRecovered: 1, scheduleRecovered: 1, streamRecovered: 1, rpcRecovered: 1, correlatedRecoveryOperations: 7, elapsedMs: 1_000 }];
  if (scenario === "family-actor-partial-failure-isolation") return [{ event: "family_actor_partial_failure_isolation_complete", targetedFamilies: 2, failedFamilyRejections: 2, siblingOperations: 2, readinessChecks: 2, crossFamilyDeliveries: 0, elapsedMs: 1_000 }];
  return [];
}

function durableDomains(): object {
  return {
    queue: { observed: [0] },
    kv: { observed: [0] },
    stream: { observed: [0] },
    schedule: { observed: [0] },
  };
}

async function writeEvents(directory: string, events: readonly object[]): Promise<void> {
  await writeFile(
    join(directory, "events.ndjson"),
    `${events.map((event) => JSON.stringify(event)).join("\n")}\n`,
    "utf8",
  );
}
