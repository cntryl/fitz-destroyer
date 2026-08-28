import type { ConcreteScenario } from "./scenario.js";
import type { EventRecord, MetricCollections } from "./operational-guidance.js";

type CompletionConfig = {
  event: string;
  fields: readonly (readonly [string, string])[];
  bandwidthFields?: readonly (readonly [string, string])[];
  recoveryField?: string;
};

const CONFIGS: Partial<Record<ConcreteScenario, CompletionConfig>> = {
  "queue-overload-recovery": {
    event: "queue_overload_recovery_complete",
    fields: [["attempted", "Queue attempts"], ["failed", "Bounded failures"], ["recovered", "Recovered records"], ["probeCompleted", "Post-overload probe completions"]],
  },
  "response-loss": {
    event: "response_loss_complete",
    fields: [["attempted", "Durable attempts"], ["observedAfterDrop", "Reconciled ambiguous outcomes"]],
  },
  "active-graceful-shutdown": {
    event: "active_graceful_shutdown_complete",
    fields: [["durableOperationsStarted", "Durable operations started"], ["rpcCallsInterrupted", "RPC calls interrupted"], ["probeFrames", "Post-shutdown probe frames"]],
  },
  "half-open-session": {
    event: "half_open_session_complete",
    fields: [["staleRejections", "Stale-handle rejections"], ["queueRedelivered", "Queue redeliveries"], ["leaseReacquired", "Lease reacquisitions"]],
  },
  "authorization-isolation": {
    event: "authorization_isolation_complete",
    fields: [["identities", "Isolated identities"], ["ownRouteOperations", "Own-route operations"], ["deniedOperations", "Denied cross-realm operations"]],
  },
  "stream-global-recovery": {
    event: "stream_global_recovery_complete",
    fields: [["loaded", "Loaded global records"], ["recovered", "Recovered global records"], ["pages", "Replay pages"]],
  },
  "queue-dead-letter-fencing": {
    event: "queue_dead_letter_fencing_complete",
    fields: [["oversizedRejected", "Undeliverable bodies rejected"], ["staleCompletionRejected", "Stale completions rejected"], ["redelivered", "Records redelivered"], ["completed", "Records completed"]],
  },
  "cold-boot-provider-outage": {
    event: "cold_boot_provider_outage_complete",
    fields: [["readinessChecks", "Outage readiness checks"], ["readyResponsesDuringOutage", "Incorrect ready responses"]],
    recoveryField: "recoveryMs",
  },
  "hostile-rpc-worker": {
    event: "hostile_rpc_worker_complete",
    fields: [
      ["returnWithoutTerminalFailures", "Missing-terminal timeouts"],
      ["returnWithoutTerminalFrames", "Missing-terminal frames"],
      ["thrownHandlerFailures", "Thrown-handler caller failures"],
      ["thrownHandlerFrames", "Thrown-handler terminal frames"],
      ["probeFrames", "Healthy probe frames"],
    ],
  },
  "upgrade-recovery": {
    event: "upgrade_recovery_complete",
    fields: [["loaded", "Pre-replacement records"], ["verified", "Post-replacement records"]],
    recoveryField: "replacementMs",
  },
  "cross-transport-recovery": {
    event: "cross_transport_recovery_complete",
    fields: [["websocketToTcpVerified", "WebSocket to TCP records"], ["tcpToWebsocketVerified", "TCP to WebSocket records"]],
  },
  "outbound-blackhole": {
    event: "outbound_blackhole_complete",
    fields: [["staleRejections", "Stale-handle rejections"], ["queueRedelivered", "Queue redeliveries"], ["leaseReacquired", "Lease reacquisitions"]],
  },
  "broker-pause": {
    event: "broker_pause_complete",
    fields: [["staleRejections", "Stale-handle rejections"], ["queueRedelivered", "Queue redeliveries"], ["leaseReacquired", "Lease reacquisitions"]],
  },
  "route-cardinality-churn": {
    event: "route_cardinality_churn_scenario_complete",
    fields: [["routes", "Unique route operations"], ["recoveryProbeOperations", "Recovery probe operations"]],
  },
  "cache-and-disk-exhaustion": {
    event: "cache_and_disk_exhaustion_complete",
    fields: [["rejectedMutations", "Exhaustion rejections"], ["verified", "Recovered baseline records"]],
  },
  "lease-route-aliasing": {
    event: "lease_route_aliasing_complete",
    fields: [["operations", "Trailing-route operations"], ["rejected", "Rejected trailing routes"], ["canonicalPreserved", "Canonical leases preserved"]],
  },
  "tcp-preauth-framing-slowloris": {
    event: "tcp_preauth_framing_slowloris_complete",
    fields: [["socketsOpened", "Pre-auth sockets opened"], ["socketsClosed", "Pre-auth sockets closed"], ["secondWaveAdmitted", "Second-wave admissions"], ["tcpCanary", "TCP canaries"], ["websocketCanary", "WebSocket canaries"]],
  },
  "connect-pipeline-family-rebind": {
    event: "connect_pipeline_family_rebind_complete",
    fields: [["transports", "Combined-frame transports"], ["accepted", "Atomically accepted pipelines"], ["rejected", "Atomically rejected pipelines"]],
  },
  "ephemeral-reply-loss-cleanup": {
    event: "ephemeral_reply_loss_cleanup_complete",
    fields: [["lostReplies", "Lost setup replies"], ["queueRedelivered", "Queue reservations recovered"], ["kvTransactions", "KV transactions recovered"], ["streamSessions", "Stream sessions recovered"], ["noticeDeliveries", "Notice subscriptions recovered"], ["scheduleSubscriptions", "Schedule subscriptions recovered"], ["leaseRoutesReacquired", "Lease routes reacquired"], ["rpcCallsCompleted", "RPC workers recovered"]],
  },
  "saturated-slow-recipient-isolation": {
    event: "saturated_slow_recipient_isolation_complete",
    fields: [["published", "Notice publications"], ["received", "Healthy deliveries"], ["siblingCanaryDomains", "Sibling canary domains"]],
    bandwidthFields: [["bytesPublished", "Notice bytes published"]],
  },
  "shutdown-reconnect-cleanup-storm": {
    event: "shutdown_reconnect_cleanup_storm_complete",
    fields: [["cycles", "Shutdown cycles"], ["reconnects", "Client reconnects"], ["staleHandleRejections", "Stale handles rejected"]],
  },
  "control-lane-cleanup-under-saturation": {
    event: "control_lane_cleanup_under_saturation_complete",
    fields: [["targets", "Cleanup targets"], ["canaryOperationsPerDomain", "Canary operations per domain"]],
  },
  "route-family-isolation-matrix": {
    event: "route_family_isolation_matrix_complete",
    fields: [["identities", "Route families"], ["domains", "Isolated domains"], ["holderDomainChecks", "Holder domain checks"], ["probeDomainChecks", "Probe domain checks"], ["crossFamilyDeliveries", "Cross-family deliveries"]],
  },
  "rpc-response-state-conformance": {
    event: "rpc_response_state_conformance_complete",
    fields: [["cases", "Response-state cases"], ["callersTerminated", "Callers terminated"], ["healthyCalls", "Healthy follow-up calls"], ["healthyFailures", "Healthy follow-up failures"]],
  },
  "response-envelope-boundaries": {
    event: "response_envelope_boundaries_complete",
    fields: [["domains", "Boundary domains"], ["exactFit", "Exact-fit responses"], ["oneOverRejected", "One-over rejections"], ["boundedAggregates", "Bounded aggregate responses"], ["canaryOperations", "Canary operations"]],
  },
  "lease-waiter-disconnect-races": {
    event: "lease_waiter_disconnect_races_complete",
    fields: [["rounds", "Race rounds"], ["waitersQueued", "Waiters queued"], ["waitersDisconnected", "Waiters disconnected"], ["replacementAcquisitions", "Replacement acquisitions"], ["ghostAcquisitions", "Ghost acquisitions"], ["fencingRegressions", "Fencing regressions"]],
  },
  "wildcard-registration-quota-reclamation": {
    event: "wildcard_registration_quota_reclamation_complete",
    fields: [["domains", "Quota domains"], ["registrations", "Registrations"], ["limitRejections", "Limit rejections"], ["unsubscribeReclaims", "Unsubscribe reclaims"], ["disconnectReclaims", "Disconnect reclaims"], ["canaryFailures", "Canary failures"]],
  },
  "stream-selector-cursor-conformance": {
    event: "stream_selector_cursor_conformance_complete",
    fields: [["selectors", "Selector shapes"], ["recordsWritten", "Records written"], ["visibleRecords", "Visible records"], ["filteredOffsets", "Filtered offsets"], ["cursorAdvances", "Cursor advances"], ["reconnectContinuations", "Reconnect continuations"], ["duplicateRecords", "Duplicate records"], ["missingRecords", "Missing records"]],
  },
  "same-shard-family-fairness": {
    event: "same_shard_family_fairness_complete",
    fields: [["noisyCompleted", "Noisy-family operations"], ["canariesAttempted", "Sibling canaries attempted"], ["canariesCompleted", "Sibling canaries completed"], ["canaryErrors", "Sibling canary errors"]],
  },
  "actor-supervision-failpoint": {
    event: "actor_supervision_failpoint_complete",
    fields: [["domainsInjected", "Single-domain actor failpoints"], ["correlatedDomainsInjected", "Correlated domain actor failpoints"], ["readinessWithdrawals", "Readiness withdrawals"], ["restartsRecovered", "Recovered restarts"], ["canaryDeliveries", "Post-restart Notice deliveries"], ["queueRecovered", "Post-restart Queue completions"], ["kvRecovered", "Post-restart KV canaries"], ["leaseRecovered", "Post-restart Lease canaries"], ["scheduleRecovered", "Post-restart Schedule canaries"], ["streamRecovered", "Post-restart Stream canaries"], ["rpcRecovered", "Post-restart RPC canaries"], ["correlatedRecoveryOperations", "Post-correlated-failure domain canaries"]],
  },
};

export function completionMetricsForScenario(
  scenario: ConcreteScenario,
  events: readonly EventRecord[],
): MetricCollections | null {
  const config = CONFIGS[scenario];
  if (config === undefined) return null;
  const complete = events.findLast(({ event }) => event === config.event);
  if (complete === undefined) throw new Error(`Missing ${config.event} event`);
  const durationMs = duration(complete.elapsedMs);
  const semantics = "verified fault and recovery outcomes";
  const counts = config.fields.map(([key, label]) => ({
    kind: "count" as const,
    key,
    label,
    value: count(complete[key], key),
    unit: "outcomes",
    completionSemantics: semantics,
  }));
  const rates = durationMs === null || durationMs === 0
    ? []
    : counts.map(({ key, label, value, unit, completionSemantics }) => ({
        kind: "rate" as const,
        key,
        label,
        count: value,
        durationMs,
        valuePerSecond: Math.round((value / (durationMs / 1_000)) * 100) / 100,
        unit: `${unit}/s`,
        completionSemantics,
      }));
  const recoveries = durationMs === null
    ? []
    : [{ kind: "recovery" as const, key: "scenario-recovery", label: "Fault injection through recovery verification", durationMs }];
  if (config.recoveryField !== undefined) {
    recoveries.push({
      kind: "recovery",
      key: config.recoveryField,
      label: "Provider restoration to verified recovery",
      durationMs: count(complete[config.recoveryField], config.recoveryField),
    });
  }
  const bandwidth = durationMs === null
    ? []
    : (config.bandwidthFields ?? []).map(([key, label]) => {
        const bytes = count(complete[key], key);
        return {
          kind: "bandwidth" as const,
          key,
          label,
          bytes,
          durationMs,
          bytesPerSecond: Math.round((bytes / (durationMs / 1_000)) * 100) / 100,
          completionSemantics: semantics,
        };
      });
  return {
    completionSemantics: counts.map(({ key }) => ({ key, label: semantics })),
    counts,
    rates,
    latencies: [],
    bandwidth,
    recoveries,
  };
}

function count(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${field} was not a non-negative integer`);
  }
  return value;
}

function duration(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}
