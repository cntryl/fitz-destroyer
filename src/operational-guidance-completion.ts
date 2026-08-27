import type { ConcreteScenario } from "./scenario.js";
import type { EventRecord, MetricCollections } from "./operational-guidance.js";

type CompletionConfig = {
  event: string;
  fields: readonly (readonly [string, string])[];
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
  return {
    completionSemantics: counts.map(({ key }) => ({ key, label: semantics })),
    counts,
    rates,
    latencies: [],
    bandwidth: [],
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
