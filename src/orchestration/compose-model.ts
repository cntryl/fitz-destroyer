export type LiveRole =
  | "durability-verifier"
  | "durability-writer"
  | "lease-contender"
  | "lease-owner"
  | "lease-probe"
  | "hot-route"
  | "canary"
  | "protocol-abuse"
  | "notice-publisher"
  | "notice-subscriber"
  | "schedule-producer"
  | "schedule-subscriber"
  | "session-boundaries"
  | "queue-redelivery-producer"
  | "queue-redelivery-victim"
  | "queue-redelivery-drainer"
  | "rpc-caller"
  | "rpc-worker"
  | "rpc-stream-caller"
  | "rpc-stream-worker"
  | "pressure-reconciler"
  | "queue-lifecycle-producer"
  | "queue-lifecycle-abandoner"
  | "queue-lifecycle-consumer"
  | "transaction-contender"
  | "transaction-holder"
  | "transaction-verifier"
  | "stream-replay-worker"
  | "schedule-outage-producer"
  | "schedule-outage-canceller"
  | "schedule-outage-cleanup"
  | "schedule-outage-subscriber"
  | "queue-overload-producer"
  | "queue-overload-drainer"
  | "authorization-isolation"
  | "stream-global-recovery"
  | "queue-dead-letter-fencing"
  | "hostile-rpc-worker"
  | "hostile-rpc-caller"
  | "route-cardinality-churn"
  | "exhaustion-probe"
  | "wire-conformance"
  | "ephemeral-reply-loss-preparer"
  | "ephemeral-reply-loss-victim"
  | "ephemeral-reply-loss-verifier"
  | "slow-recipient"
  | "slow-recipient-observer"
  | "slow-recipient-publisher"
  | "shutdown-reconnect-cleanup-storm"
  | "control-lane-cleanup-under-saturation"
  | "route-family-isolation-matrix"
  | "rpc-response-state-conformance"
  | "response-envelope-boundaries"
  | "lease-waiter-disconnect-races"
  | "wildcard-registration-quota-reclamation"
  | "stream-selector-cursor-conformance"
  | "same-shard-family-fairness"
  | "family-actor-partial-failure-isolation"
  | "same-shard-family-failure-isolation";

export type RoleContainer = {
  id: string;
  name: string;
  workerId: string;
};

export function roleContainerName(
  project: string,
  role: LiveRole,
  wave: number,
  replica: number,
): string {
  return `${project}-${role}-${wave.toString().padStart(3, "0")}-${replica.toString().padStart(3, "0")}`;
}

export type ContainerState = {
  status: string;
  exitCode: number;
};
