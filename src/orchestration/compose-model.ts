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
  | "exhaustion-probe";

export type RoleContainer = {
  id: string;
  name: string;
  workerId: string;
};

export type ContainerState = {
  status: string;
  exitCode: number;
};
