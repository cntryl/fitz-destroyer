import assert from "node:assert/strict";
import test from "node:test";
import { ephemeralReplyLossRoutes } from "../src/workloads/ephemeral-reply-loss.js";
import {
  assertEphemeralReplyLossDispatch,
  assertEphemeralReplyLossEvidence,
  isEphemeralStateArmed,
  isEphemeralDomainQuiescent,
  isQueueResourceQuiescent,
} from "../src/orchestration/ephemeral-reply-loss-cleanup.js";
import { slowRecipientSaturationShape } from "../src/workloads/slow-recipient-isolation.js";
import { assertSlowRecipientEvidence } from "../src/orchestration/saturated-slow-recipient-isolation.js";

test("should_use_distinct_concrete_routes_for_every_reply_loss_handle", () => {
  // Arrange
  const namespace = "run-42";

  // Act
  const routes = ephemeralReplyLossRoutes(namespace);

  // Assert
  assert.equal(new Set(Object.values(routes)).size, Object.values(routes).length);
  assert.ok(Object.values(routes).every((route) => route.includes(namespace)));
  assert.ok(Object.values(routes).every((route) => !route.includes("*")));
});

test("should_require_every_ephemeral_reply_loss_probe_to_recover", () => {
  // Arrange
  const valid = JSON.stringify({
    event: "ephemeral_reply_loss_verifier_complete",
    queueRedelivered: 1,
    queueWatchDeliveries: 1,
    kvTransactions: 1,
    kvWatchDeliveries: 1,
    streamSessions: 1,
    streamWatchDeliveries: 1,
    noticeDeliveries: 1,
    scheduleSubscriptions: 1,
    leaseRoutesReacquired: 2,
    leaseWatchDeliveries: 1,
    rpcCallsCompleted: 1,
  });

  // Act
  const complete = assertEphemeralReplyLossEvidence(valid);

  // Assert
  assert.equal(complete.queueRedelivered, 1);
  assert.throws(
    () => assertEphemeralReplyLossEvidence(valid.replace('"rpcCallsCompleted":1', '"rpcCallsCompleted":0')),
    /rpcCallsCompleted=0\/1/u,
  );
});

test("should_require_reply_loss_victims_to_receive_no_handle_replies", () => {
  // Arrange
  const fullVictim = JSON.stringify({
    event: "ephemeral_reply_loss_dispatched",
    requests: 11,
    repliesReceived: 0,
  });
  const leaseVictim = JSON.stringify({
    event: "ephemeral_reply_loss_dispatched",
    requests: 1,
    repliesReceived: 0,
  });

  // Act
  const total = assertEphemeralReplyLossDispatch(fullVictim) +
    assertEphemeralReplyLossDispatch(leaseVictim);

  // Assert
  assert.equal(total, 12);
  assert.throws(
    () => assertEphemeralReplyLossDispatch(fullVictim.replace('"repliesReceived":0', '"repliesReceived":1')),
    /escaped the response-loss fault/u,
  );
});

test("should_require_queue_reservations_subscriptions_and_cleanup_work_to_drain", () => {
  // Arrange
  const clean = {
    domain: { inflight_active: 0 },
    cleanup: { failures: 0, retries: 0, successes: 1, pending: 0, oldestAgeMs: 0 },
  };

  // Act
  const quiescent = isEphemeralDomainQuiescent("queue", clean);

  // Assert
  assert.equal(quiescent, true);
  assert.equal(
    isEphemeralDomainQuiescent("queue", {
      ...clean,
      domain: { ...clean.domain, inflight_active: 1 },
    }),
    false,
  );
  assert.equal(
    isEphemeralDomainQuiescent("queue", {
      ...clean,
      cleanup: { ...clean.cleanup, pending: 1 },
    }),
    false,
  );
  assert.equal(
    isQueueResourceQuiescent({ messages_inflight: 0, subscriptions_active: 0 }),
    true,
  );
  assert.equal(
    isQueueResourceQuiescent({ messages_inflight: 0, subscriptions_active: 1 }),
    false,
  );
});

test("should_require_server_side_state_before_cutting_reply_loss_victims", () => {
  // Arrange
  const armed = {
    queue: { inflight_active: 1 },
    kv: { transactions_active: 1 },
    stream: { append_sessions_active: 1, subscriptions_active: 1 },
    notice: { subscriptions_active: 1 },
    rpc: { workers_registered: 1 },
    lease: { leases_active: 2, waiter_depth: 1 },
    schedule: { subscriptions_active: 1 },
    queueResource: { messages_inflight: 1, subscriptions_active: 1 },
  };

  // Act
  const observed = isEphemeralStateArmed(armed);

  // Assert
  assert.equal(observed, true);
  assert.equal(
    isEphemeralStateArmed({ ...armed, lease: { leases_active: 2, waiter_depth: 0 } }),
    false,
  );
  assert.equal(
    isEphemeralStateArmed({
      ...armed,
      queueResource: { messages_inflight: 1, subscriptions_active: 0 },
    }),
    false,
  );
});

test("should_scale_slow_recipient_pressure_to_exceed_socket_buffers", () => {
  // Arrange
  const smokeOperations = 8;
  const smokePayloadBytes = 256;

  // Act
  const shape = slowRecipientSaturationShape(smokeOperations, smokePayloadBytes);

  // Assert
  assert.ok(shape.operations >= 256);
  assert.ok(shape.payloadBytes >= 60_000);
  assert.ok(shape.operations * shape.payloadBytes >= 15_000_000);
  assert.equal(slowRecipientSaturationShape(8, 8_000_000).payloadBytes, 60_000);
});

test("should_require_the_healthy_recipient_to_observe_every_accepted_notice", () => {
  // Arrange
  const publisher = JSON.stringify({
    event: "slow_recipient_publisher_complete",
    published: 300,
  });
  const observer = JSON.stringify({
    event: "slow_recipient_observer_complete",
    received: 300,
    duplicates: 0,
    invalid: 0,
  });

  // Act
  const evidence = assertSlowRecipientEvidence(publisher, observer);

  // Assert
  assert.equal(evidence.published, 300);
  assert.equal(evidence.received, 300);
  assert.throws(
    () => assertSlowRecipientEvidence(publisher, observer.replace('"received":300', '"received":299')),
    /received=299\/300/u,
  );
});
