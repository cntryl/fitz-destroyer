import assert from "node:assert/strict";
import test from "node:test";
import {
  cleanupDelta,
  cleanupMetrics,
  isLiveDomainQuiescent,
} from "../src/orchestration/live-observability.js";

test("should_extract_cleanup_metrics_and_compute_a_wave_delta", () => {
  // Arrange
  const metrics = {
    samples: [
      { name: "fitz_session_cleanup_failures_total", value: 3 },
      { name: "fitz_session_cleanup_successes_total", value: 2 },
      { name: "fitz_session_cleanup_pending", value: 0 },
    ],
  };
  const before = { failures: 1, retries: 0, successes: 1, pending: 0, oldestAgeMs: 0 };

  // Act
  const after = cleanupMetrics(metrics);
  const delta = cleanupDelta(before, after);

  // Assert
  assert.deepEqual(delta, {
    failures: 2,
    retries: 0,
    successes: 1,
    pending: 0,
    oldestAgeMs: 0,
  });
});

test("should_require_domain_state_and_cleanup_queue_to_quiesce", () => {
  // Arrange
  const clean = {
    domain: { workers_registered: 0, requests_pending: 0, pending_routes_active: 0 },
    cleanup: { failures: 0, retries: 0, successes: 0, pending: 0, oldestAgeMs: 0 },
  };
  const pendingCleanup = { ...clean, cleanup: { ...clean.cleanup, pending: 1 } };

  // Act
  const cleanResult = isLiveDomainQuiescent("rpc", clean);
  const pendingResult = isLiveDomainQuiescent("rpc", pendingCleanup);

  // Assert
  assert.equal(cleanResult, true);
  assert.equal(pendingResult, false);
});

test("should_require_schedule_definitions_subscriptions_and_claims_to_drain", () => {
  // Arrange
  const cleanup = { failures: 0, retries: 0, successes: 0, pending: 0, oldestAgeMs: 0 };
  const clean = {
    domain: {
      schedules_active: 0,
      subscriptions_active: 0,
      pending_fire_claims: 0,
      pending_ack_retries: 0,
    },
    cleanup,
  };
  const active = { ...clean, domain: { ...clean.domain, pending_fire_claims: 1 } };

  // Act
  const cleanResult = isLiveDomainQuiescent("schedule", clean);
  const activeResult = isLiveDomainQuiescent("schedule", active);

  // Assert
  assert.equal(cleanResult, true);
  assert.equal(activeResult, false);
});
