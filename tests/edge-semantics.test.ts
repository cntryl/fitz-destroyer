import assert from "node:assert/strict";
import test from "node:test";
import {
  analyzeQueueLifecycle,
  assertQueueLifecycle,
} from "../src/orchestration/queue-lifecycle.js";
import {
  analyzeTransactionContention,
  assertTransactionContention,
} from "../src/orchestration/transaction-contention.js";
import { assertStreamReplay } from "../src/orchestration/stream-replay.js";
import {
  assertScheduleOutage,
  nextConsecutiveMinutePair,
  nextWholeMinute,
  type ScheduleOutageLedger,
} from "../src/orchestration/schedule-outage.js";
import { consecutiveMinuteCronAt } from "../src/workloads/schedule-outage.js";
import { boundaryPayload } from "../src/workloads/stream-replay.js";

test("should_reconcile_partial_completion_abandonment_backlog_and_consumer_fairness", () => {
  const producer = new Map([
    ["0", JSON.stringify({ event: "queue_lifecycle_producer_complete", completed: [0, 1, 2, 3] })],
  ]);
  const abandoner = new Map([
    ["0", JSON.stringify({ event: "queue_lifecycle_abandoned", sequence: 4 })],
  ]);
  const consumers = new Map([
    ["0", JSON.stringify({ event: "queue_lifecycle_consumer_complete", sequences: [4, 5] })],
    ["1", JSON.stringify({ event: "queue_lifecycle_consumer_complete", sequences: [6, 7] })],
  ]);

  const ledger = analyzeQueueLifecycle(producer, abandoner, consumers, 8);

  assert.doesNotThrow(() => assertQueueLifecycle(ledger, 2));
});

test("should_reject_a_queue_lifecycle_consumer_that_never_makes_progress", () => {
  assert.throws(
    () =>
      assertQueueLifecycle(
        {
          operations: 4,
          producerCompleted: [0, 1],
          abandoned: 2,
          consumers: { first: [2, 3], second: [] },
        },
        2,
      ),
    /fairness failed/,
  );
});

test("should_require_one_kv_conflict_and_cleanup_of_long_lived_transactions", () => {
  const prepare = new Map([
    [
      "0",
      JSON.stringify({
        event: "transaction_prepare_complete",
        rollbackIsolated: true,
        deleteHidden: true,
      }),
    ],
  ]);
  const contenders = new Map([
    ["0", JSON.stringify({ event: "transaction_contender_complete", writer: 1, outcome: "rejected" })],
    ["1", JSON.stringify({ event: "transaction_contender_complete", writer: 2, outcome: "committed" })],
  ]);
  const verifier = new Map([
    ["0", JSON.stringify({ event: "transaction_cleanup_verified", winner: 2 })],
  ]);

  const ledger = analyzeTransactionContention(prepare, contenders, verifier);

  assert.doesNotThrow(() => assertTransactionContention(ledger));
});

test("should_require_conflict_paged_replay_and_boundary_round_trip", () => {
  assert.doesNotThrow(() =>
    assertStreamReplay(
      {
        winner: 1,
        conflicts: 1,
        offsetConflicts: 1,
        records: 40,
        pages: 3,
        boundaryBytes: 60_000,
        boundaryRecords: 1,
      },
      40,
    ),
  );
  assert.deepEqual(boundaryPayload(42, 64), boundaryPayload(42, 64));
  assert.notDeepEqual(boundaryPayload(42, 64), boundaryPayload(43, 64));
});

test("should_accept_skipped_missed_schedule_and_at_most_once_cancellation_race", () => {
  const ledger: ScheduleOutageLedger = {
    missedAtMs: 60_000,
    repeatedAtMs: 120_000,
    missedDeliveries: 0,
    repeatedSequences: [0, 1, 2],
    raceSequences: [0, 2],
    duplicateDeliveries: 0,
    cancellationAcknowledged: [0, 1],
    cancellationFailed: [2],
  };

  assert.doesNotThrow(() => assertScheduleOutage(ledger, 3));
  assert.equal(nextWholeMinute(60_001), 120_000);
});

test("should_reject_a_duplicate_schedule_cancellation_race_delivery", () => {
  const ledger: ScheduleOutageLedger = {
    missedAtMs: 60_000,
    repeatedAtMs: 120_000,
    missedDeliveries: 0,
    repeatedSequences: [0],
    raceSequences: [0, 0],
    duplicateDeliveries: 0,
    cancellationAcknowledged: [0],
    cancellationFailed: [],
  };

  assert.throws(() => assertScheduleOutage(ledger, 1), /delivered sequence 0 twice/);
});

test("should_choose_two_consecutive_schedule_minutes_in_one_utc_hour", () => {
  const nearHourEnd = Date.UTC(2026, 7, 25, 18, 58, 1);
  const firstAtMs = nextConsecutiveMinutePair(nearHourEnd);

  assert.equal(firstAtMs, Date.UTC(2026, 7, 25, 19, 0));
  assert.equal(consecutiveMinuteCronAt(firstAtMs), "0,1 19 25 8 *");
});
