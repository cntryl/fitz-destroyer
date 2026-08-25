import assert from "node:assert/strict";
import test from "node:test";
import {
  analyzeScheduleDeliveryLogs,
  summarizeScheduleDeliveryLogs,
} from "../src/orchestration/schedule-delivery.js";
import {
  scheduleCronAt,
  scheduleDeliveryPayload,
  scheduleDeliveryRoute,
} from "../src/workloads/schedule-delivery.js";

test("should_encode_an_exact_utc_minute_as_fitz_cron", () => {
  // Arrange
  const fireAtMs = Date.UTC(2026, 7, 25, 19, 42, 0, 0);

  // Act
  const cron = scheduleCronAt(fireAtMs);

  // Assert
  assert.equal(cron, "42 19 25 8 *");
});

test("should_generate_distinct_repeatable_schedule_routes_and_payloads", () => {
  // Arrange
  const shape = { seed: 42, payloadBytes: 256 };

  // Act
  const first = scheduleDeliveryPayload(shape, "broadcast", 7);
  const repeat = scheduleDeliveryPayload(shape, "broadcast", 7);
  const single = scheduleDeliveryPayload(shape, "single", 7);

  // Assert
  assert.equal(
    scheduleDeliveryRoute("run", "broadcast", 7),
    "schedule://destroyer/run/broadcast/job-00000007",
  );
  assert.deepEqual(first, repeat);
  assert.notDeepEqual(first, single);
});

test("should_reconcile_broadcast_and_single_schedule_delivery_across_subscribers", () => {
  // Arrange
  const logs = new Map([
    [
      "0",
      subscriberLog(
        "0",
        [received("broadcast", 0), received("broadcast", 1), received("single", 0)],
        2,
        1,
      ),
    ],
    [
      "1",
      subscriberLog(
        "1",
        [received("broadcast", 0), received("broadcast", 1), received("single", 1)],
        2,
        1,
      ),
    ],
  ]);

  // Act
  const summary = summarizeScheduleDeliveryLogs(logs, 2, 2);

  // Assert
  assert.deepEqual(summary, {
    broadcastDeliveries: 4,
    singleDeliveries: 2,
    cancelledDeliveries: 0,
    duplicates: 0,
    invalid: 0,
    maxLatenessMs: 125,
  });
});

test("should_reject_a_single_schedule_delivered_to_two_subscribers", () => {
  // Arrange
  const logs = new Map([
    ["0", subscriberLog("0", [received("broadcast", 0), received("single", 0)], 1, 1)],
    ["1", subscriberLog("1", [received("broadcast", 0), received("single", 0)], 1, 1)],
  ]);

  // Act
  const summarize = () => summarizeScheduleDeliveryLogs(logs, 1, 2);

  // Assert
  assert.throws(summarize, /duplicate Single=1 \[0\]/);
});

test("should_preserve_aggregate_delivery_deficits_before_rejecting_the_run", () => {
  // Arrange
  const logs = new Map([
    ["0", subscriberLog("0", [received("broadcast", 0), received("single", 0)], 1, 1)],
    ["1", subscriberLog("1", [received("single", 1)], 0, 1)],
  ]);

  // Act
  const evidence = analyzeScheduleDeliveryLogs(logs, 2, 2);
  const summarize = () => summarizeScheduleDeliveryLogs(logs, 2, 2);

  // Assert
  assert.equal(evidence.observed.broadcastDeliveries, 1);
  assert.equal(evidence.observed.singleDeliveries, 2);
  assert.deepEqual(evidence.subscriberBroadcastDeliveries, { 0: 1, 1: 0 });
  assert.equal(evidence.missingBroadcastCount, 2);
  assert.deepEqual(evidence.missingBroadcastSequences, [0, 1]);
  assert.equal(evidence.missingSingleCount, 0);
  assert.equal(evidence.duplicateSingleCount, 0);
  assert.throws(
    summarize,
    /Broadcast=1\/4; Single=2\/2.*subscriber Broadcast=0:1\/2,1:0\/2.*missing Broadcast=2 \[0,1\]/,
  );
});

test("should_attribute_missing_deliveries_to_client_handler_saturation", () => {
  // Arrange
  const saturated = JSON.stringify({
    event: "fitz_client_event",
    clientEvent: "fitz.connection.handler_saturated",
  });
  const logs = new Map([
    ["0", `${saturated}\n${saturated}\n${subscriberLog("0", [], 0, 0)}`],
  ]);

  // Act
  const evidence = analyzeScheduleDeliveryLogs(logs, 1, 1);
  const summarize = () => summarizeScheduleDeliveryLogs(logs, 1, 1);

  // Assert
  assert.deepEqual(evidence.clientEventCounts, {
    "fitz.connection.handler_saturated": 2,
  });
  assert.deepEqual(evidence.clientHandlerSaturations, { 0: 2 });
  assert.throws(summarize, /client handler saturation=2 \[0:2\]/);
});

function received(kind: "broadcast" | "single", sequence: number): Record<string, unknown> {
  return {
    event: "schedule_notification_received",
    kind,
    sequence,
  };
}

function subscriberLog(
  workerId: string,
  records: readonly Record<string, unknown>[],
  broadcast: number,
  single: number,
): string {
  return [
    ...records,
    {
      event: "schedule_subscriber_complete",
      workerId,
      broadcast,
      single,
      cancelled: 0,
      duplicates: 0,
      invalid: 0,
      maxLatenessMs: 125,
    },
  ]
    .map((record) => JSON.stringify(record))
    .join("\n");
}
