import assert from "node:assert/strict";
import test from "node:test";
import type { Client } from "@cntryl/fitz";
import { canaryRoute, runCanaryOperation } from "../src/workloads/canary.js";
import { protocolAttackNames } from "../src/workloads/protocol-abuse.js";

test("should_keep_canary_routes_outside_the_hot_route", () => {
  // Arrange
  const namespace = "run-1";

  // Act
  const routes = [
    canaryRoute("queue", namespace),
    canaryRoute("stream", namespace, 4),
    canaryRoute("schedule", namespace, 4),
  ];

  // Assert
  assert.deepEqual(routes, [
    "queue://destroyer/run-1/canary",
    "stream://destroyer/run-1/canary-4",
    "schedule://destroyer/run-1/canary/job-4",
  ]);
});

test("should_retry_typed_queue_backpressure_during_a_cold_canary_round_trip", async () => {
  // Arrange
  let body: Uint8Array<ArrayBufferLike> = new Uint8Array();
  let reserveAttempts = 0;
  let completions = 0;
  const client = {
    queue: {
      enqueue: async (_route: string, request: { body: Uint8Array }) => {
        body = request.body;
      },
      reserve: async () => {
        reserveAttempts += 1;
        if (reserveAttempts === 1) throw { domainCode: 4_005 };
        return [{ body, complete: async () => { completions += 1; } }];
      },
    },
  } as unknown as Client;

  // Act
  await runCanaryOperation(
    client,
    {
      namespace: "run-1",
      workerId: "canary",
      operations: 1,
      payloadBytes: 64,
      concurrency: 1,
      handlerDelayMs: 0,
      requestTimeoutMs: 1_000,
      signal: new AbortController().signal,
      domains: ["queue"],
    },
    "queue",
    0,
  );

  // Assert
  assert.equal(reserveAttempts, 2);
  assert.equal(completions, 1);
});

test("should_cover_transport_and_tlv_protocol_abuse", () => {
  // Arrange
  const attacks = protocolAttackNames();

  // Act and Assert
  assert.ok(attacks.includes("text-before-connect"));
  assert.ok(attacks.includes("duplicate-tag-after-connect"));
  assert.ok(attacks.includes("oversize-frame"));
});
