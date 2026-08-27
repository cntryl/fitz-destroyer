import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import test from "node:test";
import { createDestroyerToken, DESTROYER_AUTH_SECRET } from "../src/auth-token.js";
import { assertAuthorizationIsolation } from "../src/orchestration/authorization-isolation.js";
import { assertColdBootObservation } from "../src/orchestration/cold-boot-provider-outage.js";
import { assertHostileRpcWorker } from "../src/orchestration/hostile-rpc-worker.js";
import { assertQueueDeadLetterFencing } from "../src/orchestration/queue-dead-letter-fencing.js";
import { assertStreamGlobalRecovery } from "../src/orchestration/stream-global-recovery.js";

test("should_sign_a_bounded_local_destroyer_token", () => {
  // Arrange
  const token = createDestroyerToken("identity-a", ["kv://realm/**#read"], 1_000_000);
  const [header, payload, signature] = token.split(".");

  // Act
  const expected = createHmac("sha256", DESTROYER_AUTH_SECRET)
    .update(`${header}.${payload}`)
    .digest("base64url");
  const claims = JSON.parse(Buffer.from(payload ?? "", "base64url").toString("utf8")) as Record<string, unknown>;

  // Assert
  assert.equal(signature, expected);
  assert.equal(claims.tid, "identity-a");
  assert.equal(claims.exp, 4_600);
});

test("should_require_both_authorized_and_denied_isolation_evidence", () => {
  assert.doesNotThrow(() => assertAuthorizationIsolation(4, 4));
  assert.throws(() => assertAuthorizationIsolation(4, 3), /counts mismatch/u);
});

test("should_require_exact_global_stream_recovery", () => {
  assert.doesNotThrow(() => assertStreamGlobalRecovery(40, 40, 6, 40));
  assert.throws(() => assertStreamGlobalRecovery(40, 39, 6, 40), /recovery mismatch/u);
});

test("should_require_queue_ingress_and_delivery_fences", () => {
  assert.doesNotThrow(() => assertQueueDeadLetterFencing(1, 1, 400, 404));
  assert.throws(() => assertQueueDeadLetterFencing(1, 0, 400, 404), /fencing failed/u);
});

test("should_reject_any_ready_response_during_cold_boot_outage", () => {
  assert.doesNotThrow(() => assertColdBootObservation([0, 503, 0]));
  assert.throws(() => assertColdBootObservation([0, 200]), /reported ready/u);
});

test("should_require_hostile_failures_and_a_healthy_rpc_probe", () => {
  assert.doesNotThrow(() => assertHostileRpcWorker(1, 0, 0, 1, 2));
  assert.throws(() => assertHostileRpcWorker(1, 0, 0, 1, 0), /isolation failed/u);
});
