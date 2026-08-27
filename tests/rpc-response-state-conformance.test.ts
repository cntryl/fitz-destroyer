import assert from "node:assert/strict";
import test from "node:test";
import { assertRpcResponseStateEvidence } from "../src/orchestration/rpc-response-state-conformance.js";
import {
  decodeRpcWorkerRequest,
  encodeRpcResponsePayload,
} from "../src/workloads/rpc-response-state-conformance.js";
import { concatBytes, encodeBytes, encodeString } from "../src/workloads/raw-protocol.js";

test("should_encode_rpc_worker_responses_with_exact_correlation_sequence_and_terminal_state", () => {
  // Arrange
  const correlation = Uint8Array.from({ length: 16 }, (_, index) => index);
  const body = new Uint8Array([7, 8, 9]);

  // Act
  const payload = encodeRpcResponsePayload(correlation, 3n, true, body);

  // Assert
  assert.deepEqual([...payload.slice(0, 16)], [...correlation]);
  assert.equal(new DataView(payload.buffer, payload.byteOffset, payload.byteLength).getBigUint64(16), 3n);
  assert.equal(payload[24], 1);
  assert.deepEqual([...payload.slice(29)], [7, 8, 9]);
});

test("should_decode_broker_delivered_rpc_requests_without_losing_wire_identity", () => {
  // Arrange
  const correlation = new Uint8Array(16).fill(4);
  const payload = concatBytes(correlation, encodeString("rpc://realm/area/resource"), encodeBytes(new Uint8Array([1, 2])));

  // Act
  const request = decodeRpcWorkerRequest(payload);

  // Assert
  assert.deepEqual([...request.correlation], [...correlation]);
  assert.equal(request.route, "rpc://realm/area/resource");
  assert.deepEqual([...request.body], [1, 2]);
});

test("should_require_every_rpc_state_case_and_a_healthy_credit_probe", () => {
  // Arrange
  const evidence = {
    event: "rpc_response_state_conformance_complete",
    cases: 5,
    callersTerminated: 4,
    duplicateCallerTerminals: 0,
    unknownCorrelationRejected: 1,
    duplicateTerminalRejected: 1,
    postCancelResponsesObserved: 1,
    postDisconnectRejected: 1,
    healthyCalls: 4,
    healthyFailures: 0,
  };

  // Act
  const valid = (): void => {
    assertRpcResponseStateEvidence(evidence);
  };
  const invalid = (): void => {
    assertRpcResponseStateEvidence({ ...evidence, healthyFailures: 1 });
  };

  // Assert
  assert.doesNotThrow(valid);
  assert.throws(invalid, /healthyFailures=1\/0/u);
});
