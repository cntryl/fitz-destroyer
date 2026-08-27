import assert from "node:assert/strict";
import test from "node:test";
import {
  concatBytes,
  decodeStandardResponse,
  decodeTlvs,
  encodeBytes,
  encodeString,
  encodeTlv,
  encodeU64,
  encodeTcpFrame,
} from "../src/workloads/raw-protocol.js";
import { assertWireEvidence } from "../src/orchestration/wire-conformance.js";

test("should_round_trip_single_and_escaped_tlv_types", () => {
  // Arrange
  const frame = concatBytes(encodeTlv(1, encodeString("connect")), encodeTlv(400, encodeBytes(new Uint8Array([1, 2]))));

  // Act
  const records = decodeTlvs(frame);

  // Assert
  assert.deepEqual(records.map(({ type }) => type), [1, 400]);
  assert.equal(new TextDecoder().decode(records[0]?.payload.slice(4)), "connect");
  assert.deepEqual([...records[1]!.payload.slice(4)], [1, 2]);
});

test("should_encode_tcp_outer_length_without_mutating_the_payload", () => {
  // Arrange
  const payload = new Uint8Array([1, 2, 3]);

  // Act
  const frame = encodeTcpFrame(payload);

  // Assert
  assert.deepEqual([...frame], [0, 0, 0, 3, 1, 2, 3]);
  assert.deepEqual([...payload], [1, 2, 3]);
});

test("should_decode_standard_success_and_typed_error_envelopes", () => {
  // Arrange
  const errorMessage = encodeString("bounded rejection");
  const error = concatBytes(new Uint8Array([1]), new Uint8Array([0, 0, 0, 5]), errorMessage);

  // Act
  const success = decodeStandardResponse(new Uint8Array([0, 9]));
  const rejected = decodeStandardResponse(error);

  // Assert
  assert.equal(success.ok, true);
  assert.deepEqual([...success.data], [9]);
  assert.equal(rejected.ok, false);
  assert.equal(rejected.errorCode, 5);
  assert.equal(rejected.errorMessage, "bounded rejection");
});

test("should_reject_incomplete_tlv_and_malformed_envelopes", () => {
  // Arrange
  const incompleteTlv = new Uint8Array([1, 0]);
  const malformedEnvelope = new Uint8Array([1, 0, 0]);

  // Act
  const decodeIncompleteTlv = (): void => { decodeTlvs(incompleteTlv); };
  const decodeMalformedEnvelope = (): void => { decodeStandardResponse(malformedEnvelope); };

  // Assert
  assert.throws(decodeIncompleteTlv, /ended in length/u);
  assert.throws(decodeMalformedEnvelope, /invalid standard response/u);
});

test("should_require_all_lease_alias_operations_to_preserve_canonical_state", () => {
  // Arrange
  const event = {
    event: "lease_route_aliasing_complete",
    operations: 6,
    rejected: 6,
    canonicalPreserved: 6,
  };

  // Act
  const valid = (): void => { assertWireEvidence("lease-route-aliasing", event, ""); };
  const invalid = (): void => { assertWireEvidence("lease-route-aliasing", { ...event, rejected: 5 }, ""); };

  // Assert
  assert.doesNotThrow(valid);
  assert.throws(invalid, /reject all six/u);
});

test("should_require_pre_auth_close_and_both_transport_canaries", () => {
  // Arrange
  const event = {
    event: "tcp_preauth_framing_slowloris_complete",
    socketsOpened: 16,
    socketsClosed: 16,
    tcpCanary: 1,
    websocketCanary: 1,
  };

  // Act
  const valid = (): void => { assertWireEvidence("tcp-preauth-framing-slowloris", event, ""); };
  const invalid = (): void => {
    assertWireEvidence("tcp-preauth-framing-slowloris", { ...event, socketsClosed: 15 }, "");
  };

  // Assert
  assert.doesNotThrow(valid);
  assert.throws(invalid, /every held TCP socket/u);
});

test("should_require_a_complete_ws_tcp_pipeline_outcome_set", () => {
  // Arrange
  const log = [
    JSON.stringify({ event: "connect_pipeline_family_rebind_case", transport: "ws" }),
    JSON.stringify({ event: "connect_pipeline_family_rebind_case", transport: "tcp" }),
  ].join("\n");
  const event = { event: "connect_pipeline_family_rebind_complete", transports: 2, accepted: 1, rejected: 1 };

  // Act
  const valid = (): void => { assertWireEvidence("connect-pipeline-family-rebind", event, log); };
  const invalid = (): void => {
    assertWireEvidence("connect-pipeline-family-rebind", { ...event, transports: 1 }, log);
  };

  // Assert
  assert.doesNotThrow(valid);
  assert.throws(invalid, /WS and TCP/u);
});

test("should_encode_u64_values_in_network_order", () => {
  // Arrange

  // Act
  const value = encodeU64(0x0102_0304_0506_0708n);

  // Assert
  assert.deepEqual([...value], [1, 2, 3, 4, 5, 6, 7, 8]);
});
