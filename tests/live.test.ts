import assert from "node:assert/strict";
import test from "node:test";
import {
  rpcStreamFramePayload,
  rpcStreamRequestPayload,
} from "../src/workloads/live.js";

test("should_generate_exact_repeatable_rpc_stream_frames", () => {
  // Arrange
  const first = rpcStreamFramePayload("7", 3, 11, 19, 65_536);

  // Act
  const second = rpcStreamFramePayload("7", 3, 11, 19, 65_536);

  // Assert
  assert.equal(first.length, 65_536);
  assert.deepEqual(first, second);
});

test("should_change_rpc_stream_bytes_with_frame_identity", () => {
  // Arrange
  const first = rpcStreamFramePayload("7", 3, 11, 19, 1_024);

  // Act
  const second = rpcStreamFramePayload("7", 3, 11, 20, 1_024);

  // Assert
  assert.notDeepEqual(first, second);
});

test("should_encode_rpc_stream_request_with_fixed_small_body", () => {
  // Arrange
  const expectedPrefix = new TextEncoder().encode("h|3|11|1000|65536|");

  // Act
  const request = rpcStreamRequestPayload(3, 11, 1_000, 65_536);

  // Assert
  assert.equal(request.length, 128);
  assert.deepEqual(request.subarray(0, expectedPrefix.length), expectedPrefix);
});
