import assert from "node:assert/strict";
import test from "node:test";
import { parseArgs, RPC_STREAM_MAX_FRAME_BYTES } from "../src/config.js";

test("should_apply_standard_scale_and_live_options", () => {
  // Arrange
  const args = [
    "rpc-pressure",
    "--scale",
    "standard",
    "--clients",
    "8",
    "--phase-ms",
    "9000",
    "--concurrency",
    "96",
    "--handler-delay-ms",
    "3",
  ];

  // Act
  const config = parseArgs(args, { FITZ_SOURCE_DIR: "/tmp/fitz" });

  // Assert
  assert.equal(config.scenario, "rpc-pressure");
  assert.equal(config.resources, 10);
  assert.equal(config.entriesPerResource, 1_000);
  assert.equal(config.payloadBytes, 1_024);
  assert.equal(config.clientReplicas, 8);
  assert.equal(config.phaseMs, 9_000);
  assert.equal(config.liveConcurrency, 96);
  assert.equal(config.handlerDelayMs, 3);
  assert.equal(config.fitzSourceDir, "/tmp/fitz");
});

test("should_allow_explicit_dimensions_to_override_scale", () => {
  // Arrange
  const args = ["clean-restart", "--scale", "large", "--resources", "12", "--entries", "75"];

  // Act
  const config = parseArgs(args, {});

  // Assert
  assert.equal(config.resources, 12);
  assert.equal(config.entriesPerResource, 75);
  assert.equal(config.payloadBytes, 1_024);
});

test("should_reject_an_unknown_option", () => {
  // Arrange
  const args = ["cache-loss", "--surprise", "yes"];

  // Act
  const parse = () => parseArgs(args, {});

  // Assert
  assert.throws(parse, /Unknown option/);
});

test("should_accept_notice_fanout_with_zero_handler_delay", () => {
  // Arrange
  const args = ["notice-fanout", "--handler-delay-ms", "0"];

  // Act
  const config = parseArgs(args, {});

  // Assert
  assert.equal(config.scenario, "notice-fanout");
  assert.equal(config.handlerDelayMs, 0);
  assert.equal(config.liveConcurrency, 8);
});

test("should_accept_connection_storm", () => {
  // Arrange
  const args = [
    "connection-storm",
    "--scale",
    "standard",
    "--clients",
    "6",
    "--reuse-images",
  ];

  // Act
  const config = parseArgs(args, {});

  // Assert
  assert.equal(config.scenario, "connection-storm");
  assert.equal(config.resources, 10);
  assert.equal(config.clientReplicas, 6);
  assert.equal(config.reuseImages, true);
});

test("should_accept_a_bombard_domain_subset", () => {
  // Arrange
  const args = ["chaos", "--domains", "queue,stream"];

  // Act
  const config = parseArgs(args, {});

  // Assert
  assert.deepEqual(config.bombardDomains, ["queue", "stream"]);
});

test("should_accept_domain_pressure", () => {
  // Arrange
  const args = ["domain-pressure", "--domains", "queue,notice", "--clients", "8"];

  // Act
  const config = parseArgs(args, {});

  // Assert
  assert.equal(config.scenario, "domain-pressure");
  assert.deepEqual(config.bombardDomains, ["queue", "notice"]);
  assert.equal(config.clientReplicas, 8);
});

test("should_apply_rpc_stream_hose_scale_and_overrides", () => {
  // Arrange
  const args = [
    "rpc-stream-hose",
    "--scale",
    "standard",
    "--rpc-stream-calls",
    "40",
    "--rpc-stream-frames",
    "2000",
    "--rpc-stream-frame-bytes",
    "32768",
    "--rpc-stream-reader-delay-ms",
    "3",
  ];

  // Act
  const config = parseArgs(args, {});

  // Assert
  assert.equal(config.scenario, "rpc-stream-hose");
  assert.equal(config.rpcStreamCalls, 40);
  assert.equal(config.rpcStreamFrames, 2_000);
  assert.equal(config.rpcStreamFrameBytes, 32_768);
  assert.equal(config.rpcStreamReaderDelayMs, 3);
});

test("should_reject_rpc_stream_body_that_cannot_fit_one_tlv_value", () => {
  // Arrange
  const args = [
    "rpc-stream-hose",
    "--rpc-stream-frame-bytes",
    String(RPC_STREAM_MAX_FRAME_BYTES + 1),
  ];

  // Act
  const parse = () => parseArgs(args, {});

  // Assert
  assert.throws(parse, /--rpc-stream-frame-bytes must be an integer between 64 and 65506/);
});
