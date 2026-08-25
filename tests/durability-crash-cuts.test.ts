import assert from "node:assert/strict";
import test from "node:test";
import {
  analyzeDurabilityLedger,
  assertDurabilityLedger,
} from "../src/orchestration/durability-crash-cuts.js";
import { crashCutRoute } from "../src/workloads/durability-crash-cuts.js";

test("should_accept_acknowledged_and_ambiguous_durable_outcomes_after_restart", () => {
  // Arrange
  const baseline = new Map([["0", domainEvents("acknowledged", 0)]]);
  const cut = new Map([["0", domainEvents("failed", 1)]]);
  const verify = new Map([
    ["0", JSON.stringify({ event: "durability_verify_complete", observed: observed([0, 1]) })],
  ]);

  // Act
  const ledger = analyzeDurabilityLedger(baseline, cut, verify);

  // Assert
  assert.doesNotThrow(() => assertDurabilityLedger(ledger));
  assert.deepEqual(ledger.queue.observed, [0, 1]);
  assert.deepEqual(ledger.stream.acknowledged, [0]);
});

test("should_reject_an_acknowledged_durable_write_missing_after_restart", () => {
  // Arrange
  const baseline = new Map([["0", domainEvents("acknowledged", 0)]]);
  const cut = new Map([["0", domainEvents("acknowledged", 1)]]);
  const verify = new Map([
    ["0", JSON.stringify({ event: "durability_verify_complete", observed: observed([0]) })],
  ]);

  // Act
  const ledger = analyzeDurabilityLedger(baseline, cut, verify);
  const verifyLedger = () => assertDurabilityLedger(ledger);

  // Assert
  assert.throws(verifyLedger, /acknowledged sequence 1 disappeared/);
});

test("should_generate_concrete_crash_cut_routes", () => {
  assert.equal(crashCutRoute("queue", "run"), "queue://destroyer/run/crash-cut");
  assert.equal(
    crashCutRoute("schedule", "run", 7),
    "schedule://destroyer/run/crash-cut/job-00000007",
  );
});

function domainEvents(outcome: "acknowledged" | "failed", sequence: number): string {
  return ["queue", "kv", "stream", "schedule"]
    .flatMap((domain) => [
      { event: "durability_operation_started", domain, sequence },
      { event: `durability_operation_${outcome}`, domain, sequence },
    ])
    .map((record) => JSON.stringify(record))
    .join("\n");
}

function observed(sequences: number[]): Record<string, number[]> {
  return { queue: sequences, kv: sequences, stream: sequences, schedule: sequences };
}
