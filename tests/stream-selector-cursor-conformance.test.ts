import assert from "node:assert/strict";
import test from "node:test";
import {
  assertStreamSelectorEvidence,
  streamSelectorCases,
} from "../src/workloads/stream-selector-cursor-conformance.js";

test("should_cover_every_stream_selector_cursor_axis", () => {
  const cases = streamSelectorCases("run-1");
  assert.equal(cases.length, 8);
  assert.deepEqual([...new Set(cases.map(({ axis }) => axis))].sort(), ["area", "global", "realm", "resource"]);
});

test("should_require_exact_selector_filter_and_reconnect_evidence", () => {
  const evidence = { selectors: 8, recordsWritten: 8, visibleRecords: 21, filteredOffsets: 21, cursorAdvances: 42, duplicateRecords: 0, missingRecords: 0, reconnectContinuations: 1 };
  assert.doesNotThrow(() => assertStreamSelectorEvidence(evidence));
  assert.throws(() => assertStreamSelectorEvidence({ ...evidence, duplicateRecords: 1 }), /duplicateRecords=1\/0/u);
});
