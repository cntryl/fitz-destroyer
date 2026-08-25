import type { ConcreteScenario, FailureClassification, ScenarioResult } from "./scenario.js";

export type DestroyerReportEntry = {
  scenario: ConcreteScenario;
  verdict: "passed" | "failed" | "missing";
  durationMs: number | null;
  failureClassification: FailureClassification | "missing" | null;
  cleanupState: ScenarioResult["cleanupState"] | null;
  runId: string | null;
  error?: string;
};

export type DestroyerReport = {
  schemaVersion: 1;
  generatedAt: string;
  verdict: "passed" | "failed";
  analysisResult: string;
  matrixResult: string;
  totals: {
    expected: number;
    reported: number;
    passed: number;
    failed: number;
    missing: number;
  };
  results: readonly DestroyerReportEntry[];
};

export function buildDestroyerReport(
  expectedScenarios: readonly ConcreteScenario[],
  scenarioResults: readonly ScenarioResult[],
  analysisResult: string,
  matrixResult: string,
  generatedAt = new Date().toISOString(),
): DestroyerReport {
  const results = expectedScenarios.map((scenario): DestroyerReportEntry => {
    const matches = scenarioResults.filter((result) => result.scenario === scenario);
    if (matches.length === 0) {
      return {
        scenario,
        verdict: "missing",
        durationMs: null,
        failureClassification: "missing",
        cleanupState: null,
        runId: null,
        error: "Scenario artifact did not contain a structured summary",
      };
    }
    if (matches.length > 1) {
      return {
        scenario,
        verdict: "failed",
        durationMs: null,
        failureClassification: "setup",
        cleanupState: null,
        runId: null,
        error: `Expected one scenario summary, found ${matches.length}`,
      };
    }
    const result = matches[0]!;
    return {
      scenario,
      verdict: result.verdict,
      durationMs: result.durationMs,
      failureClassification: result.failureClassification,
      cleanupState: result.cleanupState,
      runId: result.runId,
      ...(result.error === undefined ? {} : { error: result.error }),
    };
  });
  const passed = results.filter(({ verdict }) => verdict === "passed").length;
  const failed = results.filter(({ verdict }) => verdict === "failed").length;
  const missing = results.filter(({ verdict }) => verdict === "missing").length;
  const verdict = analysisResult === "success" &&
      matrixResult === "success" &&
      failed === 0 &&
      missing === 0
    ? "passed"
    : "failed";
  return {
    schemaVersion: 1,
    generatedAt,
    verdict,
    analysisResult,
    matrixResult,
    totals: {
      expected: expectedScenarios.length,
      reported: scenarioResults.length,
      passed,
      failed,
      missing,
    },
    results,
  };
}

export function renderDestroyerReport(report: DestroyerReport): string {
  const status = report.verdict === "passed" ? "Passed" : "Failed";
  const lines = [
    "# Fitz Destroyer report",
    "",
    `**Overall: ${status}**`,
    "",
    `Scenarios: ${report.totals.passed} passed, ${report.totals.failed} failed, ${report.totals.missing} missing. Analysis: ${report.analysisResult}. Matrix: ${report.matrixResult}.`,
    "",
    "| Scenario | Verdict | Duration | Failure class | Cleanup |",
    "| --- | --- | ---: | --- | --- |",
    ...report.results.map((result) =>
      `| ${result.scenario} | ${verdictLabel(result.verdict)} | ${durationLabel(result.durationMs)} | ${result.failureClassification ?? "-"} | ${result.cleanupState ?? "-"} |`,
    ),
  ];
  const failures = report.results.filter(({ verdict }) => verdict !== "passed");
  if (failures.length > 0) {
    lines.push("", "## Failure details", "");
    for (const failure of failures) {
      lines.push(`- **${failure.scenario}:** ${oneLine(failure.error ?? "No error details were captured")}`);
    }
  }
  return `${lines.join("\n")}\n`;
}

function verdictLabel(verdict: DestroyerReportEntry["verdict"]): string {
  if (verdict === "passed") return "PASS";
  if (verdict === "failed") return "FAIL";
  return "MISSING";
}

function durationLabel(durationMs: number | null): string {
  if (durationMs === null) return "-";
  return `${(durationMs / 1_000).toFixed(1)} s`;
}

function oneLine(value: string): string {
  const line = value.replace(/\s+/gu, " ").trim();
  return line.length <= 300 ? line : `${line.slice(0, 297)}...`;
}
