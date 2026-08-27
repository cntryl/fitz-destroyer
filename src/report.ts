import type {
  CleanupState,
  ConcreteScenario,
  FailureClassification,
  ScenarioResult,
} from "./scenario.js";
import { emptyGuidance, type OperationalGuidance } from "./operational-guidance.js";

export type DestroyerReportEntry = {
  scenario: ConcreteScenario;
  verdict: "passed" | "failed" | "missing";
  durationMs: number | null;
  workloadDurationMs: number | null;
  failureClassification: FailureClassification | "missing" | null;
  cleanupState: ScenarioResult["cleanupState"] | null;
  runId: string | null;
  project: string | null;
  analysis: ScenarioReportAnalysis;
  error?: string;
  cleanupError?: string;
};

export type DestroyerReportRun = {
  repository: string | null;
  commitSha: string | null;
  refName: string | null;
  url: string | null;
  attempt: string | null;
};

type FailureClass = FailureClassification | "missing";

export type ReportObservation = {
  code: string;
  message: string;
  occurrences: number;
  files: readonly string[];
};

export type ReportWarning = {
  code: string;
  message: string;
  details: Readonly<Record<string, unknown>> | null;
};

export type ScenarioReportAnalysis = {
  observations: readonly ReportObservation[];
  warnings: readonly ReportWarning[];
  brokerSummary: Readonly<Record<string, number | null>> | null;
  operationalGuidance: OperationalGuidance;
};

export type AnalyzedScenarioResult = ScenarioResult & {
  reportAnalysis?: ScenarioReportAnalysis;
};

export type DestroyerReport = {
  schemaVersion: 3;
  generatedAt: string;
  verdict: "passed" | "failed";
  analysisResult: string;
  matrixResult: string;
  run: DestroyerReportRun;
  totals: {
    expected: number;
    reported: number;
    passed: number;
    failed: number;
    missing: number;
  };
  failureClasses: Record<FailureClass, number>;
  cleanup: Record<CleanupState | "unknown", number>;
  timing: {
    recordedTotalMs: number;
    slowestScenario: ConcreteScenario | null;
    slowestDurationMs: number | null;
  };
  results: readonly DestroyerReportEntry[];
};

export type DestroyerReportRunInput = Partial<{
  repository: string;
  commitSha: string;
  refName: string;
  url: string;
  attempt: string;
}>;

export function buildDestroyerReport(
  expectedScenarios: readonly ConcreteScenario[],
  scenarioResults: readonly AnalyzedScenarioResult[],
  analysisResult: string,
  matrixResult: string,
  generatedAt = new Date().toISOString(),
  run: DestroyerReportRunInput = {},
): DestroyerReport {
  const results = expectedScenarios.map((scenario): DestroyerReportEntry => {
    const matches = scenarioResults.filter((result) => result.scenario === scenario);
    if (matches.length === 0) {
      return {
        scenario,
        verdict: "missing",
        durationMs: null,
        workloadDurationMs: null,
        failureClassification: "missing",
        cleanupState: null,
        runId: null,
        project: null,
        analysis: emptyAnalysis(null, "Scenario artifact did not contain a structured summary."),
        error: "Scenario artifact did not contain a structured summary",
      };
    }
    if (matches.length > 1) {
      return {
        scenario,
        verdict: "failed",
        durationMs: null,
        workloadDurationMs: null,
        failureClassification: "setup",
        cleanupState: null,
        runId: null,
        project: null,
        analysis: emptyAnalysis(null, "Duplicate scenario summaries prevented operational extraction."),
        error: `Expected one scenario summary, found ${matches.length}`,
      };
    }
    const result = matches[0]!;
    return {
      scenario,
      verdict: result.verdict,
      durationMs: result.durationMs,
      workloadDurationMs: result.reportAnalysis?.operationalGuidance.workloadDurationMs ??
        result.workloadDurationMs ?? null,
      failureClassification: result.failureClassification,
      cleanupState: result.cleanupState,
      runId: result.runId,
      project: result.project,
      analysis: result.reportAnalysis ?? emptyAnalysis(result.workloadDurationMs ?? null),
      ...(result.error === undefined ? {} : { error: result.error }),
      ...(result.cleanupError === undefined ? {} : { cleanupError: result.cleanupError }),
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
  const failureClasses: DestroyerReport["failureClasses"] = {
    setup: 0,
    workload: 0,
    assertion: 0,
    timeout: 0,
    cleanup: 0,
    missing: 0,
  };
  const cleanup: DestroyerReport["cleanup"] = {
    removed: 0,
    preserved: 0,
    failed: 0,
    unknown: 0,
  };
  let recordedTotalMs = 0;
  let slowestScenario: ConcreteScenario | null = null;
  let slowestDurationMs: number | null = null;
  for (const result of results) {
    if (result.verdict !== "passed" && result.failureClassification !== null) {
      failureClasses[result.failureClassification] += 1;
    }
    cleanup[result.cleanupState ?? "unknown"] += 1;
    if (result.durationMs !== null) {
      recordedTotalMs += result.durationMs;
      if (slowestDurationMs === null || result.durationMs > slowestDurationMs) {
        slowestScenario = result.scenario;
        slowestDurationMs = result.durationMs;
      }
    }
  }
  return {
    schemaVersion: 3,
    generatedAt,
    verdict,
    analysisResult,
    matrixResult,
    run: {
      repository: normalized(run.repository),
      commitSha: normalized(run.commitSha),
      refName: normalized(run.refName),
      url: normalized(run.url),
      attempt: normalized(run.attempt),
    },
    totals: {
      expected: expectedScenarios.length,
      reported: scenarioResults.length,
      passed,
      failed,
      missing,
    },
    failureClasses,
    cleanup,
    timing: {
      recordedTotalMs,
      slowestScenario,
      slowestDurationMs,
    },
    results,
  };
}

export function renderDestroyerReport(report: DestroyerReport): string {
  const failures = report.results.filter(({ verdict }) => verdict !== "passed");
  const capturedFailures = failures.filter(({ verdict }) => verdict === "failed");
  const warnings = report.results.flatMap((result) =>
    result.analysis.warnings.map((warning) => ({ scenario: result.scenario, warning }))
  );
  const lines = [
    "# Fitz Destroyer report",
    "",
    report.verdict === "passed" ? "## ✅ Passed" : "## ❌ Failed",
    "",
    verdictSummary(report),
    "",
    runSummary(report),
    "",
    "## Outcome",
    "",
    "| Signal | Result | Detail |",
    "| --- | --- | --- |",
    `| Repository checks | ${outcomeLabel(report.analysisResult)} | ${tableCell(report.analysisResult)} |`,
    `| Scenario matrix | ${outcomeLabel(report.matrixResult)} | ${tableCell(report.matrixResult)} |`,
    `| Scenario summaries | ${failures.length === 0 ? "✅ PASS" : "❌ FAIL"} | ${report.totals.passed} passed, ${report.totals.failed} failed, ${report.totals.missing} missing |`,
    "",
    timingSummary(report),
    "",
    ...operationalGuidanceLines(report),
  ];

  if (warnings.length > 0) {
    lines.push(
      "",
      "## Diagnostic warnings",
      "",
      "These observations do not change the correctness verdict.",
      "",
      "| Scenario | Warning | Details |",
      "| --- | --- | --- |",
      ...warnings.map(({ scenario, warning }) =>
        `| ${scenario} | **${tableCell(warning.code)}:** ${tableCell(warning.message)} | ${warningDetails(warning)} |`
      ),
    );
  }

  if (failures.length > 0) {
    lines.push(
      "",
      "## What needs attention",
      "",
      "| Failure class | Count | Scenarios |",
      "| --- | ---: | --- |",
    );
    for (const failureClass of failureClassOrder) {
      const scenarios = report.results
        .filter((result) => result.verdict !== "passed" && result.failureClassification === failureClass)
        .map(({ scenario }) => `\`${scenario}\``);
      if (scenarios.length > 0) {
        lines.push(`| ${failureClass} | ${scenarios.length} | ${scenarios.join(", ")} |`);
      }
    }
    if (report.cleanup.failed > 0) {
      lines.push("", `> ⚠️ ${counted(report.cleanup.failed, "scenario")} also reported cleanup failure.`);
    }
    if (report.cleanup.preserved > 0) {
      lines.push("", `${counted(report.cleanup.preserved, "failed scenario")} preserved their exact Compose project state when the summary was captured.`);
    }
  }

  if (capturedFailures.length > 0) {
    const observations = aggregateObservations(capturedFailures);
    if (observations.length > 0) {
      lines.push(
        "",
        "## Evidence signals",
        "",
        "These are normalized observations from captured logs, not root-cause claims.",
        "",
        "| Observation | Occurrences | Scenarios |",
        "| --- | ---: | --- |",
        ...observations.map((observation) =>
          `| ${tableCell(observation.message)} | ${observation.occurrences} | ${observation.scenarios.map((scenario) => `\`${scenario}\``).join(", ")} |`
        ),
      );
    }
    lines.push("", "## Failure evidence");
    for (const failure of capturedFailures) {
      lines.push(
        "",
        `### ${failure.scenario} — ${failure.failureClassification ?? "unclassified"}`,
        "",
        `- Verdict: ${verdictLabel(failure.verdict)}`,
        `- Duration: ${durationLabel(failure.durationMs)}`,
        `- Cleanup: ${failure.cleanupState ?? "unknown"}`,
        `- Evidence: ${evidenceLabel(failure, report.run)}`,
      );
      if (failure.project !== null) lines.push(`- Compose project: \`${failure.project}\``);
      if (failure.analysis.observations.length > 0) {
        lines.push(
          "- Observed signals:",
          ...failure.analysis.observations.map((observation) =>
            `  - ${observation.message} — ${counted(observation.occurrences, "occurrence")} in ${observation.files.map((file) => `\`${file}\``).join(", ")}`
          ),
        );
      }
      if (failure.analysis.brokerSummary !== null) {
        lines.push(`- Final broker summary: ${brokerSummaryLabel(failure.analysis.brokerSummary)}`);
      }
      if (failure.error !== undefined) {
        lines.push(
          "",
          "<details>",
          "<summary>Captured error</summary>",
          "",
          ...diagnosticQuote(failure.error),
          "",
          "</details>",
        );
      } else {
        lines.push("", "> No error details were captured.");
      }
      if (failure.cleanupError !== undefined) {
        lines.push(
          "",
          "<details>",
          "<summary>Cleanup error</summary>",
          "",
          ...diagnosticQuote(failure.cleanupError),
          "",
          "</details>",
        );
      }
    }
  }

  lines.push(
    "",
    "## Scenario results",
    "",
    "| Scenario | Verdict | Duration | Failure class | Cleanup | Evidence |",
    "| --- | --- | ---: | --- | --- | --- |",
    ...report.results.map((result) =>
      `| ${result.scenario} | ${verdictLabel(result.verdict)} | ${durationLabel(result.durationMs)} | ${result.failureClassification ?? "-"} | ${result.cleanupState ?? "-"} | ${evidenceLabel(result, report.run)} |`,
    ),
  );
  return `${lines.join("\n")}\n`;
}

const failureClassOrder: readonly FailureClass[] = [
  "setup",
  "workload",
  "assertion",
  "timeout",
  "cleanup",
  "missing",
];

type AggregatedObservation = {
  code: string;
  message: string;
  occurrences: number;
  scenarios: ConcreteScenario[];
};

function aggregateObservations(failures: readonly DestroyerReportEntry[]): AggregatedObservation[] {
  const observations = new Map<string, AggregatedObservation>();
  for (const failure of failures) {
    for (const observation of failure.analysis.observations) {
      const current = observations.get(observation.code);
      if (current === undefined) {
        observations.set(observation.code, {
          code: observation.code,
          message: observation.message,
          occurrences: observation.occurrences,
          scenarios: [failure.scenario],
        });
      } else {
        current.occurrences += observation.occurrences;
        current.scenarios.push(failure.scenario);
      }
    }
  }
  return [...observations.values()].sort((left, right) =>
    right.scenarios.length - left.scenarios.length || right.occurrences - left.occurrences ||
    left.code.localeCompare(right.code)
  );
}

function verdictSummary(report: DestroyerReport): string {
  if (report.verdict === "passed") {
    return `All ${report.totals.expected} scenarios passed and the repository checks completed successfully.`;
  }
  const reasons: string[] = [];
  if (report.totals.failed > 0) reasons.push(`${counted(report.totals.failed, "scenario")} failed`);
  if (report.totals.missing > 0) {
    reasons.push(`${counted(report.totals.missing, "scenario summary artifact")} ${report.totals.missing === 1 ? "was" : "were"} missing`);
  }
  if (report.analysisResult !== "success") reasons.push(`repository checks reported ${report.analysisResult}`);
  if (report.matrixResult !== "success") reasons.push(`the scenario matrix reported ${report.matrixResult}`);
  return `The Destroyer verdict failed because ${sentenceList(reasons)}.`;
}

function runSummary(report: DestroyerReport): string {
  const context: string[] = [`Generated ${report.generatedAt}`];
  if (report.run.url !== null) context.push(`[workflow run](${report.run.url})`);
  if (report.run.repository !== null) context.push(`repository \`${report.run.repository}\``);
  if (report.run.refName !== null) context.push(`ref \`${report.run.refName}\``);
  if (report.run.commitSha !== null) {
    const shortSha = report.run.commitSha.slice(0, 7);
    const repositoryUrl = report.run.url?.replace(/\/actions\/runs\/.*$/u, "");
    context.push(repositoryUrl === undefined
      ? `commit \`${shortSha}\``
      : `commit [\`${shortSha}\`](${repositoryUrl}/commit/${report.run.commitSha})`);
  }
  if (report.run.attempt !== null) context.push(`attempt ${report.run.attempt}`);
  return `${context.join(" · ")}.`;
}

function timingSummary(report: DestroyerReport): string {
  if (report.timing.slowestScenario === null || report.timing.slowestDurationMs === null) {
    return "No scenario duration data was available.";
  }
  return `Recorded scenario runtime was ${durationLabel(report.timing.recordedTotalMs)} across parallel jobs; the slowest scenario was \`${report.timing.slowestScenario}\` at ${durationLabel(report.timing.slowestDurationMs)}. Timings are diagnostic, not correctness gates.`;
}

function operationalGuidanceLines(report: DestroyerReport): string[] {
  const lines = [
    "## Operational guidance",
    "",
    "Observed rates describe this run only; they are not capacity claims or correctness gates. `cntryl-stress` remains Fitz's authoritative benchmark suite.",
    "",
    "| Scenario | Workload duration | Headline metric | Completion semantics | Guidance |",
    "| --- | ---: | --- | --- | --- |",
    ...report.results.map((result) => {
      const guidance = result.analysis.operationalGuidance;
      return `| ${result.scenario} | ${durationLabel(guidance.workloadDurationMs)} | ${headlineMetric(guidance)} | ${completionLabel(guidance)} | ${guidanceLabel(guidance)} |`;
    }),
  ];
  for (const result of report.results) {
    const guidance = result.analysis.operationalGuidance;
    if (guidance.pressureDomains.length === 0) continue;
    lines.push(
      "",
      `### ${result.scenario} pressure detail`,
      "",
      `Guidance: ${guidanceLabel(guidance)}.`,
      "",
      "| Domain | Completion semantics | Completed operations | Observed ops/s | Errors | Ambiguous | Shutdown cancellations | Slowest stage |",
      "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
      ...guidance.pressureDomains.map((domain) =>
        `| ${domain.domain} | ${tableCell(domain.completionSemantics)} | ${domain.completedOperations} | ${domain.observedOperationsPerSecond.toFixed(2)} | ${domain.errors} | ${domain.ambiguousOutcomes} | ${domain.expectedCancellations} | ${tableCell(domain.slowestStage)} |`
      ),
      "",
      "| Domain | Stage | Mean | p50 | p95 | p99 | Max |",
      "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
      ...guidance.pressureDomains.flatMap((domain) => domain.stages.map((stage) =>
        `| ${domain.domain} | ${tableCell(stage.stage)} | ${milliseconds(stage.latency.meanMs)} | ${milliseconds(stage.latency.p50Ms)} | ${milliseconds(stage.latency.p95Ms)} | ${milliseconds(stage.latency.p99Ms)} | ${milliseconds(stage.latency.maxMs)} |`
      )),
    );
  }
  return lines;
}

function headlineMetric(guidance: OperationalGuidance): string {
  const rates = guidance.rates.slice(0, 2).map((metric) =>
    `${tableCell(metric.label)} ${metric.valuePerSecond.toFixed(2)} ${tableCell(metric.unit)}`
  );
  if (rates.length > 0) return rates.join("<br>");
  const count = guidance.counts[0];
  if (count !== undefined) return `${tableCell(count.label)} ${count.value} ${tableCell(count.unit)}`;
  const recovery = guidance.recoveries[0];
  if (recovery !== undefined) return `${tableCell(recovery.label)} ${durationLabel(recovery.durationMs)}`;
  return "not available";
}

function completionLabel(guidance: OperationalGuidance): string {
  const values = [...new Set(guidance.completionSemantics.map(({ label }) => label))].slice(0, 2);
  return values.length === 0 ? "not available" : values.map(tableCell).join("<br>");
}

function guidanceLabel(guidance: OperationalGuidance): string {
  const reasons = guidance.rating.reasons.map(tableCell).join("<br>");
  if (guidance.rating.value === "clear") return `clear — ${reasons}`;
  if (guidance.rating.value === "watch") return `watch — ${reasons}`;
  if (guidance.rating.value === "constrained") return `constrained — ${reasons}`;
  return `not rated — ${reasons}`;
}

function milliseconds(value: number): string {
  return `${value.toFixed(2)} ms`;
}

function evidenceLabel(result: DestroyerReportEntry, run: DestroyerReportRun): string {
  const artifactName = run.attempt === null ? null : `scenario-${result.scenario}-${run.attempt}`;
  const artifact = artifactName === null
    ? null
    : run.url === null
      ? `\`${artifactName}\``
      : `[\`${artifactName}\`](${run.url})`;
  const runId = result.runId === null ? null : `<code>${escapeHtml(result.runId)}</code>`;
  if (artifact !== null && runId !== null) return `${artifact}<br>${runId}`;
  return artifact ?? runId ?? "not available";
}

function warningDetails(warning: ReportWarning): string {
  if (warning.details === null) return "-";
  return `<code>${tableCell(escapeHtml(JSON.stringify(warning.details)))}</code>`;
}

function brokerSummaryLabel(summary: Readonly<Record<string, number | null>>): string {
  const fields = Object.entries(summary).map(([key, value]) => `${humanize(key)}=${value ?? "unknown"}`);
  return fields.length === 0 ? "not available" : fields.join(", ");
}

function humanize(value: string): string {
  return value.replace(/([a-z0-9])([A-Z])/gu, "$1 $2").toLowerCase();
}

function outcomeLabel(outcome: string): string {
  if (outcome === "success") return "✅ PASS";
  if (outcome === "failure") return "❌ FAIL";
  return `⚠️ ${tableCell(outcome).toUpperCase()}`;
}

function verdictLabel(verdict: DestroyerReportEntry["verdict"]): string {
  if (verdict === "passed") return "✅ PASS";
  if (verdict === "failed") return "❌ FAIL";
  return "⚠️ MISSING";
}

function durationLabel(durationMs: number | null): string {
  if (durationMs === null) return "-";
  if (durationMs < 60_000) return `${(durationMs / 1_000).toFixed(1)} s`;
  const totalSeconds = durationMs / 1_000;
  const hours = Math.floor(totalSeconds / 3_600);
  const minutes = Math.floor((totalSeconds % 3_600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) return `${hours}h ${minutes}m ${seconds.toFixed(1)}s`;
  return `${minutes}m ${seconds.toFixed(1)}s`;
}

function diagnosticQuote(value: string): string[] {
  const normalizedValue = value.replace(/\r\n?/gu, "\n").trim();
  const bounded = boundedDiagnostic(normalizedValue).replace(
    /; (?=(?:Error|AssertionError|TimeoutError):)/gu,
    ";\n",
  );
  return bounded.split("\n").map((line) => `> ${escapeHtml(line)}`);
}

function boundedDiagnostic(value: string): string {
  const maximumLength = 12_000;
  if (value.length <= maximumLength) return value;
  const headLength = 9_000;
  const tailLength = 2_000;
  const omitted = value.length - headLength - tailLength;
  return `${value.slice(0, headLength)}\n\n… ${omitted} characters omitted …\n\n${value.slice(-tailLength)}`;
}

function sentenceList(values: readonly string[]): string {
  if (values.length === 0) return "an unspecified report signal failed";
  if (values.length === 1) return values[0]!;
  if (values.length === 2) return `${values[0]} and ${values[1]}`;
  return `${values.slice(0, -1).join(", ")}, and ${values.at(-1)}`;
}

function counted(count: number, noun: string): string {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

function normalized(value: string | undefined): string | null {
  const result = value?.trim();
  return result === undefined || result === "" ? null : result;
}

function tableCell(value: string): string {
  return value.replace(/\|/gu, "\\|").replace(/\r?\n/gu, " ");
}

function escapeHtml(value: string): string {
  return value.replace(/&/gu, "&amp;").replace(/</gu, "&lt;").replace(/>/gu, "&gt;");
}

function emptyAnalysis(
  workloadDurationMs: number | null,
  reason = "Operational artifact analysis was unavailable.",
): ScenarioReportAnalysis {
  return {
    observations: [],
    warnings: [],
    brokerSummary: null,
    operationalGuidance: emptyGuidance(workloadDurationMs, reason),
  };
}
