#!/usr/bin/env node

import { appendFile, mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { ScenarioResult } from "./scenario.js";
import { buildDestroyerReport, renderDestroyerReport } from "./report.js";
import { ALL_SCENARIOS } from "./suite.js";

await main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});

async function main(): Promise<void> {
  const options = parseOptions(process.argv.slice(2));
  const summaries = await loadScenarioResults(options.artifactsDir);
  const report = buildDestroyerReport(
    ALL_SCENARIOS,
    summaries,
    options.analysisResult,
    options.matrixResult,
  );
  const markdown = renderDestroyerReport(report);
  await mkdir(options.outputDir, { recursive: true });
  await Promise.all([
    writeFile(join(options.outputDir, "summary.json"), `${JSON.stringify(report, null, 2)}\n`, "utf8"),
    writeFile(join(options.outputDir, "report.md"), markdown, "utf8"),
  ]);
  const stepSummary = process.env.GITHUB_STEP_SUMMARY;
  if (stepSummary !== undefined && stepSummary !== "") {
    await appendFile(stepSummary, markdown, "utf8");
  }
  process.stdout.write(markdown);
  if (report.verdict === "failed") process.exitCode = 1;
}

type Options = {
  artifactsDir: string;
  outputDir: string;
  analysisResult: string;
  matrixResult: string;
};

function parseOptions(args: readonly string[]): Options {
  const value = (name: string): string => {
    const index = args.indexOf(name);
    const result = index < 0 ? undefined : args[index + 1];
    if (result === undefined || result.startsWith("--")) throw new Error(`${name} requires a value`);
    return result;
  };
  return {
    artifactsDir: value("--artifacts-dir"),
    outputDir: value("--output-dir"),
    analysisResult: value("--analysis-result"),
    matrixResult: value("--matrix-result"),
  };
}

async function loadScenarioResults(directory: string): Promise<ScenarioResult[]> {
  const paths = await findSummaries(directory).catch((error: unknown) => {
    if (isMissingDirectory(error)) return [];
    throw error;
  });
  const results: ScenarioResult[] = [];
  for (const path of paths) {
    try {
      const value: unknown = JSON.parse(await readFile(path, "utf8"));
      if (isScenarioResult(value)) results.push(value);
    } catch (error) {
      process.stderr.write(
        `Ignoring unreadable scenario summary ${path}: ${error instanceof Error ? error.message : String(error)}\n`,
      );
    }
  }
  return results;
}

async function findSummaries(directory: string): Promise<string[]> {
  const paths: string[] = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) paths.push(...await findSummaries(path));
    else if (entry.isFile() && entry.name === "summary.json") paths.push(path);
  }
  return paths.sort();
}

function isScenarioResult(value: unknown): value is ScenarioResult {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const record = value as Readonly<Record<string, unknown>>;
  return typeof record.scenario === "string" &&
    (record.verdict === "passed" || record.verdict === "failed") &&
    typeof record.durationMs === "number" &&
    typeof record.runId === "string" &&
    typeof record.project === "string" &&
    typeof record.artifactPath === "string" &&
    (record.cleanupState === "removed" || record.cleanupState === "preserved" || record.cleanupState === "failed");
}

function isMissingDirectory(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";
}
