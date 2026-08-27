import { createReadStream } from "node:fs";
import { readdir, readFile } from "node:fs/promises";
import { basename, join } from "node:path";
import { createInterface } from "node:readline";
import type {
  ReportObservation,
  ReportWarning,
  ScenarioReportAnalysis,
} from "./report.js";
import type { ScenarioResult } from "./scenario.js";
import { extractOperationalGuidance } from "./operational-guidance.js";

type ObservationDefinition = {
  code: string;
  message: string;
  pattern: RegExp;
};

const observationDefinitions: readonly ObservationDefinition[] = [
  {
    code: "actor-stopped",
    message: "Fitz reported delivery to an actor that had stopped",
    pattern: /Actor has stopped/gu,
  },
  {
    code: "connection-loss",
    message: "Clients recorded a Fitz connection-loss event",
    pattern: /fitz\.connection\.lost/gu,
  },
  {
    code: "storage-object-missing",
    message: "Fitz reported a message body disappearing from storage",
    pattern: /disappeared from storage/gu,
  },
  {
    code: "queue-command-timeout",
    message: "Fitz reported a Queue actor command reply timing out",
    pattern: /Queue actor command reply failed.*timed out waiting on receive operation/gu,
  },
  {
    code: "kv-inventory-update-failure",
    message: "Fitz reported a KV inventory estimate update failure",
    pattern: /KV inventory estimate update failed/gu,
  },
];

export async function analyzeScenarioArtifacts(
  directory: string,
  result: Pick<ScenarioResult, "scenario" | "verdict" | "workloadDurationMs">,
): Promise<ScenarioReportAnalysis> {
  const pressure = await readPressureEvidence(directory);
  const operationalGuidance = await extractOperationalGuidance(
    directory,
    result.scenario,
    result.workloadDurationMs,
  );
  let observations: ReportObservation[] = [];
  const warnings = [...pressure.warnings];
  if (result.verdict === "failed") {
    try {
      observations = await analyzeLogs(directory);
    } catch (error) {
      warnings.push({
        code: "failure-logs-unreadable",
        message: error instanceof Error ? error.message : String(error),
        details: null,
      });
    }
  }
  return {
    observations,
    warnings,
    brokerSummary: pressure.brokerSummary,
    operationalGuidance,
  };
}

async function analyzeLogs(directory: string): Promise<ReportObservation[]> {
  const logPaths = (await readdir(directory, { withFileTypes: true }))
    .filter((entry) => entry.isFile() && entry.name.endsWith(".log"))
    .map((entry) => join(directory, entry.name))
    .sort();
  const observed = new Map<string, Map<string, number>>();
  for (const path of logPaths) {
    const file = basename(path);
    const lines = createInterface({ input: createReadStream(path, { encoding: "utf8" }), crlfDelay: Infinity });
    for await (const line of lines) {
      for (const definition of observationDefinitions) {
        const occurrences = line.match(definition.pattern)?.length ?? 0;
        if (occurrences === 0) continue;
        const current = observed.get(definition.code) ?? new Map<string, number>();
        current.set(file, (current.get(file) ?? 0) + occurrences);
        observed.set(definition.code, current);
      }
    }
  }
  return observationDefinitions.flatMap((definition): ReportObservation[] => {
    const result = observed.get(definition.code);
    if (result === undefined) return [];
    const entries = definition.code === "connection-loss" && [...result.keys()].some((file) => file !== "compose.log")
      ? [...result.entries()].filter(([file]) => file !== "compose.log")
      : [...result.entries()];
    return entries.length === 0
      ? []
      : [{
          code: definition.code,
          message: definition.message,
          occurrences: entries.reduce((total, [, count]) => total + count, 0),
          files: entries.map(([file]) => file).sort(),
        }];
  });
}

async function readPressureEvidence(directory: string): Promise<{
  warnings: ReportWarning[];
  brokerSummary: Readonly<Record<string, number | null>> | null;
}> {
  let value: unknown;
  try {
    value = JSON.parse(await readFile(join(directory, "pressure-evidence.json"), "utf8"));
  } catch (error) {
    if (isMissingFile(error)) return { warnings: [], brokerSummary: null };
    return {
      warnings: [{
        code: "pressure-evidence-unreadable",
        message: error instanceof Error ? error.message : String(error),
        details: null,
      }],
      brokerSummary: null,
    };
  }
  if (!isRecord(value)) return { warnings: [], brokerSummary: null };
  const warnings = Array.isArray(value.warnings)
    ? value.warnings.flatMap((warning): ReportWarning[] => {
        if (!isRecord(warning) || typeof warning.code !== "string" || typeof warning.message !== "string") {
          return [];
        }
        return [{
          code: warning.code,
          message: warning.message,
          details: isRecord(warning.details) ? warning.details : null,
        }];
      })
    : [];
  const brokerSummary = numericRecord(value.brokerSummary);
  return { warnings, brokerSummary };
}

function numericRecord(value: unknown): Readonly<Record<string, number | null>> | null {
  if (!isRecord(value)) return null;
  const entries = Object.entries(value).filter(
    (entry): entry is [string, number | null] => entry[1] === null || typeof entry[1] === "number",
  );
  return entries.length === 0 ? null : Object.fromEntries(entries);
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isMissingFile(error: unknown): boolean {
  return isRecord(error) && error.code === "ENOENT";
}
