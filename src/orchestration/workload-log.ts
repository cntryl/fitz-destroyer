export function parseJsonRecords(logs: string): Record<string, unknown>[] {
  const records: Record<string, unknown>[] = [];
  for (const line of logs.split("\n")) {
    const jsonStart = line.indexOf("{");
    if (jsonStart < 0) continue;
    try {
      const value: unknown = JSON.parse(line.slice(jsonStart));
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        records.push(value as Record<string, unknown>);
      }
    } catch {
      // npm prelude and Docker diagnostics are not workload records.
    }
  }
  return records;
}

export function requiredEvent(logs: string, event: string): Record<string, unknown> {
  const record = parseJsonRecords(logs).findLast((candidate) => candidate.event === event);
  if (record === undefined) throw new Error(`Missing ${event} workload record`);
  return record;
}

export function numericField(
  record: Readonly<Record<string, unknown>>,
  field: string,
): number {
  return numericValue(record[field], field);
}

export function numericValue(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${field} is not a non-negative integer`);
  }
  return value;
}

export function recordField(
  record: Readonly<Record<string, unknown>>,
  field: string,
): Readonly<Record<string, unknown>> {
  const value = record[field];
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${field} is not an object`);
  }
  return value as Readonly<Record<string, unknown>>;
}
