import type { RunConfig } from "../config.js";
import type { PressureBrokerSample } from "../pressure.js";
import { ALL_DOMAINS, type Domain } from "../workloads/model.js";

type ProgressRecord = {
  event?: string;
  window?: Partial<Record<Domain, { success?: number }>>;
};

export type BombardTotals = Record<Domain, { success: number; error: number }>;

export async function fetchWithTransientRetry(
  fetchOnce: () => Promise<Response>,
  attempts = 3,
  retryDelayMs = 100,
): Promise<Response> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fetchOnce();
    } catch (error) {
      lastError = error;
      if (attempt < attempts) await sleep(retryDelayMs);
    }
  }
  throw lastError;
}

export async function fetchJsonObject(url: string, timeoutMs: number): Promise<Readonly<Record<string, unknown>>> {
  const response = await fetchWithTransientRetry(() =>
    fetch(url, { signal: AbortSignal.timeout(Math.min(timeoutMs, 30_000)) })
  );
  if (!response.ok) {
    throw new Error(`${new URL(url).pathname} returned HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`);
  }
  const value: unknown = await response.json();
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${new URL(url).pathname} did not return a JSON object`);
  }
  return value as Readonly<Record<string, unknown>>;
}

export async function fetchText(url: string, label: string, timeoutMs: number): Promise<string> {
  const response = await fetchWithTransientRetry(() =>
    fetch(url, { signal: AbortSignal.timeout(Math.min(timeoutMs, 30_000)) })
  );
  if (!response.ok) {
    throw new Error(`${label} returned HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`);
  }
  return response.text();
}

export function loopbackPortUrl(output: string, label: string): string {
  const address = output.trim().split("\n")[0]?.trim();
  const match = /^(?:127\.0\.0\.1|\[::1\]|0\.0\.0\.0|\[::\]):(\d+)$/u.exec(address ?? "");
  if (match?.[1] === undefined) {
    throw new Error(`Could not resolve loopback ${label} port from '${output.trim()}'`);
  }
  return `http://127.0.0.1:${match[1]}`;
}

export function parseWindowSuccesses(logs: string): Record<Domain, number> {
  const successes = Object.fromEntries(ALL_DOMAINS.map((domain) => [domain, 0])) as Record<Domain, number>;
  for (const line of logs.split("\n")) {
    const jsonStart = line.indexOf("{");
    if (jsonStart < 0) continue;
    try {
      const record = JSON.parse(line.slice(jsonStart)) as ProgressRecord;
      if (record.event !== "progress" || record.window === undefined) continue;
      for (const domain of ALL_DOMAINS) successes[domain] += record.window[domain]?.success ?? 0;
    } catch {
      // npm prelude and Docker diagnostics are not workload progress records.
    }
  }
  return successes;
}

export function emptyBombardTotals(): BombardTotals {
  return Object.fromEntries(
    ALL_DOMAINS.map((domain) => [domain, { success: 0, error: 0 }]),
  ) as BombardTotals;
}

export function elapsedMs(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}

export function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export function clientHandlerConcurrency(config: RunConfig): number {
  return config.clientProfile === "broker-isolation" ? 1_000_000 : config.liveConcurrency;
}

export function parseDockerMemoryUsage(value: string): number {
  const used = value.split("/")[0]?.trim() ?? "";
  const match = /^(\d+(?:\.\d+)?)\s*([kmgtpe]?i?b)$/iu.exec(used);
  if (match?.[1] === undefined || match[2] === undefined) {
    throw new Error(`Cannot parse Docker memory usage '${value}'`);
  }
  const amount = Number(match[1]);
  const unit = match[2].toLowerCase();
  const powers: Readonly<Record<string, number>> = {
    b: 0,
    kb: 1,
    kib: 1,
    mb: 2,
    mib: 2,
    gb: 3,
    gib: 3,
    tb: 4,
    tib: 4,
    pb: 5,
    pib: 5,
    eb: 6,
    eib: 6,
  };
  const power = powers[unit];
  if (power === undefined) throw new Error(`Cannot parse Docker memory unit '${unit}'`);
  return Math.round(amount * 1_024 ** power);
}

export function isPressureQuiescent(sample: PressureBrokerSample): boolean {
  return (
    numericField(sample.queue, "messages_ready") === 0 &&
    numericField(sample.queue, "messages_delayed") === 0 &&
    numericField(sample.queue, "messages_pending") === 0 &&
    numericField(sample.queue, "inflight_active") === 0 &&
    numericField(sample.rpc, "workers_registered") === 0 &&
    numericField(sample.rpc, "requests_pending") === 0 &&
    numericField(sample.metrics, "sessionCleanupPending") === 0
  );
}

export function prometheusMetric(text: string, name: string): number {
  let total = 0;
  let found = false;
  for (const line of text.split("\n")) {
    if (line.startsWith("#")) continue;
    const match = /^(\S+?)(?:\{[^}]*\})?\s+(-?(?:\d+(?:\.\d+)?|\.\d+)(?:e[+-]?\d+)?)$/iu.exec(
      line.trim(),
    );
    if (match?.[1] !== name || match[2] === undefined) continue;
    const value = Number(match[2]);
    if (!Number.isFinite(value)) continue;
    total += value;
    found = true;
  }
  return found ? total : 0;
}

function numericField(value: Readonly<Record<string, unknown>>, field: string): number {
  const item = value[field];
  return typeof item === "number" && Number.isFinite(item) ? item : 0;
}
