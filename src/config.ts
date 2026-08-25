import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseDomainSelection, type Domain } from "./workloads/model.js";

export type ScenarioName =
  | "clean-restart"
  | "cache-loss"
  | "chaos"
  | "notice-fanout"
  | "schedule-delivery"
  | "session-boundaries"
  | "queue-redelivery"
  | "lease-contention"
  | "hot-route-canary"
  | "protocol-abuse"
  | "rpc-pressure"
  | "rpc-stream-hose"
  | "connection-storm"
  | "domain-pressure"
  | "durability-crash-cuts"
  | "all";
export type ScaleName = "smoke" | "standard" | "large";
export type ClientProfile = "end-to-end" | "broker-isolation";

export const RPC_STREAM_MAX_FRAME_BYTES = 65_506;

type Scale = {
  resources: number;
  entriesPerResource: number;
  payloadBytes: number;
  liveConcurrency: number;
  handlerDelayMs: number;
  rpcStreamCalls: number;
  rpcStreamFrames: number;
  rpcStreamFrameBytes: number;
  rpcStreamReaderDelayMs: number;
  scheduleLeadMs: number;
};

export const SCALE_PRESETS: Readonly<Record<ScaleName, Scale>> = {
  smoke: {
    resources: 2,
    entriesPerResource: 20,
    payloadBytes: 256,
    liveConcurrency: 8,
    handlerDelayMs: 1,
    rpcStreamCalls: 2,
    rpcStreamFrames: 100,
    rpcStreamFrameBytes: 1_024,
    rpcStreamReaderDelayMs: 1,
    scheduleLeadMs: 45_000,
  },
  standard: {
    resources: 10,
    entriesPerResource: 1_000,
    payloadBytes: 1_024,
    liveConcurrency: 64,
    handlerDelayMs: 2,
    rpcStreamCalls: 25,
    rpcStreamFrames: 1_000,
    rpcStreamFrameBytes: RPC_STREAM_MAX_FRAME_BYTES,
    rpcStreamReaderDelayMs: 1,
    scheduleLeadMs: 120_000,
  },
  large: {
    resources: 10,
    entriesPerResource: 5_000,
    payloadBytes: 1_024,
    liveConcurrency: 128,
    handlerDelayMs: 2,
    rpcStreamCalls: 100,
    rpcStreamFrames: 5_000,
    rpcStreamFrameBytes: RPC_STREAM_MAX_FRAME_BYTES,
    rpcStreamReaderDelayMs: 2,
    scheduleLeadMs: 300_000,
  },
};

export type RunConfig = Scale & {
  scenario: ScenarioName;
  scale: ScaleName;
  seed: number;
  port: number;
  startupTimeoutMs: number;
  clientReplicas: number;
  phaseMs: number;
  keep: boolean;
  reuseImages: boolean;
  bombardDomains: readonly Domain[];
  clientProfile: ClientProfile;
  rootDir: string;
  fitzSourceDir: string;
};

const USAGE = `fitz-destroyer <clean-restart|cache-loss|chaos|durability-crash-cuts|hot-route-canary|lease-contention|notice-fanout|protocol-abuse|queue-redelivery|schedule-delivery|session-boundaries|rpc-pressure|rpc-stream-hose|connection-storm|domain-pressure|all> [options]

  --scale <smoke|standard|large>  Workload preset (default: smoke)
  --resources <n>                 Families per durable domain
  --entries <n>                   Entries per family
  --payload-bytes <n>             Value/body size
  --seed <n>                      Deterministic unsigned 32-bit seed
  --port <n>                      Loopback Fitz HTTP port (default: 4390)
  --startup-timeout-ms <n>        Readiness deadline (default: 180000)
  --clients <n>                   Bombard client replicas (default: 4)
  --phase-ms <n>                  Healthy traffic time around faults (default: 5000)
  --concurrency <n>               Live operations per producer/caller (scale default)
  --handler-delay-ms <n>          Live consumer/worker delay (scale default)
  --schedule-lead-ms <n>          Minimum lead before the due minute (scale default)
  --domains <list>                Bombard domains (default: all seven)
  --client-profile <name>         end-to-end or broker-isolation (default: end-to-end)
  --rpc-stream-calls <n>          Streaming RPC calls per caller (scale default)
  --rpc-stream-frames <n>         Response frames per streaming call (scale default)
  --rpc-stream-frame-bytes <n>    Bytes per streaming response frame (scale default)
  --rpc-stream-reader-delay-ms <n> Delay after each received frame (scale default)
  --reuse-images                  Skip builds and reuse existing local images
  --keep                          Preserve a successful Compose stack`;

export function usage(): string {
  return USAGE;
}

export function parseArgs(argv: readonly string[], env = process.env): RunConfig {
  const scenario = argv[0];
  if (!isScenarioName(scenario)) {
    throw new Error(`Missing or invalid scenario.\n\n${USAGE}`);
  }

  const values = new Map<string, string>();
  let keep = false;
  let reuseImages = false;
  for (let index = 1; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--keep") {
      keep = true;
      continue;
    }
    if (arg === "--reuse-images") {
      reuseImages = true;
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      throw new Error(USAGE);
    }
    if (arg === undefined || !arg.startsWith("--")) {
      throw new Error(`Unexpected argument: ${arg ?? "<missing>"}`);
    }
    const next = argv[index + 1];
    if (next === undefined || next.startsWith("--")) {
      throw new Error(`Missing value for ${arg}`);
    }
    values.set(arg, next);
    index += 1;
  }

  const known = new Set([
    "--scale",
    "--resources",
    "--entries",
    "--payload-bytes",
    "--seed",
    "--port",
    "--startup-timeout-ms",
    "--clients",
    "--phase-ms",
    "--concurrency",
    "--handler-delay-ms",
    "--schedule-lead-ms",
    "--domains",
    "--client-profile",
    "--rpc-stream-calls",
    "--rpc-stream-frames",
    "--rpc-stream-frame-bytes",
    "--rpc-stream-reader-delay-ms",
  ]);
  for (const key of values.keys()) {
    if (!known.has(key)) throw new Error(`Unknown option: ${key}`);
  }

  const scaleValue = values.get("--scale") ?? "smoke";
  if (!isScaleName(scaleValue)) throw new Error(`Invalid scale: ${scaleValue}`);
  const preset = SCALE_PRESETS[scaleValue];
  const clientProfile = values.get("--client-profile") ?? "end-to-end";
  if (!isClientProfile(clientProfile)) throw new Error(`Invalid client profile: ${clientProfile}`);
  const rootDir = resolve(fileURLToPath(new URL("..", import.meta.url)));

  return {
    scenario,
    scale: scaleValue,
    resources: integerOption(values, "--resources", preset.resources, 1, 10_000),
    entriesPerResource: integerOption(
      values,
      "--entries",
      preset.entriesPerResource,
      1,
      10_000_000,
    ),
    payloadBytes: integerOption(values, "--payload-bytes", preset.payloadBytes, 32, 8_000_000),
    seed: integerOption(values, "--seed", 424_242, 0, 0xffff_ffff),
    port: integerOption(values, "--port", 4_390, 1, 65_535),
    startupTimeoutMs: integerOption(
      values,
      "--startup-timeout-ms",
      180_000,
      1_000,
      3_600_000,
    ),
    clientReplicas: integerOption(values, "--clients", 4, 1, 100),
    phaseMs: integerOption(values, "--phase-ms", 5_000, 1_000, 600_000),
    liveConcurrency: integerOption(
      values,
      "--concurrency",
      preset.liveConcurrency,
      1,
      1_024,
    ),
    handlerDelayMs: integerOption(
      values,
      "--handler-delay-ms",
      preset.handlerDelayMs,
      0,
      60_000,
    ),
    scheduleLeadMs: integerOption(
      values,
      "--schedule-lead-ms",
      preset.scheduleLeadMs,
      10_000,
      3_600_000,
    ),
    rpcStreamCalls: integerOption(
      values,
      "--rpc-stream-calls",
      preset.rpcStreamCalls,
      1,
      1_000_000,
    ),
    rpcStreamFrames: integerOption(
      values,
      "--rpc-stream-frames",
      preset.rpcStreamFrames,
      2,
      10_000_000,
    ),
    rpcStreamFrameBytes: integerOption(
      values,
      "--rpc-stream-frame-bytes",
      preset.rpcStreamFrameBytes,
      64,
      RPC_STREAM_MAX_FRAME_BYTES,
    ),
    rpcStreamReaderDelayMs: integerOption(
      values,
      "--rpc-stream-reader-delay-ms",
      preset.rpcStreamReaderDelayMs,
      0,
      60_000,
    ),
    keep,
    reuseImages,
    bombardDomains: parseDomainSelection(values.get("--domains")),
    clientProfile,
    rootDir,
    fitzSourceDir: resolve(env.FITZ_SOURCE_DIR ?? resolve(rootDir, "../fitz")),
  };
}

function integerOption(
  values: ReadonlyMap<string, string>,
  name: string,
  fallback: number,
  minimum: number,
  maximum: number,
): number {
  const raw = values.get(name);
  if (raw === undefined) return fallback;
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return parsed;
}

function isScenarioName(value: string | undefined): value is ScenarioName {
  return (
    value === "clean-restart" ||
    value === "cache-loss" ||
    value === "chaos" ||
    value === "notice-fanout" ||
    value === "schedule-delivery" ||
    value === "session-boundaries" ||
    value === "queue-redelivery" ||
    value === "lease-contention" ||
    value === "hot-route-canary" ||
    value === "protocol-abuse" ||
    value === "rpc-pressure" ||
    value === "rpc-stream-hose" ||
    value === "connection-storm" ||
    value === "domain-pressure" ||
    value === "durability-crash-cuts" ||
    value === "all"
  );
}

function isScaleName(value: string): value is ScaleName {
  return value === "smoke" || value === "standard" || value === "large";
}

function isClientProfile(value: string): value is ClientProfile {
  return value === "end-to-end" || value === "broker-isolation";
}
