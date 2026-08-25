export const DURABLE_DOMAINS = ["queue", "kv", "stream", "schedule"] as const;
export const ALL_DOMAINS = [
  "queue",
  "kv",
  "stream",
  "schedule",
  "notice",
  "lease",
  "rpc",
] as const;

export type DurableDomain = (typeof DURABLE_DOMAINS)[number];
export type Domain = (typeof ALL_DOMAINS)[number];

export function parseDomainSelection(value: string | undefined): readonly Domain[] {
  if (value === undefined || value.trim() === "") return ALL_DOMAINS;

  const selected = new Set<Domain>();
  for (const raw of value.split(",")) {
    const domain = raw.trim();
    if (!isDomain(domain)) {
      throw new Error(
        `Domain selection contains '${domain || "<empty>"}'; expected a comma-separated subset of ${ALL_DOMAINS.join(",")}`,
      );
    }
    selected.add(domain);
  }
  return ALL_DOMAINS.filter((domain) => selected.has(domain));
}

export type WorkloadShape = {
  namespace: string;
  seed: number;
  resources: number;
  entriesPerResource: number;
  payloadBytes: number;
};

const encoder = new TextEncoder();

export function resourceRoute(
  domain: Exclude<DurableDomain, "schedule">,
  shape: WorkloadShape,
  resource: number,
): string {
  return `${domain}://destroyer/${shape.namespace}/${domain}-${pad(resource, 4)}`;
}

export function scheduleRoute(shape: WorkloadShape, resource: number, entry: number): string {
  return `schedule://destroyer/${shape.namespace}/schedule-${pad(resource, 4)}/job-${pad(entry, 8)}`;
}

export function scheduleSelector(shape: WorkloadShape): string {
  return `schedule://destroyer/${shape.namespace}/*`;
}

export function kvKey(entry: number): Uint8Array {
  return encoder.encode(`key-${pad(entry, 8)}`);
}

export function deterministicPayload(
  shape: Pick<WorkloadShape, "seed" | "payloadBytes">,
  domain: DurableDomain,
  resource: number,
  entry: number,
): Uint8Array {
  const identity = `${shape.seed}:${domain}:${resource}:${entry}`;
  let state = fnv1a(identity) || 0x9e37_79b9;
  const payload = new Uint8Array(shape.payloadBytes);
  const prefix = encoder.encode(`${domain}:${pad(resource, 4)}:${pad(entry, 8)}:`);
  payload.set(prefix.subarray(0, payload.length));
  for (let index = Math.min(prefix.length, payload.length); index < payload.length; index += 1) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    payload[index] = state & 0xff;
  }
  return payload;
}

export function assertBytesEqual(
  actual: Uint8Array,
  expected: Uint8Array,
  context: string,
): void {
  if (actual.length !== expected.length) {
    throw new Error(`${context}: byte length ${actual.length} != ${expected.length}`);
  }
  for (let index = 0; index < actual.length; index += 1) {
    if (actual[index] !== expected[index]) {
      throw new Error(`${context}: byte mismatch at offset ${index}`);
    }
  }
}

export function totalDurableEntries(shape: WorkloadShape): number {
  return DURABLE_DOMAINS.length * shape.resources * shape.entriesPerResource;
}

function fnv1a(value: string): number {
  let hash = 0x811c_9dc5;
  for (const byte of encoder.encode(value)) {
    hash ^= byte;
    hash = Math.imul(hash, 0x0100_0193);
  }
  return hash >>> 0;
}

function pad(value: number, width: number): string {
  return value.toString().padStart(width, "0");
}

function isDomain(value: string): value is Domain {
  return (ALL_DOMAINS as readonly string[]).includes(value);
}
