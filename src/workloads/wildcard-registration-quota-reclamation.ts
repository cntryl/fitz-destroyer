import { createClient, type Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import type { Domain } from "./model.js";

const WILDCARD_LIMIT = 128;
const WILDCARD_DOMAINS = ["queue", "kv", "stream", "schedule", "notice", "rpc"] as const satisfies readonly Domain[];
type Registration = { unsubscribe(): Promise<void> };
export type WildcardQuotaOptions = LiveCommonOptions & { url: string };

export function wildcardQuotaPattern(domain: Domain, namespace: string, index: number): string {
  const tail = domain === "schedule" ? "*/*" : "*";
  return `${domain}://destroyer/${namespace}-quota-${String(index).padStart(3, "0")}/${tail}`;
}

export function assertWildcardQuotaEvidence(record: Readonly<Record<string, unknown>>): void {
  const expected = {
    domains: WILDCARD_DOMAINS.length,
    registrations: WILDCARD_DOMAINS.length * WILDCARD_LIMIT * 2,
    limitRejections: WILDCARD_DOMAINS.length * 2,
    unsubscribeReclaims: WILDCARD_DOMAINS.length,
    disconnectReclaims: WILDCARD_DOMAINS.length,
    canaryFailures: 0,
  };
  for (const [field, value] of Object.entries(expected)) {
    const actual = record[field];
    if (actual !== value) throw new Error(`${field}=${String(actual)}/${value}`);
  }
}

export async function runWildcardRegistrationQuotaReclamation(client: Client, options: WildcardQuotaOptions, log: LiveLog): Promise<void> {
  const startedAt = performance.now();
  let registrations = 0;
  let limitRejections = 0;
  let unsubscribeReclaims = 0;
  let disconnectReclaims = 0;
  let canaryFailures = 0;

  for (const domain of WILDCARD_DOMAINS) {
    const handles: Registration[] = [];
    for (let index = 0; index < WILDCARD_LIMIT; index += 1) {
      handles.push(await register(client, domain, wildcardQuotaPattern(domain, options.namespace, index)));
      registrations += 1;
    }
    limitRejections += await requireLimitRejection(client, domain, wildcardQuotaPattern(domain, options.namespace, WILDCARD_LIMIT));
    await Promise.all(handles.map((handle) => handle.unsubscribe()));
    try {
      const reclaimed = await register(client, domain, wildcardQuotaPattern(domain, options.namespace, WILDCARD_LIMIT));
      await reclaimed.unsubscribe();
      unsubscribeReclaims += 1;
    } catch (error) {
      canaryFailures += 1;
      throw error;
    }

    const transient = makeClient(options);
    await transient.connectWhenReady({ timeoutMs: options.requestTimeoutMs });
    try {
      for (let index = 0; index < WILDCARD_LIMIT; index += 1) {
        await register(transient, domain, wildcardQuotaPattern(domain, options.namespace, index));
        registrations += 1;
      }
      limitRejections += await requireLimitRejection(transient, domain, wildcardQuotaPattern(domain, options.namespace, WILDCARD_LIMIT));
    } finally {
      await transient.close().catch(() => undefined);
    }
    try {
      const reclaimed = await register(client, domain, wildcardQuotaPattern(domain, options.namespace, WILDCARD_LIMIT + 1));
      await reclaimed.unsubscribe();
      disconnectReclaims += 1;
    } catch (error) {
      canaryFailures += 1;
      throw error;
    }
  }

  const evidence = { domains: WILDCARD_DOMAINS.length, registrations, limitRejections, unsubscribeReclaims, disconnectReclaims, canaryFailures };
  assertWildcardQuotaEvidence(evidence);
  log("wildcard_registration_quota_reclamation_worker_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt) });
}

async function requireLimitRejection(client: Client, domain: Domain, pattern: string): Promise<number> {
  try {
    const unexpected = await register(client, domain, pattern);
    await unexpected.unsubscribe().catch(() => undefined);
  } catch (error) {
    if (error instanceof Error && error.name !== "Error") return 1;
    throw error;
  }
  throw new Error(`${domain} accepted wildcard registration ${WILDCARD_LIMIT + 1}`);
}

function register(client: Client, domain: Domain, pattern: string): Promise<Registration> {
  if (domain === "queue") return client.queue.subscribe(pattern, () => undefined);
  if (domain === "kv") return client.kv.subscribe(pattern, () => undefined);
  if (domain === "stream") return client.stream.subscribe(pattern, () => undefined);
  if (domain === "schedule") return client.schedule.subscribe(pattern, () => undefined);
  if (domain === "notice") return client.notice.subscribe(pattern, () => undefined);
  return client.rpc.registerWorker(pattern, async (_request, writer) => writer.end({ body: new Uint8Array() }), { maxConcurrency: 1 });
}

function makeClient(options: WildcardQuotaOptions): Client {
  return createClient({
    url: options.url,
    transport: "ws",
    timeout: options.requestTimeoutMs,
    reconnect: { enabled: false },
    retry: { enabled: false },
    heartbeat: { enabled: false },
  });
}
