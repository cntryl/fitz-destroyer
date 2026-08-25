import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual, deterministicPayload } from "./model.js";

export type TransactionContentionAction = "prepare" | "contend" | "hold" | "verify";
export type TransactionContentionOptions = LiveCommonOptions & {
  seed: number;
  action: TransactionContentionAction;
  commitAtMs: number;
};

const encoder = new TextEncoder();

export async function runTransactionContention(
  client: Client,
  options: TransactionContentionOptions,
  log: LiveLog,
): Promise<void> {
  const route = transactionContentionRoute(options.namespace);
  if (options.action === "hold") {
    const transaction = await client.kv.begin(route, {
      durability: "Sync",
      signal: operationSignal(options),
    });
    await transaction.put({
      key: key("long-lived"),
      value: payload(options, 9),
      signal: operationSignal(options),
    });
    log("transaction_holder_ready", { route });
    await waitForAbort(options.signal);
    return;
  }

  if (options.action === "verify") {
    const transaction = await client.kv.begin(route, {
      mode: "ReadOnly",
      durability: "Sync",
      signal: operationSignal(options),
    });
    try {
      const longLived = await transaction.get({
        key: key("long-lived"),
        signal: operationSignal(options),
      });
      if (longLived.type !== "not-found") {
        throw new Error("Long-lived transaction mutation escaped disconnect rollback");
      }
      const contended = await transaction.get({
        key: key("contended"),
        signal: operationSignal(options),
      });
      if (contended.type !== "found") throw new Error("Contended KV winner is missing");
      const winner = matchingWriter(options, contended.value);
      log("transaction_cleanup_verified", { route, winner });
    } finally {
      await transaction.rollback().catch(() => undefined);
    }
    return;
  }

  if (options.action === "contend") {
    const writer = Number(options.workerId) + 1;
    if (writer !== 1 && writer !== 2) throw new Error(`Invalid KV contender ${options.workerId}`);
    let transaction: Awaited<ReturnType<Client["kv"]["begin"]>> | undefined;
    let stage = "begin";
    try {
      transaction = await client.kv.begin(route, {
        durability: "Sync",
        signal: operationSignal(options),
      });
      stage = "read";
      const existing = await transaction.get({
        key: key("contended"),
        signal: operationSignal(options),
      });
      if (existing.type !== "found") throw new Error("KV contention baseline is missing");
      stage = "put";
      await transaction.put({
        key: key("contended"),
        value: payload(options, writer),
        signal: operationSignal(options),
      });
      log("transaction_contender_ready", { writer, commitAtMs: options.commitAtMs });
      await sleepUntil(options.commitAtMs);
      stage = "commit";
      await transaction.commit({ signal: operationSignal(options) });
      log("transaction_contender_complete", { writer, outcome: "committed" });
    } catch (error) {
      await transaction?.rollback().catch(() => undefined);
      log("transaction_contender_complete", {
        writer,
        outcome: "rejected",
        stage,
        error: errorMessage(error),
      });
    }
    return;
  }

  await putCommitted(client, options, route, key("contended"), payload(options, 0));

  const rollback = await client.kv.begin(route, { durability: "Sync", signal: operationSignal(options) });
  await rollback.put({ key: key("rolled-back"), value: payload(options, 3), signal: operationSignal(options) });
  await rollback.rollback({ signal: operationSignal(options) });
  await assertMissing(client, options, route, key("rolled-back"), "rollback isolation");

  await putCommitted(client, options, route, key("deleted"), payload(options, 4));
  const deletion = await client.kv.begin(route, { durability: "Sync", signal: operationSignal(options) });
  await deletion.delete({ key: key("deleted"), signal: operationSignal(options) });
  await deletion.commit({ signal: operationSignal(options) });
  await assertMissing(client, options, route, key("deleted"), "delete tombstone");

  log("transaction_prepare_complete", {
    rollbackIsolated: true,
    deleteHidden: true,
  });
}

export function transactionContentionRoute(namespace: string): string {
  return `kv://destroyer/${namespace}/transaction-contention`;
}

async function putCommitted(
  client: Client,
  options: TransactionContentionOptions,
  route: string,
  itemKey: Uint8Array,
  value: Uint8Array,
): Promise<void> {
  const transaction = await client.kv.begin(route, {
    durability: "Sync",
    signal: operationSignal(options),
  });
  try {
    await transaction.put({ key: itemKey, value, signal: operationSignal(options) });
    await transaction.commit({ signal: operationSignal(options) });
  } catch (error) {
    await transaction.rollback().catch(() => undefined);
    throw error;
  }
}

async function assertMissing(
  client: Client,
  options: TransactionContentionOptions,
  route: string,
  itemKey: Uint8Array,
  label: string,
): Promise<void> {
  const transaction = await client.kv.begin(route, {
    mode: "ReadOnly",
    durability: "Sync",
    signal: operationSignal(options),
  });
  try {
    const result = await transaction.get({ key: itemKey, signal: operationSignal(options) });
    if (result.type !== "not-found") throw new Error(`${label} key remains visible`);
  } finally {
    await transaction.rollback().catch(() => undefined);
  }
}

function key(value: string): Uint8Array {
  return encoder.encode(value);
}

function payload(options: TransactionContentionOptions, identity: number): Uint8Array {
  return deterministicPayload(options, "kv", 0, identity);
}

function operationSignal(options: LiveCommonOptions): AbortSignal {
  return AbortSignal.any([options.signal, AbortSignal.timeout(options.requestTimeoutMs)]);
}

function matchingWriter(options: TransactionContentionOptions, actual: Uint8Array): number {
  for (const writer of [1, 2]) {
    try {
      assertBytesEqual(actual, payload(options, writer), "Contended KV winner");
      return writer;
    } catch {
      // Check the other deterministic contender.
    }
  }
  throw new Error("Contended KV value does not match either writer");
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

function sleepUntil(timestampMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, timestampMs - Date.now())));
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) resolve();
    else signal.addEventListener("abort", () => resolve(), { once: true });
  });
}
