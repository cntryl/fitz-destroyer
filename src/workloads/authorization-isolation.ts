import type { Client } from "@cntryl/fitz";
import type { LiveCommonOptions, LiveLog } from "./live.js";
import { assertBytesEqual } from "./model.js";

export type AuthorizationIsolationAction = "write" | "verify";

export async function runAuthorizationIsolation(
  client: Client,
  options: LiveCommonOptions & { action: AuthorizationIsolationAction },
  log: LiveLog,
): Promise<void> {
  const startedAt = performance.now();
  const route = `kv://${options.namespace}/auth/shared`;
  const key = new TextEncoder().encode("shared-family-key");
  const value = new TextEncoder().encode(`family-value-${options.workerId}`);

  if (options.action === "write") {
    const transaction = await client.kv.begin(route, { durability: "Sync" });
    try {
      await transaction.put({ key, value });
      await transaction.commit();
    } catch (error) {
      await transaction.rollback().catch(() => undefined);
      throw error;
    }
  } else {
    const transaction = await client.kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
    try {
      const result = await transaction.get({ key });
      if (result.type !== "found") throw new Error(`${route} did not contain the family value`);
      assertBytesEqual(result.value, value, `${route} family-isolated value`);
      await transaction.rollback();
    } catch (error) {
      await transaction.rollback().catch(() => undefined);
      throw error;
    }
  }

  let denied = 0;
  try {
    await client.kv.begin(`kv://${options.namespace}-denied/auth/shared`, {
      mode: "ReadOnly",
      durability: "Sync",
    });
  } catch (error) {
    denied = 1;
    log("authorization_denial_observed", {
      code: errorField(error, "code"),
      domainCode: errorField(error, "domainCode"),
    });
  }
  if (denied !== 1) throw new Error("Cross-realm KV access was not denied");

  log("authorization_isolation_role_complete", {
    action: options.action,
    identity: options.workerId,
    ownRouteOperations: 1,
    deniedOperations: denied,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

function errorField(error: unknown, field: string): unknown {
  return typeof error === "object" && error !== null && field in error
    ? (error as Record<string, unknown>)[field]
    : undefined;
}
