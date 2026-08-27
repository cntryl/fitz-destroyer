import { createHmac } from "node:crypto";

export const DESTROYER_AUTH_SECRET = "fitz-destroyer-local-auth-only";
export const DESTROYER_AUTH_AUDIENCE = "fitz-destroyer";

export function createDestroyerToken(
  identity: "identity-a" | "identity-b",
  permissions: readonly string[],
  nowMs = Date.now(),
): string {
  const header = encode({ alg: "HS256", typ: "JWT" });
  const payload = encode({
    sub: identity,
    iss: "",
    aud: DESTROYER_AUTH_AUDIENCE,
    exp: Math.floor(nowMs / 1_000) + 3_600,
    tid: identity,
    permissions,
  });
  const input = `${header}.${payload}`;
  const signature = createHmac("sha256", DESTROYER_AUTH_SECRET)
    .update(input)
    .digest("base64url");
  return `${input}.${signature}`;
}

function encode(value: unknown): string {
  return Buffer.from(JSON.stringify(value)).toString("base64url");
}
