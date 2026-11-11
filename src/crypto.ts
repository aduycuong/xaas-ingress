// The decrypt half of xaas-workflows/lib/triggers/utils/secret-crypto.ts (same format), and the
// two ways a caller proves itself to a webhook.

const FORMAT = "v1";
const fromBase64Url = (text: string) => Uint8Array.from(atob(text.replace(/-/g, "+").replace(/_/g, "/")), (char) => char.charCodeAt(0));
const encoder = new TextEncoder();

/** The stored `v1:<iv>:<cipher>` back to the secret. Throws on a wrong key or a tampered value. */
export async function decryptSecret(stored: string, keyBase64: string): Promise<string> {
  const [format, iv, cipher] = stored.split(":");
  if (format !== FORMAT || !iv || !cipher) throw new Error("Unknown secret format.");
  const raw = Uint8Array.from(atob(keyBase64), (char) => char.charCodeAt(0));
  if (raw.length !== 32) throw new Error("TRIGGERS_ENCRYPTION_KEY must be 32 bytes, base64 encoded.");
  const key = await crypto.subtle.importKey("raw", raw, "AES-GCM", false, ["decrypt"]);
  const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv: fromBase64Url(iv) }, key, fromBase64Url(cipher));
  return new TextDecoder().decode(plain);
}

/** Constant-time comparison of two strings. */
export function sameSecret(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

/** `sha256=<hex>` of the raw body under `secret`: what a sender puts in `X-XaaS-Signature`. */
export async function signBody(secret: string, rawBody: string): Promise<string> {
  const key = await crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(rawBody)));
  return `sha256=${Array.from(signature, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}
