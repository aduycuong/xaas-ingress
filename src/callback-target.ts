// The URL a call of an API or webhook trigger names in `X-Xaas-Callback-Url` for its own run's events, on top of the
// workflow's status callbacks: allowed when its origin is in the workflow's `callback_allowed_origins` (Workflow
// settings of the app) and the call came with an authentication that has a secret to sign them. Pure, no I/O.

export type CallbackTarget = { ok: true; url: string | null } | { ok: false; reason: "callback_url_not_allowed" | "callback_needs_secret" };

/** Longest callback URL accepted (the app allows the same for a status callback). */
const MAX_URL_LENGTH = 2000;

/**
 * True when `url` is https and its origin is one of `origins`. An origin is `https://host[:port]`, lower case; a host
 * starting with `*.` matches every subdomain of the rest (not the bare domain). No credentials in the URL.
 */
export function callbackOriginAllowed(url: string, origins: readonly string[]): boolean {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return false;
  }
  if (parsed.protocol !== "https:" || parsed.username || parsed.password) return false;
  const port = parsed.port ? `:${parsed.port}` : "";
  const host = parsed.hostname.toLowerCase();
  return origins.some((origin) => {
    const match = /^https:\/\/(\*\.)?([^/:]+)(:\d+)?$/.exec(origin.trim().toLowerCase().replace(/\/+$/, ""));
    if (!match) return false;
    const [, wildcard, base, originPort = ""] = match;
    // `:443` is the default port of https: URL drops it, so an origin written with it means the same.
    if ((originPort === ":443" ? "" : originPort) !== port) return false;
    return wildcard ? host.endsWith(`.${base}`) : host === base;
  });
}

/**
 * The URL this call's events go to (null: none asked), or why the call is refused. `canSign`: the call came with an
 * authentication that has a secret (kind None cannot sign, and an unsigned event would not be trusted).
 */
export function resolveCallbackTarget(allowedOrigins: readonly string[], requested: string | null | undefined, canSign: boolean): CallbackTarget {
  const asked = requested?.trim();
  if (!asked) return { ok: true, url: null };
  if (asked.length > MAX_URL_LENGTH || !callbackOriginAllowed(asked, allowedOrigins)) return { ok: false, reason: "callback_url_not_allowed" };
  if (!canSign) return { ok: false, reason: "callback_needs_secret" };
  return { ok: true, url: new URL(asked).toString() };
}
