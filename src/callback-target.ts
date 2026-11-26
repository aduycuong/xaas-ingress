// Where the result of an API trigger's run goes: the trigger's default URL, or the one a call names in
// `X-Xaas-Callback-Url` when its origin is allowed (`triggers.callback_*` of the app). Pure, no I/O.

export type CallbackSettings = { enabled: boolean; url: string | null; allowedOrigins: string[] };

export type CallbackTarget = { ok: true; url: string | null } | { ok: false; reason: "callbacks_off" | "callback_url_not_allowed" };

/** Longest callback URL accepted (the app allows the same for the default URL). */
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

/** The URL this call's result goes to (null: no callback), or why the call is refused. */
export function resolveCallbackTarget(settings: CallbackSettings, requested: string | null | undefined): CallbackTarget {
  const asked = requested?.trim();
  if (!asked) return { ok: true, url: settings.enabled ? settings.url : null };
  if (!settings.enabled) return { ok: false, reason: "callbacks_off" };
  if (asked.length > MAX_URL_LENGTH || !callbackOriginAllowed(asked, settings.allowedOrigins)) return { ok: false, reason: "callback_url_not_allowed" };
  return { ok: true, url: new URL(asked).toString() };
}
