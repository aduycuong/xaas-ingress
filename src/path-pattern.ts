// Path patterns of file triggers. The same code as the app's lib/files/utils/path-pattern.ts: keep them identical.

/**
 * A glob over workspace file paths (`inbox/*.pdf`): `*` matches inside one folder, `**` across folders (`inbox/**` or
 * `inbox/**\/*.pdf`), `?` one character. Leading slashes are ignored; an empty pattern matches every file.
 */
export function pathPatternRegex(pattern: string | null | undefined): RegExp {
  const p = (pattern ?? "").trim().replace(/^\/+/, "");
  if (!p) return /^.*$/;
  let out = "";
  for (let i = 0; i < p.length; i++) {
    const c = p[i];
    if (c === "*" && p[i + 1] === "*") {
      const slash = p[i + 2] === "/";
      out += slash ? "(?:.*/)?" : ".*";
      i += slash ? 2 : 1;
    } else if (c === "*") out += "[^/]*";
    else if (c === "?") out += "[^/]";
    else out += c.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  }
  return new RegExp(`^${out}$`);
}

/** Whether a workspace file path (`inbox/a.pdf`) matches a trigger's pattern. */
export function matchesPathPattern(pattern: string | null | undefined, path: string): boolean {
  return pathPatternRegex(pattern).test(path);
}
