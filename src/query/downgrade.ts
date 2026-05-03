/**
 * IS-04 Query API downgrade parameter handling.
 *
 * This module provides utilities for parsing and validating the query.downgrade parameter,
 * which allows clients to request resources projected to a lower minor API version.
 * Downgrade queries are only allowed within the same major version.
 */

/**
 * Parsed IS-04 version with major, minor components and original raw string.
 */
export type ParsedIs04Version = { major: number; minor: number; raw: string };

/**
 * Parses an IS-04 version string into its components.
 * Accepts formats like "v1.2", "1.2", etc. Returns null if invalid.
 */
export function parseIs04Version(
  s: string | undefined | null,
): ParsedIs04Version | null {
  if (s === undefined || s === null) return null;
  const m = String(s)
    .trim()
    .match(/^v?(\d+)\.(\d+)$/i);
  if (!m) return null;
  return { major: Number(m[1]), minor: Number(m[2]), raw: `v${m[1]}.${m[2]}` };
}

/**
 * Compares two IS-04 versions, prioritizing major version then minor version.
 * Returns negative if a < b, positive if a > b, or 0 if equal.
 */
export function compareMinorWithinMajor(
  a: ParsedIs04Version,
  b: ParsedIs04Version,
): number {
  if (a.major !== b.major) return a.major - b.major;
  return a.minor - b.minor;
}

/**
 * Result of parsing a query.downgrade parameter.
 * Either successful with the parsed version, or an error message.
 */
export type ParseDowngradeResult =
  | { ok: true; downgrade: ParsedIs04Version | null }
  | { ok: false; error: string };

/**
 * Validates the query.downgrade parameter against the Query API path version.
 * Ensures downgrade is within the same major version and not newer than the path version.
 */
export function parseQueryDowngrade(
  pathVersionRaw: string,
  downgradeParam: string | undefined,
): ParseDowngradeResult {
  const pathVer = parseIs04Version(pathVersionRaw);
  if (!pathVer) return { ok: false, error: "Invalid API path version" };
  if (downgradeParam === undefined || downgradeParam === "") {
    return { ok: true, downgrade: null };
  }
  const d = parseIs04Version(downgradeParam);
  if (!d) return { ok: false, error: "Invalid query.downgrade" };
  if (d.major !== pathVer.major) {
    return {
      ok: false,
      error: "Downgrade queries MUST NOT span major API versions",
    };
  }
  if (d.minor > pathVer.minor) {
    return {
      ok: false,
      error: "query.downgrade must not be newer than the path version",
    };
  }
  return { ok: true, downgrade: d };
}

/**
 * Determines if a stored resource should be included in Query API results.
 * Checks API version compatibility with the requested path version and optional downgrade.
 */
export function shouldIncludeStoredResource(
  pathVer: ParsedIs04Version,
  storedApiVersionRaw: string,
  downgrade: ParsedIs04Version | null,
): boolean {
  const stored = parseIs04Version(storedApiVersionRaw);
  if (!stored) return false;
  if (stored.major !== pathVer.major) return false;
  const minMinor = downgrade !== null ? downgrade.minor : pathVer.minor;
  return stored.minor >= minMinor;
}
