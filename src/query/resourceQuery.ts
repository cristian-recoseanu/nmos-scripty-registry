/**
 * Resource query utilities for the NMOS Query API.
 *
 * This module provides functions for parsing query parameters, filtering resources,
 * applying paging, and handling attribute-based queries.
 */
import { compareTai } from "../tai.js";
const RESERVED = new Set([
  "paging.since",
  "paging.until",
  "paging.limit",
  "paging.order",
  "query.rql",
  "query.downgrade",
  "query.ancestry_id",
  "query.ancestry_type",
  "query.ancestry_generations",
]);

/**
 * Checks for unsupported query parameters in the request.
 * Returns a list of unsupported parameter names (e.g., RQL, ancestry queries).
 */
export function assertSupportedQueryParams(
  q: Record<string, string | undefined>,
): {
  unsupported: string[];
} {
  const unsupported: string[] = [];
  if (q["query.rql"] !== undefined) unsupported.push("query.rql");
  if (q["query.ancestry_id"] !== undefined)
    unsupported.push("query.ancestry_*");
  if (q["query.ancestry_type"] !== undefined)
    unsupported.push("query.ancestry_*");
  if (q["query.ancestry_generations"] !== undefined)
    unsupported.push("query.ancestry_*");
  return { unsupported };
}

/**
 * Database row representation of a resource.
 * Contains the resource ID, JSON data, API version, and timestamps.
 */
export type ResourceRow = {
  id: string;
  json: string;
  api_version: string;
  created_tai: string;
  updated_tai: string;
};

/**
 * Parses a JSON string into a resource object.
 */
export function parseResourceObject(json: string): Record<string, unknown> {
  return JSON.parse(json) as Record<string, unknown>;
}

/**
 * Tests if a resource object matches the provided attribute filters.
 * Supports direct value comparison and JSON string comparison for nested objects.
 */
export function matchesFilters(
  obj: Record<string, unknown>,
  filters: Record<string, string>,
): boolean {
  for (const [k, v] of Object.entries(filters)) {
    const cur = obj[k];
    if (cur === undefined) return false;
    if (typeof cur === "object" && cur !== null) {
      try {
        if (JSON.stringify(cur) !== v) return false;
      } catch {
        return false;
      }
    } else if (String(cur) !== v) return false;
  }
  return true;
}

/**
 * Extracts attribute filters from query parameters, excluding reserved parameters.
 * Reserved parameters include paging, RQL, and other special query parameters.
 */
export function extractAttributeFilters(
  query: Record<string, string | undefined>,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(query)) {
    if (v === undefined || v === "") continue;
    if (RESERVED.has(k)) continue;
    out[k] = v;
  }
  return out;
}

/**
 * Paging parameters for resource queries.
 * Includes optional since/until timestamps, limit, and sort order.
 */
export type Paging = {
  since?: string;
  until?: string;
  limit?: number;
  order: "create" | "update";
};

/**
 * Parses paging parameters from query string.
 * Defaults to update order if not specified.
 */
export function parsePaging(q: Record<string, string | undefined>): Paging {
  const order = q["paging.order"] === "create" ? "create" : "update";
  const limitRaw = q["paging.limit"];
  const limit = limitRaw !== undefined ? Number(limitRaw) : undefined;
  return {
    since: q["paging.since"] ?? undefined,
    until: q["paging.until"] ?? undefined,
    limit: limit !== undefined && Number.isFinite(limit) ? limit : undefined,
    order,
  };
}

/**
 * Applies sorting, filtering, and limiting to resource rows based on paging parameters.
 * Sorts by TAI timestamp (create or update), filters by since/until, and applies limit.
 */
export function applyPagingAndSort(
  rows: ResourceRow[],
  paging: Paging,
): ResourceRow[] {
  let r = [...rows];
  const taiKey = paging.order === "create" ? "created_tai" : "updated_tai";
  r.sort((a, b) => compareTai(a[taiKey], b[taiKey]));
  r.reverse();
  if (paging.since) {
    const s = paging.since;
    r = r.filter((row) => compareTai(row[taiKey], s) > 0);
  }
  if (paging.until) {
    const u = paging.until;
    r = r.filter((row) => compareTai(row[taiKey], u) <= 0);
  }
  if (paging.limit !== undefined && paging.limit >= 0) {
    r = r.slice(0, paging.limit);
  }
  return r;
}
