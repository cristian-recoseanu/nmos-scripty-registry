/**
 * Resource query utilities for the NMOS Query API.
 *
 * This module provides functions for parsing query parameters, filtering resources,
 * applying paging, and handling attribute-based queries.
 */
import { compareTai } from "../tai.js";
import logger from "../logger.js";
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

export class PagingError extends Error {}

/**
 * Parses paging parameters from query string.
 * Defaults to update order if not specified.
 * Returns a default limit of 50 when no limit is provided to ensure pagination is always in use.
 */
export function parsePaging(q: Record<string, string | undefined>): Paging {
  const order = q["paging.order"] === "create" ? "create" : "update";
  const limitRaw = q["paging.limit"];
  let limit: number;
  if (limitRaw !== undefined) {
    const parsedLimit = Number(limitRaw);
    if (
      !Number.isInteger(parsedLimit) ||
      parsedLimit < 0 ||
      !Number.isFinite(parsedLimit)
    ) {
      throw new PagingError("Invalid paging.limit");
    }
    limit = parsedLimit;
  } else {
    limit = 50;
  }
  const since = q["paging.since"] ?? undefined;
  const until = q["paging.until"] ?? undefined;
  if (
    since !== undefined &&
    until !== undefined &&
    compareTai(since, until) > 0
  ) {
    throw new PagingError("Invalid paging range");
  }
  return {
    since,
    until,
    limit,
    order,
  };
}

/**
 * Result of applying pagination to resource rows.
 */
export type PagingResult = {
  rows: ResourceRow[];
  actualSince?: string;
  actualUntil?: string;
  hasNext: boolean;
  hasPrev: boolean;
  prevCursor?: string;
};

function rowTai(row: ResourceRow, order: Paging["order"]): string {
  return order === "create" ? row.created_tai : row.updated_tai;
}

/**
 * Applies sorting, filtering, and limiting to resource rows based on paging parameters.
 * Returns detailed paging information for header generation.
 */
export function applyPagingAndSort(
  rows: ResourceRow[],
  paging: Paging,
  snapshotUntil?: string,
): PagingResult {
  const limit = paging.limit ?? rows.length;
  const sortedAsc = [...rows].sort((a, b) => {
    const taiCmp = compareTai(rowTai(a, paging.order), rowTai(b, paging.order));
    return taiCmp !== 0 ? taiCmp : a.id.localeCompare(b.id);
  });
  let window = sortedAsc;
  if (paging.since) {
    const s = paging.since;
    const beforeSince = window.length;
    window = window.filter(
      (row) => compareTai(rowTai(row, paging.order), s) > 0,
    );
    logger.debug("After since filter", {
      since: s,
      before: beforeSince,
      after: window.length,
      firstWindowTai: window[0] ? rowTai(window[0], paging.order) : undefined,
      lastWindowTai: window[window.length - 1]
        ? rowTai(window[window.length - 1], paging.order)
        : undefined,
    });
  }
  if (paging.until) {
    const u = paging.until;
    window = window.filter(
      (row) => compareTai(rowTai(row, paging.order), u) <= 0,
    );
  }

  if (limit === 0) {
    const cursor = paging.since ?? paging.until ?? snapshotUntil ?? "0:0";
    return {
      rows: [],
      actualSince: cursor,
      actualUntil: cursor,
      hasNext: false,
      hasPrev: paging.since !== undefined,
      prevCursor: paging.since !== undefined ? cursor : undefined,
    };
  }

  if (paging.since !== undefined) {
    const pageAsc = window.slice(0, limit);
    const rowsDesc = [...pageAsc].reverse();
    const hasNext = window.length > pageAsc.length;
    const lastPageRow = pageAsc.at(-1);
    const lastReturned = lastPageRow
      ? rowTai(lastPageRow, paging.order)
      : undefined;
    const actualUntil =
      hasNext && lastReturned !== undefined
        ? lastReturned
        : (paging.until ?? lastReturned ?? paging.since);

    return {
      rows: rowsDesc,
      actualSince: paging.since,
      actualUntil,
      hasNext,
      hasPrev: true,
      prevCursor: lastReturned,
    };
  }

  const desc = [...window].reverse();
  const pageDesc = desc.slice(0, limit);
  const hasPrev = window.length > pageDesc.length;
  // prevCursor is the boundary for the prev link (first item not on current page)
  const prevCursor =
    hasPrev && desc[limit] ? rowTai(desc[limit], paging.order) : undefined;
  // When no since param provided:
  // - If hasPrev, use prevCursor (boundary before this page)
  // - Otherwise use epoch (0:0) - at the beginning of the dataset
  const actualSince = paging.since ?? prevCursor ?? "0:0";
  const actualUntil =
    paging.until ??
    snapshotUntil ??
    (pageDesc[0] ? rowTai(pageDesc[0], paging.order) : undefined) ??
    "0:0";

  return {
    rows: pageDesc,
    actualSince,
    actualUntil,
    hasNext: false,
    hasPrev,
    prevCursor,
  };
}

/**
 * Generates Link header URLs for pagination navigation.
 */
export function generatePagingLinks(
  baseUrl: string,
  paging: Paging,
  result: PagingResult,
  currentQuery: Record<string, string | undefined> = {},
): string[] {
  const links: string[] = [];
  const params: Array<[string, string]> = [];

  for (const [key, value] of Object.entries(currentQuery)) {
    if (value === undefined || key === "paging.since" || key === "paging.until")
      continue;
    if (key === "paging.limit" || key === "paging.order") continue;
    params.push([key, value]);
  }

  if (paging.limit !== undefined) {
    params.push(["paging.limit", String(paging.limit)]);
  }
  if (currentQuery["paging.order"] !== undefined) {
    params.push(["paging.order", paging.order]);
  }

  const appendLink = (
    linkParams: Array<[string, string]>,
    rel: "next" | "prev" | "first" | "last",
  ) => {
    const queryString = linkParams
      .map(
        ([key, value]) =>
          `${encodeURIComponent(key)}=${encodeURIComponent(value)
            .replace(/%20/g, "+")
            .replace(/%3A/gi, ":")}`,
      )
      .join("&");
    const href = queryString ? `${baseUrl}?${queryString}` : baseUrl;
    links.push(`<${href}>; rel="${rel}"`);
  };

  // Next link
  if (paging.limit !== undefined) {
    const nextParams: Array<[string, string]> = [
      ...params,
      ["paging.since", result.actualUntil ?? "0:0"],
    ];
    appendLink(nextParams, "next");
  }

  // Prev link
  if (paging.limit !== undefined) {
    const prevParams: Array<[string, string]> = [
      ...params,
      ["paging.until", result.actualSince ?? "0:0"],
    ];
    appendLink(prevParams, "prev");
  }

  // First link (least recent)
  {
    const firstParams: Array<[string, string]> = [
      ...params,
      ["paging.since", "0:0"],
    ];
    appendLink(firstParams, "first");
  }

  // Last link (most recent - same as no parameters)
  {
    const lastParams = [...params];
    appendLink(lastParams, "last");
  }

  return links;
}
