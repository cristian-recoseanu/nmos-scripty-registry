/**
 * Tests for resource query utilities.
 *
 * Tests query parameter validation, filtering, paging, and sorting.
 */
import { describe, expect, it } from "vitest";
import {
  applyPagingAndSort,
  assertSupportedQueryParams,
  extractAttributeFilters,
  matchesFilters,
  parsePaging,
  type ResourceRow,
} from "./resourceQuery.js";

describe("resourceQuery", () => {
  /**
   * Tests that unsupported query parameters (RQL, ancestry) are flagged.
   */
  it("assertSupportedQueryParams flags RQL and ancestry", () => {
    expect(assertSupportedQueryParams({}).unsupported).toEqual([]);
    expect(
      assertSupportedQueryParams({ "query.rql": "eq(a,b)" }).unsupported,
    ).toContain("query.rql");
    expect(
      assertSupportedQueryParams({ "query.downgrade": "v1.2" }).unsupported,
    ).toEqual([]);
    expect(
      assertSupportedQueryParams({
        "query.ancestry_id": "550e8400-e29b-41d4-a716-446655440000",
      }).unsupported.length,
    ).toBeGreaterThan(0);
  });

  /**
   * Tests that reserved query parameters are excluded from attribute filters.
   */
  it("extractAttributeFilters skips reserved keys", () => {
    expect(
      extractAttributeFilters({
        label: "x",
        "paging.limit": "10",
        "query.rql": "y",
      }),
    ).toEqual({ label: "x" });
  });

  /**
   * Tests that resource objects are matched against filters correctly.
   */
  it("matchesFilters shallow string equality", () => {
    expect(matchesFilters({ label: "a", n: 3 }, { label: "a", n: "3" })).toBe(
      true,
    );
    expect(matchesFilters({ label: "a" }, { label: "b" })).toBe(false);
    expect(matchesFilters({ tags: { a: 1 } }, { tags: '{"a":1}' })).toBe(true);
  });

  /**
   * Tests that paging parameters are parsed with correct defaults.
   */
  it("parsePaging defaults order to update", () => {
    const p = parsePaging({});
    expect(p.order).toBe("update");
    expect(parsePaging({ "paging.order": "create" }).order).toBe("create");
  });

  /**
   * Tests that paging, sorting, and filtering are applied correctly.
   */
  it("applyPagingAndSort filters since/until and limits", () => {
    const rows: ResourceRow[] = [
      {
        id: "1",
        json: "{}",
        api_version: "v1.3",
        created_tai: "1:0",
        updated_tai: "10:0",
      },
      {
        id: "2",
        json: "{}",
        api_version: "v1.3",
        created_tai: "2:0",
        updated_tai: "20:0",
      },
      {
        id: "3",
        json: "{}",
        api_version: "v1.3",
        created_tai: "3:0",
        updated_tai: "30:0",
      },
    ];
    const out = applyPagingAndSort(rows, {
      since: "15:0",
      until: "25:0",
      limit: 1,
      order: "update",
    });
    expect(out.map((r) => r.id)).toEqual(["2"]);
  });
});
