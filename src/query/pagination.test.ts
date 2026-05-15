/**
 * Tests for pagination functionality according to NMOS IS-04 specification.
 */
import { describe, expect, test } from "vitest";
import {
  applyPagingAndSort,
  generatePagingLinks,
  parsePaging,
  type ResourceRow,
} from "./resourceQuery.js";

describe("pagination", () => {
  const mockRows: ResourceRow[] = [
    {
      id: "1",
      json: '{"label":"Node 1"}',
      api_version: "v1.3",
      created_tai: "0:1",
      updated_tai: "0:1",
    },
    {
      id: "2",
      json: '{"label":"Node 2"}',
      api_version: "v1.3",
      created_tai: "0:2",
      updated_tai: "0:2",
    },
    {
      id: "3",
      json: '{"label":"Node 3"}',
      api_version: "v1.3",
      created_tai: "0:3",
      updated_tai: "0:3",
    },
    {
      id: "4",
      json: '{"label":"Node 4"}',
      api_version: "v1.3",
      created_tai: "0:4",
      updated_tai: "0:4",
    },
    {
      id: "5",
      json: '{"label":"Node 5"}',
      api_version: "v1.3",
      created_tai: "0:5",
      updated_tai: "0:5",
    },
  ];

  test("returns all rows in descending order by default", () => {
    const paging = parsePaging({});
    const result = applyPagingAndSort(mockRows, paging);

    expect(result.rows.map((r) => r.id)).toEqual(["5", "4", "3", "2", "1"]);
    expect(result.actualSince).toBe("0:0");
    expect(result.actualUntil).toBe("0:5");
    expect(result.hasNext).toBe(false);
    expect(result.hasPrev).toBe(false);
  });

  test("applies limit correctly", () => {
    const paging = parsePaging({ "paging.limit": "3" });
    const result = applyPagingAndSort(mockRows, paging);

    expect(result.rows.map((r) => r.id)).toEqual(["5", "4", "3"]);
    expect(result.actualSince).toBe("0:2");
    expect(result.actualUntil).toBe("0:5");
    expect(result.hasNext).toBe(false);
    expect(result.hasPrev).toBe(true);
    expect(result.prevCursor).toBe("0:2");
  });

  test("applies since filter correctly", () => {
    const paging = parsePaging({ "paging.since": "0:2" });
    const result = applyPagingAndSort(mockRows, paging);

    expect(result.rows.map((r) => r.id)).toEqual(["5", "4", "3"]);
    expect(result.actualSince).toBe("0:2");
    expect(result.actualUntil).toBe("0:5");
    expect(result.hasPrev).toBe(true);
  });

  test("applies until filter correctly", () => {
    const paging = parsePaging({ "paging.until": "0:4" });
    const result = applyPagingAndSort(mockRows, paging);

    expect(result.rows.map((r) => r.id)).toEqual(["4", "3", "2", "1"]);
    expect(result.actualSince).toBe("0:0");
    expect(result.actualUntil).toBe("0:4");
  });

  test("applies both since and until filters", () => {
    const paging = parsePaging({
      "paging.since": "0:2",
      "paging.until": "0:4",
    });
    const result = applyPagingAndSort(mockRows, paging);

    expect(result.rows.map((r) => r.id)).toEqual(["4", "3"]);
    expect(result.actualSince).toBe("0:2");
    expect(result.actualUntil).toBe("0:4");
  });

  test("uses creation order when specified", () => {
    const paging = parsePaging({ "paging.order": "create" });
    const result = applyPagingAndSort(mockRows, paging);

    expect(result.rows.map((r) => r.id)).toEqual(["5", "4", "3", "2", "1"]);
    expect(result.actualSince).toBe("0:0");
    expect(result.actualUntil).toBe("0:5");
  });

  test("generates correct Link header URLs", () => {
    const paging = parsePaging({ "paging.limit": "2" });
    const result = applyPagingAndSort(mockRows, paging);
    const links = generatePagingLinks(
      "http://example.com/x-nmos/query/v1.3/nodes",
      paging,
      result,
      { "paging.limit": "2" },
    );

    expect(links).toContain(
      '<http://example.com/x-nmos/query/v1.3/nodes?paging.limit=2&paging.since=0:5>; rel="next"',
    );
    expect(links).toContain(
      '<http://example.com/x-nmos/query/v1.3/nodes?paging.limit=2&paging.until=0:3>; rel="prev"',
    );
    expect(links).toContain(
      '<http://example.com/x-nmos/query/v1.3/nodes?paging.limit=2>; rel="last"',
    );
    expect(links).toContain(
      '<http://example.com/x-nmos/query/v1.3/nodes?paging.limit=2&paging.since=0:0>; rel="first"',
    );
  });

  test("generates prev link when not on first page", () => {
    const paging = parsePaging({ "paging.since": "0:2", "paging.limit": "2" });
    const result = applyPagingAndSort(mockRows, paging);
    const links = generatePagingLinks(
      "http://example.com/x-nmos/query/v1.3/nodes",
      paging,
      result,
      { "paging.since": "0:2", "paging.limit": "2" },
    );

    expect(links).toContain(
      '<http://example.com/x-nmos/query/v1.3/nodes?paging.limit=2&paging.since=0:4>; rel="next"',
    );
    expect(links).toContain(
      '<http://example.com/x-nmos/query/v1.3/nodes?paging.limit=2&paging.until=0:2>; rel="prev"',
    );
  });

  test("pages forward from a since cursor", () => {
    const rows: ResourceRow[] = Array.from({ length: 20 }, (_, i) => {
      const tai = `0:${i + 1}`;
      return {
        id: String(i + 1),
        json: "{}",
        api_version: "v1.3",
        created_tai: tai,
        updated_tai: tai,
      };
    });
    const paging = parsePaging({ "paging.since": "0:4", "paging.limit": "10" });
    const result = applyPagingAndSort(rows, paging);

    expect(result.rows.map((r) => r.id)).toEqual([
      "14",
      "13",
      "12",
      "11",
      "10",
      "9",
      "8",
      "7",
      "6",
      "5",
    ]);
    expect(result.actualSince).toBe("0:4");
    expect(result.actualUntil).toBe("0:14");
  });

  test("handles empty result set", () => {
    const paging = parsePaging({ "paging.since": "999:999" });
    const result = applyPagingAndSort(mockRows, paging);

    expect(result.rows).toEqual([]);
    expect(result.actualSince).toBe("999:999");
    expect(result.actualUntil).toBe("999:999");
    expect(result.hasNext).toBe(false);
    expect(result.hasPrev).toBe(true);
  });

  test("includes order parameter in links when not default", () => {
    const paging = parsePaging({
      "paging.order": "create",
      "paging.limit": "2",
    });
    const result = applyPagingAndSort(mockRows, paging);
    const links = generatePagingLinks(
      "http://example.com/x-nmos/query/v1.3/nodes",
      paging,
      result,
      { "paging.order": "create", "paging.limit": "2" },
    );

    expect(links[0]).toContain("paging.order=create");
  });

  test("preserves non-cursor query parameters in links", () => {
    const paging = parsePaging({ "paging.limit": "2" });
    const result = applyPagingAndSort(mockRows, paging);
    const links = generatePagingLinks(
      "http://example.com/x-nmos/query/v1.3/nodes",
      paging,
      result,
      { label: "Node 1", "query.downgrade": "v1.2", "paging.since": "0:1" },
    );

    expect(links[0]).toContain("label=Node+1");
    expect(links[0]).toContain("query.downgrade=v1.2");
    expect(links[0]).toContain("paging.limit=2");
    expect(links[0]).not.toContain("paging.since=0:1");
  });

  test("includes default paging limit in links when the request did not include it", () => {
    const paging = parsePaging({});
    const result = applyPagingAndSort(mockRows, paging);
    const links = generatePagingLinks(
      "http://example.com/x-nmos/query/v1.3/nodes",
      paging,
      result,
    );

    expect(links).toContain(
      '<http://example.com/x-nmos/query/v1.3/nodes?paging.limit=50&paging.since=0:5>; rel="next"',
    );
    expect(links).toContain(
      '<http://example.com/x-nmos/query/v1.3/nodes?paging.limit=50&paging.until=0:0>; rel="prev"',
    );
    expect(links).toContain(
      '<http://example.com/x-nmos/query/v1.3/nodes?paging.limit=50&paging.since=0:0>; rel="first"',
    );
    expect(links).toContain(
      '<http://example.com/x-nmos/query/v1.3/nodes?paging.limit=50>; rel="last"',
    );
  });

  test("rejects invalid limits", () => {
    expect(() => parsePaging({ "paging.limit": "abc" })).toThrow(
      "Invalid paging.limit",
    );
    expect(() => parsePaging({ "paging.limit": "-1" })).toThrow(
      "Invalid paging.limit",
    );
  });
});
