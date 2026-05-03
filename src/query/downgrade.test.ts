/**
 * Tests for query downgrade parameter handling.
 *
 * Tests version parsing, downgrade validation, and resource inclusion logic.
 */
import { describe, expect, it } from "vitest";
import {
  parseIs04Version,
  parseQueryDowngrade,
  shouldIncludeStoredResource,
} from "./downgrade.js";

describe("downgrade", () => {
  const v13 = parseIs04Version("v1.3")!;
  const v12 = parseIs04Version("v1.2")!;
  const v11 = parseIs04Version("v1.1")!;

  /**
   * Tests that cross-major downgrade requests are rejected.
   */
  it("parseQueryDowngrade rejects cross-major", () => {
    const r = parseQueryDowngrade("v1.3", "v2.0");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toContain("major");
  });

  /**
   * Tests that downgrade to a newer version than the path is rejected.
   */
  it("parseQueryDowngrade rejects downgrade newer than path", () => {
    const r = parseQueryDowngrade("v1.2", "v1.3");
    expect(r.ok).toBe(false);
  });

  /**
   * Tests that without downgrade, only resources at or above the path minor are included.
   */
  it("shouldIncludeStoredResource without downgrade uses path minor as floor", () => {
    expect(shouldIncludeStoredResource(v13, "v1.3", null)).toBe(true);
    expect(shouldIncludeStoredResource(v13, "v1.4", null)).toBe(true);
    expect(shouldIncludeStoredResource(v13, "v1.2", null)).toBe(false);
  });

  /**
   * Tests that with downgrade, resources at or above the downgrade minor are included.
   */
  it("shouldIncludeStoredResource with downgrade expands to older minors", () => {
    expect(shouldIncludeStoredResource(v13, "v1.2", v12)).toBe(true);
    expect(shouldIncludeStoredResource(v13, "v1.1", v11)).toBe(true);
    expect(shouldIncludeStoredResource(v13, "v1.0", v11)).toBe(false);
  });
});
