/**
 * Tests for configuration module.
 *
 * Tests API version normalization and parsing functions.
 */
import { describe, expect, it } from "vitest";
import {
  normalizeApiPathVersion,
  parseSupportedApiVersions,
} from "./config.js";

describe("config version helpers", () => {
  /**
   * Tests that API version strings are normalized to RAML-style format.
   */
  it("normalizeApiPathVersion", () => {
    expect(normalizeApiPathVersion("v1.3")).toBe("v1.3");
    expect(normalizeApiPathVersion("1.3")).toBe("v1.3");
    expect(normalizeApiPathVersion("V1.2")).toBe("v1.2");
  });

  /**
   * Tests that API version strings are deduplicated and sorted by semver.
   */
  it("parseSupportedApiVersions dedupes and sorts", () => {
    expect(parseSupportedApiVersions("v1.3,v1.2")).toEqual(["v1.2", "v1.3"]);
    expect(parseSupportedApiVersions("1.3, v1.2, v1.3")).toEqual([
      "v1.2",
      "v1.3",
    ]);
  });
});
