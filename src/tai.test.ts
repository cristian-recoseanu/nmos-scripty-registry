/**
 * Tests for TAI timestamp utilities.
 *
 * Tests TAI timestamp comparison and date conversion functions.
 */
import { describe, expect, it } from "vitest";
import { compareTai, taiFromDate } from "./tai.js";

describe("tai", () => {
  /**
   * Tests that TAI timestamps are compared correctly by seconds then nanoseconds.
   */
  it("compareTai orders seconds then nanoseconds", () => {
    expect(compareTai("1:0", "2:0")).toBeLessThan(0);
    expect(compareTai("2:0", "1:0")).toBeGreaterThan(0);
    expect(compareTai("1:100", "1:200")).toBeLessThan(0);
    expect(compareTai("5:5", "5:5")).toBe(0);
  });

  /**
   * Tests that dates are converted to TAI format correctly.
   */
  it("taiFromDate follows seconds:nanoseconds pattern", () => {
    const s = taiFromDate(new Date(1_700_000_000_123));
    expect(s).toMatch(/^[0-9]+:[0-9]+$/);
    const [sec, ns] = s.split(":").map(Number);
    expect(sec).toBe(1_700_000_037); // TAI = UTC + 37 leap seconds
    expect(ns).toBe(123_000_000);
  });
});
