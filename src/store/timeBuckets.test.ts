/**
 * Tests for time bucket utilities.
 *
 * Tests hour bucket formatting and shifting functions.
 */
import { describe, expect, it } from "vitest";
import { shiftHourBucket, utcHourBucket } from "./timeBuckets.js";

describe("timeBuckets", () => {
  /**
   * Tests that UTC hour buckets are formatted correctly as YYYYMMDDHH.
   */
  it("utcHourBucket formats as YYYYMMDDHH UTC", () => {
    const d = new Date(Date.UTC(2026, 2, 28, 14, 30, 0));
    expect(utcHourBucket(d)).toBe("2026032814");
  });

  /**
   * Tests that hour buckets can be shifted backward across day boundaries.
   */
  it("shiftHourBucket moves across day boundary", () => {
    expect(shiftHourBucket("2026030100", -1)).toBe("2026022823");
  });

  /**
   * Tests that hour buckets can be shifted forward by adding hours.
   */
  it("shiftHourBucket adds hours", () => {
    expect(shiftHourBucket("2026030100", 2)).toBe("2026030102");
  });
});
