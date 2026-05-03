/**
 * TAI (International Atomic Time) timestamp utilities for the NMOS registry.
 *
 * This module provides functions for working with TAI timestamps in the format
 * used by NMOS IS-04: colon-separated seconds:nanoseconds.
 */

/**
 * Converts a JavaScript Date to a TAI timestamp string.
 * Format: seconds:nanoseconds (IS-04 style).
 * Defaults to current time if no date is provided.
 */
export function taiFromDate(d: Date = new Date()): string {
  const sec = Math.floor(d.getTime() / 1000);
  const ns = (d.getTime() % 1000) * 1_000_000;
  return `${sec}:${ns}`;
}

/**
 * Compares two TAI timestamp strings.
 * Returns negative if a < b, positive if a > b, or 0 if equal.
 */
export function compareTai(a: string, b: string): number {
  const [as, ans] = a.split(":").map(Number);
  const [bs, bns] = b.split(":").map(Number);
  if (as !== bs) return as - bs;
  return ans - bns;
}
