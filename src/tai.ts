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
const TAI_UTC_OFFSET = 37; // leap seconds accumulated since 1972, current value since 2017-01-01

export function taiFromDate(d: Date = new Date()): string {
  const sec = Math.floor(d.getTime() / 1000) + TAI_UTC_OFFSET;
  const ns = (d.getTime() % 1000) * 1_000_000;
  return `${sec}:${ns}`;
}

let _lastTai = "0:0";

/**
 * Returns a strictly monotonically increasing TAI timestamp.
 * If the wall clock would produce the same or earlier timestamp as the last
 * call, the nanosecond counter is incremented by 1 to ensure uniqueness.
 * This prevents sort collisions when multiple resources are registered within
 * the same millisecond.
 */
export function taiNow(): string {
  const candidate = taiFromDate();
  if (compareTai(candidate, _lastTai) > 0) {
    _lastTai = candidate;
  } else {
    const [s, ns] = _lastTai.split(":").map(Number);
    _lastTai = `${s}:${ns + 1}`;
  }
  return _lastTai;
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
