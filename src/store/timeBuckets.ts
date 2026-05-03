/**
 * Time bucket utilities for change log partitioning.
 *
 * This module provides functions for working with hour-based time buckets,
 * which are used to partition the change log for efficient querying.
 */
import cassandra from "cassandra-driver";

/**
 * Generates a UTC hour bucket string from a date.
 * Format: YYYYMMDDHH (e.g., "2024042912" for April 29, 2024, 12:00 UTC).
 */
export function utcHourBucket(d = new Date()): string {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const h = String(d.getUTCHours()).padStart(2, "0");
  return `${y}${m}${day}${h}`;
}

/**
 * Creates a TimeUuid representing the start of the given hour bucket.
 * Used as a lower bound for querying change log events within a bucket.
 */
export function timeUuidAtHourStart(
  hourBucket: string,
): cassandra.types.TimeUuid {
  const y = Number(hourBucket.slice(0, 4));
  const mo = Number(hourBucket.slice(4, 6)) - 1;
  const d = Number(hourBucket.slice(6, 8));
  const h = Number(hourBucket.slice(8, 10));
  return cassandra.types.TimeUuid.fromDate(
    new Date(Date.UTC(y, mo, d, h, 0, 0, 0)),
  );
}

/**
 * Shifts an hour bucket by a given number of hours.
 * Positive delta moves forward, negative delta moves backward.
 */
export function shiftHourBucket(
  hourBucket: string,
  deltaHours: number,
): string {
  const y = Number(hourBucket.slice(0, 4));
  const mo = Number(hourBucket.slice(4, 6)) - 1;
  const d = Number(hourBucket.slice(6, 8));
  const h = Number(hourBucket.slice(8, 10));
  const dt = new Date(Date.UTC(y, mo, d, h, 0, 0, 0));
  dt.setUTCHours(dt.getUTCHours() + deltaHours);
  return utcHourBucket(dt);
}
