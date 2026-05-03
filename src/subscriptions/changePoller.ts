/**
 * Change log polling service for subscription updates.
 *
 * This module polls the change log for new events and dispatches them to subscribers.
 * It scans the current and previous hour buckets to catch recent changes.
 */
import cassandra from "cassandra-driver";
import type { RegistryPort } from "../store/registryPort.js";
import { shiftHourBucket, utcHourBucket } from "../store/timeBuckets.js";
import type { SubscriptionEventSink } from "./subscriptionEventSink.js";
import logger from "../logger.js";

/**
 * Polls the change log for new events and dispatches them to subscribers.
 * Maintains cursor positions per hour bucket to avoid reprocessing events.
 */
export class ChangePoller {
  private readonly store: RegistryPort;
  private readonly sink: SubscriptionEventSink;
  private readonly intervalMs: number;
  private timer: ReturnType<typeof setInterval> | null = null;
  private readonly lastIdPerBucket = new Map<
    string,
    cassandra.types.TimeUuid | undefined
  >();

  /**
   * Creates a new change poller.
   */
  constructor(
    store: RegistryPort,
    sink: SubscriptionEventSink,
    intervalMs: number,
  ) {
    this.store = store;
    this.sink = sink;
    this.intervalMs = intervalMs;
  }

  /**
   * Starts the change poller with periodic polling.
   */
  start() {
    if (this.timer) return;
    logger.info("Starting change poller", { intervalMs: this.intervalMs });
    this.timer = setInterval(() => void this.poll(), this.intervalMs);
  }

  /**
   * Stops the change poller.
   */
  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    logger.info("Stopped change poller");
  }

  /**
   * Returns the hour buckets to scan (current and previous hour).
   */
  private bucketsToScan(): string[] {
    const now = utcHourBucket();
    const prev = shiftHourBucket(now, -1);
    return [prev, now];
  }

  /**
   * Polls a single hour bucket for new change events.
   * Pages through results to handle large volumes of changes.
   */
  private async pollBucket(bucket: string) {
    const pageSize = 250;
    for (;;) {
      const after = this.lastIdPerBucket.get(bucket);
      const rows = await this.store.fetchChangesAfter(bucket, after, pageSize);
      if (rows.length === 0) break;
      for (const row of rows) {
        this.lastIdPerBucket.set(bucket, row.event_id);
        this.sink.dispatchEvent(row);
      }
      if (rows.length < pageSize) break;
    }
  }

  /**
   * Performs a single poll of all relevant hour buckets.
   */
  async poll() {
    for (const b of this.bucketsToScan()) {
      try {
        await this.pollBucket(b);
      } catch (err) {
        logger.error("Change poll failed", { bucket: b, error: err });
      }
    }
  }
}
