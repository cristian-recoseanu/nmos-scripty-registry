/**
 * Persisted subscription synchronization poller.
 *
 * This module periodically reconciles the persisted_subscriptions table from Scylla
 * into each instance's in-memory SubscriptionManager. This ensures multi-instance
 * consistency for the Query API /subscriptions endpoint.
 */
import type { SubscriptionManager } from "./subscriptionManager.js";
import logger from "../logger.js";

/**
 * Polls ScyllaDB for persisted subscription changes and syncs them to the local manager.
 * Ensures consistency across multiple registry instances.
 */
export class PersistedSubscriptionSyncPoller {
  private readonly mgr: SubscriptionManager;
  private readonly intervalMs: number;
  private timer: ReturnType<typeof setInterval> | null = null;
  private inFlight = false;

  /**
   * Creates a new persisted subscription sync poller.
   */
  constructor(mgr: SubscriptionManager, intervalMs: number) {
    this.mgr = mgr;
    this.intervalMs = intervalMs;
  }

  /**
   * Starts the sync poller with periodic synchronization.
   */
  start() {
    if (this.timer) return;
    logger.info("Starting persisted subscription sync poller", {
      intervalMs: this.intervalMs,
    });
    this.timer = setInterval(() => void this.syncOnce(), this.intervalMs);
  }

  /**
   * Stops the sync poller.
   */
  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    logger.info("Stopped persisted subscription sync poller");
  }

  /**
   * Performs a single synchronization of persisted subscriptions.
   */
  async syncOnce(): Promise<void> {
    if (this.inFlight) return;
    this.inFlight = true;
    try {
      await this.mgr.syncPersistedFromStore();
      logger.debug("Persisted subscription sync completed");
    } finally {
      this.inFlight = false;
    }
  }
}
