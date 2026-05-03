/**
 * Heartbeat cleanup service for garbage collection of stale nodes.
 *
 * This module implements IS-04 heartbeating garbage collection by periodically
 * cleaning up nodes that haven't sent heartbeats within the configured interval,
 * along with all their associated resources.
 */
import { taiFromDate } from "../tai.js";
import type { RegistryPort } from "./registryPort.js";
import type { RegistryConfig } from "../config.js";
import logger from "../logger.js";

/**
 * Service for cleaning up stale nodes and their resources based on heartbeat intervals.
 * Implements the IS-04 heartbeating garbage collection behavior.
 */
export class HeartbeatCleanupService {
  private readonly store: RegistryPort;
  private readonly config: RegistryConfig;
  private readonly intervalMs: number;
  private timeoutId: ReturnType<typeof setTimeout> | null = null;
  private isRunning = false;

  /**
   * Creates a new heartbeat cleanup service.
   */
  constructor(store: RegistryPort, config: RegistryConfig) {
    this.store = store;
    this.config = config;
    this.intervalMs = config.heartbeatGcIntervalSeconds * 1000;
  }

  /**
   * Start the heartbeat cleanup service.
   * Runs periodically based on the configured garbage collection interval.
   */
  start(): void {
    if (this.isRunning) {
      return;
    }

    this.isRunning = true;
    logger.info(
      `Starting heartbeat cleanup service with ${this.config.heartbeatGcIntervalSeconds}s interval`,
    );

    // Run immediately on start, then schedule periodic runs
    void this.runCleanup();
    this.scheduleNextRun();
  }

  /**
   * Stop the heartbeat cleanup service.
   */
  stop(): void {
    if (!this.isRunning) {
      return;
    }

    this.isRunning = false;
    if (this.timeoutId) {
      clearTimeout(this.timeoutId);
      this.timeoutId = null;
    }
    logger.info("Stopped heartbeat cleanup service");
  }

  /**
   * Perform a single cleanup run for stale nodes.
   */
  async runCleanup(): Promise<void> {
    try {
      const cutoffTai = this.getCutoffTai();
      const staleNodes = await this.store.listStaleNodes(cutoffTai);

      if (staleNodes.length === 0) {
        return;
      }

      logger.info(`Found ${staleNodes.length} stale nodes for cleanup`);

      for (const node of staleNodes) {
        try {
          const deletedTypes = await this.store.deleteResourcesByNode(node.id);
          logger.info(
            `Cleaned up node ${node.id} (last heartbeat: ${node.updated_tai}), deleted: ${deletedTypes.join(", ")}`,
          );
        } catch (error) {
          logger.error(`Failed to cleanup node ${node.id}`, { error });
        }
      }
    } catch (error) {
      logger.error("Heartbeat cleanup failed", { error });
    }
  }

  /**
   * Calculate the cutoff TAI timestamp for determining stale nodes.
   * Nodes with heartbeats older than this cutoff will be cleaned up.
   */
  private getCutoffTai(): string {
    const cutoffDate = new Date(Date.now() - this.intervalMs);
    return taiFromDate(cutoffDate);
  }

  /**
   * Schedule the next cleanup run.
   */
  private scheduleNextRun(): void {
    if (!this.isRunning) {
      return;
    }

    this.timeoutId = setTimeout(() => {
      if (this.isRunning) {
        void this.runCleanup().then(() => {
          this.scheduleNextRun();
        });
      }
    }, this.intervalMs);
  }
}
