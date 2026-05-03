/**
 * Main entry point for the NMOS registry server.
 *
 * This module initializes all services (database, store, subscriptions, polling, cleanup),
 * creates the HTTP server, and handles graceful shutdown on SIGINT/SIGTERM.
 */
import { loadConfig } from "./config.js";
import { connectScylla } from "./db/scylla.js";
import { createApp } from "./http/createApp.js";
import { ScyllaRegistryStore } from "./store/scyllaRegistryStore.js";
import { HeartbeatCleanupService } from "./store/heartbeatCleanupService.js";
import { ChangePoller } from "./subscriptions/changePoller.js";
import { SubscriptionManager } from "./subscriptions/subscriptionManager.js";
import { PersistedSubscriptionSyncPoller } from "./subscriptions/persistedSubscriptionSyncPoller.js";
import logger from "./logger.js";

/**
 * Main application entry point.
 * Initializes all services and starts the HTTP server.
 */
async function main() {
  logger.info("Starting NMOS registry server");
  const config = loadConfig();
  const scylla = await connectScylla(config);
  const store = new ScyllaRegistryStore(scylla.client);
  logger.info("Registry store initialized");
  const subs = new SubscriptionManager(config, store);
  await subs.restorePersisted();
  logger.info("Persisted subscriptions restored");

  const persistedSyncPoller = new PersistedSubscriptionSyncPoller(
    subs,
    config.changePollMs,
  );
  persistedSyncPoller.start();
  void persistedSyncPoller.syncOnce();
  logger.info("Persisted subscription sync poller started");

  const poller = new ChangePoller(store, subs, config.changePollMs);
  poller.start();
  void poller.poll();
  logger.info("Change poller started");

  const cleanupService = new HeartbeatCleanupService(store, config);
  cleanupService.start();
  logger.info("Heartbeat cleanup service started");

  const app = await createApp({ config, store, subs });

  const shutdown = async () => {
    logger.info("Shutting down server");
    persistedSyncPoller.stop();
    poller.stop();
    cleanupService.stop();
    await app.close();
    await scylla.shutdown();
    logger.info("Server shutdown complete");
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown());
  process.on("SIGTERM", () => void shutdown());

  await app.listen({ host: config.host, port: config.port });
  logger.info(`Server listening on ${config.host}:${config.port}`);
}

main().catch((err) => {
  logger.error("Server startup failed", { error: err });
  process.exit(1);
});
