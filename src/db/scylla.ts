/**
 * ScyllaDB database connection and schema management.
 *
 * This module handles establishing connections to ScyllaDB, creating the keyspace,
 * and defining the database schema for the NMOS registry (resources, change log, health, subscriptions).
 */
import cassandra from "cassandra-driver";
import type { RegistryConfig } from "../config.js";
import logger from "../logger.js";

/**
 * ScyllaDB connection wrapper.
 * Provides the Cassandra client and a shutdown method for graceful cleanup.
 */
export type Scylla = {
  client: cassandra.Client;
  shutdown(): Promise<void>;
};

/**
 * Establishes a connection to ScyllaDB and initializes the database schema.
 * Creates the keyspace, tables, and applies any necessary schema migrations.
 * Returns a Scylla wrapper with the client and shutdown handler.
 */
export async function connectScylla(config: RegistryConfig): Promise<Scylla> {
  logger.info("Connecting to ScyllaDB", {
    contactPoints: config.scylla.contactPoints,
    localDataCenter: config.scylla.localDataCenter,
  });
  const client = new cassandra.Client({
    contactPoints: config.scylla.contactPoints,
    localDataCenter: config.scylla.localDataCenter,
    protocolOptions: { port: 9042 },
    socketOptions: { connectTimeout: 10_000 },
    policies: {
      reconnection:
        new cassandra.policies.reconnection.ExponentialReconnectionPolicy(
          1000,
          60_000,
        ),
    },
  });

  await client.connect();
  logger.info("Connected to ScyllaDB");

  const ks = config.scylla.keyspace;
  const dc = config.scylla.localDataCenter.replace(/'/g, "");
  const rf = config.scylla.replicationFactor;
  logger.info("Creating keyspace if not exists", {
    keyspace: ks,
    dataCenter: dc,
    replicationFactor: rf,
  });
  await client.execute(
    `CREATE KEYSPACE IF NOT EXISTS ${ks} WITH replication = ` +
      `{'class': 'NetworkTopologyStrategy', '${dc}': ${rf}} AND durable_writes = true`,
  );

  await client.execute(`USE ${ks}`);
  logger.info("Using keyspace", { keyspace: ks });

  logger.info("Creating resources table");
  await client.execute(`
    CREATE TABLE IF NOT EXISTS resources (
      resource_type text,
      id uuid,
      api_version text,
      json text,
      created_tai text,
      updated_tai text,
      PRIMARY KEY ((resource_type), id)
    )
  `);

  logger.info("Creating node_health table");
  await client.execute(`
    CREATE TABLE IF NOT EXISTS node_health (
      node_id uuid PRIMARY KEY,
      json text,
      updated_tai text
    )
  `);

  logger.info("Creating change_log table");
  await client.execute(`
    CREATE TABLE IF NOT EXISTS change_log (
      hour_bucket text,
      event_id timeuuid,
      resource_type text,
      resource_id uuid,
      action text,
      pre_json text,
      post_json text,
      resource_api_version text,
      PRIMARY KEY ((hour_bucket), event_id)
    ) WITH CLUSTERING ORDER BY (event_id ASC)
  `);

  try {
    await client.execute(
      `ALTER TABLE change_log ADD resource_api_version text`,
    );
    logger.info("Added resource_api_version column to change_log");
  } catch {
    /* column already present on older deployments */
    logger.debug("resource_api_version column already exists in change_log");
  }

  logger.info("Creating persisted_subscriptions table");
  await client.execute(`
    CREATE TABLE IF NOT EXISTS persisted_subscriptions (
      id uuid PRIMARY KEY,
      json text
    )
  `);

  try {
    await client.execute(
      `ALTER TABLE change_log WITH default_time_to_live = ${config.changeLogTtlSeconds}`,
    );
    logger.info("Set change_log TTL", { ttl: config.changeLogTtlSeconds });
  } catch {
    /* TTL may already match */
    logger.debug("change_log TTL already set");
  }

  return {
    client,
    async shutdown() {
      logger.info("Shutting down ScyllaDB client");
      await client.shutdown();
      logger.info("ScyllaDB client shut down");
    },
  };
}
