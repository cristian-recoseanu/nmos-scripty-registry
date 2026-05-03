/**
 * Configuration management for the NMOS registry.
 *
 * This module handles loading and validating configuration from environment variables,
 * including API versions, database connection settings, and polling intervals.
 * It provides type-safe configuration access throughout the application.
 */
import { parseIs04Version } from "./query/downgrade.js";
import logger from "./logger.js";

/**
 * Main configuration type for the NMOS registry.
 * Contains all settings needed to run the registry service.
 */
export type RegistryConfig = {
  host: string;
  port: number;
  /**
   * IS-04 API path versions exposed on both Registration and Query APIs
   * (e.g. `v1.2` and `v1.3`), sorted ascending by major/minor.
   */
  supportedApiVersions: string[];
  /** UUID string identifying this Query API instance (grain source_id). */
  queryApiSourceId: string;
  /** Base URL for advertised HTTP resources (e.g. https://registry.example.com). No trailing slash. */
  publicHttpBase: string;
  /** WebSocket base (e.g. wss://registry.example.com or ws://127.0.0.1:8080). No trailing slash. */
  publicWsBase: string;
  scylla: {
    contactPoints: string[];
    localDataCenter: string;
    keyspace: string;
    replicationFactor: number;
  };
  changePollMs: number;
  changeLogTtlSeconds: number;
  /** Heartbeat garbage collection interval in seconds (default 12 per IS-04 spec) */
  heartbeatGcIntervalSeconds: number;
};

/**
 * Helper function to read an environment variable with a fallback value.
 * Returns the environment variable if set and non-empty, otherwise returns the fallback.
 */
function envOptional(name: string, fallback: string): string {
  const v = process.env[name];
  return v !== undefined && v !== "" ? v : fallback;
}

/**
 * Normalizes API version strings to RAML-style format (e.g., "v1.3").
 * Accepts various formats like "1.3", "v1.3", or "V1.3" and standardizes them.
 */
export function normalizeApiPathVersion(raw: string): string {
  const t = raw.trim();
  const core = t.match(/^v(.+)$/i)?.[1] ?? t;
  return `v${core}`;
}

/**
 * Parses a comma-separated string of API versions into a sorted, deduplicated array.
 * Versions are sorted by semantic versioning (major, then minor).
 */
export function parseSupportedApiVersions(raw: string): string[] {
  const parts = raw
    .split(",")
    .map((s) => normalizeApiPathVersion(s.trim()))
    .filter(Boolean);
  const seen = new Set<string>();
  const uniq: string[] = [];
  for (const p of parts) {
    if (!seen.has(p)) {
      seen.add(p);
      uniq.push(p);
    }
  }
  return uniq.sort((a, b) => {
    const pa = parseIs04Version(a);
    const pb = parseIs04Version(b);
    if (!pa || !pb) return a.localeCompare(b);
    return pa.major - pb.major || pa.minor - pb.minor;
  });
}

/**
 * Loads the application configuration from environment variables.
 * Reads all required and optional settings, applies defaults, and validates the configuration.
 * Returns a fully populated RegistryConfig object.
 */
export function loadConfig(): RegistryConfig {
  logger.info("Loading configuration");
  const port = Number(envOptional("PORT", "8080"));
  const host = envOptional("HOST", "0.0.0.0");
  const supportedApiVersions = parseSupportedApiVersions(
    envOptional("NMOS_API_VERSIONS", "v1.2,v1.3"),
  );
  const queryApiSourceId = envOptional(
    "QUERY_API_SOURCE_ID",
    "00000000-0000-4000-8000-000000000001",
  );
  const publicHttpBase = envOptional(
    "PUBLIC_HTTP_BASE",
    `http://127.0.0.1:${port}`,
  );
  const publicWsBase = envOptional("PUBLIC_WS_BASE", `ws://127.0.0.1:${port}`);
  const contactPoints = envOptional("SCYLLA_CONTACT_POINTS", "127.0.0.1")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const localDataCenter = envOptional("SCYLLA_LOCAL_DC", "datacenter1");
  const keyspace = envOptional("SCYLLA_KEYSPACE", "nmos_registry");
  const replicationFactor = Number(
    envOptional("SCYLLA_REPLICATION_FACTOR", "1"),
  );
  const changePollMs = Number(envOptional("CHANGE_POLL_MS", "200"));
  const changeLogTtlSeconds = Number(
    envOptional("CHANGE_LOG_TTL_SECONDS", String(7 * 24 * 3600)),
  );
  const heartbeatGcIntervalSeconds = Number(
    envOptional("HEARTBEAT_GC_INTERVAL_SECONDS", "12"),
  );

  const config = {
    host,
    port,
    supportedApiVersions,
    queryApiSourceId,
    publicHttpBase: publicHttpBase.replace(/\/$/, ""),
    publicWsBase: publicWsBase.replace(/\/$/, ""),
    scylla: { contactPoints, localDataCenter, keyspace, replicationFactor },
    changePollMs,
    changeLogTtlSeconds,
    heartbeatGcIntervalSeconds,
  };
  logger.info("Configuration loaded", {
    host,
    port,
    supportedApiVersions,
    keyspace,
    localDataCenter,
  });
  return config;
}
