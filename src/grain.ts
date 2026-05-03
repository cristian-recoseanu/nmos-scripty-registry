/**
 * Grain message construction for NMOS WebSocket subscriptions.
 *
 * This module builds NMOS grain messages (IS-05 Connection Management format)
 * that are sent to WebSocket clients to notify them of resource changes.
 */
import type { ResourcePlural } from "./store/registryPort.js";
import { taiFromDate } from "./tai.js";
import logger from "./logger.js";

/**
 * Minimal configuration for grain message generation.
 * Allows tests to build grain messages without requiring full RegistryConfig.
 */
export type GrainSourceConfig = {
  queryApiSourceId: string;
};

/**
 * Converts a resource plural type to a grain topic string.
 * Topics are formatted as "/resource-type/" for NMOS grain messages.
 */
function topicForPlural(p: ResourcePlural): string {
  return `/${p}/`;
}

/**
 * Represents a single resource change in a grain message.
 * Contains the resource path and optional pre/post states.
 */
export type GrainChange = {
  path: string;
  pre?: Record<string, unknown>;
  post?: Record<string, unknown>;
};

/**
 * Builds a complete grain message for WebSocket transmission.
 * Constructs the grain envelope with metadata and change data.
 */
export function buildGrainMessage(args: {
  config: GrainSourceConfig;
  subscriptionId: string;
  topicPlural: ResourcePlural;
  changes: GrainChange[];
}): string {
  logger.debug("Building grain message", {
    subscriptionId: args.subscriptionId,
    topicPlural: args.topicPlural,
    changeCount: args.changes.length,
  });
  const ts = taiFromDate();
  const envelope = {
    grain_type: "event",
    source_id: args.config.queryApiSourceId,
    flow_id: args.subscriptionId,
    origin_timestamp: ts,
    sync_timestamp: ts,
    creation_timestamp: ts,
    rate: { numerator: 1000, denominator: 1000 },
    duration: { numerator: 1, denominator: 1000 },
    grain: {
      type: "urn:x-nmos:format:data.event",
      topic: topicForPlural(args.topicPlural),
      data: args.changes,
    },
  };
  return JSON.stringify(envelope);
}
