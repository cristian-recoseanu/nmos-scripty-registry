/**
 * In-memory implementation of the RegistryPort interface.
 *
 * This module provides a test double and local development store that mimics ScyllaDB behavior
 * without requiring a database. Change log is kept in memory without TTL pruning.
 */
import cassandra from "cassandra-driver";
import { compareTai, taiFromDate, taiNow } from "../tai.js";
import type {
  ChangeEventRow,
  RegistryPort,
  ResourcePlural,
} from "./registryPort.js";
import { timeUuidAtHourStart, utcHourBucket } from "./timeBuckets.js";
import logger from "../logger.js";

/**
 * Internal representation of a stored resource in memory.
 */
type ResourceEntry = {
  id: cassandra.types.Uuid;
  api_version: string;
  json: string;
  created_tai: string;
  updated_tai: string;
};

/**
 * Generates a composite key for resource storage maps.
 */
function resKey(type: ResourcePlural, id: cassandra.types.Uuid): string {
  return `${type}:${id.toString()}`;
}

/**
 * Compares two TimeUuid values for sorting.
 */
function compareTimeUuid(
  a: cassandra.types.TimeUuid,
  b: cassandra.types.TimeUuid,
): number {
  const da = a.getDate().getTime();
  const db = b.getDate().getTime();
  if (da !== db) return da - db;
  return a.toString().localeCompare(b.toString());
}

/**
 * In-memory implementation of the registry store for testing and local development.
 * Provides the same behavior as ScyllaRegistryStore without requiring a database.
 */
export class InMemoryRegistryStore implements RegistryPort {
  private readonly resources = new Map<string, ResourceEntry>();
  private readonly changeLog = new Map<string, ChangeEventRow[]>();
  private readonly health = new Map<
    string,
    { json: string; updated_tai: string }
  >();
  private readonly persistedSubs = new Map<
    string,
    { id: cassandra.types.Uuid; json: string }
  >();

  /**
   * Retrieves a single resource from memory.
   */
  async getResource(
    type: ResourcePlural,
    id: cassandra.types.Uuid,
  ): Promise<{
    api_version: string;
    json: string;
    created_tai: string;
    updated_tai: string;
  } | null> {
    const row = this.resources.get(resKey(type, id));
    if (!row) return null;
    return {
      api_version: row.api_version,
      json: row.json,
      created_tai: row.created_tai,
      updated_tai: row.updated_tai,
    };
  }

  /**
   * Lists all resources of a given type from memory.
   */
  async listResources(type: ResourcePlural) {
    const prefix = `${type}:`;
    const out: ResourceEntry[] = [];
    for (const [k, row] of this.resources) {
      if (k.startsWith(prefix)) out.push(row);
    }
    return out;
  }

  /**
   * Appends a change event to the in-memory change log.
   */
  private appendChange(row: ChangeEventRow) {
    const list = this.changeLog.get(row.hour_bucket) ?? [];
    list.push(row);
    list.sort((a, b) => compareTimeUuid(a.event_id, b.event_id));
    this.changeLog.set(row.hour_bucket, list);
  }

  /**
   * Creates or updates a resource in memory.
   */
  async upsertResource(
    type: ResourcePlural,
    id: cassandra.types.Uuid,
    apiVersion: string,
    json: string,
  ): Promise<{ created: boolean; updated_tai: string }> {
    const now = taiNow();
    const key = resKey(type, id);
    const prev = this.resources.get(key);
    const createdTai = prev?.created_tai ?? now;
    const isCreate = !prev;
    this.resources.set(key, {
      id,
      api_version: apiVersion,
      json,
      created_tai: createdTai,
      updated_tai: now,
    });
    const bucket = utcHourBucket();
    this.appendChange({
      hour_bucket: bucket,
      event_id: cassandra.types.TimeUuid.now(),
      resource_type: type,
      resource_id: id,
      action: isCreate ? "create" : "update",
      pre_json: prev?.json ?? null,
      post_json: json,
      resource_api_version: apiVersion,
    });
    logger.debug("Resource upserted in memory store", {
      type,
      id: id.toString(),
      action: isCreate ? "create" : "update",
    });
    return { created: isCreate, updated_tai: now };
  }

  /**
   * Deletes a resource from memory.
   */
  async deleteResource(
    type: ResourcePlural,
    id: cassandra.types.Uuid,
  ): Promise<boolean> {
    const key = resKey(type, id);
    const prev = this.resources.get(key);
    if (!prev) return false;
    this.resources.delete(key);
    this.appendChange({
      hour_bucket: utcHourBucket(),
      event_id: cassandra.types.TimeUuid.now(),
      resource_type: type,
      resource_id: id,
      action: "delete",
      pre_json: prev.json,
      post_json: null,
      resource_api_version: prev.api_version,
    });
    logger.debug("Resource deleted from memory store", {
      type,
      id: id.toString(),
    });
    return true;
  }

  /**
   * Fetches change log events from memory after a given timestamp.
   */
  async fetchChangesAfter(
    hourBucket: string,
    after: cassandra.types.TimeUuid | undefined,
    limit: number,
  ): Promise<ChangeEventRow[]> {
    const rows = this.changeLog.get(hourBucket) ?? [];
    const min = after ?? timeUuidAtHourStart(hourBucket);
    const filtered = rows.filter((r) => compareTimeUuid(r.event_id, min) > 0);
    return filtered.slice(0, limit);
  }

  /**
   * Updates or creates a node health record in memory.
   */
  async upsertHealth(
    nodeId: cassandra.types.Uuid,
    json: string,
  ): Promise<void> {
    this.health.set(nodeId.toString(), { json, updated_tai: taiFromDate() });
  }

  /**
   * Retrieves a node's health record from memory.
   */
  async getHealth(
    nodeId: cassandra.types.Uuid,
  ): Promise<{ json: string; updated_tai: string } | null> {
    return this.health.get(nodeId.toString()) ?? null;
  }

  /**
   * Saves a persisted subscription definition to memory.
   */
  async savePersistedSubscription(
    id: cassandra.types.Uuid,
    json: string,
    _ttlSeconds: number,
  ): Promise<void> {
    this.persistedSubs.set(id.toString(), { id, json });
  }

  /**
   * Deletes a persisted subscription from memory.
   */
  async deletePersistedSubscription(id: cassandra.types.Uuid): Promise<void> {
    this.persistedSubs.delete(id.toString());
  }

  /**
   * Fetches a single persisted subscription by ID from memory.
   */
  async getPersistedSubscription(
    id: cassandra.types.Uuid,
  ): Promise<{ id: cassandra.types.Uuid; json: string } | null> {
    return this.persistedSubs.get(id.toString()) ?? null;
  }

  /**
   * Lists all persisted subscriptions from memory.
   */
  async listPersistedSubscriptions(): Promise<
    Array<{ id: cassandra.types.Uuid; json: string }>
  > {
    return [...this.persistedSubs.values()];
  }

  /**
   * Lists nodes with stale health records for garbage collection.
   */
  async listStaleNodes(
    cutoffTai: string,
  ): Promise<Array<{ id: cassandra.types.Uuid; updated_tai: string }>> {
    const stale: Array<{ id: cassandra.types.Uuid; updated_tai: string }> = [];

    // Get all nodes from resources table
    const nodes = await this.listResources("nodes");
    const nodeIds = new Set(nodes.map((n) => n.id.toString()));

    // Check nodes that exist in resources
    for (const node of nodes) {
      const nodeIdStr = node.id.toString();
      const healthRecord = this.health.get(nodeIdStr);

      if (!healthRecord) {
        // Node has no health record: use its registration time as the baseline.
        // Only stale if it was registered before the cutoff.
        if (compareTai(node.updated_tai, cutoffTai) <= 0) {
          stale.push({ id: node.id, updated_tai: node.updated_tai });
        }
      } else if (compareTai(healthRecord.updated_tai, cutoffTai) <= 0) {
        // Node has stale health record
        stale.push({ id: node.id, updated_tai: healthRecord.updated_tai });
      }
    }

    // Also check for orphaned health records (health without corresponding node resource)
    for (const [nodeIdStr, health] of this.health) {
      if (!nodeIds.has(nodeIdStr)) {
        // Health record exists but node doesn't - treat as stale
        stale.push({
          id: cassandra.types.Uuid.fromString(nodeIdStr),
          updated_tai: health.updated_tai,
        });
      }
    }

    return stale;
  }

  /**
   * Deletes all resources associated with a node from memory.
   */
  async deleteResourcesByNode(
    nodeId: cassandra.types.Uuid,
  ): Promise<ResourcePlural[]> {
    const nodeIdStr = nodeId.toString();
    const deletedTypes: ResourcePlural[] = [];

    // Delete child resources in correct order (receivers, senders, flows, sources, devices)
    const childTypes: ResourcePlural[] = [
      "receivers",
      "senders",
      "flows",
      "sources",
      "devices",
    ];

    for (const type of childTypes) {
      const resources = await this.listResources(type);
      for (const resource of resources) {
        const resourceData = JSON.parse(resource.json);

        // Check if this resource belongs to the node
        if (type === "devices" && resourceData.node_id === nodeIdStr) {
          await this.deleteResource(type, resource.id);
          deletedTypes.push(type);
        } else if (type !== "devices") {
          // For sources, flows, senders, receivers - check device_id
          const device = await this.getResource(
            "devices",
            cassandra.types.Uuid.fromString(resourceData.device_id),
          );
          if (device && JSON.parse(device.json).node_id === nodeIdStr) {
            await this.deleteResource(type, resource.id);
            deletedTypes.push(type);
          }
        }
      }
    }

    // Delete the node itself
    const nodeDeleted = await this.deleteResource("nodes", nodeId);
    if (nodeDeleted) {
      deletedTypes.push("nodes");
    }

    // Delete health record (always delete, even if node didn't exist)
    this.health.delete(nodeIdStr);

    logger.info("Deleted resources by node from memory store", {
      nodeId: nodeIdStr,
      deletedTypes,
    });
    return deletedTypes;
  }
}
