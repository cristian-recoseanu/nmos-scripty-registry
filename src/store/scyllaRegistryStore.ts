/**
 * ScyllaDB implementation of the RegistryPort interface.
 *
 * This module provides the production persistence layer using ScyllaDB (Cassandra-compatible).
 * It handles all database operations for resources, change log, health records, and subscriptions.
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

const UPSERT_RESOURCE = `
  INSERT INTO resources (resource_type, id, api_version, json, created_tai, updated_tai)
  VALUES (?, ?, ?, ?, ?, ?)
`;

const SELECT_RESOURCE = `
  SELECT resource_type, id, api_version, json, created_tai, updated_tai
  FROM resources WHERE resource_type = ? AND id = ?
`;

const SELECT_TYPE = `
  SELECT resource_type, id, api_version, json, created_tai, updated_tai
  FROM resources WHERE resource_type = ?
`;

const DELETE_RESOURCE = `
  DELETE FROM resources WHERE resource_type = ? AND id = ?
`;

const INSERT_CHANGE = `
  INSERT INTO change_log (hour_bucket, event_id, resource_type, resource_id, action, pre_json, post_json, resource_api_version)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`;

const SELECT_CHANGES_AFTER = `
  SELECT hour_bucket, event_id, resource_type, resource_id, action, pre_json, post_json, resource_api_version
  FROM change_log WHERE hour_bucket = ? AND event_id > ?
  ORDER BY event_id ASC
  LIMIT ?
`;

const UPSERT_HEALTH = `
  INSERT INTO node_health (node_id, json, updated_tai) VALUES (?, ?, ?)
`;

const SELECT_HEALTH = `SELECT node_id, json, updated_tai FROM node_health WHERE node_id = ?`;

const UPSERT_PERSISTED_SUB = `INSERT INTO persisted_subscriptions (id, json) VALUES (?, ?)`;
const DELETE_PERSISTED_SUB = `DELETE FROM persisted_subscriptions WHERE id = ?`;
const SELECT_ALL_PERSISTED_SUBS = `SELECT id, json FROM persisted_subscriptions`;

const SELECT_STALE_NODES = `SELECT node_id, updated_tai FROM node_health`;
const SELECT_ALL_NODES = `SELECT id, updated_tai FROM resources WHERE resource_type = 'nodes'`;
const DELETE_NODE_HEALTH = `DELETE FROM node_health WHERE node_id = ?`;

/**
 * ScyllaDB-based implementation of the registry store.
 * Uses prepared statements and batch operations for performance.
 */
export class ScyllaRegistryStore implements RegistryPort {
  private readonly client: cassandra.Client;

  constructor(client: cassandra.Client) {
    this.client = client;
  }

  /**
   * Retrieves a single resource from ScyllaDB.
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
    const res = await this.client.execute(SELECT_RESOURCE, [type, id], {
      prepare: true,
    });
    const row = res.first();
    if (!row) return null;
    return {
      api_version: row.get("api_version"),
      json: row.get("json"),
      created_tai: row.get("created_tai"),
      updated_tai: row.get("updated_tai"),
    };
  }

  /**
   * Lists all resources of a given type from ScyllaDB.
   */
  async listResources(type: ResourcePlural) {
    const res = await this.client.execute(SELECT_TYPE, [type], {
      prepare: true,
    });
    return res.rows.map((row) => ({
      id: row.get("id") as cassandra.types.Uuid,
      api_version: row.get("api_version"),
      json: row.get("json"),
      created_tai: row.get("created_tai"),
      updated_tai: row.get("updated_tai"),
    }));
  }

  /**
   * Creates or updates a resource in ScyllaDB.
   * Uses a batch operation to update the resource and append to the change log atomically.
   */
  async upsertResource(
    type: ResourcePlural,
    id: cassandra.types.Uuid,
    apiVersion: string,
    json: string,
  ): Promise<{ created: boolean; updated_tai: string }> {
    const now = taiNow();
    const prev = await this.getResource(type, id);
    const createdTai = prev?.created_tai ?? now;
    const isCreate = !prev;
    const queries: Array<{ query: string; params?: unknown[] }> = [
      {
        query: UPSERT_RESOURCE,
        params: [type, id, apiVersion, json, createdTai, now],
      },
      {
        query: INSERT_CHANGE,
        params: [
          utcHourBucket(),
          cassandra.types.TimeUuid.now(),
          type,
          id,
          isCreate ? "create" : "update",
          prev?.json ?? null,
          json,
          apiVersion,
        ],
      },
    ];
    await this.client.batch(queries, { prepare: true });
    logger.debug("Resource upserted in Scylla", {
      type,
      id: id.toString(),
      action: isCreate ? "create" : "update",
    });
    return { created: isCreate, updated_tai: now };
  }

  /**
   * Deletes a resource from ScyllaDB.
   * Uses a batch operation to delete the resource and append to the change log atomically.
   */
  async deleteResource(
    type: ResourcePlural,
    id: cassandra.types.Uuid,
  ): Promise<boolean> {
    const prev = await this.getResource(type, id);
    if (!prev) return false;
    await this.client.batch(
      [
        { query: DELETE_RESOURCE, params: [type, id] },
        {
          query: INSERT_CHANGE,
          params: [
            utcHourBucket(),
            cassandra.types.TimeUuid.now(),
            type,
            id,
            "delete",
            prev.json,
            null,
            prev.api_version,
          ],
        },
      ],
      { prepare: true },
    );
    logger.debug("Resource deleted from Scylla", { type, id: id.toString() });
    return true;
  }

  /**
   * Fetches change log events from ScyllaDB after a given timestamp.
   * Queries within a specific hour bucket for efficiency.
   */
  async fetchChangesAfter(
    hourBucket: string,
    after: cassandra.types.TimeUuid | undefined,
    limit: number,
  ): Promise<ChangeEventRow[]> {
    const min = after ?? timeUuidAtHourStart(hourBucket);
    const res = await this.client.execute(
      SELECT_CHANGES_AFTER,
      [hourBucket, min, limit],
      {
        prepare: true,
      },
    );
    return res.rows.map((row) => ({
      hour_bucket: row.get("hour_bucket"),
      event_id: row.get("event_id"),
      resource_type: row.get("resource_type"),
      resource_id: row.get("resource_id"),
      action: row.get("action"),
      pre_json: row.get("pre_json"),
      post_json: row.get("post_json"),
      resource_api_version: row.get("resource_api_version") ?? null,
    }));
  }

  /**
   * Updates or creates a node health record in ScyllaDB.
   */
  async upsertHealth(
    nodeId: cassandra.types.Uuid,
    json: string,
  ): Promise<void> {
    await this.client.execute(UPSERT_HEALTH, [nodeId, json, taiFromDate()], {
      prepare: true,
    });
  }

  /**
   * Retrieves a node's health record from ScyllaDB.
   */
  async getHealth(
    nodeId: cassandra.types.Uuid,
  ): Promise<{ json: string; updated_tai: string } | null> {
    const res = await this.client.execute(SELECT_HEALTH, [nodeId], {
      prepare: true,
    });
    const row = res.first();
    if (!row) return null;
    return { json: row.get("json"), updated_tai: row.get("updated_tai") };
  }

  /**
   * Saves a persisted subscription definition to ScyllaDB.
   */
  async savePersistedSubscription(id: cassandra.types.Uuid, json: string) {
    await this.client.execute(UPSERT_PERSISTED_SUB, [id, json], {
      prepare: true,
    });
  }

  /**
   * Deletes a persisted subscription from ScyllaDB.
   */
  async deletePersistedSubscription(id: cassandra.types.Uuid) {
    await this.client.execute(DELETE_PERSISTED_SUB, [id], { prepare: true });
  }

  /**
   * Lists all persisted subscriptions from ScyllaDB.
   */
  async listPersistedSubscriptions(): Promise<
    Array<{ id: cassandra.types.Uuid; json: string }>
  > {
    const res = await this.client.execute(SELECT_ALL_PERSISTED_SUBS, [], {
      prepare: true,
    });
    return res.rows.map((row) => ({
      id: row.get("id"),
      json: row.get("json"),
    }));
  }

  /**
   * Lists nodes with stale health records for garbage collection.
   * Includes nodes without health records and orphaned health records.
   */
  async listStaleNodes(
    cutoffTai: string,
  ): Promise<Array<{ id: cassandra.types.Uuid; updated_tai: string }>> {
    // Get all nodes with health records
    const healthRes = await this.client.execute(SELECT_STALE_NODES, [], {
      prepare: true,
    });
    const nodesWithHealth = new Map<
      string,
      { id: cassandra.types.Uuid; updated_tai: string }
    >();

    for (const row of healthRes.rows) {
      const nodeId = row.get("node_id");
      const updatedTai = row.get("updated_tai");
      nodesWithHealth.set(nodeId.toString(), {
        id: nodeId,
        updated_tai: updatedTai,
      });
    }

    // Get all nodes from resources table
    const nodesRes = await this.client.execute(SELECT_ALL_NODES, [], {
      prepare: true,
    });
    const nodeIds = new Set<string>();
    const staleNodes: Array<{ id: cassandra.types.Uuid; updated_tai: string }> =
      [];

    for (const row of nodesRes.rows) {
      const nodeId = row.get("id");
      const nodeIdStr = nodeId.toString();
      nodeIds.add(nodeIdStr);

      const healthRecord = nodesWithHealth.get(nodeIdStr);

      if (!healthRecord) {
        // Node has no health record: use its registration time as the baseline.
        // Only stale if it was registered before the cutoff.
        const nodeUpdatedTai: string = row.get("updated_tai");
        if (compareTai(nodeUpdatedTai, cutoffTai) <= 0) {
          staleNodes.push({ id: nodeId, updated_tai: nodeUpdatedTai });
        }
      } else if (compareTai(healthRecord.updated_tai, cutoffTai) <= 0) {
        // Node has stale health record
        staleNodes.push(healthRecord);
      }
    }

    // Also check for orphaned health records (health without corresponding node resource)
    for (const [nodeIdStr, healthRecord] of nodesWithHealth) {
      if (!nodeIds.has(nodeIdStr)) {
        // Health record exists but node doesn't - treat as stale
        staleNodes.push(healthRecord);
      }
    }

    return staleNodes;
  }

  /**
   * Deletes all resources associated with a node (node + child resources).
   * Follows the correct deletion order: receivers, senders, flows, sources, devices, then node.
   */
  async deleteResourcesByNode(
    nodeId: cassandra.types.Uuid,
  ): Promise<ResourcePlural[]> {
    // First, get the node to find all associated resources
    const node = await this.getResource("nodes", nodeId);
    if (!node) {
      // Delete health record anyway if it exists
      await this.client.execute(DELETE_NODE_HEALTH, [nodeId], {
        prepare: true,
      });
      return [];
    }

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
        if (type === "devices" && resourceData.node_id === nodeId.toString()) {
          await this.deleteResource(type, resource.id);
          deletedTypes.push(type);
        } else if (type !== "devices") {
          // For sources, flows, senders, receivers - check device_id
          const device = await this.getResource(
            "devices",
            cassandra.types.Uuid.fromString(resourceData.device_id),
          );
          if (device && JSON.parse(device.json).node_id === nodeId.toString()) {
            await this.deleteResource(type, resource.id);
            deletedTypes.push(type);
          }
        }
      }
    }

    // Delete the node itself
    await this.deleteResource("nodes", nodeId);
    deletedTypes.push("nodes");

    // Delete health record
    await this.client.execute(DELETE_NODE_HEALTH, [nodeId], { prepare: true });

    logger.info("Deleted resources by node from Scylla", {
      nodeId: nodeId.toString(),
      deletedTypes,
    });
    return deletedTypes;
  }
}
