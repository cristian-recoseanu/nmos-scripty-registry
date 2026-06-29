/**
 * Registry store interface and type definitions.
 *
 * This module defines the RegistryPort interface that abstracts the persistence layer,
 * allowing different implementations (ScyllaDB, in-memory) to be used interchangeably.
 */

import type cassandra from "cassandra-driver";

/**
 * All NMOS resource types in plural form.
 */
export type ResourcePlural =
  "nodes" | "devices" | "sources" | "flows" | "senders" | "receivers";

/**
 * Types of change events in the change log.
 */
export type ChangeAction = "create" | "update" | "delete";

/**
 * Database row representation of a change log event.
 * Records resource modifications for subscription notifications.
 */
export type ChangeEventRow = {
  hour_bucket: string;
  event_id: cassandra.types.TimeUuid;
  resource_type: ResourcePlural;
  resource_id: cassandra.types.Uuid;
  action: ChangeAction;
  pre_json: string | null;
  post_json: string | null;
  /** API version the resource had for this event (for Query/grain projection). */
  resource_api_version: string | null;
};

/**
 * Stored resource representation from the database.
 */
export type StoredResource = {
  id: cassandra.types.Uuid;
  api_version: string;
  json: string;
  created_tai: string;
  updated_tai: string;
};

/**
 * Persistence boundary interface for the NMOS registry.
 * Abstracts the storage layer, allowing different implementations (ScyllaDB, in-memory).
 * All methods are async to support both local and remote storage backends.
 */
export interface RegistryPort {
  /**
   * Retrieves a single resource by type and ID.
   */
  getResource(...args: [ResourcePlural, cassandra.types.Uuid]): Promise<{
    api_version: string;
    json: string;
    created_tai: string;
    updated_tai: string;
  } | null>;

  /**
   * Lists all resources of a given type.
   */
  listResources(...args: [ResourcePlural]): Promise<StoredResource[]>;

  /**
   * Creates or updates a resource. Returns created=true if created (false if updated),
   * and the server-assigned updated_tai for use in X-Paging-Timestamp responses.
   */
  upsertResource(
    ...args: [ResourcePlural, cassandra.types.Uuid, string, string]
  ): Promise<{ created: boolean; updated_tai: string }>;

  /**
   * Deletes a resource. Returns true if deleted, false if not found.
   */
  deleteResource(
    ...args: [ResourcePlural, cassandra.types.Uuid]
  ): Promise<boolean>;

  /**
   * Fetches change log events after a given timestamp within an hour bucket.
   */
  fetchChangesAfter(
    ...args: [string, cassandra.types.TimeUuid | undefined, number]
  ): Promise<ChangeEventRow[]>;

  /**
   * Updates or creates a node health record.
   */
  upsertHealth(...args: [cassandra.types.Uuid, string]): Promise<void>;

  /**
   * Retrieves a node's health record.
   */
  getHealth(...args: [cassandra.types.Uuid]): Promise<{
    json: string;
    updated_tai: string;
  } | null>;

  /**
   * Saves a persisted subscription definition.
   */
  savePersistedSubscription(
    ...args: [cassandra.types.Uuid, string, number]
  ): Promise<void>;

  /**
   * Deletes a persisted subscription.
   */
  deletePersistedSubscription(...args: [cassandra.types.Uuid]): Promise<void>;

  /**
   * Fetches a single persisted subscription by ID.
   */
  getPersistedSubscription(
    ...args: [cassandra.types.Uuid]
  ): Promise<{ id: cassandra.types.Uuid; json: string } | null>;

  /**
   * Lists all persisted subscriptions.
   */
  listPersistedSubscriptions(): Promise<
    Array<{ id: cassandra.types.Uuid; json: string }>
  >;

  /**
   * Lists nodes with stale health records (for garbage collection).
   */
  listStaleNodes(
    ...args: [string]
  ): Promise<Array<{ id: cassandra.types.Uuid; updated_tai: string }>>;
  /**
   * Deletes all resources associated with a node (node + children).
   * Returns the types of resources deleted.
   */
  deleteResourcesByNode(
    ...args: [cassandra.types.Uuid]
  ): Promise<ResourcePlural[]>;
}
