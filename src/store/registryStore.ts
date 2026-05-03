/**
 * Backward compatibility barrel file for store exports.
 *
 * Prefer importing directly from specific modules:
 * - registryPort.js for the interface and types
 * - scyllaRegistryStore.js for the ScyllaDB implementation
 * - inMemoryRegistryStore.js for the in-memory test implementation
 */
export type {
  ChangeAction,
  ChangeEventRow,
  RegistryPort,
  ResourcePlural,
  StoredResource,
} from "./registryPort.js";
export { InMemoryRegistryStore } from "./inMemoryRegistryStore.js";
export { ScyllaRegistryStore } from "./scyllaRegistryStore.js";
export {
  shiftHourBucket,
  timeUuidAtHourStart,
  utcHourBucket,
} from "./timeBuckets.js";
