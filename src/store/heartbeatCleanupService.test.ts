/**
 * Tests for heartbeat cleanup service.
 *
 * Tests garbage collection of stale nodes and their associated resources.
 */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import cassandra from "cassandra-driver";
import { InMemoryRegistryStore } from "./inMemoryRegistryStore.js";
import { HeartbeatCleanupService } from "./heartbeatCleanupService.js";
import type { RegistryConfig } from "../config.js";
import { taiFromDate } from "../tai.js";

describe("HeartbeatCleanupService", () => {
  let store: InMemoryRegistryStore;
  let cleanupService: HeartbeatCleanupService;
  let config: RegistryConfig;

  beforeEach(() => {
    store = new InMemoryRegistryStore();
    config = {
      host: "localhost",
      port: 8080,
      supportedApiVersions: ["v1.2", "v1.3"],
      queryApiSourceId: "test-source-id",
      publicHttpBase: "http://localhost:8080",
      publicWsBase: "ws://localhost:8080",
      scylla: {
        contactPoints: ["127.0.0.1"],
        localDataCenter: "datacenter1",
        keyspace: "test",
        replicationFactor: 1,
      },
      changePollMs: 200,
      changeLogTtlSeconds: 604800,
      heartbeatGcIntervalSeconds: 12,
    };
    cleanupService = new HeartbeatCleanupService(store, config);
  });

  afterEach(() => {
    cleanupService.stop();
  });

  describe("cleanup logic", () => {
    /**
     * Tests that stale nodes and their resources are identified and cleaned up.
     */
    it("should identify and clean up stale nodes", async () => {
      const nodeId1 = cassandra.types.Uuid.random();
      const nodeId2 = cassandra.types.Uuid.random();
      const nodeId3 = cassandra.types.Uuid.random();

      // Create nodes with different heartbeat times
      const oldTime = taiFromDate(new Date(Date.now() - 20000)); // 20 seconds ago

      // Register all nodes in resources
      const nodeData1 = {
        id: nodeId1.toString(),
        label: "Test Node 1",
        description: "Fresh node",
        version: "1.0.0",
        href: "http://localhost:8080",
        hosts: [],
        services: [],
      };
      await store.upsertResource(
        "nodes",
        nodeId1,
        "v1.3",
        JSON.stringify(nodeData1),
      );

      const nodeData2 = {
        id: nodeId2.toString(),
        label: "Test Node 2",
        description: "Stale node",
        version: "1.0.0",
        href: "http://localhost:8080",
        hosts: [],
        services: [],
      };
      await store.upsertResource(
        "nodes",
        nodeId2,
        "v1.3",
        JSON.stringify(nodeData2),
      );

      const nodeData3 = {
        id: nodeId3.toString(),
        label: "Test Node 3",
        description: "Stale node with resources",
        version: "1.0.0",
        href: "http://localhost:8080",
        hosts: [],
        services: [],
      };
      await store.upsertResource(
        "nodes",
        nodeId3,
        "v1.3",
        JSON.stringify(nodeData3),
      );

      // Node 1: fresh heartbeat
      await store.upsertHealth(nodeId1, JSON.stringify({ test: "data1" }));

      // Node 2: stale heartbeat
      await store.upsertHealth(nodeId2, JSON.stringify({ test: "data2" }));
      // Manually set old timestamp for testing
      const healthMap = (store as any).health;
      healthMap.set(nodeId2.toString(), {
        json: JSON.stringify({ test: "data2" }),
        updated_tai: oldTime,
      });

      // Node 3: stale heartbeat with resources
      await store.upsertHealth(nodeId3, JSON.stringify({ test: "data3" }));
      healthMap.set(nodeId3.toString(), {
        json: JSON.stringify({ test: "data3" }),
        updated_tai: oldTime,
      });

      const deviceId = cassandra.types.Uuid.random();
      const deviceData = {
        id: deviceId.toString(),
        label: "Test Device",
        description: "Test device",
        node_id: nodeId3.toString(),
        version: "1.0.0",
        senders: [],
        receivers: [],
      };
      await store.upsertResource(
        "devices",
        deviceId,
        "v1.3",
        JSON.stringify(deviceData),
      );

      // Run cleanup
      await cleanupService.runCleanup();

      // Verify fresh node still exists
      const health1 = await store.getHealth(nodeId1);
      expect(health1).toBeTruthy();
      const node1 = await store.getResource("nodes", nodeId1);
      expect(node1).toBeTruthy();

      // Verify stale nodes are cleaned up
      const health2 = await store.getHealth(nodeId2);
      expect(health2).toBeNull();
      const node2 = await store.getResource("nodes", nodeId2);
      expect(node2).toBeNull();

      const health3 = await store.getHealth(nodeId3);
      expect(health3).toBeNull();

      // Verify resources are cleaned up for node 3
      const node3 = await store.getResource("nodes", nodeId3);
      expect(node3).toBeNull();

      const device = await store.getResource("devices", deviceId);
      expect(device).toBeNull();
    });

    /**
     * Tests that cleanup handles the case when no stale nodes exist.
     */
    it("should handle cleanup when no stale nodes exist", async () => {
      const nodeId = cassandra.types.Uuid.random();

      // Register the node in resources
      const nodeData = {
        id: nodeId.toString(),
        label: "Test Node",
        description: "Fresh node",
        version: "1.0.0",
        href: "http://localhost:8080",
        hosts: [],
        services: [],
      };
      await store.upsertResource(
        "nodes",
        nodeId,
        "v1.3",
        JSON.stringify(nodeData),
      );

      await store.upsertHealth(nodeId, JSON.stringify({ test: "data" }));

      // Should not throw error
      await cleanupService.runCleanup();

      // Node should still exist
      const health = await store.getHealth(nodeId);
      expect(health).toBeTruthy();
      const node = await store.getResource("nodes", nodeId);
      expect(node).toBeTruthy();
    });

    /**
     * Tests that orphaned health records (without node resources) are cleaned up.
     */
    it("should handle cleanup for nodes without registered resources", async () => {
      const nodeId = cassandra.types.Uuid.random();
      const oldTime = taiFromDate(new Date(Date.now() - 20000));

      // Create a health record without a corresponding node resource (orphaned health)
      await store.upsertHealth(nodeId, JSON.stringify({ test: "data" }));
      const healthMap = (store as any).health;
      healthMap.set(nodeId.toString(), {
        json: JSON.stringify({ test: "data" }),
        updated_tai: oldTime,
      });

      await cleanupService.runCleanup();

      // Orphaned health record should be cleaned up
      const health = await store.getHealth(nodeId);
      expect(health).toBeNull();
    });

    /**
     * Tests that nodes without health records are treated as stale and cleaned up.
     */
    it("should clean up nodes that exist in resources but have no health record", async () => {
      const nodeId = cassandra.types.Uuid.random();

      // Register a node without creating a health record (simulating pre-existing data)
      const nodeData = {
        id: nodeId.toString(),
        label: "Orphan Node",
        description: "Node without health record",
        version: "1.0.0",
        href: "http://localhost:8080",
        hosts: [],
        services: [],
      };
      await store.upsertResource(
        "nodes",
        nodeId,
        "v1.3",
        JSON.stringify(nodeData),
      );

      // Run cleanup - should detect orphaned node and clean it up
      await cleanupService.runCleanup();

      // Verify node is cleaned up
      const node = await store.getResource("nodes", nodeId);
      expect(node).toBeNull();
    });
  });

  describe("service lifecycle", () => {
    /**
     * Tests that the service can be started and stopped correctly.
     */
    it("should start and stop the service", () => {
      expect(cleanupService["isRunning"]).toBe(false);

      cleanupService.start();
      expect(cleanupService["isRunning"]).toBe(true);

      cleanupService.stop();
      expect(cleanupService["isRunning"]).toBe(false);
    });

    /**
     * Tests that starting an already-running service is idempotent.
     */
    it("should not start if already running", () => {
      cleanupService.start();
      const timeoutId1 = cleanupService["timeoutId"];

      cleanupService.start();
      const timeoutId2 = cleanupService["timeoutId"];

      expect(timeoutId1).toBe(timeoutId2);
      cleanupService.stop();
    });

    /**
     * Tests that the service can handle multiple start/stop cycles.
     */
    it("should handle multiple start/stop cycles", () => {
      for (let i = 0; i < 3; i++) {
        cleanupService.start();
        expect(cleanupService["isRunning"]).toBe(true);
        cleanupService.stop();
        expect(cleanupService["isRunning"]).toBe(false);
      }
    });
  });

  describe("cutoff calculation", () => {
    /**
     * Tests that the cutoff TAI timestamp is calculated correctly.
     */
    it("should calculate correct cutoff time", () => {
      const cutoff = cleanupService["getCutoffTai"]();
      const cutoffDate = new Date();
      cutoffDate.setSeconds(
        cutoffDate.getSeconds() - config.heartbeatGcIntervalSeconds,
      );
      const expectedCutoff = taiFromDate(cutoffDate);

      // Allow for small timing differences
      const diff = Math.abs(
        parseInt(cutoff.split(":")[0]) - parseInt(expectedCutoff.split(":")[0]),
      );
      expect(diff).toBeLessThanOrEqual(1);
    });
  });
});
