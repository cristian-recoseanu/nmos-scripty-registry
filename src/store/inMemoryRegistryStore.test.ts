/**
 * Tests for in-memory registry store implementation.
 *
 * Tests resource CRUD operations, change log, and subscription persistence.
 */
import cassandra from "cassandra-driver";
import { describe, expect, it } from "vitest";
import { InMemoryRegistryStore } from "./inMemoryRegistryStore.js";
import { utcHourBucket } from "./timeBuckets.js";

/**
 * Helper function to generate random UUIDs for tests.
 */
const id = () => cassandra.types.Uuid.random();

describe("InMemoryRegistryStore", () => {
  /**
   * Tests that upsert creates and updates resources, recording change log events.
   */
  it("upsert creates then updates and records change_log", async () => {
    const store = new InMemoryRegistryStore();
    const nid = id();
    const { created } = await store.upsertResource(
      "nodes",
      nid,
      "v1.3",
      `{"id":"${nid}"}`,
    );
    expect(created).toBe(true);
    const { created: updated } = await store.upsertResource(
      "nodes",
      nid,
      "v1.3",
      `{"id":"${nid}","x":1}`,
    );
    expect(updated).toBe(false);
    const bucket = utcHourBucket();
    const changes = await store.fetchChangesAfter(bucket, undefined, 50);
    expect(changes.length).toBeGreaterThanOrEqual(2);
    expect(changes.some((c) => c.action === "create")).toBe(true);
    expect(changes.some((c) => c.action === "update")).toBe(true);
  });

  /**
   * Tests that resource deletion appends a delete event to the change log.
   */
  it("deleteResource appends delete event", async () => {
    const store = new InMemoryRegistryStore();
    const nid = id();
    await store.upsertResource("nodes", nid, "v1.3", `{"id":"${nid}"}`);
    const before = (
      await store.fetchChangesAfter(utcHourBucket(), undefined, 100)
    ).length;
    const ok = await store.deleteResource("nodes", nid);
    expect(ok).toBe(true);
    const after = await store.fetchChangesAfter(
      utcHourBucket(),
      undefined,
      100,
    );
    expect(after.length).toBeGreaterThanOrEqual(before + 1);
    expect(after.some((c) => c.action === "delete")).toBe(true);
  });

  /**
   * Tests that change log pagination is idempotent when using cursors.
   */
  it("fetchChangesAfter is idempotent per cursor when paging poller-style", async () => {
    const store = new InMemoryRegistryStore();
    const nid = id();
    await store.upsertResource("nodes", nid, "v1.3", `{"id":"${nid}"}`);
    const bucket = utcHourBucket();
    const first = await store.fetchChangesAfter(bucket, undefined, 10);
    expect(first.length).toBe(1);
    const lastId = first[0]!.event_id;
    const second = await store.fetchChangesAfter(bucket, lastId, 10);
    expect(second.length).toBe(0);
  });

  /**
   * Tests that subscription persistence works correctly.
   */
  it("persists subscription round-trip", async () => {
    const store = new InMemoryRegistryStore();
    const sid = cassandra.types.Uuid.random();
    await store.savePersistedSubscription(sid, '{"id":"x"}', 86400);
    const list = await store.listPersistedSubscriptions();
    expect(list.some((r) => r.id.toString() === sid.toString())).toBe(true);
    await store.deletePersistedSubscription(sid);
    expect((await store.listPersistedSubscriptions()).length).toBe(0);
  });
});
