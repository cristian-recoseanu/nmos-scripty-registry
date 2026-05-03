import cassandra from "cassandra-driver";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { RegistryConfig } from "../config.js";
import type { ChangeEventRow } from "../store/registryPort.js";
import { InMemoryRegistryStore } from "../store/inMemoryRegistryStore.js";
import { utcHourBucket } from "../store/timeBuckets.js";
import type { WebSocket } from "ws";
import { SubscriptionManager } from "./subscriptionManager.js";

function testConfig(): RegistryConfig {
  return {
    host: "127.0.0.1",
    port: 8080,
    supportedApiVersions: ["v1.2", "v1.3"],
    queryApiSourceId: "aaaaaaaa-bbbb-4ccc-bddd-eeeeeeeeeeee",
    publicHttpBase: "http://127.0.0.1:8080",
    publicWsBase: "ws://127.0.0.1:8080",
    scylla: {
      contactPoints: ["127.0.0.1"],
      localDataCenter: "datacenter1",
      keyspace: "nmos_registry",
      replicationFactor: 1,
    },
    changePollMs: 200,
    changeLogTtlSeconds: 3600,
    heartbeatGcIntervalSeconds: 12,
  };
}

function mockWs(sent: string[]): WebSocket {
  return {
    OPEN: 1,
    readyState: 1,
    send: (m: string) => void sent.push(m),
    on: () => {},
    close: () => {},
  } as unknown as WebSocket;
}

function mockWsWithClose(
  sent: string[],
): WebSocket & { triggerClose: () => void } {
  let closeCb: (() => void) | null = null;
  return {
    OPEN: 1,
    readyState: 1,
    send: (m: string) => void sent.push(m),
    on: (event: string, cb: () => void) => {
      if (event === "close") closeCb = cb;
    },
    close: () => {},
    triggerClose: () => {
      closeCb?.();
    },
  } as unknown as WebSocket & { triggerClose: () => void };
}

class DelayedListStore extends InMemoryRegistryStore {
  private listGate = Promise.resolve();

  blockListResources() {
    let release!: () => void;
    this.listGate = new Promise<void>((resolve) => {
      release = resolve;
    });
    return { release };
  }

  override async listResources(
    type: "nodes" | "devices" | "sources" | "flows" | "senders" | "receivers",
  ) {
    await this.listGate;
    return super.listResources(type);
  }
}

describe("SubscriptionManager", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("dispatches grain to attached WebSocket when filters match", async () => {
    const store = new InMemoryRegistryStore();
    const mgr = new SubscriptionManager(testConfig(), store);
    const sub = await mgr.createSubscription({
      max_update_rate_ms: 0,
      persist: false,
      secure: false,
      authorization: false,
      resource_path: "/nodes",
      params: {},
      queryApiPathVersion: "v1.3",
    });
    const sent: string[] = [];
    expect(await mgr.attachSocket(sub.id, mockWs(sent))).toBe(true);
    expect(sent).toHaveLength(1);
    expect(
      JSON.parse(sent[0]!) as { grain: { data: unknown[] } },
    ).toMatchObject({
      grain: { data: [] },
    });
    const nid = cassandra.types.Uuid.random();
    const ev: ChangeEventRow = {
      hour_bucket: utcHourBucket(),
      event_id: cassandra.types.TimeUuid.now(),
      resource_type: "nodes",
      resource_id: nid,
      action: "create",
      pre_json: null,
      post_json: JSON.stringify({ id: nid.toString(), label: "unit" }),
      resource_api_version: "v1.3",
    };
    mgr.dispatchEvent(ev);
    await vi.runAllTimersAsync();
    expect(sent).toHaveLength(2);
    const grain = JSON.parse(sent[1]!) as {
      flow_id: string;
      grain: { topic: string };
    };
    expect(grain.flow_id).toBe(sub.id);
    expect(grain.grain.topic).toBe("/nodes/");
  });

  it("skips dispatch when resource_path does not match event type", async () => {
    const store = new InMemoryRegistryStore();
    const mgr = new SubscriptionManager(testConfig(), store);
    const sub = await mgr.createSubscription({
      max_update_rate_ms: 0,
      persist: false,
      secure: false,
      authorization: false,
      resource_path: "/devices",
      params: {},
      queryApiPathVersion: "v1.3",
    });
    const sent: string[] = [];
    await mgr.attachSocket(sub.id, mockWs(sent));
    expect(sent).toHaveLength(1);
    expect(
      JSON.parse(sent[0]!) as { grain: { data: unknown[] } },
    ).toMatchObject({
      grain: { data: [] },
    });
    const nid = cassandra.types.Uuid.random();
    mgr.dispatchEvent({
      hour_bucket: utcHourBucket(),
      event_id: cassandra.types.TimeUuid.now(),
      resource_type: "nodes",
      resource_id: nid,
      action: "create",
      pre_json: null,
      post_json: JSON.stringify({ id: nid.toString() }),
      resource_api_version: "v1.3",
    });
    await vi.runAllTimersAsync();
    expect(sent).toHaveLength(1);
  });

  it("sends current matching resources as initial grain on connect", async () => {
    const store = new InMemoryRegistryStore();
    const mgr = new SubscriptionManager(testConfig(), store);
    const includedId = cassandra.types.Uuid.random();
    const excludedId = cassandra.types.Uuid.random();
    await store.upsertResource(
      "nodes",
      includedId,
      "v1.3",
      JSON.stringify({ id: includedId.toString(), label: "keep-me" }),
    );
    await store.upsertResource(
      "nodes",
      excludedId,
      "v1.3",
      JSON.stringify({ id: excludedId.toString(), label: "skip-me" }),
    );
    const sub = await mgr.createSubscription({
      max_update_rate_ms: 0,
      persist: false,
      secure: false,
      authorization: false,
      resource_path: "/nodes",
      params: { label: "keep-me" },
      queryApiPathVersion: "v1.3",
    });

    const sent: string[] = [];
    expect(await mgr.attachSocket(sub.id, mockWs(sent))).toBe(true);
    expect(sent).toHaveLength(1);

    const grain = JSON.parse(sent[0]!) as {
      flow_id: string;
      grain: {
        topic: string;
        data: Array<{ path: string; post?: { label?: string } }>;
      };
    };
    expect(grain.flow_id).toBe(sub.id);
    expect(grain.grain.topic).toBe("/nodes/");
    expect(grain.grain.data).toHaveLength(1);
    expect(grain.grain.data[0]?.path).toBe(includedId.toString());
    expect(grain.grain.data[0]?.post?.label).toBe("keep-me");
  });

  it("buffers matching live events while initial snapshot is in progress", async () => {
    const store = new DelayedListStore();
    const gate = store.blockListResources();
    const mgr = new SubscriptionManager(testConfig(), store);
    const sub = await mgr.createSubscription({
      max_update_rate_ms: 0,
      persist: false,
      secure: false,
      authorization: false,
      resource_path: "/nodes",
      params: {},
      queryApiPathVersion: "v1.3",
    });

    const sent: string[] = [];
    const attachPromise = mgr.attachSocket(sub.id, mockWs(sent));
    await Promise.resolve();

    const nid = cassandra.types.Uuid.random();
    mgr.dispatchEvent({
      hour_bucket: utcHourBucket(),
      event_id: cassandra.types.TimeUuid.now(),
      resource_type: "nodes",
      resource_id: nid,
      action: "create",
      pre_json: null,
      post_json: JSON.stringify({ id: nid.toString(), label: "during-sync" }),
      resource_api_version: "v1.3",
    });
    await vi.runAllTimersAsync();

    gate.release();
    expect(await attachPromise).toBe(true);

    expect(sent).toHaveLength(2);
    expect(
      JSON.parse(sent[0]!) as { grain: { data: unknown[] } },
    ).toMatchObject({
      grain: { data: [] },
    });
    const live = JSON.parse(sent[1]!) as {
      grain: { data: Array<{ path: string; post?: { label?: string } }> };
    };
    expect(live.grain.data).toHaveLength(1);
    expect(live.grain.data[0]?.path).toBe(nid.toString());
    expect(live.grain.data[0]?.post?.label).toBe("during-sync");
  });

  it("syncs persisted subscriptions across instances", async () => {
    const store = new InMemoryRegistryStore();
    const mgr1 = new SubscriptionManager(testConfig(), store);
    const mgr2 = new SubscriptionManager(testConfig(), store);

    const sub = await mgr1.createSubscription({
      max_update_rate_ms: 0,
      persist: true,
      secure: false,
      authorization: false,
      resource_path: "/nodes",
      params: {},
      queryApiPathVersion: "v1.3",
    });

    expect(mgr2.get(sub.id)).toBe(null);

    await mgr2.syncPersistedFromStore();
    expect(mgr2.get(sub.id)?.id).toBe(sub.id);

    await mgr1.delete(sub.id);
    // mgr2 hasn't synced yet, so it still has the definition in memory.
    expect(mgr2.get(sub.id)).not.toBe(null);

    await mgr2.syncPersistedFromStore();
    expect(mgr2.get(sub.id)).toBe(null);
  });

  it("syncs non-persisted subscriptions across instances", async () => {
    const store = new InMemoryRegistryStore();
    const mgr1 = new SubscriptionManager(testConfig(), store);
    const mgr2 = new SubscriptionManager(testConfig(), store);

    const sub = await mgr1.createSubscription({
      max_update_rate_ms: 0,
      persist: false,
      secure: false,
      authorization: false,
      resource_path: "/nodes",
      params: {},
      queryApiPathVersion: "v1.3",
    });

    expect(mgr2.get(sub.id)).toBe(null);

    await mgr2.syncPersistedFromStore();
    expect(mgr2.get(sub.id)?.id).toBe(sub.id);
  });

  it("attachSocket pulls persisted subscription on demand across instances", async () => {
    const store = new InMemoryRegistryStore();
    const mgr1 = new SubscriptionManager(testConfig(), store);
    const mgr2 = new SubscriptionManager(testConfig(), store);
    const sent: string[] = [];

    const sub = await mgr1.createSubscription({
      max_update_rate_ms: 0,
      persist: false,
      secure: false,
      authorization: false,
      resource_path: "/nodes",
      params: {},
      queryApiPathVersion: "v1.3",
    });

    // No explicit sync here: attachSocket should self-heal by syncing.
    const ok = await mgr2.attachSocket(sub.id, mockWs(sent));
    expect(ok).toBe(true);
    expect(mgr2.get(sub.id)?.id).toBe(sub.id);
  });

  it("deletes persist=false subscription after last local disconnect", async () => {
    const store = new InMemoryRegistryStore();
    const mgr1 = new SubscriptionManager(testConfig(), store);
    const mgr2 = new SubscriptionManager(testConfig(), store);

    const sub = await mgr1.createSubscription({
      max_update_rate_ms: 0,
      persist: false,
      secure: false,
      authorization: false,
      resource_path: "/nodes",
      params: {},
      queryApiPathVersion: "v1.3",
    });

    // Ensure the other instance sees the definition before disconnect.
    await mgr2.syncPersistedFromStore();
    expect(mgr2.get(sub.id)).not.toBe(null);

    const ws = mockWsWithClose([]);
    expect(await mgr1.attachSocket(sub.id, ws)).toBe(true);

    ws.triggerClose();
    // `attachSocket()` triggers `this.delete()` via `void`, so wait for the async delete
    // promise chain to resolve.
    await Promise.resolve();
    await Promise.resolve();

    await mgr2.syncPersistedFromStore();
    expect(mgr2.get(sub.id)).toBe(null);
  });
});
