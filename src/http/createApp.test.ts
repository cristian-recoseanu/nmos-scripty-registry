/**
 * Integration tests for the Fastify application factory.
 *
 * Tests API routing, resource registration, querying, and version handling.
 */
import { describe, expect, it } from "vitest";
import cassandra from "cassandra-driver";
import type { RegistryConfig } from "../config.js";
import { InMemoryRegistryStore } from "../store/inMemoryRegistryStore.js";
import { SubscriptionManager } from "../subscriptions/subscriptionManager.js";
import { createApp } from "./createApp.js";

/**
 * Creates a test configuration for integration tests.
 */
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

type SeededResource = {
  id: cassandra.types.Uuid;
  api_version: string;
  json: string;
  created_tai: string;
  updated_tai: string;
};

function seedNodeAtTai(
  store: InMemoryRegistryStore,
  id: cassandra.types.Uuid,
  tai: string,
  extra: Record<string, unknown> = {},
) {
  (
    store as unknown as { resources: Map<string, SeededResource> }
  ).resources.set(`nodes:${id.toString()}`, {
    id,
    api_version: "v1.3",
    json: JSON.stringify({
      id: id.toString(),
      label: `node-${tai}`,
      version: tai,
      ...extra,
    }),
    created_tai: tai,
    updated_tai: tai,
  });
}

function linkForRel(linkHeader: string | string[] | undefined, rel: string) {
  const value = Array.isArray(linkHeader) ? linkHeader.join(", ") : linkHeader;
  const match = value?.match(new RegExp(`<([^>]+)>; rel="${rel}"`));
  return match?.[1];
}

describe("createApp", () => {
  /**
   * Tests that the x-nmos browse routes return correct path segments.
   */
  it("GET /x-nmos and API branch indices return trailing-slash segments", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const root = await app.inject({ method: "GET", url: "/x-nmos" });
    expect(root.statusCode).toBe(200);
    expect(root.json()).toEqual(["registration/", "query/"]);

    const reg = await app.inject({
      method: "GET",
      url: "/x-nmos/registration",
    });
    expect(reg.statusCode).toBe(200);
    expect(reg.json()).toEqual(config.supportedApiVersions.map((v) => `${v}/`));

    const q = await app.inject({ method: "GET", url: "/x-nmos/query" });
    expect(q.statusCode).toBe(200);
    expect(q.json()).toEqual(config.supportedApiVersions.map((v) => `${v}/`));

    await app.close();
  });

  /**
   * Tests that querying an empty resource type returns an empty array.
   */
  it("GET /nodes returns an empty array when no resources exist", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });
    const res = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes",
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual([]);
    await app.close();
  });

  /**
   * Tests the full registration and query flow for a node resource.
   */
  it("registers a node via Registration API and reads it back from Query API", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nid = cassandra.types.Uuid.random().toString();
    const regBody = {
      type: "node",
      data: {
        id: nid,
        label: "integration-node",
        version: "1:0",
      },
    };

    const post = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: regBody,
    });
    expect(post.statusCode).toBe(201);

    const list = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes`,
    });
    expect(list.statusCode).toBe(200);
    const body = list.json() as Array<{ id: string; label: string }>;
    expect(
      body.some((n) => n.id === nid && n.label === "integration-node"),
    ).toBe(true);
    await app.close();
  });

  /**
   * Tests collection pagination headers and cursors at the HTTP route level.
   */
  it("returns default paged collection headers and follows the previous cursor", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    for (let i = 1; i <= 51; i += 1) {
      seedNodeAtTai(store, cassandra.types.Uuid.random(), `0:${i}`);
    }
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const first = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes",
    });
    expect(first.statusCode).toBe(200);
    expect((first.json() as unknown[]).length).toBe(50);
    expect(first.headers["x-paging-limit"]).toBe("50");
    expect(first.headers["x-paging-since"]).toBe("0:1");
    expect(first.headers["x-paging-until"]).toMatch(/^\d{10,}:\d+$/);

    const prev = linkForRel(first.headers.link, "prev");
    expect(prev).toBeDefined();
    const prevUrl = new URL(prev as string);
    expect(prevUrl.searchParams.get("paging.until")).toBe("0:1");
    expect(prevUrl.searchParams.get("paging.limit")).toBe("50");
    const older = await app.inject({
      method: "GET",
      url: `${prevUrl.pathname}${prevUrl.search}`,
    });
    expect(older.statusCode).toBe(200);
    const olderBody = older.json() as Array<{ version: string }>;
    expect(olderBody.map((node) => node.version)).toEqual(["0:1"]);
    expect(older.headers["x-paging-since"]).toBe("0:0");
    expect(older.headers["x-paging-until"]).toBe("0:1");

    await app.close();
  });

  it("uses filtered resource versions for pagination headers", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const target = "pagination-filter";
    seedNodeAtTai(store, cassandra.types.Uuid.random(), "0:1", {
      description: target,
    });
    seedNodeAtTai(store, cassandra.types.Uuid.random(), "0:2", {
      description: target,
    });
    seedNodeAtTai(store, cassandra.types.Uuid.random(), "0:3", {
      description: target,
    });
    seedNodeAtTai(store, cassandra.types.Uuid.random(), "0:99", {
      description: "other",
    });
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const res = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?paging.limit=2&description=${target}`,
    });

    expect(res.statusCode).toBe(200);
    expect((res.json() as unknown[]).length).toBe(2);
    expect(res.headers["x-paging-since"]).toBe("0:1");
    expect(res.headers["x-paging-until"]).toMatch(/^\d{10,}:\d+$/);

    await app.close();
  });

  /**
   * Tests that invalid paging limits are rejected rather than silently changed.
   */
  it("returns 400 for invalid paging.limit values", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nonNumeric = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?paging.limit=abc",
    });
    expect(nonNumeric.statusCode).toBe(400);
    expect(nonNumeric.json()).toEqual({
      code: 400,
      error: "Invalid paging.limit",
      debug: null,
    });

    const negative = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?paging.limit=-1",
    });
    expect(negative.statusCode).toBe(400);

    await app.close();
  });

  it("returns 400 when paging.since is after paging.until", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const res = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?paging.since=10:0&paging.until=9:0",
    });
    expect(res.statusCode).toBe(400);

    await app.close();
  });

  /**
   * Tests that unsupported RQL queries return 501 Not Implemented.
   */
  it("returns 501 for unsupported query.rql", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({
      config,
      store,
      subs,
      logger: false,
    });
    const res = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?query.rql=eq(label,a)",
    });
    expect(res.statusCode).toBe(501);
    await app.close();
  });

  /**
   * Tests that registering a resource at a different API version returns 409 Conflict.
   */
  it("returns 409 when stored api version differs from registration path", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const nid = cassandra.types.Uuid.random();
    await store.upsertResource(
      "nodes",
      nid,
      "v1.2",
      JSON.stringify({ id: nid.toString(), label: "old" }),
    );
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });
    const res = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: { type: "node", data: { id: nid.toString(), label: "new" } },
    });
    expect(res.statusCode).toBe(409);
    await app.close();
  });

  /**
   * Tests that registration rejects resources without a label attribute.
   */
  it("returns 400 when POST /resource payload omits data.label", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });
    const res = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "node",
        data: { id: cassandra.types.Uuid.random().toString() },
      },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json()).toEqual({
      code: 400,
      error: "Resource data.label is required",
      debug: null,
    });
    await app.close();
  });

  /**
   * Tests that child resources cannot register before their parent resources.
   */
  it("enforces parent registration ordering across resource types", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    const sourceId = cassandra.types.Uuid.random().toString();
    const flowId = cassandra.types.Uuid.random().toString();
    const senderId = cassandra.types.Uuid.random().toString();
    const receiverId = cassandra.types.Uuid.random().toString();

    const deviceBeforeNode = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "device",
        data: { id: deviceId, label: "d", node_id: nodeId },
      },
    });
    expect(deviceBeforeNode.statusCode).toBe(400);

    const receiverBeforeDevice = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "receiver",
        data: { id: receiverId, label: "r", device_id: deviceId },
      },
    });
    expect(receiverBeforeDevice.statusCode).toBe(400);

    const sourceBeforeDevice = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "source",
        data: { id: sourceId, label: "s", device_id: deviceId },
      },
    });
    expect(sourceBeforeDevice.statusCode).toBe(400);

    const flowBeforeParents = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "flow",
        data: {
          id: flowId,
          label: "f",
          source_id: sourceId,
          device_id: deviceId,
        },
      },
    });
    expect(flowBeforeParents.statusCode).toBe(400);

    const senderBeforeDevice = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "sender",
        data: { id: senderId, label: "se", device_id: deviceId, flow_id: null },
      },
    });
    expect(senderBeforeDevice.statusCode).toBe(400);

    const senderWithUnknownFlow = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "sender",
        data: {
          id: cassandra.types.Uuid.random().toString(),
          label: "se2",
          device_id: deviceId,
          flow_id: flowId,
        },
      },
    });
    expect(senderWithUnknownFlow.statusCode).toBe(400);

    const nodePost = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "node",
        data: { id: nodeId, label: "n", version: "1:0" },
      },
    });
    expect(nodePost.statusCode).toBe(201);

    const devicePost = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "device",
        data: { id: deviceId, label: "d", node_id: nodeId },
      },
    });
    expect(devicePost.statusCode).toBe(201);

    const senderWithUnknownFlowAfterDevice = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "sender",
        data: {
          id: cassandra.types.Uuid.random().toString(),
          label: "se-unknown-flow",
          device_id: deviceId,
          flow_id: cassandra.types.Uuid.random().toString(),
        },
      },
    });
    expect(senderWithUnknownFlowAfterDevice.statusCode).toBe(201);

    const sourcePost = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "source",
        data: { id: sourceId, label: "s", device_id: deviceId },
      },
    });
    expect(sourcePost.statusCode).toBe(201);

    const flowPost = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "flow",
        data: {
          id: flowId,
          label: "f",
          source_id: sourceId,
          device_id: deviceId,
        },
      },
    });
    expect(flowPost.statusCode).toBe(201);

    const receiverPost = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "receiver",
        data: {
          id: cassandra.types.Uuid.random().toString(),
          label: "r2",
          device_id: deviceId,
        },
      },
    });
    expect(receiverPost.statusCode).toBe(201);

    const senderNullFlowPost = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "sender",
        data: {
          id: cassandra.types.Uuid.random().toString(),
          label: "se3",
          device_id: deviceId,
          flow_id: null,
        },
      },
    });
    expect(senderNullFlowPost.statusCode).toBe(201);

    const senderWithFlowPost = await app.inject({
      method: "POST",
      url: "/x-nmos/registration/v1.3/resource",
      payload: {
        type: "sender",
        data: {
          id: cassandra.types.Uuid.random().toString(),
          label: "se4",
          device_id: deviceId,
          flow_id: flowId,
        },
      },
    });
    expect(senderWithFlowPost.statusCode).toBe(201);

    await app.close();
  });

  /**
   * Tests that downgrade queries correctly include older version resources.
   */
  it("excludes v1.2-stored nodes from v1.3 Query unless query.downgrade matches", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });
    const nid = cassandra.types.Uuid.random();
    await store.upsertResource(
      "nodes",
      nid,
      "v1.2",
      JSON.stringify({ id: nid.toString(), label: "legacy" }),
    );
    const noDg = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes",
    });
    expect(noDg.statusCode).toBe(200);
    expect((noDg.json() as unknown[]).length).toBe(0);

    const withDg = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?query.downgrade=v1.2",
    });
    expect(withDg.statusCode).toBe(200);
    const body = withDg.json() as Array<{ id: string }>;
    expect(body.some((n) => n.id === nid.toString())).toBe(true);
    await app.close();
  });

  /**
   * Tests that cross-major downgrade requests return 400 Bad Request.
   */
  it("returns 400 for cross-major query.downgrade", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });
    const res = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?query.downgrade=v2.0",
    });
    expect(res.statusCode).toBe(400);
    await app.close();
  });

  /**
   * Tests that unknown Query API paths return NMOS error schema.
   */
  it("returns NMOS error schema for unknown Query API endpoint", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });
    const res = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/2b29c972-db3e-4369-a75b-00dd017e3ef7",
    });
    expect(res.statusCode).toBe(404);
    expect(res.json()).toEqual({
      code: 404,
      error: "Not found",
      debug: null,
    });
    await app.close();
  });

  /**
   * Tests that unknown Registration API paths return NMOS error schema.
   */
  it("returns NMOS error schema for unknown Registration API endpoint", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });
    const res = await app.inject({
      method: "GET",
      url: "/x-nmos/registration/v1.3/ec116a3b-8ef4-45e2-8e49-9a59201028fa",
    });
    expect(res.statusCode).toBe(404);
    expect(res.json()).toEqual({
      code: 404,
      error: "Not found",
      debug: null,
    });
    await app.close();
  });

  /**
   * Tests that JSON responses do not include a charset parameter.
   */
  it("returns application/json without charset for JSON payloads", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const res = await app.inject({ method: "GET", url: "/x-nmos" });
    expect(res.statusCode).toBe(200);
    expect(res.headers["content-type"]).toBe("application/json");

    await app.close();
  });

  /**
   * Tests that deleting a node also deletes devices and device child resources.
   */
  it("cascades delete from node to devices and device children", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    const sourceId = cassandra.types.Uuid.random().toString();
    const flowId = cassandra.types.Uuid.random().toString();
    const senderId = cassandra.types.Uuid.random().toString();
    const receiverId = cassandra.types.Uuid.random().toString();

    const register = async (payload: {
      type: string;
      data: Record<string, unknown>;
    }) =>
      app.inject({
        method: "POST",
        url: "/x-nmos/registration/v1.3/resource",
        payload,
      });

    expect(
      (
        await register({
          type: "node",
          data: { id: nodeId, label: "n", version: "1:0" },
        })
      ).statusCode,
    ).toBe(201);
    expect(
      (
        await register({
          type: "device",
          data: { id: deviceId, label: "d", node_id: nodeId },
        })
      ).statusCode,
    ).toBe(201);
    expect(
      (
        await register({
          type: "source",
          data: { id: sourceId, label: "s", device_id: deviceId },
        })
      ).statusCode,
    ).toBe(201);
    expect(
      (
        await register({
          type: "flow",
          data: {
            id: flowId,
            label: "f",
            source_id: sourceId,
            device_id: deviceId,
          },
        })
      ).statusCode,
    ).toBe(201);
    expect(
      (
        await register({
          type: "sender",
          data: {
            id: senderId,
            label: "se",
            device_id: deviceId,
            flow_id: flowId,
          },
        })
      ).statusCode,
    ).toBe(201);
    expect(
      (
        await register({
          type: "receiver",
          data: { id: receiverId, label: "r", device_id: deviceId },
        })
      ).statusCode,
    ).toBe(201);

    const del = await app.inject({
      method: "DELETE",
      url: `/x-nmos/registration/v1.3/resource/nodes/${nodeId}`,
    });
    expect(del.statusCode).toBe(204);

    const [devices, sources, flows, senders, receivers] = await Promise.all([
      app.inject({ method: "GET", url: "/x-nmos/query/v1.3/devices" }),
      app.inject({ method: "GET", url: "/x-nmos/query/v1.3/sources" }),
      app.inject({ method: "GET", url: "/x-nmos/query/v1.3/flows" }),
      app.inject({ method: "GET", url: "/x-nmos/query/v1.3/senders" }),
      app.inject({ method: "GET", url: "/x-nmos/query/v1.3/receivers" }),
    ]);

    expect(devices.json()).toEqual([]);
    expect(sources.json()).toEqual([]);
    expect(flows.json()).toEqual([]);
    expect(senders.json()).toEqual([]);
    expect(receivers.json()).toEqual([]);

    await app.close();
  });

  /**
   * Tests that parallel parent/child registrations do not fail due to race conditions.
   */
  it("allows concurrent node and device registration without child false-negative", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();

    const [nodeRes, deviceRes] = await Promise.all([
      app.inject({
        method: "POST",
        url: "/x-nmos/registration/v1.3/resource",
        payload: {
          type: "node",
          data: { id: nodeId, label: "parallel-node", version: "1:0" },
        },
      }),
      app.inject({
        method: "POST",
        url: "/x-nmos/registration/v1.3/resource",
        payload: {
          type: "device",
          data: { id: deviceId, label: "parallel-device", node_id: nodeId },
        },
      }),
    ]);

    expect([200, 201]).toContain(nodeRes.statusCode);
    expect([200, 201]).toContain(deviceRes.statusCode);

    const devices = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/devices",
    });
    const body = devices.json() as Array<{ id: string; node_id?: string }>;
    expect(body.some((d) => d.id === deviceId)).toBe(true);

    await app.close();
  });
});
