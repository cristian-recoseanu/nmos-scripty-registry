/**
 * IS-04-02 Integration Tests
 *
 * Reference: https://github.com/AMWA-TV/nmos-testing/blob/master/nmostesting/suites/IS0402Test.py
 *
 */
import { describe, expect, it, vi } from "vitest";
import cassandra from "cassandra-driver";
import type { WebSocket } from "ws";
import type { RegistryConfig } from "../config.js";
import { InMemoryRegistryStore } from "../store/inMemoryRegistryStore.js";
import { SubscriptionManager } from "../subscriptions/subscriptionManager.js";
import { utcHourBucket } from "../store/timeBuckets.js";
import type { ChangeEventRow } from "../store/registryPort.js";
import { createApp } from "./createApp.js";
import { HeartbeatCleanupService } from "../store/heartbeatCleanupService.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
    persistedSubscriptionTtlSeconds: 86400,
    heartbeatGcIntervalSeconds: 12,
  };
}

/** Parse the Link header value and return a map of rel → url */
function parseLinkHeader(
  header: string | string[] | undefined,
): Record<string, string> {
  const value = Array.isArray(header) ? header.join(", ") : (header ?? "");
  const result: Record<string, string> = {};
  for (const part of value.split(",")) {
    const m = part.match(/<([^>]+)>;\s*rel="([^"]+)"/);
    if (m) result[m[2]] = m[1];
  }
  return result;
}

/** Register a node via the Registration API and return the response */
async function registerNode(
  app: Awaited<ReturnType<typeof createApp>>,
  data: Record<string, unknown>,
  version = "v1.3",
) {
  return app.inject({
    method: "POST",
    url: `/x-nmos/registration/${version}/resource`,
    payload: { type: "node", data },
  });
}

/** Register any resource type via the Registration API */
async function registerResource(
  app: Awaited<ReturnType<typeof createApp>>,
  type: string,
  data: Record<string, unknown>,
  version = "v1.3",
) {
  return app.inject({
    method: "POST",
    url: `/x-nmos/registration/${version}/resource`,
    payload: { type, data },
  });
}

/** Minimal valid node payload */
function makeNode(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    id: cassandra.types.Uuid.random().toString(),
    label: "test-node",
    version: "1:0",
    ...overrides,
  };
}

/** Post a node and assert the expected HTTP status */
async function postNode(
  app: Awaited<ReturnType<typeof createApp>>,
  overrides: Record<string, unknown> = {},
  version = "v1.3",
) {
  const data = makeNode(overrides);
  const res = await registerNode(app, data, version);
  return { res, data };
}

/**
 * Register a full chain: node → device → source → flow → sender → receiver.
 * Returns an object of { node, device, source, flow, sender, receiver } payloads.
 */
async function registerFullChain(
  app: Awaited<ReturnType<typeof createApp>>,
  description = "chain",
  version = "v1.3",
) {
  const nodeId = cassandra.types.Uuid.random().toString();
  const deviceId = cassandra.types.Uuid.random().toString();
  const sourceId = cassandra.types.Uuid.random().toString();
  const flowId = cassandra.types.Uuid.random().toString();
  const senderId = cassandra.types.Uuid.random().toString();
  const receiverId = cassandra.types.Uuid.random().toString();

  const node = { id: nodeId, label: "n", version: "1:0", description };
  const device = { id: deviceId, label: "d", node_id: nodeId, description };
  const source = { id: sourceId, label: "s", device_id: deviceId, description };
  const flow = {
    id: flowId,
    label: "f",
    source_id: sourceId,
    device_id: deviceId,
    description,
  };
  const sender = {
    id: senderId,
    label: "se",
    device_id: deviceId,
    flow_id: null,
    description,
  };
  const receiver = {
    id: receiverId,
    label: "r",
    device_id: deviceId,
    description,
  };

  await registerResource(app, "node", node, version);
  await registerResource(app, "device", device, version);
  await registerResource(app, "source", source, version);
  await registerResource(app, "flow", flow, version);
  await registerResource(app, "sender", sender, version);
  await registerResource(app, "receiver", receiver, version);

  return { node, device, source, flow, sender, receiver };
}

/** Post N nodes with incremental TAI-like timestamps and return [timestamps, ids] */
async function postSampleNodes(
  app: Awaited<ReturnType<typeof createApp>>,
  count: number,
  description: string,
  labelFn?: (i: number) => string,
): Promise<[string[], string[]]> {
  const ids: string[] = [];
  const timestamps: string[] = [];

  for (let i = 0; i < count; i++) {
    const id = cassandra.types.Uuid.random().toString();
    const label = labelFn ? labelFn(i) : "sample";
    const data = makeNode({ id, label, description, version: `0:${i + 1}` });
    const res = await registerNode(app, data);
    expect(res.statusCode).toBe(201);
    ids.push(id);
    // Use the version field as the TAI timestamp for these tests
    timestamps.push(`0:${i + 1}`);
  }

  return [timestamps, ids];
}

const PAGING_HEADERS = [
  "Link",
  "X-Paging-Limit",
  "X-Paging-Since",
  "X-Paging-Until",
];

function assertPagingHeadersPresent(headers: Record<string, unknown>) {
  for (const h of PAGING_HEADERS) {
    expect(
      headers[h.toLowerCase()],
      `Expected paging header ${h}`,
    ).toBeDefined();
  }
}

// ---------------------------------------------------------------------------
// Mock WebSocket helper (mirrors subscriptionManager.test.ts)
// ---------------------------------------------------------------------------

function mockWs(sent: string[]): WebSocket {
  return {
    OPEN: 1,
    readyState: 1,
    send: (m: string) => void sent.push(m),
    on: () => {},
    close: () => {},
  } as unknown as WebSocket;
}

function makeChangeEvent(
  type: ChangeEventRow["resource_type"],
  id: cassandra.types.Uuid,
  apiVersion: string,
  preJson: string | null,
  postJson: string | null,
): ChangeEventRow {
  return {
    hour_bucket: utcHourBucket(),
    event_id: cassandra.types.TimeUuid.now(),
    resource_type: type,
    resource_id: id,
    action:
      postJson === null ? "delete" : preJson === null ? "create" : "update",
    pre_json: preJson,
    post_json: postJson,
    resource_api_version: apiVersion,
  };
}

// ---------------------------------------------------------------------------
// IS-04-02 Test suite
// ---------------------------------------------------------------------------

describe("IS-04-02", () => {
  // -------------------------------------------------------------------------
  // test_03 – Registration API accepts a valid Node resource
  // -------------------------------------------------------------------------

  it("test_03: Registration API accepts and stores a valid Node resource", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const { res, data } = await postNode(app, { description: "test_03" });
    expect(res.statusCode).toBe(201);

    // Verify it is visible via Query API
    const get = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes/${data.id as string}`,
    });
    expect(get.statusCode).toBe(200);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_03_1 – Registration API responds with correct Location header
  // -------------------------------------------------------------------------

  it("test_03_1: Registration API responds with correct Location header", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const { res, data } = await postNode(app, { description: "test_03_1" });
    expect(res.statusCode).toBe(201);

    const location = res.headers["location"] as string;
    expect(location).toBeDefined();
    expect(location).toMatch(
      /\/x-nmos\/registration\/v1\.3\/resource\/nodes\//,
    );
    expect(location).toContain(data.id as string);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_03_2 – Registration API accepts unicode in label/description
  // -------------------------------------------------------------------------

  it("test_03_2: Registration API accepts and stores a valid Node resource with unicode characters", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const { res } = await postNode(app, {
      label: "test_03_2 😁 😂 😃",
      description: "test_03_2 unicode",
    });
    expect(res.statusCode).toBe(201);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_04 – Registration API rejects an invalid Node (missing label)
  // -------------------------------------------------------------------------

  it("test_04: Registration API rejects an invalid Node resource with 400", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const id = cassandra.types.Uuid.random().toString();
    const res = await registerNode(app, { id, version: "1:0" /* no label */ });
    expect(res.statusCode).toBe(400);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_05 – Registration API accepts a valid Device resource
  // -------------------------------------------------------------------------

  it("test_05: Registration API accepts and stores a valid Device resource", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));

    const deviceId = cassandra.types.Uuid.random().toString();
    const res = await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
      description: "test_05",
    });
    expect(res.statusCode).toBe(201);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_06 – Registration API rejects an invalid Device (missing label)
  // -------------------------------------------------------------------------

  it("test_06: Registration API rejects an invalid Device resource with 400", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));

    const res = await registerResource(app, "device", {
      id: cassandra.types.Uuid.random().toString(),
      node_id: nodeId,
      // no label
    });
    expect(res.statusCode).toBe(400);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_07 – Registration API accepts a valid Source resource
  // -------------------------------------------------------------------------

  it("test_07: Registration API accepts and stores a valid Source resource", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });

    const res = await registerResource(app, "source", {
      id: cassandra.types.Uuid.random().toString(),
      label: "s",
      device_id: deviceId,
      description: "test_07",
    });
    expect(res.statusCode).toBe(201);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_08 – Registration API rejects an invalid Source (missing label)
  // -------------------------------------------------------------------------

  it("test_08: Registration API rejects an invalid Source resource with 400", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });

    const res = await registerResource(app, "source", {
      id: cassandra.types.Uuid.random().toString(),
      device_id: deviceId,
      // no label
    });
    expect(res.statusCode).toBe(400);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_09 – Registration API accepts a valid Flow resource
  // -------------------------------------------------------------------------

  it("test_09: Registration API accepts and stores a valid Flow resource", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    const sourceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });
    await registerResource(app, "source", {
      id: sourceId,
      label: "s",
      device_id: deviceId,
    });

    const res = await registerResource(app, "flow", {
      id: cassandra.types.Uuid.random().toString(),
      label: "f",
      source_id: sourceId,
      device_id: deviceId,
      description: "test_09",
    });
    expect(res.statusCode).toBe(201);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_10 – Registration API rejects an invalid Flow (missing label)
  // -------------------------------------------------------------------------

  it("test_10: Registration API rejects an invalid Flow resource with 400", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    const sourceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });
    await registerResource(app, "source", {
      id: sourceId,
      label: "s",
      device_id: deviceId,
    });

    const res = await registerResource(app, "flow", {
      id: cassandra.types.Uuid.random().toString(),
      source_id: sourceId,
      device_id: deviceId,
      // no label
    });
    expect(res.statusCode).toBe(400);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_11 – Registration API accepts a valid Sender resource
  // -------------------------------------------------------------------------

  it("test_11: Registration API accepts and stores a valid Sender resource", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });

    const res = await registerResource(app, "sender", {
      id: cassandra.types.Uuid.random().toString(),
      label: "se",
      device_id: deviceId,
      flow_id: null,
      description: "test_11",
    });
    expect(res.statusCode).toBe(201);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_11_1 – Registration API accepts a Sender with null flow_id (v1.1+)
  // -------------------------------------------------------------------------

  it("test_11_1: Registration API accepts a valid Sender with null flow_id", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });

    const res = await registerResource(app, "sender", {
      id: cassandra.types.Uuid.random().toString(),
      label: "se-null-flow",
      device_id: deviceId,
      flow_id: null,
      description: "test_11_1",
    });
    expect(res.statusCode).toBe(201);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_12 – Registration API rejects an invalid Sender (missing label)
  // -------------------------------------------------------------------------

  it("test_12: Registration API rejects an invalid Sender resource with 400", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });

    const res = await registerResource(app, "sender", {
      id: cassandra.types.Uuid.random().toString(),
      device_id: deviceId,
      // no label
    });
    expect(res.statusCode).toBe(400);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_13 – Registration API accepts a valid Receiver resource
  // -------------------------------------------------------------------------

  it("test_13: Registration API accepts and stores a valid Receiver resource", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });

    const res = await registerResource(app, "receiver", {
      id: cassandra.types.Uuid.random().toString(),
      label: "r",
      device_id: deviceId,
      description: "test_13",
    });
    expect(res.statusCode).toBe(201);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_14 – Registration API rejects an invalid Receiver (missing label)
  // -------------------------------------------------------------------------

  it("test_14: Registration API rejects an invalid Receiver resource with 400", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });

    const res = await registerResource(app, "receiver", {
      id: cassandra.types.Uuid.random().toString(),
      device_id: deviceId,
      // no label
    });
    expect(res.statusCode).toBe(400);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_15 – Registration API responds with 200 on updating a registered Node
  // -------------------------------------------------------------------------

  it("test_15: Registration API responds with 200 on updating a registered Node", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const { res: r1, data } = await postNode(app, { description: "test_15" });
    expect(r1.statusCode).toBe(201);

    // Re-register the same resource → 200
    const update = await registerNode(app, { ...data, version: "1:1" });
    expect(update.statusCode).toBe(200);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_16 – Registration API responds with 200 on updating a registered Device
  // -------------------------------------------------------------------------

  it("test_16: Registration API responds with 200 on updating a registered Device", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));

    const deviceData = {
      id: deviceId,
      label: "d",
      node_id: nodeId,
      description: "test_16",
    };
    const r1 = await registerResource(app, "device", deviceData);
    expect(r1.statusCode).toBe(201);

    const r2 = await registerResource(app, "device", deviceData);
    expect(r2.statusCode).toBe(200);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_17 – Registration API responds with 200 on updating a registered Source
  // -------------------------------------------------------------------------

  it("test_17: Registration API responds with 200 on updating a registered Source", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    const sourceId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });

    const sourceData = {
      id: sourceId,
      label: "s",
      device_id: deviceId,
      description: "test_17",
    };
    const r1 = await registerResource(app, "source", sourceData);
    expect(r1.statusCode).toBe(201);

    const r2 = await registerResource(app, "source", sourceData);
    expect(r2.statusCode).toBe(200);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_18 – Registration API responds with 200 on updating a registered Flow
  // -------------------------------------------------------------------------

  it("test_18: Registration API responds with 200 on updating a registered Flow", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    const sourceId = cassandra.types.Uuid.random().toString();
    const flowId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });
    await registerResource(app, "source", {
      id: sourceId,
      label: "s",
      device_id: deviceId,
    });

    const flowData = {
      id: flowId,
      label: "f",
      source_id: sourceId,
      device_id: deviceId,
      description: "test_18",
    };
    const r1 = await registerResource(app, "flow", flowData);
    expect(r1.statusCode).toBe(201);

    const r2 = await registerResource(app, "flow", flowData);
    expect(r2.statusCode).toBe(200);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_19 – Registration API responds with 200 on updating a registered Sender
  // -------------------------------------------------------------------------

  it("test_19: Registration API responds with 200 on updating a registered Sender", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    const senderId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });

    const senderData = {
      id: senderId,
      label: "se",
      device_id: deviceId,
      flow_id: null,
      description: "test_19",
    };
    const r1 = await registerResource(app, "sender", senderData);
    expect(r1.statusCode).toBe(201);

    const r2 = await registerResource(app, "sender", senderData);
    expect(r2.statusCode).toBe(200);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_20 – Registration API responds with 200 on updating a registered Receiver
  // -------------------------------------------------------------------------

  it("test_20: Registration API responds with 200 on updating a registered Receiver", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    const deviceId = cassandra.types.Uuid.random().toString();
    const receiverId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));
    await registerResource(app, "device", {
      id: deviceId,
      label: "d",
      node_id: nodeId,
    });

    const receiverData = {
      id: receiverId,
      label: "r",
      device_id: deviceId,
      description: "test_20",
    };
    const r1 = await registerResource(app, "receiver", receiverData);
    expect(r1.statusCode).toBe(201);

    const r2 = await registerResource(app, "receiver", receiverData);
    expect(r2.statusCode).toBe(200);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_21_1 – Query API implements pagination (no query or paging parameters)
  // -------------------------------------------------------------------------

  it("test_21_1: Query API implements pagination (no query or paging parameters)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    await postNode(app);

    const res = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes",
    });
    expect(res.statusCode).toBe(200);
    assertPagingHeadersPresent(res.headers);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_21_1_1 – Query API implements pagination (explicitly requested via paging.limit)
  // -------------------------------------------------------------------------

  it("test_21_1_1: Query API implements pagination (when explicitly requested)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    await postNode(app);

    const res = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?paging.limit=10",
    });
    expect(res.statusCode).toBe(200);
    assertPagingHeadersPresent(res.headers);
    expect(res.headers["x-paging-limit"]).toBe("10");

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_21_2 – Query API implements pagination (documentation examples)
  // Mirrors the 5 examples from the NMOS IS-04 pagination spec.
  // Nodes are stored with version = "0:<i+1>" so that field doubles as the
  // updated_tai cursor used by the store's sort.
  // -------------------------------------------------------------------------

  it("test_21_2: Query API implements pagination (documentation examples)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const description = "test_21_2";
    const [ts, ids] = await postSampleNodes(app, 20, description);

    // Align with spec examples: 1-indexed. ts[0] = ts for node 1, etc.
    // ts is 0-indexed here: ts[i] = "0:<i+1>"

    // The store uses wall-clock TAI for updated_tai. We read the actual paging cursors
    // from response headers and use them to drive subsequent requests, exactly as the
    // IS-04-02 spec examples describe (prev/next cursor chaining).

    // Example 1: Initial /nodes request with limit=10 → last 10 nodes (ids[10..19])
    const ex1 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.limit=10`,
    });
    expect(ex1.statusCode).toBe(200);
    assertPagingHeadersPresent(ex1.headers);
    const ex1Body = ex1.json() as Array<{ id: string }>;
    expect(ex1Body.length).toBe(10);
    // Response is reverse-chronological; last registered is first
    expect(ex1Body[0].id).toBe(ids[19]);
    expect(ex1Body[9].id).toBe(ids[10]);
    const ex1Since = ex1.headers["x-paging-since"] as string;
    const ex1Until = ex1.headers["x-paging-until"] as string;

    // Example 2: limit=5 → last 5 nodes (ids[15..19])
    const ex2 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.limit=5`,
    });
    expect(ex2.statusCode).toBe(200);
    const ex2Body = ex2.json() as Array<{ id: string }>;
    expect(ex2Body.length).toBe(5);
    expect(ex2Body[0].id).toBe(ids[19]);
    expect(ex2Body[4].id).toBe(ids[15]);
    const ex2Since = ex2.headers["x-paging-since"] as string;

    // Example 3: since = X-Paging-Since from Example 1 → the 10 nodes before those
    // i.e. nodes that were registered before the ex1 window: ids[0..9]
    const ex3 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.since=${ex1Since}&paging.until=${ex1Until}&paging.limit=10`,
    });
    expect(ex3.statusCode).toBe(200);
    const ex3Body = ex3.json() as Array<{ id: string }>;
    // Within [ex1Since, ex1Until] there should be exactly the ex1 set → 10 items
    expect(ex3Body.length).toBe(10);
    expect(ex3Body.map((n) => n.id)).toEqual(ex1Body.map((n) => n.id));

    // Example 4: 'prev' cursor from Example 1 — until=ex1Since → ids[0..9]
    const ex4 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.until=${ex1Since}&paging.limit=10`,
    });
    expect(ex4.statusCode).toBe(200);
    const ex4Body = ex4.json() as Array<{ id: string }>;
    expect(ex4Body.length).toBe(10);
    expect(ex4Body[0].id).toBe(ids[9]);
    expect(ex4Body[9].id).toBe(ids[0]);

    // Example 5: 'prev' of Example 2 — until=ex2Since → ids[0..14]
    const ex5 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.until=${ex2Since}&paging.limit=15`,
    });
    expect(ex5.statusCode).toBe(200);
    const ex5Body = ex5.json() as Array<{ id: string }>;
    expect(ex5Body.length).toBe(15);
    expect(ex5Body[0].id).toBe(ids[14]);
    expect(ex5Body[14].id).toBe(ids[0]);

    void ts;

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_21_3 – Query API pagination edge cases
  // -------------------------------------------------------------------------

  it("test_21_3: Query API implements pagination (edge cases)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const description = "test_21_3";
    const [ts, ids] = await postSampleNodes(app, 20, description);

    // since after newest resource → empty response, X-Paging-Since == requested since
    const afterLast = "9999999999:0";
    const res1 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.since=${afterLast}`,
    });
    expect(res1.statusCode).toBe(200);
    expect((res1.json() as unknown[]).length).toBe(0);
    expect(res1.headers["x-paging-since"]).toBe(afterLast);

    // until before oldest resource → empty response, X-Paging-Until == requested until
    const beforeFirst = "0:0";
    const res2 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.until=${beforeFirst}`,
    });
    expect(res2.statusCode).toBe(200);
    expect((res2.json() as unknown[]).length).toBe(0);
    expect(res2.headers["x-paging-until"]).toBe(beforeFirst);

    // Query by id → exactly one result
    const res3 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?id=${ids[12]}`,
    });
    expect(res3.statusCode).toBe(200);
    expect((res3.json() as unknown[]).length).toBe(1);
    assertPagingHeadersPresent(res3.headers);
    expect(res3.headers["x-paging-since"]).toBe("0:0");

    // Query by non-existent id → empty result
    const res4 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?id=${cassandra.types.Uuid.random().toString()}`,
    });
    expect(res4.statusCode).toBe(200);
    expect((res4.json() as unknown[]).length).toBe(0);
    assertPagingHeadersPresent(res4.headers);
    expect(res4.headers["x-paging-since"]).toBe("0:0");

    // Suppress unused variable warning
    void ts;

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_21_4 – Query API pagination: requests that require empty responses
  // -------------------------------------------------------------------------

  it("test_21_4: Query API implements pagination (requests that require empty responses)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const description = "test_21_4";
    const [ts] = await postSampleNodes(app, 20, description);

    // Use the 13th node's timestamp (0-indexed: ts[12])
    const pivot = ts[12]; // "0:13"

    // paging.since == paging.until → empty
    const res1 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.since=${pivot}&paging.until=${pivot}&paging.limit=10`,
    });
    expect(res1.statusCode).toBe(200);
    expect((res1.json() as unknown[]).length).toBe(0);
    expect(res1.headers["x-paging-since"]).toBe(pivot);
    expect(res1.headers["x-paging-until"]).toBe(pivot);
    expect(res1.headers["x-paging-limit"]).toBe("10");

    // paging.limit == 0 with paging.since → empty
    const res2 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.since=${pivot}&paging.limit=0`,
    });
    expect(res2.statusCode).toBe(200);
    expect((res2.json() as unknown[]).length).toBe(0);
    expect(res2.headers["x-paging-limit"]).toBe("0");

    // paging.limit == 0 with paging.until → empty
    const res3 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.until=${pivot}&paging.limit=0`,
    });
    expect(res3.statusCode).toBe(200);
    expect((res3.json() as unknown[]).length).toBe(0);
    expect(res3.headers["x-paging-limit"]).toBe("0");

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_21_5 – Query API pagination: filters that select discontiguous resources
  // -------------------------------------------------------------------------

  it("test_21_5: Query API implements pagination (filters that select discontiguous resources)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    // foo: indices where (index+1) % 5 < 3  → 0,1, 5,6, 10,11, 15,16
    // bar: the rest                          → 2,3,4, 7,8,9, 12,13,14, 17,18,19
    const foo = (i: number) => (i + 1) % 5 < 3;
    const [ts, ids] = await postSampleNodes(app, 20, "test_21_5", (i) =>
      foo(i) ? "foo" : "bar",
    );

    const fooIds = ids.filter((_, i) => foo(i));
    const barIds = ids.filter((_, i) => !foo(i));

    // Query 1: "foo", default paging, limit=10 → last 10 foo items
    const q1 = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?label=foo&paging.limit=10",
    });
    expect(q1.statusCode).toBe(200);
    const q1Body = (q1.json() as Array<{ id: string }>).map((n) => n.id);
    expect(q1Body.length).toBe(10);
    // Last 10 foo ids in reverse order
    expect(q1Body).toEqual([...fooIds].slice(-10).reverse());

    // Query 2: 'prev' of Query 1 — until=ts of the first item in q1 → remaining foo items
    const q1SinceHeader = q1.headers["x-paging-since"] as string;
    const q2 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?label=foo&paging.until=${q1SinceHeader}&paging.limit=10`,
    });
    expect(q2.statusCode).toBe(200);
    const q2Body = (q2.json() as Array<{ id: string }>).map((n) => n.id);
    // The remaining foo items before the q1 window
    const remainingFoo = fooIds.slice(0, fooIds.length - 10);
    expect(q2Body.length).toBe(remainingFoo.length);

    // Query 3: 'next' of Query 1 — since=ts of the last item returned → empty (no newer foo)
    const q1UntilHeader = q1.headers["x-paging-until"] as string;
    const q3 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?label=foo&paging.since=${q1UntilHeader}&paging.limit=10`,
    });
    expect(q3.statusCode).toBe(200);
    expect((q3.json() as unknown[]).length).toBe(0);

    // Query 4: "bar", default paging limit=10.
    // bar indices: 2,3,7,8,12,13,17,18 → 8 items total (all fit within limit=10)
    const q4 = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?label=bar&paging.limit=10",
    });
    expect(q4.statusCode).toBe(200);
    const q4Body = (q4.json() as Array<{ id: string }>).map((n) => n.id);
    expect(q4Body.length).toBe(barIds.length);
    expect(q4Body).toEqual([...barIds].reverse());

    // Query 5: "bar", limited to 3 → last 3 bar items
    const q5 = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?label=bar&paging.limit=3",
    });
    expect(q5.statusCode).toBe(200);
    const q5Body = (q5.json() as Array<{ id: string }>).map((n) => n.id);
    expect(q5Body.length).toBe(3);
    expect(q5Body).toEqual([...barIds].slice(-3).reverse());

    // Suppress unused variable warning
    void ts;

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_21_6 – Query API pagination: bad request (since > until)
  // -------------------------------------------------------------------------

  it("test_21_6: Query API implements pagination (bad request: since after until)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const res = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?paging.since=10:0&paging.until=5:0",
    });
    expect(res.statusCode).toBe(400);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_21_7 – Query API pagination: updates between paged requests
  // -------------------------------------------------------------------------

  it("test_21_7: Query API implements pagination (updates between paged requests)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const description = "test_21_7";
    const count = 3;
    const [ts, ids] = await postSampleNodes(app, count, description);
    // Use the actual X-Paging-Until from the initial response as the stable cursor.
    // This mirrors the spec: the client uses response headers to navigate pages.

    // Initial paged request
    const initial = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.limit=${count}`,
    });
    expect(initial.statusCode).toBe(200);
    const initialBody = initial.json() as Array<{ id: string }>;
    expect(initialBody.map((n) => n.id)).toEqual([...ids].reverse());

    // Capture the 'until' cursor from the initial response
    const snapshotUntil = initial.headers["x-paging-until"] as string;

    // 'next' page: since=snapshotUntil → should be empty (no newer resources)
    const next1 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.limit=${count}&paging.since=${snapshotUntil}`,
    });
    expect(next1.statusCode).toBe(200);
    expect((next1.json() as unknown[]).length).toBe(0);

    // 'current' page: until=snapshotUntil → same as initial response
    const current1 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.limit=${count}&paging.until=${snapshotUntil}`,
    });
    expect(current1.statusCode).toBe(200);
    expect((current1.json() as Array<{ id: string }>).map((n) => n.id)).toEqual(
      [...ids].reverse(),
    );

    // Update the middle node (ids[1]) — re-register it to get a new timestamp
    const updatedNode = makeNode({ id: ids[1], description, version: "0:10" });
    const updateRes = await registerNode(app, updatedNode);
    expect(updateRes.statusCode).toBe(200);

    // 'next' page since snapshotUntil should now contain the updated node
    const next2 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.limit=${count}&paging.since=${snapshotUntil}`,
    });
    expect(next2.statusCode).toBe(200);
    const next2Body = next2.json() as Array<{ id: string }>;
    expect(next2Body.length).toBe(1);
    expect(next2Body[0].id).toBe(ids[1]);

    // 'current' page (until=snapshotUntil) should no longer contain ids[1]
    const current2 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.limit=${count}&paging.until=${snapshotUntil}`,
    });
    expect(current2.statusCode).toBe(200);
    const current2Body = (current2.json() as Array<{ id: string }>).map(
      (n) => n.id,
    );
    expect(current2Body).not.toContain(ids[1]);
    expect(current2Body).toContain(ids[0]);
    expect(current2Body).toContain(ids[2]);

    // Update the other two nodes
    await registerNode(
      app,
      makeNode({ id: ids[2], description, version: "0:11" }),
    );
    await registerNode(
      app,
      makeNode({ id: ids[0], description, version: "0:12" }),
    );

    // 'current' page (until=snapshotUntil) should now be empty
    const current3 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.limit=${count}&paging.until=${snapshotUntil}`,
    });
    expect(current3.statusCode).toBe(200);
    expect((current3.json() as unknown[]).length).toBe(0);

    // 'next' page should now contain all 3 nodes in update order
    const next3 = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${description}&paging.limit=${count}&paging.since=${snapshotUntil}`,
    });
    expect(next3.statusCode).toBe(200);
    const next3Body = (next3.json() as Array<{ id: string }>).map((n) => n.id);
    // All 3 nodes should be visible in their update order (reverse-chron: ids[0] first, then ids[2], then ids[1])
    expect(next3Body.length).toBe(3);
    expect(next3Body).toContain(ids[0]);
    expect(next3Body).toContain(ids[1]);
    expect(next3Body).toContain(ids[2]);
    // ids[1] was updated first, so it should be last in reverse-chron order
    expect(next3Body[next3Body.length - 1]).toBe(ids[1]);

    void ts;

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_21_8 – Query API pagination: Link header encoding
  // -------------------------------------------------------------------------

  it("test_21_8: Query API implements pagination (correct encoding of URLs in Link header)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    // A label with '&' should be encoded in the Link header
    const label = "foo%26bar";
    const res = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?label=${label}&paging.limit=10`,
    });
    expect(res.statusCode).toBe(200);
    assertPagingHeadersPresent(res.headers);

    const link = parseLinkHeader(res.headers.link as string);
    // Each link URL should preserve the label parameter
    for (const rel of ["prev", "next"]) {
      if (link[rel]) {
        // The label parameter should appear in some form in the link
        expect(link[rel]).toContain("label=");
      }
    }

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_21_9 – Query API pagination: correct protocol in Link header
  // -------------------------------------------------------------------------

  it("test_21_9: Query API implements pagination (correct protocol in Link header)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const res = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes",
    });
    expect(res.statusCode).toBe(200);
    assertPagingHeadersPresent(res.headers);

    const link = parseLinkHeader(res.headers.link as string);
    for (const rel of Object.keys(link)) {
      // All Link URLs must use http:// (matching publicHttpBase in testConfig)
      expect(link[rel]).toMatch(/^http:\/\//);
    }

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_22 – Query API implements downgrade queries
  // -------------------------------------------------------------------------

  it("test_22: Query API implements downgrade queries", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nid = cassandra.types.Uuid.random();
    // Store a v1.2 node directly
    await store.upsertResource(
      "nodes",
      nid,
      "v1.2",
      JSON.stringify({
        id: nid.toString(),
        label: "legacy-22",
        version: "1:0",
      }),
    );

    // Without downgrade: v1.3 query should NOT return the v1.2 node
    const noDg = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes/${nid.toString()}`,
    });
    expect(noDg.statusCode).toBe(404);

    // With downgrade=v1.2: v1.3 query SHOULD return the v1.2 node
    const withDg = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes/${nid.toString()}?query.downgrade=v1.2`,
    });
    expect(withDg.statusCode).toBe(200);
    expect((withDg.json() as { id: string }).id).toBe(nid.toString());

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_22_1 – Query API subscriptions resource does not support downgrade queries
  // Verifies GET /subscriptions?query.downgrade=<prev> does NOT return subscriptions
  // created at a lower API version.
  // -------------------------------------------------------------------------

  it("test_22_1: Query API subscriptions resource does not support downgrade queries", async () => {
    const config = testConfig(); // supports v1.2 and v1.3
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    // Create a subscription at v1.3 (the version under test)
    const validSubRes = await app.inject({
      method: "POST",
      url: "/x-nmos/query/v1.3/subscriptions",
      payload: { resource_path: "/nodes", persist: false },
    });
    expect(validSubRes.statusCode).toBe(201);
    const validSub = validSubRes.json() as { id: string };

    // Create a subscription at v1.2 (the previous version)
    const invalidSubRes = await app.inject({
      method: "POST",
      url: "/x-nmos/query/v1.2/subscriptions",
      payload: { resource_path: "/nodes", persist: false },
    });
    expect(invalidSubRes.statusCode).toBe(201);
    const invalidSub = invalidSubRes.json() as { id: string };

    // GET /subscriptions?query.downgrade=v1.2 at v1.3 — spec says subscriptions
    // do not support downgrade: only the v1.3 subscription should appear.
    const listRes = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/subscriptions?query.downgrade=v1.2",
    });
    expect(listRes.statusCode).toBe(200);
    const listed = (listRes.json() as Array<{ id: string }>).map((s) => s.id);
    expect(listed).toContain(validSub.id);
    expect(listed).not.toContain(invalidSub.id);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_22_2 – Query API WebSockets implement downgrade queries
  // Verifies that a WS subscription with query.downgrade=v1.2 receives v1.3
  // resources but NOT v1.2-only resources, while a plain v1.3 subscription
  // does not receive any v1.2 resources.
  // -------------------------------------------------------------------------

  it("test_22_2: Query API WebSockets implement downgrade queries", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);

    // Subscription at v1.3 with downgrade=v1.2 — should see both
    const subDg = await subs.createSubscription({
      max_update_rate_ms: 0,
      persist: false,
      secure: false,
      authorization: false,
      resource_path: "/nodes",
      params: { "query.downgrade": "v1.2" },
      queryApiPathVersion: "v1.3",
    });
    // Subscription at v1.3 without downgrade — should only see v1.3
    const subNoDg = await subs.createSubscription({
      max_update_rate_ms: 0,
      persist: false,
      secure: false,
      authorization: false,
      resource_path: "/nodes",
      params: {},
      queryApiPathVersion: "v1.3",
    });

    const sentDg: string[] = [];
    const sentNoDg: string[] = [];
    await subs.attachSocket(subDg.id, mockWs(sentDg));
    await subs.attachSocket(subNoDg.id, mockWs(sentNoDg));
    // Discard SYNC messages
    sentDg.length = 0;
    sentNoDg.length = 0;

    const v13Id = cassandra.types.Uuid.random();
    const v12Id = cassandra.types.Uuid.random();

    // Dispatch a v1.3 node — both subscriptions should receive it
    subs.dispatchEvent(
      makeChangeEvent(
        "nodes",
        v13Id,
        "v1.3",
        null,
        JSON.stringify({ id: v13Id.toString(), label: "v13-node" }),
      ),
    );
    // Dispatch a v1.2 node — only the downgrade subscription should receive it
    subs.dispatchEvent(
      makeChangeEvent(
        "nodes",
        v12Id,
        "v1.2",
        null,
        JSON.stringify({ id: v12Id.toString(), label: "v12-node" }),
      ),
    );
    await vi.runAllTimersAsync();

    // Downgrade subscription: should have received both nodes
    const dgIds = sentDg.flatMap((m) => {
      const grain = JSON.parse(m) as {
        grain: { data: Array<{ path: string }> };
      };
      return grain.grain.data.map((d) => d.path);
    });
    expect(dgIds).toContain(v13Id.toString());
    expect(dgIds).toContain(v12Id.toString());

    // Plain v1.3 subscription: should only have received the v1.3 node
    const noDgIds = sentNoDg.flatMap((m) => {
      const grain = JSON.parse(m) as {
        grain: { data: Array<{ path: string }> };
      };
      return grain.grain.data.map((d) => d.path);
    });
    expect(noDgIds).toContain(v13Id.toString());
    expect(noDgIds).not.toContain(v12Id.toString());
    vi.useRealTimers();
  });

  // -------------------------------------------------------------------------
  // test_23 – Query API implements basic query parameters
  // -------------------------------------------------------------------------

  it("test_23: Query API implements basic query parameters", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const desc1 = cassandra.types.Uuid.random().toString();
    const desc2 = cassandra.types.Uuid.random().toString();

    await registerNode(app, makeNode({ label: "test_23", description: desc1 }));
    await registerNode(app, makeNode({ label: "test_23", description: desc2 }));

    // Unfiltered list should have at least 2 results
    const all = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?label=test_23",
    });
    expect(all.statusCode).toBe(200);
    expect((all.json() as unknown[]).length).toBeGreaterThanOrEqual(2);

    // Filter by description → exactly 1 result
    const filtered = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?description=${desc1}`,
    });
    expect(filtered.statusCode).toBe(200);
    expect((filtered.json() as unknown[]).length).toBe(1);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_23_1 – Query API WebSockets implement basic query parameters
  // Verifies that a subscription with a description filter only receives
  // ADDED grains for matching nodes and REMOVED grains when a node no longer matches.
  // -------------------------------------------------------------------------

  it("test_23_1: Query API WebSockets implement basic query parameters", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);

    const matchingId = cassandra.types.Uuid.random();
    const nonMatchingId = cassandra.types.Uuid.random();
    const targetDesc = matchingId.toString();

    // Subscription filtered by description
    const sub = await subs.createSubscription({
      max_update_rate_ms: 0,
      persist: false,
      secure: false,
      authorization: false,
      resource_path: "/nodes",
      params: { description: targetDesc },
      queryApiPathVersion: "v1.3",
    });
    const sent: string[] = [];
    await subs.attachSocket(sub.id, mockWs(sent));
    sent.length = 0; // discard SYNC

    // ADDED: register a matching node
    subs.dispatchEvent(
      makeChangeEvent(
        "nodes",
        matchingId,
        "v1.3",
        null,
        JSON.stringify({
          id: matchingId.toString(),
          label: "n",
          description: targetDesc,
        }),
      ),
    );
    // Non-matching node — should not appear
    subs.dispatchEvent(
      makeChangeEvent(
        "nodes",
        nonMatchingId,
        "v1.3",
        null,
        JSON.stringify({
          id: nonMatchingId.toString(),
          label: "n",
          description: "other",
        }),
      ),
    );
    await vi.runAllTimersAsync();

    expect(sent.length).toBeGreaterThanOrEqual(1);
    const addedData = sent.flatMap((m) => {
      const grain = JSON.parse(m) as {
        grain: { data: Array<{ path: string; pre?: unknown; post?: unknown }> };
      };
      return grain.grain.data;
    });
    // Matching node: ADDED — has post, no pre
    const addedEntry = addedData.find((d) => d.path === matchingId.toString());
    expect(addedEntry).toBeDefined();
    expect(addedEntry!.post).toBeDefined();
    expect(addedEntry!.pre).toBeUndefined();
    // Non-matching node: must not appear
    expect(
      addedData.find((d) => d.path === nonMatchingId.toString()),
    ).toBeUndefined();
    sent.length = 0;

    // REMOVED: update the matching node so it no longer has the description
    const newDesc = cassandra.types.Uuid.random().toString();
    subs.dispatchEvent(
      makeChangeEvent(
        "nodes",
        matchingId,
        "v1.3",
        JSON.stringify({
          id: matchingId.toString(),
          label: "n",
          description: targetDesc,
        }),
        JSON.stringify({
          id: matchingId.toString(),
          label: "n",
          description: newDesc,
        }),
      ),
    );
    await vi.runAllTimersAsync();

    expect(sent.length).toBeGreaterThanOrEqual(1);
    const removedData = sent.flatMap((m) => {
      const grain = JSON.parse(m) as {
        grain: { data: Array<{ path: string; pre?: unknown; post?: unknown }> };
      };
      return grain.grain.data;
    });
    // REMOVED: has pre (old matching data), no post
    const removedEntry = removedData.find(
      (d) => d.path === matchingId.toString(),
    );
    expect(removedEntry).toBeDefined();
    expect(removedEntry!.pre).toBeDefined();
    expect(removedEntry!.post).toBeUndefined();
    vi.useRealTimers();
  });

  // -------------------------------------------------------------------------
  // test_24 – Query API implements RQL (optional feature — expect 501)
  // -------------------------------------------------------------------------

  it("test_24: Query API implements RQL (or returns 501 if not supported)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const desc = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ description: desc }));

    const res = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/nodes?query.rql=eq(description,${desc})`,
    });
    // Registry may return 200 (with results) or 501 (not implemented)
    expect([200, 501]).toContain(res.statusCode);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_24_1 – Query API WebSockets implement RQL
  // RQL is optional (the server returns 501 for query.rql in HTTP queries).
  // For WebSocket subscriptions the server accepts the subscription body but
  // the query.rql param is not in the skip-list, so it acts as a plain filter.
  // We verify the endpoint responds correctly (either 201 or reflects 501-style
  // rejection) and does not crash.
  // -------------------------------------------------------------------------

  it("test_24_1: Query API WebSockets implement RQL (or signal non-support)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    // HTTP GET with query.rql must return 501 (not implemented)
    const httpRql = await app.inject({
      method: "GET",
      url: "/x-nmos/query/v1.3/nodes?query.rql=eq(description,foo)",
    });
    expect(httpRql.statusCode).toBe(501);

    // POST /subscriptions with query.rql param — the server should either
    // accept it (201) or reject it (400/501). It must not 500.
    const subRes = await app.inject({
      method: "POST",
      url: "/x-nmos/query/v1.3/subscriptions",
      payload: {
        resource_path: "/nodes",
        params: { "query.rql": "eq(description,foo)" },
        persist: false,
      },
    });
    expect([201, 400, 501]).toContain(subRes.statusCode);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_25 – Query API implements ancestry queries (optional feature)
  // -------------------------------------------------------------------------

  it("test_25: Query API implements ancestry queries (or returns 501/400 if not supported)", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const randomId = cassandra.types.Uuid.random().toString();
    const res = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/sources?query.ancestry_id=${randomId}&query.ancestry_type=children`,
    });
    // Optional: 200 empty, 400, or 501
    expect([200, 400, 501]).toContain(res.statusCode);
    if (res.statusCode === 200) {
      expect((res.json() as unknown[]).length).toBe(0);
    }

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_26 – Registration API responds with 400 when posting a resource without parent
  // -------------------------------------------------------------------------

  it("test_26: Registration API responds with 400 on posting a resource without parent", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const unknownNodeId = cassandra.types.Uuid.random().toString();
    const unknownDeviceId = cassandra.types.Uuid.random().toString();
    const unknownSourceId = cassandra.types.Uuid.random().toString();

    // device without node
    const r1 = await registerResource(app, "device", {
      id: cassandra.types.Uuid.random().toString(),
      label: "d",
      node_id: unknownNodeId,
    });
    expect(r1.statusCode).toBe(400);

    // source without device
    const r2 = await registerResource(app, "source", {
      id: cassandra.types.Uuid.random().toString(),
      label: "s",
      device_id: unknownDeviceId,
    });
    expect(r2.statusCode).toBe(400);

    // sender without device
    const r3 = await registerResource(app, "sender", {
      id: cassandra.types.Uuid.random().toString(),
      label: "se",
      device_id: unknownDeviceId,
    });
    expect(r3.statusCode).toBe(400);

    // receiver without device
    const r4 = await registerResource(app, "receiver", {
      id: cassandra.types.Uuid.random().toString(),
      label: "r",
      device_id: unknownDeviceId,
    });
    expect(r4.statusCode).toBe(400);

    // flow without source and device
    const r5 = await registerResource(app, "flow", {
      id: cassandra.types.Uuid.random().toString(),
      label: "f",
      source_id: unknownSourceId,
      device_id: unknownDeviceId,
    });
    expect(r5.statusCode).toBe(400);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_27 – Registration API cleans up Nodes after heartbeat timeout
  // Instead of waiting for the real timer, we call runCleanup() directly.
  // A node with no heartbeat record is treated as stale immediately.
  // -------------------------------------------------------------------------

  it("test_27: Registration API cleans up Nodes when heartbeat timeout expires", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const { node, device, source, flow, sender, receiver } =
      await registerFullChain(app, "test_27");

    // Verify all resources are present before cleanup
    for (const [type, data] of [
      ["nodes", node],
      ["devices", device],
      ["sources", source],
      ["flows", flow],
      ["senders", sender],
      ["receivers", receiver],
    ] as const) {
      const get = await app.inject({
        method: "GET",
        url: `/x-nmos/query/v1.3/${type}/${(data as Record<string, string>).id}`,
      });
      expect(get.statusCode, `${type} should exist before cleanup`).toBe(200);
    }

    // Advance fake time past the GC cutoff so the node's updated_tai (registration
    // time) falls before the cutoff. GC interval = 12s, cutoff = now - (12000 + 300ms).
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.advanceTimersByTime(
      (config.heartbeatGcIntervalSeconds * 1000 + 400) * 2,
    );

    const service = new HeartbeatCleanupService(store, config);
    await service.runCleanup();
    vi.useRealTimers();

    // Node and all child resources must be removed
    for (const [type, data] of [
      ["nodes", node],
      ["devices", device],
      ["sources", source],
      ["flows", flow],
      ["senders", sender],
      ["receivers", receiver],
    ] as const) {
      const get = await app.inject({
        method: "GET",
        url: `/x-nmos/query/v1.3/${type}/${(data as Record<string, string>).id}`,
      });
      expect(get.statusCode, `${type} should be 404 after cleanup`).toBe(404);
    }

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_28 – Registry removes stale child-resources of an unregistered Node
  // -------------------------------------------------------------------------

  it("test_28: Registry removes stale child-resources when a Node is deleted", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const { node, device, source, flow, sender, receiver } =
      await registerFullChain(app, "test_28");

    // Verify all resources exist
    for (const [type, data] of [
      ["nodes", node],
      ["devices", device],
      ["sources", source],
      ["flows", flow],
      ["senders", sender],
      ["receivers", receiver],
    ] as const) {
      const get = await app.inject({
        method: "GET",
        url: `/x-nmos/query/v1.3/${type}/${(data as Record<string, string>).id}`,
      });
      expect(get.statusCode).toBe(200);
    }

    // Delete the node via Registration API
    const del = await app.inject({
      method: "DELETE",
      url: `/x-nmos/registration/v1.3/resource/nodes/${node.id}`,
    });
    expect(del.statusCode).toBe(204);

    // All resources (node + children) should now return 404
    for (const [type, data] of [
      ["nodes", node],
      ["devices", device],
      ["sources", source],
      ["flows", flow],
      ["senders", sender],
      ["receivers", receiver],
    ] as const) {
      const get = await app.inject({
        method: "GET",
        url: `/x-nmos/query/v1.3/${type}/${(data as Record<string, string>).id}`,
      });
      expect(
        get.statusCode,
        `${type}/${(data as Record<string, string>).id} should be 404`,
      ).toBe(404);
    }

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_29 – Query API supports WebSocket subscription request
  // Verifies POST /subscriptions returns a valid subscription with ws_href
  // and that GET /subscriptions/:id retrieves it.
  // -------------------------------------------------------------------------

  it("test_29: Query API supports websocket subscription request", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const postRes = await app.inject({
      method: "POST",
      url: "/x-nmos/query/v1.3/subscriptions",
      payload: {
        resource_path: "/nodes",
        secure: false,
        persist: false,
      },
    });
    expect(postRes.statusCode).toBe(201);
    const sub = postRes.json() as {
      id: string;
      ws_href: string;
      secure: boolean;
    };

    // secure must match requested value
    expect(sub.secure).toBe(false);
    // ws_href must use ws:// (not wss://) for secure=false
    expect(sub.ws_href.toLowerCase()).toMatch(/^ws:/);

    // GET /subscriptions/:id must return the same subscription
    const getRes = await app.inject({
      method: "GET",
      url: `/x-nmos/query/v1.3/subscriptions/${sub.id}`,
    });
    expect(getRes.statusCode).toBe(200);
    expect((getRes.json() as { id: string }).id).toBe(sub.id);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_29_1 – Query API WebSocket subscriptions default to current protocol
  // When the `secure` field is omitted from the POST body the server should
  // infer it from the request protocol (http → false, https → true).
  // -------------------------------------------------------------------------

  it("test_29_1: Query API websocket subscription requests default to current protocol", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    // Omit the `secure` key entirely — server should derive it from the request
    const postRes = await app.inject({
      method: "POST",
      url: "/x-nmos/query/v1.3/subscriptions",
      payload: { resource_path: "/nodes", persist: false },
    });
    expect(postRes.statusCode).toBe(201);
    const sub = postRes.json() as { secure: boolean; ws_href: string };

    // inject() uses http, so secure should default to false and ws_href to ws://
    expect(sub.secure).toBe(false);
    expect(sub.ws_href.toLowerCase()).toMatch(/^ws:/);

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_30 – Registration API accepts heartbeat requests for a registered Node
  // -------------------------------------------------------------------------

  it("test_30: Registration API accepts heartbeat requests for a Node held in the registry", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random().toString();
    await registerNode(app, makeNode({ id: nodeId }));

    const hb = await app.inject({
      method: "POST",
      url: `/x-nmos/registration/v1.3/health/nodes/${nodeId}`,
    });
    expect(hb.statusCode).toBe(200);
    // Response body should include health timestamp
    const body = hb.json() as Record<string, unknown>;
    expect(body).toHaveProperty("health");

    await app.close();
  });

  // -------------------------------------------------------------------------
  // test_31 – Query API sends correct WebSocket event messages
  // Verifies UNCHANGED (SYNC), ADDED, MODIFIED, and REMOVED grain events for
  // all six resource types using mock WebSocket connections.
  // -------------------------------------------------------------------------

  it("test_31: Query API sends correct WebSocket event messages (UNCHANGED, ADDED, MODIFIED, REMOVED)", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);

    const resourceTypes = [
      "nodes",
      "devices",
      "sources",
      "flows",
      "senders",
      "receivers",
    ] as const;

    // Pre-populate one resource of each type so the SYNC message is non-empty
    const ids: Record<string, cassandra.types.Uuid> = {};
    for (const type of resourceTypes) {
      const id = cassandra.types.Uuid.random();
      ids[type] = id;
      await store.upsertResource(
        type,
        id,
        "v1.3",
        JSON.stringify({ id: id.toString(), label: type }),
      );
    }

    // Create one subscription per resource type
    const sentMap: Record<string, string[]> = {};
    for (const type of resourceTypes) {
      const sub = await subs.createSubscription({
        max_update_rate_ms: 0,
        persist: false,
        secure: false,
        authorization: false,
        resource_path: `/${type}`,
        params: {},
        queryApiPathVersion: "v1.3",
      });
      sentMap[type] = [];
      await subs.attachSocket(sub.id, mockWs(sentMap[type]!));
    }

    // ── UNCHANGED (SYNC) ──────────────────────────────────────────────────
    // The initial SYNC message is sent synchronously during attachSocket.
    // Each message must contain the pre-populated resource with pre == post.
    for (const type of resourceTypes) {
      const msgs = sentMap[type]!;
      expect(
        msgs.length,
        `${type}: expected SYNC message`,
      ).toBeGreaterThanOrEqual(1);
      const sync = JSON.parse(msgs[0]!) as {
        grain: {
          data: Array<{
            path: string;
            pre?: Record<string, unknown>;
            post?: Record<string, unknown>;
          }>;
        };
      };
      const entry = sync.grain.data.find(
        (d) => d.path === ids[type]!.toString(),
      );
      expect(entry, `${type}: SYNC entry missing`).toBeDefined();
      expect(entry!.pre).toBeDefined();
      expect(entry!.post).toBeDefined();
      // Clear for next phase
      msgs.length = 0;
    }

    // ── MODIFIED ──────────────────────────────────────────────────────────
    // Re-dispatch each resource as an update (pre + post both set)
    for (const type of resourceTypes) {
      const id = ids[type]!;
      subs.dispatchEvent(
        makeChangeEvent(
          type,
          id,
          "v1.3",
          JSON.stringify({ id: id.toString(), label: `${type}-old` }),
          JSON.stringify({ id: id.toString(), label: `${type}-new` }),
        ),
      );
    }
    await vi.runAllTimersAsync();

    for (const type of resourceTypes) {
      const msgs = sentMap[type]!;
      expect(
        msgs.length,
        `${type}: expected MODIFIED message`,
      ).toBeGreaterThanOrEqual(1);
      const data = msgs.flatMap((m) => {
        const g = JSON.parse(m) as {
          grain: {
            data: Array<{ path: string; pre?: unknown; post?: unknown }>;
          };
        };
        return g.grain.data;
      });
      const entry = data.find((d) => d.path === ids[type]!.toString());
      expect(entry, `${type}: MODIFIED entry missing`).toBeDefined();
      expect(entry!.pre).toBeDefined();
      expect(entry!.post).toBeDefined();
      msgs.length = 0;
    }

    // ── REMOVED ───────────────────────────────────────────────────────────
    for (const type of resourceTypes) {
      const id = ids[type]!;
      subs.dispatchEvent(
        makeChangeEvent(
          type,
          id,
          "v1.3",
          JSON.stringify({ id: id.toString(), label: type }),
          null,
        ),
      );
    }
    await vi.runAllTimersAsync();

    for (const type of resourceTypes) {
      const msgs = sentMap[type]!;
      expect(
        msgs.length,
        `${type}: expected REMOVED message`,
      ).toBeGreaterThanOrEqual(1);
      const data = msgs.flatMap((m) => {
        const g = JSON.parse(m) as {
          grain: {
            data: Array<{ path: string; pre?: unknown; post?: unknown }>;
          };
        };
        return g.grain.data;
      });
      const entry = data.find((d) => d.path === ids[type]!.toString());
      expect(entry, `${type}: REMOVED entry missing`).toBeDefined();
      expect(entry!.pre).toBeDefined();
      expect(entry!.post).toBeUndefined();
      msgs.length = 0;
    }

    // ── ADDED ─────────────────────────────────────────────────────────────
    for (const type of resourceTypes) {
      const id = cassandra.types.Uuid.random();
      ids[type] = id;
      subs.dispatchEvent(
        makeChangeEvent(
          type,
          id,
          "v1.3",
          null,
          JSON.stringify({ id: id.toString(), label: `${type}-added` }),
        ),
      );
    }
    await vi.runAllTimersAsync();

    for (const type of resourceTypes) {
      const msgs = sentMap[type]!;
      expect(
        msgs.length,
        `${type}: expected ADDED message`,
      ).toBeGreaterThanOrEqual(1);
      const data = msgs.flatMap((m) => {
        const g = JSON.parse(m) as {
          grain: {
            data: Array<{ path: string; pre?: unknown; post?: unknown }>;
          };
        };
        return g.grain.data;
      });
      const entry = data.find((d) => d.path === ids[type]!.toString());
      expect(entry, `${type}: ADDED entry missing`).toBeDefined();
      expect(entry!.pre).toBeUndefined();
      expect(entry!.post).toBeDefined();
    }
    vi.useRealTimers();
  });

  // -------------------------------------------------------------------------
  // test_32 – Registration API generates 409 when a conflicting registration exists
  // -------------------------------------------------------------------------

  it("test_32: Registration API generates 409 when a conflicting registration exists under a different API version", async () => {
    const config = testConfig();
    const store = new InMemoryRegistryStore();
    const subs = new SubscriptionManager(config, store);
    const app = await createApp({ config, store, subs, logger: false });

    const nodeId = cassandra.types.Uuid.random();

    // Register node at v1.2
    await store.upsertResource(
      "nodes",
      nodeId,
      "v1.2",
      JSON.stringify({
        id: nodeId.toString(),
        label: "node-v1.2",
        version: "1:0",
      }),
    );

    // Attempt to register the same node ID at v1.3 → 409
    const res = await registerNode(app, {
      id: nodeId.toString(),
      label: "node-v1.3",
      version: "1:1",
    });
    expect(res.statusCode).toBe(409);

    // Location header must point to existing v1.2 resource
    const location = res.headers["location"] as string;
    expect(location).toContain("v1.2");
    expect(location).toContain(nodeId.toString());

    await app.close();
  });
});
