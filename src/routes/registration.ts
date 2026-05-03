/**
 * NMOS Registration API route handlers.
 *
 * This module implements the Registration API endpoints for creating, updating, deleting,
 * and retrieving resources. It also handles node health updates and queries.
 */
import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";
import cassandra from "cassandra-driver";
import { sendError } from "../errors.js";
import type { RegistryPort, ResourcePlural } from "../store/registryPort.js";
import { taiFromDate } from "../tai.js";
import logger from "../logger.js";

/**
 * Request body for resource registration.
 */
type RegBody = {
  type: string;
  data: Record<string, unknown>;
};

/**
 * Maps singular resource types to their plural forms.
 */
const SINGULAR: Record<string, ResourcePlural> = {
  node: "nodes",
  device: "devices",
  source: "sources",
  flow: "flows",
  sender: "senders",
  receiver: "receivers",
};

/**
 * Generates the resource location URL for a registered resource.
 */
function resourceLocation(version: string, plural: ResourcePlural, id: string) {
  return `/x-nmos/registration/${version}/resource/${plural}/${id}`;
}

/**
 * Reads and validates an optional UUID string field from resource payload.
 */
function readUuidField(
  reply: FastifyReply,
  data: Record<string, unknown>,
  field: string,
  opts?: { nullable?: boolean },
): cassandra.types.Uuid | null {
  const value = data[field];
  const nullable = opts?.nullable ?? false;
  if (value === null && nullable) return null;
  if (typeof value !== "string") {
    sendError(reply, 400, `Resource data.${field} must be a string UUID`, null);
    return null;
  }
  try {
    return cassandra.types.Uuid.fromString(value);
  } catch {
    sendError(reply, 400, `Invalid UUID in data.${field}`, null);
    return null;
  }
}

class MutationLock {
  private queue = Promise.resolve();

  async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.queue.then(fn, fn);
    this.queue = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }
}

/**
 * Fastify plugin that registers all Registration API routes.
 * Handles resource CRUD operations and health management.
 */
export const registrationRoutes: FastifyPluginAsync<{
  store: RegistryPort;
  apiPathVersion: string;
}> = async (app, opts) => {
  const { store, apiPathVersion: version } = opts;
  const mutationLock = new MutationLock();

  const deviceChildTypes: ResourcePlural[] = [
    "receivers",
    "senders",
    "flows",
    "sources",
  ];

  const deleteChildrenByDevice = async (deviceId: string) => {
    for (const childType of deviceChildTypes) {
      const rows = await store.listResources(childType);
      for (const row of rows) {
        const data = JSON.parse(row.json) as Record<string, unknown>;
        if (data.device_id === deviceId) {
          await store.deleteResource(childType, row.id);
        }
      }
    }
  };

  const deleteChildrenByNode = async (nodeId: string) => {
    const devices = await store.listResources("devices");
    for (const device of devices) {
      const data = JSON.parse(device.json) as Record<string, unknown>;
      if (data.node_id !== nodeId) continue;
      const deviceId = device.id.toString();
      await deleteChildrenByDevice(deviceId);
      await store.deleteResource("devices", device.id);
    }
  };

  const sleep = async (ms: number) =>
    new Promise<void>((resolve) => {
      setTimeout(resolve, ms);
    });

  const waitForParent = async (
    parentType: ResourcePlural,
    parentId: cassandra.types.Uuid,
  ): Promise<boolean> => {
    const attempts = 5;
    const waitMs = 25;
    for (let i = 0; i < attempts; i += 1) {
      const parent = await store.getResource(parentType, parentId);
      if (parent) return true;
      if (i < attempts - 1) await sleep(waitMs);
    }
    return false;
  };

  app.get("/", async (_req, reply) => {
    return reply.send(["resource/", "health/"]);
  });

  app.options("/resource", async (_req, reply) => reply.code(200).send());
  app.post("/resource", async (req, reply) => {
    return mutationLock.runExclusive(async () => {
      const body = req.body as RegBody | undefined;
      logger.info("Resource registration attempt", { body });
      if (!body?.type || !body.data || typeof body.data !== "object") {
        return sendError(reply, 400, "Invalid registration body", null);
      }
      const plural = SINGULAR[body.type];
      if (!plural) {
        return sendError(reply, 400, "Unknown resource type", null);
      }
      if (!Object.hasOwn(body.data, "label")) {
        return sendError(reply, 400, "Resource data.label is required", null);
      }
      const idRaw = body.data.id;
      if (typeof idRaw !== "string") {
        return sendError(
          reply,
          400,
          "Resource data.id must be a string UUID",
          null,
        );
      }
      let id: cassandra.types.Uuid;
      try {
        id = cassandra.types.Uuid.fromString(idRaw);
      } catch {
        return sendError(reply, 400, "Invalid UUID in data.id", null);
      }

      const existing = await store.getResource(plural, id);
      if (existing && existing.api_version !== version) {
        void reply.header(
          "Location",
          resourceLocation(existing.api_version, plural, idRaw),
        );
        return sendError(
          reply,
          409,
          "Resource registered under a different API version",
          null,
        );
      }

      const ensureParentExists = async (
        parentType: ResourcePlural,
        parentId: cassandra.types.Uuid,
        fieldName: string,
      ) => {
        const parentExists = await waitForParent(parentType, parentId);
        if (!parentExists) {
          sendError(
            reply,
            400,
            `Resource data.${fieldName} references unregistered ${parentType.slice(
              0,
              -1,
            )}`,
            null,
          );
          return false;
        }
        return true;
      };

      if (plural === "devices") {
        const nodeId = readUuidField(reply, body.data, "node_id");
        if (!nodeId) return;
        if (!(await ensureParentExists("nodes", nodeId, "node_id"))) return;
      } else if (plural === "receivers") {
        const deviceId = readUuidField(reply, body.data, "device_id");
        if (!deviceId) return;
        if (!(await ensureParentExists("devices", deviceId, "device_id")))
          return;
      } else if (plural === "sources") {
        const deviceId = readUuidField(reply, body.data, "device_id");
        if (!deviceId) return;
        if (!(await ensureParentExists("devices", deviceId, "device_id")))
          return;
      } else if (plural === "flows") {
        const sourceId = readUuidField(reply, body.data, "source_id");
        if (!sourceId) return;
        const deviceId = readUuidField(reply, body.data, "device_id");
        if (!deviceId) return;
        if (!(await ensureParentExists("sources", sourceId, "source_id")))
          return;
        if (!(await ensureParentExists("devices", deviceId, "device_id")))
          return;
      } else if (plural === "senders") {
        const deviceId = readUuidField(reply, body.data, "device_id");
        if (!deviceId) return;
        if (!(await ensureParentExists("devices", deviceId, "device_id")))
          return;
        const flowId = readUuidField(reply, body.data, "flow_id", {
          nullable: true,
        });
        if (flowId === null && body.data.flow_id !== null) return;
      }

      const json = JSON.stringify(body.data);
      const created = await store.upsertResource(plural, id, version, json);
      const loc = resourceLocation(version, plural, idRaw);
      void reply.header("Location", loc);
      logger.info("Resource registered", { type: plural, id: idRaw, created });
      if (created) return reply.code(201).send(body.data);
      return reply.code(200).send(body.data);
    });
  });

  const resourcePath = "/resource/:resourceType/:resourceId";

  /**
   * Validates and converts a resource type string to a ResourcePlural.
   */
  async function getResourceType(
    resourceType: string,
  ): Promise<ResourcePlural | null> {
    if (
      resourceType === "nodes" ||
      resourceType === "devices" ||
      resourceType === "sources" ||
      resourceType === "flows" ||
      resourceType === "senders" ||
      resourceType === "receivers"
    ) {
      return resourceType;
    }
    return null;
  }

  /**
   * Handles CORS preflight OPTIONS requests.
   */
  const handleOptions = async (_req: FastifyRequest, reply: FastifyReply) =>
    reply.code(200).send();
  app.options(resourcePath, handleOptions);

  app.delete(resourcePath, async (req, reply) => {
    return mutationLock.runExclusive(async () => {
      const { resourceType, resourceId } = req.params as {
        resourceType: string;
        resourceId: string;
      };
      logger.info("Resource deletion attempt", {
        resourceType,
        resourceId,
        body: req.body,
      });
      const plural = await getResourceType(resourceType);
      if (!plural) return sendError(reply, 404, "Not found", null);
      let id: cassandra.types.Uuid;
      try {
        id = cassandra.types.Uuid.fromString(resourceId);
      } catch {
        return sendError(reply, 404, "Not found", null);
      }
      const existing = await store.getResource(plural, id);
      if (!existing) return sendError(reply, 404, "Not found", null);
      if (plural === "nodes") {
        await deleteChildrenByNode(resourceId);
      } else if (plural === "devices") {
        await deleteChildrenByDevice(resourceId);
      }
      await store.deleteResource(plural, id);
      logger.info("Resource deleted", { resourceType, resourceId });
      return reply.code(204).send();
    });
  });

  app.get(resourcePath, async (req, reply) => {
    const { resourceType, resourceId } = req.params as {
      resourceType: string;
      resourceId: string;
    };
    const plural = await getResourceType(resourceType);
    if (!plural) return sendError(reply, 404, "Not found", null);
    let id: cassandra.types.Uuid;
    try {
      id = cassandra.types.Uuid.fromString(resourceId);
    } catch {
      return sendError(reply, 404, "Not found", null);
    }
    const row = await store.getResource(plural, id);
    if (!row) return sendError(reply, 404, "Not found", null);
    if (row.api_version !== version) {
      void reply.header(
        "Location",
        resourceLocation(row.api_version, plural, resourceId),
      );
      return sendError(
        reply,
        409,
        "Resource registered under a different API version",
        null,
      );
    }
    return reply.send(JSON.parse(row.json));
  });

  const healthPath = "/health/nodes/:nodeId";
  app.options(healthPath, handleOptions);
  app.post(healthPath, async (req, reply) => {
    const { nodeId } = req.params as { nodeId: string };
    logger.debug("Health update received", { nodeId });
    let id: cassandra.types.Uuid;
    try {
      id = cassandra.types.Uuid.fromString(nodeId);
    } catch {
      return sendError(reply, 400, "Invalid node id", null);
    }
    const payload = (await req.body) as
      | Record<string, unknown>
      | string
      | undefined;
    const json =
      typeof payload === "string" ? payload : JSON.stringify(payload ?? {});
    await store.upsertHealth(id, json);
    const sec = taiFromDate().split(":")[0] ?? "0";
    return reply.send({ health: sec });
  });

  app.get(healthPath, async (req, reply) => {
    const { nodeId } = req.params as { nodeId: string };
    let id: cassandra.types.Uuid;
    try {
      id = cassandra.types.Uuid.fromString(nodeId);
    } catch {
      return sendError(reply, 404, "Not found", null);
    }
    const row = await store.getHealth(id);
    if (!row) return sendError(reply, 404, "Not found", null);
    return reply.send(JSON.parse(row.json));
  });
};
