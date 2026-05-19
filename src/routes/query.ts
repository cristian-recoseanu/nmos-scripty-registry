/**
 * NMOS Query API route handlers.
 *
 * This module implements the Query API endpoints for listing resources, retrieving individual resources,
 * and managing WebSocket subscriptions. It handles version projection, downgrade queries, and paging.
 */
import type { FastifyPluginAsync, FastifyReply } from "fastify";
import cassandra from "cassandra-driver";
import { sendError } from "../errors.js";
import {
  type ParsedIs04Version,
  parseIs04Version,
  parseQueryDowngrade,
  shouldIncludeStoredResource,
} from "../query/downgrade.js";
import {
  applyPagingAndSort,
  assertSupportedQueryParams,
  extractAttributeFilters,
  generatePagingLinks,
  matchesFilters,
  PagingError,
  parsePaging,
  parseResourceObject,
  type Paging,
  type ResourceRow,
} from "../query/resourceQuery.js";
import { projectResourceForQuery } from "../query/versionProjection.js";
import { taiNow } from "../tai.js";
import type { RegistryPort, ResourcePlural } from "../store/registryPort.js";
import type { SubscriptionManager } from "../subscriptions/subscriptionManager.js";
import logger from "../logger.js";

/**
 * All NMOS resource types that can be queried.
 */
const COLLECTIONS: ResourcePlural[] = [
  "nodes",
  "devices",
  "sources",
  "flows",
  "senders",
  "receivers",
];

/**
 * Request body for creating a WebSocket subscription.
 */
type SubPost = {
  max_update_rate_ms?: number;
  persist?: boolean;
  secure?: boolean;
  authorization?: boolean;
  resource_path: string;
  params?: Record<string, unknown>;
};

/**
 * Fastify plugin that registers all Query API routes.
 * Handles resource listing, retrieval, and subscription management.
 */
export const queryRoutes: FastifyPluginAsync<{
  store: RegistryPort;
  subs: SubscriptionManager;
  apiPathVersion: string;
}> = async (app, opts) => {
  const { store, subs, apiPathVersion } = opts;
  const pathVerParsed = parseIs04Version(apiPathVersion);
  if (!pathVerParsed) {
    throw new Error(`Invalid Query API path version: ${apiPathVersion}`);
  }

  app.get("/", async (_req, reply) => {
    return reply.send([
      "subscriptions/",
      "flows/",
      "sources/",
      "nodes/",
      "devices/",
      "senders/",
      "receivers/",
    ]);
  });

  /**
   * Resolves and validates the query.downgrade parameter.
   * Returns null if validation fails (error already sent).
   */
  function resolveDowngrade(
    q: Record<string, string | undefined>,
    reply: FastifyReply,
  ): { downgrade: ParsedIs04Version | null } | null {
    const downgradeRaw = q["query.downgrade"];
    const parsed = parseQueryDowngrade(apiPathVersion, downgradeRaw);
    if (!parsed.ok) {
      void sendError(reply, 400, parsed.error, null);
      return null;
    }
    return { downgrade: parsed.downgrade };
  }

  /**
   * Converts store resource rows to query resource rows.
   */
  function toRows(type: ResourcePlural): Promise<ResourceRow[]> {
    return store.listResources(type).then((rows) =>
      rows.map((r) => ({
        id: r.id.toString(),
        json: r.json,
        api_version: r.api_version,
        created_tai: r.created_tai,
        updated_tai: r.updated_tai,
      })),
    );
  }

  for (const c of COLLECTIONS) {
    const base = `/${c}`;
    app.get(base, async (req, reply) => {
      const q = req.query as Record<string, string | undefined>;
      const dg = resolveDowngrade(q, reply);
      if (!dg) return;
      const { unsupported } = assertSupportedQueryParams(q);
      if (unsupported.length) {
        return sendError(
          reply,
          501,
          "Query feature not implemented",
          unsupported.join(", "),
        );
      }
      let paging: Paging;
      try {
        paging = parsePaging(q);
      } catch (err) {
        if (err instanceof PagingError) {
          return sendError(reply, 400, err.message, null);
        }
        throw err;
      }
      const filters = extractAttributeFilters(q);
      const snapshotUntil = taiNow();
      let rows = await toRows(c);
      rows = rows.filter((row) =>
        shouldIncludeStoredResource(
          pathVerParsed,
          row.api_version,
          dg.downgrade,
        ),
      );
      if (Object.keys(filters).length) {
        rows = rows.filter((row) =>
          matchesFilters(parseResourceObject(row.json), filters),
        );
      }
      logger.debug("Query before pagination", {
        filters,
        totalRows: rows.length,
        pagingSince: paging.since,
        pagingUntil: paging.until,
        pagingLimit: paging.limit,
      });
      const pagingResult = applyPagingAndSort(rows, paging, snapshotUntil);
      logger.debug("Query after pagination", {
        returnedRows: pagingResult.rows.length,
        actualSince: pagingResult.actualSince,
        actualUntil: pagingResult.actualUntil,
        hasNext: pagingResult.hasNext,
        hasPrev: pagingResult.hasPrev,
      });
      const body = pagingResult.rows.map((row) =>
        projectResourceForQuery(
          c,
          parseResourceObject(row.json),
          pathVerParsed,
          row.api_version,
        ),
      );

      // Add pagination headers as per NMOS IS-04 spec
      // X-Paging-Limit MUST be returned in all cases where pagination is in use
      void reply.header("X-Paging-Limit", String(paging.limit));

      // X-Paging-Since and X-Paging-Until should be included when pagination is in use
      // Use actual values when available, otherwise use request parameters or defaults
      if (pagingResult.actualSince) {
        void reply.header("X-Paging-Since", pagingResult.actualSince);
      } else {
        void reply.header("X-Paging-Since", paging.since || "0:0");
      }

      if (pagingResult.actualUntil) {
        void reply.header("X-Paging-Until", pagingResult.actualUntil);
      } else {
        void reply.header("X-Paging-Until", paging.until || "0:0");
      }

      // Add Link header with navigation links
      const baseUrl = `${req.protocol}://${req.host}${req.url.split("?")[0]}`;
      const links = generatePagingLinks(baseUrl, paging, pagingResult, q);
      if (links.length > 0) {
        void reply.header("Link", links.join(", "));
      }

      return reply.send(body);
    });

    app.get(`${base}/:id`, async (req, reply) => {
      const q = req.query as Record<string, string | undefined>;
      const dg = resolveDowngrade(q, reply);
      if (!dg) return;
      const { unsupported } = assertSupportedQueryParams(q);
      if (unsupported.length) {
        return sendError(
          reply,
          501,
          "Query feature not implemented",
          unsupported.join(", "),
        );
      }
      const { id } = req.params as { id: string };
      let uid: cassandra.types.Uuid;
      try {
        uid = cassandra.types.Uuid.fromString(id);
      } catch {
        return sendError(reply, 404, "Not found", null);
      }
      const row = await store.getResource(c, uid);
      if (!row) return sendError(reply, 404, "Not found", null);
      if (
        !shouldIncludeStoredResource(
          pathVerParsed,
          row.api_version,
          dg.downgrade,
        )
      ) {
        return sendError(reply, 404, "Not found", null);
      }
      const body = projectResourceForQuery(
        c,
        parseResourceObject(row.json),
        pathVerParsed,
        row.api_version,
      );
      return reply.send(body);
    });
  }

  app.options("/subscriptions", async (_req, reply) => reply.code(200).send());
  app.post("/subscriptions", async (req, reply) => {
    const body = (await req.body) as SubPost;
    logger.info("Creating subscription", {
      resource_path: body?.resource_path,
    });
    if (!body?.resource_path) {
      return sendError(reply, 400, "resource_path required", null);
    }
    if (!subs.pathToPlural(body.resource_path)) {
      return sendError(reply, 400, "Invalid resource_path", null);
    }
    const downgradeP = body.params?.["query.downgrade"];
    if (downgradeP !== undefined && downgradeP !== null) {
      const r = parseQueryDowngrade(apiPathVersion, String(downgradeP));
      if (!r.ok) return sendError(reply, 400, r.error, null);
    }
    const proto =
      req.headers["x-forwarded-proto"] ??
      (req as { protocol?: string }).protocol ??
      "http";
    const isHttps = String(proto).includes("https");
    const secure = body.secure ?? isHttps;
    const authorization = body.authorization ?? false;
    const def = await subs.createSubscription({
      max_update_rate_ms: body.max_update_rate_ms ?? 100,
      persist: body.persist ?? false,
      secure,
      authorization,
      resource_path: body.resource_path,
      params: body.params ?? {},
      queryApiPathVersion: apiPathVersion,
    });
    logger.info("Subscription created", {
      id: def.id,
      resource_path: def.resource_path,
    });
    const location = `${req.url}/${def.id}`;
    void reply.header("Location", location);
    return reply.code(201).send(def);
  });

  app.get("/subscriptions", async (_req, reply) => {
    return reply.send(
      subs.list().filter((s) => s.queryApiPathVersion === apiPathVersion),
    );
  });

  app.options("/subscriptions/:subscriptionId", async (_req, reply) =>
    reply.code(200).send(),
  );
  app.get("/subscriptions/:subscriptionId", async (req, reply) => {
    const { subscriptionId } = req.params as { subscriptionId: string };
    const s = await subs.get(subscriptionId);
    if (!s) return sendError(reply, 404, "Not found", null);
    return reply.send(s);
  });

  app.delete("/subscriptions/:subscriptionId", async (req, reply) => {
    const { subscriptionId } = req.params as { subscriptionId: string };
    logger.info("Deleting subscription", { subscriptionId });
    const ok = await subs.delete(subscriptionId);
    if (!ok) return sendError(reply, 404, "Not found", null);
    logger.info("Subscription deleted", { subscriptionId });
    return reply.code(204).send();
  });
};
