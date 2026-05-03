/**
 * Fastify application factory for the NMOS registry HTTP server.
 *
 * This module creates and configures the Fastify application with CORS, WebSocket support,
 * and all NMOS API routes (registration, query, and browse). It does not start the server,
 * allowing the app to be used in tests and production.
 */
import cors from "@fastify/cors";
import websocket from "@fastify/websocket";
import Fastify, { type FastifyBaseLogger } from "fastify";
import type { RegistryConfig } from "../config.js";
import { sendError } from "../errors.js";
import { queryRoutes } from "../routes/query.js";
import { registrationRoutes } from "../routes/registration.js";
import { xNmosBrowseRoutes } from "../routes/xNmosBrowse.js";
import type { RegistryPort } from "../store/registryPort.js";
import type { SubscriptionManager } from "../subscriptions/subscriptionManager.js";
import winstonLogger from "../logger.js";

/**
 * Options for creating the Fastify application.
 * Includes configuration, store, subscription manager, and optional logger.
 */
export type CreateAppOptions = {
  config: RegistryConfig;
  store: RegistryPort;
  subs: SubscriptionManager;
  logger?: boolean | FastifyBaseLogger;
};

/**
 * Creates and configures the Fastify application.
 * Registers plugins, routes, and WebSocket handler. Does not start listening.
 */
export async function createApp(opts: CreateAppOptions) {
  winstonLogger.info("Creating Fastify app", {
    supportedApiVersions: opts.config.supportedApiVersions,
  });
  const { config, store, subs } = opts;
  const logger = opts.logger ?? true;
  const app = Fastify({
    logger,
    routerOptions: { ignoreTrailingSlash: true },
  });
  await app.register(cors, {
    origin: true,
    methods: ["GET", "POST", "DELETE", "OPTIONS"],
    optionsSuccessStatus: 200,
  });
  await app.register(websocket);
  winstonLogger.info("Registered CORS and WebSocket plugins");

  app.addHook("onSend", async (_req, reply, payload) => {
    const contentType = reply.getHeader("content-type");
    if (typeof contentType !== "string") return payload;
    const [mediaType] = contentType.split(";", 1);
    const trimmed = mediaType?.trim().toLowerCase();
    const isJsonType =
      trimmed === "application/json" || trimmed?.endsWith("+json") === true;
    if (!isJsonType || !mediaType) return payload;
    reply.header("Content-Type", mediaType.trim());
    return payload;
  });

  await app.register(xNmosBrowseRoutes, { config });
  winstonLogger.info("Registered x-nmos browse routes");

  for (const apiPathVersion of config.supportedApiVersions) {
    await app.register(registrationRoutes, {
      store,
      apiPathVersion,
      prefix: `/x-nmos/registration/${apiPathVersion}`,
    });
    await app.register(queryRoutes, {
      store,
      subs,
      apiPathVersion,
      prefix: `/x-nmos/query/${apiPathVersion}`,
    });
    winstonLogger.info("Registered API routes", { apiPathVersion });
  }

  app.get("/ws", { websocket: true }, (socket, req) => {
    const url = new URL(req.url, `http://${req.headers.host ?? "localhost"}`);
    const uid = url.searchParams.get("uid");
    if (!uid) {
      winstonLogger.warn("WebSocket connection without uid", { url: req.url });
      socket.close(1008, "unknown subscription");
      return;
    }
    winstonLogger.info("WebSocket connection attempt", { uid });
    void subs.attachSocket(uid, socket).then((ok) => {
      if (!ok) {
        winstonLogger.warn("WebSocket attach failed", { uid });
        socket.close(1008, "unknown subscription");
      } else {
        winstonLogger.info("WebSocket attached successfully", { uid });
      }
    });
  });

  app.setNotFoundHandler(async (_req, reply) => {
    return sendError(reply, 404, "Not found", null);
  });

  app.setErrorHandler(async (error, _req, reply) => {
    const maybeError = error as { statusCode?: unknown; message?: unknown };
    const statusCode =
      typeof maybeError.statusCode === "number" ? maybeError.statusCode : 500;
    const safeCode = statusCode >= 400 && statusCode <= 599 ? statusCode : 500;
    const rawMessage =
      typeof maybeError.message === "string"
        ? maybeError.message
        : "Internal Server Error";
    const message = safeCode >= 500 ? "Internal Server Error" : rawMessage;
    const debug = safeCode >= 500 ? rawMessage : null;
    return sendError(reply, safeCode, message, debug);
  });

  winstonLogger.info("Fastify app created");
  return app;
}
