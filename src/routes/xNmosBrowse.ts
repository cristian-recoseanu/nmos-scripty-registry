/**
 * NMOS browse API route handlers.
 *
 * This module provides hierarchical index routes under /x-nmos for API discovery.
 * Returns arrays of relative path segments for navigation.
 */
import type { FastifyPluginAsync } from "fastify";
import type { RegistryConfig } from "../config.js";
import logger from "../logger.js";

/**
 * Fastify plugin that registers x-nmos browse routes.
 * Provides hierarchical API discovery endpoints.
 */
export const xNmosBrowseRoutes: FastifyPluginAsync<{
  config: RegistryConfig;
}> = async (app, opts) => {
  const versionDirs = opts.config.supportedApiVersions.map((v) => `${v}/`);
  logger.debug("Registered x-nmos browse routes", {
    supportedApiVersions: opts.config.supportedApiVersions,
  });

  app.get("/x-nmos", async (_req, reply) => {
    return reply.send(["registration/", "query/"]);
  });

  app.get("/x-nmos/registration", async (_req, reply) => {
    return reply.send([...versionDirs]);
  });

  app.get("/x-nmos/query", async (_req, reply) => {
    return reply.send([...versionDirs]);
  });
};
