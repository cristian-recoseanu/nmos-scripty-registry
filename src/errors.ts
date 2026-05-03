/**
 * Error handling utilities for the NMOS registry API.
 *
 * This module provides standardized error response formatting for Fastify replies,
 * ensuring consistent error responses across all API endpoints.
 */
import type { FastifyReply } from "fastify";
import logger from "./logger.js";

/**
 * Standard error response body format for NMOS API errors.
 * Follows the NMOS specification for error responses.
 */
export type NmosErrorBody = {
  code: number;
  error: string;
  debug: string | null;
};

/**
 * Sends a standardized error response to the client.
 * Logs the error and returns a properly formatted error response with HTTP status code.
 */
export function sendError(
  reply: FastifyReply,
  code: number,
  message: string,
  debug: string | null = null,
) {
  const body: NmosErrorBody = { code, error: message, debug };
  logger.warn("Sending error response", { code, message, debug });
  return reply.code(code).send(body);
}
