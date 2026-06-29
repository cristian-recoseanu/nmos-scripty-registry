/**
 * Resource version projection for Query API responses.
 *
 * This module handles projecting stored resources to different API versions,
 * stripping fields that don't exist in older minor versions when downgrade is requested.
 */
import type { ParsedIs04Version } from "./downgrade.js";
import { parseIs04Version } from "./downgrade.js";
import type { ResourcePlural } from "../store/registryPort.js";

/**
 * Projects a stored resource to the appropriate shape for the Query API response.
 * When the stored resource is newer than the requested path version, strips fields
 * that don't exist in the target minor version (e.g., removing 'authorization' from v1.3 nodes for v1.2).
 */
export function projectResourceForQuery(
  plural: ResourcePlural,
  body: Record<string, unknown>,
  pathVer: ParsedIs04Version,
  storedApiVersionRaw: string,
): Record<string, unknown> {
  const stored = parseIs04Version(storedApiVersionRaw);
  if (!stored || stored.major !== pathVer.major) return { ...body };
  if (stored.minor <= pathVer.minor) return { ...body };
  return stripResourceToLowerMinor(plural, body, pathVer);
}

/**
 * Strips resource fields to match a lower minor API version.
 * Currently handles v1.3 -> v1.2 downgrade for nodes (removes authorization field).
 */
function stripResourceToLowerMinor(
  plural: ResourcePlural,
  obj: Record<string, unknown>,
  target: ParsedIs04Version,
): Record<string, unknown> {
  if (target.major === 1 && target.minor <= 2 && plural === "nodes") {
    return stripNodeV13TowardsV12(obj);
  }
  return { ...obj };
}

/**
 * Strips v1.3-specific fields from Node resources for v1.2 compatibility.
 * Removes the 'authorization' field from api.endpoints[] entries.
 */
function stripNodeV13TowardsV12(
  obj: Record<string, unknown>,
): Record<string, unknown> {
  const out = structuredClone(obj) as Record<string, unknown>;
  const api = out.api as
    { endpoints?: Array<Record<string, unknown>> } | undefined;
  if (api?.endpoints) {
    for (const ep of api.endpoints) {
      delete ep.authorization;
    }
  }
  return out;
}
