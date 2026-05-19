/**
 * WebSocket subscription manager for NMOS Query API.
 *
 * This module manages WebSocket subscriptions, handles client connections,
 * dispatches change events as grain messages, and persists subscriptions
 * for multi-instance consistency.
 */
import type { WebSocket } from "ws";
import cassandra from "cassandra-driver";
import type { RegistryConfig } from "../config.js";
import { buildGrainMessage, type GrainChange } from "../grain.js";
import {
  parseIs04Version,
  parseQueryDowngrade,
  shouldIncludeStoredResource,
} from "../query/downgrade.js";
import { matchesFilters, parseResourceObject } from "../query/resourceQuery.js";
import { projectResourceForQuery } from "../query/versionProjection.js";
import type {
  ChangeEventRow,
  RegistryPort,
  ResourcePlural,
} from "../store/registryPort.js";
import type { SubscriptionEventSink } from "./subscriptionEventSink.js";
import logger from "../logger.js";

/**
 * Definition of a WebSocket subscription.
 * Contains all parameters needed to create and manage a subscription.
 */
export type SubscriptionDef = {
  id: string;
  ws_href: string;
  max_update_rate_ms: number;
  persist: boolean;
  secure: boolean;
  authorization: boolean;
  resource_path: string;
  params: Record<string, unknown>;
  /** Query API URL version used when creating this subscription (e.g. `v1.3`). */
  queryApiPathVersion: string;
};

/**
 * Maps resource paths to their plural resource types.
 */
const PATH_TO_PLURAL: Record<string, ResourcePlural> = {
  "/nodes": "nodes",
  "/devices": "devices",
  "/sources": "sources",
  "/flows": "flows",
  "/senders": "senders",
  "/receivers": "receivers",
};

/**
 * Subscription parameters that should be skipped when matching filters.
 */
const PARAMS_SKIP = new Set(["query.downgrade"]);

/**
 * Internal subscription state with WebSocket connections and buffering.
 */
type Internal = {
  def: SubscriptionDef;
  /**
   * Whether this in-memory entry is backed by a row in `persisted_subscriptions`.
   * Used to avoid sync/deletion races while a locally-created subscription is still being written.
   */
  syncedFromStore: boolean;
  /**
   * Timestamp (Date.now()) when syncedFromStore was last set to true.
   * Used to skip premature deletion when a DB snapshot predates the subscription write.
   */
  syncedAt: number;
  sockets: Set<WebSocket>;
  socketStates: Map<
    WebSocket,
    { syncing: boolean; buffered: ChangeEventRow[] }
  >;
  lastSendAt: number;
  queued: ChangeEventRow[];
  flushTimer: ReturnType<typeof setTimeout> | null;
};

/**
 * Manages WebSocket subscriptions for the Query API.
 * Handles subscription lifecycle, WebSocket connections, and change event dispatch.
 */
export class SubscriptionManager implements SubscriptionEventSink {
  private readonly config: RegistryConfig;
  private readonly store: RegistryPort;
  private readonly subs = new Map<string, Internal>();

  /**
   * Creates a new subscription manager.
   */
  constructor(config: RegistryConfig, store: RegistryPort) {
    this.config = config;
    this.store = store;
  }

  private async sleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, ms);
    });
  }

  /**
   * Builds the WebSocket href URL for a subscription.
   * Uses the configured public WebSocket base and adjusts protocol based on secure flag.
   */
  private buildWsHref(id: string, secure: boolean): string {
    // `publicWsBase` is expected to be something like `ws://host:port`.
    // We override the protocol to match `secure` so `ws_href` is reachable by clients.
    const base = this.config.publicWsBase.replace(/\/$/, "");
    const u = new URL(base);
    u.protocol = secure ? "wss:" : "ws:";
    return `${u.origin}/ws/?uid=${id}`;
  }

  /**
   * Converts a resource path to its plural resource type.
   */
  pathToPlural(path: string): ResourcePlural | null {
    return PATH_TO_PLURAL[path] ?? null;
  }

  /**
   * Converts subscription parameters to filter-compatible format.
   * Skips special parameters like query.downgrade.
   */
  private subscriptionMatchParams(
    params: Record<string, unknown>,
  ): Record<string, string> {
    const o: Record<string, string> = {};
    for (const [k, v] of Object.entries(params)) {
      if (PARAMS_SKIP.has(k)) continue;
      if (
        typeof v === "string" ||
        typeof v === "number" ||
        typeof v === "boolean"
      ) {
        o[k] = String(v);
      } else {
        o[k] = JSON.stringify(v);
      }
    }
    return o;
  }

  /**
   * Builds grain changes for the initial snapshot sent to new WebSocket clients.
   * Applies version projection and filtering based on subscription parameters.
   */
  private buildSnapshotChanges(
    internal: Internal,
    rows: Awaited<ReturnType<RegistryPort["listResources"]>>,
  ): GrainChange[] {
    const plural = this.pathToPlural(internal.def.resource_path);
    if (!plural) return [];

    const pathVp = parseIs04Version(internal.def.queryApiPathVersion);
    if (!pathVp) return [];

    const downRaw =
      typeof internal.def.params["query.downgrade"] === "string"
        ? internal.def.params["query.downgrade"]
        : undefined;
    const dg = parseQueryDowngrade(internal.def.queryApiPathVersion, downRaw);
    const downgrade = dg.ok ? dg.downgrade : null;
    const filters = this.subscriptionMatchParams(internal.def.params);

    const changes: GrainChange[] = [];
    for (const row of rows) {
      if (!shouldIncludeStoredResource(pathVp, row.api_version, downgrade))
        continue;
      const obj = parseResourceObject(row.json);
      if (Object.keys(filters).length && !matchesFilters(obj, filters))
        continue;
      const projected = projectResourceForQuery(
        plural,
        obj,
        pathVp,
        row.api_version,
      );
      changes.push({
        path: row.id.toString(),
        pre: projected,
        post: projected,
      });
    }
    return changes;
  }

  /**
   * Sends the initial resource snapshot to a newly connected WebSocket client.
   */
  private async sendInitialState(
    internal: Internal,
    ws: WebSocket,
  ): Promise<void> {
    const plural = this.pathToPlural(internal.def.resource_path);
    if (!plural || ws.readyState !== ws.OPEN) return;

    const rows = await this.store.listResources(plural);
    const changes = this.buildSnapshotChanges(internal, rows);
    if (ws.readyState !== ws.OPEN) return;

    const msg = buildGrainMessage({
      config: this.config,
      subscriptionId: internal.def.id,
      topicPlural: plural,
      changes,
    });
    ws.send(msg);
    internal.lastSendAt = Date.now();
  }

  /**
   * Restores persisted subscriptions from the store on startup.
   * Removes non-persisted subscriptions and rebuilds WebSocket hrefs.
   */
  async restorePersisted() {
    const rows = await this.store.listPersistedSubscriptions();
    const fallbackVersion =
      this.config.supportedApiVersions[
        this.config.supportedApiVersions.length - 1
      ] ?? "v1.3";
    logger.info("Restoring persisted subscriptions", { count: rows.length });
    for (const row of rows) {
      const def = JSON.parse(row.json) as SubscriptionDef;
      if (!def.queryApiPathVersion) {
        def.queryApiPathVersion = fallbackVersion;
      }
      def.ws_href = this.buildWsHref(def.id, def.secure);
      // Persist=false subscriptions should not survive a process restart on this instance:
      // there are no connected WebSocket clients after restart. Skip restoring them into
      // memory, but do NOT delete the DB row — other instances in the cluster may still
      // be serving live WebSocket clients for these subscriptions. The DB TTL handles
      // orphan cleanup once no instance writes a refresh.
      if (!def.persist) {
        continue;
      }
      this.subs.set(def.id, {
        def,
        syncedFromStore: true,
        syncedAt: Date.now(),
        sockets: new Set(),
        socketStates: new Map(),
        lastSendAt: 0,
        queued: [],
        flushTimer: null,
      });
    }
    logger.info("Persisted subscriptions restored", { count: this.subs.size });
  }

  /**
   * Creates a new subscription and persists it to the store.
   */
  async createSubscription(
    partial: Omit<SubscriptionDef, "id" | "ws_href">,
  ): Promise<SubscriptionDef> {
    const id = cassandra.types.Uuid.random().toString();
    const def: SubscriptionDef = {
      id,
      max_update_rate_ms: partial.max_update_rate_ms,
      persist: partial.persist,
      secure: partial.secure,
      authorization: partial.authorization,
      resource_path: partial.resource_path,
      params: partial.params,
      queryApiPathVersion: partial.queryApiPathVersion,
      ws_href: this.buildWsHref(id, partial.secure),
    };
    this.subs.set(id, {
      def,
      syncedFromStore: false,
      syncedAt: 0,
      sockets: new Set(),
      socketStates: new Map(),
      lastSendAt: 0,
      queued: [],
      flushTimer: null,
    });
    // Persisted storage is required for multi-instance Query API consistency (`GET /subscriptions`).
    // The `persist` flag only controls deletion after the last WS client disconnects.
    await this.store.savePersistedSubscription(
      cassandra.types.Uuid.fromString(def.id),
      JSON.stringify(def),
      this.config.persistedSubscriptionTtlSeconds,
    );
    const entry = this.subs.get(id)!;
    entry.syncedFromStore = true;
    entry.syncedAt = Date.now();
    logger.info("Subscription created in manager", {
      id,
      resource_path: def.resource_path,
      persist: def.persist,
    });
    return def;
  }

  /**
   * Retrieves a subscription definition by ID.
   * Falls back to the store on a cache miss so that GET /subscriptions/:id
   * succeeds even when the POST landed on a different instance and the sync
   * poller has not yet run on this one.
   */
  async get(defId: string): Promise<SubscriptionDef | null> {
    const cached = this.subs.get(defId);
    if (cached) return cached.def;

    const fallbackVersion =
      this.config.supportedApiVersions[
        this.config.supportedApiVersions.length - 1
      ] ?? "v1.3";

    let row: { id: cassandra.types.Uuid; json: string } | null;
    try {
      row = await this.store.getPersistedSubscription(
        cassandra.types.Uuid.fromString(defId),
      );
    } catch {
      return null;
    }
    if (!row) return null;

    const def = JSON.parse(row.json) as SubscriptionDef;
    if (!def.queryApiPathVersion) def.queryApiPathVersion = fallbackVersion;
    def.ws_href = this.buildWsHref(def.id, def.secure);

    this.subs.set(defId, {
      def,
      syncedFromStore: true,
      syncedAt: Date.now(),
      sockets: new Set(),
      socketStates: new Map(),
      lastSendAt: 0,
      queued: [],
      flushTimer: null,
    });
    return def;
  }

  /**
   * Lists all subscription definitions.
   */
  list(): SubscriptionDef[] {
    return [...this.subs.values()].map((s) => s.def);
  }

  /**
   * Deletes a subscription and closes all connected WebSockets.
   */
  async delete(defId: string): Promise<boolean> {
    const s = this.subs.get(defId);
    if (!s) return false;
    for (const sock of s.sockets) {
      try {
        sock.close();
      } catch {
        /* ignore */
      }
    }
    if (s.flushTimer) clearTimeout(s.flushTimer);
    this.subs.delete(defId);
    await this.store.deletePersistedSubscription(
      cassandra.types.Uuid.fromString(defId),
    );
    logger.info("Subscription deleted from manager", { defId });
    return true;
  }

  /**
   * Re-writes persisted_subscriptions rows for any subscription that currently has
   * active WebSocket sockets, resetting the per-row TTL clock. Call this on the
   * same cadence as syncPersistedFromStore() so rows never expire while a client
   * is still connected.
   */
  async refreshPersistedTtls(): Promise<void> {
    for (const [id, internal] of this.subs.entries()) {
      if (internal.sockets.size === 0) continue;
      await this.store.savePersistedSubscription(
        cassandra.types.Uuid.fromString(id),
        JSON.stringify(internal.def),
        this.config.persistedSubscriptionTtlSeconds,
      );
    }
  }

  /**
   * Reconciles persisted subscriptions from the store with local in-memory state.
   * Adds new subscriptions, updates existing ones, and removes deleted ones.
   */
  async syncPersistedFromStore(): Promise<void> {
    const rows = await this.store.listPersistedSubscriptions();
    const fallbackVersion =
      this.config.supportedApiVersions[
        this.config.supportedApiVersions.length - 1
      ] ?? "v1.3";

    const remoteById = new Map<string, SubscriptionDef>();
    for (const row of rows) {
      const def = JSON.parse(row.json) as SubscriptionDef;
      if (!def.queryApiPathVersion) def.queryApiPathVersion = fallbackVersion;
      // Recompute href for *this instance* so it's reachable from where clients are connecting.
      def.ws_href = this.buildWsHref(def.id, def.secure);
      remoteById.set(def.id, def);
    }

    const snapshotAt = Date.now();
    // Remove any locally-known persisted subscriptions that disappeared from the DB.
    // Skip entries whose syncedAt is newer than the snapshot time: the DB query was
    // issued before savePersistedSubscription completed, so the absence is a stale read.
    const toDelete: string[] = [];
    for (const [id, internal] of this.subs.entries()) {
      if (!internal.syncedFromStore) continue;
      if (internal.syncedAt > snapshotAt) continue;
      if (!remoteById.has(id)) toDelete.push(id);
    }
    for (const id of toDelete) {
      await this.delete(id);
    }

    // Add missing persisted subscriptions and refresh definitions for existing ones.
    for (const [id, remoteDef] of remoteById.entries()) {
      const internal = this.subs.get(id);
      if (internal) {
        internal.def = remoteDef;
        internal.syncedFromStore = true;
        internal.syncedAt = Date.now();
        continue;
      }
      this.subs.set(id, {
        def: remoteDef,
        syncedFromStore: true,
        syncedAt: Date.now(),
        sockets: new Set(),
        socketStates: new Map(),
        lastSendAt: 0,
        queued: [],
        flushTimer: null,
      });
    }
  }

  /**
   * Builds a grain message from a batch of change events.
   * Applies version projection and filtering based on subscription parameters.
   */
  private buildBatchMessage(
    internal: Internal,
    batch: ChangeEventRow[],
  ): string | null {
    if (batch.length === 0) return null;
    const topicPlural = this.pathToPlural(internal.def.resource_path);
    if (!topicPlural) return null;
    const pathVp = parseIs04Version(internal.def.queryApiPathVersion);
    if (!pathVp) return null;

    const changes: GrainChange[] = [];
    for (const ev of batch) {
      const idStr = ev.resource_id.toString();
      const ch: GrainChange = { path: idStr };
      const storedVer = ev.resource_api_version ?? "";
      if (ev.pre_json) {
        ch.pre = storedVer
          ? projectResourceForQuery(
              topicPlural,
              parseResourceObject(ev.pre_json),
              pathVp,
              storedVer,
            )
          : parseResourceObject(ev.pre_json);
      }
      if (ev.post_json) {
        ch.post = storedVer
          ? projectResourceForQuery(
              topicPlural,
              parseResourceObject(ev.post_json),
              pathVp,
              storedVer,
            )
          : parseResourceObject(ev.post_json);
      }
      changes.push(ch);
    }

    return buildGrainMessage({
      config: this.config,
      subscriptionId: internal.def.id,
      topicPlural,
      changes,
    });
  }

  /**
   * Attaches a WebSocket connection to a subscription.
   * Sends initial state and handles connection lifecycle.
   */
  async attachSocket(defId: string, ws: WebSocket): Promise<boolean> {
    let s = this.subs.get(defId);
    if (!s) {
      // In multi-instance deployments, POST /subscriptions and GET /ws may hit
      // different instances. Try a short sync window before failing attach.
      const attempts = 3;
      for (let i = 0; i < attempts && !s; i += 1) {
        await this.syncPersistedFromStore();
        s = this.subs.get(defId);
        if (!s && i < attempts - 1) {
          await this.sleep(20);
        }
      }
    }
    if (!s) return false;
    logger.info("Attaching WebSocket to subscription", { defId });
    s.sockets.add(ws);
    s.socketStates.set(ws, { syncing: true, buffered: [] });
    ws.on("close", () => {
      const cur = this.subs.get(defId);
      if (!cur) return;
      cur.sockets.delete(ws);
      cur.socketStates.delete(ws);
      logger.debug("WebSocket closed", {
        defId,
        remainingSockets: cur.sockets.size,
      });
      if (!cur.def.persist && cur.sockets.size === 0) {
        void this.delete(defId);
      }
    });
    await this.sendInitialState(s, ws);
    const state = s.socketStates.get(ws);
    if (!state || ws.readyState !== ws.OPEN) return true;
    state.syncing = false;
    if (state.buffered.length > 0) {
      const msg = this.buildBatchMessage(
        s,
        state.buffered.splice(0, state.buffered.length),
      );
      if (msg && ws.readyState === ws.OPEN) {
        ws.send(msg);
        s.lastSendAt = Date.now();
      }
    }
    logger.info("WebSocket attached successfully", { defId });
    return true;
  }

  /**
   * Dispatches a change event to all matching subscriptions.
   * Filters by resource type, version, and subscription parameters.
   */
  dispatchEvent(ev: ChangeEventRow) {
    for (const internal of this.subs.values()) {
      const plural = this.pathToPlural(internal.def.resource_path);
      if (!plural || plural !== ev.resource_type) continue;

      const pathVp = parseIs04Version(internal.def.queryApiPathVersion);
      if (!pathVp || !ev.resource_api_version) continue;

      const downRaw =
        typeof internal.def.params["query.downgrade"] === "string"
          ? internal.def.params["query.downgrade"]
          : undefined;
      const dg = parseQueryDowngrade(internal.def.queryApiPathVersion, downRaw);
      const downgrade = dg.ok ? dg.downgrade : null;
      if (
        !shouldIncludeStoredResource(pathVp, ev.resource_api_version, downgrade)
      )
        continue;

      const preObj = ev.pre_json ? parseResourceObject(ev.pre_json) : null;
      const postObj = ev.post_json ? parseResourceObject(ev.post_json) : null;
      const filters = this.subscriptionMatchParams(internal.def.params);
      const hasFilters = Object.keys(filters).length > 0;
      const preMatches =
        preObj !== null && (!hasFilters || matchesFilters(preObj, filters));
      const postMatches =
        postObj !== null && (!hasFilters || matchesFilters(postObj, filters));
      if (!preMatches && !postMatches) continue;
      const evToDispatch: ChangeEventRow =
        preMatches && !postMatches
          ? { ...ev, post_json: null }
          : !preMatches && postMatches
            ? { ...ev, pre_json: null }
            : ev;
      for (const state of internal.socketStates.values()) {
        if (state.syncing) state.buffered.push(evToDispatch);
      }
      this.enqueue(internal, evToDispatch);
    }
  }

  /**
   * Enqueues a change event for rate-limited batched sending.
   */
  private enqueue(internal: Internal, ev: ChangeEventRow) {
    internal.queued.push(ev);
    if (internal.flushTimer) return;
    const schedule = () => {
      internal.flushTimer = null;
      if (internal.queued.length === 0) return;
      const batch = internal.queued.splice(0, internal.queued.length);
      this.flushBatch(internal, batch);
      if (internal.queued.length > 0) {
        const waitNext = Math.max(
          0,
          internal.def.max_update_rate_ms - (Date.now() - internal.lastSendAt),
        );
        internal.flushTimer = setTimeout(schedule, waitNext);
      }
    };
    const wait = Math.max(
      0,
      internal.def.max_update_rate_ms - (Date.now() - internal.lastSendAt),
    );
    internal.flushTimer = setTimeout(schedule, wait);
  }

  /**
   * Flushes a batch of change events to all connected WebSocket clients.
   */
  private flushBatch(internal: Internal, batch: ChangeEventRow[]) {
    if (batch.length === 0 || internal.sockets.size === 0) return;
    const msg = this.buildBatchMessage(internal, batch);
    if (!msg) return;
    internal.lastSendAt = Date.now();
    for (const sock of internal.sockets) {
      const state = internal.socketStates.get(sock);
      if (state?.syncing) continue;
      if (sock.readyState === sock.OPEN) {
        sock.send(msg);
      }
    }
  }
}
