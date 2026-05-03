import cassandra from "cassandra-driver";
import { describe, expect, it } from "vitest";
import { InMemoryRegistryStore } from "../store/inMemoryRegistryStore.js";
import type { SubscriptionEventSink } from "./subscriptionEventSink.js";
import { ChangePoller } from "./changePoller.js";

describe("ChangePoller", () => {
  it("delivers new change_log rows to the sink once per event", async () => {
    const store = new InMemoryRegistryStore();
    const seen: string[] = [];
    const sink: SubscriptionEventSink = {
      dispatchEvent(ev) {
        seen.push(`${ev.action}:${ev.resource_id.toString()}`);
      },
    };
    const poller = new ChangePoller(store, sink, 60_000);
    const nid = cassandra.types.Uuid.random();
    await store.upsertResource(
      "nodes",
      nid,
      "v1.3",
      `{"id":"${nid.toString()}","label":"a"}`,
    );
    await poller.poll();
    expect(seen).toEqual([`create:${nid.toString()}`]);
    await poller.poll();
    expect(seen).toEqual([`create:${nid.toString()}`]);
  });
});
