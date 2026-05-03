/**
 * Tests for grain message construction.
 *
 * Tests that grain messages are built correctly according to IS-04 specification.
 */
import { describe, expect, it } from "vitest";
import { buildGrainMessage } from "./grain.js";

describe("buildGrainMessage", () => {
  /**
   * Tests that grain messages are emitted in IS-04 format with correct structure.
   */
  it("emits IS-04 style grain envelope", () => {
    const raw = buildGrainMessage({
      config: { queryApiSourceId: "11111111-1111-4111-8111-111111111111" },
      subscriptionId: "22222222-2222-4222-8222-222222222222",
      topicPlural: "nodes",
      changes: [
        { path: "33333333-3333-4333-8333-333333333333", post: { label: "n" } },
      ],
    });
    const g = JSON.parse(raw) as Record<string, unknown>;
    expect(g.grain_type).toBe("event");
    expect(g.source_id).toBe("11111111-1111-4111-8111-111111111111");
    expect(g.flow_id).toBe("22222222-2222-4222-8222-222222222222");
    const grain = g.grain as { topic: string; data: unknown[] };
    expect(grain.topic).toBe("/nodes/");
    expect(Array.isArray(grain.data)).toBe(true);
    expect((grain.data[0] as { path: string }).path).toBe(
      "33333333-3333-4333-8333-333333333333",
    );
  });
});
