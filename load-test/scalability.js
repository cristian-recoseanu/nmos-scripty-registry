#!/usr/bin/env node
/**
 * NMOS IS-04 scalability test — pure Node.js, no external test framework.
 *
 * Each node is simulated by an independent async task that:
 *   1. Registers itself via POST /x-nmos/registration/{version}/resource
 *   2. Starts a setInterval that fires exactly HEARTBEAT_INTERVAL_MS after
 *      registration completes, then every HEARTBEAT_INTERVAL_MS thereafter.
 *
 * Nodes are spawned at SPAWN_RATE per second to avoid thundering-herd pressure
 * on the registry during ramp-up.
 *
 * Usage:
 *   node load-test/scalability.js
 *
 * Environment variable overrides:
 *   BASE_URL              Target registry URL            (default: http://127.0.0.1:8080)
 *   NODE_COUNT            Number of nodes to simulate   (default: 1000)
 *   SPAWN_RATE            Nodes spawned per second       (default: 50)
 *   HEARTBEAT_INTERVAL_S  Seconds between heartbeats     (default: 5)
 *   RUN_DURATION_S        How long to run after all nodes are up (default: 120)
 *   API_VERSION           NMOS Registration API version  (default: v1.3)
 */

import { randomUUID } from "crypto";

const BASE_URL            = process.env.BASE_URL            ?? "http://127.0.0.1:8080";
const NODE_COUNT          = parseInt(process.env.NODE_COUNT          ?? "1000", 10);
const SPAWN_RATE          = parseInt(process.env.SPAWN_RATE          ?? "50",   10);
const HEARTBEAT_INTERVAL_S = parseFloat(process.env.HEARTBEAT_INTERVAL_S ?? "5");
const RUN_DURATION_S      = parseInt(process.env.RUN_DURATION_S      ?? "120",  10);
const API_VERSION         = process.env.API_VERSION         ?? "v1.3";

const HEARTBEAT_INTERVAL_MS = HEARTBEAT_INTERVAL_S * 1000;
const REGISTER_PATH = `${BASE_URL}/x-nmos/registration/${API_VERSION}/resource`;

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------
const metrics = {
  registrations:  { ok: 0, fail: 0 },
  heartbeats:     { ok: 0, fail: 0 },
};

function recordReg(ok)  { ok ? metrics.registrations.ok++  : metrics.registrations.fail++;  }
function recordHb(ok)   { ok ? metrics.heartbeats.ok++     : metrics.heartbeats.fail++;     }

function printStats(label) {
  const r = metrics.registrations;
  const h = metrics.heartbeats;
  const hTotal = h.ok + h.fail;
  const hErrPct = hTotal ? ((h.fail / hTotal) * 100).toFixed(2) : "0.00";
  console.log(
    `[${label}] registrations ok=${r.ok} fail=${r.fail} | ` +
    `heartbeats ok=${h.ok} fail=${h.fail} (${hErrPct}% err)`
  );
}

// ---------------------------------------------------------------------------
// Per-node simulation
// ---------------------------------------------------------------------------
async function simulateNode(nodeId) {
  const url = REGISTER_PATH;
  const body = JSON.stringify({
    type: "node",
    data: { id: nodeId, label: `load-test-node-${nodeId}`, version: "1:0" },
  });
  const headers = { "Content-Type": "application/json" };

  // 1. Register
  let registered = false;
  try {
    const res = await fetch(url, { method: "POST", headers, body });
    if (res.status === 200 || res.status === 201) {
      registered = true;
      recordReg(true);
    } else {
      const text = await res.text().catch(() => "");
      console.error(`[register] ${nodeId} → ${res.status} ${text}`);
      recordReg(false);
    }
  } catch (err) {
    console.error(`[register] ${nodeId} → ${err.message}`);
    recordReg(false);
  }

  if (!registered) return;

  // 2. Heartbeat every HEARTBEAT_INTERVAL_MS, starting exactly one interval
  //    after the registration POST completed.
  const healthUrl =
    `${BASE_URL}/x-nmos/registration/${API_VERSION}/health/nodes/${nodeId}`;

  const sendHeartbeat = async () => {
    try {
      const res = await fetch(healthUrl, {
        method: "POST",
        headers,
        body: "{}",
      });
      if (res.status === 200) {
        recordHb(true);
      } else if (res.status === 404) {
        console.warn(`[heartbeat] ${nodeId} → 404 (evicted), re-registering`);
        recordHb(false);
        // Re-register then let the next interval fire the next heartbeat.
        try {
          const rRes = await fetch(url, { method: "POST", headers, body });
          if (rRes.status === 200 || rRes.status === 201) {
            recordReg(true);
          } else {
            recordReg(false);
          }
        } catch (err) {
          console.error(`[re-register] ${nodeId} → ${err.message}`);
          recordReg(false);
        }
      } else {
        const text = await res.text().catch(() => "");
        console.error(`[heartbeat] ${nodeId} → ${res.status} ${text}`);
        recordHb(false);
      }
    } catch (err) {
      console.error(`[heartbeat] ${nodeId} → ${err.message}`);
      recordHb(false);
    }
  };

  // Store the interval handle on a WeakRef-compatible structure so it can be
  // cleared during shutdown. We use a module-level Set for cleanup.
  const handle = setInterval(sendHeartbeat, HEARTBEAT_INTERVAL_MS);
  activeIntervals.add(handle);
}

// ---------------------------------------------------------------------------
// Shutdown / cleanup
// ---------------------------------------------------------------------------
const activeIntervals = new Set();

function shutdown(reason) {
  for (const h of activeIntervals) clearInterval(h);
  activeIntervals.clear();
  printStats(reason);

  const h = metrics.heartbeats;
  const hTotal = h.ok + h.fail;
  const errRate = hTotal ? h.fail / hTotal : 0;
  if (errRate > 0.01) {
    console.error(`ERROR: heartbeat error rate ${(errRate * 100).toFixed(2)}% exceeds 1% threshold`);
    process.exit(1);
  }
  process.exit(0);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
console.log(
  `Starting: ${NODE_COUNT} nodes, spawn ${SPAWN_RATE}/s, ` +
  `heartbeat every ${HEARTBEAT_INTERVAL_S}s, run ${RUN_DURATION_S}s after ramp-up`
);
console.log(`Target: ${BASE_URL}`);

// Spawn nodes at SPAWN_RATE/s
const spawnIntervalMs = 1000 / SPAWN_RATE;
let spawned = 0;

const spawnHandle = setInterval(async () => {
  const nodeId = randomUUID();
  void simulateNode(nodeId);
  spawned++;
  if (spawned >= NODE_COUNT) {
    clearInterval(spawnHandle);
    const rampDone = new Date().toISOString();
    console.log(`[${rampDone}] All ${NODE_COUNT} nodes spawned. Running for ${RUN_DURATION_S}s...`);
    const statsHandle = setInterval(() => printStats(new Date().toISOString()), 10_000);
    setTimeout(() => {
      clearInterval(statsHandle);
      shutdown("DONE");
    }, RUN_DURATION_S * 1000);
  }
}, spawnIntervalMs);
