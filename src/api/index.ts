import WebSocket, { WebSocketServer } from "ws";
import Redis from "ioredis";

const PORT = 8080;
const TOP_N_UI = 20;
const DIMENSIONS = ["editors", "pages", "wikis", "humans_vs_bots"];
const WINDOW_MS = 300_000;
const POLL_MS = 250;

const redis = new Redis();
const wss = new WebSocketServer({ port: PORT });

// What every connected client currently has, per dimension. Polling
// diffs against this to decide what's actually changed, so clients only
// receive the entries that moved, not the whole top 20 every tick.
const lastKnown: Record<string, Map<string, number>> = {};
for (const dim of DIMENSIONS) lastKnown[dim] = new Map();

async function readDim(dim: string): Promise<Array<[string, number]>> {
  const raw = await redis.zrevrange(`lb:${dim}:win`, 0, TOP_N_UI - 1, "WITHSCORES");
  const pairs: Array<[string, number]> = [];
  for (let i = 0; i < raw.length; i += 2) {
    pairs.push([raw[i], Number(raw[i + 1])]);
  }
  return pairs;
}

async function buildSnapshot() {
  const dims: Record<string, Array<[string, number]>> = {};
  for (const dim of DIMENSIONS) {
    dims[dim] = await readDim(dim);
  }
  return {
    type: "snapshot",
    window_ms: WINDOW_MS,
    server_ts: Date.now(),
    dims,
  };
}

function broadcast(msg: object) {
  const data = JSON.stringify(msg);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  }
}

async function pollAndBroadcastDeltas() {
  for (const dim of DIMENSIONS) {
    const current = new Map(await readDim(dim));
    const changes: Array<[string, number]> = [];
    const removed: string[] = [];

    for (const [member, score] of current) {
      if (lastKnown[dim].get(member) !== score) changes.push([member, score]);
    }
    for (const member of lastKnown[dim].keys()) {
      if (!current.has(member)) removed.push(member);
    }

    if (changes.length > 0 || removed.length > 0) {
      broadcast({ type: "delta", dim, server_ts: Date.now(), changes, removed });
    }

    lastKnown[dim] = current;
  }
}

wss.on("connection", async (ws) => {
  const snapshot = await buildSnapshot();
  ws.send(JSON.stringify(snapshot));
});

setInterval(() => {
  pollAndBroadcastDeltas().catch((e) => console.error("poll failed:", e));
}, POLL_MS);

console.log(`api server listening on ws://localhost:${PORT}`);
