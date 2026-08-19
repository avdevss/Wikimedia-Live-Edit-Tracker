import { WebSocketServer } from "ws";
import Redis from "ioredis";

const PORT = 8080;
const TOP_N_UI = 20;
const DIMENSIONS = ["editors", "pages", "wikis", "humans_vs_bots"];
const WINDOW_MS = 300_000;

const redis = new Redis();
const wss = new WebSocketServer({ port: PORT });

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

wss.on("connection", async (ws) => {
  const snapshot = await buildSnapshot();
  ws.send(JSON.stringify(snapshot));
});

console.log(`api server listening on ws://localhost:${PORT}`);
