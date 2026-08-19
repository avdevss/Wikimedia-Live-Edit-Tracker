import WebSocket, { WebSocketServer } from "ws";
import Redis from "ioredis";
import { Kafka } from "kafkajs";

const PORT = 8080;
const TOP_N_UI = 20;
const DIMENSIONS = ["editors", "pages", "wikis", "humans_vs_bots"];
const WINDOW_MS = 300_000;
const POLL_MS = 250;
const STATS_MS = 1000;
const KAFKA_TOPIC = "edits";
const CONSUMER_GROUP = "aggregator";
const LATENCY_SAMPLE_WINDOW = 60; // rolling ~60s of once-per-second samples

const redis = new Redis();
const wss = new WebSocketServer({ port: PORT });
const kafka = new Kafka({ clientId: "api", brokers: ["localhost:9092"] });
const admin = kafka.admin();

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

let prevHighWatermark: number | null = null;
let prevSampleTs: number | null = null;
const latencySamples: number[] = [];

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor(p * sorted.length));
  return sorted[idx];
}

async function computeStats() {
  const [topicOffsets, groupOffsets] = await Promise.all([
    admin.fetchTopicOffsets(KAFKA_TOPIC),
    admin.fetchOffsets({ groupId: CONSUMER_GROUP, topics: [KAFKA_TOPIC] }),
  ]);

  const committed = new Map<number, number>();
  for (const p of groupOffsets[0]?.partitions ?? []) {
    committed.set(p.partition, Number(p.offset));
  }

  let totalHigh = 0;
  let consumerLag = 0;
  for (const p of topicOffsets) {
    const high = Number(p.offset);
    totalHigh += high;
    consumerLag += Math.max(0, high - (committed.get(p.partition) ?? 0));
  }

  const now = Date.now();
  const eps =
    prevHighWatermark === null || prevSampleTs === null
      ? 0
      : (totalHigh - prevHighWatermark) / ((now - prevSampleTs) / 1000);
  prevHighWatermark = totalHigh;
  prevSampleTs = now;

  const rawLatestIngestTs = await redis.get("lb:latest_ingest_ts");
  if (rawLatestIngestTs) {
    latencySamples.push(now - Number(rawLatestIngestTs));
    if (latencySamples.length > LATENCY_SAMPLE_WINDOW) latencySamples.shift();
  }
  const sortedLatency = [...latencySamples].sort((a, b) => a - b);

  broadcast({
    type: "stats",
    eps: Math.round(eps * 10) / 10,
    consumer_lag: consumerLag,
    latency_ms: {
      p50: percentile(sortedLatency, 0.5),
      p95: percentile(sortedLatency, 0.95),
      p99: percentile(sortedLatency, 0.99),
    },
  });
}

wss.on("connection", async (ws) => {
  const snapshot = await buildSnapshot();
  ws.send(JSON.stringify(snapshot));
});

async function main() {
  await admin.connect();

  setInterval(() => {
    pollAndBroadcastDeltas().catch((e) => console.error("poll failed:", e));
  }, POLL_MS);

  setInterval(() => {
    computeStats().catch((e) => console.error("stats failed:", e));
  }, STATS_MS);

  console.log(`api server listening on ws://localhost:${PORT}`);
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
