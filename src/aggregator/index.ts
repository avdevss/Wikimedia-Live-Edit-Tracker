import { Kafka } from "kafkajs";
import Redis from "ioredis";
import { createWindow, ingest, advance, topK, WINDOW_SECONDS, type SlidingWindow } from "./window.ts";

const KAFKA_TOPIC = "edits";
const TOP_N_SEALED = 50;

const kafka = new Kafka({ clientId: "aggregator", brokers: ["localhost:9092"] });
const consumer = kafka.consumer({ groupId: "aggregator" });
const admin = kafka.admin();
const redis = new Redis();

const dims: Record<string, SlidingWindow> = {
  editors: createWindow(),
  pages: createWindow(),
  wikis: createWindow(),
  humans_vs_bots: createWindow(),
};

// Tracks the most recent event's own ingest_ts. Redis ZSETs only hold
// cumulative counts per member, not per-event timestamps, so this is the
// one value the API server needs read out of the aggregator to compute
// pipeline latency (producer receipt -> frame emit).
let latestIngestTs = 0;

function memberFor(dim: string, event: any): string {
  switch (dim) {
    case "editors":
      return event.user;
    case "pages":
      return `${event.wiki}:${event.title}`;
    case "wikis":
      return event.wiki;
    case "humans_vs_bots":
      return event.bot ? "bot" : "human";
    default:
      throw new Error(`unknown dimension: ${dim}`);
  }
}

// Seeks every partition to the offset nearest "now minus the window size,"
// regardless of whatever this consumer group last committed. On restart we
// only ever care about the trailing 5 minutes anyway, so replaying from
// there refills the window in ~2s instead of either resuming from a
// possibly stale committed offset or waiting 5 minutes of live traffic.
async function warmStartSeek() {
  await admin.connect();
  const seekTs = Date.now() - WINDOW_SECONDS * 1000;
  const offsets = await admin.fetchTopicOffsetsByTimestamp(KAFKA_TOPIC, seekTs);
  await admin.disconnect();

  for (const { partition, offset } of offsets) {
    if (offset === "-1") continue; // no messages that far back on this partition
    consumer.seek({ topic: KAFKA_TOPIC, partition, offset });
  }
  console.log(`warm start: seeked ${offsets.length} partitions to ~${WINDOW_SECONDS}s ago`);
}

async function main() {
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      // Bucket by when the event actually entered our system (stamped once,
      // by the producer), not by when the aggregator happened to read it.
      // Those two differ during warm-start replay and fast catch-up after
      // a restart, which would otherwise cram minutes of events into one
      // or two buckets.
      const bucketTs = Math.floor(event.ingest_ts / 1000);
      latestIngestTs = Math.max(latestIngestTs, event.ingest_ts);

      for (const dim of Object.keys(dims)) {
        ingest(dims[dim], memberFor(dim, event), bucketTs);
      }
    },
  });

  await warmStartSeek();
}

async function sealToRedis() {
  // Ages every window forward on the wall clock, even for dimensions that
  // received no events this tick. Without this, a dimension with no
  // traffic (or the whole aggregator during a producer outage) would
  // never expire its buckets and Redis would serve a frozen leaderboard
  // forever instead of draining toward empty.
  const wallClockTs = Math.floor(Date.now() / 1000);

  for (const dim of Object.keys(dims)) {
    advance(dims[dim], wallClockTs);
    const top = topK(dims[dim], TOP_N_SEALED);

    const nextKey = `lb:${dim}:next`;
    const winKey = `lb:${dim}:win`;

    if (top.length === 0) {
      await redis.del(winKey);
      continue;
    }

    const zaddArgs: (string | number)[] = [];
    for (const [member, score] of top) zaddArgs.push(score, member);

    const results = await redis
      .multi()
      .del(nextKey)
      .zadd(nextKey, ...zaddArgs)
      .rename(nextKey, winKey)
      .exec();

    const failed = results?.find(([err]) => err);
    if (failed) console.error(`seal ${dim} failed:`, failed[0]);
  }

  if (latestIngestTs > 0) {
    await redis.set("lb:latest_ingest_ts", latestIngestTs);
  }
}

setInterval(() => {
  sealToRedis().catch((e) => console.error("seal failed:", e));
}, 1000);

async function shutdown() {
  console.log("\nleaving consumer group cleanly...");
  await consumer.disconnect();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
