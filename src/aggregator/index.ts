import { Kafka } from "kafkajs";
import Redis from "ioredis";
import { createWindow, ingest, advance, topK, type SlidingWindow } from "./window.ts";

const KAFKA_TOPIC = "edits";
const TOP_N_SEALED = 50;

const kafka = new Kafka({ clientId: "aggregator", brokers: ["localhost:9092"] });
const consumer = kafka.consumer({ groupId: "aggregator" });
const redis = new Redis();

const dims: Record<string, SlidingWindow> = {
  editors: createWindow(),
  pages: createWindow(),
  wikis: createWindow(),
  humans_vs_bots: createWindow(),
};

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

      for (const dim of Object.keys(dims)) {
        ingest(dims[dim], memberFor(dim, event), bucketTs);
      }
    },
  });
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
}

setInterval(() => {
  sealToRedis().catch((e) => console.error("seal failed:", e));
}, 1000);

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
