import { Kafka } from "kafkajs";
import Redis from "ioredis";
import { createWindow, ingest, topK, type SlidingWindow } from "./window.ts";

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
      const bucketTs = Math.floor(Date.now() / 1000);

      for (const dim of Object.keys(dims)) {
        ingest(dims[dim], memberFor(dim, event), bucketTs);
      }
    },
  });
}

async function sealToRedis() {
  for (const dim of Object.keys(dims)) {
    const top = topK(dims[dim], TOP_N_SEALED);
    if (top.length === 0) continue; // nothing to seal yet, leave prior state as-is

    const nextKey = `lb:${dim}:next`;
    const winKey = `lb:${dim}:win`;
    const zaddArgs: (string | number)[] = [];
    for (const [member, score] of top) zaddArgs.push(score, member);

    await redis
      .multi()
      .del(nextKey)
      .zadd(nextKey, ...zaddArgs)
      .rename(nextKey, winKey)
      .exec();
  }
}

setInterval(() => {
  sealToRedis().catch((e) => console.error("seal failed:", e));
}, 1000);

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
